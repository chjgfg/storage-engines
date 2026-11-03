//! 长跑维护：版本切片与低频全库收尾分开调度，不往 begin/commit 里塞全库重写。
//!
//! 普通 open 不启动线程。调用方可用 [`Maintenance::tick`] 接自己的维护线程，或持有
//! [`start_maintenance`] 返回的句柄。全库收尾仍会持有引擎锁，**没有延迟/内存硬上限**：
//! Bitcask 重写日志/blob，B+ 树回收页供复用（不保证文件缩短），LSM 下发过滤并按其压实
//! 策略回收，memory 仅裁版本。它不等同于四家都有一个“立即缩盘”操作。

use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::{EngineError, KvEngine, ReclamationStatus};
use crate::engine::Result;

#[derive(Debug, Clone)]
pub struct MaintenanceConfig {
    /// 后台两轮之间的休息时间；不是操作超时。
    pub interval: Duration,
    /// 逻辑版本切片，按完整键组结束，所以超长链可能超过此数。
    pub slice_rows: u64,
    /// 全库 vacuum 的最小间隔。None = 不自动全库收尾；Some(0) 非法。
    /// 启用维护器的默认值为 30 分钟，普通 open 本身不会启用维护器。
    pub full_vacuum_interval: Option<Duration>,
}

impl Default for MaintenanceConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(1),
            slice_rows: 16 * 1024,
            full_vacuum_interval: Some(Duration::from_secs(30 * 60)),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct MaintenanceStatus {
    pub slice_runs: u64,
    pub full_runs: u64,
    pub skipped_active: u64,
    pub versions_removed: u64,
    pub rows_scanned: u64,
    /// 持续观察到活跃事务阻塞的时间，在成功维护后清零；不是事务的精确年龄。
    pub blocked_for: Duration,
    pub reclamation: Option<ReclamationStatus>,
    pub last_error: Option<String>,
}

/// 不拥有数据库的维护驱动，可接到已有管理线程/定时任务上。不要从 Tokio worker 直接调用。
pub struct Maintenance {
    config: MaintenanceConfig,
    cursor: Option<Vec<u8>>,
    last_full: Instant,
    blocked_since: Option<Instant>,
    status: MaintenanceStatus,
}

impl Maintenance {
    pub fn new(config: MaintenanceConfig) -> Result<Self> {
        if config.interval.is_zero() || config.slice_rows == 0 || config.slice_rows == u64::MAX
            || config.full_vacuum_interval.is_some_and(|d| d.is_zero())
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "维护间隔和切片必须大于零，切片不能为 u64::MAX",
            ).into());
        }
        Ok(Self {
            config,
            cursor: None,
            last_full: Instant::now(),
            blocked_since: None,
            status: MaintenanceStatus::default(),
        })
    }

    pub fn status(&self) -> &MaintenanceStatus {
        &self.status
    }

    /// 每次最多做一个逻辑片或一次全库收尾。有活跃事务则保留进度，下次重试，不强制回滚别人。
    pub fn tick(&mut self, db: &dyn KvEngine) -> Result<()> {
        let full_runs = self.status.full_runs;
        let result = self.tick_at(db, Instant::now());
        if self.status.full_runs != full_runs {
            // 间隔从收尾完成算，避免一次重写比间隔还长时连续重写整库。
            self.last_full = Instant::now();
        }
        result
    }

    fn tick_at(&mut self, db: &dyn KvEngine, now: Instant) -> Result<()> {
        self.status.reclamation = db.reclamation_status();
        let full = self.config.full_vacuum_interval
            .is_some_and(|interval| now.saturating_duration_since(self.last_full) >= interval);
        let result = if full {
            // M81（方案第 2 项）之后 `vacuum()` 不再自带 merge ⇒ 全库收尾要自己补上那一步：
            // bitcask 硬删除腾出的空间只有 merge 才归还，而维护器的合同就是"长跑不靠调用方记得
            // 做维护"（`full_maintenance_waits_for_interval_and_active_reader` 钉的就是日志字节）。
            // 其余三家 merge 回 `Unsupported` —— 那是"本引擎没有这一步"，不是失败，不许记成错误。
            // 次数上这一刀是**净减**：改前每一次全库 vacuum 都附送一次整库重写，现在只在
            // 全库收尾里做一次，且 bounded 轮（长跑的常态）一次都不付。
            db.vacuum().and_then(|stats| {
                let round = crate::VacuumRound { stats, next_cursor: None };
                match db.merge() {
                    Err(EngineError::Unsupported { .. }) => Ok(round),
                    Err(e) => Err(e),
                    Ok(()) => Ok(round),
                }
            })
        } else {
            db.vacuum_bounded(self.cursor.as_deref(), self.config.slice_rows)
        };
        match result {
            Err(e) => {
                self.status.last_error = Some(e.to_string());
                Err(e)
            }
            Ok(round) => {
                self.status.last_error = None;
                if round.stats.skipped_active_txn {
                    self.status.skipped_active += 1;
                    let since = *self.blocked_since.get_or_insert(now);
                    self.status.blocked_for = now.saturating_duration_since(since);
                    return Ok(());
                }
                self.blocked_since = None;
                self.status.blocked_for = Duration::ZERO;
                self.status.versions_removed += round.stats.versions_removed as u64;
                self.status.rows_scanned += round.stats.rows_scanned;
                self.cursor = round.next_cursor;
                if full {
                    self.status.full_runs += 1;
                    self.last_full = now;
                } else {
                    self.status.slice_runs += 1;
                }
                Ok(())
            }
        }
    }
}

/// 停止信号可以打断休息，不能中断已开始的存储重写。Drop/stop 会等待当前操作完成。
pub struct MaintenanceHandle {
    stop: Arc<(Mutex<bool>, Condvar)>,
    status: Arc<Mutex<MaintenanceStatus>>,
    thread: Option<JoinHandle<()>>,
}

impl MaintenanceHandle {
    pub fn status(&self) -> MaintenanceStatus {
        self.status.lock().unwrap().clone()
    }

    pub fn stop(mut self) -> Result<MaintenanceStatus> {
        self.join()?;
        Ok(self.status())
    }

    fn join(&mut self) -> Result<()> {
        let (lock, wake) = &*self.stop;
        *lock.lock().unwrap() = true;
        wake.notify_all();
        if let Some(thread) = self.thread.take() {
            thread.join().map_err(|_| EngineError::Io(std::io::Error::other("维护线程 panic")))?;
        }
        Ok(())
    }
}

impl Drop for MaintenanceHandle {
    fn drop(&mut self) {
        if let Err(e) = self.join() {
            log::warn!("维护线程退出失败: {e}");
        }
    }
}

/// 为一个库启动一个可停止的维护线程。调用方必须保留句柄，否则立即停止。
/// bulk loader 要求单写者：bulk 导入前先 stop，finish 后再启动维护器。
///
/// ```no_run
/// use std::sync::Arc;
/// use storage_engines::{open_engine, KvEngine, MaintenanceConfig, start_maintenance};
/// let db: Arc<dyn KvEngine> = Arc::from(open_engine("memory", None)?);
/// let maintenance = start_maintenance(db.clone(), MaintenanceConfig::default())?;
/// // 应用工作；不要在仍持有长事务/库锁的回调里等待维护器退出。
/// maintenance.stop()?;
/// # Ok::<(), storage_engines::EngineError>(())
/// ```
pub fn start_maintenance(db: Arc<dyn KvEngine>, config: MaintenanceConfig) -> Result<MaintenanceHandle> {
    let interval = config.interval;
    let mut driver = Maintenance::new(config)?;
    let stop = Arc::new((Mutex::new(false), Condvar::new()));
    let status = Arc::new(Mutex::new(MaintenanceStatus::default()));
    let worker_stop = stop.clone();
    let worker_status = status.clone();
    let thread = std::thread::Builder::new().name("storage-maintenance".into()).spawn(move || {
        loop {
            let (lock, wake) = &*worker_stop;
            let guard = lock.lock().unwrap();
            let (guard, _) = wake.wait_timeout_while(guard, interval, |stopped| !*stopped).unwrap();
            if *guard { break; }
            drop(guard);
            if let Err(e) = driver.tick(db.as_ref()) {
                log::warn!("存储维护失败，下一轮重试: {e}");
            }
            *worker_status.lock().unwrap() = driver.status().clone();
        }
    })?;
    Ok(MaintenanceHandle { stop, status, thread: Some(thread) })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{assert, assert_eq, format, println, vec};
    use crate::{open_engine, open_engine_with, EngineConfig};

    struct Scratch(std::path::PathBuf);
    impl Scratch {
        fn new(tag: &str) -> Self {
            let path = crate::common::tmp_test_path("maintenance", tag, "");
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for Scratch {
        fn drop(&mut self) { let _ = std::fs::remove_dir_all(&self.0); }
    }

    #[test]
    fn all_engines_retry_blocked_auto_vacuum_without_new_writes() {
        for name in crate::engine::ENGINE_NAMES {
            let scratch = Scratch::new(name);
            let path = if *name == "bplus_tree" { scratch.0.join("db") } else { scratch.0.join("store") };
            let db = open_engine_with(*name, Some(&path), &EngineConfig {
                vacuum_auto_slice_rows: Some(8), ..EngineConfig::default()
            }).unwrap();
            let tx = db.begin();
            assert!(tx.set_raw(b"hot", b"original".to_vec()));
            tx.commit();
            drop(tx);
            let reader = db.begin();
            for n in 0..7 {
                let writer = db.begin();
                assert!(writer.set_raw(b"hot", format!("v{n}").into_bytes()));
                writer.commit();
            }
            db.begin().commit(); // 在 reader 挡住回收时，明确触发一次 tick
            let blocked = db.reclamation_status().unwrap();
            assert_eq!(blocked.pending_rows, 8, "{name}: 跳过不许吃掉账");
            assert!(blocked.skipped_active > 0);
            assert_eq!(blocked.oldest_active_version, Some(reader.version()));
            assert_eq!(reader.get(b"hot"), Some(b"original".to_vec()));
            reader.commit();
            drop(reader);
            let reader = db.begin(); // 不再写任何新行，也必须重试
            assert_eq!(reader.get(b"hot"), Some(b"v6".to_vec()));
            reader.commit();
            drop(reader);
            assert_eq!(db.health().max_chain, 1, "{name}: 无新写时不能永久停摆");
            let done = db.reclamation_status().unwrap();
            assert_eq!(done.pending_rows, 0);
            assert_eq!(done.completed_slices, 1);
            assert_eq!(done.versions_removed, 7);
        }
    }

    #[test]
    fn all_engines_keep_large_commit_debt() {
        for name in crate::engine::ENGINE_NAMES {
            let scratch = Scratch::new(name);
            let path = scratch.0.join("db");
            let db = open_engine_with(*name, Some(&path), &EngineConfig {
                vacuum_auto_slice_rows: Some(8), ..EngineConfig::default()
            }).unwrap();
            let writer = db.begin();
            for n in 0..32 {
                assert!(writer.set_raw(format!("k{n:03}").as_bytes(), vec![42]));
            }
            writer.commit();
            drop(writer);
            for expected in [24, 16, 8, 0] {
                db.begin().commit();
                assert_eq!(db.reclamation_status().unwrap().pending_rows, expected, "{name}");
            }
        }
    }

    /// 自动回收的扫描量必须跟**写入行数**走，不能跟**事务次数**走（M81 第 5 项后半的回归守卫）。
    ///
    /// 形状：先关着自动回收灌 `PREFILL` 行（库 ≫ 一片），再开成 `SLICE` 行/片，然后写 `WRITES` 个
    /// **单行**事务 —— 下游 trading-system 逐条 `persist` 就是这个形状，台架的 `灌`/`更新` 是它的
    /// 1000 行/500 行版本。
    ///
    /// 判据（计数，不看墙钟）：`completed_slices × SLICE ≤ 流前账 + WRITES + SLICE` ——
    /// 写多少行就扫多少行（1:1：预灌时关着自动回收攒下的账也算"写过"，在流里照样 1:1 还掉），
    /// 外加一片零头。没有"每个 begin 推一片"的额度：半路的轮次是维护器的活，不是事务的。
    ///
    /// 为什么要这条：`339a559` 让 `take_tick` 在"一轮开着"时对**每次 `begin()`** 都给一片许可
    /// （`charged = 0` 那一支），而写流里一轮永远开着（账一直攒、游标一绕完就立刻再开）⇒ 单行事务
    /// 每条都扫一整片 = `SLICE` 倍放大。`docs/test2.txt` 五档 20/20 格 `灌`/`更新` 变慢、
    /// memory `冷重开` 1.3 s → 4.4 s（重放也是 1000 行/页的写流）都是这笔税。
    /// 本条在那份实现上四家各读出 **275 片 ≈ 17 600 行**（上限 4 672）⇒ 红；M82 拿掉那一支后
    /// 读出 71 片 ≈ 4 544 行 = 4096 预灌账 + 512 写入，正好 1:1。
    #[test]
    fn auto_vacuum_scanning_keeps_pace_with_writes_not_with_transactions() {
        const SLICE: u64 = 64;
        const PREFILL: u64 = SLICE * 64;
        const WRITES: u64 = 512;
        let mut red: Vec<String> = Vec::new();
        for name in crate::engine::ENGINE_NAMES {
            let scratch = Scratch::new(name);
            let path = scratch.0.join("db");
            let db = open_engine_with(*name, Some(&path), &EngineConfig {
                vacuum_auto_slice_rows: Some(0), ..EngineConfig::default()
            }).unwrap();
            let tx = db.begin();
            for i in 0..PREFILL {
                assert!(tx.set_raw(format!("p{i:06}").as_bytes(), vec![b'x'; 16]), "{name}: 预灌第 {i} 行被拒");
            }
            tx.commit();
            drop(tx);
            db.set_vacuum_auto_slice(SLICE);
            let before = db.reclamation_status().unwrap();
            for i in 0..WRITES {
                let tx = db.begin();
                assert!(tx.set_raw(format!("w{i:06}").as_bytes(), vec![b'y'; 16]), "{name}: 第 {i} 条单行事务被拒");
                tx.commit();
            }
            let after = db.reclamation_status().unwrap();
            let slices = after.completed_slices - before.completed_slices;
            let scanned = slices * SLICE;
            // 预灌是关着自动回收写的，那 PREFILL 行的账还挂着，流里会按 1:1 还掉 ⇒ 算进上限
            let allowed = before.pending_rows + WRITES + SLICE;
            println!(
                "[auto-vac pace] {name}: 流前账 {} 行 + 写 {WRITES} 行 / {WRITES} 事务 ⇒ 扫 {slices} 片 ≈ {scanned} 行（上限 {allowed}，跳过 {} 片）",
                before.pending_rows,
                after.skipped_active - before.skipped_active
            );
            if scanned > allowed {
                red.push(format!("{name}: 扫了 ~{scanned} 行（{slices} 片）却只写了 {WRITES} 行（含流前账上限 {allowed}）"));
            }
        }
        // 四家跑完再判，一次把所有红的引擎都点出来
        assert!(
            red.is_empty(),
            "自动回收的扫描量跟着事务次数而不是写入行数走：\n  {}",
            red.join("\n  ")
        );
    }

    #[test]
    fn full_maintenance_waits_for_interval_and_active_reader() {
        let scratch = Scratch::new("physical");
        let db = open_engine("bitcask", Some(&scratch.0.join("store"))).unwrap();
        db.set_vacuum_auto_slice(0);
        for _ in 0..24 {
            let tx = db.begin();
            assert!(tx.set_raw(b"hot", vec![7; 512]));
            tx.commit();
        }
        db.flush().unwrap(); // commit 只持久化 WAL；先排空数据缓冲，才能比较真实日志字节。
        let log_bytes = || -> u64 {
            std::fs::read_dir(scratch.0.join("store")).unwrap()
                .map(|entry| entry.unwrap())
                .filter(|entry| {
                    let name = entry.file_name();
                    let name = name.to_string_lossy();
                    name.starts_with("data") && name.ends_with(".log")
                })
                .map(|entry| entry.metadata().unwrap().len())
                .sum()
        };
        let before = log_bytes();
        let mut driver = Maintenance::new(MaintenanceConfig {
            interval: Duration::from_secs(1), slice_rows: 8,
            full_vacuum_interval: Some(Duration::from_secs(60)),
        }).unwrap();
        let base = driver.last_full;
        let reader = db.begin();
        driver.tick_at(db.as_ref(), base + Duration::from_secs(61)).unwrap();
        driver.tick_at(db.as_ref(), base + Duration::from_secs(71)).unwrap();
        assert_eq!(driver.status().full_runs, 0);
        assert_eq!(driver.status().blocked_for, Duration::from_secs(10));
        reader.commit();
        drop(reader);
        driver.tick_at(db.as_ref(), base + Duration::from_secs(72)).unwrap();
        assert_eq!(driver.status().full_runs, 1);
        assert_eq!(driver.status().blocked_for, Duration::ZERO);
        let after = log_bytes();
        assert!(after < before / 2, "全库收尾必须真回收日志，不只是计数: {before} -> {after}");
        driver.tick_at(db.as_ref(), base + Duration::from_secs(73)).unwrap();
        assert_eq!(driver.status().full_runs, 1, "有界片到尾不许变成全库重写");
        let tx = db.begin();
        assert_eq!(tx.get(b"hot"), Some(vec![7; 512]));
        tx.commit();
    }

    #[test]
    fn maintenance_config_rejects_busy_loop_and_full_slice() {
        for cfg in [
            MaintenanceConfig { interval: Duration::ZERO, ..MaintenanceConfig::default() },
            MaintenanceConfig { slice_rows: 0, ..MaintenanceConfig::default() },
            MaintenanceConfig { slice_rows: u64::MAX, ..MaintenanceConfig::default() },
            MaintenanceConfig { full_vacuum_interval: Some(Duration::ZERO), ..MaintenanceConfig::default() },
        ] {
            assert!(Maintenance::new(cfg).is_err());
        }
        assert!(Maintenance::new(MaintenanceConfig { full_vacuum_interval: None, ..MaintenanceConfig::default() }).is_ok());
    }

    #[test]
    fn worker_stop_wakes_long_interval_and_joins() {
        let db: Arc<dyn KvEngine> = Arc::from(open_engine("memory", None).unwrap());
        let worker = start_maintenance(db.clone(), MaintenanceConfig {
            interval: Duration::from_secs(3600), ..MaintenanceConfig::default()
        }).unwrap();
        let status = worker.stop().unwrap();
        assert_eq!(status.full_runs, 0);
        assert_eq!(Arc::strong_count(&db), 1, "stop 必须释放数据库，不能遗留后台线程");
    }

    /// 方案第 5 项：长读事务挡住自动回收 → 放开之后**排空**，一条活键都不许丢（要规模才测得到）。
    ///
    /// 为什么不并进上面那几条小库测试：bplus 的 `vacuum_bounded` 按 `VAC_CHUNK_ROWS = 8192` 行
    /// 一块续跑，而续跑是**叶粒度**定位（降到含游标的叶子，从该片第一条重喂）。一把键的版本组
    /// 要**正好骑在块边界上**才走进"同一编码键被第二次判定"那条路径 —— 小库里一块装得下全表，
    /// 边界根本不出现。11-02 那条 `vacuum_chunk_resume_must_not_readjudicate_carried_group` 是把
    /// 块压到 4 行、在小库上**确定性**复现的；这一条是它的另一侧：**自然块大小 + 真规模**。
    ///
    /// 规模怎么定的（不是随手挑的大数）：200k 把键 × 2 版 = 40 万版本行 ⇒ 一次全库回收天然切出
    /// ~49 个块边界，每个边界约一半概率落在某把两版键的中间 ⇒ 期望二十几次"组骑边"，不靠运气。
    /// 判据一律走**计数**（逐键读回 + 排空次数），墙钟只打印不作判据。
    #[test]
    #[ignore = "约 30~90 s：自然块大小下的 bplus 长跑回收守卫（块边界要规模才出现）"]
    fn bplus_long_reader_does_not_lose_live_rows_at_scale() {
        const KEYS: usize = 200_000;
        const BATCH: usize = 5_000;
        let scratch = Scratch::new("bplus_scale_vac");
        let db = open_engine("bplus_tree", Some(&scratch.0.join("db"))).unwrap();
        let key = |i: usize| format!("{i:08}#/hot/x.json").into_bytes();
        let v1 = |i: usize| format!("base{i}").into_bytes();
        let v2 = |i: usize| format!("upd{i}").into_bytes();

        // 第一遍：基线，一把键一版。分批事务（一把 20 万键的写集本身就是一份大内存账）。
        for base in (0..KEYS).step_by(BATCH) {
            let end = (base + BATCH).min(KEYS);
            let items: Vec<(Vec<u8>, Vec<u8>)> = (base..end).map(|i| (key(i), v1(i))).collect();
            let tx = db.begin();
            if let Err(bad) = tx.batch_set(&items) {
                panic!("灌基线失败 @ {base}: 冲突 {bad:?}");
            }
            tx.commit();
        }

        // 长读事务挂在这里：它不释放，回收一片都不许裁（`skipped_active_txn` 那一支）。
        let reader = db.begin();
        // 第二遍：每把键更新 ⇒ 上一版的行全变垃圾，待回收账开始攒。
        for base in (0..KEYS).step_by(BATCH) {
            let end = (base + BATCH).min(KEYS);
            let items: Vec<(Vec<u8>, Vec<u8>)> = (base..end).map(|i| (key(i), v2(i))).collect();
            let tx = db.begin();
            if let Err(bad) = tx.batch_set(&items) {
                panic!("灌更新失败 @ {base}: 冲突 {bad:?}");
            }
            tx.commit();
        }
        let blocked = db.reclamation_status().expect("bplus 必须有回收账可读");
        assert!(blocked.pending_rows > 0, "更新 20 万把键之后必须攒出待回收账");

        reader.commit();
        drop(reader);
        // ⚠️ 方案正文点名的这一条：**排空**，不是一次 begin/commit。一片只裁 `slice` 行，账可能有
        // 几十片。"排空"的终点**不是 `pending_rows == 0`** —— `VacuumScheduler::take_tick`
        // （`common/kernel.rs`）攒够一片才取一片，不足一片的尾巴按设计留着（实测停在
        // 6784 = 400000 − 24×16384，`active_txns: 0`）。
        // M82 的口径：`begin()` 的扫描量与写入量 1:1，账花完就停，**游标可以停在半路**
        // （`sweep_in_progress` 如实报 true）—— 把它推到库尾是下面维护器的活，不是下一个事务的。
        // M81 第 5 项后半曾让 begin() 续推半路的轮次，代价是写流里每个事务扫一整片（守卫
        // `auto_vacuum_scanning_keeps_pace_with_writes_not_with_transactions`），已拿掉。
        let slice = db.vacuum_auto_slice();
        let mut ticks = 0usize;
        let mut prev_removed = db.reclamation_status().unwrap().versions_removed;
        loop {
            db.begin().commit();
            let st = db.reclamation_status().unwrap();
            if st.versions_removed == prev_removed {
                assert!(
                    st.pending_rows < slice,
                    "回收停在半路：还剩 {} 行账 ≥ 一片 {slice} 行，却没有再裁（active_txns={}）",
                    st.pending_rows,
                    st.active_txns
                );
                break;
            }
            prev_removed = st.versions_removed;
            ticks += 1;
            assert!(ticks < 4_000, "回收不收敛：{ticks} 次 tick 还在前进，已裁 {prev_removed} 行");
        }
        assert!(ticks > 1, "一次 tick 就排空 ⇒ 规模没到位、块边界根本没出现，这一条等于没测: ticks={ticks}");
        let stopped = db.reclamation_status().unwrap();
        println!(
            "自动回收：{ticks} 次 tick 裁掉 {prev_removed} 行（slice={slice}），停在不足一片的尾巴上，剩余账 {}，轮次{}",
            stopped.pending_rows,
            if stopped.sweep_in_progress { "停在半路（交给维护器）" } else { "已走完" }
        );

        // 半路的轮次由维护器推到库尾（M82 的分工）：独立游标、从头扫一整遍，直到它的游标回到库头。
        // 这一遍不许被活跃事务挡（reader 已放开），也不许走全库重写（`full_vacuum_interval: None`）。
        let mut driver = Maintenance::new(MaintenanceConfig {
            interval: Duration::from_secs(1),
            slice_rows: slice,
            full_vacuum_interval: None,
        }).unwrap();
        let mut maint_ticks = 0usize;
        loop {
            driver.tick(db.as_ref()).unwrap();
            maint_ticks += 1;
            assert!(driver.status().blocked_for == Duration::ZERO, "维护器被不存在的活跃事务挡住");
            if driver.cursor.is_none() {
                break;
            }
            assert!(maint_ticks < 4_000, "维护器一轮走不完：{maint_ticks} 片");
        }
        assert!(
            driver.status().slice_runs == maint_ticks as u64,
            "维护器每次 tick 都该是一片有界回收（slice_runs={} ticks={maint_ticks}）",
            driver.status().slice_runs
        );
        println!(
            "维护器：{maint_ticks} 片走完一整遍，又裁 {} 行、扫 {} 行",
            driver.status().versions_removed, driver.status().rows_scanned
        );

        // 判据一（自动路径 + 维护器收尾）：逐键读回最新值 —— 丢任何一把（或读回旧值）就是静默丢活键。
        let tx = db.begin();
        let mut lost: Vec<usize> = Vec::new();
        for i in 0..KEYS {
            if tx.get(&key(i)).as_deref() != Some(v2(i).as_slice()) && lost.len() < 10 {
                lost.push(i);
            }
        }
        tx.commit();
        assert!(lost.is_empty(), "自动回收后丢失或读回旧值的键（前 10 把，共 {KEYS} 把）: {lost:?}");

        // 判据二（手动门）：全库 `vacuum()` 必须真把链裁到 1，且仍然一条活键不丢 ——
        // 两条路都要过，因为骑块边那条路径只在"续跑换了下界"时出现，手动全库轮走的是同一套分块。
        db.vacuum().unwrap();
        assert_eq!(db.health().max_chain, 1, "全库 vacuum 后每把键应只剩一版");
        let tx = db.begin();
        let mut lost2: Vec<usize> = Vec::new();
        for i in 0..KEYS {
            if tx.get(&key(i)).as_deref() != Some(v2(i).as_slice()) && lost2.len() < 10 {
                lost2.push(i);
            }
        }
        tx.commit();
        assert!(lost2.is_empty(), "全库 vacuum 后丢失或读回旧值的键（前 10 把）: {lost2:?}");
    }
}
