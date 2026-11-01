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
            db.vacuum().map(|stats| crate::VacuumRound { stats, next_cursor: None })
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
    use std::{assert, assert_eq, format, vec};
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
}
