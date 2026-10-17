//! MVCC 事务层 + WAL 崩溃恢复（适配 LSM 后端）
//!
//! 从 `bplus-tree` 移植：协议与语义保持一致，存储后端由 B+ 树换成 [`LSMTree`]。
//!
//! # 两条写路径（吞吐差几个数量级）
//!
//! | 路径 | API | 持久化 | 适用 |
//! |------|-----|--------|------|
//! | **普通事务** | `begin_transaction` → `put` → `commit` | 每 commit **WAL fsync**；默认不 flush SST | 正确性 / 小批量 / 可崩溃恢复 |
//! | **Bulk load** | `begin_bulk` → `put_batch_owned` → `finish` | 无事务 WAL；直接写 L0 SST；finish 落 MANIFEST | 导入 / 压测（10M/10min） |
//!
//! 10M/10min 目标靠 **bulk** 达成。若每条 `commit`（含 fsync），本机约数千 ops/s，
//! 10M 需数十分钟到数小时——属预期，不是 bug。详见 README「写路径对比」。
//!
//! 提交协议（简化版 ARIES）：
//! 1. 写路径：先 append WAL Write，再改内存 MemTable
//! 2. commit：append WAL Commit → **fsync WAL**（持久化点）→ 可选 flush 快照
//! 3. 崩溃恢复：重放已提交 Write，撤销未提交 Write，再 checkpoint 截断 WAL
//!
//! **Bulk load 模式**（[`MVCC::begin_bulk`]）：
//! - 关闭 WAL / 冲突检测 / 活跃事务表
//! - 结束时 [`BulkLoader::finish`] → flush + checkpoint（默认不 compact L0）
//! - 需要收口 L0 时用 [`BulkLoader::finish_with_compact`]
//!
//! 与 bplus 的差异：
//! - 无 Blob 外置：value 直接内联进快照（后续可接 SST / value log）
//! - Checkpoint 的 `root_page_id` / `next_page_id` 固定写 0
//! - 数据目录：`dir/CURRENT.snap` + `dir/lsm.wal` + `dir/lsm.lock`

use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use crate::lsm_tree::lsm_tree::{LSMTree, LSMTreeOptions};
use crate::lsm_tree::storage::{lock_path, wal_path, FileLock};
use crate::lsm_tree::wal::{analyze_recovery, Wal, WalRecord};
pub use crate::common::{
    DurabilityProfile, EngineTuning, ExportRecord, MvccHealth, VacuumRound, VacuumStats,
};
use crate::common::{HealthFold, VacuumScheduler};
use crate::common::txn::{BulkCore, TxnCore, WalTail, WriteSide};
use crate::common::vacuum_plan;
use crate::common::version_key_upper_bound;

/// MVCC 存储引擎（LSM 后端）
pub struct MVCC {
    kv: Arc<Mutex<LSMTree>>,
    wal: Arc<Mutex<Wal>>,
    active_txn: Arc<Mutex<HashMap<u64, Vec<Vec<u8>>>>>,
    next_version: Arc<AtomicU64>,
    /// WAL 增长到多少字节就在下一次提交后自动 checkpoint（0 = 关闭自动触发）
    wal_limit: Arc<AtomicU64>,
    /// 自动回收：切片宽度 / 记账 / 游标（策略与实现在 [`crate::common::VacuumScheduler`]）
    vac: Arc<VacuumScheduler>,
    /// Arc 包裹是因为事务也要能拿到它：阈值触发时得顺手落一份 `MVCC.NEXT`
    db_dir: Arc<PathBuf>,
    /// 持有期间保证单写者
    _lock: FileLock,
}

/// 普通 MVCC 事务
///
/// 事务语义（版本、活跃快照、写集、冲突判定、写序列、回收记账）在 [`TxnCore`]
/// （[`crate::common::txn`]，四引擎共用）；这一家只留"字节住在 memtable/SST、WAL 怎么落"。
pub struct Transaction {
    pub(crate) kv: Arc<Mutex<LSMTree>>,
    pub(crate) wal: Arc<Mutex<Wal>>,
    next_version: Arc<AtomicU64>,
    wal_limit: Arc<AtomicU64>,
    db_dir: Arc<PathBuf>,
    pub(crate) core: TxnCore,
}

/// 写路径的守卫：两把锁都已按内核要求的顺序（活跃表 → wal → kv）拿好
pub(crate) struct TxnSide<'w, 'k> {
    pub(crate) wal: &'w mut Wal,
    pub(crate) kv: &'k mut LSMTree,
}

/// 收尾用的守卫：`commit` 只要往 WAL 落一条 `Commit` 再 fsync，**不该**在这期间握着 kv 锁
/// 把读者挡在门外（改前就是先释放再收尾的）。所以这里只声明 [`WalTail`]。
struct WalOnly<'w> {
    wal: &'w mut Wal,
}

/// 批量导入会话：无 WAL、无冲突检测，单写者假定
pub struct BulkLoader {
    kv: Arc<Mutex<LSMTree>>,
    wal: Arc<Mutex<Wal>>,
    core: BulkCore,
}

/// 逻辑键 + 版本（调试用）与版本键编解码
///
/// 定义在 [`crate::common::version`]（四引擎共用）；这里 re-export 是为了
/// `crate::lsm_tree::encode_key` 这个既有路径不中断。
pub use crate::common::{decode_key, encode_key, VersionedKey};

/// 把 WAL 收口成「只剩一条 Checkpoint」。
///
/// 原先的顺序是 `append(Checkpoint) → sync → truncate()`，而 `truncate()` 只留 header
/// —— 刚写的 Checkpoint 被自己抹掉，`compute_start_version` 里「用 WAL checkpoint 拿起点」
/// 那一级其实永远走不到（本引擎靠 `MVCC.NEXT` 侧文件兜住了，所以没暴露成问题）。
/// 改成先截断、再把 Checkpoint 写成新 WAL 的第一条。
fn seal_wal_checkpoint(wal: &mut Wal, next_version: u64) -> std::io::Result<()> {
    wal.truncate()?;
    wal.append(WalRecord::Checkpoint {
        next_version,
        root_page_id: 0,
        next_page_id: 0,
    })?;
    wal.sync()
}

/// checkpoint 的实体：手动 `MVCC::checkpoint()`、提交时的阈值触发、`try_open` 收尾共用一份。
///
/// 顺序不能换：先 flush（mem→L0，必要时 compact）再收口 WAL —— WAL 一旦被截断，
/// 未落盘的内存表就再无重放来源。`MVCC.NEXT` 最后写，它是 WAL 之外的那份起点保险。
fn run_checkpoint(kv: &Mutex<LSMTree>, wal: &Mutex<Wal>, next_version: &AtomicU64, db_dir: &Path) {
    let mut kv = kv.lock().unwrap();
    let mut wal = wal.lock().unwrap();
    let _ = kv.flush();
    let next_ver = next_version.load(Ordering::SeqCst);
    let _ = seal_wal_checkpoint(&mut wal, next_ver);
    persist_mvcc_next(db_dir, next_ver);
}

/// MVCC 档里**不容下游改动**的两个字段：钉回 `false`，无论传进来的是什么。
///
/// - `enable_mem_wal`：持久化点是这层的事务 WAL（commit 时 fsync WAL 记录）。再开 mem.wal
///   就变成同一份数据有两个提交日志，崩溃后"谁重放谁、谁覆盖谁"没有定义 ——
///   而 [`MVCC::durability_profile`] 承诺的正是"只有一个"。
/// - `enable_bg_compact`：后台线程会在 MVCC 外层锁**之外**动同一棵树的层，事务 WAL 的
///   重放/截断不知道它在跑。更要命的是 [`LSMTree::set_options`] 并不会起线程（线程只在
///   open 时按当时的 option 创建），若让它被热改成 `true`，写路径会以为"有人替我 compact"
///   而**不再同步 compact** ⇒ L0 无限涨。所以这条路整个封掉。
fn pin_mvcc_invariants(patch: LSMTreeOptions) -> LSMTreeOptions {
    let mut o = patch;
    o.enable_mem_wal = false;
    o.enable_bg_compact = false;
    o
}

impl MVCC {
    /// 打开/创建磁盘上的 MVCC 引擎（含 WAL 恢复）。失败时 panic（兼容旧代码）。
    pub fn open(dir: impl AsRef<Path>) -> Self {
        Self::try_open(dir).map_err(|e| format!("MVCC::open 失败: {e}")).unwrap()
    }

    /// 打开/创建，错误以 `io::Result` 返回（锁冲突、坏文件、IO 等）。
    pub fn try_open(dir: impl AsRef<Path>) -> std::io::Result<Self> {
        Self::try_open_with(dir, None)
    }

    /// 打开并覆盖 MVCC 档的旋钮；`None` = 全用 [`LSMTreeOptions::for_mvcc`]。
    ///
    /// 传进来的是**整份** options（不是差量），所以常规写法是用引擎自己的档当基线做
    /// struct-update，改哪个字段写在哪个字段上：
    ///
    /// ```no_run
    /// use storage_engines::lsm_tree::{MVCC, LSMTreeOptions};
    /// # use std::path::Path;
    /// let opts = LSMTreeOptions {
    ///     block_cache_blocks: 1024,
    ///     ..LSMTreeOptions::for_mvcc()
    /// };
    /// let db = MVCC::try_open_with(Path::new("./data/lsm_tree"), Some(opts)).unwrap();
    /// ```
    ///
    /// 两个字段**传了也不算**，恒被钉回 `false`（见 [`MVCC::set_options`] 同处的理由）：
    /// `enable_mem_wal`、`enable_bg_compact`。
    pub fn try_open_with(
        dir: impl AsRef<Path>,
        overrides: Option<LSMTreeOptions>,
    ) -> std::io::Result<Self> {
        let db_dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&db_dir)?;

        // 锁文件路径规则是各引擎自己的（lsm: `dir/lsm.lock`），锁的**语义**在
        // [`crate::common::disk::FileLock`]（四引擎共用的一份实现）
        let lock = FileLock::try_acquire_at(&lock_path(&db_dir))?;
        let opts = pin_mvcc_invariants(overrides.unwrap_or_else(LSMTreeOptions::for_mvcc));
        let mut tree = LSMTree::open_with_config(&db_dir, opts)?;
        let mut wal = Wal::open(wal_path(&db_dir))?;

        let entries = wal.read_all()?;
        let plan = analyze_recovery(&entries);

        for (_xid, key, value) in &plan.committed_writes {
            tree.insert(key.clone(), value.clone());
        }
        for (_xid, key) in &plan.uncommitted_writes {
            let _ = tree.remove(key);
        }

        // 不再全表 scan_max_version：用 checkpoint / WAL xid / redo key / 持久化元数据
        let start_ver = compute_start_version(&db_dir, &plan);

        tree.flush()?;
        // 先截断再写回 Checkpoint（原先 append→truncate 会把刚写的记录抹掉）
        seal_wal_checkpoint(&mut wal, start_ver)?;
        persist_mvcc_next(&db_dir, start_ver);

        Ok(Self {
            kv: Arc::new(Mutex::new(tree)),
            wal: Arc::new(Mutex::new(wal)),
            active_txn: Arc::new(Mutex::new(HashMap::new())),
            next_version: Arc::new(AtomicU64::new(start_ver)),
            wal_limit: Arc::new(AtomicU64::new(MVCC::WAL_CHECKPOINT_BYTES_DEFAULT)),
            vac: Arc::new(VacuumScheduler::new(MVCC::VACUUM_AUTO_SLICE_DEFAULT)),
            db_dir: Arc::new(db_dir),
            _lock: lock,
        })
    }

    /// 在系统临时目录创建匿名库（测试用）
    pub fn new() -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "lsm_mvcc_anon_{}_{}",
            std::process::id(),
            nanos
        ));
        Self::open(path)
    }

    pub fn begin_transaction(&self) -> Transaction {
        self.tick_auto_vacuum();
        Transaction::begin(
            self.kv.clone(),
            self.wal.clone(),
            self.active_txn.clone(),
            self.next_version.clone(),
            self.wal_limit.clone(),
            self.vac.clone(),
            self.db_dir.clone(),
        )
    }

    /// 自动回收的一小步：攒够一个切片宽度的新版本，就裁同样宽度的**一片**。
    ///
    /// 挂在 `begin_transaction` 而不是 commit 上：这一处还没有本线程的活跃事务，
    /// `vacuum_bounded` 的「有活跃事务就整轮跳过」不会被自己的调用方踩中；而且每个
    /// 事务至少 begin 一次，攒账的节奏和写路径一样。
    ///
    /// 触发量 = 切片宽度 ⇒ 回收速率与产生垃圾的速率 1:1，这是「跟得上」的下界。
    /// LSM 的一片是「每个源各读 slice 行」的窗口，块读取代价由稀疏索引压住。
    fn tick_auto_vacuum(&self) {
        let Some(slice) = self.vac.take_tick() else {
            return;
        };
        let after = self.vac.cursor();
        if let Ok(round) = self.vacuum_bounded(after.as_deref(), slice) {
            self.vac.set_cursor(round.next_cursor);
        }
    }

    /// 自动回收的切片宽度（行数）：每攒够这么多行新版本，就裁一片这么多行。
    ///
    /// 默认 [`MVCC::VACUUM_AUTO_SLICE_DEFAULT`]；**设 0 关闭自动回收**，那时版本行只增不减，
    /// 得靠手动 `vacuum()`。当年就是 LSM 先撑不住（一天），这条通道必须默认是通的。
    pub fn set_vacuum_auto_slice(&self, rows: u64) {
        self.vac.set_slice(rows);
    }

    pub fn vacuum_auto_slice(&self) -> u64 {
        self.vac.slice()
    }

    /// 默认切片宽度 = 16 Ki 行。一片的代价是「每个源读不超过这么多行」的窗口，
    /// 与库总量无关 —— 这正是切片存在的理由。
    pub const VACUUM_AUTO_SLICE_DEFAULT: u64 = 16 * 1024;

    /// 开启 bulk load：单写者、无 WAL、无冲突检测。
    ///
    /// **崩溃不安全**：finish 之前进程挂掉可能丢数据。
    /// 导入结束后必须 `finish()` / `finish_with_compact(...)`。
    ///
    /// 为 10M 级吞吐：`put_batch_owned` 排序后直接写 L0 SST（跳过 MemTable）。
    pub fn begin_bulk(&self) -> BulkLoader {
        {
            let mut kv = self.kv.lock().unwrap();
            let mut o = kv.options().clone();
            o.mem_threshold_bytes = 128 * 1024 * 1024;
            o.mem_threshold_entries = 1_000_000;
            o.l0_compact_threshold = 16;
            o.l0_slowdown_trigger = 32;
            o.l0_stop_trigger = 64;
            o.target_sst_bytes = 8 * 1024 * 1024;
            o.level_base_bytes = 64 * 1024 * 1024;
            o.enable_bg_compact = false;
            o.enable_mem_wal = false;
            o.max_compactions_per_flush = 1;
            kv.set_options(o);
            kv.set_bulk_mode(true);
        }
        BulkLoader {
            kv: self.kv.clone(),
            wal: self.wal.clone(),
            core: BulkCore::begin(self.next_version.clone()),
        }
    }

    /// 默认 WAL 阈值：16 MiB。现场见过 WAL 长到几十 MB 仍无人截断，而恢复是**全量扫描**
    /// WAL（`analyze_recovery`）⇒ 不截断等于启动一次比一次慢。
    pub const WAL_CHECKPOINT_BYTES_DEFAULT: u64 = 16 << 20;

    /// WAL 字节阈值可配置项：增长到这么多字节就在**下一次提交后**自动 checkpoint。
    /// 设 0 关闭自动触发（只能手动调 `checkpoint()`）。
    pub fn set_wal_checkpoint_limit(&self, bytes: u64) {
        self.wal_limit.store(bytes, Ordering::SeqCst);
    }

    pub fn wal_checkpoint_limit(&self) -> u64 {
        self.wal_limit.load(Ordering::SeqCst)
    }

    /// 当前生效的引擎旋钮（读回用，与 [`LSMTree::options`] 同一份）。
    pub fn options(&self) -> LSMTreeOptions {
        self.kv.lock().unwrap().options().clone()
    }

    /// 热更新旋钮（打开之后也能改，不用重开库）。
    ///
    /// `enable_mem_wal` / `enable_bg_compact` 会被钉回 `false`，理由见
    /// [`pin_mvcc_invariants`]。其余字段立刻生效：`block_cache_blocks` 会重建块缓存，
    /// compact/l0 阈值影响下一次写路径。
    pub fn set_options(&self, patch: LSMTreeOptions) {
        let mut kv = self.kv.lock().unwrap();
        kv.set_options(pin_mvcc_invariants(patch));
    }

    /// 本引擎能被调的东西 —— 清单在这里，不在门面。
    pub fn tuning(&self) -> EngineTuning {
        let mut t = EngineTuning::default();
        t.push_shared(self.vacuum_auto_slice(), Some(self.wal_checkpoint_limit()));
        let o = self.options();
        t.push("mem_threshold_bytes", o.mem_threshold_bytes);
        t.push("mem_threshold_entries", o.mem_threshold_entries);
        t.push("block_cache_blocks", o.block_cache_blocks);
        t.push("use_mmap", o.use_mmap);
        t.push("l0_compact_threshold", o.l0_compact_threshold);
        t.push("l0_slowdown_trigger", o.l0_slowdown_trigger);
        t.push("l0_stop_trigger", o.l0_stop_trigger);
        t.push("target_sst_bytes", o.target_sst_bytes);
        t.push("blob_threshold", o.blob_threshold);
        t
    }

    pub fn checkpoint(&self) {
        run_checkpoint(&self.kv, &self.wal, &self.next_version, &self.db_dir);
    }

    pub fn flush(&self) {
        let _ = self.kv.lock().unwrap().flush();
    }

    pub fn db_dir(&self) -> &Path {
        &self.db_dir
    }

    /// 导出当前快照下「每个逻辑 key 的最新可见版本」。
    pub fn export_latest_visible(&self, include_deleted: bool) -> Vec<ExportRecord> {
        let tx = self.begin_transaction();
        let records = tx.export_latest_visible(include_deleted);
        tx.commit();
        records
    }

    /// 崩溃安全画像。SST 块带 CRC32、MANIFEST 是「写 tmp 再改名」的原子换，所以
    /// compaction 中途崩掉不会丢数据（旧文件集仍然有效）—— 但会留下没人引用的孤儿 SST
    /// 占空间，这条 [`MVCC::vacuum`] / compact 都不收，属于已知代价。
    ///
    /// 两条要说清的缺口：
    /// - `blobs/*.blob` 是 `fs::write` 的**裸文件、没有校验和** ⇒ 大 value 的静默损坏发现不了
    ///   （SST 里那条指针本身在块里、有 CRC，所以"指针没坏、内容坏了"这种最难查）。
    /// - MVCC 路径**关掉了 mem.wal**（`LSMTreeOptions::for_mvcc` 里 `enable_mem_wal = false`）：
    ///   持久化完全靠上面这层事务 WAL 重放，不是"两层都开着"。
    pub fn durability_profile(&self) -> DurabilityProfile {
        DurabilityProfile {
            engine: "lsm_tree",
            transaction_wal: true,
            fsync_on_commit: true,
            data_checksum: true,
            blob_checksum: false,
            // SST 不可变，只有追加式写 ⇒ 没有 torn page 这条能力
            in_place_page_writes: false,
            torn_page_guard: false,
            merge_artifact_recovery: true,
        }
    }

    /// 全库健康度快照。**只走轻量视图**（`scan_latest_light`：版本号 + 是不是墓碑），
    /// 一趟、不物化任何 value。
    ///
    /// `kind == 2`（[`crate::lsm_tree::lsm_tree::EntryValue::Drop`]，已被硬删的行）不计入
    /// —— 和 [`MVCC::vacuum`] 看到的行集合严格一致，否则「vacuum 把 max_chain 压到 1」
    /// 这条守卫会对不上。口径与 [`crate::bitcask::MVCC::health`] 一致。
    pub fn health(&self) -> MvccHealth {
        // 一次只持一把锁：先读完 `active_txn` 计数就放，再拿 `kv`。
        // 嵌套持有会和 [`MVCC::vacuum`]（`active_txn → kv`）构成反向嵌套。
        let active_txns = self.active_txn.lock().unwrap().len() as u64;
        let kv = self.kv.lock().unwrap();
        // 分组折叠用四引擎共用的 [`HealthFold`]：按 enc 升序 ⇒ 同 key 相邻成块 ⇒ 一趟数得出链长
        let mut fold = HealthFold::default();
        // merge_streaming 按 enc 升序输出 ⇒ 同一逻辑 key 的版本块相邻
        for (enc, le) in kv.scan_latest_light(u64::MAX) {
            if le.kind == 2 {
                continue;
            }
            let Some(vk) = decode_key(&enc) else {
                continue;
            };
            fold.push(&vk.raw_key);
        }
        fold.finish(active_txns)
    }

    /// 版本回收：一次扫完全库。
    ///
    /// 就是 `vacuum_bounded(None, u64::MAX)`；保留规则、跳过条件、游标语义都在
    /// [`MVCC::vacuum_bounded`] 那边。
    pub fn vacuum(&self) -> std::io::Result<VacuumStats> {
        Ok(self.vacuum_bounded(None, u64::MAX)?.stats)
    }

    /// 有界版本回收：从逻辑 key `after`（**不含**）之后开始扫，最多读 `budget_rows` 行。
    ///
    /// 保留规则和 [`MVCC::vacuum`] 完全一致：
    /// - **有任何活跃事务 ⇒ 整轮跳过**（`skipped_active_txn = true`），游标原样退回
    /// - 无活跃事务时 `xmin = next_version`
    /// - 每个逻辑 key：只保留「最后一个 version < xmin」+ 全部 version >= xmin
    /// - 若 xmin 之前最终是 tombstone 且无更新版本 → 整 key 删除
    ///
    /// 遍历用 [`LSMTree::scan_latest_light_window`]（每源封顶的窗口式归并），所以一轮的
    /// 代价与库总量无关。`kind == 2`（`EntryValue::Drop`，已被硬删的行）整行不参与，
    /// 语义与 `iter()` 一致；`Put(None)` 才是「逻辑删除行」。**最后一组只在看到后继组的
    /// 第一行之后才算收齐**，没收齐的那一组本轮不裁（版本区间的穿插可能把它切断），
    /// 留给下一轮从同一个游标重读。
    ///
    /// `set_mvcc_xmin_filter` 只在**全库轮次**（`budget_rows == u64::MAX`）下发：它影响
    /// compact 丢哪些旧墓碑，语义是「全库已按这个 xmin 裁过」。
    ///
    /// 为什么跳过而不是用旧的 `min(活跃事务 id)`：理由与锁序见
    /// [`crate::bitcask::MVCC::vacuum_bounded`]（四引擎同一套语义）。
    pub fn vacuum_bounded(
        &self,
        after: Option<&[u8]>,
        budget_rows: u64,
    ) -> std::io::Result<VacuumRound> {
        let _active_guard = self.active_txn.lock().unwrap();
        if !_active_guard.is_empty() {
            return Ok(VacuumRound {
                stats: VacuumStats {
                    xmin: 0,
                    versions_removed: 0,
                    blob_rewritten: None,
                    skipped_active_txn: true,
                    rows_scanned: 0,
                },
                next_cursor: after.map(|k| k.to_vec()),
            });
        }
        let xmin = self.next_version.load(Ordering::SeqCst);

        let mut kv = self.kv.lock().unwrap();

        // 分组走 M3a 为折叠引入的轻量视图：判「留哪一版」只需要版本号 + 是不是墓碑。
        // 旧实现用 `kv.iter()`，而它会先把**每个源整个 collect() 进内存（连 value）**
        // ⇒ 一次 vacuum 的峰值内存约等于整个库。
        let budget_rows = budget_rows.max(1);
        let mut cap = if budget_rows >= (usize::MAX as u64) {
            usize::MAX
        } else {
            budget_rows as usize
        };
        // 版本键 = raw_key ‖ 8 字节大端 ⇒ 组宽度 8；窗口只在组边界收手
        const VERSION_SUFFIX: usize = 8;

        // 窗口右界只保证「窗口里 ≥2 组」，而 `after` 之前那几组会被过滤掉 —— 极端情况下
        // 过滤后只剩一组、且正是被右界切断的那组，本轮就一个能裁的完整组都没有。
        // 这时把窗口放大 4 倍重试（几何 ⇒ 总代价仍是最终窗口的常数倍），最坏退化成一轮
        // 读完剩余全库；不重试就会原地打转（游标退回 `after`，下一轮读到一模一样的窗口）。
        let (by_key, scanned, next_cursor) = loop {
            let (rows, boundary) =
                kv.scan_latest_light_window(after, u64::MAX, cap, VERSION_SUFFIX);

            let mut by_key: BTreeMap<Vec<u8>, Vec<(u64, Vec<u8>, bool, ())>> = BTreeMap::new();
            let mut scanned = 0u64;
            let mut cur_raw: Option<Vec<u8>> = None;
            let mut last_done_raw: Option<Vec<u8>> = None;
            let mut budget_cut = false;
            for (enc, le) in &rows {
                let Some(vk) = decode_key(enc) else {
                    continue;
                };
                if after.is_some_and(|a| vk.raw_key.as_slice() <= a) {
                    continue;
                }
                let at_group_start = cur_raw.as_deref() != Some(vk.raw_key.as_slice());
                if at_group_start && scanned >= budget_rows {
                    // 破在这里时 `cur_raw` 那一组已经收齐（刚看到它的后继）
                    budget_cut = true;
                    break;
                }
                if at_group_start {
                    last_done_raw = cur_raw.replace(vk.raw_key.clone());
                }
                scanned += 1;
                // Drop 行也参与「换组」的判定（它照样推进键序），只是不作为裁剪候选
                if le.kind == 2 {
                    continue;
                }
                by_key
                    .entry(vk.raw_key.clone())
                    .or_default()
                    .push((vk.version, enc.clone(), le.kind == 1, ()));
            }
            // 窗口被定界 或 预算截断 ⇒ 后面还有没看完的行
            let truncated = boundary.is_some() || budget_cut;
            // 两种截法对「最后一组收齐没有」的答案不一样：
            // - 预算截断：是「看到了下一组的第一行」才破的圈 ⇒ `cur_raw` 那一组已经收齐，
            //   可以裁，游标就是它。
            // - 窗口定界：循环自然结束 ⇒ `cur_raw` 这一组可能被右界切断，本轮不裁，
            //   游标退回上一个收齐的组。
            let (cursor, open_group) = if budget_cut {
                (cur_raw.clone(), None)
            } else if boundary.is_some() {
                (last_done_raw.clone(), cur_raw.clone())
            } else {
                (None, None)
            };
            if let Some(open) = open_group {
                by_key.remove(&open);
            }
            let next_cursor = if truncated { cursor } else { None };

            // 一轮什么都没裁、又还没到库尾 ⇒ 窗口太窄，放大重试（见上面注释）
            if next_cursor.is_none() && truncated && by_key.is_empty() {
                if cap >= usize::MAX / 4 {
                    break (by_key, scanned, None);
                }
                cap = cap.saturating_mul(4).max(1);
                continue;
            }
            break (by_key, scanned, next_cursor);
        };

        // 「留哪一版」是四引擎共用的判定，实现在 [`crate::common::vacuum_plan`]
        let (to_delete, kept) = vacuum_plan(by_key, xmin);
        let removed = to_delete.len();

        for enc in &to_delete {
            let _ = kv.remove(enc);
        }

        // 有界轮次只把删除留在 memtable 里，交给正常的 memtable flush 阈值收口：
        // 每片都 flush 会在 L0 造出一堆只装墓碑的小 SST，压实代价比省下的那点持久性贵。
        // 崩在中间也不会读到旧值 —— 复活的行都是 version < xmin 的，新事务快照看不见。
        let mut blob_rewritten = None;
        if budget_rows == u64::MAX {
            kv.flush()?;
            // 后续 compact 丢弃 ver < xmin 的 tombstone 版本键。
            // 只在全库轮次下发：判据用 budget 而不是游标，一把 key 的小库任何一片都读到库尾，
            // 用游标判断会让自动回收每片都改动 compact 的过滤条件。
            kv.set_mvcc_xmin_filter(xmin);

            // blob 旁路的空间回收：必须在删完旧行之后、且只有全库扫完才敢做 ——
            // 「还有没有人在引用这个 blob」是个全库问题。
            let keep_enc: Vec<Vec<u8>> = kept.into_iter().map(|(enc, _)| enc).collect();
            blob_rewritten = Some(kv.gc_blobs(&keep_enc)? > 0);
        }

        Ok(VacuumRound {
            stats: VacuumStats {
                xmin,
                versions_removed: removed,
                blob_rewritten,
                skipped_active_txn: false,
                rows_scanned: scanned,
            },
            next_cursor,
        })
    }
}

impl Default for MVCC {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for MVCC {
    fn drop(&mut self) {
        // 进程正常退出时落盘 next_version，避免下次 open 回退
        let next = self.next_version.load(Ordering::SeqCst);
        persist_mvcc_next(&self.db_dir, next);
    }
}


/// 持久化文件：`dir/MVCC.NEXT`（WAL truncate 后仍保留 next_version）
fn mvcc_next_path(dir: &Path) -> PathBuf {
    dir.join("MVCC.NEXT")
}

fn load_mvcc_next(dir: &Path) -> u64 {
    let p = mvcc_next_path(dir);
    let Ok(s) = std::fs::read_to_string(p) else {
        return 0;
    };
    s.trim().parse().unwrap_or(0)
}

fn persist_mvcc_next(dir: &Path, next: u64) {
    let p = mvcc_next_path(dir);
    let _ = std::fs::write(p, format!("{next}\n"));
}

/// 从 checkpoint / WAL / redo 键 / MVCC.NEXT 推算起始版本，**不扫 SST 全表**
fn compute_start_version(dir: &Path, plan: &crate::lsm_tree::wal::RecoveryPlan) -> u64 {
    let mut start = 1u64;

    // 1) 上次成功 open/checkpoint 留下的元数据（WAL 可能已被 truncate）
    start = start.max(load_mvcc_next(dir));

    // 2) WAL 里最后一个 Checkpoint
    if let Some(crate::lsm_tree::wal::WalRecord::Checkpoint { next_version, .. }) = &plan.last_checkpoint {
        start = start.max(*next_version);
    }

    // 3) WAL 见过的最大 xid → 下一个可用
    start = start.max(plan.max_xid.saturating_add(1));

    // 4) redo/undo 键上的 version 后缀（仅 WAL 里的键，不是全库）
    for (_xid, key, _) in &plan.committed_writes {
        if let Some(vk) = decode_key(key) {
            start = start.max(vk.version.saturating_add(1));
        }
    }
    for (_xid, key) in &plan.uncommitted_writes {
        if let Some(vk) = decode_key(key) {
            start = start.max(vk.version.saturating_add(1));
        }
    }

    start.max(1)
}

impl BulkLoader {
    pub fn put(&mut self, key: &[u8], value: Vec<u8>) {
        let enc = encode_key(key, self.core.version);
        let mut kv = self.kv.lock().unwrap();
        kv.insert_fast(enc, Some(value));
    }

    /// 批量 put：一次加锁插入多条（clone value）
    pub fn put_batch(&mut self, items: &[(Vec<u8>, Vec<u8>)]) {
        let mut kv = self.kv.lock().unwrap();
        let ver = self.core.version;
        kv.insert_batch_fast(items.iter().map(|(k, v)| (encode_key(k, ver), Some(v.clone()))));
    }

    /// 零额外 clone 的批量 put（消费 items）——压测主路径
    /// 按 key 排序后 **直接写 L0 SST**，跳过 MemTable。
    pub fn put_batch_owned(&mut self, mut items: Vec<(Vec<u8>, Vec<u8>)>) {
        if items.is_empty() {
            return;
        }
        // 若已有序（顺序导入）跳过 sort
        let sorted = items.windows(2).all(|w| w[0].0 <= w[1].0);
        if !sorted {
            items.sort_by(|a, b| a.0.cmp(&b.0));
        }
        let ver = self.core.version;
        let mut out: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::with_capacity(items.len());
        for (k, v) in items {
            let enc = encode_key(&k, ver);
            if let Some((last_k, _)) = out.last() {
                if last_k == &enc {
                    out.pop();
                }
            }
            out.push((enc, Some(v)));
        }
        let mut kv = self.kv.lock().unwrap();
        let _ = kv.bulk_ingest_sorted(out);
    }

    /// 预编码 key 的批量 put（调用方已 encode_key 且排序）
    pub fn put_encoded_batch(&mut self, mut items: Vec<(Vec<u8>, Option<Vec<u8>>)>) {
        if items.is_empty() {
            return;
        }
        items.sort_by(|a, b| a.0.cmp(&b.0));
        let mut out = Vec::with_capacity(items.len());
        for (k, v) in items {
            if let Some((last_k, _)) = out.last() {
                if last_k == &k {
                    out.pop();
                }
            }
            out.push((k, v));
        }
        let mut kv = self.kv.lock().unwrap();
        let _ = kv.bulk_ingest_sorted(out);
    }

    pub fn delete(&mut self, key: &[u8]) {
        let enc = encode_key(key, self.core.version);
        let mut kv = self.kv.lock().unwrap();
        kv.insert_fast(enc, None);
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let kv = self.kv.lock().unwrap();
        let low = encode_key(key, 0);
        let high = encode_key(key, u64::MAX);
        // bulk 的读口径是「不比自己新的最大版本」：同样只定位链尾，不物化整条版本链
        kv.find_back(&low, &high, u64::MAX, |enc, _val| {
            decode_key(enc)
                .map(|vk| vk.raw_key.as_slice() == key && vk.version <= self.core.version)
                .unwrap_or(false)
        })
        .and_then(|(_, v)| v)
    }

    /// 中途可手动 flush，降低峰值内存
    pub fn flush_mem(&mut self) {
        let _ = self.kv.lock().unwrap().flush();
    }

    /// 结束 bulk：flush MANIFEST + WAL checkpoint。
    ///
    /// **默认不做大规模 L0 compact**（10M 级会卡很久）。
    /// 需要收口 L0 时用 [`finish_with_compact`]。
    pub fn finish(self) {
        self.finish_with_compact(0);
    }

    /// 结束 bulk，并最多 compact `compact_limit` 轮 L0→L1。
    ///
    /// - `compact_limit == 0`：与 [`finish`] 相同，只落盘不 compact
    /// - `compact_limit > 0`：调用 [`LSMTree::compact_l0_until`]，目标 L0 ≤ `l0_compact_threshold - 1`
    ///
    /// 大批导入后读延迟敏感时建议 `finish_with_compact(16)` 之类；纯压测吞吐用 `finish()`。
    pub fn finish_with_compact(mut self, compact_limit: usize) {
        self.finish_inner(compact_limit);
    }

    fn finish_inner(&mut self, compact_limit: usize) {
        // 幂等守卫在内核里（`finish()` 与 `Drop` 都会调），收尾动作本身是 LSM 自己的事
        if !self.core.first_finish() {
            return;
        }
        let mut kv = self.kv.lock().unwrap();
        let mut wal = self.wal.lock().unwrap();
        // bulk 期间可能未写完 MANIFEST：flush 空 mem 并落盘清单
        let _ = kv.flush();
        kv.set_bulk_mode(false);
        if compact_limit > 0 {
            let target = kv.options().l0_compact_threshold.saturating_sub(1);
            let _ = kv.compact_l0_until(target, compact_limit);
        }
        let next_ver = self.core.next_version().load(Ordering::SeqCst);
        let _ = wal.append(WalRecord::Checkpoint {
            next_version: next_ver,
            root_page_id: 0,
            next_page_id: 0,
        });
        let _ = wal.sync();
        let _ = wal.truncate();
        persist_mvcc_next(
            // BulkLoader 没有 db_dir 字段：从 kv.dir 取
            kv.dir(),
            next_ver,
        );
    }
}

impl Drop for BulkLoader {
    fn drop(&mut self) {
        self.finish_inner(0);
    }
}

/// 未终结（既没 commit 也没 rollback）就被丢弃的事务在此自动回滚。
///
/// 漏网事务会永久留在 `active_txn` 里：`xmin = min(活跃事务 id)` 被一个已死的 id 压住
/// ⇒ 版本回收永久停摆，且之后每次 `begin` 的可见性快照随漏网数线性变大。
/// 判据用「活跃表里是否还有自己」——`commit`/`rollback` 都以 `remove` 收尾，成员关系即充要条件。
impl Drop for Transaction {
    fn drop(&mut self) {
        // 判据与"只查不删"的理由见 [`crate::common::is_unfinished`]：
        // 删除必须留给下面的 rollback()，它要按表里那份 key 列表把本事务写过的版本行清掉
        if !self.core.unfinished() {
            return;
        }
        if self.wal.lock().is_err() {
            return;
        }
        log::warn!("事务 {} 未 commit/rollback 即被丢弃，已自动回滚", self.core.version);
        self.rollback();
    }
}

/// 写侧：LSM 的"字节住在哪"= memtable（`insert`），而 WAL 必须在改库之前落。
///
/// 没用到的两个槽走默认实现（`encode_value`：大 value 的 blob 旁路在 [`LSMTree::insert`] 内部
/// 做完，不像 bitcask/bplus 那样是事务层显式的一步；`row_fits`：memtable 没有页容量上限）。
///
/// ⚠️ `latest_version` 走链尾反向早停（[`Transaction::find_version_back`]），与点读同一条路径 ——
/// 判冲突如果整链扫，热点 key 的每一次写都要陪跑版本链。
impl WriteSide for TxnSide<'_, '_> {
    fn latest_version(&mut self, key: &[u8]) -> Option<u64> {
        Transaction::find_version_back(self.kv, key, |_| true)
            .and_then(|(enc, _)| decode_key(&enc).map(|vk| vk.version))
    }

    fn put_versioned(&mut self, enc: &[u8], value: Option<Vec<u8>>) -> bool {
        self.kv.insert(enc.to_vec(), value);
        // memtable 插入没有失败通道（落盘在 flush/compact 那边，失败另有账）
        true
    }

    fn remove_versioned(&mut self, enc: &[u8]) {
        let _ = self.kv.remove(enc);
    }

    fn wal_begin(&mut self, xid: u64) -> bool {
        self.wal.append(WalRecord::Begin { xid }).is_ok()
    }

    fn wal_write(&mut self, xid: u64, enc: &[u8], stored: Option<&[u8]>) -> bool {
        self.wal
            .append(WalRecord::Write {
                xid,
                key: enc.to_vec(),
                value: stored.map(|v| v.to_vec()),
            })
            .is_ok()
    }
}

/// 收尾两条：`Commit` + fsync、`Abort` + fsync。
///
/// fsync 失败只喊出来（与改前一致）：事务此刻已从活跃表摘除、行也已落库，没有能回退的通道。
impl WalTail for TxnSide<'_, '_> {
    fn wal_commit(&mut self, xid: u64) {
        let _ = self.wal.append(WalRecord::Commit { xid });
        if let Err(e) = self.wal.sync() {
            eprintln!("WAL fsync 失败: {e}");
        }
    }

    fn wal_abort(&mut self, xid: u64) {
        let _ = self.wal.append(WalRecord::Abort { xid });
        let _ = self.wal.sync();
    }
}

impl WalTail for WalOnly<'_> {
    fn wal_commit(&mut self, xid: u64) {
        let _ = self.wal.append(WalRecord::Commit { xid });
        if let Err(e) = self.wal.sync() {
            eprintln!("WAL fsync 失败: {e}");
        }
    }
}

impl Transaction {
    pub fn begin(
        kv: Arc<Mutex<LSMTree>>,
        wal: Arc<Mutex<Wal>>,
        active_txn: Arc<Mutex<HashMap<u64, Vec<Vec<u8>>>>>,
        next_version: Arc<AtomicU64>,
        wal_limit: Arc<AtomicU64>,
        vac: Arc<VacuumScheduler>,
        db_dir: Arc<PathBuf>,
    ) -> Self {
        // begin 不进 WAL：此刻还不知道会不会写；第一次 write() 才补 `Begin`
        // （只读事务因此一个字节都不落，详见 [`crate::common::WalPolicy`]）
        let core = TxnCore::begin(active_txn, &next_version, vac);
        Transaction {
            kv,
            wal,
            next_version,
            wal_limit,
            db_dir,
            core,
        }
    }

    pub fn version(&self) -> u64 {
        self.core.version
    }

    pub fn set(&self, key: &[u8], value: Vec<u8>) -> bool {
        self.write(key, Some(value))
    }

    /// 原样存这串字节 —— 引擎替你打信封，`get` 会一字不差地还回来。
    ///
    /// 与 [`Self::set`] 的分工：`set` 把传进来的字节当作**已经打过信封**的逻辑值（现有调用点
    /// 按这个约定写，改它会破坏它们），所以拿 `set` 直接存裸字节会踩坑：值以 `0x00` 打头
    /// 读回来少 1 字节，以 `0x01` 打头会被当成 TTL 信封少 9 字节、甚至被判"已过期"读成 `None`。
    /// 存二进制/序列化字节（protobuf、avro、压缩块）走这个口子，不必再记信封规矩。
    ///
    /// 空值、`0x00`/`0x01` 打头、超过 blob 阈值走旁路的长值，都由每条用例里的
    /// `test_set_raw_round_trips_every_byte_pattern` 逐字节钉住；同一条里还对照断言
    /// "`set` 存同样的字节确实会少" —— 不对照就证明不了这个口子在做事。
    ///
    /// 读侧语义不变：`get` / `scan` / `multi_get` 一律照旧剥信封（剥掉的正是这里加的那一层）。
    /// 要"原样字节 + 过期时间"用 `set_with_ttl`（它内部同样已打信封）。
    pub fn set_raw(&self, key: &[u8], value: Vec<u8>) -> bool {
        self.set(key, crate::common::pack_plain(&value))
    }

    pub fn delete(&self, key: &[u8]) -> bool {
        self.write(key, None)
    }

    fn write(&self, key: &[u8], value: Option<Vec<u8>>) -> bool {
        // 锁序：活跃表 → wal → kv（内核要的就是这两把守卫，序列它在 [`TxnCore::write`] 里）
        let mut reg = self.core.lock_registry();
        let mut wal = self.wal.lock().unwrap();
        let mut kv = self.kv.lock().unwrap();
        self.core
            .write(&mut reg, &mut TxnSide { wal: &mut wal, kv: &mut kv }, key, value)
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.latest_visible_raw(key)
            .and_then(|(_, v)| v)
            .and_then(crate::lsm_tree::kv_ops::logical_to_user)
    }

    /// 最新可见逻辑行（含 TTL 信封原始字节；tombstone 为 None）。
    /// 供 [`crate::kv_ops`] 使用，不剥用户 value。
    pub(crate) fn latest_visible_raw(
        &self,
        key: &[u8],
    ) -> Option<(u64, Option<Vec<u8>>)> {
        let kvengine = self.kv.lock().unwrap();
        // 从链尾往回走，第一个可见版本就是答案（旧实现把整条链读出来再取最后一个）
        let (enc, val) = Self::find_version_back(&kvengine, key, |v| self.is_visible(v))?;
        Some((decode_key(&enc)?.version, val))
    }

    /// 全部最新可见逻辑行（key 有序）= 无界版的 [`Self::collect_latest_raw_in`]。
    pub(crate) fn collect_latest_raw(
        &self,
        include_deleted: bool,
    ) -> Vec<(Vec<u8>, u64, Option<Vec<u8>>)> {
        self.collect_latest_raw_in(include_deleted, None, None)
    }

    /// `[start, end)` 内每个逻辑 key 的最新可见版本 —— 界**下推到每一个源**。
    ///
    /// 旧形状是 `scan_latest_light(u64::MAX)` 把每个 memtable 整表遍历、每个 SST 整篇读完
    /// 再归并，调用方随后逐条筛界 ⇒ 10 行的窗口要付全库的块读代价（大库基线实测：
    /// 窗口 scan 405 µs/行 vs 一次全量 scan 4.0 µs/行）。
    ///
    /// 界判在**版本键**上，所以前缀交错的边角仍要逐条兜：起点 `raw < start ⇒ continue`，
    /// 终点先按 `enc ≥ 哨兵` 把每个源的 range 收到上界，落在界内但 `raw ≥ end` 的再 continue。
    pub(crate) fn collect_latest_raw_in(
        &self,
        include_deleted: bool,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Vec<(Vec<u8>, u64, Option<Vec<u8>>)> {
        crate::common::collect_rows(self, include_deleted, start, end, None, None).0
    }

    /// 取一组的值：跨源点查（`get_value_at` 而不是 `get_raw_key_at` —— 大 value 存的是 blob
    /// 指针，交给调用方之前必须解回真值；GC 那种"要看指针本身"的读法才用 raw 版）。
    /// 外层 `None` = 这一行已经不在值存储里 ⇒ `Missing`（整行不参与，与改动前一致）；
    /// 内层 `None` = 墓碑。这两种情况在折叠侧必须分开，所以 `RowValue` 是三态。
    fn read_row(&self, enc: &[u8]) -> crate::common::RowValue {
        use crate::common::RowValue;
        let kvengine = self.kv.lock().unwrap();
        match kvengine.get_value_at(enc, u64::MAX) {
            None => RowValue::Missing,
            Some(None) => RowValue::Tombstone,
            Some(Some(bytes)) => RowValue::Value(bytes),
        }
    }

    /// 折出**候选组**（一把逻辑 key 一组，值还没取）。
    ///
    /// ① 轻量视图：每个 enc key 只拿到「胜者的 seq + 种类」，**不克隆任何 value**。
    ///    旧实现走 `iter()`，而它会先把每个源（含每个 SST 的每行）连 value 一起
    ///    `collect()` 进内存 ⇒ 一次全库折叠的峰值内存约等于整个库。
    ///    kind==2 即 `EntryValue::Drop`：与 `iter()` 的 filter 一致，整行不可见
    ///    （注意是「该 key 不可见」，不是「回退到更旧源里同 key 的 Put」）。
    ///
    /// `after` 是续跑游标、`accept` 是键级过滤、`budget` 是组数上限；返回的 `bool` =
    /// 已走到界尾（`false` = 被预算拦下，还能续）。⚠️ 交出一组只能在组边界。
    pub(crate) fn scan_candidates(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        accept: Option<&dyn Fn(&[u8]) -> bool>,
        after: Option<&[u8]>,
        budget: usize,
    ) -> (Vec<crate::common::WinCandidate<Vec<u8>>>, bool) {
        let hi_enc = end.map(version_key_upper_bound);
        let mut out: Vec<crate::common::WinCandidate<Vec<u8>>> = Vec::new();
        let mut walk_done = true;
        {
            let kvengine = self.kv.lock().unwrap();
            let mut winners: BTreeMap<Vec<u8>, (u64, Vec<u8>)> = BTreeMap::new();
            let mut pending: Option<Vec<u8>> = None;
            // 惰性归并：`budget` 收够就 break ⇒ 后面的键连块都不读（M15b）
            for (enc, le) in kvengine
                .light_iter_range(crate::common::fold_lower_bound(start, after), hi_enc.as_deref(), u64::MAX)
            {
                #[cfg(test)]
                crate::lsm_tree::sst::note_light_visit();
                if le.kind == 2 {
                    continue;
                }
                let Some(vk) = decode_key(&enc) else {
                    continue;
                };
                if start.is_some_and(|s| vk.raw_key.as_slice() < s) {
                    continue;
                }
                if end.is_some_and(|e| vk.raw_key.as_slice() >= e) {
                    continue;
                }
                if after.is_some_and(|a| vk.raw_key.as_slice() <= a) {
                    continue; // 上一轮已经交出去的那组
                }
                if !self.is_visible(vk.version) {
                    continue;
                }
                if pending.as_deref() != Some(vk.raw_key.as_slice()) {
                    if let Some(key) = pending.take() {
                        if let Some((ver, win_enc)) = winners.remove(&key) {
                            if accept.is_none_or(|f| f(&key)) {
                                out.push(crate::common::WinCandidate {
                                    key,
                                    version: ver,
                                    row: win_enc,
                                });
                                if out.len() >= budget {
                                    walk_done = false;
                                    break;
                                }
                            }
                        }
                    }
                    pending = Some(vk.raw_key.clone());
                }
                // enc 升序 ⇒ 同一 raw_key 最后一次覆盖即最大可见版本
                winners.insert(vk.raw_key.clone(), (vk.version, enc));
            }
            if walk_done {
                if let Some(key) = pending {
                    if let Some((ver, win_enc)) = winners.remove(&key) {
                        if accept.is_none_or(|f| f(&key)) {
                            out.push(crate::common::WinCandidate {
                                key,
                                version: ver,
                                row: win_enc,
                            });
                        }
                    }
                }
            }
        }
        (out, walk_done)
    }

    pub fn print_all(&self) -> BTreeMap<Vec<u8>, Option<Vec<u8>>> {
        let mut records = BTreeMap::new();
        for rec in self.export_latest_visible(true) {
            records.insert(rec.key, rec.value);
        }
        records
    }

    /// 导出本事务快照下每个逻辑 key 的最新可见版本（已剥 TTL）。
    pub fn export_latest_visible(&self, include_deleted: bool) -> Vec<ExportRecord> {
        self.collect_latest_raw(true)
            .into_iter()
            .filter_map(|(key, _ver, raw)| match raw {
                None => {
                    if include_deleted {
                        Some(ExportRecord { key, value: None })
                    } else {
                        None
                    }
                }
                Some(bytes) => match crate::lsm_tree::kv_ops::logical_to_user(bytes) {
                    Some(user) => Some(ExportRecord {
                        key,
                        value: Some(user),
                    }),
                    None => {
                        if include_deleted {
                            Some(ExportRecord { key, value: None })
                        } else {
                            None
                        }
                    }
                },
            })
            .collect()
    }

    /// 提交事务：WAL Commit + **fsync**。默认不 flush MemTable/SST。
    ///
    /// 每事务一次 fsync → 吞吐受盘限制（数千 ops/s 量级）。
    /// 大批量导入请用 [`MVCC::begin_bulk`]。需要把 mem 落成 SST 时用
    /// [`commit_with_options`]`(true)` 或事后 [`MVCC::flush`]。
    pub fn commit(&self) {
        self.commit_with_options(false);
    }

    /// `flush_pages`：commit 后是否 `LSMTree::flush`（mem→L0 + 可能 compact）。
    ///
    /// 频繁 `flush_pages=true` 会显著变慢；批量提交后再 `mvcc.flush()` 更合适。
    /// L0 堆积时可用 `kv.compact_l0_until(...)` 或后台 compact 收口。
    pub fn commit_with_options(&self, flush_pages: bool) {
        // 销账 + 回收记账 + WAL 收尾（`Commit` + fsync）在 [`TxnCore::commit`] 一份。
        // 收尾只交 wal 守卫：fsync 期间不许把 kv 锁着 —— 那会把读者全挡在门外。
        {
            let mut reg = self.core.lock_registry();
            let mut wal = self.wal.lock().unwrap();
            self.core.commit(&mut reg, &mut WalOnly { wal: &mut wal });
        }
        if flush_pages {
            let _ = self.kv.lock().unwrap().flush();
        }

        // WAL 超阈值就顺带 checkpoint。放在提交之后是因为此刻本事务已持久、
        // 也已从活跃表摘除，checkpoint 记到的 next_version 是干净的。
        let limit = self.wal_limit.load(Ordering::SeqCst);
        if limit > 0 {
            let size = self.wal.lock().unwrap().size_bytes().unwrap_or(0);
            if size >= limit {
                run_checkpoint(&self.kv, &self.wal, &self.next_version, &self.db_dir);
            }
        }
    }

    pub fn rollback(&self) {
        // 销账 + `Abort` + 按写集撤行，都在 [`TxnCore::rollback`]（锁序与 write 一致）
        let mut reg = self.core.lock_registry();
        let mut wal = self.wal.lock().unwrap();
        let mut kv = self.kv.lock().unwrap();
        self.core
            .rollback(&mut reg, &mut TxnSide { wal: &mut wal, kv: &mut kv });
    }

    fn is_visible(&self, version: u64) -> bool {
        // 判据在 [`crate::common::visible_at`]（四引擎共用的一份）
        self.core.is_visible(version)
    }

    /// 从链尾往回找本 key 第一个被 `accept` 的版本，返回它的版本键与逻辑 value。
    ///
    /// 定位交给 [`LSMTree::find_back`]：逐源（新→旧）命中即停，不再把整条版本链连 value
    /// 一起物化出来再挑最后一个。前缀交错的坑照旧 —— 以 `key` 为前缀的更长 key 会落进
    /// 同一个字节区间，所以必须逐条核对 `raw_key == key`。
    fn find_version_back(
        kv: &LSMTree,
        key: &[u8],
        accept: impl Fn(u64) -> bool,
    ) -> Option<(Vec<u8>, Option<Vec<u8>>)> {
        let low = encode_key(key, 0);
        let high = encode_key(key, u64::MAX);
        kv.find_back(&low, &high, u64::MAX, |enc, _val| {
            decode_key(enc)
                .map(|vk| vk.raw_key.as_slice() == key && accept(vk.version))
                .unwrap_or(false)
        })
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lsm_tree::storage::lock_path;

    fn tmp_db(tag: &str) -> PathBuf {
        crate::common::tmp_test_path("lsm_mvcc", tag, "")
    }

    fn cleanup(path: &Path) {
        let _ = std::fs::remove_dir_all(path);
        // 若路径被当文件创建过
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(wal_path(path));
        let _ = std::fs::remove_file(lock_path(path));
    }

    /// 探针：一次点读 / 一次点写访问了多少个 SST 数据块。
    ///
    /// 旧实现是 `range_scan(encode(key,0), encode(key,MAX))` 跨 memtable 与每个重叠 SST，
    /// 而 `SstFile::range_at` 又是从文件**第 0 块**开始逐条 `continue` 到 low ——
    /// 于是 LSM 的代价不是随链长涨，而是随**库总量**涨。这正是同一份数据在 LSM 上一天
    /// 就顶不住、换到 Bitcask 能撑一个多月的差别来源。
    #[test]
    fn test_point_op_cost_independent_of_chain_length() {
        fn probe(chain_len: u64) -> (u64, u64, u64) {
            let path = tmp_db(&format!("probe_{chain_len}"));
            let (get_blocks, set_blocks, scan_blocks);
            {
                let mvcc = MVCC::open(&path);
                for i in 0..chain_len {
                    let tx = mvcc.begin_transaction();
                    assert!(tx.set(b"hot", format!("v{i}").into_bytes()), "第 {i} 次写入被拒");
                    tx.commit();
                }
                // 必须落到 SST：否则读全在 memtable 里，量不到块访问
                mvcc.flush();

                let tx = mvcc.begin_transaction();
                let before = crate::lsm_tree::sst::block_reads();
                assert_eq!(
                    tx.get(b"hot"),
                    Some(format!("v{}", chain_len - 1).into_bytes())
                );
                get_blocks = crate::lsm_tree::sst::block_reads() - before;

                let before = crate::lsm_tree::sst::block_reads();
                assert!(tx.set(b"hot", b"newer".to_vec()));
                set_blocks = crate::lsm_tree::sst::block_reads() - before;
                tx.rollback();

                // 参照量：旧实现走的就是 range_scan（整段物化 + 每源从第 0 块起扫），
                // 它就摆在这里，所以「改前」是实测而不是推断。
                let kvengine = mvcc.kv.lock().unwrap();
                let before = crate::lsm_tree::sst::block_reads();
                let rows = kvengine
                    .range_scan(encode_key(b"hot", 0), encode_key(b"hot", u64::MAX))
                    .len();
                scan_blocks = crate::lsm_tree::sst::block_reads() - before;
                assert_eq!(rows as u64, chain_len);
            }
            cleanup(&path);
            (get_blocks, set_blocks, scan_blocks)
        }

        let (g_short, s_short, scan_short) = probe(50);
        let (g_long, s_long, scan_long) = probe(2000);
        eprintln!(
            "点读块访问：链长 50 → {g_short} 块，链长 2000 → {g_long} 块\n\
             点写块访问：链长 50 → {s_short} 块，链长 2000 → {s_long} 块\n\
             旧路径 range_scan 参照：链长 50 → {scan_short} 块，链长 2000 → {scan_long} 块"
        );
        assert!(
            g_long <= g_short + 8,
            "点读的块访问不应随链长/库总量增长（短链 {g_short}，长链 {g_long}）"
        );
        assert!(
            s_long <= s_short + 8,
            "点写的块访问不应随链长/库总量增长（短链 {s_short}，长链 {s_long}）"
        );
        // 反向定位必须明显便宜于整段物化。注意分母是**已经享受了起点定位改造**的
        // range_scan（本次同批修的：它原先是从文件第 0 块逐条 continue 到 low），
        // 所以真实改造前的差距比这里显示的更大 —— 旧实现还要把每行的 value 读出来并克隆。
        assert!(
            g_long * 4 <= scan_long,
            "find_back 应当明显便宜于整段 range_scan（实际 {g_long} 块 vs {scan_long} 块）"
        );
    }

    /// WAL 阈值触发必须真的把文件截住（对照「关闭自动触发」），且截断之后重开仍能读到
    /// 全部已提交数据 —— 后半段守的是「先 flush（mem→L0）再收口 WAL」这个顺序。
    ///
    /// 量测统一放在实例还活着的时候：bplus 那边 `impl Drop for MVCC` 会顺手 checkpoint，
    /// 量在作用域外会让两种配置都得同样的数字、断言空转。本引擎的 drop 只落 `MVCC.NEXT`，
    /// 但照同一个口径写，免得以后有人改 drop 时这条守卫静默失效。
    #[test]
    fn test_wal_auto_checkpoint_bounds_file_and_keeps_data() {
        const WRITES: u32 = 40;

        fn wal_bytes_after(tag: &str, limit: u64) -> u64 {
            let path = tmp_db(tag);
            let size;
            {
                let mvcc = MVCC::open(&path);
                mvcc.set_wal_checkpoint_limit(limit);
                for i in 0..WRITES {
                    let key = format!("k{i}");
                    let tx = mvcc.begin_transaction();
                    assert!(tx.set(key.as_bytes(), vec![b'x'; 200]));
                    tx.commit();
                }
                size = std::fs::metadata(wal_path(&path)).map(|m| m.len()).unwrap_or(0);
            }
            cleanup(&path);
            size
        }

        let manual = wal_bytes_after("wal_manual", 0);
        let auto = wal_bytes_after("wal_auto", 1);
        eprintln!("WAL 体积：关闭自动 checkpoint {manual} 字节 vs 阈值触发 {auto} 字节");
        assert!(
            auto * 4 <= manual,
            "阈值触发应把 WAL 截住（自动 {auto}，手动 {manual}）"
        );

        let path = tmp_db("wal_reopen");
        {
            let mvcc = MVCC::open(&path);
            mvcc.set_wal_checkpoint_limit(1);
            for i in 0..WRITES {
                let key = format!("r{i}");
                let tx = mvcc.begin_transaction();
                assert!(tx.set(key.as_bytes(), format!("v{i}").into_bytes()));
                tx.commit();
            }
        }
        {
            let mvcc = MVCC::open(&path);
            let tx = mvcc.begin_transaction();
            for i in 0..WRITES {
                let key = format!("r{i}");
                assert_eq!(
                    tx.get(key.as_bytes()),
                    Some(format!("v{i}").into_bytes()),
                    "第 {i} 条已提交写入在 WAL 被截断后仍应读到"
                );
            }
            tx.commit();
        }
        cleanup(&path);
    }

    /// ② 全库折叠：不该再物化「带 value 的条目」。
    ///
    /// 计数挂在重量路径上（`SstEntryIter::next` 与 `mem_view_at`），所以这条一旦变红
    /// 就说明折叠又被改回 `iter()` —— 那意味着一次扫描的峰值内存约等于整个库。
    #[test]
    fn test_collect_latest_does_not_materialize_values() {
        const KEYS: u64 = 8;
        const VERSIONS: u64 = 120;

        let path = tmp_db("collect_light");
        {
            let mvcc = MVCC::open(&path);
            for v in 0..VERSIONS {
                for k in 0..KEYS {
                    let key = format!("k{k}");
                    let tx = mvcc.begin_transaction();
                    assert!(tx.set(key.as_bytes(), format!("v{v}").into_bytes()));
                    tx.commit();
                }
            }
            // 落到 SST，让重量路径真的有机会被走到
            mvcc.flush();

            let tx = mvcc.begin_transaction();
            let base = crate::lsm_tree::sst::value_materializations();
            let recs = tx.export_latest_visible(false);
            let used = crate::lsm_tree::sst::value_materializations() - base;
            eprintln!(
                "折叠期重量条目物化次数：{used}（存活 key {KEYS}，历史行数 {}）",
                KEYS * VERSIONS
            );
            assert_eq!(recs.len() as u64, KEYS, "存活 key 数应等于 {KEYS}");
            assert!(
                used <= KEYS + 4,
                "折叠只该为胜出行取值：重量条目物化 {used} 次，而历史行数是 {}",
                KEYS * VERSIONS
            );
            tx.rollback();
        }
        cleanup(&path);
    }

    /// vacuum 的分组阶段不该再把每行的 value 物化出来。
    ///
    /// 量尺用「带 value 的条目物化次数」（挂在 `SstEntryIter::next` / `mem_view_at` 上）。
    /// 注意 vacuum 尾部的 flush→compact 本来就要把**存活行**读一遍重写 SST，那部分是
    /// compaction 的本职、不是 vacuum 的浪费 —— 所以界按存活行数量级给（`KEYS × 8`），
    /// 不按历史行数。实测 0 次（未修前是 1200 次）。
    #[test]
    fn test_vacuum_does_not_materialize_values() {
        const CHAIN: u64 = 300;
        const KEYS: u64 = 4;

        let path = tmp_db("vacuum_mat");
        {
            let mvcc = MVCC::open(&path);
            for i in 0..CHAIN {
                for k in 0..KEYS {
                    let key = format!("k{k}");
                    let tx = mvcc.begin_transaction();
                    assert!(tx.set(key.as_bytes(), format!("v{i}").into_bytes()));
                    tx.commit();
                }
            }
            // 落到 SST，让重量路径有机会被走到
            mvcc.flush();

            let base = crate::lsm_tree::sst::value_materializations();
            let stats = mvcc.vacuum().unwrap();
            let used = crate::lsm_tree::sst::value_materializations() - base;
            eprintln!(
                "vacuum 期间带 value 条目物化次数：{used}（裁掉 {} 行，历史行数 {}）",
                stats.versions_removed,
                CHAIN * KEYS
            );
            assert_eq!(
                stats.versions_removed,
                ((CHAIN - 1) * KEYS) as usize,
                "每 key 留一版，其余都该裁掉"
            );
            assert!(
                used <= (KEYS * 8) as u64,
                "vacuum 分组不该物化每行 value：实际 {used} 次，而历史行数是 {}",
                CHAIN * KEYS
            );
        }
        cleanup(&path);
    }

    #[test]
    fn test_encode_order() {
        let k1 = encode_key(b"user", 1);
        let k2 = encode_key(b"user", 2);
        let k10 = encode_key(b"user", 10);
        assert!(k1 < k2);
        assert!(k2 < k10);
        let d = decode_key(&k10).unwrap();
        assert_eq!(d.raw_key, b"user");
        assert_eq!(d.version, 10);
    }

    #[test]
    fn test_basic_set_get_delete() {
        let path = tmp_db("basic");
        {
            let mvcc = MVCC::open(&path);
            let tx = mvcc.begin_transaction();
            assert!(tx.set(b"a", b"1".to_vec()));
            assert_eq!(tx.get(b"a"), Some(b"1".to_vec()));
            assert!(tx.set(b"a", b"2".to_vec()));
            assert_eq!(tx.get(b"a"), Some(b"2".to_vec()));
            assert!(tx.delete(b"a"));
            assert_eq!(tx.get(b"a"), None);
            tx.commit();
        }
        cleanup(&path);
    }

    #[test]
    fn test_snapshot_isolation() {
        let path = tmp_db("si");
        {
            let mvcc = MVCC::open(&path);
            let t1 = mvcc.begin_transaction();
            assert!(t1.set(b"k", b"v1".to_vec()));
            t1.commit();
            let t2 = mvcc.begin_transaction();
            let t3 = mvcc.begin_transaction();
            assert!(t2.set(b"k", b"v2".to_vec()));
            assert_eq!(t3.get(b"k"), Some(b"v1".to_vec()));
            t2.commit();
            assert_eq!(t3.get(b"k"), Some(b"v1".to_vec()));
            let t4 = mvcc.begin_transaction();
            assert_eq!(t4.get(b"k"), Some(b"v2".to_vec()));
            t4.commit();
            t3.commit();
        }
        cleanup(&path);
    }

    #[test]
    fn test_write_write_conflict() {
        let path = tmp_db("ww");
        {
            let mvcc = MVCC::open(&path);
            let t1 = mvcc.begin_transaction();
            let t2 = mvcc.begin_transaction();
            assert!(t1.set(b"k", b"v1".to_vec()));
            assert!(!t2.set(b"k", b"v2".to_vec()));
            t1.commit();
            t2.rollback();
        }
        cleanup(&path);
    }

    #[test]
    fn test_rollback_discards_writes() {
        let path = tmp_db("rb");
        {
            let mvcc = MVCC::open(&path);
            let t1 = mvcc.begin_transaction();
            assert!(t1.set(b"x", b"1".to_vec()));
            t1.rollback();
            let t2 = mvcc.begin_transaction();
            assert_eq!(t2.get(b"x"), None);
            t2.commit();
        }
        cleanup(&path);
    }

    #[test]
    fn test_print_all_latest_visible() {
        let path = tmp_db("pa");
        {
            let mvcc = MVCC::open(&path);
            let t1 = mvcc.begin_transaction();
            assert!(t1.set(b"a", b"1".to_vec()));
            assert!(t1.set(b"b", b"2".to_vec()));
            assert!(t1.set(b"a", b"3".to_vec()));
            let all = t1.print_all();
            assert_eq!(all.get(&b"a".to_vec()).unwrap().as_ref().unwrap(), b"3");
            assert_eq!(all.get(&b"b".to_vec()).unwrap().as_ref().unwrap(), b"2");
            t1.commit();
        }
        cleanup(&path);
    }

    #[test]
    fn test_instances_isolated() {
        let p1 = tmp_db("i1");
        let p2 = tmp_db("i2");
        {
            let m1 = MVCC::open(&p1);
            let m2 = MVCC::open(&p2);
            let t1 = m1.begin_transaction();
            let t2 = m2.begin_transaction();
            assert!(t1.set(b"k", b"1".to_vec()));
            assert!(t2.set(b"k", b"2".to_vec()));
            assert_eq!(t1.get(b"k"), Some(b"1".to_vec()));
            assert_eq!(t2.get(b"k"), Some(b"2".to_vec()));
            t1.commit();
            t2.commit();
        }
        cleanup(&p1);
        cleanup(&p2);
    }

    #[test]
    fn test_self_write_visible() {
        let path = tmp_db("self");
        {
            let mvcc = MVCC::open(&path);
            let tx = mvcc.begin_transaction();
            assert!(tx.set(b"k", b"v".to_vec()));
            assert_eq!(tx.get(b"k"), Some(b"v".to_vec()));
            tx.commit();
        }
        cleanup(&path);
    }

    #[test]
    fn test_conflict_after_other_commits() {
        let path = tmp_db("cmt");
        {
            let mvcc = MVCC::open(&path);
            let t1 = mvcc.begin_transaction();
            let t2 = mvcc.begin_transaction();
            assert!(t1.set(b"k", b"1".to_vec()));
            t1.commit();
            assert!(!t2.set(b"k", b"2".to_vec()));
            t2.rollback();
            let t3 = mvcc.begin_transaction();
            assert_eq!(t3.get(b"k"), Some(b"1".to_vec()));
            t3.commit();
        }
        cleanup(&path);
    }

    #[test]
    fn test_disk_persist_across_reopen() {
        let path = tmp_db("persist");
        {
            let mvcc = MVCC::open(&path);
            let t1 = mvcc.begin_transaction();
            assert!(t1.set(b"hello", b"world".to_vec()));
            t1.commit();
            let t2 = mvcc.begin_transaction();
            assert!(t2.set(b"hello", b"rust".to_vec()));
            assert!(t2.set(b"foo", b"bar".to_vec()));
            t2.commit();
            // 确保快照落盘（commit 默认不 flush 页，靠 drop 或显式 flush）
            mvcc.flush();
        }
        {
            let mvcc = MVCC::open(&path);
            let tx = mvcc.begin_transaction();
            assert_eq!(tx.get(b"hello"), Some(b"rust".to_vec()));
            assert_eq!(tx.get(b"foo"), Some(b"bar".to_vec()));
            tx.commit();
        }
        cleanup(&path);
    }

    #[test]
    fn test_wal_redo_after_commit_without_page_flush() {
        let path = tmp_db("redo");
        {
            cleanup(&path);
            std::fs::create_dir_all(&path).unwrap();
            // 空快照
            let mut tree = LSMTree::open(&path).unwrap();
            tree.flush().unwrap();
            drop(tree);

            let mut wal = Wal::open(wal_path(&path)).unwrap();
            let xid = 1u64;
            let enc = encode_key(b"recover", xid);
            wal.append(WalRecord::Begin { xid }).unwrap();
            wal.append(WalRecord::Write {
                xid,
                key: enc,
                value: Some(b"me".to_vec()),
            })
            .unwrap();
            wal.append(WalRecord::Commit { xid }).unwrap();
            wal.sync().unwrap();
        }
        {
            let mvcc = MVCC::open(&path);
            let tx = mvcc.begin_transaction();
            assert_eq!(tx.get(b"recover"), Some(b"me".to_vec()));
            tx.commit();
        }
        cleanup(&path);
    }

    #[test]
    fn test_wal_undo_uncommitted() {
        let path = tmp_db("undo");
        {
            {
                let mvcc = MVCC::open(&path);
                drop(mvcc);
            }
            // 模拟：脏写入已进快照，但 WAL 只有 Begin+Write 无 Commit
            let mut wal = Wal::open(wal_path(&path)).unwrap();
            let xid = 1u64;
            {
                let mut tree = LSMTree::open(&path).unwrap();
                let enc = encode_key(b"ghost", xid);
                tree.insert(enc.clone(), Some(b"should-vanish".to_vec()));
                tree.flush().unwrap();
                wal.append(WalRecord::Begin { xid }).unwrap();
                wal.append(WalRecord::Write {
                    xid,
                    key: enc,
                    value: Some(b"should-vanish".to_vec()),
                })
                .unwrap();
                wal.sync().unwrap();
            }
        }
        {
            let mvcc = MVCC::open(&path);
            let tx = mvcc.begin_transaction();
            assert_eq!(tx.get(b"ghost"), None);
            tx.commit();
        }
        cleanup(&path);
    }

    #[test]
    fn test_bulk_load_and_reopen() {
        let path = tmp_db("bulk");
        {
            let mvcc = MVCC::open(&path);
            let mut bulk = mvcc.begin_bulk();
            for i in 0..1000u32 {
                let k = format!("k{i:06}").into_bytes();
                let v = format!("v{i}").into_bytes();
                bulk.put(&k, v);
            }
            assert_eq!(bulk.get(b"k000042").as_deref(), Some(b"v42".as_slice()));
            bulk.finish();
        }
        {
            let mvcc = MVCC::open(&path);
            let tx = mvcc.begin_transaction();
            assert_eq!(tx.get(b"k000042"), Some(b"v42".to_vec()));
            assert_eq!(tx.get(b"k000999"), Some(b"v999".to_vec()));
            tx.commit();
        }
        cleanup(&path);
    }

    #[test]
    fn test_bulk_finish_with_compact() {
        let path = tmp_db("bulk_compact");
        {
            let mvcc = MVCC::open(&path);
            let mut bulk = mvcc.begin_bulk();
            // 小 batch 多次 → 多个 L0
            for batch in 0..8u32 {
                let mut items = Vec::new();
                for i in 0..50u32 {
                    let n = batch * 50 + i;
                    items.push((format!("k{n:06}").into_bytes(), format!("v{n}").into_bytes()));
                }
                bulk.put_batch_owned(items);
            }
            bulk.finish_with_compact(32);
        }
        {
            let mvcc = MVCC::open(&path);
            let tx = mvcc.begin_transaction();
            assert_eq!(tx.get(b"k000000"), Some(b"v0".to_vec()));
            assert_eq!(tx.get(b"k000399"), Some(b"v399".to_vec()));
            // 抽样中间 key
            assert_eq!(tx.get(b"k000150"), Some(b"v150".to_vec()));
            tx.commit();
        }
        cleanup(&path);
    }

    #[test]
    fn test_export_latest_visible() {
        let path = tmp_db("export");
        {
            let mvcc = MVCC::open(&path);
            let t1 = mvcc.begin_transaction();
            assert!(t1.set(b"k1", b"v1".to_vec()));
            assert!(t1.set(b"k2", b"v2".to_vec()));
            t1.commit();
            let t2 = mvcc.begin_transaction();
            assert!(t2.set(b"k1", b"v1b".to_vec()));
            assert!(t2.delete(b"k2"));
            assert!(t2.set(b"k3", b"v3".to_vec()));
            t2.commit();
        }
        {
            let mvcc = MVCC::open(&path);
            let live = mvcc.export_latest_visible(false);
            assert_eq!(live.len(), 2);
            assert_eq!(
                live.iter().find(|r| r.key == b"k1").unwrap().value,
                Some(b"v1b".to_vec())
            );
            assert_eq!(
                live.iter().find(|r| r.key == b"k3").unwrap().value,
                Some(b"v3".to_vec())
            );
            assert!(!live.iter().any(|r| r.key == b"k2"));

            let all = mvcc.export_latest_visible(true);
            assert_eq!(all.len(), 3);
            assert!(all.iter().any(|r| r.key == b"k2" && r.value.is_none()));
        }
        cleanup(&path);
    }

    #[test]
    fn test_vacuum_removes_old_versions() {
        let path = tmp_db("vac");
        {
            let mvcc = MVCC::open(&path);
            let t = mvcc.begin_transaction();
            assert!(t.set(b"a", b"1".to_vec()));
            assert!(t.set(b"b", b"old".to_vec()));
            t.commit();
            let t = mvcc.begin_transaction();
            assert!(t.set(b"a", b"2".to_vec()));
            assert!(t.delete(b"b"));
            t.commit();

            let st = mvcc.vacuum().unwrap();
            assert!(
                st.versions_removed >= 2,
                "应删除 a 旧版与 b 的历史, removed={}",
                st.versions_removed
            );

            let tx = mvcc.begin_transaction();
            assert_eq!(tx.get(b"a"), Some(b"2".to_vec()));
            assert_eq!(tx.get(b"b"), None);
            tx.commit();
        }
        cleanup(&path);
    }

    #[test]
    fn test_try_open_lock_conflict() {
        let path = tmp_db("lock2");
        let m1 = MVCC::try_open(&path).unwrap();
        let err = MVCC::try_open(&path).err();
        assert!(err.is_some());
        drop(m1);
        let m2 = MVCC::try_open(&path).unwrap();
        drop(m2);
        cleanup(&path);
    }

    /// 长跑进程不喊 vacuum，版本链也不能无限涨（I2：垃圾必须自己回来）。
    ///
    /// 当年就是 LSM 先撑不住（一天）而 Bitcask 撑了一个多月 —— 同一条通道在不同引擎里
    /// 涨速不同，所以**每个引擎都得自己默认开着**，不能靠「换引擎」当修法。
    #[test]
    fn test_auto_vacuum_reclaims_without_being_asked() {
        const CHAIN: u64 = 200;
        const SLICE: u64 = 8;

        let path = tmp_db("auto_vac");
        {
            let mvcc = MVCC::open(&path);
            assert_eq!(
                mvcc.vacuum_auto_slice(),
                MVCC::VACUUM_AUTO_SLICE_DEFAULT,
                "自动回收默认必须开着"
            );
            mvcc.set_vacuum_auto_slice(SLICE);

            for i in 0..CHAIN {
                let tx = mvcc.begin_transaction();
                assert!(tx.set(b"hot", format!("v{i}").into_bytes()));
                tx.commit();
                if i % 40 == 39 {
                    // 中途落一次 SST：让自动回收的窗口横跨 memtable 与 SST 两种源
                    mvcc.flush();
                }
            }

            let h = mvcc.health();
            assert!(
                h.max_chain <= SLICE + 2,
                "没人喊 vacuum，链却涨到 {} 行 —— 自动回收没在工作",
                h.max_chain
            );

            let tx = mvcc.begin_transaction();
            assert_eq!(tx.get(b"hot"), Some(format!("v{}", CHAIN - 1).into_bytes()));
            tx.commit();
        }
        cleanup(&path);
    }

    /// 关掉的开关必须真关掉：自动回收不能是「没法关掉的那种」。
    #[test]
    fn test_auto_vacuum_off_switch() {
        const CHAIN: u64 = 60;

        let path = tmp_db("auto_off");
        {
            let mvcc = MVCC::open(&path);
            mvcc.set_vacuum_auto_slice(0);
            for i in 0..CHAIN {
                let tx = mvcc.begin_transaction();
                assert!(tx.set(b"hot", format!("v{i}").into_bytes()));
                tx.commit();
            }
            assert_eq!(mvcc.health().max_chain, CHAIN, "关闭后不该裁掉任何一版");
        }
        cleanup(&path);
    }

    /// 大 value（≥ blob_threshold 4 KiB）走 LSM 自己的 blob 旁路，SST/memtable 里存的是指针。
    ///
    /// 这条用例先只证明一件事：**没 vacuum、没 compact，纯 set→get 也必须拿回原值**。
    /// 引擎里只有 `get`/`get_at` 会解析指针，而 MVCC 读走的是 `find_version_back`/
    /// `collect_latest_raw`/`range_scan`/`iter` 那一族 ⇒ 全都在往外发 9 字节的指针。
    /// Bitcask / B+ 树没这个洞，因为它们的 blob 分层在 MVCC 之上（storage.rs 里
    /// `decode_stored_value`），而 LSM 的分层在引擎**里面** —— 任何不走 `get` 的读都漏。
    #[test]
    fn test_big_value_roundtrips_through_every_read_path() {
        let path = tmp_db("blob_read");
        {
            let mvcc = MVCC::open(&path);
            let big = vec![0x5Au8; 8 * 1024];
            let tx = mvcc.begin_transaction();
            assert!(tx.set(b"big", big.clone()));
            assert!(tx.set(b"small", b"tiny".to_vec()));
            tx.commit();

            let tx = mvcc.begin_transaction();
            assert_eq!(tx.get(b"big"), Some(big.clone()), "点读拿到的是指针而不是值");
            assert_eq!(tx.get(b"small"), Some(b"tiny".to_vec()));
            tx.commit();

            // 折叠类读法（export / scan）同一条路，也要给回值
            let exported = mvcc.export_latest_visible(false);
            let got = exported
                .iter()
                .find(|r| r.key == b"big")
                .and_then(|r| r.value.clone());
            assert_eq!(got, Some(big.clone()), "export 拿到的是指针而不是值");

            // 落一次 SST：指针解析不能只在 memtable 里成立
            mvcc.flush();
            let tx = mvcc.begin_transaction();
            assert_eq!(tx.get(b"big"), Some(big));
            tx.commit();
        }
        cleanup(&path);
    }

    /// blob 旁路（`blobs/`，大 value 写外面）过去**从来没人回收**：compact 只看得到自己
    /// 正在合并的那几个文件，判断不了"这个键在别的层里还有更新的版本正引用同一个 blob"。
    ///
    /// 现在由 vacuum 的全库轮次收：先裁掉旧版本行，再删没人引用的 blob 文件。
    /// 断言的是**文件个数**，不是"跑完没报错"—— 这条通道原来的症状就是文件只增不减。
    #[test]
    fn test_vacuum_reclaims_unreferenced_blobs() {
        fn blob_count(mvcc: &MVCC) -> usize {
            let dir = mvcc.db_dir().join("blobs");
            std::fs::read_dir(dir)
                .map(|rd| rd.flatten().count())
                .unwrap_or(0)
        }

        const CHAIN: u64 = 20;
        let path = tmp_db("blob_gc");
        {
            let mvcc = MVCC::open(&path);
            // LSM 的 blob_threshold 默认 4 KiB ⇒ 8 KiB 的 value 一定走旁路
            for i in 0..CHAIN {
                let tx = mvcc.begin_transaction();
                assert!(tx.set(b"big", vec![i as u8; 8 * 1024]));
                tx.commit();
            }
            assert_eq!(blob_count(&mvcc), CHAIN as usize, "每版都该是一个独立的 blob 文件");

            // 有界轮次不动 blob 文件：全库收尾只属于 vacuum()
            let slice = mvcc.vacuum_bounded(None, 4).unwrap();
            assert!(slice.next_cursor.is_some() || slice.stats.versions_removed > 0);
            assert_eq!(
                blob_count(&mvcc),
                CHAIN as usize,
                "有界片不该去动 blob —— 引用关系只有全库才看得全"
            );

            let stats = mvcc.vacuum().unwrap();
            // 上一片可能已经裁掉过几版，所以按两轮之和核总账
            assert!(
                slice.stats.versions_removed + stats.versions_removed >= CHAIN as usize - 1,
                "版本行没被裁干净：{} + {}",
                slice.stats.versions_removed,
                stats.versions_removed
            );
            assert_eq!(stats.blob_rewritten, Some(true), "全库 vacuum 必须报它收了 blob");
            assert_eq!(blob_count(&mvcc), 1, "只剩存活那一版引用的 blob");
            assert_eq!(mvcc.health().total_rows, 1, "全库 vacuum 之后每把 key 只剩一行");

            // 活着的 value 必须还读得出来（回收不能把可见版本的 blob 删了）
            let tx = mvcc.begin_transaction();
            assert_eq!(tx.get(b"big"), Some(vec![(CHAIN - 1) as u8; 8 * 1024]));
            tx.commit();
        }
        cleanup(&path);
    }

    /// 有界回收：每轮只碰一小段，多轮跑完必须和一次全库 vacuum 得到**同样**的结果。
    ///
    /// LSM 的切片比另外三家麻烦：一扇窗口要横跨 memtable 和若干 SST，而**只有每个源都
    /// 读满了这段窗口**，归并结果才敢在窗口尾截断。所以这里中途 `flush()` 一次，
    /// 让版本行同时分布在 memtable 和 SST 里，逼这条路径真的被走到。
    /// 键里放了前缀嵌套的 `k1` / `k1x`：版本键 `raw ‖ 8B` 下两把 key 的区间会互相穿插，
    /// 这是窗口右界最容易切在组中间的情形。
    #[test]
    fn test_vacuum_bounded_slices_and_converges() {
        const CHAIN: u64 = 120;
        const KEYS: [&[u8]; 6] = [b"k1", b"k1x", b"k2", b"k3", b"k4", b"k5"];
        const SLICE: u64 = 40;

        fn fill(mvcc: &MVCC) {
            for i in 0..CHAIN {
                for k in KEYS {
                    let tx = mvcc.begin_transaction();
                    assert!(tx.set(k, format!("v{i}").into_bytes()), "写入被拒：{k:?}");
                    tx.commit();
                }
                if i == CHAIN / 2 {
                    mvcc.flush();
                }
            }
        }

        // 对照组
        let full_path = tmp_db("bounded_full");
        let full_removed;
        {
            let mvcc = MVCC::open(&full_path);
            fill(&mvcc);
            full_removed = mvcc.vacuum().unwrap().versions_removed;
        }
        cleanup(&full_path);

        let path = tmp_db("bounded_slice");
        {
            let mvcc = MVCC::open(&path);
            fill(&mvcc);

            let mut cursor: Option<Vec<u8>> = None;
            let mut rounds = 0usize;
            let mut removed = 0usize;
            loop {
                let round = mvcc.vacuum_bounded(cursor.as_deref(), SLICE).unwrap();
                assert!(!round.stats.skipped_active_txn, "第 {rounds} 轮不该被活跃事务挡住");
                assert!(
                    round.stats.rows_scanned <= SLICE + CHAIN,
                    "第 {rounds} 轮读了 {} 行（预算 {SLICE} + 一条链 {CHAIN}）",
                    round.stats.rows_scanned
                );
                assert!(round.stats.versions_removed > 0, "第 {rounds} 轮必须裁掉东西");
                removed += round.stats.versions_removed;
                rounds += 1;
                match round.next_cursor {
                    Some(c) => {
                        assert_ne!(cursor.as_ref(), Some(&c), "游标必须前进（第 {rounds} 轮）");
                        cursor = Some(c);
                    }
                    None => break,
                }
                assert!(rounds < 60, "切片应在有限轮内收敛，别是原地打转：已 {rounds} 轮");
            }

            assert!(rounds > 1, "6 把 key × {CHAIN} 版不该一轮就扫完：{rounds} 轮");
            assert_eq!(removed, full_removed, "分片裁的总行数应与整库一轮一致");
            let h = mvcc.health();
            assert_eq!(h.max_chain, 1, "分片跑完后链必须压平");
            assert_eq!(h.total_rows, KEYS.len() as u64);

            let tx = mvcc.begin_transaction();
            for k in KEYS {
                assert_eq!(tx.get(k), Some(format!("v{}", CHAIN - 1).into_bytes()));
            }
            tx.commit();
        }
        cleanup(&path);
    }

    /// 只读事务不许往 WAL 落一个字节，也不许付 fsync 的代价。
    ///
    /// 反向那半同样重要：**写过就必须有 `Commit` 记录 + fsync** —— 不然这条优化会悄悄
    /// 把"提交的持久化点"一起省掉，那比慢一千倍严重。
    #[test]
    fn test_read_only_transaction_touches_no_wal() {
        const READS: u64 = 200;

        let path = tmp_db("ro_wal");
        {
            let mvcc = MVCC::open(&path);
            let tx = mvcc.begin_transaction();
            assert!(tx.set(b"k", b"v".to_vec()));
            tx.commit();

            let wal = wal_path(&path);
            let size0 = std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0);
            for _ in 0..READS {
                let tx = mvcc.begin_transaction();
                assert_eq!(tx.get(b"k"), Some(b"v".to_vec()));
                tx.commit();
            }
            let size1 = std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0);
            assert_eq!(size1, size0, "{READS} 次只读事务往 WAL 落了 {} 字节", size1 - size0);

            // 写过的事务照旧进 WAL
            let tx = mvcc.begin_transaction();
            assert!(tx.set(b"k", b"w".to_vec()));
            tx.commit();
            let size2 = std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0);
            assert!(size2 > size1, "写入必须留下 Begin/Write/Commit 记录，持久化点不能被省掉");

            // 字节数当尺子**不够**：只落一条 `Commit` 也能让体积涨，于是"`Write` 记录整个没写"
            // 这种形状照样绿（M18d 实测：把 `wal_write` 改成不 append 直接返回成功，bitcask 那条
            // 一路通过）。判据必须精确到记录类型 —— 一次写要在 WAL 里留下一条 `Write`。
            {
                let mut reader = Wal::open(&wal).expect("打开 WAL 读记录");
                let records = reader.read_all().expect("读 WAL");
                let writes = records
                    .iter()
                    .filter(|e| matches!(&e.record, WalRecord::Write { .. }))
                    .count();
                assert_eq!(
                    writes, 2,
                    "两次真写要在 WAL 里各留一条 Write，实际 {writes} 条 \
                     （只数 Begin/Commit 的尺子抓不到丢了的 Write）"
                );
            }

            // 只读事务也不能把数据读坏（版本号照旧推进，快照语义不变）
            let tx = mvcc.begin_transaction();
            assert_eq!(tx.get(b"k"), Some(b"w".to_vec()));
            tx.commit();
        }
        cleanup(&path);
    }

    /// 库必须能自己报出「最长版本链」。
    ///
    /// 这次事故的核心数字（某把热点 key 堆了多少个版本）一直是靠数日志行数得出来的 ——
    /// 一个当基座用的库不该靠这个诊断自己。四引擎共用同一份 `MvccHealth`，
    /// 所以换引擎时这两组断言必须照样成立（涨速不同，指标是同一个）。
    ///
    /// 先 `flush()` 让版本行落在 SST 里：这样走的是 `light_view` + 跨源归并那条路，
    /// 而不是全在 memtable 里读内存。
    #[test]
    fn test_health_reports_chain_length() {
        const CHAIN: u64 = 500;

        let path = tmp_db("health");
        {
            let mvcc = MVCC::open(&path);
            for i in 0..CHAIN {
                let tx = mvcc.begin_transaction();
                assert!(tx.set(b"hot", format!("v{i}").into_bytes()), "第 {i} 次写入被拒");
                tx.commit();
            }
            for k in 0..3u64 {
                let key = format!("other{k}");
                let tx = mvcc.begin_transaction();
                assert!(tx.set(key.as_bytes(), b"x".to_vec()));
                tx.commit();
            }
            mvcc.flush();

            let base = crate::lsm_tree::sst::value_materializations();
            let h = mvcc.health();
            let used = crate::lsm_tree::sst::value_materializations() - base;
            assert_eq!(h.live_keys, 4, "逻辑 key 数");
            assert_eq!(h.total_rows, CHAIN + 3, "物理行数（含历史版本）");
            assert_eq!(h.max_chain, CHAIN, "最长版本链应由库自己报出来");
            assert_eq!(h.rows_above_one, CHAIN - 1);
            assert_eq!(h.active_txns, 0);
            // 采样链长这件事本身不该物化任何 value
            assert_eq!(used, 0, "health 期间 value 物化次数：{used}（历史行数 {}）", h.total_rows);

            let stats = mvcc.vacuum().unwrap();
            assert!(!stats.skipped_active_txn, "无活跃事务时不该跳过");
            let after = mvcc.health();
            assert_eq!(after.max_chain, 1, "vacuum 后最长链应降到 1");
            assert_eq!(after.total_rows, 4, "vacuum 后应只剩每 key 一行");
            assert_eq!(after.live_keys, 4);
            assert_eq!(after.rows_above_one, 0);
        }
        cleanup(&path);
    }

    // ---- 下游报告的形状：点查墙钟时间基线（手动 release 跑） -------------------
    //
    // 下游把本 crate 嵌进去之后实测：**单次点查 0.86 ms**（5002 键 / 40B 键 / 60B 值 /
    // 10005 行 / max_chain=3 / active_txns=0，全 release）。而 `health()` 与
    // `test_point_op_cost_independent_of_chain_length` 那套守卫给的都是**块访问次数** ——
    // 它把块缓存命中和未命中算成同一个数，所以"链没失控、块访问只有 4 次"和"每次点查快 1 毫秒"
    // 这两件事**能同时成立**。这个基线就是补上漏掉的那一环。
    //
    // 本机（Windows / NTFS / release）复现到的分配：
    //   改造前  乱序 289.0 µs/次（未命中 1.61/次）  vs  有序 45.6 µs/次（未命中 0.15/次）
    //           ⇒ 差值 243 µs ÷ 1.46 次未命中 ≈ **一次 File::open ≈ 167 µs**
    //             —— 旧 `read_slice` 每次未命中都重开一次文件，而这个 open 比读那几 KB 贵一个量级。
    //   改造后 乱序 120.3 µs/次、有序 21.0 µs/次、multi_get 19.5、一次全量 scan 19.1 µs/行
    //           ⇒ 未命中次数没变（1.61/0.15），墙钟少了一半多：省掉的正是那次重开文件。
    //           剩下 乱序/有序 还差 5.7 倍，那部分不是 open，而是 **4 块/次**（定位一趟 +
    //           定论一趟）× 未命中的解码，属于下一条里程碑（一次定论 + 索引二分 + 缓存容量）。
    // 结论"用法也有份"的另一半：读全部键时一次 `scan` 是 19 µs/行，逐条 `get` 是它的 1~6 倍。
    //
    // 复现命令（本仓库）：
    //   cargo test --release --lib -- --ignored --nocapture point_read_baseline
    #[test]
    #[ignore = "墙钟基线，手动 release 跑：cargo test --release --lib -- --ignored --nocapture point_read_baseline"]
    fn test_lsm_point_read_baseline_5000_keys() {
        use std::time::Instant;

        const N: usize = 5002;

        // 键形状照抄下游：1 字节标签 + 4 字节项目 id + 路径文本 ≈ 40 字节
        fn make_key(i: usize) -> Vec<u8> {
            let tag = b'1' + (i % 3) as u8;
            let pid = 1000 + i % 7;
            let mut k = Vec::new();
            k.push(tag);
            k.extend_from_slice(format!("#{pid:04}#worldquant/datafields/{i}/f.json").as_bytes());
            while k.len() < 40 {
                k.push(b'0');
            }
            k
        }
        // 值 ≈ 60 字节：必须远小于 blob 阈值 4 KiB，否则就不是同一条路径
        fn make_val(i: usize) -> Vec<u8> {
            format!("{{\"i\":{i},\"h\":\"{:0<24}\",\"n\":97}}", i * 7)
                .into_bytes()
        }

        let path = tmp_db("point_baseline_5k");
        let keys: Vec<Vec<u8>> = (0..N).map(make_key).collect();
        let vals: Vec<Vec<u8>> = (0..N).map(make_val).collect();
        assert!(vals.iter().all(|v| v.len() < 4 * 1024), "值必须小到不进 blob 旁路");

        // 写路径照抄下游：begin → batch_set(2000) → commit，两轮全量 + 一轮单键
        // ⇒ total_rows = 5002×2+1 = 10005、live_keys = 5002、max_chain = 3
        {
            let mvcc = MVCC::open(&path);
            for round in 0..3 {
                let upto = if round == 2 { 1 } else { N };
                for base in (0..upto).step_by(2000) {
                    let end = (base + 2000).min(upto);
                    let items: Vec<(Vec<u8>, Vec<u8>)> =
                        (base..end).map(|i| (keys[i].clone(), vals[i].clone())).collect();
                    let tx = mvcc.begin_transaction();
                    if let Err(bad) = tx.batch_set(&items) {
                        panic!("batch_set 冲突: {}", String::from_utf8_lossy(&bad));
                    }
                    tx.commit();
                }
            }
            // 让数据真的落到 SST（下游是重开进程后读的；flush 是同一条路的近似）
            mvcc.flush();
            let h = mvcc.health();
            eprintln!(
                "形状: rows={} live_keys={} max_chain={} active_txns={}",
                h.total_rows, h.live_keys, h.max_chain, h.active_txns
            );
            assert_eq!(h.total_rows as usize, N * 2 + 1);
            assert_eq!(h.live_keys as usize, N);
            assert_eq!(h.max_chain, 3);
        }

        // 重新打开，模拟"进程启动后读"
        let mvcc = MVCC::open(&path);
        let stats = mvcc.kv.lock().unwrap().stats();
        eprintln!(
            "布局: levels={:?} total_sst={} l0={} mem_entries={}",
            stats.level_files, stats.total_sst, stats.l0_files, stats.mem_entries
        );

        // 乱序（≈ 目录序 / NTFS 哈希序）与有序两种访问顺序，分开量：局部性影响块缓存命中率
        let mut shuffled: Vec<usize> = (0..N).collect();
        let mut s = 0x853c_49e6_77b9_5b3eu64;
        for i in (1..N).rev() {
            s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let j = (s >> 33) as usize % (i + 1);
            shuffled.swap(i, j);
        }

        let tx = mvcc.begin_transaction();

        let report = |name: &str, dur: std::time::Duration, n: usize, blocks: u64, opens: u64| {
            let us = dur.as_micros() as f64;
            eprintln!(
                "{name:<22} 合计 {:>9.1} ms | 单次 {:>8.1} µs | 块/次 {:>5.2} | 未命中/次 {:>5.2}",
                us / 1000.0,
                if n == 0 { 0.0 } else { us / n as f64 },
                blocks as f64 / n.max(1) as f64,
                opens as f64 / n.max(1) as f64
            );
        };

        // (a) 同一个事务里连读 5002 次 —— 乱序
        let (b0, o0) = (crate::lsm_tree::sst::block_reads(), crate::lsm_tree::sst::block_misses());
        let t = Instant::now();
        for &i in &shuffled {
            assert!(tx.get(&keys[i]).is_some(), "乱序读第 {i} 个键落空");
        }
        let (d, db, do_) = (
            t.elapsed(),
            crate::lsm_tree::sst::block_reads() - b0,
            crate::lsm_tree::sst::block_misses() - o0,
        );
        report("点读×N 乱序", d, N, db, do_);

        // (b) 同一个事务里连读 5002 次 —— 有序（键升序 = 块升序）
        let (b0, o0) = (crate::lsm_tree::sst::block_reads(), crate::lsm_tree::sst::block_misses());
        let t = Instant::now();
        for k in &keys {
            assert!(tx.get(k).is_some(), "有序读落空");
        }
        let (d, db, do_) = (
            t.elapsed(),
            crate::lsm_tree::sst::block_reads() - b0,
            crate::lsm_tree::sst::block_misses() - o0,
        );
        report("点读×N 有序", d, N, db, do_);

        // (c) multi_get：门面把它实现成 `map(get)`，所以理论上和 (a) 同价 —— 量出来才算数
        let (b0, o0) = (crate::lsm_tree::sst::block_reads(), crate::lsm_tree::sst::block_misses());
        let t = Instant::now();
        let got = tx.multi_get(&keys);
        let (d, db, do_) = (
            t.elapsed(),
            crate::lsm_tree::sst::block_reads() - b0,
            crate::lsm_tree::sst::block_misses() - o0,
        );
        assert_eq!(got.len(), N);
        report("multi_get×N", d, N, db, do_);

        // (d) 一次全量 scan —— "干脆整段读回来"那条路
        let (b0, o0) = (crate::lsm_tree::sst::block_reads(), crate::lsm_tree::sst::block_misses());
        let t = Instant::now();
        let rows = tx.scan(None, None);
        let (d, db, do_) = (
            t.elapsed(),
            crate::lsm_tree::sst::block_reads() - b0,
            crate::lsm_tree::sst::block_misses() - o0,
        );
        eprintln!("全量 scan 命中 {} 行", rows.len());
        report("一次全量 scan", d, rows.len().max(1), db, do_);

        // (e) 只要 4 条（下游"进程启动 + 打开 + 扫 4 条"那一段）
        let (b0, o0) = (crate::lsm_tree::sst::block_reads(), crate::lsm_tree::sst::block_misses());
        let t = Instant::now();
        for k in &keys[..4] {
            assert!(tx.get(k).is_some());
        }
        let (d, db, do_) = (
            t.elapsed(),
            crate::lsm_tree::sst::block_reads() - b0,
            crate::lsm_tree::sst::block_misses() - o0,
        );
        report("点读×4", d, 4, db, do_);

        drop(tx);
        drop(mvcc);
        cleanup(&path);

        // (f) 单次点查的代价 = **两趟**（定位 + 定论）× **含这个键的 SST 数** × 未命中。
        //     所以真正该盯的量是"同一把键的版本散在几个文件里"：库里每多一层/每多一个覆盖
        //     该键的 SST，点查就多探一次块。compact 的意义就是把这件事压回层数。
        //     造法：每轮全量写一遍再 flush ⇒ 每个键在每个文件里都有一份。
        eprintln!("--- 覆盖同一把键的 SST 数 → 单次点查代价 ---");
        for files in [1usize, 2, 4, 7] {
            let p2 = tmp_db(&format!("point_spread_{files}"));
            {
                let mv = MVCC::open(&p2);
                // 关掉自动回收：7 轮全量写会把 pending 推过切片宽度，实验形状会被库自己改掉
                mv.set_vacuum_auto_slice(0);
                for _round in 0..files {
                    for base in (0..N).step_by(2000) {
                        let end = (base + 2000).min(N);
                        let items: Vec<(Vec<u8>, Vec<u8>)> =
                            (base..end).map(|i| (keys[i].clone(), vals[i].clone())).collect();
                        let tx = mv.begin_transaction();
                        if let Err(bad) = tx.batch_set(&items) {
                            panic!("batch_set 冲突: {}", String::from_utf8_lossy(&bad));
                        }
                        tx.commit();
                    }
                    mv.flush();
                }
                let st = mv.kv.lock().unwrap().stats();
                let h = mv.health();
                let tx = mv.begin_transaction();
                let b0 = crate::lsm_tree::sst::block_reads();
                let m0 = crate::lsm_tree::sst::block_misses();
                let t = Instant::now();
                for &i in &shuffled {
                    assert!(tx.get(&keys[i]).is_some());
                }
                let (d, db, dm) = (
                    t.elapsed(),
                    crate::lsm_tree::sst::block_reads() - b0,
                    crate::lsm_tree::sst::block_misses() - m0,
                );
                report(&format!("乱序点读 (每键 {files} 份)"), d, N, db, dm);
                eprintln!(
                    "    ↳ total_sst={} levels={:?} rows={} max_chain={}",
                    st.total_sst, st.level_files, h.total_rows, h.max_chain
                );
            }
            cleanup(&p2);
        }
    }

    /// 写侧归因：**灌一行**的代价为什么会随库规模涨。
    ///
    /// 触发它的那次测量：2026-10-17 的大规模基线（release，同一份构建对照跑）显示
    /// `灌 20000 行(事务)` = 38.8 µs/行、`灌 200000 行(事务)` = **227.8 µs/行** ⇒ 每行贵 5.9 倍，
    /// 十倍的量花掉约五十九倍的时间。台架只报"µs/行"，报不出**为什么**，而这条通道在写侧 ——
    /// 恰恰是下游最在意的那一条（进程启动后一直往里灌）。
    ///
    /// 这条按块（默认每 2 万行）打一行：块内每行耗时 + 当时的 memtable 字节 + 每千行块读次数 +
    /// `begin`/`set`/`commit` 三段各自的每事务耗时，并在块末做两个探针（见下面的排除清单）。
    /// 目的只有一个：**把"每行为什么变贵"归到某一段上**，而不是让 `µs/行` 一个数替整条路径说话。
    ///
    /// 跑法（默认 20 万行、每 2 万一行；想小一点给环境变量）：
    /// ```text
    /// LSM_FILL_ROWS=200000 LSM_FILL_CHUNK=20000 cargo test --release --lib -- --ignored \
    ///     --nocapture --test-threads=1 lsm_fill_attribution
    /// ```
    #[test]
    #[ignore = "写侧归因，手动 release 单线程跑：LSM_FILL_ROWS=200000 LSM_FILL_CHUNK=20000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 lsm_fill_attribution"]
    fn lsm_fill_attribution_per_row_cost() {
        use std::time::Instant;

        let rows: usize = std::env::var("LSM_FILL_ROWS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(200_000);
        let chunk: usize = std::env::var("LSM_FILL_CHUNK")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(20_000);

        // 键/值形状照抄门面台架：40 B 键、60 B 值（值必须小到不进 blob 旁路，否则不是同一条路）
        fn make_key(i: usize) -> Vec<u8> {
            let tag = b'1' + (i % 3) as u8;
            let pid = 1000 + i % 7;
            let mut k = Vec::new();
            k.push(tag);
            k.extend_from_slice(format!("#{pid:04}#worldquant/datafields/{i}/f.json").as_bytes());
            while k.len() < 40 {
                k.push(b'0');
            }
            k
        }
        fn make_val(i: usize) -> Vec<u8> {
            format!("{{\"i\":{i},\"h\":\"{:0<24}\",\"n\":97}}", i * 7).into_bytes()
        }

        // 已经用这条台架排除掉的：
        //   · 探 SST ⇒ 块读全程 0（前十万行 total_sst=0），不是"写侧冲突探测在探文件"；
        //   · 自动回收的 tick ⇒ 开/关两条曲线一模一样（55→143→259→558→1047 µs/行）；
        //   · memtable 插入本身 ⇒ 走 `insert_fast` 的 bulk 在 200k 是 2.0 µs/行（平的）；
        //   · begin/commit ⇒ 三段计时里 begin≈1 µs、commit≈2.7 ms 恒定，全部代价都在 `set` 段
        //     （每事务 69 ms → 1001 ms）。
        // 剩下两个还没分开的因子，各由一次探针认出来：
        //   A `事务内连写` —— 开一个事务连写 PROBE 行，**分别计时前 200 行与后 200 行**：
        //     后 200 明显贵 ⇒ 代价 ∝ **写集宽度**（`TxnCore::write` 里那句 `writes.iter().any(...)`
        //     线性查重）；两者一样贵、但随块号涨 ⇒ 代价 ∝ **当时的 memtable 规模**。
        //   B `裸 insert` —— 不经 MVCC（无冲突探测、无 WAL、无写集），直接 `LSMTree::insert`
        //     （仍是 `!fast`，带 live 统计维护）⇒ 若它也随规模涨，问题在存储层而不事务层。
        const PROBE: usize = 2_000;
        const EDGE: usize = 200;

        let path = tmp_db("fill_attribution");
        let mvcc = MVCC::open(&path);
        mvcc.set_vacuum_auto_slice(0);
        eprintln!("=== 灌入形状：每 1000 行一事务（自动回收关，已排除）===");
        eprintln!(
            "{:>8} {:>9} {:>9} {:>9} {:>9} {:>8} {:>11} {:>10} {:>10} {:>10}",
            "已灌行", "µs/行", "begin/事务", "set/事务", "commit/事务", "mem_KB", "块读/千行",
            "探针前200", "探针后200", "裸ins/行"
        );
        let mut done = 0usize;
        while done < rows {
            let end = (done + chunk).min(rows);
            let b0 = crate::lsm_tree::sst::block_reads();
            // 三段各自计时：begin（含自动回收 tick）/ batch_set（写序列）/ commit（WAL 收尾）。
            let (mut t_begin, mut t_write, mut t_commit, mut ntxn) = (0u128, 0u128, 0u128, 0u64);
            let t = Instant::now();
            for base in (done..end).step_by(1000) {
                let stop = (base + 1000).min(end);
                let items: Vec<(Vec<u8>, Vec<u8>)> =
                    (base..stop).map(|i| (make_key(i), make_val(i))).collect();
                let s = Instant::now();
                let tx = mvcc.begin_transaction();
                t_begin += s.elapsed().as_micros();
                let s = Instant::now();
                if let Err(bad) = tx.batch_set(&items) {
                    panic!("batch_set 冲突: {}", String::from_utf8_lossy(&bad));
                }
                t_write += s.elapsed().as_micros();
                let s = Instant::now();
                tx.commit();
                t_commit += s.elapsed().as_micros();
                ntxn += 1;
            }
            let ms = t.elapsed().as_secs_f64() * 1000.0;
            let n = end - done;
            let blocks = crate::lsm_tree::sst::block_reads() - b0;

            // 探针 A：同一个事务里连写 PROBE 行全新键，前 EDGE 与后 EDGE 分别计时
            let (mut first, mut last) = (0u128, 0u128);
            {
                let tx = mvcc.begin_transaction();
                for j in 0..PROBE {
                    let s = Instant::now();
                    tx.set_raw(&make_key(rows + 10 + j), make_val(rows + 10 + j));
                    let us = s.elapsed().as_micros();
                    if j < EDGE {
                        first += us;
                    } else if j >= PROBE - EDGE {
                        last += us;
                    }
                }
                tx.rollback();
            }

            // 探针 B：裸 insert（不经事务：无冲突探测、无 WAL、无写集）
            let s = Instant::now();
            {
                let mut kv = mvcc.kv.lock().unwrap();
                for j in 0..PROBE {
                    let k = make_key(rows + 5_000 + j);
                    let enc = encode_key(&k, 7);
                    kv.insert(enc, Some(crate::common::pack_plain(&make_val(rows + 5_000 + j))));
                }
            }
            let raw_us = s.elapsed().as_micros() as f64 / PROBE as f64;

            let st = mvcc.kv.lock().unwrap().stats();
            let per = |v: u128| v as f64 / ntxn.max(1) as f64;
            eprintln!(
                "{:>8} {:>9.1} {:>9.1} {:>9.1} {:>9.1} {:>8} {:>11.2} {:>10.1} {:>10.1} {:>10.1}",
                end,
                ms * 1000.0 / n as f64,
                per(t_begin),
                per(t_write),
                per(t_commit),
                st.mem_bytes / 1024,
                blocks as f64 * 1000.0 / n as f64,
                first as f64 / EDGE as f64,
                last as f64 / EDGE as f64,
                raw_us,
            );
            done = end;
        }
        let h = mvcc.health();
        eprintln!(
            "收尾: rows={} live_keys={} max_chain={}",
            h.total_rows, h.live_keys, h.max_chain
        );
        drop(mvcc);
        cleanup(&path);
    }

    /// 窗口 scan 的代价必须随**窗口宽度**缩 —— LSM 的尺子是「读了多少个 SST 数据块」。
    ///
    /// 旧形状：`collect_latest_raw` 走 `scan_latest_light(全库)`，把每个 memtable 整表遍历、
    /// 每个 SST **整篇块读完**再交给归并，调用方随后逐条筛界 ⇒ 10 行的窗口要付全库的块读代价
    /// （大库基线 release 实测：窗口 scan 405 µs/行 vs 一次全量 scan 4.0 µs/行）。
    /// 现在界下推到每一个源：memtable 走 `range`，SST 从 `from` 所在块起读、键越过上界立即
    /// 收手，并用 min/max 先挡掉整篇不相干的文件。
    ///
    /// 必须先 `flush()`：数据若还全在 memtable 里，块读次数为 0，那条比值断言就是空转。
    #[test]
    fn test_windowed_scan_cost_scales_with_window_not_library() {
        const KEYS: u64 = 3000;
        let path = tmp_db("window_cost");
        {
            let mvcc = MVCC::open(&path);
            for i in 0..KEYS {
                let tx = mvcc.begin_transaction();
                assert!(tx.set(format!("k{i:05}").as_bytes(), format!("v{i}").into_bytes()));
                tx.commit();
            }
            mvcc.flush();

            let tx = mvcc.begin_transaction();
            let b0 = crate::lsm_tree::sst::block_reads();
            let all = tx.scan(None, None);
            let full = crate::lsm_tree::sst::block_reads() - b0;
            assert!(
                full >= 30,
                "样本要有足够多的块才分得出窗口与全库（全量只读了 {full} 块）"
            );

            let lo = b"k01000".to_vec();
            let hi = b"k01010".to_vec();
            let b0 = crate::lsm_tree::sst::block_reads();
            let win = tx.scan(Some(&lo), Some(&hi));
            let windowed = crate::lsm_tree::sst::block_reads() - b0;

            assert_eq!(all.len() as u64, KEYS, "全量应折出 {KEYS} 行");
            assert_eq!(win.len(), 10, "[k01000, k01010) 应给 10 行");
            let want: Vec<Vec<u8>> = all
                .iter()
                .filter(|r| r.key.as_slice() >= lo.as_slice() && r.key.as_slice() < hi.as_slice())
                .map(|r| r.key.clone())
                .collect();
            let got: Vec<Vec<u8>> = win.iter().map(|r| r.key.clone()).collect();
            assert_eq!(got, want, "下推界之后窗口结果必须与全量筛一致");
            assert!(
                windowed * 10 <= full,
                "10 行窗口的块读不该是全库量级（{windowed} vs 全量 {full}）"
            );

            // 上界哨兵的陷阱形状（`seek_prev` 的界是 `key ‖ 0x00`）
            assert_eq!(tx.seek_prev(b"k01005").unwrap().key, b"k01005");
            assert_eq!(tx.seek(b"k01000").unwrap().key, b"k01000");

            // 前缀同理：k001xx ⇒ 100 个键
            let b0 = crate::lsm_tree::sst::block_reads();
            let pre = tx.prefix_scan(b"k001");
            let prefixed = crate::lsm_tree::sst::block_reads() - b0;
            assert_eq!(pre.len(), 100, "前缀 k001 应命中 100 行");
            assert!(
                prefixed * 4 <= full,
                "前缀扫的块读不该是全库量级（{prefixed} vs 全量 {full}）"
            );

            // 边界形状：前缀是**所有键的真前缀**（比第 0 块的首键还短 ⇒ 稀疏索引里没有
            // "first_key ≤ from" 的块）。这一步一旦按"没找到就跳过整篇"处理，
            // prefix_scan 会静默返回近乎空的结果 —— 复测表里 lsm 的 key_count 报 0 就是这么来的。
            let whole = tx.prefix_scan(b"k");
            assert_eq!(whole.len() as u64, KEYS, "前缀 k 应命中全部 {KEYS} 行");
            assert_eq!(tx.key_count(b"k") as u64, KEYS, "key_count 同一个形状");
            tx.rollback();
        }
        cleanup(&path);
    }
}

impl crate::common::VersionedStore for Transaction {
    /// 
    type Row = Vec<u8>;

    fn scan_candidates(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        accept: Option<&dyn Fn(&[u8]) -> bool>,
        after: Option<&[u8]>,
        budget: usize,
    ) -> (Vec<crate::common::WinCandidate<Vec<u8>>>, bool) {
        // 走本类型上的同名 inherent 方法（inherent 优先于 trait ⇒ 不会自我递归）
        self.scan_candidates(start, end, accept, after, budget)
    }

    fn materialize(&self, row: Vec<u8>) -> crate::common::RowValue {
        self.read_row(&row)
    }
}
