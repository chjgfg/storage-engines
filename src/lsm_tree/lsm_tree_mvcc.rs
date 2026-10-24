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

use std::eprintln;
#[cfg(test)]
use std::thread_local;
use std::format;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
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
use crate::common::txn::{BulkCore, Registry, TxnCore, WalTail, WriteSide};
use crate::common::vacuum_plan_group;
use crate::common::version_key_upper_bound;

/// M37：一次 `vacuum_bounded` 走查的**块大小**（物理行）—— 与 bitcask 那条同源同值，
/// 两家共用"块界"这件事由各自的守卫各自钉住（`test_vacuum_deletes_in_bounded_chunks`）。
///
/// 为什么要有块：`light_iter_range` 的 `LightMerge` 按 `&self` 借住 LSM 树，而删除要 `&mut kv`
/// ⇒ 借住期间只能攒，攒出来的 `to_delete` + 交给 blob GC 的存活行清单就是 M37 分段量出来的
/// 那笔 O(全库) 的账（600k 档 `vacuum()` 瞬时 101.2 MB）。按块出借用后立刻清空 ⇒ O(本常数)。
const VAC_CHUNK_ROWS: usize = 8192;

// 测试专用（M37 的尺子，与 bitcask 那把同形）：单次 flush 前最多攒了多少待处理行。
#[cfg(test)]
thread_local! {
    static VAC_PENDING_MAX: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn note_vac_pending(n: usize) {
    VAC_PENDING_MAX.with(|c| {
        let n = n as u64;
        if n > c.get() {
            c.set(n);
        }
    });
}

#[cfg(test)]
pub(crate) fn vac_pending_max() -> u64 {
    VAC_PENDING_MAX.with(|c| c.get())
}

#[cfg(test)]
pub(crate) fn reset_vac_pending() {
    VAC_PENDING_MAX.with(|c| c.set(0));
}

/// MVCC 存储引擎（LSM 后端）
pub struct MVCC {
    kv: Arc<Mutex<LSMTree>>,
    wal: Arc<Mutex<Wal>>,
    active_txn: Arc<Mutex<Registry>>,
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
    /// ⚠️ `for_mvcc` 档开着块缓存**自动档**（`block_cache_auto_max: 1024`），所以
    /// `block_cache_blocks` 在这里是**地板**而不是生效值：
    /// `生效容量 = clamp(库的 SST 块数, block_cache_blocks, block_cache_auto_max)`。
    /// 上面那个例子读作"至少 1024 块、最多 1024 块"= 钉死 1024。想要钉死别的数就把上限
    /// 一起改（`block_cache_auto_max: 0` = 关自动档，回到"配置值即生效值"）。
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
        // 「有没有东西要重放」必须在插进 memtable 之前判 —— 之后 memtable 非空就再也分不出
        // 是 redo 撑的还是别人撑的（`pin_mvcc_invariants` 已把 LSM 自己的 mem WAL 关掉，
        // 所以空 plan ⇒ 空 memtable 这条推理在这里是成立的）。
        let has_redo = !plan.committed_writes.is_empty() || !plan.uncommitted_writes.is_empty();

        for (_xid, key, value) in &plan.committed_writes {
            tree.insert(key.clone(), value.clone());
        }
        for (_xid, key) in &plan.uncommitted_writes {
            let _ = tree.remove(key);
        }

        // 不再全表 scan_max_version：用 checkpoint / WAL xid / redo key / 持久化元数据
        let start_ver = compute_start_version(&db_dir, &plan);

        // **只有 redo 非空才 flush**。理由不是"省一次 fsync"：`LSMTree::flush()` 末尾无条件
        // 走 `maybe_compact()`，而压实是**无界的写路径工作**。实测（`reopen_shape_probe`，
        // release，600k 键 + bulk 收尾）：bulk 的 `finish()` 把 WAL 收干净了（redo 0 条），
        // 却把 L0 停在 `l0_compact_threshold`(8) 上 ⇒ 这次 open 自己把 85 MB 从 L0 搬进
        // L1/L2、写出 37 个文件。同一轮剖面 A/B（`uncond_flush` vs `cond_flush`）：总
        // **13 947 618.8 µs vs 8 362.3 µs = 1668×**，其中 flush 段 13 938 766.2 vs 0.0；
        // 门面台架同档跨轮读数是 17 487 030.8 → 9 106.7 µs。同一形状在 200k 只有 4 个 L0
        // 文件（<8）⇒ 不压 ⇒ 两臂同价 —— 这就是那条 1700× 悬崖的来源：**开关是 L0 文件数
        // 是否越线，跟"恢复"这件事本身没关系**。
        //
        // redo 非空时仍然必须 flush：`seal_wal_checkpoint` 随后就把 WAL 截断了，那些重放进
        // memtable 的行只有落了盘才不会跟着 WAL 一起消失（这条由
        // `test_open_persists_redo_before_truncating_wal` 钉着）。
        if has_redo {
            tree.flush()?;
        }
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
    /// [`pin_mvcc_invariants`]。其余字段立刻生效：块缓存**就地改容量**（不清空已驻留的块），
    /// compact/l0 阈值影响下一次写路径。
    pub fn set_options(&self, patch: LSMTreeOptions) {
        let mut kv = self.kv.lock().unwrap();
        kv.set_options(pin_mvcc_invariants(patch));
    }

    /// 本引擎能被调的东西 —— 清单在这里，不在门面。
    pub fn tuning(&self) -> EngineTuning {
        let mut t = EngineTuning::default();
        t.push_shared(self.vacuum_auto_slice(), Some(self.wal_checkpoint_limit()));
        let kv = self.kv.lock().unwrap();
        let o = kv.options().clone();
        t.push("mem_threshold_bytes", o.mem_threshold_bytes);
        t.push("mem_threshold_entries", o.mem_threshold_entries);
        t.push("block_cache_blocks", o.block_cache_blocks);
        t.push("block_cache_auto_max", o.block_cache_auto_max);
        // 生效值，不是配置值：自动档下 `block_cache_blocks` 只是地板
        t.push("block_cache_capacity", kv.block_cache_capacity());
        // 分母（M32）：没有它，调用方看见"容量 1024"分不清是"顶撞上了"还是"库就这么点块"
        t.push("sst_blocks", kv.sst_block_count());
        // 预算档（M76）：**给的字节**要报 —— 生效块数已经在 `block_cache_capacity` 那一格里了，
        // 两个数同时在场，调用方才看得出"预算换算成了多少槽"以及"是不是被库自己的块数封顶了"。
        t.push("cache_budget_bytes", kv.cache_budget_bytes());
        drop(kv);
        t.push("use_mmap", o.use_mmap);
        t.push("l0_compact_threshold", o.l0_compact_threshold);
        t.push("l0_slowdown_trigger", o.l0_slowdown_trigger);
        t.push("l0_stop_trigger", o.l0_stop_trigger);
        t.push("target_sst_bytes", o.target_sst_bytes);
        t.push("blob_threshold", o.blob_threshold);
        t
    }

    /// 块缓存自动档的上限（`0` = 关自动档，`usize::MAX` = 不设顶）。见
    /// [`LSMTree::set_block_cache_auto_max`]。
    pub fn set_cache_auto_max(&self, blocks: usize) {
        self.kv.lock().unwrap().set_block_cache_auto_max(blocks);
    }

    pub fn cache_auto_max(&self) -> usize {
        self.kv.lock().unwrap().options().block_cache_auto_max
    }

    /// 按**内存预算**给块缓存档（M76）：`bytes ÷ 每槽真实常驻` = 槽数上限，返回换算后的生效块数。
    ///
    /// `0` = 撤掉预算档（回到 `block_cache_blocks` / `block_cache_auto_max` 那条，与改前一字不差）。
    /// 它与"抬顶"是两道正交的约束（取小），预算**允许压到地板以下**，且在 flush/compact 之后的
    /// 重估里继续生效。存在的理由见 [`LSMTree::cache_budget_bytes`]。
    pub fn set_cache_budget_bytes(&self, bytes: u64) -> usize {
        self.kv.lock().unwrap().set_cache_budget_bytes(bytes)
    }

    /// 调用方给的那一档字节（生效块数 = `tuning()["block_cache_capacity"]`）
    pub fn cache_budget_bytes(&self) -> u64 {
        self.kv.lock().unwrap().cache_budget_bytes()
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
        // M44：收集壳 —— 判据与遍历只有一份，在 `export_latest_visible_for_each`。
        // ⚠️ 返回整库的 `Vec` 是这条的合同（600k 档实测 +123.7 MB）；只需要"把行交出去"的
        // 人（导出成文件、搬去另一家、数个数）请走流式那条。
        let mut out: Vec<ExportRecord> = Vec::new();
        self.export_latest_visible_for_each(include_deleted, |rec| {
            out.push(rec);
            true
        });
        out
    }

    /// 流式导出（M44）：开一个快照事务，折一行交一行，**一行都不攒**；`on` 返回 `false`
    /// 当场收手。返回实际交出去的行数。
    ///
    /// 为什么库侧要提供这条：折叠那一段早就是流式的（M27），但出口只有返回 `Vec` 的那条 ——
    /// 于是"把 600k 键的库导成文件"必须先在内存里摆一整套 `ExportRecord`，而这套东西
    /// 存在的唯一目的就是等着被逐行写出去。`export_kv` 那几个 CLI 原先还在这之上**又**
    /// `map` 成一份 `Vec<KvRecord>` ⇒ 全库同时在内存里摆了两遍。
    pub fn export_latest_visible_for_each(
        &self,
        include_deleted: bool,
        mut on: impl FnMut(ExportRecord) -> bool,
    ) -> usize {
        let tx = self.begin_transaction();
        let handed = tx.export_latest_visible_for_each(include_deleted, &mut on);
        tx.commit();
        handed
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

    /// 全库健康度快照。**只走惰性的轻量归并**（[`LSMTree::light_iter_range`]：版本号 + 是不是
    /// 墓碑），一趟、不物化任何 value，也不把任何一个源整篇读进 `Vec`（M39 之前它走
    /// `scan_latest_light`，那个会把每个 SST 整篇折进内存 ⇒ 瞬时 +126.1 MB @600k）。
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
        //
        // ⚠️ M39：必须走**惰性**的 `light_iter_range`，不能走 `scan_latest_light` —— 后者以前
        // 把每个 SST 用 `SstFile::light_view` 整篇折进 `Vec`、所有源同时活着，600k 档一次
        // `health()` 瞬时 **+126.1 MB**（旧内存量具报 +0.0，直到 M38 修好尺子才看见）。
        // 守卫：`test_health_does_not_materialize_its_sources`。
        for (enc, le) in kv.light_iter_range(None, None, u64::MAX) {
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
    /// 遍历用 [`LSMTree::light_iter_range`]（惰性 k 路归并，**凑够预算就当场收手**），
    /// 所以一轮的代价与库总量无关 —— M29 之前这里走的是 `scan_latest_light_window`
    /// （**每个源各取满 `预算` 行**再归并），代价 = `源数 × 预算`，于是"一片"的每行单价
    /// 随 SST 文件数涨、也就是随库涨（600k 实测 5.8~11×，凭据见
    /// [`vacuum_slice_fold_attribution`] 与守卫 `test_vacuum_slice_walks_only_its_budget`）。
    /// `kind == 2`（`EntryValue::Drop`，已被硬删的行）整行不参与，语义与 `iter()` 一致；
    /// `Put(None)` 才是「逻辑删除行」。
    ///
    /// **游标只在组边界上落**：收手的条件是"看到了后继组的第一行"，所以被当游标的那一组
    /// 一定已经收齐；自然走到库尾时最后一组同样收齐。旧窗口实现还要额外防"右界把最后一组
    /// 切成半条链"（那个 `boundary` 与"放大 4 倍重试"的循环都是为它存在的），惰性流从结构上
    /// 不会切在半组上，所以那套补偿一并删掉了。
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
        #[cfg(test)]
        // M41 的分段标尺（与 bitcask 共用 [`crate::mem_probe`] 那一把）：①走查+删除、
        // ②`kv.flush()`（memtable → SST，顺带压实）、③`set_mvcc_xmin_filter` + blob GC。
        // ⚠️ 带门控（`VACUUM_STAGE_SPLIT`），不设时纯读高水位、不压 —— 常开会把外层台架
        // 对 `vacuum()` 的读数锯短（bitcask 那边实测 171.4 → 47.2 MB）。
        let mut vac_base = crate::mem_probe::stage_base();

        // 分组走 M3a 为轻量视图：判「留哪一版」只需要版本号 + 是不是墓碑。
        // 旧实现用 `kv.iter()`，而它会先把**每个源整个 collect() 进内存（连 value）**
        // ⇒ 一次 vacuum 的峰值内存约等于整个库。
        let budget_rows = budget_rows.max(1);

        // ⚠️ 这里必须用**惰性**归并（`light_iter_range`），不能用旧的
        // `scan_latest_light_window`：那个是「每个源各取 `budget` 行再归并」，所以一片取到的
        // 行数是 `源数 × 预算`（2026-10-18 台架实测：24 个 SST 时一片交出 2048 行却取了
        // 22000 行；48 个文件时干脆等于整库），一片的每行单价因此**随 SST 文件数涨 = 随库涨**，
        // 600k 档那一片 14.7 µs/行就是这么来的（同档全量 scan 才 4.9 µs/行）。
        // 惰性那条每源只先取一行定堆顶，后面的行只有真被合并用到才取 ⇒ 代价 = 走到的行数。
        //
        // 游标语义跟着变简单了：自然走到库尾 ⇒ `next_cursor = None`（这一轮就是最后一片，
        // 最后一组也是收齐的，不用退回）；被预算截断 ⇒ 破圈时已经「看到了下一组的第一行」，
        // 所以 `cur_raw` 那一组收齐了，可以直接当游标。旧那份"窗口太窄就放大 4 倍重试"
        // 整个不需要了 —— 它存在的理由正是窗口右界可能把唯一的完整组切断。
        // M36：**只攒当前这一把键的那一组**（以前是全库归组的 `BTreeMap`，每行两个 `Vec`
        // ⇒ vacuum 自己的瞬时内存 = O(全库物理行)）。归并流的键序是 `raw ‖ version_be` 升序
        // ⇒ 同一把键的行必然连续，判完一组就地交出去。
        // M37：再把**块**加上 —— 每块至多 `VAC_CHUNK_ROWS` 行，出借用（`LightMerge` 按 `&self`
        // 借住 kv，删除要 `&mut kv`）之后立刻删这一块、并把保留行的 blob 引用当场并进 `referenced`
        // 然后丢掉 ⇒ `to_delete` 与"整张存活行清单"（原来交给 `gc_blobs(&keep_enc)`）这两笔
        // O(全库) 的账一起塌成 O(一块)。块间下界用 `enc ‖ 0x00`（严格后继方向，⚠️ 不是
        // `‖ 0xFF×8` 那种右界写法），与 bitcask 那条同源、同一个理由。
        let full_sweep = budget_rows == u64::MAX;
        let mut group: Vec<(u64, Vec<u8>, bool, ())> = Vec::new();
        let mut to_delete: Vec<Vec<u8>> = Vec::new();
        let mut keep_enc: Vec<Vec<u8>> = Vec::new();
        // 只数"真有大 value 的行"，一条一个 u64 ⇒ 没有大 value 时这一份一分钱不花
        let mut referenced: HashSet<u64> = HashSet::new();
        let mut removed = 0usize;
        let mut scanned = 0u64;
        let mut cur_raw: Option<Vec<u8>> = None;
        let mut budget_cut = false;
        let mut lower: Option<Vec<u8>> = after.map(|k| k.to_vec());
        loop {
            let mut chunk_rows = 0usize;
            let mut next_lower: Option<Vec<u8>> = None;
            let mut done = false;
            {
                let mut it = kv.light_iter_range(lower.as_deref(), None, u64::MAX);
                while let Some((enc, le)) = it.next() {
                    // 尺子：这片真的折过多少把编码键（与 M27 那四家守卫同一把）
                    #[cfg(test)]
                    crate::lsm_tree::sst::note_light_visit();
                    let Some(vk) = decode_key(&enc) else {
                        continue;
                    };
                    if after.is_some_and(|a| vk.raw_key.as_slice() <= a) {
                        continue;
                    }
                    let at_group_start = cur_raw.as_deref() != Some(vk.raw_key.as_slice());
                    if at_group_start && scanned >= budget_rows {
                        budget_cut = true;
                        done = true;
                        break;
                    }
                    if at_group_start {
                        // 「留哪一版」是四引擎共用的判定，内核在 [`crate::common::vacuum_plan_group`]
                        let (d, kept) = vacuum_plan_group(&mut group, xmin);
                        to_delete.extend(d);
                        if full_sweep {
                            keep_enc.extend(kept.into_iter().map(|(enc, _)| enc));
                        }
                        cur_raw = Some(vk.raw_key.clone());
                    }
                    scanned += 1;
                    // Drop 行也参与「换组」的判定（它照样推进键序），只是不作为裁剪候选
                    if le.kind == 2 {
                        continue;
                    }
                    group.push((vk.version, enc.clone(), le.kind == 1, ()));
                    chunk_rows += 1;
                    if chunk_rows >= VAC_CHUNK_ROWS {
                        let mut bound = enc.clone();
                        bound.push(0u8);
                        next_lower = Some(bound);
                        break;
                    }
                }
            }
            if done || next_lower.is_none() {
                // 最后一块：挂在 `group` 里的那一组判掉（它不会再有后继了）
                let (d, kept) = vacuum_plan_group(&mut group, xmin);
                to_delete.extend(d);
                if full_sweep {
                    keep_enc.extend(kept.into_iter().map(|(enc, _)| enc));
                }
            }
            #[cfg(test)]
            note_vac_pending(to_delete.len() + keep_enc.len());
            for enc in to_delete.drain(..) {
                let _ = kv.remove(&enc);
                removed += 1;
            }
            if full_sweep {
                kv.collect_blob_refs(&keep_enc, &mut referenced);
                keep_enc.clear();
            }
            match next_lower {
                Some(l) if !done => lower = Some(l),
                _ => break,
            }
        }
        let next_cursor = if budget_cut { cur_raw } else { None };
        #[cfg(test)]
        {
            vac_base = crate::mem_probe::stage_mark(0, vac_base);
        }

        // 有界轮次只把删除留在 memtable 里，交给正常的 memtable flush 阈值收口：
        // 每片都 flush 会在 L0 造出一堆只装墓碑的小 SST，压实代价比省下的那点持久性贵。
        // 崩在中间也不会读到旧值 —— 复活的行都是 version < xmin 的，新事务快照看不见。
        let mut blob_rewritten = None;
        if budget_rows == u64::MAX {
            kv.flush()?;
            #[cfg(test)]
            {
                vac_base = crate::mem_probe::stage_mark(1, vac_base);
            }
            // 后续 compact 丢弃 ver < xmin 的 tombstone 版本键。
            // 只在全库轮次下发：判据用 budget 而不是游标，一把 key 的小库任何一片都读到库尾，
            // 用游标判断会让自动回收每片都改动 compact 的过滤条件。
            kv.set_mvcc_xmin_filter(xmin);

            // blob 旁路的空间回收：必须在删完旧行之后、且只有全库扫完才敢做 ——
            // 「还有没有人在引用这个 blob」是个全库问题。引用集合是上面逐块并进来的，
            // 这里只剩"按集合删孤儿文件"那一半（`gc_blobs` 那个收整张清单的入口仍然在，
            // 它就是这两半的壳）。
            blob_rewritten = Some(kv.gc_blobs_referenced(&referenced)? > 0);
            #[cfg(test)]
            {
                let _ = crate::mem_probe::stage_mark(2, vac_base);
            }
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
        // M52：与 bitcask 同一个洞 —— `append(Checkpoint) → sync → truncate()` 把自己刚写的
        // 记录抹掉，下一次 open 只能退回全库扫描拿起点。收口只有一份：`seal_wal_checkpoint`。
        let _ = seal_wal_checkpoint(&mut wal, next_ver);
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
        active_txn: Arc<Mutex<Registry>>,
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
        // M44：这条改成 [`Self::export_latest_visible_for_each`] 的**收集壳** ——
        // 墓碑/过期/读不到那三份判据从此全库只剩一份实现（在
        // [`crate::common::export_rows_streaming`]），"导出"这件事不再有两条会各自漂移的路径。
        // ⚠️ 返回整库的 `Vec` 就是这条的合同：600k 档实测一次 `export 全库(物化)` 峰值
        // **+123.7 MB**。只要"把行交出去"的人（导出到文件、搬几家、数个数）都该走流式那条。
        let mut out: Vec<ExportRecord> = Vec::new();
        self.export_latest_visible_for_each(include_deleted, |rec| {
            out.push(rec);
            true
        });
        out
    }

    /// 流式导出（M44）：折一行、判一行、立刻交给 `on`，**一行都不攒**；`on` 返回 `false`
    /// 就当场收手，后面的键一个都不再折。返回实际交出去的行数。
    ///
    /// 存在的理由：折叠侧从 M27 起就是流式的（[`crate::common::fold_rows_streaming`]），
    /// 可出口这一头只有 `export_latest_visible` —— 它返回 `Vec<ExportRecord>`，于是
    /// "导一个 600k 键的库到文件"要在内存里先把 600k 行摆好，等着一行一行写出去。
    /// 判据（墓碑/过期坏壳/读不到）住在共用件里，这一家只供"逻辑字节 → 用户字节"那一步。
    pub fn export_latest_visible_for_each(
        &self,
        include_deleted: bool,
        mut on: impl FnMut(ExportRecord) -> bool,
    ) -> usize {
        crate::common::export_rows_streaming(
            self,
            include_deleted,
            crate::lsm_tree::kv_ops::logical_to_user,
            &mut on,
        )
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
    use std::assert;
    use std::assert_eq;
    use std::assert_ne;
    use std::panic;
    use std::println;
    use std::vec;
    use std::matches;
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
    /// M52：`BulkLoader` 收尾必须给 WAL 留下**一条还能用的 Checkpoint**。
    ///
    /// 洞（M48 那 89 ms 的真因）：`finish_inner` 原来是 `append(Checkpoint) -> sync -> truncate()`，
    /// 而 `truncate()` 只留 header —— **刚写的那条记录被自己抹掉**。下一次 open 因此拿不到起点，
    /// 只能退回 O(全库 KeyDir) 的 `scan_max_version`（bitcask 600k 档实测多花 89 ms）。
    /// 正确的收口顺序在 `seal_wal_checkpoint` 里本来就有一份（先截断再写回）——
    /// 这次把抄写的几处全部接回那一份：bitcask / bplus / lsm 的 bulk 收尾，加 bplus 的 `Drop`。
    ///
    /// 判据不看墙钟：收口之后**直接读 WAL** —— 应当恰好只剩一条 `Checkpoint`；
    /// 再重开一趟，数据必须一行不少（起点拿小了真正危险的是版本号复用，不只是慢）。
    #[test]
    fn test_bulk_finish_leaves_a_usable_wal_checkpoint() {
        use crate::lsm_tree::storage::wal_path;
        use crate::lsm_tree::wal::{Wal, WalRecord};
        let path = tmp_db("bulk_wal");
        {
            let mvcc = MVCC::open(&path);
            let tx = mvcc.begin_transaction();
            for i in 0..200u64 {
                assert!(tx.set(format!("k{i:06}").as_bytes(), b"v".to_vec()), "灌一把键失败");
            }
            tx.commit();
            let mut bulk = mvcc.begin_bulk();
            for i in 200..260u64 {
                bulk.put(format!("k{i:06}").as_bytes(), b"w".to_vec());
            }
            bulk.finish();
            let mut wal = Wal::open(wal_path(&path)).unwrap();
            let entries = wal.read_all().unwrap();
            let cps: Vec<u64> = entries
                .iter()
                .filter_map(|e| match &e.record {
                    WalRecord::Checkpoint { next_version, .. } => Some(*next_version),
                    _ => None,
                })
                .collect();
            eprintln!("[bulk 收口] lsm WAL 剩 {} 条，Checkpoint = {:?}", entries.len(), cps);
            assert_eq!(
                cps.len(),
                1,
                "bulk 收尾没留下可用的 Checkpoint => 又退回 append -> truncate（自己抹掉自己）"
            );
            assert_eq!(entries.len(), 1, "截断没生效：WAL 里还剩 {} 条", entries.len());
        }
        let mvcc = MVCC::open(&path);
        let tx = mvcc.begin_transaction();
        assert_eq!(tx.key_count(b"k"), 260, "重开之后行数不对 => 收口把数据弄坏了");
        tx.commit();
        drop(mvcc);
        cleanup(&path);
    }

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

    /// 块缓存自动档：容量跟着库长，而不是让人盯着块数过日子（M23）。
    ///
    /// 归因出自下面那条 `LSM_POINT_ROWS` 台架：200k 键那一档乱序点读 220.9 µs/次，块/次 6.24、
    /// **未命中/次 3.42**；同样 6.24 块/次 的**有序**读只要 21.7 µs（未命中 0.11）
    /// ⇒ 随规模涨的那一段是"未命中的随机读"（一次 ≈ 60 µs），不是条目比较，也不是文件数本身。
    /// 而容量固定 256 槽在 ~1.6 万行之后就不再装得下工作集 —— 于是点读单价随库长，且没人能
    /// 提前算出该设多少。
    ///
    /// 钉四件事：① 库比地板小 ⇒ 容量停在地板（不凭空开槽）② 库长在中间 ⇒ 容量 = 实际块数
    /// ③ 超过天花板 ⇒ 封顶（内存是有价的，不是想花多少花多少）④ 装得下 ⇒ 第二趟乱序重读
    /// **0 未命中**；把自动档关掉、钉回地板，同一趟读**必须**重新出现未命中 —— 最后这半句是
    /// 尺子自检，没有它这条测试可以永远绿。
    #[test]
    fn test_block_cache_capacity_follows_the_library() {
        const BLOCK_ENTRIES: usize = 8; // 故意把块做小：让"块数"在测试里廉价地涨起来
        const FLOOR: usize = 4;
        const CEIL: usize = 16;

        fn opts(ceil: usize) -> LSMTreeOptions {
            LSMTreeOptions {
                sst_block_entries: BLOCK_ENTRIES,
                block_cache_blocks: FLOOR,
                block_cache_auto_max: ceil,
                ..LSMTreeOptions::for_mvcc()
            }
        }

        let path = tmp_db("cache_auto_tier");
        let keys: Vec<Vec<u8>> = (0..200).map(|i| format!("k{i:04}").into_bytes()).collect();
        {
            let mvcc = MVCC::try_open_with(&path, Some(opts(CEIL))).unwrap();
            let write = |range: std::ops::Range<usize>| {
                let items: Vec<(Vec<u8>, Vec<u8>)> = range
                    .map(|i| (keys[i].clone(), format!("v{i}").into_bytes()))
                    .collect();
                let tx = mvcc.begin_transaction();
                if let Err(bad) = tx.batch_set(&items) {
                    panic!("batch_set 冲突: {}", String::from_utf8_lossy(&bad));
                }
                tx.commit();
                mvcc.flush();
            };
            let shape = || {
                let kv = mvcc.kv.lock().unwrap();
                (kv.sst_blocks_for_test(), kv.block_cache_capacity())
            };

            // ① 一小撮键 ⇒ 块数远低于地板 ⇒ 容量就该停在地板
            write(0..3);
            let (blocks_small, cap_small) = shape();
            assert!(blocks_small < FLOOR, "形状没造出来：{blocks_small} 块不算小");
            assert_eq!(cap_small, FLOOR, "库比地板小 ⇒ 容量该停在地板");

            // ② 长到地板与天花板之间 ⇒ 容量 = 实际块数
            write(3..100);
            let (blocks, cap) = shape();
            assert!(
                blocks > FLOOR && blocks < CEIL,
                "形状没造出来：块数 {blocks} 不在 ({FLOOR},{CEIL}) 里"
            );
            assert_eq!(cap, blocks, "容量该跟着库长（块数 {blocks}，实测 {cap}）");

            // ④ 全都装得下 ⇒ 第二趟乱序重读不该有未命中
            // ⚠️ 读的顺序只覆盖**此刻已经写进去的** 100 把键（后面 100 把是拿来把块数顶过天花板的）
            let mut order: Vec<usize> = (0..100).collect();
            let mut s = 0x853c_49e6_77b9_5b3eu64;
            for i in (1..order.len()).rev() {
                s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                let j = (s >> 33) as usize % (i + 1);
                order.swap(i, j);
            }
            let tx = mvcc.begin_transaction();
            for &i in &order {
                assert!(tx.get(&keys[i]).is_some(), "读第 {i} 把键落空");
            }
            let m0 = crate::lsm_tree::sst::block_misses();
            for &i in &order {
                assert!(tx.get(&keys[i]).is_some());
            }
            let warm_misses = crate::lsm_tree::sst::block_misses() - m0;
            drop(tx);
            assert_eq!(
                warm_misses, 0,
                "块数 {blocks} ≤ 容量 {cap} 时，第二趟重读不该有未命中（实测 {warm_misses}）"
            );

            // ③ 再长过天花板 ⇒ 封顶，一槽不多占
            write(100..200);
            let (blocks_big, cap_big) = shape();
            assert!(blocks_big > CEIL, "形状没造出来：{blocks_big} 块没超过天花板 {CEIL}");
            assert_eq!(cap_big, CEIL, "容量必须封顶（块数 {blocks_big}，实测 {cap_big}）");

            // ④ 的对照 ⇒ 关掉自动档就回到"配置值即生效值"，同一趟读必须有未命中
            mvcc.set_options(opts(0));
            let (_, cap_pinned) = shape();
            assert_eq!(cap_pinned, FLOOR, "auto_max=0 就该钉死配置值");
            let tx = mvcc.begin_transaction();
            let m0 = crate::lsm_tree::sst::block_misses();
            for &i in order.iter().take(100) {
                assert!(tx.get(&keys[i]).is_some());
            }
            let cold_misses = crate::lsm_tree::sst::block_misses() - m0;
            drop(tx);
            assert!(
                cold_misses > 0,
                "{FLOOR} 槽装 {blocks_big} 块，重读居然一次未命中都没有 ⇒ 这把尺子在空转"
            );
            eprintln!(
                "[块缓存自动档] 地板 {FLOOR} / 天花板 {CEIL}：小块库容量 {cap_small}，\
                 {blocks} 块时容量 {cap}（第二趟未命中 {warm_misses}），{blocks_big} 块时容量 {cap_big}；\
                 关掉自动档钉回 {FLOOR} 槽 ⇒ 同一趟未命中 {cold_misses}"
            );

            // 生效值得看得见：运维调完要知道现在到底是多少
            let t = mvcc.tuning();
            assert_eq!(t.get("block_cache_auto_max"), Some("0"), "{t}");
            assert_eq!(t.get("block_cache_capacity"), Some("4"), "{t}");
        }
        cleanup(&path);
    }

    /// 天花板本身可以抬，而且**分母要看得见**（M32）。
    ///
    /// 为什么单独一条：`for_mvcc` 的默认上限 1024 块 ≈ 7.1 MB 是**预算**，可 600k 键那档的块数
    /// 远不止 1024 ⇒ 覆盖率掉下来，每次点读多付一次随机读（实测 ≈54~94 µs）。默认值不该替调用方
    /// 决定"要不要把库复制进 RAM"，但**要能让他决定**：`set_cache_auto_max(usize::MAX)` = 不设顶。
    ///
    /// 钉四件事：⓪ 封顶时**焐热一趟之后仍然缺块**（26 块 / 容量 8 实测一轮缺 26 —— 冷启第一趟
    /// 谁都要缺，拿它当"顶绑住了"的证据等于什么都没证，与 bplus 那条守卫同一个口径）
    /// ① 抬顶之后容量 = 实际块数（不是"配了没生效"）② `tuning()` 同时给出分子
    /// (`block_cache_capacity`) 与分母 (`sst_blocks`)，否则运维看到 1024 分不清"撞上顶"还是"库就这么小"
    /// ③ 抬顶**不重建缓存** ⇒ 之前焐热的块还在（M23 的教训：自动档自己清自己的缓存）。
    ///
    /// 变异对照（与 bplus 那两条一起跑的）：A「只记值、不重估容量」⇒ 红在 ① 的
    /// `cap_after == blocks`（left 8 / right 26）；B「重建缓存」⇒ 红在 ③ 的"住着几块"
    /// （left 0 / right 8）。两条都**不会**被门面的 `cache_auto_max_knob_…` 抓到，效应归引擎。
    #[test]
    fn test_block_cache_auto_max_is_raisable_and_coverage_is_readable() {
        const BLOCK_ENTRIES: usize = 8;
        const FLOOR: usize = 4;
        // 200 把键、每块 8 条 ⇒ ≈26 块。顶给 8（不是 16）才够"被明显甩开"——
        // 形状没造出来的守卫等于没写。
        const CEIL: usize = 8;
        let path = tmp_db("cache_auto_raise");
        let keys: Vec<Vec<u8>> = (0..200).map(|i| format!("k{i:04}").into_bytes()).collect();
        let opts = LSMTreeOptions {
            sst_block_entries: BLOCK_ENTRIES,
            block_cache_blocks: FLOOR,
            block_cache_auto_max: CEIL,
            ..LSMTreeOptions::for_mvcc()
        };
        {
            let mvcc = MVCC::try_open_with(&path, Some(opts)).unwrap();
            for base in (0..200).step_by(20) {
                let end = (base + 20).min(200);
                let items: Vec<(Vec<u8>, Vec<u8>)> = (base..end)
                    .map(|i| (keys[i].clone(), format!("v{i}").into_bytes()))
                    .collect();
                let tx = mvcc.begin_transaction();
                if let Err(bad) = tx.batch_set(&items) {
                    panic!("batch_set 冲突: {}", String::from_utf8_lossy(&bad));
                }
                tx.commit();
                mvcc.flush();
            }
            let shape = || {
                let kv = mvcc.kv.lock().unwrap();
                (kv.sst_blocks_for_test(), kv.block_cache_capacity())
            };
            let (blocks, cap) = shape();
            assert!(
                blocks > CEIL * 2,
                "形状没造出来：{blocks} 块没把天花板 {CEIL} 甩开"
            );
            assert_eq!(cap, CEIL, "抬顶之前容量必须封顶（块数 {blocks}，实测 {cap}）");

            // 先在封顶的状态下焐热：容量 8 而库有 26 块 ⇒ 缓存里住着东西，但装不下全部
            let tx = mvcc.begin_transaction();
            for k in &keys {
                assert!(tx.get(k).is_some(), "读 {} 落空", String::from_utf8_lossy(k));
            }
            // ⚠️ 焐热一趟**之后**再量缺页：冷启第一趟谁都要缺，拿它当"顶绑住了"的证据等于什么都没证
            let capped0 = crate::lsm_tree::sst::block_misses();
            for k in &keys {
                assert!(tx.get(k).is_some());
            }
            let capped_misses = crate::lsm_tree::sst::block_misses() - capped0;
            drop(tx);
            assert!(
                capped_misses > 0,
                "容量 {cap} / 全库 {blocks} 块，焐热后第二趟一次都不缺 ⇒ 这个顶没绑住，后面全是空转"
            );
            let resident_before = mvcc.kv.lock().unwrap().cached_blocks_for_test();
            assert!(
                resident_before > 0,
                "形状没造出来：焐过一轮之后缓存里一块都没有 ⇒ 下面那条「没重建」的断言会空转"
            );

            mvcc.set_cache_auto_max(usize::MAX);
            let (blocks2, cap_after) = shape();
            assert_eq!(blocks2, blocks, "抬顶不该改动库的形状");
            assert_eq!(
                cap_after, blocks,
                "顶抬到不设限 ⇒ 容量必须等于实际块数（块数 {blocks}，实测 {cap_after}）"
            );
            assert_eq!(mvcc.cache_auto_max(), usize::MAX, "上限要能读回来");
            // M23 的纪律：**改容量是就地改上限，不许重建**（重建 = 调一次旋钮就把焐热的块全丢掉）
            let resident_after = mvcc.kv.lock().unwrap().cached_blocks_for_test();
            assert_eq!(
                resident_after, resident_before,
                "抬顶把缓存从 {resident_before} 块变成 {resident_after} 块 ⇒ 又走回「重建缓存」那条路"
            );

            // 再跑两趟：第一趟把新容量填满，第二趟才该 0 未命中。
            // ⚠️ 别在抬顶后**立刻**要求 0 未命中：封顶那会儿只装得下 8/26 块，缺的那 18 块是
            //   应有账（第一版这条就是写出假红来的地方）。
            let tx = mvcc.begin_transaction();
            for k in &keys {
                assert!(tx.get(k).is_some());
            }
            let m0 = crate::lsm_tree::sst::block_misses();
            for k in &keys {
                assert!(tx.get(k).is_some());
            }
            let misses = crate::lsm_tree::sst::block_misses() - m0;
            drop(tx);
            assert_eq!(
                misses, 0,
                "容量 = 块数 {blocks} 之后第二趟仍缺 {misses} 块 ⇒ 抬上来的容量没真的生效"
            );

            let t = mvcc.tuning();
            assert_eq!(
                t.get("sst_blocks"),
                Some(blocks.to_string().as_str()),
                "分母必须看得见，否则运维算不出覆盖率：{t}"
            );
            assert_eq!(t.get("block_cache_capacity"), Some(blocks.to_string().as_str()), "{t}");
            eprintln!(
                "[抬顶] 块数 {blocks}：封顶 {cap} 时一轮缺 {capped_misses} 块 ⇒ 抬到不设顶后容量 \
                 {cap_after}，重读未命中 {misses}"
            );
        }
        cleanup(&path);
    }

    /// M76 的守卫（LSM 一侧）：块缓存能按**内存预算（字节）**给档，而预算真的当约束。
    ///
    /// 与 bplus 那条同形，形状是 LSM 自己的：`sst_block_entries = 8` 造出 ≈26 块的小库，
    /// 地板 4 / 顶 8 ⇒ 预算必须**既能压到地板以下**（它替换 `clamp(块数, 地板, 顶)` 那道夹，
    /// 不是叠在下面）、**又能长到顶以上**（给够字节就全库驻留），同时
    /// ① 不许把调用方**显式关掉**的缓存（地板 0）悄悄打开；
    /// ② 不许抬他**显式定过**的容量（顶 0 那条路，预算只当天花板）；
    /// ③ 撤预算（`0`）要回到没设过的那个默认封顶；
    /// ④ 封顶要被真的执行（走完一轮，住着块数 ≤ 预算槽数）；
    /// ⑤ 换算一格不差 —— 价签在这里**重述一遍**（10 400 B/槽），库里的常数被改小便红，
    ///    而"改小"正是"按名义块大小乘 ⇒ 内存爆炸"那个事故形状。
    #[test]
    fn test_block_cache_budget_bytes_replaces_the_clamp_and_respects_explicit() {
        const BLOCK_ENTRIES: usize = 8;
        const FLOOR: usize = 4;
        const CEIL: usize = 8;
        const PRICE: u64 = 10_400;
        // 200 把键 × 每块 8 条 ⇒ ≈26 块：地板 4、顶 8 都被它甩开，两个方向都量得动
        const KEYS: usize = 200;
        // 预算取"槽数 × 价签 + 一点余量"，让换算数是整数好核对
        const SMALL: u64 = 20_000; // ⇒ 1 槽（比地板 4 还小）
        const BIG: u64 = 500_000; // ⇒ 48 槽（比这块库的块数还大 ⇒ 全库驻留）

        fn build(
            tag: &str,
            floor: usize,
            ceil: usize,
        ) -> (MVCC, std::path::PathBuf, Vec<Vec<u8>>) {
            let path = tmp_db(tag);
            let keys: Vec<Vec<u8>> = (0..KEYS).map(|i| format!("k{i:04}").into_bytes()).collect();
            let opts = LSMTreeOptions {
                sst_block_entries: BLOCK_ENTRIES,
                block_cache_blocks: floor,
                block_cache_auto_max: ceil,
                ..LSMTreeOptions::for_mvcc()
            };
            let mvcc = MVCC::try_open_with(&path, Some(opts)).unwrap();
            for base in (0..KEYS).step_by(20) {
                let end = (base + 20).min(KEYS);
                let items: Vec<(Vec<u8>, Vec<u8>)> = (base..end)
                    .map(|i| (keys[i].clone(), format!("v{i}").into_bytes()))
                    .collect();
                let tx = mvcc.begin_transaction();
                if let Err(bad) = tx.batch_set(&items) {
                    panic!("batch_set 冲突: {}", String::from_utf8_lossy(&bad));
                }
                tx.commit();
                mvcc.flush();
            }
            (mvcc, path, keys)
        }
        let shape = |mvcc: &MVCC| -> (usize, usize) {
            let kv = mvcc.kv.lock().unwrap();
            (kv.sst_blocks_for_test(), kv.block_cache_capacity())
        };
        let held = |mvcc: &MVCC| -> usize { mvcc.kv.lock().unwrap().cached_blocks_for_test() };

        // ── 自动档那条路（地板 4 / 顶 8）──
        let (mvcc, path, keys) = build("budget_auto", FLOOR, CEIL);
        let (blocks, cap_default) = shape(&mvcc);
        assert!(blocks > CEIL * 2, "形状没造出来：{blocks} 块没把顶 {CEIL} 甩开 ⇒ 后面是空转");
        assert_eq!(cap_default, CEIL, "没设预算就该是自动档封顶 {CEIL}");
        assert_eq!(mvcc.cache_budget_bytes(), 0, "没设过预算要读回 0");

        // ⑤② 小预算：替换那道夹 ⇒ 能压到地板 4 以下
        let cap_small = mvcc.set_cache_budget_bytes(SMALL);
        assert_eq!(cap_small, (SMALL / PRICE) as usize, "小预算换算不是「预算 ÷ 每槽价签」");
        assert!(cap_small < FLOOR, "小预算 {cap_small} 没能压到地板 {FLOOR} 以下 ⇒ 预算没替换 clamp");
        assert_eq!(mvcc.cache_budget_bytes(), SMALL, "给的字节要读得回来");
        assert_eq!(
            mvcc.tuning().get("cache_budget_bytes"),
            Some(SMALL.to_string().as_str()),
            "tuning 里看不见给的预算 = 调用方分不清「设过」与「设了没生效」"
        );

        // ④ 封顶真的被执行：小预算下走完一整轮，住着块数不得超过 1 槽
        {
            let tx = mvcc.begin_transaction();
            for k in &keys {
                assert!(tx.get(k).is_some(), "点读一把键落空");
            }
        }
        let held_small = held(&mvcc);
        assert!(
            held_small <= cap_small,
            "说容量 {cap_small} 块，走完一轮却住着 {held_small} 块 ⇒ 封顶只是名义"
        );

        // ③ 大预算：长到顶 8 以上（覆盖率 = 全库）
        let cap_big = mvcc.set_cache_budget_bytes(BIG);
        assert_eq!(cap_big, blocks.min((BIG / PRICE) as usize), "大预算换算不是 min(块数, 预算槽数)");
        assert!(cap_big > CEIL, "给了 {BIG} B 却仍封顶 {CEIL} ⇒ 被自动档那道顶夹住");

        // ③ 撤预算 ⇒ 回到没设过的那个默认数
        assert_eq!(mvcc.set_cache_budget_bytes(0), CEIL, "撤掉预算要回到自动档默认 {CEIL}");
        assert_eq!(mvcc.cache_budget_bytes(), 0, "撤掉之后要读回 0");
        drop(mvcc);
        let _ = std::fs::remove_dir_all(&path);

        // ① 显式关掉缓存（地板 0）：预算**不许把它打开**
        let (mvcc, path, _k) = build("budget_off", 0, CEIL);
        assert_eq!(shape(&mvcc).1, 0, "地板 0 就该是关缓存");
        assert_eq!(
            mvcc.set_cache_budget_bytes(BIG),
            0,
            "调用方显式关了块缓存，预算把它打开了 ⇒ 「关」这个决定被库替客户改了"
        );
        drop(mvcc);
        let _ = std::fs::remove_dir_all(&path);

        // ② 显式容量（顶 0 ⇒ 自动档不参与）：预算只当天花板
        let (mvcc, path, _k) = build("budget_explicit", 6, 0);
        assert_eq!(shape(&mvcc).1, 6, "顶 0 ⇒ 生效容量应恒等于显式的 6 块");
        assert_eq!(
            mvcc.set_cache_budget_bytes(BIG),
            6,
            "调用方显式要过 6 块，大预算不许把它抬上去"
        );
        assert_eq!(
            mvcc.set_cache_budget_bytes(SMALL),
            cap_small,
            "显式 6 块时小预算照样要压得下去（封顶 {cap_small}）"
        );
        eprintln!(
            "[块缓存预算] 块数 {blocks}｜默认封顶 {CEIL}｜预算 {SMALL} B ⇒ {cap_small} 块（走完一轮住着 \
             {held_small} 块）｜预算 {BIG} B ⇒ {cap_big} 块（顶没夹住）｜撤掉 ⇒ {CEIL}｜关缓存 ⇒ 0\
             ｜显式 6 块：大预算 6 / 小预算 {cap_small}"
        );
        drop(mvcc);
        // 台架自己造的目录要自己收掉 —— 这颗基座的临时现场没有 Drop 守卫，漏一次就积一次
        let _ = std::fs::remove_dir_all(&path);
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
    /// 计数挂在重量路径上（`SstEntryIter::next` 与 `mem_iter_at`），所以这条一旦变红
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
    /// 量尺用「带 value 的条目物化次数」（挂在 `SstEntryIter::next` / `mem_iter_at` 上）。
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
            // 老日志的形状：M25 起写侧不再落 `Begin`，这里手写一份**带 Begin** 的日志，
            // 恢复必须照样是对的那份（读侧兼容由 `plan_is_identical_with_or_without_begin` 钉死）
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

    /// 冷重开**不许自己做写路径的活**（M28）。
    ///
    /// 形状：bulk 收尾留下「L0 文件数正好停在 `l0_compact_threshold` 上」而 WAL 已被
    /// `finish()` 收干净（redo 0 条）—— 这正是 600k 基线里那条悬崖的形状。旧的
    /// `try_open_with` 无条件 `tree.flush()`，而 `LSMTree::flush()` 末尾无条件
    /// `maybe_compact()` ⇒ "打开一个库"自己把整层 L0 搬进 L1/L2：600k 那档整段悬崖都在
    /// `flush` 里（`reopen_shape_probe` 的同轮 A/B：无条件 **13.95 s** vs 有条件 **8.4 ms**），
    /// 而 200k 只有 4 个 L0 文件（<8）不越线 ⇒ 两臂同价。**开关是 L0 文件数越没越线，
    /// 跟"恢复"本身没关系。**
    ///
    /// 尺子选**层文件数**而不是墙钟：判据是"open 没动写路径"，墙钟在 debug 里噪声太大。
    #[test]
    fn test_open_does_not_compact_when_nothing_to_redo() {
        let path = tmp_db("open_no_compact");
        let opts = {
            let mut o = LSMTreeOptions::for_mvcc();
            // 阈值压到 2，好让"越线"这件事在小库里也造得出来
            o.l0_compact_threshold = 2;
            o
        };
        {
            let mvcc = MVCC::try_open_with(&path, Some(opts.clone())).unwrap();
            // 两轮 bulk：每轮 `finish()` 落一个 L0 文件并收口 WAL，而 bulk 模式下不 compact
            for part in 0..2u8 {
                let mut bulk = mvcc.begin_bulk();
                let items: Vec<(Vec<u8>, Vec<u8>)> = (0..50)
                    .map(|i| (format!("k{part}{i:04}").into_bytes(), b"v".to_vec()))
                    .collect();
                bulk.put_batch_owned(items);
                bulk.finish();
            }
        }
        // 裸树读一遍"重开之前"的层形状当基线（`LSMTree::open_with_config` 不 compact，
        // 也不写 MANIFEST ⇒ 这一步本身不会把形状改掉）
        let before = {
            let t = LSMTree::open_with_config(&path, opts.clone()).unwrap();
            let s = t.stats();
            drop(t);
            s
        };
        assert!(
            before.level_files.first().copied().unwrap_or(0) >= 2,
            "形状没造出来：L0 只有 {:?} 个文件，够不到阈值 2 ⇒ 下面的断言会空转",
            before.level_files
        );

        {
            let mvcc = MVCC::try_open_with(&path, Some(opts)).unwrap();
            let after = mvcc.kv.lock().unwrap().stats();
            assert_eq!(
                after.level_files, before.level_files,
                "没有东西要重放时，open 不该动层 —— 压实是写路径的活，不该由\"打开库\"触发"
            );
            // 不 compact ≠ 丢数据：两轮 bulk 的键都必须读得回来
            let tx = mvcc.begin_transaction();
            for part in 0..2u8 {
                for i in 0..50 {
                    let k = format!("k{part}{i:04}").into_bytes();
                    assert_eq!(
                        tx.get(&k),
                        Some(b"v".to_vec()),
                        "第 {part} 轮第 {i} 把键读不回来"
                    );
                }
            }
            tx.commit();
        }
        cleanup(&path);
    }

    /// 反面对照（M28）：**有 redo 时 open 必须先把它落盘，再截断 WAL**。
    ///
    /// 上面那条把 `tree.flush()` 改成有条件，就必须有一条钉住"条件为真时仍然要 flush"——
    /// 否则把 flush 整个删掉也能过第一条，而那样 `seal_wal_checkpoint` 会先把重放来源
    /// （WAL）截掉，重放进 memtable 的行只活在内存里，进程一没电就真的丢了。
    /// 所以这条量的是**崩溃之后的第二次重开还读不读得回来**。
    #[test]
    fn test_open_persists_redo_before_truncating_wal() {
        let path = tmp_db("open_redo_persist");
        {
            // 空库：M28 起此时没有 redo，open 不落 SST（上一条已钉住这件事）
            drop(MVCC::open(&path));
        }
        {
            // 模拟崩溃：已提交的 Write 只在 WAL 里，SST 里没有它
            let mut wal = Wal::open(wal_path(&path)).unwrap();
            let xid = 7u64;
            wal.append(WalRecord::Begin { xid }).unwrap();
            wal.append(WalRecord::Write {
                xid,
                key: encode_key(b"crash-key", xid),
                value: Some(b"payload".to_vec()),
            })
            .unwrap();
            wal.append(WalRecord::Commit { xid }).unwrap();
            wal.sync().unwrap();
        }
        {
            let mvcc = MVCC::open(&path);
            let st = mvcc.kv.lock().unwrap().stats();
            assert_eq!(
                st.mem_entries, 0,
                "重放进 memtable 的 redo 必须在截断 WAL 之前 flush 掉（还留在 memtable = 只活在内存里）"
            );
            let tx = mvcc.begin_transaction();
            assert_eq!(tx.get(b"crash-key"), Some(b"payload".to_vec()));
            tx.commit();
        }
        // 第二次重开：WAL 已被第一次重开截断 ⇒ 这次读到的只能来自 SST
        {
            let mvcc = MVCC::open(&path);
            let tx = mvcc.begin_transaction();
            assert_eq!(
                tx.get(b"crash-key"),
                Some(b"payload".to_vec()),
                "WAL 截断后还读得回来，才说明 redo 真的落了盘"
            );
            tx.commit();
        }
        cleanup(&path);
    }

    /// 一片有界回收的**读放大**归因（M29）。
    ///
    /// 触发它的测量：2026-10-18 三档基线里 `lsm vacuum_bounded 一片`（预算 2048）=
    /// 2 716.3 / 10 277.0 / 30 090.7 µs（600k 复跑 15 676.8）⇒ 每行单价随库涨 **5.8~11×**，
    /// 而 README 那句"平 —— 有界回收这条是干净的（长跑该用它的依据）"就是这么被推翻的。
    /// 那一片只交 2046 行、裁 0 行 ⇒ 14.7 µs/行，是同档 `一次全量 scan`（4.9 µs/行）的 3 倍。
    ///
    /// 尺子是 `light_visits`（这片真的折过多少把编码键）—— **计数与构建档位无关，debug 就能跑**，
    /// 所以这条不占 release 台架的时间。旧写法（每源各取满 `预算` 行再归并）在这里报的是
    /// `源数 × 预算`：24 个 SST 时交出 2048 行、折 22000 把键；换成惰性归并后折/交 ≈ 1。
    ///
    /// ```text
    /// LSM_VAC_ROWS=200000 cargo test --lib -- --ignored --nocapture --test-threads=1 vacuum_slice_fold_attribution
    /// ```
    #[test]
    #[ignore = "一片读放大归因，单线程跑：LSM_VAC_ROWS=200000 cargo test --lib -- --ignored --nocapture --test-threads=1 vacuum_slice_fold_attribution"]
    fn vacuum_slice_fold_attribution() {
        use std::time::Instant;

        let rows: usize = std::env::var("LSM_VAC_ROWS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(200_000);
        // 源数（= SST 文件数）是自变量：默认把文件数顶到 24，压实整个关掉免得被合回去。
        // ⚠️ 为什么用"多次 flush"而不是"调小 target_sst_bytes"：那个旋钮只在**压实输出**时
        // 切文件，`flush_mem` 一次只写一个文件 —— 第一版就是照旋钮调的，量出来永远 1 个文件。
        let files: usize = std::env::var("LSM_VAC_FILES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(24);
        fn key(i: usize) -> Vec<u8> {
            format!("{i:08}#/worldquant/datafields/x.json").into_bytes()
        }
        fn val(i: usize) -> Vec<u8> {
            format!("{{\"i\":{i},\"h\":\"{:0<24}\",\"v\":1}}", i).into_bytes()
        }
        const BUDGET: u64 = 2048;

        let path = tmp_db("vac_slice");
        {
            let mut opts = LSMTreeOptions::for_mvcc();
            opts.l0_compact_threshold = usize::MAX / 4;
            opts.max_compactions_per_flush = 0;
            // 写路径自己有一档 L0 背压：`maybe_stall_on_l0` 在 L0 ≥ `l0_slowdown_trigger`
            // (MVCC 档 = 12) 时**直接** `compact_level(0)`，绕开上面两个旋钮 —— 第一版台架
            // 就是在第 13 个文件处被它把 12 个文件合成了 2 个，"源数"这个自变量当场失效。
            opts.l0_slowdown_trigger = usize::MAX / 8;
            opts.l0_stop_trigger = usize::MAX / 8;
            let mvcc = MVCC::try_open_with(&path, Some(opts)).unwrap();
            // 每个文件写 `per` 行，写完立刻 flush ⇒ 一个文件（`flush_mem` 一次只写一个 SST）。
            // 这样"源数"就是这条台架唯一在动的自变量。
            let per = (rows + files - 1) / files;
            for f in 0..files {
                let lo = f * per;
                let hi = (lo + per).min(rows);
                if lo >= hi {
                    break;
                }
                for base in (lo..hi).step_by(1000) {
                    let end = (base + 1000).min(hi);
                    let items: Vec<(Vec<u8>, Vec<u8>)> =
                        (base..end).map(|i| (key(i), val(i))).collect();
                    let tx = mvcc.begin_transaction();
                    if let Err(bad) = tx.batch_set(&items) {
                        panic!("灌第 {base} 批冲突: {}", String::from_utf8_lossy(&bad));
                    }
                    tx.commit();
                }
                mvcc.flush();
                // ⚠️ 门面那个 `MVCC::flush` 是 `let _ = ...`（吞错的），台架必须直接问树要结果，
                // 否则"文件数没长上去"这种形状失败会静默变成一条空转的读数。
                let s = mvcc.kv.lock().unwrap().stats();
                assert_eq!(s.total_sst, f + 1, "第 {} 次 flush 后该有 {} 个文件", f, f + 1);
            }
            // 更新 10% 的键：制造版本链，vacuum 才有得裁（形状照门面台架）
            let updates = (rows / 10).max(1);
            for base in (0..updates).step_by(500) {
                let end = (base + 500).min(updates);
                let items: Vec<(Vec<u8>, Vec<u8>)> =
                    (base..end).map(|i| (key(i), val(i + rows))).collect();
                let tx = mvcc.begin_transaction();
                if let Err(bad) = tx.batch_set(&items) {
                    panic!("更新第 {base} 批冲突: {}", String::from_utf8_lossy(&bad));
                }
                tx.commit();
            }
            let st = mvcc.kv.lock().unwrap().stats();
            eprintln!(
                "=== 一片读放大 rows={rows} 预算={BUDGET} | sst {} 个 / {} MB | mem_entries {} ===",
                st.total_sst,
                st.level_bytes.iter().sum::<u64>() / 1_048_576,
                st.mem_entries
            );

            let mut last_cursor: Option<Vec<u8>> = None;
            for which in ["第1片", "第2片"] {
                let base = crate::lsm_tree::sst::light_visits();
                let t = Instant::now();
                let r = mvcc.vacuum_bounded(last_cursor.as_deref(), BUDGET).unwrap();
                let d = t.elapsed();
                let walked = crate::lsm_tree::sst::light_visits() - base;
                let scanned = r.stats.rows_scanned;
                eprintln!(
                    "  {which}: 交出 {scanned} 行 / 裁 {} 行 | 折过 {walked} 把编码键 | 折/交 = {:.1}× \
                     | 源数 {} | {:.1} µs | 每行 {:.2} µs",
                    r.stats.versions_removed,
                    if scanned == 0 {
                        0.0
                    } else {
                        walked as f64 / scanned as f64
                    },
                    st.total_sst + 1,
                    d.as_secs_f64() * 1e6,
                    if scanned == 0 {
                        0.0
                    } else {
                        d.as_secs_f64() * 1e6 / scanned as f64
                    },
                );
                last_cursor = r.next_cursor;
            }
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
                // 同上：带 `Begin` 的老日志形状
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
    /// LSM 的切片比另外三家麻烦：一扇窗口要横跨 memtable 和若干 SST，而版本键
    /// `raw ‖ 8B` 下**同一把逻辑 key 的各版本会散在不同源里**，所以"留哪一版"必须在
    /// 整组收齐之后才敢判。这里中途 `flush()` 一次，让版本行同时分布在 memtable 和 SST 里，
    /// 逼这条跨源归并的路径真的被走到。
    /// 键里放了前缀嵌套的 `k1` / `k1x`：两把 key 的区间会互相穿插，这是游标最容易切在
    /// 组中间的情形。
    /// `vacuum()` 必须**按块**删除，不许把整库的待删行与存活行攒在两个 `Vec` 里（M37）。
    ///
    /// 判据（单位：行）：库里可删的旧版**远多于一块**时，单次 flush 前最多攒的待处理行数
    /// `vac_pending_max()` 必须 ≤ `2 × VAC_CHUNK_ROWS`；与 bitcask 那条同形同值，各家钉各家。
    /// `versions_removed > VAC_CHUNK_ROWS` 那条断言保证块界**真的被跨过**（小库永远装得下 ⇒ 空转）。
    /// 为什么这条对 lsm 尤其要紧：它除了 `to_delete` 还要把"存活行清单"交给 `gc_blobs`
    /// （600k 档 `vacuum()` 瞬时 **101.2 MB** 的四家最大的一笔）—— 现在引用集合逐块并入，
    /// 清单不再存在。
    ///
    /// 变异验证：把块界 `VAC_CHUNK_ROWS` 临时抬到一整库那么大 ⇒ 待攒行数变成全部可删行数
    /// （20 000 > 界 16 384），红。
    #[test]
    fn test_vacuum_deletes_in_bounded_chunks() {
        const KEYS: usize = 20_000;
        let path = tmp_db("vac_chunks");
        {
            let mvcc = MVCC::open(&path);
            // 自动回收会在提交那一刻先裁掉一片，那这条判据就量不到手动 vacuum 的界了
            mvcc.set_vacuum_auto_slice(0);
            for _ in 0..2u64 {
                let tx = mvcc.begin_transaction();
                for i in 0..KEYS {
                    let k = format!("k{i:06}");
                    assert!(tx.set(k.as_bytes(), vec![b'v'; 24]), "第 {i} 把键写失败");
                }
                tx.commit();
            }
            reset_vac_pending();
            let st = mvcc.vacuum().unwrap();
            let pending = vac_pending_max();
            eprintln!(
                "[vac chunks lsm] 裁掉 {} 行 / 扫 {} 行 ⇒ 单次 flush 前最多攒 {} 行（块界 {VAC_CHUNK_ROWS}）",
                st.versions_removed, st.rows_scanned, pending
            );
            assert!(st.rows_scanned > 0, "一行都没扫到 ⇒ 空转");
            assert!(
                st.versions_removed > VAC_CHUNK_ROWS,
                "可删行数 {} 没超过一块 ⇒ 这条判据量不到界（空转）",
                st.versions_removed
            );
            assert!(pending > 0, "这把尺子没数到待删行 ⇒ 空转");
            assert!(
                pending <= 2 * VAC_CHUNK_ROWS as u64,
                "单次 flush 前攒了 {pending} 行待处理（界 {}）⇒ 又变回整库一个 `Vec`",
                2 * VAC_CHUNK_ROWS as u64
            );
            // 语义还得对：裁完之后每把键读得回最新那一版
            let tx = mvcc.begin_transaction();
            for i in (0..KEYS).step_by(997) {
                let k = format!("k{i:06}");
                assert_eq!(
                    tx.get(k.as_bytes()),
                    Some(vec![b'v'; 24]),
                    "第 {i} 把键在按块回收后读不回来"
                );
            }
            tx.commit();
        }
        cleanup(&path);
    }

    /// M41 的**先量**：lsm 全库 `vacuum()` 的分段瞬时峰值。
    ///
    /// 为什么还要量：M37b 之后 600k 档 `vacuum()` 还剩 **+45.5 MB**，而它已经不在累加器里
    /// （`to_delete` 与存活行清单都没了）—— 剩下的三段（①走查+删除 / ②`kv.flush()` /
    /// ③xmin 过滤 + blob GC）里哪一段是元凶，M30/M31 两轮的教训是"最合理的那件事不是元凶"，
    /// 所以先分段再动手。⚠️ 标尺带门控：不设 `VACUUM_STAGE_SPLIT` 时它是纯读数，不压高水位。
    ///
    /// `LSM_MEM_ENTRIES` 那一档是用来做**同构建 A/B**的：①段里如果装的是 memtable 攒的墓碑，
    /// 把 memtable 的条数阈值调小，①就得跟着小（阈值不变而①不动 ⇒ 元凶不是 memtable）。
    #[test]
    #[ignore = "lsm vacuum 分段内存台架，手动 release 单线程跑：VACUUM_STAGE_SPLIT=1 LSM_VAC_ROWS=600000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 lsm_vacuum_stage_split"]
    fn lsm_vacuum_stage_split() {
        use std::time::Instant;
        let rows: u64 = std::env::var("LSM_VAC_ROWS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(200_000);
        let mem_entries: usize = std::env::var("LSM_MEM_ENTRIES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        let mb = |b: u64| b as f64 / 1024.0 / 1024.0;
        let path = tmp_db("vac_stages");
        {
            let mem = if mem_entries == 0 {
                LSMTreeOptions::for_mvcc().mem_threshold_entries
            } else {
                mem_entries
            };
            let opts = if mem_entries == 0 {
                None
            } else {
                Some(LSMTreeOptions {
                    mem_threshold_entries: mem_entries,
                    ..LSMTreeOptions::for_mvcc()
                })
            };
            let mvcc = MVCC::try_open_with(&path, opts).unwrap();
            // 自动回收会在提交那一刻先裁掉一片，那这段台架量的就不是手动 vacuum 了
            mvcc.set_vacuum_auto_slice(0);
            for round in 0..2u64 {
                let tx = mvcc.begin_transaction();
                for i in 0..rows {
                    let k = format!("k{i:06}");
                    assert!(
                        tx.set(k.as_bytes(), format!("round{round}").into_bytes()),
                        "第 {i} 把键写失败"
                    );
                }
                tx.commit();
            }
            let t = Instant::now();
            // M42 的先量：builder 单文件最多握住多少条（reset 之后整段 vacuum 里的 flush 都算进来）
            crate::lsm_tree::sst::reset_builder_held_max();
            let st = mvcc.vacuum().unwrap();
            let d = t.elapsed();
            let p = crate::mem_probe::stage_peaks();
            let held = crate::lsm_tree::sst::builder_held_max();
            println!(
                "  （builder 单文件最多同时握住 {held} 条未落盘条目 —— M42 的先量）"
            );
            println!(
                "=== lsm vacuum 分段瞬时峰值 rows={rows} mem阈值={mem} 裁掉 {} 行 / 扫 {} 行，总耗时 {:.2} s ===",
                st.versions_removed,
                st.rows_scanned,
                d.as_secs_f64()
            );
            println!(
                "  ① 走查+删除 {:>8.1} MB ｜ ② flush(memtable→SST，顺带压实) {:>8.1} MB ｜ ③ xmin 过滤+blob GC {:>8.1} MB",
                mb(p[0]),
                mb(p[1]),
                mb(p[2])
            );
            println!(
                "  合计（各段自己的峰值，非同时成立）{:>8.1} MB ｜ 每扫描行摊 ①={:.0} B ②={:.0} B ③={:.0} B",
                mb(p[0] + p[1] + p[2]),
                p[0] as f64 / st.rows_scanned.max(1) as f64,
                p[1] as f64 / st.rows_scanned.max(1) as f64,
                p[2] as f64 / st.rows_scanned.max(1) as f64,
            );
        }
        cleanup(&path);
    }

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

    /// 压实的耗时与瞬时内存到底各在哪一段（M30 先量时间，M33 在同一台架上并上内存列）。
    ///
    /// 为什么要拆：`reopen_shape_probe` 只给出"13.9 s 全在 `flush` 里"，而 `flush` 里混着
    /// **三件事** —— ①把每个输入文件整篇读进 `Vec`（`iter_entries` = `iter_all().collect()`）、
    /// ②k 路归并（每步克隆键与值）、③边归并边写新 SST（`SstBuilder` 侧本来就是流式的）。
    /// "改成流式源"只治 ①，如果时间其实在 ② 或 ③，那这一刀就买错了东西。
    ///
    /// 现在四段（M33 之后）：**① 物化输入** → **② 旧形状**（源=①那份 Vec）归并+写盘 →
    /// **③ 新形状**（源=`iter_all()` 每源一路）归并+写盘 → **④ 纯归并不写盘**（sink 只计数）。
    /// ⇒ 写 ≈ ② − ④；**整段旧形状 = ① + ②**，整段新形状 = ③ —— 报"省了多少内存"必须按整段比，
    /// ⚠️ 第一版就是只比了 ② 与 ③ 两段，得出"流式反而更占内存"的反向结论（旧形的大头记在 ① 上）。
    /// 每段之前调 `mem_probe::reset_peak()`：`peak` 是进程高水位，不压回去的话靠后的段只能报 0。
    ///
    /// # 量出来的答案（release，600k 行 = 8 个输入文件 61.7 MB → 34 个输出文件 67.6 MB）
    ///
    /// | 段 | M30 之前 | M30 之后（=现在） | 每输入行 | 峰值涨 |
    /// |---|---|---|---|---|
    /// | ① 物化输入 | 618 ms | 366.9 ms | 0.61 µs | **154.0 MB**（条目本身 61.7 MB） |
    /// | ② 旧形状 归并+写盘 | **18 018 ms** | 1471.8 ms | 2.45 µs | 2.9 MB（叠在 ① 上） |
    /// | ③ 新形状 流式源 | — | 1888.4 ms | 3.15 µs | **6.3 MB** |
    /// | ④ 纯归并不写盘 | 476 ms | 307.9 ms | 0.51 µs | — |
    ///
    /// ⇒ **时间**的病在 ②/③，占当时 ①+③ 的 94%，而那段里唯一"每行都做一次且随表长"的事是
    /// `merge_to_ssts` 的 `if b.estimated_bytes() >= target_bytes` —— 旧实现每次调用都把 builder
    /// 的整表从头求和一遍（一张 2 MiB 的表 ≈19 000 行 ⇒ 每行摊 ~30 µs，正是实测那个数）。
    /// 修法与 M19 同一个动作：改增量账。连带效果：带 redo 的重开 14.43 s → **3.55 s**。
    /// ⇒ **内存**的病在 ①：整段旧形状 ①+② = **156.8 MB** → 新形状 ③ = **6.3 MB（省 25.0×）**，
    /// 时间 1838.7 → 1888.4 ms（+2.7%，本机噪声带内 ⇒ 持平）。M33 收的就是这笔。
    /// 每行摊 269 B 而条目只有 108 B ⇒ 多出来的是每行两个堆分配 + `Vec` 扩容翻倍。
    ///
    /// ```text
    /// LSM_COMPACT_ROWS=600000 cargo test --release --lib -- --ignored --nocapture \
    ///     --test-threads=1 compaction_cost_split
    /// ```
    #[test]
    #[ignore = "压实耗时拆分，手动 release 单线程跑：LSM_COMPACT_ROWS=600000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 compaction_cost_split"]
    fn compaction_cost_split() {
        use std::time::Instant;
        use crate::lsm_tree::sst::{
            merge_streaming, merge_to_ssts, vec_inputs, MergeInput, SstBuilder, StoredEntry,
        };

        let rows: usize = std::env::var("LSM_COMPACT_ROWS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(200_000);
        let files: usize = std::env::var("LSM_COMPACT_FILES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(8);
        fn key(i: usize) -> Vec<u8> {
            format!("{i:08}#/worldquant/datafields/x.json").into_bytes()
        }
        fn val(i: usize) -> Vec<u8> {
            format!("{{\"i\":{i},\"h\":\"{:0<28}\",\"v\":1}}", i * 7).into_bytes()
        }

        // 压实与 L0 背压都抬到不会触发：这条要在**压实之前**把输入抓在手里反复量
        let mut opts = LSMTreeOptions::for_mvcc();
        opts.l0_compact_threshold = usize::MAX / 4;
        opts.max_compactions_per_flush = 0;
        opts.l0_slowdown_trigger = usize::MAX / 8;
        opts.l0_stop_trigger = usize::MAX / 8;

        let dir = tmp_db("compact_split");
        let out_dir = tmp_db("compact_split_out");
        {
            let mvcc = MVCC::try_open_with(&dir, Some(opts.clone())).unwrap();
            let per = (rows + files - 1) / files;
            for f in 0..files {
                let lo = f * per;
                let hi = (lo + per).min(rows);
                if lo >= hi {
                    break;
                }
                let mut bulk = mvcc.begin_bulk();
                let items: Vec<(Vec<u8>, Vec<u8>)> =
                    (lo..hi).map(|i| (key(i), val(i))).collect();
                bulk.put_batch_owned(items);
                bulk.finish();
            }
            let st = mvcc.kv.lock().unwrap().stats();
            eprintln!(
                "=== 压实拆分 rows={rows} 文件={files} | L0 实际 {} 个 | 盘上 {} MB ===",
                st.l0_files,
                st.level_bytes.iter().sum::<u64>() / 1_048_576
            );
        }

        let tree = LSMTree::open_with_config(&dir, opts.clone()).unwrap();
        let n_files = tree.l0_for_test().len();

        // ── ① 物化输入（旧形状的第一步）：时间 + **这一段自己涨了多少内存** ──
        // ⚠️ `reset_peak()` 是关键：`peak` 是进程高水位，不压回去的话这一段涨的会被前面的
        // 灌入高峰盖住、报成 0（M27 的盲区）。基准取"当前活着的字节"，之后减它。
        let t = Instant::now();
        let m0 = crate::mem_probe::reset_peak();
        let live0 = crate::mem_probe::live();
        let sources: Vec<Vec<_>> = tree.l0_for_test().iter().rev().map(|s| s.iter_entries()).collect();
        let t_materialize = t.elapsed();
        let mat_peak = crate::mem_probe::peak().saturating_sub(m0);
        let mat_live = (crate::mem_probe::live() as i64 - live0 as i64).max(0) as u64;
        let total_rows: usize = sources.iter().map(|s| s.len()).sum();
        let bytes: usize = sources
            .iter()
            .flat_map(|s| s.iter())
            .map(|(k, se)| k.len() + se.estimate_bytes())
            .sum();

        let block_entries = opts.sst_block_entries;
        let bloom = opts.enable_bloom;
        let bits = opts.bloom_bits_per_key;
        let target = opts.target_sst_bytes;
        let mut next_id = 0u64;
        let out_path = out_dir.clone();
        let mut make = move || {
            next_id += 1;
            let p = out_path.join("sst").join(format!("{next_id:08}.sst"));
            SstBuilder::with_options(next_id, p, block_entries, bloom).with_bloom_bits(bits)
        };
        // ── ② 旧形状：源 = ① 那份物化好的 Vec ──
        let t = Instant::now();
        let m1 = crate::mem_probe::reset_peak();
        crate::lsm_tree::sst::reset_builder_held_max();
        let written = merge_to_ssts(
            vec_inputs(sources),
            |_: &[u8], _: &StoredEntry| true,
            &mut make,
            target,
        )
        .unwrap();
        let t_merge_write = t.elapsed();
        let old_peak = crate::mem_probe::peak().saturating_sub(m1);
        let held_old = crate::lsm_tree::sst::builder_held_max();
        let out_bytes: u64 = written.iter().map(|s| s.size_bytes).sum();
        let n_out = written.len();
        drop(written);

        // ── ③ 新形状（生产码现在走这条）：源 = 每源一路 `iter_all()` 迭代器 ──
        // 两臂的输出必须**逐字节同口径**，否则"省了内存"可能只是"少写了东西"。
        let t = Instant::now();
        let m2 = crate::mem_probe::reset_peak();
        crate::lsm_tree::sst::reset_builder_held_max();
        let written_stream = merge_to_ssts(
            tree.l0_for_test()
                .iter()
                .rev()
                .map(|s| Box::new(s.iter_all()) as MergeInput<'_, StoredEntry>)
                .collect(),
            |_: &[u8], _: &StoredEntry| true,
            &mut make,
            target,
        )
        .unwrap();
        let t_stream = t.elapsed();
        let new_peak = crate::mem_probe::peak().saturating_sub(m2);
        let held_new = crate::lsm_tree::sst::builder_held_max();
        let out_bytes_stream: u64 = written_stream.iter().map(|s| s.size_bytes).sum();
        assert_eq!(
            written_stream.len(),
            n_out,
            "流式源改变了切文件的数量（{n_out} → {}）⇒ 判据被动过了",
            written_stream.len()
        );
        assert_eq!(
            out_bytes_stream, out_bytes,
            "两臂输出字节不同（{out_bytes} → {out_bytes_stream}）⇒ 归并结果被改动污染，不是省内存"
        );
        drop(written_stream);

        // ④ 纯归并（sink 只计数，不写盘）：源仍走物化那份，好与 ② 直接对比"写"占多少
        let mut emitted = 0usize;
        let sources3: Vec<Vec<_>> =
            tree.l0_for_test().iter().rev().map(|s| s.iter_entries()).collect();
        let t = Instant::now();
        merge_streaming(
            vec_inputs(sources3),
            &mut |_: &[u8], _: &StoredEntry| true,
            |_: Vec<u8>, _| {
                emitted += 1;
            },
        );
        let t_merge_only = t.elapsed();

        let mb = |b: u64| b as f64 / 1_048_576.0;
        eprintln!(
            "  输入 {total_rows} 行 / {:.1} MB | 输出 {n_out} 个文件 {:.1} MB | 纯归并交出 {emitted} 把键",
            mb(bytes as u64),
            mb(out_bytes),
        );
        eprintln!(
            "  ① 物化输入 {:>10.1} ms ⇒ 每行 {:.3} µs | 这一段峰值涨 {:.1} MB（条目本身 {:.1} MB、\
             结束时仍活 {:.1} MB）⇒ 每行摊 {:.0} B",
            t_materialize.as_secs_f64() * 1e3,
            t_materialize.as_secs_f64() * 1e6 / total_rows as f64,
            mb(mat_peak),
            mb(bytes as u64),
            mb(mat_live),
            mat_peak as f64 / total_rows.max(1) as f64,
        );
        eprintln!(
            "  ② 旧形状 归并+写盘 {:>10.1} ms ⇒ 每输入行 {:.3} µs | 这一段峰值涨 {:.1} MB\
             （源=①那份 Vec ⇒ 输入的 154 MB 已记在 ① 上）| builder 单文件最多握住 {held_old} 条",
            t_merge_write.as_secs_f64() * 1e3,
            t_merge_write.as_secs_f64() * 1e6 / total_rows as f64,
            mb(old_peak),
        );
        eprintln!(
            "  ③ 新形状 流式源     {:>10.1} ms ⇒ 每输入行 {:.3} µs | 峰值涨 {:.1} MB\
             | builder 单文件最多握住 {held_new} 条（输出 {n_out} 个文件、平均 {:.0} 行/文件）",
            t_stream.as_secs_f64() * 1e3,
            t_stream.as_secs_f64() * 1e6 / total_rows as f64,
            mb(new_peak),
            total_rows as f64 / (n_out.max(1) as f64),
        );
        eprintln!(
            "  ⇒ builder 那块缓冲占 ③ 峰值的 {:.0}%（{held_new} 条 × 平均每条 {:.0} B = {:.1} MB，\
             段峰值 {:.1} MB）｜ 一文件切在 target={target} B",
            100.0 * held_new as f64 * (bytes as f64 / total_rows.max(1) as f64)
                / (new_peak.max(1) as f64),
            bytes as f64 / total_rows.max(1) as f64,
            mb(held_new as u64 * (bytes as u64 / total_rows.max(1) as u64)),
            mb(new_peak),
        );
        // ⚠️ 报"省了多少"必须按**整条操作**比，不能拿 ② 与 ③ 两段各截一段比 ——
        // 旧形状的大头（物化那 154 MB）在 ① 段，② 只是叠在它上面多涨的 2.9 MB。
        // 只比 ② vs ③ 会得到"新形状反而更占内存"的错误结论（第一版这句就印错了）。
        let old_total = mat_peak + old_peak;
        eprintln!(
            "  ⇒ 整段瞬时内存：旧形状 ①+② = {:.1} MB → 新形状 ③ = {:.1} MB（省 {:.1}×）\
             | 整段时间：旧 ①+② = {:.1} ms → 新 ③ = {:.1} ms",
            mb(old_total),
            mb(new_peak),
            if new_peak > 0 { old_total as f64 / new_peak as f64 } else { 0.0 },
            (t_materialize.as_secs_f64() + t_merge_write.as_secs_f64()) * 1e3,
            t_stream.as_secs_f64() * 1e3,
        );
        eprintln!(
            "  ④ 纯归并不写盘 {:>10.1} ms ⇒ 每行 {:.3} µs | 写这一段 ≈ {:.1} ms（占 ①+② 的 {:.0}%）",
            t_merge_only.as_secs_f64() * 1e3,
            t_merge_only.as_secs_f64() * 1e6 / total_rows as f64,
            (t_merge_write.as_secs_f64() - t_merge_only.as_secs_f64()) * 1e3,
            (t_merge_write.as_secs_f64() - t_merge_only.as_secs_f64())
                * 100.0
                / (t_materialize.as_secs_f64() + t_merge_write.as_secs_f64()).max(1e-9),
        );
        eprintln!("  文件数={n_files}（压实输入个数）| 输出目标 {target} B/文件");
        drop(tree);
        cleanup(&dir);
        cleanup(&out_dir);
    }

    /// 一片有界回收**只许走它预算那么多行**（M29）。
    ///
    /// 触发它的测量：三档基线里 `lsm vacuum_bounded 一片`（预算 2048）= 2 716.3 / 10 277.0 /
    /// 30 090.7 µs ⇒ **每行单价随库涨 5.8~11×**，而 README 那句"平 —— 有界回收这条是干净的
    /// （长跑该用它的依据）"就是这么被推翻的。归因台架 [`vacuum_slice_fold_attribution`]
    /// 给出机制：旧写法走 `scan_latest_light_window` —— **每个源各取满 `预算` 行再归并**，
    /// 于是取到的行数是 `源数 × 预算`（24 个 SST 时交出 2048 行、折 22000 把键；48 个文件时
    /// 直接等于整库），而 SST 文件数 ∝ 库大小 ⇒ 单价随库涨。
    ///
    /// 尺子 = `light_visits`（这片交到我手里多少把编码键）。两条断言配对才不空转：
    /// **折过的 ≤ 预算 × 2**，而**交出的行数必须接近预算**（不然"只走了三行"也能过）。
    /// 源数是这里的自变量：形状造到 17 个源 ⇒ 老写法该折 17× 预算 ⇒ 必红。
    #[test]
    fn test_vacuum_slice_walks_only_its_budget() {
        const ROWS: usize = 8_000;
        const FILES: usize = 16;
        const BUDGET: u64 = 512;

        let path = tmp_db("vac_budget");
        let mut opts = LSMTreeOptions::for_mvcc();
        // 压实与 L0 背压整个关掉：这条要的是"文件多"这个形状。⚠️ 只关 `l0_compact_threshold`
        // 和 `max_compactions_per_flush` 不够 —— 写路径还有一档 `maybe_stall_on_l0`，
        // L0 到 `l0_slowdown_trigger`(MVCC 档 = 12) 会**绕过那两个旋钮**直接 compact_level(0)。
        opts.l0_compact_threshold = usize::MAX / 4;
        opts.max_compactions_per_flush = 0;
        opts.l0_slowdown_trigger = usize::MAX / 8;
        opts.l0_stop_trigger = usize::MAX / 8;
        {
            let mvcc = MVCC::try_open_with(&path, Some(opts)).unwrap();
            let per = ROWS / FILES;
            for f in 0..FILES {
                let lo = f * per;
                let items: Vec<(Vec<u8>, Vec<u8>)> = (lo..lo + per)
                    .map(|i| (format!("k{i:05}").into_bytes(), format!("v{i}").into_bytes()))
                    .collect();
                let tx = mvcc.begin_transaction();
                if let Err(bad) = tx.batch_set(&items) {
                    panic!("灌第 {f} 片冲突: {}", String::from_utf8_lossy(&bad));
                }
                tx.commit();
                mvcc.flush();
                // 门面那个 `flush` 是 `let _ = ...`（吞错的），形状没造出来必须当场炸
                let s = mvcc.kv.lock().unwrap().stats();
                assert_eq!(s.total_sst, f + 1, "第 {f} 次 flush 没落出一个文件，形状没造出来");
            }
            // 第二版：让每把键都有两条版本行（vacuum 有得裁，也保证折的是"版本行"而不是"键"）
            for base in (0..ROWS).step_by(2000) {
                let end = (base + 2000).min(ROWS);
                let items: Vec<(Vec<u8>, Vec<u8>)> = (base..end)
                    .map(|i| (format!("k{i:05}").into_bytes(), format!("w{i}").into_bytes()))
                    .collect();
                let tx = mvcc.begin_transaction();
                if let Err(bad) = tx.batch_set(&items) {
                    panic!("更新第 {base} 批冲突: {}", String::from_utf8_lossy(&bad));
                }
                tx.commit();
            }

            let before = crate::lsm_tree::sst::light_visits();
            let r = mvcc.vacuum_bounded(None, BUDGET).unwrap();
            let walked = crate::lsm_tree::sst::light_visits() - before;
            eprintln!(
                "一片: 交出 {} 行 / 折过 {walked} 把键 / 裁 {} 行 / 源数 {}",
                r.stats.rows_scanned, r.stats.versions_removed, FILES + 1
            );
            assert!(
                r.stats.rows_scanned >= BUDGET - BUDGET / 8,
                "预算 {BUDGET} 行的一片只交了 {} 行 ⇒ 上面那条比值断言会空转（形状或游标有问题）",
                r.stats.rows_scanned
            );
            assert!(
                walked <= BUDGET * 2,
                "预算 {BUDGET} 行的一片折了 {walked} 把编码键（源数 {}）⇒ 又回到「每源各取满预算」，\
                 一片的代价就跟着 SST 文件数涨",
                FILES + 1
            );
            assert!(r.next_cursor.is_some(), "库里还有 {} 行没走完，游标不该是 None", ROWS * 2);
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
            assert!(size2 > size1, "写入必须留下 Write/Commit 记录，持久化点不能被省掉");

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
                // M25：写侧不再落 `Begin`（恢复不读它）。老日志**还能读**那半边由
                // `plan_is_identical_with_or_without_begin` 钉住，两边缺一条都是洞。
                let begins = records
                    .iter()
                    .filter(|e| matches!(&e.record, WalRecord::Begin { .. }))
                    .count();
                assert_eq!(begins, 0, "写侧不再落 Begin，实际留下 {begins} 条");
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

    /// `health()` 不许把任何一个 SST 的轻量视图**整篇物化**（M39）。
    ///
    /// 判据：尺子 `light_view_materialized()`，**单位 = 条目数**；一次 `health()` 的差值必须是 0。
    /// 为什么钉"路径"而不钉内存：分配计数（`mem_probe`）是**进程级**的，并行用例互相抬高峰值 ⇒
    /// 内存读数只能待在 `--test-threads=1` 的台架里（M27 栽过一次，M36 又拿坏量具的读数当过凭据）。
    /// 而"有没有走 `SstFile::light_view`"是一条一次就能数出来的路径事实。
    /// 内存那笔（M38 修好量具之后）：600k 档 `health()` 瞬时改前 **+126.1 MB**。
    ///
    /// 变异验证：把 `LSMTree::light_iter_range` 里 SST 那几路源临时改成
    /// `Box::new(sst.light_view(snap_seq).into_iter())`（旧形状：每个文件整篇折进 `Vec` 再归并）
    /// ⇒ 差值 = 全库条目数，红。
    #[test]
    fn test_health_does_not_materialize_its_sources() {
        const KEYS: u64 = 300;
        let path = tmp_db("health_lazy");
        {
            let mvcc = MVCC::open(&path);
            for i in 0..KEYS {
                let key = format!("k{i:04}");
                let tx = mvcc.begin_transaction();
                assert!(tx.set(key.as_bytes(), b"v".to_vec()), "第 {i} 把键写失败");
                tx.commit();
            }
            // 必须落在 SST 里：全在 memtable 时这条判据是空转
            mvcc.flush();

            let before = crate::lsm_tree::sst::light_view_materialized();
            let h = mvcc.health();
            let used = crate::lsm_tree::sst::light_view_materialized() - before;
            assert!(
                h.live_keys >= KEYS,
                "库里必须有东西可折（live_keys {}）",
                h.live_keys
            );
            assert_eq!(
                used, 0,
                "health 期间有 {used} 条被 `SstFile::light_view` 整篇物化 ⇒ 又走回\
                 「每个源先整份 Vec 化再归并」那个形状（600k 档 +126.1 MB 的来处）"
            );
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
    // M23 把这条通道量到底，靠的是同一个台架换两个旋钮：`LSM_POINT_ROWS` 换规模、
    // `LSM_CACHE_AUTO=0` 关掉块缓存自动档（= 改造前那个钉死的 256 槽）。同构建、同库目录的读法：
    //   5002 键（314 块）   钉死 256：乱序 27.9 µs/次、未命中 0.39  vs  自动档 314：**8.5**、0.03
    //                      块/次两边都是 4.00、条目比较/次 128 ⇒ 省掉的只有"其中几次真读盘"
    //                      "启动后只读 4 条"这一格最扎眼：57.5 → 11.5 µs/次（下游的进程启动形状）
    //   40000 键（2681 块） 256：117.3、未命中 1.81  vs  自动档 1024：90.6、1.24
    //                      vs 上限放到 4096（全库驻留 18.7 MB）：21.0、0.03
    //                      ⇒ 残差 ≈ 未命中次数 × 一次随机读（这台上 ≈ 54 µs），上限是**预算**不是正确性
    //   ⚠️ 第一版自动档在这里抓到自己的回归：容量 256→1024 后**未命中次数不动而有序读 22.2→36.0**
    //     —— `BlockCache` 每次命中都 `order.iter().position()`，代价 O(驻留块数)。换成定长槽 +
    //     双向链表（O(1)）之后同一档有序读 17.1 ≈ 地板那臂的 17.4。守卫见 `lru_touch_cost_...`。
    //
    // 复现命令（本仓库）：
    //   cargo test --release --lib -- --ignored --nocapture point_read_baseline
    //   LSM_POINT_ROWS=40000 LSM_CACHE_AUTO=0 cargo test --release --lib -- --ignored \
    //     --nocapture --test-threads=1 point_read_baseline   # 关掉自动档做对照
    #[test]
    #[ignore = "墙钟基线，手动 release 跑：cargo test --release --lib -- --ignored --nocapture point_read_baseline"]
    fn test_lsm_point_read_baseline_5000_keys() {
        use std::time::Instant;

        // 键数可由 `LSM_POINT_ROWS` 调（默认 5002 = 下游那份现场）：M23 要判"点读单价随什么长"，
        // 只能同一份构建跑两个规模比每单位成本 —— 单规模只能看见"贵"，看不见"超线性"。
        #[allow(non_snake_case)]
        let N: usize = std::env::var("LSM_POINT_ROWS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5002);
        type Snap = (u64, u64, u64, u64);

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
            // ⚠️ 物理行数与链长都只断"不超过"：自动回收是按提交记账触发的，N 大了它就在写过程中
            // 已经把部分链压平了（实测 40000 键那一档 rows=68505 < 80001、max_chain=2；
            // 200k 那一档 rows=341868 < 400001）。这条台架量的是**读代价**，回收的口径由 M4
            // 那批守卫守着；这里唯一硬合同是 live_keys —— 键不能丢也不能凭空多。
            assert!(
                (h.total_rows as usize) <= N * 2 + 1,
                "物理行数超过形状上限：{} > {}",
                h.total_rows,
                N * 2 + 1
            );
            assert_eq!(h.live_keys as usize, N);
            assert!(h.max_chain <= 3, "链长不该超过写入轮数 3：实测 {}", h.max_chain);
        }

        // 重新打开，模拟"进程启动后读"
        //
        // `LSM_CACHE_AUTO=0` ⇒ 关掉块缓存自动档，回到改造前那个"钉死 256 槽"。M23 的 A/B
        // 只能这样做：同一份构建、同一个库目录，只换这一档 ⇒ "点读单价随规模涨"到底是不是缓存容量
        // 造成的，才有凭据（墙钟在本机 run 间能差 ±2 倍，结论只能落在计数上）。
        let auto_max = std::env::var("LSM_CACHE_AUTO")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or_else(|| LSMTreeOptions::for_mvcc().block_cache_auto_max);
        let mvcc = MVCC::try_open_with(
            &path,
            Some(LSMTreeOptions {
                block_cache_auto_max: auto_max,
                ..LSMTreeOptions::for_mvcc()
            }),
        )
        .map_err(|e| format!("重开失败: {e}"))
        .unwrap();
        let (stats, cap, blocks) = {
            let kv = mvcc.kv.lock().unwrap();
            (kv.stats(), kv.block_cache_capacity(), kv.sst_blocks_for_test())
        };
        let sst_bytes: u64 = stats.level_bytes.iter().sum();
        // 缓存的价位用"实测每块平均多少字节"来推，不靠估：块是按条数切的，条子大小才是分母
        let per_block = if blocks == 0 { 0.0 } else { sst_bytes as f64 / blocks as f64 };
        eprintln!(
            "布局: levels={:?} total_sst={} l0={} mem_entries={} 块={blocks} 容量={cap} \
             自动档={auto_max} SST={:.1} MB 每块均值={per_block:.0} B ⇒ 满缓存价位≈{:.1} MB",
            stats.level_files,
            stats.total_sst,
            stats.l0_files,
            stats.mem_entries,
            sst_bytes as f64 / 1048576.0,
            cap as f64 * per_block / 1048576.0,
        );
        // A/B 的有效性凭据：这一档必须真的落到容量上了，否则两个臂量的是同一件事
        let floor = LSMTreeOptions::for_mvcc().block_cache_blocks;
        let expect = if auto_max == 0 { floor } else { blocks.clamp(floor, auto_max) };
        assert_eq!(
            cap, expect,
            "自动档={auto_max} 下容量该是 {expect}（块 {blocks}，地板 {floor}），实测 {cap}"
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

        let report = |name: &str, dur: std::time::Duration, n: usize, a: Snap, b: Snap| {
            let us = dur.as_micros() as f64;
            let per = |x: u64| x as f64 / n.max(1) as f64;
            eprintln!(
                "{name:<22} 合计 {:>9.1} ms | 单次 {:>8.1} µs | 块/次 {:>5.2} | 未命中/次 {:>5.2} \
                 | 探文件/次 {:>5.2} | 条目比较/次 {:>9.1}",
                us / 1000.0,
                if n == 0 { 0.0 } else { us / n as f64 },
                per(b.0 - a.0),
                per(b.1 - a.1),
                per(b.2 - a.2),
                per(b.3 - a.3),
            );
        };
        let snap = || {
            (
                crate::lsm_tree::sst::block_reads(),
                crate::lsm_tree::sst::block_misses(),
                crate::lsm_tree::sst::sst_probes(),
                crate::lsm_tree::sst::sst_entry_walks(),
            )
        };

        // (a) 同一个事务里连读 5002 次 —— 乱序
        let a = snap();
        let t = Instant::now();
        for &i in &shuffled {
            assert!(tx.get(&keys[i]).is_some(), "乱序读第 {i} 个键落空");
        }
        report("点读×N 乱序", t.elapsed(), N, a, snap());

        // (b) 同一个事务里连读 5002 次 —— 有序（键升序 = 块升序）
        let a = snap();
        let t = Instant::now();
        for k in &keys {
            assert!(tx.get(k).is_some(), "有序读落空");
        }
        report("点读×N 有序", t.elapsed(), N, a, snap());

        // (c) multi_get：门面把它实现成 `map(get)`，所以理论上和 (a) 同价 —— 量出来才算数
        let a = snap();
        let t = Instant::now();
        let got = tx.multi_get(&keys);
        assert_eq!(got.len(), N);
        report("multi_get×N", t.elapsed(), N, a, snap());

        // (d) 一次全量 scan —— "干脆整段读回来"那条路
        let a = snap();
        let t = Instant::now();
        let rows = tx.scan(None, None);
        eprintln!("全量 scan 命中 {} 行", rows.len());
        report("一次全量 scan", t.elapsed(), rows.len().max(1), a, snap());

        // (e) 只要 4 条（下游"进程启动 + 打开 + 扫 4 条"那一段）
        let a = snap();
        let t = Instant::now();
        for k in &keys[..4] {
            assert!(tx.get(k).is_some());
        }
        report("点读×4", t.elapsed(), 4, a, snap());

        drop(tx);
        drop(mvcc);
        cleanup(&path);

        // (f) 单次点查的代价 = **两趟**（定位 + 定论）× **含这个键的 SST 数** × 未命中。
        //     所以真正该盯的量是"同一把键的版本散在几个文件里"：库里每多一层/每多一个覆盖
        //     该键的 SST，点查就多探一次块。compact 的意义就是把这件事压回层数。
        //     造法：每轮全量写一遍再 flush ⇒ 每个键在每个文件里都有一份。
        eprintln!("--- 覆盖同一把键的 SST 数 → 单次点查代价 ---");
        if N > 50_000 {
            // 这一段要按 1/2/4/7 轮**全量重写**来造分散度，N=200k 时是 320 万行写 + 压实，
            // 一台架跑不动；分散度那一档已经在 5002 键上量过（M10 的结论就是从它来的）。
            eprintln!("    （跳过：N={N} > 50000，这段要全量重写 1+2+4+7 轮）");
        }
        for files in [1usize, 2, 4, 7].into_iter().take(if N > 50_000 { 0 } else { 4 }) {
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
                let a = snap();
                let t = Instant::now();
                for &i in &shuffled {
                    assert!(tx.get(&keys[i]).is_some());
                }
                let d = t.elapsed();
                report(&format!("乱序点读 (每键 {files} 份)"), d, N, a, snap());
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

    /// 重开剖面（M28 归因）：**bulk 之后那次冷重开**为什么在 600k 涨到 17.5 s。
    ///
    /// 触发它的测量（2026-10-18 三档基线，同一构建 `f20b7fd`，release，数据在 C: 的 SSD）：
    /// `lsm_tree` 的第一次 `冷重开(恢复)` = 13 982.3 µs，而**第二次**（`再冷重开`）
    /// 20k = 10 237.3 / 200k = 10 296.5 / **600k = 17 487 030.8 µs** ⇒ 原始 1708 倍，
    /// 除掉"整库一趟"该涨的 30 倍还剩 57 倍。重开后常驻 1.6 → **12.1 MB**。
    /// 两次重开之间变了只有那串写（更新 10% 键、vacuum、flush）和 `bulk 灌 rows/10`。
    ///
    /// 三条候选，本条逐条证伪或坐实（**只量，不改任何语义**）：
    /// 1. `Wal::read_all` + `analyze_recovery` 全量解析 —— 帧头/帧体各一次定位读，而
    ///    [`crate::common::DiskFile::read_exact_at`] 是 seek+read ⇒ **每条记录 4 次系统调用**
    /// 2. open 结尾那句无条件 `tree.flush()` ⇒ redo 落成 SST 并顺带 `maybe_compact` 级联
    /// 3. `load_manifest` 逐个开 SST —— bulk 模式不在写路径 compact，L0 文件数在 600k 才越过
    ///    `l0_compact_threshold`(8)
    ///
    /// 拆成四条臂，各拿一份目录副本（同一份 shape，只差在臂本身）：
    /// - `facade`：就走门面 `MVCC::open`，给的是台架里那根尺子 —— 用来确认剖面**复现了悬崖**
    /// - `uncond_flush` / `cond_flush`：手拆 `try_open_with` 的每一步、逐段计时 + 计数器，
    ///   两臂只差「open 里的 flush 是无条件还是有条件」⇒ **同一轮里的 A/B**，不用跨构建比墙钟
    /// - `no_bulk(有 redo)`：另一份不跑 bulk 的 shape，WAL 里留着 6 万条待重放 ⇒ 条件为真、
    ///   照样 flush —— 这条留着是为了**如实报剩下那笔没修的账**（见下）
    ///
    /// # 量出来的答案（release，600k 键，数据在 C: 的 SSD）
    ///
    /// - 悬崖**整段在 `flush` 里**：同一轮里 `uncond_flush` 总 **13 947 618.8 µs**（flush 段
    ///   13 938 766.2），`cond_flush` 总 **8 362.3 µs**（flush 段 0.0）⇒ **1668×**，其余九段
    ///   合起来 ≈ 3 ms。机制是 `LSMTree::flush()` 末尾无条件 `maybe_compact()`，而 bulk 的
    ///   `finish()` 把 L0 正好留在 `l0_compact_threshold`(8) 上：**8 个 L0 越线 → 打开库这件事
    ///   自己把 85 MB 搬进 L1/L2、写出 37 个文件**（层文件 `[8,0,0,0] → [0,34,3,0]`、盘上
    ///   8 个 85 MB → 37 个 74 MB；有条件那臂两处都不动）。
    /// - 候选 1（WAL 全量解析）**证伪**：有 bulk 时重开前 WAL 只剩 32 B / 0 条记录（`finish()`
    ///   已收口），`read_all` 3.4 µs。但这条通道本身是贵的 —— `no_bulk` 臂里 60121 条记录 /
    ///   8.7 MB 的 `read_all` = 254.7 ms ⇒ **每条 ≈ 4.2 µs**（帧头+帧体各一次定位读，而
    ///   `DiskFile::read_exact_at` 是 seek+read），这是"崩溃点离上次 checkpoint 越远、重开越慢"
    ///   的另一笔账，量级远小于压实那笔。
    /// - 200k 不炸的原因就是那条阈值：同一 shape 在 200k 只有 **4 个 L0 文件**（<8）⇒ 两臂同价
    ///   （9.8 ms vs 6.4 ms，都不压实）。
    /// - `[facade]`（走修后的门面）= **13.3 ms**。同档修前的台架读数是 17.5~19.8 s（跨轮噪声 1.3×，
    ///   只当量级看）；因果凭据取上面那对**同轮**数字 + 层文件数不动。
    /// - ✅ 当时"没修完的部分"：`no_bulk` 那条（有 redo 的崩溃恢复）仍然 14.4 s，其中 flush 13.7 s ——
    ///   因为 redo 落了盘就必须 compact。**这笔后来分两次收掉**：M30 查到时间元凶是
    ///   `SstBuilder::estimated_bytes()` 每交一行重求和整表（同档 14.43 s → **3.55 s**），
    ///   M33 把剩下的"输入整篇物化"（内存账，峰值 156.8 MB）改成流式源（→ 6.3 MB）。
    ///   下面 `compaction_cost_split` 的注释里有两笔的完整拆分。
    ///
    /// 略去的写：`set_with_ttl×1000` / `incr×1000`（各 1000 行 ≈ 0.2 MB WAL，跟 60 万行那串比是零头）。
    ///
    /// ```text
    /// REOPEN_PROBE_ROWS=600000 cargo test --release --lib -- --ignored --nocapture \
    ///     --test-threads=1 reopen_shape_probe
    /// ```
    #[test]
    #[ignore = "重开剖面，手动 release 单线程跑：REOPEN_PROBE_ROWS=600000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 reopen_shape_probe"]
    fn reopen_shape_probe() {
        use std::time::{Duration, Instant};

        let rows: usize = std::env::var("REOPEN_PROBE_ROWS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(200_000);

        // 键/值形状照抄门面台架 engine.rs 的 scale_baseline（键 ≈35 B、值 ≈60 B，都不进 blob 旁路）
        fn key(i: usize) -> Vec<u8> {
            format!("{i:08}#/worldquant/datafields/x.json").into_bytes()
        }
        fn val(i: usize) -> Vec<u8> {
            format!("{{\"i\":{i},\"h\":\"{:0<28}\",\"v\":1}}", i * 7).into_bytes()
        }
        fn us(d: Duration) -> f64 {
            d.as_secs_f64() * 1e6
        }

        fn copy_tree(from: &Path, to: &Path) -> std::io::Result<()> {
            std::fs::create_dir_all(to)?;
            for e in std::fs::read_dir(from)? {
                let e = e?;
                let dest = to.join(e.file_name());
                if e.file_type()?.is_dir() {
                    copy_tree(&e.path(), &dest)?;
                } else {
                    std::fs::copy(e.path(), &dest)?;
                }
            }
            Ok(())
        }

        /// `dir/sst` 里的文件数与字节数 —— 候选 3 的直接计数（不看 stats，看盘上真有几个文件）
        fn sst_dir_stats(dir: &Path) -> (usize, u64) {
            let mut n = 0usize;
            let mut b = 0u64;
            let Ok(rd) = std::fs::read_dir(dir.join("sst")) else {
                return (0, 0);
            };
            for e in rd.flatten() {
                if let Ok(m) = e.metadata() {
                    if m.is_file() {
                        n += 1;
                        b += m.len();
                    }
                }
            }
            (n, b)
        }

        /// 把门面台架里「两次重开之间」那串做完，返回给调用方（此刻库已 close）
        fn build_shape(dir: &Path, rows: usize, with_bulk: bool) {
            let mvcc = MVCC::open(dir);
            for base in (0..rows).step_by(1000) {
                let end = (base + 1000).min(rows);
                let items: Vec<(Vec<u8>, Vec<u8>)> =
                    (base..end).map(|i| (key(i), val(i))).collect();
                let tx = mvcc.begin_transaction();
                if let Err(bad) = tx.batch_set(&items) {
                    panic!("灌第 {base} 批冲突: {}", String::from_utf8_lossy(&bad));
                }
                tx.commit();
            }
            mvcc.flush();
            mvcc.checkpoint();
            drop(mvcc);

            // ---- 第一次冷重开（就是台架里的 冷重开(恢复)，它自己会把 WAL 收口）----
            let mvcc = MVCC::open(dir);
            // 更新 10% 的键：制造版本链，vacuum 才有得裁
            let updates = (rows / 10).max(1);
            for base in (0..updates).step_by(500) {
                let end = (base + 500).min(updates);
                let items: Vec<(Vec<u8>, Vec<u8>)> =
                    (base..end).map(|i| (key(i), val(i + rows))).collect();
                let tx = mvcc.begin_transaction();
                if let Err(bad) = tx.batch_set(&items) {
                    panic!("更新第 {base} 批冲突: {}", String::from_utf8_lossy(&bad));
                }
                tx.commit();
            }
            let h = mvcc.health();
            let v = mvcc.vacuum().unwrap();
            let _ = mvcc.vacuum_bounded(None, 2048);
            eprintln!(
                "  shape(rows={rows}, bulk={with_bulk}): health rows={} live={} | vacuum 裁 {} 行",
                h.total_rows, h.live_keys, v.versions_removed
            );
            mvcc.flush();
            if with_bulk {
                let bulk_rows = (rows / 10).max(1);
                let mut bulk = mvcc.begin_bulk();
                let items: Vec<(Vec<u8>, Vec<u8>)> =
                    (0..bulk_rows).map(|i| (key(rows + i), val(i))).collect();
                bulk.put_batch_owned(items);
                bulk.finish();
            }
            drop(mvcc);
        }

        /// 手拆 `try_open_with`：每一步单独计时，计数打在最后。
        /// `flush_at_open` = 这次拆的是修前(真)还是修后(假)的那份序列 —— 留成同一轮里的 A/B 对照
        fn probe_open(arm: &str, dir: &Path, flush_at_open: bool) {
            let t = Instant::now();
            let opts = pin_mvcc_invariants(LSMTreeOptions::for_mvcc());
            let mut tree = LSMTree::open_with_config(dir, opts).unwrap();
            let d_tree = t.elapsed();

            let t = Instant::now();
            let mut wal = Wal::open(wal_path(dir)).unwrap();
            let d_wal_open = t.elapsed();
            let wal_bytes = wal.size_bytes().unwrap();

            let t = Instant::now();
            let entries = wal.read_all().unwrap();
            let d_read_all = t.elapsed();

            let t = Instant::now();
            let plan = analyze_recovery(&entries);
            let d_analyze = t.elapsed();

            let t = Instant::now();
            for (_xid, k, v) in &plan.committed_writes {
                tree.insert(k.clone(), v.clone());
            }
            for (_xid, k) in &plan.uncommitted_writes {
                let _ = tree.remove(k);
            }
            let d_redo = t.elapsed();

            let t = Instant::now();
            let start_ver = compute_start_version(dir, &plan);
            let d_start_ver = t.elapsed();

            let before = tree.stats();
            let (files_before, bytes_before) = sst_dir_stats(dir);
            // `has_redo` 的口径与 `try_open_with` 里那句一致：判的是"有没有东西要重放"，
            // 不是"memtable 现在空不空"（那在插过之后就已经分不出了）。
            let has_redo =
                !plan.committed_writes.is_empty() || !plan.uncommitted_writes.is_empty();
            let did_flush = flush_at_open || has_redo;
            let t = Instant::now();
            if did_flush {
                tree.flush().unwrap();
            }
            let d_flush = t.elapsed();
            let after = tree.stats();
            let (files_after, bytes_after) = sst_dir_stats(dir);

            let t = Instant::now();
            seal_wal_checkpoint(&mut wal, start_ver).unwrap();
            let d_seal = t.elapsed();
            let t = Instant::now();
            persist_mvcc_next(dir, start_ver);
            let d_persist = t.elapsed();
            let total = d_tree
                + d_wal_open
                + d_read_all
                + d_analyze
                + d_redo
                + d_start_ver
                + d_flush
                + d_seal
                + d_persist;

            eprintln!(
                "  [{arm}] 总计 {:>12.1} µs | tree.open_with_config {:>12.1} | Wal::open {:>8.1} \
                 | read_all {:>12.1} | analyze {:>10.1} | redo {:>10.1} | start_ver {:>8.1} \
                 | flush {:>12.1} | seal {:>8.1} | persist {:>8.1}",
                us(total),
                us(d_tree),
                us(d_wal_open),
                us(d_read_all),
                us(d_analyze),
                us(d_redo),
                us(d_start_ver),
                us(d_flush),
                us(d_seal),
                us(d_persist),
            );
            eprintln!(
                "  [{arm}] 计数: flush_at_open={flush_at_open} did_flush={did_flush} | \
                 WAL {} B / 记录 {} 条（帧均 {} B）| redo committed={} uncommitted={} \
                 | start_ver={} | 盘上 sst {} 个 {} MB → {} 个 {} MB | L0 {} → {} | 层文件 {:?} → {:?}",
                wal_bytes,
                entries.len(),
                if entries.is_empty() {
                    0
                } else {
                    wal_bytes.saturating_sub(16) / entries.len() as u64
                },
                plan.committed_writes.len(),
                plan.uncommitted_writes.len(),
                start_ver,
                files_before,
                bytes_before / 1_048_576,
                files_after,
                bytes_after / 1_048_576,
                before.l0_files,
                after.l0_files,
                before.level_files,
                after.level_files,
            );
        }

        eprintln!("=== 重开剖面 rows={rows}（µs，release，单线程）===");
        let base_dir = tmp_db("reopen_probe_src");
        let t = Instant::now();
        build_shape(&base_dir, rows, true);
        eprintln!("  shape 构建耗时 {:.1} s", t.elapsed().as_secs_f64());
        let (f0, b0) = sst_dir_stats(&base_dir);
        eprintln!("  构建完盘上: sst {} 个 {} MB", f0, b0 / 1_048_576);

        // 臂 1：门面重开（台架里那根尺子）
        let d_facade = tmp_db("reopen_probe_facade");
        copy_tree(&base_dir, &d_facade).unwrap();
        let t = Instant::now();
        drop(MVCC::open(&d_facade));
        eprintln!("  [facade] MVCC::open 整条 {:>12.1} µs", us(t.elapsed()));

        // 臂 2/3：同一份目录副本，只换"open 里 flush 是无条件还是有条件" ⇒ 一轮内的 A/B
        let d_uncond = tmp_db("reopen_probe_uncond");
        copy_tree(&base_dir, &d_uncond).unwrap();
        probe_open("uncond_flush", &d_uncond, true);
        let d_cond = tmp_db("reopen_probe_cond");
        copy_tree(&base_dir, &d_cond).unwrap();
        probe_open("cond_flush", &d_cond, false);

        // 臂 4：不跑 bulk 的另一份 shape（WAL 里有 6 万条待重放）⇒ 条件为真，仍然会 flush，
        // 于是这条把**有 redo 那条路径的整笔代价**单独暴露出来（当年挂的是"输入整篇物化"，
        // 时间那一半 M30 收掉、内存那一半 M33 收掉；留着这条臂是为了看两笔收完之后还剩多少）
        let d_nobulk = tmp_db("reopen_probe_nobulk");
        build_shape(&d_nobulk, rows, false);
        probe_open("no_bulk(有 redo)", &d_nobulk, false);

        for p in [
            &base_dir,
            &d_facade,
            &d_uncond,
            &d_cond,
            &d_nobulk,
        ] {
            cleanup(p);
        }
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
    /// `scan_foreach` 提前叫停时，**不许先把整库折一遍**（M27）。
    ///
    /// 这条路径的文档从写下那天起就声称「不收集全库到单一大 Vec」，而实现是
    /// `for … in collect_latest_raw_in(...)` —— 那一句会把界内每一行都先折出来，再逐条搬成
    /// `ExportRecord`。200k 键实测因此比结果集本身还多占 ≈ 15 MB（`一次全量 scan`
    /// 峰值 +45.5 MB / 常驻 30.7 MB）。现在走共用件 `fold_rows_streaming`：折一行交一行，
    /// 叫停之后的键不再碰。
    ///
    /// LSM 的候选来自轻量视图（只带 seq 与种类，不克隆 value）⇒ 没有「取了几次值」这笔账，
    /// 能数的是**折过多少把编码键**。尺子单位不同，判据就按比例给：一片 = `FOLD_CHUNK`(4096)，
    /// 所以早停只该走到全库的一小截（老写法先把全库折完才交第一行 ⇒ 比例 1.0）。
    #[test]
    fn test_scan_foreach_stops_without_folding_the_library() {
        const N: usize = 3 * 4096 + 100;
        fn ruler() -> u64 {
            crate::lsm_tree::sst::light_visits()
        }
        let path = tmp_db("foreach_stop");
        {
            let mvcc = MVCC::open(&path);
            for base in (0..N).step_by(2000) {
                let end = (base + 2000).min(N);
                let items: Vec<(Vec<u8>, Vec<u8>)> = (base..end)
                    .map(|i| {
                        (
                            format!("k{i:05}").into_bytes(),
                            format!("v{i}").into_bytes(),
                        )
                    })
                    .collect();
                let tx = mvcc.begin_transaction();
                if let Err(bad) = tx.batch_set(&items) {
                    panic!("灌第 {base} 批: {}", String::from_utf8_lossy(&bad));
                }
                tx.commit();
            }
            let tx = mvcc.begin_transaction();
            let base = ruler();
            let mut handed = 0usize;
            tx.scan_foreach(None, None, |rec| {
                assert!(rec.value.is_some(), "交出来的行没有值");
                handed += 1;
                false // 只要第一行
            });
            let early = ruler() - base;
            let base = ruler();
            let all = tx.scan(None, None);
            let full = ruler() - base;
            tx.commit();

            assert_eq!(handed, 1, "回调明明叫停了");
            assert_eq!(all.len(), N, "参照量：全量 scan 要折出所有行");
            assert!(
                early * 2 <= full,
                "只要 1 行就折了 {early} 把键，全量也才 {full} ⇒ 没提前收手（一片该是 4096 级别）"
            );
            eprintln!("[scan_foreach 早停] 交 1 行付 {early}，全量 {N} 行付 {full}");
        }
        cleanup(&path);
    }

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

    /// M50 的量具重做 + 结构归因（`#[ignore]`，手动 release 单线程跑）。
    ///
    /// 为什么要重做量具：10-20 那张三档表里 `seek_prev 中间键` 读的是 `timed()` 里的**一次调用**
    /// （`src/engine.rs:3522`）。µs 级单次采样在这台机上不能当 A/B 凭据（本机规则：**µs 级 A/B
    /// 必须交错多轮取最小**），而一次反向定位很容易 just 撞上"某个 SST 块不在缓存里"
    /// （600k 档点读实测 块/次 5.31、未命中/次 1.31、一次未命中 ≈ 60 µs）
    /// ⇒ "20k 27.1 µs → 600k 351.1 µs = 12.96×" 里多少是结构、多少是单次运气，那张表答不了。
    ///
    /// 这条台架把**同一个动作量 N 次**，并一次给出五把结构尺子（全是计数，不是墙钟）：
    /// `步/次`（M20 那把 `seek_prev_steps`：一次调用往回退过几把死键）、`探文件/次`、`块/次`、
    /// `未命中/次`、`条目比较/次`。哪一把跟着规模涨，责任就在哪一层：
    /// 步数涨 ⇒ 反向定位真在跳更多死键（结构问题，要修）；探文件涨 ⇒ 盘上形状变了；
    /// 只有 未命中/次 涨 ⇒ 是覆盖率，M23/M32 那本账，不是这一刀。
    ///
    /// 两臂只差一个现成旋钮（M32 的手法：**转旋钮、不动代码**）：
    /// `默认` = `for_mvcc()`（M41b 之后 `mem_threshold_bytes` 读**真内存** ⇒ 8 B 值的库每 ~7.4 万行 flush）；
    /// `大 memtable` = 字节档抬到不触发、条数档留在 262 144 ⇒ 就是 **M41b 之前**的行为
    /// （当时 32 MB 载荷账在 8 B 值上从不触发，flush 只由条数说话 ⇒ 60 万行只 flush ~2 次）。
    /// 两臂都先报**盘上形状**（每层文件数 / SST 总数 / 块数 / 缓存容量）：形状没变，
    /// "351 µs 是 M41b 换来的形状代价"这句话就还没有对照。
    ///
    /// ```text
    /// LSM_SEEK_SHAPE_AB_ROWS=600000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 lsm_seek_prev_shape_ab
    /// ```
    #[test]
    #[ignore = "seek_prev 的量具重做 + memtable 形状 A/B，手动 release 单线程跑：LSM_SEEK_SHAPE_AB_ROWS=600000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 lsm_seek_prev_shape_ab"]
    fn lsm_seek_prev_shape_ab() {
        use crate::common::store::seek_prev_steps;
        use crate::lsm_tree::lsm_tree::LSMTreeOptions;
        use crate::lsm_tree::sst::{block_misses, block_reads, sst_entry_walks, sst_probes};
        let rows: usize = std::env::var("LSM_SEEK_SHAPE_AB_ROWS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(200_000);
        let probes: usize = std::env::var("LSM_SEEK_SHAPE_AB_PROBES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(2000);
        let key = |i: usize| format!("k{i:06}").into_bytes();
        // 照抄基线台架的形状：一把键 8 B、值 1 B、一个大事务灌进去、flush + checkpoint、冷重开
        let snap = || (block_reads(), block_misses(), sst_probes(), sst_entry_walks());

        for big_mem in [false, true] {
            let name = if big_mem { "大 memtable(照 M41b 前)" } else { "默认(M41b 后)" };
            let opts = || {
                let mut o = LSMTreeOptions::for_mvcc();
                if big_mem {
                    o.mem_threshold_bytes = usize::MAX / 8;
                    o.mem_threshold_entries = 256 * 1024;
                }
                o
            };
            let path = tmp_db("seek_shape_ab");
            {
                let mvcc = MVCC::try_open_with(&path, Some(opts())).unwrap();
                let tx = mvcc.begin_transaction();
                for i in 0..rows {
                    assert!(tx.set(&key(i), b"v".to_vec()), "灌第 {i} 把失败");
                }
                tx.commit();
                mvcc.flush();
                mvcc.checkpoint();
            }
            // ⚠️ 必须冷重开再量：写阶段焐热的块缓存会让"反向一步"看起来比生产便宜
            let mvcc = MVCC::try_open_with(&path, Some(opts())).unwrap();
            let (st, cap, blocks) = {
                let kv = mvcc.kv.lock().unwrap();
                (kv.stats(), kv.block_cache_capacity(), kv.sst_blocks_for_test())
            };
            eprintln!(
                "[形状] 臂={name} 行数 {rows} ⇒ SST {} 篇、层文件 {:?}、块 {blocks}、缓存容量 {cap}",
                st.total_sst, st.level_files
            );
            let mut best = u128::MAX;
            let mut cold = 0u128;
            let mut c_best = (0u64, 0u64, 0u64, 0u64);
            let mut steps_best = 0u64;
            for round in 0..2 {
                let tx = mvcc.begin_transaction();
                let a = snap();
                let p0 = seek_prev_steps();
                let t = std::time::Instant::now();
                for j in 0..probes {
                    let i = (j * 7919 + 13) % rows;
                    assert!(tx.seek_prev(&key(i)).is_some(), "seek_prev 第 {i} 把落空");
                }
                let spent = t.elapsed().as_micros();
                let b = snap();
                let steps = seek_prev_steps() - p0;
                drop(tx);
                if round == 0 {
                    cold = spent;
                } else if spent < best {
                    best = spent;
                    c_best = (b.0 - a.0, b.1 - a.1, b.2 - a.2, b.3 - a.3);
                    steps_best = steps;
                }
            }
            let per = |x: u64| x as f64 / probes as f64;
            eprintln!(
                "[seek_prev A/B] 臂={name} {probes} 次 ⇒ 冷启一趟 {:>7.1} µs/次、焐热一趟 {:>7.1} µs/次 \
                 | 步/次 {:.2} | 探文件/次 {:.2} | 块/次 {:.2} | 未命中/次 {:.2} | 条目比较/次 {:.1}",
                cold as f64 / probes as f64,
                best as f64 / probes as f64,
                steps_best as f64 / probes as f64,
                per(c_best.2),
                per(c_best.0),
                per(c_best.1),
                per(c_best.3),
            );
            let _ = std::fs::remove_dir_all(&path);
        }
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
