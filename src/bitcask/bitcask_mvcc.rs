//! MVCC 事务层 + WAL 崩溃恢复（适配 Bitcask 后端）
//!
//! 从 `bplus-tree` / `lsm-tree` 移植：协议与语义保持一致，存储后端换成 [`Bitcask`]。
//!
//! 提交协议（简化版 ARIES）：
//! 1. 写路径：先 append WAL Write，再写 Bitcask 日志
//! 2. commit：append WAL Commit → **fsync WAL**（持久化点）→ 可选 fsync 数据文件
//! 3. 崩溃恢复：重放已提交 Write，撤销未提交 Write，再 checkpoint 截断 WAL
//!
//! **Bulk load 模式**（[`MVCC::begin_bulk`]）：
//! - 关闭 WAL / 冲突检测 / 活跃事务表
//! - 结束时 [`BulkLoader::finish`] → flush + checkpoint
//!
//! 与 bplus 的差异：
//! - 大 value（> [`crate::storage::BLOB_THRESHOLD`]）外置到 `data.blob`，主日志只存指针
//! - Checkpoint 的 `root_page_id` / `next_page_id` 固定写 0
//! - 数据目录：`dir/data.log` + `dir/data.blob` + `dir/bitcask.wal` + `dir/bitcask.lock`
//! - 版本行 tombstone 用 Bitcask **软 tombstone**（KeyDir 保留）；rollback/vacuum 用 **硬删除**
//! - bulk 路径：无 WAL + 4MB 写缓冲 + insert_batch，目标 10M / 10min

use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use crate::bitcask::{Bitcask, storage::{lock_path,
    BlobStore, FileLock, VAL_TAG_BLOB, data_path, decode_stored_value, encode_stored_value, wal_path,
}};
use crate::bitcask::wal::{analyze_recovery, Wal, WalRecord};
pub use crate::common::{
    DurabilityProfile, EngineTuning, ExportRecord, MvccHealth, VacuumRound, VacuumStats,
};
use crate::common::{HealthFold, VacuumScheduler};
use crate::common::txn::{BulkCore, Registry, TxnCore, WalTail, WriteSide};
use crate::common::vacuum_plan_group;
use crate::common::version_key_upper_bound;

/// MVCC 存储引擎（Bitcask 后端）
pub struct MVCC {
    kv: Arc<Mutex<Bitcask>>,
    blob: Arc<Mutex<BlobStore>>,
    wal: Arc<Mutex<Wal>>,
    active_txn: Arc<Mutex<Registry>>,
    next_version: Arc<AtomicU64>,
    /// WAL 增长到多少字节就在下一次提交后自动 checkpoint（0 = 关闭自动触发）
    wal_limit: Arc<AtomicU64>,
    /// 自动回收：切片宽度 / 记账 / 游标（策略与实现在 [`crate::common::VacuumScheduler`]）
    vac: Arc<VacuumScheduler>,
    db_dir: PathBuf,
    /// 持有期间保证单写者
    _lock: FileLock,
}

/// 普通 MVCC 事务
///
/// 事务语义（版本、活跃快照、写集、冲突判定、整条写序列、回收记账）在 [`TxnCore`]
/// （[`crate::common::txn`]，四引擎共用）；这一家只留"字节在日志的哪个偏移、大值走不走 blob"。
pub struct Transaction {
    pub(crate) kv: Arc<Mutex<Bitcask>>,
    pub(crate) blob: Arc<Mutex<BlobStore>>,
    pub(crate) wal: Arc<Mutex<Wal>>,
    next_version: Arc<AtomicU64>,
    wal_limit: Arc<AtomicU64>,
    pub(crate) core: TxnCore,
}

/// 写路径的守卫：三把锁都已按内核要求的顺序（活跃表 → wal → blob → kv）拿好。
///
/// 回滚与部分撤销复用同一个守卫（会顺手多锁一次 blob —— 那两条路不碰它，锁是无竞争的，
/// 换来的是"写侧只有一种守卫形状"）。
pub(crate) struct TxnSide<'w, 'b, 'k> {
    pub(crate) wal: &'w mut Wal,
    pub(crate) blob: &'b mut BlobStore,
    pub(crate) kv: &'k mut Bitcask,
}

/// 收尾用的守卫：`commit` 只要往 WAL 落一条 `Commit` 再 fsync，不该在这期间握着 kv/blob 锁。
struct WalOnly<'w> {
    wal: &'w mut Wal,
}

/// 批量导入会话：无 WAL、无冲突检测，单写者假定
pub struct BulkLoader {
    kv: Arc<Mutex<Bitcask>>,
    blob: Arc<Mutex<BlobStore>>,
    wal: Arc<Mutex<Wal>>,
    core: BulkCore,
}

/// 逻辑键 + 版本（调试用）与版本键编解码
///
/// 定义在 [`crate::common::version`]（四引擎共用）；这里 re-export 是为了
/// `crate::bitcask::encode_key` 这个既有路径不中断。
pub use crate::common::{decode_key, encode_key, VersionedKey};

/// 链尾反向查找的首轮预算，不够再翻倍重试。
///
/// 常态下一个逻辑 key 只有个位数版本，32 足够一次命中；翻倍只在
/// 「整条链没有一个可见」的退化场景才付到 O(链长)，与旧的整链扫描同阶。
const VERSION_BACK_STEP: usize = 32;

/// M37：一次全库 `vacuum()` 走查的**块大小**（物理行）。
///
/// 为什么要有块：`iter_meta_from` 按 `&self` 借住 KeyDir，而删除与读值都要 `&mut kv` ⇒
/// 借住期间只能**攒**。攒下来的 `to_delete` / `keep` 就是 M37 分段量出来的那 106 MB
/// （600k 键 × 2 版 = 120 万物理行那档，93 B/行）。按块走查、每块出借用后立刻清空，
/// 这两笔就从 O(全库) 变成 O(本常数) —— 实测 ① 段 **106.0 MB → 0.5 MB**。
/// 取 8192 行：按每行 40 B 算约 0.3 MB，比一页大、比一次事务的写集小，且远小于
/// 下游会遇到的库量级（守卫 `test_vacuum_deletes_in_bounded_chunks` 钉的就是"不随库涨"）。
const VAC_CHUNK_ROWS: usize = 8192;

// 测试专用（M37 的尺子）：一次 `vacuum_bounded` 里**单次 flush 前最多攒了多少待处理行**。
// 判据要的是"这道界真的生效"，所以数的是 `to_delete.len() + kept.len()` 的历史最大值。
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

// 测试专用：本线程调用 `scan_max_version`（O(总行数) 的全库 KeyDir 扫描）的次数。
// 用来守住冷启动的三级优先：有 Checkpoint 时就不该再全库扫。
#[cfg(test)]
thread_local! {
    static SCAN_MAX_CALLS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// 当前线程发生过几次「全库扫求最大版本」（测试探针用）
#[cfg(test)]
pub(crate) fn scan_max_calls() -> u64 {
    SCAN_MAX_CALLS.with(|c| c.get())
}

// 测试专用（M37 的先量）：一次全库 `vacuum_bounded` 的分段瞬时峰值标尺在 [`crate::mem_probe`]
// 里（`stage_base` / `stage_mark` / `stage_peaks`），M41 起 lsm 也用同一把。
//
// 为什么要分段而不是只看整段：600k 档 `vacuum()` 用修好的量具（M38）读到 **171.4 MB**，
// 而这段里叠着四件事 —— ①归组判定、②删除 + flush + blob sync、③blob GC（读存活行 +
// `rewrite_in_place`）、④`kv.merge()` 重写主日志。M30/M31 两次的教训都是"最合理的那件事
// 不是元凶"，所以先量再动手。⚠️ 标尺带门控（`VACUUM_STAGE_SPLIT`），常开会把外层台架的
// 读数锯短，见 `mem_probe` 里那段注释。

/// 把 WAL 收口成「只剩一条 Checkpoint」。
///
/// 顺序反直觉但必须这样：原先是 `append(Checkpoint) → sync → truncate()`，而 `truncate()`
/// 只留 header —— **刚写的 Checkpoint 被自己抹掉了**，于是下次 open 无从知道 `next_version`，
/// 只能退回 O(总行数) 的全库 KeyDir 重扫。改成先截断、再把 Checkpoint 写成新 WAL 的第一条。
///
/// 崩在截断之后、写入之前也不影响正确性：那时 WAL 为空，open 走全库扫拿起点，只是慢。
fn seal_wal_checkpoint(wal: &mut Wal, next_version: u64) -> std::io::Result<()> {
    wal.truncate()?;
    wal.append(WalRecord::Checkpoint {
        next_version,
        root_page_id: 0,
        next_page_id: 0,
    })?;
    wal.sync()
}

/// checkpoint 的实体：手动 `MVCC::checkpoint()` 与提交时的 WAL 阈值触发共用同一份实现。
///
/// 顺序不能换：`Commit` 记录落盘 = 事务持久，数据页可以之后慢慢刷；但 WAL 一旦被截断，
/// 未落盘的数据就再无重放来源 —— 所以先 flush 数据与 blob，最后才收口 WAL。
/// 顺手 `dump_hint` 是为了下次 open 不必全量扫日志。
fn run_checkpoint(
    kv: &Mutex<Bitcask>,
    blob: &Mutex<BlobStore>,
    wal: &Mutex<Wal>,
    next_version: &AtomicU64,
) {
    let mut kv = kv.lock().unwrap();
    let mut blob = blob.lock().unwrap();
    let mut wal = wal.lock().unwrap();
    let _ = kv.flush();
    let _ = blob.sync();
    let _ = kv.dump_hint();
    let next_ver = next_version.load(Ordering::SeqCst);
    let _ = seal_wal_checkpoint(&mut wal, next_ver);
}

impl MVCC {
    /// 崩溃安全画像 —— 本引擎**如实**的构件清单，跨引擎不等价（见 [`MvccHealth`] 同层的
    /// [`DurabilityProfile`] 说明）。
    ///
    /// Bitcask 唯一的缺口是主日志记录没有校验和：`data.log` 的裸记录是
    /// `key_len | flag | key | value`，坏一个字节不会报错，只会读出一个错值。
    /// 补 CRC 要改磁盘格式 —— 本仓库明确不采纳破坏性格式改动，所以把它**说清楚**而不是藏起来。
    pub fn durability_profile(&self) -> DurabilityProfile {
        DurabilityProfile {
            engine: "bitcask",
            transaction_wal: true,
            fsync_on_commit: true,
            // ⚠️ 见上面的说明：data.log 裸记录
            data_checksum: false,
            // blob 记录是 `len | crc32 | data`
            blob_checksum: true,
            // 追加式布局：没有原地页写 ⇒ 不存在 torn page 这条能力
            in_place_page_writes: false,
            torn_page_guard: false,
            // M6a：open 会认回崩在改名之前的 data.merge / 清掉残骸
            merge_artifact_recovery: true,
        }
    }

    /// 全库健康度快照。**只走 KeyDir 元数据**（`iter_meta`），不读 value、不碰 blob。
    ///
    /// 代价是 O(物理行数) 的内存遍历，属管理面调用：别放进热路径，长跑里按分钟级采样就够。
    pub fn health(&self) -> MvccHealth {
        // 一次只持一把锁：`active_txn` 读完就放，再拿 `kv`。
        // 嵌套拿锁会和 [`MVCC::vacuum`]（`active_txn → kv → blob`）构成反向嵌套。
        let active_txns = self.active_txn.lock().unwrap().len() as u64;
        let kv = self.kv.lock().unwrap();
        // 分组折叠用四引擎共用的 [`HealthFold`]：KeyDir 按 enc 升序 ⇒ 同一逻辑 key 的
        // 版本块相邻 ⇒ 顺序一遍就数得出链长，不必真的建 `BTreeMap<raw, Vec<..>>`
        let mut fold = HealthFold::default();
        for (enc, _loc) in kv.iter_meta() {
            let Some(vk) = decode_key(enc) else {
                continue;
            };
            fold.push(&vk.raw_key);
        }
        fold.finish(active_txns)
    }

    /// 打开/创建磁盘上的 MVCC 引擎（含 WAL 恢复）。失败时 panic（兼容旧代码）。
    pub fn open(dir: impl AsRef<Path>) -> Self {
        Self::try_open(dir).map_err(|e| format!("MVCC::open 失败: {e}")).unwrap()
    }

    /// 打开/创建，错误以 `io::Result` 返回（锁冲突、坏文件、IO 等）。
    pub fn try_open(dir: impl AsRef<Path>) -> std::io::Result<Self> {
        Self::try_open_with(dir, None)
    }

    /// 打开并允许覆盖分片滚动阈值（`max_file_bytes = None` ⇒ 用引擎默认）。
    ///
    /// 门面层 `EngineConfig::bitcask` 走的就是这里 —— 目的是让下游**不必绕开门面自己构造
    /// `Bitcask`**。基座被依赖而不是被复制，缺一个入口就多一层复制。
    pub fn try_open_with(
        dir: impl AsRef<Path>,
        max_file_bytes: Option<u64>,
    ) -> std::io::Result<Self> {
        let db_dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&db_dir)?;

        // 锁文件路径规则是各引擎自己的（bitcask: `dir/bitcask.lock`），锁的**语义**在
        // [`crate::common::disk::FileLock`]（四引擎共用的一份实现）
        let lock = FileLock::try_acquire_at(&lock_path(&db_dir))?;
        let data_log = data_path(&db_dir);
        let mut tree = match max_file_bytes {
            Some(n) => Bitcask::with_max_file_bytes(data_log, n)?,
            None => Bitcask::new(data_log)?,
        };
        let blob = BlobStore::open(&db_dir)?;
        let mut wal = Wal::open(wal_path(&db_dir))?;

        let entries = wal.read_all()?;
        let plan = analyze_recovery(&entries);

        for (_xid, key, value) in &plan.committed_writes {
            tree.insert(key.clone(), value.clone())?;
        }
        for (_xid, key) in &plan.uncommitted_writes {
            let _ = tree.remove(key);
        }

        // 冷启动的起始版本号，三级优先（照 bplus 的口径，别让同一个优化在三处有三种成熟度）：
        // 1. WAL 里最后一个 Checkpoint 的 next_version —— checkpoint 先 flush 数据再截断 WAL，
        //    所以它之前的版本行都已落盘、编号必然小于它，它之后的记录才是新增写入；
        // 2. 没有 Checkpoint 就用 WAL 见过的最大 xid；
        // 3. 两者都没有（首次打开/老库）才退化到全库扫 KeyDir 求最大版本。
        //
        // 走 1 才让「库越大 open 越慢」这条通道断开：`scan_max_version` 是 O(总行数)。
        // 注意这里省的是**启动开销**而不是正确性 —— 即便退化到 3，扫出来的最大值也不会
        // 小于某个仍存活的行，所以不会发生版本号复用。
        let ckpt_nv = match plan.last_checkpoint.as_ref() {
            Some(WalRecord::Checkpoint { next_version, .. }) => Some(*next_version),
            _ => None,
        };
        let start_ver = match ckpt_nv {
            Some(nv) => nv.max(plan.max_xid.saturating_add(1)).max(1),
            None => {
                let disk_max = scan_max_version(&mut tree)?;
                disk_max.max(plan.max_xid).saturating_add(1).max(1)
            }
        };

        // 这一家**保持无条件**：`Bitcask::flush` = 写缓冲空就直接返回 + 一次 fsync，是有界工作。
        // LSM 那一档在 M28 改成了「有 redo 才 flush」，因为它的 `flush()` 末尾挂着无界的
        // `maybe_compact()` —— 600k 实测那次悬崖 13.9 s 全花在那儿。若将来这里的 flush 长出
        // 随库涨的活（文件滚动、合并），照 `test_open_does_not_compact_when_nothing_to_redo`
        // 那把尺子（层文件数不动）改，别在这里加一条只会空转的断言。
        tree.flush()?;
        // 同样要「先截断再写 Checkpoint」，否则这次 open 留下的起点信息会被截断抹掉，
        // 下次 open 只能再全库扫一遍（首次打开的库尤其需要这条快路）。
        seal_wal_checkpoint(&mut wal, start_ver)?;

        Ok(Self {
            kv: Arc::new(Mutex::new(tree)),
            blob: Arc::new(Mutex::new(blob)),
            wal: Arc::new(Mutex::new(wal)),
            active_txn: Arc::new(Mutex::new(HashMap::new())),
            next_version: Arc::new(AtomicU64::new(start_ver)),
            wal_limit: Arc::new(AtomicU64::new(MVCC::WAL_CHECKPOINT_BYTES_DEFAULT)),
            vac: Arc::new(VacuumScheduler::new(MVCC::VACUUM_AUTO_SLICE_DEFAULT)),
            db_dir,
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
            "bitcask_mvcc_anon_{}_{}",
            std::process::id(),
            nanos
        ));
        Self::open(path)
    }

    pub fn begin_transaction(&self) -> Transaction {
        self.tick_auto_vacuum();
        Transaction::begin(
            self.kv.clone(),
            self.blob.clone(),
            self.wal.clone(),
            self.active_txn.clone(),
            self.next_version.clone(),
            self.wal_limit.clone(),
            self.vac.clone(),
        )
    }

    /// 自动回收的一小步：攒够一个切片宽度的新版本，就裁同样宽度的**一片**。
    ///
    /// 挂在 `begin_transaction` 而不是 commit 上：这一处还没有本线程的活跃事务，
    /// `vacuum_bounded` 的「有活跃事务就整轮跳过」不会被自己的调用方踩中；而且每个
    /// 事务至少 begin 一次，攒账的节奏和写路径一样。
    ///
    /// 触发量 = 切片宽度 ⇒ 回收速率与产生垃圾的速率 1:1，这是「跟得上」的下界；
    /// 一片的代价 = 读 `slice` 行元数据（不读 value），锁也就按这么久。
    /// 长尾那一片（库尾之后）会自动从头开始下一遍。
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
    /// 得靠手动 `vacuum()` —— 也就是回到「热点 key 堆二十万版、点读陪跑整条链」的状态。
    /// 记账/取账/游标的策略在 [`crate::common::VacuumScheduler`]（四引擎共用）。
    pub fn set_vacuum_auto_slice(&self, rows: u64) {
        self.vac.set_slice(rows);
    }

    pub fn vacuum_auto_slice(&self) -> u64 {
        self.vac.slice()
    }

    /// 默认切片宽度 = 16 Ki 行。
    ///
    /// 依据：一片的代价是读 16 Ki 行元数据（KeyDir 内存遍历，不读 value），单核机器上
    /// 亚毫秒级；而事故现场那条链有 23.8 万行 —— 默认值开着他就会在自己被需要之前
    /// 被裁掉。设这么小是为了「每次只挡一小下」，不是省总开销。
    pub const VACUUM_AUTO_SLICE_DEFAULT: u64 = 16 * 1024;

    /// 开启 bulk load：单写者、无 WAL、无冲突检测。
    ///
    /// **崩溃不安全**：finish 之前进程挂掉可能丢数据。
    /// 导入结束后必须 `finish()`。
    pub fn begin_bulk(&self) -> BulkLoader {
        BulkLoader {
            kv: self.kv.clone(),
            blob: self.blob.clone(),
            wal: self.wal.clone(),
            core: BulkCore::begin(self.next_version.clone()),
        }
    }

    /// 默认 WAL 阈值：16 MiB。
    ///
    /// 现场见过 WAL 长到 55MB 仍无人截断，而恢复路径是**全量扫描** WAL（`analyze_recovery`）
    /// —— 不截断等于启动一次比一次慢，连「重启止血」这个动作本身都在退化。
    pub const WAL_CHECKPOINT_BYTES_DEFAULT: u64 = 16 << 20;

    pub fn checkpoint(&self) {
        run_checkpoint(&self.kv, &self.blob, &self.wal, &self.next_version);
    }

    /// WAL 字节阈值可配置项：增长到这么多字节就在**下一次提交后**自动 checkpoint。
    /// 设 0 关闭自动触发（只能手动调 `checkpoint()`）。
    pub fn set_wal_checkpoint_limit(&self, bytes: u64) {
        self.wal_limit.store(bytes, Ordering::SeqCst);
    }

    pub fn wal_checkpoint_limit(&self) -> u64 {
        self.wal_limit.load(Ordering::SeqCst)
    }

    /// 当前生效的 active 分片软上限（字节），写满后 rotate 到下一个 `data.{id}.log`。
    pub fn max_file_bytes(&self) -> u64 {
        self.kv.lock().unwrap().max_file_bytes()
    }

    /// 本引擎能被调的东西 —— 清单在这里，不在门面。
    pub fn tuning(&self) -> EngineTuning {
        let mut t = EngineTuning::default();
        t.push_shared(self.vacuum_auto_slice(), Some(self.wal_checkpoint_limit()));
        t.push("max_file_bytes", self.max_file_bytes());
        t
    }

    pub fn flush(&self) {
        let mut kv = self.kv.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();
        let _ = kv.flush();
        let _ = blob.sync();
    }

    /// 压缩数据文件 + 写出 hint
    ///
    /// ⚠️ M43 起 `merge()` 内部有一段"索引重扫"：它先把旧的内存索引腾掉，再原地重扫新的
    /// `data.log`。正常那一次没人看得见（这段代码全程持 `kv` 锁，并发请求只是阻塞在锁上）。
    /// 但如果**重扫失败**，本方法返回 `Err` 而引擎进入隔离态：此后每一扇能报错的门
    /// （`get` / `scan_page` 背后的取值 / 写路径 / `dump_hint`）都返回错，**不会再回"键不存在"**。
    /// 磁盘上的数据是完整的，出路只有重新 `open`（那里会全量扫日志重建索引）。
    pub fn merge(&self) -> std::io::Result<()> {
        self.kv.lock().unwrap().merge()
    }

    /// 主动写出 KeyDir hint（不 merge）
    pub fn dump_hint(&self) -> std::io::Result<()> {
        self.kv.lock().unwrap().dump_hint()
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

    /// 版本回收：一次扫完全库。
    ///
    /// 就是 `vacuum_bounded(None, u64::MAX)`；保留规则、「有活跃事务就整轮跳过」的理由和
    /// 锁序说明都在 [`MVCC::vacuum_bounded`] 那边。
    pub fn vacuum(&self) -> std::io::Result<VacuumStats> {
        Ok(self.vacuum_bounded(None, u64::MAX)?.stats)
    }

    /// 有界版本回收：从逻辑 key `after`（**不含**）之后开始扫，最多读 `budget_rows` 行物理行。
    ///
    /// 保留规则和 [`MVCC::vacuum`] 完全一致，只是作用范围是一段而不是全库。
    ///
    /// **截断只发生在逻辑 key 边界**：半条链不能收 —— 「留哪一版」的判据要求整组版本都在手上。
    /// 所以一轮实际读的行数可能超出 `budget_rows`，超出量就是那一条链的长度：某把热点 key
    /// 堆了 23.8 万版时，裁它这一下就得一次读 23.8 万行元数据（走 KeyDir，不读 value）。
    /// 这是本设计明确接受的代价。
    ///
    /// blob 回收与日志压缩只在**全库轮次**（`budget_rows == u64::MAX`，也就是手动
    /// [`MVCC::vacuum`]）做：两者都是全库代价。判据不能是「游标回到 None」—— 一把 key 的
    /// 小库任何一片都正好读到库尾，那样自动回收每轮都会重写一遍库。
    ///
    /// 为什么要切片：自动回收要能在单核机器上跑，而 [`MVCC::vacuum`] 会把 kv 锁按到整库
    /// 扫完 —— 那期间点读点写全在排队。切片后每轮只按住一小段。
    ///
    /// 为什么是「整轮跳过」而不是旧的 `xmin = min(活跃事务 id)`：那个水位线**会裁掉
    /// 活跃事务快照仍需要的版本**。T 的可见判据是「v ≤ T.version 且 v ∉ T.active_xid」，
    /// 若「最后一个 < xmin」那一版本身就属于 T 的并发快照，T 得往前再退一版 ——
    /// 而那一版刚被裁掉 ⇒ T 静默读到更旧的值或 None。裁错版本是**静默数据错误**，
    /// 比多等一轮 vacuum 危险得多（守卫见 `test_vacuum_keeps_versions_needed_by_active_txn`）。
    ///
    /// 活跃表这把锁**全程持有**：确认它为空之后不会有新事务抢先拿到快照。
    /// 锁序 `active_txn → kv → blob` 与 [`Transaction::write`] 一致（那边是
    /// active_txn → wal → blob → kv，都在真正动手前拿齐，不会与这里交叉等待）。
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
                    blob_rewritten: Some(false),
                    skipped_active_txn: true,
                    rows_scanned: 0,
                },
                // 游标原样退回：跳过不丢进度，下一轮从同一处接着扫
                next_cursor: after.map(|k| k.to_vec()),
            });
        }
        let xmin = self.next_version.load(Ordering::SeqCst);

        let mut kv = self.kv.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();
        #[cfg(test)]
        // 分段标尺在 [`crate::mem_probe`]（M41 起 lsm 共用同一把）。⚠️ 它带门控：不设
        // `VACUUM_STAGE_SPLIT` 时这里绝不能压高水位，否则外层大库台架对 `vacuum()` 的读数会被锯短。
        let mut vac_base = crate::mem_probe::stage_base();

        // ① 分组只走 KeyDir 元数据：判「留哪一版」只需要版本号 + 是不是软墓碑，不需要 value。
        //    旧实现是两次 `kv.iter()`（每一行连 value 全部读出来），于是 vacuum 自己成了
        //    全库最重的一条路径 —— 在单核机器上「开自动回收」反而会在回收那一刻制造新事故。
        let budget_rows = budget_rows.max(1);
        //
        // M36：**只攒当前这一把键的那一组**。以前这里攒的是全库归组的
        // `BTreeMap<Vec<u8>, Vec<(u64, Vec<u8>, bool, ())>>`（每行两个 `Vec`：raw_key 与 enc）。
        // ⚠️ 当时报的"600k 档 +89.4 MB"是**坏量具**读数（M38 修好尺子之前，`峰值` 只看得见超过
        // 历史最高点的部分）；修好后同一形状同一档：整段 `vacuum()` 瞬时 **171.4 MB**，其中
        // **① 归组判定 106.0 MB（93 B/扫描行）+ ④ `merge()` 115.0 MB**，②③ 都是 0
        // ⇒ ①那笔现在的主人是 `to_delete` + `keep` 这两个跨全库累加器，不是"当前一组"（M37）。
        // 版本键按 `raw ‖ version_be` 升序到达 ⇒ 同一把键的行必然连续，判完一组就地交出去。
        // 守卫：`all_engines_vacuum_judges_per_group_not_whole_library`（数"判定发生了几次"与
        // "最大一组的行数"，四家一条各核一次）。
        // M37：**按块走查**。`iter_meta_from` 按 `&self` 借住 KeyDir，而删除与读值都要 `&mut kv`
        // ⇒ 一块一块来：块内只克隆 `(版本号, enc, 是不是软墓碑)`（至多 `VAC_CHUNK_ROWS` 行），
        // 出借用之后立刻把这一块判出来的行删掉、把保留行的 blob 引用当场读出来然后丢掉。
        // 于是 M36 之后剩下的那两笔 O(全库) 累加器（分段实测 ①=106.0 MB、93 B/扫描行）
        // 塌成 O(一块)。只有真有大 value 时 `blob_keys` 才随库涨 —— 那是 `rewrite_in_place`
        // 必须一次拿到全部存活 value 的结构，不是这一层的账（守卫：同一条四家守卫 + 分段台架）。
        //
        // 块间下界用「上一块最后一把 `enc ‖ 0x00`」：那是 enc 域里的**严格后继方向**
        // （`enc‖0x00 > enc`，且任何 `K > enc` 都满足 `K ≥ enc‖0x00`）⇒ 一行不漏、一行不重。
        // ⚠️ 反过来 `enc ‖ 0xFF×8`（[`crate::common::version_key_upper_bound`] 那种**右界**用法）
        // 会把"以 `enc` 为前缀的更长键"整把跳掉 —— 同一个坑在 `common::store::fold_lower_bound`
        // 的注释里已经写过一次，这里别再踩。
        // 只有全库轮次要把保留行读出来查 blob 引用（有界片不碰 blob）⇒ 才需要那条当场读的路
        let full_sweep = budget_rows == u64::MAX;
        let mut group: Vec<(u64, Vec<u8>, bool, ())> = Vec::new();
        let mut to_delete: Vec<Vec<u8>> = Vec::new();
        let mut kept: Vec<Vec<u8>> = Vec::new();
        let mut blob_keys: Vec<(Vec<u8>, Vec<u8>)> = Vec::new(); // (enc, 完整用户值) —— 只有大 value 才进
        let mut removed = 0usize;
        let mut scanned = 0u64;
        let mut cur_raw: Option<Vec<u8>> = None;
        let mut truncated = false;
        // 首块的下界是调用方给的 `after`（那是**逻辑键**域，靠下面那句显式跳过补齐），之后逐块前推
        let mut lower: Option<Vec<u8>> = after.map(|k| k.to_vec());
        loop {
            let mut chunk_rows = 0usize;
            let mut next_lower: Option<Vec<u8>> = None;
            let mut done = false;
            for (enc, loc) in kv.iter_meta_from(lower.as_deref()) {
                let Some(vk) = decode_key(enc) else {
                    continue;
                };
                // 下界落在 enc 域，而 `raw_key ≤ after` 的那些行（包括 `after` 自己那一整组）
                // 都还在范围内 —— 显式跳过，游标才是逻辑 key 域的。
                if after.is_some_and(|a| vk.raw_key.as_slice() <= a) {
                    continue;
                }
                let at_group_start = cur_raw.as_deref() != Some(vk.raw_key.as_slice());
                if at_group_start && scanned >= budget_rows {
                    // 破在这里时 `cur_raw` 那一组已经收齐（刚看到了它的后继）⇒ 游标就是它
                    truncated = true;
                    done = true;
                    break;
                }
                if at_group_start {
                    // 「留哪一版」是四引擎共用的判定，内核在 [`crate::common::vacuum_plan_group`]
                    let (d, k) = vacuum_plan_group(&mut group, xmin);
                    to_delete.extend(d);
                    if full_sweep {
                        kept.extend(k.into_iter().map(|(enc, _)| enc));
                    }
                    cur_raw = Some(vk.raw_key.clone());
                }
                scanned += 1;
                group.push((vk.version, enc.clone(), loc.is_soft_tombstone(), ()));
                chunk_rows += 1;
                if chunk_rows >= VAC_CHUNK_ROWS {
                    let mut bound = enc.clone();
                    bound.push(0u8);
                    next_lower = Some(bound);
                    break;
                }
            }
            // ⚠️ 走到这里 `for` 里那个按引用迭代 KeyDir 的迭代器已经 drop ⇒ 才拿得到 `&mut kv`
            if done || next_lower.is_none() {
                // 最后一块：把还挂在 `group` 里的那一组判掉（它不会再有后继了）
                let (d, k) = vacuum_plan_group(&mut group, xmin);
                to_delete.extend(d);
                if full_sweep {
                    kept.extend(k.into_iter().map(|(enc, _)| enc));
                }
            }
            #[cfg(test)]
            note_vac_pending(to_delete.len() + kept.len());
            for enc in to_delete.drain(..) {
                let _ = kv.remove(&enc);
                removed += 1;
            }
            if full_sweep {
                // 保留行的 blob 引用**当场读、读完就丢**：这是这一刀能不要 `keep` 那张清单的原因
                for enc in kept.drain(..) {
                    let Ok(Some(stored)) = kv.get_entry(&enc) else {
                        continue;
                    };
                    let Some(stored) = stored else {
                        continue; // 软墓碑没有 value
                    };
                    if stored.first() != Some(&VAL_TAG_BLOB) {
                        continue;
                    }
                    match decode_stored_value(&mut blob, &stored) {
                        Ok(Some(user)) => blob_keys.push((enc, user)),
                        Ok(None) => {}
                        Err(e) => {
                            log::warn!("blob decode 失败 key={enc:?}: {e}");
                        }
                    }
                }
            } else {
                kept.clear();
            }
            match next_lower {
                Some(l) if !done => lower = Some(l),
                _ => break,
            }
        }
        let next_cursor = if truncated { cur_raw } else { None };

        #[cfg(test)]
        {
            vac_base = crate::mem_probe::stage_mark(0, vac_base);
        }
        kv.flush()?;
        blob.sync()?;
        #[cfg(test)]
        {
            vac_base = crate::mem_probe::stage_mark(1, vac_base);
        }

        // Blob GC / 日志压缩都是**全库代价**，只在这一次调用本来就是全库扫描时才做。
        // 判据用 `budget_rows` 而不是「游标回到 None」：一个只有一把 key 的小库，任何
        // 有界轮次都会正好读到库尾 —— 用游标判断会让自动回收每轮都重写一遍库。
        // 也就是说：有界轮次只裁行，全库收尾（blob 重写 + merge）归手动 `vacuum()`。
        //
        // Blob GC：
        // - 无任何 blob 引用 → clear
        // - 仍有引用 → rewrite_in_place 只保留存活大 value，并更新 KeyDir 中的指针
        //
        // `blob_keys` 在上面那个按块的循环里就已经填好了（保留行**当场读、读完就丢**），
        // 所以这里不再有"整张存活行清单"，也不再有第二次全库 `iter()`。
        // ⚠️ `rewrite_in_place` 要一次拿到**全部**存活大 value —— 那是 blob 文件的结构，
        // 不是这一层能省掉的账（没有大 value 时这一段一分钱都不花，M37 分段实测 ③ = 0.0 MB）。
        let blob_rewritten = if !full_sweep {
            false
        } else if blob_keys.is_empty() {
            let old_len = blob.path().metadata().map(|m| m.len()).unwrap_or(0);
            if old_len > 8 {
                blob.clear_in_place()?;
                true
            } else {
                false
            }
        } else {
            let values: Vec<Vec<u8>> = blob_keys.iter().map(|(_, v)| v.clone()).collect();
            let new_offsets = blob.rewrite_in_place(&values)?;
            // 更新主日志里的 blob 指针（覆盖写同一 storage key）
            for ((enc, user), new_off) in blob_keys.into_iter().zip(new_offsets.into_iter()) {
                let mut stored = Vec::with_capacity(1 + 8 + 4);
                stored.push(VAL_TAG_BLOB);
                stored.extend_from_slice(&new_off.to_le_bytes());
                stored.extend_from_slice(&(user.len() as u32).to_le_bytes());
                let _ = kv.insert(enc, Some(stored));
            }
            true
        }
        ;

        // 压缩：丢掉已硬删除的空间（同样只在全库轮次）
        #[cfg(test)]
        {
            vac_base = crate::mem_probe::stage_mark(2, vac_base);
        }
        if full_sweep && (removed > 0 || blob_rewritten) {
            // M43：这一句的 Err 从此**不许再吞**。B1 之后 `merge()` 失败会把引擎留在"旧索引已
            // 腾掉、新索引还没扫完"的隔离态（此后每一扇能报错的门都报错，见 `Bitcask::usable`）；
            // 吞掉它，`vacuum()` 回 `Ok` 就是宣称"回收完成、库还好"。
            // 改前的形状里吞得起：那时 merge 要么全程不动 `key_dir`、要么一次性换成完整的新索引，
            // 失败也留不下半截状态。**这一处跟着改法是必须的，不是顺手清理。**
            kv.merge()?;
            #[cfg(test)]
            {
                let _ = crate::mem_probe::stage_mark(3, vac_base);
            }
        }

        Ok(VacuumRound {
            stats: VacuumStats {
                xmin,
                versions_removed: removed,
                blob_rewritten: Some(blob_rewritten),
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

/// 决定 open 后 next_version 的位置（旧库：WAL 兜底）。

fn scan_max_version(tree: &mut Bitcask) -> std::io::Result<u64> {
    #[cfg(test)]
    SCAN_MAX_CALLS.with(|c| c.set(c.get() + 1));
    let mut max_v = 0u64;
    // 只扫 KeyDir，不读 value
    for (enc, _) in tree.iter_meta() {
        if let Some(vk) = decode_key(enc) {
            max_v = max_v.max(vk.version);
        }
    }
    Ok(max_v)
}

impl BulkLoader {
    pub fn put(&mut self, key: &[u8], value: Vec<u8>) {
        let enc = encode_key(key, self.core.version);
        let stored = {
            let mut blob = self.blob.lock().unwrap();
            // bulk：不每条 fsync blob
            encode_stored_value(&mut blob, value, false).expect("blob encode")
        };
        let mut kv = self.kv.lock().unwrap();
        let _ = kv.insert(enc, Some(stored));
    }

    /// 批量 put：一次加锁插入多条
    pub fn put_batch(&mut self, items: &[(Vec<u8>, Vec<u8>)]) {
        let mut blob = self.blob.lock().unwrap();
        let mut kv = self.kv.lock().unwrap();
        let ver = self.core.version;
        let batch: Vec<(Vec<u8>, Option<Vec<u8>>)> = items
            .iter()
            .map(|(k, v)| {
                let enc = encode_key(k, ver);
                let stored = encode_stored_value(&mut blob, v.clone(), false).expect("blob");
                (enc, Some(stored))
            })
            .collect();
        let _ = kv.insert_batch(batch);
    }

    /// 零额外 clone 的批量 put（消费 items）— 压测主路径
    pub fn put_batch_owned(&mut self, items: Vec<(Vec<u8>, Vec<u8>)>) {
        let mut blob = self.blob.lock().unwrap();
        let mut kv = self.kv.lock().unwrap();
        let ver = self.core.version;
        let batch: Vec<(Vec<u8>, Option<Vec<u8>>)> = items
            .into_iter()
            .map(|(k, v)| {
                let enc = encode_key(&k, ver);
                let stored = encode_stored_value(&mut blob, v, false).expect("blob");
                (enc, Some(stored))
            })
            .collect();
        let _ = kv.insert_batch(batch);
    }

    pub fn delete(&mut self, key: &[u8]) {
        let enc = encode_key(key, self.core.version);
        let mut kv = self.kv.lock().unwrap();
        let _ = kv.insert(enc, None);
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut kv = self.kv.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();
        let low = encode_key(key, 0);
        let high = encode_key(key, u64::MAX);
        let mut best: Option<Vec<u8>> = None;
        if let Ok(range) = kv.range_scan(low, high) {
            for (enc, val) in range {
                if let Some(vk) = decode_key(&enc) {
                    if vk.raw_key.as_slice() == key && vk.version <= self.core.version {
                        best = match val {
                            None => None,
                            Some(stored) => decode_stored_value(&mut blob, &stored)
                                .ok()
                                .flatten()
                                .and_then(crate::bitcask::kv_ops::logical_to_user),
                        };
                    }
                }
            }
        }
        best
    }

    /// 中途 flush 写缓冲（不 fsync），降低峰值内存
    pub fn flush_buf(&mut self) {
        let mut kv = self.kv.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();
        let _ = kv.flush_buf();
        let _ = blob.sync();
    }

    pub fn finish(mut self) {
        self.finish_inner();
    }

    fn finish_inner(&mut self) {
        // 幂等守卫在内核里（`finish()` 与 `Drop` 都会调），收尾动作本身是 bitcask 自己的事
        if !self.core.first_finish() {
            return;
        }
        let mut kv = self.kv.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();
        let mut wal = self.wal.lock().unwrap();
        let _ = blob.sync();
        let _ = kv.flush();
        // bulk 结束后写 hint，避免下次 open 全量扫
        let _ = kv.dump_hint();
        let next_ver = self.core.next_version().load(Ordering::SeqCst);
        let _ = wal.append(WalRecord::Checkpoint {
            next_version: next_ver,
            root_page_id: 0,
            next_page_id: 0,
        });
        let _ = wal.sync();
        let _ = wal.truncate();
    }
}

impl Drop for BulkLoader {
    fn drop(&mut self) {
        self.finish_inner();
    }
}

/// 未终结事务的兜底回滚。
///
/// 「未终结」= 既没 `commit` 也没 `rollback` 就被丢弃（提前 `return`、panic、或纯粹忘了）。
/// 这不是便利功能，是止血：漏网的事务会**永久留在 `active_txn`**，于是
/// 1. `xmin = min(活跃事务 id)` 被一个已经死掉的 id 压住 ⇒ **版本回收从此永久停摆**；
/// 2. 之后每次 `begin` 的可见性快照 `keys().cloned().collect()` 随漏网数线性变大。
///
/// 判据用「活跃表里是否还有自己」而不是额外标志位：`commit`/`rollback` 都以 `remove` 收尾，
/// 所以成员关系本身就是「未终结」的充要条件。
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

impl Transaction {
    pub fn begin(
        kv: Arc<Mutex<Bitcask>>,
        blob: Arc<Mutex<BlobStore>>,
        wal: Arc<Mutex<Wal>>,
        active_txn: Arc<Mutex<Registry>>,
        next_version: Arc<AtomicU64>,
        wal_limit: Arc<AtomicU64>,
        vac: Arc<VacuumScheduler>,
    ) -> Self {
        // begin 不进 WAL：此刻还不知道这个事务会不会写。第一次 write() 才补 `Begin`
        // （只读事务因此一个字节都不落 —— 详见 [`crate::common::WalPolicy`]）
        let core = TxnCore::begin(active_txn, &next_version, vac);
        Transaction {
            kv,
            blob,
            wal,
            next_version,
            wal_limit,
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
        // 锁序：活跃表 → wal → blob → kv；那八步序列在 [`TxnCore::write`]（四引擎共用的一份）
        let mut reg = self.core.lock_registry();
        let mut wal = self.wal.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();
        let mut kv = self.kv.lock().unwrap();
        self.core.write(
            &mut reg,
            &mut TxnSide {
                wal: &mut wal,
                blob: &mut blob,
                kv: &mut kv,
            },
            key,
            value,
        )
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.latest_visible_raw(key)
            .and_then(|(_, v)| v)
            .and_then(crate::bitcask::kv_ops::logical_to_user)
    }

    /// 最新可见逻辑行（含 TTL 信封原始字节；tombstone 为 None）。
    /// 供 [`crate::kv_ops`] 使用，不剥用户 value。
    pub(crate) fn latest_visible_raw(
        &self,
        key: &[u8],
    ) -> Option<(u64, Option<Vec<u8>>)> {
        let mut kvengine = self.kv.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();
        // 从链尾往回走，第一个可见版本就是答案 —— 只对那一行读盘解码
        let enc = Self::find_version_back(&mut kvengine, key, |v| self.is_visible(v))?;
        let version = decode_key(&enc)?.version;
        // 外层 None：索引里没有这行；内层 None：软墓碑（逻辑值为 None）
        let stored = kvengine.get_entry(&enc).ok()??;
        let logical = match stored {
            None => None,
            Some(s) => decode_stored_value(&mut blob, &s).ok().flatten(),
        };
        Some((version, logical))
    }

    /// 取一组的值：KeyDir 里只有元数据 ⇒ 凭据是编码键，值要按位置回日志读
    /// （大 value 还要过 blob 旁路）。这正是"布局相关的那一半"，各家不同。
    fn read_row(&self, enc: &[u8]) -> crate::common::RowValue {
        use crate::common::RowValue;
        let mut kvengine = self.kv.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();
        match kvengine.get_entry(enc) {
            // 索引里查无此键 / IO 出错：与"墓碑"不是一回事，整行不参与结果
            Ok(None) | Err(_) => RowValue::Missing,
            Ok(Some(None)) => RowValue::Tombstone,
            Ok(Some(Some(stored))) => {
                match decode_stored_value(&mut blob, &stored).ok().flatten() {
                    Some(bytes) => RowValue::Value(bytes),
                    None => RowValue::Tombstone,
                }
            }
        }
    }

    /// 折出**候选组**（一把逻辑 key 一组，值还没读）。
    ///
    /// 只折键、不碰日志：KeyDir 里就是元数据，`iter_meta_from` 按 enc 升序 ⇒ 同一
    /// raw_key 的版本块连续，组内最后一次覆盖就是最大可见版本。值留到 `keep` 里读，
    /// 于是"命中几条读几条"，而不是"全库折完再筛"。
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
        // 键流走完了没：`for` 自然结束 = 走完；`break` 有两种 —— 越界(走完) 与 预算(没走完)
        let mut walk_done = true;
        {
            let kvengine = self.kv.lock().unwrap();
            let mut winners: BTreeMap<Vec<u8>, (u64, Vec<u8>)> = BTreeMap::new();
            let mut pending: Option<Vec<u8>> = None;
                for (enc, _loc) in kvengine.iter_meta_from(crate::common::fold_lower_bound(start, after)) {
                #[cfg(test)]
                crate::bitcask::bitcask::note_keydir_visit();
                if hi_enc.as_deref().is_some_and(|h| enc.as_slice() >= h) {
                    break; // 越界：键流在本引擎里升序，后面全在界外
                }
                let Some(vk) = decode_key(enc) else {
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
                winners.insert(vk.raw_key.clone(), (vk.version, enc.to_vec()));
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
            crate::bitcask::kv_ops::logical_to_user,
            &mut on,
        )
    }

    pub fn commit(&self) {
        self.commit_with_options(false);
    }

    pub fn commit_with_options(&self, flush_data: bool) {
        // 销账 + 回收记账 + WAL 收尾（`Commit` + fsync）在 [`TxnCore::commit`] 一份。
        // 收尾只交 wal 守卫：fsync 期间不许握着 kv/blob 把读者挡在门外。
        {
            let mut reg = self.core.lock_registry();
            let mut wal = self.wal.lock().unwrap();
            self.core.commit(&mut reg, &mut WalOnly { wal: &mut wal });
        }
        if flush_data {
            let mut kv = self.kv.lock().unwrap();
            let mut blob = self.blob.lock().unwrap();
            let _ = kv.flush();
            let _ = blob.sync();
        }

        // WAL 超阈值就顺带 checkpoint。放在提交之后是因为此刻本事务已持久、
        // 活跃表也已摘除，checkpoint 记到的 next_version 是干净的。
        let limit = self.wal_limit.load(Ordering::SeqCst);
        if limit > 0 {
            let size = self.wal.lock().unwrap().size_bytes().unwrap_or(0);
            if size >= limit {
                run_checkpoint(&self.kv, &self.blob, &self.wal, &self.next_version);
            }
        }
    }

    pub fn rollback(&self) {
        // 销账 + `Abort` + 按写集撤行，都在 [`TxnCore::rollback`]（锁序与 write 一致）
        let mut reg = self.core.lock_registry();
        let mut wal = self.wal.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();
        let mut kv = self.kv.lock().unwrap();
        self.core.rollback(
            &mut reg,
            &mut TxnSide {
                wal: &mut wal,
                blob: &mut blob,
                kv: &mut kv,
            },
        );
    }

    /// 部分撤销：批量写撞墙时把已落下的那几把 key 退回（连同写集一起摘掉）。
    pub(crate) fn undo(&self, keys: &[Vec<u8>]) {
        let mut reg = self.core.lock_registry();
        let mut wal = self.wal.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();
        let mut kv = self.kv.lock().unwrap();
        self.core.undo(
            &mut reg,
            &mut TxnSide {
                wal: &mut wal,
                blob: &mut blob,
                kv: &mut kv,
            },
            keys,
        );
    }

    fn is_visible(&self, version: u64) -> bool {
        // 判据在 [`crate::common::visible_at`]（四引擎共用的一份）
        self.core.is_visible(version)
    }

    /// 从链尾往回找本 key 第一个被 `accept` 的版本，返回它的版本键（原始 enc 字节）。
    ///
    /// 版本键是 `raw_key ‖ version.to_be_bytes()`，大端 ⇒ 同一 key 的所有版本在字节序里
    /// 相邻且按版本号升序，所以**链尾就是最新版本**：往回走第一个满足条件的即可停。
    ///
    /// `raw_key` 仍是 `key` 前缀的其它逻辑 key 会落进同一个字节区间，因此必须逐条核对
    /// `raw_key == key`（旧实现靠 `filter` 做这件事，这里保留同样的判定）。
    fn find_version_back(
        kv: &mut Bitcask,
        key: &[u8],
        accept: impl Fn(u64) -> bool,
    ) -> Option<Vec<u8>> {
        let low = encode_key(key, 0);
        let high = encode_key(key, u64::MAX);
        let mut budget = VERSION_BACK_STEP;
        loop {
            // 只取索引元数据，不碰 value；条数不足预算即说明已扫到区间下界
            let rows = kv.range_meta_back(&low, &high, budget);
            let reached_bottom = rows.len() < budget;
            for (enc, _) in rows {
                if let Some(vk) = decode_key(&enc) {
                    if vk.raw_key.as_slice() == key && accept(vk.version) {
                        return Some(enc);
                    }
                }
            }
            if reached_bottom {
                return None;
            }
            budget = budget.saturating_mul(2);
        }
    }

    fn latest_version_of(kv: &mut Bitcask, key: &[u8]) -> Option<u64> {
        Self::find_version_back(kv, key, |_| true)
            .and_then(|enc| decode_key(&enc).map(|vk| vk.version))
    }
}

/// 写侧：bitcask 的"字节住在哪"= 追加式数据日志（`insert` 会更新 keydir，且**会失败** ——
/// 那条 io::Result 通道原先只 `eprintln!` 一下就被丢掉，于是写集里留着一条库里没有的行；
/// 现在它经 [`WriteSide::put_versioned`] 的返回值交给内核，内核据此不登记写集）。
///
/// 用到 `encode_value` 槽：大 value 走 blob 旁路，而旁路在落 WAL **之前**就可能失败 ——
/// 失败必须在那里判掉，否则 WAL 里留下一条指向不存在 blob 的记录。
/// 没用 `row_fits`：日志没有页容量。
impl WriteSide for TxnSide<'_, '_, '_> {
    fn latest_version(&mut self, key: &[u8]) -> Option<u64> {
        Transaction::latest_version_of(self.kv, key)
    }

    fn put_versioned(&mut self, enc: &[u8], value: Option<Vec<u8>>) -> bool {
        match self.kv.insert(enc.to_vec(), value) {
            Ok(()) => true,
            Err(e) => {
                eprintln!("Bitcask insert 失败: {e}");
                false
            }
        }
    }

    fn remove_versioned(&mut self, enc: &[u8]) {
        let _ = self.kv.remove(enc);
    }

    fn encode_value(&mut self, value: Vec<u8>) -> Option<Vec<u8>> {
        // 事务路径每条都 fsync blob（崩了不能留下"已提交行指着不存在的字节"）
        match encode_stored_value(self.blob, value, true) {
            Ok(s) => Some(s),
            Err(e) => {
                eprintln!("blob encode 失败: {e}");
                None
            }
        }
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

/// 收尾两条：`Commit` + fsync、`Abort` + fsync。fsync 失败只喊出来（与改前一致）——
/// 事务此刻已从活跃表摘除、行也已落库，没有能回退的通道。
impl WalTail for TxnSide<'_, '_, '_> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bitcask::storage::{blob_path, data_path, hint_path, lock_path};

    fn tmp_db(tag: &str) -> PathBuf {
        crate::common::tmp_test_path("bitcask_mvcc", tag, "")
    }

    fn cleanup(path: &Path) {
        let _ = std::fs::remove_dir_all(path);
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(wal_path(path));
        let _ = std::fs::remove_file(lock_path(path));
        let _ = std::fs::remove_file(data_path(path));
        let _ = std::fs::remove_file(blob_path(path));
        let _ = std::fs::remove_file(hint_path(path));
    }

    /// 探针：一次点读 / 一次点写到底读了几行盘。
    ///
    /// 版本键 `raw_key ‖ version.to_be_bytes()` 大端有序 ⇒ 最新版本必然在链尾，
    /// 定位到它就够 —— 所以**读盘次数不该随链长增长**。
    /// 当前实现走 `scan_key_versions` → `range_scan` → 每行一次 `read_live`，
    /// 于是链长 N 就付 N 次 pread；点写更冤：`latest_version_of` 只要一个版本号，
    /// 却把 N 个 value 全读出来丢掉。
    ///
    /// 这条断言在当前代码上**必然失败**，失败信息里的两个数字就是这个 bug 的量级证明。
    #[test]
    fn test_point_op_cost_independent_of_chain_length() {
        fn probe(chain_len: u64) -> (u64, u64) {
            let path = tmp_db(&format!("probe_{chain_len}"));
            let (get_reads, set_reads);
            {
                let mvcc = MVCC::open(&path);
                for i in 0..chain_len {
                    let tx = mvcc.begin_transaction();
                    assert!(tx.set(b"hot", format!("v{i}").into_bytes()), "第 {i} 次写入被拒");
                    tx.commit();
                }

                let tx = mvcc.begin_transaction();
                let before = crate::bitcask::bitcask::read_value_calls();
                assert_eq!(tx.get(b"hot"), Some(format!("v{}", chain_len - 1).into_bytes()));
                get_reads = crate::bitcask::bitcask::read_value_calls() - before;

                let before = crate::bitcask::bitcask::read_value_calls();
                assert!(tx.set(b"hot", b"newer".to_vec()));
                set_reads = crate::bitcask::bitcask::read_value_calls() - before;
                tx.rollback();
            }
            cleanup(&path);
            (get_reads, set_reads)
        }

        let (g_short, s_short) = probe(50);
        let (g_long, s_long) = probe(2000);
        eprintln!(
            "点读读盘次数：链长 50 → {g_short} 次，链长 2000 → {g_long} 次\n\
             点写读盘次数：链长 50 → {s_short} 次，链长 2000 → {s_long} 次"
        );
        assert_eq!(g_short, g_long, "点读的读盘次数不应随版本链长度增长");
        assert_eq!(s_short, s_long, "点写的读盘次数不应随版本链长度增长（它只需要版本号）");
    }

    /// 长跑基线：**事故形状**（一把热点 key 被持续更新 + 稳定点读），自动回收开着 vs 关着。
    ///
    /// 跑法（release，几十秒）：
    /// ```text
    /// cargo test --release --lib bitcask::bitcask_mvcc::tests::test_long_run_baseline -- --ignored --nocapture
    /// ```
    ///
    /// 三条断言，一条比一条重要：
    /// 1. 开着自动回收 ⇒ 链长与总行数**不随 uptime 增长**（贴着切片宽度）—— I1 + I2。
    /// 2. 关着自动回收 ⇒ 链长必须涨到接近写入次数。这条是**反向证据**：同一份读代码、
    ///    同一个负载，只是不开回收，`max_chain` 就一路涨 ⇒ 说明这条守卫真的咬得到东西。
    /// 3. 两阶段的点读耗时**都不该随链长涨**（通道 ① 已断）。耗时只打印不断言 ——
    ///    在共享机器上做时间断言会 flaky；而"关掉回收后时间也不爆、只有行数涨"本身就是结论：
    ///    **点读成本与垃圾回收是两条独立通道**，修了一条不等于修了另一条。
    ///
    /// 实测出来的界（不是推出来的）：链的稳态上界是 **2 × 切片宽度**，不是 1 ×。
    /// 原因：一片的截断只能落在逻辑 key 边界，热点 key 那一组太大时会把这一整片吃满，
    /// 于是它被"轮到"的频率是每两小轮一次 ⇒ 平均攒到 1×、最坏 2×。默认片 16 Ki 行时
    /// 稳态链 ≲ 32 Ki —— 对 23.8 万版的现场来说是数量级的差距，但别误报成"贴着 1"。
    #[test]
    #[ignore = "长跑基线：手动 cargo test --release -- --ignored"]
    fn test_long_run_baseline() {
        use std::time::Instant;

        const N: u64 = 20_000;
        const SLICE: u64 = 2_048;
        const OTHER: u64 = 200;
        const READ_EVERY: u64 = 4;
        const READ_SAMPLES: usize = 250;

        fn run_workload(path: &Path, auto_slice: u64) -> (MvccHealth, f64, f64) {
            let mut reads: Vec<u128> = Vec::with_capacity((N / READ_EVERY) as usize + 1);
            let mvcc = MVCC::open(path);
            mvcc.set_vacuum_auto_slice(auto_slice);

            for k in 0..OTHER {
                let tx = mvcc.begin_transaction();
                assert!(tx.set(format!("other{k}").as_bytes(), b"v".to_vec()));
                tx.commit();
            }

            for i in 0..N {
                let tx = mvcc.begin_transaction();
                assert!(tx.set(b"hot", format!("v{i}").into_bytes()));
                tx.commit();
                if i % READ_EVERY == 0 {
                    let t0 = Instant::now();
                    let tx = mvcc.begin_transaction();
                    let hit = tx.get(b"hot");
                    tx.commit();
                    assert!(hit.is_some(), "第 {i} 次采样读不到热点 key");
                    reads.push(t0.elapsed().as_micros());
                }
            }
            let head = reads[..READ_SAMPLES].iter().sum::<u128>() as f64 / READ_SAMPLES as f64;
            let tail =
                reads[reads.len() - READ_SAMPLES..].iter().sum::<u128>() as f64 / READ_SAMPLES as f64;
            (mvcc.health(), head, tail)
        }

        let on_path = tmp_db("baseline_on");
        let off_path = tmp_db("baseline_off");

        let (h_on, head_on, tail_on) = run_workload(&on_path, SLICE);
        let (h_off, head_off, tail_off) = run_workload(&off_path, 0);

        println!("负载：{N} 次热点更新 + 每 {READ_EVERY} 次一次点读 + {OTHER} 把稳定 key");
        println!(
            "自动回收 开(片={SLICE})  max_chain={:>6}  total_rows={:>6}  点读均耗 前 {head_on:>7.1}µs / 后 {tail_on:>7.1}µs",
            h_on.max_chain, h_on.total_rows
        );
        println!(
            "自动回收 关             max_chain={:>6}  total_rows={:>6}  点读均耗 前 {head_off:>7.1}µs / 后 {tail_off:>7.1}µs",
            h_off.max_chain, h_off.total_rows
        );

        // 1) 开着：链与行数都贴住切片宽度（稳态上界 2×，理由见函数头），不随 2 万次更新增长
        assert!(
            h_on.max_chain <= 2 * SLICE + 2,
            "开着自动回收链却涨到 {} 行（界 {}）—— 回收没跟上产生速度",
            h_on.max_chain,
            2 * SLICE
        );
        assert!(
            h_on.total_rows <= 2 * SLICE + OTHER + 8,
            "开着自动回收总行数却到 {}",
            h_on.total_rows
        );
        assert_eq!(h_on.live_keys, OTHER + 1);
        // 2) 关着：链必须一路涨到接近写入次数（反向证据，守卫咬得到）
        assert!(
            h_off.max_chain >= N - 2,
            "关掉自动回收后链只有 {} 行？那这条基线什么都没测到",
            h_off.max_chain
        );
        // 3) 两者的行数差就是回收在干的事：实测比值 ≈ 5.0（N / 2×切片），
        //    界取 3× 留余量 —— 别把比值写成断言，跑一次没红不代表它对
        assert!(
            h_off.total_rows > 3 * h_on.total_rows,
            "同样负载，关回收才 {} 行、开回收 {} 行，差距不该这么小",
            h_off.total_rows,
            h_on.total_rows
        );

        cleanup(&on_path);
        cleanup(&off_path);

        // ── 只读事务的代价：读路径到底是不是写路径 ────────────────────────────
        //
        // 关掉自动回收免得混进别的变量，只看"一次只读事务花多少、往 WAL 落多少字节"。
        let ro_path = tmp_db("baseline_readonly");
        let (ro_us_per, wal_bytes_per) = {
            let mvcc = MVCC::open(&ro_path);
            mvcc.set_vacuum_auto_slice(0);
            let tx = mvcc.begin_transaction();
            assert!(tx.set(b"k", b"v".to_vec()));
            tx.commit();
            let wal = crate::bitcask::storage::wal_path(&ro_path);
            let before = std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0);
            let t0 = std::time::Instant::now();
            const R: u64 = 2_000;
            for _ in 0..R {
                let tx = mvcc.begin_transaction();
                assert!(tx.get(b"k").is_some());
                tx.commit();
            }
            let total = t0.elapsed().as_micros() as f64;
            let after = std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0);
            (total / R as f64, (after.saturating_sub(before)) as f64 / R as f64)
        };
        println!(
            "只读事务：{:>7.1}µs / 次，WAL 增长 {:>5.1} 字节 / 次",
            ro_us_per, wal_bytes_per
        );
        cleanup(&ro_path);
    }

    /// 泄漏事务（既不 commit 也不 rollback 就丢弃）**不得**永久卡死 vacuum 的水位线。
    ///
    /// 没有 `impl Drop for Transaction` 时，漏网的 xid 会一直挂在 `active_txn` 里，
    /// `xmin = min(活跃事务 id)` 因此永久停在它身上 —— 它之后的每一次写入都变成
    /// 「比 xmin 新、永远不能裁」，于是版本回收**整体停摆**，且之后每个事务的
    /// 可见性快照都多带一个死 id。
    ///
    /// 故意把泄漏放在造旧版本**之前**：这样漏网 id 是全场最小，`xmin` 被它钉死，
    /// 去掉 Drop 兜底这条测试就会红（顺序反过来则仍能裁掉，断言咬不住）。
    #[test]
    fn test_leaked_transaction_does_not_block_vacuum() {
        let path = tmp_db("leak_vacuum");
        {
            let mvcc = MVCC::open(&path);

            // 先泄漏：只 begin + 写，然后直接丢弃
            {
                let tx = mvcc.begin_transaction();
                assert!(tx.set(b"leak", b"x".to_vec()));
            }

            for i in 0..3u64 {
                let tx = mvcc.begin_transaction();
                assert!(tx.set(b"k", format!("v{i}").into_bytes()));
                tx.commit();
            }

            assert!(
                mvcc.active_txn.lock().unwrap().is_empty(),
                "泄漏事务应已被 Drop 回收，而不是永久留在活跃表里"
            );

            let stats = mvcc.vacuum().unwrap();
            assert!(
                stats.versions_removed > 0,
                "xmin 应能推进到 next_version 并裁掉 k 的旧版本；裁不动说明活跃表里有死 id 事务"
            );

            let tx = mvcc.begin_transaction();
            assert_eq!(tx.get(b"leak"), None, "泄漏事务的写入应已被回滚");
            assert_eq!(tx.get(b"k"), Some(b"v2".to_vec()));
            tx.rollback();
        }
        cleanup(&path);
    }

    /// 全库类 API（`print_all` / 扫描 / 搜索 / `exists` / `export` / `purge_expired` 都走
    /// `collect_latest_raw`）的读盘次数必须只与**存活 key 数**相关，与历史行数无关。
    ///
    /// 旧实现把每一行连 value 全读出来再挑最后一个，所以这里刻意让「历史行数」远大于
    /// 「存活 key 数」，这样断言才有区分力（旧实现会读 KEYS×VERSIONS 次）。
    #[test]
    fn test_collect_latest_reads_scale_with_live_keys_not_rows() {
        const KEYS: u64 = 8;
        const VERSIONS: u64 = 120;

        let path = tmp_db("collect_cost");
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

            let tx = mvcc.begin_transaction();
            let before = crate::bitcask::bitcask::read_value_calls();
            let all = tx.print_all();
            let used = crate::bitcask::bitcask::read_value_calls() - before;
            assert_eq!(all.len() as u64, KEYS, "存活 key 数应等于 {KEYS}");
            assert!(
                used <= KEYS + 4,
                "只应按存活 key 读盘：实际读 {} 次，而历史行数是 {}",
                used,
                KEYS * VERSIONS
            );
            tx.rollback();

            // 参照：旧实现走的就是「全行物化」（`Bitcask::iter` 每行连 value 读出来再挑），
            // 这条原语现在还在，所以「改前」是实测而不是推断。
            let before = crate::bitcask::bitcask::read_value_calls();
            let rows = mvcc.kv.lock().unwrap().iter().unwrap().len() as u64;
            let old_style = crate::bitcask::bitcask::read_value_calls() - before;
            eprintln!(
                "全库折叠读盘：新实现 {used} 次 vs 旧路径（全行物化）{old_style} 次，历史行数 {rows}"
            );
            assert!(
                used * 10 <= old_style,
                "折叠读盘应比全行物化便宜一个量级以上（{used} vs {old_style}）"
            );
        }
        cleanup(&path);
    }

    /// **窗口 scan 的代价必须随窗口宽度缩，不随全库缩。**
    ///
    /// 旧形状是 `collect_latest_raw()` 折完**全库**再由调用方逐条筛界 ⇒ 10 行的窗口和
    /// 2000 行的全量读盘次数一样（大库基线实测：窗口 scan 376.6 µs/行 vs 全量 3.8 µs/行）。
    /// 现在界下推进折叠，这条用例同时钉两件事：① 结果与"全量之后筛"逐条一致（界判得对），
    /// ② 读盘次数随窗口缩（不是又对又慢，也不是快了但筛错）。
    /// `scan_foreach` 提前叫停时，**不许先把整库折一遍**（M27）。
    ///
    /// 这条路径的文档从写下那天起就声称「不收集全库到单一大 Vec」，而实现是
    /// `for … in collect_latest_raw_in(...)` —— 那一句会把界内每一行都先折出来，再逐条搬成
    /// `ExportRecord`。200k 键实测因此比结果集本身还多占 ≈ 15 MB（`一次全量 scan`
    /// 峰值 +45.5 MB / 常驻 30.7 MB）。现在走共用件 `fold_rows_streaming`：折一行交一行，
    /// 叫停之后的键不再碰。
    ///
    /// 判据用**取值次数**（本引擎那把尺子）：交 1 行只该取 1 次值，老写法是 N 次 ——
    /// 差三个数量级，不必按各家单位调预算。
    #[test]
    fn test_scan_foreach_stops_without_folding_the_library() {
        const N: usize = 2000;
        fn ruler() -> u64 {
            crate::bitcask::bitcask::read_value_calls()
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
            assert!(full >= N as u64, "取值尺子在空转（全量只取 {full} 次）");
            assert!(
                early <= 2,
                "只要 1 行却取了 {early} 次值（老写法：先把全库 {N} 行折出来）"
            );
            eprintln!("[scan_foreach 早停] 交 1 行付 {early}，全量 {N} 行付 {full}");
        }
        cleanup(&path);
    }

    #[test]
    fn test_windowed_scan_cost_scales_with_window_not_library() {
        const KEYS: u64 = 2000;

        let path = tmp_db("window_cost");
        {
            let mvcc = MVCC::open(&path);
            for i in 0..KEYS {
                let tx = mvcc.begin_transaction();
                assert!(tx.set(format!("k{i:05}").as_bytes(), format!("v{i}").into_bytes()));
                tx.commit();
            }

            let tx = mvcc.begin_transaction();
            let before = crate::bitcask::bitcask::read_value_calls();
            let all = tx.scan(None, None);
            let full = crate::bitcask::bitcask::read_value_calls() - before;

            let lo = b"k01000".to_vec();
            let hi = b"k01010".to_vec();
            let before = crate::bitcask::bitcask::read_value_calls();
            let win = tx.scan(Some(&lo), Some(&hi));
            let windowed = crate::bitcask::bitcask::read_value_calls() - before;

            assert_eq!(all.len() as u64, KEYS, "全量应折出 {KEYS} 行");
            assert_eq!(win.len(), 10, "[k01000, k01010) 应给 10 行");
            // 界判对了才算数：与"全量之后筛"逐条一致
            let want: Vec<Vec<u8>> = all
                .iter()
                .filter(|r| r.key.as_slice() >= lo.as_slice() && r.key.as_slice() < hi.as_slice())
                .map(|r| r.key.clone())
                .collect();
            let got: Vec<Vec<u8>> = win.iter().map(|r| r.key.clone()).collect();
            assert_eq!(got, want, "下推界之后窗口结果必须与全量筛一致");

            // 代价：窗口只有全库的 0.5%
            assert!(
                windowed <= 20,
                "10 行窗口的读盘次数应随窗口缩（实际 {windowed}，全量 {full}）"
            );
            assert!(
                windowed * 10 <= full,
                "窗口代价不该是全库量级（{windowed} vs 全量 {full}）"
            );

            // 前缀同理（twosync 那种"按路径前缀取一段"的读法）：k001xx ⇒ 100 个键
            let before = crate::bitcask::bitcask::read_value_calls();
            let pre = tx.prefix_scan(b"k001");
            let prefixed = crate::bitcask::bitcask::read_value_calls() - before;
            assert_eq!(pre.len(), 100, "前缀 k001 应命中 100 行");
            assert!(
                prefixed <= 120,
                "前缀扫的读盘应随前缀命中等同宽度（实际 {prefixed}，全量 {full}）"
            );

            // 边界形状：前缀是**所有键的真前缀**（比第一个键还短 ⇒ seek 落不到任何"键 ≥ from"
            // 的位置）。lsm 在复测表里把这种前缀扫成空结果，四家都钉一遍。
            let whole = tx.prefix_scan(b"k");
            assert_eq!(whole.len() as u64, KEYS, "前缀 k 应命中全部 {KEYS} 行");
            assert_eq!(tx.key_count(b"k") as u64, KEYS, "key_count 同一个形状");

            // 上界哨兵的陷阱形状：`seek_prev` 造出来的界是 `key ‖ 0x00`，若哨兵取
            // `encode_key(end, 0)` 就会把 `key` 自己的版本行判成越界 —— 表现为
            // `seek_prev(k)` 返回比 k 小的那一版。内存引擎先撞到，四家现在共用一个哨兵函数。
            assert_eq!(tx.seek_prev(b"k01005").unwrap().key, b"k01005");
            assert_eq!(tx.seek(b"k01000").unwrap().key, b"k01000");
            assert_eq!(tx.seek_prev(b"k01000").unwrap().key, b"k01000");
            tx.rollback();
        }
        cleanup(&path);
    }

    /// WAL 阈值触发必须真的把文件截住（对照「关闭自动触发」），且截断之后重开仍能读到
    /// 全部已提交数据 —— 后半段守的是「先 flush 数据、再截 WAL」这个顺序：
    /// 顺序反了就是**丢已提交写入**。
    #[test]
    fn test_wal_auto_checkpoint_bounds_file_and_keeps_data() {
        const WRITES: u32 = 40;

        fn wal_bytes_after(tag: &str, limit: u64) -> u64 {
            let path = tmp_db(tag);
            {
                let mvcc = MVCC::open(&path);
                mvcc.set_wal_checkpoint_limit(limit);
                for i in 0..WRITES {
                    let key = format!("k{i}");
                    let tx = mvcc.begin_transaction();
                    assert!(tx.set(key.as_bytes(), vec![b'x'; 200]));
                    tx.commit();
                }
            }
            let size = std::fs::metadata(wal_path(&path)).map(|m| m.len()).unwrap_or(0);
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

    /// 冷启动三级优先：WAL 里有 Checkpoint 时**不该**再做 O(总行数) 的全库 KeyDir 扫描。
    ///
    /// 守的是「启动一次比一次慢」那条通道：`scan_max_version` 随总行数线性，而
    /// `checkpoint()` 早已把 `next_version` 写进 WAL（且 `try_open` 收尾也会补一条），
    /// 之前却完全没读它。这里用调用计数证明快路真的走到了，而不只是代码看起来对。
    #[test]
    fn test_reopen_uses_checkpoint_instead_of_full_scan() {
        const KEYS: u32 = 60;
        let path = tmp_db("ckpt_start_ver");
        let before;
        {
            let mvcc = MVCC::open(&path);
            mvcc.set_wal_checkpoint_limit(0); // 关掉自动触发，本用例只测手动 checkpoint
            for i in 0..KEYS {
                let key = format!("k{i}");
                let tx = mvcc.begin_transaction();
                assert!(tx.set(key.as_bytes(), format!("v{i}").into_bytes()));
                tx.commit();
            }
            before = mvcc.next_version.load(Ordering::SeqCst);
            mvcc.checkpoint();
        }

        {
            let base = scan_max_calls();
            let mvcc = MVCC::open(&path);
            assert_eq!(
                scan_max_calls() - base,
                0,
                "有 Checkpoint 时重开不应再全库扫 KeyDir"
            );
            let after = mvcc.next_version.load(Ordering::SeqCst);
            assert!(after >= before, "重开后 next_version 不应回退（{after} < {before}）");
            let tx = mvcc.begin_transaction();
            for i in 0..KEYS {
                let key = format!("k{i}");
                assert_eq!(tx.get(key.as_bytes()), Some(format!("v{i}").into_bytes()));
            }
            tx.commit();
        }
        cleanup(&path);
    }

    /// 复现「旧水位线会裁掉活跃事务快照仍需要的版本」⇒ **静默读到错值**。
    ///
    /// 形状：`hot` 有 v1、v2 两版。B 提交 v2 **之前** T1 已经 begin ⇒ v2 落在 T1 的
    /// 并发快照里、对 T1 永久不可见 ⇒ T1 只能回退到 v1。而此时
    /// `xmin = min(活跃事务 id) = T1.id`，「最后一个 < xmin」的版本正好是 v2，
    /// 于是按规则 v1 被裁掉 —— T1 再读就成了 None。
    ///
    /// 这就是 12e3696 想修、当天又被 f78edbb revert 回来的那个洞。revert 信息里
    /// 承认「旧水位线的已知问题随之回来了」，但仓库里没有一条测试钉住它 ——
    /// 缺的就是这条。
    #[test]
    fn test_vacuum_keeps_versions_needed_by_active_txn() {
        let path = tmp_db("vacuum_active");
        {
            let mvcc = MVCC::open(&path);

            let t0 = mvcc.begin_transaction();
            assert!(t0.set(b"hot", b"v1".to_vec()));
            t0.commit();

            let b = mvcc.begin_transaction();
            assert!(b.set(b"hot", b"v2".to_vec()));
            let t1 = mvcc.begin_transaction(); // T1 的快照里有 b
            b.commit(); // v2 提交了，但对 T1 永久不可见

            assert_eq!(
                t1.get(b"hot"),
                Some(b"v1".to_vec()),
                "读前提：T1 应当看到 v1（v2 在它的并发快照里）"
            );

            let stats = mvcc.vacuum().unwrap();
            assert!(
                stats.skipped_active_txn,
                "有活跃事务时 vacuum 应整轮跳过，而不是按 min(活跃 id) 去裁版本"
            );
            assert_eq!(stats.versions_removed, 0, "跳过的那一轮不该裁任何东西");
            assert_eq!(
                t1.get(b"hot"),
                Some(b"v1".to_vec()),
                "vacuum 不得裁掉活跃事务快照仍需要的版本（裁错 = 静默读到旧值/None）；本次 xmin={}",
                stats.xmin
            );

            t1.rollback();

            // T1 结束之后再 vacuum：这次必须真的能裁（否则「整轮跳过」会变成永久不回收）
            let stats = mvcc.vacuum().unwrap();
            assert!(
                !stats.skipped_active_txn,
                "没有活跃事务时不该再跳过，否则版本链永远裁不掉"
            );
            assert!(
                stats.versions_removed > 0,
                "hot 的旧版本这次应被裁掉，实测 {} 次",
                stats.versions_removed
            );
            let t2 = mvcc.begin_transaction();
            assert_eq!(t2.get(b"hot"), Some(b"v2".to_vec()), "裁完应读到最新版本");
            t2.rollback();
        }
        cleanup(&path);
    }

    /// 长跑进程不喊 vacuum，版本链也不能无限涨（I2：垃圾必须自己回来）。
    ///
    /// 事故那条曲线的本质就是版本行只增不减 —— 一天/一个多月里 CPU 慢慢爬。
    /// 这里把切片宽度调到 8 行，写 200 版热点 key，断言：
    /// 1. 没人叫过 vacuum，链也被压住（自动回收到位在跑）；
    /// 2. 最新值始终读得到 —— 自动裁的是旧版本，不是可见版本；
    /// 3. 默认是**开着**的（关掉它就等于把这次事故原样留给下一个项目）。
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
    ///
    /// 有了它就允许上层把回收完全交给自己调度（比如只在交易空档跑），
    /// 也保证默认值不是「悄悄多做了一件事」。
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

    /// 有界回收：每轮只碰一小段，多轮跑完必须和一次全库 vacuum 得到**同样**的结果。
    ///
    /// `vacuum()` 把 kv 锁按到整库扫完 —— 单核机器上「开自动回收」就会在回收那一刻
    /// 制造新事故。切片后每轮读的行数有上限（偏差 = 一条链的长度：截断只能落在逻辑 key
    /// 边界，半条链没法判「留哪一版」），而游标每轮必须真的前进（`rounds` 的上界就是这条）。
    ///
    /// 键里刻意放了前缀嵌套的 `k1` / `k1x`：版本键是 `raw ‖ 8B`，真前缀会让两把 key 的
    /// 版本区间互相穿插，切片最容易在这里切错。
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
            }
        }

        // 对照组：一次全库 vacuum 裁掉多少
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
                assert!(round.stats.versions_removed > 0, "第 {rounds} 轮必须裁掉东西，否则游标没前进");
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

    /// 一次全库 `vacuum()` 的瞬时内存分在哪一段（M37 的先量，`#[ignore]`）。
    ///
    /// 为什么要拆：修好量具（M38）之后 600k 档 `vacuum()` 读到 **171.4 MB**，而这一段里叠着
    /// 四件事 —— ①归组判定、②删除 + `flush` + blob `sync`、③blob GC（读存活行 +
    /// `rewrite_in_place`）、④`kv.merge()` 重写主日志。"存活行清单"是①里那块，但 M30/M31
    /// 两次的教训都是**最合理的那件事不是元凶**，所以先分段再决定这一刀砍哪里。
    /// 每段之前都 `reset_peak()` ⇒ 报的是**这一段自己**的瞬时峰值，不是进程历史最高点。
    ///
    /// ```text
    /// BITCASK_VAC_ROWS=200000 cargo test --release --lib -- --ignored --nocapture \
    ///     --test-threads=1 bitcask_vacuum_stage_split
    /// ```
    #[test]
    #[ignore = "vacuum 分段内存台架，手动 release 单线程跑：BITCASK_VAC_ROWS=200000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 bitcask_vacuum_stage_split"]
    fn bitcask_vacuum_stage_split() {
        use std::time::Instant;
        let rows: u64 = std::env::var("BITCASK_VAC_ROWS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(200_000);
        let mb = |b: u64| b as f64 / 1024.0 / 1024.0;
        let path = tmp_db("vac_stages");
        {
            let mvcc = MVCC::open(&path);
            // 每把键两个版本 ⇒ 第二趟写完之后第一版全都 < xmin，全部可裁
            for round in 0..2u64 {
                let tx = mvcc.begin_transaction();
                for i in 0..rows {
                    let k = format!("{i:08}#/worldquant/datafields/x.json");
                    assert!(tx.set(k.as_bytes(), format!("round{round}").into_bytes()), "第 {i} 把键写失败");
                }
                tx.commit();
            }
            let t = Instant::now();
            let st = mvcc.vacuum().unwrap();
            let d = t.elapsed();
            let p = crate::mem_probe::stage_peaks();
            println!(
                "=== bitcask vacuum 分段瞬时峰值 rows={rows} 裁掉 {} 行 / 扫 {} 行，总耗时 {:.2} s ===",
                st.versions_removed,
                st.rows_scanned,
                d.as_secs_f64()
            );
            println!(
                "  ① 归组判定 {:>8.1} MB ｜ ② 删除+flush+sync {:>8.1} MB ｜ ③ blob GC {:>8.1} MB ｜ ④ merge {:>8.1} MB",
                mb(p[0]),
                mb(p[1]),
                mb(p[2]),
                mb(p[3])
            );
            println!(
                "  合计（分段各自峰值，非同时成立）{:>8.1} MB ｜ 每扫描行摊 ①={:.0} B ②={:.0} B ③={:.0} B ④={:.0} B",
                mb(p[0] + p[1] + p[2] + p[3]),
                p[0] as f64 / st.rows_scanned.max(1) as f64,
                p[1] as f64 / st.rows_scanned.max(1) as f64,
                p[2] as f64 / st.rows_scanned.max(1) as f64,
                p[3] as f64 / st.rows_scanned.max(1) as f64,
            );
        }
        cleanup(&path);
    }

    /// 每笔"单行事务"在 WAL 里到底留下多少字节、几条记录（`#[ignore]`，手动 release 跑）。
    ///
    /// 存在的理由有两个：
    /// 1. **M25 的代价数字**。交易进程的常态是"一笔事务就改一两把键"，所以 WAL 的**每条记录**
    ///    都要算钱：`wal_checkpoint_bytes` 那个阈值什么时候到、恢复要重放多少条，都按记录数走。
    /// 2. 挑 WAL 收口阈值的人需要"多少笔事务撑满一 MiB"这种换算，而不是只有一个总字节数。
    ///
    /// ```text
    /// WAL_COST_TXNS=20000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 wal_cost_per_single_write_txn
    /// ```
    #[test]
    #[ignore = "WAL 单价台架，手动 release 单线程跑：WAL_COST_TXNS=20000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 wal_cost_per_single_write_txn"]
    fn wal_cost_per_single_write_txn() {
        let txns: u64 = std::env::var("WAL_COST_TXNS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(20000);
        let path = tmp_db("wal_cost");
        cleanup(&path);
        std::fs::create_dir_all(&path).unwrap();
        let bytes;
        let counts;
        {
            let mvcc = MVCC::open(&path);
            let wal = crate::bitcask::storage::wal_path(&path);
            // 基线从**空 WAL** 起算：`checkpoint()` 会把 WAL 截断并把 `Checkpoint` 写成第一条，
            // 所以先写一笔、再 checkpoint，之后量到的字节就只剩"每笔事务的价"。
            {
                let tx = mvcc.begin_transaction();
                assert!(tx.set(b"seed", b"v0".to_vec()));
                tx.commit();
            }
            mvcc.checkpoint();
            let base = std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0);
            for i in 0..txns {
                let tx = mvcc.begin_transaction();
                assert!(tx.set(b"hot", format!("v{i}").into_bytes()));
                tx.commit();
            }
            bytes = std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0) - base;
            let mut reader = Wal::open(&wal).expect("打开 WAL 读记录");
            let records = reader.read_all().expect("读 WAL");
            let of = |kind: u8| {
                records
                    .iter()
                    .filter(|e| {
                        let tag = match &e.record {
                            WalRecord::Begin { .. } => 0,
                            WalRecord::Write { .. } => 1,
                            WalRecord::Commit { .. } => 2,
                            WalRecord::Abort { .. } => 3,
                            WalRecord::Checkpoint { .. } => 4,
                        };
                        tag == kind
                    })
                    .count()
            };
            counts = (of(0), of(1), of(2), records.len());
        }
        eprintln!(
            "[WAL 单价] {txns} 笔单行事务：{} B 合计 / {:.1} B 每笔 —— \
             Begin {} 条、Write {} 条、Commit {} 条、总 {} 条（每笔 {:.2} 条）",
            bytes,
            bytes as f64 / txns as f64,
            counts.0,
            counts.1,
            counts.2,
            counts.3,
            counts.3 as f64 / txns as f64
        );
        assert_eq!(counts.1, txns as usize, "每笔写必须留下一条 Write");
        assert_eq!(counts.2, txns as usize, "每笔写必须留下一条 Commit");
        assert_eq!(counts.0, 0, "M25 起写侧不再落 Begin");
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

            let wal = crate::bitcask::storage::wal_path(&path);
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
            // 这种形状照样绿（实测把 `wal_write` 改成不 append 直接返回成功，本函数一路通过）。
            // 判据必须精确到记录类型 —— 一次写要在 WAL 里留下一条 `Write`。
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
                // M25：写侧不再落 `Begin` —— 恢复要的信息全在 Write/Commit/Abort 里，
                // 而"只留一条 Begin 就崩了"的那种事务本来一行都没写。
                // ⚠️ 这条断言守的是"别再写它"；老日志**还能读**那半边由
                // `plan_is_identical_with_or_without_begin` 钉住，两边缺一条都是洞。
                let begins = records
                    .iter()
                    .filter(|e| matches!(&e.record, WalRecord::Begin { .. }))
                    .count();
                assert_eq!(begins, 0, "写侧不再落 Begin，实际留下 {begins} 条");
            }

            let tx = mvcc.begin_transaction();
            assert_eq!(tx.get(b"k"), Some(b"w".to_vec()));
            tx.commit();
        }
        cleanup(&path);
    }

    /// 健康度快照必须如实报出「最长版本链」。
    ///
    /// 这次事故的核心数字（某把热点 key 堆了多少个版本）一直是靠
    /// `grep data.log | uniq -c` 数出来的 —— 一个当基座用的库不该靠这个诊断自己。
    /// 顺带验证 vacuum 之后能看得见「链被压平」，那正是将来自动回收要盯的指标。
    #[test]
    fn test_health_reports_chain_length() {
        const CHAIN: u64 = 500;

        let path = tmp_db("health");
        {
            let mvcc = MVCC::open(&path);
            for i in 0..CHAIN {
                let tx = mvcc.begin_transaction();
                assert!(tx.set(b"hot", format!("v{i}").into_bytes()));
                tx.commit();
            }
            for k in 0..3u64 {
                let key = format!("other{k}");
                let tx = mvcc.begin_transaction();
                assert!(tx.set(key.as_bytes(), b"x".to_vec()));
                tx.commit();
            }

            let h = mvcc.health();
            assert_eq!(h.live_keys, 4, "逻辑 key 数");
            assert_eq!(h.total_rows, CHAIN + 3, "物理行数（含历史版本）");
            assert_eq!(h.max_chain, CHAIN, "最长版本链应由库自己报出来");
            assert_eq!(h.rows_above_one, CHAIN - 1);
            assert_eq!(h.active_txns, 0);

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

    /// vacuum 自己不该是全库最重的路径：读盘次数必须只与**留下来的行数**相关。
    ///
    /// 旧实现里 vacuum 有两次 `kv.iter()`（每一行连 value 全读出来）。也就是说在
    /// 「链已经涨到几十万行」的库上跑 vacuum，那一刻本身就是一次事故 ——
    /// 这正是「先让 vacuum 变轻，再谈自动触发」这个施工顺序的理由。
    #[test]
    fn test_vacuum_does_not_read_every_version() {
        const CHAIN: u64 = 400;
        const KEYS: u64 = 5;

        let path = tmp_db("vacuum_cost");
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
            assert_eq!(mvcc.health().total_rows, CHAIN * KEYS);

            let before = crate::bitcask::bitcask::read_value_calls();
            let stats = mvcc.vacuum().unwrap();
            let used = crate::bitcask::bitcask::read_value_calls() - before;
            eprintln!(
                "vacuum 读盘次数：{used} 次（裁掉 {} 行，存活 key {KEYS} 个）",
                stats.versions_removed
            );
            assert_eq!(stats.versions_removed, ((CHAIN as usize - 1) * (KEYS as usize)) as usize);
            // 界取 2×存活行数：每个留下来的行最多被读两次 —— 一次查 blob 引用，
            // 一次是 merge 重写日志时把它搬走（那是 merge 的本职，不是 vacuum 的浪费）。
            // 关键是它只与**存活行数**相关；未修前这里是 2000 次（每行连 value 全读）。
            assert!(
                used <= 2 * KEYS + 4,
                "vacuum 只该读留下来的那些行：实际读盘 {used} 次，而历史行数是 {}",
                CHAIN * KEYS
            );
            assert_eq!(mvcc.health().max_chain, 1, "裁完最长链应降到 1");
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
    fn test_wal_redo_after_commit_without_data_flush() {
        let path = tmp_db("redo");
        {
            cleanup(&path);
            std::fs::create_dir_all(&path).unwrap();
            // 空数据文件
            let mut tree = Bitcask::new(data_path(&path)).unwrap();
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

    #[test]
    fn test_wal_undo_uncommitted() {
        let path = tmp_db("undo");
        {
            {
                let mvcc = MVCC::open(&path);
                drop(mvcc);
            }
            // 模拟：脏写入已进数据文件，但 WAL 只有 Begin+Write 无 Commit
            let mut wal = Wal::open(wal_path(&path)).unwrap();
            let xid = 1u64;
            {
                let mut tree = Bitcask::new(data_path(&path)).unwrap();
                let enc = encode_key(b"ghost", xid);
                tree.insert(enc.clone(), Some(b"should-vanish".to_vec()))
                    .unwrap();
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

    /// `vacuum()` 必须**按块**删除，不许把整库的待删行攒在一个 `Vec` 里（M37）。
    ///
    /// 判据（单位：行）：库里可删的旧版**远多于一块**时，单次 flush 前最多攒的待处理行数
    /// `vac_pending_max()` 必须 ≤ `2 × VAC_CHUNK_ROWS`。
    /// 为什么不钉内存而钉这个数：分配计数（`mem_probe`）是**进程级**的，并行跑用例时别家分配的
    /// 字节会算进来（M27 栽过、M36 又拿坏量具的读数当过凭据）；而"块界有没有生效"是个纯计数。
    /// `versions_removed > VAC_CHUNK_ROWS` 那条断言保证界**真的被跨过** —— 一个小库任何一片都
    /// 装得下，那样这条判据就是空转（M33 的守卫第一次就红在这种空转上）。
    ///
    /// 变异验证：把 `to_delete.drain(..)` 从块循环里挪到"整库走完之后" ⇒
    /// `vac_pending_max()` 变成全部可删行数（20 000 > 界 16 384），红。
    #[test]
    fn test_vacuum_deletes_in_bounded_chunks() {
        const KEYS: usize = 20_000;
        let path = tmp_db("vac_chunks");
        {
            let mvcc = MVCC::open(&path);
            // 自动回收会在提交那一刻就先裁掉一片，那这条判据就量不到手动 vacuum 的界了
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
                "[vac chunks] 裁掉 {} 行 / 扫 {} 行 ⇒ 单次 flush 前最多攒 {} 行（块界 {VAC_CHUNK_ROWS}）",
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
    fn test_large_value_blob_roundtrip() {
        let path = tmp_db("blob");
        let big = vec![0xABu8; 10_000]; // > BLOB_THRESHOLD
        {
            let mvcc = MVCC::open(&path);
            let tx = mvcc.begin_transaction();
            assert!(tx.set(b"big", big.clone()));
            assert!(tx.set(b"small", b"hi".to_vec()));
            assert_eq!(tx.get(b"big"), Some(big.clone()));
            assert_eq!(tx.get(b"small"), Some(b"hi".to_vec()));
            tx.commit();
            mvcc.flush();
        }
        {
            let mvcc = MVCC::open(&path);
            let tx = mvcc.begin_transaction();
            assert_eq!(tx.get(b"big"), Some(big));
            assert_eq!(tx.get(b"small"), Some(b"hi".to_vec()));
            tx.commit();
        }
        let blob = blob_path(&path);
        assert!(blob.exists());
        assert!(std::fs::metadata(&blob).unwrap().len() > 8);
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
}

impl crate::common::VersionedStore for Transaction {
    /// KeyDir 只有元数据 ⇒ 凭据是编码键，值在日志里另一处。
    type Row = Vec<u8>;

    fn scan_candidates(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        accept: Option<&dyn Fn(&[u8]) -> bool>,
        after: Option<&[u8]>,
        budget: usize,
    ) -> (Vec<crate::common::WinCandidate<Vec<u8>>>, bool) {
        // 走的是本类型上的同名 inherent 方法（inherent 优先于 trait ⇒ 不会自我递归）
        self.scan_candidates(start, end, accept, after, budget)
    }

    fn materialize(&self, row: Vec<u8>) -> crate::common::RowValue {
        self.read_row(&row)
    }
}
