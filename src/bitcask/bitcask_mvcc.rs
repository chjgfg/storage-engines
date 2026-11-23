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
//! - 大 value（> [`crate::bitcask::storage::BLOB_THRESHOLD`]）外置到 `data.blob`，主日志只存指针
//! - Checkpoint 的 `root_page_id` / `next_page_id` 固定写 0
//! - 数据目录：`dir/data.log` + `dir/data.blob` + `dir/bitcask.wal` + `dir/bitcask.lock`
//! - 版本行 tombstone 用 Bitcask **软 tombstone**（KeyDir 保留）；rollback/vacuum 用 **硬删除**
//! - bulk 路径：无 WAL + 4MB 写缓冲 + insert_batch，目标 10M / 10min

use std::eprintln;
use std::format;
#[cfg(test)]
use std::thread_local;
use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use crate::bitcask::wal::{Wal, WalRecord, analyze_recovery};
use crate::bitcask::{
    Bitcask,
    storage::{
        BlobStore, FileLock, VAL_TAG_BLOB, data_path, decode_stored_value, encode_stored_value,
        lock_path, wal_path,
    },
};
use crate::common::VacuumScheduler;
use crate::common::txn::{BulkCore, Registry, TxnCore, WalTail, WriteSide};
use crate::common::version::vacuum_drain_group;
pub use crate::common::{
    DurabilityProfile, EngineTuning, ExportRecord, MvccHealth, VacuumRound, VacuumStats,
};

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
pub use crate::common::{VersionedKey, decode_key, encode_key};

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

// 测试专用：本线程调用 `scan_max_version`（O(总行数) 的全库 KeyDir 扫描）的次数。
// 用来守住冷启动的三级优先：有 Checkpoint 时就不该再全库扫。
#[cfg(test)]
thread_local! {
    static SCAN_MAX_CALLS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) mod vacuum_cost_reference;

// 按职责组织的内部实现；公开类型与原模块路径保持一致。
mod bulk;
mod export;
mod health;
mod lifecycle;
#[cfg(test)]
mod probes;
mod recovery;
mod search;
mod transaction;
mod vacuum;
#[cfg(test)]
use probes::note_vac_pending;
#[cfg(test)]
pub(crate) use probes::reset_vac_pending;
#[cfg(test)]
pub(crate) use probes::scan_max_calls;
#[cfg(test)]
pub(crate) use probes::vac_pending_max;
use recovery::run_checkpoint;
use recovery::scan_max_version;
use recovery::seal_wal_checkpoint;
