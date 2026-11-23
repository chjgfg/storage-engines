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

#[cfg(test)]
use std::eprintln;
use std::format;
#[cfg(test)]
use std::thread_local;
use std::unreachable;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use crate::common::VacuumScheduler;
use crate::common::txn::{BulkCore, Registry, TxnCore, WalTail, WriteSide};
use crate::common::version::vacuum_drain_group;
pub use crate::common::{
    DurabilityProfile, EngineTuning, ExportRecord, MvccHealth, VacuumRound, VacuumStats,
};
use crate::lsm_tree::lsm_tree::{BlobGcPlan, LSMTree, LSMTreeOptions};
use crate::lsm_tree::storage::{FileLock, lock_path, wal_path};
use crate::lsm_tree::wal::{Wal, WalRecord, analyze_recovery};

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
pub use crate::common::{VersionedKey, decode_key, encode_key};

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) mod search_borrow_reference;

#[cfg(test)]
pub(crate) mod scan_row_reference;

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
pub(crate) use probes::vac_pending_max;
use recovery::compute_start_version;
use recovery::persist_mvcc_next;
use recovery::pin_mvcc_invariants;
use recovery::run_checkpoint;
use recovery::seal_wal_checkpoint;
