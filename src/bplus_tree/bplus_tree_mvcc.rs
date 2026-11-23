//! MVCC 事务层 + WAL 崩溃恢复
//!
//! 提交协议（简化版 ARIES）：
//! 1. 写路径：先 append WAL Write，再改内存 B+ 树
//! 2. commit：append WAL Commit → **fsync WAL**（持久化点）→ 可选刷页
//! 3. 崩溃恢复：重放已提交 Write，撤销未提交 Write，再 checkpoint 截断 WAL
//!
//! **Bulk load 模式**（[`MVCC::begin_bulk`]）：
//! - 关闭 WAL / 冲突检测 / 活跃事务表
//! - 页换出跳过双写与频繁 fsync
//! - 结束时 [`BulkLoader::finish`] → flush_all + checkpoint

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

use crate::bplus_tree::storage::{BlobStore, decode_stored_value, encode_stored_value, wal_path};
use crate::bplus_tree::wal::{Wal, WalRecord, analyze_recovery};
use crate::bplus_tree::{BPlusTree, bplus_tree::RowId};
use crate::common::VacuumScheduler;
use crate::common::txn::{BulkCore, Registry, TxnCore, WalTail, WriteSide};
use crate::common::version::vacuum_drain_group;
pub use crate::common::{
    DurabilityProfile, EngineTuning, ExportRecord, MvccHealth, VacuumRound, VacuumStats,
};

/// MVCC 存储引擎
/// M37：一次 `vacuum_bounded` 走查的**块大小**（物理行）—— 与 bitcask / lsm / memory 同值，
/// 各家钉各家（守卫同名同形 `test_vacuum_deletes_in_bounded_chunks`）。
/// 这一家实测基数也小（600k 档 `vacuum()` 瞬时 3.9 MB），改它的理由同 memory：**同根因不留三家**。
const VAC_CHUNK_ROWS: usize = 8192;

// 测试专用（M37 的尺子）：单次 flush 前最多攒了多少待删行。
#[cfg(test)]
thread_local! {
    static VAC_PENDING_MAX: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

// 测试专用：把 `vacuum_bounded` 的块大小临时压小，好在小库上**确定性**复现"版本组跨块边界"
// 那条路径（自然块 8192 行 ⇒ 要几十万行才碰得到一次对齐）。0 = 用默认 `VAC_CHUNK_ROWS`。
#[cfg(test)]
thread_local! {
    static VAC_CHUNK_OVERRIDE: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

pub struct MVCC {
    kv: Arc<Mutex<BPlusTree>>,
    /// 大 value 外置存储（与 kv 同库目录）
    blob: Arc<Mutex<BlobStore>>,
    wal: Arc<Mutex<Wal>>,
    active_txn: Arc<Mutex<Registry>>,
    next_version: Arc<AtomicU64>,
    /// WAL 增长到多少字节就在下一次提交后自动 checkpoint（0 = 关闭自动触发）
    wal_limit: Arc<AtomicU64>,
    /// 自动回收：切片宽度 / 记账 / 游标（策略与实现在 [`crate::common::VacuumScheduler`]）
    vac: Arc<VacuumScheduler>,
    db_path: PathBuf,
}

/// 普通 MVCC 事务
///
/// 事务语义（版本、活跃快照、写集、冲突判定、整条写序列、回收记账）在 [`TxnCore`]
/// （[`crate::common::txn`]，四引擎共用）；这一家只留"字节在哪一页、大值走不走 blob、
/// 这一行装不装得下一页"。
pub struct Transaction {
    pub(crate) kv: Arc<Mutex<BPlusTree>>,
    pub(crate) blob: Arc<Mutex<BlobStore>>,
    pub(crate) wal: Arc<Mutex<Wal>>,
    next_version: Arc<AtomicU64>,
    wal_limit: Arc<AtomicU64>,
    pub(crate) core: TxnCore,
}

/// 写路径的守卫：三把锁按内核要求的顺序（活跃表 → wal → blob → kv）拿好。
///
/// 四家里只有这一家要用满 [`WriteSide`] 的槽：`encode_value`（blob 旁路）与 `row_fits`
/// （定长页的容量判定）。回滚与部分撤销复用同一个守卫（多锁一次 blob，无竞争）。
pub(crate) struct TxnSide<'w, 'b, 'k> {
    pub(crate) wal: &'w mut Wal,
    pub(crate) blob: &'b mut BlobStore,
    pub(crate) kv: &'k mut BPlusTree,
}

/// 收尾用的守卫：`commit` 只要往 WAL 落一条 `Commit` 再 fsync，不该在这期间握着 kv/blob 锁
/// （`flush_all` 与 checkpoint 由调用方在放掉这两把锁之后自己做）。
struct WalOnly<'w> {
    wal: &'w mut Wal,
}

/// 批量导入会话：无 WAL、无冲突检测，单写者假定
pub struct BulkLoader {
    kv: Arc<Mutex<BPlusTree>>,
    blob: Arc<Mutex<BlobStore>>,
    wal: Arc<Mutex<Wal>>,
    core: BulkCore,
}

/// 逻辑键 + 版本（调试用）与版本键编解码
///
/// 定义在 [`crate::common::version`]（四引擎共用）；这里 re-export 是为了
/// `crate::bplus_tree::encode_key` 这个既有路径不中断。
pub use crate::common::{VersionedKey, decode_key, encode_key};

#[cfg(test)]
pub(crate) mod vacuum_sync_reference;

#[cfg(test)]
pub(crate) mod vacuum_scan_reference;

#[cfg(test)]
mod tests;

// 按职责组织的内部实现；公开类型与原模块路径保持一致。
mod bulk;
mod export;
mod health;
mod lifecycle;
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
pub(crate) use probes::set_vac_chunk_override;
use probes::vac_chunk_limit;
#[cfg(test)]
pub(crate) use probes::vac_pending_max;
use recovery::insert_or_shout;
use recovery::resolve_start_version;
use recovery::run_checkpoint;
use recovery::seal_wal_checkpoint;
