//! MVCC 事务层（纯内存版）
//!
//! 对标 bplus-tree 的快照隔离语义，但**没有** WAL / 页刷盘 / 崩溃恢复：
//! - begin：分配 xid，拍下当时活跃事务快照
//! - 写：版本键 `raw_key || version_be`，写冲突检测
//! - 读：可见性过滤后取最新版本
//! - commit / rollback：维护活跃表，回滚物理删本事务 version 行
//!
//! 存储落到 [`crate::memory::mem_store::MemStore`]。

#[cfg(test)]
use std::thread_local;
use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use crate::common::VacuumScheduler;
use crate::common::txn::{BulkCore, Registry, TxnCore, WriteSide};
use crate::common::version::vacuum_drain_group;
pub use crate::common::{
    DurabilityProfile, EngineTuning, ExportRecord, MvccHealth, VacuumRound, VacuumStats,
};
use crate::memory::mem_store::MemStore;
use std::borrow::Cow;

enum SnapshotOwnership {
    Exclusive(std::collections::btree_map::IntoIter<Vec<u8>, Option<Vec<u8>>>),
    Shared,
}

/// 下一条物理记录可以证明当前逻辑组完整，而无需拥有窗口键；前缀重叠和批尾仍走通用折叠。
enum SnapshotFoldRoute {
    Complete,
    Window,
}

// 测试专用：本线程在「每个逻辑 key 的胜出行」那次折叠里**走了多少个版本键**。
//
// 内存引擎既没有磁盘读也没有 WAL，所以它的代价单位就是"比了多少把键"：
// 窗口扫必须只走窗口内的键，而不是折完全表再由调用方筛界。
#[cfg(test)]
thread_local! {
    static FOLD_KEYS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// MVCC 引擎（单进程内存）
pub struct MVCC {
    store: Arc<Mutex<MemStore>>,
    active_txn: Arc<Mutex<Registry>>,
    next_version: Arc<AtomicU64>,
    /// 自动回收：切片宽度 / 记账 / 游标（策略与实现在 [`crate::common::VacuumScheduler`]）
    vac: Arc<VacuumScheduler>,
    /// M81 第 6 项：启停快照文件。`None` = 不落盘（裸 [`MVCC::new`] 的既有形状，一字未变）。
    ///
    /// 语义钉死成**只在启停各碰一次文件**（用户口径）：open 读一次、`Drop` 写一次。
    /// 运行期的 set/delete/事务/`flush()`/`checkpoint()` 一律不碰文件 —— `flush` 维持 no-op，
    /// 所以台架里 memory 的 `flush` 格一个数都不变。
    /// 代价如实写明：**没有手动存盘口子**，中途要落盘只能释放句柄（或走 export 工具）。
    /// 这是"优雅停止不丢"，**不是崩溃安全**：进程被杀或被 `std::process::exit` 收尾都不跑 `Drop`。
    snapshot: Option<std::path::PathBuf>,
}

/// 快照文件名（放在门面给的引擎目录里：`data_dir/memory/snapshot.bxp`）
pub const SNAPSHOT_FILE: &str = "snapshot.bxp";

/// 重放期间把自动回收**临时关掉**（切片宽度置 0），`Drop` 时还原（M82）。
///
/// 为什么用 RAII 而不是"进去设 0、出来设回来"：`load_snapshot_from` 有三条提前返回
/// （快照读失败、重放被引擎拒收、尾巴半页出错），漏掉任何一条都会把自动回收**永久关掉** ——
/// 那种坏法不崩、不报错，只是版本链从此只增不减。析构里还原，panic 展开也兜得住。
struct AutoVacuumOff<'a> {
    vac: &'a VacuumScheduler,
    saved: u64,
}

/// 普通 MVCC 事务
///
/// 事务语义那部分（版本、活跃快照、写集、回收记账、冲突判定、回滚）在
/// [`TxnCore`]（[`crate::common::txn`]，四引擎共用）；这里只留"这一家的字节住在哪"。
pub struct Transaction {
    pub(crate) store: Arc<Mutex<MemStore>>,
    pub(crate) core: TxnCore,
}

/// 批量导入：无冲突检测、不登记活跃表（单写者假定）。会话号与收尾顺序在 [`BulkCore`]。
pub struct BulkLoader {
    store: Arc<Mutex<MemStore>>,
    core: BulkCore,
}

/// 逻辑键 + 版本（调试用）与版本键编解码
///
/// 定义在 [`crate::common::version`]（四引擎共用）；这里 re-export 是为了
/// `crate::memory::encode_key` 这个既有路径不中断。
pub use crate::common::{VersionedKey, decode_key, encode_key};

/// M37：一次 `vacuum_bounded` 走查的**块大小**（物理行）—— 与 bitcask / lsm 同值，
/// 各家钉各家（守卫同名同形：`test_vacuum_deletes_in_bounded_chunks`）。
/// 这一家改的收益最小（600k 档 `vacuum()` 瞬时 4.0 MB），改它的理由是**同根因不留三家**：
/// 走查形状一模一样的 `to_delete` 攒法，四家如果只改两家，下一次改一家漏一家的事故还会重演。
const VAC_CHUNK_ROWS: usize = 8192;

// 测试专用（M37 的尺子）：单次 flush 前最多攒了多少待删行。
#[cfg(test)]
thread_local! {
    static VAC_PENDING_MAX: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// M121 对照冻结 M117 的逐页回放和表尾查找行为。
#[cfg(test)]
pub(crate) mod page_apply_reference;

#[cfg(test)]
/// 只对照 M109 的成本，保留 M105 缓冲读写和事务键所有权。
pub(crate) mod replay_cost_reference;

#[cfg(test)]
pub(crate) mod scan_reference;

#[cfg(test)]
pub(crate) mod stop_fold_reference;

#[cfg(test)]
pub(crate) mod stop_batch_release;

#[cfg(test)]
pub(crate) mod stop_reference;

#[cfg(test)]
pub(crate) mod replay_reference;

#[cfg(test)]
#[cfg(test)]
pub(crate) mod search_key_reference;

#[cfg(test)]
pub(crate) mod search_copy_reference;

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) mod search_stream_reference;

#[cfg(test)]
pub(crate) mod vacuum_cost_reference;

#[cfg(test)]
pub(crate) mod stop_rows_reference;

// 按职责组织的内部实现；公开类型与原模块路径保持一致。
mod bulk;
mod export;
mod health;
mod lifecycle;
#[cfg(test)]
mod probes;
mod search;
mod snapshot;
mod transaction;
mod vacuum;
#[cfg(test)]
pub(crate) use probes::fold_keys;
#[cfg(test)]
use probes::note_vac_pending;
#[cfg(test)]
pub(crate) use probes::reset_vac_pending;
#[cfg(test)]
pub(crate) use probes::vac_pending_max;
