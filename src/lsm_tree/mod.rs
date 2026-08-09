//! 教学用 LSM 存储引擎 + MVCC 事务层。
//!
//! - [`lsm_tree`]：MemTable + SST 分层 + flush / compaction + Snapshot
//! - [`sst`]：SSTable（block + 稀疏索引 + bloom + mmap + 流式 iter）
//! - [`cf`]：多列族 [`LsmDB`]
//! - [`mvcc`]：快照隔离事务 / WAL / vacuum / bulk load
//! - [`kv_ops`]：批量 / 范围 / 搜索分页 / 元数据 / TTL（`Transaction` 扩展）
//! - [`wal`] / [`mem_wal`] / [`storage`]
//!
//! # 写路径与 stats
//!
//! - **普通事务**（`commit` = WAL fsync）vs **bulk**（直接 L0）——吞吐差几个数量级，见 README。
//! - **`stats()`** 的 `live_keys` 是缓存近似（`open` 后可能为 0）；精确用 `stats_exact()`。

pub mod bloom;
pub mod cf;
pub mod kv_ops;
pub mod lsm_tree;
pub mod mem_wal;
pub mod lsm_tree_mvcc;
pub mod sst;
pub mod storage;
pub mod wal;

pub use cf::LsmDB;
pub use kv_ops::{IncrError, KeyMatchMode, KeyMeta, SearchPage, SearchQuery};
pub use lsm_tree::{
    spawn_bg_compactor, CompactionFilter, LSMTree, LSMTreeOptions, LsmStats, Snapshot,
};
pub use lsm_tree_mvcc::{ExportRecord, VacuumStats, MVCC};
pub use sst::StoredEntry;
