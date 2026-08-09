//! # memory
//!
//! 教学向 **纯内存 KV 数据库**，架构对齐 [`bplus-tree`](../bplus-tree)：
//!
//! ```text
//! 应用 / main / bench
//!   ↓ begin / set / get / delete / commit
//!   ↓ batch_set / scan / search_keys / TTL …
//! MVCC / Transaction / BulkLoader     ← mvcc.rs
//!   ↓ encode_key + insert / range_scan
//! MemStore (BTreeMap 多版本)          ← mem_store.rs
//!
//! kv_ops.rs 扩展 Transaction：批量 / 扫描 / 搜索分页 / 元数据 / TTL
//! ```
//!
//! 与 bplus-tree 的差异：
//! - **无** 页管理 / WAL / blob / 文件锁 / 崩溃恢复
//! - 存储层换成 [`MemStore`]（`BTreeMap` 有序多版本）
//! - 事务语义（快照隔离、写冲突、vacuum）保持一致

pub mod kv_ops;
pub mod mem_store;
pub mod memory_mvcc;

pub use kv_ops::{IncrError, KeyMatchMode, KeyMeta, SearchPage, SearchQuery};
pub use mem_store::MemStore;
pub use memory_mvcc::{ExportRecord, MVCC, VacuumStats};
