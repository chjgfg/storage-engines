//! 教学用 Bitcask 存储引擎 + MVCC 事务层。
//!
//! - [`bitcask`]：append-only 日志 + 内存 KeyDir + hint 快启动（可多分片）
//! - [`mvcc`]：快照隔离事务 / WAL / vacuum / bulk load
//! - [`kv_ops`]：批量 / 范围 / 搜索分页 / 元数据 / TTL（`Transaction` 扩展）
//! - [`wal`] / [`storage`] / [`error`]

pub mod bitcask;
pub mod error;
pub mod kv_ops;
pub mod bitcask_mvcc;
pub mod storage;
pub mod wal;

pub use bitcask::{
    hint_path_for, Bitcask, KeyDir, KeyLoc, Log, Result as BitcaskResult, DEFAULT_MAX_FILE_BYTES,
};
pub use error::{Error, Result};
pub use kv_ops::{IncrError, KeyMatchMode, KeyMeta, SearchPage, SearchQuery};
pub use bitcask_mvcc::{EngineTuning, ExportRecord, VacuumStats, MVCC};
