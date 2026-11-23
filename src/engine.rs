//! 统一引擎门面：对象安全的 [`KvEngine`] / [`KvTxn`] / [`KvBulk`]。
//!
//! 四个引擎的 `MVCC` / `Transaction` / `BulkLoader` 结构体互不相同（各自的
//! 后端、WAL、blob 都是独立类型），但对外暴露的方法签名高度一致。本模块把
//! 这份一致性提取成 trait，于是可以按**枚举或运行时字符串**选引擎：
//!
//! ```no_run
//! use storage_engines::engine::{open_engine, EngineKind};
//! use std::path::Path;
//!
//! // 编译期就知道自己用哪个引擎
//! let db = open_engine(EngineKind::LsmTree, Some(Path::new("./data/lsm")))?;
//! // 也可把上面的 EngineKind::LsmTree 换成配置字符串 "lsm"。
//! let tx = db.begin();
//! tx.try_set_raw(b"k", b"v".to_vec())?;
//! tx.try_commit()?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! # 设计要点
//!
//! - **对象安全**：所有方法都不带泛型参数，`&[u8]` / `Vec<u8>` 取代
//!   `impl AsRef<[u8]>`，因此 `Box<dyn KvEngine>` 可用。
//! - **生命周期**：[`KvEngine::begin`] 返回 `Box<dyn KvTxn + '_>`，事务借用
//!   引擎，避免把 `MVCC` 的内部 `Arc` 再包一层。
//! - **引擎差异**在适配层吸收，不污染各引擎自身实现：
//!   - `memory` 的 `vacuum` 不返回 `io::Result` → 适配时包成 `Ok`
//!   - `memory` 没有 `flush` / `checkpoint` → 实现为 no-op
//!   - `bplus_tree` 的 `open` 需要 `order` / `max_cache_pages` → 走
//!     [`EngineConfig`]
//!   - 只有 `bitcask` 有 `merge` → 默认实现返回 [`EngineError::Unsupported`]

use std::path::{Path, PathBuf};
use std::time::Duration;
use std::unreachable;
use std::{fmt, write};

use crate::common::{
    DurabilityProfile, EngineTuning, ExportRecord, IncrError, KeyMeta, MvccHealth, SearchPage,
    SearchQuery, VacuumRound, VacuumStats,
};

// 只有 bitcask 支持 merge：宏生成的 impl 用了 trait 的默认实现（返回
// Unsupported），这里没法再开一个 `impl KvEngine for BitcaskEngine`，所以
// merge 的 override 由宏的 `merge` 开关注入，见 impl_engine! 的调用点。

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) mod migration_probe;

#[cfg(test)]
mod migration_stage_tests;

// 按职责组织的内部实现；公开类型与原模块路径保持一致。
mod adapters;
mod bulk_api;
mod config;
mod engine_api;
mod error;
mod factory;
mod migration;
mod selection;
mod txn_api;
pub use bulk_api::KvBulk;
pub use config::EngineConfig;
pub use config::StorageConfig;
pub use config::open_from_env;
pub use engine_api::KvEngine;
pub use error::EngineError;
pub use error::Result;
pub use factory::open_engine;
pub use factory::open_engine_with;
pub use migration::MigrateStats;
pub use migration::migrate;
pub use selection::ENGINE_NAMES;
pub use selection::EngineKind;
pub use selection::EngineName;
#[cfg(test)]
use selection::canonical_name;
pub use txn_api::KvTxn;

use adapters::{BPlusTreeEngine, BitcaskEngine, LsmTreeEngine, MemoryEngine};
