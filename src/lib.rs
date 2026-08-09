//! 四个教学用 KV 存储引擎 + 统一 trait 门面。
//!
//! - [`common`]：四引擎共享的公共类型与 TTL 编解码
//! - [`engine`]：对象安全的 [`KvEngine`] / [`KvTxn`] / [`KvBulk`]，支持按字符串在运行时选引擎
//! - [`bitcask`] / [`bplus_tree`] / [`lsm_tree`] / [`memory`]：四个引擎实现
//!
//! ```no_run
//! use storage_engines::engine::open_engine;
//! use std::path::Path;
//!
//! let db = open_engine("lsm", Some(Path::new("./data/lsm")))?;
//! let tx = db.begin();
//! tx.set(b"k", b"v".to_vec());
//! tx.commit();
//! # Ok::<(), storage_engines::engine::EngineError>(())
//! ```

pub mod common;
pub mod engine;

pub mod bitcask;
pub mod bplus_tree;
pub mod lsm_tree;
pub mod memory;

pub use common::{
    ExportRecord, IncrError, KeyMatchMode, KeyMeta, SearchPage, SearchQuery, VacuumStats, ValueMeta,
};
pub use engine::{open_engine, open_engine_with, EngineConfig, EngineError, KvBulk, KvEngine, KvTxn};
