//! 四个教学用 KV 存储引擎 + 统一 trait 门面。
//!
//! - [`common`]：四引擎共享的公共类型与 TTL 编解码
//! - [`engine`]：对象安全的 [`KvEngine`] / [`KvTxn`] / [`KvBulk`]，支持按枚举或字符串选引擎
//! - [`bitcask`] / [`bplus_tree`] / [`lsm_tree`] / [`memory`]：四个引擎实现
//!
//! ```no_run
//! use storage_engines::{open_engine, EngineKind};
//! use std::path::Path;
//!
//! // 编译期定引擎
//! let db = open_engine(EngineKind::LsmTree, Some(Path::new("./data/lsm")))?;
//! // 引擎名来自配置文件/环境变量时
//! let db = open_engine("lsm", Some(Path::new("./data/lsm")))?;
//! let tx = db.begin();
//! tx.set(b"k", b"v".to_vec());
//! tx.commit();
//! # Ok::<(), storage_engines::EngineError>(())
//! ```

pub mod common;
pub mod engine;

pub mod bitcask;
pub mod bplus_tree;
pub mod lsm_tree;
pub mod memory;

pub use common::{
    DurabilityProfile, EngineTuning, ExportRecord, IncrError, KeyMatchMode, KeyMeta, MvccHealth,
    SearchPage, SearchQuery, TuningEntry, VacuumRound, VacuumStats, ValueMeta,
};
pub use engine::{
    EngineConfig, EngineError, EngineKind, EngineName, KvBulk, KvEngine, KvTxn, MigrateStats, StorageConfig,
    open_engine, open_engine_with, open_from_env, migrate,
};
/// LSM 的旋钮集：`EngineConfig::lsm` 收的就是这个类型，所以在根上直接给个名字，
/// 免得下游为了配一个缓存容量还得写全 `lsm_tree::` 路径（那正是"绕开门面自己拼"的起点）。
pub use lsm_tree::LSMTreeOptions;
