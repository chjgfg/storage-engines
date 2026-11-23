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
pub mod maintenance;
pub use maintenance::{
    Maintenance, MaintenanceConfig, MaintenanceHandle, MaintenanceStatus, start_maintenance,
};

/// KV 终端命令解释器（`shell` / `web` 两个 bin 共用的一份壳内核心）。
/// 库使用者也可以拿它给自己的运维界面接同一套命令面。
pub mod terminal;

pub mod bitcask;
pub mod bplus_tree;
pub mod lsm_tree;
pub mod memory;
/// 搬运数据层（跨引擎 migrate / 快照 import-export）。不属于任何引擎核心。
pub mod tools;

pub use common::{
    CommitError, DurabilityProfile, EngineTuning, ExportRecord, IncrError, KeyMatchMode, KeyMeta,
    MvccHealth, ReclamationStatus, SearchPage, SearchQuery, TuningEntry, VacuumRound, VacuumStats,
    ValueMeta, WriteError,
};
pub use engine::{
    EngineConfig, EngineError, EngineKind, EngineName, KvBulk, KvEngine, KvTxn, MigrateStats,
    StorageConfig, migrate, open_engine, open_engine_with, open_from_env,
};
/// LSM 的旋钮集：`EngineConfig::lsm` 收的就是这个类型，所以在根上直接给个名字，
/// 免得下游为了配一个缓存容量还得写全 `lsm_tree::` 路径（那正是"绕开门面自己拼"的起点）。
pub use lsm_tree::LSMTreeOptions;

/// 测试专用：记账分配器 + 峰值水位线。`#[cfg(test)]` ⇒ 对 crate 使用者零成本。
///
/// 原来这份埋在 `engine::tests` 里，只有门面台架用得到。M33 要量的是**存储层内部**一次压实的
/// 瞬时内存，所以提到 crate 根，两家共用一把尺子 —— 一个 crate 只能有一个 `#[global_allocator]`，
/// 在 lsm 那边再抄一份是直接编译失败，不是"多测一遍"。
///
/// `reset_peak()` 是这次新增的那一格：`peak` 是**进程高水位**，同一轮里靠后的操作永远报 `+0`
/// （M27 记下的度量盲区，当时写的是"要拆得做隔离台架"）。把水位线在一段操作开始前压回
/// "当前活着的字节"，那段自己涨了多少才量得出来。
#[cfg(test)]
pub(crate) mod mem_probe;

#[cfg(test)]
mod scale_probe;

// 按职责组织的内部实现；公开类型与原模块路径保持一致。
