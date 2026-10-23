//! 搬运数据层：跨引擎 `migrate` + 快照文件的 `export` / `import`。
//!
//! **不属于任何引擎核心**，只服务运维与换引擎那一次动作（停进程 → 搬 → 起进程）。
//!
//! ## 为什么要单独有这一层
//! 在这之前，本仓库的"搬运"散在八个 `bin/*-import-kv` / `bin/*-export-kv` 里：每家一份
//! 参数解析 + 一份 bulk 灌法 + 一份格式分支 —— 那正是"基座被复制而不是被依赖"的形状。
//! 这一层把**四家共用的那部分收成一次实现**（走 [`crate::KvEngine`] 门面），bin 只留自己的
//! 命令行外壳。
//!
//! ## 与下游 `trading-system/src/storage/tools` 的对应关系
//! 那份是这一层的**调用合同**：函数名、参数顺序、返回结构体的字段名、eprintln 的日志形状、
//! 以及"`memory` 不能当源/目标""bplus 的文件名是 `data.db`""迁移用的树阶是 16"这几条
//! 现场约定。对齐它们是为了**下游可以把整个 `src/storage` 删掉换成依赖本 crate**，
//! 而不必改 `bin/migrate_storage.rs` 与 `tests/storage_migrate.rs` 的任何一行。
//! 三条口径差异（都写在 [`migrate`] 的文档里）：
//! - 这里**不物化整库**（那边 `Vec<KvRecord>` 一份、600k 档实测 ≈123 MB），走 M44 的流式出口；
//! - 这里的 `bulk` 收的是**用户字节**，信封由引擎自己打 ⇒ 调用方**不许**再 `pack_plain`
//!   （打两遍 = 读出来带一层假信封，这是 M44/M45 那一串教训里唯一会静默错的一支）；
//! - `verify_live` 用 `health().live_keys` 数，不再"为了数一个数把整库再导一遍"（M35/M47 同一条纪律）。

pub mod kv_snapshot {
    //! 快照文件编解码：与引擎核心无关，只有一种格式两种载体。
    //!
    //! 实体在 [`crate::common::kv_snapshot`]（四家的 bin 与这里共用一份解码器 —— 一份格式
    //! 不该有两种解法，见 M45）。这一层只是把路径摆成 `tools::kv_snapshot::*`。
    pub use crate::common::kv_snapshot::*;
}

pub mod migrate;

pub use kv_snapshot::{
    read_bin, read_bin_with, read_jsonl, read_jsonl_with, write_bin, write_jsonl, IoStats,
    KvRecord, BIN_MAGIC, BIN_MAGIC_V2, TOMBSTONE_LEN,
};
pub use migrate::{
    bplus_db_path, engine_data_path, export_to_file, import_from_file, migrate, parse_arrow,
    MigrateStats, SnapshotFormat,
};
