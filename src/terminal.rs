//! KV 终端的命令解释器 —— `shell` / `web` 两个 bin 共用的一份壳内核心。
//!
//! 为什么收进库而不是让两个 bin 各抄一份：命令表、参数校验、输出形状只写一遍，
//! 两个外壳只做各自的输入输出（shell 的行式表格 / web 的 JSON）。职责边界与
//! `tools::{export_to_dir, import_from_dir}` 之于 export-kv / import-kv 相同。
//!
//! 状态模型：解释器**不持有引擎**。[`crate::terminal::run_line`] 是纯函数 —— 引擎与挂起事务都由调用方拿着：
//!
//! ```text
//! run_line(&dyn KvEngine, &mut Option<Box<dyn KvTxn>>, "set k v") -> LineOutcome
//! ```
//!
//! 挂起事务（`begin` 之后、`commit` / `rollback` 之前）由调用方的 `Option` 持有，后续命令都
//! 走它；隐式路径（没开事务的普通读写）在 `with_tx` 里开一条、跑完即提交，冲突时整体回滚。
//! 引擎切换（`engine <名>`）返回 [`crate::terminal::LineOutcome::Switch`] 让调用方重建引擎 —— 解释器不能自己
//! 换，因为事务借用了引擎，开着事务换引擎会悬垂；这个拒绝判定在解释器里做（`txn.is_some()`）。
//!
//! 输出统一是 [`crate::terminal::Out`]：一行消息，或一张 `columns × rows` 表。shell 把表画成对齐文本
//! （CJK 感知列宽），web 把表折成 JSON 的 `columns/rows` 交给前端渲染 —— 两边看到的是
//! 同一次执行的结果。
//!
//! ```text
//! cargo run --bin shell -- --engine memory
//! cargo run --bin web   -- --engine memory --port 8080
//! ```

use std::format;
use std::path::PathBuf;
use std::string::{String, ToString};
use std::vec::Vec;

use crate::common::types::{ExportRecord, IncrError, KeyMatchMode, SearchQuery};
use crate::engine::{EngineKind, KvEngine, KvTxn};

// ── 输出形状 ─────────────────────────────────────────────────────────────────

/// 一张结果表：`columns × rows`（单元格数允许少于列数，渲染方补空）。
#[derive(Debug, Clone)]
pub struct Table {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

/// 一条命令的输出：消息行 + 可选表格。`kind` 给 web 前端着色用
/// （`ERROR` 红 / `TXN` 事务蓝 / `MAINT` 维护 / `HELP` 弱化 …），shell 忽略它。
#[derive(Debug, Clone)]
pub struct Out {
    pub ok: bool,
    pub kind: &'static str,
    pub message: String,
    pub table: Option<Table>,
}

/// 一行命令的去向。
#[derive(Debug)]
pub enum LineOutcome {
    /// 普通命令的（可能多条）输出
    Outputs(Vec<Out>),
    /// `engine <名> [目录]`：让调用方重建引擎（目录 `None` = 调用方自己的默认目录约定）
    Switch {
        kind: &'static str,
        dir: Option<PathBuf>,
    },
    /// exit / quit
    Exit,
}

// ── 事务包装 ────────────────────────────────────────────────────────────────

/// 隐式事务的收尾方式：数据命令在冲突时选 `Rollback`（未提交的写入整体作废），其余 `Commit`。
enum TxnEnd {
    Commit,
    Rollback,
}

// ── 常量 ────────────────────────────────────────────────────────────────────

/// 全量扫描的显示上限：终端是给人看的，库大了把 START/END 收窄，总量用 `count` 看。
const SCAN_CAP: usize = 100;
/// `find` 默认每页条数（页码是 offset 式，页宽太大深翻变贵 —— 见 `SearchQuery` 的注释）。
const FIND_PAGE_SIZE: usize = 20;

// ── 测试 ────────────────────────────────────────────────────────────────────
//
// 解释器是纯函数，直接在 memory 引擎上跑真命令断言输出 —— shell / web 两个外壳
// 只是输入输出，这里的断言就是它们俩共同的行为合同。

#[cfg(test)]
mod tests;

// 按职责组织的内部实现；公开类型与原模块路径保持一致。
mod data_commands;
mod dispatch;
mod engine_commands;
mod help;
mod output;
mod parsing;
use data_commands::cmd_count;
use data_commands::cmd_del;
use data_commands::cmd_exists;
use data_commands::cmd_find;
use data_commands::cmd_get;
use data_commands::cmd_incr;
use data_commands::cmd_meta;
use data_commands::cmd_mget;
use data_commands::cmd_mset;
use data_commands::cmd_persist;
use data_commands::cmd_prefix;
use data_commands::cmd_purge;
use data_commands::cmd_refresh;
use data_commands::cmd_scan;
use data_commands::cmd_seek;
use data_commands::cmd_set;
use data_commands::cmd_setex;
use data_commands::cmd_ttl;
pub use dispatch::run_line;
use engine_commands::cmd_begin;
use engine_commands::cmd_engine;
use engine_commands::cmd_engine_op;
use engine_commands::cmd_finish;
use engine_commands::cmd_health;
use engine_commands::cmd_info;
use engine_commands::cmd_vacuum;
use help::help_text;
pub use output::show;
use parsing::one;
use parsing::three;
pub use parsing::tokenize;
use parsing::two;
