//! KV 终端的命令解释器 —— `shell` / `web` 两个 bin 共用的一份壳内核心。
//!
//! 为什么收进库而不是让两个 bin 各抄一份：命令表、参数校验、输出形状只写一遍，
//! 两个外壳只做各自的输入输出（shell 的行式表格 / web 的 JSON）。职责边界与
//! `tools::{export_to_dir, import_from_dir}` 之于 export-kv / import-kv 相同。
//!
//! 状态模型：解释器**不持有引擎**。[`run_line`] 是纯函数 —— 引擎与挂起事务都由调用方拿着：
//!
//! ```text
//! run_line(&dyn KvEngine, &mut Option<Box<dyn KvTxn>>, "set k v") -> LineOutcome
//! ```
//!
//! 挂起事务（`begin` 之后、`commit` / `rollback` 之前）由调用方的 `Option` 持有，后续命令都
//! 走它；隐式路径（没开事务的普通读写）在 [`with_tx`] 里开一条、跑完即提交，冲突时整体回滚。
//! 引擎切换（`engine <名>`）返回 [`LineOutcome::Switch`] 让调用方重建引擎 —— 解释器不能自己
//! 换，因为事务借用了引擎，开着事务换引擎会悬垂；这个拒绝判定在解释器里做（`txn.is_some()`）。
//!
//! 输出统一是 [`Out`]：一行消息，或一张 `columns × rows` 表。shell 把表画成对齐文本
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

impl Out {
    /// 纯消息行（成功）
    pub fn msg(kind: &'static str, message: impl Into<String>) -> Self {
        Self { ok: true, kind, message: message.into(), table: None }
    }

    /// 错误行（不中断后续命令，与逐条执行的习惯一致）
    pub fn err(message: impl Into<String>) -> Self {
        Self { ok: false, kind: "ERROR", message: message.into(), table: None }
    }

    /// 消息 + 表（消息作脚注，如「SCAN 3 行」）
    pub fn with_table(kind: &'static str, message: String, table: Table) -> Self {
        Self { ok: true, kind, message, table: Some(table) }
    }
}

/// 一行命令的去向。
#[derive(Debug)]
pub enum LineOutcome {
    /// 普通命令的（可能多条）输出
    Outputs(Vec<Out>),
    /// `engine <名> [目录]`：让调用方重建引擎（目录 `None` = 调用方自己的默认目录约定）
    Switch { kind: &'static str, dir: Option<PathBuf> },
    /// exit / quit
    Exit,
}

// ── 字节与 token ─────────────────────────────────────────────────────────────

/// 字节 → 可读文本：合法 UTF-8 原样（换行/制表转义，免得撑破表格行），否则 `<hex …>`。
pub fn show(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(s) => s.replace('\n', "\\n").replace('\r', "\\r").replace('\t', "\\t"),
        Err(_) => {
            let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
            format!("<hex {hex}>")
        }
    }
}

/// 一行命令切 token：空白分隔；`"..."` / `'...'` 内空白原样保留，成对引号（`''` / `""`）
/// = 一个字面引号 —— 与引擎词法器的转义规则同一套。反斜杠**不是**转义符（KV 的值里它太常见）。
/// 引号可以在 token 中段出现（`ab"cd ef"` → `abcd ef`），对终端输入足够了。
pub fn tokenize(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut quoted = false; // 当前 token 是否在引号里（决定空白是否切分）
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '"' || c == '\'' {
            if !quoted {
                quoted = true;
                continue;
            }
            // 引号内再看一个字符：成对引号 = 字面引号，否则本引号闭合
            if chars.peek() == Some(&c) {
                chars.next();
                cur.push(c);
            } else {
                quoted = false;
            }
            continue;
        }
        if !quoted && c.is_whitespace() {
            if !cur.is_empty() {
                out.push(std::mem::take(&mut cur));
            }
            continue;
        }
        cur.push(c);
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

// ── 事务包装 ────────────────────────────────────────────────────────────────

/// 隐式事务的收尾方式：数据命令在冲突时选 `Rollback`（未提交的写入整体作废），其余 `Commit`。
enum TxnEnd {
    Commit,
    Rollback,
}

/// 数据命令统一走这里：挂起事务开着就复用它（归调用方继续持有），否则隐式开一条、
/// 跑完按 `f` 给的收尾方式提交或回滚。`begin` 显式开的事务**不**在这里提交 —— 那正是挂起的含义。
fn with_tx<'e>(
    engine: &'e dyn KvEngine,
    txn: &mut Option<Box<dyn KvTxn + 'e>>,
    f: impl FnOnce(&dyn KvTxn) -> (Out, TxnEnd),
) -> Out {
    let auto = txn.is_none();
    if auto {
        *txn = Some(engine.begin());
    }
    let r = {
        let tx = txn.as_mut().unwrap();
        f(tx.as_ref())
    };
    if auto {
        if let Some(t) = txn.take() {
            match r.1 {
                TxnEnd::Commit => {
                    if let Err(e) = t.try_commit() { return Out::err(e.to_string()); }
                },
                TxnEnd::Rollback => t.rollback(),
            }
        }
    }
    r.0
}

// ── 常量 ────────────────────────────────────────────────────────────────────

/// 全量扫描的显示上限：终端是给人看的，库大了把 START/END 收窄，总量用 `count` 看。
const SCAN_CAP: usize = 100;
/// `find` 默认每页条数（页码是 offset 式，页宽太大深翻变贵 —— 见 `SearchQuery` 的注释）。
const FIND_PAGE_SIZE: usize = 20;

// ── 主入口 ──────────────────────────────────────────────────────────────────

/// 解释一行 KV 命令。空行返回空输出；未知命令报错不 panic。
pub fn run_line<'e>(
    engine: &'e dyn KvEngine,
    txn: &mut Option<Box<dyn KvTxn + 'e>>,
    line: &str,
) -> LineOutcome {
    let tokens = tokenize(line);
    let Some(cmd) = tokens.first().cloned() else {
        return LineOutcome::Outputs(Vec::new());
    };
    let args = &tokens[1..];
    // 命令名大小写不敏感（redis 习惯），key/value 保持原样
    match cmd.to_ascii_lowercase().as_str() {
        "get" => run_data(engine, txn, args, cmd_get),
        "set" => run_data(engine, txn, args, cmd_set),
        "setex" => run_data(engine, txn, args, cmd_setex),
        "del" | "delete" | "rm" => run_data(engine, txn, args, cmd_del),
        "exists" => run_data(engine, txn, args, cmd_exists),
        "meta" => run_data(engine, txn, args, cmd_meta),
        "incr" | "decr" => {
            let is_incr = cmd.eq_ignore_ascii_case("incr");
            run_data(engine, txn, args, |tx, a| cmd_incr(tx, a, is_incr))
        }
        "mset" => run_data(engine, txn, args, cmd_mset),
        "mget" => run_data(engine, txn, args, cmd_mget),
        "scan" => run_data(engine, txn, args, |tx, a| cmd_scan(tx, a, false)),
        "rscan" => run_data(engine, txn, args, |tx, a| cmd_scan(tx, a, true)),
        "prefix" => run_data(engine, txn, args, cmd_prefix),
        "seek" => run_data(engine, txn, args, |tx, a| cmd_seek(tx, a, false)),
        "seekprev" | "seek_prev" => run_data(engine, txn, args, |tx, a| cmd_seek(tx, a, true)),
        "count" => run_data(engine, txn, args, cmd_count),
        "find" => run_data(engine, txn, args, cmd_find),
        "ttl" => run_data(engine, txn, args, cmd_ttl),
        "refresh" => run_data(engine, txn, args, cmd_refresh),
        "persist" => run_data(engine, txn, args, cmd_persist),
        "purge" => run_data(engine, txn, args, |tx, _| cmd_purge(tx)),
        "begin" => LineOutcome::Outputs(vec![cmd_begin(engine, txn)]),
        "commit" => LineOutcome::Outputs(vec![cmd_finish(txn, true)]),
        "rollback" => LineOutcome::Outputs(vec![cmd_finish(txn, false)]),
        "info" => LineOutcome::Outputs(cmd_info(engine)),
        "health" => LineOutcome::Outputs(vec![cmd_health(engine)]),
        "vacuum" => LineOutcome::Outputs(vec![cmd_vacuum(engine)]),
        "flush" => LineOutcome::Outputs(vec![cmd_engine_op(engine, "flush")]),
        "checkpoint" => LineOutcome::Outputs(vec![cmd_engine_op(engine, "checkpoint")]),
        "merge" => LineOutcome::Outputs(vec![cmd_engine_op(engine, "merge")]),
        "engine" => cmd_engine(engine, txn, args),
        "help" | "h" | "?" => LineOutcome::Outputs(vec![Out::msg("HELP", help_text())]),
        "exit" | "quit" | "bye" | "q" => LineOutcome::Exit,
        other => LineOutcome::Outputs(vec![Out::err(format!(
            "未知命令: {other}（help 看命令表）"
        ))]),
    }
}

/// 数据命令的统一入口：`with_tx` 的语法糖 —— 所有 `cmd_*(&dyn KvTxn, &[String]) -> (Out, TxnEnd)`
/// 都长一个样，没必要在 match 里逐条展开同一个包装。
fn run_data<'e>(
    engine: &'e dyn KvEngine,
    txn: &mut Option<Box<dyn KvTxn + 'e>>,
    args: &[String],
    f: impl FnOnce(&dyn KvTxn, &[String]) -> (Out, TxnEnd),
) -> LineOutcome {
    LineOutcome::Outputs(vec![with_tx(engine, txn, |tx| f(tx, args))])
}

// ── 基础读写 ────────────────────────────────────────────────────────────────

fn cmd_get(tx: &dyn KvTxn, args: &[String]) -> (Out, TxnEnd) {
    let [k] = match one(args, "get K") {
        Ok(v) => v,
        Err(e) => return (e, TxnEnd::Commit),
    };
    match tx.get(k.as_bytes()) {
        Some(v) => {
            let t = Table {
                columns: vec!["key".into(), "value".into()],
                rows: vec![vec![show(k.as_bytes()), show(&v)]],
            };
            (Out::with_table("GET", "1 行".into(), t), TxnEnd::Commit)
        }
        None => (Out::msg("GET", "(nil)"), TxnEnd::Commit),
    }
}

fn cmd_set(tx: &dyn KvTxn, args: &[String]) -> (Out, TxnEnd) {
    let [k, v] = match two(args, "set K V") {
        Ok(v) => v,
        Err(e) => return (e, TxnEnd::Commit),
    };
    match tx.try_set_raw(k.as_bytes(), v.as_bytes().to_vec()) {
        Ok(()) => (Out::msg("OK", "OK"), TxnEnd::Commit),
        Err(e) => (Out::err(e.to_string()), TxnEnd::Rollback),
    }
}

fn cmd_setex(tx: &dyn KvTxn, args: &[String]) -> (Out, TxnEnd) {
    let [k, secs, v] = match three(args, "setex K 秒 V") {
        Ok(v) => v,
        Err(e) => return (e, TxnEnd::Commit),
    };
    let Ok(secs) = secs.parse::<u64>() else {
        return (Out::err(format!("TTL 要非负整数秒，收到 {secs:?}")), TxnEnd::Rollback);
    };
    if tx.set_with_ttl(k.as_bytes(), v.as_bytes().to_vec(), std::time::Duration::from_secs(secs)) {
        (Out::msg("OK", format!("OK（{secs}s 后过期）")), TxnEnd::Commit)
    } else {
        (Out::err("写入失败（可能是冲突、容量限制或存储错误）"), TxnEnd::Rollback)
    }
}

fn cmd_del(tx: &dyn KvTxn, args: &[String]) -> (Out, TxnEnd) {
    let [k] = match one(args, "del K") {
        Ok(v) => v,
        Err(e) => return (e, TxnEnd::Commit),
    };
    match tx.try_delete(k.as_bytes()) {
        Ok(()) => (Out::msg("OK", "OK（写墓碑）"), TxnEnd::Commit),
        Err(e) => (Out::err(e.to_string()), TxnEnd::Rollback),
    }
}

fn cmd_exists(tx: &dyn KvTxn, args: &[String]) -> (Out, TxnEnd) {
    let [k] = match one(args, "exists K") {
        Ok(v) => v,
        Err(e) => return (e, TxnEnd::Commit),
    };
    let yes = tx.exists(k.as_bytes());
    (Out::msg("OK", if yes { "1（存在）" } else { "0（不存在）" }), TxnEnd::Commit)
}

fn cmd_meta(tx: &dyn KvTxn, args: &[String]) -> (Out, TxnEnd) {
    let [k] = match one(args, "meta K") {
        Ok(v) => v,
        Err(e) => return (e, TxnEnd::Commit),
    };
    match tx.get_meta(k.as_bytes()) {
        None => (Out::msg("META", "(nil)"), TxnEnd::Commit),
        Some(m) => {
            let t = Table {
                columns: vec!["字段".into(), "值".into()],
                rows: vec![
                    vec!["key".into(), show(&m.key)],
                    vec!["version".into(), m.version.to_string()],
                    vec![
                        "value_len".into(),
                        m.value_len.map(|n| n.to_string()).unwrap_or_else(|| "—".into()),
                    ],
                    vec!["deleted".into(), m.deleted.to_string()],
                    vec!["expired".into(), m.expired.to_string()],
                    vec![
                        "expire_unix_secs".into(),
                        m.expire_unix_secs.map(|s| s.to_string()).unwrap_or_else(|| "永久".into()),
                    ],
                ],
            };
            (
                Out::with_table(
                    "META",
                    format!("meta {}（version {}）", show(k.as_bytes()), m.version),
                    t,
                ),
                TxnEnd::Commit,
            )
        }
    }
}

// ── 计数器 ──────────────────────────────────────────────────────────────────

fn cmd_incr(tx: &dyn KvTxn, args: &[String], is_incr: bool) -> (Out, TxnEnd) {
    let (name, usage) = if is_incr { ("incr", "incr K [N]") } else { ("decr", "decr K [N]") };
    // 步长是可选参数，这里不能拿 `one` 卡恰好一个
    let k = match args {
        [k] | [k, _] => k.as_str(),
        _ => return (Out::err(format!("参数不对，用法: {usage}")), TxnEnd::Commit),
    };
    let delta: i64 = match args.get(1).map(|s| s.parse::<i64>()) {
        None => 1,
        Some(Ok(n)) => n,
        Some(Err(e)) => {
            return (Out::err(format!("{name} 的步长要是整数：{e}")), TxnEnd::Rollback);
        }
    };
    // decr(K, N) 走自己的门；负步长交给 i64 溢出判定
    let r = if is_incr { tx.incr(k.as_bytes(), delta) } else { tx.decr(k.as_bytes(), delta) };
    match r {
        Ok(n) => (Out::msg("OK", format!("{} = {n}", show(k.as_bytes()))), TxnEnd::Commit),
        Err(IncrError::NotInteger) => (
            Out::err(format!(
                "{} 的值不是十进制整数，incr/decr 只对计数器生效",
                show(k.as_bytes())
            )),
            TxnEnd::Rollback,
        ),
        Err(IncrError::Overflow) => (Out::err("i64 溢出"), TxnEnd::Rollback),
        Err(IncrError::WriteConflict) => {
            (Out::err("写入失败（可能是冲突、容量限制或存储错误）"), TxnEnd::Rollback)
        }
    }
}

// ── 批量 ────────────────────────────────────────────────────────────────────

fn cmd_mset(tx: &dyn KvTxn, args: &[String]) -> (Out, TxnEnd) {
    if args.is_empty() || args.len() % 2 != 0 {
        return (Out::err("mset 要偶数个参数: mset K V [K V ...]"), TxnEnd::Rollback);
    }
    let items: Vec<(Vec<u8>, Vec<u8>)> =
        args.chunks(2).map(|p| (p[0].as_bytes().to_vec(), p[1].as_bytes().to_vec())).collect();
    match tx.batch_set(&items) {
        Ok(()) => (Out::msg("OK", format!("OK（{} 键）", items.len())), TxnEnd::Commit),
        Err(bad) => (
            Out::err(format!(
                "写入失败于 {} —— 未提交部分已整体回滚（batch_set 本身不回滚，回滚是这一层做的）",
                show(&bad)
            )),
            TxnEnd::Rollback,
        ),
    }
}

fn cmd_mget(tx: &dyn KvTxn, args: &[String]) -> (Out, TxnEnd) {
    if args.is_empty() {
        return (Out::err("mget 要至少一个 key: mget K [K ...]"), TxnEnd::Commit);
    }
    let keys: Vec<Vec<u8>> = args.iter().map(|k| k.as_bytes().to_vec()).collect();
    let vals = tx.multi_get(&keys);
    let rows = keys
        .iter()
        .zip(&vals)
        .map(|(k, v)| vec![show(k), v.as_ref().map(|b| show(b)).unwrap_or_else(|| "(nil)".into())])
        .collect();
    let n = vals.iter().filter(|v| v.is_some()).count();
    let t = Table { columns: vec!["key".into(), "value".into()], rows };
    (Out::with_table("SCAN", format!("{} 键中 {} 个有值", keys.len(), n), t), TxnEnd::Commit)
}

// ── 扫描 / 搜索 ─────────────────────────────────────────────────────────────

/// 把一批记录折成 key/value 表；超过 [`SCAN_CAP`] 截断并在脚注说明。
/// 墓碑（`value=None`，只在 include_deleted 时出现）显示 `<deleted>`。
fn records_table(recs: Vec<ExportRecord>, what: &str) -> Out {
    let total = recs.len();
    let truncated = total > SCAN_CAP;
    let rows: Vec<Vec<String>> = recs
        .into_iter()
        .take(SCAN_CAP)
        .map(|r| {
            vec![
                show(&r.key),
                r.value.as_ref().map(|v| show(v)).unwrap_or_else(|| "<deleted>".into()),
            ]
        })
        .collect();
    let mut foot = format!("{what} {total} 行");
    if truncated {
        foot.push_str(&format!("（只显示前 {SCAN_CAP} 行，用范围/前缀收窄或 count 看总量）"));
    }
    let t = Table { columns: vec!["key".into(), "value".into()], rows };
    Out::with_table("SCAN", foot, t)
}

fn cmd_scan(tx: &dyn KvTxn, args: &[String], reverse: bool) -> (Out, TxnEnd) {
    if args.len() > 2 {
        return (
            Out::err("scan 至多两个参数: scan [START] [END]（区间 [START, END)，两端都可省）"),
            TxnEnd::Commit,
        );
    }
    let start = args.first().map(|s| s.as_bytes().to_vec());
    let end = args.get(1).map(|s| s.as_bytes().to_vec());
    let recs = if reverse {
        tx.reverse_scan(start.as_deref(), end.as_deref())
    } else {
        tx.scan(start.as_deref(), end.as_deref())
    };
    (records_table(recs, if reverse { "RSCAN" } else { "SCAN" }), TxnEnd::Commit)
}

fn cmd_prefix(tx: &dyn KvTxn, args: &[String]) -> (Out, TxnEnd) {
    let [p] = match one(args, "prefix P") {
        Ok(v) => v,
        Err(e) => return (e, TxnEnd::Commit),
    };
    let recs = tx.prefix_scan(p.as_bytes());
    // 分隔用 CJK 引号：前缀本身常以 `:` 结尾（如 user:），再接一个 `:` 会读成两个
    (
        records_table(recs, &format!("PREFIX「{}」", show(p.as_bytes()))),
        TxnEnd::Commit,
    )
}

fn cmd_seek(tx: &dyn KvTxn, args: &[String], prev: bool) -> (Out, TxnEnd) {
    let (name, usage) = if prev {
        ("seekprev", "seekprev K（最后一条 ≤ K）")
    } else {
        ("seek", "seek K（第一条 ≥ K）")
    };
    let [k] = match one(args, usage) {
        Ok(v) => v,
        Err(e) => return (e, TxnEnd::Commit),
    };
    let r = if prev { tx.seek_prev(k.as_bytes()) } else { tx.seek(k.as_bytes()) };
    match r {
        None => (Out::msg("SEEK", "(nil)"), TxnEnd::Commit),
        Some(rec) => {
            let t = Table {
                columns: vec!["key".into(), "value".into()],
                rows: vec![vec![
                    show(&rec.key),
                    rec.value.as_ref().map(|v| show(v)).unwrap_or_else(|| "<deleted>".into()),
                ]],
            };
            (
                Out::with_table("SEEK", format!("{name} → {}", show(&rec.key)), t),
                TxnEnd::Commit,
            )
        }
    }
}

fn cmd_count(tx: &dyn KvTxn, args: &[String]) -> (Out, TxnEnd) {
    if args.len() > 1 {
        return (Out::err("count 至多一个前缀: count [P]"), TxnEnd::Commit);
    }
    let p = args.first().map(|s| s.as_str()).unwrap_or("");
    let n = tx.key_count(p.as_bytes());
    let label: String = if p.is_empty() { "全库".into() } else { format!("前缀 {p}") };
    (Out::msg("OK", format!("count({label}) = {n}")), TxnEnd::Commit)
}

fn cmd_find(tx: &dyn KvTxn, args: &[String]) -> (Out, TxnEnd) {
    if args.is_empty() || args.len() > 3 {
        return (Out::err("find 要 1~3 个参数: find 子串 [页码] [每页条数]"), TxnEnd::Commit);
    }
    let page: usize = match args.get(1).map(|s| s.parse()) {
        None => 0,
        Some(Ok(n)) => n,
        Some(Err(_)) => return (Out::err("页码要非负整数"), TxnEnd::Commit),
    };
    let size: usize = match args.get(2).map(|s| s.parse()) {
        None => FIND_PAGE_SIZE,
        Some(Ok(0)) => FIND_PAGE_SIZE, // 传 0 引擎会按 1 处理 —— 归到默认页宽，少一个陷阱
        Some(Ok(n)) => n,
        Some(Err(_)) => return (Out::err("每页条数要正整数"), TxnEnd::Commit),
    };
    let q = SearchQuery {
        pattern: args[0].as_bytes().to_vec(),
        page,
        page_size: size,
        mode: KeyMatchMode::Contains,
        include_deleted: false,
        need_total: true,
    };
    let pg = tx.search_keys(&q);
    let rows: Vec<Vec<String>> = pg
        .items
        .iter()
        .map(|r| {
            vec![
                show(&r.key),
                r.value.as_ref().map(|v| show(v)).unwrap_or_else(|| "<deleted>".into()),
            ]
        })
        .collect();
    let n = pg.items.len();
    let total = pg.total.map(|t| t.to_string()).unwrap_or_else(|| "—".into());
    let pages = pg.total_pages.map(|t| t.to_string()).unwrap_or_else(|| "—".into());
    let mut foot =
        format!("FIND 第 {} 页 / 共 {pages} 页（本页 {n} 行，全库匹配 {total}）", pg.page);
    if pg.has_next {
        foot.push_str(&format!("；下一页: find {} {}", args[0], pg.page + 1));
    }
    let t = Table { columns: vec!["key".into(), "value".into()], rows };
    (Out::with_table("SCAN", foot, t), TxnEnd::Commit)
}

// ── TTL ─────────────────────────────────────────────────────────────────────

fn cmd_ttl(tx: &dyn KvTxn, args: &[String]) -> (Out, TxnEnd) {
    let [k] = match one(args, "ttl K") {
        Ok(v) => v,
        Err(e) => return (e, TxnEnd::Commit),
    };
    match tx.get_ttl(k.as_bytes()) {
        None => (Out::msg("TTL", "(nil)（键不存在或已过期）"), TxnEnd::Commit),
        Some(None) => (Out::msg("TTL", "永久（没设 TTL）"), TxnEnd::Commit),
        Some(Some(d)) => (Out::msg("TTL", format!("剩 {}s（{d:?}）", d.as_secs())), TxnEnd::Commit),
    }
}

fn cmd_refresh(tx: &dyn KvTxn, args: &[String]) -> (Out, TxnEnd) {
    let [k, secs] = match two(args, "refresh K 秒") {
        Ok(v) => v,
        Err(e) => return (e, TxnEnd::Commit),
    };
    let Ok(secs) = secs.parse::<u64>() else {
        return (Out::err(format!("TTL 要非负整数秒，收到 {secs:?}")), TxnEnd::Commit);
    };
    if tx.refresh_ttl(k.as_bytes(), std::time::Duration::from_secs(secs)) {
        (Out::msg("OK", format!("OK（从现在起 {secs}s）")), TxnEnd::Commit)
    } else {
        (Out::err("没设上（键不存在或已过期）"), TxnEnd::Commit)
    }
}

fn cmd_persist(tx: &dyn KvTxn, args: &[String]) -> (Out, TxnEnd) {
    let [k] = match one(args, "persist K") {
        Ok(v) => v,
        Err(e) => return (e, TxnEnd::Commit),
    };
    if tx.persist_key(k.as_bytes()) {
        (Out::msg("OK", "OK（改成永久）"), TxnEnd::Commit)
    } else {
        (Out::err("没改成（键不存在或已过期）"), TxnEnd::Commit)
    }
}

fn cmd_purge(tx: &dyn KvTxn) -> (Out, TxnEnd) {
    let n = tx.purge_expired();
    (Out::msg("MAINT", format!("清掉 {n} 个过期键")), TxnEnd::Commit)
}

// ── 事务 ────────────────────────────────────────────────────────────────────

fn cmd_begin<'e>(engine: &'e dyn KvEngine, txn: &mut Option<Box<dyn KvTxn + 'e>>) -> Out {
    if txn.is_some() {
        return Out::err("已有未提交事务（先 commit 或 rollback）");
    }
    let t = engine.begin();
    let v = t.version();
    *txn = Some(t);
    Out::msg("TXN", format!("BEGIN (version {v})"))
}

/// `commit=true` 提交，`false` 回滚 —— 两条只有动词不同，错误面是同一张。
fn cmd_finish(txn: &mut Option<Box<dyn KvTxn + '_>>, commit: bool) -> Out {
    let verb = if commit { "COMMIT" } else { "ROLLBACK" };
    match txn.take() {
        None => Out::err("没有开着的事务"),
        Some(t) => {
            let v = t.version();
            if commit {
                if let Err(e) = t.try_commit() { return Out::err(e.to_string()); }
            } else {
                t.rollback();
            }
            Out::msg("TXN", format!("{verb} (version {v})"))
        }
    }
}

// ── 维护面（不走事务，直接在引擎上） ────────────────────────────────────────

fn cmd_info(engine: &dyn KvEngine) -> Vec<Out> {
    let path =
        engine.path().map(|p| p.display().to_string()).unwrap_or_else(|| "（内存，无路径）".into());
    let dur = engine.durability_profile();
    let head = Out::msg(
        "INFO",
        format!("引擎: {}\n路径: {}\n持久性缺口: {dur}", engine.name(), path),
    );
    let t = engine.tuning();
    let table = Table {
        columns: vec!["旋钮".into(), "值".into()],
        rows: t.entries.iter().map(|e| vec![e.key.to_string(), e.value.clone()]).collect(),
    };
    vec![head, Out::with_table("INFO", format!("tuning（{} 项）", table.rows.len()), table)]
}

fn cmd_health(engine: &dyn KvEngine) -> Out {
    let h = engine.health();
    let t = Table {
        columns: vec!["指标".into(), "值".into()],
        rows: vec![
            vec!["物理行数（含旧版本与墓碑）".into(), h.total_rows.to_string()],
            vec!["逻辑键数".into(), h.live_keys.to_string()],
            vec!["最长版本链".into(), h.max_chain.to_string()],
            vec!["超一行多余行".into(), h.rows_above_one.to_string()],
            vec!["活跃事务数".into(), h.active_txns.to_string()],
        ],
    };
    Out::with_table("MAINT", "health（口径四家一致，可直接对比）".into(), t)
}

fn cmd_vacuum(engine: &dyn KvEngine) -> Out {
    match engine.vacuum() {
        Err(e) => Out::err(format!("vacuum 失败: {e}")),
        Ok(s) => {
            if s.skipped_active_txn {
                return Out::msg(
                    "MAINT",
                    format!(
                        "vacuum 整轮跳过：有 {} 个活跃事务挂着（挂起的事务会让回收不敢动）",
                        engine.health().active_txns
                    ),
                );
            }
            let mut m = format!(
                "裁掉 {} 个旧版本（xmin={}，扫了 {} 物理行",
                s.versions_removed, s.xmin, s.rows_scanned
            );
            match s.blob_rewritten {
                Some(true) => m.push_str("，blob 文件已重写"),
                Some(false) => m.push_str("，blob 无需重写"),
                None => {}
            }
            m.push('）');
            Out::msg("MAINT", m)
        }
    }
}

/// flush / checkpoint / merge：同签名同错误面，一个函数派发
fn cmd_engine_op(engine: &dyn KvEngine, op: &'static str) -> Out {
    let r = match op {
        "flush" => engine.flush(),
        "checkpoint" => engine.checkpoint(),
        _ => engine.merge(),
    };
    match r {
        Ok(()) => Out::msg("MAINT", format!("{op} OK")),
        Err(e) => Out::err(format!("{op} 失败: {e}")),
    }
}

// ── 终端命令 ────────────────────────────────────────────────────────────────

fn cmd_engine<'e>(
    engine: &'e dyn KvEngine,
    txn: &mut Option<Box<dyn KvTxn + 'e>>,
    args: &[String],
) -> LineOutcome {
    if args.is_empty() {
        let path =
            engine.path().map(|p| p.display().to_string()).unwrap_or_else(|| "（内存）".into());
        return LineOutcome::Outputs(vec![Out::msg(
            "ENGINE",
            format!("引擎: {}  路径: {path}", engine.name()),
        )]);
    }
    if args.len() > 2 {
        return LineOutcome::Outputs(vec![Out::err("engine 至多两个参数: engine 名 [目录]")]);
    }
    let Some(kind) = EngineKind::parse(&args[0]) else {
        return LineOutcome::Outputs(vec![Out::err(format!(
            "未知引擎 {:?}（可给 bitcask / bplus_tree / lsm_tree / memory，含 bplus、lsm、mem 等别名）",
            args[0]
        ))]);
    };
    if txn.is_some() {
        return LineOutcome::Outputs(vec![Out::err(
            "有未提交事务 —— 事务借用了引擎，先 commit 或 rollback 再切",
        )]);
    }
    LineOutcome::Switch { kind: kind.as_str(), dir: args.get(1).map(PathBuf::from) }
}

fn help_text() -> String {
    "\
命令表（key/value 含空格用引号包：\"a b\"）:
  get K                    读 K
  set K V                  写 K=V
  setex K 秒 V             写入并设 TTL
  del K                    逻辑删除（写墓碑）
  exists K                 判断存在
  meta K                   版本号 / 长度 / 删除标记 / TTL
  incr K [N] | decr K [N]  计数器（值须是十进制整数），默认步长 1
  mset K V [K V ...]       批量写（整批原子，冲突整体回滚）
  mget K [K ...]           批量读
  scan [起] [止]           范围扫描 [起, 止)，两端都可省；rscan 倒着扫
  prefix P                 按前缀扫
  seek K | seekprev K      第一条 ≥ K / 最后一条 ≤ K
  count [P]                键计数（可按前缀）
  find 子串 [页] [每页]    分页模糊找 key
  ttl K | refresh K 秒 | persist K | purge    TTL 四件套
  begin | commit | rollback   MVCC 事务：之后命令都走这条事务
  info | health | vacuum | flush | checkpoint | merge   维护面
  engine [名] [目录]       看当前引擎 / 切引擎（memory|bitcask|bplus_tree|lsm_tree）
  help | exit              本表 / 退出"
        .to_string()
}

// ── 参数小工具 ──────────────────────────────────────────────────────────────

/// 恰好 N 个参数；数不对就把用法当错误交回去（调用方 `match` 一行接住）。
fn one<'a>(args: &'a [String], usage: &str) -> Result<[&'a str; 1], Out> {
    match args {
        [k] => Ok([k.as_str()]),
        _ => Err(Out::err(format!("参数不对，用法: {usage}"))),
    }
}

fn two<'a>(args: &'a [String], usage: &str) -> Result<[&'a str; 2], Out> {
    match args {
        [a, b] => Ok([a.as_str(), b.as_str()]),
        _ => Err(Out::err(format!("参数不对，用法: {usage}"))),
    }
}

fn three<'a>(args: &'a [String], usage: &str) -> Result<[&'a str; 3], Out> {
    match args {
        [a, b, c] => Ok([a.as_str(), b.as_str(), c.as_str()]),
        _ => Err(Out::err(format!("参数不对，用法: {usage}"))),
    }
}

// ── 测试 ────────────────────────────────────────────────────────────────────
//
// 解释器是纯函数，直接在 memory 引擎上跑真命令断言输出 —— shell / web 两个外壳
// 只是输入输出，这里的断言就是它们俩共同的行为合同。

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::open_engine;
    use std::assert_eq;
    use std::option::Option;
    use std::string::String;

    fn mem() -> Box<dyn KvEngine> {
        open_engine("memory", None).expect("memory 引擎应能无路径打开")
    }

    /// 跑一行、拿输出（Switch / Exit 都折成空 —— 需要细分时直接调 run_line）
    fn outs<'e>(
        db: &'e dyn KvEngine,
        txn: &mut Option<Box<dyn KvTxn + 'e>>,
        line: &str,
    ) -> Vec<Out> {
        match run_line(db, txn, line) {
            LineOutcome::Outputs(v) => v,
            LineOutcome::Switch { .. } | LineOutcome::Exit => Vec::new(),
        }
    }

    /// 取第 i 条输出的表（没有表 / 行数不对都当场红）
    fn table(o: &[Out], i: usize, rows: usize) -> &Table {
        let t = o[i].table.as_ref().unwrap_or_else(|| panic!("第 {i} 条输出应有表: {:?}", o[i]));
        assert_eq!(t.rows.len(), rows, "第 {i} 条输出的行数不对: {:?}", t);
        &t
    }

    #[test]
    fn tokenize_keeps_quoted_whitespace_and_doubled_quotes() {
        assert_eq!(tokenize("set k \"a b\"  'c  d'"), vec!["set", "k", "a b", "c  d"]);
        // '' / "" 成对引号 = 字面引号，与引擎词法器同一套
        assert_eq!(tokenize(r#"set k "he said ""hi"""#), vec!["set", "k", "he said \"hi\""]);
        assert_eq!(tokenize("set k 'a''b'"), vec!["set", "k", "a'b"]);
        // 空行 / 全空白 → 没有 token
        assert!(tokenize("   ").is_empty());
        // 引号中段出现也照切（终端输入不追求 SQL 词法器的严格）
        assert_eq!(tokenize("ab\"cd ef\""), vec!["abcd ef"]);
    }

    #[test]
    fn set_get_roundtrip_and_nil() {
        let db = mem();
        let mut txn = None;
        let o = outs(&*db, &mut txn, "set name Alice");
        assert!(o[0].ok && o[0].message == "OK", "{o:?}");
        let o = outs(&*db, &mut txn, "get name");
        let t = table(&o, 0, 1);
        assert_eq!(t.rows[0], vec!["name", "Alice"]);
        // 不存在的键 → (nil)，不报错
        let o = outs(&*db, &mut txn, "get ghost");
        assert!(o[0].ok && o[0].message == "(nil)", "{o:?}");
        // 值里的空格要用引号包；非法 UTF-8 显示 <hex>
        let o = outs(&*db, &mut txn, "set city \"new york\"");
        assert!(o[0].ok, "{o:?}");
        let o = outs(&*db, &mut txn, "get city");
        assert_eq!(table(&o, 0, 1).rows[0][1], "new york");
    }

    #[test]
    fn arity_errors_carry_usage() {
        let db = mem();
        let mut txn = None;
        let o = outs(&*db, &mut txn, "get");
        assert!(!o[0].ok && o[0].message.contains("get K"), "{o:?}");
        let o = outs(&*db, &mut txn, "set onlykey");
        assert!(!o[0].ok && o[0].message.contains("set K V"), "{o:?}");
        let o = outs(&*db, &mut txn, "nosuchcmd 1");
        assert!(!o[0].ok && o[0].message.contains("未知命令"), "{o:?}");
    }

    #[test]
    fn incr_needs_integer_value() {
        let db = mem();
        let mut txn = None;
        outs(&*db, &mut txn, "set visits 10");
        let o = outs(&*db, &mut txn, "incr visits 5");
        assert!(o[0].ok && o[0].message == "visits = 15", "{o:?}");
        let o = outs(&*db, &mut txn, "decr visits");
        assert!(o[0].ok && o[0].message == "visits = 14", "{o:?}");
        // 默认步长 1、负步长也行
        let o = outs(&*db, &mut txn, "incr visits -4");
        assert!(o[0].ok && o[0].message == "visits = 10", "{o:?}");
        outs(&*db, &mut txn, "set name Alice");
        let o = outs(&*db, &mut txn, "incr name");
        assert!(!o[0].ok && o[0].message.contains("不是十进制整数"), "{o:?}");
    }

    #[test]
    fn mset_mget_count_scan_and_prefix() {
        let db = mem();
        let mut txn = None;
        let o = outs(&*db, &mut txn, "mset u:01 a u:02 b u:03 c");
        assert!(o[0].ok && o[0].message.contains("3 键"), "{o:?}");
        // 偶数个参数校验
        let o = outs(&*db, &mut txn, "mset lonely");
        assert!(!o[0].ok && o[0].message.contains("偶数"), "{o:?}");
        let o = outs(&*db, &mut txn, "mget u:01 ghost u:02");
        let t = table(&o, 0, 3);
        assert_eq!(t.rows[1], vec!["ghost", "(nil)"]);
        let o = outs(&*db, &mut txn, "count u:");
        assert!(o[0].message.contains("= 3"), "{o:?}");
        let o = outs(&*db, &mut txn, "count");
        assert!(o[0].message.contains("= 3"), "{o:?}");
        let o = outs(&*db, &mut txn, "prefix u:");
        assert_eq!(table(&o, 0, 3).rows[0][0], "u:01");
        let o = outs(&*db, &mut txn, "scan u:02");
        assert_eq!(table(&o, 0, 2).rows[0][0], "u:02", "scan START 应含 START");
        let o = outs(&*db, &mut txn, "rscan u:02");
        assert_eq!(table(&o, 0, 2).rows[0][0], "u:03", "rscan START 倒着走");
        // seek / seekprev 的边界语义
        let o = outs(&*db, &mut txn, "seek u:02x");
        assert_eq!(table(&o, 0, 1).rows[0][0], "u:03", "seek = 第一条 ≥ K");
        let o = outs(&*db, &mut txn, "seekprev u:02x");
        assert_eq!(table(&o, 0, 1).rows[0][0], "u:02", "seekprev = 最后一条 ≤ K");
        let o = outs(&*db, &mut txn, "seek zz");
        assert!(o[0].message == "(nil)", "{o:?}");
    }

    #[test]
    fn scan_caps_display_at_100_rows() {
        let db = mem();
        // 直接走门面灌 150 行（一条 mset 命令行也行，但 300 个 token 的断言没必要）
        let items: Vec<(Vec<u8>, Vec<u8>)> =
            (0..150).map(|i| (format!("big-{i:03}").into_bytes(), b"v".to_vec())).collect();
        let tx = db.begin();
        tx.batch_set(&items).unwrap();
        tx.commit();
        let mut txn = None;
        let o = outs(&*db, &mut txn, "scan");
        let t = o[0].table.as_ref().expect("scan 应有表");
        assert_eq!(t.rows.len(), SCAN_CAP, "显示要封顶");
        assert!(o[0].message.contains("150 行") && o[0].message.contains("只显示前 100"), "{:?}", o[0].message);
    }

    #[test]
    fn find_paginates_with_total_and_next_hint() {
        let db = mem();
        let mut txn = None;
        let mut pairs = String::new();
        for i in 0..25 {
            pairs.push_str(&format!("u:{i:02} v{i:02} "));
        }
        let o = outs(&*db, &mut txn, &format!("mset {pairs}"));
        assert!(o[0].ok, "{o:?}");
        let o = outs(&*db, &mut txn, "find u:");
        let t = table(&o, 0, 20);
        assert_eq!(t.rows[0][0], "u:00");
        assert!(o[0].message.contains("全库匹配 25") && o[0].message.contains("下一页: find u: 1"), "{:?}", o[0].message);
        // 翻到尾页：5 行、没有下一页提示
        let o = outs(&*db, &mut txn, "find u: 1");
        assert_eq!(table(&o, 0, 5).rows[0][0], "u:20");
        assert!(!o[0].message.contains("下一页"), "{:?}", o[0].message);
    }

    #[test]
    fn checked_terminal_commit_failures_are_errors() {
        for explicit in [false, true] {
            let dir = crate::common::ScratchPath::new("terminal_commit", "bitcask", "");
            let db = crate::open_engine("bitcask", Some(dir.as_ref())).unwrap();
            let mut txn = None;
            if explicit {
                assert!(outs(&*db, &mut txn, "begin")[0].ok);
                assert!(outs(&*db, &mut txn, "set k v")[0].ok);
            }
            crate::common::wal::fail_next_commit(1);
            let command = if explicit { "commit" } else { "set k v" };
            let result = outs(&*db, &mut txn, command);
            assert!(!result[0].ok && result[0].message.contains("indeterminate"), "{result:?}");
            assert!(txn.is_none());
            let read = db.begin();
            assert_eq!(read.get(b"k"), None);
        }
    }

    #[test]
    fn txn_begin_commit_and_rollback_roundtrip() {
        let db = mem();
        let mut txn = None;
        // commit 没开事务要红
        let o = outs(&*db, &mut txn, "commit");
        assert!(!o[0].ok && o[0].message.contains("没有开着的事务"), "{o:?}");
        // 开事务后命令都走它；自己写自己读可见
        let o = outs(&*db, &mut txn, "begin");
        assert!(o[0].ok && o[0].message.starts_with("BEGIN (version "), "{o:?}");
        let o = outs(&*db, &mut txn, "begin");
        assert!(!o[0].ok && o[0].message.contains("已有未提交事务"), "{o:?}");
        let o = outs(&*db, &mut txn, "set txkey txval");
        assert!(o[0].ok, "{o:?}");
        let o = outs(&*db, &mut txn, "get txkey");
        assert_eq!(table(&o, 0, 1).rows[0][1], "txval");
        // rollback 丢弃；挂起的事务也让 vacuum 不敢动
        let o = outs(&*db, &mut txn, "rollback");
        assert!(o[0].ok && o[0].message.starts_with("ROLLBACK"), "{o:?}");
        let o = outs(&*db, &mut txn, "get txkey");
        assert!(o[0].message == "(nil)", "rollback 后不该读到: {o:?}");
        // commit 落地
        let _ = outs(&*db, &mut txn, "begin");
        let _ = outs(&*db, &mut txn, "set txkey2 v2");
        let o = outs(&*db, &mut txn, "commit");
        assert!(o[0].ok && o[0].message.starts_with("COMMIT"), "{o:?}");
        let o = outs(&*db, &mut txn, "get txkey2");
        assert_eq!(table(&o, 0, 1).rows[0][1], "v2");
    }

    #[test]
    fn vacuum_skips_while_txn_pending() {
        let db = mem();
        let mut txn = None;
        let _ = outs(&*db, &mut txn, "begin");
        let o = outs(&*db, &mut txn, "vacuum");
        assert!(o[0].ok && o[0].message.contains("整轮跳过"), "{o:?}");
        let _ = outs(&*db, &mut txn, "commit");
    }

    #[test]
    fn engine_switch_blocked_by_pending_txn_and_parses_names() {
        let db = mem();
        let mut txn = None;
        // 无参 = 显示当前
        let o = outs(&*db, &mut txn, "engine");
        assert!(o[0].ok && o[0].message.contains("memory"), "{o:?}");
        // 未知引擎
        let o = match run_line(&*db, &mut txn, "engine nosuch") {
            LineOutcome::Outputs(v) => v,
            other => panic!("应为输出: {other:?}"),
        };
        assert!(!o[0].ok && o[0].message.contains("未知引擎"), "{o:?}");
        // 有挂起事务 → 拒绝切换（事务借用引擎）
        let _ = outs(&*db, &mut txn, "begin");
        let o = outs(&*db, &mut txn, "engine lsm");
        assert!(!o[0].ok && o[0].message.contains("未提交事务"), "{o:?}");
        let _ = outs(&*db, &mut txn, "commit");
        // 干净时 → Switch 结果带规范名；别名也认
        match run_line(&*db, &mut txn, "engine lsm") {
            LineOutcome::Switch { kind, dir } => {
                assert_eq!(kind, "lsm_tree");
                assert!(dir.is_none(), "没给目录就交给外壳按默认来");
            }
            other => panic!("应为 Switch: {other:?}"),
        }
        match run_line(&*db, &mut txn, "engine bplus ./my-db") {
            LineOutcome::Switch { kind, dir } => {
                assert_eq!(kind, "bplus_tree");
                assert_eq!(dir, Some(PathBuf::from("./my-db")));
            }
            other => panic!("应为 Switch: {other:?}"),
        }
    }

    #[test]
    fn meta_ttl_persist_and_exit() {
        let db = mem();
        let mut txn = None;
        let _ = outs(&*db, &mut txn, "setex temp 60 hot");
        let o = outs(&*db, &mut txn, "ttl temp");
        assert!(o[0].ok && o[0].message.contains("剩 60s"), "{o:?}");
        let o = outs(&*db, &mut txn, "meta temp");
        let t = table(&o, 0, 6);
        assert_eq!(t.rows[4][1], "false", "没到点不算过期");
        let o = outs(&*db, &mut txn, "refresh temp 90");
        assert!(o[0].ok, "{o:?}");
        let o = outs(&*db, &mut txn, "ttl temp");
        assert!(o[0].message.contains("剩 90s"), "{o:?}");
        let o = outs(&*db, &mut txn, "persist temp");
        assert!(o[0].ok, "{o:?}");
        let o = outs(&*db, &mut txn, "ttl temp");
        assert!(o[0].message.contains("永久"), "{o:?}");
        let o = outs(&*db, &mut txn, "ttl ghost");
        assert!(o[0].message.contains("(nil)"), "{o:?}");
        // exit / quit → Exit
        assert!(matches!(run_line(&*db, &mut txn, "exit"), LineOutcome::Exit));
        assert!(matches!(run_line(&*db, &mut txn, "q"), LineOutcome::Exit));
        // 空行 → 空输出
        assert!(outs(&*db, &mut txn, "   ").is_empty());
    }
}
