//! Web 版 KV 终端（axum）：把命令解释器交给 HTTP，页面是 DOS 风格终端。
//!
//! ```text
//! cargo run --bin web                       # memory，浏览器开 http://localhost:8080/
//! cargo run --bin web -- -e lsm --port 8080 # 盘引擎，库在 ./data/web/lsm_tree
//! ```
//!
//! 路由：
//! - `GET  /`        返回 `static/index.html`（编译期嵌进 binary）
//! - `POST /execute` body 为 `{"input":"..."}`，**按行切分**逐条跑，返回 JSON
//!
//! 与 sql-engine 的 web 版同构，差异有两处：
//! 1. 语句切分按**行**而不是分号 —— KV 的值里分号太常见，按分号切是给自己埋雷；
//! 2. 多一个**引擎切换**：`engine <名>` 命令在服务端把 `Box<dyn KvEngine>` 整个换掉
//!    （解释器挡住了带挂起事务的切换），响应里的 `engine` 字段让前端同步下拉框。
//!    盘引擎默认目录 `./data/web/<引擎名>`，与 shell 的 `./data/<引擎名>` 互不踩。

use std::format;
use std::path::{Path, PathBuf};
use std::process::exit;
use std::string::{String, ToString};
use std::sync::{Arc, Mutex};
use std::vec::Vec;

use axum::Router;
use axum::extract::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use serde::Deserialize;
use serde_json::{Value, json};

use storage_engines::engine::{KvEngine, open_engine};
use storage_engines::terminal::{self, LineOutcome, Out};

// 编译期把页面嵌进 binary（src/bin/ 上两级到 crate 根再进 static/）
const INDEX_HTML: &str = include_str!("../../static/index.html");

/// 共享的引擎。所有请求共用一个引擎实例（`engine` 命令可以整个换掉它），
/// `Mutex` 提供串行 —— 同一时刻只跑一批命令，与 shell 的逐行执行一致。
type Db = Arc<Mutex<Box<dyn KvEngine>>>;

// ── 启动参数 ────────────────────────────────────────────────────────────────

struct Opts {
    engine: String,
    dir: Option<PathBuf>,
    port: u16,
}

pub fn usage() -> String {
    "\
用法: web [-e 引擎] [-d 目录] [-p 端口] [-h]
  -e, --engine <名>   memory | bitcask | bplus_tree | lsm_tree（含别名，默认 memory）
  -d, --dir <目录>    盘引擎的库目录（默认 ./data/web/<引擎名>）
  -p, --port <端口>   监听端口（默认 8080）
  -h, --help          本帮助"
        .to_string()
}

/// 与 shell 的 parse_args 同一手写游标风格（多个 -p / --port）
fn parse_args(argv: &[String]) -> Result<Opts, String> {
    let mut o = Opts {
        engine: "memory".to_string(),
        dir: None,
        port: 8083,
    };
    let mut i = 0;
    while i < argv.len() {
        let flag = argv[i].clone();
        let value = |i: &mut usize| -> Result<String, String> {
            *i += 1;
            argv.get(*i)
                .cloned()
                .ok_or_else(|| format!("{flag} 后面缺值"))
        };
        match flag.as_str() {
            "-e" | "--engine" => o.engine = value(&mut i)?,
            "-d" | "--dir" => o.dir = Some(PathBuf::from(value(&mut i)?)),
            "-p" | "--port" => {
                let v = value(&mut i)?;
                o.port = v
                    .parse()
                    .map_err(|_| format!("--port 要 1~65535 的整数，收到 {v:?}"))?;
            }
            "-h" | "--help" => return Err(usage()),
            other => return Err(format!("未知参数: {other}\n{}", usage())),
        }
        i += 1;
    }
    Ok(o)
}

// ── 引擎开关（启动与 `engine` 切换共用） ────────────────────────────────────

/// web 的默认库目录：./data/web/<引擎名>，与 shell 的 ./data/<引擎名> 互不踩
fn default_dir(kind: &str) -> Option<PathBuf> {
    (kind != "memory").then(|| PathBuf::from("./data/web").join(kind))
}

/// open 前把目录建出来（B+ 树只建父目录，与 export-kv / shell 同一条规矩）
fn ensure_dirs(kind: &str, dir: &Path) {
    let target = if kind == "bplus_tree" {
        dir.parent().unwrap_or(dir)
    } else {
        dir
    };
    if let Err(e) = std::fs::create_dir_all(target) {
        eprintln!("建目录 {} 失败: {e}", target.display());
        exit(1);
    }
}

fn open(kind: &str, dir: Option<&Path>) -> Result<Box<dyn KvEngine>, String> {
    let d = match dir {
        Some(d) => Some(d.to_path_buf()),
        None => default_dir(kind),
    };
    if let Some(d) = &d {
        ensure_dirs(kind, d);
    }
    open_engine(kind, d.as_deref()).map_err(|e| format!("打开 {kind} 失败: {e}"))
}

// ── HTTP 层 ─────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let opts = match parse_args(&argv) {
        Ok(o) => o,
        Err(e) => {
            eprintln!("{e}");
            exit(2);
        }
    };
    let db: Db = match open(&opts.engine, opts.dir.as_deref()) {
        Ok(b) => Arc::new(Mutex::new(b)),
        Err(e) => {
            eprintln!("{e}");
            exit(1);
        }
    };
    let name = db.lock().expect("刚建好的锁不该毒").name().to_string();
    eprintln!("引擎: {name}（网页里可用 engine <名> 随时切）");

    let app = Router::new()
        .route("/", get(index_page))
        .route("/execute", post(execute))
        .with_state(db);

    let socket = format!("0.0.0.0:{}", opts.port);
    eprintln!("监听 {socket}，浏览器打开 http://localhost:{}/", opts.port);
    let listener = tokio::net::TcpListener::bind(&socket).await.expect("bind");
    axum::serve(listener, app).await.expect("serve");
}

/// 页面。
async fn index_page() -> impl IntoResponse {
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/html; charset=utf-8")
        .header("cache-control", "no-store")
        .body(INDEX_HTML.to_owned())
        .unwrap()
}

/// 请求体：一段输入（可多行，一行一条命令）。
#[derive(Deserialize)]
struct ExecBody {
    input: String,
}

/// 一批命令跑完后的去向：`Switch` 交回外层换引擎 —— 为什么不在循环里直接换：
/// 挂起事务的类型里带着引擎的借用区，事务只要还可能被用，那个借用就活着；
/// 把「开事务 → 跑 → 收尾」整段关进 [`run_batch`] 的函数体里，返回时借用一并交还，
/// 外层再动 `*guard` 借用检查才肯放行。
enum BatchFlow {
    Done,
    Switch {
        kind: &'static str,
        dir: Option<PathBuf>,
    },
}

/// 跑一批命令（到行尽 / `exit` / `engine` 切换为止），返回去向与吃掉的行数。
/// 请求内开的事务在这里收尾：批末还开着就自动提交并说一声 —— 不能留，
/// 事务借用的锁这批结束就还了。
fn run_batch(
    engine: &dyn KvEngine,
    lines: &[String],
    results: &mut Vec<Value>,
    all_ok: &mut bool,
) -> (BatchFlow, usize) {
    let mut pending = None;
    for (i, line) in lines.iter().enumerate() {
        let t0 = std::time::Instant::now();
        match terminal::run_line(engine, &mut pending, line) {
            LineOutcome::Outputs(outputs) => {
                for o in &outputs {
                    let mut v = out_to_json(o);
                    v["ms"] = json!(t0.elapsed().as_secs_f64() * 1000.0);
                    if !o.ok {
                        *all_ok = false;
                    }
                    results.push(v);
                }
            }
            // ⚠️ consumed 必须含本行（i + 1）：外层从 idx+consumed 接着跑，
            // 少 1 就会重跑同一条 engine 命令 —— 而目录已被自己刚开的引擎锁住，
            // 第二次 open 必 err，外层的 loop 就是死循环。
            LineOutcome::Switch { kind, dir } => return (BatchFlow::Switch { kind, dir }, i + 1),
            LineOutcome::Exit => {
                // 挂起的事务不能就这么丢：引擎 Drop 会兜底自动回滚，但那只是日志里的事，
                // 这里显式回滚并说出口
                let txn_note = if let Some(t) = pending.take() {
                    t.rollback();
                    "（挂起事务已回滚）"
                } else {
                    ""
                };
                let left = lines.len() - i - 1;
                let msg = if left > 0 {
                    format!(
                        "（web 终端没有会话可退 —— 忽略 exit，后面还有 {left} 行未执行）{txn_note}"
                    )
                } else {
                    format!("（web 终端没有会话可退 —— 忽略 exit）{txn_note}")
                };
                results.push(json!({ "ok": true, "type": "EXIT", "message": msg }));
                return (BatchFlow::Done, i + 1);
            }
        }
    }
    if let Some(t) = pending.take() {
        let v = t.version();
        match t.try_commit() {
            Ok(()) => results.push(json!({
                "ok": true,
                "type": "TXN",
                "message": format!("（请求结束还有未提交事务，已自动提交，version {v}）"),
            })),
            Err(e) => {
                *all_ok = false;
                results.push(json!({ "ok": false, "type": "ERROR", "message": e.to_string() }));
            }
        }
    }
    (BatchFlow::Done, lines.len())
}

/// 执行一批命令：加锁 → 按行切分逐条跑（`engine` 切换就当场换共享引擎）→ 收集结果。
/// 某条失败不中断后续（解释器给错误 Out，不 panic）；`exit` 只结束本批。
async fn execute(State(db): State<Db>, Json(body): Json<ExecBody>) -> Response {
    let mut guard = db.lock().expect("session poisoned");
    let lines = split_lines(&body.input);
    let mut results: Vec<Value> = Vec::with_capacity(lines.len());
    let mut all_ok = true;

    let mut idx = 0;
    loop {
        let (flow, consumed) = run_batch(guard.as_ref(), &lines[idx..], &mut results, &mut all_ok);
        idx += consumed;
        match flow {
            BatchFlow::Done => break,
            BatchFlow::Switch { kind, dir } => match open(kind, dir.as_deref()) {
                Ok(new_db) => {
                    *guard = new_db;
                    results.push(json!({
                        "ok": true,
                        "type": "ENGINE",
                        "message": format!("已切到 {}", guard.name()),
                    }));
                }
                Err(e) => {
                    all_ok = false;
                    results.push(json!({
                        "ok": false,
                        "type": "ERROR",
                        "message": format!("{e}（还在 {} 上）", guard.name()),
                    }));
                }
            },
        }
    }

    let engine = guard.name().to_string();
    let body = json!({
        "ok": all_ok,
        "count": results.len(),
        "engine": engine,
        "results": results,
    });
    (StatusCode::OK, axum::Json(body)).into_response()
}

/// `Out` → 前端友好的 JSON：表格摊平成 `columns/rows`（与 sql-engine 的 SELECT 同形）。
fn out_to_json(o: &Out) -> Value {
    let mut v = json!({
        "ok": o.ok,
        "type": o.kind,
        "message": o.message,
    });
    if let Some(t) = &o.table {
        v["columns"] = json!(t.columns);
        v["rows"] = json!(t.rows);
    }
    v
}

/// 按行切分：一行一条命令。容忍 \r\n（浏览器/剪贴板常见），空行跳过。
fn split_lines(input: &str) -> Vec<String> {
    input
        .split('\n')
        .map(str::trim_end)
        .filter(|l| !l.trim().is_empty())
        .map(String::from)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::split_lines;
    use std::assert_eq;
    use std::vec;
    use std::vec::Vec;

    #[test]
    fn splits_by_line_and_tolerates_crlf_and_blanks() {
        let got = split_lines("set a 1\r\n\r\n  \nget a\n");
        assert_eq!(got, vec!["set a 1", "get a"]);
        assert!(split_lines("").is_empty());
        assert!(split_lines("\n\n").is_empty());
    }

    /// 值里的分号不切 —— KV 的值什么都可能有，按分号切是埋雷
    #[test]
    fn keeps_semicolons_inside_values() {
        let got = split_lines("set k \"a;b\"");
        assert_eq!(got, vec!["set k \"a;b\""]);
    }
}
