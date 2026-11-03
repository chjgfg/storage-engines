//! 交互式 KV 终端（REPL）：一行一条命令，命令解释器是 [`storage_engines::terminal`] 那份。
//!
//! ```text
//! cargo run --bin shell                        # memory，不落盘
//! cargo run --bin shell -- -e lsm              # ./data/lsm_tree
//! cargo run --bin shell -- -e bitcask -d ./my-db
//! cargo run --bin shell -- -f                  # 只清默认路径 ./data/memory 再开
//! ```
//!
//! 会话里也能切引擎（`engine <名> [目录]`），换的只是解释器背后的那个引擎，
//! 命令面一个字不变 —— 门面层的意义就在这。
//!
//! 参数：
//! - `-e, --engine <名>`  memory | bitcask | bplus_tree | lsm_tree（含别名，默认 memory）
//! - `-d, --dir <目录>`   盘引擎的库目录（默认 `./data/<引擎名>`）
//! - `-f, --fresh`        启动前清掉默认路径的旧库；**只对默认路径生效**，
//!                        自己 `-d` 给的目录不碰（防手滑删错地方，要删请手动删）
//! - `-h, --help`         帮助

use std::format;
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::process::exit;
use std::string::{String, ToString};
use std::time::Instant;
use std::vec::Vec;

use storage_engines::engine::{open_engine, KvEngine};
use storage_engines::terminal::{self, LineOutcome, Out, Table};

// ── 参数 ────────────────────────────────────────────────────────────────────

struct Opts {
    /// 引擎名原文（规范名在 open 时由门面再校一遍）
    engine: String,
    dir: Option<PathBuf>,
    fresh: bool,
}

pub fn usage() -> String {
    "\
用法: shell [-e 引擎] [-d 目录] [-f] [-h]
  -e, --engine <名>   memory | bitcask | bplus_tree | lsm_tree（含别名，默认 memory）
  -d, --dir <目录>    盘引擎的库目录（默认 ./data/<引擎名>）
  -f, --fresh         启动前清掉默认路径 ./data/<引擎名> 的旧库（-d 给的目录不碰）
  -h, --help          本帮助

一行一条命令，help 看命令表，exit 退出。"
        .to_string()
}

/// 吃 `argv`（不含程序名）。带值旗标缺值即报错 —— 与 export-kv / import-kv 同一约定。
fn parse_args(argv: &[String]) -> Result<Opts, String> {
    let mut o = Opts { engine: "memory".to_string(), dir: None, fresh: false };
    let mut i = 0;
    while i < argv.len() {
        let flag = argv[i].clone();
        let value = |i: &mut usize| -> Result<String, String> {
            *i += 1;
            argv.get(*i).cloned().ok_or_else(|| format!("{flag} 后面缺值"))
        };
        match flag.as_str() {
            "-e" | "--engine" => o.engine = value(&mut i)?,
            "-d" | "--dir" => o.dir = Some(PathBuf::from(value(&mut i)?)),
            "-f" | "--fresh" => o.fresh = true,
            "-h" | "--help" => return Err(usage()),
            other => return Err(format!("未知参数: {other}\n{}", usage())),
        }
        i += 1;
    }
    Ok(o)
}

// ── 引擎开关（启动与 `engine` 切换共用） ────────────────────────────────────

/// 引擎的默认库目录。memory 不落盘，给 `None`。
fn default_dir(kind: &str) -> Option<PathBuf> {
    (kind != "memory").then(|| PathBuf::from("./data").join(kind))
}

/// open 前把目录建出来。B+ 树的「目录」其实是库文件本身 ⇒ 只建它的父目录
/// （与 export-kv 的 seed_demo 同一条规矩）。
fn ensure_dirs(kind: &str, dir: &Path) {
    let target = if kind == "bplus_tree" { dir.parent().unwrap_or(dir) } else { dir };
    if let Err(e) = std::fs::create_dir_all(target) {
        eprintln!("建目录 {} 失败: {e}", target.display());
        exit(1);
    }
}

/// 打开引擎（`dir` 缺省走默认路径）。出错给中文一句话，调用方决定退出还是继续。
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

// ── 输出渲染 ────────────────────────────────────────────────────────────────

/// 显示宽度：CJK 与全角字符按 2 列算，其余 1 列。手写这一个函数就够 ——
/// 为一张对齐表引入 unicode-width 不值。
fn display_width(s: &str) -> usize {
    s.chars()
        .map(|c| {
            let u = c as u32;
            let wide = (0x2E80..=0x9FFF).contains(&u)   // CJK 部首/音标/汉字
                || (0xF900..=0xFAFF).contains(&u)       // CJK 兼容表意
                || (0xFF00..=0xFF60).contains(&u)       // 全角形式
                || (0xFFE0..=0xFFE6).contains(&u);      // 全角符号
            if wide { 2 } else { 1 }
        })
        .sum()
}

/// 结果表 → 对齐文本。列宽取每列所有单元格显示宽的最大值。
fn render_table(t: &Table) -> String {
    let ncols = t.columns.len().max(t.rows.iter().map(|r| r.len()).max().unwrap_or(0));
    let mut widths = vec![0usize; ncols];
    for (i, h) in t.columns.iter().enumerate() {
        widths[i] = widths[i].max(display_width(h));
    }
    for row in &t.rows {
        for (i, c) in row.iter().enumerate() {
            if i < ncols {
                widths[i] = widths[i].max(display_width(c));
            }
        }
    }
    let pad = |s: &str, w: usize| {
        let gap = w.saturating_sub(display_width(s));
        format!("{s}{}", " ".repeat(gap))
    };
    let mut lines = Vec::new();
    if !t.columns.is_empty() {
        let head: Vec<String> =
            t.columns.iter().enumerate().map(|(i, h)| pad(h, widths[i])).collect();
        lines.push(head.join(" | "));
        let sep: Vec<String> = widths.iter().map(|w| "-".repeat((*w).max(1))).collect();
        lines.push(sep.join("-+-"));
    }
    for row in &t.rows {
        let line: Vec<String> = (0..ncols)
            .map(|i| pad(row.get(i).map(String::as_str).unwrap_or(""), widths[i]))
            .collect();
        lines.push(line.join(" | "));
    }
    lines.join("\n")
}

/// 把一条输出画到 stdout：消息先走（多行消息原样），表格随后。
fn print_out(o: &Out) {
    if !o.message.is_empty() {
        println!("{}", o.message);
    }
    if let Some(t) = &o.table {
        println!("{}", render_table(t));
    }
}

// ── REPL ────────────────────────────────────────────────────────────────────

fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let opts = match parse_args(&argv) {
        Ok(o) => o,
        Err(e) => {
            eprintln!("{e}");
            exit(2);
        }
    };
    if let Err(e) = run(opts) {
        eprintln!("致命错误: {e}");
        exit(1);
    }
}

fn run(opts: Opts) -> Result<(), String> {
    // --fresh 只清默认路径：用户自己指的目录不碰。宁可不做事，也不做删错目录的事。
    if opts.fresh {
        match (&default_dir(&opts.engine), &opts.dir) {
            (Some(d), None) => match std::fs::remove_dir_all(d) {
                Ok(()) => eprintln!("已清空 {}", d.display()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(format!("清空 {} 失败: {e}", d.display())),
            },
            (None, _) => return Err("--fresh 只对有默认目录的盘引擎生效（memory 无目录）".into()),
            (Some(_), Some(d)) => {
                return Err(format!(
                    "--fresh 只清默认路径；你自己给的目录 {} 请手动删（防手滑）",
                    d.display()
                ));
            }
        }
    }

    let mut db = open(&opts.engine, opts.dir.as_deref())?;
    print_banner(db.as_ref());

    // 挂起事务由这里持有：begin 之后所有命令都走它，直到 commit / rollback。
    // ⚠️ 不要给类型标注：Box<dyn KvTxn> 的省略界是 'static，而事务借用引擎；
    //    留给推断去绑 db 的借用区（`engine <名>` 切引擎前后各借一次也能对上）。
    let mut pending = None;

    let stdin = std::io::stdin();
    let mut stdin = stdin.lock();
    let mut stdout = std::io::stdout();
    let mut buffer = String::new();

    loop {
        print!("kv:{}> ", db.name());
        stdout.flush().map_err(|e| format!("写 stdout 失败: {e}"))?;
        buffer.clear();
        let n = stdin.read_line(&mut buffer).map_err(|e| format!("读 stdin 失败: {e}"))?;
        if n == 0 {
            // EOF (Ctrl+D / Ctrl+Z)：与 exit 同一条收尾路径
            println!();
            break;
        }
        let line = buffer.trim_end();
        if line.trim().is_empty() {
            continue;
        }

        let t0 = Instant::now();
        match terminal::run_line(&*db, &mut pending, line) {
            LineOutcome::Outputs(outputs) => {
                for o in &outputs {
                    print_out(o);
                }
            }
            LineOutcome::Switch { kind, dir } => {
                // 能走到这里说明没有挂起事务（解释器挡了），先把类型层的借位清掉再换引擎
                pending = None;
                match open(kind, dir.as_deref()) {
                    Ok(new_db) => {
                        db = new_db;
                        match db.path() {
                            Some(p) => eprintln!("已切到 {}（路径: {}）", db.name(), p.display()),
                            None => eprintln!("已切到 {}（纯内存）", db.name()),
                        }
                    }
                    Err(e) => eprintln!("{e}（还在 {} 上）", db.name()),
                }
            }
            LineOutcome::Exit => break,
        }
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        eprintln!("({ms:.2} ms)");
    }

    // 退出时还有挂起事务：回滚并说一声 —— commit 是调用者显式的动作，退出不该替他提交
    if let Some(t) = pending.take() {
        t.rollback();
        eprintln!("（退出时还有未提交事务，已回滚）");
    }
    println!("再见。");
    Ok(())
}

fn print_banner(db: &dyn KvEngine) {
    match db.path() {
        Some(p) => eprintln!("引擎: {}  路径: {}（help 看命令表，exit 退出）", db.name(), p.display()),
        None => eprintln!("引擎: {}（纯内存，进程退出即丢；help 看命令表，exit 退出）", db.name()),
    }
}
