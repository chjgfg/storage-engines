//! 统一导入 CLI：`--engine <家> --dir <库目录> --in <文件> [--format bin|jsonl] [--wipe]`
//!
//! 与 [`export_kv`] 同理：四家原先各有一份，调的都是同一个 `tools::import_from_dir` ⇒ 只留一份外壳。
//!
//! 语义（**覆盖**，不是补齐）：文件里有的键直接写进去（同键后到者胜），目标里文件没有的键**不删**。
//! 要"整库换掉"加 `--wipe`（先清目标库文件）。
//! 要跨库**补齐**（"目标已有的一个字不动"）请走库对库的 `tools::sync` —— 它有墓碑判据与 TTL 传递，
//! 这个文件口子今天没有（见下）。
//!
//! ⚠️ 两条如实的能力边界：
//! ① 快照文件里今天**没有 TTL**（导出侧交的是用户字节，`BPEXP001`/`002` 也没有放 expire 的位置）。
//!    `BPEXP003`（`kv_snapshot::BIN_MAGIC_V3`）与 JSONL 的可选 `expire` 字段已经就位、读侧三家版本
//!    都认，缺的是导出侧往里填 ⇒ 导出去再导回来的键一律按永久处理。
//! ② 这个口子不认墓碑做"见过"判定 ⇒ 想同步又不复活已删键，别拿文件导入当 `sync` 用。
//!
//! ```text
//! cargo run --release --bin import-kv -- --engine lsm_tree --dir ./data/lsm_tree --in snap.bin
//! cargo run --release --bin import-kv -- --engine bplus_tree --dir ./data/bplus --in snap.jsonl --format jsonl --wipe
//! ```

use std::env;
use std::path::PathBuf;
use std::println;

use storage_engines::EngineKind;
use storage_engines::tools::migrate::{SnapshotFormat, import_from_dir};

fn usage() -> ! {
    eprintln!(
        "用法: import-kv --engine <bitcask|bplus_tree|lsm_tree|memory> --dir <库目录> --in <文件> \
         [--format bin|jsonl] [--wipe]"
    );
    std::process::exit(2);
}

pub fn main() {
    let argv: Vec<String> = env::args().skip(1).collect();
    let mut engine = String::new();
    let mut dir = PathBuf::new();
    let mut input = PathBuf::new();
    let mut format = SnapshotFormat::Bin;
    let mut wipe = false;
    let mut i = 0;
    while i < argv.len() {
        // 带值参数：吃掉下一个；缺值就直接报用法，不许静默按默认值跑（那会把运维脚本引到错库上）
        let next = |i: &mut usize| -> Option<String> {
            *i += 1;
            argv.get(*i).cloned()
        };
        let flag = argv[i].as_str();
        let value = match flag {
            "--engine" | "--dir" | "--in" | "--format" => Some(next(&mut i).unwrap_or_default()),
            "--wipe" => Some(String::new()),
            _ => None,
        };
        match (flag, value) {
            ("--engine", Some(v)) => engine = v,
            ("--dir", Some(v)) => dir = PathBuf::from(v),
            ("--in", Some(v)) | ("--file", Some(v)) => input = PathBuf::from(v),
            ("--format", Some(v)) => match v.as_str() {
                "jsonl" => format = SnapshotFormat::Jsonl,
                "bin" => format = SnapshotFormat::Bin,
                other => {
                    eprintln!("未知 --format {other}（只认 bin / jsonl）");
                    std::process::exit(2);
                }
            },
            ("--wipe", _) => wipe = true,
            ("-h" | "--help", _) => usage(),
            (other, _) => {
                eprintln!("未知参数: {other}");
                usage();
            }
        }
        i += 1;
    }
    if engine.is_empty() || dir.as_os_str().is_empty() || input.as_os_str().is_empty() {
        usage();
    }
    let kind = match EngineKind::parse(&engine) {
        Some(k) => k,
        None => {
            eprintln!("未知引擎: {engine}");
            std::process::exit(2);
        }
    };
    match import_from_dir(kind, &dir, &input, format, wipe) {
        Err(e) => {
            eprintln!("导入失败: {e}");
            std::process::exit(1);
        }
        Ok(stats) => println!(
            "导入 {} ← {}：{} 行（活 {} / 墓碑 {}）",
            kind.as_str(),
            input.display(),
            stats.records,
            stats.live,
            stats.deleted
        ),
    }
}
