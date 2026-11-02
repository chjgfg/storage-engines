//! 统一导出 CLI：`--engine <家> --dir <库目录> --out <文件> [--format bin|jsonl] [--include-deleted]`
//!
//! 为什么只有一份（M81 第 6 项）：原先四家各有一个 `export_kv.rs`，每家一份参数解析、
//! 一份格式分支、一份"写完数一遍条数"的收尾 —— 而它们调的其实是**同一个** `tools::export_to_dir`。
//! 那正是"基座被复制而不是被依赖"的形状（本仓库一路在收的就是它）。这一份只留命令行外壳。
//!
//! ⚠️ 两条如实写在这儿的能力边界：
//! ① **导出交的是用户字节，TTL 不在文件里**。源侧出口 `live_record_of_logical`
//!    （`common/store.rs:475`）把值信封剥掉，`ExportRecord` 里没有放 expire 的位置 ⇒
//!    带 TTL 的键导出去再导回来就变永久。要修得动公开结构体（对下游是破坏性变更），
//!    那条决定还没点；`v3`（`BPEXP003`）的编码本身已经就位，缺的是**导出侧没往里填**。
//! ② 跨库**补齐**（"目标已有的不动"）不走这个文件口子，走 `tools::sync`（库↔库）。
//!    文件导入 today 是覆盖语义（`--wipe` 才是"整库换掉"），别把它当增量同步用。
//!
//! ```text
//! cargo run --release --bin export-kv -- --engine bitcask --dir ./data/bitcask --out snap.bin
//! cargo run --release --bin export-kv -- --engine lsm_tree --dir ./data/lsm_tree --out snap.jsonl --format jsonl
//! ```

use std::env;
use std::path::PathBuf;
use std::println;

use storage_engines::tools::migrate::{SnapshotFormat, export_to_dir};
use storage_engines::EngineKind;

fn usage() -> ! {
    eprintln!(
        "用法: export-kv --engine <bitcask|bplus_tree|lsm_tree|memory> --dir <库目录> --out <文件> \
         [--format bin|jsonl] [--include-deleted]"
    );
    std::process::exit(2);
}

pub fn main() {
    let argv: Vec<String> = env::args().skip(1).collect();
    let mut engine = String::new();
    let mut dir = PathBuf::new();
    let mut out = PathBuf::new();
    let mut format = SnapshotFormat::Bin;
    let mut include_deleted = false;
    let mut i = 0;
    while i < argv.len() {
        // 带值参数：吃掉下一个；缺值就直接报用法，不许静默按默认值跑（那会把运维脚本引到错库上）
        let next = |i: &mut usize| -> Option<String> {
            *i += 1;
            argv.get(*i).cloned()
        };
        let flag = argv[i].as_str();
        let value = match flag {
            "--engine" | "--dir" | "--out" | "--format" => Some(next(&mut i).unwrap_or_default()),
            "--include-deleted" => Some(String::new()),
            _ => None,
        };
        match (flag, value) {
            ("--engine", Some(v)) => engine = v,
            ("--dir", Some(v)) => dir = PathBuf::from(v),
            ("--out", Some(v)) => out = PathBuf::from(v),
            ("--format", Some(v)) => match v.as_str() {
                "jsonl" => format = SnapshotFormat::Jsonl,
                "bin" => format = SnapshotFormat::Bin,
                other => {
                    eprintln!("未知 --format {other}（只认 bin / jsonl）");
                    std::process::exit(2);
                }
            },
            ("--include-deleted", _) => include_deleted = true,
            ("-h" | "--help", _) => usage(),
            (other, _) => {
                eprintln!("未知参数: {other}");
                usage();
            }
        }
        i += 1;
    }
    if engine.is_empty() || dir.as_os_str().is_empty() || out.as_os_str().is_empty() {
        usage();
    }
    // 引擎名解析与 `migrate` / 下游 `StorageEngine::parse` 同一套别名（bc / bplus / lsm / mem…）
    let kind = match EngineKind::parse(&engine) {
        Some(k) => k,
        None => {
            eprintln!("未知引擎: {engine}");
            std::process::exit(2);
        }
    };
    match export_to_dir(kind, &dir, &out, format, include_deleted) {
        Err(e) => {
            eprintln!("导出失败: {e}");
            std::process::exit(1);
        }
        Ok(stats) => println!(
            "导出 {} → {}：{} 行（活 {} / 墓碑 {}）",
            kind.as_str(),
            out.display(),
            stats.records,
            stats.live,
            stats.deleted
        ),
    }
}
