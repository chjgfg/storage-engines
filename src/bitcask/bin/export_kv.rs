//! 从 bitcask-mvcc 导出「最新可见」逻辑 KV
//!
//! ```text
//! cargo run --release --bin export_kv -- --dir ./bitcask_mvcc_demo --out snapshot.bin --format bin
//! cargo run --release --bin export_kv -- --dir ./data --out snapshot.jsonl --format jsonl
//! ```

#[path = "../tools/kv_snapshot.rs"]
mod kv_snapshot;

use bitcask::MVCC;
use kv_snapshot::{write_bin, write_jsonl, IoStats, KvRecord};
use std::env;
use std::path::PathBuf;
use std::time::Instant;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Format {
    Jsonl,
    Bin,
}

struct Args {
    dir: PathBuf,
    out: PathBuf,
    format: Format,
    include_deleted: bool,
}

fn parse_args() -> Args {
    let mut dir = PathBuf::from("bitcask_mvcc_demo");
    let mut out = PathBuf::from("export.jsonl");
    let mut format = Format::Jsonl;
    let mut include_deleted = false;

    let mut args = env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--dir" | "--db" => dir = PathBuf::from(args.next().expect("--dir 需要路径")),
            "--out" => out = PathBuf::from(args.next().expect("--out 需要路径")),
            "--format" => {
                format = match args.next().expect("--format jsonl|bin").as_str() {
                    "bin" | "binary" => Format::Bin,
                    "jsonl" | "json" => Format::Jsonl,
                    other => {
                        eprintln!("未知 format: {other}");
                        std::process::exit(2);
                    }
                }
            }
            "--include-deleted" => include_deleted = true,
            "-h" | "--help" => {
                eprintln!(
                    "\
export_kv — 导出 bitcask-mvcc 最新可见 KV

  --dir, --db PATH     数据目录 (default bitcask_mvcc_demo)
  --out PATH           输出路径 (default export.jsonl)
  --format jsonl|bin
  --include-deleted
  -h, --help
"
                );
                std::process::exit(0);
            }
            other => {
                eprintln!("未知参数: {other}");
                std::process::exit(2);
            }
        }
    }
    Args {
        dir,
        out,
        format,
        include_deleted,
    }
}

fn main() {
    let args = parse_args();
    if !args.dir.exists() {
        eprintln!("数据目录不存在: {:?}", args.dir);
        std::process::exit(1);
    }

    println!("=== export_kv (bitcask) ===");
    println!(
        "dir={:?} out={:?} format={:?} include_deleted={}",
        args.dir,
        args.out,
        match args.format {
            Format::Jsonl => "jsonl",
            Format::Bin => "bin",
        },
        args.include_deleted
    );

    let t0 = Instant::now();
    let mvcc = MVCC::open(&args.dir);
    let records: Vec<KvRecord> = mvcc
        .export_latest_visible(args.include_deleted)
        .into_iter()
        .map(|r| KvRecord {
            key: r.key,
            value: r.value,
        })
        .collect();
    let export_elapsed = t0.elapsed();

    let t1 = Instant::now();
    let stats: IoStats = match args.format {
        Format::Jsonl => write_jsonl(&args.out, &records).expect("写 jsonl 失败"),
        Format::Bin => write_bin(&args.out, &records).expect("写 bin 失败"),
    };
    let write_elapsed = t1.elapsed();

    let out_size = std::fs::metadata(&args.out).map(|m| m.len()).unwrap_or(0);
    println!(
        "SCAN   {} keys (live={} deleted={}) in {:.3}s",
        stats.records,
        stats.live,
        stats.deleted,
        export_elapsed.as_secs_f64()
    );
    println!(
        "WRITE  {} records → {:?} ({:.2} MB) in {:.3}s",
        stats.records,
        args.out,
        out_size as f64 / 1e6,
        write_elapsed.as_secs_f64()
    );
    println!(
        "TOTAL  {:.3}s",
        (export_elapsed + write_elapsed).as_secs_f64()
    );
}
