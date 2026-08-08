//! 将 JSONL / BPEXP001 快照导入 bitcask-mvcc
//!
//! ```text
//! cargo run --release --bin import_kv -- \
//!   --input snapshot.bin --format bin --dir ./imported
//! ```

#[path = "../tools/kv_snapshot.rs"]
mod kv_snapshot;

use bitcask::MVCC;
use kv_snapshot::{read_bin, read_jsonl, IoStats};
use std::env;
use std::path::PathBuf;
use std::time::Instant;

#[derive(Clone, Copy)]
enum Format {
    Jsonl,
    Bin,
}

struct Args {
    input: PathBuf,
    dir: PathBuf,
    format: Format,
}

fn parse() -> Args {
    let mut input = PathBuf::from("snapshot.bin");
    let mut dir = PathBuf::from("imported");
    let mut format = Format::Bin;

    let mut args = env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--input" | "-i" => input = PathBuf::from(args.next().expect("--input")),
            "--dir" | "--db" => dir = PathBuf::from(args.next().expect("--dir")),
            "--format" | "-f" => {
                format = match args.next().expect("jsonl|bin").as_str() {
                    "jsonl" | "json" => Format::Jsonl,
                    "bin" | "binary" => Format::Bin,
                    o => {
                        eprintln!("未知 format: {o}");
                        std::process::exit(2);
                    }
                }
            }
            "-h" | "--help" => {
                eprintln!(
                    "\
import_kv — 导入 JSONL / BPEXP001 到 bitcask-mvcc

  --input, -i PATH     输入快照
  --dir, --db PATH     目标数据目录 (default imported)
  --format, -f jsonl|bin
  -h, --help
"
                );
                std::process::exit(0);
            }
            o => {
                eprintln!("未知参数: {o}");
                std::process::exit(2);
            }
        }
    }
    Args { input, dir, format }
}

fn apply_to_db(mvcc: &MVCC, records: &[kv_snapshot::KvRecord]) -> IoStats {
    let mut bulk = mvcc.begin_bulk();
    let mut stats = IoStats::default();
    let mut batch: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(4096);
    for rec in records {
        match &rec.value {
            Some(v) => {
                batch.push((rec.key.clone(), v.clone()));
                if batch.len() >= 4096 {
                    bulk.put_batch_owned(std::mem::take(&mut batch));
                    batch = Vec::with_capacity(4096);
                }
                stats.live += 1;
            }
            None => {
                bulk.delete(&rec.key);
                stats.deleted += 1;
            }
        }
        stats.records += 1;
    }
    if !batch.is_empty() {
        bulk.put_batch_owned(batch);
    }
    bulk.finish();
    stats
}

fn main() {
    let args = parse();
    if !args.input.exists() {
        eprintln!("输入不存在: {:?}", args.input);
        std::process::exit(1);
    }

    println!("=== bitcask import_kv ===");
    println!(
        "input={:?} dir={:?} format={}",
        args.input,
        args.dir,
        match args.format {
            Format::Jsonl => "jsonl",
            Format::Bin => "bin",
        }
    );

    let t0 = Instant::now();
    let (records, _) = match args.format {
        Format::Bin => read_bin(&args.input).expect("读 bin 失败"),
        Format::Jsonl => read_jsonl(&args.input).expect("读 jsonl 失败"),
    };
    let _ = std::fs::remove_dir_all(&args.dir);
    let mvcc = MVCC::open(&args.dir);
    let stats = apply_to_db(&mvcc, &records);
    let elapsed = t0.elapsed();

    let live = mvcc.export_latest_visible(false);
    println!(
        "IMPORT records={} live={} deleted={} in {:.3}s",
        stats.records,
        stats.live,
        stats.deleted,
        elapsed.as_secs_f64()
    );
    println!(
        "VERIFY export_latest_visible live_keys={} dir={:?}",
        live.len(),
        args.dir
    );
}
