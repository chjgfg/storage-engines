//! 导入 bplus / 通用 JSONL·BPEXP001 到 LSM 目录（迁移工具，非引擎核心）。

use std::println;
use std::eprintln;
use storage_engines::common::kv_snapshot::{read_bin, read_jsonl};
use storage_engines::lsm_tree::LSMTree;
use std::env;
use std::path::PathBuf;
use std::time::Instant;

#[derive(Clone, Copy)]
enum Format {
    Jsonl,
    Bin,
}

fn main() {
    let mut input = PathBuf::from("snapshot.bin");
    let mut dir = PathBuf::from("lsm_data");
    let mut format = Format::Bin;

    let mut args = env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--input" | "-i" => input = PathBuf::from(args.next().expect("--input")),
            "--dir" | "-d" => dir = PathBuf::from(args.next().expect("--dir")),
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
                    "import_kv — 导入快照到 lsm-tree\n  --input PATH  --dir PATH  --format jsonl|bin"
                );
                std::process::exit(0);
            }
            o => {
                eprintln!("未知参数: {o}");
                std::process::exit(2);
            }
        }
    }

    if !input.exists() {
        eprintln!("输入不存在: {input:?}");
        std::process::exit(1);
    }

    println!("=== lsm import_kv ===");
    let t0 = Instant::now();
    let (records, file_stats) = match format {
        Format::Bin => read_bin(&input).expect("读 bin"),
        Format::Jsonl => read_jsonl(&input).expect("读 jsonl"),
    };
    let mut db = LSMTree::open(&dir).expect("打开 LSM");
    for rec in &records {
        match &rec.value {
            Some(v) => db.put(rec.key.clone(), v.clone()),
            None => db.delete(rec.key.clone()),
        }
    }
    db.flush().expect("flush");
    let s = db.stats();
    println!(
        "IMPORT records={} live={} deleted={} in {:.3}s",
        file_stats.records,
        file_stats.live,
        file_stats.deleted,
        t0.elapsed().as_secs_f64()
    );
    println!(
        "DB     live_keys={} tombstones={} dir={:?}",
        s.live_keys, s.tombstones, s.dir
    );
}
