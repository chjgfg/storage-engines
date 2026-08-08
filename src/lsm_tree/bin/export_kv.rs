//! 导出 lsm-tree 为 JSONL / BPEXP001|002（可选带 seq）。

#[path = "../tools/kv_snapshot.rs"]
mod kv_snapshot;

use kv_snapshot::{write_bin, write_jsonl, KvRecord};
use lsm_tree::LSMTree;
use std::env;
use std::path::PathBuf;
use std::time::Instant;

#[derive(Clone, Copy)]
enum Format {
    Jsonl,
    Bin,
}

fn main() {
    let mut dir = PathBuf::from("lsm_data");
    let mut out = PathBuf::from("export.bin");
    let mut format = Format::Bin;
    let mut include_deleted = false;
    let mut with_seq = false;

    let mut args = env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--dir" | "-d" => dir = PathBuf::from(args.next().expect("--dir")),
            "--out" | "-o" => out = PathBuf::from(args.next().expect("--out")),
            "--include-deleted" => include_deleted = true,
            "--with-seq" => with_seq = true,
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
                    "export_kv — 导出 lsm-tree\n  --dir PATH  --out PATH  --format jsonl|bin  --include-deleted  --with-seq"
                );
                std::process::exit(0);
            }
            o => {
                eprintln!("未知参数: {o}");
                std::process::exit(2);
            }
        }
    }

    if !dir.exists() {
        eprintln!("目录不存在: {dir:?}");
        std::process::exit(1);
    }

    println!("=== lsm export_kv ===");
    let t0 = Instant::now();
    let db = LSMTree::open(&dir).expect("打开 LSM");
    let records: Vec<KvRecord> = if with_seq {
        db.export_records_with_seq(include_deleted)
            .into_iter()
            .map(|(k, v, seq)| KvRecord {
                key: k,
                value: v,
                seq: Some(seq),
            })
            .collect()
    } else {
        db.export_records(include_deleted)
            .into_iter()
            .map(|(k, v)| KvRecord {
                key: k,
                value: v,
                seq: None,
            })
            .collect()
    };
    let stats = match format {
        Format::Bin => write_bin(&out, &records).expect("写 bin"),
        Format::Jsonl => write_jsonl(&out, &records).expect("写 jsonl"),
    };
    let size = std::fs::metadata(&out).map(|m| m.len()).unwrap_or(0);
    println!(
        "EXPORT records={} live={} deleted={} with_seq={with_seq} → {:?} ({:.2} MB) in {:.3}s",
        stats.records,
        stats.live,
        stats.deleted,
        out,
        size as f64 / 1e6,
        t0.elapsed().as_secs_f64()
    );
}
