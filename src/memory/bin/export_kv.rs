//! 从 memory 导出「最新可见」逻辑 KV（迁移工具，非引擎核心）。
//!
//! memory **无磁盘库**：CLI 用 `--demo N` 生成样例后导出；库内请
//! `export_latest_visible` + `kv_snapshot::write_*`。
//! 快照格式与 bplus-tree / lsm-tree 共用（`BPEXP001` / JSONL）。
//!
//! ```text
//! cargo run --release --bin export_kv -- --demo 100 --out snapshot.bin --format bin
//! cargo run --release --bin export_kv -- --demo 50 --out snapshot.jsonl --format jsonl
//! ```

use storage_engines::common::kv_snapshot::{write_bin, write_jsonl, IoStats, KvRecord};
use storage_engines::memory::MVCC;
use std::env;
use std::path::PathBuf;
use std::time::Instant;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Format {
    Jsonl,
    Bin,
}

struct Args {
    out: PathBuf,
    format: Format,
    include_deleted: bool,
    /// 生成样例条数；0 表示不生成（空库导出，通常无意义）
    demo: usize,
}

fn parse_args() -> Args {
    let mut out = PathBuf::from("export.jsonl");
    let mut format = Format::Jsonl;
    let mut include_deleted = false;
    let mut demo = 0usize;

    let mut args = env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
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
            "--demo" => {
                demo = args
                    .next()
                    .expect("--demo 需要 N")
                    .parse()
                    .expect("--demo 需要整数")
            }
            "-h" | "--help" => {
                eprintln!(
                    "\
export_kv — 导出 memory 最新可见 KV（工具）

  memory 无持久化磁盘库；CLI 请用 --demo N 生成样例再导出。
  程序内填充后请调 MVCC::export_latest_visible + write_bin/write_jsonl。

  --out PATH             输出路径 (default export.jsonl)
  --format jsonl|bin
  --include-deleted
  --demo N               写入 N 条样例 key 后导出
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
        out,
        format,
        include_deleted,
        demo,
    }
}

fn seed_demo(mvcc: &MVCC, n: usize) {
    let mut bulk = mvcc.begin_bulk();
    for i in 0..n {
        let k = format!("demo:{i:06}").into_bytes();
        let v = format!("val-{i}").into_bytes();
        bulk.put(&k, v);
    }
    // 再写几条有趣 key，方便对照
    bulk.put(b"apple", b"fruit".to_vec());
    bulk.put(b"user:01", b"alice".to_vec());
    bulk.delete(b"demo:000000"); // 一条 tombstone（若 n>=1）
    bulk.finish();
}

fn main() {
    let args = parse_args();

    println!("=== memory export_kv ===");
    println!(
        "out={:?} format={:?} include_deleted={} demo={}",
        args.out,
        match args.format {
            Format::Jsonl => "jsonl",
            Format::Bin => "bin",
        },
        args.include_deleted,
        args.demo
    );

    if args.demo == 0 {
        eprintln!(
            "提示: 未指定 --demo N，将导出空库（0 条）。\
             memory 无磁盘 --db；跨库迁移请用 import_kv 读入 bplus/lsm 快照。"
        );
    }

    let t0 = Instant::now();
    let mvcc = MVCC::new();
    if args.demo > 0 {
        seed_demo(&mvcc, args.demo);
    }
    let records: Vec<KvRecord> = mvcc
        .export_latest_visible(args.include_deleted)
        .into_iter()
        .map(|r| KvRecord::new(r.key, r.value))
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

#[cfg(test)]
mod tests {
    use super::*;
    use storage_engines::common::kv_snapshot::{read_bin, write_bin, write_jsonl};

    fn tmp(tag: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("memory_export_{tag}_{nanos}"))
    }

    #[test]
    fn test_export_roundtrip() {
        let jsonl = tmp("out.jsonl");
        let bin = tmp("out.bin");
        let mvcc = MVCC::new();
        {
            let tx = mvcc.begin_transaction();
            assert!(tx.set(b"a", b"1".to_vec()));
            assert!(tx.set(b"b", b"2".to_vec()));
            assert!(tx.delete(b"b"));
            tx.commit();
        }
        let live: Vec<_> = mvcc
            .export_latest_visible(false)
            .into_iter()
            .map(|r| KvRecord::new(r.key, r.value))
            .collect();
        assert_eq!(live.len(), 1);
        write_jsonl(&jsonl, &live).unwrap();
        let all: Vec<_> = mvcc
            .export_latest_visible(true)
            .into_iter()
            .map(|r| KvRecord::new(r.key, r.value))
            .collect();
        write_bin(&bin, &all).unwrap();
        let (back, st) = read_bin(&bin).unwrap();
        assert_eq!(st.deleted, 1);
        assert!(back.iter().any(|r| r.key == b"b" && r.value.is_none()));
        let _ = std::fs::remove_file(&jsonl);
        let _ = std::fs::remove_file(&bin);
    }
}
