//! 将 JSONL / BPEXP001 快照导入 bplus-tree（迁移工具，非引擎核心）。
//!
//! ```text
//! cargo run --release --bin import_kv -- \
//!   --input ../lsm-tree/snap.bin --format bin --db ./from_lsm.db
//! ```

use storage_engines::bplus_tree::MVCC;
use storage_engines::common::kv_snapshot::{read_bin, read_jsonl, IoStats, KvRecord};
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
    db: PathBuf,
    format: Format,
    order: usize,
    cache: usize,
}

fn parse() -> Args {
    let mut input = PathBuf::from("snapshot.bin");
    let mut db = PathBuf::from("imported.db");
    let mut format = Format::Bin;
    let mut order = 128usize;
    let mut cache = 2048usize;

    let mut args = env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--input" | "-i" => input = PathBuf::from(args.next().expect("--input")),
            "--db" => db = PathBuf::from(args.next().expect("--db")),
            "--order" => order = args.next().unwrap().parse().unwrap(),
            "--cache" => cache = args.next().unwrap().parse().unwrap(),
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
import_kv — 导入 JSONL / BPEXP001 到 bplus-tree（工具，非库 API）

  --input, -i PATH     输入快照
  --db PATH            目标 bplus 库 (default imported.db)
  --format, -f jsonl|bin
  --order N            B+ 阶数 (default 128)
  --cache N            缓存页数 (default 2048)
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
    Args {
        input,
        db,
        format,
        order,
        cache,
    }
}

fn apply_to_db(mvcc: &MVCC, records: &[KvRecord]) -> IoStats {
    let mut bulk = mvcc.begin_bulk();
    let mut stats = IoStats::default();
    for rec in records {
        match &rec.value {
            Some(v) => bulk.put(&rec.key, v.clone()),
            None => bulk.delete(&rec.key),
        }
        if rec.value.is_some() {
            stats.live += 1;
        } else {
            stats.deleted += 1;
        }
        stats.records += 1;
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

    println!("=== bplus import_kv ===");
    println!(
        "input={:?} db={:?} format={}",
        args.input,
        args.db,
        match args.format {
            Format::Jsonl => "jsonl",
            Format::Bin => "bin",
        }
    );

    let t0 = Instant::now();
    let (records, _file_stats) = match args.format {
        Format::Bin => read_bin(&args.input).expect("读 bin 失败"),
        Format::Jsonl => read_jsonl(&args.input).expect("读 jsonl 失败"),
    };
    let mvcc = MVCC::open(&args.db, args.order, args.cache);
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
        "VERIFY export_latest_visible live_keys={} db={:?}",
        live.len(),
        args.db
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use storage_engines::common::kv_snapshot::{write_bin, KvRecord};
    use std::path::Path;

    fn tmp(tag: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("bplus_imp_bin_{tag}_{nanos}"))
    }

    fn cleanup_db(db: &Path) {
        for suf in ["", ".wal", ".dblwr", ".freelist", ".lock", ".blob"] {
            let p = if suf.is_empty() {
                db.to_path_buf()
            } else {
                PathBuf::from(format!("{}{suf}", db.display()))
            };
            let _ = std::fs::remove_file(p);
        }
    }

    #[test]
    fn test_import_bin_into_bplus() {
        let db_path = tmp("db.db");
        let bin = tmp("snap.bin");
        cleanup_db(&db_path);
        write_bin(
            &bin,
            &[
                KvRecord::new(b"k".to_vec(), Some(b"v42".to_vec())),
                KvRecord::new(b"d".to_vec(), None),
            ],
        )
        .unwrap();
        {
            let mvcc = MVCC::open(&db_path, 16, 32);
            let (recs, _) = read_bin(&bin).unwrap();
            let st = apply_to_db(&mvcc, &recs);
            assert_eq!(st.live, 1);
            assert_eq!(st.deleted, 1);
            let tx = mvcc.begin_transaction();
            assert_eq!(tx.get(b"k"), Some(b"v42".to_vec()));
            assert_eq!(tx.get(b"d"), None);
            tx.commit();
        }
        cleanup_db(&db_path);
        let _ = std::fs::remove_file(&bin);
    }
}
