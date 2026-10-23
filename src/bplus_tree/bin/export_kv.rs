//! 从 bplus-tree 导出「最新可见」逻辑 KV（迁移工具，非引擎核心）。
//!
//! 编解码见同目录 [`kv_snapshot`]（`#[path]` 共享模块）。
//!
//! ```text
//! cargo run --release --bin export_kv -- --db data.db --out snapshot.bin --format bin
//! cargo run --release --bin export_kv -- --db data.db --out snapshot.jsonl --format jsonl
//! ```

use storage_engines::bplus_tree::MVCC;
use storage_engines::common::kv_snapshot::{ExportWriter, IoStats};
use std::env;
use std::path::PathBuf;
use std::time::Instant;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Format {
    Jsonl,
    Bin,
}

struct Args {
    db: PathBuf,
    out: PathBuf,
    format: Format,
    include_deleted: bool,
    order: usize,
    cache: usize,
}

fn parse_args() -> Args {
    let mut db = PathBuf::from("data.db");
    let mut out = PathBuf::from("export.jsonl");
    let mut format = Format::Jsonl;
    let mut include_deleted = false;
    let mut order = 128usize;
    let mut cache = 2048usize;

    let mut args = env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--db" => db = PathBuf::from(args.next().expect("--db 需要路径")),
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
            "--order" => order = args.next().unwrap().parse().unwrap(),
            "--cache" => cache = args.next().unwrap().parse().unwrap(),
            "-h" | "--help" => {
                eprintln!(
                    "\
export_kv — 导出 bplus 最新可见 KV（工具）

  --db PATH              数据文件 (default data.db)
  --out PATH             输出路径 (default export.jsonl)
  --format jsonl|bin
  --include-deleted
  --order N / --cache N
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
        db,
        out,
        format,
        include_deleted,
        order,
        cache,
    }
}

fn main() {
    let args = parse_args();
    if !args.db.exists() {
        eprintln!("数据文件不存在: {:?}", args.db);
        std::process::exit(1);
    }

    println!("=== export_kv ===");
    println!(
        "db={:?} out={:?} format={:?} include_deleted={}",
        args.db,
        args.out,
        match args.format {
            Format::Jsonl => "jsonl",
            Format::Bin => "bin",
        },
        args.include_deleted
    );

    let t0 = Instant::now();
    let mvcc = MVCC::open(&args.db, args.order, args.cache);
    // M44：**折一行、写一行**。原先这两步是"先把整库折成 `Vec<ExportRecord>`，再 `.map()`
    // 成第二份 `Vec<KvRecord>`，最后交给 writer"—— 一次导出要在内存里同时摆**两遍**全库
    // （600k 键那一档，光是返回 `Vec` 的那条实测峰值就 +123.7 MB）。现在两趟并成一趟：
    // 内存里只剩"当前这一行"加写缓冲，输出字节一个没变（编码只有一份实现）。
    let mut writer = match args.format {
        Format::Jsonl => ExportWriter::jsonl(&args.out).expect("创建导出文件失败"),
        Format::Bin => ExportWriter::bin(&args.out, false).expect("创建导出文件失败"),
    };
    let handed =
        mvcc.export_latest_visible_for_each(args.include_deleted, |rec| writer.push(rec));
    let export_elapsed = t0.elapsed();

    let t1 = Instant::now();
    let stats: IoStats = writer.finish().expect("写导出文件失败");
    let write_elapsed = t1.elapsed();
    // 折出来几行就必须写出去几行：中途的 IO 错误由 `finish()` 抛，这里只核对计数
    assert_eq!(handed, stats.records, "折出的行数与写出的行数不一致");

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
    use std::format;
    use storage_engines::common::kv_snapshot::{read_bin, write_bin, write_jsonl, KvRecord};

    fn tmp(tag: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("export_kv_{tag}_{nanos}"))
    }

    fn cleanup_db(db: &std::path::Path) {
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
    fn test_export_roundtrip() {
        let db = tmp("db.db");
        let jsonl = tmp("out.jsonl");
        let bin = tmp("out.bin");
        cleanup_db(&db);
        {
            let mvcc = MVCC::open(&db, 16, 32);
            let tx = mvcc.begin_transaction();
            assert!(tx.set(b"a", b"1".to_vec()));
            assert!(tx.set(b"b", b"2".to_vec()));
            assert!(tx.delete(b"b"));
            tx.commit();
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
        }
        cleanup_db(&db);
        let _ = std::fs::remove_file(&jsonl);
        let _ = std::fs::remove_file(&bin);
    }
}
