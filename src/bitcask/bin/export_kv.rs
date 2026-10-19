//! 从 bitcask-mvcc 导出「最新可见」逻辑 KV
//!
//! ```text
//! cargo run --release --bin export_kv -- --dir ./bitcask_mvcc_demo --out snapshot.bin --format bin
//! cargo run --release --bin export_kv -- --dir ./data --out snapshot.jsonl --format jsonl
//! ```

use storage_engines::bitcask::MVCC;
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
