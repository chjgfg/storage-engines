//! 将 JSONL / BPEXP001 快照导入 bitcask-mvcc
//!
//! ```text
//! cargo run --release --bin import_kv -- \
//!   --input snapshot.bin --format bin --dir ./imported
//! ```

use storage_engines::bitcask::MVCC;
use storage_engines::common::codec::pack_plain;
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

fn apply_to_db(mvcc: &MVCC, records: &[KvRecord]) -> IoStats {
    let mut bulk = mvcc.begin_bulk();
    let mut stats = IoStats::default();
    let mut batch: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(4096);
    for rec in records {
        match &rec.value {
            Some(v) => {
                // ⚠️ 快照文件里放的是**用户字节**（导出侧已剥信封），而 `bulk.put*` 的合同是
                //   "已打过值信封的 logical 字节"（与 `tx.set` 同一口径）。少这一步 ⇒ 首字节被
                //   当成信封标签吃掉：`[00,..]` 少一字节、`[01,..]` 会被当成 TTL 行（甚至判过期→读成 None）。
                //   守卫在 `tools::migrate::tests::tools_import_leaves_no_double_envelope`（同一条合同的另一侧）。
                batch.push((rec.key.clone(), pack_plain(v)));
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

    // M44：这里要的只是**一个行数**，原先却把整库折成 `Vec<ExportRecord>` 再 `.len()` ——
    // 与 M35 那一刀里 `key_count` 的形状一模一样（为一个数把全库摆一遍）。现在逐行数，
    // 内存里只过当前那一行。
    let mut live_keys = 0usize;
    mvcc.export_latest_visible_for_each(false, |_| {
        live_keys += 1;
        true
    });
    println!(
        "IMPORT records={} live={} deleted={} in {:.3}s",
        stats.records,
        stats.live,
        stats.deleted,
        elapsed.as_secs_f64()
    );
    println!(
        "VERIFY export 流式数行数 live_keys={} dir={:?}",
        live_keys,
        args.dir
    );
}
