//! 将 JSONL / BPEXP001 快照导入 memory（迁移工具，非引擎核心）。
//!
//! 可来自本库 export、bplus-tree 或 lsm-tree 的同格式快照。
//!
//! ```text
//! cargo run --release --bin import_kv -- \
//!   --input snapshot.bin --format bin
//!
//! # 导入后立刻 re-export 做往返校验
//! cargo run --release --bin import_kv -- \
//!   --input snapshot.bin --format bin --export roundtrip.bin
//! ```

use storage_engines::common::kv_snapshot::{read_bin, read_jsonl, write_bin, write_jsonl, IoStats, KvRecord};
use storage_engines::memory::MVCC;
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
    format: Format,
    /// 导入后可选 re-export 路径（格式同 --format）
    export: Option<PathBuf>,
}

fn parse() -> Args {
    let mut input = PathBuf::from("snapshot.bin");
    let mut format = Format::Bin;
    let mut export = None;

    let mut args = env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--input" | "-i" => input = PathBuf::from(args.next().expect("--input")),
            "--export" | "-o" => {
                export = Some(PathBuf::from(args.next().expect("--export 需要路径")))
            }
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
import_kv — 导入 JSONL / BPEXP001 到 memory（工具，非库 API）

  --input, -i PATH     输入快照
  --format, -f jsonl|bin
  --export, -o PATH    导入后 re-export 到此路径（同 format）
  -h, --help

说明：memory 纯内存，进程退出数据即失；本工具用于迁移格式校验与联调。
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
        format,
        export,
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

    println!("=== memory import_kv ===");
    println!(
        "input={:?} format={} export={:?}",
        args.input,
        match args.format {
            Format::Jsonl => "jsonl",
            Format::Bin => "bin",
        },
        args.export
    );

    let t0 = Instant::now();
    let (records, _file_stats) = match args.format {
        Format::Bin => read_bin(&args.input).expect("读 bin 失败"),
        Format::Jsonl => read_jsonl(&args.input).expect("读 jsonl 失败"),
    };
    let mvcc = MVCC::new();
    let stats = apply_to_db(&mvcc, &records);
    let elapsed = t0.elapsed();

    // M44：行数 + 前 5 条抽样。原先这里把整库折成 `Vec<ExportRecord>`，只为数一个个数、
    // 再打印前五条 ⇒ 全库在内存里摆一整份。现在流式数，抽样只留最多 5 条。
    let mut live_keys = 0usize;
    let mut sample: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::new();
    mvcc.export_latest_visible_for_each(false, |rec| {
        live_keys += 1;
        if sample.len() < 5 {
            sample.push((rec.key, rec.value));
            true
        } else {
            false // 抽样够了就收手：后面的键一个都不再折
        }
    });
    println!(
        "IMPORT records={} live={} deleted={} in {:.3}s",
        stats.records,
        stats.live,
        stats.deleted,
        elapsed.as_secs_f64()
    );
    println!(
        "VERIFY export 流式数行数 live_keys={}",
        live_keys
    );

    // 抽样打印（最多 5 条 ⇒ 这一支现在与库的大小无关）
    for (k, v) in &sample {
        let k = String::from_utf8_lossy(k);
        let v = v
            .as_ref()
            .map(|b| String::from_utf8_lossy(b).into_owned())
            .unwrap_or_else(|| "<none>".into());
        println!("  sample {k:?} => {v:?}");
    }
    if live_keys > sample.len() {
        println!("  ...");
    }

    if let Some(out) = &args.export {
        let recs: Vec<KvRecord> = mvcc
            .export_latest_visible(false)
            .into_iter()
            .map(|r| KvRecord::new(r.key, r.value))
            .collect();
        let st = match args.format {
            Format::Bin => write_bin(out, &recs).expect("re-export bin"),
            Format::Jsonl => write_jsonl(out, &recs).expect("re-export jsonl"),
        };
        println!(
            "RE-EXPORT {} live records → {:?}",
            st.live, out
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use storage_engines::common::kv_snapshot::{write_bin, KvRecord};

    fn tmp(tag: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("memory_imp_{tag}_{nanos}"))
    }

    #[test]
    fn test_import_bin_into_memory() {
        let bin = tmp("snap.bin");
        write_bin(
            &bin,
            &[
                KvRecord::new(b"k".to_vec(), Some(b"v42".to_vec())),
                KvRecord::new(b"d".to_vec(), None),
            ],
        )
        .unwrap();
        {
            let mvcc = MVCC::new();
            let (recs, _) = read_bin(&bin).unwrap();
            let st = apply_to_db(&mvcc, &recs);
            assert_eq!(st.live, 1);
            assert_eq!(st.deleted, 1);
            let tx = mvcc.begin_transaction();
            assert_eq!(tx.get(b"k"), Some(b"v42".to_vec()));
            assert_eq!(tx.get(b"d"), None);
            tx.commit();
        }
        let _ = std::fs::remove_file(&bin);
    }

    #[test]
    fn test_import_export_roundtrip() {
        let bin_in = tmp("in.bin");
        let bin_out = tmp("out.bin");
        write_bin(
            &bin_in,
            &[
                KvRecord::new(b"x".to_vec(), Some(b"1".to_vec())),
                KvRecord::new(b"y".to_vec(), Some(b"2".to_vec())),
            ],
        )
        .unwrap();
        let mvcc = MVCC::new();
        let (recs, _) = read_bin(&bin_in).unwrap();
        apply_to_db(&mvcc, &recs);
        let out: Vec<KvRecord> = mvcc
            .export_latest_visible(false)
            .into_iter()
            .map(|r| KvRecord::new(r.key, r.value))
            .collect();
        write_bin(&bin_out, &out).unwrap();
        let (back, st) = read_bin(&bin_out).unwrap();
        assert_eq!(st.live, 2);
        assert_eq!(back.len(), 2);
        let _ = std::fs::remove_file(&bin_in);
        let _ = std::fs::remove_file(&bin_out);
    }
}
