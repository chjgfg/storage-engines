//! 导出 lsm-tree 为 JSONL / BPEXP001|002（可选带 seq）。

use storage_engines::common::kv_snapshot::{ExportWriter, KvRecord};
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
    // M46：**归并一行、写一行**。原先这里两条分支都要先把整库物化成 `Vec<KvRecord>` 再交给
    // writer —— 而它归并的**源**当时每个都先整篇折进 `Vec`（`merge_all_at`），于是"导一个库"
    // 同时在内存里握着：全部源 + 归并输出 + 那份 `Vec<KvRecord>`。现在三笔都塌成"当前这一行"。
    // 两分支共用同一条流式判据（`export_records_with_seq_for_each`），不带 seq 时把 seq 丢掉。
    let mut writer = match format {
        Format::Bin => ExportWriter::bin(&out, with_seq).expect("创建导出文件失败"),
        Format::Jsonl => ExportWriter::jsonl(&out).expect("创建导出文件失败"),
    };
    let mut handed = 0usize;
    db.export_records_with_seq_for_each(include_deleted, &mut |k, v, seq| {
        handed += 1;
        let rec = if with_seq {
            KvRecord::with_seq(k, v, seq)
        } else {
            KvRecord::new(k, v)
        };
        writer.push_record(rec)
    });
    let stats = writer.finish().expect("写导出文件失败");
    assert_eq!(handed, stats.records, "折出的行数与写出的行数不同");
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
