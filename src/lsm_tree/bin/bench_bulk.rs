//! 压测：1000 万 KV 写入，目标约 10 分钟（≈1.67 万条/秒）。
//!
//! 路径：BulkLoader → 排序 batch → 直接写 L0 SST（跳过 MemTable）
//!
//! ```bash
//! cargo run --release --bin bench_bulk -- --n 10000000 --dir ./bench_data
//! ```

use std::vec;
use std::format;
use storage_engines::lsm_tree::MVCC;
use std::env;
use std::time::Instant;

#[inline]
fn make_key(i: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(17);
    k.push(b'k');
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut x = i;
    let mut buf = [0u8; 16];
    for j in (0..16).rev() {
        buf[j] = HEX[(x & 0xf) as usize];
        x >>= 4;
    }
    k.extend_from_slice(&buf);
    k
}

#[inline]
fn make_val(i: u64, vlen: usize) -> Vec<u8> {
    let mut v = vec![b'v'; vlen.max(8)];
    let b = i.to_le_bytes();
    let n = 8.min(v.len());
    v[..n].copy_from_slice(&b[..n]);
    v
}

fn main() {
    let mut n: u64 = 10_000_000;
    let mut dir = "./bench_data".to_string();
    let mut batch: usize = 65_536;
    let mut value_len: usize = 16;

    let mut args = env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--n" => n = args.next().unwrap().parse().unwrap(),
            "--dir" => dir = args.next().unwrap(),
            "--batch" => batch = args.next().unwrap().parse().unwrap(),
            "--vlen" => value_len = args.next().unwrap().parse().unwrap(),
            "-h" | "--help" => {
                eprintln!("bench_bulk --n N --dir PATH --batch N --vlen N");
                return;
            }
            o => {
                eprintln!("未知参数 {o}");
                std::process::exit(2);
            }
        }
    }

    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    println!("=== bench_bulk (direct L0 SST) ===");
    println!("n={n} batch={batch} vlen={value_len} dir={dir}");
    println!("target ≥ {:.0} ops/s (10M / 10min)", 10_000_000.0 / 600.0);

    let mvcc = MVCC::open(&dir);
    let t0 = Instant::now();
    let mut bulk = mvcc.begin_bulk();
    let mut buf: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(batch);
    let mut last_report = Instant::now();

    for i in 0..n {
        buf.push((make_key(i), make_val(i, value_len)));
        if buf.len() >= batch {
            let chunk = std::mem::take(&mut buf);
            bulk.put_batch_owned(chunk);
            buf = Vec::with_capacity(batch);
        }
        if last_report.elapsed().as_secs() >= 5 {
            let done = i + 1;
            let elapsed = t0.elapsed().as_secs_f64();
            let rate = done as f64 / elapsed.max(1e-9);
            println!(
                "  {done}/{n} ({:.1}%)  {:.0} ops/s  eta {:.0}s",
                100.0 * done as f64 / n as f64,
                rate,
                (n - done) as f64 / rate.max(1.0)
            );
            last_report = Instant::now();
        }
    }
    if !buf.is_empty() {
        bulk.put_batch_owned(buf);
    }
    println!("  finish...");
    bulk.finish();
    let elapsed = t0.elapsed();
    let rate = n as f64 / elapsed.as_secs_f64().max(1e-9);

    let tx = mvcc.begin_transaction();
    for &i in &[0u64, n / 2, n.saturating_sub(1)] {
        assert!(tx.get(&make_key(i)).is_some(), "missing {i}");
    }
    tx.commit();

    let sst_n = std::fs::read_dir(format!("{dir}/sst"))
        .map(|r| r.filter_map(|e| e.ok()).count())
        .unwrap_or(0);

    println!("\n=== result ===");
    println!(
        "wrote {n} in {:.3}s  →  {:.0} ops/s",
        elapsed.as_secs_f64(),
        rate
    );
    println!("10M 等价: {:.1} min", (10_000_000.0 / rate) / 60.0);
    println!("sst files ≈ {sst_n}");
    if rate >= 10_000_000.0 / 600.0 {
        println!("PASS ≥ 10M/10min");
    } else {
        println!("BELOW TARGET (need {:.0})", 10_000_000.0 / 600.0);
    }
}
