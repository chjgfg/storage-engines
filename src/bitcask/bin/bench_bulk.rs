//! 压测：1000 万 KV 写入，目标约 10 分钟（≈1.67 万条/秒）。
//!
//! ```bash
//! cargo run --release --bin bench_bulk -- --n 10000000 --dir ./bench_data
//! ```
//!
//! 可选：
//! - `--batch 16384`  每批条数（默认 16384）
//! - `--vlen 16`      value 长度；`>256` 会走 blob 外置
//! - `--flush-every N`  每 N 条 flush 写缓冲（0=不主动 flush）

use std::assert;
use std::println;
use std::eprintln;
use std::vec;
use std::format;
use storage_engines::bitcask::MVCC;
use std::env;
use std::time::Instant;

#[inline]
fn make_key(i: u64) -> Vec<u8> {
    // 固定 17 字节：b'k' + 16 hex digits，避免 format! 分配
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
    v[..8].copy_from_slice(&b);
    v
}

fn main() {
    let mut n: u64 = 10_000_000;
    let mut dir = "./bench_data".to_string();
    let mut batch: usize = 16_384;
    let mut value_len: usize = 16;
    let mut flush_every: u64 = 0;

    let mut args = env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--n" => n = args.next().unwrap().parse().unwrap(),
            "--dir" => dir = args.next().unwrap(),
            "--batch" => batch = args.next().unwrap().parse().unwrap(),
            "--vlen" => value_len = args.next().unwrap().parse().unwrap(),
            "--flush-every" => flush_every = args.next().unwrap().parse().unwrap(),
            "-h" | "--help" => {
                eprintln!(
                    "bench_bulk — 大批量写入\n\
                     --n N  --dir PATH  --batch N  --vlen N  --flush-every N"
                );
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

    println!("=== bitcask bench_bulk ===");
    println!("n={n} batch={batch} vlen={value_len} dir={dir}");
    println!(
        "target: 10 min for 10M ≈ {:.0} ops/s",
        10_000_000.0 / 600.0
    );
    if value_len > 256 {
        println!("note: vlen>256 → 大 value 走 blob 外置");
    }

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
        if flush_every > 0 && (i + 1) % flush_every == 0 {
            bulk.flush_buf();
        }
        if last_report.elapsed().as_secs() >= 5 {
            let done = i + 1;
            let elapsed = t0.elapsed().as_secs_f64();
            let rate = done as f64 / elapsed.max(1e-9);
            let eta = (n - done) as f64 / rate.max(1.0);
            println!(
                "  progress {done}/{n} ({:.1}%)  {:.0} ops/s  eta {:.0}s",
                100.0 * done as f64 / n as f64,
                rate,
                eta
            );
            last_report = Instant::now();
        }
    }
    if !buf.is_empty() {
        bulk.put_batch_owned(buf);
    }
    println!("  finishing (flush + checkpoint)...");
    bulk.finish();
    let elapsed = t0.elapsed();
    let rate = n as f64 / elapsed.as_secs_f64().max(1e-9);

    let t1 = Instant::now();
    let tx = mvcc.begin_transaction();
    for &i in &[0u64, n / 2, n.saturating_sub(1)] {
        let got = tx.get(&make_key(i));
        assert!(got.is_some(), "missing key {i}");
    }
    tx.commit();
    let verify_ms = t1.elapsed().as_millis();

    println!("=== result ===");
    println!("wrote {n} keys in {:.2}s  ({:.0} ops/s)", elapsed.as_secs_f64(), rate);
    println!("verify 3 keys: {verify_ms} ms");
    println!(
        "pass 10min target? {}",
        if elapsed.as_secs_f64() <= 600.0 {
            "YES"
        } else {
            "NO"
        }
    );

    // 磁盘占用
    for name in ["data.log", "data.hint", "data.blob", "bitcask.wal", "bitcask.lock"] {
        let p = format!("{dir}/{name}");
        if let Ok(m) = std::fs::metadata(&p) {
            println!("  {name:14} {:>12} B", m.len());
        }
    }
}
