//! 简单压测：纯内存 MVCC 写入 / 读取吞吐
//!
//! ```bash
//! cargo run --release --bin bench -- --n 100000
//! cargo run --release --bin bench -- --n 100000 --bulk
//! ```

use std::assert;
use std::assert_eq;
use std::println;
use std::eprintln;
use std::format;
use storage_engines::memory::MVCC;
use std::env;
use std::time::Instant;

fn main() {
    let mut n: usize = 100_000;
    let mut bulk = false;
    let mut args = env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--n" => {
                n = args
                    .next()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(n);
            }
            "--bulk" => bulk = true,
            "-h" | "--help" => {
                eprintln!("usage: bench [--n N] [--bulk]");
                return;
            }
            other => eprintln!("unknown arg: {other}"),
        }
    }

    println!("memory bench: n={n} bulk={bulk}");
    let mvcc = MVCC::new();

    if bulk {
        let t0 = Instant::now();
        {
            let mut b = mvcc.begin_bulk();
            let mut batch = Vec::with_capacity(1024);
            for i in 0..n {
                let k = format!("k{i:08}").into_bytes();
                let v = format!("v{i}").into_bytes();
                batch.push((k, v));
                if batch.len() >= 1024 {
                    b.put_batch_owned(std::mem::take(&mut batch));
                }
            }
            if !batch.is_empty() {
                b.put_batch_owned(batch);
            }
            b.finish();
        }
        let elapsed = t0.elapsed();
        let ops = n as f64 / elapsed.as_secs_f64();
        println!(
            "bulk put: {n} keys in {elapsed:?} → {:.0} ops/s",
            ops
        );
    } else {
        let t0 = Instant::now();
        {
            let tx = mvcc.begin_transaction();
            for i in 0..n {
                let k = format!("k{i:08}").into_bytes();
                let v = format!("v{i}").into_bytes();
                assert!(tx.set(&k, v));
            }
            tx.commit();
        }
        let elapsed = t0.elapsed();
        let ops = n as f64 / elapsed.as_secs_f64();
        println!(
            "tx put:   {n} keys in {elapsed:?} → {:.0} ops/s",
            ops
        );
    }

    // 读
    let t0 = Instant::now();
    {
        let tx = mvcc.begin_transaction();
        let mut hits = 0usize;
        for i in 0..n {
            let k = format!("k{i:08}").into_bytes();
            if tx.get(&k).is_some() {
                hits += 1;
            }
        }
        tx.commit();
        assert_eq!(hits, n);
    }
    let elapsed = t0.elapsed();
    let ops = n as f64 / elapsed.as_secs_f64();
    println!(
        "get:      {n} keys in {elapsed:?} → {:.0} ops/s",
        ops
    );

    // 前缀扫描
    let t0 = Instant::now();
    {
        let tx = mvcc.begin_transaction();
        let rows = tx.prefix_scan(b"k");
        tx.commit();
        assert_eq!(rows.len(), n);
    }
    let elapsed = t0.elapsed();
    println!("prefix_scan all: {n} rows in {elapsed:?}");

    println!("raw_len (versions) = {}", mvcc.raw_len());
}
