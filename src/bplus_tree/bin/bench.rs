//! 压测工具
//!
//! ```text
//! # 普通事务路径
//! cargo run --release --bin bench -- --n 100000 --order 128 --cache 2048 --batch 5000
//!
//! # bulk load（关 WAL，冲千万）
//! cargo run --release --bin bench -- --n 10000000 --order 256 --cache 8192 --bulk --value-size 16
//! ```

use bplus_tree::MVCC;
use std::env;
use std::path::PathBuf;
use std::time::Instant;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Write,
    WriteRead,
    Mix,
}

struct Args {
    n: usize,
    order: usize,
    cache: usize,
    batch: usize,
    mode: Mode,
    value_size: usize,
    db: PathBuf,
    keep: bool,
    bulk: bool,
}

fn parse_args() -> Args {
    let mut n = 10_000usize;
    let mut order = 128usize;
    let mut cache = 2048usize;
    let mut batch = 5_000usize;
    let mut mode = Mode::WriteRead;
    let mut value_size = 16usize;
    let mut db = PathBuf::from("bench_data.db");
    let mut keep = false;
    let mut bulk = false;

    let mut args = env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--n" => n = args.next().unwrap().parse().unwrap(),
            "--order" => order = args.next().unwrap().parse().unwrap(),
            "--cache" => cache = args.next().unwrap().parse().unwrap(),
            "--batch" => batch = args.next().unwrap().parse().unwrap(),
            "--value-size" => value_size = args.next().unwrap().parse().unwrap(),
            "--db" => db = PathBuf::from(args.next().unwrap()),
            "--keep" => keep = true,
            "--bulk" => bulk = true,
            "--mode" => {
                mode = match args.next().unwrap().as_str() {
                    "write" => Mode::Write,
                    "mix" => Mode::Mix,
                    _ => Mode::WriteRead,
                }
            }
            "-h" | "--help" => {
                print_help();
                std::process::exit(0);
            }
            other => {
                eprintln!("未知参数: {other}");
                print_help();
                std::process::exit(1);
            }
        }
    }

    Args {
        n,
        order,
        cache,
        batch: batch.max(1),
        mode,
        value_size: value_size.max(1),
        db,
        keep,
        bulk,
    }
}

fn print_help() {
    eprintln!(
        "\
bplus-tree 压测

  --n N            记录数 (default 10000)
  --order N        B+ 树阶数 (default 128；建议 128~256)
  --cache N        缓存页数 (default 2048)
  --batch N        普通模式每 N 条 commit (default 5000；bulk 忽略)
  --value-size N   value 字节数 (default 16)
  --mode MODE      write | write-read | mix (default write-read)
  --bulk           bulk load：关 WAL/冲突检测，结束一次 fsync
  --db PATH        数据文件 (default bench_data.db)
  --keep           结束后保留文件
  -h, --help
"
    );
}

fn key_of(i: usize) -> Vec<u8> {
    // 固定 12 字节：'k' + 11 位十进制，避免 format! 分配抖动
    let mut buf = [b'0'; 12];
    buf[0] = b'k';
    let mut n = i;
    for pos in (1..12).rev() {
        buf[pos] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    buf.to_vec()
}

fn value_of(i: usize, size: usize) -> Vec<u8> {
    let mut v = Vec::with_capacity(size);
    v.extend_from_slice(b"v");
    let mut n = i;
    let mut digits = [0u8; 20];
    let mut len = 0;
    if n == 0 {
        digits[0] = b'0';
        len = 1;
    } else {
        while n > 0 {
            digits[len] = b'0' + (n % 10) as u8;
            n /= 10;
            len += 1;
        }
        digits[..len].reverse();
    }
    v.extend_from_slice(&digits[..len]);
    if v.len() < size {
        v.resize(size, b'x');
    } else {
        v.truncate(size);
    }
    v
}

fn cleanup(path: &std::path::Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.wal", path.display()));
    let _ = std::fs::remove_file(format!("{}.dblwr", path.display()));
    let _ = std::fs::remove_file(format!("{}.freelist", path.display()));
    let _ = std::fs::remove_file(format!("{}.lock", path.display()));
    let _ = std::fs::remove_file(format!("{}.blob", path.display()));
}

fn file_size(path: &std::path::Path) -> u64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

fn main() {
    let args = parse_args();
    cleanup(&args.db);

    println!("=== bplus-tree bench ===");
    println!(
        "n={} order={} cache={} batch={} value_size={} bulk={} mode={} db={:?}",
        args.n,
        args.order,
        args.cache,
        args.batch,
        args.value_size,
        args.bulk,
        match args.mode {
            Mode::Write => "write",
            Mode::WriteRead => "write-read",
            Mode::Mix => "mix",
        },
        args.db
    );
    println!("提示: 务必 --release；千万级请加 --bulk");
    println!();

    let mvcc = MVCC::open(&args.db, args.order, args.cache);

    let (write_ops, write_elapsed) = if args.bulk {
        // ── bulk load（分批加锁，降低 Mutex 开销）──
        let t0 = Instant::now();
        let mut bulk = mvcc.begin_bulk();
        let report_every = (args.n / 10).max(1);
        const BATCH: usize = 512;
        let mut buf: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(BATCH);
        for i in 0..args.n {
            buf.push((key_of(i), value_of(i, args.value_size)));
            if buf.len() >= BATCH {
                bulk.put_batch_owned(std::mem::take(&mut buf));
                buf = Vec::with_capacity(BATCH);
            }
            if i > 0 && i % report_every == 0 {
                let elapsed = t0.elapsed().as_secs_f64();
                let rate = i as f64 / elapsed.max(1e-9);
                eprintln!(
                    "  ... {i}/{} ({:.1}%)  {:.0} ops/s  elapsed={:.1}s",
                    args.n,
                    100.0 * i as f64 / args.n as f64,
                    rate,
                    elapsed
                );
            }
        }
        if !buf.is_empty() {
            bulk.put_batch_owned(buf);
        }
        bulk.finish();
        let elapsed = t0.elapsed();
        (args.n as f64 / elapsed.as_secs_f64().max(1e-9), elapsed)
    } else {
        // ── 普通事务 ──
        let t0 = Instant::now();
        let mut committed = 0usize;
        let mut tx = mvcc.begin_transaction();
        let mut in_batch = 0usize;
        for i in 0..args.n {
            let ok = tx.set(&key_of(i), value_of(i, args.value_size));
            if !ok {
                panic!("set 冲突/失败 at {i}");
            }
            in_batch += 1;
            if in_batch >= args.batch {
                tx.commit();
                committed += in_batch;
                in_batch = 0;
                tx = mvcc.begin_transaction();
            }
            if args.mode == Mode::Mix && i % 10 == 9 {
                let _ = tx.get(&key_of(i / 2));
            }
        }
        if in_batch > 0 {
            tx.commit();
            committed += in_batch;
        }
        mvcc.checkpoint();
        let elapsed = t0.elapsed();
        (committed as f64 / elapsed.as_secs_f64().max(1e-9), elapsed)
    };

    println!(
        "WRITE  {} ops in {:.3}s  →  {:.0} ops/s{}",
        args.n,
        write_elapsed.as_secs_f64(),
        write_ops,
        if args.bulk { "  [bulk]" } else { "" }
    );

    if args.mode != Mode::Write {
        let t1 = Instant::now();
        let tx = mvcc.begin_transaction();
        let mut hits = 0usize;
        // 抽样读（全量千万读太久）；n<=1e5 全读，否则抽 10 万
        let read_n = args.n.min(100_000);
        let step = (args.n / read_n).max(1);
        let mut checked = 0usize;
        let mut i = 0usize;
        while i < args.n && checked < read_n {
            if tx.get(&key_of(i)).is_some() {
                hits += 1;
            }
            checked += 1;
            i += step;
        }
        let read_elapsed = t1.elapsed();
        let read_ops = checked as f64 / read_elapsed.as_secs_f64().max(1e-9);
        println!(
            "READ   {hits}/{checked} hits in {:.3}s  →  {:.0} ops/s",
            read_elapsed.as_secs_f64(),
            read_ops
        );
        tx.commit();
    }

    let db_sz = file_size(&args.db);
    let wal_sz = file_size(std::path::Path::new(&format!("{}.wal", args.db.display())));
    let dbl_sz = file_size(std::path::Path::new(&format!(
        "{}.dblwr",
        args.db.display()
    )));
    println!(
        "SIZE   data={:.2} MB  wal={:.2} MB  dblwr={:.2} MB",
        db_sz as f64 / 1e6,
        wal_sz as f64 / 1e6,
        dbl_sz as f64 / 1e6
    );

    // 重启抽样
    drop(mvcc);
    let t3 = Instant::now();
    let mvcc2 = MVCC::open(&args.db, args.order, args.cache);
    let tx = mvcc2.begin_transaction();
    for &i in &[0usize, args.n / 2, args.n.saturating_sub(1)] {
        if i < args.n {
            assert!(tx.get(&key_of(i)).is_some(), "重启后丢失 key {i}");
        }
    }
    tx.commit();
    println!(
        "REOPEN ok in {:.3}s (抽样校验通过)",
        t3.elapsed().as_secs_f64()
    );

    let target = 10_000_000.0 / 600.0; // 1000万 / 10分钟
    if args.n >= 1_000_000 {
        println!(
            "目标参考: 1000万/10分钟 ≈ {:.0} ops/s；本次 {:.0} ops/s → {}",
            target,
            write_ops,
            if write_ops >= target {
                "达标"
            } else {
                "未达标"
            }
        );
    }

    if !args.keep {
        cleanup(&args.db);
        println!("已清理临时文件（加 --keep 可保留）");
    } else {
        println!("文件已保留: {:?}", args.db);
    }
}
