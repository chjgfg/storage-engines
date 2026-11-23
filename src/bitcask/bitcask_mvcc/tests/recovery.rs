// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn automatic_checkpoint_amortizes_hint_work() {
    use crate::bitcask::checkpoint_probe::Scope;
    let dir = tmp_db("hint_amortized");
    let db = MVCC::try_open(&dir).unwrap();
    db.set_wal_checkpoint_limit(1);
    let scope = Scope::start();
    for i in 0u64..64 {
        let tx = db.begin_transaction();
        tx.try_set_raw(&i.to_be_bytes(), vec![i as u8]).unwrap();
        tx.try_commit().unwrap();
    }
    let stats = scope.stats();
    assert_eq!(stats.checkpoints, 64, "WAL maintenance must still run");
    assert!(
        stats.hint_rows <= 3 * 64,
        "hint amplification: {} rows",
        stats.hint_rows
    );
    db.try_checkpoint().unwrap();
    assert_eq!(
        scope.stats().hint_rows - stats.hint_rows,
        64,
        "explicit checkpoint refreshes hint"
    );
    drop(scope);
    drop(db);
    let db = MVCC::try_open(&dir).unwrap();
    let tx = db.begin_transaction();
    for i in 0u64..64 {
        assert_eq!(tx.get(&i.to_be_bytes()), Some(vec![i as u8]));
    }
}

#[test]
#[ignore = "5M targeted checkpoint cost probe; run release, single thread"]
fn checkpoint_cost_5m() {
    use crate::bitcask::checkpoint_probe::Scope;
    const ROWS: usize = 5_000_000;
    fn key(i: usize) -> Vec<u8> {
        format!("{i:08}#/worldquant/datafields/x.json").into_bytes()
    }
    fn val(i: usize) -> Vec<u8> {
        format!("{{\"i\":{i},\"h\":\"{:0<28}\",\"v\":1}}", i * 7).into_bytes()
    }
    let dir = tmp_db("checkpoint_cost_5m");
    let db = MVCC::try_open(&dir).unwrap();
    for (phase, rows) in [("insert", ROWS), ("update", ROWS / 10)] {
        let scope = Scope::start();
        let start = std::time::Instant::now();
        for base in (0..rows).step_by(1000) {
            let tx = db.begin_transaction();
            for i in base..(base + 1000).min(rows) {
                let value = if phase == "insert" {
                    val(i)
                } else {
                    b"updated".to_vec()
                };
                tx.try_set(&key(i), value).unwrap();
            }
            tx.try_commit().unwrap();
            if (base + 1000) % 250_000 == 0 {
                eprintln!(
                    "CHECKPOINT_PROGRESS phase={phase} rows={} elapsed_s={:.3}",
                    base + 1000,
                    start.elapsed().as_secs_f64()
                );
            }
        }
        let elapsed = start.elapsed().as_secs_f64();
        let mut stats = scope.stats();
        stats.commit_ns.sort_unstable();
        let quantile =
            |p: usize| stats.commit_ns[(stats.commit_ns.len() * p).div_ceil(100) - 1] as f64 / 1e6;
        eprintln!(
            "CHECKPOINT_COST phase={phase} rows={rows} wall_s={elapsed:.6} us_per_row={:.3} checkpoints={} checkpoint_s={:.6} flush_s={:.6} hint_s={:.6} wal_s={:.6} hint_rows={} hint_bytes={} commit_p95_ms={:.3} commit_p99_ms={:.3} commit_max_ms={:.3}",
            elapsed * 1e6 / rows as f64,
            stats.checkpoints,
            stats.checkpoint_ns as f64 / 1e9,
            stats.flush_ns as f64 / 1e9,
            stats.hint_ns as f64 / 1e9,
            stats.wal_ns as f64 / 1e9,
            stats.hint_rows,
            stats.hint_bytes,
            quantile(95),
            quantile(99),
            quantile(100)
        );
        assert!(stats.checkpoints > 0);
        if phase == "insert" {
            assert!(stats.hint_rows > 0);
            assert!(
                stats.hint_rows <= 3 * rows as u64,
                "hint work must grow linearly"
            );
        }
    }
    db.try_checkpoint().unwrap();
    drop(db);
    let db = MVCC::try_open(&dir).unwrap();
    let tx = db.begin_transaction();
    let mut seen = 0;
    tx.export_latest_visible_for_each(false, |r| {
        assert_eq!(r.key, key(seen));
        assert_eq!(
            r.value,
            Some(if seen < ROWS / 10 {
                b"updated".to_vec()
            } else {
                val(seen)
            })
        );
        seen += 1;
        true
    });
    assert_eq!(seen, ROWS);
    eprintln!("CHECKPOINT_REOPEN verified_rows={seen}");
}

/// WAL 阈值触发必须真的把文件截住（对照「关闭自动触发」），且截断之后重开仍能读到
/// 全部已提交数据 —— 后半段守的是「先 flush 数据、再截 WAL」这个顺序：
/// 顺序反了就是**丢已提交写入**。
#[test]
fn test_wal_auto_checkpoint_bounds_file_and_keeps_data() {
    const WRITES: u32 = 40;

    fn wal_bytes_after(tag: &str, limit: u64) -> u64 {
        let path = tmp_db(tag);
        {
            let mvcc = MVCC::open(&path);
            mvcc.set_wal_checkpoint_limit(limit);
            for i in 0..WRITES {
                let key = format!("k{i}");
                let tx = mvcc.begin_transaction();
                assert!(tx.set(key.as_bytes(), vec![b'x'; 200]));
                tx.commit();
            }
        }
        let size = std::fs::metadata(wal_path(&path))
            .map(|m| m.len())
            .unwrap_or(0);
        cleanup(&path);
        size
    }

    let manual = wal_bytes_after("wal_manual", 0);
    let auto = wal_bytes_after("wal_auto", 1);
    eprintln!("WAL 体积：关闭自动 checkpoint {manual} 字节 vs 阈值触发 {auto} 字节");
    assert!(
        auto * 4 <= manual,
        "阈值触发应把 WAL 截住（自动 {auto}，手动 {manual}）"
    );

    let path = tmp_db("wal_reopen");
    {
        let mvcc = MVCC::open(&path);
        mvcc.set_wal_checkpoint_limit(1);
        for i in 0..WRITES {
            let key = format!("r{i}");
            let tx = mvcc.begin_transaction();
            assert!(tx.set(key.as_bytes(), format!("v{i}").into_bytes()));
            tx.commit();
        }
    }
    {
        let mvcc = MVCC::open(&path);
        let tx = mvcc.begin_transaction();
        for i in 0..WRITES {
            let key = format!("r{i}");
            assert_eq!(
                tx.get(key.as_bytes()),
                Some(format!("v{i}").into_bytes()),
                "第 {i} 条已提交写入在 WAL 被截断后仍应读到"
            );
        }
        tx.commit();
    }
    cleanup(&path);
}

#[test]
fn test_disk_persist_across_reopen() {
    let path = tmp_db("persist");
    {
        let mvcc = MVCC::open(&path);
        let t1 = mvcc.begin_transaction();
        assert!(t1.set(b"hello", b"world".to_vec()));
        t1.commit();
        let t2 = mvcc.begin_transaction();
        assert!(t2.set(b"hello", b"rust".to_vec()));
        assert!(t2.set(b"foo", b"bar".to_vec()));
        t2.commit();
        mvcc.flush();
    }
    {
        let mvcc = MVCC::open(&path);
        let tx = mvcc.begin_transaction();
        assert_eq!(tx.get(b"hello"), Some(b"rust".to_vec()));
        assert_eq!(tx.get(b"foo"), Some(b"bar".to_vec()));
        tx.commit();
    }
    cleanup(&path);
}
