// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
#[ignore = "bitcask vacuum old/new/new/old; release single thread"]
fn bitcask_vacuum_buffer_cost() {
    let rows: usize = std::env::var("BITCASK_VAC_ROWS")
        .map(|v| v.parse().unwrap())
        .unwrap_or(600_000);
    assert!(rows >= 10);
    let mut costs = Vec::new();
    for (round, reference) in [true, false, false, true].into_iter().enumerate() {
        let dir = ScaleScratch::new("bitcask_vacuum_buffer");
        let db = crate::bitcask::MVCC::open(&dir.0);
        db.set_vacuum_auto_slice(0);
        let mut bulk = db.begin_bulk();
        for i in 0..rows {
            bulk.put(&scale_key(i), crate::common::pack_plain(&scale_val(i)));
        }
        bulk.finish();
        let mut bulk = db.begin_bulk();
        for i in 0..rows / 10 {
            bulk.put(
                &scale_key(i),
                crate::common::pack_plain(&scale_val(i + rows)),
            );
        }
        bulk.finish();
        let mode =
            reference.then(crate::bitcask::bitcask_mvcc::vacuum_cost_reference::Scope::start);
        let probe = crate::common::maintenance_probe::Scope::start();
        let before_reads = crate::bitcask::bitcask::read_value_calls();
        let before = crate::mem_probe::allocations();
        let start = std::time::Instant::now();
        let stats = db.vacuum().unwrap();
        let seconds = start.elapsed().as_secs_f64();
        let allocations = crate::mem_probe::allocations() - before;
        let reads = crate::bitcask::bitcask::read_value_calls() - before_reads;
        costs.push(allocations);
        eprintln!(
            "BITCASK_VACUUM_BUFFER round={round} reference={reference} rows={rows} seconds={seconds:.6} allocations={allocations} value_reads={reads} stages={:?}",
            probe.stats()
        );
        assert_eq!(stats.versions_removed, rows / 10);
        assert_eq!(stats.rows_scanned, (rows + rows / 10) as u64);
        assert_eq!(reads, if reference { rows as u64 } else { 0 });
        drop(probe);
        drop(mode);
        drop(db);
        let db = crate::bitcask::MVCC::open(&dir.0);
        let mut seen = 0;
        db.export_latest_visible_for_each(false, |r| {
            assert_eq!(r.key, scale_key(seen));
            assert_eq!(
                r.value,
                Some(scale_val(if seen < rows / 10 { seen + rows } else { seen }))
            );
            seen += 1;
            true
        });
        assert_eq!(seen, rows);
    }
    assert!(costs[0] >= costs[1] + 2 * rows as u64);
    assert!(costs[3] >= costs[2] + 2 * rows as u64);
}
