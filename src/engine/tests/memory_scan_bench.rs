// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
#[ignore = "memory vacuum allocation old/new/new/old; release single thread"]
fn memory_vacuum_buffer_cost() {
    let rows: usize = std::env::var("VACUUM_ROWS")
        .map(|v| v.parse().unwrap())
        .unwrap_or(600_000);
    assert!(rows >= 10);
    let mut costs = Vec::new();
    for (round, reference) in [true, false, false, true].into_iter().enumerate() {
        let db = crate::memory::MVCC::new();
        db.set_vacuum_auto_slice(0);
        for base in (0..rows).step_by(1000) {
            let tx = db.begin_transaction();
            for i in base..(base + 1000).min(rows) {
                assert!(tx.set_raw(&scale_key(i), scale_val(i)));
            }
            tx.try_commit().unwrap();
        }
        let tx = db.begin_transaction();
        for i in 0..rows / 10 {
            assert!(tx.set_raw(&scale_key(i), scale_val(i + rows)));
        }
        tx.try_commit().unwrap();
        drop(tx);
        let mode = reference.then(crate::memory::memory_mvcc::vacuum_cost_reference::Scope::start);
        let probe = crate::common::maintenance_probe::Scope::start();
        let before = crate::mem_probe::allocations();
        let start = std::time::Instant::now();
        let stats = db.vacuum();
        let seconds = start.elapsed().as_secs_f64();
        let allocations = crate::mem_probe::allocations() - before;
        costs.push(allocations);
        eprintln!(
            "MEMORY_VACUUM_BUFFER round={round} reference={reference} rows={rows} seconds={seconds:.6} allocations={allocations} stages={:?}",
            probe.stats()
        );
        assert_eq!(stats.versions_removed, rows / 10);
        assert_eq!(stats.rows_scanned, (rows + rows / 10) as u64);
        drop(probe);
        drop(mode);
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

#[test]
#[ignore = "memory scan and pagination old/new/new/old; release single thread"]
fn memory_scan_buffer_cost() {
    use crate::common::store::scan_cost_reference;
    let rows: usize = std::env::var("SCAN_ROWS")
        .map(|s| s.parse().unwrap())
        .unwrap_or(600_000);
    assert!(rows >= 1000);
    for ttl in [false, true] {
        let expiry = ttl.then_some(u64::MAX);
        let db = crate::memory::MVCC::new();
        db.set_vacuum_auto_slice(0);
        for base in (0..rows).step_by(1000) {
            let tx = db.begin_transaction();
            for i in base..(base + 1000).min(rows) {
                let value = scale_val(i);
                let logical = match expiry {
                    Some(e) => crate::common::pack_ttl(&value, e),
                    None => crate::common::pack_plain(&value),
                };
                assert!(tx.set(&scale_key(i), logical));
            }
            tx.try_commit().unwrap();
        }
        let tx = db.begin_transaction();
        for i in 0..rows / 10 {
            let value = scale_val(i + rows);
            let logical = match expiry {
                Some(e) => crate::common::pack_ttl(&value, e),
                None => crate::common::pack_plain(&value),
            };
            assert!(tx.set(&scale_key(i), logical));
        }
        tx.try_commit().unwrap();
        drop(tx);
        let mut full_costs = Vec::new();
        let mut page_costs = Vec::new();
        for (round, reference) in [true, false, false, true].into_iter().enumerate() {
            let _mode = reference.then(scan_cost_reference::Scope::start);
            let tx = db.begin_transaction();
            let key_before = scan_cost_reference::key_copies();
            let cursor_before = scan_cost_reference::fold_cursor_copies();
            let before = crate::mem_probe::allocations();
            let start = std::time::Instant::now();
            let full = tx.scan(None, None);
            let seconds = start.elapsed().as_secs_f64();
            let allocations = crate::mem_probe::allocations() - before;
            let key_copies = scan_cost_reference::key_copies() - key_before;
            let fold_cursor_copies = scan_cost_reference::fold_cursor_copies() - cursor_before;
            full_costs.push(allocations);
            assert_eq!(full.len(), rows);
            for (i, rec) in full.iter().enumerate() {
                assert_eq!(rec.key, scale_key(i));
                assert_eq!(
                    rec.value,
                    Some(scale_val(if i < rows / 10 { i + rows } else { i }))
                );
                assert_eq!(rec.expire_unix_secs, expiry);
            }
            let mut cursor = None;
            let cursor_before = scan_cost_reference::page_cursor_copies();
            let (mut seen, mut pages, mut page_ns, mut page_allocations) = (0, 0, 0_u128, 0_u64);
            loop {
                let before = crate::mem_probe::allocations();
                let start = std::time::Instant::now();
                let (chunk, next) = tx.scan_page(cursor.as_deref(), 500);
                page_ns += start.elapsed().as_nanos();
                page_allocations += crate::mem_probe::allocations() - before;
                assert_eq!(chunk, full[seen..seen + chunk.len()]);
                seen += chunk.len();
                pages += 1;
                assert!(pages <= rows / 250 + 8, "pagination must progress");
                let Some(next) = next else {
                    break;
                };
                assert!(cursor.as_ref().is_none_or(|old| old < &next));
                cursor = Some(next);
            }
            assert_eq!(seen, rows);
            page_costs.push(page_allocations);
            let chunks = rows.div_ceil(crate::common::kernel::FOLD_CHUNK);
            let revisited: usize = (crate::common::kernel::FOLD_CHUNK..rows)
                .step_by(crate::common::kernel::FOLD_CHUNK)
                .map(|handed| if handed - 1 < rows / 10 { 2 } else { 1 })
                .sum();
            assert_eq!(
                key_copies,
                if reference {
                    (rows + rows / 10 + revisited) as u64
                } else {
                    rows as u64
                }
            );
            assert_eq!(
                fold_cursor_copies,
                if reference {
                    rows as u64
                } else {
                    (chunks - 1) as u64
                }
            );
            let page_cursor_copies = scan_cost_reference::page_cursor_copies() - cursor_before;
            assert_eq!(
                page_cursor_copies,
                if reference {
                    rows as u64
                } else {
                    (pages - 1) as u64
                }
            );
            eprintln!(
                "MEMORY_SCAN_BUFFER round={round} reference={reference} ttl={ttl} rows={rows} scan_seconds={seconds:.6} scan_allocations={allocations} scan_key_copies={key_copies} fold_cursor_copies={fold_cursor_copies} page_ms={:.3} page_allocations={page_allocations} page_cursor_copies={page_cursor_copies} pages={pages}",
                page_ns as f64 / 1e6
            );
        }
        for (old, new) in [(0, 1), (3, 2)] {
            assert!(full_costs[old] >= full_costs[new] + 2 * rows as u64);
            assert!(page_costs[old] >= page_costs[new] + 2 * rows as u64);
        }
    }
}
