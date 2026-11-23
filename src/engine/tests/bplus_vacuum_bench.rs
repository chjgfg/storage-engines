// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
#[ignore = "B+ vacuum redundant blob sync ABBA; release single thread"]
fn bplus_vacuum_sync_cost() {
    bplus_delete_cost(false, false, true);
}

#[test]
#[ignore = "B+ borrowed maintenance scan old/new/new/old; release single thread"]
fn bplus_maintenance_large_cost() {
    use crate::bplus_tree::bplus_tree::{MaintenanceEnd, maintenance_reference};
    let rows: usize = std::env::var("BPLUS_SCAN_ROWS")
        .map(|s| s.parse().unwrap())
        .unwrap_or(20_000_000);
    assert!(rows >= 10);
    let dir = ScaleScratch::new("bplus_maintenance_large");
    let path = dir.0.join("bplus.db");
    let db = crate::bplus_tree::MVCC::open(&path, 64, 1024);
    db.set_vacuum_auto_slice(0);
    eprintln!("BPLUS_LARGE_BUILD rows={rows}");
    let mut bulk = db.begin_bulk();
    for i in 0..rows {
        bulk.put(&scale_key(i), crate::common::pack_plain(&scale_val(i)));
    }
    bulk.try_finish().unwrap();
    for base in (0..rows / 10).step_by(1000) {
        let tx = db.begin_transaction();
        for i in base..(base + 1000).min(rows / 10) {
            assert!(tx.set_raw(&scale_key(i), scale_val(i + rows)));
        }
        tx.try_commit().unwrap();
    }
    let mut counts = Vec::new();
    let mut fingerprints = Vec::new();
    // 四个读取对照臂共用同一个不可变物理树，初始化和更新
    // 都放在计量区外；最终真实回收和重开独立核验。
    let tx = db.begin_transaction();
    for (round, reference) in [true, false, false, true].into_iter().enumerate() {
        let mode = reference.then(maintenance_reference::Scope::start);
        let scope = crate::common::maintenance_probe::Scope::start();
        let before = crate::mem_probe::allocations();
        let start = std::time::Instant::now();
        let mut seen = 0usize;
        let mut checksum = 0u64;
        let mut last = Vec::new();
        let end = tx
            .kv
            .lock()
            .unwrap()
            .scan_maintenance_from(None, |key, value| {
                if seen > 0 {
                    assert!(last.as_slice() < key);
                }
                last.clear();
                last.extend_from_slice(key);
                let value = value.expect("seeded physical row must have an encoded value");
                checksum = checksum
                    .wrapping_add(crate::common::disk::crc32(key) as u64)
                    .wrapping_add(crate::common::disk::crc32(value) as u64);
                seen += 1;
                false
            })
            .unwrap();
        let seconds = start.elapsed().as_secs_f64();
        let allocations = crate::mem_probe::allocations() - before;
        assert_eq!(end, MaintenanceEnd::Complete);
        assert_eq!(seen, rows + rows / 10);
        eprintln!(
            "BPLUS_LARGE_READER round={round} reference={reference} rows={rows} physical={seen} checksum={checksum} seconds={seconds:.6} allocations={allocations} stages={:?}",
            scope.stats()
        );
        counts.push(allocations);
        fingerprints.push(checksum);
        drop(scope);
        drop(mode);
    }
    tx.try_commit().unwrap();
    drop(tx);
    assert!(fingerprints.iter().all(|f| *f == fingerprints[0]));
    assert!(counts[0] >= counts[1] + 2 * (rows + rows / 10) as u64);
    assert!(counts[3] >= counts[2] + 2 * (rows + rows / 10) as u64);
    let start = std::time::Instant::now();
    let vacuum = db.vacuum().unwrap();
    assert_eq!(vacuum.rows_scanned, (rows + rows / 10) as u64);
    assert_eq!(vacuum.versions_removed, rows / 10);
    eprintln!(
        "BPLUS_LARGE_VACUUM rows={rows} seconds={:.6} stats={vacuum:?}",
        start.elapsed().as_secs_f64()
    );
    drop(db);
    let reopened = crate::bplus_tree::MVCC::open(&path, 64, 1024);
    let mut seen = 0;
    reopened.export_latest_visible_for_each(false, |r| {
        assert_eq!(r.key, scale_key(seen));
        assert_eq!(
            r.value,
            Some(scale_val(if seen < rows / 10 { seen + rows } else { seen }))
        );
        seen += 1;
        true
    });
    assert_eq!(seen, rows);
    eprintln!("BPLUS_LARGE_VERIFIED rows={seen}");
}

#[test]
#[ignore = "B+ borrowed maintenance scan old/new/new/old; release single thread"]
fn bplus_maintenance_scan_cost() {
    use crate::common::maintenance_probe::{Scope, Stage};
    let rows: usize = std::env::var("BPLUS_SCAN_ROWS")
        .map(|s| s.parse().unwrap())
        .unwrap_or(600_000);
    let mut costs = Vec::new();
    for (round, reference) in [true, false, false, true].into_iter().enumerate() {
        let dir = ScaleScratch::new("bplus_maintenance_scan");
        let path = dir.0.join("bplus.db");
        let db = crate::bplus_tree::MVCC::open(&path, 64, 1024);
        db.set_vacuum_auto_slice(0);
        let mut bulk = db.begin_bulk();
        for i in 0..rows {
            bulk.put(&scale_key(i), crate::common::pack_plain(&scale_val(i)));
        }
        bulk.try_finish().unwrap();
        for base in (0..rows / 10).step_by(1000) {
            let tx = db.begin_transaction();
            for i in base..(base + 1000).min(rows / 10) {
                assert!(tx.set_raw(&scale_key(i), scale_val(i + rows)));
            }
            tx.try_commit().unwrap();
        }
        let mode =
            reference.then(crate::bplus_tree::bplus_tree::maintenance_reference::Scope::start);
        let scope = Scope::start();
        let start = std::time::Instant::now();
        let result = db.vacuum().unwrap();
        let seconds = start.elapsed().as_secs_f64();
        let stats = scope.stats();
        costs.push(stats.allocations[Stage::VacuumScan as usize]);
        assert_eq!(stats.bplus_maintenance_pages == 0, reference);
        if !reference {
            assert!(stats.bplus_maintenance_rows >= result.rows_scanned);
        }
        assert_eq!(result.versions_removed, rows / 10);
        assert_eq!(result.rows_scanned, (rows + rows / 10) as u64);
        eprintln!(
            "BPLUS_MAINTENANCE_SCAN round={round} reference={reference} rows={rows} seconds={seconds:.6} stages={stats:?}"
        );
        drop(scope);
        drop(mode);
        drop(db);
        let db = crate::bplus_tree::MVCC::open(&path, 64, 1024);
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
#[ignore = "B+ vacuum scan and delete old/new/new/old; release single thread"]
fn bplus_vacuum_buffer_cost() {
    use crate::common::maintenance_probe::{Scope, Stage};
    let rows: usize = std::env::var("BPLUS_VAC_ROWS")
        .map(|s| s.parse().unwrap())
        .unwrap_or(600_000);
    let mut costs = Vec::new();
    for (round, reference) in [true, false, false, true].into_iter().enumerate() {
        let dir = ScaleScratch::new("bplus_vacuum_buffers");
        let path = dir.0.join("bplus.db");
        let db = crate::bplus_tree::MVCC::open(&path, 64, 1024);
        db.set_vacuum_auto_slice(0);
        let mut bulk = db.begin_bulk();
        for i in 0..rows {
            bulk.put(&scale_key(i), crate::common::pack_plain(&scale_val(i)));
        }
        bulk.try_finish().unwrap();
        for base in (0..rows / 10).step_by(1000) {
            let tx = db.begin_transaction();
            for i in base..(base + 1000).min(rows / 10) {
                assert!(tx.set_raw(&scale_key(i), scale_val(i + rows)));
            }
            tx.try_commit().unwrap();
        }
        let mode =
            reference.then(crate::bplus_tree::bplus_tree_mvcc::vacuum_scan_reference::Scope::start);
        let scope = Scope::start();
        let start = std::time::Instant::now();
        let result = db.vacuum().unwrap();
        let seconds = start.elapsed().as_secs_f64();
        let stats = scope.stats();
        costs.push(stats.allocations[Stage::VacuumScan as usize]);
        assert_eq!(result.versions_removed, rows / 10);
        assert_eq!(result.rows_scanned, (rows + rows / 10) as u64);
        eprintln!(
            "BPLUS_VACUUM_BUFFER round={round} reference={reference} rows={rows} seconds={seconds:.6} stages={stats:?}"
        );
        drop(scope);
        drop(mode);
        drop(db);
        let db = crate::bplus_tree::MVCC::open(&path, 64, 1024);
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
