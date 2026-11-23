// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
#[ignore = "LSM shared scan rows ABBA; release single thread"]
fn lsm_scan_row_cost() {
    use crate::lsm_tree::{
        lsm_tree_mvcc::scan_row_reference,
        sst::{block_reads, entry_value_clones, point_read_reference, sst_entry_walks, sst_probes},
    };
    let rows: usize = std::env::var("LSM_SCAN_ROWS")
        .map(|v| v.parse().unwrap())
        .unwrap_or(600_000);
    assert!(rows >= 1000);
    crate::scale_probe::fingerprint();
    // 显式传入的压测种子只复用，不归本测试清理。
    // 清理守卫只拥有本测试自己创建的目录。
    let existing = std::env::var_os("LSM_SCAN_EXISTING").map(PathBuf::from);
    let owned = existing
        .is_none()
        .then(|| ScaleScratch::new("lsm_scan_rows"));
    let path = existing
        .as_ref()
        .unwrap_or_else(|| &owned.as_ref().unwrap().0);
    let updates: usize = std::env::var("LSM_SCAN_UPDATES")
        .map(|v| v.parse().unwrap())
        .unwrap_or(0);
    assert!(updates <= rows && (existing.is_some() || updates == 0));
    if existing.is_some() {
        assert!(
            path.join("MANIFEST").is_file(),
            "existing fixture must already be an LSM database"
        );
    }
    if owned.is_some() {
        let db = crate::lsm_tree::MVCC::open(path);
        let mut bulk = db.begin_bulk();
        for i in 0..rows {
            bulk.put(&scale_key(i), scale_val(i));
            if (i + 1) % 1_000_000 == 0 {
                eprintln!("LSM_SCAN_BUILD rows={}", i + 1);
            }
        }
        bulk.finish();
    }
    for (round, old) in [true, false, false, true].into_iter().enumerate() {
        let db = crate::lsm_tree::MVCC::open(path);
        if existing.is_some() {
            let mode = old.then(point_read_reference::Scope::start);
            let copies = entry_value_clones();
            let walks = sst_entry_walks();
            let start = std::time::Instant::now();
            let tx = db.begin_transaction();
            let reads = rows.min(200_000);
            for j in 0..reads {
                let i = j * 104729 % rows;
                assert_eq!(
                    tx.get(&scale_key(i)),
                    Some(scale_val(if i < updates { i + rows } else { i }))
                );
            }
            tx.commit();
            eprintln!(
                "LSM_REUSE_GET round={round} reference={old} rows={rows} reads={reads} seconds={:.6} value_clones={} matching_or_walked_rows={}",
                start.elapsed().as_secs_f64(),
                entry_value_clones() - copies,
                sst_entry_walks() - walks
            );
            let tx = db.begin_transaction();
            let copies = entry_value_clones();
            for j in 0..1000 {
                assert!(tx.set_raw(&scale_key(j * 104729 % rows), vec![7; 64]));
            }
            let copies = entry_value_clones() - copies;
            if old {
                assert!(copies > 0);
            } else {
                assert_eq!(copies, 0);
            }
            tx.rollback();
            eprintln!(
                "LSM_REUSE_CONFLICT round={round} reference={old} writes=1000 value_clones={copies} rolled_back=true"
            );
            drop(mode);
        }
        let mode = old.then(scan_row_reference::Scope::start);
        let probes = sst_probes();
        let blocks = block_reads();
        let cpu = crate::scale_probe::Phase::start();
        let start = std::time::Instant::now();
        let mut seen = 0;
        db.export_latest_visible_for_each(false, |row| {
            assert_eq!(row.key, scale_key(seen));
            assert_eq!(
                row.value,
                Some(scale_val(if seen < updates { seen + rows } else { seen }))
            );
            seen += 1;
            true
        });
        let probes = sst_probes() - probes;
        assert_eq!(seen, rows);
        if old {
            assert!(probes >= rows as u64);
        } else {
            assert_eq!(probes, 0, "ordinary scan must use retained rows");
        }
        eprintln!(
            "LSM_SCAN_ROWS round={round} reference={old} verified={seen} seconds={:.6} point_probes={probes} blocks={}",
            start.elapsed().as_secs_f64(),
            block_reads() - blocks
        );
        cpu.finish("lsm_tree", &format!("scan_rows_round_{round}"));
        drop(mode);
    }
}

#[test]
#[ignore = "LSM ordinary fill/get/update ABBA; release single thread"]
fn lsm_ordinary_rw_cost() {
    use crate::lsm_tree::sst::{
        block_misses, block_reads, entry_value_clones, point_read_reference, sst_entry_walks,
    };
    let rows: usize = std::env::var("LSM_RW_ROWS")
        .map(|v| v.parse().unwrap())
        .unwrap_or(600_000);
    let reads: usize = std::env::var("LSM_RW_READS")
        .map(|v| v.parse().unwrap())
        .unwrap_or(rows.min(200_000));
    assert!(rows >= 1000 && reads > 0 && reads <= rows);
    crate::scale_probe::fingerprint();
    for (round, old) in [true, false, false, true].into_iter().enumerate() {
        let dir = ScaleScratch::new("lsm_ordinary_rw");
        eprintln!(
            "LSM_RW_BEGIN round={round} reference={old} rows={rows} reads={reads} scratch={}",
            dir.0.display()
        );
        let db = crate::lsm_tree::MVCC::open(&dir.0);
        let mode = old.then(point_read_reference::Scope::start);
        let maint = crate::lsm_tree::lsm_tree::maintenance_probe::Scope::start();
        let mut stages = [0_u128; 3];
        let cpu = crate::scale_probe::Phase::start();
        let allocations = crate::mem_probe::allocations();
        let start = std::time::Instant::now();
        for base in (0..rows).step_by(1000) {
            let items: Vec<_> = (base..(base + 1000).min(rows))
                .map(|i| (scale_key(i), scale_val(i)))
                .collect();
            let clock = std::time::Instant::now();
            let tx = db.begin_transaction();
            stages[0] += clock.elapsed().as_nanos();
            let clock = std::time::Instant::now();
            assert!(tx.batch_set(&items).is_ok());
            stages[1] += clock.elapsed().as_nanos();
            let clock = std::time::Instant::now();
            tx.commit();
            stages[2] += clock.elapsed().as_nanos();
            if (base + 1000) % 1_000_000 == 0 {
                eprintln!(
                    "LSM_RW_FILL_PROGRESS round={round} inserted={}",
                    base + 1000
                );
            }
        }
        eprintln!(
            "LSM_RW_FILL round={round} reference={old} rows={rows} seconds={:.6} allocations={} begin_ns={} write_ns={} commit_ns={} maintenance={:?}",
            start.elapsed().as_secs_f64(),
            crate::mem_probe::allocations() - allocations,
            stages[0],
            stages[1],
            stages[2],
            maint.stats()
        );
        cpu.finish("lsm_tree", &format!("rw_fill_round_{round}"));
        db.flush();
        db.checkpoint();
        drop(maint);
        drop(mode);
        drop(db);
        let db = crate::lsm_tree::MVCC::open(&dir.0);
        let order = shuffled(rows);
        let mode = old.then(point_read_reference::Scope::start);
        let blocks = block_reads();
        let misses = block_misses();
        let walks = sst_entry_walks();
        let clones = entry_value_clones();
        let allocations = crate::mem_probe::allocations();
        let cpu = crate::scale_probe::Phase::start();
        let start = std::time::Instant::now();
        let tx = db.begin_transaction();
        for &i in &order[..reads] {
            assert_eq!(tx.get(&scale_key(i)), Some(scale_val(i)));
        }
        tx.commit();
        eprintln!(
            "LSM_RW_GET round={round} reference={old} rows={rows} reads={reads} seconds={:.6} allocations={} blocks={} misses={} matching_or_walked_rows={} value_clones={}",
            start.elapsed().as_secs_f64(),
            crate::mem_probe::allocations() - allocations,
            block_reads() - blocks,
            block_misses() - misses,
            sst_entry_walks() - walks,
            entry_value_clones() - clones
        );
        cpu.finish("lsm_tree", &format!("rw_get_round_{round}"));
        let updates = rows / 10;
        let cpu = crate::scale_probe::Phase::start();
        let start = std::time::Instant::now();
        let clones = entry_value_clones();
        for base in (0..updates).step_by(1000) {
            let tx = db.begin_transaction();
            for i in base..(base + 1000).min(updates) {
                assert!(tx.set_raw(&scale_key(i), scale_val(i + rows)));
            }
            tx.commit();
        }
        eprintln!(
            "LSM_RW_UPDATE round={round} reference={old} rows={rows} updates={updates} seconds={:.6} value_clones={}",
            start.elapsed().as_secs_f64(),
            entry_value_clones() - clones
        );
        cpu.finish("lsm_tree", &format!("rw_update_round_{round}"));
        drop(mode);
        drop(db);
        let db = crate::lsm_tree::MVCC::open(&dir.0);
        let mut seen = 0;
        db.export_latest_visible_for_each(false, |record| {
            assert_eq!(record.key, scale_key(seen));
            assert_eq!(
                record.value,
                Some(scale_val(if seen < updates { seen + rows } else { seen }))
            );
            seen += 1;
            true
        });
        assert_eq!(seen, rows);
        eprintln!("LSM_RW_VERIFIED round={round} reference={old} unique_keys={seen}");
    }
}

#[test]
#[ignore = "LSM packed mixed reads ABBA, then updates/reopen; release single thread"]
fn lsm_mixed_read_cost() {
    let _run = crate::scale_probe::RunGuard::acquire("lsm_mixed_read_cost");
    use crate::lsm_tree::sst::{
        block_misses, block_reads, packed_promotions, packed_scan_reference,
    };
    let rows: usize = std::env::var("LSM_POINT_ROWS")
        .map(|s| s.parse().unwrap())
        .unwrap_or(600_000);
    let reads: usize = std::env::var("POINT_READS")
        .map(|s| s.parse().unwrap())
        .unwrap_or(rows.min(200_000));
    assert!(rows > 0 && reads > 0);
    let dir = ScaleScratch::new("lsm_mixed_read");
    let db = crate::lsm_tree::MVCC::open(&dir.0);
    db.set_vacuum_auto_slice(0);
    eprintln!("LSM_MIXED_BUILD rows={rows} shape=bulk_plus_10_percent_updates");
    let mut bulk = db.begin_bulk();
    for i in 0..rows {
        bulk.put(&scale_key(i), crate::common::pack_plain(&scale_val(i)));
    }
    bulk.finish();
    for base in (0..rows / 10).step_by(1000) {
        let tx = db.begin_transaction();
        for i in base..(base + 1000).min(rows / 10) {
            assert!(tx.set_raw(&scale_key(i), scale_val(i + rows)));
        }
        tx.try_commit().unwrap();
    }
    db.try_flush().unwrap();
    drop(db);
    let expected = |i| scale_val(if i < rows / 10 { i + rows } else { i });
    let mut scan_costs = Vec::new();
    // 每臂重开同一个不可变物理种子，只切换扫描路径的对照开关。
    for (round, reference) in [true, false, false, true].into_iter().enumerate() {
        let db = crate::lsm_tree::MVCC::open(&dir.0);
        db.set_vacuum_auto_slice(0);
        let tx = db.begin_transaction();
        let _mode = reference.then(packed_scan_reference::Scope::start);
        eprintln!("LSM_MIXED_ARM round={round} reference={reference} rows={rows} reads={reads}");
        timed_phase("lsm_mixed", "random", || {
            let mut state = 42_u64;
            for _ in 0..reads {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                let i = (state % rows as u64) as usize;
                assert_eq!(tx.get(&scale_key(i)), Some(expected(i)));
            }
        });
        timed_phase("lsm_mixed", "ordered", || {
            for i in 0..reads.min(rows) {
                assert_eq!(tx.get(&scale_key(i)), Some(expected(i)));
            }
        });
        let before = (packed_promotions(), block_reads(), block_misses());
        let allocations_before = crate::mem_probe::allocations();
        timed_phase("lsm_mixed", "scan", || {
            let mut seen = 0;
            tx.scan_foreach(None, None, |r| {
                assert_eq!(r.key, scale_key(seen));
                assert_eq!(r.value, Some(expected(seen)));
                seen += 1;
                true
            });
            assert_eq!(seen, rows);
        });
        scan_costs.push(crate::mem_probe::allocations() - allocations_before);
        let promoted = packed_promotions() - before.0;
        if !reference {
            assert_eq!(promoted, 0);
        }
        eprintln!(
            "LSM_MIXED_SCAN round={round} promotions={promoted} block_reads={} block_misses={}",
            block_reads() - before.1,
            block_misses() - before.2
        );
        timed_phase("lsm_mixed", "pages", || {
            let mut after = None;
            let mut seen = 0;
            while seen < reads.min(rows) {
                let (page, next) =
                    tx.scan_page(after.as_deref(), (reads.min(rows) - seen).min(1000));
                assert!(!page.is_empty());
                for r in page {
                    assert_eq!(r.key, scale_key(seen));
                    assert_eq!(r.value, Some(expected(seen)));
                    seen += 1;
                }
                after = next;
                if after.is_none() {
                    break;
                }
            }
            assert_eq!(seen, reads.min(rows));
        });
        tx.commit();
        drop(db);
    }
    // SST 测试已经对热缓存紧凑块复用设置确定性的分配守卫。
    // 冷缓存全扫描在两个对照臂里使用相同的解码表示；
    // 这里只报告该路径的成本，不据此断言全库提速。
    let db = crate::lsm_tree::MVCC::open(&dir.0);
    db.set_vacuum_auto_slice(0);
    timed_phase("lsm_mixed", "update", || {
        let tx = db.begin_transaction();
        for i in 0..rows.min(1000) {
            assert!(tx.set_raw(&scale_key(i), scale_val(i + 2 * rows)));
        }
        tx.try_commit().unwrap();
    });
    drop(db);
    let db = crate::lsm_tree::MVCC::open(&dir.0);
    let mut seen = 0;
    db.export_latest_visible_for_each(false, |r| {
        assert_eq!(r.key, scale_key(seen));
        assert_eq!(
            r.value,
            Some(if seen < rows.min(1000) {
                scale_val(seen + 2 * rows)
            } else {
                expected(seen)
            })
        );
        seen += 1;
        true
    });
    assert_eq!(seen, rows);
    eprintln!("LSM_MIXED_VERIFIED rows={seen} scan_allocations={scan_costs:?}");
}

#[test]
#[ignore = "LSM packed point blocks old/new/new/old; release single thread"]
fn lsm_packed_point_cost() {
    let _run = crate::scale_probe::RunGuard::acquire("lsm_packed_point_cost");
    use crate::lsm_tree::sst::{block_misses, block_reads, packed_point_reference, sst_probes};
    let rows: usize = std::env::var("LSM_POINT_ROWS")
        .map(|s| s.parse().unwrap())
        .unwrap_or(600_000);
    let reads: usize = std::env::var("POINT_READS")
        .map(|s| s.parse().unwrap())
        .unwrap_or(rows.min(200_000));
    assert!(rows > 0 && reads > 0);
    let dir = ScaleScratch::new("lsm_packed_point");
    let path = dir.0.clone();
    let db = crate::lsm_tree::MVCC::open(&path);
    db.set_vacuum_auto_slice(0);
    eprintln!("LSM_POINT_BUILD rows={rows}");
    let mut bulk = db.begin_bulk();
    for i in 0..rows {
        bulk.put(&scale_key(i), crate::common::pack_plain(&scale_val(i)));
    }
    bulk.finish();
    drop(db);
    let mut costs = Vec::new();
    for (round, reference) in [true, false, false, true].into_iter().enumerate() {
        let db = crate::lsm_tree::MVCC::open(&path);
        db.set_vacuum_auto_slice(0);
        let tx = db.begin_transaction();
        let mode = reference.then(packed_point_reference::Scope::start);
        let before = (
            crate::mem_probe::allocations(),
            sst_probes(),
            block_reads(),
            block_misses(),
        );
        let start = std::time::Instant::now();
        let mut state = 42_u64;
        for _ in 0..reads {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let i = (state % rows as u64) as usize;
            assert_eq!(tx.get(&scale_key(i)), Some(scale_val(i)));
        }
        let seconds = start.elapsed().as_secs_f64();
        let allocations = crate::mem_probe::allocations() - before.0;
        eprintln!(
            "LSM_POINT round={round} reference={reference} rows={rows} reads={reads} seconds={seconds:.6} allocations={allocations} sst_probes={} block_reads={} block_misses={}",
            sst_probes() - before.1,
            block_reads() - before.2,
            block_misses() - before.3
        );
        costs.push(allocations);
        drop(mode);
        tx.commit();
        drop(db);
    }
    assert!(costs[1] < costs[0]);
    assert!(costs[2] < costs[3]);
    let db = crate::lsm_tree::MVCC::open(&path);
    let mut seen = 0;
    db.export_latest_visible_for_each(false, |r| {
        assert_eq!(r.key, scale_key(seen));
        assert_eq!(r.value, Some(scale_val(seen)));
        seen += 1;
        true
    });
    assert_eq!(seen, rows);
    eprintln!("LSM_POINT_VERIFIED rows={seen}");
}
