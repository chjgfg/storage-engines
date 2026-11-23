// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
#[ignore = "LSM vacuum old/new/new/old; release single thread"]
fn lsm_vacuum_delete_cost() {
    let rows: usize = std::env::var("LSM_DEL_ROWS")
        .map(|v| v.parse().unwrap())
        .unwrap_or(600_000);
    assert!(rows >= 10);
    let mut costs = Vec::new();
    for (round, reference) in [true, false, false, true].into_iter().enumerate() {
        let dir = ScaleScratch::new("lsm_vacuum_delete");
        let db = crate::lsm_tree::MVCC::open(&dir.0);
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
        let mode = reference.then(crate::lsm_tree::lsm_tree::vacuum_delete_probe::Scope::start);
        let probe = crate::common::maintenance_probe::Scope::start();
        let internal = crate::lsm_tree::lsm_tree::maintenance_probe::Scope::start();
        let reads_before = crate::lsm_tree::lsm_tree::vacuum_delete_probe::reads();
        let before = crate::mem_probe::allocations();
        let start = std::time::Instant::now();
        let stats = db.vacuum().unwrap();
        let seconds = start.elapsed().as_secs_f64();
        let allocations = crate::mem_probe::allocations() - before;
        costs.push(allocations);
        let reads = crate::lsm_tree::lsm_tree::vacuum_delete_probe::reads() - reads_before;
        assert_eq!(reads, if reference { 2 * (rows / 10) as u64 } else { 0 });
        assert_eq!(
            probe.stats().lsm_known_deletes,
            if reference { 0 } else { (rows / 10) as u64 }
        );
        eprintln!(
            "LSM_VACUUM_DELETE round={round} reference={reference} rows={rows} seconds={seconds:.6} allocations={allocations} point_reads={reads} stages={:?}",
            probe.stats()
        );
        eprintln!(
            "LSM_VACUUM_DELETE_INTERNAL round={round} rows={rows} stats={:?}",
            internal.stats()
        );
        assert_eq!(stats.versions_removed, rows / 10);
        assert_eq!(stats.rows_scanned, (rows + rows / 10) as u64);
        if !reference {
            assert!(probe.stats().lsm_vacuum_owned_rows > 0);
        }
        drop(internal);
        drop(probe);
        drop(mode);
        drop(db);
        let db = crate::lsm_tree::MVCC::open(&dir.0);
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
    assert!(costs[0] >= costs[1] + 2 * (rows / 10) as u64);
    assert!(costs[3] >= costs[2] + 2 * (rows / 10) as u64);
}

#[test]
#[ignore = "LSM disjoint L0 movement ABBA, fill/read/update/vacuum/reopen; release single thread"]
fn lsm_disjoint_move_cost() {
    use crate::lsm_tree::lsm_tree::{disjoint_move_reference, maintenance_probe};
    let _run = crate::scale_probe::RunGuard::acquire("lsm_disjoint_move_cost");
    let rows: usize = std::env::var("LSM_MOVE_ROWS")
        .map(|v| v.parse().unwrap())
        .unwrap_or(600_000);
    assert!(rows >= 1000 && rows.checked_mul(16).is_some());
    crate::scale_probe::fingerprint();
    for (round, reference) in [true, false, false, true].into_iter().enumerate() {
        let scratch = ScaleScratch::new("lsm_disjoint_move");
        let mode = reference.then(disjoint_move_reference::Scope::start);
        let mut db = open_engine(EngineKind::LsmTree, Some(&scratch.0)).unwrap();
        for phase in [
            "insert",
            "reopen",
            "read_sample",
            "update",
            "vacuum",
            "final_reopen",
        ] {
            let maint = maintenance_probe::Scope::start();
            let clock = std::time::Instant::now();
            let cpu = crate::scale_probe::Phase::begin("lsm_tree", phase);
            let before = crate::mem_probe::allocations();
            match phase {
                "insert" | "update" => {
                    let count = if phase == "insert" { rows } else { rows / 10 };
                    let batch = if phase == "insert" { 1000 } else { 500 };
                    for base in (0..count).step_by(batch) {
                        let items: Vec<_> = (base..(base + batch).min(count))
                            .map(|i| {
                                (
                                    scale_key(i),
                                    scale_val(if phase == "insert" { i } else { i + rows }),
                                )
                            })
                            .collect();
                        let tx = db.begin();
                        tx.batch_set(&items).unwrap();
                        tx.commit();
                        if (base + batch) % 1_000_000 == 0 {
                            eprintln!(
                                "LSM_DISJOINT_PROGRESS rows={rows} round={round} reference={reference} phase={phase} completed={}",
                                base + batch
                            );
                        }
                    }
                }
                "reopen" | "final_reopen" => {
                    db.flush().unwrap();
                    db.checkpoint().unwrap();
                    drop(db);
                    db = open_engine(EngineKind::LsmTree, Some(&scratch.0)).unwrap();
                }
                "read_sample" => {
                    let tx = db.begin();
                    for n in 0..rows.min(20_000) {
                        let i = ((n as u64 * 104729) % rows as u64) as usize;
                        assert_eq!(tx.get(&scale_key(i)), Some(scale_val(i)));
                    }
                    tx.commit();
                }
                "vacuum" => {
                    assert!(!db.vacuum().unwrap().skipped_active_txn);
                }
                _ => unreachable!(),
            }
            let seconds = clock.elapsed().as_secs_f64();
            let allocations = crate::mem_probe::allocations() - before;
            cpu.finish("lsm_tree", phase);
            let stats = maint.stats();
            eprintln!(
                "LSM_DISJOINT rows={rows} round={round} reference={reference} phase={phase} seconds={seconds:.6} allocations={allocations} rewrite_input_files={} rewrite_input_bytes={} stages={stats:?}",
                stats.input_files - stats.trivial_moves,
                stats.input_bytes - stats.moved_bytes
            );
        }
        // 独立核验种子和最终持久化状态，逐个比较全部键和值。
        let tx = db.begin();
        let mut seen = 0;
        tx.export_latest_visible_for_each(false, &mut |record| {
            assert_eq!(record.key, scale_key(seen));
            assert_eq!(
                record.value,
                Some(scale_val(if seen < rows / 10 { seen + rows } else { seen }))
            );
            seen += 1;
            true
        });
        assert_eq!(seen, rows);
        drop(tx);
        drop(db);
        drop(mode);
        eprintln!(
            "LSM_DISJOINT_VERIFIED rows={rows} round={round} reference={reference} unique_keys={seen}"
        );
    }
}

#[test]
#[ignore = "LSM vacuum old/new/new/old; release single thread"]
fn lsm_vacuum_buffer_cost() {
    let rows: usize = std::env::var("LSM_VAC_ROWS")
        .map(|v| v.parse().unwrap())
        .unwrap_or(600_000);
    assert!(rows >= 10);
    let mut costs = Vec::new();
    for (round, reference) in [true, false, false, true].into_iter().enumerate() {
        let dir = ScaleScratch::new("lsm_vacuum_buffer");
        let db = crate::lsm_tree::MVCC::open(&dir.0);
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
            reference.then(crate::lsm_tree::lsm_tree_mvcc::vacuum_cost_reference::Scope::start);
        let probe = crate::common::maintenance_probe::Scope::start();
        let before = crate::mem_probe::allocations();
        let start = std::time::Instant::now();
        let stats = db.vacuum().unwrap();
        let seconds = start.elapsed().as_secs_f64();
        let allocations = crate::mem_probe::allocations() - before;
        costs.push(allocations);
        eprintln!(
            "LSM_VACUUM_BUFFER round={round} reference={reference} rows={rows} seconds={seconds:.6} allocations={allocations} stages={:?}",
            probe.stats()
        );
        assert_eq!(stats.versions_removed, rows / 10);
        assert_eq!(stats.rows_scanned, (rows + rows / 10) as u64);
        if !reference {
            assert!(probe.stats().lsm_vacuum_owned_rows > 0);
        }
        drop(probe);
        drop(mode);
        drop(db);
        let db = crate::lsm_tree::MVCC::open(&dir.0);
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
#[ignore = "LSM overlapping compaction allocation old/new/new/old; release single thread"]
fn lsm_owned_compaction_cost() {
    use crate::lsm_tree::{LSMTree, LSMTreeOptions};
    use crate::lsm_tree::{
        lsm_tree::maintenance_probe,
        sst::{StoredEntry, compaction_reference, entry_value_clones},
    };
    let rows: usize = std::env::var("COMPACTION_ROWS")
        .map(|s| s.parse().unwrap())
        .unwrap_or(600_000);
    let mut costs = Vec::new();
    for (round, reference) in [true, false, false, true].into_iter().enumerate() {
        let dir = ScaleScratch::new("owned_compaction");
        let opts = LSMTreeOptions {
            sst_block_entries: 128,
            target_sst_bytes: 4 * 1024 * 1024,
            ..LSMTreeOptions::default()
        };
        let mut db = LSMTree::open_with_config(&dir.0, opts.clone()).unwrap();
        for generation in 1..=2_u64 {
            db.inject_sst_stream_for_test(
                0,
                (0..rows).map(|i| {
                    (
                        scale_key(i),
                        StoredEntry::put(generation, Some(scale_val(i + generation as usize))),
                    )
                }),
            )
            .unwrap();
        }
        let mode = reference.then(compaction_reference::Scope::start);
        let scope = maintenance_probe::Scope::start();
        let clones = entry_value_clones();
        let before = crate::mem_probe::allocations();
        let start = std::time::Instant::now();
        assert_eq!(db.compact_l0_until(0, 8).unwrap(), 1);
        let seconds = start.elapsed().as_secs_f64();
        let allocations = crate::mem_probe::allocations() - before;
        let clones = entry_value_clones() - clones;
        let stats = scope.stats();
        costs.push(allocations);
        if !reference {
            assert_eq!(clones, 0);
            assert_eq!(stats.owned_input_rows, 2 * rows as u64);
        } else {
            assert_eq!(clones, 4 * rows as u64);
        }
        eprintln!(
            "LSM_OWNED_COMPACTION round={round} reference={reference} rows={rows} seconds={seconds:.6} allocations={allocations} value_clones={clones} stages={stats:?}"
        );
        drop(scope);
        drop(mode);
        drop(db);
        let db = LSMTree::open_with_config(&dir.0, opts).unwrap();
        let mut seen = 0;
        db.export_records_with_seq_for_each(false, &mut |key, value, seq| {
            assert_eq!(key, scale_key(seen));
            assert_eq!(value, Some(scale_val(seen + 2)));
            assert_eq!(seq, 2);
            seen += 1;
            true
        });
        assert_eq!(seen, rows);
    }
    assert!(costs[0] >= costs[1] + 4 * rows as u64);
    assert!(costs[3] >= costs[2] + 4 * rows as u64);
}
