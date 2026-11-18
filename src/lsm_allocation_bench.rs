// Included in engine::tests. Same ordinary transaction seed as the full scale
// harness; reference switches only M147, leaving the M145 compaction unchanged.
#[test]
#[ignore = "five-scale LSM allocation ABBA; release, one test thread"]
fn lsm_allocation_cost() {
    let _run = crate::scale_probe::RunGuard::acquire("lsm_allocation_cost");
    crate::scale_probe::fingerprint();
    let sizes = std::env::var("LSM_ALLOCATION_ROWS")
        .unwrap_or_else(|_| "600000,1000000,5000000,10000000,20000000".into());
    for rows in sizes.split(',').map(|v| v.parse::<usize>().unwrap()) {
        assert!(rows >= 1000);
        let mut allocations = Vec::new();
        let mut work = Vec::new();
        for (round, reference) in [true, false, false, true].into_iter().enumerate() {
            let scratch = ScaleScratch::new("lsm_allocation");
            let mode = reference.then(crate::lsm_tree::lsm_tree::allocation_reference::Scope::start);
            let maint = crate::lsm_tree::lsm_tree::maintenance_probe::Scope::start();
            let mut db = open_engine(EngineKind::LsmTree, Some(&scratch.0)).unwrap();
            eprintln!("LSM_ALLOC_BEGIN rows={rows} round={round} reference={reference}");
            let mut measured = Vec::new();
            let mut work_stages = Vec::new();
            for phase in ["insert", "checkpoint", "first_reopen", "random_read", "scan_page", "search_first",
                "update", "health", "vacuum", "flush", "bulk", "bulk_reopen"] {
                let start = std::time::Instant::now();
                let before = crate::mem_probe::allocations();
                let mut stages = [0_u128; 3];
                match phase {
                    "insert" | "update" => {
                        let count = if phase == "insert" { rows } else { rows / 10 };
                        let batch = if phase == "insert" { 1000 } else { 500 };
                        for base in (0..count).step_by(batch) {
                            let t = std::time::Instant::now();
                            let items: Vec<_> = (base..(base + batch).min(count)).map(|i|
                                (scale_key(i), scale_val(if phase == "insert" { i } else { i + rows }))).collect();
                            stages[0] += t.elapsed().as_nanos();
                            let tx = db.begin();
                            let t = std::time::Instant::now(); tx.batch_set(&items).unwrap();
                            stages[1] += t.elapsed().as_nanos();
                            let t = std::time::Instant::now(); tx.commit();
                            stages[2] += t.elapsed().as_nanos();
                            if (base + batch) % 1_000_000 == 0 {
                                eprintln!("LSM_ALLOC_PROGRESS rows={rows} round={round} phase={phase} done={}", base + batch);
                            }
                        }
                    }
                    "checkpoint" => { db.flush().unwrap(); db.checkpoint().unwrap(); }
                    "first_reopen" | "bulk_reopen" => {
                        drop(db); db = open_engine(EngineKind::LsmTree, Some(&scratch.0)).unwrap();
                    }
                    "random_read" => {
                        let tx = db.begin();
                        // Deterministic spread over the whole store. 200k sample
                        // is not a full N-read throughput measurement.
                        for i in shuffled(rows).into_iter().take(rows.min(200_000)) {
                            assert_eq!(tx.get(&scale_key(i)), Some(scale_val(i)));
                        }
                        tx.commit();
                    }
                    "scan_page" => {
                        let tx = db.begin(); let mut cursor = None; let mut seen = 0;
                        loop {
                            let (page, next) = tx.scan_page(cursor.as_deref(), 500);
                            for row in page {
                                assert_eq!(row.key, scale_key(seen));
                                assert_eq!(row.value, Some(scale_val(seen))); seen += 1;
                            }
                            assert!(seen <= rows);
                            let Some(next) = next else { break };
                            assert!(cursor.as_ref().is_none_or(|previous| previous < &next));
                            cursor = Some(next);
                        }
                        assert_eq!(seen, rows); tx.commit();
                    }
                    "search_first" => {
                        let tx = db.begin();
                        let result = tx.search_keys(&crate::common::SearchQuery::contains(b"", 0, 100));
                        assert_eq!(result.total, Some(rows));
                        assert_eq!(result.items.len(), 100); tx.commit();
                    }
                    "health" => { assert_eq!(db.health().live_keys, rows as u64); }
                    "vacuum" => { assert!(!db.vacuum().unwrap().skipped_active_txn); }
                    "flush" => { db.flush().unwrap(); }
                    "bulk" => {
                        let mut bulk = db.begin_bulk();
                        for base in (0..rows / 10).step_by(1000) {
                            bulk.put_batch_owned((base..(base + 1000).min(rows / 10))
                                .map(|i| (scale_key(rows + i), scale_val(i))).collect());
                        }
                        bulk.finish();
                    }
                    _ => unreachable!(),
                }
                let count = crate::mem_probe::allocations() - before;
                measured.push(count);
                let stats = maint.stats();
                work_stages.push([stats.input_files, stats.input_bytes, stats.trivial_moves,
                    stats.moved_bytes, stats.output_files, stats.output_bytes, stats.owned_input_rows]);
                eprintln!("LSM_ALLOC_PHASE rows={rows} round={round} reference={reference} phase={phase} seconds={:.6} allocations={count} prepare_ns={} write_ns={} commit_ns={} stats={:?}",
                    start.elapsed().as_secs_f64(), stages[0], stages[1], stages[2], maint.stats());
            }
            let mut seen = 0;
            db.export_latest_visible_for_each(false, &mut |record| {
                assert_eq!(record.key, scale_key(seen));
                let value_i = if seen >= rows { seen - rows } else if seen < rows / 10 { seen + rows } else { seen };
                assert_eq!(record.value, Some(scale_val(value_i)));
                assert_eq!(record.expire_unix_secs, None); seen += 1; true
            });
            assert_eq!(seen, rows + rows / 10);
            allocations.push(measured);
            work.push(work_stages);
            drop(db); drop(maint); drop(mode);
            eprintln!("LSM_ALLOC_VERIFIED rows={rows} round={round} reference={reference} unique_keys={seen}");
        }
        // Independent allocation guard, not a wall-clock threshold. The old
        // branch must pay two range bounds and one undo-list key per ordinary insert.
        for new in [1, 2] {
            for old in [0, 3] {
                assert!(allocations[new][0] + 3 * rows as u64 <= allocations[old][0]);
                assert!(allocations[new][3] < allocations[old][3]);
                assert!(allocations[new][8] < allocations[old][8]);
                assert_eq!(work[new], work[old], "no shifted compaction work at any stage");
            }
        }
        eprintln!("LSM_ALLOC_SCALE_PASS rows={rows}");
    }
}
