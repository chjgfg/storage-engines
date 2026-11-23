// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
#[ignore = "ordinary-transaction B+ vacuum node reuse ABBA at 5M/10M/20M; release single thread"]
fn bplus_rebalance_read_cost() {
    use crate::bplus_tree::bplus_tree::{rebalance_counts, rebalance_read_reference, reorg_stats};
    use crate::common::maintenance_probe::{Scope, Stage};
    let _run = crate::scale_probe::RunGuard::acquire("bplus_rebalance_read_cost");
    crate::scale_probe::fingerprint();
    let sizes: Vec<usize> = std::env::var("BPLUS_REBALANCE_ROWS")
        .unwrap_or_else(|_| "5000000,10000000,20000000".into())
        .split(',')
        .map(|s| s.trim().parse().unwrap())
        .collect();
    assert!(!sizes.is_empty() && sizes[0] >= 100 && sizes.windows(2).all(|w| w[0] < w[1]));
    fn copy_seed(from: &Path, to: &Path) {
        for entry in std::fs::read_dir(from).unwrap() {
            let entry = entry.unwrap();
            assert!(entry.file_type().unwrap().is_file());
            std::fs::copy(entry.path(), to.join(entry.file_name())).unwrap();
        }
    }
    let base = ScaleScratch::new("bplus_rebalance_base");
    let mut built = 0;
    for rows in sizes {
        // 用普通事务和自动维护逐档扩展原始种子；
        // 更新在独立副本中执行，避免前一档回收
        // 改变后续对照臂的物理种子与历史版本。
        let db = open_engine(EngineKind::BPlusTree, Some(&base.0.join("bplus.db"))).unwrap();
        for start in (built..rows).step_by(1000) {
            let end = (start + 1000).min(rows);
            let items: Vec<_> = (start..end).map(|i| (scale_key(i), scale_val(i))).collect();
            let tx = db.begin();
            tx.batch_set(&items).unwrap();
            tx.commit();
            if end % 1_000_000 == 0 || end == rows {
                eprintln!("BPLUS_REBALANCE_BUILD rows={rows} completed={end}");
            }
        }
        db.flush().unwrap();
        db.checkpoint().unwrap();
        drop(db);
        built = rows;
        let seed = ScaleScratch::new("bplus_rebalance_updated");
        copy_seed(&base.0, &seed.0);
        let db = crate::bplus_tree::MVCC::open(seed.0.join("bplus.db"), 64, 1024);
        db.set_vacuum_auto_slice(0); // Retain exactly 10% history for every arm.
        for start in (0..rows / 10).step_by(1000) {
            let tx = db.begin_transaction();
            for i in start..(start + 1000).min(rows / 10) {
                assert!(tx.set_raw(&scale_key(i), scale_val(i + rows)));
            }
            tx.try_commit().unwrap();
        }
        db.try_checkpoint().unwrap();
        drop(db);
        let mut outcomes = Vec::new();
        for (round, reference) in [true, false, false, true].into_iter().enumerate() {
            let dir = ScaleScratch::new("bplus_rebalance_arm");
            copy_seed(&seed.0, &dir.0);
            let path = dir.0.join("bplus.db");
            let db = crate::bplus_tree::MVCC::open(&path, 64, 1024);
            db.set_vacuum_auto_slice(0);
            let mode = reference.then(rebalance_read_reference::Scope::start);
            let scope = Scope::start();
            let counts = rebalance_counts();
            let reorg = reorg_stats();
            let reads = crate::bplus_tree::page::page_reads();
            eprintln!("BPLUS_REBALANCE_ARM rows={rows} round={round} reference={reference}");
            let phase = crate::scale_probe::Phase::begin("bplus_tree", "vacuum_abba");
            let result = db.vacuum().unwrap();
            phase.finish("bplus_tree", "vacuum_abba");
            let stats = scope.stats();
            let after = rebalance_counts();
            let now = reorg_stats();
            let reused = after.0 - counts.0;
            let reloads = after.1 - counts.1;
            let reorg = (now.0 - reorg.0, now.1 - reorg.1, now.2 - reorg.2);
            let reads = crate::bplus_tree::page::page_reads() - reads;
            assert_eq!(result.versions_removed, rows / 10);
            assert_eq!(result.rows_scanned, (rows + rows / 10) as u64);
            assert_eq!(reused > 0, !reference);
            assert!(reorg.1 + reorg.2 > 0);
            eprintln!(
                "BPLUS_REBALANCE_COST rows={rows} round={round} reused={reused} reloads={reloads} page_reads={reads} reorg={reorg:?} stages={stats:?}"
            );
            outcomes.push((
                reads,
                stats.allocations[Stage::VacuumDelete as usize],
                reorg,
            ));
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
            eprintln!("BPLUS_REBALANCE_VERIFIED rows={rows} round={round}");
        }
        for new in [1, 2] {
            for old in [0, 3] {
                assert!(outcomes[new].0 < outcomes[old].0);
                assert!(outcomes[new].1 < outcomes[old].1);
                assert_eq!(outcomes[new].2, outcomes[old].2);
            }
        }
    }
}

#[test]
#[ignore = "ordinary-transaction B+ internal-node ABBA at 5M/10M/20M; release single thread"]
fn bplus_internal_projection_cost() {
    use crate::bplus_tree::bplus_tree::point_internal_reference;
    let _run = crate::scale_probe::RunGuard::acquire("bplus_internal_projection_cost");
    crate::scale_probe::fingerprint();
    let sizes: Vec<usize> = std::env::var("BPLUS_INTERNAL_ROWS")
        .unwrap_or_else(|_| "5000000,10000000,20000000".into())
        .split(',')
        .map(|s| s.trim().parse().unwrap())
        .collect();
    assert!(!sizes.is_empty() && sizes[0] > 0 && sizes.windows(2).all(|w| w[0] < w[1]));
    let reads: usize = std::env::var("POINT_READS")
        .map(|s| s.parse().unwrap())
        .unwrap_or(200_000);
    assert!(reads > 0);
    let scratch = ScaleScratch::new("bplus_internal_projection");
    let path = scratch.0.join("bplus.db");
    let mut built = 0;
    for rows in sizes {
        // 各臂使用同一种子、默认缓存、自动维护和每批 1000 行的事务。
        // 使用与 run_scale_baseline 一致的普通事务，不走批量灌入。
        // 各档逐步扩展同一数据库；每个 ABBA 对照臂重新打开
        // 同一个不可变物理库，使初始缓存预热状态一致。
        let db = open_engine(EngineKind::BPlusTree, Some(&path)).unwrap();
        let build_phase = crate::scale_probe::Phase::begin("bplus_tree", "ordinary_build");
        for base in (built..rows).step_by(1000) {
            let end = (base + 1000).min(rows);
            let items: Vec<_> = (base..end).map(|i| (scale_key(i), scale_val(i))).collect();
            let tx = db.begin();
            tx.batch_set(&items).unwrap();
            tx.commit();
            if end % 1_000_000 == 0 || end == rows {
                eprintln!("BPLUS_INTERNAL_BUILD rows={rows} completed={end}");
            }
        }
        db.flush().unwrap();
        db.checkpoint().unwrap();
        drop(db);
        build_phase.finish("bplus_tree", "ordinary_build");
        built = rows;
        let order = shuffled(rows);
        let reads = reads.min(rows);
        for (round, reference) in [true, false, false, true].into_iter().enumerate() {
            let db = open_engine(EngineKind::BPlusTree, Some(&path)).unwrap();
            let mode = reference.then(point_internal_reference::Scope::start);
            eprintln!(
                "BPLUS_INTERNAL_ARM rows={rows} reads={reads} round={round} reference={reference}"
            );
            let tx = db.begin();
            let phase = crate::scale_probe::Phase::begin("bplus_tree", "random_read");
            for &i in &order[..reads] {
                assert_eq!(tx.get(&scale_key(i)), Some(scale_val(i)));
            }
            phase.finish("bplus_tree", "random_read");
            let phase = crate::scale_probe::Phase::begin("bplus_tree", "ordered_read");
            for i in 0..reads {
                assert_eq!(tx.get(&scale_key(i)), Some(scale_val(i)));
            }
            phase.finish("bplus_tree", "ordered_read");
            let phase = crate::scale_probe::Phase::begin("bplus_tree", "scan_page");
            let mut cursor = None;
            let mut seen = 0;
            loop {
                let (page, next) = tx.scan_page(cursor.as_deref(), 500);
                if page.is_empty() {
                    break;
                }
                for row in &page {
                    assert_eq!(row.key, scale_key(seen));
                    assert_eq!(row.value, Some(scale_val(seen)));
                    seen += 1;
                }
                cursor = next;
                if cursor.is_none() {
                    break;
                }
            }
            assert_eq!(seen, rows);
            phase.finish("bplus_tree", "scan_page");
            tx.commit();
            drop(tx);
            drop(mode);
            drop(db);
            eprintln!("BPLUS_INTERNAL_VERIFIED rows={rows} round={round}");
        }
    }
}

#[test]
#[ignore = "B+ point projection old/new/new/old; release single thread"]
fn bplus_point_projection_cost() {
    let _run = crate::scale_probe::RunGuard::acquire("bplus_point_projection_cost");
    use crate::bplus_tree::{
        bplus_tree::{node_parses, point_projection_reference},
        page,
    };
    let rows: usize = std::env::var("BPLUS_POINT_ROWS")
        .map(|s| s.parse().unwrap())
        .unwrap_or(600_000);
    let reads: usize = std::env::var("POINT_READS")
        .map(|s| s.parse().unwrap())
        .unwrap_or(rows.min(200_000));
    assert!(rows > 0 && reads > 0);
    let dir = ScaleScratch::new("bplus_point_projection");
    let path = dir.0.join("bplus.db");
    let db = crate::bplus_tree::MVCC::open(&path, 64, 1024);
    db.set_vacuum_auto_slice(0);
    eprintln!("BPLUS_POINT_BUILD rows={rows}");
    let mut bulk = db.begin_bulk();
    for i in 0..rows {
        bulk.put(&scale_key(i), crate::common::pack_plain(&scale_val(i)));
    }
    bulk.try_finish().unwrap();
    drop(db);
    let mut costs = Vec::new();
    for (round, reference) in [true, false, false, true].into_iter().enumerate() {
        let db = crate::bplus_tree::MVCC::open(&path, 64, 1024);
        db.set_vacuum_auto_slice(0);
        let tx = db.begin_transaction();
        let mode = reference.then(point_projection_reference::Scope::start);
        let before = (
            crate::mem_probe::allocations(),
            node_parses(),
            page::cache_hits(),
            page::cache_misses(),
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
            "BPLUS_POINT round={round} reference={reference} rows={rows} reads={reads} seconds={seconds:.6} allocations={allocations} parses={} hits={} misses={}",
            node_parses() - before.1,
            page::cache_hits() - before.2,
            page::cache_misses() - before.3
        );
        costs.push(allocations);
        drop(mode);
        tx.commit();
        drop(db);
    }
    assert!(costs[1] < costs[0]);
    assert!(costs[2] < costs[3]);
    let db = crate::bplus_tree::MVCC::open(&path, 64, 1024);
    let mut seen = 0;
    db.export_latest_visible_for_each(false, |r| {
        assert_eq!(r.key, scale_key(seen));
        assert_eq!(r.value, Some(scale_val(seen)));
        seen += 1;
        true
    });
    assert_eq!(seen, rows);
    eprintln!("BPLUS_POINT_VERIFIED rows={seen}");
}
