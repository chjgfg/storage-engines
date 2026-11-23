// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
#[ignore = "memory shutdown CRC A/B and stage timings; release single thread"]
fn memory_snapshot_stop_cost() {
    use crate::common::kv_snapshot::{read_snapshot_with, snapshot_probe};
    let rows: usize = std::env::var("SNAPSHOT_ROWS")
        .map(|s| s.parse().unwrap())
        .unwrap_or(600_000);
    for (round, legacy) in [true, false, false, true].into_iter().enumerate() {
        let dir = ScaleScratch::new("snapshot_stop_cost");
        let db = open_engine(EngineKind::Memory, Some(&dir.0)).unwrap();
        for base in (0..rows).step_by(1000) {
            let tx = db.begin();
            for i in base..(base + 1000).min(rows) {
                assert!(tx.set_raw(&scale_key(i), scale_val(i)));
            }
            tx.commit();
        }
        let scope = snapshot_probe::Scope::start(legacy);
        let start = std::time::Instant::now();
        drop(db);
        let drop_ns = start.elapsed().as_nanos();
        let st = scope.stats();
        drop(scope);
        eprintln!(
            "SNAPSHOT_STOP round={round} rows={rows} legacy_fields={legacy} drop_ms={:.3} snapshot_ms={:.3} body_ms={:.3} write_ms={:.3} crc_ms={:.3} sync_ms={:.3} publish_ms={:.3} release_ms={:.3} crc_calls={} writes={}",
            drop_ns as f64 / 1e6,
            st.total_ns as f64 / 1e6,
            st.body_ns as f64 / 1e6,
            st.write_ns as f64 / 1e6,
            st.crc_ns as f64 / 1e6,
            st.sync_ns as f64 / 1e6,
            st.publish_ns as f64 / 1e6,
            drop_ns.saturating_sub(st.total_ns) as f64 / 1e6,
            st.crc_calls,
            st.write_calls
        );
        // 独立核验种子内容，不计入停止阶段计时。
        let mut seen = 0;
        read_snapshot_with(
            &dir.0.join("snapshot.bxp"),
            crate::common::kv_snapshot::ValueBuffer::Exact,
            &mut |r| {
                assert_eq!(r.key, scale_key(seen));
                assert_eq!(r.value, Some(scale_val(seen)));
                seen += 1;
                true
            },
        )
        .unwrap();
        assert_eq!(seen, rows);
    }
}

#[test]
#[ignore = "snapshot scan old/new/new/old; release single thread"]
fn memory_snapshot_scan_cost() {
    use crate::common::kv_snapshot::{read_snapshot_with, snapshot_probe};
    let rows: usize = std::env::var("SNAPSHOT_ROWS")
        .map(|s| s.parse().unwrap())
        .unwrap_or(600_000);
    let mut allocations = Vec::new();
    for (round, reference) in [true, false, false, true].into_iter().enumerate() {
        let dir = ScaleScratch::new("snapshot_scan");
        let db = open_engine(EngineKind::Memory, Some(&dir.0)).unwrap();
        for base in (0..rows).step_by(1000) {
            let tx = db.begin();
            for i in base..(base + 1000).min(rows) {
                assert!(tx.set_raw(&scale_key(i), scale_val(i)));
            }
            tx.commit();
        }
        let mode = reference.then(crate::memory::memory_mvcc::scan_reference::Scope::start);
        let probe = snapshot_probe::Scope::start(false);
        let before = crate::mem_probe::allocations();
        let lookups = crate::memory::mem_store::point_lookups();
        let start = std::time::Instant::now();
        drop(db);
        let seconds = start.elapsed().as_secs_f64();
        let allocs = crate::mem_probe::allocations() - before;
        let lookups = crate::memory::mem_store::point_lookups() - lookups;
        allocations.push(allocs);
        eprintln!(
            "SNAPSHOT_SCAN round={round} reference={reference} rows={rows} seconds={seconds:.6} allocations={allocs} lookups={lookups} stages={:?}",
            probe.stats()
        );
        assert_eq!(lookups, if reference { rows as u64 } else { 0 });
        drop(probe);
        drop(mode);
        let mut seen = 0;
        read_snapshot_with(
            &dir.0.join("snapshot.bxp"),
            crate::common::kv_snapshot::ValueBuffer::Exact,
            &mut |record| {
                assert_eq!(record.key, scale_key(seen));
                assert_eq!(record.value, Some(scale_val(seen)));
                assert_eq!(record.expire_unix_secs, None);
                seen += 1;
                true
            },
        )
        .unwrap();
        assert_eq!(seen, rows);
        let replay_mode =
            reference.then(crate::memory::memory_mvcc::replay_reference::Scope::start);
        let locks = crate::memory::memory_mvcc::replay_reference::row_locks();
        let start = std::time::Instant::now();
        let db = open_engine(EngineKind::Memory, Some(&dir.0)).unwrap();
        let seconds = start.elapsed().as_secs_f64();
        let locks = crate::memory::memory_mvcc::replay_reference::row_locks() - locks;
        eprintln!(
            "SNAPSHOT_REPLAY round={round} reference={reference} rows={rows} seconds={seconds:.6} row_lock_pairs={locks}"
        );
        assert_eq!(locks, if reference { rows as u64 } else { 0 });
        drop(replay_mode);
        let mut seen = 0;
        db.export_latest_visible_for_each(false, &mut |record| {
            assert_eq!(record.key, scale_key(seen));
            assert_eq!(record.value, Some(scale_val(seen)));
            seen += 1;
            true
        });
        assert_eq!(seen, rows);
    }
    assert!(allocations[1] + rows as u64 / 2 < allocations[0]);
    assert!(allocations[2] + rows as u64 / 2 < allocations[3]);
}

#[test]
#[ignore = "snapshot IO and owned replay comparison; release single thread"]
fn memory_snapshot_io_owned_cost() {
    use crate::common::kv_snapshot::{io_reference, recovery_probe, snapshot_probe};
    let rows: usize = std::env::var("SNAPSHOT_ROWS")
        .map(|s| s.parse().unwrap())
        .unwrap_or(600_000);
    assert!(rows >= 1000);
    let mut recovery_allocations = Vec::new();
    for (round, reference) in [true, false, false, true].into_iter().enumerate() {
        let dir = ScaleScratch::new("snapshot_io_owned");
        let db = open_engine(EngineKind::Memory, Some(&dir.0)).unwrap();
        for base in (0..rows).step_by(1000) {
            let tx = db.begin();
            for i in base..(base + 1000).min(rows) {
                assert!(tx.set_raw(&scale_key(i), scale_val(i)));
            }
            tx.commit();
        }
        let _io_mode = reference.then(io_reference::Scope::start);
        let probe = snapshot_probe::Scope::start(false);
        let start = std::time::Instant::now();
        drop(db);
        let elapsed = start.elapsed().as_nanos();
        let stats = probe.stats();
        eprintln!(
            "SNAPSHOT_IO_STOP round={round} reference={reference} rows={rows} total_ms={:.3} release_ms={:.3} stages={stats:?}",
            elapsed as f64 / 1e6,
            elapsed.saturating_sub(stats.total_ns) as f64 / 1e6
        );
        drop(probe);
        let mode = reference.then(crate::common::txn::owned_reference::Scope::start);
        let recovery = recovery_probe::Scope::start();
        let before = crate::mem_probe::allocations();
        let start = std::time::Instant::now();
        let db = open_engine(EngineKind::Memory, Some(&dir.0)).unwrap();
        let elapsed = start.elapsed().as_secs_f64();
        let allocations = crate::mem_probe::allocations() - before;
        recovery_allocations.push(allocations);
        eprintln!(
            "SNAPSHOT_IO_RECOVER round={round} reference={reference} rows={rows} seconds={elapsed:.6} allocations={allocations} stages={:?}",
            recovery.stats()
        );
        drop(recovery);
        drop(mode);
        let mut seen = 0;
        db.export_latest_visible_for_each(false, &mut |record| {
            assert_eq!(record.key, scale_key(seen));
            assert_eq!(record.value, Some(scale_val(seen)));
            assert_eq!(record.expire_unix_secs, None);
            seen += 1;
            true
        });
        assert_eq!(seen, rows);
        eprintln!("SNAPSHOT_IO_VERIFIED round={round} rows={seen}");
    }
    assert!(recovery_allocations[1] + (rows as u64) < recovery_allocations[0]);
    assert!(recovery_allocations[2] + (rows as u64) < recovery_allocations[3]);
}

#[test]
#[ignore = "memory stop owned/borrowed/reference cost; release single thread"]
fn memory_stop_ownership_cost() {
    use crate::common::kv_snapshot::{ValueBuffer, read_snapshot_with, snapshot_probe};
    let rows: usize = std::env::var("SNAPSHOT_ROWS")
        .map(|s| s.parse().unwrap())
        .unwrap_or(600_000);
    let mut costs = Vec::new();
    for (round, reference) in [true, false, false, true].into_iter().enumerate() {
        let dir = ScaleScratch::new("stop_ownership");
        let db = crate::memory::MVCC::open_with_snapshot(&dir.0).unwrap();
        db.set_vacuum_auto_slice(0);
        for base in (0..rows).step_by(1000) {
            let tx = db.begin_transaction();
            for i in base..(base + 1000).min(rows) {
                assert!(tx.set_raw(&scale_key(i), scale_val(i)));
            }
            tx.try_commit().unwrap();
        }
        let reference_mode =
            reference.then(crate::memory::memory_mvcc::stop_reference::Scope::start);
        let scope = snapshot_probe::Scope::start(false);
        let before = crate::mem_probe::allocations();
        let start = std::time::Instant::now();
        drop(db);
        let seconds = start.elapsed().as_secs_f64();
        let allocations = crate::mem_probe::allocations() - before;
        let stats = scope.stats();
        costs.push(allocations);
        eprintln!(
            "MEMORY_STOP_OWNERSHIP round={round} reference={reference} rows={rows} seconds={seconds:.6} allocations={allocations} release_remainder_ms={:.3} stages={stats:?}",
            seconds * 1000.0 - stats.total_ns as f64 / 1e6
        );
        if !reference {
            assert_eq!(stats.consumed_rows, rows as u64);
        }
        drop(scope);
        drop(reference_mode);
        let mut seen = 0;
        read_snapshot_with(&dir.0.join("snapshot.bxp"), ValueBuffer::Exact, &mut |r| {
            assert_eq!(r.key, scale_key(seen));
            assert_eq!(r.value, Some(scale_val(seen)));
            seen += 1;
            true
        })
        .unwrap();
        assert_eq!(seen, rows);
        let reopened = crate::memory::MVCC::open_with_snapshot(&dir.0).unwrap();
        let mut seen = 0;
        reopened.export_latest_visible_for_each(false, |r| {
            assert_eq!(r.key, scale_key(seen));
            assert_eq!(r.value, Some(scale_val(seen)));
            seen += 1;
            true
        });
        assert_eq!(seen, rows);
    }
    assert!(costs[0] >= costs[1] + rows as u64);
    assert!(costs[3] >= costs[2] + rows as u64);
}

#[test]
#[ignore = "memory stop input batches old/new/new/old; release single thread"]
fn memory_stop_batch_cost() {
    use crate::common::kv_snapshot::{ValueBuffer, read_snapshot_with, snapshot_probe};
    let rows: usize = std::env::var("SNAPSHOT_ROWS")
        .map(|s| s.parse().unwrap())
        .unwrap_or(600_000);
    for (round, reference) in [true, false, false, true].into_iter().enumerate() {
        let dir = ScaleScratch::new("stop_batches");
        let db = crate::memory::MVCC::open_with_snapshot(&dir.0).unwrap();
        db.set_vacuum_auto_slice(0);
        for base in (0..rows).step_by(1000) {
            let tx = db.begin_transaction();
            for i in base..(base + 1000).min(rows) {
                assert!(tx.set_raw(&scale_key(i), scale_val(i)));
            }
            tx.try_commit().unwrap();
        }
        let mode = reference.then(crate::memory::memory_mvcc::stop_rows_reference::Scope::start);
        let scope = snapshot_probe::Scope::start(false);
        let before = crate::mem_probe::allocations();
        let start = std::time::Instant::now();
        drop(db);
        let seconds = start.elapsed().as_secs_f64();
        let allocations = crate::mem_probe::allocations() - before;
        let stats = scope.stats();
        assert_eq!(stats.consumed_rows, rows as u64);
        if reference {
            assert_eq!(stats.input_batches, 0);
        } else {
            assert_eq!(stats.input_batches, rows.div_ceil(8192) as u64);
            assert_eq!(
                stats.input_buffer_bytes,
                8192 * std::mem::size_of::<(
                    std::borrow::Cow<'_, [u8]>,
                    Option<std::borrow::Cow<'_, [u8]>>
                )>()
            );
            assert!(stats.pull_ns + stats.fold_emit_ns <= stats.body_ns);
        }
        eprintln!(
            "MEMORY_STOP_BATCH round={round} reference={reference} rows={rows} seconds={seconds:.6} allocations={allocations} stages={stats:?}"
        );
        drop(scope);
        drop(mode);
        let mut seen = 0;
        read_snapshot_with(&dir.0.join("snapshot.bxp"), ValueBuffer::Exact, &mut |r| {
            assert_eq!(r.key, scale_key(seen));
            assert_eq!(r.value, Some(scale_val(seen)));
            seen += 1;
            true
        })
        .unwrap();
        assert_eq!(seen, rows);
        let reopened = crate::memory::MVCC::open_with_snapshot(&dir.0).unwrap();
        let mut seen = 0;
        reopened.export_latest_visible_for_each(false, |r| {
            assert_eq!(r.key, scale_key(seen));
            assert_eq!(r.value, Some(scale_val(seen)));
            seen += 1;
            true
        });
        assert_eq!(seen, rows);
    }
}

#[test]
#[ignore = "memory stop record batches, first/bulk shapes ABBA; release single thread"]
fn memory_stop_emit_cost() {
    memory_stop_fold_or_emit_cost(false, false);
}

#[test]
#[ignore = "memory completed group folding, first/bulk shapes ABBA; release single thread"]
fn memory_stop_fold_cost() {
    memory_stop_fold_or_emit_cost(true, false);
}

#[test]
#[ignore = "memory bounded batch release, first/updated-vacuum-migrated-bulk ABBA; release single thread"]
fn memory_stop_release_cost() {
    memory_stop_fold_or_emit_cost(false, true);
}
