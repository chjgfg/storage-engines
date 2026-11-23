// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
#[ignore = "memory page apply old/new/new/old; release single thread"]
fn memory_replay_page_cost() {
    use crate::common::kv_snapshot::{KvRecord, atomic_write_bin_v4, recovery_probe};
    use crate::memory::{mem_store::key_lookup_probe, memory_mvcc::page_apply_reference};
    let rows: usize = std::env::var("SNAPSHOT_ROWS")
        .map(|s| s.parse().unwrap())
        .unwrap_or(600_000);
    assert!(rows >= 1000);
    for ttl in [false, true] {
        let dir = ScaleScratch::new("replay_page_cost");
        let path = dir.0.join("snapshot.bxp");
        let expiry = ttl.then_some(u64::MAX);
        atomic_write_bin_v4(&path, |writer| {
            for i in 0..rows {
                let mut record = KvRecord::new(scale_key(i), Some(scale_val(i)));
                record.expire_unix_secs = expiry;
                writer.write(&record)?;
            }
            Ok(())
        })
        .unwrap();
        let mut costs = Vec::new();
        for (round, reference) in [true, false, false, true].into_iter().enumerate() {
            let mode = reference.then(page_apply_reference::Scope::start);
            let lookups = key_lookup_probe::Scope::start();
            let recovery = recovery_probe::Scope::start();
            let before = crate::mem_probe::allocations();
            let start = std::time::Instant::now();
            let db = crate::memory::MVCC::open_with_snapshot(&dir.0).unwrap();
            let seconds = start.elapsed().as_secs_f64();
            let allocations = crate::mem_probe::allocations() - before;
            let stats = recovery.stats();
            assert_eq!(stats.replay_pages, rows.div_ceil(1000) as u64);
            assert_eq!(stats.replay_input_rows, rows as u64);
            assert_eq!(
                stats.replay_registry_lookups,
                if reference {
                    rows as u64
                } else {
                    stats.replay_pages
                }
            );
            assert_eq!(
                stats.replay_tail_reads,
                if reference { rows as u64 } else { 0 }
            );
            assert_eq!(lookups.stats().searches, 0);
            assert_eq!(lookups.stats().end_misses, rows as u64);
            eprintln!(
                "MEMORY_REPLAY_PAGE round={round} reference={reference} ttl={ttl} rows={rows} seconds={seconds:.6} allocations={allocations} stages={stats:?}"
            );
            costs.push(allocations);
            drop(recovery);
            drop(lookups);
            drop(mode);
            let mut seen = 0;
            db.export_latest_visible_for_each(false, |record| {
                assert_eq!(record.key, scale_key(seen));
                assert_eq!(record.value, Some(scale_val(seen)));
                assert_eq!(record.expire_unix_secs, expiry);
                seen += 1;
                true
            });
            assert_eq!(seen, rows);
            eprintln!("MEMORY_REPLAY_PAGE_VERIFIED round={round} ttl={ttl} rows={seen}");
        }
        assert!(costs[0] > costs[1] + rows.div_ceil(1000) as u64);
        assert!(costs[3] > costs[2] + rows.div_ceil(1000) as u64);
    }
}

#[test]
#[ignore = "M109-only replay allocation and stop comparison; release single thread"]
fn memory_replay_allocation_cost() {
    use crate::common::kv_snapshot::{
        KvRecord, atomic_write_bin_v4, recovery_probe, snapshot_probe,
    };
    use crate::memory::{mem_store::key_lookup_probe, memory_mvcc::replay_cost_reference};
    let rows: usize = std::env::var("SNAPSHOT_ROWS")
        .map(|s| s.parse().unwrap())
        .unwrap_or(600_000);
    assert!(rows >= 1000);
    for ttl in [false, true] {
        let dir = ScaleScratch::new("replay_allocation");
        let path = dir.0.join("snapshot.bxp");
        let expiry = ttl.then(|| crate::common::now_unix_secs() + 3600);
        atomic_write_bin_v4(&path, |writer| {
            for i in 0..rows {
                let mut record = KvRecord::new(scale_key(i), Some(scale_val(i)));
                record.expire_unix_secs = expiry;
                writer.write(&record)?;
            }
            Ok(())
        })
        .unwrap();
        let mut allocations = Vec::new();
        for (round, reference) in [true, false, false, true].into_iter().enumerate() {
            crate::scale_probe::memory("memory", "replay_cost_before_open");
            let mode = reference.then(replay_cost_reference::Scope::start);
            let lookups = key_lookup_probe::Scope::start();
            let recovery = recovery_probe::Scope::start();
            let before = crate::mem_probe::allocations();
            let start = std::time::Instant::now();
            let db = open_engine(EngineKind::Memory, Some(&dir.0)).unwrap();
            let seconds = start.elapsed().as_secs_f64();
            let count = crate::mem_probe::allocations() - before;
            allocations.push(count);
            let lookup_stats = lookups.stats();
            eprintln!(
                "MEMORY_REPLAY_COST round={round} reference={reference} ttl={ttl} rows={rows} seconds={seconds:.6} allocations={count} lookups={lookup_stats:?} stages={:?}",
                recovery.stats()
            );
            assert_eq!(
                lookup_stats.searches,
                if reference { rows as u64 } else { 0 }
            );
            assert_eq!(
                lookup_stats.end_misses,
                if reference { 0 } else { rows as u64 }
            );
            assert_eq!(lookup_stats.bound_grows, 0);
            drop(recovery);
            drop(lookups);
            drop(mode);
            crate::scale_probe::memory("memory", "replay_cost_after_open");
            let mut seen = 0;
            db.export_latest_visible_for_each(false, &mut |record| {
                assert_eq!(record.key, scale_key(seen));
                assert_eq!(record.value, Some(scale_val(seen)));
                assert_eq!(record.expire_unix_secs, expiry);
                seen += 1;
                true
            });
            assert_eq!(seen, rows);
            crate::scale_probe::memory("memory", "replay_cost_before_stop");
            let stop = snapshot_probe::Scope::start(false);
            let start = std::time::Instant::now();
            drop(db);
            let elapsed = start.elapsed().as_nanos();
            let stats = stop.stats();
            eprintln!(
                "MEMORY_REPLAY_STOP round={round} reference={reference} ttl={ttl} rows={rows} total_ms={:.3} release_ms={:.3} stages={stats:?}",
                elapsed as f64 / 1e6,
                elapsed.saturating_sub(stats.total_ns) as f64 / 1e6
            );
            drop(stop);
            crate::scale_probe::memory("memory", "replay_cost_after_stop");
        }
        // 两侧都保留 M105 的 I/O 和键所有权路径；非空普通值
        // 每行减少两次边界分配和一次值信封分配；
        // TTL 仍承担扩容八字节的成本，只去掉边界检查。
        let saved_per_row = if ttl { 2 } else { 3 };
        assert!(allocations[1] + saved_per_row * rows as u64 <= allocations[0]);
        assert!(allocations[2] + saved_per_row * rows as u64 <= allocations[3]);
    }
}
