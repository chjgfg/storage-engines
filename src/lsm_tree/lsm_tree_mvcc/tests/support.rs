// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

fn compare_vacuum_compaction_schedule(rows: usize, small: bool) {
    use crate::lsm_tree::lsm_tree::{maintenance_probe, vacuum_compaction_reference};
    fn copy_tree(from: &Path, to: &Path) {
        std::fs::create_dir_all(to).unwrap();
        for e in std::fs::read_dir(from).unwrap() {
            let e = e.unwrap();
            let dest = to.join(e.file_name());
            if e.file_type().unwrap().is_dir() {
                copy_tree(&e.path(), &dest);
            } else {
                std::fs::copy(e.path(), dest).unwrap();
            }
        }
    }
    fn value(i: usize, round: u8) -> Vec<u8> {
        let mut value = vec![round; 128];
        value[..8].copy_from_slice(&(i as u64).to_le_bytes());
        value
    }
    let mut opts = LSMTreeOptions::for_mvcc();
    if small {
        opts.mem_threshold_entries = 128;
        opts.mem_threshold_bytes = usize::MAX;
        opts.l0_compact_threshold = 2;
    }
    let seed = tmp_db("vacuum_compaction_seed");
    {
        let db = MVCC::try_open_with(&seed, Some(opts.clone())).unwrap();
        db.set_vacuum_auto_slice(0);
        for round in 0..2u8 {
            for base in (0..rows).step_by(1000) {
                let tx = db.begin_transaction();
                for i in base..(base + 1000).min(rows) {
                    assert!(tx.set_raw(&(i as u64).to_be_bytes(), value(i, round)));
                }
                tx.commit();
            }
        }
        db.checkpoint();
    }
    let mut measured = Vec::new();
    for (arm, old) in [true, false, false, true].into_iter().enumerate() {
        let target = tmp_db("vacuum_compaction_arm");
        copy_tree(&seed, &target);
        let db = MVCC::try_open_with(&target, Some(opts.clone())).unwrap();
        db.set_vacuum_auto_slice(0);
        let reference = old.then(vacuum_compaction_reference::Scope::start);
        let probe = maintenance_probe::Scope::start();
        let now = std::time::Instant::now();
        assert_eq!(db.vacuum().unwrap().versions_removed, rows);
        let ms = now.elapsed().as_secs_f64() * 1000.0;
        let stats = probe.stats();
        drop(probe);
        drop(reference);
        assert!(stats.threshold_flushes > 0);
        let tx = db.begin_transaction();
        for i in 0..rows {
            assert_eq!(tx.get(&(i as u64).to_be_bytes()), Some(value(i, 1)));
        }
        tx.commit();
        drop(tx);
        drop(db);
        let db = MVCC::try_open_with(&target, Some(opts.clone())).unwrap();
        let tx = db.begin_transaction();
        for i in 0..rows {
            assert_eq!(tx.get(&(i as u64).to_be_bytes()), Some(value(i, 1)));
        }
        tx.commit();
        drop(tx);
        drop(db);
        eprintln!(
            "LSM_VACUUM_BATCH_ABBA rows={rows} arm={arm} old={old} ms={ms:.3} threshold_flushes={} compactions={} merge_rows={} sst_syncs={} sst_sync_ms={:.3} all_values_verified={rows} reopened_values_verified={rows}",
            stats.threshold_flushes,
            stats.compactions,
            stats.owned_input_rows,
            stats.sst_syncs,
            stats.sst_sync_ns as f64 / 1e6
        );
        measured.push((old, stats));
    }
    assert!(
        measured
            .iter()
            .all(|x| x.1.threshold_flushes == measured[0].1.threshold_flushes)
    );
    // 不相交删除范围可能读同样多的行，却生成
    // 更少的输出文件；持久化验收统计实际同步调用数。
    let old: u64 = measured.iter().filter(|x| x.0).map(|x| x.1.sst_syncs).sum();
    let new: u64 = measured
        .iter()
        .filter(|x| !x.0)
        .map(|x| x.1.sst_syncs)
        .sum();
    let guard = |measured| {
        assert!(
            measured < old,
            "coalescing must reduce actual SST syncs: old={old} new={measured}"
        )
    };
    guard(new);
    assert!(std::panic::catch_unwind(|| guard(old)).is_err());
}

fn measure_vacuum_delete_stages(rows: usize, small_threshold: bool) {
    use crate::common::maintenance_probe::{Scope as VacuumScope, Stage};
    use crate::lsm_tree::lsm_tree::maintenance_probe::{Kind, Scope, Timer};
    use std::{assert, assert_eq, eprintln, vec};
    assert!(Timer::start(Kind::DeleteWrite).is_none());
    let path = tmp_db("vacuum_delete_stages");
    let mut opts = LSMTreeOptions::for_mvcc();
    if small_threshold {
        opts.mem_threshold_entries = 128;
        opts.mem_threshold_bytes = usize::MAX;
        opts.l0_compact_threshold = 2;
    }
    let db = MVCC::try_open_with(&path, Some(opts)).unwrap();
    db.set_vacuum_auto_slice(0);
    for round in 0..2u8 {
        for base in (0..rows).step_by(1000) {
            let tx = db.begin_transaction();
            for i in base..(base + 1000).min(rows) {
                let mut value = vec![round; 128];
                value[..8].copy_from_slice(&(i as u64).to_le_bytes());
                assert!(tx.set_raw(&(i as u64).to_be_bytes(), value));
            }
            tx.commit();
        }
    }
    db.checkpoint();
    let scope = Scope::start();
    let vacuum = VacuumScope::start();
    let now = std::time::Instant::now();
    let stats = db.vacuum().unwrap();
    let wall_ms = now.elapsed().as_secs_f64() * 1e3;
    let measured = scope.stats();
    let doors = vacuum.stats();
    drop(vacuum);
    drop(scope);
    let delete_ns = doors.ns[Stage::VacuumDelete as usize];
    let residual = delete_ns
        .checked_sub(measured.delete_write_ns + measured.threshold_flush_ns)
        .expect("write and threshold flush must be disjoint parts of delete");
    assert_eq!(stats.versions_removed, rows);
    assert_eq!(measured.delete_calls, rows as u64);
    assert_eq!(measured.mem_wal_syncs, 0, "MVCC uses its transaction WAL");
    assert!(
        measured.threshold_flushes > 0,
        "seed must trigger flush during deletion"
    );
    if small_threshold {
        assert!(measured.compactions > 0);
    }
    let tx = db.begin_transaction();
    for i in 0..rows {
        let mut value = vec![1; 128];
        value[..8].copy_from_slice(&(i as u64).to_le_bytes());
        assert_eq!(tx.get(&(i as u64).to_be_bytes()), Some(value));
    }
    tx.commit();
    drop(tx);
    drop(db);
    let db = MVCC::open(&path);
    let tx = db.begin_transaction();
    for i in 0..rows {
        let mut value = vec![1; 128];
        value[..8].copy_from_slice(&(i as u64).to_le_bytes());
        assert_eq!(tx.get(&(i as u64).to_be_bytes()), Some(value));
    }
    tx.commit();
    eprintln!(
        "LSM_VACUUM_DELETE_STAGES rows={rows} wall_ms={wall_ms:.3} delete_ms={:.3} write_ms={:.3} threshold_flush_ms={:.3} delete_residual_ms={:.3} threshold_flushes={} sst_flush_ms={:.3} compact_ms={:.3} manifest_ms={:.3} sst_finish_ms={:.3} sst_sync_ms={:.3} latest_values_verified={rows} reopened_values_verified={rows}",
        delete_ns as f64 / 1e6,
        measured.delete_write_ns as f64 / 1e6,
        measured.threshold_flush_ns as f64 / 1e6,
        residual as f64 / 1e6,
        measured.threshold_flushes,
        measured.flush_ns as f64 / 1e6,
        measured.compact_ns as f64 / 1e6,
        measured.manifest_ns as f64 / 1e6,
        measured.sst_finish_ns as f64 / 1e6,
        measured.sst_sync_ns as f64 / 1e6
    );
    assert!(Timer::start(Kind::DeleteWrite).is_none());
}

/// 返回 **RAII 句柄**（M82）：把它声明在引擎之前，用例结束（含 panic 展开）自动删，
/// 不必再指望人记得写 `cleanup` —— 而"引擎还开着就删"在 Windows 上是静默失败。
fn tmp_db(tag: &str) -> crate::common::ScratchPath {
    crate::common::ScratchPath::new("lsm_mvcc", tag, "")
}

fn cleanup(path: &Path) {
    let _ = std::fs::remove_dir_all(path);
    // 若路径被当文件创建过
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(wal_path(path));
    let _ = std::fs::remove_file(lock_path(path));
}
