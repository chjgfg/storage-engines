// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn scale_last_page_matches_seed_across_page_boundaries() {
    for kind in [
        EngineKind::Bitcask,
        EngineKind::BPlusTree,
        EngineKind::LsmTree,
        EngineKind::Memory,
    ] {
        for rows in [0, 1, 99, 100, 101, 200, 201] {
            let dir = ScaleScratch::new("last_page");
            let path = if kind == EngineKind::BPlusTree {
                dir.0.join("bplus.db")
            } else {
                dir.0.clone()
            };
            let db = open_engine(kind, Some(&path)).unwrap();
            let tx = db.begin();
            for i in 0..rows {
                assert!(tx.set_raw(&scale_key(i), scale_val(i)));
            }
            tx.commit();
            let tx = db.begin();
            let page = tx.search_keys(&SearchQuery::contains(
                b"",
                scale_last_page_index(rows, 100),
                100,
            ));
            scale_check_last_page(&page, rows, 100);
        }
    }
}

#[test]
fn scale_fixture_persist_preserves_all_engines_and_migration_values() {
    for kind in [
        EngineKind::Bitcask,
        EngineKind::BPlusTree,
        EngineKind::LsmTree,
        EngineKind::Memory,
    ] {
        let scratch = ScaleScratch::new("scale_persist_test");
        let path = match kind {
            EngineKind::Memory => None,
            EngineKind::BPlusTree => Some(scratch.0.join("bplus.db")),
            _ => Some(scratch.0.clone()),
        };
        let db = open_engine(kind, path.as_deref()).unwrap();
        scale_seed_test_fixture(&*db, 12);
        scale_persist_auxiliary(&*db, 12);
        let target = open_engine(EngineKind::Memory, None).unwrap();
        let stats = migrate(&*db, &*target, 3).unwrap();
        scale_check_migration(&*target, 12, stats).unwrap();
        let tx = target.begin();
        for i in 0..12 {
            assert_eq!(tx.get_ttl(&scale_key(48 + i)), Some(None));
        }
        tx.commit();
    }
}

#[test]
fn scale_ttl_deadline_and_migration_have_exact_before_after_sets() {
    // 短 TTL 的边界用明确的逻辑时刻：100 写、2 秒 TTL，101 活，102/103 死。
    // 实际四引擎再分别注入「尚未到期/已经到期」的信封状态来验迁移，
    // 不 sleep，也不要求 CI/磁盘必须抢在两秒内完成（没有全局时钟补丁）。
    let value = b"short-lived";
    let meta = crate::common::decode_value_meta(&crate::common::pack_ttl(value, 102));
    assert_eq!(meta.user(), value);
    assert!(!meta.is_expired_at(101));
    assert!(meta.is_expired_at(102));
    assert!(meta.is_expired_at(103));
    for kind in [
        EngineKind::Bitcask,
        EngineKind::BPlusTree,
        EngineKind::LsmTree,
        EngineKind::Memory,
    ] {
        let scratch = ScaleScratch::new("scale_ttl_test");
        let path = match kind {
            EngineKind::Memory => None,
            EngineKind::BPlusTree => Some(scratch.0.join("bplus.db")),
            _ => Some(scratch.0.clone()),
        };
        let db = open_engine(kind, path.as_deref()).unwrap();
        let tx = db.begin();
        assert!(tx.set_raw(b"plain", vec![0x01, 0x00, 0xFE]));
        // 两个确定的状态，期限本身不是长跑台架的解决方案；台架使用 persist。
        assert!(tx.set(b"ttl", crate::common::pack_ttl(value, u64::MAX)));
        tx.commit();
        let before = open_engine(EngineKind::Memory, None).unwrap();
        let stats = migrate(&*db, &*before, 1).unwrap();
        assert_eq!(stats.rows, 2, "{}: 未过期集合", kind.as_str());
        assert_eq!(stats.with_ttl, 1);
        let tx = before.begin();
        assert_eq!(tx.get(b"plain"), Some(vec![0x01, 0x00, 0xFE]));
        assert_eq!(tx.get(b"ttl"), Some(value.to_vec()));
        assert!(tx.get_ttl(b"ttl").flatten().is_some());
        assert_eq!(tx.scan(None, None).len(), 2);
        tx.commit();

        // 只改变辅助键的过期元数据，普通键不动；迁移期望固定为 1，不问 health。
        let tx = db.begin();
        assert!(tx.set(b"ttl", crate::common::pack_ttl(value, 0)));
        tx.commit();
        let tx = db.begin();
        assert_eq!(tx.get(b"ttl"), None);
        assert_eq!(tx.get_ttl(b"ttl"), None);
        tx.commit();
        let after = open_engine(EngineKind::Memory, None).unwrap();
        let stats = migrate(&*db, &*after, 1).unwrap();
        assert_eq!(stats.rows, 1, "{}: 已过期集合", kind.as_str());
        assert_eq!(stats.with_ttl, 0);
        let tx = after.begin();
        assert_eq!(tx.get(b"plain"), Some(vec![0x01, 0x00, 0xFE]));
        assert_eq!(tx.get(b"ttl"), None);
        assert_eq!(tx.scan(None, None).len(), 1);
        tx.commit();
    }
}

#[test]
fn scale_migration_manifest_rejects_missing_and_corrupt_data() {
    let rows = 12;
    let db = open_engine(EngineKind::Memory, None).unwrap();
    scale_seed_test_fixture(&*db, rows);
    scale_persist_auxiliary(&*db, rows);
    for defect in ["missing", "wrong-value", "same-count-wrong-key", "counter"] {
        let target = open_engine(EngineKind::Memory, None).unwrap();
        let stats = migrate(&*db, &*target, 3).unwrap();
        scale_check_migration(&*target, rows, stats).unwrap();
        let tx = target.begin();
        match defect {
            "missing" => assert!(tx.delete(&scale_key(1))),
            "wrong-value" => assert!(tx.set_raw(&scale_key(0), scale_val(0))), // 忘带更新版
            "same-count-wrong-key" => {
                assert!(tx.delete(&scale_key(1)));
                assert!(tx.set_raw(b"unexpected", scale_val(1)));
            }
            "counter" => assert!(tx.set_raw(b"counter:hits", b"999".to_vec())),
            _ => unreachable!(),
        }
        tx.commit();
        // stats 仍是原本正确的数：不能仅信迁移自己的计数，目标缺/错行也必须失败。
        assert!(
            scale_check_migration(&*target, rows, stats).is_err(),
            "必须抓到 {defect}"
        );
    }
    // 源端先丢一个非 TTL 键，migrate 与目标就会自洽地同少一行；独立种子账仍必须红。
    let tx = db.begin();
    assert!(tx.delete(&scale_key(1)));
    tx.commit();
    let target = open_engine(EngineKind::Memory, None).unwrap();
    let stats = migrate(&*db, &*target, 3).unwrap();
    assert_eq!(stats.rows, (rows * 2) as u64);
    assert!(scale_check_migration(&*target, rows, stats).is_err());
}

#[test]
fn scale_persist_auxiliary_rejects_missing_keys_without_reseeding() {
    let db = open_engine(EngineKind::Memory, None).unwrap();
    scale_seed_test_fixture(&*db, 12);
    let missing = scale_key(48);
    let tx = db.begin();
    assert!(tx.delete(&missing));
    tx.commit();
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        scale_persist_auxiliary(&*db, 12);
    }));
    assert!(failed.is_err());
    assert_eq!(db.begin().get(&missing), None);
}

#[test]
fn scale_bulk_reopen_verifies_all_engines() {
    for kind in [
        EngineKind::Bitcask,
        EngineKind::BPlusTree,
        EngineKind::LsmTree,
        EngineKind::Memory,
    ] {
        let scratch = ScaleScratch::new("bulk_manifest");
        let path = if kind == EngineKind::BPlusTree {
            scratch.0.join("bplus.db")
        } else {
            scratch.0.clone()
        };
        let db = open_engine(kind, Some(&path)).unwrap();
        scale_seed_bulk(&*db, 40, 4);
        drop(db);
        let db = open_engine(kind, Some(&path)).unwrap();
        scale_check_contents(&*db, 40, ScaleExpected::BulkReopen { bulk_rows: 4 }).unwrap();
    }
}

#[test]
fn scale_bulk_manifest_rejects_nonfirst_damage() {
    for defect in ["missing", "value", "extra-key", "old-row", "ttl"] {
        let db = open_engine(EngineKind::Memory, None).unwrap();
        scale_seed_bulk(&*db, 40, 4);
        scale_check_contents(&*db, 40, ScaleExpected::BulkReopen { bulk_rows: 4 }).unwrap();
        let tx = db.begin();
        match defect {
            "missing" => assert!(tx.delete(&scale_key(42))),
            "value" => assert!(tx.set_raw(&scale_key(42), b"wrong".to_vec())),
            "extra-key" => {
                assert!(tx.delete(&scale_key(42)));
                assert!(tx.set_raw(b"unexpected", scale_val(2)));
            }
            "old-row" => assert!(tx.set_raw(&scale_key(2), b"old-corrupt".to_vec())),
            "ttl" => assert!(tx.set(
                &scale_key(42),
                crate::common::pack_ttl(&scale_val(2), u64::MAX)
            )),
            _ => unreachable!(),
        }
        tx.commit();
        assert!(
            db.begin().get(&scale_key(40)).is_some(),
            "old first-key guard still passes"
        );
        assert!(
            scale_check_contents(&*db, 40, ScaleExpected::BulkReopen { bulk_rows: 4 }).is_err(),
            "missed {defect}"
        );
    }
}

#[test]
fn scale_scratch_cleans_owned_directory_on_unwind() {
    let scratch = ScaleScratch::new("scale_cleanup_test");
    let path = scratch.0.clone();
    assert!(path.is_dir());
    let failed = std::panic::catch_unwind(move || {
        let _scratch = scratch;
        panic!("simulate benchmark assertion failure");
    });
    assert!(failed.is_err());
    assert!(!path.exists());
}

#[test]
fn scale_bounded_vacuum_seed_is_required_and_preserves_latest_values() {
    const ROWS: usize = 32;
    for (name, db, _dir) in open_all() {
        let auto = db.vacuum_auto_slice();
        db.set_vacuum_auto_slice(0);
        let tx = db.begin();
        for i in 0..ROWS {
            tx.try_set_raw(&scale_key(i), scale_val(i)).unwrap();
        }
        tx.try_commit().unwrap();
        drop(tx);
        db.vacuum().unwrap();
        assert_eq!(
            db.vacuum_bounded(None, 2048)
                .unwrap()
                .stats
                .versions_removed,
            0,
            "{name}: control without old versions must be an empty sweep"
        );
        db.set_vacuum_auto_slice(auto);
        let (restore, keys) = scale_prepare_bounded_vacuum(&*db, ROWS);
        let round = db.vacuum_bounded(None, 2048).unwrap();
        db.set_vacuum_auto_slice(restore);
        assert_eq!(
            round.stats.versions_removed,
            keys * 2,
            "{name}: removing the seed must fail the non-empty delete guard"
        );
        assert_eq!(db.vacuum_auto_slice(), auto);
        let tx = db.begin();
        for i in 0..ROWS {
            assert_eq!(tx.get(&scale_key(i)), Some(scale_val(i)));
        }
    }
}

#[test]
fn scale_maintenance_probe_observes_real_flush_and_obeys_gate() {
    use crate::lsm_tree::lsm_tree::maintenance_probe::{Kind, Scope, Timer};
    assert!(Timer::start(Kind::Flush).is_none());
    let dir = ScaleScratch::new("maintenance_probe");
    let db = open_engine(EngineKind::LsmTree, Some(&dir.0)).unwrap();
    let tx = db.begin();
    assert!(tx.set_raw(b"key", b"value".to_vec()));
    tx.commit();
    drop(tx);
    let scope = Scope::start();
    db.flush().unwrap();
    assert_eq!(scope.stats().flushes, 1);
    db.flush().unwrap();
    assert_eq!(
        scope.stats().flushes,
        1,
        "empty flush does not create another SST"
    );
    assert_eq!(db.begin().get(b"key"), Some(b"value".to_vec()));
    drop(scope);
    assert!(Timer::start(Kind::Compact).is_none());
}

#[test]
fn scale_update_probe_observes_bplus_doors_and_obeys_gate() {
    use crate::common::maintenance_probe::{Scope, Stage, Timer};
    assert!(Timer::start(Stage::Commit).is_none());
    let dir = ScaleScratch::new("update_probe");
    let db = open_engine(EngineKind::BPlusTree, Some(&dir.0.join("bplus.db"))).unwrap();
    let scope = Scope::start();
    for value in [b"old", b"new"] {
        let tx = db.begin();
        assert!(tx.set_raw(b"key", value.to_vec()));
        tx.commit();
    }
    db.checkpoint().unwrap();
    assert_eq!(db.vacuum().unwrap().versions_removed, 1);
    assert_eq!(db.begin().get(b"key"), Some(b"new".to_vec()));
    let s = scope.stats();
    assert_eq!(s.calls[Stage::Commit as usize], 2);
    assert_eq!(s.calls[Stage::Checkpoint as usize], 1);
    for stage in [Stage::VacuumScan, Stage::VacuumDelete, Stage::VacuumSync] {
        assert!(s.calls[stage as usize] > 0);
    }
    assert_eq!(s.calls[Stage::AutoVacuum as usize], 0);
    drop(scope);
    assert!(Timer::start(Stage::VacuumScan).is_none());
}

#[test]
#[ignore = "focused write/update/vacuum recheck; release single thread"]
fn scale_maintenance_recheck() {
    let rows: usize = std::env::var("RECHECK_ROWS")
        .map(|s| s.parse().unwrap())
        .unwrap_or(10_000_000);
    let rounds: usize = std::env::var("RECHECK_ROUNDS")
        .map(|s| s.parse().unwrap())
        .unwrap_or(2);
    let name = std::env::var("RECHECK_ENGINE").unwrap_or_else(|_| "memory".to_string());
    let compare_blobs = std::env::var("RECHECK_BLOB_COMPARE").is_ok_and(|v| v == "1");
    let compare_dblwr = std::env::var("RECHECK_DBLWR_COMPARE").is_ok_and(|v| v == "1");
    let compare_manifest = std::env::var("RECHECK_MANIFEST_COMPARE").is_ok_and(|v| v == "1");
    assert!(
        !compare_manifest || (name == "lsm_tree" && rounds % 4 == 0 && !compare_blobs),
        "manifest comparison requires lsm_tree and isolated old/new/new/old rounds"
    );
    assert!(
        !compare_dblwr || (name == "bplus_tree" && rounds % 4 == 0),
        "double-write comparison requires bplus_tree and complete old/new/new/old rounds"
    );
    assert!(
        !compare_blobs || (name == "lsm_tree" && rounds % 4 == 0),
        "blob comparison requires lsm_tree and complete old/new/new/old rounds"
    );
    let kind = match name.as_str() {
        "memory" => EngineKind::Memory,
        "lsm_tree" => EngineKind::LsmTree,
        "bplus_tree" => EngineKind::BPlusTree,
        _ => panic!("RECHECK_ENGINE must be memory, bplus_tree or lsm_tree"),
    };
    assert!(rows > 0 && rounds > 0);
    for round in 0..rounds {
        let manifest_reference = compare_manifest && (round % 4 == 0 || round % 4 == 3);
        let _manifest_mode =
            manifest_reference.then(crate::lsm_tree::lsm_tree::manifest_reference::Scope::start);
        let dblwr_reference = compare_dblwr && (round % 4 == 0 || round % 4 == 3);
        let _dblwr_mode =
            dblwr_reference.then(crate::bplus_tree::storage::dblwr_probe::Scope::start);
        let dir = ScaleScratch::new("maintenance_recheck");
        let path = if kind == EngineKind::BPlusTree {
            dir.0.join("bplus.db")
        } else {
            dir.0.clone()
        };
        let scope = crate::lsm_tree::lsm_tree::maintenance_probe::Scope::start();
        let db = open_engine(kind, Some(&path)).unwrap();
        let start = std::time::Instant::now();
        for base in (0..rows).step_by(1000) {
            let tx = db.begin();
            for i in base..(base + 1000).min(rows) {
                assert!(tx.set_raw(&scale_key(i), scale_val(i)));
            }
            tx.commit();
            if (base + 1000) % 1_000_000 == 0 {
                eprintln!(
                    "RECHECK_PROGRESS engine={name} round={round} rows={} elapsed_s={:.3}",
                    base + 1000,
                    start.elapsed().as_secs_f64()
                );
            }
        }
        eprintln!(
            "RECHECK_INSERT engine={name} round={round} rows={rows} seconds={:.6}",
            start.elapsed().as_secs_f64()
        );
        scale_note_maintenance(&*db, "insert", &scope);
        db.flush().unwrap();
        db.checkpoint().unwrap();
        eprintln!("RECHECK_REOPEN engine={name} round={round}");
        drop(db);
        let db = open_engine(kind, Some(&path)).unwrap();
        let updates = (rows / 10).max(1);
        let dblwr_writes = crate::bplus_tree::storage::dblwr_probe::writes();
        let manifest_writes = crate::lsm_tree::lsm_tree::manifest_reference::writes();
        let syncs = crate::common::disk::fsync_counts();
        let stages = crate::common::maintenance_probe::Scope::start();
        let (mut begin_ns, mut write_ns, mut commit_ns) = (0u128, 0u128, 0u128);
        let start = std::time::Instant::now();
        for base in (0..updates).step_by(500) {
            let timer = std::time::Instant::now();
            let tx = db.begin();
            begin_ns += timer.elapsed().as_nanos();
            let timer = std::time::Instant::now();
            for i in base..(base + 500).min(updates) {
                assert!(tx.set_raw(&scale_key(i), scale_val(i + rows)));
            }
            write_ns += timer.elapsed().as_nanos();
            let timer = std::time::Instant::now();
            tx.commit();
            commit_ns += timer.elapsed().as_nanos();
        }
        eprintln!(
            "RECHECK_UPDATE engine={name} round={round} rows={updates} seconds={:.6}",
            start.elapsed().as_secs_f64()
        );
        eprintln!(
            "RECHECK_UPDATE_STAGES engine={name} round={round} begin_ms={:.3} write_ms={:.3} commit_ms={:.3} doors={:?}",
            begin_ns as f64 / 1e6,
            write_ns as f64 / 1e6,
            commit_ns as f64 / 1e6,
            stages.stats()
        );
        drop(stages);
        if kind == EngineKind::BPlusTree {
            eprintln!(
                "RECHECK_DBLWR round={round} reference={dblwr_reference} payload_writes={} fsync_delta={:?}",
                crate::bplus_tree::storage::dblwr_probe::writes() - dblwr_writes,
                std::array::from_fn::<_, 14, _>(
                    |i| crate::common::disk::fsync_counts()[i] - syncs[i]
                )
            );
        }
        if kind == EngineKind::LsmTree {
            eprintln!(
                "RECHECK_MANIFEST round={round} reference={manifest_reference} writes={}",
                crate::lsm_tree::lsm_tree::manifest_reference::writes() - manifest_writes
            );
        }
        scale_note_maintenance(&*db, "update", &scope);
        let start = std::time::Instant::now();
        let health = db.health();
        eprintln!(
            "RECHECK_HEALTH engine={name} round={round} seconds={:.6} stats={health:?}",
            start.elapsed().as_secs_f64()
        );
        let auto_removed = db.reclamation_status().unwrap().versions_removed;
        assert_eq!(
            auto_removed + health.rows_above_one,
            updates as u64,
            "old versions reappeared after automatic reclamation/compaction"
        );
        let reference = compare_blobs && (round % 4 == 0 || round % 4 == 3);
        let blob_reference =
            reference.then(crate::lsm_tree::lsm_tree::blob_reference::Scope::start);
        let stages = crate::common::maintenance_probe::Scope::start();
        let start = std::time::Instant::now();
        let vacuum = db.vacuum().unwrap();
        eprintln!(
            "RECHECK_VACUUM engine={name} round={round} reference={reference} seconds={:.6} stats={vacuum:?}",
            start.elapsed().as_secs_f64()
        );
        eprintln!(
            "RECHECK_VACUUM_STAGES engine={name} round={round} doors={:?}",
            stages.stats()
        );
        if kind == EngineKind::LsmTree {
            assert_eq!(
                stages.stats().blob_point_reads,
                if reference { rows as u64 } else { 0 }
            );
        }
        drop(stages);
        drop(blob_reference);
        assert!(!vacuum.skipped_active_txn);
        assert_eq!(vacuum.versions_removed as u64, health.rows_above_one);
        scale_note_maintenance(&*db, "vacuum", &scope);
        let mut seen = 0;
        db.export_latest_visible_for_each(false, &mut |rec| {
            assert_eq!(rec.key, scale_key(seen));
            assert_eq!(
                rec.value,
                Some(scale_val(if seen < updates { seen + rows } else { seen }))
            );
            seen += 1;
            true
        });
        assert_eq!(seen, rows);
        eprintln!("RECHECK_VERIFIED engine={name} round={round} rows={seen}");
    }
}
