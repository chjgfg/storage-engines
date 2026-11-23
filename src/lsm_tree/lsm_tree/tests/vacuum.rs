// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn known_vacuum_delete_matches_remove_without_point_reads() {
    let mut results = Vec::new();
    for reference in [true, false] {
        let dir = tmp_dir("known_delete");
        let mut db = LSMTree::open(&dir).unwrap();
        db.opts.mem_threshold_entries = usize::MAX;
        db.opts.mem_threshold_bytes = usize::MAX;
        db.insert(b"live".to_vec(), Some(b"v".to_vec()));
        db.insert(b"tomb".to_vec(), None);
        db.ttl_meta.insert(b"live".to_vec(), u64::MAX);
        let seq = db.write_seq.load(Ordering::SeqCst);
        let before = vacuum_delete_probe::reads();
        let mode = reference.then(vacuum_delete_probe::Scope::start);
        db.remove_vacuum_known(VacuumDelete::observed(b"live".to_vec(), 0).unwrap())
            .unwrap();
        db.remove_vacuum_known(VacuumDelete::observed(b"tomb".to_vec(), 1).unwrap())
            .unwrap();
        drop(mode);
        assert_eq!(
            vacuum_delete_probe::reads() - before,
            if reference { 4 } else { 0 }
        );
        assert_eq!(db.write_seq.load(Ordering::SeqCst), seq + 2);
        assert_eq!(
            LSMTree::mem_get_at(&db.mem, b"live", seq - 1)
                .unwrap()
                .value,
            EntryValue::Put(Some(b"v".to_vec()))
        );
        assert!(db.ttl_meta.is_empty());
        assert!(VacuumDelete::observed(b"live".to_vec(), 2).is_err());
        assert!(VacuumDelete::observed(b"live".to_vec(), 255).is_err());
        results.push((
            db.mem.clone(),
            db.mem_entries,
            db.mem_bytes,
            db.cached_live,
            db.dirty,
            db.stats_dirty,
        ));
    }
    assert_eq!(results[0], results[1]);
}

#[test]
fn known_vacuum_delete_wal_failure_does_not_apply_memory() {
    use crate::lsm_tree::mem_wal::TestFailure;
    for reference in [true, false] {
        for failure in [TestFailure::Append, TestFailure::Sync] {
            let dir = tmp_dir("known_delete_wal_failure");
            let mut opts = LSMTreeOptions::default();
            opts.enable_mem_wal = true;
            opts.mem_wal_sync_on_write = true;
            let mut db = LSMTree::open_with_config(&dir, opts).unwrap();
            db.insert(b"key".to_vec(), Some(b"v".to_vec()));
            db.ttl_meta.insert(b"key".to_vec(), u64::MAX);
            let before = (
                db.mem.clone(),
                db.mem_entries,
                db.mem_bytes,
                db.cached_live,
                db.ttl_meta.clone(),
            );
            db.mem_wal.as_mut().unwrap().fail_next = Some(failure);
            let mode = reference.then(vacuum_delete_probe::Scope::start);
            let result =
                db.remove_vacuum_known(VacuumDelete::observed(b"key".to_vec(), 0).unwrap());
            drop(mode);
            let after = (
                db.mem.clone(),
                db.mem_entries,
                db.mem_bytes,
                db.cached_live,
                db.ttl_meta.clone(),
            );
            if reference {
                assert!(result.is_ok());
                assert_ne!(
                    after, before,
                    "old remove ignores WAL failures and applies Drop"
                );
            } else {
                assert!(result.is_err());
                assert_eq!(after, before);
            }
            // 同步失败时记录可能已经追加，持久化结果仍不确定；
            // 不能据此断言 WAL 没有发生改变。
        }
    }
}

#[test]
fn known_vacuum_delete_propagates_threshold_publish_failure() {
    for reference in [true, false] {
        let dir = tmp_dir("known_delete_publish_failure");
        let mut db = LSMTree::open(&dir).unwrap();
        db.insert(b"key".to_vec(), Some(b"v".to_vec()));
        db.flush().unwrap();
        db.opts.mem_threshold_entries = 1;
        fs::create_dir(db.dir.join("MANIFEST.tmp")).unwrap();
        let mode = reference.then(vacuum_delete_probe::Scope::start);
        let result = db.remove_vacuum_known(VacuumDelete::observed(b"key".to_vec(), 0).unwrap());
        drop(mode);
        assert_eq!(
            result.is_err(),
            !reference,
            "old threshold flush silently discards publication errors"
        );
        fs::remove_dir(db.dir.join("MANIFEST.tmp")).unwrap();
    }
}

#[test]
fn vacuum_merge_matches_light_merge_for_ties_drops_and_lazy_bounds() {
    let inputs = vec![
        vec![
            (b"a".to_vec(), LightEntry { seq: 3, kind: 1 }),
            (b"c".to_vec(), LightEntry { seq: 4, kind: 0 }),
        ],
        vec![
            (b"a".to_vec(), LightEntry { seq: 3, kind: 0 }),
            (b"b".to_vec(), LightEntry { seq: 5, kind: 2 }),
        ],
        vec![(b"z".to_vec(), LightEntry { seq: 6, kind: 0 })],
    ];
    let bounds = vec![
        Some(b"a".as_slice()),
        Some(b"a".as_slice()),
        Some(b"z".as_slice()),
    ];
    let expected: Vec<_> = LightMerge::build(
        inputs
            .clone()
            .into_iter()
            .map(|v| Box::new(v.into_iter()) as Box<dyn Iterator<Item = _>>)
            .collect(),
        bounds.clone(),
    )
    .collect();
    let actual = VacuumMerge::build(
        inputs
            .into_iter()
            .map(|v| Box::new(v.into_iter().map(Ok)) as Box<dyn Iterator<Item = _>>)
            .collect(),
        bounds,
    )
    .collect::<std::io::Result<Vec<_>>>()
    .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(actual[0].1.kind, 1, "equal seq keeps earliest source");
    assert_eq!(actual[1].1.kind, 2, "Drop must participate in key progress");
    assert_eq!(actual.last().unwrap().0, b"z");
}

#[test]
fn vacuum_merge_late_error_stops_and_discards_pending_output() {
    let source = vec![
        Ok((b"a".to_vec(), LightEntry { seq: 1, kind: 0 })),
        Ok((b"b".to_vec(), LightEntry { seq: 2, kind: 0 })),
        Ok((b"c".to_vec(), LightEntry { seq: 3, kind: 0 })),
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "bad later block",
        )),
    ];
    let mut merge = VacuumMerge::build(vec![Box::new(source.into_iter())], vec![None]);
    assert_eq!(merge.next().unwrap().unwrap().0, b"a");
    assert_eq!(
        merge.next().unwrap().unwrap_err().kind(),
        std::io::ErrorKind::InvalidData
    );
    assert!(
        merge.next().is_none(),
        "failed scan cannot flush its unproven pending key"
    );
}

#[test]
fn vacuum_schedule_keeps_threshold_durability_and_restores_failure_paths() {
    let dir = tmp_dir("vacuum_schedule");
    let mut db = LSMTree::open(&dir).unwrap();
    db.opts.mem_threshold_entries = 1;
    db.opts.mem_threshold_bytes = usize::MAX;
    db.opts.l0_compact_threshold = 2;
    db.opts.l0_stop_trigger = 3;
    db.with_vacuum_writes(true, |tree| {
        for i in 0..8u64 {
            tree.insert(i.to_be_bytes().to_vec(), Some(vec![i as u8; 16]));
            assert!(tree.levels[0].len() < tree.opts.l0_stop_trigger);
        }
        Ok(())
    })
    .unwrap();
    assert!(db.vacuum_write_schedule == VacuumWriteSchedule::Immediate);
    db.write_manifest().unwrap();
    let blocked = dir.join("MANIFEST.tmp");
    fs::create_dir(&blocked).unwrap();
    let failed = db.with_vacuum_writes(true, |tree| {
        tree.remove_vacuum_known(VacuumDelete::observed(0u64.to_be_bytes().to_vec(), 0)?)
    });
    assert!(failed.is_err());
    assert!(db.vacuum_write_schedule == VacuumWriteSchedule::Immediate);
    fs::remove_dir(&blocked).unwrap();
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = db.with_vacuum_writes(true, |_| -> std::io::Result<()> {
            panic!("scope test");
        });
    }));
    assert!(panicked.is_err());
    assert!(db.vacuum_write_schedule == VacuumWriteSchedule::Immediate);
    db.flush().unwrap();
    drop(db);
    let db = LSMTree::open(&dir).unwrap();
    assert_eq!(db.get(&0u64.to_be_bytes()), None);
    for i in 1..8u64 {
        assert_eq!(db.get(&i.to_be_bytes()), Some(vec![i as u8; 16]));
    }
}
