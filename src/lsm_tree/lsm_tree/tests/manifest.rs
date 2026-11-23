// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn manifest_publication_guard_requires_target_level() {
    let text = "LSMAN002\nedit 17\nnext_id 18\nnext_seq 19\nL0 20\nL1 21\nL2 22\n";
    assert!(manifest_publishes(text, 1, &[21]));
    assert!(!manifest_publishes("L1 21\nL1 22\n", 1, &[21]));
    assert!(!manifest_publishes("L1 21 bad\n", 1, &[21]));
    assert!(!manifest_publishes(text, 3, &[]));
    for id in [17, 18, 19, 20, 22] {
        assert!(manifest_publishes_reference(text, &[id]));
        assert!(
            !manifest_publishes(text, 1, &[id]),
            "unpublished id {id} passed the guard"
        );
    }
}

#[test]
fn trivial_move_manifest_failure_preserves_file_and_source_membership() {
    let dir = tmp_dir("trivial_publish_failure");
    let mut db = LSMTree::open(&dir).unwrap();
    db.inject_sst_for_test(
        1,
        vec![(b"a".to_vec(), StoredEntry::put(1, Some(b"v".to_vec())))],
    )
    .unwrap();
    db.write_manifest().unwrap();
    let old_manifest = fs::read(db.manifest_path()).unwrap();
    let id = db.levels[1][0].id;
    let path = db.levels[1][0].path.clone();
    // 占用临时 manifest 路径，触发真实的清单发布失败。
    fs::create_dir(db.dir.join("MANIFEST.tmp")).unwrap();
    assert!(db.compact_level(1).is_err());
    assert_eq!(db.levels[1][0].id, id);
    assert!(db.levels[2].is_empty());
    assert!(path.exists());
    assert_eq!(fs::read(db.manifest_path()).unwrap(), old_manifest);
    fs::remove_dir(db.dir.join("MANIFEST.tmp")).unwrap();
    db.compact_level(1).unwrap();
    assert_eq!(db.levels[2][0].id, id);
}

#[test]
fn manifest_buffer_preserves_bytes_and_bounds_writes() {
    let dir = tmp_dir("manifest_buffer");
    let mut db = LSMTree::open(&dir).unwrap();
    db.manifest_edit_id = 17;
    db.next_sst_id.store(18, Ordering::SeqCst);
    db.write_seq.store(19, Ordering::SeqCst);
    let mut bytes = Vec::new();
    let before = manifest_reference::writes();
    db.write_manifest_output(manifest_reference::Sink(&mut bytes))
        .unwrap();
    assert_eq!(
        bytes,
        b"LSMAN002\nedit 17\nnext_id 18\nnext_seq 19\nL0\nL1\nL2\nL3\n"
    );
    assert_eq!(manifest_reference::writes() - before, 1);
    let _reference = manifest_reference::Scope::start();
    let mut old = Vec::new();
    let before = manifest_reference::writes();
    db.write_manifest_output(manifest_reference::Sink(&mut old))
        .unwrap();
    assert_eq!(old, bytes);
    assert!(manifest_reference::writes() - before > 10);
}

#[test]
fn flush_manifest_reuses_exact_published_state_with_negative_control() {
    use crate::common::disk::{SITE_LSM_MANIFEST, fsync_counts};
    for reference in [true, false] {
        let dir = tmp_dir("manifest_reuse");
        let mut db = LSMTree::open(&dir).unwrap();
        db.opts.mem_threshold_entries = usize::MAX;
        db.opts.mem_threshold_bytes = usize::MAX;
        db.opts.l0_compact_threshold = 1;
        for i in 0..16u64 {
            db.insert(i.to_be_bytes().to_vec(), Some(vec![i as u8; 128]));
        }
        let mode = reference.then(manifest_repeat_reference::Scope::start);
        let scope = maintenance_probe::Scope::start();
        let before = fsync_counts()[SITE_LSM_MANIFEST];
        db.flush().unwrap();
        let syncs = fsync_counts()[SITE_LSM_MANIFEST] - before;
        assert_eq!(syncs, if reference { 3 } else { 1 });
        assert_eq!(scope.stats().manifest_reuses, if reference { 0 } else { 2 });
        let guard = std::panic::catch_unwind(|| assert_eq!(syncs, 1));
        assert_eq!(
            guard.is_err(),
            reference,
            "repeated publications must fail unchanged guard"
        );
        drop(scope);
        drop(mode);
        drop(db);
        let db = LSMTree::open(&dir).unwrap();
        for i in 0..16u64 {
            assert_eq!(db.get(&i.to_be_bytes()), Some(vec![i as u8; 128]));
        }
    }
}

#[test]
fn flush_manifest_failed_publication_cannot_be_reused() {
    let dir = tmp_dir("manifest_reuse_failure");
    let mut db = LSMTree::open(&dir).unwrap();
    db.opts.mem_threshold_entries = usize::MAX;
    db.opts.mem_threshold_bytes = usize::MAX;
    db.opts.l0_compact_threshold = 1;
    db.write_manifest().unwrap(); // A fresh open does not create MANIFEST.
    let old = fs::read(db.manifest_path()).unwrap();
    db.insert(b"key".to_vec(), Some(b"value".to_vec()));
    let blocked = dir.join("MANIFEST.tmp");
    fs::create_dir(&blocked).unwrap();
    assert!(db.flush().is_err());
    assert_eq!(fs::read(db.manifest_path()).unwrap(), old);
    assert_eq!(db.get(b"key"), Some(b"value".to_vec()));
    fs::remove_dir(&blocked).unwrap();
    db.flush().unwrap();
    drop(db);
    assert_eq!(
        LSMTree::open(&dir).unwrap().get(b"key"),
        Some(b"value".to_vec())
    );
}

#[test]
fn manifest_buffer_propagates_flush_write_error() {
    struct Broken;
    impl Write for Broken {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("manifest write interrupted"))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let dir = tmp_dir("manifest_write_error");
    let db = LSMTree::open(&dir).unwrap();
    assert!(db.write_manifest_output(Broken).is_err());
}

#[test]
fn failed_compaction_input_restores_manifest_and_sources() {
    let dir = tmp_dir("compaction_read_error");
    let mut opts = pruning_options();
    opts.sst_block_entries = 1;
    opts.target_sst_bytes = 1;
    let mut db = LSMTree::open_with_config(&dir, opts.clone()).unwrap();
    db.block_cache.lock().unwrap().set_capacity(0);
    db.inject_sst_stream_for_test(
        0,
        (0..20_u64).map(|i| (i.to_be_bytes().to_vec(), StoredEntry::put(1, Some(vec![1])))),
    )
    .unwrap();
    db.inject_sst_stream_for_test(
        0,
        (0..10_u64).map(|i| (i.to_be_bytes().to_vec(), StoredEntry::put(2, Some(vec![2])))),
    )
    .unwrap();
    let manifest = fs::read(db.manifest_path()).unwrap();
    let layout: Vec<Vec<_>> = db
        .levels
        .iter()
        .map(|files| files.iter().map(|file| file.id).collect())
        .collect();
    let path = db.levels[0][0].path.clone();
    let clean = fs::read(&path).unwrap();
    let mut bad = clean.clone();
    // 只损坏数据区，保留尾部和索引，使源文件仍能打开。
    bad[150] ^= 1;
    fs::write(&path, &bad).unwrap();
    assert!(
        db.compact_level(0).is_err(),
        "a bad input must stop publication"
    );
    assert_eq!(fs::read(db.manifest_path()).unwrap(), manifest);
    assert_eq!(
        db.levels
            .iter()
            .map(|files| files.iter().map(|file| file.id).collect::<Vec<_>>())
            .collect::<Vec<_>>(),
        layout
    );
    let files = fs::read_dir(db.dir.join("sst")).unwrap().count();
    assert_eq!(
        files, 2,
        "failed merge must remove all its unpublished outputs"
    );
    assert_eq!(db.get(&0_u64.to_be_bytes()), Some(vec![2]));
    fs::write(&path, clean).unwrap();
    db.compact_level(0).unwrap();
    for i in 0..20_u64 {
        assert_eq!(
            db.get(&i.to_be_bytes()),
            Some(vec![if i < 10 { 2 } else { 1 }])
        );
    }
    drop(db);
    let db = LSMTree::open_with_config(&dir, opts).unwrap();
    for i in 0..20_u64 {
        assert_eq!(
            db.get(&i.to_be_bytes()),
            Some(vec![if i < 10 { 2 } else { 1 }])
        );
    }
}
