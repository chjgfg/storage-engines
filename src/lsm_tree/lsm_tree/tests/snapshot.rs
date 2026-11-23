// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn known_vacuum_delete_wal_replay_and_deep_source_barrier() {
    let dir = tmp_dir("known_delete_reopen");
    let mut opts = LSMTreeOptions::default();
    opts.enable_mem_wal = true;
    opts.mem_wal_sync_on_write = true;
    opts.mem_threshold_entries = usize::MAX;
    opts.mem_threshold_bytes = usize::MAX;
    let mut db = LSMTree::open_with_config(&dir, opts.clone()).unwrap();
    db.inject_sst_for_test(
        3,
        vec![(b"key".to_vec(), StoredEntry::put(1, Some(b"deep".to_vec())))],
    )
    .unwrap();
    db.write_seq.store(2, Ordering::SeqCst);
    db.insert(b"key".to_vec(), Some(b"new".to_vec()));
    db.remove_vacuum_known(VacuumDelete::observed(b"key".to_vec(), 0).unwrap())
        .unwrap();
    let next_seq = db.write_seq.load(Ordering::SeqCst);
    // 只丢弃易失测试状态来模拟崩溃，避免
    // Drop 自动刷盘，从而绕过 mem WAL 的恢复入口。
    db.mem.clear();
    db.mem_entries = 0;
    db.mem_bytes = 0;
    db.dirty = false;
    drop(db);
    let mut recovered = LSMTree::open_with_config(&dir, opts.clone()).unwrap();
    assert_eq!(
        recovered.get_raw(b"key"),
        None,
        "mem WAL Drop must hide the deep source"
    );
    assert_eq!(recovered.write_seq.load(Ordering::SeqCst), next_seq);
    recovered.flush().unwrap();
    recovered.compact_level(0).unwrap();
    assert_eq!(recovered.get_raw(b"key"), None);
    drop(recovered);
    let recovered = LSMTree::open_with_config(&dir, opts).unwrap();
    assert_eq!(
        recovered.get_raw(b"key"),
        None,
        "compaction/reopen cannot resurrect the old value"
    );
}

#[test]
fn test_sst_pruning_rejection_fallback_preserves_snapshot() {
    let dir = tmp_dir("sst_pruning_fallback");
    {
        let mut db = LSMTree::open_with_config(&dir, pruning_options()).unwrap();
        for n in 0..4098 {
            db.mem_insert_no_wal(
                format!("k{n:04}").into_bytes(),
                StoredEntry::put(1, Some(vec![1])),
            );
        }
        db.mem_insert_no_wal(b"k0000".to_vec(), StoredEntry::drop_at(10));
        let accept = |k: &[u8], _: &Option<Vec<u8>>| k == b"k0000";
        let want = db
            .range_scan_at_seq(b"k0000", b"k9999", 1)
            .into_iter()
            .rev()
            .find(|(k, v)| accept(k, v));
        assert!(want.is_some());
        #[cfg(feature = "lsm-allocation-reference")]
        let references = [true, false];
        #[cfg(not(feature = "lsm-allocation-reference"))]
        let references = [false];
        for _reference in references {
            #[cfg(feature = "lsm-allocation-reference")]
            let mode = _reference.then(allocation_reference::Scope::start);
            assert_eq!(db.find_back(b"k0000", b"k9999", 1, accept), want);
            assert!(
                matches!(db.find_key_back(b"k0000", b"k9999", 1, |k| k == b"k0000"),
                    KeyBackResult::FullScan(Some(key)) if key == b"k0000")
            );
            assert!(matches!(
                db.find_key_back(b"k0000", b"k9999", 10, |k| k == b"k0000"),
                KeyBackResult::FullScan(None)
            ));
            #[cfg(feature = "lsm-allocation-reference")]
            drop(mode);
        }
        db.mem.clear();
    }
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_set_options_and_snapshot_seq() {
    let dir = tmp_dir("opts");
    let mut db = LSMTree::open(&dir).unwrap();
    let s0 = db.snapshot_seq();
    db.put(b"a".to_vec(), b"1".to_vec());
    assert!(db.snapshot_seq() > s0);
    let mut o = db.options().clone();
    o.l0_compact_threshold = 99;
    db.set_options(o);
    assert_eq!(db.options().l0_compact_threshold, 99);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_snapshot_hides_newer_writes() {
    let dir = tmp_dir("snap");
    let mut db = LSMTree::open(&dir).unwrap();
    db.put(b"k".to_vec(), b"v1".to_vec());
    let snap = db.snapshot();
    db.put(b"k".to_vec(), b"v2".to_vec());
    assert_eq!(db.get(b"k"), Some(b"v2".to_vec()));
    assert_eq!(db.get_at(&snap, b"k"), Some(b"v1".to_vec()));
    db.flush().unwrap();
    assert_eq!(db.get_at(&snap, b"k"), Some(b"v1".to_vec()));
    db.release_snapshot(snap);
    let _ = fs::remove_dir_all(&dir);
}
