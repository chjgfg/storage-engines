// 该文件集中验证对应职责，保留原测试名称和字面值。

#[test]
fn vacuum_delete_flush_failure_preserves_blobs() {
    let path = tmp_db("vacuum_delete_flush_failure");
    let mut opts = LSMTreeOptions::for_mvcc();
    opts.l0_compact_threshold = usize::MAX / 4;
    opts.max_compactions_per_flush = 0;
    let db = MVCC::try_open_with(&path, Some(opts)).unwrap();
    db.set_vacuum_auto_slice(0);
    for pass in 0..2 {
        let tx = db.begin_transaction();
        assert!(tx.set_raw(b"large", vec![pass; 8192]));
        tx.try_commit().unwrap();
        db.try_flush().unwrap();
    }
    let blobs = || std::fs::read_dir(path.join("blobs")).unwrap().count();
    assert_eq!(blobs(), 2);
    let mut opts = db.options();
    opts.mem_threshold_entries = 1;
    db.set_options(opts);
    std::fs::create_dir(path.join("MANIFEST.tmp")).unwrap();
    assert!(db.vacuum().is_err());
    assert_eq!(
        blobs(),
        2,
        "failed deletion publication must precede blob reclamation"
    );
    std::fs::remove_dir(path.join("MANIFEST.tmp")).unwrap();
}

#[test]
fn vacuum_bad_sst_aborts_before_blob_gc_with_reference_control() {
    for reference in [true, false] {
        let path = tmp_db("vacuum_bad_sst_gc");
        let mut opts = LSMTreeOptions::for_mvcc();
        opts.l0_compact_threshold = usize::MAX / 4;
        opts.max_compactions_per_flush = 0;
        let db = MVCC::try_open_with(&path, Some(opts)).unwrap();
        db.set_vacuum_auto_slice(0);
        for pass in 0..2 {
            let tx = db.begin_transaction();
            assert!(tx.set_raw(b"large", vec![pass; 8192]));
            tx.try_commit().unwrap();
            db.try_flush().unwrap();
        }
        let blobs = || std::fs::read_dir(path.join("blobs")).unwrap().count();
        let before = blobs();
        assert_eq!(before, 2);
        let mut files = 0;
        for entry in std::fs::read_dir(path.join("sst")).unwrap() {
            let entry = entry.unwrap();
            if entry.path().extension().is_some_and(|ext| ext == "sst") {
                std::fs::OpenOptions::new()
                    .write(true)
                    .open(entry.path())
                    .unwrap()
                    .set_len(8)
                    .unwrap();
                files += 1;
            }
        }
        assert_eq!(files, 2);
        let mode = reference.then(vacuum_cost_reference::Scope::start);
        let result = db.vacuum();
        if reference {
            assert!(
                result.is_ok(),
                "old scanner silently skipped both unreadable sources"
            );
            // 第二次写入在冲突查询时预热了第一个 SST。
            // 旧扫描器因此会保留 v1 并误删 v2 的 blob，守卫必须覆盖这个热缓存场景。
            assert_eq!(
                blobs(),
                1,
                "old cache fallback kept an obsolete reference and lost the latest blob"
            );
            let tx = db.begin_transaction();
            assert_eq!(tx.get(b"large"), Some(vec![0; 8192]));
            tx.try_commit().unwrap();
        } else {
            assert!(result.is_err());
            assert_eq!(
                blobs(),
                before,
                "failed maintenance scan must preserve every blob"
            );
        }
        drop(mode);
    }
}
