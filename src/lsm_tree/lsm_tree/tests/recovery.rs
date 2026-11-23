// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn test_put_get_flush_reopen() {
    let dir = tmp_dir("basic");
    {
        let mut db = LSMTree::open(&dir).unwrap();
        db.put(b"a".to_vec(), b"1".to_vec());
        db.put(b"b".to_vec(), b"2".to_vec());
        db.delete(b"b".to_vec());
        assert_eq!(db.get(b"a"), Some(b"1".to_vec()));
        assert_eq!(db.get(b"b"), None);
        db.flush().unwrap();
    }
    {
        let mut db = LSMTree::open(&dir).unwrap();
        assert_eq!(db.get(b"a"), Some(b"1".to_vec()));
        assert_eq!(db.get(b"b"), None);
        assert_eq!(db.stats_exact().live_keys, 1);
    }
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_mem_wal_recovers() {
    let dir = tmp_dir("memwal");
    {
        let mut opts = LSMTreeOptions::default();
        opts.enable_mem_wal = true;
        opts.mem_wal_sync_on_write = true; // 崩溃安全
        opts.mem_threshold_entries = 1_000_000;
        opts.mem_threshold_bytes = usize::MAX / 4;
        let mut db = LSMTree::open_with_config(&dir, opts).unwrap();
        db.put(b"persist_me".to_vec(), b"yes".to_vec());
        // 不 flush：靠 mem.wal fsync 恢复
        db.dirty = false;
        std::mem::forget(db); // 跳过 Drop flush
    }
    {
        let mut opts = LSMTreeOptions::default();
        opts.enable_mem_wal = true;
        let db = LSMTree::open_with_config(&dir, opts).unwrap();
        assert_eq!(db.get(b"persist_me"), Some(b"yes".to_vec()));
    }
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_stats_open_is_approximate() {
    let dir = tmp_dir("stats_approx");
    {
        let mut db = LSMTree::open(&dir).unwrap();
        for i in 0..20u32 {
            db.put(format!("k{i}").into_bytes(), b"v".to_vec());
        }
        db.flush().unwrap();
        let exact = db.stats_exact().live_keys;
        assert_eq!(exact, 20);
    }
    {
        // open 不扫盘：live_keys 缓存为 0；文件计数准确
        let mut db = LSMTree::open(&dir).unwrap();
        let cheap = db.stats();
        assert_eq!(
            cheap.live_keys, 0,
            "open 后 stats().live_keys 应为 0（未 merge）"
        );
        assert!(cheap.total_sst >= 1, "SST 文件计数应准确");
        assert_eq!(db.stats_exact().live_keys, 20);
    }
    let _ = fs::remove_dir_all(&dir);
}
