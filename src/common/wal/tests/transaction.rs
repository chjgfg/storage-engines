// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn commit_probe_separates_wal_append_and_sync() {
    let path = tmp_wal("commit_probe");
    let mut wal = Wal::open(&path).unwrap();
    let probe = commit_probe::Scope::start();
    wal.commit_transaction(1).unwrap();
    let stats = probe.stats();
    assert_eq!(stats.attempts, 1);
    assert_eq!(stats.append_calls, 1);
    assert_eq!(stats.sync_calls, 1);
    assert_eq!(stats.fsync_calls, 1);
    assert_eq!(stats.sync_histogram.iter().sum::<u64>(), 1);
    drop(probe);
    drop(wal);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn test_recovery_undo_uncommitted() {
    let path = tmp_wal("undo");
    {
        let mut wal = Wal::open(&path).unwrap();
        wal.append(WalRecord::Begin { xid: 1 }).unwrap();
        wal.append(WalRecord::Write {
            xid: 1,
            key: b"a".to_vec(),
            value: Some(b"1".to_vec()),
        })
        .unwrap();
        wal.append(WalRecord::Commit { xid: 1 }).unwrap();

        wal.append(WalRecord::Begin { xid: 2 }).unwrap();
        wal.append(WalRecord::Write {
            xid: 2,
            key: b"b".to_vec(),
            value: Some(b"2".to_vec()),
        })
        .unwrap();
        // 无 Commit → 崩溃
        wal.sync().unwrap();
    }
    {
        let mut wal = Wal::open(&path).unwrap();
        let plan = analyze_recovery(&wal.read_all().unwrap());
        assert_eq!(plan.committed_writes.len(), 1);
        assert_eq!(plan.uncommitted_writes.len(), 1);
        assert_eq!(plan.uncommitted_writes[0].1, b"b");
    }
    let _ = std::fs::remove_file(&path);
}
