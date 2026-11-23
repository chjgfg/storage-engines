// 该文件集中验证对应职责，保留原测试名称和字面值。

#[test]
fn test_export_latest_visible() {
    let path = tmp_db("export");
    {
        let mvcc = MVCC::open(&path, 16, 32);
        let t1 = mvcc.begin_transaction();
        assert!(t1.set(b"k1", b"v1".to_vec()));
        assert!(t1.set(b"k2", b"v2".to_vec()));
        t1.commit();
        let t2 = mvcc.begin_transaction();
        assert!(t2.set(b"k1", b"v1b".to_vec())); // 覆盖
        assert!(t2.delete(b"k2"));
        assert!(t2.set(b"k3", b"v3".to_vec()));
        t2.commit();
    }
    {
        let mvcc = MVCC::open(&path, 16, 32);
        let live = mvcc.export_latest_visible(false);
        assert_eq!(live.len(), 2);
        assert_eq!(
            live.iter().find(|r| r.key == b"k1").unwrap().value,
            Some(b"v1b".to_vec())
        );
        assert_eq!(
            live.iter().find(|r| r.key == b"k3").unwrap().value,
            Some(b"v3".to_vec())
        );
        assert!(!live.iter().any(|r| r.key == b"k2"));

        let all = mvcc.export_latest_visible(true);
        assert_eq!(all.len(), 3);
        assert!(all.iter().any(|r| r.key == b"k2" && r.value.is_none()));
    }
    cleanup(&path);
}
