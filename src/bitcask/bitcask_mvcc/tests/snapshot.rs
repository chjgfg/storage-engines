// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn test_snapshot_isolation() {
    let path = tmp_db("si");
    {
        let mvcc = MVCC::open(&path);
        let t1 = mvcc.begin_transaction();
        assert!(t1.set(b"k", b"v1".to_vec()));
        t1.commit();
        let t2 = mvcc.begin_transaction();
        let t3 = mvcc.begin_transaction();
        assert!(t2.set(b"k", b"v2".to_vec()));
        assert_eq!(t3.get(b"k"), Some(b"v1".to_vec()));
        t2.commit();
        assert_eq!(t3.get(b"k"), Some(b"v1".to_vec()));
        let t4 = mvcc.begin_transaction();
        assert_eq!(t4.get(b"k"), Some(b"v2".to_vec()));
        t4.commit();
        t3.commit();
    }
    cleanup(&path);
}
