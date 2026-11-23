// 该文件集中验证对应职责，保留原测试名称和字面值。

#[test]
fn test_basic_set_get_delete() {
    let path = tmp_db("basic");
    {
        let mvcc = MVCC::open(&path, 4, 16);
        let tx = mvcc.begin_transaction();
        assert!(tx.set(b"a", b"1".to_vec()));
        assert_eq!(tx.get(b"a"), Some(b"1".to_vec()));
        assert!(tx.set(b"a", b"2".to_vec()));
        assert_eq!(tx.get(b"a"), Some(b"2".to_vec()));
        assert!(tx.delete(b"a"));
        assert_eq!(tx.get(b"a"), None);
        tx.commit();
    }
    cleanup(&path);
}

#[test]
fn test_print_all_latest_visible() {
    let path = tmp_db("pa");
    {
        let mvcc = MVCC::open(&path, 4, 16);
        let t1 = mvcc.begin_transaction();
        assert!(t1.set(b"a", b"1".to_vec()));
        assert!(t1.set(b"b", b"2".to_vec()));
        assert!(t1.set(b"a", b"3".to_vec()));
        let all = t1.print_all();
        assert_eq!(all.get(&b"a".to_vec()).unwrap().0, Some(b"3".to_vec()));
        assert_eq!(all.get(&b"b".to_vec()).unwrap().0, Some(b"2".to_vec()));
        t1.commit();
    }
    cleanup(&path);
}

#[test]
fn test_instances_isolated() {
    let p1 = tmp_db("i1");
    let p2 = tmp_db("i2");
    {
        let m1 = MVCC::open(&p1, 4, 8);
        let m2 = MVCC::open(&p2, 4, 8);
        let t1 = m1.begin_transaction();
        let t2 = m2.begin_transaction();
        assert!(t1.set(b"k", b"1".to_vec()));
        assert!(t2.set(b"k", b"2".to_vec()));
        assert_eq!(t1.get(b"k"), Some(b"1".to_vec()));
        assert_eq!(t2.get(b"k"), Some(b"2".to_vec()));
        t1.commit();
        t2.commit();
    }
    cleanup(&p1);
    cleanup(&p2);
}

#[test]
fn test_large_value_blob_roundtrip() {
    let path = tmp_db("blob");
    let big = vec![0xABu8; 10_000]; // > BLOB_THRESHOLD
    {
        let mvcc = MVCC::open(&path, 32, 64);
        let tx = mvcc.begin_transaction();
        assert!(tx.set(b"big", big.clone()));
        assert!(tx.set(b"small", b"hi".to_vec()));
        assert_eq!(tx.get(b"big"), Some(big.clone()));
        assert_eq!(tx.get(b"small"), Some(b"hi".to_vec()));
        tx.commit();
    }
    {
        let mvcc = MVCC::open(&path, 32, 64);
        let tx = mvcc.begin_transaction();
        assert_eq!(tx.get(b"big"), Some(big));
        assert_eq!(tx.get(b"small"), Some(b"hi".to_vec()));
        tx.commit();
    }
    // blob 文件应存在且大于 header
    let blob = crate::bplus_tree::storage::blob_path(&path);
    assert!(blob.exists());
    assert!(std::fs::metadata(&blob).unwrap().len() > 8);
    cleanup(&path);
}
