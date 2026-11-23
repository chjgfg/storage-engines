// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn search_borrow_preserves_blob_ttl_binary_and_snapshot_fallbacks() {
    let path = tmp_db("search_borrow_fallbacks");
    let db = MVCC::open(&path);
    db.set_vacuum_auto_slice(0);
    let tx = db.begin_transaction();
    let mut keys: Vec<Vec<u8>> = (0..4200u64).map(|i| i.to_be_bytes().to_vec()).collect();
    keys.extend([
        vec![],
        b"a".to_vec(),
        b"a\0".to_vec(),
        b"a\0\0".to_vec(),
        vec![255],
    ]);
    for (i, key) in keys.iter().enumerate() {
        let value = if i < 4100 {
            crate::common::codec::pack_ttl(b"expired", 0)
        } else if i % 3 == 0 {
            crate::common::codec::pack_ttl(&vec![7; 8192], u64::MAX)
        } else {
            crate::common::codec::pack_plain(b"live")
        };
        assert!(tx.set(key, value));
    }
    tx.commit();
    db.flush();
    let snapshot = db.begin_transaction();
    let writer = db.begin_transaction();
    for key in keys.iter().skip(4100).step_by(3) {
        assert!(writer.delete(key));
    }
    fn verify(tx: &Transaction) {
        for deleted in [false, true] {
            for page in [0, 1, 247] {
                for total in [false, true] {
                    let q = crate::common::SearchQuery {
                        pattern: vec![],
                        mode: crate::common::KeyMatchMode::Prefix,
                        include_deleted: deleted,
                        page,
                        page_size: 17,
                        need_total: total,
                    };
                    let reference = search_borrow_reference::Scope::start();
                    let old = tx.search_keys(&q);
                    let count = crate::common::count_window(tx, deleted, None, q.mode);
                    drop(reference);
                    let actual = tx.search_keys(&q);
                    assert_eq!(actual.items, old.items);
                    assert_eq!(actual.total, old.total);
                    assert_eq!(actual.has_next, old.has_next);
                    assert_eq!(
                        crate::common::count_window(tx, deleted, None, q.mode),
                        count
                    );
                }
            }
        }
    }
    verify(&writer);
    verify(&snapshot);
    writer.commit();
    verify(&snapshot);
    drop(snapshot);
    let tx = db.begin_transaction();
    verify(&tx);
    tx.commit();
    db.flush();
    drop(db);
    let db = MVCC::open(&path);
    let tx = db.begin_transaction();
    verify(&tx);
    tx.commit();
}

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
