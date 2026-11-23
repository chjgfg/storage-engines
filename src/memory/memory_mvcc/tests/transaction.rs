// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn memory_owned_write_retains_registry_key_buffer() {
    for page in [false, true] {
        let db = MVCC::new();
        let tx = db.begin_transaction();
        let key = b"owned".to_vec();
        let ptr = key.as_ptr();
        {
            let mut reg = tx.core.lock_registry();
            let mut store = db.store.lock().unwrap();
            if page {
                tx.core
                    .try_write_owned_page(
                        &mut reg,
                        &mut *store,
                        vec![(key, Some(vec![3]))].into_iter(),
                    )
                    .unwrap();
            } else {
                tx.core
                    .try_write_owned(&mut reg, &mut *store, key, Some(vec![3]))
                    .unwrap();
            }
            assert_eq!(
                reg[&tx.core.version]
                    .get(b"owned".as_slice())
                    .unwrap()
                    .as_ptr(),
                ptr
            );
        }
        tx.rollback();
        assert_eq!(db.health().total_rows, 0);
    }
}

#[test]
fn memory_owned_write_keeps_conflict_and_closed_checks() {
    let db = MVCC::new();
    let older = db.begin_transaction();
    let newer = db.begin_transaction();
    assert!(newer.set_raw(b"k", vec![7]));
    newer.commit();
    {
        let mut reg = older.core.lock_registry();
        let mut store = db.store.lock().unwrap();
        assert_eq!(
            older
                .core
                .try_write_owned(&mut reg, &mut *store, b"k".to_vec(), Some(vec![8])),
            Err(crate::WriteError::Conflict)
        );
        assert_eq!(
            older.core.try_write_owned_page(
                &mut reg,
                &mut *store,
                vec![(b"k".to_vec(), Some(vec![8]))].into_iter()
            ),
            Err(crate::WriteError::Conflict)
        );
    }
    older.rollback();
    {
        let mut reg = older.core.lock_registry();
        let mut store = db.store.lock().unwrap();
        assert_eq!(
            older
                .core
                .try_write_owned(&mut reg, &mut *store, b"new".to_vec(), None),
            Err(crate::WriteError::TransactionClosed)
        );
        assert_eq!(
            older
                .core
                .try_write_owned_page(&mut reg, &mut *store, std::iter::empty()),
            Err(crate::WriteError::TransactionClosed)
        );
    }
    assert_eq!(db.begin_transaction().get(b"k"), Some(vec![7]));
}

#[test]
fn test_write_write_conflict() {
    let mvcc = MVCC::new();
    let t1 = mvcc.begin_transaction();
    let t2 = mvcc.begin_transaction();
    assert!(t1.set(b"k", b"v1".to_vec()));
    assert!(!t2.set(b"k", b"v2".to_vec()));
    t1.commit();
    t2.rollback();
}

#[test]
fn test_rollback_discards_writes() {
    let mvcc = MVCC::new();
    let t1 = mvcc.begin_transaction();
    assert!(t1.set(b"x", b"1".to_vec()));
    t1.rollback();
    let t2 = mvcc.begin_transaction();
    assert_eq!(t2.get(b"x"), None);
    t2.commit();
}

#[test]
fn test_self_write_visible() {
    let mvcc = MVCC::new();
    let tx = mvcc.begin_transaction();
    assert!(tx.set(b"k", b"v".to_vec()));
    assert_eq!(tx.get(b"k"), Some(b"v".to_vec()));
    tx.commit();
}

#[test]
fn test_conflict_after_other_commits() {
    let mvcc = MVCC::new();
    let t1 = mvcc.begin_transaction();
    let t2 = mvcc.begin_transaction();
    assert!(t1.set(b"k", b"1".to_vec()));
    t1.commit();
    assert!(!t2.set(b"k", b"2".to_vec()));
    t2.rollback();
    let t3 = mvcc.begin_transaction();
    assert_eq!(t3.get(b"k"), Some(b"1".to_vec()));
    t3.commit();
}

#[test]
fn test_bulk_load() {
    let mvcc = MVCC::new();
    {
        let mut bulk = mvcc.begin_bulk();
        bulk.put(b"a", b"1".to_vec());
        bulk.put(b"b", b"2".to_vec());
        bulk.put_batch(&[(b"c".to_vec(), b"3".to_vec())]);
        assert_eq!(bulk.get(b"a"), Some(b"1".to_vec()));
        bulk.finish();
    }
    let tx = mvcc.begin_transaction();
    assert_eq!(tx.get(b"a"), Some(b"1".to_vec()));
    assert_eq!(tx.get(b"b"), Some(b"2".to_vec()));
    assert_eq!(tx.get(b"c"), Some(b"3".to_vec()));
    tx.commit();
}
