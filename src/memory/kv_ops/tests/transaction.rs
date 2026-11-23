// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn test_batch_and_multi_get() {
    let mvcc = MVCC::new();
    let tx = mvcc.begin_transaction();
    let items = vec![
        (b"a".to_vec(), b"1".to_vec()),
        (b"b".to_vec(), b"2".to_vec()),
        (b"c".to_vec(), b"3".to_vec()),
    ];
    assert!(tx.batch_set(&items).is_ok());
    assert!(tx.batch_delete(&[b"b".to_vec()]).is_ok());
    let got = tx.multi_get(&[b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    assert_eq!(got[0], Some(b"1".to_vec()));
    assert_eq!(got[1], None);
    assert_eq!(got[2], Some(b"3".to_vec()));
    tx.commit();
}

#[test]
fn test_batch_conflict_rolls_back_batch() {
    let mvcc = MVCC::new();
    let t1 = mvcc.begin_transaction();
    let t2 = mvcc.begin_transaction();
    assert!(t1.set(b"k", pack_plain(b"1")));
    t1.commit();
    let items = vec![
        (b"a".to_vec(), b"1".to_vec()),
        (b"k".to_vec(), b"2".to_vec()),
    ];
    let err = t2.batch_set(&items);
    assert!(err.is_err());
    assert_eq!(t2.get(b"a"), None);
    t2.rollback();
}
