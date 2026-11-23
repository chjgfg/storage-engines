// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn test_batch_and_multi_get() {
    let path = tmp("batch");
    {
        let mvcc = MVCC::open(&path);
        let tx = mvcc.begin_transaction();
        let items = vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
        ];
        tx.batch_set(&items).unwrap();
        let got = tx.multi_get(&[b"a".to_vec(), b"b".to_vec(), b"z".to_vec()]);
        assert_eq!(got[0], Some(b"1".to_vec()));
        assert_eq!(got[1], Some(b"2".to_vec()));
        assert_eq!(got[2], None);
        tx.batch_delete(&[b"b".to_vec()]).unwrap();
        assert!(!tx.exists(b"b"));
        tx.commit();
    }
    cleanup(&path);
}
