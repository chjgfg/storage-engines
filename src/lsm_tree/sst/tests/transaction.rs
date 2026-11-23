// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn test_write_read_get_v2() {
    let path = tmp("basic");
    let mut b = SstBuilder::new(1, &path);
    b.add(b"a".to_vec(), StoredEntry::put(1, Some(b"1".to_vec())));
    b.add(b"b".to_vec(), StoredEntry::put(2, None));
    b.add(b"c".to_vec(), StoredEntry::drop_at(3));
    let sst = b.finish().unwrap();
    assert_eq!(
        sst.get(b"a").map(|e| e.value),
        Some(EntryValue::Put(Some(b"1".to_vec())))
    );
    assert_eq!(sst.get(b"a").unwrap().seq, 1);
    assert_eq!(sst.get(b"b").map(|e| e.value), Some(EntryValue::Put(None)));
    assert_eq!(sst.get(b"c").map(|e| e.value), Some(EntryValue::Drop));
    assert!(sst.is_v2);
    let _ = std::fs::remove_file(&path);
}
