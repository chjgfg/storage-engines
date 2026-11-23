// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn test_snapshot_hides_newer_seq() {
    let path = tmp("snap");
    let mut b = SstBuilder::new(1, &path);
    b.add(b"k".to_vec(), StoredEntry::put(5, Some(b"old".to_vec())));
    b.add(b"k".to_vec(), StoredEntry::put(10, Some(b"new".to_vec())));
    let sst = b.finish().unwrap();
    assert_eq!(
        sst.get_at(b"k", 7).map(|e| e.value),
        Some(EntryValue::Put(Some(b"old".to_vec())))
    );
    assert_eq!(
        sst.get_at(b"k", 10).map(|e| e.value),
        Some(EntryValue::Put(Some(b"new".to_vec())))
    );
    let _ = std::fs::remove_file(&path);
}
