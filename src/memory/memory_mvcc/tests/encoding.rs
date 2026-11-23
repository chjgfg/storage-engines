// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn memory_owned_write_retains_encoded_key_buffer() {
    let mut store = MemStore::new();
    let key = encode_key(b"owned", 1);
    let ptr = key.as_ptr();
    assert!(store.put_versioned_owned(key, Some(vec![3])));
    assert_eq!(store.iter_keys_in(None, None).next().unwrap().as_ptr(), ptr);
}

#[test]
fn test_encode_order() {
    let k1 = encode_key(b"user", 1);
    let k2 = encode_key(b"user", 2);
    let k10 = encode_key(b"user", 10);
    assert!(k1 < k2);
    assert!(k2 < k10);
    let d = decode_key(&k10).unwrap();
    assert_eq!(d.raw_key, b"user");
    assert_eq!(d.version, 10);
}
