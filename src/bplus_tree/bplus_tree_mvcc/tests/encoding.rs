// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

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
