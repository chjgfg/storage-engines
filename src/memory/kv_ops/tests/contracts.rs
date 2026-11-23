// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn test_prefix_upper_bound() {
    assert_eq!(prefix_upper_bound(b"ab"), Some(b"ac".to_vec()));
    assert_eq!(prefix_upper_bound(b"a\xff"), Some(b"b".to_vec()));
    assert_eq!(prefix_upper_bound(b"\xff\xff"), None);
}

#[test]
fn test_key_matches_helpers() {
    assert!(key_matches(b"hello", b"ell", KeyMatchMode::Contains));
    assert!(!key_matches(b"hello", b"xyz", KeyMatchMode::Contains));
    assert!(key_matches(b"hello", b"hel", KeyMatchMode::Prefix));
    assert!(!key_matches(b"hello", b"ello", KeyMatchMode::Prefix));
    assert!(key_matches(b"any", b"", KeyMatchMode::Contains));
    assert!(key_matches(b"any", b"", KeyMatchMode::Prefix));
}
