// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn test_prefix_upper_bound() {
    assert_eq!(prefix_upper_bound(b"ab"), Some(b"ac".to_vec()));
    assert_eq!(prefix_upper_bound(b"a\xff"), Some(b"b".to_vec()));
}
