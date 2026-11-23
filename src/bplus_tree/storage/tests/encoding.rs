// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn test_crc32_known() {
    // "123456789" 的 IEEE CRC32 = 0xCBF43926
    assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
}

#[test]
fn test_crc_detects_corruption() {
    let mut page = pack_page(1, b"data");
    page[100] ^= 0xFF; // 破坏 payload
    assert!(unpack_page(&page).is_err());
}
