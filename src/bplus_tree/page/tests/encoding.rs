// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn test_crc_on_disk() {
    let path = temp_db("crc");
    {
        let mut pm = PageManager::open(&path, 4).unwrap();
        let p = pm.allocate_page();
        pm.write_page(p, b"crc-test");
        pm.flush_all();
    }
    // 直接读文件，确认页头有 BPG1 魔数
    let raw = std::fs::read(&path).unwrap();
    // page 1 在 offset 4096
    assert_eq!(&raw[4096..4100], b"BPG1");
    cleanup(&path);
}
