// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn paged_file_reads_and_writes_whole_blocks() {
    const BS: usize = 64;
    let path = std::env::temp_dir().join(format!("se_disk_blk_{}.bin", std::process::id()));
    {
        let mut f = DiskFile::open_paged(&path, BS).unwrap();
        // 没写过的槽位 = 全零
        assert_eq!(f.read_page(3).unwrap(), vec![0u8; BS]);
        f.write_page(1, &vec![7u8; BS]).unwrap();
        assert_eq!(f.read_page(1).unwrap(), vec![7u8; BS]);
        assert_eq!(f.len().unwrap(), 2 * BS as u64);
        // 长度不对的写入必须被挡住
        assert!(f.write_page(0, &[0u8; BS - 1]).is_err());
    }
    let _ = std::fs::remove_file(&path);
}
