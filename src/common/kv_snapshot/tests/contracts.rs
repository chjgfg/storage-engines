// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn test_bin_roundtrip() {
    let path = std::env::temp_dir().join(format!("kv_snap_{}.bin", std::process::id()));
    let recs = vec![
        KvRecord::new(b"a".to_vec(), Some(b"1".to_vec())),
        KvRecord::new(b"b".to_vec(), None),
    ];
    write_bin(&path, &recs).unwrap();
    let (back, st) = read_bin(&path).unwrap();
    assert_eq!(st.live, 1);
    assert_eq!(st.deleted, 1);
    assert_eq!(back, recs);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_bin_v2_roundtrip() {
    let path = std::env::temp_dir().join(format!("kv_snap_v2_{}.bin", std::process::id()));
    let recs = vec![
        KvRecord::with_seq(b"k".to_vec(), Some(b"v".to_vec()), 42),
        KvRecord::with_seq(b"d".to_vec(), None, 99),
    ];
    write_bin(&path, &recs).unwrap();
    let (back, _) = read_bin(&path).unwrap();
    assert_eq!(back, recs);
    // v2 每条多 8 字节 seq；v1 不写
    assert_eq!(back[0].seq, Some(42));
    let _ = std::fs::remove_file(&path);
}

/// 不带 seq 的导出与旧版 BPEXP001 逐字节一致
#[test]
fn test_plain_is_bpexp001() {
    let path = std::env::temp_dir().join(format!("kv_snap_plain_{}.bin", std::process::id()));
    let recs = vec![KvRecord::new(b"k".to_vec(), Some(b"v".to_vec()))];
    write_bin(&path, &recs).unwrap();
    let data = std::fs::read(&path).unwrap();
    assert_eq!(&data[..8], b"BPEXP001");
    let _ = std::fs::remove_file(&path);
}
