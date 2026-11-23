// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// 与下游那份 `BPEXP001` **逐字节**兼容：手拼一份它的输出，本 crate 要读得一模一样；
/// 反向写出来的字节也必须正好是那个形状（多一个 trailer 下游就读不动）。
#[test]
fn tools_bin_format_matches_downstream_bytes() {
    let r = root("bytes");
    let mut want: Vec<u8> = Vec::new();
    want.extend_from_slice(b"BPEXP001");
    let recs = vec![
        KvRecord::new(b"ka".to_vec(), Some(b"va".to_vec())),
        KvRecord::new(b"kb".to_vec(), None),
    ];
    for rec in &recs {
        want.extend_from_slice(&(rec.key.len() as u32).to_le_bytes());
        want.extend_from_slice(&rec.key);
        match &rec.value {
            Some(v) => {
                want.extend_from_slice(&(v.len() as u32).to_le_bytes());
                want.extend_from_slice(v);
            }
            None => want.extend_from_slice(&u32::MAX.to_le_bytes()),
        }
    }
    let f = r.join("down.bin");
    std::fs::write(&f, &want).unwrap();
    let (back, st) = read_bin(&f).unwrap();
    assert_eq!(back, recs, "读下游那份字节的结果不对");
    assert_eq!((st.records, st.live, st.deleted), (2, 1, 1));
    let out = r.join("ours.bin");
    write_bin(&out, &recs).unwrap();
    assert_eq!(
        std::fs::read(&out).unwrap(),
        want,
        "bin 格式与下游那份不再是同一串字节"
    );
    // 把那份文件灌进引擎：tombstone 那行不能变成一条活键
    import_from_file(EngineKind::Bitcask, &r, &f, SnapshotFormat::Bin, true).unwrap();
    assert_eq!(
        read_back(EngineKind::Bitcask, &r, b"ka").as_deref(),
        Some(&b"va"[..])
    );
    assert_eq!(
        read_back(EngineKind::Bitcask, &r, b"kb"),
        None,
        "tombstone 被灌成活值了"
    );
    let _ = std::fs::remove_dir_all(&r);
}
