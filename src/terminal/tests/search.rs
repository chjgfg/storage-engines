// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn mset_mget_count_scan_and_prefix() {
    let db = mem();
    let mut txn = None;
    let o = outs(&*db, &mut txn, "mset u:01 a u:02 b u:03 c");
    assert!(o[0].ok && o[0].message.contains("3 键"), "{o:?}");
    // 偶数个参数校验
    let o = outs(&*db, &mut txn, "mset lonely");
    assert!(!o[0].ok && o[0].message.contains("偶数"), "{o:?}");
    let o = outs(&*db, &mut txn, "mget u:01 ghost u:02");
    let t = table(&o, 0, 3);
    assert_eq!(t.rows[1], vec!["ghost", "(nil)"]);
    let o = outs(&*db, &mut txn, "count u:");
    assert!(o[0].message.contains("= 3"), "{o:?}");
    let o = outs(&*db, &mut txn, "count");
    assert!(o[0].message.contains("= 3"), "{o:?}");
    let o = outs(&*db, &mut txn, "prefix u:");
    assert_eq!(table(&o, 0, 3).rows[0][0], "u:01");
    let o = outs(&*db, &mut txn, "scan u:02");
    assert_eq!(table(&o, 0, 2).rows[0][0], "u:02", "scan START 应含 START");
    let o = outs(&*db, &mut txn, "rscan u:02");
    assert_eq!(table(&o, 0, 2).rows[0][0], "u:03", "rscan START 倒着走");
    // seek / seekprev 的边界语义
    let o = outs(&*db, &mut txn, "seek u:02x");
    assert_eq!(table(&o, 0, 1).rows[0][0], "u:03", "seek = 第一条 ≥ K");
    let o = outs(&*db, &mut txn, "seekprev u:02x");
    assert_eq!(
        table(&o, 0, 1).rows[0][0],
        "u:02",
        "seekprev = 最后一条 ≤ K"
    );
    let o = outs(&*db, &mut txn, "seek zz");
    assert!(o[0].message == "(nil)", "{o:?}");
}

#[test]
fn scan_caps_display_at_100_rows() {
    let db = mem();
    // 直接走门面灌 150 行（一条 mset 命令行也行，但 300 个 token 的断言没必要）
    let items: Vec<(Vec<u8>, Vec<u8>)> = (0..150)
        .map(|i| (format!("big-{i:03}").into_bytes(), b"v".to_vec()))
        .collect();
    let tx = db.begin();
    tx.batch_set(&items).unwrap();
    tx.commit();
    let mut txn = None;
    let o = outs(&*db, &mut txn, "scan");
    let t = o[0].table.as_ref().expect("scan 应有表");
    assert_eq!(t.rows.len(), SCAN_CAP, "显示要封顶");
    assert!(
        o[0].message.contains("150 行") && o[0].message.contains("只显示前 100"),
        "{:?}",
        o[0].message
    );
}
