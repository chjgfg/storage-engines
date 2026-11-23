// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn vacuum_skips_while_txn_pending() {
    let db = mem();
    let mut txn = None;
    let _ = outs(&*db, &mut txn, "begin");
    let o = outs(&*db, &mut txn, "vacuum");
    assert!(o[0].ok && o[0].message.contains("整轮跳过"), "{o:?}");
    let _ = outs(&*db, &mut txn, "commit");
}
