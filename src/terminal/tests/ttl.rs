// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn meta_ttl_persist_and_exit() {
    let db = mem();
    let mut txn = None;
    let _ = outs(&*db, &mut txn, "setex temp 60 hot");
    let o = outs(&*db, &mut txn, "ttl temp");
    assert!(o[0].ok && o[0].message.contains("剩 60s"), "{o:?}");
    let o = outs(&*db, &mut txn, "meta temp");
    let t = table(&o, 0, 6);
    assert_eq!(t.rows[4][1], "false", "没到点不算过期");
    let o = outs(&*db, &mut txn, "refresh temp 90");
    assert!(o[0].ok, "{o:?}");
    let o = outs(&*db, &mut txn, "ttl temp");
    assert!(o[0].message.contains("剩 90s"), "{o:?}");
    let o = outs(&*db, &mut txn, "persist temp");
    assert!(o[0].ok, "{o:?}");
    let o = outs(&*db, &mut txn, "ttl temp");
    assert!(o[0].message.contains("永久"), "{o:?}");
    let o = outs(&*db, &mut txn, "ttl ghost");
    assert!(o[0].message.contains("(nil)"), "{o:?}");
    // exit / quit → Exit
    assert!(matches!(
        run_line(&*db, &mut txn, "exit"),
        LineOutcome::Exit
    ));
    assert!(matches!(run_line(&*db, &mut txn, "q"), LineOutcome::Exit));
    // 空行 → 空输出
    assert!(outs(&*db, &mut txn, "   ").is_empty());
}
