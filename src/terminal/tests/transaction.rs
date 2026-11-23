// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn checked_terminal_commit_failures_are_errors() {
    for explicit in [false, true] {
        let dir = crate::common::ScratchPath::new("terminal_commit", "bitcask", "");
        let db = crate::open_engine("bitcask", Some(dir.as_ref())).unwrap();
        let mut txn = None;
        if explicit {
            assert!(outs(&*db, &mut txn, "begin")[0].ok);
            assert!(outs(&*db, &mut txn, "set k v")[0].ok);
        }
        crate::common::wal::fail_next_commit(1);
        let command = if explicit { "commit" } else { "set k v" };
        let result = outs(&*db, &mut txn, command);
        assert!(
            !result[0].ok && result[0].message.contains("indeterminate"),
            "{result:?}"
        );
        assert!(txn.is_none());
        let read = db.begin();
        assert_eq!(read.get(b"k"), None);
    }
}

#[test]
fn txn_begin_commit_and_rollback_roundtrip() {
    let db = mem();
    let mut txn = None;
    // commit 没开事务要红
    let o = outs(&*db, &mut txn, "commit");
    assert!(!o[0].ok && o[0].message.contains("没有开着的事务"), "{o:?}");
    // 开事务后命令都走它；自己写自己读可见
    let o = outs(&*db, &mut txn, "begin");
    assert!(
        o[0].ok && o[0].message.starts_with("BEGIN (version "),
        "{o:?}"
    );
    let o = outs(&*db, &mut txn, "begin");
    assert!(!o[0].ok && o[0].message.contains("已有未提交事务"), "{o:?}");
    let o = outs(&*db, &mut txn, "set txkey txval");
    assert!(o[0].ok, "{o:?}");
    let o = outs(&*db, &mut txn, "get txkey");
    assert_eq!(table(&o, 0, 1).rows[0][1], "txval");
    // rollback 丢弃；挂起的事务也让 vacuum 不敢动
    let o = outs(&*db, &mut txn, "rollback");
    assert!(o[0].ok && o[0].message.starts_with("ROLLBACK"), "{o:?}");
    let o = outs(&*db, &mut txn, "get txkey");
    assert!(o[0].message == "(nil)", "rollback 后不该读到: {o:?}");
    // commit 落地
    let _ = outs(&*db, &mut txn, "begin");
    let _ = outs(&*db, &mut txn, "set txkey2 v2");
    let o = outs(&*db, &mut txn, "commit");
    assert!(o[0].ok && o[0].message.starts_with("COMMIT"), "{o:?}");
    let o = outs(&*db, &mut txn, "get txkey2");
    assert_eq!(table(&o, 0, 1).rows[0][1], "v2");
}

#[test]
fn engine_switch_blocked_by_pending_txn_and_parses_names() {
    let db = mem();
    let mut txn = None;
    // 无参 = 显示当前
    let o = outs(&*db, &mut txn, "engine");
    assert!(o[0].ok && o[0].message.contains("memory"), "{o:?}");
    // 未知引擎
    let o = match run_line(&*db, &mut txn, "engine nosuch") {
        LineOutcome::Outputs(v) => v,
        other => panic!("应为输出: {other:?}"),
    };
    assert!(!o[0].ok && o[0].message.contains("未知引擎"), "{o:?}");
    // 有挂起事务 → 拒绝切换（事务借用引擎）
    let _ = outs(&*db, &mut txn, "begin");
    let o = outs(&*db, &mut txn, "engine lsm");
    assert!(!o[0].ok && o[0].message.contains("未提交事务"), "{o:?}");
    let _ = outs(&*db, &mut txn, "commit");
    // 干净时 → Switch 结果带规范名；别名也认
    match run_line(&*db, &mut txn, "engine lsm") {
        LineOutcome::Switch { kind, dir } => {
            assert_eq!(kind, "lsm_tree");
            assert!(dir.is_none(), "没给目录就交给外壳按默认来");
        }
        other => panic!("应为 Switch: {other:?}"),
    }
    match run_line(&*db, &mut txn, "engine bplus ./my-db") {
        LineOutcome::Switch { kind, dir } => {
            assert_eq!(kind, "bplus_tree");
            assert_eq!(dir, Some(PathBuf::from("./my-db")));
        }
        other => panic!("应为 Switch: {other:?}"),
    }
}
