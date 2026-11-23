// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn tokenize_keeps_quoted_whitespace_and_doubled_quotes() {
    assert_eq!(
        tokenize("set k \"a b\"  'c  d'"),
        vec!["set", "k", "a b", "c  d"]
    );
    // '' / "" 成对引号 = 字面引号，与引擎词法器同一套
    assert_eq!(
        tokenize(r#"set k "he said ""hi"""#),
        vec!["set", "k", "he said \"hi\""]
    );
    assert_eq!(tokenize("set k 'a''b'"), vec!["set", "k", "a'b"]);
    // 空行 / 全空白 → 没有 token
    assert!(tokenize("   ").is_empty());
    // 引号中段出现也照切（终端输入不追求 SQL 词法器的严格）
    assert_eq!(tokenize("ab\"cd ef\""), vec!["abcd ef"]);
}

#[test]
fn set_get_roundtrip_and_nil() {
    let db = mem();
    let mut txn = None;
    let o = outs(&*db, &mut txn, "set name Alice");
    assert!(o[0].ok && o[0].message == "OK", "{o:?}");
    let o = outs(&*db, &mut txn, "get name");
    let t = table(&o, 0, 1);
    assert_eq!(t.rows[0], vec!["name", "Alice"]);
    // 不存在的键 → (nil)，不报错
    let o = outs(&*db, &mut txn, "get ghost");
    assert!(o[0].ok && o[0].message == "(nil)", "{o:?}");
    // 值里的空格要用引号包；非法 UTF-8 显示 <hex>
    let o = outs(&*db, &mut txn, "set city \"new york\"");
    assert!(o[0].ok, "{o:?}");
    let o = outs(&*db, &mut txn, "get city");
    assert_eq!(table(&o, 0, 1).rows[0][1], "new york");
}

#[test]
fn arity_errors_carry_usage() {
    let db = mem();
    let mut txn = None;
    let o = outs(&*db, &mut txn, "get");
    assert!(!o[0].ok && o[0].message.contains("get K"), "{o:?}");
    let o = outs(&*db, &mut txn, "set onlykey");
    assert!(!o[0].ok && o[0].message.contains("set K V"), "{o:?}");
    let o = outs(&*db, &mut txn, "nosuchcmd 1");
    assert!(!o[0].ok && o[0].message.contains("未知命令"), "{o:?}");
}

#[test]
fn incr_needs_integer_value() {
    let db = mem();
    let mut txn = None;
    outs(&*db, &mut txn, "set visits 10");
    let o = outs(&*db, &mut txn, "incr visits 5");
    assert!(o[0].ok && o[0].message == "visits = 15", "{o:?}");
    let o = outs(&*db, &mut txn, "decr visits");
    assert!(o[0].ok && o[0].message == "visits = 14", "{o:?}");
    // 默认步长 1、负步长也行
    let o = outs(&*db, &mut txn, "incr visits -4");
    assert!(o[0].ok && o[0].message == "visits = 10", "{o:?}");
    outs(&*db, &mut txn, "set name Alice");
    let o = outs(&*db, &mut txn, "incr name");
    assert!(!o[0].ok && o[0].message.contains("不是十进制整数"), "{o:?}");
}

#[test]
fn find_paginates_with_total_and_next_hint() {
    let db = mem();
    let mut txn = None;
    let mut pairs = String::new();
    for i in 0..25 {
        pairs.push_str(&format!("u:{i:02} v{i:02} "));
    }
    let o = outs(&*db, &mut txn, &format!("mset {pairs}"));
    assert!(o[0].ok, "{o:?}");
    let o = outs(&*db, &mut txn, "find u:");
    let t = table(&o, 0, 20);
    assert_eq!(t.rows[0][0], "u:00");
    assert!(
        o[0].message.contains("全库匹配 25") && o[0].message.contains("下一页: find u: 1"),
        "{:?}",
        o[0].message
    );
    // 翻到尾页：5 行、没有下一页提示
    let o = outs(&*db, &mut txn, "find u: 1");
    assert_eq!(table(&o, 0, 5).rows[0][0], "u:20");
    assert!(!o[0].message.contains("下一页"), "{:?}", o[0].message);
}
