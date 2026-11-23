// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

fn mem() -> Box<dyn KvEngine> {
    open_engine("memory", None).expect("memory 引擎应能无路径打开")
}

/// 跑一行、拿输出（Switch / Exit 都折成空 —— 需要细分时直接调 run_line）
fn outs<'e>(db: &'e dyn KvEngine, txn: &mut Option<Box<dyn KvTxn + 'e>>, line: &str) -> Vec<Out> {
    match run_line(db, txn, line) {
        LineOutcome::Outputs(v) => v,
        LineOutcome::Switch { .. } | LineOutcome::Exit => Vec::new(),
    }
}

/// 取第 i 条输出的表（没有表 / 行数不对都当场红）
fn table(o: &[Out], i: usize, rows: usize) -> &Table {
    let t = o[i]
        .table
        .as_ref()
        .unwrap_or_else(|| panic!("第 {i} 条输出应有表: {:?}", o[i]));
    assert_eq!(t.rows.len(), rows, "第 {i} 条输出的行数不对: {:?}", t);
    &t
}
