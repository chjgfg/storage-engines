// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

fn search_seed(rows: usize, bytes: usize) -> MVCC {
    let db = MVCC::new();
    db.set_vacuum_auto_slice(0);
    let tx = db.begin_transaction();
    for i in 0..rows {
        assert!(tx.set_raw(&(i as u64).to_be_bytes(), vec![b'v'; bytes]));
    }
    tx.commit();
    db
}
