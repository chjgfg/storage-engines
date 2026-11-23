// 该文件集中验证对应职责，保留原测试名称和字面值。

/// 单行装不进一页时，事务层必须**在写 WAL 之前**就拒掉：`set` 返回 `false`、WAL 里
/// 不许留下这条 Write、重开之后也不许出现"WAL 说有、树里查无此键"。
///
/// 为什么单独盯 WAL 这一半：`insert` 拒收只解决"别写坏已有数据"，而事务层是
/// 先 append WAL 再写树 —— 顺序错了就会留下一条永久无法重放的提交记录。
#[test]
fn test_oversized_row_is_refused_before_the_wal() {
    let path = tmp_db("oversize");
    let big_key = vec![b'K'; 5000]; // 5 KB 键 + 8 字节版本后缀 > 一页载荷 4080 B
    {
        let mvcc = MVCC::open(&path, 64, 512);
        let tx = mvcc.begin_transaction();
        assert!(tx.set(b"small", vec![b'v'; 60]), "正常行要写得进去");
        tx.commit();

        let tx = mvcc.begin_transaction();
        assert!(
            !tx.set(&big_key, vec![b'v'; 8]),
            "装不下的行必须报 false（这里不是写冲突，是引擎拒收）"
        );
        assert!(tx.get(&big_key).is_none());
        tx.commit();

        // 判据要精确到**记录类型**：被拒的那行不许留下 `Write`（带键的记录只有 `Write` 一种，
        // 所以"容量判定必须在落 WAL 之前"这条顺序就由下面这两条计数守着 —— M25 之前还多一条
        // `Begin` 可以数，现在它整个不再落盘）。
        let mut wal = Wal::open(wal_path(&path)).expect("打开 WAL 读记录");
        let records = wal.read_all().expect("读 WAL");
        let writes_for = |prefix: &[u8]| {
            records
                .iter()
                .filter(|e| {
                    matches!(&e.record, WalRecord::Write { key, .. }
                        if key.starts_with(prefix))
                })
                .count()
        };
        assert_eq!(
            writes_for(&big_key),
            0,
            "拒收的一行不许在 WAL 里留下 Write 记录"
        );
        // 对照：同一把尺子量得到"真写进去的那行"，否则上面那条断言是空转
        assert_eq!(writes_for(b"small"), 1, "探针必须能数到一条真实写入");
        // ⚠️ M18e 时这里断的是 `begins == 1`（"拒收的那次连 `Begin` 都不留"，用来钉
        // `row_fits` 必须在落 WAL **之前**）。M25 起写侧一个字 `Begin` 都不落，那条断言
        // 会退化成"常数等于 0"而看不见顺序 —— 顺序现在由上面那两条 `Write` 计数守着
        // （带键的记录只有 `Write` 一种，`row_fits` 一旦挪到落 WAL 之后，`writes_for(big_key)`
        // 立刻变 1）。下面这条只守新不变量："写侧不再产生 `Begin`"。
        let begins = records
            .iter()
            .filter(|e| matches!(&e.record, WalRecord::Begin { .. }))
            .count();
        assert_eq!(begins, 0, "写侧不再落 Begin，实际留下 {begins} 条");
        drop(mvcc);
    }
    // 重开：小行还在，大行不在，恢复流程也没被那条不存在的记录卡住
    let mvcc = MVCC::open(&path, 64, 512);
    let tx = mvcc.begin_transaction();
    assert!(tx.get(b"small").is_some(), "拒收一行不许牵连已有的行");
    assert!(tx.get(&big_key).is_none());
    tx.commit();
    drop(mvcc);
}
