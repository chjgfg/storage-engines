// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn test_basic_set_get_delete() {
    let mvcc = MVCC::new();
    let tx = mvcc.begin_transaction();
    assert!(tx.set(b"a", b"1".to_vec()));
    assert_eq!(tx.get(b"a"), Some(b"1".to_vec()));
    assert!(tx.set(b"a", b"2".to_vec()));
    assert_eq!(tx.get(b"a"), Some(b"2".to_vec()));
    assert!(tx.delete(b"a"));
    assert_eq!(tx.get(b"a"), None);
    tx.commit();
}

/// 探针：一次点读 / 一次点写访问了多少行。
///
/// 断言的是**代价不随链长增长**：版本链的链尾就是最新版本，往回一步就该停。
/// 内存引擎没有磁盘读，所以代价的对应物是「扫过多少行」。
/// 修复前这里是 50 vs 2000（严格 1:1 线性）。
#[test]
fn test_point_op_cost_independent_of_chain_length() {
    fn probe(chain_len: u64) -> (u64, u64) {
        let mvcc = MVCC::new();
        for i in 0..chain_len {
            let tx = mvcc.begin_transaction();
            assert!(
                tx.set(b"hot", format!("v{i}").into_bytes()),
                "第 {i} 次写入被拒"
            );
            tx.commit();
        }

        let tx = mvcc.begin_transaction();
        let before = crate::memory::mem_store::rows_touched();
        assert_eq!(
            tx.get(b"hot"),
            Some(format!("v{}", chain_len - 1).into_bytes())
        );
        let get_rows = crate::memory::mem_store::rows_touched() - before;

        let before = crate::memory::mem_store::rows_touched();
        assert!(tx.set(b"hot", b"newer".to_vec()));
        let set_rows = crate::memory::mem_store::rows_touched() - before;
        tx.rollback();
        (get_rows, set_rows)
    }

    let (g_short, s_short) = probe(50);
    let (g_long, s_long) = probe(2000);
    eprintln!(
        "点读访问行数：链长 50 → {g_short} 行，链长 2000 → {g_long} 行\n\
             点写访问行数：链长 50 → {s_short} 行，链长 2000 → {s_long} 行"
    );
    assert_eq!(g_short, g_long, "点读访问的行数不应随版本链长度增长");
    assert_eq!(
        s_short, s_long,
        "点写访问的行数不应随版本链长度增长（它只需要版本号）"
    );
}

#[test]
fn test_print_all_latest_visible() {
    let mvcc = MVCC::new();
    let t1 = mvcc.begin_transaction();
    assert!(t1.set(b"a", b"1".to_vec()));
    assert!(t1.set(b"b", b"2".to_vec()));
    assert!(t1.set(b"a", b"3".to_vec()));
    let all = t1.print_all();
    assert_eq!(all.get(&b"a".to_vec()).unwrap(), &Some(b"3".to_vec()));
    assert_eq!(all.get(&b"b".to_vec()).unwrap(), &Some(b"2".to_vec()));
    t1.commit();
}

#[test]
fn test_instances_isolated() {
    let m1 = MVCC::new();
    let m2 = MVCC::new();
    let t1 = m1.begin_transaction();
    let t2 = m2.begin_transaction();
    assert!(t1.set(b"k", b"1".to_vec()));
    assert!(t2.set(b"k", b"2".to_vec()));
    assert_eq!(t1.get(b"k"), Some(b"1".to_vec()));
    assert_eq!(t2.get(b"k"), Some(b"2".to_vec()));
    t1.commit();
    t2.commit();
}

/// 库必须能自己报出「最长版本链」。
///
/// 这次事故的核心数字（某把热点 key 堆了多少个版本）一直是靠数日志行数得出来的 ——
/// 一个当基座用的库不该靠这个诊断自己。四引擎共用同一份 `MvccHealth`，
/// 所以换引擎时这两组断言必须照样成立（涨速不同，指标是同一个）。
#[test]
fn test_health_reports_chain_length() {
    const CHAIN: u64 = 500;

    let mvcc = MVCC::new();
    for i in 0..CHAIN {
        let tx = mvcc.begin_transaction();
        assert!(tx.set(b"hot", format!("v{i}").into_bytes()));
        tx.commit();
    }
    for k in 0..3u64 {
        let key = format!("other{k}");
        let tx = mvcc.begin_transaction();
        assert!(tx.set(key.as_bytes(), b"x".to_vec()));
        tx.commit();
    }

    let h = mvcc.health();
    assert_eq!(h.live_keys, 4, "逻辑 key 数");
    assert_eq!(h.total_rows, CHAIN + 3, "物理行数（含历史版本）");
    assert_eq!(h.max_chain, CHAIN, "最长版本链应由库自己报出来");
    assert_eq!(h.rows_above_one, CHAIN - 1);
    assert_eq!(h.active_txns, 0);

    let stats = mvcc.vacuum();
    assert!(!stats.skipped_active_txn, "无活跃事务时不该跳过");
    let after = mvcc.health();
    assert_eq!(after.max_chain, 1, "vacuum 后最长链应降到 1");
    assert_eq!(after.total_rows, 4, "vacuum 后应只剩每 key 一行");
    assert_eq!(after.live_keys, 4);
    assert_eq!(after.rows_above_one, 0);
}

/// `health()` 是管理面调用，但它也不该顺手把每行 value 克隆一遍 ——
/// 否则「采样一下链长」这件事本身就会在热点库上制造一次内存峰值。
#[test]
fn test_health_does_not_clone_values() {
    const CHAIN: u64 = 400;

    let mvcc = MVCC::new();
    for _ in 0..CHAIN {
        let tx = mvcc.begin_transaction();
        assert!(tx.set(b"hot", vec![0u8; 1024]));
        tx.commit();
    }

    let before = crate::memory::mem_store::value_clones();
    let h = mvcc.health();
    let clones = crate::memory::mem_store::value_clones() - before;
    assert_eq!(h.max_chain, CHAIN);
    assert_eq!(
        clones, 0,
        "health 期间 value 克隆次数：{clones}（历史行数 {CHAIN}）"
    );
}
