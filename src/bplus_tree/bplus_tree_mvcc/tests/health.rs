// 该文件集中验证对应职责，保留原测试名称和字面值。

/// 库必须能自己报出「最长版本链」。
///
/// 这次事故的核心数字（某把热点 key 堆了多少个版本）一直是靠数日志行数得出来的 ——
/// 一个当基座用的库不该靠这个诊断自己。四引擎共用同一份 `MvccHealth`，
/// 所以换引擎时这两组断言必须照样成立（涨速不同，指标是同一个）。
#[test]
fn test_health_reports_chain_length() {
    const CHAIN: u64 = 500;

    let path = tmp_db("health");
    {
        let mvcc = MVCC::open(&path, 16, 32);
        for i in 0..CHAIN {
            let tx = mvcc.begin_transaction();
            assert!(
                tx.set(b"hot", format!("v{i}").into_bytes()),
                "第 {i} 次写入被拒"
            );
            tx.commit();
        }
        for k in 0..3u64 {
            let key = format!("other{k}");
            let tx = mvcc.begin_transaction();
            assert!(tx.set(key.as_bytes(), b"x".to_vec()));
            tx.commit();
        }

        let base = crate::bplus_tree::page::value_clones();
        let h = mvcc.health();
        let clones = crate::bplus_tree::page::value_clones() - base;
        assert_eq!(h.live_keys, 4, "逻辑 key 数");
        assert_eq!(h.total_rows, CHAIN + 3, "物理行数（含历史版本）");
        assert_eq!(h.max_chain, CHAIN, "最长版本链应由库自己报出来");
        assert_eq!(h.rows_above_one, CHAIN - 1);
        assert_eq!(h.active_txns, 0);
        // 采样链长这件事本身不该制造一次全库 value 拷贝
        assert_eq!(
            clones, 0,
            "health 期间 value 克隆次数：{clones}（历史行数 {}）",
            h.total_rows
        );

        let stats = mvcc.vacuum().unwrap();
        assert!(!stats.skipped_active_txn, "无活跃事务时不该跳过");
        let after = mvcc.health();
        assert_eq!(after.max_chain, 1, "vacuum 后最长链应降到 1");
        assert_eq!(after.total_rows, 4, "vacuum 后应只剩每 key 一行");
        assert_eq!(after.live_keys, 4);
        assert_eq!(after.rows_above_one, 0);
    }
    cleanup(&path);
}
