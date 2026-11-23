// 该文件集中验证对应职责，保留原测试名称和字面值。

/// 门面必须让上层**不看引擎源码**就能问出「最长版本链有多长」。
///
/// 换引擎只是换了涨速（LSM 一天、Bitcask 一个多月），盯的指标是同一个 ——
/// 所以四家在这一组断言上必须给出完全相同的读数，否则跨引擎对比没有意义。
#[test]
fn all_engines_report_the_same_health() {
    const CHAIN: u64 = 100;

    let engines = open_all();
    let mut dirs = Vec::new();
    for (name, db, dir) in engines {
        dirs.push(dir);
        for i in 0..CHAIN {
            let tx = db.begin();
            assert!(
                tx.set(b"hot", format!("v{i}").into_bytes()),
                "{name} 第 {i} 次写入被拒"
            );
            tx.commit();
        }
        for k in 0..2u64 {
            let tx = db.begin();
            assert!(tx.set(format!("other{k}").as_bytes(), b"x".to_vec()));
            tx.commit();
        }

        let h = db.health();
        assert_eq!(h.live_keys, 3, "{name}: 逻辑 key 数");
        assert_eq!(h.total_rows, CHAIN + 2, "{name}: 物理行数（含历史版本）");
        assert_eq!(h.max_chain, CHAIN, "{name}: 最长版本链应由库自己报出来");
        assert_eq!(h.rows_above_one, CHAIN - 1, "{name}: 多余行数");
        assert_eq!(h.active_txns, 0, "{name}: 无活跃事务");

        db.vacuum().expect("vacuum");
        let after = db.health();
        assert_eq!(after.max_chain, 1, "{name}: vacuum 后最长链应降到 1");
        assert_eq!(after.total_rows, 3, "{name}: vacuum 后应只剩每 key 一行");

        drop(db);
    }
    cleanup(dirs);
}
