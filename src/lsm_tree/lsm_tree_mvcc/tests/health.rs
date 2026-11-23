// 该文件集中验证对应职责，保留原测试名称和字面值。

/// 库必须能自己报出「最长版本链」。
///
/// 这次事故的核心数字（某把热点 key 堆了多少个版本）一直是靠数日志行数得出来的 ——
/// 一个当基座用的库不该靠这个诊断自己。四引擎共用同一份 `MvccHealth`，
/// 所以换引擎时这两组断言必须照样成立（涨速不同，指标是同一个）。
///
/// 先 `flush()` 让版本行落在 SST 里：这样走的是 `light_view` + 跨源归并那条路，
/// 而不是全在 memtable 里读内存。
#[test]
fn test_health_reports_chain_length() {
    const CHAIN: u64 = 500;

    let path = tmp_db("health");
    {
        let mvcc = MVCC::open(&path);
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
        mvcc.flush();

        let base = crate::lsm_tree::sst::value_materializations();
        let h = mvcc.health();
        let used = crate::lsm_tree::sst::value_materializations() - base;
        assert_eq!(h.live_keys, 4, "逻辑 key 数");
        assert_eq!(h.total_rows, CHAIN + 3, "物理行数（含历史版本）");
        assert_eq!(h.max_chain, CHAIN, "最长版本链应由库自己报出来");
        assert_eq!(h.rows_above_one, CHAIN - 1);
        assert_eq!(h.active_txns, 0);
        // 采样链长这件事本身不该物化任何 value
        assert_eq!(
            used, 0,
            "health 期间 value 物化次数：{used}（历史行数 {}）",
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

/// `health()` 不许把任何一个 SST 的轻量视图**整篇物化**（M39）。
///
/// 判据：尺子 `light_view_materialized()`，**单位 = 条目数**；一次 `health()` 的差值必须是 0。
/// 为什么钉"路径"而不钉内存：分配计数（`mem_probe`）是**进程级**的，并行用例互相抬高峰值 ⇒
/// 内存读数只能待在 `--test-threads=1` 的台架里（M27 栽过一次，M36 又拿坏量具的读数当过凭据）。
/// 而"有没有走 `SstFile::light_view`"是一条一次就能数出来的路径事实。
/// 内存那笔（M38 修好量具之后）：600k 档 `health()` 瞬时改前 **+126.1 MB**。
///
/// 变异验证：把 `LSMTree::light_iter_range` 里 SST 那几路源临时改成
/// `Box::new(sst.light_view(snap_seq).into_iter())`（旧形状：每个文件整篇折进 `Vec` 再归并）
/// ⇒ 差值 = 全库条目数，红。
#[test]
fn test_health_does_not_materialize_its_sources() {
    const KEYS: u64 = 300;
    let path = tmp_db("health_lazy");
    {
        let mvcc = MVCC::open(&path);
        for i in 0..KEYS {
            let key = format!("k{i:04}");
            let tx = mvcc.begin_transaction();
            assert!(tx.set(key.as_bytes(), b"v".to_vec()), "第 {i} 把键写失败");
            tx.commit();
        }
        // 必须落在 SST 里：全在 memtable 时这条判据是空转
        mvcc.flush();

        let before = crate::lsm_tree::sst::light_view_materialized();
        let h = mvcc.health();
        let used = crate::lsm_tree::sst::light_view_materialized() - before;
        assert!(
            h.live_keys >= KEYS,
            "库里必须有东西可折（live_keys {}）",
            h.live_keys
        );
        assert_eq!(
            used, 0,
            "health 期间有 {used} 条被 `SstFile::light_view` 整篇物化 ⇒ 又走回\
                 「每个源先整份 Vec 化再归并」那个形状（600k 档 +126.1 MB 的来处）"
        );
    }
    cleanup(&path);
}
