// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn test_point_op_cost_independent_of_chain_length() {
    fn probe(chain_len: u64) -> (u64, u64) {
        let path = tmp_db(&format!("probe_{chain_len}"));
        let (get_reads, set_reads);
        {
            let mvcc = MVCC::open(&path);
            for i in 0..chain_len {
                let tx = mvcc.begin_transaction();
                assert!(
                    tx.set(b"hot", format!("v{i}").into_bytes()),
                    "第 {i} 次写入被拒"
                );
                tx.commit();
            }

            let tx = mvcc.begin_transaction();
            let before = crate::bitcask::bitcask::read_value_calls();
            assert_eq!(
                tx.get(b"hot"),
                Some(format!("v{}", chain_len - 1).into_bytes())
            );
            get_reads = crate::bitcask::bitcask::read_value_calls() - before;

            let before = crate::bitcask::bitcask::read_value_calls();
            assert!(tx.set(b"hot", b"newer".to_vec()));
            set_reads = crate::bitcask::bitcask::read_value_calls() - before;
            tx.rollback();
        }
        cleanup(&path);
        (get_reads, set_reads)
    }

    let (g_short, s_short) = probe(50);
    let (g_long, s_long) = probe(2000);
    eprintln!(
        "点读读盘次数：链长 50 → {g_short} 次，链长 2000 → {g_long} 次\n\
             点写读盘次数：链长 50 → {s_short} 次，链长 2000 → {s_long} 次"
    );
    assert_eq!(g_short, g_long, "点读的读盘次数不应随版本链长度增长");
    assert_eq!(
        s_short, s_long,
        "点写的读盘次数不应随版本链长度增长（它只需要版本号）"
    );
}

/// 长跑基线：**事故形状**（一把热点 key 被持续更新 + 稳定点读），自动回收开着 vs 关着。
///
/// 跑法（release，几十秒）：
/// ```text
/// cargo test --release --lib bitcask::bitcask_mvcc::tests::test_long_run_baseline -- --ignored --nocapture
/// ```
///
/// 三条断言，一条比一条重要：
/// 1. 开着自动回收 ⇒ 链长与总行数**不随 uptime 增长**（贴着切片宽度）—— I1 + I2。
/// 2. 关着自动回收 ⇒ 链长必须涨到接近写入次数。这条是**反向证据**：同一份读代码、
///    同一个负载，只是不开回收，`max_chain` 就一路涨 ⇒ 说明这条守卫真的咬得到东西。
/// 3. 两阶段的点读耗时**都不该随链长涨**（通道 ① 已断）。耗时只打印不断言 ——
///    在共享机器上做时间断言会 flaky；而"关掉回收后时间也不爆、只有行数涨"本身就是结论：
///    **点读成本与垃圾回收是两条独立通道**，修了一条不等于修了另一条。
///
/// 实测出来的界（不是推出来的）：链的稳态上界是 **2 × 切片宽度**，不是 1 ×。
/// 原因：一片的截断只能落在逻辑 key 边界，热点 key 那一组太大时会把这一整片吃满，
/// 于是它被"轮到"的频率是每两小轮一次 ⇒ 平均攒到 1×、最坏 2×。默认片 16 Ki 行时
/// 稳态链 ≲ 32 Ki —— 对 23.8 万版的现场来说是数量级的差距，但别误报成"贴着 1"。
#[test]
#[ignore = "长跑基线：手动 cargo test --release -- --ignored"]
fn test_long_run_baseline() {
    use std::time::Instant;

    const N: u64 = 20_000;
    const SLICE: u64 = 2_048;
    const OTHER: u64 = 200;
    const READ_EVERY: u64 = 4;
    const READ_SAMPLES: usize = 250;

    fn run_workload(path: &Path, auto_slice: u64) -> (MvccHealth, f64, f64) {
        let mut reads: Vec<u128> = Vec::with_capacity((N / READ_EVERY) as usize + 1);
        let mvcc = MVCC::open(path);
        mvcc.set_vacuum_auto_slice(auto_slice);

        for k in 0..OTHER {
            let tx = mvcc.begin_transaction();
            assert!(tx.set(format!("other{k}").as_bytes(), b"v".to_vec()));
            tx.commit();
        }

        for i in 0..N {
            let tx = mvcc.begin_transaction();
            assert!(tx.set(b"hot", format!("v{i}").into_bytes()));
            tx.commit();
            if i % READ_EVERY == 0 {
                let t0 = Instant::now();
                let tx = mvcc.begin_transaction();
                let hit = tx.get(b"hot");
                tx.commit();
                assert!(hit.is_some(), "第 {i} 次采样读不到热点 key");
                reads.push(t0.elapsed().as_micros());
            }
        }
        let head = reads[..READ_SAMPLES].iter().sum::<u128>() as f64 / READ_SAMPLES as f64;
        let tail =
            reads[reads.len() - READ_SAMPLES..].iter().sum::<u128>() as f64 / READ_SAMPLES as f64;
        (mvcc.health(), head, tail)
    }

    let on_path = tmp_db("baseline_on");
    let off_path = tmp_db("baseline_off");

    let (h_on, head_on, tail_on) = run_workload(&on_path, SLICE);
    let (h_off, head_off, tail_off) = run_workload(&off_path, 0);

    println!("负载：{N} 次热点更新 + 每 {READ_EVERY} 次一次点读 + {OTHER} 把稳定 key");
    println!(
        "自动回收 开(片={SLICE})  max_chain={:>6}  total_rows={:>6}  点读均耗 前 {head_on:>7.1}µs / 后 {tail_on:>7.1}µs",
        h_on.max_chain, h_on.total_rows
    );
    println!(
        "自动回收 关             max_chain={:>6}  total_rows={:>6}  点读均耗 前 {head_off:>7.1}µs / 后 {tail_off:>7.1}µs",
        h_off.max_chain, h_off.total_rows
    );

    // 1) 开着：链与行数都贴住切片宽度（稳态上界 2×，理由见函数头），不随 2 万次更新增长
    assert!(
        h_on.max_chain <= 2 * SLICE + 2,
        "开着自动回收链却涨到 {} 行（界 {}）—— 回收没跟上产生速度",
        h_on.max_chain,
        2 * SLICE
    );
    assert!(
        h_on.total_rows <= 2 * SLICE + OTHER + 8,
        "开着自动回收总行数却到 {}",
        h_on.total_rows
    );
    assert_eq!(h_on.live_keys, OTHER + 1);
    // 2) 关着：链必须一路涨到接近写入次数（反向证据，守卫咬得到）
    assert!(
        h_off.max_chain >= N - 2,
        "关掉自动回收后链只有 {} 行？那这条基线什么都没测到",
        h_off.max_chain
    );
    // 3) 两者的行数差就是回收在干的事：实测比值 ≈ 5.0（N / 2×切片），
    //    界取 3× 留余量 —— 别把比值写成断言，跑一次没红不代表它对
    assert!(
        h_off.total_rows > 3 * h_on.total_rows,
        "同样负载，关回收才 {} 行、开回收 {} 行，差距不该这么小",
        h_off.total_rows,
        h_on.total_rows
    );

    cleanup(&on_path);
    cleanup(&off_path);

    // ── 只读事务的代价：读路径到底是不是写路径 ────────────────────────────
    //
    // 关掉自动回收免得混进别的变量，只看"一次只读事务花多少、往 WAL 落多少字节"。
    let ro_path = tmp_db("baseline_readonly");
    let (ro_us_per, wal_bytes_per) = {
        let mvcc = MVCC::open(&ro_path);
        mvcc.set_vacuum_auto_slice(0);
        let tx = mvcc.begin_transaction();
        assert!(tx.set(b"k", b"v".to_vec()));
        tx.commit();
        let wal = crate::bitcask::storage::wal_path(&ro_path);
        let before = std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0);
        let t0 = std::time::Instant::now();
        const R: u64 = 2_000;
        for _ in 0..R {
            let tx = mvcc.begin_transaction();
            assert!(tx.get(b"k").is_some());
            tx.commit();
        }
        let total = t0.elapsed().as_micros() as f64;
        let after = std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0);
        (
            total / R as f64,
            (after.saturating_sub(before)) as f64 / R as f64,
        )
    };
    println!(
        "只读事务：{:>7.1}µs / 次，WAL 增长 {:>5.1} 字节 / 次",
        ro_us_per, wal_bytes_per
    );
    cleanup(&ro_path);
}

/// 健康度快照必须如实报出「最长版本链」。
///
/// 这次事故的核心数字（某把热点 key 堆了多少个版本）一直是靠
/// `grep data.log | uniq -c` 数出来的 —— 一个当基座用的库不该靠这个诊断自己。
/// 顺带验证 vacuum 之后能看得见「链被压平」，那正是将来自动回收要盯的指标。
#[test]
fn test_health_reports_chain_length() {
    const CHAIN: u64 = 500;

    let path = tmp_db("health");
    {
        let mvcc = MVCC::open(&path);
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

#[test]
fn test_basic_set_get_delete() {
    let path = tmp_db("basic");
    {
        let mvcc = MVCC::open(&path);
        let tx = mvcc.begin_transaction();
        assert!(tx.set(b"a", b"1".to_vec()));
        assert_eq!(tx.get(b"a"), Some(b"1".to_vec()));
        assert!(tx.set(b"a", b"2".to_vec()));
        assert_eq!(tx.get(b"a"), Some(b"2".to_vec()));
        assert!(tx.delete(b"a"));
        assert_eq!(tx.get(b"a"), None);
        tx.commit();
    }
    cleanup(&path);
}

#[test]
fn test_print_all_latest_visible() {
    let path = tmp_db("pa");
    {
        let mvcc = MVCC::open(&path);
        let t1 = mvcc.begin_transaction();
        assert!(t1.set(b"a", b"1".to_vec()));
        assert!(t1.set(b"b", b"2".to_vec()));
        assert!(t1.set(b"a", b"3".to_vec()));
        let all = t1.print_all();
        assert_eq!(all.get(&b"a".to_vec()).unwrap().as_ref().unwrap(), b"3");
        assert_eq!(all.get(&b"b".to_vec()).unwrap().as_ref().unwrap(), b"2");
        t1.commit();
    }
    cleanup(&path);
}

#[test]
fn test_instances_isolated() {
    let p1 = tmp_db("i1");
    let p2 = tmp_db("i2");
    {
        let m1 = MVCC::open(&p1);
        let m2 = MVCC::open(&p2);
        let t1 = m1.begin_transaction();
        let t2 = m2.begin_transaction();
        assert!(t1.set(b"k", b"1".to_vec()));
        assert!(t2.set(b"k", b"2".to_vec()));
        assert_eq!(t1.get(b"k"), Some(b"1".to_vec()));
        assert_eq!(t2.get(b"k"), Some(b"2".to_vec()));
        t1.commit();
        t2.commit();
    }
    cleanup(&p1);
    cleanup(&p2);
}

#[test]
fn test_export_latest_visible() {
    let path = tmp_db("export");
    {
        let mvcc = MVCC::open(&path);
        let t1 = mvcc.begin_transaction();
        assert!(t1.set(b"k1", b"v1".to_vec()));
        assert!(t1.set(b"k2", b"v2".to_vec()));
        t1.commit();
        let t2 = mvcc.begin_transaction();
        assert!(t2.set(b"k1", b"v1b".to_vec()));
        assert!(t2.delete(b"k2"));
        assert!(t2.set(b"k3", b"v3".to_vec()));
        t2.commit();
    }
    {
        let mvcc = MVCC::open(&path);
        let live = mvcc.export_latest_visible(false);
        assert_eq!(live.len(), 2);
        assert_eq!(
            live.iter().find(|r| r.key == b"k1").unwrap().value,
            Some(b"v1b".to_vec())
        );
        assert_eq!(
            live.iter().find(|r| r.key == b"k3").unwrap().value,
            Some(b"v3".to_vec())
        );
        assert!(!live.iter().any(|r| r.key == b"k2"));

        let all = mvcc.export_latest_visible(true);
        assert_eq!(all.len(), 3);
        assert!(all.iter().any(|r| r.key == b"k2" && r.value.is_none()));
    }
    cleanup(&path);
}

#[test]
fn test_large_value_blob_roundtrip() {
    let path = tmp_db("blob");
    let big = vec![0xABu8; 10_000]; // > BLOB_THRESHOLD
    {
        let mvcc = MVCC::open(&path);
        let tx = mvcc.begin_transaction();
        assert!(tx.set(b"big", big.clone()));
        assert!(tx.set(b"small", b"hi".to_vec()));
        assert_eq!(tx.get(b"big"), Some(big.clone()));
        assert_eq!(tx.get(b"small"), Some(b"hi".to_vec()));
        tx.commit();
        mvcc.flush();
    }
    {
        let mvcc = MVCC::open(&path);
        let tx = mvcc.begin_transaction();
        assert_eq!(tx.get(b"big"), Some(big));
        assert_eq!(tx.get(b"small"), Some(b"hi".to_vec()));
        tx.commit();
    }
    let blob = blob_path(&path);
    assert!(blob.exists());
    assert!(std::fs::metadata(&blob).unwrap().len() > 8);
    cleanup(&path);
}
