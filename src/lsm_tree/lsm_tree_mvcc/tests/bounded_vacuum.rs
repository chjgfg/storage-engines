// 该文件集中验证对应职责，保留原测试名称和字面值。

/// 一片有界回收的**读放大**归因（M29）。
///
/// 触发它的测量：2026-10-18 三档基线里 `lsm vacuum_bounded 一片`（预算 2048）=
/// 2 716.3 / 10 277.0 / 30 090.7 µs（600k 复跑 15 676.8）⇒ 每行单价随库涨 **5.8~11×**，
/// 而 README 那句"平 —— 有界回收这条是干净的（长跑该用它的依据）"就是这么被推翻的。
/// 那一片只交 2046 行、裁 0 行 ⇒ 14.7 µs/行，是同档 `一次全量 scan`（4.9 µs/行）的 3 倍。
///
/// 尺子是 `light_visits`（这片真的折过多少把编码键）—— **计数与构建档位无关，debug 就能跑**，
/// 所以这条不占 release 台架的时间。旧写法（每源各取满 `预算` 行再归并）在这里报的是
/// `源数 × 预算`：24 个 SST 时交出 2048 行、折 22000 把键；换成惰性归并后折/交 ≈ 1。
///
/// ```text
/// LSM_VAC_ROWS=200000 cargo test --lib -- --ignored --nocapture --test-threads=1 vacuum_slice_fold_attribution
/// ```
#[test]
#[ignore = "一片读放大归因，单线程跑：LSM_VAC_ROWS=200000 cargo test --lib -- --ignored --nocapture --test-threads=1 vacuum_slice_fold_attribution"]
fn vacuum_slice_fold_attribution() {
    use std::time::Instant;

    let rows: usize = std::env::var("LSM_VAC_ROWS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200_000);
    // 源数（= SST 文件数）是自变量：默认把文件数顶到 24，压实整个关掉免得被合回去。
    // ⚠️ 为什么用"多次 flush"而不是"调小 target_sst_bytes"：那个旋钮只在**压实输出**时
    // 切文件，`flush_mem` 一次只写一个文件 —— 第一版就是照旋钮调的，量出来永远 1 个文件。
    let files: usize = std::env::var("LSM_VAC_FILES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(24);
    fn key(i: usize) -> Vec<u8> {
        format!("{i:08}#/worldquant/datafields/x.json").into_bytes()
    }
    fn val(i: usize) -> Vec<u8> {
        format!("{{\"i\":{i},\"h\":\"{:0<24}\",\"v\":1}}", i).into_bytes()
    }
    const BUDGET: u64 = 2048;

    let path = tmp_db("vac_slice");
    {
        let mut opts = LSMTreeOptions::for_mvcc();
        opts.l0_compact_threshold = usize::MAX / 4;
        opts.max_compactions_per_flush = 0;
        // 写路径自己有一档 L0 背压：`maybe_stall_on_l0` 在 L0 ≥ `l0_slowdown_trigger`
        // (MVCC 档 = 12) 时**直接** `compact_level(0)`，绕开上面两个旋钮 —— 第一版台架
        // 就是在第 13 个文件处被它把 12 个文件合成了 2 个，"源数"这个自变量当场失效。
        opts.l0_slowdown_trigger = usize::MAX / 8;
        opts.l0_stop_trigger = usize::MAX / 8;
        let mvcc = MVCC::try_open_with(&path, Some(opts)).unwrap();
        // 每个文件写 `per` 行，写完立刻 flush ⇒ 一个文件（`flush_mem` 一次只写一个 SST）。
        // 这样"源数"就是这条台架唯一在动的自变量。
        let per = (rows + files - 1) / files;
        for f in 0..files {
            let lo = f * per;
            let hi = (lo + per).min(rows);
            if lo >= hi {
                break;
            }
            for base in (lo..hi).step_by(1000) {
                let end = (base + 1000).min(hi);
                let items: Vec<(Vec<u8>, Vec<u8>)> =
                    (base..end).map(|i| (key(i), val(i))).collect();
                let tx = mvcc.begin_transaction();
                if let Err(bad) = tx.batch_set(&items) {
                    panic!("灌第 {base} 批冲突: {}", String::from_utf8_lossy(&bad));
                }
                tx.commit();
            }
            mvcc.flush();
            // ⚠️ 门面那个 `MVCC::flush` 是 `let _ = ...`（吞错的），台架必须直接问树要结果，
            // 否则"文件数没长上去"这种形状失败会静默变成一条空转的读数。
            let s = mvcc.kv.lock().unwrap().stats();
            assert_eq!(
                s.total_sst,
                f + 1,
                "第 {} 次 flush 后该有 {} 个文件",
                f,
                f + 1
            );
        }
        // 更新 10% 的键：制造版本链，vacuum 才有得裁（形状照门面台架）
        let updates = (rows / 10).max(1);
        for base in (0..updates).step_by(500) {
            let end = (base + 500).min(updates);
            let items: Vec<(Vec<u8>, Vec<u8>)> =
                (base..end).map(|i| (key(i), val(i + rows))).collect();
            let tx = mvcc.begin_transaction();
            if let Err(bad) = tx.batch_set(&items) {
                panic!("更新第 {base} 批冲突: {}", String::from_utf8_lossy(&bad));
            }
            tx.commit();
        }
        let st = mvcc.kv.lock().unwrap().stats();
        eprintln!(
            "=== 一片读放大 rows={rows} 预算={BUDGET} | sst {} 个 / {} MB | mem_entries {} ===",
            st.total_sst,
            st.level_bytes.iter().sum::<u64>() / 1_048_576,
            st.mem_entries
        );

        let mut last_cursor: Option<Vec<u8>> = None;
        for which in ["第1片", "第2片"] {
            let base = crate::lsm_tree::sst::light_visits();
            let t = Instant::now();
            let r = mvcc.vacuum_bounded(last_cursor.as_deref(), BUDGET).unwrap();
            let d = t.elapsed();
            let walked = crate::lsm_tree::sst::light_visits() - base;
            let scanned = r.stats.rows_scanned;
            eprintln!(
                "  {which}: 交出 {scanned} 行 / 裁 {} 行 | 折过 {walked} 把编码键 | 折/交 = {:.1}× \
                     | 源数 {} | {:.1} µs | 每行 {:.2} µs",
                r.stats.versions_removed,
                if scanned == 0 {
                    0.0
                } else {
                    walked as f64 / scanned as f64
                },
                st.total_sst + 1,
                d.as_secs_f64() * 1e6,
                if scanned == 0 {
                    0.0
                } else {
                    d.as_secs_f64() * 1e6 / scanned as f64
                },
            );
            last_cursor = r.next_cursor;
        }
    }
    cleanup(&path);
}

/// 有界回收：每轮只碰一小段，多轮跑完必须和一次全库 vacuum 得到**同样**的结果。
///
/// LSM 的切片比另外三家麻烦：一扇窗口要横跨 memtable 和若干 SST，而版本键
/// `raw ‖ 8B` 下**同一把逻辑 key 的各版本会散在不同源里**，所以"留哪一版"必须在
/// 整组收齐之后才敢判。这里中途 `flush()` 一次，让版本行同时分布在 memtable 和 SST 里，
/// 逼这条跨源归并的路径真的被走到。
/// 键里放了前缀嵌套的 `k1` / `k1x`：两把 key 的区间会互相穿插，这是游标最容易切在
/// 组中间的情形。
/// `vacuum()` 必须**按块**删除，不许把整库的待删行与存活行攒在两个 `Vec` 里（M37）。
///
/// 判据（单位：行）：库里可删的旧版**远多于一块**时，单次 flush 前最多攒的待处理行数
/// `vac_pending_max()` 必须 ≤ `2 × VAC_CHUNK_ROWS`；与 bitcask 那条同形同值，各家钉各家。
/// `versions_removed > VAC_CHUNK_ROWS` 那条断言保证块界**真的被跨过**（小库永远装得下 ⇒ 空转）。
/// 为什么这条对 lsm 尤其要紧：它除了 `to_delete` 还要把"存活行清单"交给 `gc_blobs`
/// （600k 档 `vacuum()` 瞬时 **101.2 MB** 的四家最大的一笔）—— 现在引用集合逐块并入，
/// 清单不再存在。
///
/// 变异验证：把块界 `VAC_CHUNK_ROWS` 临时抬到一整库那么大 ⇒ 待攒行数变成全部可删行数
/// （20 000 > 界 16 384），红。
#[test]
fn test_vacuum_deletes_in_bounded_chunks() {
    const KEYS: usize = 20_000;
    let path = tmp_db("vac_chunks");
    {
        let mvcc = MVCC::open(&path);
        // 自动回收会在提交那一刻先裁掉一片，那这条判据就量不到手动 vacuum 的界了
        mvcc.set_vacuum_auto_slice(0);
        for _ in 0..2u64 {
            let tx = mvcc.begin_transaction();
            for i in 0..KEYS {
                let k = format!("k{i:06}");
                assert!(tx.set(k.as_bytes(), vec![b'v'; 24]), "第 {i} 把键写失败");
            }
            tx.commit();
        }
        reset_vac_pending();
        let st = mvcc.vacuum().unwrap();
        let pending = vac_pending_max();
        eprintln!(
            "[vac chunks lsm] 裁掉 {} 行 / 扫 {} 行 ⇒ 单次 flush 前最多攒 {} 行（块界 {VAC_CHUNK_ROWS}）",
            st.versions_removed, st.rows_scanned, pending
        );
        assert!(st.rows_scanned > 0, "一行都没扫到 ⇒ 空转");
        assert!(
            st.versions_removed > VAC_CHUNK_ROWS,
            "可删行数 {} 没超过一块 ⇒ 这条判据量不到界（空转）",
            st.versions_removed
        );
        assert!(pending > 0, "这把尺子没数到待删行 ⇒ 空转");
        assert!(
            pending <= 2 * VAC_CHUNK_ROWS as u64,
            "单次 flush 前攒了 {pending} 行待处理（界 {}）⇒ 又变回整库一个 `Vec`",
            2 * VAC_CHUNK_ROWS as u64
        );
        // 语义还得对：裁完之后每把键读得回最新那一版
        let tx = mvcc.begin_transaction();
        for i in (0..KEYS).step_by(997) {
            let k = format!("k{i:06}");
            assert_eq!(
                tx.get(k.as_bytes()),
                Some(vec![b'v'; 24]),
                "第 {i} 把键在按块回收后读不回来"
            );
        }
        tx.commit();
    }
    cleanup(&path);
}

#[test]
fn test_vacuum_bounded_slices_and_converges() {
    const CHAIN: u64 = 120;
    const KEYS: [&[u8]; 6] = [b"k1", b"k1x", b"k2", b"k3", b"k4", b"k5"];
    const SLICE: u64 = 40;

    fn fill(mvcc: &MVCC) {
        for i in 0..CHAIN {
            for k in KEYS {
                let tx = mvcc.begin_transaction();
                assert!(tx.set(k, format!("v{i}").into_bytes()), "写入被拒：{k:?}");
                tx.commit();
            }
            if i == CHAIN / 2 {
                mvcc.flush();
            }
        }
    }

    // 对照组
    let full_path = tmp_db("bounded_full");
    let full_removed;
    {
        let mvcc = MVCC::open(&full_path);
        fill(&mvcc);
        full_removed = mvcc.vacuum().unwrap().versions_removed;
    }
    cleanup(&full_path);

    let path = tmp_db("bounded_slice");
    {
        let mvcc = MVCC::open(&path);
        fill(&mvcc);

        let mut cursor: Option<Vec<u8>> = None;
        let mut rounds = 0usize;
        let mut removed = 0usize;
        loop {
            let round = mvcc.vacuum_bounded(cursor.as_deref(), SLICE).unwrap();
            assert!(
                !round.stats.skipped_active_txn,
                "第 {rounds} 轮不该被活跃事务挡住"
            );
            assert!(
                round.stats.rows_scanned <= SLICE + CHAIN,
                "第 {rounds} 轮读了 {} 行（预算 {SLICE} + 一条链 {CHAIN}）",
                round.stats.rows_scanned
            );
            assert!(
                round.stats.versions_removed > 0,
                "第 {rounds} 轮必须裁掉东西"
            );
            removed += round.stats.versions_removed;
            rounds += 1;
            match round.next_cursor {
                Some(c) => {
                    assert_ne!(cursor.as_ref(), Some(&c), "游标必须前进（第 {rounds} 轮）");
                    cursor = Some(c);
                }
                None => break,
            }
            assert!(
                rounds < 60,
                "切片应在有限轮内收敛，别是原地打转：已 {rounds} 轮"
            );
        }

        assert!(
            rounds > 1,
            "6 把 key × {CHAIN} 版不该一轮就扫完：{rounds} 轮"
        );
        assert_eq!(removed, full_removed, "分片裁的总行数应与整库一轮一致");
        let h = mvcc.health();
        assert_eq!(h.max_chain, 1, "分片跑完后链必须压平");
        assert_eq!(h.total_rows, KEYS.len() as u64);

        let tx = mvcc.begin_transaction();
        for k in KEYS {
            assert_eq!(tx.get(k), Some(format!("v{}", CHAIN - 1).into_bytes()));
        }
        tx.commit();
    }
    cleanup(&path);
}

/// 一片有界回收**只许走它预算那么多行**（M29）。
///
/// 触发它的测量：三档基线里 `lsm vacuum_bounded 一片`（预算 2048）= 2 716.3 / 10 277.0 /
/// 30 090.7 µs ⇒ **每行单价随库涨 5.8~11×**，而 README 那句"平 —— 有界回收这条是干净的
/// （长跑该用它的依据）"就是这么被推翻的。归因台架 [`vacuum_slice_fold_attribution`]
/// 给出机制：旧写法走 `scan_latest_light_window` —— **每个源各取满 `预算` 行再归并**，
/// 于是取到的行数是 `源数 × 预算`（24 个 SST 时交出 2048 行、折 22000 把键；48 个文件时
/// 直接等于整库），而 SST 文件数 ∝ 库大小 ⇒ 单价随库涨。
///
/// 尺子 = `light_visits`（这片交到我手里多少把编码键）。两条断言配对才不空转：
/// **折过的 ≤ 预算 × 2**，而**交出的行数必须接近预算**（不然"只走了三行"也能过）。
/// 源数是这里的自变量：形状造到 17 个源 ⇒ 老写法该折 17× 预算 ⇒ 必红。
#[test]
fn test_vacuum_slice_walks_only_its_budget() {
    const ROWS: usize = 8_000;
    const FILES: usize = 16;
    const BUDGET: u64 = 512;

    let path = tmp_db("vac_budget");
    let mut opts = LSMTreeOptions::for_mvcc();
    // 压实与 L0 背压整个关掉：这条要的是"文件多"这个形状。⚠️ 只关 `l0_compact_threshold`
    // 和 `max_compactions_per_flush` 不够 —— 写路径还有一档 `maybe_stall_on_l0`，
    // L0 到 `l0_slowdown_trigger`(MVCC 档 = 12) 会**绕过那两个旋钮**直接 compact_level(0)。
    opts.l0_compact_threshold = usize::MAX / 4;
    opts.max_compactions_per_flush = 0;
    opts.l0_slowdown_trigger = usize::MAX / 8;
    opts.l0_stop_trigger = usize::MAX / 8;
    {
        let mvcc = MVCC::try_open_with(&path, Some(opts)).unwrap();
        let per = ROWS / FILES;
        for f in 0..FILES {
            let lo = f * per;
            let items: Vec<(Vec<u8>, Vec<u8>)> = (lo..lo + per)
                .map(|i| {
                    (
                        format!("k{i:05}").into_bytes(),
                        format!("v{i}").into_bytes(),
                    )
                })
                .collect();
            let tx = mvcc.begin_transaction();
            if let Err(bad) = tx.batch_set(&items) {
                panic!("灌第 {f} 片冲突: {}", String::from_utf8_lossy(&bad));
            }
            tx.commit();
            mvcc.flush();
            // 门面那个 `flush` 是 `let _ = ...`（吞错的），形状没造出来必须当场炸
            let s = mvcc.kv.lock().unwrap().stats();
            assert_eq!(
                s.total_sst,
                f + 1,
                "第 {f} 次 flush 没落出一个文件，形状没造出来"
            );
        }
        // 第二版：让每把键都有两条版本行（vacuum 有得裁，也保证折的是"版本行"而不是"键"）
        for base in (0..ROWS).step_by(2000) {
            let end = (base + 2000).min(ROWS);
            let items: Vec<(Vec<u8>, Vec<u8>)> = (base..end)
                .map(|i| {
                    (
                        format!("k{i:05}").into_bytes(),
                        format!("w{i}").into_bytes(),
                    )
                })
                .collect();
            let tx = mvcc.begin_transaction();
            if let Err(bad) = tx.batch_set(&items) {
                panic!("更新第 {base} 批冲突: {}", String::from_utf8_lossy(&bad));
            }
            tx.commit();
        }

        let before = crate::lsm_tree::sst::light_visits();
        let r = mvcc.vacuum_bounded(None, BUDGET).unwrap();
        let walked = crate::lsm_tree::sst::light_visits() - before;
        eprintln!(
            "一片: 交出 {} 行 / 折过 {walked} 把键 / 裁 {} 行 / 源数 {}",
            r.stats.rows_scanned,
            r.stats.versions_removed,
            FILES + 1
        );
        assert!(
            r.stats.rows_scanned >= BUDGET - BUDGET / 8,
            "预算 {BUDGET} 行的一片只交了 {} 行 ⇒ 上面那条比值断言会空转（形状或游标有问题）",
            r.stats.rows_scanned
        );
        assert!(
            walked <= BUDGET * 2,
            "预算 {BUDGET} 行的一片折了 {walked} 把编码键（源数 {}）⇒ 又回到「每源各取满预算」，\
                 一片的代价就跟着 SST 文件数涨",
            FILES + 1
        );
        assert!(
            r.next_cursor.is_some(),
            "库里还有 {} 行没走完，游标不该是 None",
            ROWS * 2
        );
    }
    cleanup(&path);
}
