// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn scan_mem_versions_copy_only_the_logical_winner() {
    use crate::common::VersionedStore;
    let path = tmp_db("scan_mem_winner");
    let db = MVCC::open(&path);
    db.set_vacuum_auto_slice(0);
    for i in 0..100 {
        let tx = db.begin_transaction();
        assert!(tx.set_raw(b"key", vec![i; 64]));
        tx.commit();
    }
    let tx = db.begin_transaction();
    let before = crate::lsm_tree::sst::entry_value_clones();
    {
        let kv = tx.kv.lock().unwrap();
        for (_, row) in kv.row_iter_range(None, None, u64::MAX) {
            drop(row.freeze(Vec::new));
        }
    }
    assert_eq!(
        crate::lsm_tree::sst::entry_value_clones() - before,
        100,
        "owning before logical folding is the actual copying control"
    );
    let copies = crate::lsm_tree::sst::entry_value_clones();
    let (mut rows, complete) = tx.scan_candidates(None, None, None, None, 4096);
    assert!(complete);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        crate::lsm_tree::sst::entry_value_clones() - copies,
        1,
        "only the final logical winner may copy a borrowed payload"
    );
    assert_eq!(
        tx.materialize(rows.pop().unwrap().row),
        crate::common::RowValue::Value(crate::common::pack_plain(&vec![99; 64]))
    );
    tx.commit();
}

#[test]
fn scan_rows_reuse_blocks_and_filter_before_copying_values() {
    use crate::common::VersionedStore;
    use crate::lsm_tree::sst::{ScanRow, entry_value_clones, sst_probes};
    let path = tmp_db("scan_row_ownership");
    let db = MVCC::open(&path);
    db.set_vacuum_auto_slice(0);
    let tx = db.begin_transaction();
    for i in 0..128 {
        assert!(tx.set_raw(format!("k{i:03}").as_bytes(), vec![i as u8; 64]));
    }
    tx.commit();
    db.flush();
    let tx = db.begin_transaction();
    for old in [true, false] {
        let mode = old.then(super::scan_row_reference::Scope::start);
        let (rows, complete) = tx.scan_candidates(None, None, None, None, 256);
        assert!(complete);
        assert_eq!(rows.len(), 128);
        let probes = sst_probes();
        for (i, row) in rows.into_iter().enumerate() {
            assert_eq!(
                tx.materialize(row.row),
                crate::common::RowValue::Value(crate::common::codec::pack_plain(&vec![
                    i as u8;
                    64
                ]))
            );
        }
        let probes = sst_probes() - probes;
        if old {
            assert!(
                probes >= 128,
                "old materialization must exercise the real point lookup"
            );
        } else {
            assert_eq!(
                probes, 0,
                "shared SST rows must be used, not looked up again"
            );
        }
        drop(mode);
    }
    let copies = entry_value_clones();
    let (rows, complete) = tx.scan_candidates(None, None, Some(&|_| false), None, 256);
    assert!(complete && rows.is_empty());
    assert_eq!(
        entry_value_clones(),
        copies,
        "rejected keys must not copy values"
    );
    let (rows, _) = tx.scan_candidates(None, None, None, None, 7);
    assert_eq!(rows.len(), 7);
    assert!(rows.iter().all(|r| matches!(r.row, ScanRow::Inline(_))));
    // 保留行的生命周期必须跨过扫描以及后续缓存换出或替换。
    db.flush();
    for (i, row) in rows.into_iter().enumerate() {
        assert_eq!(
            tx.materialize(row.row),
            crate::common::RowValue::Value(crate::common::codec::pack_plain(&vec![i as u8; 64]))
        );
    }
    tx.commit();
    let tx = db.begin_transaction();
    assert!(tx.set_raw(b"blob", vec![9; 16384]));
    tx.commit();
    db.flush();
    let tx = db.begin_transaction();
    let (mut rows, _) = tx.scan_candidates(Some(b"blob"), Some(b"blob\0"), None, None, 1);
    assert_eq!(rows.len(), 1);
    let row = rows.pop().unwrap().row;
    assert!(
        matches!(&row, ScanRow::BlobLookup(_)),
        "blob lifetime fallback must be explicit"
    );
    let probes = sst_probes();
    assert_eq!(
        tx.materialize(row),
        crate::common::RowValue::Value(crate::common::codec::pack_plain(&vec![9; 16384]))
    );
    assert!(
        sst_probes() > probes,
        "blob fallback has a separate real lookup cost"
    );
    tx.commit();
}

#[test]
fn search_inline_count_borrows_values_with_real_old_path_control() {
    use crate::lsm_tree::sst::entry_value_clones;
    let path = tmp_db("search_borrow_count");
    let db = MVCC::open(&path);
    db.set_vacuum_auto_slice(0);
    let tx = db.begin_transaction();
    for i in 0..10_000u64 {
        assert!(tx.set_raw(&i.to_be_bytes(), vec![9; 128]));
    }
    tx.commit();
    db.flush();
    let tx = db.begin_transaction();
    for old in [true, false] {
        let mode = old.then(search_borrow_reference::Scope::start);
        let before = entry_value_clones();
        assert_eq!(tx.key_count(b""), 10_000);
        let copies = entry_value_clones() - before;
        let guard = std::panic::catch_unwind(|| assert_eq!(copies, 0));
        assert_eq!(
            guard.is_err(),
            old,
            "restoring actual owned count must fail zero-copy guard"
        );
        assert_eq!(copies, if old { 10_000 } else { 0 });
        let before = entry_value_clones();
        let page = tx.search_keys(&crate::common::SearchQuery::contains(b"", 0, 100));
        assert_eq!(page.total, Some(10_000));
        assert_eq!(page.items.len(), 100);
        assert_eq!(page.items[99].value, Some(vec![9; 128]));
        assert_eq!(
            entry_value_clones() - before,
            if old { 10_000 } else { 100 }
        );
        drop(mode);
    }
    tx.commit();
}

#[test]
#[ignore = "same SST seed count/first ABBA for borrowed search classification"]
fn search_inline_count_same_seed_abba() {
    let rows: usize = std::env::var("LSM_SEARCH_ROWS")
        .map(|s| s.parse().unwrap())
        .unwrap_or(200_000);
    let path = tmp_db("search_inline_abba");
    let db = MVCC::open(&path);
    db.set_vacuum_auto_slice(0);
    let tx = db.begin_transaction();
    for i in 0..rows {
        assert!(tx.set_raw(&(i as u64).to_be_bytes(), vec![9; 128]));
    }
    tx.commit();
    db.flush();
    let tx = db.begin_transaction();
    assert_eq!(tx.key_count(b""), rows);
    for task in ["count", "first"] {
        for (arm, old) in [true, false, false, true].into_iter().enumerate() {
            let mode = old.then(search_borrow_reference::Scope::start);
            let before = crate::lsm_tree::sst::entry_value_clones();
            let now = std::time::Instant::now();
            if task == "count" {
                assert_eq!(tx.key_count(b""), rows);
            } else {
                let page = tx.search_keys(&crate::common::SearchQuery::contains(b"", 0, 100));
                assert_eq!(page.total, Some(rows));
                assert_eq!(page.items.len(), 100);
                assert_eq!(page.items[99].key, 99u64.to_be_bytes());
                assert_eq!(page.items[99].value, Some(vec![9; 128]));
            }
            let ms = now.elapsed().as_secs_f64() * 1000.0;
            let copies = crate::lsm_tree::sst::entry_value_clones() - before;
            assert_eq!(
                copies,
                if old {
                    rows as u64
                } else if task == "count" {
                    0
                } else {
                    100
                }
            );
            std::eprintln!(
                "LSM_SEARCH_ABBA rows={rows} task={task} arm={arm} old={old} ms={ms:.3} copies={copies}"
            );
            drop(mode);
        }
    }
    tx.commit();
}

/// ② 全库折叠：不该再物化「带 value 的条目」。
///
/// 计数挂在重量路径上（`SstEntryIter::next` 与 `mem_iter_at`），所以这条一旦变红
/// 就说明折叠又被改回 `iter()` —— 那意味着一次扫描的峰值内存约等于整个库。
#[test]
fn test_collect_latest_does_not_materialize_values() {
    const KEYS: u64 = 8;
    const VERSIONS: u64 = 120;

    let path = tmp_db("collect_light");
    {
        let mvcc = MVCC::open(&path);
        for v in 0..VERSIONS {
            for k in 0..KEYS {
                let key = format!("k{k}");
                let tx = mvcc.begin_transaction();
                assert!(tx.set(key.as_bytes(), format!("v{v}").into_bytes()));
                tx.commit();
            }
        }
        // 落到 SST，让重量路径真的有机会被走到
        mvcc.flush();

        let tx = mvcc.begin_transaction();
        let base = crate::lsm_tree::sst::value_materializations();
        let recs = tx.export_latest_visible(false);
        let used = crate::lsm_tree::sst::value_materializations() - base;
        eprintln!(
            "折叠期重量条目物化次数：{used}（存活 key {KEYS}，历史行数 {}）",
            KEYS * VERSIONS
        );
        assert_eq!(recs.len() as u64, KEYS, "存活 key 数应等于 {KEYS}");
        assert!(
            used <= KEYS + 4,
            "折叠只该为胜出行取值：重量条目物化 {used} 次，而历史行数是 {}",
            KEYS * VERSIONS
        );
        tx.rollback();
    }
    cleanup(&path);
}

/// 窗口 scan 的代价必须随**窗口宽度**缩 —— LSM 的尺子是「读了多少个 SST 数据块」。
///
/// 旧形状：`collect_latest_raw` 走 `scan_latest_light(全库)`，把每个 memtable 整表遍历、
/// 每个 SST **整篇块读完**再交给归并，调用方随后逐条筛界 ⇒ 10 行的窗口要付全库的块读代价
/// （大库基线 release 实测：窗口 scan 405 µs/行 vs 一次全量 scan 4.0 µs/行）。
/// 现在界下推到每一个源：memtable 走 `range`，SST 从 `from` 所在块起读、键越过上界立即
/// 收手，并用 min/max 先挡掉整篇不相干的文件。
///
/// 必须先 `flush()`：数据若还全在 memtable 里，块读次数为 0，那条比值断言就是空转。
/// `scan_foreach` 提前叫停时，**不许先把整库折一遍**（M27）。
///
/// 这条路径的文档从写下那天起就声称「不收集全库到单一大 Vec」，而实现是
/// `for … in collect_latest_raw_in(...)` —— 那一句会把界内每一行都先折出来，再逐条搬成
/// `ExportRecord`。200k 键实测因此比结果集本身还多占 ≈ 15 MB（`一次全量 scan`
/// 峰值 +45.5 MB / 常驻 30.7 MB）。现在走共用件 `fold_rows_streaming`：折一行交一行，
/// 叫停之后的键不再碰。
///
/// LSM 的候选来自轻量视图（只带 seq 与种类，不克隆 value）⇒ 没有「取了几次值」这笔账，
/// 能数的是**折过多少把编码键**。尺子单位不同，判据就按比例给：一片 = `FOLD_CHUNK`(4096)，
/// 所以早停只该走到全库的一小截（老写法先把全库折完才交第一行 ⇒ 比例 1.0）。
#[test]
fn test_scan_foreach_stops_without_folding_the_library() {
    const N: usize = 3 * 4096 + 100;
    fn ruler() -> u64 {
        crate::lsm_tree::sst::light_visits()
    }
    let path = tmp_db("foreach_stop");
    {
        let mvcc = MVCC::open(&path);
        for base in (0..N).step_by(2000) {
            let end = (base + 2000).min(N);
            let items: Vec<(Vec<u8>, Vec<u8>)> = (base..end)
                .map(|i| {
                    (
                        format!("k{i:05}").into_bytes(),
                        format!("v{i}").into_bytes(),
                    )
                })
                .collect();
            let tx = mvcc.begin_transaction();
            if let Err(bad) = tx.batch_set(&items) {
                panic!("灌第 {base} 批: {}", String::from_utf8_lossy(&bad));
            }
            tx.commit();
        }
        let tx = mvcc.begin_transaction();
        let base = ruler();
        let mut handed = 0usize;
        tx.scan_foreach(None, None, |rec| {
            assert!(rec.value.is_some(), "交出来的行没有值");
            handed += 1;
            false // 只要第一行
        });
        let early = ruler() - base;
        let base = ruler();
        let all = tx.scan(None, None);
        let full = ruler() - base;
        tx.commit();

        assert_eq!(handed, 1, "回调明明叫停了");
        assert_eq!(all.len(), N, "参照量：全量 scan 要折出所有行");
        assert!(
            early * 2 <= full,
            "只要 1 行就折了 {early} 把键，全量也才 {full} ⇒ 没提前收手（一片该是 4096 级别）"
        );
        eprintln!("[scan_foreach 早停] 交 1 行付 {early}，全量 {N} 行付 {full}");
    }
    cleanup(&path);
}

#[test]
fn test_windowed_scan_cost_scales_with_window_not_library() {
    const KEYS: u64 = 3000;
    let path = tmp_db("window_cost");
    {
        let mvcc = MVCC::open(&path);
        for i in 0..KEYS {
            let tx = mvcc.begin_transaction();
            assert!(tx.set(format!("k{i:05}").as_bytes(), format!("v{i}").into_bytes()));
            tx.commit();
        }
        mvcc.flush();

        let tx = mvcc.begin_transaction();
        let b0 = crate::lsm_tree::sst::block_reads();
        let all = tx.scan(None, None);
        let full = crate::lsm_tree::sst::block_reads() - b0;
        assert!(
            full >= 30,
            "样本要有足够多的块才分得出窗口与全库（全量只读了 {full} 块）"
        );

        let lo = b"k01000".to_vec();
        let hi = b"k01010".to_vec();
        let b0 = crate::lsm_tree::sst::block_reads();
        let win = tx.scan(Some(&lo), Some(&hi));
        let windowed = crate::lsm_tree::sst::block_reads() - b0;

        assert_eq!(all.len() as u64, KEYS, "全量应折出 {KEYS} 行");
        assert_eq!(win.len(), 10, "[k01000, k01010) 应给 10 行");
        let want: Vec<Vec<u8>> = all
            .iter()
            .filter(|r| r.key.as_slice() >= lo.as_slice() && r.key.as_slice() < hi.as_slice())
            .map(|r| r.key.clone())
            .collect();
        let got: Vec<Vec<u8>> = win.iter().map(|r| r.key.clone()).collect();
        assert_eq!(got, want, "下推界之后窗口结果必须与全量筛一致");
        assert!(
            windowed * 10 <= full,
            "10 行窗口的块读不该是全库量级（{windowed} vs 全量 {full}）"
        );

        // 上界哨兵的陷阱形状（`seek_prev` 的界是 `key ‖ 0x00`）
        assert_eq!(tx.seek_prev(b"k01005").unwrap().key, b"k01005");
        assert_eq!(tx.seek(b"k01000").unwrap().key, b"k01000");

        // 前缀同理：k001xx ⇒ 100 个键
        let b0 = crate::lsm_tree::sst::block_reads();
        let pre = tx.prefix_scan(b"k001");
        let prefixed = crate::lsm_tree::sst::block_reads() - b0;
        assert_eq!(pre.len(), 100, "前缀 k001 应命中 100 行");
        assert!(
            prefixed * 4 <= full,
            "前缀扫的块读不该是全库量级（{prefixed} vs 全量 {full}）"
        );

        // 边界形状：前缀是**所有键的真前缀**（比第 0 块的首键还短 ⇒ 稀疏索引里没有
        // "first_key ≤ from" 的块）。这一步一旦按"没找到就跳过整篇"处理，
        // prefix_scan 会静默返回近乎空的结果 —— 复测表里 lsm 的 key_count 报 0 就是这么来的。
        let whole = tx.prefix_scan(b"k");
        assert_eq!(whole.len() as u64, KEYS, "前缀 k 应命中全部 {KEYS} 行");
        assert_eq!(tx.key_count(b"k") as u64, KEYS, "key_count 同一个形状");
        tx.rollback();
    }
    cleanup(&path);
}

/// M50 的量具重做 + 结构归因（`#[ignore]`，手动 release 单线程跑）。
///
/// 为什么要重做量具：10-20 那张三档表里 `seek_prev 中间键` 读的是 `timed()` 里的**一次调用**
/// （`src/engine.rs:3522`）。µs 级单次采样在这台机上不能当 A/B 凭据（本机规则：**µs 级 A/B
/// 必须交错多轮取最小**），而一次反向定位很容易 just 撞上"某个 SST 块不在缓存里"
/// （600k 档点读实测 块/次 5.31、未命中/次 1.31、一次未命中 ≈ 60 µs）
/// ⇒ "20k 27.1 µs → 600k 351.1 µs = 12.96×" 里多少是结构、多少是单次运气，那张表答不了。
///
/// 这条台架把**同一个动作量 N 次**，并一次给出五把结构尺子（全是计数，不是墙钟）：
/// `步/次`（M20 那把 `seek_prev_steps`：一次调用往回退过几把死键）、`探文件/次`、`块/次`、
/// `未命中/次`、`条目比较/次`。哪一把跟着规模涨，责任就在哪一层：
/// 步数涨 ⇒ 反向定位真在跳更多死键（结构问题，要修）；探文件涨 ⇒ 盘上形状变了；
/// 只有 未命中/次 涨 ⇒ 是覆盖率，M23/M32 那本账，不是这一刀。
///
/// 两臂只差一个现成旋钮（M32 的手法：**转旋钮、不动代码**）：
/// `默认` = `for_mvcc()`（M41b 之后 `mem_threshold_bytes` 读**真内存** ⇒ 8 B 值的库每 ~7.4 万行 flush）；
/// `大 memtable` = 字节档抬到不触发、条数档留在 262 144 ⇒ 就是 **M41b 之前**的行为
/// （当时 32 MB 载荷账在 8 B 值上从不触发，flush 只由条数说话 ⇒ 60 万行只 flush ~2 次）。
/// 两臂都先报**盘上形状**（每层文件数 / SST 总数 / 块数 / 缓存容量）：形状没变，
/// "351 µs 是 M41b 换来的形状代价"这句话就还没有对照。
///
/// ```text
/// LSM_SEEK_SHAPE_AB_ROWS=600000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 lsm_seek_prev_shape_ab
/// ```
#[test]
#[ignore = "seek_prev 的量具重做 + memtable 形状 A/B，手动 release 单线程跑：LSM_SEEK_SHAPE_AB_ROWS=600000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 lsm_seek_prev_shape_ab"]
fn lsm_seek_prev_shape_ab() {
    use crate::common::store::seek_prev_steps;
    use crate::lsm_tree::lsm_tree::LSMTreeOptions;
    use crate::lsm_tree::sst::{block_misses, block_reads, sst_entry_walks, sst_probes};
    let rows: usize = std::env::var("LSM_SEEK_SHAPE_AB_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200_000);
    let probes: usize = std::env::var("LSM_SEEK_SHAPE_AB_PROBES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2000);
    let key = |i: usize| format!("k{i:06}").into_bytes();
    // 照抄基线台架的形状：一把键 8 B、值 1 B、一个大事务灌进去、flush + checkpoint、冷重开
    let snap = || {
        (
            block_reads(),
            block_misses(),
            sst_probes(),
            sst_entry_walks(),
        )
    };

    for big_mem in [false, true] {
        let name = if big_mem {
            "大 memtable(照 M41b 前)"
        } else {
            "默认(M41b 后)"
        };
        let opts = || {
            let mut o = LSMTreeOptions::for_mvcc();
            if big_mem {
                o.mem_threshold_bytes = usize::MAX / 8;
                o.mem_threshold_entries = 256 * 1024;
            }
            o
        };
        let path = tmp_db("seek_shape_ab");
        {
            let mvcc = MVCC::try_open_with(&path, Some(opts())).unwrap();
            let tx = mvcc.begin_transaction();
            for i in 0..rows {
                assert!(tx.set(&key(i), b"v".to_vec()), "灌第 {i} 把失败");
            }
            tx.commit();
            mvcc.flush();
            mvcc.checkpoint();
        }
        // ⚠️ 必须冷重开再量：写阶段焐热的块缓存会让"反向一步"看起来比生产便宜
        let mvcc = MVCC::try_open_with(&path, Some(opts())).unwrap();
        let (st, cap, blocks) = {
            let kv = mvcc.kv.lock().unwrap();
            (
                kv.stats(),
                kv.block_cache_capacity(),
                kv.sst_blocks_for_test(),
            )
        };
        eprintln!(
            "[形状] 臂={name} 行数 {rows} ⇒ SST {} 篇、层文件 {:?}、块 {blocks}、缓存容量 {cap}",
            st.total_sst, st.level_files
        );
        let mut best = u128::MAX;
        let mut cold = 0u128;
        let mut c_best = (0u64, 0u64, 0u64, 0u64);
        let mut steps_best = 0u64;
        for round in 0..2 {
            let tx = mvcc.begin_transaction();
            let a = snap();
            let p0 = seek_prev_steps();
            let t = std::time::Instant::now();
            for j in 0..probes {
                let i = (j * 7919 + 13) % rows;
                assert!(tx.seek_prev(&key(i)).is_some(), "seek_prev 第 {i} 把落空");
            }
            let spent = t.elapsed().as_micros();
            let b = snap();
            let steps = seek_prev_steps() - p0;
            drop(tx);
            if round == 0 {
                cold = spent;
            } else if spent < best {
                best = spent;
                c_best = (b.0 - a.0, b.1 - a.1, b.2 - a.2, b.3 - a.3);
                steps_best = steps;
            }
        }
        let per = |x: u64| x as f64 / probes as f64;
        eprintln!(
            "[seek_prev A/B] 臂={name} {probes} 次 ⇒ 冷启一趟 {:>7.1} µs/次、焐热一趟 {:>7.1} µs/次 \
                 | 步/次 {:.2} | 探文件/次 {:.2} | 块/次 {:.2} | 未命中/次 {:.2} | 条目比较/次 {:.1}",
            cold as f64 / probes as f64,
            best as f64 / probes as f64,
            steps_best as f64 / probes as f64,
            per(c_best.2),
            per(c_best.0),
            per(c_best.1),
            per(c_best.3),
        );
        let _ = std::fs::remove_dir_all(&path);
    }
}
