// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// ② 全库折叠：value 解码次数必须只与**存活 key 数**相关，不随历史行数增长。
///
/// 诚实边界：B+ 树没有独立键索引，整表走一遍的**页读取**省不掉，这条不守页读取；
/// 它守的是「不再对每一行都做 value 解码」—— 内联值省一次 Vec 分配，
/// 大 value 省一次 blob 读盘（后者是真 IO）。
#[test]
fn test_collect_latest_decodes_only_live_keys() {
    const KEYS: u64 = 8;
    const VERSIONS: u64 = 120;

    let path = tmp_db("collect_decode");
    {
        let mvcc = MVCC::open(&path, 4, 16);
        for v in 0..VERSIONS {
            for k in 0..KEYS {
                let key = format!("k{k}");
                let tx = mvcc.begin_transaction();
                assert!(tx.set(key.as_bytes(), format!("v{v}").into_bytes()));
                tx.commit();
            }
        }

        let tx = mvcc.begin_transaction();
        let base = crate::bplus_tree::storage::value_decodes();
        let recs = tx.export_latest_visible(false);
        let used = crate::bplus_tree::storage::value_decodes() - base;
        eprintln!(
            "折叠 value 解码次数：{used}（存活 key {KEYS}，历史行数 {}）",
            KEYS * VERSIONS
        );
        assert_eq!(recs.len() as u64, KEYS, "存活 key 数应等于 {KEYS}");
        assert!(
            used <= KEYS + 4,
            "只应给胜出行解码：实际 {used} 次，而历史行数是 {}",
            KEYS * VERSIONS
        );
        tx.rollback();
    }
    cleanup(&path);
}

/// 窗口 scan 的代价必须随**窗口宽度**缩 —— B+ 树的尺子是「读了多少页」。
///
/// 旧形状：`collect_latest_raw()` 从最左叶子一路走完**全表**，调用方再逐条筛界
/// ⇒ 10 行的窗口和 3000 行的全量读一样多的页（大库基线实测：窗口 scan 3290 µs/行，
/// 而同库一次全量 scan 是 32.9 µs/行）。现在起点下降到 containing 叶子（前面的叶子
/// 一篇都不读）、终点靠回调喊停。
///
/// B+ 树没有 Bitcask KeyDir 那样的独立键索引，"窗口覆盖到的那几页要反序列化"省不掉 ——
/// 这条省的是**页数**，不是每页的解析成本。断言两半：① 窗口结果与"全量之后筛"逐条一致
/// （界判得对，不是筛错换来快）；② 页数随窗口缩。
/// `scan_foreach` 提前叫停时，**不许先把整库折一遍**（M27）。
///
/// 这条路径的文档从写下那天起就声称「不收集全库到单一大 Vec」，而实现是
/// `for … in collect_latest_raw_in(...)` —— 那一句会把界内每一行都先折出来，再逐条搬成
/// `ExportRecord`。200k 键实测因此比结果集本身还多占 ≈ 15 MB（`一次全量 scan`
/// 峰值 +45.5 MB / 常驻 30.7 MB）。现在走共用件 `fold_rows_streaming`：折一行交一行，
/// 叫停之后的键不再碰。
///
/// 判据用**值解码次数**：B+ 树的值就住在叶子条目里，折出来那一步顺手把它解码 ——
/// 所以老写法是每把键一次，新写法只在真要交出去那一行才解。
#[test]
fn test_scan_foreach_stops_without_folding_the_library() {
    const N: usize = 2000;
    fn ruler() -> u64 {
        crate::bplus_tree::storage::value_decodes()
    }
    let path = tmp_db("foreach_stop");
    {
        // order 64 = 默认档；页缓存 0 = 自动档（M22）
        let mvcc = MVCC::open(&path, 64, 0);
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
        assert!(full >= N as u64, "值解码尺子在空转（全量只解 {full} 次）");
        assert!(
            early <= 2,
            "只要 1 行却解了 {early} 次值（老写法：先把全库 {N} 行折出来）"
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
        let mvcc = MVCC::open(&path, 64, 256);
        for i in 0..KEYS {
            let tx = mvcc.begin_transaction();
            assert!(tx.set(format!("k{i:05}").as_bytes(), format!("v{i}").into_bytes()));
            tx.commit();
        }

        let tx = mvcc.begin_transaction();
        let before = crate::bplus_tree::page::page_reads();
        let all = tx.scan(None, None);
        let full = crate::bplus_tree::page::page_reads() - before;

        let lo = b"k01000".to_vec();
        let hi = b"k01010".to_vec();
        let before = crate::bplus_tree::page::page_reads();
        let win = tx.scan(Some(&lo), Some(&hi));
        let windowed = crate::bplus_tree::page::page_reads() - before;

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
            "10 行窗口的页数不该是全库量级（{windowed} vs 全量 {full}）"
        );

        // 上界哨兵的陷阱形状（`seek_prev` 造的界是 `key ‖ 0x00`）：这条在内存引擎先红过
        assert_eq!(tx.seek_prev(b"k01005").unwrap().key, b"k01005");
        assert_eq!(tx.seek(b"k01000").unwrap().key, b"k01000");

        // 前缀同理：k001xx ⇒ 100 个键
        let before = crate::bplus_tree::page::page_reads();
        let pre = tx.prefix_scan(b"k001");
        let prefixed = crate::bplus_tree::page::page_reads() - before;
        assert_eq!(pre.len(), 100, "前缀 k001 应命中 100 行");
        assert!(
            prefixed * 4 <= full,
            "前缀扫的页数不该是全库量级（{prefixed} vs 全量 {full}）"
        );

        // 边界形状：前缀是**所有键的真前缀**（比第一个键还短 ⇒ 下降到最左叶子）。
        // lsm 曾把这种扫法做成空结果，四家都钉一遍。
        let whole = tx.prefix_scan(b"k");
        assert_eq!(whole.len() as u64, KEYS, "前缀 k 应命中全部 {KEYS} 行");
        assert_eq!(tx.key_count(b"k") as u64, KEYS, "key_count 同一个形状");
        tx.rollback();
    }
    cleanup(&path);
}

/// 0.057 页/行、1.7 µs/行（同一份 20000 行的库）。
/// 断言留了大余量（每 10 行不许超过 1 次页访问）：旧实现会红 30 倍以上。
#[test]
fn test_full_scan_pages_scale_with_leaves_not_with_keys_times_height() {
    const KEYS: u64 = 2000;
    let path = tmp_db("scan_pages");
    {
        let mvcc = MVCC::open(&path, 64, 1024);
        for i in 0..KEYS {
            let tx = mvcc.begin_transaction();
            assert!(tx.set(format!("k{i:05}").as_bytes(), format!("v{i}").into_bytes()));
            tx.commit();
        }

        let tx = mvcc.begin_transaction();
        let before = crate::bplus_tree::page::page_reads();
        let rows = tx.scan(None, None);
        let pages = crate::bplus_tree::page::page_reads() - before;
        assert_eq!(rows.len() as u64, KEYS);
        eprintln!("一次全量 scan：{KEYS} 行用了 {pages} 次页访问");
        assert!(
            pages * 10 <= KEYS,
            "{pages} 次页访问对 {KEYS} 行来说太多：折叠不该按行再降一次树（旧实现 3 页/行）"
        );
    }
    cleanup(&path);
}
