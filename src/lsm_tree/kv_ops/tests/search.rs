// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn test_scan_prefix_seek() {
    let path = tmp("scan");
    {
        let mvcc = MVCC::open(&path);
        let tx = mvcc.begin_transaction();
        for (k, v) in [
            (&b"order:01"[..], &b"a"[..]),
            (&b"order:02"[..], &b"b"[..]),
            (&b"order:10"[..], &b"c"[..]),
            (&b"user:1"[..], &b"u"[..]),
        ] {
            assert!(tx.set(k, pack_plain(v)));
        }
        tx.commit();

        let tx = mvcc.begin_transaction();
        let s = tx.prefix_scan(b"order:");
        assert_eq!(s.len(), 3);
        assert_eq!(s[0].key, b"order:01");
        let one = tx.seek(b"order:05").unwrap();
        assert_eq!(one.key, b"order:10");
        let prev = tx.seek_prev(b"order:05").unwrap();
        assert_eq!(prev.key, b"order:02");
        assert_eq!(tx.key_count(b"order:"), 3);
        tx.commit();
    }
    cleanup(&path);
}

/// 不要精确 total 的翻页，代价必须只看**窗口宽**，不看库有多大（M15 的验收）。
///
/// 以前 `search_keys` 为了报一个 `total` 把整库折一遍 ⇒ 首页与末页一样贵
/// （实测 20000 键时首页也要 100 ms 量级）。尺子是本引擎自己的代价单位。
/// 顺序是**先量窗口、再量全量**：后跑那次缓存更热，偏差只会让断言更难通过，不会骗人。
/// M21 守卫：**翻页的代价要跟着页宽走，不能跟着库大小走** —— 而且必须测"后面的页"。
///
/// 首页从库头起，游标 `after` 就算没下推它也一样便宜 ⇒ 只测首页会一路绿灯（bitcask 那条
/// `scan_page_costs_the_page_and_walks_the_whole_library_once` 量的就是首页，这个洞在它眼皮
/// 底下活了一整个版本）。所以这里量**最后一页**，并要求它与首页同阶。
/// 尺子用本引擎自己的（`light_visits()`（归并吐出多少把键）），判完代价还判**等价**：最后一页的起点必须正好是全库
/// 第 `(pages-1)×PAGE` 把键 —— 只测便宜不测内容，就是拿错数据换性能。
#[test]
fn test_scan_page_later_pages_cost_the_page_not_the_library() {
    const N: usize = 2000;
    const PAGE: usize = 50;
    let path = tmp("scan_page_later");
    {
        let mvcc = MVCC::open(&path);
        {
            let tx = mvcc.begin_transaction();
            for i in 0..N {
                assert!(tx.set(format!("k{i:05}").as_bytes(), pack_plain(b"v")));
            }
            tx.commit();
        }
        let _ = mvcc.flush();
        let b0 = crate::lsm_tree::sst::light_visits();
        let (_first, mut cur) = {
            let tx = mvcc.begin_transaction();
            let p = tx.scan_page(None, PAGE);
            tx.commit();
            p
        };
        let first_cost = crate::lsm_tree::sst::light_visits() - b0;

        let mut pages = 1usize;
        let mut last_rows = Vec::new();
        // **只量最后一页**：把整段翻页的总和当一页看，会把 39 页的成本当成一页去断言
        let mut last_cost = 0u64;
        while let Some(c) = cur {
            let b1 = crate::lsm_tree::sst::light_visits();
            let tx = mvcc.begin_transaction();
            let (rows, next) = tx.scan_page(Some(&c), PAGE);
            tx.commit();
            last_cost = crate::lsm_tree::sst::light_visits() - b1;
            last_rows = rows;
            pages += 1;
            cur = next;
            assert!(pages <= N / PAGE + 2, "游标不前进？已 {pages} 页");
        }

        let want = (pages - 1) * PAGE;
        assert!(!last_rows.is_empty(), "最后一页不能是空的");
        {
            let tx = mvcc.begin_transaction();
            let all = tx.scan(None, None);
            tx.commit();
            assert_eq!(all.len(), N, "全库应折出 {N} 行");
            assert_eq!(
                last_rows[0].key, all[want].key,
                "第 {pages} 页的起点不是全库第 {want} 把键 ⇒ 翻页漏行或重行"
            );
            assert_eq!(
                all[want + last_rows.len() - 1].key,
                last_rows.last().unwrap().key,
                "最后一页的终点也对不上"
            );
        }
        eprintln!(
            "[scan_page 翻页] 首页 {first_cost} / 最后一页 {last_cost}（页宽 {PAGE}，共 {pages} 页，N={N}）"
        );
        assert!(
            last_cost <= 2 * (first_cost + PAGE as u64),
            "最后一页付了 {last_cost}，首页只 {first_cost} ⇒ 游标 `after` 没下推成迭代起点，\
                 每页都从库头重走（翻完整库 = O(库²/页宽)）"
        );
        cleanup(&path);
    }
}

#[test]
fn test_search_window_costs_the_window_not_the_library() {
    const N: usize = 2000;
    const PAGE: usize = 20;
    let path = tmp("search_window");
    {
        let mvcc = MVCC::open(&path);
        let tx = mvcc.begin_transaction();
        for i in 0..N {
            assert!(
                tx.set(format!("user{i:05}").as_bytes(), pack_plain(b"x")),
                "第 {i} 行没写进去"
            );
        }
        tx.commit();
        let _ = mvcc.flush();

        let vb = crate::lsm_tree::sst::light_visits();
        let bb = crate::lsm_tree::sst::block_reads();
        let w = mvcc.search_keys(&SearchQuery::contains(b"user", 0, PAGE).without_total());
        let window_cost = crate::lsm_tree::sst::light_visits() - vb;
        let window_blocks = crate::lsm_tree::sst::block_reads() - bb;

        let vb = crate::lsm_tree::sst::light_visits();
        let bb = crate::lsm_tree::sst::block_reads();
        let full = mvcc.search_keys(&SearchQuery::contains(b"user", 0, PAGE));
        let full_cost = crate::lsm_tree::sst::light_visits() - vb;
        let full_blocks = crate::lsm_tree::sst::block_reads() - bb;

        // 尺子的对照臂（M75b）：`key_count` 走 [`crate::common::count_window`] = **一趟全界**，
        // 是"数一个数最少要付多少"的现成基线。旧判据 `window_cost * 2 <= full_cost` 把这个
        // 守卫要钉的东西写成了"**两趟**"（窗口一趟 + 计数一趟），合成一趟之后它必然红 ⇒
        // 换成"要 total 的成本 ≤ 那一趟全界 + 一页"。改回两步会读到近 2 倍，这条就红。
        let yard = mvcc.begin_transaction();
        let vb = crate::lsm_tree::sst::light_visits();
        let counted = yard.key_count(b"user");
        let count_cost = crate::lsm_tree::sst::light_visits() - vb;

        assert_eq!(full.total, Some(N), "要 total 时语义与改前完全一致");
        assert_eq!(w.total, None, "没数就不许报一个数出来");
        assert_eq!(w.total_pages, None);
        assert_eq!(w.items.len(), PAGE);
        assert_eq!(
            w.items, full.items,
            "窗口必须是同一页的全量结果：分页语义一个字节都不许变"
        );
        assert!(w.has_next(), "后面还有 {} 条，必须报得出来", N - PAGE);
        assert_eq!(
            counted, N,
            "对照臂数出来的条数不对 ⇒ 对照没建立，下面那条不算数"
        );
        eprintln!(
            "[search 窗口] 首页 {window_cost} 把键 / {window_blocks} 块；全量 {full_cost} 把 / {full_blocks} 块；\
                 对照臂 `key_count` 一趟全界 {count_cost} 把（N={N}）"
        );

        assert!(
            full_cost > 0 && full_cost <= count_cost + (2 * PAGE) as u64,
            "要 total 走了 {full_cost} 把键，而『只数不取』那一趟全界只走 {count_cost} 把 ⇒ \
                 取页与数数还是各走一趟（M75b 合成一趟后这两个数应当同量）"
        );
        // 块读也要跟着窗口走。**这条才是 M15b 的守卫**：上一行只数"归并吐出多少键",
        // 而旧的 `scan_latest_light_range` 先把每个源整篇物化 ⇒ 键数看着对了,块却读满全库
        // （实测首页 53 块,而这库总共才 32 块）。判据取"每交付一把键最多两块"——
        // 内含每个源启动一块的余量,改回 eager 就会红。
        assert!(
            window_blocks <= 2 * (window_cost + 1),
            "首页交付 {window_cost} 把键却读了 {window_blocks} 个块 ⇒ 源被整篇物化了，                 不是惰性拉取（改回 `scan_latest_light_range` 正是这个形状）"
        );

        let last = N / PAGE - 1;
        let w2 = mvcc.search_keys(&SearchQuery::contains(b"user", last, PAGE).without_total());
        let f2 = mvcc.search_keys(&SearchQuery::contains(b"user", last, PAGE));
        assert_eq!(w2.items.len(), PAGE);
        assert_eq!(w2.items, f2.items, "同一页的内容与要不要 total 无关");
        assert!(!w2.has_next(), "最后一页之后没有东西了");
    }
    cleanup(&path);
}

#[test]
fn test_search_pagination() {
    let path = tmp("search");
    {
        let mvcc = MVCC::open(&path);
        let tx = mvcc.begin_transaction();
        for k in [&b"apple"[..], &b"apply"[..], &b"banana"[..], &b"user:1"[..]] {
            assert!(tx.set(k, pack_plain(b"v")));
        }
        tx.commit();

        let page = mvcc.search_keys(&SearchQuery::contains(b"pp", 0, 10));
        assert_eq!(page.total, Some(2));
        assert_eq!(page.items.len(), 2);

        let p0 = mvcc.search_keys(&SearchQuery::prefix(b"user:", 0, 1));
        assert_eq!(p0.total, Some(1));
        assert!(!p0.has_next());
    }
    cleanup(&path);
}
