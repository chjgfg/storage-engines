// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn test_scan_prefix_seek() {
    let mvcc = MVCC::new();
    let tx = mvcc.begin_transaction();
    let rows: &[(&[u8], &[u8])] = &[
        (b"a1", b"1"),
        (b"a2", b"2"),
        (b"b1", b"3"),
        (b"order_01", b"o1"),
        (b"order_02", b"o2"),
        (b"z", b"9"),
    ];
    for &(k, v) in rows {
        assert!(tx.set(k, pack_plain(v)));
    }
    tx.commit();

    let tx = mvcc.begin_transaction();
    let range = tx.scan(Some(b"a1"), Some(b"b1"));
    let keys: Vec<_> = range.iter().map(|r| r.key.as_slice()).collect();
    assert_eq!(keys, vec![b"a1".as_slice(), b"a2"]);

    let pref = tx.prefix_scan(b"order_");
    assert_eq!(pref.len(), 2);
    assert_eq!(pref[0].key, b"order_01");
    assert_eq!(pref[1].key, b"order_02");

    let rev = tx.reverse_scan(Some(b"a1"), Some(b"b2"));
    assert_eq!(rev[0].key, b"b1");

    let s = tx.seek(b"a2").unwrap();
    assert_eq!(s.key, b"a2");
    let s2 = tx.seek(b"a15").unwrap();
    assert_eq!(s2.key, b"a2");

    let p = tx.seek_prev(b"a2").unwrap();
    assert_eq!(p.key, b"a2");
    let p2 = tx.seek_prev(b"a15").unwrap();
    assert_eq!(p2.key, b"a1");

    assert!(tx.exists(b"order_01"));
    assert!(!tx.exists(b"nope"));
    assert_eq!(tx.key_count(b"order_"), 2);

    let meta = tx.get_meta(b"a1").unwrap();
    assert_eq!(meta.value_len, Some(1));
    assert!(!meta.deleted);
    assert!(!meta.expired);
    tx.commit();
}

#[test]
fn test_search_keys_fuzzy_and_paginate() {
    let mvcc = MVCC::new();
    let tx = mvcc.begin_transaction();
    let rows: &[(&[u8], &[u8])] = &[
        (b"apple", b"1"),
        (b"apply", b"2"),
        (b"banana", b"3"),
        (b"grape", b"4"),
        (b"user:1", b"u1"),
        (b"user:2", b"u2"),
        (b"user:10", b"u10"),
        (b"zebra", b"z"),
    ];
    for &(k, v) in rows {
        assert!(tx.set(k, v.to_vec()));
    }
    assert!(tx.delete(b"grape"));
    tx.commit();

    let p0 = mvcc.search_keys(&SearchQuery::contains(b"pp", 0, 10));
    assert_eq!(p0.total, Some(2));
    assert_eq!(p0.total_pages, Some(1));
    assert_eq!(p0.items.len(), 2);
    assert_eq!(p0.items[0].key, b"apple");
    assert_eq!(p0.items[1].key, b"apply");
    assert!(!p0.has_next());

    let pref = mvcc.search_keys(&SearchQuery::prefix(b"user:", 0, 10));
    assert_eq!(pref.total, Some(3));
    let keys: Vec<_> = pref.items.iter().map(|r| r.key.as_slice()).collect();
    assert_eq!(keys, vec![b"user:1".as_slice(), b"user:10", b"user:2"]);

    let all = mvcc.search_keys(&SearchQuery::contains(Vec::<u8>::new(), 0, 3));
    assert_eq!(all.total, Some(7));
    assert_eq!(all.total_pages, Some(3));
    assert_eq!(all.items.len(), 3);
    assert!(all.has_next());
    assert!(!all.has_prev());

    let p1 = mvcc.search_keys(&SearchQuery::contains(Vec::<u8>::new(), 1, 3));
    assert_eq!(p1.items.len(), 3);
    assert!(p1.has_next());
    assert!(p1.has_prev());

    let p2 = mvcc.search_keys(&SearchQuery::contains(Vec::<u8>::new(), 2, 3));
    assert_eq!(p2.items.len(), 1);
    assert!(!p2.has_next());
    assert_eq!(p2.page, 2);

    let p9 = mvcc.search_keys(&SearchQuery::contains(b"user:", 99, 2));
    assert_eq!(p9.total, Some(3));
    assert!(p9.items.is_empty());

    let with_del = mvcc.search_keys(&SearchQuery::contains(b"rape", 0, 10).with_deleted(true));
    assert_eq!(with_del.total, Some(1));
    assert!(with_del.items[0].value.is_none());

    let t2 = mvcc.begin_transaction();
    assert!(t2.set(b"user:draft", b"d".to_vec()));
    let mine = t2.search_keys(&SearchQuery::prefix(b"user:", 0, 20));
    assert_eq!(mine.total, Some(4));
    assert!(mine.items.iter().any(|r| r.key == b"user:draft"));
    t2.rollback();
}

/// 不要精确 total 的翻页，代价必须只看**窗口宽**，不看库有多大（M15 的验收）。
///
/// 以前 `search_keys` 为了报一个 `total` 把整库折一遍 ⇒ 首页与末页一样贵
/// （实测 20000 键时首页也要 100 ms 量级）。尺子是本引擎的折叠计数 `fold_keys()`。
/// 顺序是**先量窗口、再量全量**：后跑的那次缓存更热，把偏差留给"全量更贵"那一侧，
/// 这条断言只会因此更难通过，不会反过来骗人。
/// M21 守卫：**翻页的代价要跟着页宽走，不能跟着库大小走** —— 而且必须测"后面的页"。
///
/// 首页从库头起，游标 `after` 就算没下推它也一样便宜 ⇒ 只测首页会一路绿灯（bitcask 那条
/// `scan_page_costs_the_page_and_walks_the_whole_library_once` 量的就是首页，这个洞在它眼皮
/// 底下活了一整个版本）。所以这里量**最后一页**，并要求它与首页同阶。
/// 尺子用本引擎自己的（`fold_keys()`（比过多少把编码键）），判完代价还判**等价**：最后一页的起点必须正好是全库
/// 第 `(pages-1)×PAGE` 把键 —— 只测便宜不测内容，就是拿错数据换性能。
#[test]
fn test_scan_page_later_pages_cost_the_page_not_the_library() {
    const N: usize = 2000;
    const PAGE: usize = 50;
    {
        let mvcc = MVCC::new();
        {
            let tx = mvcc.begin_transaction();
            for i in 0..N {
                assert!(tx.set(format!("k{i:05}").as_bytes(), b"x".to_vec()));
            }
            tx.commit();
        }

        let b0 = fold_keys();
        let (_first, mut cur) = {
            let tx = mvcc.begin_transaction();
            let p = tx.scan_page(None, PAGE);
            tx.commit();
            p
        };
        let first_cost = fold_keys() - b0;

        let mut pages = 1usize;
        let mut last_rows = Vec::new();
        // **只量最后一页**：把整段翻页的总和当一页看，会把 39 页的成本当成一页去断言
        let mut last_cost = 0u64;
        while let Some(c) = cur {
            let b1 = fold_keys();
            let tx = mvcc.begin_transaction();
            let (rows, next) = tx.scan_page(Some(&c), PAGE);
            tx.commit();
            last_cost = fold_keys() - b1;
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
    }
}

#[test]
fn test_search_window_costs_the_window_not_the_library() {
    const N: usize = 2000;
    const PAGE: usize = 20;

    let mvcc = MVCC::new();
    let tx = mvcc.begin_transaction();
    for i in 0..N {
        assert!(tx.set(format!("user{i:05}").as_bytes(), b"x".to_vec()));
    }
    tx.commit();

    let base = fold_keys();
    let w = mvcc.search_keys(&SearchQuery::contains(b"user", 0, PAGE).without_total());
    let window_keys = fold_keys() - base;

    let base = fold_keys();
    let full = mvcc.search_keys(&SearchQuery::contains(b"user", 0, PAGE));
    let full_keys = fold_keys() - base;

    // 尺子的对照臂（M75b）：`key_count` 走 [`crate::common::count_window`] = **一趟全界**，
    // 是"数一个数最少要付多少"的现成基线。旧判据 `window_keys * 2 <= full_keys` 把这个
    // 守卫要钉的东西写成了"**两趟**"（窗口一趟 + 计数一趟 = 2N），合成一趟之后它必然红 ⇒
    // 换成"要 total 的成本 ≤ 那一趟全界 + 一页"。改回两步会读到 2 倍，这条就红。
    let yard = mvcc.begin_transaction();
    let base = fold_keys();
    let counted = yard.key_count(b"user");
    let count_keys = fold_keys() - base;

    assert_eq!(full.total, Some(N), "要 total 时语义与改前完全一致");
    assert_eq!(w.total, None, "没数就不许报一个数出来");
    assert_eq!(w.total_pages, None);
    assert_eq!(w.items.len(), PAGE);
    assert_eq!(
        w.items, full.items,
        "窗口必须是全量结果的前缀：分页语义一个字节都不许变"
    );
    assert!(w.has_next(), "后面还有 {} 条，必须报得出来", N - PAGE);
    assert_eq!(
        counted, N,
        "对照臂数出来的条数不对 ⇒ 对照没建立，下面那条不算数"
    );
    eprintln!(
        "[search 窗口] 折了 {window_keys} 把键就拿回首页；要精确 total 折了 {full_keys} 把；\
             对照臂 `key_count` 一趟全界折 {count_keys} 把（N={N}）"
    );
    assert!(
        full_keys <= count_keys + (2 * PAGE) as u64,
        "要 total 折了 {full_keys} 把键，而『只数不取』那一趟全界只折 {count_keys} 把 ⇒ \
             取页与数数还是各走一趟（M75b 合成一趟后这两个数应当同量）"
    );

    // 最后一面：探不到"多出来的那一条"就必须如实报"没有下一页"（既不许把"没数"
    // 说成"到底了"，也不许反过来吓唬人）
    let last = N / PAGE - 1;
    let w2 = mvcc.search_keys(&SearchQuery::contains(b"user", last, PAGE).without_total());
    let f2 = mvcc.search_keys(&SearchQuery::contains(b"user", last, PAGE));
    assert_eq!(w2.items.len(), PAGE);
    assert_eq!(w2.items, f2.items, "同一页的内容与要不要 total 无关");
    assert!(!w2.has_next(), "最后一页之后没有东西了");
}

#[test]
fn test_scan_foreach_early_stop() {
    let mvcc = MVCC::new();
    let tx = mvcc.begin_transaction();
    for &(k, v) in &[
        (&b"k1"[..], &b"1"[..]),
        (&b"k2"[..], &b"2"[..]),
        (&b"k3"[..], &b"3"[..]),
    ] {
        assert!(tx.set(k, pack_plain(v)));
    }
    tx.commit();

    let tx = mvcc.begin_transaction();
    let mut seen: Vec<Vec<u8>> = Vec::new();
    tx.scan_foreach(None, None, |rec| {
        seen.push(rec.key);
        seen.len() < 2
    });
    assert_eq!(seen, vec![b"k1".to_vec(), b"k2".to_vec()]);

    let mut pref: Vec<Vec<u8>> = Vec::new();
    tx.prefix_scan_foreach(b"k", |rec| {
        pref.push(rec.key);
        true
    });
    assert_eq!(pref.len(), 3);
    tx.commit();
}
