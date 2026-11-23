// 该文件集中验证对应职责，保留原测试名称和字面值。

#[test]
fn last_page_fallback_bounds_results_with_dead_tail_and_binary_keys() {
    use std::collections::BTreeMap;
    const PAGE: usize = 7;
    for name in ["memory", "bitcask", "bplus_tree"] {
        let dir = tmp_dir("last_page_fallback");
        let db = open_engine(name, Some(&dir)).unwrap();
        db.set_vacuum_auto_slice(0);
        let mut expected = BTreeMap::new();
        {
            let tx = db.begin();
            for i in 0u16..512 {
                let mut key = vec![b'a'];
                key.extend_from_slice(&i.to_be_bytes());
                let value = i.to_le_bytes().to_vec();
                tx.try_set_raw(&key, value.clone()).unwrap();
                expected.insert(key, value);
            }
            tx.try_set_raw(b"a", vec![0, 65]).unwrap();
            expected.insert(b"a".to_vec(), vec![0, 65]);
            for i in 0..9000 {
                let key = format!("z{i:05}");
                if i % 2 == 0 {
                    tx.try_delete(key.as_bytes()).unwrap();
                } else {
                    tx.try_set(key.as_bytes(), crate::common::pack_ttl(b"expired", 1))
                        .unwrap();
                }
            }
            tx.try_commit().unwrap();
        }
        let tx = db.begin();
        // 当前快照不能看到二进制前缀在后续事务中写入的新版本。
        for _ in 0..70 {
            let writer = db.begin();
            writer.try_set_raw(b"a", b"new".to_vec()).unwrap();
            writer.try_commit().unwrap();
        }
        crate::common::order::take_tail_buffer_peak();
        let control = tx.search_last_page(20_000);
        assert_eq!(
            crate::common::order::take_tail_buffer_peak(),
            expected.len()
        );
        assert_eq!(control.items.len(), expected.len());
        assert_eq!(
            control
                .items
                .iter()
                .map(|r| (r.key.clone(), r.value.clone().unwrap()))
                .collect::<BTreeMap<_, _>>(),
            expected
        );
        drop(control);
        let want: Vec<_> = expected
            .iter()
            .rev()
            .take(PAGE)
            .rev()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let f0 = crate::common::back_n_fallbacks();
        let page = tx.search_last_page(PAGE);
        assert_eq!(
            crate::common::back_n_fallbacks() - f0,
            1,
            "{name}: fallback was not exercised"
        );
        assert_eq!(
            page.items
                .iter()
                .map(|r| (r.key.clone(), r.value.clone().unwrap()))
                .collect::<Vec<_>>(),
            want
        );
        let peak = crate::common::order::take_tail_buffer_peak();
        assert_eq!(peak, PAGE, "{name}: fallback retained {peak} records");
        assert_eq!(
            tx.get(b"a"),
            Some(vec![0, 65]),
            "old snapshot lost its version"
        );
    }
}

/// M51 的守卫：**深翻到末页不许把前面每一页都攒在堆上**（复测抓到的那格：600k 档
/// `search 末页(深翻)` 瞬时峰值 **188 MB**、四家同形，比"一次全量 scan"的 123.7 MB 还高）。
///
/// 改之前取页 = `search_window(limit = skip + 页宽 + 1)` 再切片 ⇒ 第 159 页要先把前 7 950 行
/// 连键带值折进一个 `Vec`。现在走流式门：跳过的行当场丢掉，堆上最多只有一页。
///
/// 判据 = **收集门攒了几行**（[`crate::common::collect_held_max`]，M44 那把尺子；线程局部的计数，
/// 不像进程高水位会被并行用例污染）：取页改走流式门之后这一格必须是 **0**，
/// 而旧形状（`search_window(limit = skip + 页宽 + 1)`）会读到 8001。
/// 先给尺子作证：同一趟里 `scan_page(None, ROWS)`（真走收集门）必须读到 ≈ 全库 ——
/// ⚠️ 我第一版的对照臂错用了 `prefix_scan`，它 M27 之后就逐片走了 ⇒ 读到 0，
/// 看着像"没攒"其实是"门不在这条路上"（读数 0 有两种解释，M38/M44 就是这个坑）。
///
/// **M75b 在这条守卫上加的两格**（`need_total` 从"两步"合成"一趟"之后）：
/// ① 用户字节只在这一页里才拷：末页读 `user_value_copies` 必须 ≤ 页宽+1。
///    对照臂换成**同一扇门**的"一页装得下全库"那一格（读到 8000），⚠️ 我第一版又踩了一次
///    同一个坑：拿 `scan_page` 作证 —— 它自己 `decode_value_meta` + `to_vec`，不经过
///    `logical_to_user` ⇒ 读数 0 是"门不在这条路上"。
///    变异：把 `seen <= skip` 那一支改成"照样剥壳" ⇒ 四家全红在 8000（＝这就是旧形状的量）。
/// ② 口径没分家：`total` 与 `key_count`（另一条只数不取的路）同数；`items`/`has_next`
///    与 `without_total()` 同数；删 3 把之后末页行数必须与 `total − skip` 自洽
///    （旧的两步里 `total` 与 `items` 来自两个瞬间，这种自洽是碰运气）。
#[test]
fn deep_search_pages_do_not_materialize_earlier_pages() {
    const ROWS: usize = 8000;
    const PAGE: usize = 50;
    let engines = open_all();
    let mut dirs = Vec::new();
    for (name, db, dir) in engines {
        dirs.push(dir);
        let last = ROWS / PAGE - 1;
        let tx = db.begin();
        for i in 0..ROWS {
            assert!(
                tx.set(format!("k{i:06}").as_bytes(), vec![b'v'; 24]),
                "{name}: 第 {i} 把键写失败"
            );
        }
        // 作证：这把尺子在真走收集门的那条路上读得到"整段"
        crate::common::reset_collect_held();
        let (many, _next) = tx.scan_page(None, ROWS);
        let held_door = crate::common::collect_held_max();
        assert_eq!(
            many.len(),
            ROWS,
            "{name}: scan_page 一次交不满全库 ⇒ 对照臂没建立"
        );
        assert!(
            held_door >= ROWS as u64,
            "{name}: 收集门在 scan_page(限 {ROWS}) 上只读到 {held_door} ⇒ 尺子没接在门上，\
                 下面那个 0 不算数"
        );

        // 判据：深翻末页不得再走收集门
        crate::common::reset_collect_held();
        let p0 = tx.search_keys(&SearchQuery::prefix(b"k", 0, PAGE));
        let held_p0 = crate::common::collect_held_max();
        crate::common::reset_collect_held();
        let pd = tx.search_keys(&SearchQuery::prefix(b"k", last, PAGE));
        let held_deep = crate::common::collect_held_max();
        eprintln!(
            "[深翻取页] {name}: {ROWS} 行 ÷ 页宽 {PAGE}｜对照臂 scan_page 收集门读到 {held_door}；\
                 第 0 页攒 {held_p0} 行、末页(第 {last} 页)攒 {held_deep} 行\
                 （旧形状那一臂末页要攒 {} 行）",
            last * PAGE + PAGE + 1
        );
        // 语义没动：两页都是满页、total 照给、末页第一行就是第 `last*PAGE` 行
        assert_eq!(p0.items.len(), PAGE, "{name}: 第 0 页不是满页");
        assert_eq!(pd.items.len(), PAGE, "{name}: 末页不是满页");
        assert_eq!(pd.total, Some(ROWS), "{name}: 末页的 total 不对");
        assert_eq!(
            pd.items[0].key,
            format!("k{:06}", last * PAGE).into_bytes(),
            "{name}: 末页起点跳错了 ⇒ `skip` 那一笔多跳/少跳"
        );
        assert!(!pd.has_next, "{name}: 末页还报有下一页");
        assert_eq!(
            held_deep, 0,
            "{name}: 末页这一趟在收集门攒了 {held_deep} 行 ⇒ 取页还在把前面每一页物化成 `Vec`"
        );
        assert_eq!(
            held_p0, 0,
            "{name}: 第 0 页也攒了 {held_p0} 行 ⇒ 流式门根本没生效"
        );

        // ---- M75b：要 total 的那条路现在是**一趟**折叠 ⇒ 用户字节只在这一页里才拷 ----
        // 先给尺子作证：对照臂必须走**同一扇门**（`search_page_walk` 里那支 `logical_to_user`），
        // 且形状要能读出大数 ⇒ 用"一页装得下全库"那一格。
        // ⚠️ 第一版的对照臂错用了 `scan_page` —— 它自己解信封（`decode_value_meta` + `to_vec`），
        // 根本不经过 `logical_to_user` ⇒ 读数 0 的解释是"门不在这条路上"而不是"没拷"
        // （M38/M44 那个坑，读数 0 有两种解释）。
        let base_probe = crate::common::user_value_copies();
        let wide = tx.search_keys(&SearchQuery::prefix(b"k", 0, ROWS));
        let probe_copies = crate::common::user_value_copies() - base_probe;
        assert_eq!(
            wide.items.len(),
            ROWS,
            "{name}: 对照臂那一页没装下全库 ⇒ 对照没建立"
        );
        assert!(
            probe_copies >= ROWS as u64,
            "{name}: 拷值尺子在取页那扇门上只读到 {probe_copies} ⇒ 尺子没接上，下面的数不算"
        );

        let base_copies = crate::common::user_value_copies();
        let pd_deep = tx.search_keys(&SearchQuery::prefix(b"k", last, PAGE));
        let copies = crate::common::user_value_copies() - base_copies;
        eprintln!(
            "[深翻拷值] {name}: 末页(第 {last} 页)拷 {copies} 条；对照臂(一页装全库)拷 {probe_copies} 条\
                 ＝跳过的行也剥壳那一臂的量（变异实测读回 {probe_copies}）"
        );
        assert_eq!(
            pd_deep.items, pd.items,
            "{name}: 同一份库两次取末页内容不一致"
        );
        assert!(
            copies <= (PAGE + 1) as u64,
            "{name}: 深翻末页拷了 {copies} 条用户值 ⇒ 又走回\"两趟全库\"或把跳过的行也拷了\
                 （一趟折叠只该拷 ≤ 页宽+1 = {}）",
            PAGE + 1
        );
        // 口径没分家：total 必须与"另一条只数不取的路"(`key_count` 走 `count_window`) 同数，
        // 而 items 必须与"免 total"那条同数
        assert_eq!(
            pd.total,
            Some(tx.key_count(b"k")),
            "{name}: search 的 total 与 key_count 不同数 ⇒ 两条可见性判定分家了"
        );
        let nt = tx.search_keys(&SearchQuery::prefix(b"k", last, PAGE).without_total());
        assert_eq!(nt.items, pd.items, "{name}: 要不要 total 取到了不同的页");
        assert_eq!(nt.has_next, pd.has_next, "{name}: 两条路 has_next 分家");

        // 墓碑也在同一条路上判：删 3 把之后 total 必须跟着掉（不是"数完再取"那两个瞬间的和）
        for i in [0usize, PAGE, last * PAGE] {
            assert!(
                tx.delete(format!("k{i:06}").as_bytes()),
                "{name}: 删 k{i} 失败"
            );
        }
        let after = tx.search_keys(&SearchQuery::prefix(b"k", last, PAGE));
        let tot = after.total.unwrap();
        assert_eq!(tot, ROWS - 3, "{name}: 删 3 把后 total 没跟着掉");
        assert_eq!(
            after.items.len(),
            tot - last * PAGE,
            "{name}: 末页行数与 total 不自洽 ⇒ skip 与 total 不是同一趟算出来的"
        );
        tx.commit();
    }
    cleanup(dirs);
}
