// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// 末页(免 total) 的**对拍 + 有界性**守卫（M81 第 4 项）。两层判据缺一不可：
///
/// ① **对拍**（四家都要过）：`search_last_page(k)` 交出的 k 行必须逐字节等于
///   `scan(None,None)` 的**最后** k 行活记录。这一层管"快不许换错数据"：把推进时的
///   `bound = raw`（逻辑键）写成 `bound = enc`（编码键），就会在同一把键的旧版本上打转 ——
///   `find_back` 的 `excluded` 记的是编码键、拦不住同键旧版本（方案第 4 项坑 2），
///   末页会全是重复键 + 旧值 ⇒ 这条立刻红。
///   形状里故意塞了**尾部墓碑**与一把"**入库时就已过期**"的键：反向走时它们不计数但必须跳过，
///   漏判任何一支就是短页（少一行）或把死键交出去。
/// ② **有界性**（四家都要过）：取末页的代价不许随库涨。钉**计数**不钉墙钟（本机跨轮漂
///   1.3~2.6×）；改前那一格是 `scan` 全库再截尾（三家 20M 档 68.9~105.6 s + 常驻 2.1 GB）。
///   - lsm：`block_reads` / `light_visits`（它那条走 SST 块读）
///   - 其余三家：`back_n_candidates`（反向退过的候选键数）必须 ≈ 页宽，且
///     `back_n_fallbacks == 0` —— 回退那一支是"退过 8192 步、改走全库截尾"，它一旦被踩中，
///     这一格就悄悄回到 O(库)。两个计数一人盯一种退化形状（M82）。
#[test]
fn last_page_agrees_with_the_tail_and_stays_bounded() {
    const N: usize = 3_000;
    const PAGE: usize = 10;
    for entry in open_all() {
        // 整条 `entry` 不拆开：元组字段按 0→1→2 析构 ⇒ 引擎先落地、scratch 守卫后删目录。
        // 拆成 `for (name, db, dir)` 会按反向声明序走、守卫先跑 —— 而 Windows 上引擎还开着的
        // `.log`/`.lock` 那时删不掉，目录就白留在 `%TEMP%` 里（M82）。
        let (name, db) = (entry.0, &*entry.1);
        {
            let tx = db.begin();
            for i in 0..N {
                assert!(
                    tx.set(format!("k{i:04}").as_bytes(), format!("v{i}").into_bytes()),
                    "{name}: 灌第 {i} 把"
                );
            }
            tx.commit();
        }
        // 库尾 5 把删掉 + 最后一把改成"入库即已过期" ⇒ 末页要往回多退 6 把才凑够 PAGE 行
        {
            let tx = db.begin();
            for i in (N - 6)..(N - 1) {
                tx.delete(format!("k{i:04}").as_bytes());
            }
            assert!(
                tx.set(
                    format!("k{:04}", N - 1).as_bytes(),
                    crate::common::codec::pack_ttl(b"v", 1)
                ),
                "{name}: 写过期键失败"
            );
            tx.commit();
        }
        let _ = db.flush(); // 让 lsm 的尾巴真落到 SST 里，否则"块读有界"那一支测的是空集

        // 参考值：正向全库折叠的最后 PAGE 行（`scan` 的合同就是"活且未过期"，与末页同源）
        let want: Vec<ExportRecord> = {
            let tx = db.begin();
            let mut all = tx.scan(None, None);
            tx.commit();
            let take = all.len().min(PAGE);
            all.split_off(all.len() - take)
        };
        let (blocks, visits, cands, fallbacks, page) = {
            let tx = db.begin();
            let s0 = crate::lsm_tree::sst::block_reads();
            let v0 = crate::lsm_tree::sst::light_visits();
            let c0 = crate::common::back_n_candidates();
            let f0 = crate::common::back_n_fallbacks();
            let page = tx.search_last_page(PAGE);
            let out = (
                crate::lsm_tree::sst::block_reads() - s0,
                crate::lsm_tree::sst::light_visits() - v0,
                crate::common::back_n_candidates() - c0,
                crate::common::back_n_fallbacks() - f0,
                page,
            );
            tx.commit();
            out
        };
        let keys_of = |v: &Vec<ExportRecord>| {
            v.iter()
                .map(|r| (r.key.clone(), r.value.clone()))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            keys_of(&page.items),
            keys_of(&want),
            "{name}: 末页与全库折叠的尾巴不一致（拿到 {} 行，应为 {} 行）",
            page.items.len(),
            want.len()
        );
        assert!(
            page.total.is_none() && page.total_pages.is_none(),
            "{name}: 免 total 不许谎称数过"
        );
        assert!(!page.has_next, "{name}: 已在库尾，后面不该还有东西");
        assert_eq!(page.page_size, PAGE, "{name}: page_size 要如实带回");
        if name == "lsm_tree" {
            // 每退一步 ≈ 层数 × 一块 + 内存表；界给得很松（400），
            // 而"物化全库"那一支在这个库上是几千次 ⇒ 松界也咬得住。
            assert!(
                blocks <= 400,
                "{name}: 取 {PAGE} 行末页读了 {blocks} 块 ⇒ 不像 O(页) 的走法"
            );
            assert!(
                visits <= 400,
                "{name}: 取 {PAGE} 行末页轻量访问 {visits} 次 ⇒ 不像 O(页) 的走法"
            );
        } else {
            // 三家走 `back_n_with`：候选键数只该是"页宽 + 尾巴上那几把死键"（这里 6 把），
            // 与库里的 3000 把无关；回退那一支一次都不许被踩中（踩中 = 悄悄退回 O(库)）。
            // 下界那一头也钉住：`cands == 0` 说明这一家根本没走 `back_n_with`（把宏臂翻回
            // `last_page: no`、退回 trait 默认的全库截尾），那时上界是过得去的 ⇒ 会漏检。
            assert!(
                cands >= PAGE as u64,
                "{name}: 取 {PAGE} 行末页只退了 {cands} 个候选键 ⇒ 没走反向逐步退（宏臂翻回 no 了？）"
            );
            assert!(
                cands <= (PAGE + 8) as u64,
                "{name}: 取 {PAGE} 行末页退了 {cands} 个候选键（库里有 {N} 把）⇒ 不像 O(页宽) 的走法"
            );
            assert_eq!(
                fallbacks, 0,
                "{name}: 末页走了'退太多 ⇒ 全库截尾'的回退支 ⇒ 这一格又成了 O(库)"
            );
        }
        // 页宽超过库存量 ⇒ 如实交出短页，不 panic、不补空行
        let wide = {
            let tx = db.begin();
            let p = tx.search_last_page(N * 2);
            tx.commit();
            p
        };
        assert_eq!(
            wide.items.len(),
            want.len().max(N - 6),
            "{name}: 超宽页应如实交出全部活行"
        );
    }
}
