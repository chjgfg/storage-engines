// 该文件集中验证对应职责，保留原测试名称和字面值。

#[test]
fn memory_export_does_not_reseek_each_winner() {
    let db = open_engine(EngineKind::Memory, None).unwrap();
    let tx = db.begin();
    for i in 0..2001 {
        assert!(tx.set_raw(&scale_key(i), scale_val(i)));
    }
    tx.commit();
    let before = crate::memory::mem_store::point_lookups();
    let mut seen = 0;
    db.export_latest_visible_for_each(false, &mut |record| {
        assert_eq!(record.key, scale_key(seen));
        assert_eq!(record.value, Some(scale_val(seen)));
        seen += 1;
        true
    });
    assert_eq!(seen, 2001);
    assert_eq!(crate::memory::mem_store::point_lookups() - before, 0);
    let _reference = crate::memory::memory_mvcc::scan_reference::Scope::start();
    let before = crate::memory::mem_store::point_lookups();
    assert_eq!(
        db.export_latest_visible_for_each(false, &mut |_| true),
        2001
    );
    assert_eq!(
        crate::memory::mem_store::point_lookups() - before,
        2001,
        "reference must still exercise one lookup per winner"
    );
}

/// 导出**不必把全库摆进内存**（M44）：`export_latest_visible_for_each` 走的是流式门
/// （`fold_window_emit`），一行都不攒；而它交出来的行必须与"整库返回"那条一模一样。
///
/// 尺子是 [`crate::common::collect_held_max`]：**收集门**（`fold_window`）最多同时攒了几行。
/// 为什么拿它当凭据而不是拿内存高水位：这条路径的差别就在"中间有没有一个 `Vec`"，
/// 而进程高水位在并行用例里互相污染（M27/M36 各栽过一次）。三件事分开钉，因为它们
/// 各自会坏：
/// ① 流式那趟里收集门必须读到 **0**（在第一次回调处采一次、跑完再采一次 —— 只采末尾
///    抓不到"先把全库折完再逐行回调"那种混血形状）。
/// ② 同一把尺子在**真走收集门**的路径（`scan_page`）上必须读到页宽 ⇒ 证明它接在门上，
///    不是空转（M35 变异第一次没红就是栽在尺子只钉新门上）。
/// ③ 语义对拍走**另一条遍历**（`scan_page` 翻页，游标续跑，与流式导出是两份代码）。
///    拿"收集壳 vs 流式核"互比是恒真的 —— 壳就是核的壳，比不出东西。
#[test]
fn all_engines_export_streams_without_holding_the_library() {
    const KEYS: usize = 5_000;
    const DEAD: usize = 40;
    const STOP: usize = 100;
    const PAGE: usize = 700;
    let want = KEYS - DEAD;

    let engines = open_all();
    let mut dirs = Vec::new();
    for (name, db, dir) in engines {
        dirs.push(dir);
        db.set_vacuum_auto_slice(0);
        let tx = db.begin();
        for i in 0..KEYS {
            assert!(
                tx.set(format!("k{i:05}").as_bytes(), vec![b'v'; 16]),
                "{name}: 第 {i} 把键写失败"
            );
        }
        for i in 0..DEAD {
            assert!(
                tx.delete(format!("k{i:05}").as_bytes()),
                "{name}: 第 {i} 把键删失败"
            );
        }
        tx.commit();

        // ── ② 先把尺子接在真走收集门的那条路上验一次它响（不这么钉，下面那个 0 不算数）──
        crate::common::reset_collect_held();
        let tx = db.begin();
        let (rows, _next) = tx.scan_page(None, PAGE);
        let page_rows = rows.len();
        tx.commit();
        let held_page = crate::common::collect_held_max();
        eprintln!("[export stream] {name}: scan_page 交出 {page_rows} 行 ⇒ 收集门读到 {held_page}");
        assert!(
            held_page >= page_rows as u64 && held_page <= page_rows as u64 + 1,
            "{name}: 收集门在 scan_page 上读到 {held_page}（页宽 {page_rows}，合同里多要一条做 has_next 的证据）\
                 ⇒ 要么尺子没接在门上（那下面那个 0 不算数），要么页宽失控成了全库"
        );

        // ── ① 两扇门都必须"折一行交一行" —— 尺子 = **只交出一行时拷出去的用户值条数** ──
        // ⚠️ 第一版这里用的是 `collect_held_max()`（收集门攒了几行），而它**分不开这两种形状**：
        //    M44 之后收集门（`fold_window`）根本不在导出路径上，"悄悄退回先物化整库再逐行交"
        //    的那个 `Vec` 是调用方自己攒的 ⇒ 尺子读 0 照样绿。去掉事务层流式覆盖的那次变异
        //    第一次就没红（见 README M44 那一节）。"只要 1 行时拷了几条用户值"才真分得开：
        //    流式 = 1，先物化 = 全库。
        let base = crate::common::user_value_copies();
        let mut one_only = 0usize;
        let handed_one = db.export_latest_visible_for_each(false, &mut |_| {
            one_only += 1;
            false // 第一行就收手
        });
        let copies_one = crate::common::user_value_copies() - base;
        eprintln!(
            "[export stream] {name}: 只要 1 行 ⇒ 交出 {handed_one} 行、拷出 {copies_one} 条用户值"
        );
        assert_eq!(
            handed_one, 1,
            "{name}: 收手之后不该再交第二行（实际 {handed_one}）"
        );
        assert_eq!(one_only, 1, "{name}: 回调次数与返回行数不同");
        assert!(
            copies_one <= 2,
            "{name}: 只要 1 行却拷出 {copies_one} 条用户值（库里 {want} 行）⇒ KvEngine 那扇门先把整库物化了"
        );

        // 同一把尺子在事务层那扇门（`KvTxn::export_latest_visible_for_each`）上再走一遍：
        // trait 上挂的是默认实现（先物化再逐行交），`impl_engine!` 里的覆盖要是漏了，
        // 编译照过、语义也对 ⇒ 只有这一支抓得到。
        let base = crate::common::user_value_copies();
        let tx = db.begin();
        let mut tx_one = 0usize;
        let handed_tx = tx.export_latest_visible_for_each(false, &mut |_| {
            tx_one += 1;
            false
        });
        let copies_tx = crate::common::user_value_copies() - base;
        tx.commit();
        eprintln!(
            "[export stream] {name}: 事务层只要 1 行 ⇒ 交出 {handed_tx} 行、拷出 {copies_tx} 条用户值"
        );
        assert_eq!(
            handed_tx, 1,
            "{name}: 事务层收手之后还在交行（{handed_tx}）"
        );
        assert!(
            copies_tx <= 2,
            "{name}: 事务层只要 1 行却拷出 {copies_tx} 条用户值 ⇒ `impl_engine!` 的覆盖没生效，悄悄退回了物化默认实现"
        );

        // ── ①b 全库那一趟：收集门一次都不该响（第一行处采一次、收完再采一次）──
        crate::common::reset_collect_held();
        let mut streamed: Vec<ExportRecord> = Vec::new();
        let mut first_held = u64::MAX;
        let copies_base = crate::common::user_value_copies();
        let mut f = |rec: ExportRecord| {
            if streamed.is_empty() {
                first_held = crate::common::collect_held_max();
            }
            streamed.push(rec);
            true
        };
        let handed = db.export_latest_visible_for_each(false, &mut f);
        let copies_all = crate::common::user_value_copies() - copies_base;
        let held_end = crate::common::collect_held_max();
        eprintln!(
            "[export stream] {name}: 交出 {handed} 行 ⇒ 第一行时收集门 {first_held} 行、收完 {held_end} 行、拷出 {copies_all} 条"
        );
        assert_eq!(handed, want, "{name}: 流式导出的行数 ≠ 活键数 ⇒ 漏行或重行");
        assert_eq!(streamed.len(), want, "{name}: 回调次数与返回行数不同");
        // 尺子这一档自己也要"响一次给后面那个 0 作证"：全库导出就该拷全库条用户值
        assert_eq!(
            copies_all, want as u64,
            "{name}: 全库流式导出拷出 {copies_all} 条用户值（应为 {want}）⇒ 上面那个 ≤2 可能是尺子空转"
        );
        assert_eq!(
            first_held, 0,
            "{name}: 交第一行之前收集门已经攒了 {first_held} 行 ⇒ 还是先把全库折完再逐行回调"
        );
        assert_eq!(
            held_end, 0,
            "{name}: 流式导出这一趟在收集门里攒过 {held_end} 行 ⇒ 全库又在内存里摆了一份"
        );

        // ── 语义：行数与 `health()` 对得上、硬删掉的键不许出现 ──
        let keys: Vec<Vec<u8>> = streamed.iter().map(|r| r.key.clone()).collect();
        // ⚠️ 这里**不**拿 `health().live_keys` 当对拍基准：那个口径是"库里出现过的逻辑键数"，
        // 最新一版是墓碑的键照样算一个 ⇒ 它比"导出的存活行数"大 {DEAD}。行数与
        // `KEYS - DEAD` 的对齐在上面 `handed` 那一格已经钉了；与另一条遍历的逐行对拍在下面。
        assert_eq!(
            keys.len(),
            want,
            "{name}: 导出的存活行数不等于活键数 ⇒ 两条观测口径分了家"
        );
        assert_eq!(
            keys.iter().filter(|k| **k == b"k00000".to_vec()).count(),
            0,
            "{name}: 硬删掉的键出现在导出里"
        );
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(sorted, keys, "{name}: 导出不是升序 ⇒ 与翻页那条路对不上");

        // ── ③ 与另一条遍历（scan_page 一路翻到底）逐行对拍 ──
        let mut paged: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::new();
        let mut cursor: Option<Vec<u8>> = None;
        loop {
            let tx = db.begin();
            let (rows, next) = tx.scan_page(cursor.as_deref(), PAGE);
            for r in &rows {
                paged.push((r.key.clone(), r.value.clone()));
            }
            tx.commit();
            match next {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }
        assert_eq!(
            paged.len(),
            streamed.len(),
            "{name}: 翻页与流式导出的行数不同（翻页 {} vs 导出 {}）",
            paged.len(),
            streamed.len()
        );
        for (i, (p, s)) in paged.iter().zip(streamed.iter()).enumerate() {
            assert_eq!(p.0, s.key, "{name}: 第 {i} 行两条遍历给的键不同");
            assert_eq!(p.1, s.value, "{name}: 第 {i} 行两条遍历给的值不同");
        }

        // ── 提前收手：交够 STOP 行就停，后面的键不再折 ──
        crate::common::reset_collect_held();
        let mut got: Vec<Vec<u8>> = Vec::new();
        let mut g = |rec: ExportRecord| {
            got.push(rec.key);
            got.len() < STOP
        };
        let handed_stop = db.export_latest_visible_for_each(false, &mut g);
        assert_eq!(
            handed_stop, STOP,
            "{name}: 提前收手没停在 {STOP}（实际 {handed_stop}）"
        );
        assert_eq!(
            got,
            keys[..STOP].to_vec(),
            "{name}: 提前收手交出的不是同一批前缀 ⇒ 收手处漏了行"
        );
        assert_eq!(
            crate::common::collect_held_max(),
            0,
            "{name}: 收手那一趟走了收集门"
        );
    }
    cleanup(dirs);
}
