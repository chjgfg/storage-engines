// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn batched_delete_reuses_unchanged_parent_with_old_path_control() {
    let victims: Vec<_> = (0..1000)
        .filter(|i| i % 8 == 5)
        .map(|i| format!("{i:08}").into_bytes())
        .collect();
    let mut previous = None;
    for reference in [true, false] {
        let (_scratch, mut tree) = anon(64, 4096);
        for i in 0..1000 {
            assert!(tree.insert(format!("{i:08}").into_bytes(), RowId(Some(vec![7]))));
        }
        assert_eq!(
            tree.node_buffer.capacity(),
            0,
            "insertion uses its own page writer; delete starts cold"
        );
        let mode = reference.then(delete_cost_reference::Scope::start);
        let scope = crate::common::maintenance_probe::Scope::start();
        let before = crate::bplus_tree::page::page_reads();
        assert_eq!(tree.delete_batch(&victims), victims.len());
        let reads = crate::bplus_tree::page::page_reads() - before;
        let stats = scope.stats();
        if reference {
            assert_eq!(stats.bplus_backtrack_reuses, 0);
            assert!(
                stats.bplus_pack_buffers > 0,
                "reference must allocate serialized pages"
            );
            previous = Some(reads);
        } else {
            assert!(
                stats.bplus_backtrack_reuses > 0,
                "shared routing must actually skip parent rereads"
            );
            assert_eq!(
                stats.bplus_pack_buffers, 1,
                "lazy serializer must allocate once then be reused"
            );
            assert!(reads < previous.unwrap());
        }
        drop(scope);
        drop(mode);
        for i in 0..1000 {
            assert_eq!(
                tree.get(format!("{i:08}").into_bytes()).is_some(),
                i % 8 != 5
            );
        }
        if !reference {
            let pointer = tree.node_buffer.as_ptr();
            let warm = crate::common::maintenance_probe::Scope::start();
            assert!(tree.update(format!("{:08}", 0).into_bytes(), RowId(Some(vec![9]))));
            assert_eq!(
                warm.stats().bplus_pack_buffers,
                0,
                "a subsequent node write must not grow the buffer"
            );
            assert_eq!(tree.node_buffer.as_ptr(), pointer);
        }
    }
}

#[test]
fn test_overwrite_on_insert() {
    let (_s, mut tree) = anon(4, 8);
    tree.insert(key(1), rid(1));
    tree.insert(key(1), rid(2));
    assert_eq!(tree.get(key(1)).unwrap().0, Some(vec![2]));
    assert_eq!(collect_all(&mut tree).len(), 1);
}

/// 守卫（M56）：**成块删**的形状 —— 上一轮那条守卫用的形状根本没触发过重组（μE 与改后一字不差）。
///
/// 四件事钉在一起：
/// - **证人先成立**：`借位 + 合并 > 0`（实测 借位 122 / 合并 116）。不然"重组也向父层上报"那一半
///   仍然没被任何形状检验过；
/// - **`页·找祖父` 必须为 0**（M56b）：那趟"每次合并从根走一趟整棵树 DFS"在 200k 档占**每删除行
///   39.69 页（77%）**、每趟扫 2820 页，而该档 `缺页 38.76` 就是它造成的 ⇒ 谁把它抄回来这一格就不为 0
///   （μG 实测：抄回来 ⇒ 本断言红，页读 10.63 → 15.85 页/删除行）；
/// - **`问根` 次数 ≤ 合并次数 + 每次删除 1/8**（M56b）：根的孩子数只在真合并过一页时才会变。
///   改前每行删除无条件问两次（`delete()` 与回溯段的根分支各一次，每次读+解析一遍根页）
///   ⇒ 固定 2.00 页/删除行；μI 把无条件问抄回来 ⇒ 本断言红（600 次 > 界 191）；
/// - **量具自检**：四个阶段桶之和 == `page_reads()` 的增量 ⇒ 标注漏了一扇门就会当场读出来，
///   不会静默少计（桶少算比桶读 0 更危险，它看着像证据）；
/// - **代价界**：每删除行的页读、以及 `页/次下降`（③ 的尺子：合批下降之前，先看清一次下降花几页）。
#[test]
fn batched_deletes_really_restructure_and_stay_correct() {
    const N: usize = 1_200;
    // 三条界都从**两臂实测数中间**定（本形状：改后 10.63 页/删除行、3.54 页/次下降；
    // μG 把整树 DFS 抄回来 15.85 / 5.28；μI 把无条件问根抄回来 11.46 且问根 600 次 > 191 的界）
    const PAGE_BOUND: f64 = 12.5;
    const PER_DESCENT_BOUND: f64 = 4.2;
    fn k(n: usize) -> Key {
        format!("{n:08}").into_bytes()
    }
    fn r(n: usize) -> RowId {
        RowId(Some((n as u64).to_le_bytes().to_vec()))
    }

    // order=12 ⇒ 叶子满约 11 条；成块删一半 ⇒ 叶子成片空掉 ⇒ 必然走到借位/合并
    let (_s, mut tree) = anon(12, 4096);
    for i in 0..N {
        assert!(tree.insert(k(i), r(i)), "插入第 {i} 个键失败");
    }
    for i in 0..N {
        assert!(tree.get(k(i)).is_some(), "第 {i} 个键写完读不回来");
    }

    let p0 = crate::bplus_tree::page::page_reads();
    let b0 = crate::bplus_tree::page::page_reads_by_phase();
    let (u0, bor0, mg0) = reorg_stats();
    let d0 = descent_lookups();
    let s0 = sep_refreshes();
    reorg_stats_clear();
    sep_refreshes_clear();

    let mut dels = 0usize;
    for i in (0..N).step_by(2) {
        assert!(tree.delete(k(i)), "删除第 {i} 个键失败");
        dels += 1;
    }
    let reads = crate::bplus_tree::page::page_reads() - p0;
    let b1 = crate::bplus_tree::page::page_reads_by_phase();
    let buckets: Vec<u64> = (0..b1.len()).map(|i| b1[i] - b0[i]).collect();
    let (u, bor, m) = reorg_stats();
    let (u, bor, m) = (u - u0, bor - bor0, m - mg0);
    let descents = descent_lookups() - d0;
    let refreshes = sep_refreshes() - s0;
    let per_del = reads as f64 / dels as f64;
    let per_descent = reads as f64 / descents.max(1) as f64;
    let (b_other, b_descent, b_back, b_sep, b_reorg, b_gp, b_rc) = (
        buckets[0], buckets[1], buckets[2], buckets[3], buckets[4], buckets[5], buckets[6],
    );
    let rc = root_checks();
    eprintln!(
        "[del_guard] {dels} 次删除 · 页读 {reads}（{per_del:.2} 页/删除行）· 下降 {descents} · \
             {per_descent:.2} 页/次下降\n  桶：其它 {b_other} 下降 {b_descent} 回溯 {b_back} \
             分隔键 {b_sep} 重组 {b_reorg} 找祖父 {b_gp} 问根 {b_rc}\n\
             重组：真下溢 {u} / 借位 {bor} / 合并 {m} / 问根 {rc} 次 / 分隔键刷新 {refreshes} 趟"
    );

    // ① 量具自检
    assert_eq!(
        buckets.iter().sum::<u64>(),
        reads,
        "阶段桶之和 {} != 页读 {reads} ⇒ 有一扇读页门没被标注，桶的读数全是漏的",
        buckets.iter().sum::<u64>()
    );
    // ②' M56b 的两条硬判据：整树 DFS 不许回来；问根只许在真合并过一页时发生
    assert_eq!(
        b_gp, 0,
        "找祖父桶读出 {b_gp} 页 ⇒ 那趟从根走的整树 DFS 回来了（200k 档它占每删除行 39.69 页 = 77%）"
    );
    assert!(
        rc <= m + dels as u64 / 8,
        "问根 {rc} 次（删除 {dels} 次、合并 {m} 次）⇒ `shrink_root_if_needed` 又变成每行无条件问了：             根的孩子数只在合并过时才会变"
    );
    // ② 证人：这个形状必须真的做过重组
    assert!(
        bor + m > 0,
        "借位与合并都是 0 ⇒ 这个形状还是没触发重组，证人失效（别把\"重组也上报\"读成已检验）"
    );
    // ③ 代价界
    assert!(
        per_del <= PAGE_BOUND,
        "每次删除读 {per_del:.2} 页（界 {PAGE_BOUND}）⇒ 删除路径又长出重复读了；\
             看桶里哪一段涨了就修哪一段（下降/回溯/分隔键/重组）"
    );
    assert!(
        per_descent <= PER_DESCENT_BOUND,
        "每次下降读 {per_descent:.2} 页（界 {PER_DESCENT_BOUND}）⇒ 一趟下降除了路上每层那一页还多读了页"
    );
    // ④ 正确性扫
    for i in 0..N {
        if i % 2 == 0 {
            assert!(tree.get(k(i)).is_none(), "第 {i} 个键已删，还能读到");
        } else {
            assert!(tree.get(k(i)).is_some(), "第 {i} 个幸存键读不回来");
        }
    }
}

/// M61b：**批量门真的把同一片叶子里的连续行合成一趟下降**，而且结果与逐行删除逐键一致。
///
/// 为什么是"两臂都在同一个测试里跑"而不是"钉一个绝对界"：界会随形状漂（这片叶子能装几行由
/// `order` 定），而**比值**是这一刀的定性结论（M61 读到的上限 k = 行数/叶子游程）。
/// 三件事各自钉住：
/// ① `k` 两臂必须**完全相同** —— 行数与游程数都由"哪些键被删"决定，与怎么删无关；
///    批量臂的 k 若掉到 1，说明它退化成了逐行（那就是变异臂，不需要另写）；
/// ② 批量臂的 `页·下降/删除行` 必须 ≤ 逐行臂的 1/3（本形状每片叶子有 5~6 行可删 ⇒ 理论 1/5.5，
///    留 1/3 是防"批了但每行还是重下一趟"这种假批量）；
/// ③ 删完之后两臂的**可读性逐键相同**（该没了的读不到、该留的读得到），且桶之和 == 页读照旧。
#[test]
fn batch_door_really_shares_one_descent_per_leaf() {
    const N: usize = 1_200;
    fn k(n: usize) -> Key {
        format!("{n:08}").into_bytes()
    }
    fn r(n: usize) -> RowId {
        RowId(Some((n as u64).to_le_bytes().to_vec()))
    }
    // order=12 ⇒ 一片叶子约 11 条；每 2 把删 1 把 ⇒ 同一片叶子里平均 5~6 行连续可删（k ≈ 5.5）
    // 守卫跟着库一起交出去：库还要被量（写入、删除、读回），守卫若在闭包里析构，
    // 目录会在引擎还开着的时候被删 —— Windows 上删不掉，就留在 `%TEMP%` 里（M82）
    let build = || {
        let (s, mut t) = anon(12, 4096);
        for i in 0..N {
            assert!(t.insert(k(i), r(i)), "插入第 {i} 个键失败");
        }
        (s, t)
    };
    let victims: Vec<Key> = (0..N).step_by(2).map(k).collect();

    let arm = |tree: &mut BPlusTree, batch: bool| -> (f64, f64, u64, u64, usize) {
        let p0 = crate::bplus_tree::page::page_reads();
        let b0 = crate::bplus_tree::page::page_reads_by_phase();
        let (r0, g0) = delete_leaf_runs();
        let removed = if batch {
            tree.delete_batch(&victims)
        } else {
            victims
                .iter()
                .filter(|key| tree.delete((*key).clone()))
                .count()
        };
        let reads = crate::bplus_tree::page::page_reads() - p0;
        let b1 = crate::bplus_tree::page::page_reads_by_phase();
        let buckets: Vec<u64> = (0..b1.len()).map(|i| b1[i] - b0[i]).collect();
        let (r1, g1) = delete_leaf_runs();
        let n = removed.max(1) as f64;
        assert_eq!(
            buckets.iter().sum::<u64>(),
            reads,
            "阶段桶之和 {} != 页读 {reads} ⇒ 批量门里有一扇读页门没被标注",
            buckets.iter().sum::<u64>()
        );
        assert_eq!(
            buckets[crate::bplus_tree::page::PHASE_GRANDPARENT as usize],
            0,
            "找祖父桶回来了"
        );
        (
            buckets[PHASE_DESCENT as usize] as f64 / n,
            reads as f64 / n,
            r1 - r0,
            g1 - g0,
            removed,
        )
    };
    let check_readable = |tree: &mut BPlusTree, who: &str| {
        for i in 0..N {
            let got = tree.get(k(i)).is_some();
            assert_eq!(
                got,
                i % 2 == 1,
                "{who}：第 {i} 把键的可读性不对（偶数该被删、奇数该留下）"
            );
        }
    };

    let (_s_row, mut per_row) = build();
    let (d_per_row, all_per_row, rows_a, runs_a, removed_a) = arm(&mut per_row, false);
    check_readable(&mut per_row, "逐行臂");

    let (_s_batch, mut batched) = build();
    let (d_batch, all_batch, rows_b, runs_b, removed_b) = arm(&mut batched, true);
    check_readable(&mut batched, "批量臂");

    eprintln!(
        "[batch door] 逐行臂 下降 {d_per_row:.2} 页/删除行、总 {all_per_row:.2} 页/删除行（{rows_a} 行 / {runs_a} 游程）"
    );
    eprintln!(
        "           批量臂 下降 {d_batch:.2} 页/删除行、总 {all_batch:.2} 页/删除行（{rows_b} 行 / {runs_b} 游程）"
    );
    assert_eq!(rows_a, rows_b, "两臂删的行数不同 ⇒ 有一臂漏删了键");
    // ⚠️ 游程数**允许小幅不同**：合批把"查下溢/借位/合并"的时机从每行一次改成每片叶子一次
    // ⇒ 叶子的划分随之漂移（600k 档实测 20 018 vs 19 419，差 3%）。所以这里钉"同一量级"，
    // 不钉"逐个相等" —— 后者会在别的形状上无故发红（大档 A/B 抓出来的这条就是预警）。
    assert!(
        runs_a > 0 && (runs_a as i64 - runs_b as i64).abs() * 4 <= runs_a as i64,
        "两臂的叶子游程差太多：{runs_a} vs {runs_b} ⇒ 有一臂走错了叶子/路由"
    );
    assert_eq!(removed_a, removed_b, "两臂实删行数不同 ⇒ 有一臂漏删了键");
    assert!(
        runs_a > 0 && rows_a > runs_a * 3,
        "这个形状的 k = {}/{} 本来就不大于 3 ⇒ 证人失效：换不到收益是因为没合批，还是因为没得合批？",
        rows_a,
        runs_a
    );
    assert!(
        d_batch <= d_per_row / 3.0,
        "批量臂每删除行仍读 {d_batch:.2} 页下降（逐行臂 {d_per_row:.2}）⇒ 没合上批：每行还在重下一趟"
    );
}
