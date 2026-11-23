// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// 无界折叠（`wanted = None`，"一趟走完全库"）必须**逐片**走完，而不是把整库候选
/// 一次装进内存（M27）。
///
/// 尺子选的是"每轮要了多少组候选"，不是墙钟也不是字节数：字节数要靠进程级分配计数
/// （并行跑就把别家算进来，10-17 栽过一次），而这条改动的判据本来就是**每轮的上限**。
/// 三件事一起断，缺一半都能被糊过去：① 语义没变（行数与内容 == 一次要到底）；
/// ② 每轮要的组数不超过 `FOLD_CHUNK`；③ 确实分了多轮（否则 ② 是空转的）。
#[test]
fn fold_window_unbounded_walks_in_chunks() {
    // 故意不是整片：最后那一片必须也能走到底
    const N: usize = 3 * 4096 + 7;

    fn enc(i: usize) -> Vec<u8> {
        (i as u64).to_le_bytes().to_vec()
    }
    fn dec(k: &[u8]) -> usize {
        u64::from_le_bytes(k.try_into().unwrap()) as usize
    }

    let mut budgets: Vec<usize> = Vec::new();
    let out: Vec<usize> = fold_window(
        None,
        |round_after, budget| {
            let start = round_after.map(dec).map(|i| i + 1).unwrap_or(0);
            let take = budget.min(N.saturating_sub(start));
            let batch: Vec<WinCandidate<usize>> = (start..start + take)
                .map(|i| WinCandidate {
                    key: enc(i),
                    version: i as u64,
                    row: i,
                })
                .collect();
            budgets.push(take);
            (batch, start + take >= N)
        },
        |c| Some(c.row),
    );

    assert_eq!(out.len(), N, "逐片走完全库却少了行");
    assert_eq!(
        out,
        (0..N).collect::<Vec<usize>>(),
        "分片之后行序/内容与一次要到底不一致"
    );
    let max = budgets.iter().copied().max().unwrap_or(0);
    assert!(
        budgets.len() >= 2,
        "只走了一轮 ⇒ 每轮上限那条断言是空转（轮数 {:?}）",
        budgets
    );
    assert!(
        max <= super::FOLD_CHUNK,
        "某一轮要了 {max} 组候选，超过一片（{:?}）",
        budgets
    );
}

/// 驱动"续跑第二轮"那一支的守卫：`keep` 丢掉候选时，必须从上一轮最后一个键接着折，
/// 而不是把窗口交短。四家的 `search_keys` 都走这条路，而引擎级守卫里
/// 没有过期行 ⇒ 那条分支一次都不会被踩到，只能在这里直接喂给它。
#[test]
fn fold_window_resumes_when_keep_drops_candidates() {
    let mut rounds = 0usize;
    let mut seen_budgets: Vec<usize> = Vec::new();

    let out: Vec<usize> = fold_window(
        Some(5),
        |after, budget| {
            rounds += 1;
            seen_budgets.push(budget);
            // 假想的键流：0..60，`after` 之后严格续跑
            let start = after.map(|a| a[0] as usize + 1).unwrap_or(0);
            let mut batch = Vec::new();
            for i in start..60usize {
                if batch.len() >= budget {
                    return (batch, false); // 被预算拦下 ⇒ 还能续
                }
                batch.push(WinCandidate {
                    key: vec![i as u8],
                    version: 1,
                    row: i,
                });
            }
            (batch, true) // 走到界尾
        },
        // 偶数下标全被丢掉：模拟"TTL 正好过期 / 是墓碑"的那些行
        |c| if c.row % 2 == 0 { None } else { Some(c.row) },
    );

    assert_eq!(
        out,
        vec![1, 3, 5, 7, 9],
        "被丢掉的候选必须由第二轮补上，窗口不许交短"
    );
    assert_eq!(
        rounds, 2,
        "第一轮 5 组里只活下来 2 条 ⇒ 必须续跑，一次都不续就是走不到窗口"
    );
    assert!(
        seen_budgets[1] > seen_budgets[0],
        "续跑的预算必须变大（几何 ⇒ 总代价与最终那一轮同阶）：{seen_budgets:?}"
    );
}

/// 分页核自己的守卫：四家都靠它，所以它错了就是四个地方一起错。
///
/// 重点盯 `has_next` 的两条分支 —— "没数"与"到底了"必须能区分开，
/// 把任何一侧写错都会让翻页提前停（少数据）或永远停不下来（死循环）。
#[test]
fn search_page_kernel_distinguishes_unmeasured_from_exhausted() {
    use crate::common::{ExportRecord, KeyMatchMode, SearchQuery};

    let rec = |i: usize| ExportRecord::new(format!("k{i:02}").into_bytes(), Some(vec![b'v']));
    let keys =
        |p: &crate::common::SearchPage| p.items.iter().map(|r| r.key.clone()).collect::<Vec<_>>();

    // ① 要 total：7 条 / 每页 3 ⇒ 三页，最后一页不满，翻页判据由总数算出来
    let q = SearchQuery {
        pattern: Vec::new(),
        page: 0,
        page_size: 3,
        mode: KeyMatchMode::Contains,
        include_deleted: false,
        need_total: true,
    };
    let all: Vec<_> = (0..7).map(rec).collect();
    let p0 = search_page_from(all.clone(), &q);
    assert_eq!(p0.total, Some(7));
    assert_eq!(p0.total_pages, Some(3));
    assert_eq!(
        keys(&p0),
        vec![b"k00".to_vec(), b"k01".to_vec(), b"k02".to_vec()]
    );
    assert!(p0.has_next());

    let p2 = search_page_from(
        all.clone(),
        &SearchQuery {
            page: 2,
            ..q.clone()
        },
    );
    assert_eq!(p2.items.len(), 1, "最后一页只剩一条");
    assert!(!p2.has_next(), "第三页之后没了");

    let p9 = search_page_from(
        all.clone(),
        &SearchQuery {
            page: 9,
            ..q.clone()
        },
    );
    assert!(p9.items.is_empty(), "越界的页给空，不许给尾");
    assert!(!p9.has_next());

    // ② 不要 total：上层最多交 `skip + page_size + 1` 条 —— 多出来的那一条就是证据
    let qn = SearchQuery {
        need_total: false,
        ..q.clone()
    };
    let w = search_page_from(all.iter().take(4).cloned().collect(), &qn);
    assert_eq!(w.total, None, "没数就不许报一个数");
    assert_eq!(w.total_pages, None);
    assert_eq!(w.items.len(), 3);
    assert!(w.has_next(), "手里有第 4 条 ⇒ 后面确实还有");

    // 交过来的正好不超过窗口 ⇒ 说明键流走到了头，这就是"到底了"，不是"没数"
    let end = search_page_from(all.iter().take(3).cloned().collect(), &qn);
    assert_eq!(end.items.len(), 3);
    assert!(!end.has_next(), "拿不满窗口 = 已经到底，必须报假");

    // 深一页：上层会交 skip+page_size+1 = 4+3+1 = 8 条，窗口从第 4 条起
    let deep = search_page_from(
        all.iter().take(7).cloned().collect(),
        &SearchQuery {
            page: 1,
            need_total: false,
            ..q.clone()
        },
    );
    assert_eq!(
        keys(&deep),
        vec![b"k03".to_vec(), b"k04".to_vec(), b"k05".to_vec()]
    );
    assert!(deep.has_next());
    let last = search_page_from(
        all.iter().take(7).cloned().collect(),
        &SearchQuery {
            page: 2,
            need_total: false,
            ..q.clone()
        },
    );
    assert_eq!(keys(&last), vec![b"k06".to_vec()]);
    assert!(!last.has_next(), "最后一页之后没有东西");

    // ③ `page_size` 至少为 1：传 0 不许把自己除尽
    let zero = search_page_from(
        all.clone(),
        &SearchQuery {
            page_size: 0,
            need_total: false,
            ..q.clone()
        },
    );
    assert_eq!(zero.items.len(), 1, "page_size=0 按 1 处理");
    assert!(zero.has_next());
}

#[test]
fn health_fold_counts_adjacent_blocks() {
    let mut fold = HealthFold::default();
    for _ in 0..3 {
        fold.push(b"hot");
    }
    fold.push(b"other");
    let h = fold.finish(2);
    assert_eq!((h.total_rows, h.live_keys, h.max_chain), (4, 2, 3));
    assert_eq!(h.rows_above_one, 2);
    assert_eq!(h.active_txns, 2);
}
