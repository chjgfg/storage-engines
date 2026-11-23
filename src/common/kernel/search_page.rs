//! 共享分页装配与精确 total。

/// 分页核（四引擎共用的一份）：从"已按 pattern 过滤好的命中序列"里跳 `skip` 条取 `page_size` 条。
///
/// 存在的理由：这四行逻辑原先在四个引擎里各抄一遍，而 M15 的改动恰好只动这四行 ——
/// 一处改完，四家一起变，不存在"三家改了、一家漏了"。
///
/// 这条入口假定调用方**已经把界内命中全交过来**，所以 `total` 就是 `matched.len()`。
/// M35 之后 `search_keys` 不走这条（改成"只数一遍 + 只取那一页"，见
/// [`crate::common::search_page`]）；留着是给"手里本来就有全量"的调用方与测试对拍用的。
pub fn search_page_from(
    matched: Vec<crate::common::ExportRecord>,
    query: &crate::common::SearchQuery,
) -> crate::common::SearchPage {
    let total = query.need_total.then_some(matched.len());
    search_page_from_with_total(matched, query, total)
}

/// 一页的**装配口径唯一一份**（M51）。
///
/// 为什么要把 `has_next` / `total_pages` 的算法从 [`search_page_from_with_total`] 里拆出来：
/// M51 之后取页的那条路不再交回"从头到这一页的整段"，而是交回"这一页 + 有没有下一条"
/// （[`crate::common::store::search_page_rows`]）⇒ 两个形状都要算同一个 `has_next`。
/// 分开写就是两处会各自漂移的那种"同一判定两份"（M5a/M5b 一路的规矩），所以这里只留一份，
/// 老的"整段切片版"改成它的适配层。
pub fn search_page_build(
    items: Vec<crate::common::ExportRecord>,
    page: usize,
    page_size: usize,
    total: Option<usize>,
    has_more: bool,
) -> crate::common::SearchPage {
    let (has_next, total_pages) = match total {
        Some(n) => (
            n > 0 && page + 1 < (n + page_size - 1) / page_size,
            Some(if n == 0 {
                0
            } else {
                (n + page_size - 1) / page_size
            }),
        ),
        None => (has_more, None),
    };
    crate::common::SearchPage {
        items,
        total,
        page,
        page_size,
        total_pages,
        has_next,
    }
}

/// [`search_page_from`] 的**外部 total 版**（M35）：`total` 是调用方另外数出来的精确命中数，
/// `matched` 只需要装得下**那一页**（外加"多取一条"当 `has_next` 的证据）。
///
/// 为什么要分开：数 total 与取一页是两笔账 —— 前者要**走一遍界内**（每行的值都得读出来才判得了
/// 过期），后者只关心 `skip + page_size` 行。把它们合在一条折叠里，就等于"为了一个数把整库
/// 物化成 `Vec` 再 `len()`"（600k 档实测峰值 **+123.7 MB**）。
///
/// `total` 的两条路：
/// - `Some(n)`：`has_next` / `total_pages` 都由 `n` 推（`n > 0 && page + 1 < ceil(n / 页宽)`）。
/// - `None`（没数）：`has_next` 只能靠"窗口外那一条在不在" —— `matched.len() > skip + 页宽`。
///   ⚠️ **不许把"没数"报成"到底了"**就是靠这条区分的：走到库尾 / 界尾是真的到底，
///   而探到第 `end+1` 条说明后面还有。
///
/// ⚠️ M51：`search_page` 已经不走这条了（它用 [`search_page_build]`，跳过的行当场丢掉、不再攒整段）。
/// 这里留着是给"手上已经有一段命中"的调用方（[`search_page_from`] 以及下游自己折叠的形状）。
pub fn search_page_from_with_total(
    matched: Vec<crate::common::ExportRecord>,
    query: &crate::common::SearchQuery,
    total: Option<usize>,
) -> crate::common::SearchPage {
    let page_size = query.page_size.max(1);
    let page = query.page;
    let skip = page.saturating_mul(page_size);
    let has_more = matched.len() > skip + page_size;
    let items = if skip >= matched.len() {
        Vec::new()
    } else {
        let end = (skip + page_size).min(matched.len());
        matched[skip..end].to_vec()
    };
    search_page_build(items, page, page_size, total, has_more)
}
