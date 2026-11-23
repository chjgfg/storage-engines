//! 本文件负责可见版本候选与搜索。

use super::*;

impl SearchRetention {
    pub fn new(include_deleted: bool, skip: usize, take: usize) -> Self {
        Self {
            include_deleted,
            skip,
            take,
        }
    }

    pub(crate) fn select(&mut self, logical: Option<&[u8]>) -> SearchSelection {
        let live = logical.is_some_and(crate::common::codec::logical_is_live);
        if !live && !self.include_deleted {
            return SearchSelection::Excluded;
        }
        if self.skip > 0 {
            self.skip -= 1;
            return SearchSelection::Counted;
        }
        if self.take == 0 {
            return SearchSelection::Counted;
        }
        self.take -= 1;
        if live {
            SearchSelection::Value
        } else {
            SearchSelection::Deleted
        }
    }
}

pub(crate) fn retain_search_row<R>(
    selection: SearchSelection,
    owned: impl FnOnce() -> Option<Vec<u8>>,
) -> SearchRow<R> {
    match selection {
        SearchSelection::Excluded => SearchRow::Excluded,
        SearchSelection::Counted => SearchRow::Counted,
        SearchSelection::Value => SearchRow::Retained(owned()),
        SearchSelection::Deleted => SearchRow::Retained(None),
    }
}

pub(crate) fn classify_search_value<R>(
    value: RowValue,
    retention: &mut SearchRetention,
) -> SearchRow<R> {
    match value {
        RowValue::Missing => SearchRow::Excluded,
        RowValue::Tombstone => retain_search_row(retention.select(None), || None),
        RowValue::Value(bytes) => retain_search_row(retention.select(Some(&bytes)), || Some(bytes)),
    }
}

/// 负向对照保留 M153 的整批物化路径。
#[cfg(test)]
pub fn owned_search_candidates<S: VersionedStore + ?Sized>(
    store: &S,
    start: Option<&[u8]>,
    end: Option<&[u8]>,
    accept: Option<&dyn Fn(&[u8]) -> bool>,
    after: Option<&[u8]>,
    budget: usize,
    mut retention: SearchRetention,
) -> (Vec<WinCandidate<SearchRow<S::Row>>>, bool) {
    let (rows, complete) = store.scan_candidates(start, end, accept, after, budget);
    let rows = rows
        .into_iter()
        .map(|c| {
            let row = classify_search_value(store.materialize(c.row), &mut retention);
            WinCandidate {
                key: c.key,
                version: c.version,
                row,
            }
        })
        .collect();
    (rows, complete)
}

/// 模式 ⇒ 折叠界。前缀是有界的（可以把界下推给折叠），Contains 只能全库走。
pub fn search_bounds<'a>(
    pattern: Option<&'a [u8]>,
    mode: KeyMatchMode,
) -> (Option<&'a [u8]>, Option<Vec<u8>>) {
    let pat = pattern.unwrap_or_default();
    match mode {
        KeyMatchMode::Prefix => {
            let lo = if pat.is_empty() { None } else { Some(pat) };
            (lo, prefix_upper_bound(pat))
        }
        KeyMatchMode::Contains => (None, None),
    }
}

/// 一组成员行折出来的**候选** → 用户该看到的那一行（判定的唯一一份实现，M51）。
///
/// 为什么单独成一个函数而不是留在 `search_window` 的闭包里：M51 把"取那一页"从
/// "先把前 `skip+page_size` 行全攒起来"改成"流式只留那一页"，于是**同一条判定出现了第二个用户**。
/// 判定分两处写就是本仓库反复在防的那件事（M5a/M5b 一路的做法：同形状的判定不许留两份）——
/// 过期与墓碑同待这条规矩，两处必须永远一致。
pub(super) fn record_of<S: VersionedStore>(
    store: &S,
    key: Vec<u8>,
    row: S::Row,
    include_deleted: bool,
) -> Option<ExportRecord> {
    match store.materialize(row) {
        RowValue::Missing => None,
        RowValue::Tombstone => include_deleted.then_some(ExportRecord::new(key, None)),
        // 逻辑值还要剥一次信封；过期与墓碑同待（只在 include_deleted 时出现）
        RowValue::Value(logical) => {
            // 取账要在 `logical_to_user` 收走字节之前（同上，`bytes`/`logical` 都是移动语义）
            let expire = crate::common::codec::logical_expiry(&logical);
            match logical_to_user(logical) {
                Some(user) => Some(ExportRecord {
                    key,
                    value: Some(user),
                    expire_unix_secs: expire,
                }),
                None => include_deleted.then_some(ExportRecord::new(key, None)),
            }
        }
    }
}

/// 窗口化的模式搜索：返回**按 key 升序**的命中行，最多 `limit` 条（多要一条给 `has_next` 用）。
///
/// 与 [`collect_rows`] 的分工：这里额外做两件事 —— 把 `pattern`/`mode` 变成折叠侧的
/// `accept`（键级，先判它才能少取值）与界，再把存储字节剥成用户 value（`logical_to_user`）。
/// 剥壳后为 `None`（过期）的行与墓碑同待：只在 `include_deleted` 时出现。
///
/// ⚠️ 它**攒的是一个 `Vec`**：`limit` 给多大就攒多大。翻到第 1200 页**不要**用它 ——
/// 那条路走 [`search_page_rows`]（只留那一页），见 M51。
pub fn search_window<S: VersionedStore>(
    store: &S,
    include_deleted: bool,
    pattern: Option<&[u8]>,
    mode: KeyMatchMode,
    limit: Option<usize>,
) -> Vec<ExportRecord> {
    let (lo, hi) = search_bounds(pattern, mode);
    let accept = |k: &[u8]| pattern.is_none_or(|pat| key_matches(k, pat, mode));
    fold_window(
        limit,
        |after, budget| store.scan_candidates(lo, hi.as_deref(), Some(&accept), after, budget),
        |c| record_of(store, c.key, c.row, include_deleted),
    )
}

/// 分页要的**那一页**：跳过 `skip` 行、留 `page_size` 行，外加"还有没有下一条"这一格证据。
///
/// 只要窗口 ⇒ 走到第 `page_size + 1` 条就收手。**不要精确 `total` 的话别数它**（M75b：
/// 要 total 就走 [`search_page_walk`] 的 `want_total = true`，一趟同时给数与页）。
///
/// 存在的理由（M51，复测抓到的一格）：`search_page` 以前是
/// `search_window(limit = skip + page_size + 1)` 再切片 ⇒ **翻到末页要先把前面每一页都攒在堆上**。
/// 600k 键的库翻到第 1200 页，瞬时峰值实测 **188 MB**（四家同形，比"一次全量 scan"的 123.7 MB 还高）。
/// 那条折叠本身没省（每一行的值都得读出来才判得了过期 ⇒ 见 [`count_window`] 那条同样的老实话），
/// 省的是**留存**：跳过的行当场丢掉，堆上最多只有 `page_size + 1` 行 ⇒ 峰值从 O(页码×页宽) 变成 O(页宽)。
pub fn search_page_rows<S: VersionedStore>(
    store: &S,
    include_deleted: bool,
    pattern: Option<&[u8]>,
    mode: KeyMatchMode,
    skip: usize,
    page_size: usize,
) -> (Vec<ExportRecord>, bool) {
    let (items, has_more, _) = search_page_walk(
        store,
        include_deleted,
        pattern,
        mode,
        skip,
        page_size,
        false,
    );
    (items, has_more)
}
/// 按跳过量、页宽和 total 需求决定是否仅判断、计数或物化。
/// 精确 total 需要走完匹配范围；免 total 页面达到足够结果后可提前结束。

/// **唯一一份**分页折叠：跳 `skip` 行、留 `page_size` 行，`want_total` 决定要不要顺手数完全界。
///
/// 判定口径与 [`count_window`] **一字不差**：`Missing` 不算；墓碑与过期行同待，只在
/// `include_deleted` 时算一条。所以同一份库上 `total` 与旧的两步必然同数（有守卫对拍）。
///
/// 两条合同合成一条的理由（M75b）：以前 `need_total` 那格是"先取窗口、再 `count_window` 数全库"
/// 两趟，实测 600k 档 ①只数 + ②只翻页 ≈ ③表里那一格（差 −3.7%~+5.3%）⇒ 同一次调用**走了两遍全库**。
/// 合成一趟除了省掉一遍，还顺带没掉一条瑕疵：两趟之间库被别的线程写过，`total` 与 `items`
/// 就来自两个瞬间。
///
/// ⚠️ 跳过的行**一次都不拷用户字节**：`keep` 只判"算不算一条"并把逻辑字节原样带下去，
/// 只有落进这一页的那些才过 `logical_to_user`（断这个数的尺子是 `user_value_copies`）。
///
/// 返回 `(那一页, 还有没有下一条, 精确命中数)`；`want_total = false` 时第三个数**无意义**
/// （提前收手了，只是"到目前为止交出的条数"）。
///
/// 为什么两支合成一个函数而不是两份：`page_size` 那一段判定（含"多一条当证据"）在两支里
/// 必须永远一致，分两处写就是本仓库一路在防的那件事（M5a/M5b 的做法）。
pub fn search_page_walk<S: VersionedStore>(
    store: &S,
    include_deleted: bool,
    pattern: Option<&[u8]>,
    mode: KeyMatchMode,
    skip: usize,
    page_size: usize,
    want_total: bool,
) -> (Vec<ExportRecord>, bool, usize) {
    if let SearchWalkOutcome::Completed {
        items,
        has_more,
        total,
    } = store.search_walk(include_deleted, pattern, mode, skip, page_size, want_total)
    {
        return (items, has_more, total);
    }
    let (lo, hi) = search_bounds(pattern, mode);
    let accept = |k: &[u8]| pattern.is_none_or(|pat| key_matches(k, pat, mode));
    let mut items = Vec::with_capacity(page_size.min(crate::common::kernel::FOLD_CHUNK));
    let seen = std::cell::Cell::new(0usize);
    let retained = std::cell::Cell::new(0usize);
    let mut has_more = false;
    let total = fold_window_emit(
        None,
        |after, budget| {
            store.scan_search_candidates(
                lo,
                hi.as_deref(),
                Some(&accept),
                after,
                budget,
                SearchRetention::new(
                    include_deleted,
                    skip.saturating_sub(seen.get()),
                    page_size.saturating_sub(retained.get()),
                ),
            )
        },
        |c| {
            let row = match c.row {
                SearchRow::Deferred(credential) => classify_search_value(
                    store.materialize(credential),
                    &mut SearchRetention::new(
                        include_deleted,
                        skip.saturating_sub(seen.get()),
                        page_size.saturating_sub(retained.get()),
                    ),
                ),
                row => row,
            };
            match row {
                SearchRow::Excluded => None,
                row => Some((c.key, row)),
            }
        },
        |(key, row)| {
            let next = seen.get() + 1;
            seen.set(next);
            if next <= skip {
                return true;
            }
            match row {
                SearchRow::Retained(logical) => {
                    items.push(ExportRecord {
                        key,
                        expire_unix_secs: logical
                            .as_ref()
                            .and_then(|l| crate::common::codec::logical_expiry(l)),
                        value: logical.and_then(logical_to_user),
                    });
                    retained.set(items.len());
                    true
                }
                SearchRow::Counted => {
                    has_more = true;
                    want_total
                }
                SearchRow::Deferred(_) | SearchRow::Excluded => unreachable!(),
            }
        },
    );
    (items, has_more, total)
}
/// 沿相同 MVCC、墓碑与 TTL 判据统计逻辑键。
/// 计数不需要用户值副本，但不能因此漏掉删除或过期屏障。

/// **只数不物化**（M35）：界内"活着行数"，与 [`search_window`] 走同一条折叠、同一套判定，
/// 但每行只判"算不算一条"，不把用户字节拷出来、不攒结果 `Vec`。
///
/// 为什么要有它：`key_count` 原先是 `prefix_scan(prefix).len()`，而 `search_keys` 要精确
/// total 时是"把界内命中全物化成 `Vec<ExportRecord>` 再取 `len()`" —— 为数一个数把整库的键与值
/// 都物化一遍（600k 档实测 `key_count` **2.36 s / 峰值 +123.7 MB**、`search 首页(要 total)`
/// **2.05 s / +123.8 MB**，而免 total 只要 **1.2 ms**）。
///
/// ⚠️ 判据与 `search_window` 一字不差，所以两者对同一份库必然给出同一个数：
/// `Missing` 不算；墓碑与**过期行**同待 —— 只在 `include_deleted` 时算一条。
/// 过期这一判走 [`crate::common::codec::logical_is_live`]，那是 `decode_value_meta` 的
/// 不拷贝版（`decode_value_meta` 会把 user 段 `to_vec()` 出来 ⇒ 数一个数要付两次堆分配）。
///
/// 落点是 [`crate::common::fold_window_emit`] 的返回值：它数的是"真交给 `emit` 的行数"，
/// 也就是命中数 ⇒ 一份累加器都不需要。
pub fn count_window<S: VersionedStore>(
    store: &S,
    include_deleted: bool,
    pattern: Option<&[u8]>,
    mode: KeyMatchMode,
) -> usize {
    if let SearchWalkOutcome::Completed { total, .. } =
        store.search_walk(include_deleted, pattern, mode, 0, 0, true)
    {
        return total;
    }
    let (lo, hi) = search_bounds(pattern, mode);
    let accept = |k: &[u8]| pattern.is_none_or(|pat| key_matches(k, pat, mode));
    fold_window_emit(
        None,
        |after, budget| {
            store.scan_search_candidates(
                lo,
                hi.as_deref(),
                Some(&accept),
                after,
                budget,
                SearchRetention::new(include_deleted, 0, 0),
            )
        },
        |c| {
            let row = match c.row {
                SearchRow::Deferred(credential) => classify_search_value(
                    store.materialize(credential),
                    &mut SearchRetention::new(include_deleted, 0, 0),
                ),
                row => row,
            };
            match row {
                SearchRow::Excluded => None,
                _ => Some(()),
            }
        },
        |_| true,
    )
}

/// 模式搜索 + 分页的公共入口；借用后端可直接消费已收齐组，保留共享的可见性和保留判据。
///
/// `need_total` 那一条分支是全部的差别，两支都走 [`search_page_walk`]（M75b 起只有一趟折叠）：
/// - **不要 total**：`want_total = false` ⇒ 走到第 `page_size + 1` 条就收手。
/// - **要 total**：`want_total = true` ⇒ 同一趟走完全界，`total` 与那一页来自**同一个瞬间**。
///
/// ⚠️ 这一格以前是"两步"（先取窗口、再 `count_window` 数全库），实测 600k 档
/// ①只数 + ②只翻页 ≈ ③表里那一格（差 −3.7%~+5.3%）⇒ 同一次调用**走了两遍全库**。
pub fn search_page<S: VersionedStore>(store: &S, query: &SearchQuery) -> SearchPage {
    let page_size = query.page_size.max(1);
    let skip = query.page.saturating_mul(page_size);
    // M51：取那一页走**流式**（只留 `page_size` 行 + 一条"还有下一页"的证据），
    // 不再把前面每一页都攒进 `Vec`。窗口那条"多要一条"的合同原样保留。
    let (items, has_more, total) = search_page_walk(
        store,
        query.include_deleted,
        Some(query.pattern.as_slice()),
        query.mode,
        skip,
        page_size,
        query.need_total,
    );
    crate::common::search_page_build(
        items,
        query.page,
        page_size,
        query.need_total.then_some(total),
        has_more,
    )
}
