//! 本文件负责测试计量与机制对照。

use super::*;

#[cfg(test)]
pub(crate) fn split_drops() -> u64 {
    SPLIT_DROPS.with(|c| c.get())
}

#[cfg(test)]
pub(crate) fn node_parses() -> u64 {
    NODE_PARSES.with(|c| c.get())
}

#[cfg(test)]
pub(crate) fn node_cache_sweeps() -> u64 {
    NODE_CACHE_SWEEPS.with(|c| c.get())
}

#[cfg(test)]
pub(crate) fn node_cache_visited() -> u64 {
    NODE_CACHE_VISITED.with(|c| c.get())
}

#[cfg(test)]
pub(crate) fn descent_lookups() -> u64 {
    DESCENT_LOOKUPS.with(|c| c.get())
}

#[cfg(test)]
pub(crate) fn descent_key_comps() -> u64 {
    DESCENT_KEY_COMPS.with(|c| c.get())
}

#[cfg(test)]
pub(crate) fn descent_max_comps() -> u64 {
    DESCENT_MAX_COMPS.with(|c| c.get())
}
#[cfg(test)]
pub(crate) fn point_internal_packs() -> u64 {
    POINT_INTERNAL_PACKS.with(|c| c.get())
}
#[cfg(test)]
pub(crate) fn point_internal_promotions() -> u64 {
    POINT_INTERNAL_PROMOTIONS.with(|c| c.get())
}
#[cfg(test)]
pub(crate) fn rebalance_counts() -> (u64, u64) {
    (
        REBALANCE_REUSES.with(|c| c.get()),
        REBALANCE_RELOADS.with(|c| c.get()),
    )
}

#[cfg(test)]
pub(crate) fn sep_refreshes() -> u64 {
    SEP_REFRESHES.with(|c| c.get())
}

#[cfg(test)]
pub(crate) fn sep_refreshes_clear() {
    SEP_REFRESHES.with(|c| c.set(0));
}

/// `(真下溢次数, 借位, 合并)`
#[cfg(test)]
pub(crate) fn reorg_stats() -> (u64, u64, u64) {
    UNDERFLOW_CALLS.with(|c| (c.get(), BORROWS.with(|b| b.get()), MERGES.with(|m| m.get())))
}

#[cfg(test)]
pub(crate) fn reorg_stats_clear() {
    UNDERFLOW_CALLS.with(|c| c.set(0));
    BORROWS.with(|c| c.set(0));
    MERGES.with(|c| c.set(0));
    ROOT_CHECKS.with(|c| c.set(0));
}

/// 每行删除问了几次"根要不要降层"（每次都读一遍根页）
#[cfg(test)]
pub(crate) fn root_checks() -> u64 {
    ROOT_CHECKS.with(|c| c.get())
}

#[cfg(test)]
pub(super) fn note_delete_leaf(leaf: PageId) {
    DELETE_ROWS_SEEN.with(|r| r.set(r.get() + 1));
    DELETE_LEAF_LAST.with(|last| {
        if last.get() != Some(leaf) {
            last.set(Some(leaf));
            DELETE_LEAF_RUNS.with(|c| c.set(c.get() + 1));
        }
    });
}

/// `(累计删除行数, 累计叶子游程数)` ⇒ 两者之比 = 合批后"每几行共用一趟下降"的上限。
/// ⚠️ 读数用**差值**（台架与守卫都按 `p1 − p0` 取，跟 `reorg_stats` 同一口径，不就地清零）。
#[cfg(test)]
pub(crate) fn delete_leaf_runs() -> (u64, u64) {
    (
        DELETE_ROWS_SEEN.with(|c| c.get()),
        DELETE_LEAF_RUNS.with(|c| c.get()),
    )
}
