//! 候选窗口的分片折叠与即时消费。

use super::*;

/// 窗口折叠的共用驱动（四引擎一份）。
///
/// `round(after, budget)`：从 `after` 这组**之后**开始折，最多交 `budget` 组候选，
/// 返回 `(候选, 是否已走到界尾)`。`keep(candidate)`：候选 ⇒ 记录（取值 + 剥壳 +
/// TTL/墓碑过滤），`None` = 这一条不算命中窗口。`wanted = None` = 一趟走完全库
/// （**逐片**走完，每片 `FOLD_CHUNK` 组：语义还是"全库"，只是不再把整库候选一次装进内存）。
///
/// 为什么要能续跑第二轮：**"这把键命不命中 pattern"在键这一层就能判，而"这行到底
/// 取不取到值"（blob 指针、TTL 是否过期）要取了值才知道** ⇒ 提前收手有可能少数几条。
/// 于是从上一轮的最后一个键续跑、预算翻倍 ⇒ 总代价仍是最终那一轮的同阶，最坏退化成
/// "走完全库"，而那正好是 `wanted = None`（要精确 total）的语义。同一个几何重试的形状
/// 在 `vacuum_bounded` 的窗口放大里已经用过一次。
/// ⚠️ 几何翻倍只给**有界**窗口：无界折叠要的是"每片固定大小、走到库尾"，跟着翻等于把
///   刚省下来的瞬时内存又按轮数放大回去。
///
/// 为什么值读取放在 `keep` 而不是折在 `round` 里：bitcask/bplus/lsm 的取值要拿 blob 锁，
/// 而候选是**拿着 kv 锁走元数据**折出来的 —— 今天的两段式就是为了不在这两把锁上打结。
pub fn fold_window<V, R, K, T>(wanted: Option<usize>, round: R, keep: K) -> Vec<T>
where
    R: FnMut(Option<&[u8]>, usize) -> (Vec<WinCandidate<V>>, bool),
    K: FnMut(WinCandidate<V>) -> Option<T>,
{
    let mut out: Vec<T> = Vec::new();
    // `wanted` 那条"多要一条只为知道后面还有没有"的合同留在调用方（collect_rows），
    // 这里只负责交够数就停
    fold_window_emit(wanted, round, keep, |t| {
        out.push(t);
        #[cfg(test)]
        note_collect_held(out.len());
        true
    });
    out
}
/// 按片折叠候选并即时交付结果，仅为跨片续扫保留最后游标。
/// 预算数的是完成的逻辑组，过滤掉的行仍必须推进扫描；不能以返回行数代替进度。

/// [`fold_window`] 的**落点版**：每折出一行立刻交给 `emit`，**一行都不攒**。
///
/// 为什么要它（M27）：`KvTxn::scan_foreach` 的文档一直写着"不收集全库到单一大 Vec"，
/// 可它的实现是 `for … in collect_latest_raw_in(...)` —— 先把全库的
/// `(key, version, 存储字节)` 元组攒成 `Vec`，再逐条搬成 `ExportRecord`。
/// 200k 键实测因此多占 ~15 MB（峰值 +45.5 而结果集只有 30.7），而且"提前叫停"根本省不到
/// 折的那一段。有了落点版，流式才是真的：`emit` 返回 `false` 当场收手，后面的键**不再折**。
///
/// 返回值 = 实际交给 `emit` 的行数（`wanted` 凑满或走到界尾或被打断都是它）。
pub fn fold_window_emit<V, R, K, T, E>(
    wanted: Option<usize>,
    mut round: R,
    mut keep: K,
    mut emit: E,
) -> usize
where
    R: FnMut(Option<&[u8]>, usize) -> (Vec<WinCandidate<V>>, bool),
    K: FnMut(WinCandidate<V>) -> Option<T>,
    E: FnMut(T) -> bool,
{
    let mut handed = 0usize;
    let mut after: Option<Vec<u8>> = None;
    let mut budget = wanted.unwrap_or(FOLD_CHUNK);
    loop {
        let (batch, reached_end) = round(after.as_deref(), budget);
        let produced = batch.len();
        for (index, cand) in batch.into_iter().enumerate() {
            #[cfg(test)]
            let old_cursor = crate::common::store::scan_cost_reference::enabled();
            #[cfg(not(test))]
            let old_cursor = false;
            // 完整切片续扫只需要最后一个物理候选；即使行映射拒绝了墓碑，也要保留该游标。
            if old_cursor || (!reached_end && index + 1 == produced) {
                #[cfg(test)]
                crate::common::store::scan_cost_reference::note_fold_cursor_copy();
                after = Some(cand.key.clone());
            }
            if let Some(kept) = keep(cand) {
                if !emit(kept) {
                    return handed;
                }
                handed += 1;
                if wanted.is_some_and(|w| handed >= w) {
                    return handed;
                }
            }
        }
        if reached_end || produced == 0 {
            return handed;
        }
        // 几何放大只服务于**有界窗口**（"这一片没凑够 limit 条"那种情况）；
        // 无界折叠每片固定 FOLD_CHUNK —— 跟着翻等于把刚省下的瞬时内存按轮数放大回去。
        if wanted.is_some() {
            budget = budget.saturating_mul(2).max(64);
        }
    }
}
