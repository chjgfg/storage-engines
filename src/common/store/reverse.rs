//! 反向候选查找与有界末页。

use super::*;

/// 本线程 `seek_prev` 往回退过的键数（测试探针用）
#[cfg(test)]
pub fn seek_prev_steps() -> u64 {
    SEEK_PREV_STEPS.with(|c| c.get())
}

/// `seek_prev` 的一份循环：**反向一步取候选键，判定复用引擎现成的单键路径**。
///
/// - `prev_raw(bound)`：编码键 `≤ bound` 的**最大那一行**属于哪把逻辑键；`None` = 走到库头了
/// - `live(raw)`：这把键对**当前事务**可见、未过期、非墓碑时交出那一行（用
///   [`live_record_of_logical`] 配引擎的 `latest_visible_raw`，别在这里另写判定）
///
/// 上界那两条规则都不显然：
/// - 第一轮用 `version_key_upper_bound(key)` ⇒ 含 `key` **自己那一组**（语义是"≤ key"不是"< key"）；
/// - 之后用被拒的那把 raw 本身当上界：它的每行都是 `raw ‖ version_be` > `raw` ⇒ 整组自动排除，
///   而所有更小的键都还在。
///
/// ⚠️ "反向第一个见到的 raw 就是最大的那把键"依赖版本键的老合同 **`version < 2^56`**
/// （见 [`crate::common::version_key_upper_bound`]）：首字节 0x00 小于任何 ASCII，短前缀键的版本行
/// 才翻不到更长键之上。合同破了这里会**静默返回偏小的键** —— 那条合同自己有守卫，别拆。
pub fn seek_prev_with(
    at_or_below: &[u8],
    mut prev_raw: impl FnMut(&[u8]) -> Option<Vec<u8>>,
    mut live: impl FnMut(&[u8]) -> Option<ExportRecord>,
) -> Option<ExportRecord> {
    let mut bound = crate::common::version_key_upper_bound(at_or_below);
    loop {
        #[cfg(test)]
        SEEK_PREV_STEPS.with(|c| c.set(c.get() + 1));
        let raw = prev_raw(&bound)?;
        if let Some(rec) = live(&raw) {
            return Some(rec);
        }
        if raw.as_slice() >= bound.as_slice() {
            // 只有某家的 `prev_raw` 没按界排除整组时才会走到这里；宁可返回 None 也不原地打转
            return None;
        }
        bound = raw;
    }
}

/// 本线程 `back_n_with` 走过的候选键数（测试探针用）
#[cfg(test)]
pub fn back_n_candidates() -> u64 {
    BACK_N_CANDIDATES.with(|c| c.get())
}

/// 本线程 `back_n_with` 退过头、让调用方回退到全库截尾的次数（测试探针用）
#[cfg(test)]
pub fn back_n_fallbacks() -> u64 {
    BACK_N_FALLBACKS.with(|c| c.get())
}

#[cfg(test)]
pub(crate) fn note_reverse_candidate() {
    BACK_N_CANDIDATES.with(|c| c.set(c.get() + 1));
    SEEK_PREV_STEPS.with(|c| c.set(c.get() + 1));
}

#[cfg(test)]
pub(crate) fn note_reverse_fallback() {
    BACK_N_FALLBACKS.with(|c| c.set(c.get() + 1));
}

/// 从库尾反向收集 `n` 条**逻辑键**（升序交出）—— `KvTxn::search_last_page` 的逐步退实现（M82）。
///
/// 与 [`seek_prev_with`] 同源（那条是"取 1 条"），推进规则也一样：每拿到一把键，就把**它本身**
/// 当下一轮的上界 —— 版本键合同 `raw ‖ version_be > raw` ⇒ 同一把键的整组自动出局，不会在同一把
/// 键的旧版本上打转（`find_back` 的 `excluded` 记的是**编码键**，按它拦不住同键旧版本）。
///
/// - `max_raw()`：库里**真实存在**的最大 raw_key（`None` = 空库 ⇒ 空页）。
///   ⚠️ **不造 `\xFF…` 哨兵**：任意字节空间没有最大值，一个"足够长的 0xFF"上界在键更长时就会把
///   库尾的键整批漏掉。让引擎交出自己索引里的尾键才是对的（三家都是 O(log n)：BTreeMap 的
///   `next_back`、B+ 树的最右下降）。
/// - `prev_raw(bound)` / `live(raw)`：与 [`seek_prev_with`] 的两个口子同义，**含端点**（`≤ bound`）。
/// - 退过 `max_steps` 个候选还凑不满 `n` ⇒ 返回 `None`，调用方回退到 `reverse_scan` 截尾
///   （慢，但正确）。这一支是给"库尾连着一大片墓碑/过期键"准备的形状，与 lsm 那条
///   `LAST_PAGE_MAX_STEPS` 同一类问题、同一个界。
pub fn back_n_with(
    n: usize,
    max_steps: usize,
    max_raw: impl FnOnce() -> Option<Vec<u8>>,
    mut prev_raw: impl FnMut(&[u8]) -> Option<Vec<u8>>,
    mut live: impl FnMut(&[u8]) -> Option<ExportRecord>,
) -> Option<Vec<ExportRecord>> {
    let mut out: Vec<ExportRecord> = Vec::with_capacity(n.min(4096));
    let Some(tail) = max_raw() else {
        return Some(out);
    };
    // 第一轮上界含端点：`version_key_upper_bound` 让它覆盖尾键**自己那一组**（语义是"≤ 尾键"）
    let mut bound = crate::common::version_key_upper_bound(&tail);
    let mut steps = 0usize;
    while out.len() < n {
        let raw = prev_raw(&bound)?;
        steps += 1;
        #[cfg(test)]
        BACK_N_CANDIDATES.with(|c| c.set(c.get() + 1));
        if steps > max_steps {
            #[cfg(test)]
            BACK_N_FALLBACKS.with(|c| c.set(c.get() + 1));
            return None;
        }
        if let Some(rec) = live(&raw) {
            out.push(rec);
            bound = raw;
            continue;
        }
        if raw.as_slice() >= bound.as_slice() {
            // 同 seek_prev_with：`prev_raw` 没按界排除整组时的兜底，宁可收尾也不原地打转
            break;
        }
        bound = raw;
    }
    out.reverse(); // 收集是降序，`SearchPage::items` 的合同是升序
    Some(out)
}
