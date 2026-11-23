//! 本文件负责测试计量与机制对照。

use super::*;

/// 当前线程已发生的页访问次数（测试探针用）
#[cfg(test)]
pub(crate) fn page_reads() -> u64 {
    PAGE_READS.with(|c| c.get())
}

/// 标注"接下来的页读属于哪一段"（只在删除路径里调；一趟删除结束要设回 [`PHASE_OTHER`]）。
/// 非 test 构建里这个函数体是空的 ⇒ 标注留在业务代码里没有代价。
#[inline(always)]
pub(crate) fn set_page_phase(_phase: u8) {
    #[cfg(test)]
    PAGE_PHASE.with(|c| c.set(_phase));
}

/// 各阶段各读了多少页（长度 [`PHASE_BUCKETS`]）
#[cfg(test)]
pub(crate) fn page_reads_by_phase() -> [u64; PHASE_BUCKETS] {
    PAGE_READS_PHASE.with(|a| {
        let mut out = [0u64; PHASE_BUCKETS];
        for (i, c) in a.iter().enumerate() {
            out[i] = c.get();
        }
        out
    })
}

/// 两扇读页门共用的记账：把当前阶段那只桶 +1
#[cfg(test)]
pub(super) fn note_phase_read() {
    let p = PAGE_PHASE.with(|c| c.get()) as usize;
    PAGE_READS_PHASE.with(|a| {
        let i = p.min(PHASE_BUCKETS - 1);
        a[i].set(a[i].get() + 1);
    });
}

/// 本线程发生的超载写次数（诊断与守卫用）
pub fn oversized_writes() -> u64 {
    OVERSIZED_WRITES.with(|c| c.get())
}

#[cfg(test)]
pub(crate) fn cache_hits() -> u64 {
    CACHE_HITS.with(|c| c.get())
}

#[cfg(test)]
pub(crate) fn cache_misses() -> u64 {
    CACHE_MISSES.with(|c| c.get())
}

#[cfg(test)]
pub(crate) fn page_evictions() -> u64 {
    PAGE_EVICTIONS.with(|c| c.get())
}

#[cfg(test)]
pub(crate) fn lru_compactions() -> u64 {
    LRU_COMPACTIONS.with(|c| c.get())
}

#[cfg(test)]
pub(crate) fn batch_flushed_pages() -> u64 {
    BATCH_FLUSHED_PAGES.with(|c| c.get())
}

/// 当前线程已发生的 value 克隆次数（测试探针用）
#[cfg(test)]
pub(crate) fn value_clones() -> u64 {
    VALUE_CLONES.with(|c| c.get())
}

/// 记一次 value 克隆（重量路径调用；轻量路径不该调用）
#[cfg(test)]
pub(crate) fn note_value_clone() {
    VALUE_CLONES.with(|c| c.set(c.get() + 1));
}
