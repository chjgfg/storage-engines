//! 本文件负责测试计量与机制对照。

use super::*;

/// 当前线程复制过的 `EntryValue` 条数（测试探针用）
#[cfg(test)]
pub(crate) fn entry_value_clones() -> u64 {
    ENTRY_VALUE_CLONES.with(|c| c.get())
}
#[cfg(test)]
pub(crate) fn packed_promotions() -> u64 {
    PACKED_PROMOTIONS.with(std::cell::Cell::get)
}

/// 当前线程已发生的数据块访问次数（测试探针用）
#[cfg(test)]
pub(crate) fn block_reads() -> u64 {
    BLOCK_READS.with(|c| c.get())
}

/// 当前线程的 SST 块缓存未命中次数（测试探针用）
#[cfg(test)]
pub(crate) fn block_misses() -> u64 {
    BLOCK_MISSES.with(|c| c.get())
}

/// 当前线程在 SST 稀疏索引上做过的比较次数（测试探针用）
#[cfg(test)]
pub(crate) fn index_probes() -> u64 {
    INDEX_PROBES.with(|c| c.get())
}

#[cfg(test)]
pub(crate) fn note_light_visit() {
    LIGHT_VISITS.with(|c| c.set(c.get() + 1));
}

/// 当前线程折候选时检查过的编码键数（测试探针用）
#[cfg(test)]
pub(crate) fn light_visits() -> u64 {
    LIGHT_VISITS.with(|c| c.get())
}

#[cfg(test)]
pub(crate) fn note_merge_input_materialized(n: u64) {
    MERGE_INPUTS_MATERIALIZED.with(|c| c.set(c.get() + n));
}

/// 当前线程被整篇物化进内存的条目数（测试探针用）
#[cfg(test)]
pub(crate) fn merge_inputs_materialized() -> u64 {
    MERGE_INPUTS_MATERIALIZED.with(|c| c.get())
}

#[cfg(test)]
pub(crate) fn note_light_view_materialized(n: u64) {
    LIGHT_VIEWS_MATERIALIZED.with(|c| c.set(c.get() + n));
}

#[cfg(test)]
pub(crate) fn light_view_materialized() -> u64 {
    LIGHT_VIEWS_MATERIALIZED.with(|c| c.get())
}

/// 记一次「点读真的进了这个 SST 文件」（范围 + bloom 都过了才算；被 bloom 挡掉的不算代价）
#[cfg(test)]
pub(crate) fn note_sst_probe() {
    SST_PROBES.with(|c| c.set(c.get() + 1));
}

/// 记 n 次「条目键比较」
#[cfg(test)]
pub(crate) fn note_sst_entry_walks(n: u64) {
    SST_ENTRY_WALKS.with(|c| c.set(c.get() + n));
}

/// 当前线程点读探过的 SST 文件数（测试探针用）
#[cfg(test)]
pub(crate) fn sst_probes() -> u64 {
    SST_PROBES.with(|c| c.get())
}

/// 当前线程点读比较过的 SST 条目键数（测试探针用）
#[cfg(test)]
pub(crate) fn sst_entry_walks() -> u64 {
    SST_ENTRY_WALKS.with(|c| c.get())
}

/// 当前线程已物化过多少个带 value 的条目（测试探针用）
#[cfg(test)]
pub(crate) fn value_materializations() -> u64 {
    VALUE_MATERIALIZATIONS.with(|c| c.get())
}

/// 记一次「带 value 的条目被物化」；轻量路径不该调用它
#[cfg(test)]
pub(crate) fn note_value_materialization() {
    VALUE_MATERIALIZATIONS.with(|c| c.set(c.get() + 1));
}

#[cfg(test)]
pub(super) fn note_lru_inspection() {
    LRU_INSPECTIONS.with(|c| c.set(c.get() + 1));
}

/// 当前线程检查过的缓存槽位数（测试探针用）
#[cfg(test)]
pub(crate) fn lru_inspections() -> u64 {
    LRU_INSPECTIONS.with(|c| c.get())
}

#[cfg(test)]
pub(crate) fn builder_size_walks() -> u64 {
    BUILDER_SIZE_WALKS.with(|c| c.get())
}

#[cfg(test)]
pub(crate) fn builder_held_max() -> u64 {
    BUILDER_HELD_MAX.with(|c| c.get())
}

#[cfg(test)]
pub(crate) fn reset_builder_held_max() {
    BUILDER_HELD_MAX.with(|c| c.set(0));
}

#[cfg(test)]
pub(crate) fn builder_hashed_max() -> u64 {
    BUILDER_HASHED_MAX.with(|c| c.get())
}

#[cfg(test)]
pub(crate) fn reset_builder_hashed_max() {
    BUILDER_HASHED_MAX.with(|c| c.set(0));
}
