//! 本文件负责测试计量与机制对照。

use super::*;

/// 记一次「把 memtable 数了一遍」的步数（只由 `scan_mem_entry_count` 调用）
#[cfg(test)]
pub(crate) fn note_mem_scan_steps(n: u64) {
    MEM_SCAN_STEPS.with(|c| c.set(c.get() + n));
}

/// 当前线程为"数 memtable 条目"走过的条目总数（测试探针用）
#[cfg(test)]
pub(crate) fn mem_scan_steps() -> u64 {
    MEM_SCAN_STEPS.with(|c| c.get())
}

/// 当前线程被激活的源数（测试探针用）
#[cfg(test)]
pub(crate) fn merge_activations() -> u64 {
    SOURCE_ACTIVATIONS.with(|c| c.get())
}

#[cfg(test)]
pub(super) fn note_source_activation() {
    SOURCE_ACTIVATIONS.with(|c| c.set(c.get() + 1));
}
