//! 本文件负责测试计量与机制对照。

use super::*;

#[cfg(test)]
pub(crate) fn note_tail_buffer(rows: usize) {
    TAIL_BUFFER_PEAK.with(|c| c.set(c.get().max(rows)));
}
#[cfg(test)]
pub(crate) fn take_tail_buffer_peak() -> usize {
    TAIL_BUFFER_PEAK.with(|c| c.replace(0))
}
