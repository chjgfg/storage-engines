//! 本文件负责测试计量与机制对照。

use super::*;

#[cfg(test)]
pub(crate) fn note_collect_held(n: usize) {
    COLLECT_HELD_MAX.with(|c| {
        let n = n as u64;
        if n > c.get() {
            c.set(n);
        }
    });
}

#[cfg(test)]
pub(crate) fn collect_held_max() -> u64 {
    COLLECT_HELD_MAX.with(|c| c.get())
}

#[cfg(test)]
pub(crate) fn reset_collect_held() {
    COLLECT_HELD_MAX.with(|c| c.set(0));
}
