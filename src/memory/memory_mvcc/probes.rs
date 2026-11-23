//! 本文件负责测试计量与机制对照。

use super::*;

/// 当前线程折叠时检查过的版本键数（测试探针用）
#[cfg(test)]
pub(crate) fn fold_keys() -> u64 {
    FOLD_KEYS.with(|c| c.get())
}

#[cfg(test)]
pub(super) fn note_vac_pending(n: usize) {
    VAC_PENDING_MAX.with(|c| {
        let n = n as u64;
        if n > c.get() {
            c.set(n);
        }
    });
}

#[cfg(test)]
pub(crate) fn vac_pending_max() -> u64 {
    VAC_PENDING_MAX.with(|c| c.get())
}

#[cfg(test)]
pub(crate) fn reset_vac_pending() {
    VAC_PENDING_MAX.with(|c| c.set(0));
}
