//! 本文件负责测试计量与机制对照。

use super::*;

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

#[cfg(test)]
pub(crate) fn set_vac_chunk_override(n: usize) {
    VAC_CHUNK_OVERRIDE.with(|c| c.set(n));
}

#[inline]
pub(super) fn vac_chunk_limit() -> usize {
    #[cfg(test)]
    {
        let o = VAC_CHUNK_OVERRIDE.with(|c| c.get());
        if o > 0 {
            return o;
        }
    }
    VAC_CHUNK_ROWS
}
