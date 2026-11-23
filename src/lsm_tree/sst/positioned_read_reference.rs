//! positioned_read_reference 的测试对照门与计量。
use std::{cell::Cell, thread_local};

thread_local! {
    static ENABLED: Cell<bool> = const { Cell::new(false) };
    static SEEKS: Cell<u64> = const { Cell::new(0) };
}

pub(crate) fn enabled() -> bool {
    ENABLED.with(Cell::get)
}
pub(crate) fn seeks() -> u64 {
    SEEKS.with(Cell::get)
}
pub(super) fn note_seek() {
    SEEKS.with(|count| count.set(count.get() + 1));
}

pub(crate) struct Scope(bool);
impl Scope {
    pub(crate) fn start() -> Self {
        Self(ENABLED.with(|enabled| enabled.replace(true)))
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        ENABLED.with(|enabled| enabled.set(self.0));
    }
}
