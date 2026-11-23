//! search_key_reference 的测试对照门与计量。
use std::{assert, cell::Cell, thread_local};
thread_local! { static ENABLED: Cell<bool> = const { Cell::new(false) }; static COPIES: Cell<u64> = const { Cell::new(0) }; }
pub(crate) fn enabled() -> bool {
    ENABLED.with(Cell::get)
}
pub(crate) fn note_copy() {
    COPIES.with(|c| c.set(c.get() + 1));
}
pub(crate) fn copies() -> u64 {
    COPIES.with(Cell::get)
}
pub(crate) struct Scope;
impl Scope {
    pub(crate) fn start() -> Self {
        ENABLED.with(|c| assert!(!c.replace(true)));
        Self
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        ENABLED.with(|c| c.set(false));
    }
}
