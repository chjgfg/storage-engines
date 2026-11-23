//! eviction_reference 的测试对照门与计量。
use std::{assert, cell::Cell, thread_local};
thread_local! { static ENABLED: Cell<bool> = const { Cell::new(false) }; }
pub(crate) fn enabled() -> bool {
    ENABLED.with(|c| c.get())
}
pub(crate) struct Scope;
impl Scope {
    pub fn start() -> Self {
        ENABLED.with(|c| assert!(!c.replace(true)));
        Self
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        ENABLED.with(|c| c.set(false));
    }
}
