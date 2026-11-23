//! rebalance_read_reference 的测试对照门与计量。
use std::{assert, cell::Cell, thread_local};
thread_local! { static ENABLED: Cell<bool> = const { Cell::new(false) }; }
pub(crate) fn enabled() -> bool {
    ENABLED.with(Cell::get)
}
pub(crate) struct Scope;
impl Scope {
    pub(crate) fn start() -> Self {
        ENABLED.with(|s| assert!(!s.replace(true)));
        Self
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        ENABLED.with(|s| s.set(false));
    }
}
