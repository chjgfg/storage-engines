//! vacuum_compaction_reference 的测试对照门与计量。
use std::{assert, cell::Cell, thread_local};
thread_local! { static OLD: Cell<bool> = const { Cell::new(false) }; }
pub(crate) fn enabled() -> bool {
    OLD.with(Cell::get)
}
pub(crate) struct Scope;
impl Scope {
    pub(crate) fn start() -> Self {
        OLD.with(|c| assert!(!c.replace(true)));
        Self
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        OLD.with(|c| c.set(false));
    }
}
