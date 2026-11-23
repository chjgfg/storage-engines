//! vacuum_delete_probe 的测试对照门与计量。
use std::{assert, cell::Cell, thread_local};
thread_local! {
    static OLD: Cell<bool> = const { Cell::new(false) };
    static READS: Cell<u64> = const { Cell::new(0) };
}
pub(crate) fn reference() -> bool {
    OLD.with(Cell::get)
}
pub(crate) fn reads() -> u64 {
    READS.with(Cell::get)
}
pub(super) fn read() {
    READS.with(|s| s.set(s.get() + 1));
}
pub(crate) struct Scope;
impl Scope {
    pub(crate) fn start() -> Self {
        OLD.with(|s| assert!(!s.replace(true)));
        Self
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        OLD.with(|s| s.set(false));
    }
}
