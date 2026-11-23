//! trivial_move_probe 的测试对照门与计量。
use std::{assert, cell::Cell, thread_local};
thread_local! { static REFERENCE: Cell<bool> = const { Cell::new(false) }; }
pub fn reference() -> bool {
    REFERENCE.with(|c| c.get())
}
pub struct Scope;
impl Scope {
    pub fn start() -> Self {
        REFERENCE.with(|c| assert!(!c.replace(true)));
        Self
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        REFERENCE.with(|c| c.set(false));
    }
}
