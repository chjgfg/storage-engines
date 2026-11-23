//! scan_cost_reference 的测试对照门与计量。
use std::{assert, cell::Cell, thread_local};
thread_local! { static ENABLED: Cell<bool> = const { Cell::new(false) }; }
thread_local! { static KEY_COPIES: Cell<u64> = const { Cell::new(0) }; }
thread_local! { static FOLD_CURSOR_COPIES: Cell<u64> = const { Cell::new(0) }; }
thread_local! { static PAGE_CURSOR_COPIES: Cell<u64> = const { Cell::new(0) }; }
pub(crate) fn enabled() -> bool {
    ENABLED.with(Cell::get)
}
pub(crate) fn note_key_copy() {
    KEY_COPIES.with(|s| s.set(s.get() + 1));
}
pub(crate) fn key_copies() -> u64 {
    KEY_COPIES.with(Cell::get)
}
pub(crate) fn note_fold_cursor_copy() {
    FOLD_CURSOR_COPIES.with(|s| s.set(s.get() + 1));
}
pub(crate) fn fold_cursor_copies() -> u64 {
    FOLD_CURSOR_COPIES.with(Cell::get)
}
pub(crate) fn note_page_cursor_copy() {
    PAGE_CURSOR_COPIES.with(|s| s.set(s.get() + 1));
}
pub(crate) fn page_cursor_copies() -> u64 {
    PAGE_CURSOR_COPIES.with(Cell::get)
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
