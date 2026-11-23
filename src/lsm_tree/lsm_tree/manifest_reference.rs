//! manifest_reference 的测试对照门与计量。
use std::{
    assert,
    cell::Cell,
    io::{Result, Write},
    thread_local,
};
thread_local! {
    static ENABLED: Cell<bool> = const { Cell::new(false) };
    static WRITES: Cell<u64> = const { Cell::new(0) };
}
pub fn enabled() -> bool {
    ENABLED.with(|c| c.get())
}
pub fn writes() -> u64 {
    WRITES.with(|c| c.get())
}
pub struct Scope;
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
pub struct Sink<W>(pub W);
impl<W: Write> Write for Sink<W> {
    fn write(&mut self, bytes: &[u8]) -> Result<usize> {
        WRITES.with(|c| c.set(c.get() + 1));
        self.0.write(bytes)
    }
    fn flush(&mut self) -> Result<()> {
        self.0.flush()
    }
}
