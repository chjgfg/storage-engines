//! dblwr_probe 的测试对照门与计量。
use std::{assert, assert_eq, cell::Cell, thread_local};
thread_local! {
    static REFERENCE: Cell<bool> = const { Cell::new(false) };
    static PAYLOAD_WRITES: Cell<u64> = const { Cell::new(0) };
    static FAULT: Cell<u8> = const { Cell::new(0) };
}
pub fn reference() -> bool {
    REFERENCE.with(|c| c.get())
}
pub fn writes() -> u64 {
    PAYLOAD_WRITES.with(|c| c.get())
}
pub fn note_write() {
    PAYLOAD_WRITES.with(|c| c.set(c.get() + 1));
}
// 1～3 为双写区故障，4 为主文件写入，5 为主文件同步，6 为空闲清单写入，7 为空闲清单同步。
pub fn fail_at(at: u8) -> std::io::Result<()> {
    if FAULT.with(|c| c.get() == at && c.replace(0) == at) {
        return Err(std::io::Error::other("injected double-write interruption"));
    }
    Ok(())
}
pub struct Scope;
impl Scope {
    pub fn start() -> Self {
        REFERENCE.with(|c| assert!(!c.replace(true)));
        Self
    }
    pub fn fault(at: u8) -> Self {
        FAULT.with(|c| assert_eq!(c.replace(at), 0));
        Self
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        REFERENCE.with(|c| c.set(false));
        FAULT.with(|c| c.set(0));
    }
}
