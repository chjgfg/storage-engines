//! snapshot_probe 的测试对照门与计量。
use std::{assert, cell::RefCell, thread_local};
#[derive(Clone, Debug, Default)]
pub(crate) struct Stats {
    pub body_ns: u128,
    pub write_ns: u128,
    pub crc_ns: u128,
    pub sync_ns: u128,
    pub publish_ns: u128,
    pub total_ns: u128,
    pub crc_calls: u64,
    pub write_calls: u64,
    pub stop_mode: &'static str,
    pub visited_rows: u64,
    pub consumed_rows: u64,
    /// 输入拉取与折叠输出分段互斥，均在 body_ns 内；折叠输出仍包含 write_ns 和 crc_ns，嵌套分段不能相加。
    pub pull_ns: u128,
    pub fold_emit_ns: u128,
    pub input_batches: u64,
    pub input_buffer_bytes: usize,
    /// 普通逻辑组完成后直接输出，不进入二进制重叠窗口。
    pub fold_direct_rows: u64,
    pub fold_window_rows: u64,
    /// 清理有界输入批次属于 body_ns 内层开销，与折叠和输出分别计量。
    pub input_release_ns: u128,
    /// 延迟路径清理的输入槽数量，包括移走槽和窗口槽。
    pub release_rows: u64,
    pub record_buffer_bytes: usize,
    pub record_flushes: u64,
    pub record_buffered_rows: u64,
    /// 属于折叠输出内部，包含底层文件写入及 CRC。
    pub record_flush_ns: u128,
    pub record_direct_large_rows: u64,
    pub record_direct_large_ns: u128,
    pub record_reference_rows: u64,
}
thread_local! { static ACTIVE: RefCell<Option<(bool, Stats)>> = const { RefCell::new(None) }; }
pub(crate) struct Scope;
impl Scope {
    pub fn start(legacy_fields: bool) -> Self {
        ACTIVE.with(|s| {
            assert!(s.borrow().is_none());
            *s.borrow_mut() = Some((legacy_fields, Stats::default()));
        });
        Self
    }
    pub fn stats(&self) -> Stats {
        ACTIVE.with(|s| s.borrow().as_ref().unwrap().1.clone())
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        ACTIVE.with(|s| *s.borrow_mut() = None);
    }
}
pub(super) fn options() -> Option<bool> {
    ACTIVE.with(|s| s.borrow().as_ref().map(|s| s.0))
}
pub(crate) fn is_active() -> bool {
    options().is_some()
}
pub(crate) fn note(f: impl FnOnce(&mut Stats)) {
    ACTIVE.with(|s| {
        if let Some((_, stats)) = s.borrow_mut().as_mut() {
            f(stats);
        }
    });
}
