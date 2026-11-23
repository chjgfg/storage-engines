//! maintenance_probe 的测试对照门与计量。
use std::{assert, cell::RefCell, thread_local, time::Instant};
#[derive(Debug, Clone, Default)]
pub(crate) struct Stats {
    pub flushes: u64,
    pub flush_ns: u128,
    pub compactions: u64,
    pub compact_ns: u128,
    pub input_files: u64,
    pub input_bytes: u64,
    pub trivial_moves: u64,
    pub moved_bytes: u64,
    pub output_files: u64,
    pub output_bytes: u64,
    pub owned_input_rows: u64,
    pub legacy_input_clones: u64,
    pub reference_input_clones: u64,
    pub compaction_blocks: u64,
    pub merge_ns: u128,
    pub repair_ns: u128,
    pub manifest_ns: u128,
    pub manifest_reuses: u64,
    pub guard_ns: u128,
    pub cleanup_ns: u128,
    pub sst_finish_ns: u128,
    pub sst_sync_ns: u128,
    pub sst_syncs: u64,
    pub delete_calls: u64,
    pub delete_write_ns: u128,
    pub threshold_flushes: u64,
    pub threshold_flush_ns: u128,
    pub mem_wal_syncs: u64,
    pub mem_wal_sync_ns: u128,
}
thread_local! { static ACTIVE: RefCell<Option<Stats>> = const { RefCell::new(None) }; }
pub(crate) struct Scope;
impl Scope {
    pub fn start() -> Self {
        ACTIVE.with(|s| {
            assert!(s.borrow().is_none());
            *s.borrow_mut() = Some(Stats::default());
        });
        Self
    }
    pub fn stats(&self) -> Stats {
        ACTIVE.with(|s| s.borrow().as_ref().unwrap().clone())
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        ACTIVE.with(|s| *s.borrow_mut() = None);
    }
}
pub(crate) fn input_row(kind: u8) {
    ACTIVE.with(|s| {
        if let Some(stats) = s.borrow_mut().as_mut() {
            match kind {
                0 => stats.owned_input_rows += 1,
                1 => stats.legacy_input_clones += 1,
                _ => stats.reference_input_clones += 1,
            }
        }
    });
}
pub(crate) fn input_block() {
    ACTIVE.with(|s| {
        if let Some(stats) = s.borrow_mut().as_mut() {
            stats.compaction_blocks += 1;
        }
    });
}
pub(crate) fn manifest_reused() {
    ACTIVE.with(|s| {
        if let Some(stats) = s.borrow_mut().as_mut() {
            stats.manifest_reuses += 1;
        }
    });
}
pub(crate) enum Kind {
    Flush,
    Compact,
    Merge,
    Repair,
    Manifest,
    Guard,
    Cleanup,
    SstFinish,
    SstSync,
    DeleteWrite,
    ThresholdFlush,
    MemWalSync,
}
pub(crate) struct Timer {
    kind: Kind,
    start: Instant,
}
impl Timer {
    pub fn start(kind: Kind) -> Option<Self> {
        ACTIVE.with(|s| {
            s.borrow().is_some().then(|| Self {
                kind,
                start: Instant::now(),
            })
        })
    }
}
impl Drop for Timer {
    fn drop(&mut self) {
        let ns = self.start.elapsed().as_nanos();
        ACTIVE.with(|s| {
            if let Some(stats) = s.borrow_mut().as_mut() {
                match self.kind {
                    Kind::Flush => {
                        stats.flushes += 1;
                        stats.flush_ns += ns;
                    }
                    Kind::Compact => {
                        stats.compactions += 1;
                        stats.compact_ns += ns;
                    }
                    Kind::Merge => stats.merge_ns += ns,
                    Kind::Repair => stats.repair_ns += ns,
                    Kind::Manifest => stats.manifest_ns += ns,
                    Kind::Guard => stats.guard_ns += ns,
                    Kind::Cleanup => stats.cleanup_ns += ns,
                    Kind::SstFinish => stats.sst_finish_ns += ns,
                    Kind::SstSync => {
                        stats.sst_sync_ns += ns;
                        stats.sst_syncs += 1;
                    }
                    Kind::DeleteWrite => {
                        stats.delete_calls += 1;
                        stats.delete_write_ns += ns;
                    }
                    Kind::ThresholdFlush => {
                        stats.threshold_flushes += 1;
                        stats.threshold_flush_ns += ns;
                    }
                    Kind::MemWalSync => {
                        stats.mem_wal_syncs += 1;
                        stats.mem_wal_sync_ns += ns;
                    }
                }
            }
        });
    }
}
pub(crate) fn moved(bytes: u64) {
    ACTIVE.with(|s| {
        if let Some(stats) = s.borrow_mut().as_mut() {
            stats.trivial_moves += 1;
            stats.moved_bytes += bytes;
        }
    });
}
pub(crate) fn inputs(files: u64, bytes: u64) {
    ACTIVE.with(|s| {
        if let Some(stats) = s.borrow_mut().as_mut() {
            stats.input_files += files;
            stats.input_bytes += bytes;
        }
    });
}
pub(crate) fn outputs(files: usize, bytes: u64) {
    ACTIVE.with(|s| {
        if let Some(stats) = s.borrow_mut().as_mut() {
            stats.output_files += files as u64;
            stats.output_bytes += bytes;
        }
    });
}
