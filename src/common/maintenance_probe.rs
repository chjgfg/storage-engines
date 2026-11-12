//! Opt-in, thread-local timings of maintenance doors in test builds only.
use std::{assert, cell::RefCell, thread_local, time::Instant};

#[derive(Clone, Copy)]
pub(crate) enum Stage { Checkpoint, Commit, AutoVacuum, VacuumScan, VacuumDelete, VacuumSync, VacuumBlobRefs, VacuumBlobGc, DblwrReset, DblwrData, DblwrPublish, DblwrClear, PagePack, PageWrite, PageSync, Freelist }

#[derive(Clone, Default)]
pub(crate) struct Stats {
    pub ns: [u128; 16],
    pub calls: [u64; 16],
    pub max_ns: [u128; 16],
    pub allocations: [u64; 16],
    /// Scan calls in <=1, <=5, <=10, <=50, <=100, >100 ms buckets.
    pub scan_histogram: [u64; 6],
    pub blob_point_reads: u64,
    pub bitcask_empty_sweeps: u64,
    pub bitcask_reference_sweeps: u64,
    pub bitcask_value_reads: u64,
    pub lsm_vacuum_owned_rows: u64,
    pub lsm_vacuum_legacy_key_copies: u64,
    pub lsm_vacuum_blocks: u64,
    pub bplus_pack_buffers: u64,
    pub bplus_backtrack_reuses: u64,
    pub bplus_maintenance_pages: u64,
    pub bplus_maintenance_rows: u64,
    pub lsm_known_deletes: u64,
}

impl std::fmt::Debug for Stats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut out = f.debug_struct("MaintenanceMs");
        for (i, name) in ["checkpoint", "commit", "auto_vacuum", "vacuum_scan", "vacuum_delete", "vacuum_sync", "vacuum_blob_refs", "vacuum_blob_gc", "dblwr_reset", "dblwr_data", "dblwr_publish", "dblwr_clear", "page_pack", "page_write", "page_sync", "freelist"].into_iter().enumerate() {
            out.field(name, &(self.calls[i], self.ns[i] as f64 / 1e6));
        }
        out.field("vacuum_scan_max_ms", &(self.max_ns[Stage::VacuumScan as usize] as f64 / 1e6))
            .field("vacuum_scan_allocations", &self.allocations[Stage::VacuumScan as usize])
            .field("vacuum_scan_histogram", &self.scan_histogram)
            .field("blob_point_reads", &self.blob_point_reads)
            .field("bitcask_empty_sweeps", &self.bitcask_empty_sweeps)
            .field("bitcask_reference_sweeps", &self.bitcask_reference_sweeps)
            .field("bitcask_value_reads", &self.bitcask_value_reads)
            .field("lsm_vacuum_owned_rows", &self.lsm_vacuum_owned_rows)
            .field("lsm_vacuum_legacy_key_copies", &self.lsm_vacuum_legacy_key_copies)
            .field("lsm_vacuum_blocks", &self.lsm_vacuum_blocks)
            .field("bplus_pack_buffers", &self.bplus_pack_buffers)
            .field("bplus_backtrack_reuses", &self.bplus_backtrack_reuses)
            .field("bplus_maintenance_pages", &self.bplus_maintenance_pages)
            .field("bplus_maintenance_rows", &self.bplus_maintenance_rows)
            .field("lsm_known_deletes", &self.lsm_known_deletes).finish()
    }
}

thread_local! { static ACTIVE: RefCell<Option<Stats>> = const { RefCell::new(None) }; }

pub(crate) fn note_blob_read() {
    ACTIVE.with(|s| if let Some(stats) = s.borrow_mut().as_mut() { stats.blob_point_reads += 1; });
}

pub(crate) fn note_bitcask_scan(inspect: bool) {
    ACTIVE.with(|s| if let Some(stats) = s.borrow_mut().as_mut() {
        if inspect { stats.bitcask_reference_sweeps += 1; } else { stats.bitcask_empty_sweeps += 1; }
    });
}
pub(crate) fn note_bitcask_value_read() {
    ACTIVE.with(|s| if let Some(stats) = s.borrow_mut().as_mut() { stats.bitcask_value_reads += 1; });
}

pub(crate) fn note_lsm_vacuum_row(owned: bool) {
    ACTIVE.with(|s| if let Some(stats) = s.borrow_mut().as_mut() {
        if owned { stats.lsm_vacuum_owned_rows += 1; } else { stats.lsm_vacuum_legacy_key_copies += 1; }
    });
}
pub(crate) fn note_lsm_vacuum_block() {
    ACTIVE.with(|s| if let Some(stats) = s.borrow_mut().as_mut() { stats.lsm_vacuum_blocks += 1; });
}

pub(crate) fn note_bplus_pack_buffer() {
    ACTIVE.with(|s| if let Some(stats) = s.borrow_mut().as_mut() { stats.bplus_pack_buffers += 1; });
}
pub(crate) fn note_bplus_backtrack_reuse() {
    ACTIVE.with(|s| if let Some(stats) = s.borrow_mut().as_mut() { stats.bplus_backtrack_reuses += 1; });
}

pub(crate) fn note_bplus_maintenance_page(rows: usize) {
    ACTIVE.with(|s| if let Some(stats) = s.borrow_mut().as_mut() {
        stats.bplus_maintenance_pages += 1; stats.bplus_maintenance_rows += rows as u64;
    });
}

pub(crate) fn note_lsm_known_delete() {
    ACTIVE.with(|s| if let Some(stats) = s.borrow_mut().as_mut() { stats.lsm_known_deletes += 1; });
}

pub(crate) struct Scope;
impl Scope {
    pub fn start() -> Self {
        ACTIVE.with(|s| { assert!(s.borrow().is_none()); *s.borrow_mut() = Some(Stats::default()); });
        Self
    }
    pub fn stats(&self) -> Stats { ACTIVE.with(|s| s.borrow().as_ref().unwrap().clone()) }
}
impl Drop for Scope { fn drop(&mut self) { ACTIVE.with(|s| *s.borrow_mut() = None); } }

pub(crate) struct Timer { stage: Stage, start: Instant, allocations: u64 }
impl Timer {
    pub fn start(stage: Stage) -> Option<Self> {
        ACTIVE.with(|s| s.borrow().is_some().then(|| Self { stage, start: Instant::now(), allocations: crate::mem_probe::allocations() }))
    }
}
impl Drop for Timer {
    fn drop(&mut self) {
        let ns = self.start.elapsed().as_nanos();
        ACTIVE.with(|s| if let Some(stats) = s.borrow_mut().as_mut() {
            stats.ns[self.stage as usize] += ns;
            stats.calls[self.stage as usize] += 1;
            stats.max_ns[self.stage as usize] = stats.max_ns[self.stage as usize].max(ns);
            stats.allocations[self.stage as usize] += crate::mem_probe::allocations().saturating_sub(self.allocations);
            if let Stage::VacuumScan = self.stage {
                let bucket = [1_000_000, 5_000_000, 10_000_000, 50_000_000, 100_000_000]
                    .partition_point(|bound| ns > *bound);
                stats.scan_histogram[bucket] += 1;
            }
        });
    }
}
