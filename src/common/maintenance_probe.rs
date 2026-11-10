//! Opt-in, thread-local timings of maintenance doors in test builds only.
use std::{assert, cell::RefCell, thread_local, time::Instant};

#[derive(Clone, Copy)]
pub(crate) enum Stage { Checkpoint, Commit, AutoVacuum, VacuumScan, VacuumDelete, VacuumSync, VacuumBlobRefs, VacuumBlobGc }

#[derive(Clone, Default)]
pub(crate) struct Stats {
    pub ns: [u128; 8],
    pub calls: [u64; 8],
    pub blob_point_reads: u64,
}

impl std::fmt::Debug for Stats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut out = f.debug_struct("MaintenanceMs");
        for (i, name) in ["checkpoint", "commit", "auto_vacuum", "vacuum_scan", "vacuum_delete", "vacuum_sync", "vacuum_blob_refs", "vacuum_blob_gc"].into_iter().enumerate() {
            out.field(name, &(self.calls[i], self.ns[i] as f64 / 1e6));
        }
        out.field("blob_point_reads", &self.blob_point_reads).finish()
    }
}

thread_local! { static ACTIVE: RefCell<Option<Stats>> = const { RefCell::new(None) }; }

pub(crate) fn note_blob_read() {
    ACTIVE.with(|s| if let Some(stats) = s.borrow_mut().as_mut() { stats.blob_point_reads += 1; });
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

pub(crate) struct Timer { stage: Stage, start: Instant }
impl Timer {
    pub fn start(stage: Stage) -> Option<Self> {
        ACTIVE.with(|s| s.borrow().is_some().then(|| Self { stage, start: Instant::now() }))
    }
}
impl Drop for Timer {
    fn drop(&mut self) {
        let ns = self.start.elapsed().as_nanos();
        ACTIVE.with(|s| if let Some(stats) = s.borrow_mut().as_mut() {
            stats.ns[self.stage as usize] += ns;
            stats.calls[self.stage as usize] += 1;
        });
    }
}
