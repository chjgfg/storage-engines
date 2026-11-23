//! commit_probe 的测试对照门与计量。
use std::{assert, cell::RefCell, time::Instant};

const SYNC_LIMITS_NS: [u128; 7] = [
    1_000_000,
    5_000_000,
    10_000_000,
    25_000_000,
    50_000_000,
    100_000_000,
    250_000_000,
];

#[derive(Clone, Debug, Default)]
pub(crate) struct Stats {
    pub(crate) attempts: u64,
    pub(crate) append_calls: u64,
    pub(crate) append_ns: u128,
    pub(crate) append_max_ns: u128,
    pub(crate) sync_calls: u64,
    pub(crate) sync_ns: u128,
    pub(crate) sync_max_ns: u128,
    /// Buckets: <=1, 1-5, 5-10, 10-25, 25-50, 50-100, 100-250, >250 ms.
    pub(crate) sync_histogram: [u64; 8],
    pub(crate) fsync_calls: u64,
    pub(crate) fsync_ns: u128,
    pub(crate) fsync_max_ns: u128,
}

std::thread_local! {
    static ACTIVE: RefCell<Option<Stats>> = const { RefCell::new(None) };
}

pub(crate) struct Scope;

impl Scope {
    pub(crate) fn start() -> Self {
        ACTIVE.with(|active| {
            assert!(active.borrow().is_none(), "nested WAL commit probe scope");
            *active.borrow_mut() = Some(Stats::default());
        });
        Self
    }

    pub(crate) fn stats(&self) -> Stats {
        ACTIVE.with(|active| active.borrow().as_ref().unwrap().clone())
    }
}

impl Drop for Scope {
    fn drop(&mut self) {
        ACTIVE.with(|active| *active.borrow_mut() = None);
    }
}

pub(super) enum Stage {
    Append,
    Sync,
    Fsync,
}

pub(super) struct Timer {
    stage: Stage,
    start: Instant,
}

impl Timer {
    pub(super) fn start(stage: Stage) -> Option<Self> {
        ACTIVE.with(|active| {
            active.borrow().is_some().then(|| Self {
                stage,
                start: Instant::now(),
            })
        })
    }
}

pub(super) fn note_attempt() {
    ACTIVE.with(|active| {
        if let Some(stats) = active.borrow_mut().as_mut() {
            stats.attempts += 1;
        }
    });
}

impl Drop for Timer {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed().as_nanos();
        ACTIVE.with(|active| {
            if let Some(stats) = active.borrow_mut().as_mut() {
                match self.stage {
                    Stage::Append => {
                        stats.append_calls += 1;
                        stats.append_ns += elapsed;
                        stats.append_max_ns = stats.append_max_ns.max(elapsed);
                    }
                    Stage::Sync => {
                        stats.sync_calls += 1;
                        stats.sync_ns += elapsed;
                        stats.sync_max_ns = stats.sync_max_ns.max(elapsed);
                        let bucket = SYNC_LIMITS_NS.partition_point(|limit| elapsed > *limit);
                        stats.sync_histogram[bucket] += 1;
                    }
                    Stage::Fsync => {
                        stats.fsync_calls += 1;
                        stats.fsync_ns += elapsed;
                        stats.fsync_max_ns = stats.fsync_max_ns.max(elapsed);
                    }
                }
            }
        });
    }
}
