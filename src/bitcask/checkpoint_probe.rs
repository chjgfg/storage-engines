//! Opt-in, thread-local test instrumentation. Never compiled into the library.
use std::{cell::RefCell, thread_local, time::Instant};

#[derive(Clone, Default, Debug)]
pub(crate) struct Stats {
    pub checkpoints: u64,
    pub checkpoint_ns: u128,
    pub flush_ns: u128,
    pub hint_ns: u128,
    pub wal_ns: u128,
    pub hint_rows: u64,
    pub hint_bytes: u64,
    pub commit_ns: Vec<u128>,
}

thread_local! {
    static ACTIVE: RefCell<Option<Stats>> = const { RefCell::new(None) };
}

pub(crate) struct Scope;
impl Scope {
    pub fn start() -> Self {
        ACTIVE.with(|slot| {
            assert!(slot.borrow().is_none(), "nested checkpoint probe");
            *slot.borrow_mut() = Some(Stats::default());
        });
        Self
    }
    pub fn stats(&self) -> Stats {
        ACTIVE.with(|slot| slot.borrow().as_ref().expect("active probe").clone())
    }
}
impl Drop for Scope {
    fn drop(&mut self) { ACTIVE.with(|slot| *slot.borrow_mut() = None); }
}

#[derive(Clone, Copy)]
pub(crate) enum Stage { Checkpoint, Flush, Hint, Wal, Commit }
pub(crate) struct Timer { stage: Stage, start: Instant }
impl Timer {
    pub fn start(stage: Stage) -> Option<Self> {
        ACTIVE.with(|slot| slot.borrow().is_some().then(|| Self { stage, start: Instant::now() }))
    }
}
impl Drop for Timer {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed().as_nanos();
        ACTIVE.with(|slot| {
            if let Some(stats) = slot.borrow_mut().as_mut() {
                match self.stage {
                    Stage::Checkpoint => { stats.checkpoints += 1; stats.checkpoint_ns += elapsed; }
                    Stage::Flush => stats.flush_ns += elapsed,
                    Stage::Hint => stats.hint_ns += elapsed,
                    Stage::Wal => stats.wal_ns += elapsed,
                    Stage::Commit => stats.commit_ns.push(elapsed),
                }
            }
        });
    }
}

pub(crate) fn hint_written(rows: u64, bytes: u64) {
    ACTIVE.with(|slot| {
        if let Some(stats) = slot.borrow_mut().as_mut() {
            stats.hint_rows += rows;
            stats.hint_bytes += bytes;
        }
    });
}
