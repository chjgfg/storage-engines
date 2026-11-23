//! recovery_probe 的测试对照门与计量。
use std::{assert, cell::RefCell, thread_local, time::Instant};
#[derive(Debug, Clone, Default)]
pub struct Stats {
    pub total_ns: u128,
    pub verify_ns: u128,
    pub decode_ns: u128,
    pub replay_ns: u128,
    pub replay_setup_ns: u128,
    pub replay_encode_ns: u128,
    pub replay_apply_ns: u128,
    pub replay_commit_ns: u128,
    pub replay_legacy_ns: u128,
    pub replay_pages: u64,
    pub replay_input_rows: u64,
    pub replay_registry_lookups: u64,
    pub replay_tail_reads: u64,
    pub reads: u64,
}
thread_local! { static ACTIVE: RefCell<Option<Stats>> = const { RefCell::new(None) }; }
pub struct Scope;
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
pub fn note(f: impl FnOnce(&mut Stats)) {
    ACTIVE.with(|s| {
        if let Some(stats) = s.borrow_mut().as_mut() {
            f(stats);
        }
    });
}
pub enum Kind {
    Total,
    Verify,
    Records,
    Replay,
    ReplaySetup,
    ReplayEncode,
    ReplayApply,
    ReplayCommit,
    ReplayLegacy,
}
pub struct Timer {
    kind: Kind,
    start: Instant,
    replay_before: u128,
    registry_before: u64,
    tail_before: u64,
}
impl Timer {
    pub fn start(kind: Kind) -> Option<Self> {
        ACTIVE.with(|s| {
            s.borrow().as_ref().map(|stats| Self {
                kind,
                start: Instant::now(),
                replay_before: stats.replay_ns,
                registry_before: crate::common::txn::write_registry_lookups(),
                tail_before: crate::memory::mem_store::key_lookup_probe::tail_reads(),
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
                    Kind::Total => stats.total_ns += ns,
                    Kind::Verify => stats.verify_ns += ns,
                    Kind::Records => {
                        stats.decode_ns += ns.saturating_sub(stats.replay_ns - self.replay_before)
                    }
                    Kind::Replay => stats.replay_ns += ns,
                    Kind::ReplaySetup => stats.replay_setup_ns += ns,
                    Kind::ReplayEncode => stats.replay_encode_ns += ns,
                    Kind::ReplayApply => {
                        stats.replay_apply_ns += ns;
                        stats.replay_registry_lookups +=
                            crate::common::txn::write_registry_lookups() - self.registry_before;
                        stats.replay_tail_reads +=
                            crate::memory::mem_store::key_lookup_probe::tail_reads()
                                - self.tail_before;
                    }
                    Kind::ReplayCommit => stats.replay_commit_ns += ns,
                    Kind::ReplayLegacy => stats.replay_legacy_ns += ns,
                }
            }
        });
    }
}
pub struct Input(pub std::fs::File);
impl std::io::Read for Input {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        ACTIVE.with(|s| {
            if let Some(stats) = s.borrow_mut().as_mut() {
                stats.reads += 1;
            }
        });
        std::io::Read::read(&mut self.0, bytes)
    }
}
impl std::io::Seek for Input {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        std::io::Seek::seek(&mut self.0, pos)
    }
}
