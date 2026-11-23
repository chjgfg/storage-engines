//! 迁移源扫描、目标写入和提交分段计量。
use std::{assert, cell::RefCell, thread_local, time::Instant};
#[derive(Clone, Copy)]
pub(crate) enum Stage {
    SourceBegin,
    SourceScan,
    Projection,
    SourceCommit,
    TargetBegin,
    TargetWrite,
    TargetCommit,
    TargetRollback,
}
#[derive(Clone, Debug, Default)]
pub(crate) struct Stats {
    pub ns: [u128; 8],
    pub calls: [u64; 8],
}
thread_local! { static ACTIVE: RefCell<Option<Stats>> = const { RefCell::new(None) }; }
pub(crate) struct Scope;
impl Scope {
    pub(crate) fn start() -> Self {
        ACTIVE.with(|s| {
            assert!(s.borrow().is_none());
            *s.borrow_mut() = Some(Stats::default());
        });
        Self
    }
    pub(crate) fn stats(&self) -> Stats {
        ACTIVE.with(|s| s.borrow().as_ref().unwrap().clone())
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        ACTIVE.with(|s| *s.borrow_mut() = None);
    }
}
pub(crate) struct Timer {
    stage: Stage,
    start: Instant,
}
impl Timer {
    pub(crate) fn start(stage: Stage) -> Option<Self> {
        ACTIVE.with(|s| s.borrow().is_some()).then(|| Self {
            stage,
            start: Instant::now(),
        })
    }
}
impl Drop for Timer {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed().as_nanos();
        ACTIVE.with(|s| {
            if let Some(stats) = s.borrow_mut().as_mut() {
                stats.ns[self.stage as usize] += elapsed;
                stats.calls[self.stage as usize] += 1;
            }
        });
    }
}
