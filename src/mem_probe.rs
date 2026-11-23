//! mem_probe 的测试对照门与计量。
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{
    AtomicU64, AtomicUsize,
    Ordering::{Relaxed, SeqCst},
};

static LIVE: AtomicU64 = AtomicU64::new(0);
static PEAK: AtomicU64 = AtomicU64::new(0);
static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
// Windows 初始化 Rust 线程局部变量可能分配内存，计量期间不能在外部线程触发初始化；普通压测阶段只读取作用域原子计数。
static ACTIVE_SCOPES: AtomicUsize = AtomicUsize::new(0);
const LOCAL_SCOPE_SLOTS: usize = 64;
struct LocalScope {
    thread_id: AtomicU64,
    depth: AtomicUsize,
    allocations: AtomicU64,
}
static LOCAL_SCOPES: [LocalScope; LOCAL_SCOPE_SLOTS] = [const {
    LocalScope {
        thread_id: AtomicU64::new(0),
        depth: AtomicUsize::new(0),
        allocations: AtomicU64::new(0),
    }
}; LOCAL_SCOPE_SLOTS];

#[cfg(windows)]
fn current_thread_id() -> u64 {
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetCurrentThreadId() -> u32;
    }
    // SAFETY: GetCurrentThreadId 无前置条件，也不访问 Rust 线程局部变量。
    unsafe { GetCurrentThreadId() as u64 }
}

#[cfg(unix)]
fn current_thread_id() -> u64 {
    unsafe extern "C" {
        fn pthread_self() -> usize;
    }
    // SAFETY: pthread_self 无前置条件，返回调用线程的标识。
    unsafe { pthread_self() as u64 }
}

#[cfg(not(any(windows, unix)))]
fn current_thread_id() -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hash = std::collections::hash_map::DefaultHasher::new();
    std::thread::current().id().hash(&mut hash);
    hash.finish().max(1)
}

pub struct Tracking;

unsafe impl GlobalAlloc for Tracking {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        // SAFETY: 委托给 System，契约由调用方（Rust 运行时）保证
        let p = unsafe { System.alloc(l) };
        if !p.is_null() {
            ALLOCATIONS.fetch_add(1, Relaxed);
            if ACTIVE_SCOPES.load(Relaxed) != 0 {
                let thread_id = current_thread_id();
                for scope in &LOCAL_SCOPES {
                    if scope.thread_id.load(Relaxed) == thread_id {
                        scope.allocations.fetch_add(1, Relaxed);
                        break;
                    }
                }
            }
            let now = LIVE.fetch_add(l.size() as u64, Relaxed) + l.size() as u64;
            PEAK.fetch_max(now, Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        LIVE.fetch_sub(l.size() as u64, Relaxed);
        // SAFETY: 指针与布局由调用方按 GlobalAlloc 契约提供
        unsafe { System.dealloc(p, l) };
    }
}

#[global_allocator]
static ALLOC: Tracking = Tracking;

/// 此刻还活着的堆字节
pub(crate) fn live() -> u64 {
    LIVE.load(Relaxed)
}

/// 分配调用次数用于隔离的资源形状对照，不代表进程 RSS。
pub(crate) fn allocations() -> u64 {
    ALLOCATIONS.load(Relaxed)
}

/// 同步计量本线程工作，排除其它并行测试的分配；正式压测阶段仍使用进程计数。
pub(crate) fn count_thread_allocations<T>(f: impl FnOnce() -> T) -> (T, u64) {
    struct Scope(&'static LocalScope);
    impl Drop for Scope {
        fn drop(&mut self) {
            if self.0.depth.fetch_sub(1, Relaxed) == 1 {
                self.0.thread_id.store(0, SeqCst);
                ACTIVE_SCOPES.fetch_sub(1, SeqCst);
            }
        }
    }

    let thread_id = current_thread_id();
    let existing = LOCAL_SCOPES
        .iter()
        .find(|scope| scope.thread_id.load(Relaxed) == thread_id);
    let (scope, fresh) = if let Some(scope) = existing {
        (scope, false)
    } else {
        let claimed = LOCAL_SCOPES.iter().find(|scope| {
            scope
                .thread_id
                .compare_exchange(0, u64::MAX, SeqCst, Relaxed)
                .is_ok()
        });
        (
            claimed.expect("too many concurrent allocation measurements"),
            true,
        )
    };
    if fresh {
        scope.allocations.store(0, Relaxed);
        scope.depth.store(1, Relaxed);
        scope.thread_id.store(thread_id, SeqCst);
        ACTIVE_SCOPES.fetch_add(1, SeqCst);
    } else {
        scope.depth.fetch_add(1, Relaxed);
    }
    let before = scope.allocations.load(Relaxed);
    let _scope = Scope(scope);
    let value = f();
    let after = scope.allocations.load(Relaxed);
    (value, after - before)
}

#[test]
fn thread_allocation_scope_excludes_concurrent_allocations() {
    use std::hint::black_box;
    use std::sync::atomic::{
        AtomicU8,
        Ordering::{Acquire, Release},
    };

    let state = AtomicU8::new(0);
    std::thread::scope(|threads| {
        threads.spawn(|| {
            while state.load(Acquire) != 1 {
                std::thread::yield_now();
            }
            drop(black_box(Box::new(black_box(42_u64))));
            state.store(2, Release);
        });
        let before = allocations();
        let ((), foreign) = count_thread_allocations(|| {
            state.store(1, Release);
            while state.load(Acquire) != 2 {
                std::thread::yield_now();
            }
        });
        assert!(
            allocations() > before,
            "process counter sees the other thread"
        );
        assert_eq!(
            foreign, 0,
            "parallel work must not change this thread's count"
        );
        let ((value, inner), total) = count_thread_allocations(|| {
            count_thread_allocations(|| black_box(Box::new(black_box(7_u64))))
        });
        assert_eq!((*value, inner, total), (7, 1, 1));
    });
    // 释放的计量槽必须能在长时间并行回归中重复使用。
    for _ in 0..LOCAL_SCOPE_SLOTS * 2 {
        let (value, allocations) =
            count_thread_allocations(|| black_box(Box::new(black_box(9_u64))));
        assert_eq!((*value, allocations), (9, 1));
    }
}

/// 开机到此刻的高水位
pub(crate) fn peak() -> u64 {
    PEAK.load(Relaxed)
}

/// 把高水位压回"当前活着的字节"，并返回那个基准值
/// ⇒ 之后 `peak() - 基准` 就是**这一段操作**的瞬时增量（不会被前面的高点压成 0）
pub(crate) fn reset_peak() -> u64 {
    let l = LIVE.load(Relaxed);
    PEAK.store(l, Relaxed);
    l
}

// ── vacuum 的**分段**瞬时峰值标尺（M37 给 bitcask 装、M41 给 lsm 装 ⇒ 一把尺子两个用户）──
//
// ⚠️ 门控：只有设了 `VACUUM_STAGE_SPLIT` 才真的动高水位。`reset_peak()` 是把水位**压低**，
// 常开会让大库台架外层 `timed()` 对 `vacuum()` 的读数被锯短（实测 171.4 → 47.2 MB）——
// 那不是改进，是尺子少了一截。默认（不设环境变量）这里完全是 no-op。
static STAGE_PEAKS: [AtomicU64; 4] = [const { AtomicU64::new(0) }; 4];

/// 分段标尺的**起点**：门控开着就把高水位压回当前活字节并返回基准，否则纯读 `peak()`。
///
/// 顺手把四个槽清成 0 —— 同一进程里两个引擎先后用这把尺子（大库台架就是四家排着跑），
/// 不清的话上一家留下的槽会被下一家当成自己的读数。
pub(crate) fn stage_base() -> u64 {
    if std::env::var("VACUUM_STAGE_SPLIT").is_err() {
        return peak();
    }
    for s in &STAGE_PEAKS {
        s.store(0, Relaxed);
    }
    reset_peak()
}

/// 记下第 `slot` 段相对 `base` 涨了多少，并把水位压回来，返回下一段的基准。
pub(crate) fn stage_mark(slot: usize, base: u64) -> u64 {
    if std::env::var("VACUUM_STAGE_SPLIT").is_err() {
        return base;
    }
    STAGE_PEAKS[slot].store(peak().saturating_sub(base), Relaxed);
    reset_peak()
}

/// 四段的读数（字节）。没被写过的槽是 0 ⇒ ⚠️ 读到 0 先确认门控开没开。
pub(crate) fn stage_peaks() -> [u64; 4] {
    let mut out = [0u64; 4];
    for (o, s) in out.iter_mut().zip(&STAGE_PEAKS) {
        *o = s.load(Relaxed);
    }
    out
}

// ── 冷启动的**分段耗时**标尺（M48 给 bitcask 的 `open` 装）──
//
// ⚠️ 同样的门控（`OPEN_STAGE_SPLIT`）：默认完全 no-op —— 门控开着才取一次时钟。
// 常开的代价不在 `SystemTime` 那几十 ns，而在于它会诱导人在**产品路径**上长期插桩，
// 而大库台架外层 `timed()` 量的就是这扇门 ⇒ 尺子与被打量的对象会互相污染。
//
// ⚠️ 槽**不会**自己清：读的人必须先调 `open_stages_clear()`（台架在每次重开之前调）。
// 没这一句，第二个引擎会读到上一个引擎留下的数 —— M48 第一次跑 20k 时 bplus/lsm
// 就原样复述了 bitcask 的读数，那比读 0 更危险，因为它看着像证据。
//
// 用法：门的**最外面**调一次 `open_stage_t0()`（放基准），每走完一段调
// `open_stage_mark(槽号)`；槽号 = 这一段的名字（见 bitcask 里那张表）。
// 内层函数只 mark、不 t0 —— 否则会把外层已经记下的槽清掉。
static OPEN_STAGE_NS: [AtomicU64; 8] = [const { AtomicU64::new(0) }; 8];
static OPEN_LAST_NS: AtomicU64 = AtomicU64::new(0);

#[inline]
fn gated_now() -> u64 {
    if std::env::var("OPEN_STAGE_SPLIT").is_err() {
        return 0;
    }
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// 起点：放基准（**不**清槽 —— 清槽是读的人的活，见 `open_stages_clear`）。门控关着就什么都不做。
pub(crate) fn open_stage_t0() {
    let now = gated_now();
    if now == 0 {
        return;
    }
    OPEN_LAST_NS.store(now, Relaxed);
}

/// 把八个槽清成 0 ⇒ **每次重开之前**由读的人调一次。
/// 不清的话，下一个引擎会读到上一个引擎留下的数（看着像证据，比读 0 更坏）。
pub(crate) fn open_stages_clear() {
    for s in &OPEN_STAGE_NS {
        s.store(0, Relaxed);
    }
}

/// 记下"从上一段结束到此刻"这一段。没调过 `open_stage_t0()`（基准为 0）时只放基准、不记槽。
pub(crate) fn open_stage_mark(slot: usize) {
    let now = gated_now();
    if now == 0 {
        return;
    }
    let last = OPEN_LAST_NS.swap(now, Relaxed);
    if last != 0 {
        OPEN_STAGE_NS[slot].store(now.saturating_sub(last), Relaxed);
    }
}

/// 八段读数（**纳秒**）。槽没被写过就是 0 ⇒ ⚠️ 读到 0 先确认门控开没开、有没有清槽。
pub(crate) fn open_stages() -> [u64; 8] {
    let mut out = [0u64; 8];
    for (o, s) in out.iter_mut().zip(&OPEN_STAGE_NS) {
        *o = s.load(Relaxed);
    }
    out
}
