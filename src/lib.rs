//! 四个教学用 KV 存储引擎 + 统一 trait 门面。
//!
//! - [`common`]：四引擎共享的公共类型与 TTL 编解码
//! - [`engine`]：对象安全的 [`KvEngine`] / [`KvTxn`] / [`KvBulk`]，支持按枚举或字符串选引擎
//! - [`bitcask`] / [`bplus_tree`] / [`lsm_tree`] / [`memory`]：四个引擎实现
//!
//! ```no_run
//! use storage_engines::{open_engine, EngineKind};
//! use std::path::Path;
//!
//! // 编译期定引擎
//! let db = open_engine(EngineKind::LsmTree, Some(Path::new("./data/lsm")))?;
//! // 引擎名来自配置文件/环境变量时
//! let db = open_engine("lsm", Some(Path::new("./data/lsm")))?;
//! let tx = db.begin();
//! tx.set(b"k", b"v".to_vec());
//! tx.commit();
//! # Ok::<(), storage_engines::EngineError>(())
//! ```

pub mod common;
pub mod engine;

pub mod bitcask;
pub mod bplus_tree;
pub mod lsm_tree;
pub mod memory;

pub use common::{
    DurabilityProfile, EngineTuning, ExportRecord, IncrError, KeyMatchMode, KeyMeta, MvccHealth,
    SearchPage, SearchQuery, TuningEntry, VacuumRound, VacuumStats, ValueMeta,
};
pub use engine::{
    EngineConfig, EngineError, EngineKind, EngineName, KvBulk, KvEngine, KvTxn, MigrateStats, StorageConfig,
    open_engine, open_engine_with, open_from_env, migrate,
};
/// LSM 的旋钮集：`EngineConfig::lsm` 收的就是这个类型，所以在根上直接给个名字，
/// 免得下游为了配一个缓存容量还得写全 `lsm_tree::` 路径（那正是"绕开门面自己拼"的起点）。
pub use lsm_tree::LSMTreeOptions;

/// 测试专用：记账分配器 + 峰值水位线。`#[cfg(test)]` ⇒ 对 crate 使用者零成本。
///
/// 原来这份埋在 `engine::tests` 里，只有门面台架用得到。M33 要量的是**存储层内部**一次压实的
/// 瞬时内存，所以提到 crate 根，两家共用一把尺子 —— 一个 crate 只能有一个 `#[global_allocator]`，
/// 在 lsm 那边再抄一份是直接编译失败，不是"多测一遍"。
///
/// `reset_peak()` 是这次新增的那一格：`peak` 是**进程高水位**，同一轮里靠后的操作永远报 `+0`
/// （M27 记下的度量盲区，当时写的是"要拆得做隔离台架"）。把水位线在一段操作开始前压回
/// "当前活着的字节"，那段自己涨了多少才量得出来。
#[cfg(test)]
pub(crate) mod mem_probe {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

    static LIVE: AtomicU64 = AtomicU64::new(0);
    static PEAK: AtomicU64 = AtomicU64::new(0);

    pub struct Tracking;

    unsafe impl GlobalAlloc for Tracking {
        unsafe fn alloc(&self, l: Layout) -> *mut u8 {
            // SAFETY: 委托给 System，契约由调用方（Rust 运行时）保证
            let p = unsafe { System.alloc(l) };
            if !p.is_null() {
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
}
