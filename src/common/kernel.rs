//! 事务内核里可以共用的三段逻辑：快照可见性、health 分组折叠、自动回收记账。
//!
//! 这三段在四个引擎里原本是**逐字相同**的复制（`is_visible` 四份一模一样）。复制的代价
//! 不是行数，而是"同一个 bug 会长四个实例、修一个要修四遍" —— 自动回收那次就是我把
//! `swap` 当成取账写进了四份代码里，四家一起坏。所以这里把它们收成一份，
//! 各引擎仍然保留自己的遍历原语和自己的守卫（合并的是判定与记账，不是扫描）。

use std::collections::HashSet;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
#[cfg(test)]
use std::thread_local;

use crate::common::txn::Registry;
use crate::common::types::MvccHealth;

/// 对已经按逻辑键分组的输入计数；保留给现有外部调用者。
/// 不能直接喂物理版本键序：二进制前缀在版本 1、2、3 就能打断分组。
/// 四个内置引擎改用 `common::order::LogicalHealth` 等待完整逻辑组。
/// 修订（2026-11-09）：此前“需要极大版本号才会交错”的说明不成立。
#[derive(Debug, Clone, Default)]
pub struct HealthFold {
    prev_raw: Option<Vec<u8>>,
    cur_chain: u64,
    total_rows: u64,
    live_keys: u64,
    max_chain: u64,
}

/// 自动回收的记账与游标。
///
/// 记账规则：**攒够一个切片宽度才取走这笔账**。
/// ⚠️ 这里不能用 `swap`：`tick` 每次 `begin_transaction` 都会跑，`swap(0)` 会把还没攒够的账
/// 清零 ⇒ 阈值永远到不了（四引擎曾经都这么错过，守卫直接报"链涨到 200 行"才发现）。
/// 取账用 `compare_exchange`，让并发下只有一个调用方真去裁。
pub struct VacuumScheduler {
    /// 切片宽度（行数），同时是触发阈值；0 = 关闭自动回收
    slice: Arc<AtomicU64>,
    /// 自上一轮以来新写入的版本行数
    pending: Arc<AtomicU64>,
    /// 下一片从哪个逻辑 key 之后继续（`None` = 从头）
    cursor: Arc<Mutex<Option<Vec<u8>>>>,
    running: AtomicBool,
    committed_rows: AtomicU64,
    completed_slices: AtomicU64,
    skipped_active: AtomicU64,
    failed_slices: AtomicU64,
    versions_removed: AtomicU64,
}

/// 一片回收的**许可**。
///
/// - `slice`：这一片扫多少行（= 切片宽度，恒定）。
/// - `charged`：从"新增版本行账"里真取走了多少行。自 M82 起**恒等于 `slice`** —— `begin()`
///   只在账攒够一片时才扫一片（见 [`VacuumScheduler::take_tick`]），没有"不取账也扫"的许可。
///   字段保留是因为租约归还的必须是 `charged` 而不是别的数，这条合同要能从类型上读出来。
pub struct VacuumTick {
    pub slice: u64,
    pub charged: u64,
}

/// 持有整个切片的独占权，而不是只保护取账那一下。失败/跳过/panic 自动退回预算。
struct VacuumLease<'a> {
    scheduler: &'a VacuumScheduler,
    /// 待退还的**账**（= `VacuumTick::charged`，不是 `slice`）
    reserved: u64,
}

/// 只读事务不进 WAL —— 策略的一份实现（三个磁盘引擎共用）。
///
/// 为什么要专门管这件事：**读路径原来是写路径**。`Transaction::begin` 无条件 append
/// 一条 `Begin`、`commit` 无条件 append `Commit` 并 **fsync** —— 而 `Wal::append` 是
/// `file.append`（立刻落盘，不是缓冲）。实测一次纯只读事务 2.27 ms、往 WAL 落 56 字节：
/// 对一个要轮询存储的交易进程来说这不是"慢一点"，是每秒四百次读的天花板，
/// 外加只读负载把 WAL 撑到阈值、去触发本该由写入才做的 checkpoint。
///
/// 规矩：**begin 时还不该进 WAL**（此时不知道会不会写）；第一次 `write()` 才算"这条事务进过
/// WAL"，`commit` 只在进过 WAL 时才写 `Commit` + fsync。
///
/// ⚠️ M25 起这条"第一次写"只**记账**，不再落 `Begin` 记录 —— 见 [`crate::common::wal::WalRecord::Begin`]：
/// 恢复要的信息全在 `Write`/`Commit`/`Abort` 里，`Begin` 只是把 xid 提前一步登记成一个空条目。
///
/// 为什么少一条 `Begin` 不影响恢复：`analyze_recovery` 的 `max_xid` 是从
/// `Begin`/`Write`/`Commit`/`Abort` **四种记录一起**取的，写过的事务必然带 `Write`，
/// 所以版本号起点不会回退；而没写过的事务本来就没有需要 REDO/UNDO 的东西。
/// 重启后 `start_ver = max(checkpoint.next_version, max_xid + 1)`，
/// 存活行的版本都 ≤ `max_xid` ⇒ 新事务的编号不会和已有行撞上。
#[derive(Debug, Default)]
pub struct WalPolicy {
    opened: std::cell::Cell<bool>,
}

/// 折叠一轮产出的**候选组**：一把逻辑 key 的胜出版本行，值**还没取**。
///
/// `row` 是各家自己的"再拿到这一行的凭据"，形状本来就不一样，所以留成泛型：
/// memory / bitcask / lsm 是编码键（值在另一处，要再读一次），bplus 是叶子条目里
/// 顺手带出来的原始字节。共同点只有一个，也正是驱动需要的那个：**键已经折好了、
/// 按 key 升序、每组一条**。
pub struct WinCandidate<V, K = Vec<u8>> {
    pub key: K,
    pub version: u64,
    pub row: V,
}

/// 无界折叠（`wanted = None`，即"一趟走完全库"）每轮要多少组候选。
///
/// 为什么不能"一次要到底"（原先就是 `usize::MAX`）：`round` 把候选交出来就得**先装在 `Vec`
/// 里**，于是一次全库折叠的瞬时内存 = 全部存活键 ×（raw 键 + 取值凭据 + 结构体）。200k 键
/// 的实测差额在 bitcask 上是 **峰值 +48 MB**（同一轮里 bplus / lsm 报 `+0`，因为它们那条
/// 候选流更薄），而这笔钱和调用方拿到的结果 `Vec`（常驻 30.7 MB）是**两笔**——也就是说
/// 一次 `scan` 的占用能到结果集的两倍。削成固定片之后瞬时只剩"一片候选 + 结果 Vec"。
///
/// ⚠️ 片大小与"每轮固定开销"是反向的：每轮都要从游标重新起一条键流（`fold_lower_bound`
/// 已经把界下推了，见 M21），所以轮数是 `存活键 / 本片`。200k 键 = 49 轮，实测单价没测出
/// 可分辨的差别（判据见守卫，别凭感觉调这个数字）。
///
/// `pub(crate)`（M51）：取页那条路也要按它钳一次预留 —— 页宽可以传很大的数，
/// 一次 `with_capacity(page_size)` 会把刚省下来的瞬时内存按页宽买回来。
pub(crate) const FOLD_CHUNK: usize = 4096;

// 测试专用（M44 的尺子）：**收集门**最多同时攒了多少行。
//
// 为什么钉这一格而不是钉内存：`export` 这类"整库返回"的路径与"流式逐行交出去"的路径，
// 语义判据是同一份（流式那条就是收集壳的核），真正的差别只有一条 —— **中间有没有一个
// `Vec`**。`fold_window` 是收集门、`fold_window_emit` 是流式门，所以"这一趟走的哪扇门"
// 可以直接数：流式臂必须读到 **0**。进程高水位在并行用例里互相污染（M27/M36 各栽过），
// 而这个数是线程局部的计数。
#[cfg(test)]
thread_local! {
    static COLLECT_HELD_MAX: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
mod tests;

// 按职责组织的内部实现；公开类型与原模块路径保持一致。
mod health;
#[cfg(test)]
mod probes;
mod search_page;
mod vacuum_scheduler;
mod visibility;
mod wal_policy;
mod window_fold;
#[cfg(test)]
pub(crate) use probes::collect_held_max;
#[cfg(test)]
pub(crate) use probes::note_collect_held;
#[cfg(test)]
pub(crate) use probes::reset_collect_held;
pub use search_page::search_page_build;
pub use search_page::search_page_from;
pub use search_page::search_page_from_with_total;
pub use visibility::is_unfinished;
pub use visibility::visible_at;
pub use window_fold::fold_window;
pub use window_fold::fold_window_emit;

#[cfg(test)]
include!("kernel/cursor_tests.rs");
