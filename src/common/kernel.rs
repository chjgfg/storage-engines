//! 事务内核里可以共用的三段逻辑：快照可见性、health 分组折叠、自动回收记账。
//!
//! 这三段在四个引擎里原本是**逐字相同**的复制（`is_visible` 四份一模一样）。复制的代价
//! 不是行数，而是"同一个 bug 会长四个实例、修一个要修四遍" —— 自动回收那次就是我把
//! `swap` 当成取账写进了四份代码里，四家一起坏。所以这里把它们收成一份，
//! 各引擎仍然保留自己的遍历原语和自己的守卫（合并的是判定与记账，不是扫描）。

#[cfg(test)]
use std::thread_local;
use std::collections::HashSet;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex,
};

use crate::common::txn::Registry;
use crate::common::types::MvccHealth;

/// 快照可见性判据：版本 `v` 对「拿着 `my_version` 快照、看见活跃表 `active_xid`」的事务可见吗。
///
/// 两条，缺一不可：
/// 1. `v ∉ active_xid` —— 快照时刻还没提交的事务，其一版**不可见**（哪怕版本号更小）。
///    少了这条就会读到别人写了一半的值。
/// 2. `v <= my_version` —— 只看得见不比自己新的版本。
///
/// ⚠️ 顺序也要留意：`is_visible` 只看版本号与快照，**不看 TTL 是否过期、不看是不是墓碑**。
/// 那两件事属于取值之后的剥壳（`logical_to_user`），别混进来。
pub fn visible_at(v: u64, my_version: u64, active_xid: &HashSet<u64>) -> bool {
    if active_xid.contains(&v) {
        return false;
    }
    v <= my_version
}

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

impl HealthFold {
    /// 喂进一行（只给 `raw_key`，不需要 value —— health 不该物化任何值）
    pub fn push(&mut self, raw_key: &[u8]) {
        self.total_rows += 1;
        if self.prev_raw.as_deref() == Some(raw_key) {
            self.cur_chain += 1;
        } else {
            self.max_chain = self.max_chain.max(self.cur_chain);
            self.live_keys += 1;
            self.cur_chain = 1;
            self.prev_raw = Some(raw_key.to_vec());
        }
    }

    /// 收尾并把活跃事务数填进去（各引擎的 `active_txn` 表是自己的，由调用方给）
    pub fn finish(mut self, active_txns: u64) -> MvccHealth {
        self.max_chain = self.max_chain.max(self.cur_chain);
        MvccHealth {
            total_rows: self.total_rows,
            live_keys: self.live_keys,
            max_chain: self.max_chain,
            rows_above_one: self.total_rows.saturating_sub(self.live_keys),
            active_txns,
        }
    }
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

impl Drop for VacuumLease<'_> {
    fn drop(&mut self) {
        self.scheduler.pending.fetch_add(self.reserved, Ordering::SeqCst);
        self.scheduler.running.store(false, Ordering::Release);
    }
}

impl Default for VacuumScheduler {
    fn default() -> Self {
        Self::new(0)
    }
}

impl VacuumScheduler {
    pub fn new(slice_rows: u64) -> Self {
        VacuumScheduler {
            slice: Arc::new(AtomicU64::new(slice_rows)),
            pending: Arc::new(AtomicU64::new(0)),
            cursor: Arc::new(Mutex::new(None)),
            running: AtomicBool::new(false),
            committed_rows: AtomicU64::new(0),
            completed_slices: AtomicU64::new(0),
            skipped_active: AtomicU64::new(0),
            failed_slices: AtomicU64::new(0),
            versions_removed: AtomicU64::new(0),
        }
    }

    pub fn slice(&self) -> u64 {
        self.slice.load(Ordering::SeqCst)
    }

    /// 设切片宽度；0 = 关闭自动回收（那时版本行只增不减，得靠手动 `vacuum()`）
    pub fn set_slice(&self, rows: u64) {
        self.slice.store(rows, Ordering::SeqCst);
    }

    /// 提交时记一笔账（行数是近似值：算逻辑 key 数，宁可少算也不为了精确再走一遍写集）
    pub fn on_commit(&self, rows: u64) {
        if rows > 0 {
            self.pending.fetch_add(rows, Ordering::SeqCst);
            self.committed_rows.fetch_add(rows, Ordering::SeqCst);
        }
    }

    /// 丢掉"新增版本行账"里还没被消费的部分，返回丢掉的行数（M82，快照重放用）。
    ///
    /// 为什么可以丢：这笔账的语义是"**攒了多少待回收的旧版本**"，而重放出来的行每把键只有一版
    /// —— 一行垃圾都没有。留着它只会让重放刚结束就来一整轮空扫（`slice` 行/次）：20M 档实测
    /// 1221 片 ≈ 十几秒，600k 档 `冷重开` 1.3 s → 4.4 s。⚠️ 只该在"新写入行确定不是垃圾"的场合用
    /// （今天只有快照重放这一处），别拿它当"清账"的通用口子。
    pub fn discard_pending(&self) -> u64 {
        self.pending.swap(0, Ordering::SeqCst)
    }

    /// 该裁一片了吗：**新增版本行账攒够一片才取走一片**，否则 `None`。
    ///
    /// 这条口径是 M82 定的，把 M81 第 5 项后半加进来的"游标不在库头 ⇒ 账不够也给许可（`charged = 0`）"
    /// 那一支**拿掉了**。那一支想解决的事是真的（一片把账扣到不足阈值、游标停在库中段，没有新写
    /// 就永远不再推进 ⇒ 后半库的垃圾无人认领），但它把代价挂到了 `begin()` 热路径上：写流里一轮
    /// 永远开着（账一直攒、游标一绕完就立刻再开），于是**每个事务**的 `begin()` 都扫一整片 ——
    /// 1000 行/事务放大 16×、单行事务放大 16384×。`docs/test2.txt` 五档 20/20 格 `灌`/`更新`
    /// 变慢、memory `冷重开` 1.3 s → 4.4 s、冷重开后每个单事务读格多付 8~10 ms，全是这笔税；
    /// 守卫 `maintenance::tests::auto_vacuum_scanning_keeps_pace_with_writes_not_with_transactions`
    /// 在那份实现上读出 275 片 ≈ 17 600 行（写了 512 行）。
    ///
    /// 现在的分工：`begin()` 的扫描量与写入量 1:1（攒多少扫多少，不多不少）；"账花完而一轮还在半路"
    /// 由 [`crate::maintenance::Maintenance`] 兜 —— 它有自己的游标和间隔，本来就是干这个的，
    /// 不在任何事务的路径上。普通 `open` 不跑维护器时，后半库的垃圾等下一次写或手动 `vacuum()`，
    /// 这是有意的取舍，不是漏。`sweep_in_progress`（= 游标不在库头）照旧如实报告，好让调用方
    /// 知道该不该去推一把。
    pub fn take_tick(&self) -> Option<VacuumTick> {
        let slice = self.slice.load(Ordering::SeqCst);
        if slice == 0 {
            return None;
        }
        let seen = self.pending.load(Ordering::SeqCst);
        if seen < slice {
            return None;
        }
        // 只取一片，不能把大事务剩余的账清零；下一次 begin 继续偿还积压。
        match self
            .pending
            .compare_exchange(seen, seen - slice, Ordering::SeqCst, Ordering::SeqCst)
        {
            Ok(_) => Some(VacuumTick { slice, charged: slice }),
            Err(_) => None,
        }
    }

    /// 一轮是否走在半路（游标不在库头）。维护器与守卫都读这一个口子，不许自己猜。
    /// 自 M82 起它只是**状态**，不再是 `begin()` 要不要扫一片的依据。
    pub fn round_open(&self) -> bool {
        self.cursor.lock().unwrap().is_some()
    }

    /// 自动切片的唯一驱动。跳过/失败保留账与游标；并发调用不重复使用同一游标。
    pub fn run_pending<E>(
        &self,
        run: impl FnOnce(Option<&[u8]>, u64) -> Result<crate::common::VacuumRound, E>,
    ) -> Result<(), E> {
        if self.slice() == 0
            || self.running.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed).is_err()
        {
            return Ok(());
        }
        let mut lease = VacuumLease { scheduler: self, reserved: 0 };
        // 能不能取一片由 `take_tick` 一处回答（账够一片才给），这里不另设预检。
        let Some(tick) = self.take_tick() else { return Ok(()) };
        lease.reserved = tick.charged;
        let cursor = self.cursor();
        match run(cursor.as_deref(), tick.slice) {
            Ok(round) if round.stats.skipped_active_txn => {
                self.skipped_active.fetch_add(1, Ordering::SeqCst);
            }
            Ok(round) => {
                self.set_cursor(round.next_cursor);
                self.versions_removed.fetch_add(round.stats.versions_removed as u64, Ordering::SeqCst);
                self.completed_slices.fetch_add(1, Ordering::SeqCst);
                lease.reserved = 0;
            }
            Err(e) => {
                self.failed_slices.fetch_add(1, Ordering::SeqCst);
                return Err(e);
            }
        }
        Ok(())
    }

    /// O(活跃事务数)，不读取存储页、value 或全库键。
    pub fn status(&self, active: &Registry) -> crate::common::types::ReclamationStatus {
        crate::common::types::ReclamationStatus {
            pending_rows: self.pending.load(Ordering::SeqCst),
            committed_rows: self.committed_rows.load(Ordering::SeqCst),
            completed_slices: self.completed_slices.load(Ordering::SeqCst),
            skipped_active: self.skipped_active.load(Ordering::SeqCst),
            failed_slices: self.failed_slices.load(Ordering::SeqCst),
            versions_removed: self.versions_removed.load(Ordering::SeqCst),
            running: self.running.load(Ordering::Acquire),
            sweep_in_progress: self.round_open(),
            active_txns: active.len() as u64,
            oldest_active_version: active.keys().copied().min(),
        }
    }

    pub fn cursor(&self) -> Option<Vec<u8>> {
        self.cursor.lock().unwrap().clone()
    }

    pub fn set_cursor(&self, cursor: Option<Vec<u8>>) {
        *self.cursor.lock().unwrap() = cursor;
    }
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
/// ⚠️ M25 起这条"第一次写"只**记账**，不再落 `Begin` 记录 —— 见 [`crate::common::WalRecord::Begin`]：
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

impl WalPolicy {
    /// 第一次写之前调用：把"这条事务进过 WAL"记下来。幂等 —— 调几次都是同一个状态。
    ///
    /// M25 之前它返回"是不是第一次"，因为第一次要补一条 `Begin`；`Begin` 不再落盘之后
    /// 这个返回值没有调用方了，所以改成纯粹的置位（留着返回 `bool` 只会让人以为
    /// "返回 true 的那一次要做点什么"）。
    pub fn mark_opened(&self) {
        self.opened.set(true);
    }

    /// 本事务是否进过 WAL ⇒ `commit` 要不要写 `Commit` 并 fsync、`rollback` 要不要写 `Abort`
    pub fn opened(&self) -> bool {
        self.opened.get()
    }
}

/// 分页核（四引擎共用的一份）：从"已按 pattern 过滤好的命中序列"里跳 `skip` 条取 `page_size` 条。
///
/// 存在的理由：这四行逻辑原先在四个引擎里各抄一遍，而 M15 的改动恰好只动这四行 ——
/// 一处改完，四家一起变，不存在"三家改了、一家漏了"。
///
/// 这条入口假定调用方**已经把界内命中全交过来**，所以 `total` 就是 `matched.len()`。
/// M35 之后 `search_keys` 不走这条（改成"只数一遍 + 只取那一页"，见
/// [`crate::common::search_page`]）；留着是给"手里本来就有全量"的调用方与测试对拍用的。
pub fn search_page_from(
    matched: Vec<crate::common::ExportRecord>,
    query: &crate::common::SearchQuery,
) -> crate::common::SearchPage {
    let total = query.need_total.then_some(matched.len());
    search_page_from_with_total(matched, query, total)
}

/// 一页的**装配口径唯一一份**（M51）。
///
/// 为什么要把 `has_next` / `total_pages` 的算法从 [`search_page_from_with_total`] 里拆出来：
/// M51 之后取页的那条路不再交回"从头到这一页的整段"，而是交回"这一页 + 有没有下一条"
/// （[`crate::common::store::search_page_rows`]）⇒ 两个形状都要算同一个 `has_next`。
/// 分开写就是两处会各自漂移的那种"同一判定两份"（M5a/M5b 一路的规矩），所以这里只留一份，
/// 老的"整段切片版"改成它的适配层。
pub fn search_page_build(
    items: Vec<crate::common::ExportRecord>,
    page: usize,
    page_size: usize,
    total: Option<usize>,
    has_more: bool,
) -> crate::common::SearchPage {
    let (has_next, total_pages) = match total {
        Some(n) => (
            n > 0 && page + 1 < (n + page_size - 1) / page_size,
            Some(if n == 0 { 0 } else { (n + page_size - 1) / page_size }),
        ),
        None => (has_more, None),
    };
    crate::common::SearchPage {
        items,
        total,
        page,
        page_size,
        total_pages,
        has_next,
    }
}

/// [`search_page_from`] 的**外部 total 版**（M35）：`total` 是调用方另外数出来的精确命中数，
/// `matched` 只需要装得下**那一页**（外加"多取一条"当 `has_next` 的证据）。
///
/// 为什么要分开：数 total 与取一页是两笔账 —— 前者要**走一遍界内**（每行的值都得读出来才判得了
/// 过期），后者只关心 `skip + page_size` 行。把它们合在一条折叠里，就等于"为了一个数把整库
/// 物化成 `Vec` 再 `len()`"（600k 档实测峰值 **+123.7 MB**）。
///
/// `total` 的两条路：
/// - `Some(n)`：`has_next` / `total_pages` 都由 `n` 推（`n > 0 && page + 1 < ceil(n / 页宽)`）。
/// - `None`（没数）：`has_next` 只能靠"窗口外那一条在不在" —— `matched.len() > skip + 页宽`。
///   ⚠️ **不许把"没数"报成"到底了"**就是靠这条区分的：走到库尾 / 界尾是真的到底，
///   而探到第 `end+1` 条说明后面还有。
///
/// ⚠️ M51：`search_page` 已经不走这条了（它用 [`search_page_build]`，跳过的行当场丢掉、不再攒整段）。
/// 这里留着是给"手上已经有一段命中"的调用方（[`search_page_from`] 以及下游自己折叠的形状）。
pub fn search_page_from_with_total(
    matched: Vec<crate::common::ExportRecord>,
    query: &crate::common::SearchQuery,
    total: Option<usize>,
) -> crate::common::SearchPage {
    let page_size = query.page_size.max(1);
    let page = query.page;
    let skip = page.saturating_mul(page_size);
    let has_more = matched.len() > skip + page_size;
    let items = if skip >= matched.len() {
        Vec::new()
    } else {
        let end = (skip + page_size).min(matched.len());
        matched[skip..end].to_vec()
    };
    search_page_build(items, page, page_size, total, has_more)
}

/// 折叠一轮产出的**候选组**：一把逻辑 key 的胜出版本行，值**还没取**。
///
/// `row` 是各家自己的"再拿到这一行的凭据"，形状本来就不一样，所以留成泛型：
/// memory / bitcask / lsm 是编码键（值在另一处，要再读一次），bplus 是叶子条目里
/// 顺手带出来的原始字节。共同点只有一个，也正是驱动需要的那个：**键已经折好了、
/// 按 key 升序、每组一条**。
pub struct WinCandidate<V> {
    pub key: Vec<u8>,
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
pub(crate) fn note_collect_held(n: usize) {
    COLLECT_HELD_MAX.with(|c| {
        let n = n as u64;
        if n > c.get() {
            c.set(n);
        }
    });
}

#[cfg(test)]
pub(crate) fn collect_held_max() -> u64 {
    COLLECT_HELD_MAX.with(|c| c.get())
}

#[cfg(test)]
pub(crate) fn reset_collect_held() {
    COLLECT_HELD_MAX.with(|c| c.set(0));
}

/// 窗口折叠的共用驱动（四引擎一份）。
///
/// `round(after, budget)`：从 `after` 这组**之后**开始折，最多交 `budget` 组候选，
/// 返回 `(候选, 是否已走到界尾)`。`keep(candidate)`：候选 ⇒ 记录（取值 + 剥壳 +
/// TTL/墓碑过滤），`None` = 这一条不算命中窗口。`wanted = None` = 一趟走完全库
/// （**逐片**走完，每片 `FOLD_CHUNK` 组：语义还是"全库"，只是不再把整库候选一次装进内存）。
///
/// 为什么要能续跑第二轮：**"这把键命不命中 pattern"在键这一层就能判，而"这行到底
/// 取不取到值"（blob 指针、TTL 是否过期）要取了值才知道** ⇒ 提前收手有可能少数几条。
/// 于是从上一轮的最后一个键续跑、预算翻倍 ⇒ 总代价仍是最终那一轮的同阶，最坏退化成
/// "走完全库"，而那正好是 `wanted = None`（要精确 total）的语义。同一个几何重试的形状
/// 在 `vacuum_bounded` 的窗口放大里已经用过一次。
/// ⚠️ 几何翻倍只给**有界**窗口：无界折叠要的是"每片固定大小、走到库尾"，跟着翻等于把
///   刚省下来的瞬时内存又按轮数放大回去。
///
/// 为什么值读取放在 `keep` 而不是折在 `round` 里：bitcask/bplus/lsm 的取值要拿 blob 锁，
/// 而候选是**拿着 kv 锁走元数据**折出来的 —— 今天的两段式就是为了不在这两把锁上打结。
pub fn fold_window<V, R, K, T>(wanted: Option<usize>, round: R, keep: K) -> Vec<T>
where
    R: FnMut(Option<&[u8]>, usize) -> (Vec<WinCandidate<V>>, bool),
    K: FnMut(WinCandidate<V>) -> Option<T>,
{
    let mut out: Vec<T> = Vec::new();
    // `wanted` 那条"多要一条只为知道后面还有没有"的合同留在调用方（collect_rows），
    // 这里只负责交够数就停
    fold_window_emit(wanted, round, keep, |t| {
        out.push(t);
        #[cfg(test)]
        note_collect_held(out.len());
        true
    });
    out
}

/// [`fold_window`] 的**落点版**：每折出一行立刻交给 `emit`，**一行都不攒**。
///
/// 为什么要它（M27）：`KvTxn::scan_foreach` 的文档一直写着"不收集全库到单一大 Vec"，
/// 可它的实现是 `for … in collect_latest_raw_in(...)` —— 先把全库的
/// `(key, version, 存储字节)` 元组攒成 `Vec`，再逐条搬成 `ExportRecord`。
/// 200k 键实测因此多占 ~15 MB（峰值 +45.5 而结果集只有 30.7），而且"提前叫停"根本省不到
/// 折的那一段。有了落点版，流式才是真的：`emit` 返回 `false` 当场收手，后面的键**不再折**。
///
/// 返回值 = 实际交给 `emit` 的行数（`wanted` 凑满或走到界尾或被打断都是它）。
pub fn fold_window_emit<V, R, K, T, E>(
    wanted: Option<usize>,
    mut round: R,
    mut keep: K,
    mut emit: E,
) -> usize
where
    R: FnMut(Option<&[u8]>, usize) -> (Vec<WinCandidate<V>>, bool),
    K: FnMut(WinCandidate<V>) -> Option<T>,
    E: FnMut(T) -> bool,
{
    let mut handed = 0usize;
    let mut after: Option<Vec<u8>> = None;
    let mut budget = wanted.unwrap_or(FOLD_CHUNK);
    loop {
        let (batch, reached_end) = round(after.as_deref(), budget);
        let produced = batch.len();
        for (index, cand) in batch.into_iter().enumerate() {
            #[cfg(test)]
            let old_cursor = super::store::scan_cost_reference::enabled();
            #[cfg(not(test))]
            let old_cursor = false;
            // Only the last physical candidate is needed to resume a complete
            // chunk. Keep it even when the row mapper rejects a tombstone.
            if old_cursor || (!reached_end && index + 1 == produced) {
                #[cfg(test)]
                super::store::scan_cost_reference::note_fold_cursor_copy();
                after = Some(cand.key.clone());
            }
            if let Some(kept) = keep(cand) {
                if !emit(kept) {
                    return handed;
                }
                handed += 1;
                if wanted.is_some_and(|w| handed >= w) {
                    return handed;
                }
            }
        }
        if reached_end || produced == 0 {
            return handed;
        }
        // 几何放大只服务于**有界窗口**（"这一片没凑够 limit 条"那种情况）；
        // 无界折叠每片固定 FOLD_CHUNK —— 跟着翻等于把刚省下的瞬时内存按轮数放大回去。
        if wanted.is_some() {
            budget = budget.saturating_mul(2).max(64);
        }
    }
}

#[cfg(test)]
#[test]
fn streaming_cursor_copies_only_chunk_ends_with_rejected_last_rows() {
    use std::{assert_eq, vec::Vec};
    use super::store::scan_cost_reference;
    const ROWS: usize = 9000;
    for reference in [true, false] {
        let _mode = reference.then(scan_cost_reference::Scope::start);
        let before = scan_cost_reference::fold_cursor_copies();
        let mut out = Vec::new();
        let handed = fold_window_emit(None, |after, budget| {
            let start = after.map(|b| u64::from_be_bytes(b.try_into().unwrap()) as usize + 1).unwrap_or(0);
            let end = (start + budget).min(ROWS);
            let rows = (start..end).map(|i| WinCandidate { key: (i as u64).to_be_bytes().to_vec(), version: 1, row: i }).collect();
            (rows, end == ROWS)
        }, |c| (c.row % FOLD_CHUNK != FOLD_CHUNK - 1).then_some(c.row), |i| { out.push(i); true });
        let expected: Vec<_> = (0..ROWS).filter(|i| i % FOLD_CHUNK != FOLD_CHUNK - 1).collect();
        assert_eq!(handed, expected.len());
        assert_eq!(out, expected);
        assert_eq!(scan_cost_reference::fold_cursor_copies() - before, if reference { ROWS as u64 } else { 2 });
    }
}

/// 漏网事务的判据：活跃表里还留着"自己"，就说明既没 `commit` 也没 `rollback`。
///
/// 为什么用成员关系而不是额外标志位：`commit` / `rollback` 都以 `remove` 收尾，
/// 所以「还在表里」就是「未终结」的充要条件，多一个标志位就多一处会写漏的可能。
///
/// ⚠️ 这里**只查不删**。删除必须留给引擎自己的 `rollback()` —— 它还要按表里那份写集
/// 把本事务写过的版本行挨个清掉；在这里删掉，`rollback()` 就拿到空写集，
/// 事务的行会留在库里（判据写成 `remove` 是我第一版的想法，看了 rollback 才否掉）。
///
/// 锁中毒（别的线程 panic 过）时返回 `false`：Drop 里 panic 会直接 abort 进程，
/// 宁可不回滚也不能把宿主进程带下去。
pub fn is_unfinished(active_txn: &Mutex<Registry>, version: u64) -> bool {
    match active_txn.lock() {
        Ok(guard) => guard.contains_key(&version),
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    /// 无界折叠（`wanted = None`，"一趟走完全库"）必须**逐片**走完，而不是把整库候选
    /// 一次装进内存（M27）。
    ///
    /// 尺子选的是"每轮要了多少组候选"，不是墙钟也不是字节数：字节数要靠进程级分配计数
    /// （并行跑就把别家算进来，10-17 栽过一次），而这条改动的判据本来就是**每轮的上限**。
    /// 三件事一起断，缺一半都能被糊过去：① 语义没变（行数与内容 == 一次要到底）；
    /// ② 每轮要的组数不超过 `FOLD_CHUNK`；③ 确实分了多轮（否则 ② 是空转的）。
    #[test]
    fn fold_window_unbounded_walks_in_chunks() {
        // 故意不是整片：最后那一片必须也能走到底
        const N: usize = 3 * 4096 + 7;

        fn enc(i: usize) -> Vec<u8> {
            (i as u64).to_le_bytes().to_vec()
        }
        fn dec(k: &[u8]) -> usize {
            u64::from_le_bytes(k.try_into().unwrap()) as usize
        }

        let mut budgets: Vec<usize> = Vec::new();
        let out: Vec<usize> = fold_window(
            None,
            |round_after, budget| {
                let start = round_after.map(dec).map(|i| i + 1).unwrap_or(0);
                let take = budget.min(N.saturating_sub(start));
                let batch: Vec<WinCandidate<usize>> = (start..start + take)
                    .map(|i| WinCandidate {
                        key: enc(i),
                        version: i as u64,
                        row: i,
                    })
                    .collect();
                budgets.push(take);
                (batch, start + take >= N)
            },
            |c| Some(c.row),
        );

        assert_eq!(out.len(), N, "逐片走完全库却少了行");
        assert_eq!(
            out,
            (0..N).collect::<Vec<usize>>(),
            "分片之后行序/内容与一次要到底不一致"
        );
        let max = budgets.iter().copied().max().unwrap_or(0);
        assert!(
            budgets.len() >= 2,
            "只走了一轮 ⇒ 每轮上限那条断言是空转（轮数 {:?}）",
            budgets
        );
        assert!(
            max <= super::FOLD_CHUNK,
            "某一轮要了 {max} 组候选，超过一片（{:?}）",
            budgets
        );
    }

    /// 驱动"续跑第二轮"那一支的守卫：`keep` 丢掉候选时，必须从上一轮最后一个键接着折，
    /// 而不是把窗口交短。四家的 `search_keys` 都走这条路，而引擎级守卫里
    /// 没有过期行 ⇒ 那条分支一次都不会被踩到，只能在这里直接喂给它。
    #[test]
    fn fold_window_resumes_when_keep_drops_candidates() {
        let mut rounds = 0usize;
        let mut seen_budgets: Vec<usize> = Vec::new();

        let out: Vec<usize> = fold_window(
            Some(5),
            |after, budget| {
                rounds += 1;
                seen_budgets.push(budget);
                // 假想的键流：0..60，`after` 之后严格续跑
                let start = after.map(|a| a[0] as usize + 1).unwrap_or(0);
                let mut batch = Vec::new();
                for i in start..60usize {
                    if batch.len() >= budget {
                        return (batch, false); // 被预算拦下 ⇒ 还能续
                    }
                    batch.push(WinCandidate {
                        key: vec![i as u8],
                        version: 1,
                        row: i,
                    });
                }
                (batch, true) // 走到界尾
            },
            // 偶数下标全被丢掉：模拟"TTL 正好过期 / 是墓碑"的那些行
            |c| if c.row % 2 == 0 { None } else { Some(c.row) },
        );

        assert_eq!(out, vec![1, 3, 5, 7, 9], "被丢掉的候选必须由第二轮补上，窗口不许交短");
        assert_eq!(rounds, 2, "第一轮 5 组里只活下来 2 条 ⇒ 必须续跑，一次都不续就是走不到窗口");
        assert!(
            seen_budgets[1] > seen_budgets[0],
            "续跑的预算必须变大（几何 ⇒ 总代价与最终那一轮同阶）：{seen_budgets:?}"
        );
    }

    /// 分页核自己的守卫：四家都靠它，所以它错了就是四个地方一起错。
    ///
    /// 重点盯 `has_next` 的两条分支 —— "没数"与"到底了"必须能区分开，
    /// 把任何一侧写错都会让翻页提前停（少数据）或永远停不下来（死循环）。
    #[test]
    fn search_page_kernel_distinguishes_unmeasured_from_exhausted() {
        use crate::common::{ExportRecord, KeyMatchMode, SearchQuery};

        let rec = |i: usize| ExportRecord::new(format!("k{i:02}").into_bytes(), Some(vec![b'v']));
        let keys = |p: &crate::common::SearchPage| {
            p.items.iter().map(|r| r.key.clone()).collect::<Vec<_>>()
        };

        // ① 要 total：7 条 / 每页 3 ⇒ 三页，最后一页不满，翻页判据由总数算出来
        let q = SearchQuery {
            pattern: Vec::new(),
            page: 0,
            page_size: 3,
            mode: KeyMatchMode::Contains,
            include_deleted: false,
            need_total: true,
        };
        let all: Vec<_> = (0..7).map(rec).collect();
        let p0 = search_page_from(all.clone(), &q);
        assert_eq!(p0.total, Some(7));
        assert_eq!(p0.total_pages, Some(3));
        assert_eq!(keys(&p0), vec![b"k00".to_vec(), b"k01".to_vec(), b"k02".to_vec()]);
        assert!(p0.has_next());

        let p2 = search_page_from(all.clone(), &SearchQuery { page: 2, ..q.clone() });
        assert_eq!(p2.items.len(), 1, "最后一页只剩一条");
        assert!(!p2.has_next(), "第三页之后没了");

        let p9 = search_page_from(all.clone(), &SearchQuery { page: 9, ..q.clone() });
        assert!(p9.items.is_empty(), "越界的页给空，不许给尾");
        assert!(!p9.has_next());

        // ② 不要 total：上层最多交 `skip + page_size + 1` 条 —— 多出来的那一条就是证据
        let qn = SearchQuery {
            need_total: false,
            ..q.clone()
        };
        let w = search_page_from(all.iter().take(4).cloned().collect(), &qn);
        assert_eq!(w.total, None, "没数就不许报一个数");
        assert_eq!(w.total_pages, None);
        assert_eq!(w.items.len(), 3);
        assert!(w.has_next(), "手里有第 4 条 ⇒ 后面确实还有");

        // 交过来的正好不超过窗口 ⇒ 说明键流走到了头，这就是"到底了"，不是"没数"
        let end = search_page_from(all.iter().take(3).cloned().collect(), &qn);
        assert_eq!(end.items.len(), 3);
        assert!(!end.has_next(), "拿不满窗口 = 已经到底，必须报假");

        // 深一页：上层会交 skip+page_size+1 = 4+3+1 = 8 条，窗口从第 4 条起
        let deep = search_page_from(
            all.iter().take(7).cloned().collect(),
            &SearchQuery { page: 1, need_total: false, ..q.clone() },
        );
        assert_eq!(keys(&deep), vec![b"k03".to_vec(), b"k04".to_vec(), b"k05".to_vec()]);
        assert!(deep.has_next());
        let last = search_page_from(
            all.iter().take(7).cloned().collect(),
            &SearchQuery { page: 2, need_total: false, ..q.clone() },
        );
        assert_eq!(keys(&last), vec![b"k06".to_vec()]);
        assert!(!last.has_next(), "最后一页之后没有东西");

        // ③ `page_size` 至少为 1：传 0 不许把自己除尽
        let zero = search_page_from(
            all.clone(),
            &SearchQuery { page_size: 0, need_total: false, ..q.clone() },
        );
        assert_eq!(zero.items.len(), 1, "page_size=0 按 1 处理");
        assert!(zero.has_next());
    }

    use super::*;
    use std::assert;
    use std::assert_eq;
    use std::vec;
    use std::format;

    #[test]
    fn visible_at_requires_committed_and_not_newer() {
        let mut active = HashSet::new();
        active.insert(7u64);
        // 提交过的旧版本可见
        assert!(visible_at(5, 10, &active));
        // 比我新 ⇒ 不可见
        assert!(!visible_at(11, 10, &active));
        // 快照时刻还活着的事务，哪怕版本号更小也不可见（少了这条就读到别人写一半的值）
        assert!(!visible_at(7, 10, &active));
        // 边界：等于自己可见（就是本事务自己写的行）
        assert!(visible_at(10, 10, &active));
    }

    #[test]
    fn health_fold_counts_adjacent_blocks() {
        let mut fold = HealthFold::default();
        for _ in 0..3 {
            fold.push(b"hot");
        }
        fold.push(b"other");
        let h = fold.finish(2);
        assert_eq!((h.total_rows, h.live_keys, h.max_chain), (4, 2, 3));
        assert_eq!(h.rows_above_one, 2);
        assert_eq!(h.active_txns, 2);
    }

    /// 记账必须"攒够才取走"：曾经四引擎都用 `swap(0)` 取账，每次 begin 把没攒够的账清零
    /// ⇒ 阈值永远到不了，自动回收静默停摆。这条断言就是钉那个反例的。
    #[test]
    fn scheduler_only_ticks_when_debt_reaches_slice() {
        let s = VacuumScheduler::new(8);
        for _ in 0..7 {
            s.on_commit(1);
            assert!(s.take_tick().is_none(), "没攒够不许取账");
        }
        s.on_commit(1);
        let t = s.take_tick().expect("攒够 8 行必须触发一片");
        assert_eq!((t.slice, t.charged), (8, 8));
        assert!(s.take_tick().is_none(), "取走后账要清零，不许连续空转");

        // 关闭：一律不触发，账也不许被偷偷吃掉
        s.set_slice(0);
        s.on_commit(100);
        assert!(s.take_tick().is_none());
        s.set_slice(8);
        let t = s.take_tick().expect("重新打开后原来的账还在");
        assert_eq!((t.slice, t.charged), (8, 8));
    }

    /// M82 的口径：**账不足一片 ⇒ `begin()` 不扫**，哪怕一轮还开在半路。
    ///
    /// 这是 M81 第 5 项后半那条"游标不在库头也给许可（`charged = 0`）"的**反面**——那一支让写流里
    /// 每个事务都扫一整片（守卫 `auto_vacuum_scanning_keeps_pace_with_writes_not_with_transactions`
    /// 钉的就是它）。半路的轮次由 `Maintenance` 推，不由下一个事务推；但 `sweep_in_progress`
    /// 必须照实报 true，调用方才知道有一轮没走完。
    /// 把 `take_tick` 里那一支加回来（账不够而游标非空 ⇒ `Some`）⇒ 本条立即红在"不该给许可"上。
    #[test]
    fn scheduler_does_not_push_an_open_round_from_begin_without_debt() {
        let s = VacuumScheduler::new(8);
        let sweep = || s.status(&Registry::new()).sweep_in_progress;
        s.on_commit(8);
        // 第一片：账够 ⇒ 取走 8，游标停在库头 ⇒ 这还没"开轮"
        let t = s.take_tick().expect("攒够一片必须给许可");
        assert_eq!((t.slice, t.charged), (8, 8));
        assert_eq!(s.pending.load(Ordering::SeqCst), 0);
        assert!(!s.round_open(), "游标还在库头 = 一轮都没走");
        assert!(!sweep(), "对外那格必须和游标同义，否则规模守卫判的是个数不是状态");

        // 走到半路：游标非空 = 一轮已发起
        s.set_cursor(Some(b"mid".to_vec()));
        assert!(s.round_open());
        assert!(sweep(), "账已经花光而一轮还在半路 ⇒ 这一格必须报 true");
        // 账为 0 ⇒ **不给许可**：半路的轮次不是 begin() 的活
        assert!(s.take_tick().is_none(), "账不足一片时 begin() 不许扫，哪怕一轮开着（M82）");
        assert_eq!(s.pending.load(Ordering::SeqCst), 0);
        assert!(sweep(), "没推进 ⇒ 状态照旧报'半路'，不许因为没扫就假装走完");

        // 新写把账攒够 ⇒ 从同一个游标接着扫，不从头
        s.on_commit(8);
        let t = s.take_tick().expect("账够了必须给许可");
        assert_eq!((t.slice, t.charged), (8, 8));
        assert_eq!(s.cursor().as_deref(), Some(&b"mid"[..]), "续扫要从停下的地方接着走");

        // 推到库尾：游标归 None ⇒ 没有新写就不该触发
        s.set_cursor(None);
        assert!(s.take_tick().is_none(), "一轮走完了、账又不够 ⇒ 不该空转");
        assert!(!sweep());
    }

    /// 一片失败/跳过时归还的必须正好是取走的那 `charged` 行：既不少（账不许被失败吃掉），
    /// 也不多（不许凭空造账开出下一轮）。游标在半路时也一样。
    #[test]
    fn failed_slice_refunds_exactly_what_it_charged() {
        let s = VacuumScheduler::new(8);
        s.set_cursor(Some(b"mid".to_vec()));
        let pending = || s.pending.load(Ordering::SeqCst);
        s.on_commit(8);
        let fail: Result<(), &str> = s.run_pending(|_, _| Err("disk error"));
        assert_eq!(fail, Err("disk error"));
        assert_eq!(pending(), 8, "失败要把取走的 8 行原样退回");
        let _: Result<(), &str> = s.run_pending(|_, _| Err("disk error"));
        assert_eq!(pending(), 8, "连着失败也只是原样退回，不许越堆越高");
        assert_eq!(s.cursor().as_deref(), Some(&b"mid"[..]), "失败不许动游标");
    }

    #[test]
    fn scheduler_refunds_errors_and_panics_and_serializes_cursor() {
        use crate::common::{VacuumRound, VacuumStats};
        let s = VacuumScheduler::new(8);
        s.on_commit(24);
        let status = || s.status(&Registry::new());
        let fail: Result<(), &str> = s.run_pending(|_, _| Err("disk error"));
        assert_eq!(fail, Err("disk error"));
        assert_eq!(status().pending_rows, 24);
        assert_eq!(status().failed_slices, 1);
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _: Result<(), ()> = s.run_pending(|_, _| panic!("probe"));
        }));
        assert!(panic.is_err());
        assert!(!status().running);
        assert_eq!(status().pending_rows, 24);
        let ok: Result<(), ()> = s.run_pending(|after, rows| {
            assert_eq!(after, None);
            assert_eq!(rows, 8);
            // 同一片还没返回时，再入不能取账或读写游标（等价于另一线程撞进来）。
            let nested: Result<(), ()> = s.run_pending(|_, _| panic!("duplicate slice"));
            assert!(nested.is_ok());
            Ok(VacuumRound {
                stats: VacuumStats { xmin: 1, versions_removed: 3, blob_rewritten: None,
                    skipped_active_txn: false, rows_scanned: 8 },
                next_cursor: Some(b"next".to_vec()),
            })
        });
        assert!(ok.is_ok());
        assert_eq!(status().pending_rows, 16);
        assert_eq!(status().committed_rows, 24);
        assert_eq!(status().versions_removed, 3);
        assert_eq!(s.cursor().as_deref(), Some(&b"next"[..]));
    }

    #[test]
    fn cursor_round_trips() {
        let s = VacuumScheduler::new(8);
        assert_eq!(s.cursor(), None);
        s.set_cursor(Some(b"k1".to_vec()));
        assert_eq!(s.cursor().as_deref(), Some(&b"k1"[..]));
        s.set_cursor(None);
        assert_eq!(s.cursor(), None);
    }

    /// 只读事务不许进 WAL；写过一次之后，整条事务的 `Commit`/fsync 都要回来。
    #[test]
    fn wal_policy_opens_only_on_first_write() {
        let p = WalPolicy::default();
        assert!(!p.opened(), "刚 begin 的事务不该写 Commit、也不该 fsync");
        p.mark_opened();
        assert!(p.opened(), "写过就得写 Commit + fsync，否则持久化点丢了");
        p.mark_opened();
        assert!(p.opened(), "重复标记不许把状态打回去（M25 起这里只记账，不再有「第一次」的返回值）");
    }

    #[test]
    fn unfinished_is_answered_by_membership() {
        let mut ledger: crate::common::txn::Registry = std::collections::HashMap::new();
        ledger.insert(7u64, [b"k".to_vec()].into_iter().collect());
        let active: Mutex<crate::common::txn::Registry> = Mutex::new(ledger);
        assert!(is_unfinished(&active, 7), "还在活跃表里 ⇒ 未终结");
        // 判据必须**只查不删**：删除是引擎 rollback 的活，它要按表里那份 key 列表清行
        assert!(is_unfinished(&active, 7), "查一次不能把条目吃掉，否则 rollback 拿到空列表、行留在库里");
        assert!(!is_unfinished(&active, 8), "没注册过的事务不该被回滚");
    }
}
