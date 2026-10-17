//! 事务内核里可以共用的三段逻辑：快照可见性、health 分组折叠、自动回收记账。
//!
//! 这三段在四个引擎里原本是**逐字相同**的复制（`is_visible` 四份一模一样）。复制的代价
//! 不是行数，而是"同一个 bug 会长四个实例、修一个要修四遍" —— 自动回收那次就是我把
//! `swap` 当成取账写进了四份代码里，四家一起坏。所以这里把它们收成一份，
//! 各引擎仍然保留自己的遍历原语和自己的守卫（合并的是判定与记账，不是扫描）。

use std::collections::HashSet;
use std::sync::{
    atomic::{AtomicU64, Ordering},
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

/// 把「按 enc 升序到达的版本行」折成 [`MvccHealth`] 的累加器。
///
/// 依赖的唯一事实：`encode_key` 是大端版本号接在 raw_key 后面 ⇒ 同一把逻辑 key 的行
/// 在键序里相邻成块，所以**顺序一遍**就能数出链长，不需要真的建 `BTreeMap<raw, Vec<..>>`
/// （那会变成 O(全库) 的额外内存）。
///
/// ⚠️ 「相邻」对**真前缀嵌套**的 key 会破（`"a"` 与 `"a\0…"` 的版本区间能互相穿插）：
/// 那时同一个 raw_key 会被数成两段，`max_chain` 于是**偏小**。这里刻意不为了它建全图 ——
/// 版本号要涨到 `0x31_0000_0000_0000_000` 量级才可能撞上，而把链数少只会让告警变迟钝，
/// 不会导致裁错版本（裁错的守卫在 [`crate::common::vacuum_plan`] 那侧）。
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
        }
    }

    /// 该裁一片了吗：攒够阈值就取走这笔账并返回切片宽度，否则 `None`。
    pub fn take_tick(&self) -> Option<u64> {
        let slice = self.slice.load(Ordering::SeqCst);
        if slice == 0 {
            return None;
        }
        let seen = self.pending.load(Ordering::SeqCst);
        if seen < slice {
            return None;
        }
        // CAS 失败 = 别的线程已经取走这笔账正在裁，本轮让给它
        match self
            .pending
            .compare_exchange(seen, 0, Ordering::SeqCst, Ordering::SeqCst)
        {
            Ok(_) => Some(slice),
            Err(_) => None,
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
/// `matched` 有两种长度，含义不同，所以 `has_next` 分两条路算：
/// - **要 total**（`need_total = true`）：调用方把整段（界内）命中都交过来，
///   `total` 就是它的长度，"还有没有下一页"由页码推出来。
/// - **不要 total**（`need_total = false`）：调用方最多交 `skip + page_size + 1` 条
///   （多取那一条就是用来探边的）。此时要么正好数到"窗口外还有一条"，要么已经走到库尾 /
///   界尾 —— 后者 `matched.len() <= end` ⇒ 没有下一页。**不许把"没数"报成"到底了"是
///   靠这条区分的**：走到库尾是真的到底，而探到第 `end+1` 条说明后面还有。
pub fn search_page_from(
    matched: Vec<crate::common::ExportRecord>,
    query: &crate::common::SearchQuery,
) -> crate::common::SearchPage {
    let page_size = query.page_size.max(1);
    let page = query.page;
    let skip = page.saturating_mul(page_size);
    let total = query.need_total.then_some(matched.len());
    let items = if skip >= matched.len() {
        Vec::new()
    } else {
        let end = (skip + page_size).min(matched.len());
        matched[skip..end].to_vec()
    };
    let has_next = if query.need_total {
        let total = matched.len();
        total > 0 && page + 1 < (total + page_size - 1) / page_size
    } else {
        matched.len() > skip + page_size
    };
    let total_pages = if query.need_total {
        let total = matched.len();
        Some(if total == 0 {
            0
        } else {
            (total + page_size - 1) / page_size
        })
    } else {
        None
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

/// 窗口折叠的共用驱动（四引擎一份）。
///
/// `round(after, budget)`：从 `after` 这组**之后**开始折，最多交 `budget` 组候选，
/// 返回 `(候选, 是否已走到界尾)`。`keep(candidate)`：候选 ⇒ 记录（取值 + 剥壳 +
/// TTL/墓碑过滤），`None` = 这一条不算命中窗口。`wanted = None` = 一趟走完全库。
///
/// 为什么要能续跑第二轮：**"这把键命不命中 pattern"在键这一层就能判，而"这行到底
/// 取不取到值"（blob 指针、TTL 是否过期）要取了值才知道** ⇒ 提前收手有可能少数几条。
/// 于是从上一轮的最后一个键续跑、预算翻倍 ⇒ 总代价仍是最终那一轮的同阶，最坏退化成
/// "走完全库"，而那正好是 `wanted = None`（要精确 total）的语义。同一个几何重试的形状
/// 在 `vacuum_bounded` 的窗口放大里已经用过一次。
///
/// 为什么值读取放在 `keep` 而不是折在 `round` 里：bitcask/bplus/lsm 的取值要拿 blob 锁，
/// 而候选是**拿着 kv 锁走元数据**折出来的 —— 今天的两段式就是为了不在这两把锁上打结。
pub fn fold_window<V, R, K, T>(wanted: Option<usize>, mut round: R, mut keep: K) -> Vec<T>
where
    R: FnMut(Option<&[u8]>, usize) -> (Vec<WinCandidate<V>>, bool),
    K: FnMut(WinCandidate<V>) -> Option<T>,
{
    let mut out: Vec<T> = Vec::new();
    let mut after: Option<Vec<u8>> = None;
    let mut budget = wanted.unwrap_or(usize::MAX);
    loop {
        let (batch, reached_end) = round(after.as_deref(), budget);
        let produced = batch.len();
        for cand in batch {
            after = Some(cand.key.clone());
            if let Some(kept) = keep(cand) {
                out.push(kept);
                if wanted.is_some_and(|w| out.len() >= w) {
                    return out;
                }
            }
        }
        if reached_end || produced == 0 {
            return out;
        }
        budget = budget.saturating_mul(2).max(64);
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

        let rec = |i: usize| ExportRecord {
            key: format!("k{i:02}").into_bytes(),
            value: Some(vec![b'v']),
        };
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
        assert_eq!(s.take_tick(), Some(8), "攒够 8 行必须触发一片");
        assert!(s.take_tick().is_none(), "取走后账要清零，不许连续空转");

        // 关闭：一律不触发，账也不许被偷偷吃掉
        s.set_slice(0);
        s.on_commit(100);
        assert_eq!(s.take_tick(), None);
        s.set_slice(8);
        assert_eq!(s.take_tick(), Some(8), "重新打开后原来的账还在");
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
