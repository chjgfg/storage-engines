//! 事务内核里可以共用的三段逻辑：快照可见性、health 分组折叠、自动回收记账。
//!
//! 这三段在四个引擎里原本是**逐字相同**的复制（`is_visible` 四份一模一样）。复制的代价
//! 不是行数，而是"同一个 bug 会长四个实例、修一个要修四遍" —— 自动回收那次就是我把
//! `swap` 当成取账写进了四份代码里，四家一起坏。所以这里把它们收成一份，
//! 各引擎仍然保留自己的遍历原语和自己的守卫（合并的是判定与记账，不是扫描）。

use std::collections::{HashMap, HashSet};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};

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

/// 漏网事务的判据：活跃表里还留着"自己"，就说明既没 `commit` 也没 `rollback`。
///
/// 为什么用成员关系而不是额外标志位：`commit` / `rollback` 都以 `remove` 收尾，
/// 所以「还在表里」就是「未终结」的充要条件，多一个标志位就多一处会写漏的可能。
///
/// ⚠️ 这里**只查不删**。删除必须留给引擎自己的 `rollback()` —— 它还要按表里那份 key 列表
/// 把本事务写过的版本行挨个清掉；在这里删掉，`rollback()` 就拿到空列表，
/// 事务的行会留在库里（判据写成 `remove` 是我第一版的想法，看了 rollback 才否掉）。
///
/// 锁中毒（别的线程 panic 过）时返回 `false`：Drop 里 panic 会直接 abort 进程，
/// 宁可不回滚也不能把宿主进程带下去。
pub fn is_unfinished(active_txn: &Mutex<HashMap<u64, Vec<Vec<u8>>>>, version: u64) -> bool {
    match active_txn.lock() {
        Ok(guard) => guard.contains_key(&version),
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
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

    #[test]
    fn unfinished_is_answered_by_membership() {
        let active: Mutex<HashMap<u64, Vec<Vec<u8>>>> =
            Mutex::new(HashMap::from([(7u64, vec![b"k".to_vec()])]));
        assert!(is_unfinished(&active, 7), "还在活跃表里 ⇒ 未终结");
        // 判据必须**只查不删**：删除是引擎 rollback 的活，它要按表里那份 key 列表清行
        assert!(is_unfinished(&active, 7), "查一次不能把条目吃掉，否则 rollback 拿到空列表、行留在库里");
        assert!(!is_unfinished(&active, 8), "没注册过的事务不该被回滚");
    }
}
