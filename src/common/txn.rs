//! 事务内核（M18b/M18c）：begin 的登记、写冲突判定、写序列、commit 的销账、rollback 的撤销
//! —— 收成一份。
//!
//! 判据仍是 M5 那条：**与存储布局无关的合，有关的各留一份**。
//! "什么时候算写冲突、动手前要按什么顺序问哪几件事、写集登记在哪、回滚要撤哪些行、提交给自动
//! 回收记几笔账"四家算的是同一件事；而"这一行的字节落在哪个结构、blob 旁路怎么写、WAL 记录怎么
//! 排"是各家的布局 —— 那半边留在 [`WriteSide`] 的方法里（有 WAL / blob / 页容量判定的人家的
//! 只需实现自己的那几个槽，其余走默认实现 = 什么都不做）。
//!
//! ## 写序列只有一份
//!
//! [`TxnCore::write`] 走的顺序（各家原先各抄一遍、顺序一字不差）：
//!
//! ```text
//! 1. latest_version(key)      —— 引擎侧读"这一把 key 当前最新版本"
//! 2. is_visible(latest)?       —— 不可见 ⇒ 写冲突，返回 false（此刻库里一个字都没动）
//! 3. encode_value(value)       —— blob 旁路在这里（默认：原样）
//! 4. row_fits(enc, stored)?    —— bplus 的"这行装不进一页"必须在这里就拒（默认：装得下）
//! 5. wal_write(xid, enc, ...)  —— 落 WAL 在先，改库在后（默认：成功）；同时把"进过 WAL"记账
//! 6. put_versioned(enc, ...)   —— 真正改库（**可失败**：bitcask 写数据文件会 Err）
//! 7. 写集登记                   —— 只登记真落进库的那一行
//! ```
//!
//! 3~5 都在 6 之前是**刻意的**：任何一步失败都不能留下"写集里有、库里没有"的悬账，否则随后的
//! `rollback` 会去删一批根本不存在的行、`commit` 的回收账还会多算。
//! 4 必须在 5 之前：bplus 原先把容量判定放在 WAL 之后 ⇒ 一条被拒收的超大行在盘上留下
//! 一条永远写不进去的记录。内核把判定提到它前面，于是拒收的那次**一个字都不落**。
//!
//! ⚠️ M25 之前这里是**八步**，第 5 步是"第一次写补一条 `WalRecord::Begin`"。那条记录被删掉了，
//! 不是因为它贵，而是因为**恢复不读它**：`analyze_recovery` 要的信息全在 `Write`/`Commit`/`Abort` 里
//! （`Write` 自己就把 `open_writes` 的条目建起来、`max_xid` 也从它取），把整步作废之后全量测试
//! 一条都不红 —— 也就是说它一直是个**没有守卫覆盖的假设**。留着它的害处不是字节，是下一个人会
//! 以为"事务开始"这件事在日志里有凭据。`WalRecord::Begin` 变体与解析分支**保留**（0.4.0 建的库
//! 里那些老日志必须还能读），兼容由守卫钉：`plan_is_identical_with_or_without_begin`。
//!
//! ## 锁的顺序
//!
//! `active_txn`（活跃表）必须在存储锁**之前**拿：[`crate::memory::MVCC::vacuum_bounded`] 走的
//! 就是 `active_txn → store`（它还要求活跃表为空才动手），健康度那条则一次只持一把。
//! 但内核不能自己去拿第二把 —— 存储侧要锁几样（kv / wal / blob）是各家的布局。所以规则是：
//! **调用方先 [`TxnCore::lock_registry`]，再按自己家的顺序锁存储侧，然后把两个守卫一起交给内核**。
//! 顺序由调用方的两行保证，判定与序列由内核保证。
//!
//! 搬的进度：四家都跑在这份内核上（`memory` M18b、`lsm_tree` M18c、`bitcask` M18d、
//! `bplus_tree` M18e）。各家剩的是自己那几个槽：内存只给三个必需方法，LSM 多给两条 WAL，
//! bitcask 再多给一条 `encode_value`，bplus 用满（`encode_value` + `row_fits`）。
//! 每搬一家都当场证明它没改语义 —— 靠的是 `txn_kernel_agrees_across_engines`（同一段事务脚本
//! 在四家跑出同一个终态），各家原有测试只测自家那一份副本，跨引擎对拍才看得见"内核写歪了"。

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::common::version::encode_key;
use crate::common::{WalPolicy, is_unfinished, visible_at, VacuumScheduler};

/// 活跃事务表：`xid → 本事务写过的逻辑 key 集合`。四家同一个类型，所以这里给个名字。
///
/// ⚠️ 值是**集合**不是 `Vec`：登记一把键之前要判"这把是否已经登记过"（同一把键在一个事务里
/// 写三次只算一行账），遍历写集去判就是**单个事务内 O(写集²)** —— 与 M19 之前 LSM 的 flush
/// 判定（每插一行把整个 memtable 重数一遍）是同一个形状，只是这次藏在事务里，只有"一个大事务
/// 塞几万行"才浮出来（2026-10-17 实测它不是灌行超线性的主导项，因为台架每 1000 行一事务）。
///
/// 这里也**不需要顺序**：`rollback` 是逐把自己写过的版本行物理删掉，删的先后互不影响；
/// `undo` 是集合减法。刻意只留一份账（不并存 `Vec` 顺序表）—— 两份账就会有一份写漏。
pub type Registry = HashMap<u64, HashSet<Vec<u8>>>;

// 测试专用：写集**加减一次要考察多少把已登记的键**。
//
// M24 的复杂度尺。哈希 = 每次 1（一次查重）；写成遍历写集就是每次 `写集当前长度`
// ⇒ K 行合计 K(K−1)/2 次。⚠️ 这把尺子只有长在**唯一的写集入口** [`register_write`] /
// [`unregister_write`] 里才有意义：绕过它再抄一份查重，计数就看不见那一份。
#[cfg(test)]
thread_local! {
    static WRITE_SET_SCANS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn note_write_set_scan(n: u64) {
    WRITE_SET_SCANS.with(|c| c.set(c.get() + n));
}

/// 本线程"写集加减考察过的已登记键数"（测试探针用）
#[cfg(test)]
pub fn write_set_scans() -> u64 {
    WRITE_SET_SCANS.with(|c| c.get())
}

/// 登记一行进写集：`true` = 这行是新登记的那把键（同一把键在一个事务里只记一次账）。
///
/// 查重走哈希 —— **不许**退回 `writes.iter().any(|k| k == key)`，那是单事务内 O(写集²)。
/// 尺子就在这个唯一入口里，所以退化会当场被 [`write_set_scans`] 看见。
fn register_write(writes: &mut HashSet<Vec<u8>>, key: &[u8]) -> bool {
    #[cfg(test)]
    note_write_set_scan(1);
    writes.insert(key.to_vec())
}

/// 从写集摘掉一行（批量写撞墙后的部分撤销）。同样是哈希减法，不是遍历。
fn unregister_write(writes: &mut HashSet<Vec<u8>>, key: &[u8]) {
    #[cfg(test)]
    note_write_set_scan(1);
    writes.remove(key);
}

/// **对照用的慢算法**：把写集整个走一遍来找重复。
///
/// 只为守卫存在（M19 同一手法）：同一条用例里主动慢数一次，既证明那把尺子不是空转，
/// 也当场量出"退回遍历写集"要走多少把键。
#[cfg(test)]
pub fn write_set_contains_linear(writes: &HashSet<Vec<u8>>, key: &[u8]) -> bool {
    note_write_set_scan(writes.len() as u64);
    writes.iter().any(|k| k.as_slice() == key)
}

/// 一家的**写侧**：这一行的字节住在哪、WAL 与 blob 怎么落。
///
/// 只有 `latest_version` / `put_versioned` / `remove_versioned` 是必需的，其余四个槽是给
/// 有 WAL / blob / 页容量判定的人家准备的，默认实现 = "这一步不做、也不会失败"。
/// 实现者应当是**已经把锁拿好的守卫**（或其包装），这样序列全程在同一把锁下 ——
/// 判冲突与落行之间插进别人的写，冲突检测就白做了。
pub trait WriteSide {
    /// 本逻辑 key 当前最大的版本号（一把都没有 ⇒ `None`）。
    ///
    /// 写冲突判据唯一的外部输入，拿到之后由 [`TxnCore::is_visible`] 定论。
    /// **必须能看到本事务刚写下的行**（同一次 `write` 里锁一直持着，所以是同一份存储状态）。
    fn latest_version(&mut self, key: &[u8]) -> Option<u64>;

    /// 落一行版本：`value = None` 是墓碑。`enc` 已由内核编好（`encode_key(key, version)`）。
    ///
    /// 返回 `false` = 这一行**没落进去**（bitcask 的 `insert` 会因写数据文件失败而 Err ——
    /// 那条通道原先只在引擎里 `eprintln!` 一下就被丢掉，于是写集里留着一条库里不存在的行）。
    /// 内核据此**不登记写集**，所以"写集里有、库里没有"这种悬账不可能再出现。
    fn put_versioned(&mut self, enc: &[u8], value: Option<Vec<u8>>) -> bool;

    /// 物理撤掉本事务写下的一行（回滚用 —— **不是**写墓碑，那会让这行永远留在库里）。
    fn remove_versioned(&mut self, enc: &[u8]);

    /// 逻辑 value → 存储字节（大值走 blob 旁路就在这里）。`None` = 写旁路失败 ⇒ 整次写作废。
    fn encode_value(&mut self, value: Vec<u8>) -> Option<Vec<u8>> {
        Some(value)
    }

    /// 这一行装得下吗？`false` ⇒ 内核拒收本次写，返回 `false`。
    ///
    /// 只有定长页的引擎需要实现（bplus：单行超过一页载荷就**必须**在落 WAL 之前拒掉，
    /// 否则盘上会留下永远写不进去的记录）。
    fn row_fits(&mut self, _enc: &[u8], _stored: Option<&[u8]>) -> bool {
        true
    }

    /// 改库之前先落 WAL：`false` = 失败 ⇒ 整次写作废（此刻库里一个字都没动）。
    fn wal_write(&mut self, _xid: u64, _enc: &[u8], _stored: Option<&[u8]>) -> bool {
        true
    }
}

/// 事务**收尾**要动的东西：只有 WAL 的那一半。
///
/// 为什么单列一个 trait 而不是并进 [`WriteSide`]：`commit` 只销账 + 记账 + 落一条 `Commit`，
/// 根本不需要存储侧 —— 硬要它交一个写侧守卫，等于让 LSM 那样的引擎在 fsync 期间一直握着 kv 锁
/// 把读者挡在门外（改前的代码就是先放掉再收尾的）。所以这里只声明收尾那两条。
///
/// 默认实现 = "什么都不做"，因此没有 WAL 的引擎传 `&mut ()` 就行。
pub trait WalTail {
    /// 提交尾：`Commit` 记录 + fsync。只在事务真进过 WAL 时才被调用。
    fn wal_commit(&mut self, _xid: u64) {}

    /// 回滚尾：`Abort` 记录（+ fsync），让恢复时跳过本事务写过的行。
    fn wal_abort(&mut self, _xid: u64) {}
}

/// 没有 WAL 的引擎（`memory`）用它当收尾占位；顺带也是"这一家不实现写侧"时的空守卫。
impl WalTail for () {}

/// 事务内核：版本、可见性快照、写集、WAL 开窗状态、回收记账。
pub struct TxnCore {
    /// 本事务的版本号 = 写进去的每一行的版本号
    pub version: u64,
    /// begin 那一刻的活跃事务快照（可见性判据的另一半）
    pub active_xid: HashSet<u64>,
    active_txn: Arc<Mutex<Registry>>,
    vac: Arc<VacuumScheduler>,
    /// 本事务是否已经进过 WAL（第一次写时才补 `Begin`）—— 见 [`crate::common::WalPolicy`]
    wal_policy: WalPolicy,
}

impl TxnCore {
    /// 分配版本号 + 拍活跃快照 + 把自己登记进活跃表。
    ///
    /// 三步是一个原子动作：登记之后到 `commit`/`rollback` 之前，别的事务看得见本事务的 id，
    /// 于是它们对本事务写过的 key 会判成写冲突。
    ///
    /// `begin` **不进 WAL**：此刻还不知道会不会写，所以只读事务一个字节都不落（见
    /// [`crate::common::WalPolicy`]）。
    pub fn begin(
        active_txn: Arc<Mutex<Registry>>,
        next_version: &AtomicU64,
        vac: Arc<VacuumScheduler>,
    ) -> Self {
        let version = next_version.fetch_add(1, Ordering::SeqCst);
        let active_xid = {
            let mut guard = active_txn.lock().unwrap();
            let snapshot: HashSet<u64> = guard.keys().cloned().collect();
            guard.insert(version, HashSet::new());
            snapshot
        };
        Self {
            version,
            active_xid,
            active_txn,
            vac,
            wal_policy: WalPolicy::default(),
        }
    }

    /// 那个版本对本事务可见吗（判据在 [`crate::common::visible_at`]，四引擎共用的一份）。
    pub fn is_visible(&self, version: u64) -> bool {
        visible_at(version, self.version, &self.active_xid)
    }

    /// 还没终结（既没 commit 也没 rollback）？`Drop` 的兜底判据。
    ///
    /// 只查不删：删除必须留给 [`Self::rollback`]，它要按表里那份 key 列表把本事务写过的行清掉。
    pub fn unfinished(&self) -> bool {
        is_unfinished(&self.active_txn, self.version)
    }

    /// 本事务是否进过 WAL ⇒ `commit` 要不要写 `Commit`、`rollback` 要不要写 `Abort`。
    pub fn wal_opened(&self) -> bool {
        self.wal_policy.opened()
    }

    /// **写路径的第一行该是这个**：先锁活跃表，再锁这家的存储侧（顺序理由见模块头）。
    pub fn lock_registry(&self) -> MutexGuard<'_, Registry> {
        self.active_txn.lock().unwrap()
    }

    /// 一次写：按模块头那 8 步走。返回 `false` 有**三种**原因，只有第一种是冲突：
    ///
    /// 1. 写冲突（别的事务在我之后改了同一把 key）—— 三家共同的语义，调用方该 `rollback` 重试
    /// 2. 这一行装不下（bplus：超过一页载荷）
    /// 3. WAL 写失败
    ///
    /// 三者此刻库里都一个字没动，所以可以共用一个返回值（要区分原因得再开一个口子，未承诺）。
    pub fn write<S: WriteSide>(
        &self,
        reg: &mut Registry,
        side: &mut S,
        key: &[u8],
        value: Option<Vec<u8>>,
    ) -> bool {
        if let Some(latest) = side.latest_version(key) {
            if !self.is_visible(latest) {
                return false;
            }
        }

        let enc = encode_key(key, self.version);
        let stored = match value {
            None => None,
            Some(v) => match side.encode_value(v) {
                Some(s) => Some(s),
                // blob 旁路写失败：不能留下"库里指着个不存在的 blob"的行
                None => return false,
            },
        };

        // 只有定长页的引擎会在这里说"装不下"；必须在 WAL 之前，否则盘上留下一条永远写不进的记录
        if !side.row_fits(&enc, stored.as_deref()) {
            return false;
        }
        if !side.wal_write(self.version, &enc, stored.as_deref()) {
            return false;
        }
        // M25：第一次写只**记账**，不再落 `Begin`（恢复不读它，守卫
        // `plan_is_identical_with_or_without_begin`）。标记打在 `wal_write` **成功之后**：
        // 这样 "进过 WAL ⇒ commit 要写 `Commit` + fsync" 这句话字面成立，
        // 而一次 WAL 都没落成的事务也不会去付那次无意义的 fsync。
        self.wal_policy.mark_opened();

        let writes = reg.entry(self.version).or_default();
        if side.put_versioned(&enc, stored) {
            register_write(writes, key);
            return true;
        }
        // 落行失败：写集里一行都不许留（上面 `entry` 建的那个空条目不算账，
        // `commit` 数的是 `keys.len()`，空条目 = 0 行）
        false
    }

    /// 提交：从活跃表销账 + 给自动回收记一笔账 + WAL 收尾（进过 WAL 才收尾）。
    ///
    /// 行数是**近似**（按逻辑 key 数算）：要精确就得再走一遍写集，而这一笔账只决定回收的节奏，
    /// 宁可少算也不为它多付一次遍历。
    ///
    /// ⚠️ WAL 之后的 fsync、`flush_mem`、超阈值 checkpoint 这些"提交之后各家想做的事"不在这里 ——
    /// 它们要么只有磁盘引擎有，要么参数是各家的旋钮。调用方在拿到本方法返回之后接着做。
    pub fn commit<W: WalTail>(&self, reg: &mut Registry, wal: &mut W) {
        let rows_written = reg.remove(&self.version).map(|k| k.len()).unwrap_or(0);
        self.vac.on_commit(rows_written as u64);
        if self.wal_policy.opened() {
            wal.wal_commit(self.version);
        }
    }

    /// 回滚：按写集把本事务落下的版本行**物理删掉**，并给 WAL 留一条 `Abort`。
    pub fn rollback<S: WriteSide + WalTail>(&self, reg: &mut Registry, side: &mut S) {
        let keys = reg.remove(&self.version).unwrap_or_default();
        if self.wal_policy.opened() {
            side.wal_abort(self.version);
        }
        if keys.is_empty() {
            return;
        }
        for k in keys {
            let enc = encode_key(&k, self.version);
            side.remove_versioned(&enc);
        }
    }

    /// **部分**撤销：把 `keys` 这几把已写过的 key 从库里撤掉，并从写集里摘掉。
    ///
    /// 批量写（`batch_set` / `batch_delete`）撞墙时用它：那批不能只撤到"出错那一条"为止 ——
    /// 前面已经落下的行也得退回去，然后调用方才 `rollback()` 整个事务重试。
    /// 摘写集是必须的：留着的话随后的 [`Self::rollback`] 会去删一批早已不存在的行（不报错，
    /// 但 `commit` 的回收账也会多算这几条）。
    ///
    /// 四家原先各写一份"先锁存储、再 `try_lock` 活跃表，拿不到就跳过摘写集"，那是内核里唯一的
    /// 反向嵌套点，且"跳过"让写集与库内容对不上 —— 合成一份之后锁序正向、摘写集必达。
    pub fn undo<S: WriteSide>(&self, reg: &mut Registry, side: &mut S, keys: &[Vec<u8>]) {
        if keys.is_empty() {
            return;
        }
        for k in keys {
            let enc = encode_key(k, self.version);
            side.remove_versioned(&enc);
        }
        if let Some(writes) = reg.get_mut(&self.version) {
            // 减法也走哈希：`writes.retain(|w| !keys.iter().any(...))` 是 O(写集 × 撤销数)
            for k in keys {
                unregister_write(writes, k);
            }
        }
    }
}

/// bulk 会话的内核：一个会话一个版本号、无冲突检测、**不登记活跃表**（单写者假定）。
///
/// 与 [`TxnCore`] 的区别不只是"少了检测"：bulk 不拍快照、不能回滚，它的正确性前提是
/// "没有别人在写"。落库的口子各家差得远（内存直接插、LSM 可以跳 memtable 直接写 L0、
/// bitcask/bplus 要走 blob），所以这里只收**版本号编排 + 收尾幂等守卫**这两件共同的事。
pub struct BulkCore {
    /// 本会话所有行共用的版本号
    pub version: u64,
    next_version: Arc<AtomicU64>,
    finished: bool,
}

impl BulkCore {
    pub fn begin(next_version: Arc<AtomicU64>) -> Self {
        let version = next_version.fetch_add(1, Ordering::SeqCst);
        Self {
            version,
            next_version,
            finished: false,
        }
    }

    /// 收尾的幂等守卫：`finish()` 与 `Drop` 都会调，只有第一次返回 `true`。
    ///
    /// 各家"收尾要做什么"完全不同（落 MANIFEST / 退 bulk 模式 / WAL checkpoint / 持久化
    /// `next_version`），所以那一半留在调用方 —— 这里只保证"别做两遍"。
    pub fn first_finish(&mut self) -> bool {
        if self.finished {
            return false;
        }
        self.finished = true;
        true
    }

    /// 当前水位（收尾时用来写 checkpoint / 持久化）
    pub fn next_version(&self) -> &Arc<AtomicU64> {
        &self.next_version
    }

    /// 下面三个口子给"落库就是插一行"的引擎用（内存引擎；LSM/bitcask/bplus 各有更快的路）。
    pub fn put<S: WriteSide>(&mut self, side: &mut S, key: &[u8], value: Vec<u8>) {
        let enc = encode_key(key, self.version);
        side.put_versioned(&enc, Some(value));
    }

    pub fn delete<S: WriteSide>(&mut self, side: &mut S, key: &[u8]) {
        let enc = encode_key(key, self.version);
        side.put_versioned(&enc, None);
    }

    /// 一次加锁灌一批（消费 items，零额外 clone）
    pub fn put_batch<S: WriteSide>(&mut self, side: &mut S, items: Vec<(Vec<u8>, Vec<u8>)>) {
        for (key, value) in items {
            let enc = encode_key(&key, self.version);
            side.put_versioned(&enc, Some(value));
        }
    }

    /// 一次加锁灌一批（借用 items）
    pub fn put_batch_ref<S: WriteSide>(&mut self, side: &mut S, items: &[(Vec<u8>, Vec<u8>)]) {
        for (key, value) in items {
            let enc = encode_key(key, self.version);
            side.put_versioned(&enc, Some(value.clone()));
        }
    }
}
