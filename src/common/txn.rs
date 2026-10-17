//! 事务内核（M18b）：begin 的登记、写冲突判定、commit 的销账、rollback 的撤销 —— 收成一份。
//!
//! 判据仍是 M5 那条：**与存储布局无关的合，有关的各留一份**。
//! "什么时候算写冲突、写集登记在哪、回滚要撤哪些行、提交给自动回收记几笔账"四家算的是同一件事
//! （都是 `版本 = next_version++` + 拍活跃快照 + 「最新版本不可见 ⇒ 拒写」+ 按写集撤销）；
//! 而"这一行的字节落在哪个结构、动手之前要不要先落 WAL、大值要不要走 blob 旁路"是各家的布局 ——
//! 那半边留在 [`WriteSide`] 的三个方法里。
//!
//! ⚠️ **锁的顺序是内核的一部分，不是实现细节**：`active_txn` 必须在存储锁**之前**拿。
//! [`crate::memory::MVCC::vacuum_bounded`] 走的就是 `active_txn → store`（它还要求活跃表为空才动手），
//! 健康度那条则是"一次只持一把"。谁在外面先锁了存储再进内核，就和这些路径构成反向嵌套。
//! 所以 [`TxnCore::write`] / [`TxnCore::rollback`] 自己按固定顺序取两把锁，调用方只交句柄。
//!
//! 搬的进度：M18b 只有 `memory` 一家用上（另三家各一个里程碑，带 WAL / blob 的那两支到时候再往
//! [`WriteSide`] 上加口子）。一家用上看不出收益，收益要在第三家搬完才兑现 —— 但每搬一家都当场
//! 全量跑它自己的守卫，这是"基座"这一刀唯一的验收方式。

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::common::version::encode_key;
use crate::common::{is_unfinished, visible_at, VacuumScheduler};

/// 一家的**写侧**：这一行的字节住在哪、怎么落下去、怎么撤掉。
///
/// 三个方法都不含事务语义（可见性判据在 [`TxnCore`]，版本号的编排在 [`TxnCore::begin`]）。
pub trait WriteSide {
    /// 本逻辑 key 当前最大的版本号（一把都没有 ⇒ `None`）。
    ///
    /// 这是写冲突判据唯一的外部输入：拿到之后由 [`TxnCore::is_visible`] 决定"能写"还是"别人
    /// 在我之后改过 ⇒ 返回 `false`"。**必须能看到本事务刚写下的行**（同一次 `write` 里两把锁
    /// 都持着，所以是同一份存储状态，不会读到旧快照）。
    fn latest_version(&mut self, key: &[u8]) -> Option<u64>;

    /// 落一个版本行：`value = None` 是墓碑。`enc` 已由调用方编好（`encode_key(key, version)`）。
    fn put_versioned(&mut self, enc: &[u8], value: Option<Vec<u8>>);

    /// 物理撤掉本事务写下的一行（回滚用 —— **不是**写墓碑，那会让这行永远留在库里）。
    fn remove_versioned(&mut self, enc: &[u8]);
}

/// 事务内核：本事务的版本、可见性快照、写集、回收记账。
pub struct TxnCore {
    /// 本事务的版本号 = 写进去的每一行的版本号
    pub version: u64,
    /// begin 那一刻的活跃事务快照（可见性判据的另一半）
    pub active_xid: HashSet<u64>,
    active_txn: Arc<Mutex<HashMap<u64, Vec<Vec<u8>>>>>,
    vac: Arc<VacuumScheduler>,
}

impl TxnCore {
    /// 分配版本号 + 拍活跃快照 + 把自己登记进活跃表。
    ///
    /// 三步是一个原子动作：登记之后到 `commit`/`rollback` 之前，别的事务看得见本事务的 id，
    /// 于是它们对本事务写过的 key 会判成写冲突。
    pub fn begin(
        active_txn: Arc<Mutex<HashMap<u64, Vec<Vec<u8>>>>>,
        next_version: &AtomicU64,
        vac: Arc<VacuumScheduler>,
    ) -> Self {
        let version = next_version.fetch_add(1, Ordering::SeqCst);
        let active_xid = {
            let mut guard = active_txn.lock().unwrap();
            let snapshot: HashSet<u64> = guard.keys().cloned().collect();
            guard.insert(version, Vec::new());
            snapshot
        };
        Self {
            version,
            active_xid,
            active_txn,
            vac,
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

    /// 一次写：冲突判定 → 登记写集 → 落行。返回 `false` **只表示写冲突**（不是"没写进去"）。
    ///
    /// 顺序不能换：判冲突要在登记写集之前（否则回滚会撤一把根本没写的行），
    /// 落行要在判冲突之后（否则冲突方会把已写的行留在库里）。
    pub fn write<S: WriteSide>(&self, store: &Mutex<S>, key: &[u8], value: Option<Vec<u8>>) -> bool {
        let mut active_txn = self.active_txn.lock().unwrap();
        let mut store = store.lock().unwrap();

        if let Some(latest) = store.latest_version(key) {
            if !self.is_visible(latest) {
                return false;
            }
        }

        let enc = encode_key(key, self.version);
        let writes = active_txn.entry(self.version).or_default();
        if !writes.iter().any(|k| k == key) {
            writes.push(key.to_vec());
        }

        store.put_versioned(&enc, value);
        true
    }

    /// 提交：从活跃表销账，并把本事务写的行数记给自动回收。
    ///
    /// 行数是**近似**（按逻辑 key 数算）：要精确就得再走一遍写集，而这一笔账只决定回收的节奏，
    /// 宁可少算也不为它多付一次遍历。
    pub fn commit(&self) {
        let rows_written = {
            let mut active_txn = self.active_txn.lock().unwrap();
            active_txn
                .remove(&self.version)
                .map(|k| k.len())
                .unwrap_or(0)
        };
        self.vac.on_commit(rows_written as u64);
    }

    /// 回滚：按写集把本事务落下的版本行**物理删掉**。
    pub fn rollback<S: WriteSide>(&self, store: &Mutex<S>) {
        let keys = {
            let mut active_txn = self.active_txn.lock().unwrap();
            active_txn.remove(&self.version).unwrap_or_default()
        };
        if keys.is_empty() {
            return;
        }
        let mut store = store.lock().unwrap();
        for k in keys {
            let enc = encode_key(&k, self.version);
            store.remove_versioned(&enc);
        }
    }

    /// **部分**撤销：把 `keys` 这几把已写过的 key 从库里撤掉，并从写集里摘掉。
    ///
    /// 批量写（`batch_set` / `batch_delete`）撞墙时用它：那批不能只撤到"出错那一条"为止 ——
    /// 前面已经落下的行也得退回去，然后调用方才 `rollback()` 整个事务重试。
    /// 摘写集是必须的：留着的话随后的 [`Self::rollback`] 会去删一批早已不存在的行（不报错，
    /// 但 `commit` 的回收账也会多算这几条）。
    ///
    /// 锁序与 [`Self::write`] / [`Self::rollback`] 一致（`active_txn → store`）。四家原来各自
    /// 写的是"先存储后活跃表 + `try_lock` 拿不到就跳过摘写集"，那是内核里唯一的反向嵌套点，
    /// 合成一份时顺手改成正向且**必达**。
    pub fn undo<S: WriteSide>(&self, store: &Mutex<S>, keys: &[Vec<u8>]) {
        if keys.is_empty() {
            return;
        }
        let mut active_txn = self.active_txn.lock().unwrap();
        let mut store = store.lock().unwrap();
        for k in keys {
            let enc = encode_key(k, self.version);
            store.remove_versioned(&enc);
        }
        if let Some(writes) = active_txn.get_mut(&self.version) {
            writes.retain(|w| !keys.iter().any(|k| k == w));
        }
    }
}

/// bulk 会话的内核：一个会话一个版本号、无冲突检测、**不登记活跃表**（单写者假定）。
///
/// 与 [`TxnCore`] 的区别不只是"少了检测"：bulk 不落 WAL、不拍快照，所以它也不能回滚 ——
/// 它的正确性前提是"没有别人在写"。
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

    pub fn put<S: WriteSide>(&mut self, store: &Mutex<S>, key: &[u8], value: Vec<u8>) {
        let enc = encode_key(key, self.version);
        store.lock().unwrap().put_versioned(&enc, Some(value));
    }

    pub fn delete<S: WriteSide>(&mut self, store: &Mutex<S>, key: &[u8]) {
        let enc = encode_key(key, self.version);
        store.lock().unwrap().put_versioned(&enc, None);
    }

    /// 一次加锁灌一批（消费 items，零额外 clone）
    pub fn put_batch<S: WriteSide>(&mut self, store: &Mutex<S>, items: Vec<(Vec<u8>, Vec<u8>)>) {
        let mut store = store.lock().unwrap();
        for (key, value) in items {
            let enc = encode_key(&key, self.version);
            store.put_versioned(&enc, Some(value));
        }
    }

    /// 一次加锁灌一批（借用 items）
    pub fn put_batch_ref<S: WriteSide>(&mut self, store: &Mutex<S>, items: &[(Vec<u8>, Vec<u8>)]) {
        let mut store = store.lock().unwrap();
        for (key, value) in items {
            let enc = encode_key(key, self.version);
            store.put_versioned(&enc, Some(value.clone()));
        }
    }

    /// 收尾。**必须**调用（或让 `Drop` 兜底）：版本号已在这次会话占掉，重复 finish 无效。
    ///
    /// 返回 `false` = 已经收尾过了。
    pub fn finish(&mut self) -> bool {
        if self.finished {
            return false;
        }
        self.finished = true;
        // 本会话的 version 在 begin 时就已 fetch_add 占掉，这里只需确认水位已越过它
        debug_assert!(self.next_version.load(Ordering::SeqCst) > self.version);
        true
    }
}
