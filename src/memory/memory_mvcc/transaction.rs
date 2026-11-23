//! 本文件负责事务写入、冲突、提交与回滚。
//! 模块边界仅组织代码：锁顺序、版本可见性、磁盘格式和发布失败的处理均沿用原契约。

use super::*;

/// 写侧：内存引擎的"这一行的字节住在哪"= 同一张 `BTreeMap`，所以三个方法都是薄壳。
///
/// [`WriteSide::latest_version`] 借用链尾键并复用查询边界；仅能证明越过表尾时省掉 seek。
/// 区间分支仍从链尾早停，借用候选键；可见性与写冲突继续由共享事务内核决定。
impl WriteSide for MemStore {
    fn latest_version(&mut self, key: &[u8]) -> Option<u64> {
        #[cfg(test)]
        if replay_cost_reference::enabled() {
            return Transaction::find_version_back(self, key, |_| true)
                .and_then(|enc| decode_key(&enc).map(|vk| vk.version));
        }
        use crate::common::version::decode_key_ref;
        use crate::memory::mem_store::KeyLookup;
        match self.find_prefixed_key_back(key, &[0; 8], &[255; 8], |enc| {
            decode_key_ref(enc).and_then(|(raw, version)| (raw == key).then_some(version))
        }) {
            KeyLookup::PastEnd => None,
            KeyLookup::Searched(version) => version,
        }
    }

    fn put_versioned(&mut self, enc: &[u8], value: Option<Vec<u8>>) -> bool {
        self.insert(enc.to_vec(), value);
        // 内存引擎的插入不会失败（没有磁盘通道），所以恒真
        true
    }

    fn put_versioned_owned(&mut self, enc: Vec<u8>, value: Option<Vec<u8>>) -> bool {
        self.insert(enc, value);
        true
    }

    fn remove_versioned(&mut self, enc: &[u8]) {
        self.delete(enc);
    }
}

/// 内存引擎没有 WAL：收尾两条都用默认实现（空 impl 就是"什么都不做"的声明）。
impl crate::common::WalTail for MemStore {}

/// 未终结（既没 commit 也没 rollback）就被丢弃的事务在此自动回滚。
///
/// 漏网事务会永久留在 `active_txn` 里：`xmin = min(活跃事务 id)` 被一个已死的 id 压住
/// ⇒ 版本回收永久停摆，且之后每次 `begin` 的可见性快照随漏网数线性变大。
/// 判据用「活跃表里是否还有自己」——`commit`/`rollback` 都以 `remove` 收尾，成员关系即充要条件。
/// 本引擎无 WAL，所以没有磁盘侧的 Abort 记录，只撤销内存行。
impl Drop for Transaction {
    fn drop(&mut self) {
        // 判据与"只查不删"的理由见 [`crate::common::is_unfinished`]：
        // 删除必须留给下面的 rollback()，它要按表里那份 key 列表把本事务写过的版本行清掉
        if !self.core.unfinished() {
            return;
        }
        log::warn!(
            "事务 {} 未 commit/rollback 即被丢弃，已自动回滚",
            self.core.version
        );
        self.rollback();
    }
}

impl Transaction {
    pub fn begin(
        store: Arc<Mutex<MemStore>>,
        active_txn: Arc<Mutex<Registry>>,
        next_version: Arc<AtomicU64>,
        vac: Arc<VacuumScheduler>,
    ) -> Self {
        Transaction {
            store,
            core: TxnCore::begin(active_txn, &next_version, vac),
        }
    }

    pub fn version(&self) -> u64 {
        self.core.version
    }

    pub fn set(&self, key: &[u8], value: Vec<u8>) -> bool {
        self.write(key, Some(value))
    }

    /// 原样存这串字节 —— 引擎替你打信封，`get` 会一字不差地还回来。
    ///
    /// 与 [`Self::set`] 的分工：`set` 把传进来的字节当作**已经打过信封**的逻辑值（现有调用点
    /// 按这个约定写，改它会破坏它们），所以拿 `set` 直接存裸字节会踩坑：值以 `0x00` 打头
    /// 读回来少 1 字节，以 `0x01` 打头会被当成 TTL 信封少 9 字节、甚至被判"已过期"读成 `None`。
    /// 存二进制/序列化字节（protobuf、avro、压缩块）走这个口子，不必再记信封规矩。
    ///
    /// 空值、`0x00`/`0x01` 打头、超过 blob 阈值走旁路的长值，都由每条用例里的
    /// `test_set_raw_round_trips_every_byte_pattern` 逐字节钉住；同一条里还对照断言
    /// "`set` 存同样的字节确实会少" —— 不对照就证明不了这个口子在做事。
    ///
    /// 读侧语义不变：`get` / `scan` / `multi_get` 一律照旧剥信封（剥掉的正是这里加的那一层）。
    /// 要"原样字节 + 过期时间"用 `set_with_ttl`（它内部同样已打信封）。
    pub fn set_raw(&self, key: &[u8], value: Vec<u8>) -> bool {
        self.set(key, crate::common::pack_plain(&value))
    }

    pub fn delete(&self, key: &[u8]) -> bool {
        self.write(key, None)
    }

    pub fn try_set(&self, key: &[u8], value: Vec<u8>) -> Result<(), crate::WriteError> {
        self.try_write(key, Some(value))
    }

    pub fn try_set_raw(&self, key: &[u8], value: Vec<u8>) -> Result<(), crate::WriteError> {
        self.try_set(key, crate::common::pack_plain(&value))
    }

    pub fn try_delete(&self, key: &[u8]) -> Result<(), crate::WriteError> {
        self.try_write(key, None)
    }

    pub(super) fn write(&self, key: &[u8], value: Option<Vec<u8>>) -> bool {
        self.try_write(key, value).is_ok()
    }
    /// 先检查事务状态、写冲突与存储容量，再记录 WAL 并修改版本化存储。
    /// 拒绝写入时保留原值和注册表状态；所有权优化必须遵守同一检查顺序。

    pub(super) fn try_write(
        &self,
        key: &[u8],
        value: Option<Vec<u8>>,
    ) -> Result<(), crate::WriteError> {
        #[cfg(test)]
        replay_reference::note_row_lock();
        // 锁序：先活跃表，再存储侧（理由见 [`crate::common::TxnCore`] 的模块头）。
        // 那 8 步序列（冲突判定 → 编码 → WAL → 落行 → 写集登记）在 [`TxnCore::write`] 里，四引擎一份。
        let mut reg = self.core.lock_registry();
        let mut side = self.store.lock().unwrap();
        self.core.try_write(&mut reg, &mut *side, key, value)
    }

    pub fn commit(&self) {
        self.try_commit().expect("transaction commit failed");
    }

    pub fn try_commit(&self) -> Result<(), crate::CommitError> {
        let mut reg = self.core.lock_registry();
        self.core.try_commit(&mut reg, &mut ())
    }
    /// 撤销本事务写入并清理活动注册。
    /// 内存引擎直接清理本事务版本，提交不具有磁盘持久性。

    pub fn rollback(&self) {
        let mut reg = self.core.lock_registry();
        let mut side = self.store.lock().unwrap();
        self.core.rollback(&mut reg, &mut *side);
    }

    /// 部分撤销：批量写撞墙时把已落下的那几把 key 退回（连同写集一起摘掉）。
    pub(crate) fn undo(&self, keys: &[Vec<u8>]) {
        let mut reg = self.core.lock_registry();
        let mut side = self.store.lock().unwrap();
        self.core.undo(&mut reg, &mut *side, keys);
    }
}
