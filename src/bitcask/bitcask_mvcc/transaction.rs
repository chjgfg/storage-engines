//! 本文件负责事务写入、冲突、提交与回滚。
//! 模块边界仅组织代码：锁顺序、版本可见性、磁盘格式和发布失败的处理均沿用原契约。

use super::*;

/// 未终结事务的兜底回滚。
///
/// 「未终结」= 既没 `commit` 也没 `rollback` 就被丢弃（提前 `return`、panic、或纯粹忘了）。
/// 这不是便利功能，是止血：漏网的事务会**永久留在 `active_txn`**，于是
/// 1. `xmin = min(活跃事务 id)` 被一个已经死掉的 id 压住 ⇒ **版本回收从此永久停摆**；
/// 2. 之后每次 `begin` 的可见性快照 `keys().cloned().collect()` 随漏网数线性变大。
///
/// 仅 Active 状态自动回滚；Indeterminate 保留活跃表记录，必须重开恢复。
impl Drop for Transaction {
    fn drop(&mut self) {
        // 判据与"只查不删"的理由见 [`crate::common::is_unfinished`]：
        // 删除必须留给下面的 rollback()，它要按表里那份 key 列表把本事务写过的版本行清掉
        if !self.core.unfinished() {
            return;
        }
        if self.wal.lock().is_err() {
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
        kv: Arc<Mutex<Bitcask>>,
        blob: Arc<Mutex<BlobStore>>,
        wal: Arc<Mutex<Wal>>,
        active_txn: Arc<Mutex<Registry>>,
        next_version: Arc<AtomicU64>,
        wal_limit: Arc<AtomicU64>,
        vac: Arc<VacuumScheduler>,
    ) -> Self {
        // begin 不进 WAL：此刻还不知道这个事务会不会写。第一次 write() 才补 `Begin`
        // （只读事务因此一个字节都不落 —— 详见 [`crate::common::WalPolicy`]）
        let core = TxnCore::begin(active_txn, &next_version, vac);
        Transaction {
            kv,
            blob,
            wal,
            next_version,
            wal_limit,
            core,
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
        // 锁序：活跃表 → wal → blob → kv；那八步序列在 [`TxnCore::write`]（四引擎共用的一份）
        let mut reg = self.core.lock_registry();
        let mut wal = self.wal.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();
        let mut kv = self.kv.lock().unwrap();
        self.core.try_write(
            &mut reg,
            &mut TxnSide {
                wal: &mut wal,
                blob: &mut blob,
                kv: &mut kv,
            },
            key,
            value,
        )
    }

    pub fn commit(&self) {
        self.try_commit().expect("transaction commit failed");
    }

    pub fn try_commit(&self) -> Result<(), crate::CommitError> {
        self.try_commit_with_options(false)
    }

    pub fn commit_with_options(&self, flush_data: bool) {
        self.try_commit_with_options(flush_data)
            .expect("transaction commit failed");
    }
    /// 提交先完成 WAL 提交记录与同步，再注销活动事务并累计回收债务。
    /// 可选数据刷盘和阈值 checkpoint 在提交之后执行；此时维护失败返回“已提交但维护失败”，不能当作事务已回滚。

    pub fn try_commit_with_options(&self, flush_data: bool) -> Result<(), crate::CommitError> {
        #[cfg(test)]
        let _commit = crate::bitcask::checkpoint_probe::Timer::start(
            crate::bitcask::checkpoint_probe::Stage::Commit,
        );
        // WAL 收尾成功后才销账与回收记账，统一走 [`TxnCore::try_commit`]。
        // 收尾只交 wal 守卫：fsync 期间不许握着 kv/blob 把读者挡在门外。
        {
            let mut reg = self.core.lock_registry();
            let mut wal = self.wal.lock().unwrap();
            self.core
                .try_commit(&mut reg, &mut WalOnly { wal: &mut wal })?;
        }
        if flush_data {
            let mut kv = self.kv.lock().unwrap();
            let mut blob = self.blob.lock().unwrap();
            kv.flush()
                .map_err(|source| crate::CommitError::CommittedMaintenance {
                    version: self.core.version,
                    source,
                })?;
            blob.sync()
                .map_err(|source| crate::CommitError::CommittedMaintenance {
                    version: self.core.version,
                    source,
                })?;
        }

        // WAL 超阈值就顺带 checkpoint。放在提交之后是因为此刻本事务已持久、
        // 活跃表也已摘除，checkpoint 记到的 next_version 是干净的。
        let limit = self.wal_limit.load(Ordering::SeqCst);
        if limit > 0 {
            let size = self.wal.lock().unwrap().size_bytes().map_err(|source| {
                crate::CommitError::CommittedMaintenance {
                    version: self.core.version,
                    source,
                }
            })?;
            if size >= limit {
                let active = self.core.lock_registry();
                if active.is_empty() {
                    run_checkpoint(
                        &self.kv,
                        &self.blob,
                        &self.wal,
                        &self.next_version,
                        crate::bitcask::bitcask::HintPolicy::Amortized,
                    )
                    .map_err(|source| {
                        crate::CommitError::CommittedMaintenance {
                            version: self.core.version,
                            source,
                        }
                    })?;
                }
            }
        }
        Ok(())
    }
    /// 撤销本事务写入并清理活动注册。
    /// 磁盘状态不确定时不能用回滚假装恢复成功，后续读取与恢复依赖失败封锁。

    pub fn rollback(&self) {
        // 销账 + `Abort` + 按写集撤行，都在 [`TxnCore::rollback`]（锁序与 write 一致）
        let mut reg = self.core.lock_registry();
        let mut wal = self.wal.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();
        let mut kv = self.kv.lock().unwrap();
        self.core.rollback(
            &mut reg,
            &mut TxnSide {
                wal: &mut wal,
                blob: &mut blob,
                kv: &mut kv,
            },
        );
    }

    /// 部分撤销：批量写撞墙时把已落下的那几把 key 退回（连同写集一起摘掉）。
    pub(crate) fn undo(&self, keys: &[Vec<u8>]) {
        let mut reg = self.core.lock_registry();
        let mut wal = self.wal.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();
        let mut kv = self.kv.lock().unwrap();
        self.core.undo(
            &mut reg,
            &mut TxnSide {
                wal: &mut wal,
                blob: &mut blob,
                kv: &mut kv,
            },
            keys,
        );
    }
}

/// 写侧：bitcask 的"字节住在哪"= 追加式数据日志（`insert` 会更新 keydir，且**会失败** ——
/// 那条 io::Result 通道原先只 `eprintln!` 一下就被丢掉，于是写集里留着一条库里没有的行；
/// 现在错误交给内核，阻止提交；已写 WAL 的键保留在写集中供回滚清理）。
///
/// 用到 `encode_value` 槽：大 value 走 blob 旁路，而旁路在落 WAL **之前**就可能失败 ——
/// 失败必须在那里判掉，否则 WAL 里留下一条指向不存在 blob 的记录。
/// 没用 `row_fits`：日志没有页容量。
impl WriteSide for TxnSide<'_, '_, '_> {
    fn latest_version(&mut self, key: &[u8]) -> Option<u64> {
        Transaction::latest_version_of(self.kv, key)
    }

    fn put_versioned(&mut self, enc: &[u8], value: Option<Vec<u8>>) -> bool {
        match self.kv.insert(enc.to_vec(), value) {
            Ok(()) => true,
            Err(e) => {
                eprintln!("Bitcask insert 失败: {e}");
                false
            }
        }
    }

    fn remove_versioned(&mut self, enc: &[u8]) {
        let _ = self.kv.remove(enc);
    }

    fn encode_value(&mut self, value: Vec<u8>) -> Option<Vec<u8>> {
        // 事务路径每条都 fsync blob（崩了不能留下"已提交行指着不存在的字节"）
        match encode_stored_value(self.blob, value, true) {
            Ok(s) => Some(s),
            Err(e) => {
                eprintln!("blob encode 失败: {e}");
                None
            }
        }
    }

    fn wal_write(&mut self, xid: u64, enc: &[u8], stored: Option<&[u8]>) -> bool {
        self.wal
            .append(WalRecord::Write {
                xid,
                key: enc.to_vec(),
                value: stored.map(|v| v.to_vec()),
            })
            .is_ok()
    }
}

/// Commit 追加与 fsync 的错误返回内核；成功后才能从活跃表销账。
/// Abort 保留原兼容接口；失败会封锁 WAL，后续提交和截断均被拒绝。
impl WalTail for TxnSide<'_, '_, '_> {
    fn try_wal_commit(&mut self, xid: u64) -> std::io::Result<()> {
        self.wal.commit_transaction(xid)
    }

    fn wal_commit(&mut self, xid: u64) {
        self.try_wal_commit(xid).expect("WAL commit failed");
    }

    fn wal_abort(&mut self, xid: u64) {
        let _ = self.wal.append(WalRecord::Abort { xid });
        let _ = self.wal.sync();
    }
}

impl WalTail for WalOnly<'_> {
    fn try_wal_commit(&mut self, xid: u64) -> std::io::Result<()> {
        self.wal.commit_transaction(xid)
    }

    fn wal_commit(&mut self, xid: u64) {
        self.try_wal_commit(xid).expect("WAL commit failed");
    }
}
