//! 本文件负责事务写入、冲突、提交与回滚。
//! 模块边界仅组织代码：锁顺序、版本可见性、磁盘格式和发布失败的处理均沿用原契约。

use super::*;

/// 未终结（既没 commit 也没 rollback）就被丢弃的事务在此自动回滚。
///
/// 漏网事务会永久留在 `active_txn` 里：`xmin = min(活跃事务 id)` 被一个已死的 id 压住
/// ⇒ 版本回收永久停摆，且之后每次 `begin` 的可见性快照随漏网数线性变大。
/// 判据用「活跃表里是否还有自己」——`commit`/`rollback` 都以 `remove` 收尾，成员关系即充要条件。
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
        kv: Arc<Mutex<BPlusTree>>,
        blob: Arc<Mutex<BlobStore>>,
        wal: Arc<Mutex<Wal>>,
        active_txn: Arc<Mutex<Registry>>,
        next_version: Arc<AtomicU64>,
        wal_limit: Arc<AtomicU64>,
        vac: Arc<VacuumScheduler>,
    ) -> Self {
        // begin 不进 WAL：此刻还不知道会不会写；第一次 write() 才补 `Begin`
        // （只读事务因此一个字节都不落，详见 [`crate::common::WalPolicy`]）
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
        kv.ensure_healthy()
            .map_err(|_| crate::WriteError::StorageWriteFailed)?;
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

    pub fn commit_with_options(&self, flush_pages: bool) {
        self.try_commit_with_options(flush_pages)
            .expect("transaction commit failed");
    }
    /// 提交先完成 WAL 提交记录与同步，再注销活动事务并累计回收债务。
    /// 可选数据刷盘和阈值 checkpoint 在提交之后执行；此时维护失败返回“已提交但维护失败”，不能当作事务已回滚。

    pub fn try_commit_with_options(&self, flush_pages: bool) -> Result<(), crate::CommitError> {
        // WAL 收尾成功后才销账与回收记账，统一走 [`TxnCore::try_commit`]。
        // 收尾只交 wal 守卫：fsync 期间不许握着 kv/blob —— `flush_all` 与 checkpoint
        // 是放掉锁之后再做的（它们各自要再拿一次 kv 锁）。
        {
            #[cfg(test)]
            let _probe = crate::common::maintenance_probe::Timer::start(
                crate::common::maintenance_probe::Stage::Commit,
            );
            let mut reg = self.core.lock_registry();
            let mut wal = self.wal.lock().unwrap();
            if self.kv.lock().unwrap().ensure_healthy().is_err() {
                self.core.mark_storage_failed();
            }
            self.core
                .try_commit(&mut reg, &mut WalOnly { wal: &mut wal })?;
        }
        if flush_pages {
            self.kv.lock().unwrap().try_flush_all().map_err(|source| {
                crate::CommitError::CommittedMaintenance {
                    version: self.core.version,
                    source,
                }
            })?;
        }

        // WAL 超阈值就顺带 checkpoint。放在提交之后是因为此刻本事务已持久、
        // 也已从活跃表摘除，checkpoint 记到的 next_version 是干净的。
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
                    run_checkpoint(&self.kv, &self.wal, &self.next_version).map_err(|source| {
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
        // 恢复前保留活动版本的不可见状态及其完整 WAL。
        if kv.ensure_healthy().is_err() {
            return;
        }
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
        if kv.ensure_healthy().is_err() {
            return;
        }
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

/// 写侧：四家里唯一用满 [`WriteSide`] 槽的一家 —— 它既要 blob 旁路（`encode_value`），
/// 又要定长页的容量判定（`row_fits`）。
///
/// 容量拒收同时经 checked API 返回 WriteError::RowTooLarge。
impl WriteSide for TxnSide<'_, '_, '_> {
    fn latest_version(&mut self, key: &[u8]) -> Option<u64> {
        Transaction::latest_version_of(self.kv, key)
    }

    fn put_versioned(&mut self, enc: &[u8], value: Option<Vec<u8>>) -> bool {
        // `row_fits` 已经判过装得下，`insert` 仍可能因为别的形状失败 —— 那条 bool 就是它的答复，
        // 失败时内核阻止提交，并保留写集以清理可能的部分修改。
        self.kv.insert(enc.to_vec(), RowId(value))
    }

    fn remove_versioned(&mut self, enc: &[u8]) {
        let _ = self.kv.delete(enc.to_vec());
    }

    fn try_remove_versioned(&mut self, enc: &[u8]) -> Result<(), crate::WriteError> {
        self.kv
            .ensure_healthy()
            .map_err(|_| crate::WriteError::StorageWriteFailed)?;
        self.kv.delete(enc.to_vec());
        self.kv
            .ensure_healthy()
            .map_err(|_| crate::WriteError::StorageWriteFailed)
    }

    fn encode_value(&mut self, value: Vec<u8>) -> Option<Vec<u8>> {
        // 逻辑 value → 叶子存储编码（大 value 写 blob；事务路径每条都 fsync）
        match encode_stored_value(self.blob, value, true) {
            Ok(s) => Some(s),
            Err(e) => {
                eprintln!("blob encode 失败: {e}");
                None
            }
        }
    }

    fn row_fits(&mut self, enc: &[u8], stored: Option<&[u8]>) -> bool {
        // 先问树装不装得下，再动 WAL。树的"这一行比一页还大"必须在这里就变成 `false`：
        // 否则 WAL 里留下一条"提交了但树里没有"的记录，恢复时只会再拒一次。
        let owned = stored.map(|v| v.to_vec());
        if self.kv.row_fits(enc, &owned) {
            return true;
        }
        log::error!(
            "bplus_tree 拒收一行：键 {} 字节 + 值 {} 字节，单行已超过一页载荷 {} 字节 ⇒ 键必须短于一页",
            enc.len(),
            owned.as_ref().map_or(0, |v| v.len()),
            crate::bplus_tree::page::MAX_PAYLOAD
        );
        false
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
