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

/// 写侧：LSM 的"字节住在哪"= memtable（`insert`），而 WAL 必须在改库之前落。
///
/// 没用到的两个槽走默认实现（`encode_value`：大 value 的 blob 旁路在 [`LSMTree::insert`] 内部
/// 做完，不像 bitcask/bplus 那样是事务层显式的一步；`row_fits`：memtable 没有页容量上限）。
///
/// ⚠️ `latest_version` 走链尾反向早停（[`Transaction::find_version_back`]），与点读同一条路径 ——
/// 判冲突如果整链扫，热点 key 的每一次写都要陪跑版本链。
impl WriteSide for TxnSide<'_, '_> {
    fn latest_version(&mut self, key: &[u8]) -> Option<u64> {
        #[cfg(test)]
        if crate::lsm_tree::sst::point_read_reference::enabled() {
            return Transaction::find_version_back(self.kv, key, |_| true)
                .and_then(|(enc, _)| decode_key(&enc).map(|vk| vk.version));
        }
        let low = encode_key(key, 0);
        let high = encode_key(key, u64::MAX);
        self.kv
            .find_key_back(&low, &high, u64::MAX, |enc| {
                crate::common::version::decode_key_ref(enc).is_some_and(|(raw, _)| raw == key)
            })
            .into_key()
            .and_then(|enc| {
                crate::common::version::decode_key_ref(&enc).map(|(_, version)| version)
            })
    }

    fn put_versioned(&mut self, enc: &[u8], value: Option<Vec<u8>>) -> bool {
        self.kv.insert(enc.to_vec(), value);
        // memtable 插入没有失败通道（落盘在 flush/compact 那边，失败另有账）
        true
    }

    fn remove_versioned(&mut self, enc: &[u8]) {
        let _ = self.kv.remove(enc);
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
impl WalTail for TxnSide<'_, '_> {
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

impl Transaction {
    pub fn begin(
        kv: Arc<Mutex<LSMTree>>,
        wal: Arc<Mutex<Wal>>,
        active_txn: Arc<Mutex<Registry>>,
        next_version: Arc<AtomicU64>,
        wal_limit: Arc<AtomicU64>,
        vac: Arc<VacuumScheduler>,
        db_dir: Arc<PathBuf>,
    ) -> Self {
        // begin 不进 WAL：此刻还不知道会不会写；第一次 write() 才补 `Begin`
        // （只读事务因此一个字节都不落，详见 [`crate::common::WalPolicy`]）
        let core = TxnCore::begin(active_txn, &next_version, vac);
        Transaction {
            kv,
            wal,
            next_version,
            wal_limit,
            db_dir,
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
        // 锁序：活跃表 → wal → kv（内核要的就是这两把守卫，序列它在 [`TxnCore::write`] 里）
        let mut reg = self.core.lock_registry();
        let mut wal = self.wal.lock().unwrap();
        let mut kv = self.kv.lock().unwrap();
        self.core.try_write(
            &mut reg,
            &mut TxnSide {
                wal: &mut wal,
                kv: &mut kv,
            },
            key,
            value,
        )
    }

    /// 提交事务：WAL Commit + **fsync**。默认不 flush MemTable/SST。
    ///
    /// 每事务一次 fsync → 吞吐受盘限制（数千 ops/s 量级）。
    /// 大批量导入请用 [`MVCC::begin_bulk`]。需要把 mem 落成 SST 时用
    /// [`Self::commit_with_options`]`(true)` 或事后 [`MVCC::flush`]。
    pub fn commit(&self) {
        self.try_commit().expect("transaction commit failed");
    }

    /// `flush_pages`：commit 后是否 `LSMTree::flush`（mem→L0 + 可能 compact）。
    ///
    /// 频繁 `flush_pages=true` 会显著变慢；批量提交后再 `mvcc.flush()` 更合适。
    /// L0 堆积时可用 `kv.compact_l0_until(...)` 或后台 compact 收口。
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
        // 收尾只交 wal 守卫：fsync 期间不许把 kv 锁着 —— 那会把读者全挡在门外。
        {
            let mut reg = self.core.lock_registry();
            let mut wal = self.wal.lock().unwrap();
            self.core
                .try_commit(&mut reg, &mut WalOnly { wal: &mut wal })?;
        }
        if flush_pages {
            self.kv.lock().unwrap().flush().map_err(|source| {
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
                    run_checkpoint(&self.kv, &self.wal, &self.next_version, &self.db_dir).map_err(
                        |source| crate::CommitError::CommittedMaintenance {
                            version: self.core.version,
                            source,
                        },
                    )?;
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
        let mut kv = self.kv.lock().unwrap();
        self.core.rollback(
            &mut reg,
            &mut TxnSide {
                wal: &mut wal,
                kv: &mut kv,
            },
        );
    }
}
