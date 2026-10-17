//! MVCC 事务层（纯内存版）
//!
//! 对标 bplus-tree 的快照隔离语义，但**没有** WAL / 页刷盘 / 崩溃恢复：
//! - begin：分配 xid，拍下当时活跃事务快照
//! - 写：版本键 `raw_key || version_be`，写冲突检测
//! - 读：可见性过滤后取最新版本
//! - commit / rollback：维护活跃表，回滚物理删本事务 version 行
//!
//! 存储落到 [`crate::mem_store::MemStore`]。

use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

pub use crate::common::{
    DurabilityProfile, EngineTuning, ExportRecord, MvccHealth, VacuumRound, VacuumStats,
};
use crate::common::{HealthFold, VacuumScheduler};
use crate::common::vacuum_plan;
use crate::common::version_key_upper_bound;
use crate::common::txn::{BulkCore, TxnCore, WriteSide};
use crate::memory::mem_store::MemStore;

// 测试专用：本线程在「每个逻辑 key 的胜出行」那次折叠里**走了多少个版本键**。
//
// 内存引擎既没有磁盘读也没有 WAL，所以它的代价单位就是"比了多少把键"：
// 窗口扫必须只走窗口内的键，而不是折完全表再由调用方筛界。
#[cfg(test)]
thread_local! {
    static FOLD_KEYS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// 当前线程折叠时检查过的版本键数（测试探针用）
#[cfg(test)]
pub(crate) fn fold_keys() -> u64 {
    FOLD_KEYS.with(|c| c.get())
}

/// MVCC 引擎（单进程内存）
pub struct MVCC {
    store: Arc<Mutex<MemStore>>,
    active_txn: Arc<Mutex<HashMap<u64, Vec<Vec<u8>>>>>,
    next_version: Arc<AtomicU64>,
    /// 自动回收：切片宽度 / 记账 / 游标（策略与实现在 [`crate::common::VacuumScheduler`]）
    vac: Arc<VacuumScheduler>,
}

/// 普通 MVCC 事务
///
/// 事务语义那部分（版本、活跃快照、写集、回收记账、冲突判定、回滚）在
/// [`TxnCore`]（[`crate::common::txn`]，四引擎共用）；这里只留"这一家的字节住在哪"。
pub struct Transaction {
    pub(crate) store: Arc<Mutex<MemStore>>,
    pub(crate) core: TxnCore,
}

/// 批量导入：无冲突检测、不登记活跃表（单写者假定）。会话号与收尾顺序在 [`BulkCore`]。
pub struct BulkLoader {
    store: Arc<Mutex<MemStore>>,
    core: BulkCore,
}

/// 逻辑键 + 版本（调试用）与版本键编解码
///
/// 定义在 [`crate::common::version`]（四引擎共用）；这里 re-export 是为了
/// `crate::memory::encode_key` 这个既有路径不中断。
pub use crate::common::{decode_key, encode_key, VersionedKey};

impl MVCC {
    /// 新建空库
    pub fn new() -> Self {
        Self {
            store: Arc::new(Mutex::new(MemStore::new())),
            active_txn: Arc::new(Mutex::new(HashMap::new())),
            next_version: Arc::new(AtomicU64::new(1)),
            vac: Arc::new(VacuumScheduler::new(MVCC::VACUUM_AUTO_SLICE_DEFAULT)),
        }
    }

    pub fn begin_transaction(&self) -> Transaction {
        self.tick_auto_vacuum();
        Transaction::begin(
            self.store.clone(),
            self.active_txn.clone(),
            self.next_version.clone(),
            self.vac.clone(),
        )
    }

    /// 自动回收的一小步：攒够一个切片宽度的新版本，就裁同样宽度的**一片**。
    ///
    /// 挂在 `begin_transaction` 而不是 commit 上：这一处还没有本线程的活跃事务，
    /// `vacuum_bounded` 的「有活跃事务就整轮跳过」不会被自己的调用方踩中；而且每个
    /// 事务至少 begin 一次，攒账的节奏和写路径一样。
    ///
    /// 触发量 = 切片宽度 ⇒ 回收速率与产生垃圾的速率 1:1，这是「跟得上」的下界。
    fn tick_auto_vacuum(&self) {
        let Some(slice) = self.vac.take_tick() else {
            return;
        };
        let after = self.vac.cursor();
        let round = self.vacuum_bounded(after.as_deref(), slice);
        self.vac.set_cursor(round.next_cursor);
    }

    /// 自动回收的切片宽度（行数）：每攒够这么多行新版本，就裁一片这么多行。
    ///
    /// 默认 [`MVCC::VACUUM_AUTO_SLICE_DEFAULT`]；**设 0 关闭自动回收**。内存引擎没有磁盘
    /// 空间问题，但版本链一样会让点读点写陪跑整条链 —— 涨的慢不等于不涨。
    pub fn set_vacuum_auto_slice(&self, rows: u64) {
        self.vac.set_slice(rows);
    }

    pub fn vacuum_auto_slice(&self) -> u64 {
        self.vac.slice()
    }

    /// 本引擎能被调的东西 —— 清单在这里，不在门面。
    ///
    /// 只有一条：`wal_checkpoint_bytes` 对内存引擎没有意义（连字段都不存在，见
    /// [`MVCC::durability_profile`] 的"没有任何持久化"），所以共用清单里它对的那一半不列。
    pub fn tuning(&self) -> EngineTuning {
        let mut t = EngineTuning::default();
        t.push_shared(self.vacuum_auto_slice(), None);
        t
    }

    /// 默认切片宽度 = 16 Ki 行（四引擎同一个默认值，方便跨引擎对比观测）。
    pub const VACUUM_AUTO_SLICE_DEFAULT: u64 = 16 * 1024;

    /// bulk load：单写者、无冲突检测。结束必须 `finish()`。
    pub fn begin_bulk(&self) -> BulkLoader {
        BulkLoader {
            store: self.store.clone(),
            core: BulkCore::begin(self.next_version.clone()),
        }
    }

    /// 导出当前快照下每个逻辑 key 的最新可见版本
    pub fn export_latest_visible(&self, include_deleted: bool) -> Vec<ExportRecord> {
        let tx = self.begin_transaction();
        let records = tx.export_latest_visible(include_deleted);
        tx.commit();
        records
    }

    /// 崩溃安全画像：**全空，这是设计而非疏漏** —— 纯内存引擎没有任何持久化，
    /// 进程一exit 数据就没了。它存在的意义是让四家能用同一套断言比对语义
    /// （门面层 `open_engine("memory")` 也就能被测试/演示用）。
    pub fn durability_profile(&self) -> DurabilityProfile {
        DurabilityProfile {
            engine: "memory",
            transaction_wal: false,
            fsync_on_commit: false,
            data_checksum: false,
            blob_checksum: false,
            in_place_page_writes: false,
            torn_page_guard: false,
            merge_artifact_recovery: false,
        }
    }

    /// 全库健康度快照。**只按引用遍历键**（`scan_entries_with`），一趟、不克隆 value。
    ///
    /// 代价是 O(物理行数)，属管理面调用：别放进热路径，长跑里按分钟级采样就够。
    /// 口径与 [`crate::bitcask::MVCC::health`] 完全一致。
    pub fn health(&self) -> MvccHealth {
        // 一次只持一把锁：先读完 `active_txn` 计数就放，再拿 `store`。
        // 嵌套持有会和 [`MVCC::vacuum`]（`active_txn → store`）构成反向嵌套。
        let active_txns = self.active_txn.lock().unwrap().len() as u64;
        let store = self.store.lock().unwrap();
        // 分组折叠用四引擎共用的 [`HealthFold`]：按 enc 升序 ⇒ 同 key 相邻成块 ⇒ 一趟数得出链长
        let mut fold = HealthFold::default();
        // BTreeMap 按 enc 升序 ⇒ 同一逻辑 key 的版本块相邻
        store.scan_entries_with(|enc, _val| {
            let Some(vk) = decode_key(enc) else {
                return;
            };
            fold.push(&vk.raw_key);
        });
        fold.finish(active_txns)
    }

    /// 版本回收：一次扫完全库。
    ///
    /// 就是 `vacuum_bounded(None, u64::MAX)`；保留规则、「有活跃事务就整轮跳过」的理由和
    /// 游标语义都在 [`MVCC::vacuum_bounded`] 那边。
    pub fn vacuum(&self) -> VacuumStats {
        self.vacuum_bounded(None, u64::MAX).stats
    }

    /// 有界版本回收：从逻辑 key `after`（**不含**）之后开始扫，最多读 `budget_rows` 行。
    ///
    /// 规则与 [`MVCC::vacuum`] 一致：每个逻辑 key 只保留「最后一个 version < xmin」+
    /// 全部 version >= xmin；若 xmin 之前最终是 tombstone 且无更新版本 → 整 key 删除。
    /// **有任何活跃事务 ⇒ 整轮跳过**（`skipped_active_txn = true`），此时游标原样退回，
    /// 下一轮从同一处接着扫。理由见 [`crate::bitcask::MVCC::vacuum_bounded`]：旧的
    /// `min(活跃事务 id)` 会裁掉活跃事务快照仍需要的版本 ⇒ 静默读到旧值。
    ///
    /// 截断只发生在逻辑 key 边界（半条链不能收：判「留哪一版」要求整组版本都在手上），
    /// 所以一行的预算偏差 = 那一条链的长度。本引擎无 WAL/blob，也就没有 blob 回收和磁盘压缩。
    pub fn vacuum_bounded(&self, after: Option<&[u8]>, budget_rows: u64) -> VacuumRound {
        let _active_guard = self.active_txn.lock().unwrap();
        if !_active_guard.is_empty() {
            return VacuumRound {
                stats: VacuumStats {
                    xmin: 0,
                    versions_removed: 0,
                    blob_rewritten: None,
                    skipped_active_txn: true,
                    rows_scanned: 0,
                },
                next_cursor: after.map(|k| k.to_vec()),
            };
        }
        let xmin = self.next_version.load(Ordering::SeqCst);

        let store = self.store.lock().unwrap();

        // 分组按引用走表：判「留哪一版」只需要版本号 + 是不是墓碑，
        // 不需要把每行 value 克隆出来（旧实现是 `store.iter().collect()`）。
        let budget_rows = budget_rows.max(1);
        let mut by_key: BTreeMap<Vec<u8>, Vec<(u64, Vec<u8>, bool, ())>> = BTreeMap::new();
        let mut scanned = 0u64;
        let mut cur_raw: Option<Vec<u8>> = None;
        let mut truncated = false;
        store.scan_entries_from(after, |enc, val| {
            let Some(vk) = decode_key(enc) else {
                return false;
            };
            if after.is_some_and(|a| vk.raw_key.as_slice() <= a) {
                return false;
            }
            let at_group_start = cur_raw.as_deref() != Some(vk.raw_key.as_slice());
            if at_group_start && scanned >= budget_rows {
                // 破在这里时 `cur_raw` 那一组已经收齐（刚看到它的后继）⇒ 游标就是它
                truncated = true;
                return true;
            }
            if at_group_start {
                cur_raw = Some(vk.raw_key.clone());
            }
            scanned += 1;
            by_key
                .entry(vk.raw_key.clone())
                .or_default()
                .push((vk.version, enc.to_vec(), val.is_none(), ()));
            false
        });
        let next_cursor = if truncated { cur_raw } else { None };
        drop(store);

        // 「留哪一版」是四引擎共用的判定，实现在 [`crate::common::vacuum_plan`]
        let (to_delete, _kept) = vacuum_plan(by_key, xmin);
        let removed = to_delete.len();

        let mut store = self.store.lock().unwrap();
        for enc in &to_delete {
            let _ = store.delete(enc);
        }

        VacuumRound {
            stats: VacuumStats {
                xmin,
                versions_removed: removed,
                blob_rewritten: None,
                skipped_active_txn: false,
                rows_scanned: scanned,
            },
            next_cursor,
        }
    }

    /// 底层行数（含历史版本 / tombstone）
    pub fn raw_len(&self) -> usize {
        self.store.lock().unwrap().len()
    }

    pub fn next_version(&self) -> u64 {
        self.next_version.load(Ordering::SeqCst)
    }
}

impl Default for MVCC {
    fn default() -> Self {
        Self::new()
    }
}

impl BulkLoader {
    pub fn put(&mut self, key: &[u8], value: Vec<u8>) {
        let mut side = self.store.lock().unwrap();
        self.core.put(&mut *side, key, value);
    }

    pub fn put_batch(&mut self, items: &[(Vec<u8>, Vec<u8>)]) {
        let mut side = self.store.lock().unwrap();
        self.core.put_batch_ref(&mut *side, items);
    }

    pub fn put_batch_owned(&mut self, items: Vec<(Vec<u8>, Vec<u8>)>) {
        let mut side = self.store.lock().unwrap();
        self.core.put_batch(&mut *side, items);
    }

    pub fn delete(&mut self, key: &[u8]) {
        let mut side = self.store.lock().unwrap();
        self.core.delete(&mut *side, key);
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let store = self.store.lock().unwrap();
        let low = encode_key(key, 0);
        let high = encode_key(key, u64::MAX);
        let mut best: Option<Vec<u8>> = None;
        for (enc, row) in store.range_scan(&low, &high) {
            if let Some(vk) = decode_key(&enc) {
                if vk.raw_key.as_slice() == key && vk.version <= self.core.version {
                    best = match row {
                        None => None,
                        Some(raw) => crate::memory::kv_ops::logical_to_user(raw),
                    };
                }
            }
        }
        best
    }

    pub fn finish(mut self) {
        self.core.first_finish();
    }
}

impl Drop for BulkLoader {
    fn drop(&mut self) {
        // 忘了 `finish()` 也得把这次会话的版本号占掉（bulk 不写 WAL、不回滚，见 [`BulkCore`]）。
        // 内存引擎收尾没有别的事要做，所以这里只剩幂等守卫本身。
        self.core.first_finish();
    }
}

/// 写侧：内存引擎的"这一行的字节住在哪"= 同一张 `BTreeMap`，所以三个方法都是薄壳。
///
/// ⚠️ [`WriteSide::latest_version`] 走的是链尾反向早停（[`Transaction::find_version_back`]），
/// 不是整链扫 —— 与点读同一条路径，所以"点写陪跑整条版本链"这个洞只会在一处出现。
impl WriteSide for MemStore {
    fn latest_version(&mut self, key: &[u8]) -> Option<u64> {
        Transaction::find_version_back(self, key, |_| true)
            .and_then(|enc| decode_key(&enc).map(|vk| vk.version))
    }

    fn put_versioned(&mut self, enc: &[u8], value: Option<Vec<u8>>) -> bool {
        self.insert(enc.to_vec(), value);
        // 内存引擎的插入不会失败（没有磁盘通道），所以恒真
        true
    }

    fn remove_versioned(&mut self, enc: &[u8]) {
        self.delete(enc);
    }
}

/// 内存引擎没有 WAL：收尾两条都用默认实现（空 impl 就是"什么都不做"的声明）。
impl crate::common::WalTail for MemStore {}

/// 链尾反向查找的首轮预算，不够再翻倍重试。
///
/// 常态下一个逻辑 key 只有个位数版本，32 足够一次命中；翻倍只在
/// 「整条链没有一个满足条件」的退化场景才付到 O(链长)，与旧的整链扫描同阶。
const VERSION_BACK_STEP: usize = 32;

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
        log::warn!("事务 {} 未 commit/rollback 即被丢弃，已自动回滚", self.core.version);
        self.rollback();
    }
}

impl Transaction {
    pub fn begin(
        store: Arc<Mutex<MemStore>>,
        active_txn: Arc<Mutex<HashMap<u64, Vec<Vec<u8>>>>>,
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

    fn write(&self, key: &[u8], value: Option<Vec<u8>>) -> bool {
        // 锁序：先活跃表，再存储侧（理由见 [`crate::common::TxnCore`] 的模块头）。
        // 那 8 步序列（冲突判定 → 编码 → WAL → 落行 → 写集登记）在 [`TxnCore::write`] 里，四引擎一份。
        let mut reg = self.core.lock_registry();
        let mut side = self.store.lock().unwrap();
        self.core.write(&mut reg, &mut *side, key, value)
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        // 转发给 latest_visible_raw：以前这里复制过一份同样的循环，
        // 于是「改了事务层却漏了点读」成为必然 —— 结构与 bitcask 保持一致。
        self.latest_visible_raw(key)
            .and_then(|(_, v)| v)
            .and_then(crate::memory::kv_ops::logical_to_user)
    }

    /// 最新可见逻辑行（含 TTL 信封；tombstone 为 None）
    pub(crate) fn latest_visible_raw(&self, key: &[u8]) -> Option<(u64, Option<Vec<u8>>)> {
        let store = self.store.lock().unwrap();
        // 从链尾往回走，第一个可见版本就是答案
        let enc = Self::find_version_back(&store, key, |v| self.is_visible(v))?;
        let version = decode_key(&enc)?.version;
        Some((version, store.get(&enc).cloned()?))
    }

    /// 全部最新可见逻辑行（key 有序）= 无界版的 [`Self::collect_latest_raw_in`]。
    pub(crate) fn collect_latest_raw(
        &self,
        include_deleted: bool,
    ) -> Vec<(Vec<u8>, u64, Option<Vec<u8>>)> {
        self.collect_latest_raw_in(include_deleted, None, None)
    }

    /// `[start, end)` 内每个逻辑 key 的最新可见版本 —— 界**下推到 BTreeMap 的 range** 上。
    ///
    /// 旧形状是折完全库再由调用方逐条筛界 ⇒ 窗口再窄也要走完全表。界判在**版本键**上，
    /// 所以 raw_key 与界之间的前缀交错的边角仍需逐条比：起点用 `continue`，终点先按
    /// `enc ≥ encode(end,0)` 把 range 收到上界（有序迭代下后面只会更大，可以真停），
    /// 落在界内但 raw ≥ end 的个别键再 `continue`。
    pub(crate) fn collect_latest_raw_in(
        &self,
        include_deleted: bool,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Vec<(Vec<u8>, u64, Option<Vec<u8>>)> {
        crate::common::collect_rows(self, include_deleted, start, end, None, None).0
    }

    /// 取一组的值：折叠时已经把行从 map 里带出来了（内存引擎没有"另一处"），
    /// 所以凭据本身就是值，`None` 即墓碑。
    fn read_row(row: Option<Vec<u8>>) -> crate::common::RowValue {
        match row {
            Some(bytes) => crate::common::RowValue::Value(bytes),
            None => crate::common::RowValue::Tombstone,
        }
    }

    /// 折出**候选组**（一把逻辑 key 一组，值已在手里）：按 enc 升序，组内最后一次覆盖
    /// 就是最大可见版本。`after` 是续跑游标（上一轮最后一个键），`accept` 是键级过滤
    /// —— 放在这里而不是放上层，是为了让"整库折完再筛"变成"不命中的键一个值都不取"。
    ///
    /// ⚠️ 交出一组只能在**组边界**：一组的胜出行要等看见下一组的第一行才算收齐
    /// （版本大端接在 key 后面 ⇒ 组内升序）。在半截组上收手会把旧版本当成最新行。
    /// 返回的 `bool` = 是否已走到界尾（`false` = 是被 `budget` 拦下的，还能续跑）。
    pub(crate) fn scan_candidates(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        accept: Option<&dyn Fn(&[u8]) -> bool>,
        after: Option<&[u8]>,
        budget: usize,
    ) -> (Vec<crate::common::WinCandidate<Option<Vec<u8>>>>, bool) {
        let hi_enc = end.map(version_key_upper_bound);
        // 游标下推：`after` 不只是过滤器，它同时抬高迭代起点（M21，否则翻每页都从库头重走）
        let lo = crate::common::fold_lower_bound(start, after);
        let store = self.store.lock().unwrap();
        let mut out: Vec<crate::common::WinCandidate<Option<Vec<u8>>>> = Vec::new();
        let mut winners: BTreeMap<Vec<u8>, (u64, Vec<u8>)> = BTreeMap::new();
        let mut pending: Option<Vec<u8>> = None;
        // 组边界处理写成一个嵌套 fn：闭包会把 `winners`/`out` 一直借在手里，
        // 循环体里就再也插不进去（E0499，借用检查器在这里是对的）。
        fn hand_off(
            out: &mut Vec<crate::common::WinCandidate<Option<Vec<u8>>>>,
            winners: &mut BTreeMap<Vec<u8>, (u64, Vec<u8>)>,
            store: &MemStore,
            accept: Option<&dyn Fn(&[u8]) -> bool>,
            key: Vec<u8>,
        ) {
            let Some((ver, win_enc)) = winners.remove(&key) else {
                return;
            };
            if accept.is_some_and(|f| !f(&key)) {
                return;
            }
            // 刚还在 winners 里的行不见了：单写者模型下不会发生，宁可跳过这组也不猜
            let Some(row) = store.get(&win_enc).cloned() else {
                return;
            };
            out.push(crate::common::WinCandidate {
                key,
                version: ver,
                row,
            });
        }

        let mut stopped_by_budget = false;
        for enc in store.iter_keys_in(lo, hi_enc.as_deref()) {
            #[cfg(test)]
            FOLD_KEYS.with(|c| c.set(c.get() + 1));
            let Some(vk) = decode_key(enc) else {
                continue;
            };
            if start.is_some_and(|s| vk.raw_key.as_slice() < s) {
                continue;
            }
            if end.is_some_and(|e| vk.raw_key.as_slice() >= e) {
                continue;
            }
            if after.is_some_and(|a| vk.raw_key.as_slice() <= a) {
                continue; // 上一轮已经交出去的那组
            }
            if !self.is_visible(vk.version) {
                continue;
            }
            if pending.as_deref() != Some(vk.raw_key.as_slice()) {
                if let Some(key) = pending.take() {
                    hand_off(&mut out, &mut winners, &store, accept, key);
                    if out.len() >= budget {
                        stopped_by_budget = true;
                        break;
                    }
                }
                pending = Some(vk.raw_key.clone());
            }
            winners.insert(vk.raw_key.clone(), (vk.version, enc.to_vec()));
        }
        if !stopped_by_budget {
            if let Some(key) = pending {
                hand_off(&mut out, &mut winners, &store, accept, key);
            }
        }
        (out, !stopped_by_budget)
    }

    pub fn print_all(&self) -> BTreeMap<Vec<u8>, Option<Vec<u8>>> {
        let mut records = BTreeMap::new();
        for rec in self.export_latest_visible(true) {
            records.insert(rec.key, rec.value);
        }
        records
    }

    /// 导出本事务快照下每个逻辑 key 的最新可见版本（剥 TTL）
    pub fn export_latest_visible(&self, include_deleted: bool) -> Vec<ExportRecord> {
        self.collect_latest_raw(true)
            .into_iter()
            .filter_map(|(key, _ver, raw)| match raw {
                None => {
                    if include_deleted {
                        Some(ExportRecord { key, value: None })
                    } else {
                        None
                    }
                }
                Some(bytes) => match crate::memory::kv_ops::logical_to_user(bytes) {
                    Some(user) => Some(ExportRecord {
                        key,
                        value: Some(user),
                    }),
                    None => {
                        if include_deleted {
                            Some(ExportRecord { key, value: None })
                        } else {
                            None
                        }
                    }
                },
            })
            .collect()
    }

    pub fn commit(&self) {
        // 销账 + 给自动回收记一笔账，都在 [`TxnCore::commit`]；内存引擎没有 WAL，收尾交个空占位
        let mut reg = self.core.lock_registry();
        self.core.commit(&mut reg, &mut ());
    }

    pub fn rollback(&self) {
        let mut reg = self.core.lock_registry();
        let mut side = self.store.lock().unwrap();
        self.core.rollback(&mut reg, &mut *side);
    }

    fn is_visible(&self, version: u64) -> bool {
        // 判据在 [`crate::common::visible_at`]（四引擎共用的一份）
        self.core.is_visible(version)
    }

    /// 部分撤销：批量写撞墙时把已落下的那几把 key 退回（连同写集一起摘掉）。
    pub(crate) fn undo(&self, keys: &[Vec<u8>]) {
        let mut reg = self.core.lock_registry();
        let mut side = self.store.lock().unwrap();
        self.core.undo(&mut reg, &mut *side, keys);
    }

    /// 从链尾往回找本 key 第一个被 `accept` 的版本，返回它的版本键。
    ///
    /// 版本键大端有序 ⇒ 同一 key 的版本相邻且按版本号升序 ⇒ **链尾就是最新版本**。
    /// `raw_key` 仍是 `key` 前缀的其它逻辑 key 会落进同一个字节区间（版本号首字节 0x00
    /// 小于任何 ASCII 字节），所以只能**跳过**而不是「见到别的 key 就停」。
    fn find_version_back(
        store: &MemStore,
        key: &[u8],
        accept: impl Fn(u64) -> bool,
    ) -> Option<Vec<u8>> {
        let low = encode_key(key, 0);
        let high = encode_key(key, u64::MAX);
        let mut budget = VERSION_BACK_STEP;
        loop {
            let rows = store.range_back(&low, &high, budget);
            let reached_bottom = rows.len() < budget;
            for (enc, _) in rows {
                if let Some(vk) = decode_key(&enc) {
                    if vk.raw_key.as_slice() == key && accept(vk.version) {
                        return Some(enc);
                    }
                }
            }
            if reached_bottom {
                return None;
            }
            budget = budget.saturating_mul(2);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_order() {
        let k1 = encode_key(b"user", 1);
        let k2 = encode_key(b"user", 2);
        let k10 = encode_key(b"user", 10);
        assert!(k1 < k2);
        assert!(k2 < k10);
        let d = decode_key(&k10).unwrap();
        assert_eq!(d.raw_key, b"user");
        assert_eq!(d.version, 10);
    }

    #[test]
    fn test_basic_set_get_delete() {
        let mvcc = MVCC::new();
        let tx = mvcc.begin_transaction();
        assert!(tx.set(b"a", b"1".to_vec()));
        assert_eq!(tx.get(b"a"), Some(b"1".to_vec()));
        assert!(tx.set(b"a", b"2".to_vec()));
        assert_eq!(tx.get(b"a"), Some(b"2".to_vec()));
        assert!(tx.delete(b"a"));
        assert_eq!(tx.get(b"a"), None);
        tx.commit();
    }

    /// 探针：一次点读 / 一次点写访问了多少行。
    ///
    /// 断言的是**代价不随链长增长**：版本链的链尾就是最新版本，往回一步就该停。
    /// 内存引擎没有磁盘读，所以代价的对应物是「扫过多少行」。
    /// 修复前这里是 50 vs 2000（严格 1:1 线性）。
    #[test]
    fn test_point_op_cost_independent_of_chain_length() {
        fn probe(chain_len: u64) -> (u64, u64) {
            let mvcc = MVCC::new();
            for i in 0..chain_len {
                let tx = mvcc.begin_transaction();
                assert!(tx.set(b"hot", format!("v{i}").into_bytes()), "第 {i} 次写入被拒");
                tx.commit();
            }

            let tx = mvcc.begin_transaction();
            let before = crate::memory::mem_store::rows_touched();
            assert_eq!(
                tx.get(b"hot"),
                Some(format!("v{}", chain_len - 1).into_bytes())
            );
            let get_rows = crate::memory::mem_store::rows_touched() - before;

            let before = crate::memory::mem_store::rows_touched();
            assert!(tx.set(b"hot", b"newer".to_vec()));
            let set_rows = crate::memory::mem_store::rows_touched() - before;
            tx.rollback();
            (get_rows, set_rows)
        }

        let (g_short, s_short) = probe(50);
        let (g_long, s_long) = probe(2000);
        eprintln!(
            "点读访问行数：链长 50 → {g_short} 行，链长 2000 → {g_long} 行\n\
             点写访问行数：链长 50 → {s_short} 行，链长 2000 → {s_long} 行"
        );
        assert_eq!(g_short, g_long, "点读访问的行数不应随版本链长度增长");
        assert_eq!(s_short, s_long, "点写访问的行数不应随版本链长度增长（它只需要版本号）");
    }

    /// ② 全库折叠：不该再逐行克隆 value。
    ///
    /// 计数挂在重量路径 `MemStore::iter` 上（它每行克隆一次 value），所以将来谁把折叠
    /// 改回 `iter()`，这条会当场红。内存引擎没有磁盘读也没有 WAL，② 在这里的
    /// 代价单位就是 value 克隆 —— 别的引擎用读盘/页/块来量，这里只能用克隆来量。
    #[test]
    fn test_collect_latest_does_not_clone_values() {
        const KEYS: u64 = 8;
        const VERSIONS: u64 = 120;

        let mvcc = MVCC::new();
        for v in 0..VERSIONS {
            for k in 0..KEYS {
                let key = format!("k{k}");
                let tx = mvcc.begin_transaction();
                assert!(tx.set(key.as_bytes(), format!("v{v}").into_bytes()));
                tx.commit();
            }
        }

        let tx = mvcc.begin_transaction();
        let base = crate::memory::mem_store::value_clones();
        let recs = tx.export_latest_visible(false);
        let used = crate::memory::mem_store::value_clones() - base;
        eprintln!(
            "折叠期 value 克隆次数：{used}（存活 key {KEYS}，历史行数 {}）",
            KEYS * VERSIONS
        );
        assert_eq!(recs.len() as u64, KEYS, "存活 key 数应等于 {KEYS}");
        assert_eq!(
            used, 0,
            "折叠只该比键，不该逐行克隆 value：实际 {used} 次，历史行数 {}",
            KEYS * VERSIONS
        );
        tx.rollback();
    }

    /// vacuum 期间不该发生任何 value 克隆：它只需要版本号 + 「是不是墓碑」这个标志位。
    ///
    /// 量尺与 bplus 同一理由 —— 内存引擎没有 IO，`iter()` 的代价就是每行一次 value 克隆。
    #[test]
    fn test_vacuum_does_not_clone_values() {
        const CHAIN: u64 = 300;
        const KEYS: u64 = 4;

        let mvcc = MVCC::new();
        for i in 0..CHAIN {
            for k in 0..KEYS {
                let key = format!("k{k}");
                let tx = mvcc.begin_transaction();
                assert!(tx.set(key.as_bytes(), format!("v{i}").into_bytes()));
                tx.commit();
            }
        }

        let base = crate::memory::mem_store::value_clones();
        let stats = mvcc.vacuum();
        let used = crate::memory::mem_store::value_clones() - base;
        eprintln!(
            "vacuum 期间 value 克隆次数：{used}（裁掉 {} 行，历史行数 {}）",
            stats.versions_removed,
            CHAIN * KEYS
        );
        assert_eq!(
            stats.versions_removed,
            ((CHAIN - 1) * KEYS) as usize,
            "每 key 留一版，其余都该裁掉"
        );
        assert_eq!(used, 0, "vacuum 只需版本号与墓碑标志，不该克隆任何 value");
    }

    #[test]
    fn test_snapshot_isolation() {
        let mvcc = MVCC::new();
        let t1 = mvcc.begin_transaction();
        assert!(t1.set(b"k", b"v1".to_vec()));
        t1.commit();
        let t2 = mvcc.begin_transaction();
        let t3 = mvcc.begin_transaction();
        assert!(t2.set(b"k", b"v2".to_vec()));
        assert_eq!(t3.get(b"k"), Some(b"v1".to_vec()));
        t2.commit();
        assert_eq!(t3.get(b"k"), Some(b"v1".to_vec()));
        let t4 = mvcc.begin_transaction();
        assert_eq!(t4.get(b"k"), Some(b"v2".to_vec()));
        t4.commit();
        t3.commit();
    }

    #[test]
    fn test_write_write_conflict() {
        let mvcc = MVCC::new();
        let t1 = mvcc.begin_transaction();
        let t2 = mvcc.begin_transaction();
        assert!(t1.set(b"k", b"v1".to_vec()));
        assert!(!t2.set(b"k", b"v2".to_vec()));
        t1.commit();
        t2.rollback();
    }

    #[test]
    fn test_rollback_discards_writes() {
        let mvcc = MVCC::new();
        let t1 = mvcc.begin_transaction();
        assert!(t1.set(b"x", b"1".to_vec()));
        t1.rollback();
        let t2 = mvcc.begin_transaction();
        assert_eq!(t2.get(b"x"), None);
        t2.commit();
    }

    #[test]
    fn test_print_all_latest_visible() {
        let mvcc = MVCC::new();
        let t1 = mvcc.begin_transaction();
        assert!(t1.set(b"a", b"1".to_vec()));
        assert!(t1.set(b"b", b"2".to_vec()));
        assert!(t1.set(b"a", b"3".to_vec()));
        let all = t1.print_all();
        assert_eq!(all.get(&b"a".to_vec()).unwrap(), &Some(b"3".to_vec()));
        assert_eq!(all.get(&b"b".to_vec()).unwrap(), &Some(b"2".to_vec()));
        t1.commit();
    }

    #[test]
    fn test_instances_isolated() {
        let m1 = MVCC::new();
        let m2 = MVCC::new();
        let t1 = m1.begin_transaction();
        let t2 = m2.begin_transaction();
        assert!(t1.set(b"k", b"1".to_vec()));
        assert!(t2.set(b"k", b"2".to_vec()));
        assert_eq!(t1.get(b"k"), Some(b"1".to_vec()));
        assert_eq!(t2.get(b"k"), Some(b"2".to_vec()));
        t1.commit();
        t2.commit();
    }

    #[test]
    fn test_self_write_visible() {
        let mvcc = MVCC::new();
        let tx = mvcc.begin_transaction();
        assert!(tx.set(b"k", b"v".to_vec()));
        assert_eq!(tx.get(b"k"), Some(b"v".to_vec()));
        tx.commit();
    }

    #[test]
    fn test_conflict_after_other_commits() {
        let mvcc = MVCC::new();
        let t1 = mvcc.begin_transaction();
        let t2 = mvcc.begin_transaction();
        assert!(t1.set(b"k", b"1".to_vec()));
        t1.commit();
        assert!(!t2.set(b"k", b"2".to_vec()));
        t2.rollback();
        let t3 = mvcc.begin_transaction();
        assert_eq!(t3.get(b"k"), Some(b"1".to_vec()));
        t3.commit();
    }

    #[test]
    fn test_bulk_load() {
        let mvcc = MVCC::new();
        {
            let mut bulk = mvcc.begin_bulk();
            bulk.put(b"a", b"1".to_vec());
            bulk.put(b"b", b"2".to_vec());
            bulk.put_batch(&[(b"c".to_vec(), b"3".to_vec())]);
            assert_eq!(bulk.get(b"a"), Some(b"1".to_vec()));
            bulk.finish();
        }
        let tx = mvcc.begin_transaction();
        assert_eq!(tx.get(b"a"), Some(b"1".to_vec()));
        assert_eq!(tx.get(b"b"), Some(b"2".to_vec()));
        assert_eq!(tx.get(b"c"), Some(b"3".to_vec()));
        tx.commit();
    }

    #[test]
    fn test_vacuum_removes_old_versions() {
        let mvcc = MVCC::new();
        let t1 = mvcc.begin_transaction();
        assert!(t1.set(b"k", b"v1".to_vec()));
        t1.commit();
        let t2 = mvcc.begin_transaction();
        assert!(t2.set(b"k", b"v2".to_vec()));
        t2.commit();
        let before = mvcc.raw_len();
        assert!(before >= 2);
        let stats = mvcc.vacuum();
        assert!(stats.versions_removed >= 1);
        let after = mvcc.raw_len();
        assert!(after < before);
        let tx = mvcc.begin_transaction();
        assert_eq!(tx.get(b"k"), Some(b"v2".to_vec()));
        tx.commit();
    }

    /// 长跑进程不喊 vacuum，版本链也不能无限涨（I2：垃圾必须自己回来）。
    ///
    /// 内存引擎没有磁盘空间问题，容易被当成「不用回收」—— 但事故的量是**行数**不是字节数，
    /// 点读点写照样要陪跑整条链。四引擎同一个默认值、同一个守卫。
    #[test]
    fn test_auto_vacuum_reclaims_without_being_asked() {
        const CHAIN: u64 = 200;
        const SLICE: u64 = 8;

        let mvcc = MVCC::new();
        assert_eq!(
            mvcc.vacuum_auto_slice(),
            MVCC::VACUUM_AUTO_SLICE_DEFAULT,
            "自动回收默认必须开着"
        );
        mvcc.set_vacuum_auto_slice(SLICE);

        for i in 0..CHAIN {
            let tx = mvcc.begin_transaction();
            assert!(tx.set(b"hot", format!("v{i}").into_bytes()));
            tx.commit();
        }

        let h = mvcc.health();
        assert!(
            h.max_chain <= SLICE + 2,
            "没人喊 vacuum，链却涨到 {} 行 —— 自动回收没在工作",
            h.max_chain
        );

        let tx = mvcc.begin_transaction();
        assert_eq!(tx.get(b"hot"), Some(format!("v{}", CHAIN - 1).into_bytes()));
        tx.commit();
    }

    /// 关掉的开关必须真关掉：自动回收不能是「没法关掉的那种」。
    #[test]
    fn test_auto_vacuum_off_switch() {
        const CHAIN: u64 = 60;

        let mvcc = MVCC::new();
        mvcc.set_vacuum_auto_slice(0);
        for i in 0..CHAIN {
            let tx = mvcc.begin_transaction();
            assert!(tx.set(b"hot", format!("v{i}").into_bytes()));
            tx.commit();
        }
        assert_eq!(mvcc.health().max_chain, CHAIN, "关闭后不该裁掉任何一版");
    }

    /// 有界回收：每轮只碰一小段，多轮跑完必须和一次全库 vacuum 得到**同样**的结果。
    ///
    /// `vacuum()` 把 store 锁按到整库走完；切片后每轮读的行数有上限（偏差 = 一条链的长度：
    /// 截断只能落在逻辑 key 边界），游标每轮必须前进。
    /// 键里放了前缀嵌套的 `k1` / `k1x`：版本键 `raw ‖ 8B` 下两把 key 的区间会互相穿插。
    #[test]
    fn test_vacuum_bounded_slices_and_converges() {
        const CHAIN: u64 = 120;
        const KEYS: [&[u8]; 6] = [b"k1", b"k1x", b"k2", b"k3", b"k4", b"k5"];
        const SLICE: u64 = 40;

        fn fill(mvcc: &MVCC) {
            for i in 0..CHAIN {
                for k in KEYS {
                    let tx = mvcc.begin_transaction();
                    assert!(tx.set(k, format!("v{i}").into_bytes()), "写入被拒：{k:?}");
                    tx.commit();
                }
            }
        }

        // 对照组
        let full = {
            let mvcc = MVCC::new();
            fill(&mvcc);
            mvcc.vacuum().versions_removed
        };

        let mvcc = MVCC::new();
        fill(&mvcc);

        let mut cursor: Option<Vec<u8>> = None;
        let mut rounds = 0usize;
        let mut removed = 0usize;
        loop {
            let round = mvcc.vacuum_bounded(cursor.as_deref(), SLICE);
            assert!(!round.stats.skipped_active_txn, "第 {rounds} 轮不该被活跃事务挡住");
            assert!(
                round.stats.rows_scanned <= SLICE + CHAIN,
                "第 {rounds} 轮读了 {} 行（预算 {SLICE} + 一条链 {CHAIN}）",
                round.stats.rows_scanned
            );
            assert!(round.stats.versions_removed > 0, "第 {rounds} 轮必须裁掉东西");
            removed += round.stats.versions_removed;
            rounds += 1;
            match round.next_cursor {
                Some(c) => {
                    assert_ne!(cursor.as_ref(), Some(&c), "游标必须前进（第 {rounds} 轮）");
                    cursor = Some(c);
                }
                None => break,
            }
            assert!(rounds < 60, "切片应在有限轮内收敛，别是原地打转：已 {rounds} 轮");
        }

        assert!(rounds > 1, "6 把 key × {CHAIN} 版不该一轮就扫完：{rounds} 轮");
        assert_eq!(removed, full, "分片裁的总行数应与整库一轮一致");
        let h = mvcc.health();
        assert_eq!(h.max_chain, 1, "分片跑完后链必须压平");
        assert_eq!(h.total_rows, KEYS.len() as u64);

        let tx = mvcc.begin_transaction();
        for k in KEYS {
            assert_eq!(tx.get(k), Some(format!("v{}", CHAIN - 1).into_bytes()));
        }
        tx.commit();
    }

    /// 库必须能自己报出「最长版本链」。
    ///
    /// 这次事故的核心数字（某把热点 key 堆了多少个版本）一直是靠数日志行数得出来的 ——
    /// 一个当基座用的库不该靠这个诊断自己。四引擎共用同一份 `MvccHealth`，
    /// 所以换引擎时这两组断言必须照样成立（涨速不同，指标是同一个）。
    #[test]
    fn test_health_reports_chain_length() {
        const CHAIN: u64 = 500;

        let mvcc = MVCC::new();
        for i in 0..CHAIN {
            let tx = mvcc.begin_transaction();
            assert!(tx.set(b"hot", format!("v{i}").into_bytes()));
            tx.commit();
        }
        for k in 0..3u64 {
            let key = format!("other{k}");
            let tx = mvcc.begin_transaction();
            assert!(tx.set(key.as_bytes(), b"x".to_vec()));
            tx.commit();
        }

        let h = mvcc.health();
        assert_eq!(h.live_keys, 4, "逻辑 key 数");
        assert_eq!(h.total_rows, CHAIN + 3, "物理行数（含历史版本）");
        assert_eq!(h.max_chain, CHAIN, "最长版本链应由库自己报出来");
        assert_eq!(h.rows_above_one, CHAIN - 1);
        assert_eq!(h.active_txns, 0);

        let stats = mvcc.vacuum();
        assert!(!stats.skipped_active_txn, "无活跃事务时不该跳过");
        let after = mvcc.health();
        assert_eq!(after.max_chain, 1, "vacuum 后最长链应降到 1");
        assert_eq!(after.total_rows, 4, "vacuum 后应只剩每 key 一行");
        assert_eq!(after.live_keys, 4);
        assert_eq!(after.rows_above_one, 0);
    }

    /// `health()` 是管理面调用，但它也不该顺手把每行 value 克隆一遍 ——
    /// 否则「采样一下链长」这件事本身就会在热点库上制造一次内存峰值。
    #[test]
    fn test_health_does_not_clone_values() {
        const CHAIN: u64 = 400;

        let mvcc = MVCC::new();
        for _ in 0..CHAIN {
            let tx = mvcc.begin_transaction();
            assert!(tx.set(b"hot", vec![0u8; 1024]));
            tx.commit();
        }

        let before = crate::memory::mem_store::value_clones();
        let h = mvcc.health();
        let clones = crate::memory::mem_store::value_clones() - before;
        assert_eq!(h.max_chain, CHAIN);
        assert_eq!(clones, 0, "health 期间 value 克隆次数：{clones}（历史行数 {CHAIN}）");
    }

    /// 窗口 scan 的代价必须随**窗口宽度**缩 —— 内存引擎的尺子是「折叠走了多少把键」。
    ///
    /// 旧形状：`collect_latest_raw()` 折完**全库**再由调用方逐条筛界 ⇒ 10 行的窗口也要走
    /// 2000 把键（大库基线实测：窗口 scan 106.3 µs/行 vs 全量 1.1 µs/行）。
    /// 这条同时钉正确性：窗口结果必须与"全量之后筛"逐条一致，不能是筛错了才快。
    #[test]
    fn test_windowed_scan_cost_scales_with_window_not_library() {
        const KEYS: u64 = 2000;
        let mvcc = MVCC::new();
        for i in 0..KEYS {
            let tx = mvcc.begin_transaction();
            assert!(tx.set(format!("k{i:05}").as_bytes(), format!("v{i}").into_bytes()));
            tx.commit();
        }

        let tx = mvcc.begin_transaction();
        let before = fold_keys();
        let all = tx.scan(None, None);
        let full = fold_keys() - before;

        let lo = b"k01000".to_vec();
        let hi = b"k01010".to_vec();
        let before = fold_keys();
        let win = tx.scan(Some(&lo), Some(&hi));
        let windowed = fold_keys() - before;

        assert_eq!(all.len() as u64, KEYS, "全量应折出 {KEYS} 行");
        assert_eq!(win.len(), 10, "[k01000, k01010) 应给 10 行");
        let want: Vec<Vec<u8>> = all
            .iter()
            .filter(|r| r.key >= lo && r.key < hi)
            .map(|r| r.key.clone())
            .collect();
        let got: Vec<Vec<u8>> = win.iter().map(|r| r.key.clone()).collect();
        assert_eq!(got, want, "下推界之后窗口结果必须与全量筛一致");
        assert!(
            windowed <= 20,
            "窗口只该走窗口内的键（实际 {windowed}，全量 {full}）"
        );
        assert!(
            windowed * 10 <= full,
            "窗口折叠不该是全库量级（{windowed} vs {full}）"
        );

        // 前缀同理：k001xx ⇒ 100 个键
        let before = fold_keys();
        let pre = tx.prefix_scan(b"k001");
        let prefixed = fold_keys() - before;
        assert_eq!(pre.len(), 100, "前缀 k001 应命中 100 行");
        // 实测 200 而不是 100：哨兵是 `end ‖ 0xFF×8`，于是"以 end 为前缀的那一组键"
        // （这里是 k002xx，raw ≥ end）也会被 range 带进来，再由 `raw ≥ end ⇒ continue` 滤掉。
        // 代价合同因此是「窗口宽度 + 至多一个同前缀组」——宁可多走，不可误删
        // （`seek_prev` 的界是 `key ‖ 0x00`，收紧哨兵就会误删，见 version_key_upper_bound 的文档）。
        assert!(
            prefixed <= 250,
            "前缀扫该只走前缀覆盖的键 + 至多一个同前缀组（实际 {prefixed}，全量 {full}）"
        );
        assert!(
            prefixed * 4 <= full,
            "前缀扫的代价不该是全库量级（{prefixed} vs {full}）"
        );

        // 边界形状：前缀是**所有键的真前缀**（比第一个键还短）。lsm 曾把这种扫法做成空结果，
        // 四家都钉一遍。
        let whole = tx.prefix_scan(b"k");
        assert_eq!(whole.len() as u64, KEYS, "前缀 k 应命中全部 {KEYS} 行");
        assert_eq!(tx.key_count(b"k") as u64, KEYS, "key_count 同一个形状");

        // 上界哨兵的陷阱形状：`seek_prev` 的界是 `key ‖ 0x00`，用 `encode_key(end, 0)`
        // 当哨兵会把 `key` 自己的版本行判成越界（这条用例最初就是因为这个而红）。
        assert_eq!(tx.seek_prev(b"k01005").unwrap().key, b"k01005");
        assert_eq!(tx.seek(b"k01000").unwrap().key, b"k01000");
    }
}

impl crate::common::VersionedStore for Transaction {
    /// 
    type Row = Option<Vec<u8>>;

    fn scan_candidates(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        accept: Option<&dyn Fn(&[u8]) -> bool>,
        after: Option<&[u8]>,
        budget: usize,
    ) -> (Vec<crate::common::WinCandidate<Option<Vec<u8>>>>, bool) {
        // 走本类型上的同名 inherent 方法（inherent 优先于 trait ⇒ 不会自我递归）
        self.scan_candidates(start, end, accept, after, budget)
    }

    fn materialize(&self, row: Option<Vec<u8>>) -> crate::common::RowValue {
        Self::read_row(row)
    }
}
