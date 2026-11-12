//! MVCC 事务层（纯内存版）
//!
//! 对标 bplus-tree 的快照隔离语义，但**没有** WAL / 页刷盘 / 崩溃恢复：
//! - begin：分配 xid，拍下当时活跃事务快照
//! - 写：版本键 `raw_key || version_be`，写冲突检测
//! - 读：可见性过滤后取最新版本
//! - commit / rollback：维护活跃表，回滚物理删本事务 version 行
//!
//! 存储落到 [`crate::mem_store::MemStore`]。

#[cfg(test)]
use std::thread_local;
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
use crate::common::VacuumScheduler;
use crate::common::version::vacuum_drain_group;
use crate::common::txn::{BulkCore, Registry, TxnCore, WriteSide};
use crate::memory::mem_store::MemStore;
use std::borrow::Cow;

enum SnapshotOwnership {
    Exclusive(std::collections::btree_map::IntoIter<Vec<u8>, Option<Vec<u8>>>),
    Shared,
}

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
    active_txn: Arc<Mutex<Registry>>,
    next_version: Arc<AtomicU64>,
    /// 自动回收：切片宽度 / 记账 / 游标（策略与实现在 [`crate::common::VacuumScheduler`]）
    vac: Arc<VacuumScheduler>,
    /// M81 第 6 项：启停快照文件。`None` = 不落盘（裸 [`MVCC::new`] 的既有形状，一字未变）。
    ///
    /// 语义钉死成**只在启停各碰一次文件**（用户口径）：open 读一次、`Drop` 写一次。
    /// 运行期的 set/delete/事务/`flush()`/`checkpoint()` 一律不碰文件 —— `flush` 维持 no-op，
    /// 所以台架里 memory 的 `flush` 格一个数都不变。
    /// 代价如实写明：**没有手动存盘口子**，中途要落盘只能释放句柄（或走 export 工具）。
    /// 这是"优雅停止不丢"，**不是崩溃安全**：进程被杀或被 `std::process::exit` 收尾都不跑 `Drop`。
    snapshot: Option<std::path::PathBuf>,
}

/// 快照文件名（放在门面给的引擎目录里：`data_dir/memory/snapshot.bxp`）
pub const SNAPSHOT_FILE: &str = "snapshot.bxp";

/// 重放期间把自动回收**临时关掉**（切片宽度置 0），`Drop` 时还原（M82）。
///
/// 为什么用 RAII 而不是"进去设 0、出来设回来"：`load_snapshot_from` 有三条提前返回
/// （快照读失败、重放被引擎拒收、尾巴半页出错），漏掉任何一条都会把自动回收**永久关掉** ——
/// 那种坏法不崩、不报错，只是版本链从此只增不减。析构里还原，panic 展开也兜得住。
struct AutoVacuumOff<'a> {
    vac: &'a VacuumScheduler,
    saved: u64,
}

impl<'a> AutoVacuumOff<'a> {
    fn pause(vac: &'a VacuumScheduler) -> Self {
        let saved = vac.slice();
        vac.set_slice(0);
        Self { vac, saved }
    }
}

impl Drop for AutoVacuumOff<'_> {
    fn drop(&mut self) {
        self.vac.set_slice(self.saved);
    }
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
            snapshot: None,
        }
    }

    /// 打开一个带启停快照的 memory 库（M81 第 6 项）：`dir` 里放 [`SNAPSHOT_FILE`]。
    /// 文件在就读入重放，不在就从空库开始 —— 两种都算打开成功。
    ///
    /// ⚠️ `snapshot` 字段是**重放成功之后**才赋值的，顺序不能反过来：读中途失败就直接返回 `Err`，
    /// 此时这个半成品对象的 `Drop` 里 `snapshot` 还是 `None` ⇒ 不会把一份好快照覆盖成半截库。
    /// （反过来写就是"打开失败一次，库没了"——比不落盘坏得多。）
    pub fn open_with_snapshot(dir: &std::path::Path) -> std::io::Result<Self> {
        let path = dir.join(SNAPSHOT_FILE);
        let mut mvcc = Self::new();
        mvcc.load_snapshot_from(&path)?;
        mvcc.snapshot = Some(path);
        Ok(mvcc)
    }

    /// 快照路径（门面的 `path()` 用它；`MVCC::new()` 开的库返回 `None`，行为与改前一致）
    pub fn snapshot_path(&self) -> Option<&std::path::Path> {
        self.snapshot.as_deref()
    }

    /// 读入快照并重放。三条硬规矩都是这个仓库一路在收的东西：
    /// ① **已过期的行不许复活**（拿文件里的绝对 expire 与 now 比，不信任"它当时是活的"）；
    /// ② 按页一事务（照 `migrate` 的写侧），不把两千万行攒进一个写集；
    /// ③ TTL 保留文件内绝对到期时刻；不把重放耗时加回剩余寿命。
    fn load_snapshot_from(&mut self, path: &std::path::Path) -> std::io::Result<()> {
        #[cfg(test)]
        let _timer = crate::common::kv_snapshot::recovery_probe::Timer::start(crate::common::kv_snapshot::recovery_probe::Kind::Total);
        if !path.exists() {
            return Ok(());
        }
        // 重放期间关掉自动回收，收尾把账也丢掉（M82）：重放出来的行每把键只有一版 ⇒ **一行垃圾
        // 都没有**，而重放走的是"每 1000 行一个事务"的写流 ⇒ 不关的话每个事务的 `begin()` 都要
        // 扫一片（1:1 跟着"写"的量走）：600k 档 `冷重开` 1.3 s → 4.4 s、20M 档白扫一整库。
        // ⚠️ 还原走 RAII：下面三条提前返回（读失败、重放被拒收）都不许把自动回收永久关掉。
        let _vac_paused = AutoVacuumOff::pause(&self.vac);
        const PAGE: usize = 1_000;
        let now = crate::common::codec::now_unix_secs();
        let mut batch: Vec<(Vec<u8>, Option<Vec<u8>>, Option<u64>)> = Vec::with_capacity(PAGE);
        let mut expired = 0usize;
        let mut err: Option<std::io::Error> = None;
        let values = crate::common::kv_snapshot::ValueBuffer::PlainEnvelope;
        #[cfg(test)]
        let values = if replay_cost_reference::enabled() { crate::common::kv_snapshot::ValueBuffer::Exact } else { values };
        let stats = crate::common::kv_snapshot::read_snapshot_with(&path, values, &mut |rec| {
            // `value = None` = 墓碑行，照样进重放队列（重放成 `delete`）——
            // 丢掉它就是"重启即失忆"：判据①的地基没了，后续同步能复活已删键。
            let value = rec.value;
            if let Some(e) = rec.expire_unix_secs {
                if e <= now {
                    expired += 1;
                    return true;
                }
            }
            batch.push((rec.key, value, rec.expire_unix_secs));
            if batch.len() >= PAGE {
                if let Err(e) = MVCC::replay_page(self, &mut batch) {
                    err = Some(e);
                    return false;
                }
            }
            true
        })?;
        if let Some(e) = err {
            return Err(e);
        }
        MVCC::replay_page(self, &mut batch)?;
        log::info!(
            "memory: 快照读入 {} 行（跳过已过期 {} 行）{}",
            stats.records - expired,
            expired,
            path.display()
        );
        // 收尾把重放攒下的触发账丢掉：那些行不是垃圾（见上面那段注释）。
        // ⚠️ 位置在还原切片宽度**之前**也无所谓（`take_tick` 只看账与游标），但要紧的是：
        // 这个口子只在这儿用 —— 别的地方丢账就是把"该回收"写成"不用回收"。
        let dropped = self.vac.discard_pending();
        if dropped > 0 {
            log::debug!("memory: 快照重放丢弃 {dropped} 行触发账（新写入的行没有旧版本可回收）");
        }
        Ok(())
    }

    /// 一页一事务；任何写入/提交失败都返回错误，未提交的页由事务 Drop 回滚。
    /// 读取回调把错误带出，尾页直接使用同一个 Result 入口。
    ///
    /// 为什么单独抽出来：回调里要同时用 `&MVCC`（开事务）与 `&mut batch`，写成内联块会让
    /// 借用检查在两件事之间反复；而"攒满一页就冲"与"收尾把剩的冲掉"必须是**同一份**代码，
    /// 否则尾巴那半页会静默不写（本仓库收过这类账）。
    fn replay_page(
        mvcc: &MVCC,
        batch: &mut Vec<(Vec<u8>, Option<Vec<u8>>, Option<u64>)>,
    ) -> std::io::Result<()> {
        #[cfg(test)]
        let _timer = crate::common::kv_snapshot::recovery_probe::Timer::start(crate::common::kv_snapshot::recovery_probe::Kind::Replay);
        #[cfg(test)]
        if replay_reference::enabled() { return Self::replay_page_reference(mvcc, batch); }
        if batch.is_empty() { return Ok(()); }
        #[cfg(test)]
        crate::common::kv_snapshot::recovery_probe::note(|s| { s.replay_pages += 1; s.replay_input_rows += batch.len() as u64; });
        let tx = {
            #[cfg(test)]
            let _setup = crate::common::kv_snapshot::recovery_probe::Timer::start(crate::common::kv_snapshot::recovery_probe::Kind::ReplaySetup);
            mvcc.begin_transaction()
        };
        {
            // Seal the owned page before locking the registry/store. Replaying
            // remains one normal transaction, including rollback of a rejected page.
            #[cfg(test)]
            let _encode = crate::common::kv_snapshot::recovery_probe::Timer::start(crate::common::kv_snapshot::recovery_probe::Kind::ReplayEncode);
            for (_, value, expire) in batch.iter_mut() {
                *value = value.take().map(|v| {
                    #[cfg(test)]
                    if replay_cost_reference::enabled() {
                        return match expire { Some(e) => crate::common::pack_ttl(&v, *e), None => crate::common::pack_plain(&v) };
                    }
                    crate::common::codec::pack_owned(v, *expire).into_bytes()
                });
            }
        }
        {
            // The unopened instance keeps normal conflict checks, registry ->
            // store lock ordering and bounded page transactions.
            #[cfg(test)]
            let _apply = crate::common::kv_snapshot::recovery_probe::Timer::start(crate::common::kv_snapshot::recovery_probe::Kind::ReplayApply);
            let mut reg = tx.core.lock_registry();
            let mut side = mvcc.store.lock().unwrap();
            #[cfg(test)]
            let reference = page_apply_reference::enabled();
            #[cfg(not(test))]
            let reference = false;
            if reference {
                for (key, logical, _) in batch.drain(..) {
                    tx.core.try_write_owned(&mut reg, &mut *side, key, logical)
                        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData,
                            format!("快照重放被引擎拒收: {error:?}")))?;
                }
            } else {
                tx.core.try_write_owned_page(&mut reg, &mut *side,
                    batch.drain(..).map(|(key, logical, _)| (key, logical)))
                    .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData,
                        format!("快照重放被引擎拒收: {error:?}")))?;
            }
        }
        #[cfg(test)]
        let _commit = crate::common::kv_snapshot::recovery_probe::Timer::start(crate::common::kv_snapshot::recovery_probe::Kind::ReplayCommit);
        tx.try_commit().map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData,
            format!("快照重放提交失败: {error:?}")))
    }

    #[cfg(test)]
    fn replay_page_reference(
        mvcc: &MVCC,
        batch: &mut Vec<(Vec<u8>, Option<Vec<u8>>, Option<u64>)>,
    ) -> std::io::Result<()> {
        if batch.is_empty() { return Ok(()); }
        let _legacy = crate::common::kv_snapshot::recovery_probe::Timer::start(crate::common::kv_snapshot::recovery_probe::Kind::ReplayLegacy);
        crate::common::kv_snapshot::recovery_probe::note(|s| { s.replay_pages += 1; s.replay_input_rows += batch.len() as u64; });
        let tx = mvcc.begin_transaction();
        for (key, value, expire) in batch.drain(..) {
            match (value, expire) {
                (None, _) => tx.try_delete(&key),
                (Some(value), Some(expiry)) => tx.try_set(&key, crate::common::pack_ttl(&value, expiry)),
                (Some(value), None) => tx.try_set_raw(&key, value),
            }.map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData,
                format!("快照重放被引擎拒收: {error:?}")))?;
        }
        tx.try_commit().map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData,
            format!("快照重放提交失败: {error:?}")))
    }

    /// 写一次带完整性校验的整库快照（`BPEXP004`），同步后原子替换旧文件。
    ///
    /// 三条口径：
    /// - **流式**：走 `export_latest_visible_for_each` 交一行折一行，不把整库摆进 `Vec`
    ///   （M44 那条纪律对快照同样成立；20M 档那一格本来就要 GB 级）。
    /// - **带墓碑**（`include_deleted = true`）：`tools::sync` / `import --skip-existing` 的判据①
    ///   是"目标见过这把键吗（含墓碑）"，快照里没墓碑 ⇒ 重启一次判据的地基就没了。
    /// - TTL 从出口那一份 `ExportRecord::expire_unix_secs` 直接拿（0.5.0 起有这一格），
    ///   文件里存**绝对**时刻 ⇒ 躺多久都不影响这一行还剩多少命。
    ///
    /// 写盘失败**不吞**：一路 `?` 交给 `Drop`，那里降成 `warn`（析构里不能抛）。
    #[cfg(test)]
    fn write_snapshot_reference(&self, path: &std::path::Path) -> std::io::Result<usize> {
        use crate::common::kv_snapshot::{KvRecord, atomic_write_bin_v4};
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut written = 0usize;
        let mut err: Option<std::io::Error> = None;
        let stats = atomic_write_bin_v4(path, |sink| {
        // `include_deleted = true` ⇒ 墓碑也进快照（`value = None` 那一支）。
        // 为什么必须带上：memory 当 `tools::sync` 的目标时，判据是"目标见过这把键吗"，
        // 而"见过"含墓碑 —— 快照里没墓碑，重启一次之后"这把键被删过"就消失了，
        // 后续同步能把已删键复活（`memory_snapshot_keeps_tombstones_alive` 钉这条）。
        let handed = self.export_latest_visible_for_each(true, &mut |rec: ExportRecord| {
            let mut kr = KvRecord::new(rec.key, rec.value);
            if let Some(e) = rec.expire_unix_secs {
                kr = kr.with_expire(e);
            }
            match sink.write(&kr) {
                Ok(()) => {
                    written += 1;
                    true
                }
                Err(e) => {
                    err = Some(e);
                    false
                }
            }
        });
        if let Some(e) = err {
            return Err(e);
        }
        if handed != written {
            // 不拿 assert：这条在 `Drop` 之路上，panic 会在已经 panic 的展开里再炸一次。
            // 两份账不一致 = 有行静默没落盘，必须报出去而不是"看起来成功了"。
            return Err(std::io::Error::other(format!(
                "memory 快照写出 {written} 行、出口交出 {handed} 行"
            )));
        }
        Ok(())
        })?;
        debug_assert_eq!(stats.records, written);
        Ok(written)
    }

    /// Keep shared handles readable; consume the tree only on the final owner.
    fn write_stop_snapshot(&mut self, path: &std::path::Path) -> std::io::Result<usize> {
        #[cfg(test)]
        if stop_reference::enabled() || scan_reference::enabled() {
            let result = self.write_snapshot_reference(path);
            crate::common::kv_snapshot::snapshot_probe::note(|s| s.stop_mode = "reference");
            return result;
        }
        self.tick_auto_vacuum();
        let ownership = match Arc::get_mut(&mut self.store) {
            Some(store) => SnapshotOwnership::Exclusive(store.get_mut().unwrap().take_entries()),
            None => SnapshotOwnership::Shared,
        };
        match ownership {
            SnapshotOwnership::Exclusive(entries) => {
                let ceiling = self.next_version.load(Ordering::SeqCst);
                let rows = entries.map(|(key, value)| (Cow::Owned(key), value.map(Cow::Owned)));
                Self::write_snapshot_rows(path, rows, ceiling, |v| v < ceiling, "exclusive")
            }
            SnapshotOwnership::Shared => {
                let tx = Transaction::begin(self.store.clone(), self.active_txn.clone(), self.next_version.clone(), self.vac.clone());
                let store = self.store.lock().unwrap();
                let rows = store.borrowed_entries().map(|(key, value)| (Cow::Borrowed(key), value.map(Cow::Borrowed)));
                let result = Self::write_snapshot_rows(path, rows, tx.version(), |v| tx.core.is_visible(v), "shared");
                drop(store);
                tx.commit();
                result
            }
        }
    }

    fn write_snapshot_rows<'a>(path: &std::path::Path,
        rows: impl Iterator<Item = (Cow<'a, [u8]>, Option<Cow<'a, [u8]>>)>,
        ceiling: u64, visible: impl Fn(u64) -> bool, mode: &'static str,
    ) -> std::io::Result<usize> {
        use crate::common::{kv_snapshot::{atomic_write_bin_v4, BinWriter}, order::KeyWindow, version::decode_key_ref};
        #[cfg(test)]
        if stop_rows_reference::enabled() {
            return Self::write_snapshot_rows_reference(path, rows, ceiling, visible, mode);
        }
        if let Some(parent) = path.parent() { std::fs::create_dir_all(parent)?; }
        let mut visited = 0_u64;
        #[cfg(test)]
        let mut detail = crate::common::kv_snapshot::snapshot_probe::Stats::default();
        #[cfg(test)]
        let measured = crate::common::kv_snapshot::snapshot_probe::is_active();
        let stats = atomic_write_bin_v4(path, |sink| {
            let mut groups: KeyWindow<(u64, Option<Cow<'a, [u8]>>)> = KeyWindow::new();
            let mut spare = Vec::new();
            let mut rows = rows;
            const INPUT_BATCH: usize = 8192;
            let mut batch = Vec::with_capacity(rows.size_hint().0.min(INPUT_BATCH));
            let emit = |sink: &mut BinWriter, key: &[u8], value: Option<Cow<'a, [u8]>>| {
                let (user, expire) = match value.as_deref() {
                    Some(bytes) => crate::common::codec::snapshot_value(bytes),
                    None => (None, None),
                };
                sink.write_borrowed(key, user, None, expire)
            };
            loop {
                #[cfg(test)]
                let pull = measured.then(std::time::Instant::now);
                batch.extend(rows.by_ref().take(INPUT_BATCH));
                #[cfg(test)]
                if let Some(pull) = pull { detail.pull_ns += pull.elapsed().as_nanos(); }
                #[cfg(test)]
                { detail.input_buffer_bytes = detail.input_buffer_bytes.max(batch.capacity() * std::mem::size_of::<(Cow<'a, [u8]>, Option<Cow<'a, [u8]>>)>()); }
                if batch.is_empty() { break; }
                #[cfg(test)]
                { detail.input_batches += 1; }
                #[cfg(test)]
                let fold = measured.then(std::time::Instant::now);
                for (enc, value) in batch.drain(..) {
                    visited += 1;
                    while let Some((key, (_, value))) = groups.pop_before(&enc, ceiling) {
                        emit(sink, &key, value)?;
                        spare = key;
                    }
                    let Some((raw, version)) = decode_key_ref(&enc) else { continue; };
                    if !visible(version) { continue; }
                    let len = raw.len();
                    let winner = match enc {
                        Cow::Owned(mut key) => {
                            key.truncate(len);
                            groups.entry(key)
                        }
                        Cow::Borrowed(enc) => groups.entry_with(&enc[..len], || {
                            let mut key = std::mem::take(&mut spare);
                            key.clear(); key.extend_from_slice(&enc[..len]);
                            (key, (0, None))
                        }),
                    };
                    if version >= winner.0 { *winner = (version, value); }
                }
                #[cfg(test)]
                if let Some(fold) = fold { detail.fold_emit_ns += fold.elapsed().as_nanos(); }
            }
            #[cfg(test)]
            let tail = measured.then(std::time::Instant::now);
            while let Some((key, (_, value))) = groups.pop_first() { emit(sink, &key, value)?; }
            #[cfg(test)]
            if let Some(tail) = tail { detail.fold_emit_ns += tail.elapsed().as_nanos(); }
            Ok(())
        })?;
        #[cfg(test)]
        crate::common::kv_snapshot::snapshot_probe::note(|s| {
            s.stop_mode = mode; s.visited_rows = visited;
            s.consumed_rows = if mode == "exclusive" { visited } else { 0 };
            s.pull_ns = detail.pull_ns; s.fold_emit_ns = detail.fold_emit_ns;
            s.input_batches = detail.input_batches; s.input_buffer_bytes = detail.input_buffer_bytes;
        });
        #[cfg(not(test))]
        let _ = (mode, visited);
        Ok(stats.records)
    }

    #[cfg(test)]
    fn write_snapshot_rows_reference<'a>(path: &std::path::Path,
        rows: impl Iterator<Item = (Cow<'a, [u8]>, Option<Cow<'a, [u8]>>)>,
        ceiling: u64, visible: impl Fn(u64) -> bool, mode: &'static str,
    ) -> std::io::Result<usize> {
        use crate::common::{kv_snapshot::{atomic_write_bin_v4, BinWriter}, order::KeyWindow, version::decode_key_ref};
        if let Some(parent) = path.parent() { std::fs::create_dir_all(parent)?; }
        let mut visited = 0_u64;
        let stats = atomic_write_bin_v4(path, |sink| {
            let mut groups: KeyWindow<(u64, Option<Cow<'a, [u8]>>)> = KeyWindow::new();
            let mut spare = Vec::new();
            let emit = |sink: &mut BinWriter, key: &[u8], value: Option<Cow<'a, [u8]>>| {
                let (user, expire) = match value.as_deref() {
                    Some(bytes) => crate::common::codec::snapshot_value(bytes),
                    None => (None, None),
                };
                sink.write_borrowed(key, user, None, expire)
            };
            for (enc, value) in rows {
                visited += 1;
                while let Some((key, (_, value))) = groups.pop_before(&enc, ceiling) {
                    emit(sink, &key, value)?;
                    spare = key;
                }
                let Some((raw, version)) = decode_key_ref(&enc) else { continue; };
                if !visible(version) { continue; }
                let len = raw.len();
                let winner = match enc {
                    Cow::Owned(mut key) => { key.truncate(len); groups.entry(key) }
                    Cow::Borrowed(enc) => groups.entry_with(&enc[..len], || {
                        let mut key = std::mem::take(&mut spare);
                        key.clear(); key.extend_from_slice(&enc[..len]);
                        (key, (0, None))
                    }),
                };
                if version >= winner.0 { *winner = (version, value); }
            }
            while let Some((key, (_, value))) = groups.pop_first() { emit(sink, &key, value)?; }
            Ok(())
        })?;
        crate::common::kv_snapshot::snapshot_probe::note(|s| {
            s.stop_mode = mode; s.visited_rows = visited;
            s.consumed_rows = if mode == "exclusive" { visited } else { 0 };
        });
        Ok(stats.records)
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
    /// 触发量 = 切片宽度 ⇒ 攒够一片垃圾才扫一片，扫描量与写入量 **1:1**，`begin()` 不付额外的税
    /// （M82：M81 第 5 项后半那条"一轮开着就每个 begin 推一片"已拿掉 —— 它让写流里每个事务都扫
    /// 一整片，`docs/test2.txt` 五档 `灌`/`更新` 全变慢）。账花完而一轮还在半路时，由
    /// [`crate::maintenance::Maintenance`]（独立游标与间隔）把它推到库尾，不由下一个事务推；
    /// 不跑维护器就等下一次写或手动 `vacuum()`。
    fn tick_auto_vacuum(&self) {
        let _: Result<(), std::convert::Infallible> = self.vac.run_pending(|after, rows| {
            #[cfg(test)]
            let _probe = crate::common::maintenance_probe::Timer::start(crate::common::maintenance_probe::Stage::AutoVacuum);
            Ok(self.vacuum_bounded(after, rows))
        });
    }

    /// 轻量回收状态，不扫描数据库。
    pub fn reclamation_status(&self) -> crate::common::ReclamationStatus {
        self.vac.status(&self.active_txn.lock().unwrap())
    }

    /// 自动回收的切片宽度（行数）：每攒够这么多行新版本，就裁一片这么多行。
    ///
    /// 默认 [`MVCC::VACUUM_AUTO_SLICE_DEFAULT`]；**设 0 关闭自动回收**。内存引擎没有磁盘
    /// 空间问题，但旧版本仍占堆内存。点读早停与版本回收是两件独立的事。
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
        // M44：收集壳 —— 判据与遍历只有一份，在 `export_latest_visible_for_each`。
        // ⚠️ 返回整库的 `Vec` 是这条的合同（600k 档实测 +123.7 MB）；只需要"把行交出去"的
        // 人（导出成文件、搬去另一家、数个数）请走流式那条。
        let mut out: Vec<ExportRecord> = Vec::new();
        self.export_latest_visible_for_each(include_deleted, |rec| {
            out.push(rec);
            true
        });
        out
    }

    /// 流式导出（M44）：开一个快照事务，折一行交一行，**一行都不攒**；`on` 返回 `false`
    /// 当场收手。返回实际交出去的行数。
    ///
    /// 为什么库侧要提供这条：折叠那一段早就是流式的（M27），但出口只有返回 `Vec` 的那条 ——
    /// 于是"把 600k 键的库导成文件"必须先在内存里摆一整套 `ExportRecord`，而这套东西
    /// 存在的唯一目的就是等着被逐行写出去。`export_kv` 那几个 CLI 原先还在这之上**又**
    /// `map` 成一份 `Vec<KvRecord>` ⇒ 全库同时在内存里摆了两遍。
    pub fn export_latest_visible_for_each(
        &self,
        include_deleted: bool,
        mut on: impl FnMut(ExportRecord) -> bool,
    ) -> usize {
        let tx = self.begin_transaction();
        let handed = tx.export_latest_visible_for_each(include_deleted, &mut on);
        tx.commit();
        handed
    }

    /// 崩溃安全画像：**全空，这是设计而非疏漏**。
    ///
    /// ⚠️ M81 第 6 项之后"全空"的含义要说准：它**没有崩溃安全**（进程被杀 / `exit` 都不跑析构，
    /// 启停快照写不出来），仍然没有事务 WAL。BPEXP004 快照有 CRC，但不提供运行期持久提交。
    /// 变的是：给了快照目录的 `open_engine("memory", Some(dir))` 现在**优雅停止不丢**
    /// （`Drop` 写整库快照、下次 open 读回）。这不改变它"对照实验"的定位：
    /// 快照既不是 WAL 也不带校验和，扛不住掉电。
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
        // Count complete logical groups, including interleaved binary prefixes.
        let mut fold = crate::common::order::LogicalHealth::new(self.next_version.load(Ordering::SeqCst));
        store.scan_entries_with(|enc, _val| {
            fold.push(&enc);
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
        #[cfg(test)]
        if vacuum_cost_reference::enabled() { return self.vacuum_bounded_reference(after, budget_rows); }
        use crate::common::version::decode_key_ref;
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

        // 分组按引用走表：判「留哪一版」只需要版本号 + 是不是墓碑，
        // 不需要把每行 value 克隆出来（旧实现是 `store.iter().collect()`）。
        //
        // M36：**只攒当前这一把键的那一组**，看到后继就把上一组交出去判 ⇒ 瞬时内存从
        // O(全库物理行)（600k 档实测 +89.4 MB，比要裁的东西大两个数量级）降到
        // O(最长那条链)。版本键按 `raw ‖ version_be` 升序到达，同一把键的行可能被二进制前缀打断，按公共 KeyWindow 等待完整组。
        // M37：再按块走 —— 走查持读锁、删除要写锁 ⇒ 每块读完就**放掉读锁**再删这一块，
        // 于是 `to_delete` 从 O(全库) 变成 O(一块)（这一家实测基数小：600k 档 `vacuum()`
        // 瞬时 4.0 MB，改完是 O(块)；改它的理由是"同根因不留三家"，不是那 4 MB）。
        // 块间下界 `enc ‖ 0x00`：enc 域的严格后继方向，与 bitcask/lsm 同一个理由。
        let budget_rows = budget_rows.max(1);
        let mut groups: crate::common::order::KeyWindow<Vec<(u64, u64, bool, ())>> = crate::common::order::KeyWindow::new();
        let mut spare = Vec::new();
        let mut to_delete: Vec<Vec<u8>> = Vec::new();
        let mut removed = 0usize;
        let mut scanned = 0u64;
        let mut cur_raw: Option<Vec<u8>> = None;
        let mut truncated = false;
        let mut lower: Option<Vec<u8>> = after.map(|k| k.to_vec());
        loop {
            let mut chunk_rows = 0usize;
            let mut next_lower: Option<Vec<u8>> = None;
            let mut done = false;
            #[cfg(test)]
            let scan_probe = crate::common::maintenance_probe::Timer::start(crate::common::maintenance_probe::Stage::VacuumScan);
            {
                let store = self.store.lock().unwrap();
                store.scan_entries_from(lower.as_deref(), |enc, val| {
                    let Some((raw_key, version)) = decode_key_ref(enc) else {
                        return false;
                    };
                    if after.is_some_and(|a| raw_key <= a) {
                        return false;
                    }
                    while let Some((key, mut group)) = groups.pop_before(&enc, xmin) {
                        vacuum_drain_group(&mut group, xmin, |version| to_delete.push(encode_key(&key, version)), |_, _| {});
                        if group.capacity() > spare.capacity() { spare = group; }
                        cur_raw = Some(key);
                    }
                    // Stop only after the complete overlapping prefix window is finalized.
                    if groups.is_empty() && scanned >= budget_rows {
                        truncated = true;
                        done = true;
                        return true;
                    }
                    scanned += 1;
                    groups.entry_with(raw_key, || {
                        let mut owned = cur_raw.take().unwrap_or_default();
                        owned.clear();
                        owned.extend_from_slice(raw_key);
                        (owned, std::mem::take(&mut spare))
                    }).push((version, version, val.is_none(), ()));
                    chunk_rows += 1;
                    if chunk_rows >= VAC_CHUNK_ROWS {
                        let mut bound = enc.to_vec();
                        bound.push(0u8);
                        next_lower = Some(bound);
                        return true;
                    }
                    false
                });
            }
            if done || next_lower.is_none() {
                while let Some((key, mut group)) = groups.pop_first() {
                    vacuum_drain_group(&mut group, xmin, |version| to_delete.push(encode_key(&key, version)), |_, _| {});
                    if group.capacity() > spare.capacity() { spare = group; }
                    cur_raw = Some(key);
                }
            }
            #[cfg(test)]
            std::mem::drop(scan_probe);
            #[cfg(test)]
            note_vac_pending(to_delete.len());
            // 读锁已经放掉 ⇒ 这一块单独拿写锁删（以前是"整库攒完再删一次"）
            {
                #[cfg(test)]
                let _probe = crate::common::maintenance_probe::Timer::start(crate::common::maintenance_probe::Stage::VacuumDelete);
                let mut store = self.store.lock().unwrap();
                for enc in to_delete.drain(..) {
                    let _ = store.delete(&enc);
                    removed += 1;
                }
            }
            match next_lower {
                Some(l) if !done => lower = Some(l),
                _ => break,
            }
        }
        let next_cursor = if truncated { cur_raw } else { None };

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

    #[cfg(test)]
    fn vacuum_bounded_reference(&self, after: Option<&[u8]>, budget_rows: u64) -> VacuumRound {
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

        // 分组按引用走表：判「留哪一版」只需要版本号 + 是不是墓碑，
        // 不需要把每行 value 克隆出来（旧实现是 `store.iter().collect()`）。
        //
        // M36：**只攒当前这一把键的那一组**，看到后继就把上一组交出去判 ⇒ 瞬时内存从
        // O(全库物理行)（600k 档实测 +89.4 MB，比要裁的东西大两个数量级）降到
        // O(最长那条链)。版本键按 `raw ‖ version_be` 升序到达，同一把键的行可能被二进制前缀打断，按公共 KeyWindow 等待完整组。
        // M37：再按块走 —— 走查持读锁、删除要写锁 ⇒ 每块读完就**放掉读锁**再删这一块，
        // 于是 `to_delete` 从 O(全库) 变成 O(一块)（这一家实测基数小：600k 档 `vacuum()`
        // 瞬时 4.0 MB，改完是 O(块)；改它的理由是"同根因不留三家"，不是那 4 MB）。
        // 块间下界 `enc ‖ 0x00`：enc 域的严格后继方向，与 bitcask/lsm 同一个理由。
        let budget_rows = budget_rows.max(1);
        let mut groups: crate::common::order::KeyWindow<Vec<(u64, Vec<u8>, bool, ())>> = crate::common::order::KeyWindow::new();
        let mut to_delete: Vec<Vec<u8>> = Vec::new();
        let mut removed = 0usize;
        let mut scanned = 0u64;
        let mut cur_raw: Option<Vec<u8>> = None;
        let mut truncated = false;
        let mut lower: Option<Vec<u8>> = after.map(|k| k.to_vec());
        loop {
            let mut chunk_rows = 0usize;
            let mut next_lower: Option<Vec<u8>> = None;
            let mut done = false;
            #[cfg(test)]
            let scan_probe = crate::common::maintenance_probe::Timer::start(crate::common::maintenance_probe::Stage::VacuumScan);
            {
                let store = self.store.lock().unwrap();
                store.scan_entries_from(lower.as_deref(), |enc, val| {
                    let Some(vk) = decode_key(enc) else {
                        return false;
                    };
                    if after.is_some_and(|a| vk.raw_key.as_slice() <= a) {
                        return false;
                    }
                    while let Some((key, mut group)) = groups.pop_before(&enc, xmin) {
                        vacuum_drain_group(&mut group, xmin, |row| to_delete.push(row), |_, _| {});
                        cur_raw = Some(key);
                    }
                    // Stop only after the complete overlapping prefix window is finalized.
                    if groups.is_empty() && scanned >= budget_rows {
                        truncated = true;
                        done = true;
                        return true;
                    }
                    scanned += 1;
                    groups.entry(vk.raw_key).push((vk.version, enc.to_vec(), val.is_none(), ()));
                    chunk_rows += 1;
                    if chunk_rows >= VAC_CHUNK_ROWS {
                        let mut bound = enc.to_vec();
                        bound.push(0u8);
                        next_lower = Some(bound);
                        return true;
                    }
                    false
                });
            }
            if done || next_lower.is_none() {
                while let Some((key, mut group)) = groups.pop_first() {
                    vacuum_drain_group(&mut group, xmin, |row| to_delete.push(row), |_, _| {});
                    cur_raw = Some(key);
                }
            }
            #[cfg(test)]
            std::mem::drop(scan_probe);
            #[cfg(test)]
            note_vac_pending(to_delete.len());
            // 读锁已经放掉 ⇒ 这一块单独拿写锁删（以前是"整库攒完再删一次"）
            {
                #[cfg(test)]
                let _probe = crate::common::maintenance_probe::Timer::start(crate::common::maintenance_probe::Stage::VacuumDelete);
                let mut store = self.store.lock().unwrap();
                for enc in to_delete.drain(..) {
                    let _ = store.delete(&enc);
                    removed += 1;
                }
            }
            match next_lower {
                Some(l) if !done => lower = Some(l),
                _ => break,
            }
        }
        let next_cursor = if truncated { cur_raw } else { None };

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

/// M81 第 6 项：**停止落盘**。`snapshot` 为 `None`（裸 [`MVCC::new`]，四个 CLI 与所有既有测试的
/// 形状）时这里什么都不做 —— 一行字节都不写，改前行为一字不变。
///
/// ⚠️ 这是"优雅停止不丢"，**不是崩溃安全**：进程被杀、被 `std::process::exit` 收尾都不跑析构。
/// 所以统一 import 工具收尾必须先让句柄正常离开作用域，不能 `exit` 了事（方案第 6 项记的坑）。
impl Drop for MVCC {
    fn drop(&mut self) {
        let Some(path) = self.snapshot.clone() else {
            return;
        };
        match self.write_stop_snapshot(&path) {
            Ok(n) => log::info!("memory: 停止落盘 {n} 行 → {}", path.display()),
            Err(e) => log::warn!("memory 快照落盘失败（{}）: {e}", path.display()),
        }
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

/// M37：一次 `vacuum_bounded` 走查的**块大小**（物理行）—— 与 bitcask / lsm 同值，
/// 各家钉各家（守卫同名同形：`test_vacuum_deletes_in_bounded_chunks`）。
/// 这一家改的收益最小（600k 档 `vacuum()` 瞬时 4.0 MB），改它的理由是**同根因不留三家**：
/// 走查形状一模一样的 `to_delete` 攒法，四家如果只改两家，下一次改一家漏一家的事故还会重演。
const VAC_CHUNK_ROWS: usize = 8192;

// 测试专用（M37 的尺子）：单次 flush 前最多攒了多少待删行。
#[cfg(test)]
thread_local! {
    static VAC_PENDING_MAX: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn note_vac_pending(n: usize) {
    VAC_PENDING_MAX.with(|c| {
        let n = n as u64;
        if n > c.get() {
            c.set(n);
        }
    });
}

#[cfg(test)]
pub(crate) fn vac_pending_max() -> u64 {
    VAC_PENDING_MAX.with(|c| c.get())
}

#[cfg(test)]
pub(crate) fn reset_vac_pending() {
    VAC_PENDING_MAX.with(|c| c.set(0));
}

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

    fn write(&self, key: &[u8], value: Option<Vec<u8>>) -> bool {
        self.try_write(key, value).is_ok()
    }

    fn try_write(&self, key: &[u8], value: Option<Vec<u8>>) -> Result<(), crate::WriteError> {
        #[cfg(test)]
        replay_reference::note_row_lock();
        // 锁序：先活跃表，再存储侧（理由见 [`crate::common::TxnCore`] 的模块头）。
        // 那 8 步序列（冲突判定 → 编码 → WAL → 落行 → 写集登记）在 [`TxnCore::write`] 里，四引擎一份。
        let mut reg = self.core.lock_registry();
        let mut side = self.store.lock().unwrap();
        self.core.try_write(&mut reg, &mut *side, key, value)
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
        &self, start: Option<&[u8]>, end: Option<&[u8]>,
        accept: Option<&dyn Fn(&[u8]) -> bool>, after: Option<&[u8]>, budget: usize,
    ) -> (Vec<crate::common::WinCandidate<Option<Vec<u8>>>>, bool) {
        #[cfg(test)]
        if scan_reference::enabled() {
            return self.scan_candidates_reference(start, end, accept, after, budget);
        }
        let hi = end.map(|e| crate::common::order::range_end(e, self.core.version, false));
        let lo = crate::common::fold_lower_bound(start, after);
        let store = self.store.lock().unwrap();
        let mut fold = crate::common::order::CandidateFold::new(
            start, end, after, accept, self.core.version, budget);
        for (enc, value) in store.iter_entries_in(lo, hi.as_deref()) {
            #[cfg(test)]
            FOLD_KEYS.with(|c| c.set(c.get() + 1));
            // Retain a reference while selecting winners. Clone only final
            // winners under the same store lock; no second tree search.
            if fold.push(enc, |v| self.is_visible(v), || value) { break; }
        }
        let (rows, complete) = fold.finish();
        (rows.into_iter().map(|c| crate::common::WinCandidate {
            key: c.key, version: c.version, row: c.row.clone(),
        }).collect(), complete)
    }

    #[cfg(test)]
    fn scan_candidates_reference(
        &self, start: Option<&[u8]>, end: Option<&[u8]>,
        accept: Option<&dyn Fn(&[u8]) -> bool>, after: Option<&[u8]>, budget: usize,
    ) -> (Vec<crate::common::WinCandidate<Option<Vec<u8>>>>, bool) {
        let hi = end.map(|e| crate::common::order::range_end(e, self.core.version, false));
        let lo = crate::common::fold_lower_bound(start, after);
        let mut fold = crate::common::order::CandidateFold::new(
            start, end, after, accept, self.core.version, budget);
        let store = self.store.lock().unwrap();
        for enc in store.iter_keys_in(lo, hi.as_deref()) {
            #[cfg(test)]
            FOLD_KEYS.with(|c| c.set(c.get() + 1));
            if fold.push(enc, |v| self.is_visible(v), || enc.clone()) { break; }
        }
        let (rows, complete) = fold.finish();
        let rows = rows.into_iter().filter_map(|c| {
            store.get(&c.row).cloned().map(|row| crate::common::WinCandidate {
                key: c.key, version: c.version, row,
            })
        }).collect();
        (rows, complete)
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
        // M44：这条改成 [`Self::export_latest_visible_for_each`] 的**收集壳** ——
        // 墓碑/过期/读不到那三份判据从此全库只剩一份实现（在
        // [`crate::common::export_rows_streaming`]），"导出"这件事不再有两条会各自漂移的路径。
        // ⚠️ 返回整库的 `Vec` 就是这条的合同：600k 档实测一次 `export 全库(物化)` 峰值
        // **+123.7 MB**。只要"把行交出去"的人（导出到文件、搬几家、数个数）都该走流式那条。
        let mut out: Vec<ExportRecord> = Vec::new();
        self.export_latest_visible_for_each(include_deleted, |rec| {
            out.push(rec);
            true
        });
        out
    }

    /// 流式导出（M44）：折一行、判一行、立刻交给 `on`，**一行都不攒**；`on` 返回 `false`
    /// 就当场收手，后面的键一个都不再折。返回实际交出去的行数。
    ///
    /// 存在的理由：折叠侧从 M27 起就是流式的（[`crate::common::fold_rows_streaming`]），
    /// 可出口这一头只有 `export_latest_visible` —— 它返回 `Vec<ExportRecord>`，于是
    /// "导一个 600k 键的库到文件"要在内存里先把 600k 行摆好，等着一行一行写出去。
    /// 判据（墓碑/过期坏壳/读不到）住在共用件里，这一家只供"逻辑字节 → 用户字节"那一步。
    pub fn export_latest_visible_for_each(
        &self,
        include_deleted: bool,
        mut on: impl FnMut(ExportRecord) -> bool,
    ) -> usize {
        crate::common::export_rows_streaming(
            self,
            include_deleted,
            crate::memory::kv_ops::logical_to_user,
            &mut on,
        )
    }

    pub fn commit(&self) {
        self.try_commit().expect("transaction commit failed");
    }

    pub fn try_commit(&self) -> Result<(), crate::CommitError> {
        let mut reg = self.core.lock_registry();
        self.core.try_commit(&mut reg, &mut ())
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
    /// 同一 key 的编码按版本号升序，但可能被二进制前缀交错；借用反向迭代只复制命中键。
    /// `raw_key` 仍是 `key` 前缀的其它逻辑 key 会落进同一个字节区间（版本号首字节 0x00
    /// 小于任何 ASCII 字节），所以只能**跳过**而不是「见到别的 key 就停」。
    fn find_version_back(
        store: &MemStore,
        key: &[u8],
        accept: impl Fn(u64) -> bool,
    ) -> Option<Vec<u8>> {
        let low = encode_key(key, 0);
        let high = encode_key(key, u64::MAX);
        store.find_key_back(&low, &high, |enc| {
            decode_key(enc).is_some_and(|vk| vk.raw_key.as_slice() == key && accept(vk.version))
        })
    }
}

/// Freeze the M117 page apply and last-key lookup for the M121 comparison.
#[cfg(test)]
pub(crate) mod page_apply_reference {
    use std::{assert, cell::Cell, thread_local};
    thread_local! { static OLD: Cell<bool> = const { Cell::new(false) }; }
    pub(crate) fn enabled() -> bool { OLD.with(Cell::get) }
    pub(crate) struct Scope;
    impl Scope { pub(crate) fn start() -> Self { OLD.with(|s| assert!(!s.replace(true))); Self } }
    impl Drop for Scope { fn drop(&mut self) { OLD.with(|s| s.set(false)); } }
}

#[cfg(test)]
/// M109-only cost reference: retain M105 buffered IO and owned transaction keys.
pub(crate) mod replay_cost_reference {
    use std::{assert, cell::Cell, thread_local};
    thread_local! { static ENABLED: Cell<bool> = const { Cell::new(false) }; }
    pub(crate) fn enabled() -> bool { ENABLED.with(|c| c.get()) }
    pub(crate) struct Scope;
    impl Scope { pub(crate) fn start() -> Self { ENABLED.with(|c| assert!(!c.replace(true))); Self } }
    impl Drop for Scope { fn drop(&mut self) { ENABLED.with(|c| c.set(false)); } }
}

#[cfg(test)]
pub(crate) mod scan_reference {
    use std::{assert, cell::Cell, thread_local};
    thread_local! { static ENABLED: Cell<bool> = const { Cell::new(false) }; }
    pub(crate) fn enabled() -> bool { ENABLED.with(|c| c.get()) }
    pub(crate) struct Scope;
    impl Scope {
        pub fn start() -> Self { ENABLED.with(|c| assert!(!c.replace(true))); Self }
    }
    impl Drop for Scope { fn drop(&mut self) { ENABLED.with(|c| c.set(false)); } }
}

#[cfg(test)]
pub(crate) mod stop_reference {
    use std::{assert, cell::Cell, thread_local};
    thread_local! { static ENABLED: Cell<bool> = const { Cell::new(false) }; }
    pub(crate) fn enabled() -> bool { ENABLED.with(|c| c.get()) }
    pub(crate) struct Scope;
    impl Scope { pub fn start() -> Self { ENABLED.with(|c| assert!(!c.replace(true))); Self } }
    impl Drop for Scope { fn drop(&mut self) { ENABLED.with(|c| c.set(false)); } }
}

#[cfg(test)]
pub(crate) mod replay_reference {
    use std::{assert, cell::Cell, thread_local};
    thread_local! {
        static ENABLED: Cell<bool> = const { Cell::new(false) };
        static ROW_LOCKS: Cell<u64> = const { Cell::new(0) };
    }
    pub(crate) fn enabled() -> bool { ENABLED.with(|c| c.get()) }
    pub(crate) fn note_row_lock() { ROW_LOCKS.with(|c| c.set(c.get() + 1)); }
    pub(crate) fn row_locks() -> u64 { ROW_LOCKS.with(|c| c.get()) }
    pub(crate) struct Scope;
    impl Scope {
        pub fn start() -> Self { ENABLED.with(|c| assert!(!c.replace(true))); Self }
    }
    impl Drop for Scope { fn drop(&mut self) { ENABLED.with(|c| c.set(false)); } }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::assert;
    use std::assert_eq;
    use std::assert_ne;
    use std::panic;
    use std::eprintln;
    use std::vec;
    use std::format;

    #[test]
    fn snapshot_batches_keep_cross_batch_versions_and_prefixes() {
        use crate::common::{encode_key, pack_plain, pack_ttl, kv_snapshot::{snapshot_probe, read_snapshot_with, ValueBuffer}};
        let mut rows = Vec::new();
        for version in 1..=9000 { rows.push((encode_key(b"k", version), Some(pack_plain(&version.to_le_bytes())))); }
        rows.push((encode_key(b"", 1), None));
        rows.push((encode_key(b"k\0", 1), Some(pack_ttl(b"future", u64::MAX))));
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        let mut golden = None;
        for reference in [true, false] {
            let path = crate::common::ScratchPath::new("memory", "snapshot_batches", ".bxp");
            let _mode = reference.then(stop_rows_reference::Scope::start);
            let scope = snapshot_probe::Scope::start(false);
            let input = rows.clone().into_iter().map(|(k, v)| (Cow::Owned(k), v.map(Cow::Owned)));
            assert_eq!(MVCC::write_snapshot_rows(&path, input, 9001, |v| v <= 9000, "exclusive").unwrap(), 3);
            let stats = scope.stats();
            assert_eq!(stats.consumed_rows, 9002);
            assert_eq!(stats.input_batches, if reference { 0 } else { 2 });
            drop(scope);
            let bytes = std::fs::read(&path).unwrap();
            if let Some(expected) = &golden { assert_eq!(&bytes, expected); } else { golden = Some(bytes); }
            let mut records = Vec::new();
            read_snapshot_with(&path, ValueBuffer::Exact, &mut |r| { records.push(r); true }).unwrap();
            assert_eq!(records[0], crate::common::kv_snapshot::KvRecord::new(vec![], None));
            assert_eq!(records[1].value, Some(9000_u64.to_le_bytes().to_vec()));
            assert_eq!(records[2].key, b"k\0");
            assert_eq!(records[2].value, Some(b"future".to_vec()));
            assert_eq!(records[2].expire_unix_secs, Some(u64::MAX));
        }
    }

    #[test]
    fn scan_owned_pages_preserve_expiry_progress_and_visibility() {
        let db = MVCC::new();
        db.set_vacuum_auto_slice(0);
        let tx = db.begin_transaction();
        for i in 0..1003 {
            let key = format!("{i:08}").into_bytes();
            let logical = if i % 3 == 0 { crate::common::pack_ttl(b"expired", 0) }
                else if i % 3 == 1 { crate::common::pack_ttl(&[0, 1, 255], u64::MAX) }
                else { crate::common::pack_plain(b"") };
            assert!(tx.set(&key, logical));
        }
        assert!(tx.delete(b"00000002"));
        tx.try_commit().unwrap(); drop(tx);
        let held = db.begin_transaction();
        assert!(held.set_raw(b"00000001", vec![9]));
        let mut golden = None;
        for reference in [true, false] {
            let _mode = reference.then(crate::common::store::scan_cost_reference::Scope::start);
            let reader = db.begin_transaction();
            let full = reader.scan(None, None);
            assert_eq!(full.len(), 667);
            let mut page_rows = Vec::new();
            let mut cursor = None;
            let mut pages = 0;
            loop {
                let (page, next) = reader.scan_page(cursor.as_deref(), 1);
                pages += 1;
                assert!(pages < 1100, "expired groups must still advance the cursor");
                page_rows.extend(page);
                let Some(next) = next else { break; };
                assert!(cursor.as_ref().is_none_or(|old| old < &next));
                cursor = Some(next);
            }
            assert_eq!(page_rows, full);
            if let Some(expected) = &golden { assert_eq!(&full, expected); } else { golden = Some(full); }
            assert_eq!(page_rows[0].value, Some(vec![0, 1, 255]));
            assert_eq!(page_rows[0].expire_unix_secs, Some(u64::MAX));
        }
        held.rollback();
    }

    #[test]
    fn snapshot_stop_modes_preserve_bytes_and_shared_transactions() {
        use crate::common::kv_snapshot::{snapshot_probe, read_snapshot_with, ValueBuffer};
        let mut golden = None;
        for (mode, old_rows) in [("reference", false), ("exclusive", true), ("exclusive", false), ("shared", true), ("shared", false)] {
            let path = crate::common::ScratchPath::new("memory", "stop_modes", ".bxp");
            let mut db = MVCC::new();
            db.set_vacuum_auto_slice(0);
            let keys = [vec![], b"a".to_vec(), b"a\0".to_vec(), b"a\0\0".to_vec(), vec![255], vec![255, 0]];
            let tx = db.begin_transaction();
            for key in &keys { assert!(tx.set_raw(key, vec![0, 1, 255])); }
            tx.try_commit().unwrap(); drop(tx);
            let tx = db.begin_transaction();
            assert!(tx.delete(&keys[0]));
            assert!(tx.set(&keys[1], crate::common::pack_ttl(b"future", u64::MAX)));
            assert!(tx.set(&keys[2], crate::common::pack_ttl(b"expired", 0)));
            assert!(tx.set_raw(&keys[3], vec![]));
            tx.try_commit().unwrap(); drop(tx);
            let held = (mode == "shared").then(|| db.begin_transaction());
            if let Some(tx) = &held { assert!(tx.set_raw(b"uncommitted", vec![7])); }
            let reference = (mode == "reference").then(stop_reference::Scope::start);
            let rows_reference = old_rows.then(stop_rows_reference::Scope::start);
            let scope = snapshot_probe::Scope::start(false);
            let clones = crate::common::codec::user_value_copies();
            assert_eq!(db.write_stop_snapshot(&path).unwrap(), 6);
            let stats = scope.stats();
            assert_eq!(stats.stop_mode, mode);
            if mode != "reference" { assert_eq!(crate::common::codec::user_value_copies(), clones); }
            if mode == "exclusive" {
                assert_eq!(stats.consumed_rows, 10);
                assert_eq!(db.store.lock().unwrap().iter_keys().count(), 0, "exclusive snapshot must consume its input tree");
            } else { assert!(db.store.lock().unwrap().iter_keys().count() >= 10); }
            if mode != "reference" && !old_rows {
                assert_eq!(stats.input_batches, 1);
                assert_eq!(stats.input_buffer_bytes, stats.visited_rows.min(8192) as usize * std::mem::size_of::<(Cow<'_, [u8]>, Option<Cow<'_, [u8]>>)>());
                assert!(stats.pull_ns > 0 && stats.fold_emit_ns > 0);
                assert!(stats.pull_ns + stats.fold_emit_ns <= stats.body_ns);
            } else { assert_eq!(stats.input_batches, 0); }
            drop(scope); drop(reference); drop(rows_reference);
            let bytes = std::fs::read(&path).unwrap();
            if let Some(expected) = &golden { assert_eq!(&bytes, expected); } else { golden = Some(bytes); }
            let mut records = Vec::new();
            read_snapshot_with(&path, ValueBuffer::Exact, &mut |rec| { records.push(rec); true }).unwrap();
            assert_eq!(records[0].value, None);
            assert_eq!(records[1].value, Some(b"future".to_vec()));
            assert_eq!(records[1].expire_unix_secs, Some(u64::MAX));
            assert_eq!(records[2].value, None);
            assert_eq!(records[3].value, Some(vec![]));
            if let Some(tx) = held {
                assert_eq!(tx.get(b"uncommitted"), Some(vec![7]));
                assert_eq!(tx.get(&keys[4]), Some(vec![0, 1, 255]));
                tx.rollback();
            }
            let mut reopened = MVCC::new();
            reopened.load_snapshot_from(&path).unwrap();
            assert_eq!(reopened.export_latest_visible(true).len(), 6);
        }
    }

    #[test]
    fn ordered_writes_skip_only_proven_absent_version_searches() {
        const ROWS: u64 = 1003;
        for reference in [true, false] {
            let _mode = reference.then(replay_cost_reference::Scope::start);
            let db = MVCC::new();
            let tx = db.begin_transaction();
            let probe = crate::memory::mem_store::key_lookup_probe::Scope::start();
            for i in 0..ROWS { assert!(tx.set_raw(&i.to_be_bytes(), vec![1])); }
            let insert = probe.stats();
            assert_eq!(insert.searches, if reference { ROWS } else { 0 });
            assert_eq!(insert.end_misses, if reference { 0 } else { ROWS });
            for i in (0..ROWS).rev() { assert!(tx.set_raw(&i.to_be_bytes(), vec![2])); }
            let update = probe.stats();
            assert_eq!(update.searches - insert.searches, ROWS);
            assert_eq!(update.bound_grows, if reference { 0 } else { 1 });
            drop(probe);
            assert_eq!(tx.get(&0u64.to_be_bytes()), Some(vec![2]));
            tx.rollback();
            assert_eq!(db.health().total_rows, 0);
        }
    }

    #[test]
    fn replay_owned_page_uses_one_registry_lookup_and_preserves_duplicate_keys() {
        for reference in [true, false] {
            let mode = reference.then(page_apply_reference::Scope::start);
            let db = MVCC::new();
            let mut page = vec![
                (b"a".to_vec(), Some(vec![1]), None),
                (b"a\0".to_vec(), Some(vec![2]), Some(u64::MAX)),
                (b"a".to_vec(), Some(vec![3]), None),
                (b"tomb".to_vec(), None, None),
            ];
            let before = crate::common::txn::write_registry_lookups();
            MVCC::replay_page(&db, &mut page).unwrap();
            assert_eq!(crate::common::txn::write_registry_lookups() - before, if reference { 4 } else { 1 });
            assert!(page.is_empty());
            assert_eq!(db.health().total_rows, 3);
            let tx = db.begin_transaction();
            assert_eq!(tx.get(b"a"), Some(vec![3]));
            assert_eq!(tx.get(b"a\0"), Some(vec![2]));
            assert_eq!(tx.get(b"tomb"), None);
            assert_eq!(db.export_latest_visible(true).into_iter().find(|r| r.key == b"a\0").unwrap().expire_unix_secs, Some(u64::MAX));
            tx.try_commit().unwrap();
            assert!(db.active_txn.lock().unwrap().is_empty());
            drop(mode);
        }
    }

    #[test]
    fn replay_failure_rolls_back_entire_page() {
        for reference in [true, false] {
        let _mode = reference.then(page_apply_reference::Scope::start);
        let db = MVCC::new();
        let held = db.begin_transaction();
        assert!(held.set_raw(b"locked", vec![7]));
        let mut batch = vec![
            (b"fresh".to_vec(), Some(vec![1]), None),
            (b"locked".to_vec(), Some(vec![2]), None),
            (b"unread".to_vec(), Some(vec![3]), None),
        ];
        let scope = crate::common::kv_snapshot::recovery_probe::Scope::start();
        let error = MVCC::replay_page(&db, &mut batch);
        let stages = scope.stats();
        assert_eq!(stages.replay_input_rows, 3);
        assert_eq!(stages.replay_pages, 1);
        assert!(stages.replay_encode_ns > 0 && stages.replay_apply_ns > 0);
        assert_eq!(stages.replay_commit_ns, 0, "a rejected page must never reach commit");
        assert!(error.is_err());
        assert!(batch.is_empty());
        assert_eq!(db.health().total_rows, 1, "a rejected page must not publish its earlier rows");
        assert_eq!(held.get(b"locked"), Some(vec![7]));
        assert_eq!(db.active_txn.lock().unwrap().len(), 1);
        held.rollback();
        }
    }

    #[test]
    fn memory_owned_write_retains_encoded_key_buffer() {
        let mut store = MemStore::new();
        let key = encode_key(b"owned", 1);
        let ptr = key.as_ptr();
        assert!(store.put_versioned_owned(key, Some(vec![3])));
        assert_eq!(store.iter_keys_in(None, None).next().unwrap().as_ptr(), ptr);
    }

    #[test]
    fn memory_owned_write_retains_registry_key_buffer() {
        for page in [false, true] {
        let db = MVCC::new();
        let tx = db.begin_transaction();
        let key = b"owned".to_vec();
        let ptr = key.as_ptr();
        {
            let mut reg = tx.core.lock_registry();
            let mut store = db.store.lock().unwrap();
            if page {
                tx.core.try_write_owned_page(&mut reg, &mut *store, vec![(key, Some(vec![3]))].into_iter()).unwrap();
            } else {
                tx.core.try_write_owned(&mut reg, &mut *store, key, Some(vec![3])).unwrap();
            }
            assert_eq!(reg[&tx.core.version].get(b"owned".as_slice()).unwrap().as_ptr(), ptr);
        }
        tx.rollback();
        assert_eq!(db.health().total_rows, 0);
        }
    }

    #[test]
    fn memory_owned_write_keeps_conflict_and_closed_checks() {
        let db = MVCC::new();
        let older = db.begin_transaction();
        let newer = db.begin_transaction();
        assert!(newer.set_raw(b"k", vec![7]));
        newer.commit();
        {
            let mut reg = older.core.lock_registry();
            let mut store = db.store.lock().unwrap();
            assert_eq!(older.core.try_write_owned(&mut reg, &mut *store, b"k".to_vec(), Some(vec![8])),
                Err(crate::WriteError::Conflict));
            assert_eq!(older.core.try_write_owned_page(&mut reg, &mut *store,
                vec![(b"k".to_vec(), Some(vec![8]))].into_iter()), Err(crate::WriteError::Conflict));
        }
        older.rollback();
        {
            let mut reg = older.core.lock_registry();
            let mut store = db.store.lock().unwrap();
            assert_eq!(older.core.try_write_owned(&mut reg, &mut *store, b"new".to_vec(), None),
                Err(crate::WriteError::TransactionClosed));
            assert_eq!(older.core.try_write_owned_page(&mut reg, &mut *store, std::iter::empty()),
                Err(crate::WriteError::TransactionClosed));
        }
        assert_eq!(db.begin_transaction().get(b"k"), Some(vec![7]));
    }

    #[test]
    fn snapshot_owned_replay_preserves_duplicates_prefixes_and_ttl() {
        use crate::common::kv_snapshot::{KvRecord, atomic_write_bin_v4};
        let path = crate::common::ScratchPath::new("memory", "owned_replay", ".bxp");
        let expiry = crate::common::now_unix_secs() + 3600;
        atomic_write_bin_v4(&path, |writer| {
            for i in 0..1003u64 {
                writer.write(&KvRecord::new(i.to_be_bytes().to_vec(), Some(i.to_le_bytes().to_vec())))?;
            }
            writer.write(&KvRecord::new(0u64.to_be_bytes().to_vec(), Some(vec![99])))?;
            writer.write(&KvRecord::new(1u64.to_be_bytes().to_vec(), None))?;
            writer.write(&KvRecord::new(vec![], Some(vec![0, 1])))?;
            writer.write(&KvRecord::new(vec![0], Some(vec![1, 0])).with_expire(expiry))?;
            writer.write(&KvRecord::new(b"expired".to_vec(), Some(vec![8])).with_expire(1))?;
            Ok(())
        }).unwrap();
        let mut expected = BTreeMap::new();
        for i in 0..1003u64 { expected.insert(i.to_be_bytes().to_vec(), (Some(i.to_le_bytes().to_vec()), None)); }
        expected.insert(0u64.to_be_bytes().to_vec(), (Some(vec![99]), None));
        expected.insert(1u64.to_be_bytes().to_vec(), (None, None));
        expected.insert(vec![], (Some(vec![0, 1]), None));
        expected.insert(vec![0], (Some(vec![1, 0]), Some(expiry)));
        for (reference, cost_reference) in [(true, true), (true, false), (false, true), (false, false)] {
            let _mode = reference.then(crate::common::txn::owned_reference::Scope::start);
            let _cost_mode = cost_reference.then(replay_cost_reference::Scope::start);
            let recovery = crate::common::kv_snapshot::recovery_probe::Scope::start();
            let mut db = MVCC::new();
            db.load_snapshot_from(&path).unwrap();
            let stages = recovery.stats();
            assert!(stages.verify_ns > 0 && stages.decode_ns > 0 && stages.replay_ns > 0);
            assert!(stages.total_ns >= stages.verify_ns + stages.decode_ns + stages.replay_ns);
            assert_eq!(stages.replay_input_rows, 1007);
            assert_eq!(stages.replay_pages, 2);
            assert!(stages.replay_setup_ns > 0 && stages.replay_encode_ns > 0 && stages.replay_apply_ns > 0 && stages.replay_commit_ns > 0);
            assert!(stages.replay_ns >= stages.replay_setup_ns + stages.replay_encode_ns + stages.replay_apply_ns + stages.replay_commit_ns);
            drop(recovery);
            assert!(crate::common::kv_snapshot::recovery_probe::Timer::start(crate::common::kv_snapshot::recovery_probe::Kind::Total).is_none());
            let actual: BTreeMap<_, _> = db.export_latest_visible(true).into_iter()
                .map(|r| (r.key, (r.value, r.expire_unix_secs))).collect();
            assert_eq!(actual, expected);
            assert!(db.active_txn.lock().unwrap().is_empty());
        }
    }

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
    /// `vacuum()` 必须**按块**删除，不许把整库的待删行攒在一个 `Vec` 里（M37）。
    ///
    /// 判据与 bitcask / lsm 那条同形同值：可删旧版远多于一块时，单次 flush 前最多攒的待删行
    /// `vac_pending_max()` ≤ `2 × VAC_CHUNK_ROWS`；`versions_removed > VAC_CHUNK_ROWS` 保证块界
    /// 真的被跨过（否则空转）。这一家改的收益最小（4.0 MB），但形状是同一份 —— 留下它就是
    /// "改一家漏两家"那个事故形状的复刻。
    ///
    /// 变异验证：把块界临时抬到一整库那么大 ⇒ 待删行数变成 20 000 > 界 16 384，红。
    #[test]
    fn test_vacuum_deletes_in_bounded_chunks() {
        const KEYS: usize = 20_000;
        let mvcc = MVCC::new();
        mvcc.set_vacuum_auto_slice(0);
        for _ in 0..2u64 {
            let tx = mvcc.begin_transaction();
            for i in 0..KEYS {
                let k = format!("k{i:06}");
                assert!(tx.set(k.as_bytes(), vec![b'v'; 24]), "第 {i} 把键写失败");
            }
            tx.commit();
        }
        reset_vac_pending();
        let st = mvcc.vacuum();
        let pending = vac_pending_max();
        eprintln!(
            "[vac chunks memory] 裁掉 {} 行 / 扫 {} 行 ⇒ 单次 flush 前最多攒 {} 行（块界 {VAC_CHUNK_ROWS}）",
            st.versions_removed, st.rows_scanned, pending
        );
        assert!(st.rows_scanned > 0, "一行都没扫到 ⇒ 空转");
        assert!(
            st.versions_removed > VAC_CHUNK_ROWS,
            "可删行数 {} 没超过一块 ⇒ 这条判据量不到界（空转）",
            st.versions_removed
        );
        assert!(pending > 0, "这把尺子没数到待删行 ⇒ 空转");
        assert!(
            pending <= 2 * VAC_CHUNK_ROWS as u64,
            "单次 flush 前攒了 {pending} 行待删（界 {}）⇒ 又变回整库一个 `Vec`",
            2 * VAC_CHUNK_ROWS as u64
        );
        let tx = mvcc.begin_transaction();
        for i in (0..KEYS).step_by(997) {
            let k = format!("k{i:06}");
            assert_eq!(
                tx.get(k.as_bytes()),
                Some(vec![b'v'; 24]),
                "第 {i} 把键在按块回收后读不回来"
            );
        }
        tx.commit();
    }

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
    /// `scan_foreach` 提前叫停时，**不许先把整库折一遍**（M27）。
    ///
    /// 这条路径的文档从写下那天起就声称「不收集全库到单一大 Vec」，而实现是
    /// `for … in collect_latest_raw_in(...)` —— 那一句会把界内每一行都先折出来，再逐条搬成
    /// `ExportRecord`。200k 键实测因此比结果集本身还多占 ≈ 15 MB（`一次全量 scan`
    /// 峰值 +45.5 MB / 常驻 30.7 MB）。现在走共用件 `fold_rows_streaming`：折一行交一行，
    /// 叫停之后的键不再碰。
    ///
    /// 内存引擎没有「取值」这一步（行就在手里），所以能数的是**折叠走过的键数**；
    /// 判据同 LSM：按比例给，老写法是比例 1.0。
    #[test]
    fn test_scan_foreach_stops_without_folding_the_library() {
        const N: usize = 3 * 4096 + 100;
        fn ruler() -> u64 {
            crate::memory::memory_mvcc::fold_keys()
        }
        {
            let mvcc = MVCC::new();
            for base in (0..N).step_by(2000) {
                let end = (base + 2000).min(N);
                let items: Vec<(Vec<u8>, Vec<u8>)> = (base..end)
                    .map(|i| {
                        (
                            format!("k{i:05}").into_bytes(),
                            format!("v{i}").into_bytes(),
                        )
                    })
                    .collect();
                let tx = mvcc.begin_transaction();
                if let Err(bad) = tx.batch_set(&items) {
                    panic!("灌第 {base} 批: {}", String::from_utf8_lossy(&bad));
                }
                tx.commit();
            }
            let tx = mvcc.begin_transaction();
            let base = ruler();
            let mut handed = 0usize;
            tx.scan_foreach(None, None, |rec| {
                assert!(rec.value.is_some(), "交出来的行没有值");
                handed += 1;
                false // 只要第一行
            });
            let early = ruler() - base;
            let base = ruler();
            let all = tx.scan(None, None);
            let full = ruler() - base;
            tx.commit();

            assert_eq!(handed, 1, "回调明明叫停了");
            assert_eq!(all.len(), N, "参照量：全量 scan 要折出所有行");
            assert!(
                early * 2 <= full,
                "只要 1 行就走了 {early} 把键，全量也才 {full} ⇒ 没提前收手"
            );
            eprintln!("[scan_foreach 早停] 交 1 行付 {early}，全量 {N} 行付 {full}");
        }
    }

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

#[cfg(test)]
pub(crate) mod vacuum_cost_reference {
    use std::{assert, cell::Cell, thread_local};
    thread_local! { static ENABLED: Cell<bool> = const { Cell::new(false) }; }
    pub(crate) fn enabled() -> bool { ENABLED.with(Cell::get) }
    pub(crate) struct Scope;
    impl Scope { pub(crate) fn start() -> Self { ENABLED.with(|s| assert!(!s.replace(true))); Self } }
    impl Drop for Scope { fn drop(&mut self) { ENABLED.with(|s| s.set(false)); } }
}

#[cfg(test)]
pub(crate) mod stop_rows_reference {
    use std::{assert, cell::Cell, thread_local};
    thread_local! { static ENABLED: Cell<bool> = const { Cell::new(false) }; }
    pub(crate) fn enabled() -> bool { ENABLED.with(Cell::get) }
    pub(crate) struct Scope;
    impl Scope { pub(crate) fn start() -> Self { ENABLED.with(|s| assert!(!s.replace(true))); Self } }
    impl Drop for Scope { fn drop(&mut self) { ENABLED.with(|s| s.set(false)); } }
}
