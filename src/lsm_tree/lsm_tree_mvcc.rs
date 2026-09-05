//! MVCC 事务层 + WAL 崩溃恢复（适配 LSM 后端）
//!
//! 从 `bplus-tree` 移植：协议与语义保持一致，存储后端由 B+ 树换成 [`LSMTree`]。
//!
//! # 两条写路径（吞吐差几个数量级）
//!
//! | 路径 | API | 持久化 | 适用 |
//! |------|-----|--------|------|
//! | **普通事务** | `begin_transaction` → `put` → `commit` | 每 commit **WAL fsync**；默认不 flush SST | 正确性 / 小批量 / 可崩溃恢复 |
//! | **Bulk load** | `begin_bulk` → `put_batch_owned` → `finish` | 无事务 WAL；直接写 L0 SST；finish 落 MANIFEST | 导入 / 压测（10M/10min） |
//!
//! 10M/10min 目标靠 **bulk** 达成。若每条 `commit`（含 fsync），本机约数千 ops/s，
//! 10M 需数十分钟到数小时——属预期，不是 bug。详见 README「写路径对比」。
//!
//! 提交协议（简化版 ARIES）：
//! 1. 写路径：先 append WAL Write，再改内存 MemTable
//! 2. commit：append WAL Commit → **fsync WAL**（持久化点）→ 可选 flush 快照
//! 3. 崩溃恢复：重放已提交 Write，撤销未提交 Write，再 checkpoint 截断 WAL
//!
//! **Bulk load 模式**（[`MVCC::begin_bulk`]）：
//! - 关闭 WAL / 冲突检测 / 活跃事务表
//! - 结束时 [`BulkLoader::finish`] → flush + checkpoint（默认不 compact L0）
//! - 需要收口 L0 时用 [`BulkLoader::finish_with_compact`]
//!
//! 与 bplus 的差异：
//! - 无 Blob 外置：value 直接内联进快照（后续可接 SST / value log）
//! - Checkpoint 的 `root_page_id` / `next_page_id` 固定写 0
//! - 数据目录：`dir/CURRENT.snap` + `dir/lsm.wal` + `dir/lsm.lock`

use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use crate::lsm_tree::lsm_tree::{LSMTree, LSMTreeOptions};
use crate::lsm_tree::storage::{wal_path, FileLock};
use crate::lsm_tree::wal::{analyze_recovery, Wal, WalRecord};
pub use crate::common::{ExportRecord, VacuumStats};

/// MVCC 存储引擎（LSM 后端）
pub struct MVCC {
    kv: Arc<Mutex<LSMTree>>,
    wal: Arc<Mutex<Wal>>,
    active_txn: Arc<Mutex<HashMap<u64, Vec<Vec<u8>>>>>,
    next_version: Arc<AtomicU64>,
    db_dir: PathBuf,
    /// 持有期间保证单写者
    _lock: FileLock,
}

/// 普通 MVCC 事务
pub struct Transaction {
    pub(crate) kv: Arc<Mutex<LSMTree>>,
    wal: Arc<Mutex<Wal>>,
    pub(crate) active_txn: Arc<Mutex<HashMap<u64, Vec<Vec<u8>>>>>,
    version: u64,
    active_xid: HashSet<u64>,
}

/// 批量导入会话：无 WAL、无冲突检测，单写者假定
pub struct BulkLoader {
    kv: Arc<Mutex<LSMTree>>,
    wal: Arc<Mutex<Wal>>,
    next_version: Arc<AtomicU64>,
    version: u64,
    finished: bool,
}

/// 逻辑键 + 版本（调试用）
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VersionedKey {
    pub raw_key: Vec<u8>,
    pub version: u64,
}

/// 编码：`raw_key || version.to_be_bytes()`
pub fn encode_key(raw_key: &[u8], version: u64) -> Vec<u8> {
    let mut enc = Vec::with_capacity(raw_key.len() + 8);
    enc.extend_from_slice(raw_key);
    enc.extend_from_slice(&version.to_be_bytes());
    enc
}

/// 解码
pub fn decode_key(enc: &[u8]) -> Option<VersionedKey> {
    if enc.len() < 8 {
        return None;
    }
    let split = enc.len() - 8;
    let mut ver_bytes = [0u8; 8];
    ver_bytes.copy_from_slice(&enc[split..]);
    Some(VersionedKey {
        raw_key: enc[..split].to_vec(),
        version: u64::from_be_bytes(ver_bytes),
    })
}

impl MVCC {
    /// 打开/创建磁盘上的 MVCC 引擎（含 WAL 恢复）。失败时 panic（兼容旧代码）。
    pub fn open(dir: impl AsRef<Path>) -> Self {
        Self::try_open(dir).map_err(|e| format!("MVCC::open 失败: {e}")).unwrap()
    }

    /// 打开/创建，错误以 `io::Result` 返回（锁冲突、坏文件、IO 等）。
    pub fn try_open(dir: impl AsRef<Path>) -> std::io::Result<Self> {
        let db_dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&db_dir)?;

        let lock = FileLock::try_acquire(&db_dir)?;
        let mut tree = LSMTree::open_with_config(&db_dir, LSMTreeOptions::for_mvcc())?;
        let mut wal = Wal::open(wal_path(&db_dir))?;

        let entries = wal.read_all()?;
        let plan = analyze_recovery(&entries);

        for (_xid, key, value) in &plan.committed_writes {
            tree.insert(key.clone(), value.clone());
        }
        for (_xid, key) in &plan.uncommitted_writes {
            let _ = tree.remove(key);
        }

        // 不再全表 scan_max_version：用 checkpoint / WAL xid / redo key / 持久化元数据
        let start_ver = compute_start_version(&db_dir, &plan);

        tree.flush()?;
        let _ = wal.append(WalRecord::Checkpoint {
            next_version: start_ver,
            root_page_id: 0,
            next_page_id: 0,
        });
        wal.sync()?;
        wal.truncate()?;
        persist_mvcc_next(&db_dir, start_ver);

        Ok(Self {
            kv: Arc::new(Mutex::new(tree)),
            wal: Arc::new(Mutex::new(wal)),
            active_txn: Arc::new(Mutex::new(HashMap::new())),
            next_version: Arc::new(AtomicU64::new(start_ver)),
            db_dir,
            _lock: lock,
        })
    }

    /// 在系统临时目录创建匿名库（测试用）
    pub fn new() -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "lsm_mvcc_anon_{}_{}",
            std::process::id(),
            nanos
        ));
        Self::open(path)
    }

    pub fn begin_transaction(&self) -> Transaction {
        Transaction::begin(
            self.kv.clone(),
            self.wal.clone(),
            self.active_txn.clone(),
            self.next_version.clone(),
        )
    }

    /// 开启 bulk load：单写者、无 WAL、无冲突检测。
    ///
    /// **崩溃不安全**：finish 之前进程挂掉可能丢数据。
    /// 导入结束后必须 `finish()` / `finish_with_compact(...)`。
    ///
    /// 为 10M 级吞吐：`put_batch_owned` 排序后直接写 L0 SST（跳过 MemTable）。
    pub fn begin_bulk(&self) -> BulkLoader {
        {
            let mut kv = self.kv.lock().unwrap();
            let mut o = kv.options().clone();
            o.mem_threshold_bytes = 128 * 1024 * 1024;
            o.mem_threshold_entries = 1_000_000;
            o.l0_compact_threshold = 16;
            o.l0_slowdown_trigger = 32;
            o.l0_stop_trigger = 64;
            o.target_sst_bytes = 8 * 1024 * 1024;
            o.level_base_bytes = 64 * 1024 * 1024;
            o.enable_bg_compact = false;
            o.enable_mem_wal = false;
            o.max_compactions_per_flush = 1;
            kv.set_options(o);
            kv.set_bulk_mode(true);
        }
        let version = self.next_version.fetch_add(1, Ordering::SeqCst);
        BulkLoader {
            kv: self.kv.clone(),
            wal: self.wal.clone(),
            next_version: self.next_version.clone(),
            version,
            finished: false,
        }
    }

    pub fn checkpoint(&self) {
        let mut kv = self.kv.lock().unwrap();
        let mut wal = self.wal.lock().unwrap();
        let _ = kv.flush();
        let next_ver = self.next_version.load(Ordering::SeqCst);
        let _ = wal.append(WalRecord::Checkpoint {
            next_version: next_ver,
            root_page_id: 0,
            next_page_id: 0,
        });
        let _ = wal.sync();
        let _ = wal.truncate();
        persist_mvcc_next(&self.db_dir, next_ver);
    }

    pub fn flush(&self) {
        let _ = self.kv.lock().unwrap().flush();
    }

    pub fn db_dir(&self) -> &Path {
        &self.db_dir
    }

    /// 导出当前快照下「每个逻辑 key 的最新可见版本」。
    pub fn export_latest_visible(&self, include_deleted: bool) -> Vec<ExportRecord> {
        let tx = self.begin_transaction();
        let records = tx.export_latest_visible(include_deleted);
        tx.commit();
        records
    }

    /// 版本回收（vacuum）。
    ///
    /// 规则：
    /// - `xmin` = 活跃事务 id 最小值；无活跃事务时为 `next_version`
    /// - 每个逻辑 key：只保留「最后一个 version < xmin」+ 全部 version >= xmin
    /// - 若 xmin 之前最终是 tombstone 且无更新版本 → 整 key 删除
    pub fn vacuum(&self) -> std::io::Result<VacuumStats> {
        let active = self.active_txn.lock().unwrap();
        let xmin = if active.is_empty() {
            self.next_version.load(Ordering::SeqCst)
        } else {
            *active.keys().min().unwrap()
        };
        drop(active);

        let mut kv = self.kv.lock().unwrap();

        let all: Vec<(Vec<u8>, Option<Vec<u8>>)> = kv.iter().collect();
        let mut by_key: BTreeMap<Vec<u8>, Vec<(u64, Vec<u8>, Option<Vec<u8>>)>> = BTreeMap::new();
        for (enc, val) in all {
            let Some(vk) = decode_key(&enc) else {
                continue;
            };
            by_key
                .entry(vk.raw_key)
                .or_default()
                .push((vk.version, enc, val));
        }

        let mut removed = 0usize;
        let mut to_delete: Vec<Vec<u8>> = Vec::new();

        for (_raw, mut versions) in by_key {
            versions.sort_by_key(|(v, _, _)| *v);
            let mut last_old: Option<usize> = None;
            for (i, (ver, _, _)) in versions.iter().enumerate() {
                if *ver < xmin {
                    last_old = Some(i);
                }
            }
            if let Some(keep_old) = last_old {
                for i in 0..keep_old {
                    to_delete.push(versions[i].1.clone());
                    removed += 1;
                }
                let old_is_tomb = versions[keep_old].2.is_none();
                let has_newer = versions.iter().any(|(v, _, _)| *v >= xmin);
                if old_is_tomb && !has_newer {
                    to_delete.push(versions[keep_old].1.clone());
                    removed += 1;
                }
            }
        }

        for enc in &to_delete {
            let _ = kv.remove(enc);
        }

        kv.flush()?;
        // 后续 compact 丢弃 ver < xmin 的 tombstone 版本键
        kv.set_mvcc_xmin_filter(xmin);

        Ok(VacuumStats {
            xmin,
            versions_removed: removed,
            blob_rewritten: None,
        })
    }
}

impl Default for MVCC {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for MVCC {
    fn drop(&mut self) {
        // 进程正常退出时落盘 next_version，避免下次 open 回退
        let next = self.next_version.load(Ordering::SeqCst);
        persist_mvcc_next(&self.db_dir, next);
    }
}


/// 持久化文件：`dir/MVCC.NEXT`（WAL truncate 后仍保留 next_version）
fn mvcc_next_path(dir: &Path) -> PathBuf {
    dir.join("MVCC.NEXT")
}

fn load_mvcc_next(dir: &Path) -> u64 {
    let p = mvcc_next_path(dir);
    let Ok(s) = std::fs::read_to_string(p) else {
        return 0;
    };
    s.trim().parse().unwrap_or(0)
}

fn persist_mvcc_next(dir: &Path, next: u64) {
    let p = mvcc_next_path(dir);
    let _ = std::fs::write(p, format!("{next}\n"));
}

/// 从 checkpoint / WAL / redo 键 / MVCC.NEXT 推算起始版本，**不扫 SST 全表**
fn compute_start_version(dir: &Path, plan: &crate::lsm_tree::wal::RecoveryPlan) -> u64 {
    let mut start = 1u64;

    // 1) 上次成功 open/checkpoint 留下的元数据（WAL 可能已被 truncate）
    start = start.max(load_mvcc_next(dir));

    // 2) WAL 里最后一个 Checkpoint
    if let Some(crate::lsm_tree::wal::WalRecord::Checkpoint { next_version, .. }) = &plan.last_checkpoint {
        start = start.max(*next_version);
    }

    // 3) WAL 见过的最大 xid → 下一个可用
    start = start.max(plan.max_xid.saturating_add(1));

    // 4) redo/undo 键上的 version 后缀（仅 WAL 里的键，不是全库）
    for (_xid, key, _) in &plan.committed_writes {
        if let Some(vk) = decode_key(key) {
            start = start.max(vk.version.saturating_add(1));
        }
    }
    for (_xid, key) in &plan.uncommitted_writes {
        if let Some(vk) = decode_key(key) {
            start = start.max(vk.version.saturating_add(1));
        }
    }

    start.max(1)
}

impl BulkLoader {
    pub fn put(&mut self, key: &[u8], value: Vec<u8>) {
        let enc = encode_key(key, self.version);
        let mut kv = self.kv.lock().unwrap();
        kv.insert_fast(enc, Some(value));
    }

    /// 批量 put：一次加锁插入多条（clone value）
    pub fn put_batch(&mut self, items: &[(Vec<u8>, Vec<u8>)]) {
        let mut kv = self.kv.lock().unwrap();
        let ver = self.version;
        kv.insert_batch_fast(items.iter().map(|(k, v)| (encode_key(k, ver), Some(v.clone()))));
    }

    /// 零额外 clone 的批量 put（消费 items）——压测主路径
    /// 按 key 排序后 **直接写 L0 SST**，跳过 MemTable。
    pub fn put_batch_owned(&mut self, mut items: Vec<(Vec<u8>, Vec<u8>)>) {
        if items.is_empty() {
            return;
        }
        // 若已有序（顺序导入）跳过 sort
        let sorted = items.windows(2).all(|w| w[0].0 <= w[1].0);
        if !sorted {
            items.sort_by(|a, b| a.0.cmp(&b.0));
        }
        let ver = self.version;
        let mut out: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::with_capacity(items.len());
        for (k, v) in items {
            let enc = encode_key(&k, ver);
            if let Some((last_k, _)) = out.last() {
                if last_k == &enc {
                    out.pop();
                }
            }
            out.push((enc, Some(v)));
        }
        let mut kv = self.kv.lock().unwrap();
        let _ = kv.bulk_ingest_sorted(out);
    }

    /// 预编码 key 的批量 put（调用方已 encode_key 且排序）
    pub fn put_encoded_batch(&mut self, mut items: Vec<(Vec<u8>, Option<Vec<u8>>)>) {
        if items.is_empty() {
            return;
        }
        items.sort_by(|a, b| a.0.cmp(&b.0));
        let mut out = Vec::with_capacity(items.len());
        for (k, v) in items {
            if let Some((last_k, _)) = out.last() {
                if last_k == &k {
                    out.pop();
                }
            }
            out.push((k, v));
        }
        let mut kv = self.kv.lock().unwrap();
        let _ = kv.bulk_ingest_sorted(out);
    }

    pub fn delete(&mut self, key: &[u8]) {
        let enc = encode_key(key, self.version);
        let mut kv = self.kv.lock().unwrap();
        kv.insert_fast(enc, None);
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let kv = self.kv.lock().unwrap();
        let low = encode_key(key, 0);
        let high = encode_key(key, u64::MAX);
        let mut best: Option<Vec<u8>> = None;
        for (enc, val) in kv.range_scan(low, high) {
            if let Some(vk) = decode_key(&enc) {
                if vk.raw_key.as_slice() == key && vk.version <= self.version {
                    best = val;
                }
            }
        }
        best
    }

    /// 中途可手动 flush，降低峰值内存
    pub fn flush_mem(&mut self) {
        let _ = self.kv.lock().unwrap().flush();
    }

    /// 结束 bulk：flush MANIFEST + WAL checkpoint。
    ///
    /// **默认不做大规模 L0 compact**（10M 级会卡很久）。
    /// 需要收口 L0 时用 [`finish_with_compact`]。
    pub fn finish(self) {
        self.finish_with_compact(0);
    }

    /// 结束 bulk，并最多 compact `compact_limit` 轮 L0→L1。
    ///
    /// - `compact_limit == 0`：与 [`finish`] 相同，只落盘不 compact
    /// - `compact_limit > 0`：调用 [`LSMTree::compact_l0_until`]，目标 L0 ≤ `l0_compact_threshold - 1`
    ///
    /// 大批导入后读延迟敏感时建议 `finish_with_compact(16)` 之类；纯压测吞吐用 `finish()`。
    pub fn finish_with_compact(mut self, compact_limit: usize) {
        self.finish_inner(compact_limit);
    }

    fn finish_inner(&mut self, compact_limit: usize) {
        if self.finished {
            return;
        }
        self.finished = true;
        let mut kv = self.kv.lock().unwrap();
        let mut wal = self.wal.lock().unwrap();
        // bulk 期间可能未写完 MANIFEST：flush 空 mem 并落盘清单
        let _ = kv.flush();
        kv.set_bulk_mode(false);
        if compact_limit > 0 {
            let target = kv.options().l0_compact_threshold.saturating_sub(1);
            let _ = kv.compact_l0_until(target, compact_limit);
        }
        let next_ver = self.next_version.load(Ordering::SeqCst);
        let _ = wal.append(WalRecord::Checkpoint {
            next_version: next_ver,
            root_page_id: 0,
            next_page_id: 0,
        });
        let _ = wal.sync();
        let _ = wal.truncate();
        persist_mvcc_next(
            // BulkLoader 没有 db_dir 字段：从 kv.dir 取
            kv.dir(),
            next_ver,
        );
    }
}

impl Drop for BulkLoader {
    fn drop(&mut self) {
        if !self.finished {
            self.finish_inner(0);
        }
    }
}

impl Transaction {
    pub fn begin(
        kv: Arc<Mutex<LSMTree>>,
        wal: Arc<Mutex<Wal>>,
        active_txn: Arc<Mutex<HashMap<u64, Vec<Vec<u8>>>>>,
        next_version: Arc<AtomicU64>,
    ) -> Self {
        let version = next_version.fetch_add(1, Ordering::SeqCst);
        let active_txn_arc = Arc::clone(&active_txn);

        let mut guard = active_txn.lock().unwrap();
        let active_xid: HashSet<u64> = guard.keys().cloned().collect();
        guard.insert(version, Vec::new());
        drop(guard);

        {
            let mut w = wal.lock().unwrap();
            let _ = w.append(WalRecord::Begin { xid: version });
        }

        Transaction {
            kv,
            wal,
            active_txn: active_txn_arc,
            version,
            active_xid,
        }
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn set(&self, key: &[u8], value: Vec<u8>) -> bool {
        self.write(key, Some(value))
    }

    pub fn delete(&self, key: &[u8]) -> bool {
        self.write(key, None)
    }

    fn write(&self, key: &[u8], value: Option<Vec<u8>>) -> bool {
        let mut active_txn = self.active_txn.lock().unwrap();
        let mut wal = self.wal.lock().unwrap();
        let mut kvengine = self.kv.lock().unwrap();

        if let Some(latest_version) = Self::latest_version_of(&kvengine, key) {
            if !self.is_visible(latest_version) {
                return false;
            }
        }

        let enc_key = encode_key(key, self.version);

        if let Err(e) = wal.append(WalRecord::Write {
            xid: self.version,
            key: enc_key.clone(),
            value: value.clone(),
        }) {
            eprintln!("WAL append 失败: {e}");
            return false;
        }

        let writes = active_txn.entry(self.version).or_default();
        if !writes.iter().any(|k| k == key) {
            writes.push(key.to_vec());
        }

        kvengine.insert(enc_key, value);
        true
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.latest_visible_raw(key)
            .and_then(|(_, v)| v)
            .and_then(crate::lsm_tree::kv_ops::logical_to_user)
    }

    /// 最新可见逻辑行（含 TTL 信封原始字节；tombstone 为 None）。
    /// 供 [`crate::kv_ops`] 使用，不剥用户 value。
    pub(crate) fn latest_visible_raw(
        &self,
        key: &[u8],
    ) -> Option<(u64, Option<Vec<u8>>)> {
        let kvengine = self.kv.lock().unwrap();
        let mut best: Option<(u64, Option<Vec<u8>>)> = None;
        for (enc, val) in Self::scan_key_versions(&kvengine, key) {
            let Some(vk) = decode_key(&enc) else {
                continue;
            };
            if vk.raw_key.as_slice() != key {
                continue;
            }
            if self.is_visible(vk.version) {
                best = Some((vk.version, val));
            }
        }
        best
    }

    /// 全部最新可见逻辑行（key 有序）：(key, version, raw_or_tombstone)
    pub(crate) fn collect_latest_raw(
        &self,
        include_deleted: bool,
    ) -> Vec<(Vec<u8>, u64, Option<Vec<u8>>)> {
        let mut latest: BTreeMap<Vec<u8>, (u64, Option<Vec<u8>>)> = BTreeMap::new();
        let kvengine = self.kv.lock().unwrap();
        for (enc, val) in kvengine.iter() {
            let Some(vk) = decode_key(&enc) else {
                continue;
            };
            if !self.is_visible(vk.version) {
                continue;
            }
            latest.insert(vk.raw_key, (vk.version, val));
        }
        latest
            .into_iter()
            .filter_map(|(key, (ver, value))| {
                if value.is_none() && !include_deleted {
                    return None;
                }
                Some((key, ver, value))
            })
            .collect()
    }

    pub fn print_all(&self) -> BTreeMap<Vec<u8>, Option<Vec<u8>>> {
        let mut records = BTreeMap::new();
        for rec in self.export_latest_visible(true) {
            records.insert(rec.key, rec.value);
        }
        records
    }

    /// 导出本事务快照下每个逻辑 key 的最新可见版本（已剥 TTL）。
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
                Some(bytes) => match crate::lsm_tree::kv_ops::logical_to_user(bytes) {
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

    /// 提交事务：WAL Commit + **fsync**。默认不 flush MemTable/SST。
    ///
    /// 每事务一次 fsync → 吞吐受盘限制（数千 ops/s 量级）。
    /// 大批量导入请用 [`MVCC::begin_bulk`]。需要把 mem 落成 SST 时用
    /// [`commit_with_options`]`(true)` 或事后 [`MVCC::flush`]。
    pub fn commit(&self) {
        self.commit_with_options(false);
    }

    /// `flush_pages`：commit 后是否 `LSMTree::flush`（mem→L0 + 可能 compact）。
    ///
    /// 频繁 `flush_pages=true` 会显著变慢；批量提交后再 `mvcc.flush()` 更合适。
    /// L0 堆积时可用 `kv.compact_l0_until(...)` 或后台 compact 收口。
    pub fn commit_with_options(&self, flush_pages: bool) {
        {
            let mut active_txn = self.active_txn.lock().unwrap();
            active_txn.remove(&self.version);
        }
        {
            let mut wal = self.wal.lock().unwrap();
            let _ = wal.append(WalRecord::Commit { xid: self.version });
            if let Err(e) = wal.sync() {
                eprintln!("WAL fsync 失败: {e}");
            }
        }
        if flush_pages {
            let _ = self.kv.lock().unwrap().flush();
        }
    }

    pub fn rollback(&self) {
        let mut active_txn = self.active_txn.lock().unwrap();
        let keys = active_txn.remove(&self.version).unwrap_or_default();

        {
            let mut wal = self.wal.lock().unwrap();
            let _ = wal.append(WalRecord::Abort { xid: self.version });
            let _ = wal.sync();
        }

        if !keys.is_empty() {
            let mut kvengine = self.kv.lock().unwrap();
            for k in keys {
                let enc_key = encode_key(&k, self.version);
                let _ = kvengine.remove(&enc_key);
            }
        }
    }

    fn is_visible(&self, version: u64) -> bool {
        if self.active_xid.contains(&version) {
            return false;
        }
        version <= self.version
    }

    fn scan_key_versions(kv: &LSMTree, key: &[u8]) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        let low = encode_key(key, 0);
        let high = encode_key(key, u64::MAX);
        kv.range_scan(low, high)
            .into_iter()
            .filter(|(enc, _)| {
                decode_key(enc)
                    .map(|vk| vk.raw_key.as_slice() == key)
                    .unwrap_or(false)
            })
            .collect()
    }

    fn latest_version_of(kv: &LSMTree, key: &[u8]) -> Option<u64> {
        Self::scan_key_versions(kv, key)
            .into_iter()
            .filter_map(|(enc, _)| decode_key(&enc).map(|vk| vk.version))
            .max()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lsm_tree::storage::lock_path;

    fn tmp_db(tag: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("lsm_mvcc_{tag}_{nanos}"))
    }

    fn cleanup(path: &Path) {
        let _ = std::fs::remove_dir_all(path);
        // 若路径被当文件创建过
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(wal_path(path));
        let _ = std::fs::remove_file(lock_path(path));
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
        let path = tmp_db("basic");
        {
            let mvcc = MVCC::open(&path);
            let tx = mvcc.begin_transaction();
            assert!(tx.set(b"a", b"1".to_vec()));
            assert_eq!(tx.get(b"a"), Some(b"1".to_vec()));
            assert!(tx.set(b"a", b"2".to_vec()));
            assert_eq!(tx.get(b"a"), Some(b"2".to_vec()));
            assert!(tx.delete(b"a"));
            assert_eq!(tx.get(b"a"), None);
            tx.commit();
        }
        cleanup(&path);
    }

    #[test]
    fn test_snapshot_isolation() {
        let path = tmp_db("si");
        {
            let mvcc = MVCC::open(&path);
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
        cleanup(&path);
    }

    #[test]
    fn test_write_write_conflict() {
        let path = tmp_db("ww");
        {
            let mvcc = MVCC::open(&path);
            let t1 = mvcc.begin_transaction();
            let t2 = mvcc.begin_transaction();
            assert!(t1.set(b"k", b"v1".to_vec()));
            assert!(!t2.set(b"k", b"v2".to_vec()));
            t1.commit();
            t2.rollback();
        }
        cleanup(&path);
    }

    #[test]
    fn test_rollback_discards_writes() {
        let path = tmp_db("rb");
        {
            let mvcc = MVCC::open(&path);
            let t1 = mvcc.begin_transaction();
            assert!(t1.set(b"x", b"1".to_vec()));
            t1.rollback();
            let t2 = mvcc.begin_transaction();
            assert_eq!(t2.get(b"x"), None);
            t2.commit();
        }
        cleanup(&path);
    }

    #[test]
    fn test_print_all_latest_visible() {
        let path = tmp_db("pa");
        {
            let mvcc = MVCC::open(&path);
            let t1 = mvcc.begin_transaction();
            assert!(t1.set(b"a", b"1".to_vec()));
            assert!(t1.set(b"b", b"2".to_vec()));
            assert!(t1.set(b"a", b"3".to_vec()));
            let all = t1.print_all();
            assert_eq!(all.get(&b"a".to_vec()).unwrap().as_ref().unwrap(), b"3");
            assert_eq!(all.get(&b"b".to_vec()).unwrap().as_ref().unwrap(), b"2");
            t1.commit();
        }
        cleanup(&path);
    }

    #[test]
    fn test_instances_isolated() {
        let p1 = tmp_db("i1");
        let p2 = tmp_db("i2");
        {
            let m1 = MVCC::open(&p1);
            let m2 = MVCC::open(&p2);
            let t1 = m1.begin_transaction();
            let t2 = m2.begin_transaction();
            assert!(t1.set(b"k", b"1".to_vec()));
            assert!(t2.set(b"k", b"2".to_vec()));
            assert_eq!(t1.get(b"k"), Some(b"1".to_vec()));
            assert_eq!(t2.get(b"k"), Some(b"2".to_vec()));
            t1.commit();
            t2.commit();
        }
        cleanup(&p1);
        cleanup(&p2);
    }

    #[test]
    fn test_self_write_visible() {
        let path = tmp_db("self");
        {
            let mvcc = MVCC::open(&path);
            let tx = mvcc.begin_transaction();
            assert!(tx.set(b"k", b"v".to_vec()));
            assert_eq!(tx.get(b"k"), Some(b"v".to_vec()));
            tx.commit();
        }
        cleanup(&path);
    }

    #[test]
    fn test_conflict_after_other_commits() {
        let path = tmp_db("cmt");
        {
            let mvcc = MVCC::open(&path);
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
        cleanup(&path);
    }

    #[test]
    fn test_disk_persist_across_reopen() {
        let path = tmp_db("persist");
        {
            let mvcc = MVCC::open(&path);
            let t1 = mvcc.begin_transaction();
            assert!(t1.set(b"hello", b"world".to_vec()));
            t1.commit();
            let t2 = mvcc.begin_transaction();
            assert!(t2.set(b"hello", b"rust".to_vec()));
            assert!(t2.set(b"foo", b"bar".to_vec()));
            t2.commit();
            // 确保快照落盘（commit 默认不 flush 页，靠 drop 或显式 flush）
            mvcc.flush();
        }
        {
            let mvcc = MVCC::open(&path);
            let tx = mvcc.begin_transaction();
            assert_eq!(tx.get(b"hello"), Some(b"rust".to_vec()));
            assert_eq!(tx.get(b"foo"), Some(b"bar".to_vec()));
            tx.commit();
        }
        cleanup(&path);
    }

    #[test]
    fn test_wal_redo_after_commit_without_page_flush() {
        let path = tmp_db("redo");
        {
            cleanup(&path);
            std::fs::create_dir_all(&path).unwrap();
            // 空快照
            let mut tree = LSMTree::open(&path).unwrap();
            tree.flush().unwrap();
            drop(tree);

            let mut wal = Wal::open(wal_path(&path)).unwrap();
            let xid = 1u64;
            let enc = encode_key(b"recover", xid);
            wal.append(WalRecord::Begin { xid }).unwrap();
            wal.append(WalRecord::Write {
                xid,
                key: enc,
                value: Some(b"me".to_vec()),
            })
            .unwrap();
            wal.append(WalRecord::Commit { xid }).unwrap();
            wal.sync().unwrap();
        }
        {
            let mvcc = MVCC::open(&path);
            let tx = mvcc.begin_transaction();
            assert_eq!(tx.get(b"recover"), Some(b"me".to_vec()));
            tx.commit();
        }
        cleanup(&path);
    }

    #[test]
    fn test_wal_undo_uncommitted() {
        let path = tmp_db("undo");
        {
            {
                let mvcc = MVCC::open(&path);
                drop(mvcc);
            }
            // 模拟：脏写入已进快照，但 WAL 只有 Begin+Write 无 Commit
            let mut wal = Wal::open(wal_path(&path)).unwrap();
            let xid = 1u64;
            {
                let mut tree = LSMTree::open(&path).unwrap();
                let enc = encode_key(b"ghost", xid);
                tree.insert(enc.clone(), Some(b"should-vanish".to_vec()));
                tree.flush().unwrap();
                wal.append(WalRecord::Begin { xid }).unwrap();
                wal.append(WalRecord::Write {
                    xid,
                    key: enc,
                    value: Some(b"should-vanish".to_vec()),
                })
                .unwrap();
                wal.sync().unwrap();
            }
        }
        {
            let mvcc = MVCC::open(&path);
            let tx = mvcc.begin_transaction();
            assert_eq!(tx.get(b"ghost"), None);
            tx.commit();
        }
        cleanup(&path);
    }

    #[test]
    fn test_bulk_load_and_reopen() {
        let path = tmp_db("bulk");
        {
            let mvcc = MVCC::open(&path);
            let mut bulk = mvcc.begin_bulk();
            for i in 0..1000u32 {
                let k = format!("k{i:06}").into_bytes();
                let v = format!("v{i}").into_bytes();
                bulk.put(&k, v);
            }
            assert_eq!(bulk.get(b"k000042").as_deref(), Some(b"v42".as_slice()));
            bulk.finish();
        }
        {
            let mvcc = MVCC::open(&path);
            let tx = mvcc.begin_transaction();
            assert_eq!(tx.get(b"k000042"), Some(b"v42".to_vec()));
            assert_eq!(tx.get(b"k000999"), Some(b"v999".to_vec()));
            tx.commit();
        }
        cleanup(&path);
    }

    #[test]
    fn test_bulk_finish_with_compact() {
        let path = tmp_db("bulk_compact");
        {
            let mvcc = MVCC::open(&path);
            let mut bulk = mvcc.begin_bulk();
            // 小 batch 多次 → 多个 L0
            for batch in 0..8u32 {
                let mut items = Vec::new();
                for i in 0..50u32 {
                    let n = batch * 50 + i;
                    items.push((format!("k{n:06}").into_bytes(), format!("v{n}").into_bytes()));
                }
                bulk.put_batch_owned(items);
            }
            bulk.finish_with_compact(32);
        }
        {
            let mvcc = MVCC::open(&path);
            let tx = mvcc.begin_transaction();
            assert_eq!(tx.get(b"k000000"), Some(b"v0".to_vec()));
            assert_eq!(tx.get(b"k000399"), Some(b"v399".to_vec()));
            // 抽样中间 key
            assert_eq!(tx.get(b"k000150"), Some(b"v150".to_vec()));
            tx.commit();
        }
        cleanup(&path);
    }

    #[test]
    fn test_export_latest_visible() {
        let path = tmp_db("export");
        {
            let mvcc = MVCC::open(&path);
            let t1 = mvcc.begin_transaction();
            assert!(t1.set(b"k1", b"v1".to_vec()));
            assert!(t1.set(b"k2", b"v2".to_vec()));
            t1.commit();
            let t2 = mvcc.begin_transaction();
            assert!(t2.set(b"k1", b"v1b".to_vec()));
            assert!(t2.delete(b"k2"));
            assert!(t2.set(b"k3", b"v3".to_vec()));
            t2.commit();
        }
        {
            let mvcc = MVCC::open(&path);
            let live = mvcc.export_latest_visible(false);
            assert_eq!(live.len(), 2);
            assert_eq!(
                live.iter().find(|r| r.key == b"k1").unwrap().value,
                Some(b"v1b".to_vec())
            );
            assert_eq!(
                live.iter().find(|r| r.key == b"k3").unwrap().value,
                Some(b"v3".to_vec())
            );
            assert!(!live.iter().any(|r| r.key == b"k2"));

            let all = mvcc.export_latest_visible(true);
            assert_eq!(all.len(), 3);
            assert!(all.iter().any(|r| r.key == b"k2" && r.value.is_none()));
        }
        cleanup(&path);
    }

    #[test]
    fn test_vacuum_removes_old_versions() {
        let path = tmp_db("vac");
        {
            let mvcc = MVCC::open(&path);
            let t = mvcc.begin_transaction();
            assert!(t.set(b"a", b"1".to_vec()));
            assert!(t.set(b"b", b"old".to_vec()));
            t.commit();
            let t = mvcc.begin_transaction();
            assert!(t.set(b"a", b"2".to_vec()));
            assert!(t.delete(b"b"));
            t.commit();

            let st = mvcc.vacuum().unwrap();
            assert!(
                st.versions_removed >= 2,
                "应删除 a 旧版与 b 的历史, removed={}",
                st.versions_removed
            );

            let tx = mvcc.begin_transaction();
            assert_eq!(tx.get(b"a"), Some(b"2".to_vec()));
            assert_eq!(tx.get(b"b"), None);
            tx.commit();
        }
        cleanup(&path);
    }

    #[test]
    fn test_try_open_lock_conflict() {
        let path = tmp_db("lock2");
        let m1 = MVCC::try_open(&path).unwrap();
        let err = MVCC::try_open(&path).err();
        assert!(err.is_some());
        drop(m1);
        let m2 = MVCC::try_open(&path).unwrap();
        drop(m2);
        cleanup(&path);
    }
}
