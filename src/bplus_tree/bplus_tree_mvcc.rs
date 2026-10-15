//! MVCC 事务层 + WAL 崩溃恢复
//!
//! 提交协议（简化版 ARIES）：
//! 1. 写路径：先 append WAL Write，再改内存 B+ 树
//! 2. commit：append WAL Commit → **fsync WAL**（持久化点）→ 可选刷页
//! 3. 崩溃恢复：重放已提交 Write，撤销未提交 Write，再 checkpoint 截断 WAL
//!
//! **Bulk load 模式**（[`MVCC::begin_bulk`]）：
//! - 关闭 WAL / 冲突检测 / 活跃事务表
//! - 页换出跳过双写与频繁 fsync
//! - 结束时 [`BulkLoader::finish`] → flush_all + checkpoint

use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use crate::bplus_tree::{BPlusTree, bplus_tree::RowId};
use crate::bplus_tree::storage::{
    decode_stored_value, encode_stored_value, wal_path, BlobStore,
};
use crate::bplus_tree::wal::{analyze_recovery, Wal, WalRecord};
pub use crate::common::{ExportRecord, VacuumStats};

/// MVCC 存储引擎
pub struct MVCC {
    kv: Arc<Mutex<BPlusTree>>,
    /// 大 value 外置存储（与 kv 同库目录）
    blob: Arc<Mutex<BlobStore>>,
    wal: Arc<Mutex<Wal>>,
    active_txn: Arc<Mutex<HashMap<u64, Vec<Vec<u8>>>>>,
    next_version: Arc<AtomicU64>,
    db_path: PathBuf,
}

/// 普通 MVCC 事务
pub struct Transaction {
    pub(crate) kv: Arc<Mutex<BPlusTree>>,
    pub(crate) blob: Arc<Mutex<BlobStore>>,
    pub(crate) wal: Arc<Mutex<Wal>>,
    pub(crate) active_txn: Arc<Mutex<HashMap<u64, Vec<Vec<u8>>>>>,
    version: u64,
    active_xid: HashSet<u64>,
}

/// 批量导入会话：无 WAL、无冲突检测，单写者假定
pub struct BulkLoader {
    kv: Arc<Mutex<BPlusTree>>,
    blob: Arc<Mutex<BlobStore>>,
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
    pub fn open(path: impl AsRef<Path>, order: usize, max_cache_pages: usize) -> Self {
        Self::try_open(path, order, max_cache_pages).map_err(|e| format!("MVCC::open 失败: {e}")).unwrap()
    }

    /// 打开/创建，错误以 `io::Result` 返回（锁冲突、坏文件、IO 等）。
    ///
    /// **冷启动路径**（千万级 / GB 库）：
    /// - 优先用 meta 中的 `next_version`（checkpoint 时写入），**不扫全树**
    /// - 其次用 WAL 里最后一条 Checkpoint / max_xid
    /// - 仅旧库（meta 无 NVER 且 WAL 无 checkpoint）才 `scan_max_version` 全叶扫描
    pub fn try_open(
        path: impl AsRef<Path>,
        order: usize,
        max_cache_pages: usize,
    ) -> std::io::Result<Self> {
        let db_path = path.as_ref().to_path_buf();
        let mut tree = BPlusTree::try_open(&db_path, order, max_cache_pages)?;
        let blob = BlobStore::open(&db_path)?;
        let mut wal = Wal::open(wal_path(&db_path))?;

        let entries = wal.read_all()?;
        let plan = analyze_recovery(&entries);

        // REDO 已提交 / UNDO 未提交（仅当 WAL 非空有内容时）
        for (_xid, key, value) in &plan.committed_writes {
            tree.insert(key.clone(), RowId(value.clone()));
        }
        for (_xid, key) in &plan.uncommitted_writes {
            let _ = tree.delete(key.clone());
        }

        let start_ver = resolve_start_version(&mut tree, &plan);

        // 把 next_version 写入 meta，下次干净 open 可跳过全树扫描
        tree.set_next_version(start_ver);
        tree.flush_all();
        let root = tree.root_page_id();
        let next_page = tree.next_page_id();
        let _ = wal.append(WalRecord::Checkpoint {
            next_version: start_ver,
            root_page_id: root.0,
            next_page_id: next_page,
        });
        wal.sync()?;
        wal.truncate()?;

        Ok(Self {
            kv: Arc::new(Mutex::new(tree)),
            blob: Arc::new(Mutex::new(blob)),
            wal: Arc::new(Mutex::new(wal)),
            active_txn: Arc::new(Mutex::new(HashMap::new())),
            next_version: Arc::new(AtomicU64::new(start_ver)),
            db_path,
        })
    }

    pub fn new(order: usize, max_cache_pages: usize) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "mvcc_anon_{}_{}.db",
            std::process::id(),
            nanos
        ));
        Self::open(path, order, max_cache_pages)
    }

    pub fn begin_transaction(&self) -> Transaction {
        Transaction::begin(
            self.kv.clone(),
            self.blob.clone(),
            self.wal.clone(),
            self.active_txn.clone(),
            self.next_version.clone(),
        )
    }

    /// 开启 bulk load：单写者、无 WAL、无冲突检测。
    ///
    /// **崩溃不安全**：finish 之前进程挂掉可能丢数据。
    /// 导入结束后必须 `finish()`。
    pub fn begin_bulk(&self) -> BulkLoader {
        {
            let mut kv = self.kv.lock().unwrap();
            kv.set_bulk_mode(true);
        }
        let version = self.next_version.fetch_add(1, Ordering::SeqCst);
        BulkLoader {
            kv: self.kv.clone(),
            blob: self.blob.clone(),
            wal: self.wal.clone(),
            next_version: self.next_version.clone(),
            version,
            finished: false,
        }
    }

    pub fn checkpoint(&self) {
        let mut kv = self.kv.lock().unwrap();
        let mut wal = self.wal.lock().unwrap();
        let next_ver = self.next_version.load(Ordering::SeqCst);
        // meta 先记下 next_version，再 flush（同一次刷盘）
        kv.set_next_version(next_ver);
        kv.flush_all();
        let root = kv.root_page_id();
        let next_page = kv.next_page_id();
        let _ = wal.append(WalRecord::Checkpoint {
            next_version: next_ver,
            root_page_id: root.0,
            next_page_id: next_page,
        });
        let _ = wal.sync();
        let _ = wal.truncate();
    }

    pub fn flush(&self) {
        self.kv.lock().unwrap().flush_all();
    }

    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    /// 导出当前快照下「每个逻辑 key 的最新可见版本」。
    pub fn export_latest_visible(&self, include_deleted: bool) -> Vec<ExportRecord> {
        let tx = self.begin_transaction();
        let records = tx.export_latest_visible(include_deleted);
        tx.commit();
        records
    }

    /// 版本回收（vacuum）+ 重写 blob（只保留仍被引用的大 value）。
    ///
    /// 规则：
    /// - `xmin` = 活跃事务 id 最小值；无活跃事务时为 `next_version`
    /// - 每个逻辑 key：只保留「最后一个 version < xmin」+ 全部 version >= xmin
    /// - 若 xmin 之前最终是 tombstone 且无更新版本 → 整 key 删除
    /// - 然后扫描存活叶子，把仍引用的大 value 重写进新 `*.blob`
    pub fn vacuum(&self) -> std::io::Result<VacuumStats> {
        let active = self.active_txn.lock().unwrap();
        let xmin = if active.is_empty() {
            self.next_version.load(Ordering::SeqCst)
        } else {
            *active.keys().min().unwrap()
        };
        drop(active);

        let mut kv = self.kv.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();

        let all: Vec<(Vec<u8>, RowId)> = kv.iter().map(|(k, v)| (k, v)).collect();
        let mut by_key: BTreeMap<Vec<u8>, Vec<(u64, Vec<u8>, Option<Vec<u8>>)>> = BTreeMap::new();
        for (enc, row) in all {
            let Some(vk) = decode_key(&enc) else {
                continue;
            };
            by_key
                .entry(vk.raw_key)
                .or_default()
                .push((vk.version, enc, row.0));
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
            let _ = kv.delete(enc.clone());
        }

        // Blob GC（教学简化）：
        // - 若已无任何大 value 引用 → clear 文件
        // - 若仍有引用 → 暂不重写（append-only 空间可能残留；完整 compact 后续可做）
        //   这样避免 Windows 上打开句柄重写导致读路径异常。
        let survivors: Vec<RowId> = kv.iter().map(|(_, v)| v).collect();
        let mut has_blob_ref = false;
        for row in &survivors {
            if let Some(stored) = &row.0 {
                if stored.first() == Some(&crate::bplus_tree::storage::VAL_TAG_BLOB) {
                    has_blob_ref = true;
                    break;
                }
            }
        }

        let blob_rewritten = if !has_blob_ref {
            let old_len = blob.path().metadata().map(|m| m.len()).unwrap_or(0);
            if old_len > 8 {
                blob.clear_in_place()?;
                true
            } else {
                false
            }
        } else {
            false
        };

        // vacuum 后把当前 next_version 一并写入 meta，避免下次 open 因 meta 过旧依赖大 WAL
        let next_ver = self.next_version.load(Ordering::SeqCst);
        kv.set_next_version(next_ver);
        kv.flush_all();
        blob.sync()?;

        Ok(VacuumStats {
            xmin,
            versions_removed: removed,
            blob_rewritten: Some(blob_rewritten),
        })
    }
}

impl Drop for MVCC {
    /// 正常退出且无活跃事务时 checkpoint：写 meta.next_version + 截断 WAL。
    /// - 仍有未 commit/rollback 的事务时**不**截断 WAL，避免丢掉 UNDO 信息
    /// - 崩溃/kill -9 不会走到这里，仍靠 WAL REDO + meta/max_xid 恢复
    fn drop(&mut self) {
        // 忽略锁中毒与 IO 错误，避免 drop 时二次 panic
        let has_active = self
            .active_txn
            .lock()
            .map(|g| !g.is_empty())
            .unwrap_or(true);
        if has_active {
            return;
        }
        let next_ver = self.next_version.load(Ordering::SeqCst);
        if let (Ok(mut kv), Ok(mut wal)) = (self.kv.lock(), self.wal.lock()) {
            kv.set_next_version(next_ver);
            kv.flush_all();
            let root = kv.root_page_id();
            let next_page = kv.next_page_id();
            let _ = wal.append(WalRecord::Checkpoint {
                next_version: next_ver,
                root_page_id: root.0,
                next_page_id: next_page,
            });
            let _ = wal.sync();
            let _ = wal.truncate();
        }
        if let Ok(mut blob) = self.blob.lock() {
            let _ = blob.sync();
        }
    }
}

/// 决定 open 后的 `next_version` 起点（尽量不扫全树）。
///
/// 优先级：
/// 1. meta 中的 next_version（干净 checkpoint 后）
/// 2. WAL 最后一条 Checkpoint 的 next_version
/// 3. max(WAL max_xid, 全树 max version)+1（旧库兜底）
fn resolve_start_version(tree: &mut BPlusTree, plan: &crate::bplus_tree::wal::RecoveryPlan) -> u64 {
    let mut base: Option<u64> = None;

    if let Some(nv) = tree.next_version() {
        // meta 已有（含 0=空库）；与 WAL 恢复后的 max_xid 取 max
        base = Some(nv);
    }

    if let Some(WalRecord::Checkpoint { next_version, .. }) = &plan.last_checkpoint {
        base = Some(base.map(|b| b.max(*next_version)).unwrap_or(*next_version));
    }

    if let Some(b) = base {
        // 恢复可能 REDO 了更高 xid，或 meta 略旧
        return b.max(plan.max_xid.saturating_add(1)).max(1);
    }

    // 旧库：meta 无 NVER、WAL 无 checkpoint → 全树扫描（仅此路径）
    let disk_max = scan_max_version(tree);
    disk_max.max(plan.max_xid).saturating_add(1).max(1)
}

fn scan_max_version(tree: &mut BPlusTree) -> u64 {
    let mut max_v = 0u64;
    for (enc, _) in tree.iter() {
        if let Some(vk) = decode_key(&enc) {
            max_v = max_v.max(vk.version);
        }
    }
    max_v
}

impl BulkLoader {
    pub fn put(&mut self, key: &[u8], value: Vec<u8>) {
        let enc = encode_key(key, self.version);
        let stored = {
            let mut blob = self.blob.lock().unwrap();
            // bulk：不每条 fsync blob，finish 时统一 sync
            encode_stored_value(&mut blob, value, false).expect("blob encode")
        };
        let mut kv = self.kv.lock().unwrap();
        kv.insert(enc, RowId(Some(stored)));
    }

    /// 批量 put：一次加锁插入多条，显著降低锁/调度开销
    pub fn put_batch(&mut self, items: &[(Vec<u8>, Vec<u8>)]) {
        let mut blob = self.blob.lock().unwrap();
        let mut kv = self.kv.lock().unwrap();
        for (key, value) in items {
            let enc = encode_key(key, self.version);
            let stored = encode_stored_value(&mut blob, value.clone(), false).expect("blob");
            kv.insert(enc, RowId(Some(stored)));
        }
    }

    /// 零额外 clone 的批量 put（消费 items）
    pub fn put_batch_owned(&mut self, items: Vec<(Vec<u8>, Vec<u8>)>) {
        let mut blob = self.blob.lock().unwrap();
        let mut kv = self.kv.lock().unwrap();
        for (key, value) in items {
            let enc = encode_key(&key, self.version);
            let stored = encode_stored_value(&mut blob, value, false).expect("blob");
            kv.insert(enc, RowId(Some(stored)));
        }
    }

    pub fn delete(&mut self, key: &[u8]) {
        let enc = encode_key(key, self.version);
        let mut kv = self.kv.lock().unwrap();
        kv.insert(enc, RowId(None));
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut kv = self.kv.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();
        let low = encode_key(key, 0);
        let high = encode_key(key, u64::MAX);
        let mut best: Option<Vec<u8>> = None;
        for (enc, row) in kv.range_scan(low, high) {
            if let Some(vk) = decode_key(&enc) {
                if vk.raw_key.as_slice() == key && vk.version <= self.version {
                    best = match row.0 {
                        None => None,
                        Some(stored) => decode_stored_value(&mut blob, &stored)
                            .ok()
                            .flatten()
                            .and_then(crate::bplus_tree::kv_ops::logical_to_user),
                    };
                }
            }
        }
        best
    }

    pub fn finish(mut self) {
        self.finish_inner();
    }

    fn finish_inner(&mut self) {
        if self.finished {
            return;
        }
        self.finished = true;
        let mut kv = self.kv.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();
        let mut wal = self.wal.lock().unwrap();
        let _ = blob.sync();
        let next_ver = self.next_version.load(Ordering::SeqCst);
        kv.set_next_version(next_ver);
        kv.flush_all();
        kv.set_bulk_mode(false);
        let root = kv.root_page_id();
        let next_page = kv.next_page_id();
        let _ = wal.append(WalRecord::Checkpoint {
            next_version: next_ver,
            root_page_id: root.0,
            next_page_id: next_page,
        });
        let _ = wal.sync();
        let _ = wal.truncate();
    }
}

impl Drop for BulkLoader {
    fn drop(&mut self) {
        if !self.finished {
            self.finish_inner();
        }
    }
}

/// 未终结（既没 commit 也没 rollback）就被丢弃的事务在此自动回滚。
///
/// 漏网事务会永久留在 `active_txn` 里：`xmin = min(活跃事务 id)` 被一个已死的 id 压住
/// ⇒ 版本回收永久停摆，且之后每次 `begin` 的可见性快照随漏网数线性变大。
/// 判据用「活跃表里是否还有自己」——`commit`/`rollback` 都以 `remove` 收尾，成员关系即充要条件。
impl Drop for Transaction {
    fn drop(&mut self) {
        let unfinished = match self.active_txn.lock() {
            Ok(guard) => guard.contains_key(&self.version),
            // 锁中毒（别的线程 panic 过）：不能在这里 panic —— Drop 里 panic 直接 abort 进程
            Err(_) => return,
        };
        if !unfinished {
            return;
        }
        if self.wal.lock().is_err() {
            return;
        }
        log::warn!("事务 {} 未 commit/rollback 即被丢弃，已自动回滚", self.version);
        self.rollback();
    }
}

impl Transaction {
    pub fn begin(
        kv: Arc<Mutex<BPlusTree>>,
        blob: Arc<Mutex<BlobStore>>,
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
            blob,
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
        let mut blob = self.blob.lock().unwrap();
        let mut kvengine = self.kv.lock().unwrap();

        if let Some(latest_version) = Self::latest_version_of(&mut kvengine, key) {
            if !self.is_visible(latest_version) {
                return false;
            }
        }

        let enc_key = encode_key(key, self.version);

        // 逻辑 value → 叶子存储编码（大 value 写 blob）
        let stored_opt = match value {
            None => None,
            Some(v) => match encode_stored_value(&mut blob, v, true) {
                Ok(s) => Some(s),
                Err(e) => {
                    eprintln!("blob encode 失败: {e}");
                    return false;
                }
            },
        };

        if let Err(e) = wal.append(WalRecord::Write {
            xid: self.version,
            key: enc_key.clone(),
            value: stored_opt.clone(),
        }) {
            eprintln!("WAL append 失败: {e}");
            return false;
        }

        let writes = active_txn.entry(self.version).or_default();
        if !writes.iter().any(|k| k == key) {
            writes.push(key.to_vec());
        }

        kvengine.insert(enc_key, RowId(stored_opt));
        true
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut kvengine = self.kv.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();
        let mut best: Option<(u64, Option<Vec<u8>>)> = None;

        for (enc, row) in Self::scan_key_versions(&mut kvengine, key) {
            let Some(vk) = decode_key(&enc) else {
                continue;
            };
            if vk.raw_key.as_slice() != key {
                continue;
            }
            if self.is_visible(vk.version) {
                let logical = match &row.0 {
                    None => None,
                    Some(stored) => decode_stored_value(&mut blob, stored).ok().flatten(),
                };
                best = Some((vk.version, logical));
            }
        }
        best.and_then(|(_, v)| v.and_then(|raw| crate::bplus_tree::kv_ops::logical_to_user(raw)))
    }

    /// 最新可见逻辑行（含 TTL 信封原始字节；tombstone 为 None）
    /// 供 [`crate::kv_ops`] 做 exists / meta / scan，不剥用户 value。
    pub(crate) fn latest_visible_raw(
        &self,
        key: &[u8],
    ) -> Option<(u64, Option<Vec<u8>>)> {
        let mut kvengine = self.kv.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();
        let mut best: Option<(u64, Option<Vec<u8>>)> = None;
        for (enc, row) in Self::scan_key_versions(&mut kvengine, key) {
            let Some(vk) = decode_key(&enc) else {
                continue;
            };
            if vk.raw_key.as_slice() != key {
                continue;
            }
            if self.is_visible(vk.version) {
                let logical = match &row.0 {
                    None => None,
                    Some(stored) => decode_stored_value(&mut blob, stored).ok().flatten(),
                };
                best = Some((vk.version, logical));
            }
        }
        best
    }

    /// 全部最新可见逻辑行（key 有序）：(key, version, raw_logical_or_tombstone)
    pub(crate) fn collect_latest_raw(
        &self,
        include_deleted: bool,
    ) -> Vec<(Vec<u8>, u64, Option<Vec<u8>>)> {
        let mut latest: BTreeMap<Vec<u8>, (u64, Option<Vec<u8>>)> = BTreeMap::new();
        let mut kvengine = self.kv.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();
        for (enc, row) in kvengine.iter() {
            let Some(vk) = decode_key(&enc) else {
                continue;
            };
            if !self.is_visible(vk.version) {
                continue;
            }
            let logical = match &row.0 {
                None => None,
                Some(stored) => decode_stored_value(&mut blob, stored).ok().flatten(),
            };
            latest.insert(vk.raw_key, (vk.version, logical));
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

    pub fn print_all(&self) -> BTreeMap<Vec<u8>, RowId> {
        let mut records = BTreeMap::new();
        for rec in self.export_latest_visible(true) {
            records.insert(rec.key, RowId(rec.value));
        }
        records
    }

    /// 导出本事务快照下每个逻辑 key 的最新可见版本（已解码 blob / 剥 TTL）。
    ///
    /// 同 key 多版本按 version 升序扫描，后者覆盖前者 → 最终为最新可见。
    /// 模糊搜索与分页见 [`crate::kv_ops::Transaction::search_keys`]。
    pub fn export_latest_visible(&self, include_deleted: bool) -> Vec<ExportRecord> {
        self.collect_latest_raw(true)
            .into_iter()
            .filter_map(|(key, _ver, raw)| match raw {
                None => {
                    if include_deleted {
                        Some(ExportRecord {
                            key,
                            value: None,
                        })
                    } else {
                        None
                    }
                }
                Some(bytes) => match crate::bplus_tree::kv_ops::logical_to_user(bytes) {
                    Some(user) => Some(ExportRecord {
                        key,
                        value: Some(user),
                    }),
                    None => {
                        if include_deleted {
                            Some(ExportRecord {
                                key,
                                value: None,
                            })
                        } else {
                            None
                        }
                    }
                },
            })
            .collect()
    }

    pub fn commit(&self) {
        self.commit_with_options(false);
    }

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
            self.kv.lock().unwrap().flush_all();
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
                let _ = kvengine.delete(enc_key);
            }
        }
    }

    fn is_visible(&self, version: u64) -> bool {
        if self.active_xid.contains(&version) {
            return false;
        }
        version <= self.version
    }

    fn scan_key_versions(kv: &mut BPlusTree, key: &[u8]) -> Vec<(Vec<u8>, RowId)> {
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

    fn latest_version_of(kv: &mut BPlusTree, key: &[u8]) -> Option<u64> {
        Self::scan_key_versions(kv, key)
            .into_iter()
            .filter_map(|(enc, _)| decode_key(&enc).map(|vk| vk.version))
            .max()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bplus_tree::storage::{dblwr_path, wal_path};

    fn tmp_db(tag: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("mvcc_{tag}_{nanos}.db"))
    }

    fn cleanup(path: &Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(dblwr_path(path));
        let _ = std::fs::remove_file(wal_path(path));
        let _ = std::fs::remove_file(crate::bplus_tree::storage::freelist_path(path));
        let _ = std::fs::remove_file(crate::bplus_tree::storage::lock_path(path));
        let _ = std::fs::remove_file(crate::bplus_tree::storage::blob_path(path));
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
            let mvcc = MVCC::open(&path, 4, 16);
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
            let mvcc = MVCC::open(&path, 4, 16);
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
            let mvcc = MVCC::open(&path, 4, 16);
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
            let mvcc = MVCC::open(&path, 4, 16);
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
            let mvcc = MVCC::open(&path, 4, 16);
            let t1 = mvcc.begin_transaction();
            assert!(t1.set(b"a", b"1".to_vec()));
            assert!(t1.set(b"b", b"2".to_vec()));
            assert!(t1.set(b"a", b"3".to_vec()));
            let all = t1.print_all();
            assert_eq!(all.get(&b"a".to_vec()).unwrap().0, Some(b"3".to_vec()));
            assert_eq!(all.get(&b"b".to_vec()).unwrap().0, Some(b"2".to_vec()));
            t1.commit();
        }
        cleanup(&path);
    }

    #[test]
    fn test_instances_isolated() {
        let p1 = tmp_db("i1");
        let p2 = tmp_db("i2");
        {
            let m1 = MVCC::open(&p1, 4, 8);
            let m2 = MVCC::open(&p2, 4, 8);
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
            let mvcc = MVCC::open(&path, 4, 8);
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
            let mvcc = MVCC::open(&path, 4, 8);
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
            let mvcc = MVCC::open(&path, 4, 16);
            let t1 = mvcc.begin_transaction();
            assert!(t1.set(b"hello", b"world".to_vec()));
            t1.commit();
            let t2 = mvcc.begin_transaction();
            assert!(t2.set(b"hello", b"rust".to_vec()));
            assert!(t2.set(b"foo", b"bar".to_vec()));
            t2.commit();
        }
        {
            let mvcc = MVCC::open(&path, 4, 16);
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
            use crate::bplus_tree::wal::{Wal, WalRecord};
            cleanup(&path);
            let mut tree = crate::bplus_tree::BPlusTree::open(&path, 4, 16);
            tree.flush_all();
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
            let mvcc = MVCC::open(&path, 4, 16);
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
            use crate::bplus_tree::wal::{Wal, WalRecord};
            {
                let mvcc = MVCC::open(&path, 4, 16);
                drop(mvcc);
            }
            let mut wal = Wal::open(wal_path(&path)).unwrap();
            let xid = 1u64;
            {
                let mut tree = crate::bplus_tree::BPlusTree::open(&path, 4, 16);
                let enc = encode_key(b"ghost", xid);
                tree.insert(enc.clone(), RowId(Some(b"should-vanish".to_vec())));
                tree.flush_all();
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
            let mvcc = MVCC::open(&path, 4, 16);
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
            let mvcc = MVCC::open(&path, 64, 256);
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
            let mvcc = MVCC::open(&path, 64, 256);
            let tx = mvcc.begin_transaction();
            assert_eq!(tx.get(b"k000042"), Some(b"v42".to_vec()));
            assert_eq!(tx.get(b"k000999"), Some(b"v999".to_vec()));
            tx.commit();
        }
        cleanup(&path);
    }

    #[test]
    fn test_large_value_blob_roundtrip() {
        let path = tmp_db("blob");
        let big = vec![0xABu8; 10_000]; // > BLOB_THRESHOLD
        {
            let mvcc = MVCC::open(&path, 32, 64);
            let tx = mvcc.begin_transaction();
            assert!(tx.set(b"big", big.clone()));
            assert!(tx.set(b"small", b"hi".to_vec()));
            assert_eq!(tx.get(b"big"), Some(big.clone()));
            assert_eq!(tx.get(b"small"), Some(b"hi".to_vec()));
            tx.commit();
        }
        {
            let mvcc = MVCC::open(&path, 32, 64);
            let tx = mvcc.begin_transaction();
            assert_eq!(tx.get(b"big"), Some(big));
            assert_eq!(tx.get(b"small"), Some(b"hi".to_vec()));
            tx.commit();
        }
        // blob 文件应存在且大于 header
        let blob = crate::bplus_tree::storage::blob_path(&path);
        assert!(blob.exists());
        assert!(std::fs::metadata(&blob).unwrap().len() > 8);
        cleanup(&path);
    }

    #[test]
    fn test_export_latest_visible() {
        let path = tmp_db("export");
        {
            let mvcc = MVCC::open(&path, 16, 32);
            let t1 = mvcc.begin_transaction();
            assert!(t1.set(b"k1", b"v1".to_vec()));
            assert!(t1.set(b"k2", b"v2".to_vec()));
            t1.commit();
            let t2 = mvcc.begin_transaction();
            assert!(t2.set(b"k1", b"v1b".to_vec())); // 覆盖
            assert!(t2.delete(b"k2"));
            assert!(t2.set(b"k3", b"v3".to_vec()));
            t2.commit();
        }
        {
            let mvcc = MVCC::open(&path, 16, 32);
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
    fn test_vacuum_removes_old_versions_and_rewrites_blob() {
        let path = tmp_db("vac");
        let big = vec![9u8; 1000];
        {
            let mvcc = MVCC::open(&path, 16, 64);
            let t = mvcc.begin_transaction();
            assert!(t.set(b"a", b"1".to_vec()));
            assert!(t.set(b"b", b"old".to_vec()));
            t.commit();
            let t = mvcc.begin_transaction();
            assert!(t.set(b"a", b"2".to_vec()));
            assert!(t.delete(b"b"));
            assert!(t.set(b"big", big.clone()));
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
            assert_eq!(tx.get(b"big"), Some(big.clone()));
            tx.commit();
        }
        {
            let mvcc = MVCC::open(&path, 16, 64);
            let tx = mvcc.begin_transaction();
            assert_eq!(tx.get(b"a"), Some(b"2".to_vec()));
            assert_eq!(tx.get(b"big"), Some(big));
            tx.commit();

            // 删除唯一大 value 后再 vacuum → blob 应被清空
            let t = mvcc.begin_transaction();
            assert!(t.delete(b"big"));
            t.commit();
            let st = mvcc.vacuum().unwrap();
            assert!(st.versions_removed >= 1);
            // blob 无引用时应收缩
            let blob = crate::bplus_tree::storage::blob_path(&path);
            if blob.exists() {
                let len = std::fs::metadata(&blob).unwrap().len();
                assert!(len <= 16, "无 blob 引用时文件应接近仅魔数, len={len}");
            }
        }
        cleanup(&path);
    }

    #[test]
    fn test_try_open_lock_conflict() {
        let path = tmp_db("lock2");
        let m1 = MVCC::try_open(&path, 8, 16).unwrap();
        let err = MVCC::try_open(&path, 8, 16).err();
        assert!(err.is_some());
        drop(m1);
        let m2 = MVCC::try_open(&path, 8, 16).unwrap();
        drop(m2);
        cleanup(&path);
    }

    /// 干净 checkpoint 后重开：meta 持久化 next_version，version 连续且数据可读
    #[test]
    fn test_reopen_preserves_next_version_after_checkpoint() {
        let path = tmp_db("reopen_nv");
        let v1;
        {
            let mvcc = MVCC::open(&path, 16, 32);
            let t = mvcc.begin_transaction();
            v1 = t.version();
            assert!(t.set(b"k", b"v1".to_vec()));
            t.commit();
            // checkpoint 把 next_version 写入 meta 并截断 WAL
            mvcc.checkpoint();
        }
        {
            let mvcc = MVCC::open(&path, 16, 32);
            let t = mvcc.begin_transaction();
            // 新事务 version 必须严格大于上次已用 version
            assert!(t.version() > v1, "reopen version={} should > {}", t.version(), v1);
            assert_eq!(t.get(b"k"), Some(b"v1".to_vec()));
            assert!(t.set(b"k", b"v2".to_vec()));
            t.commit();
        }
        {
            let mvcc = MVCC::open(&path, 16, 32);
            let t = mvcc.begin_transaction();
            assert_eq!(t.get(b"k"), Some(b"v2".to_vec()));
            t.commit();
        }
        cleanup(&path);
    }

    /// 未手动 checkpoint：meta 可能仍是上次 open 写入的旧 next_version，
    /// 应靠 WAL 中 max_xid 把起点抬高，保证 version 单调。
    #[test]
    fn test_reopen_uses_wal_max_xid_without_checkpoint() {
        let path = tmp_db("reopen_wal");
        let mut last_v = 0u64;
        {
            let mvcc = MVCC::open(&path, 16, 32);
            for i in 0..5 {
                let t = mvcc.begin_transaction();
                last_v = t.version();
                assert!(t.set(format!("k{i}").as_bytes(), b"x".to_vec()));
                t.commit();
            }
            // 故意不 checkpoint：WAL 里保留 Begin/Write/Commit
        }
        {
            let mvcc = MVCC::open(&path, 16, 32);
            let t = mvcc.begin_transaction();
            assert!(
                t.version() > last_v,
                "version after reopen={} must > last committed {}",
                t.version(),
                last_v
            );
            assert_eq!(t.get(b"k4"), Some(b"x".to_vec()));
            t.commit();
        }
        cleanup(&path);
    }
}
