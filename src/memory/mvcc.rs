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
    collections::{BTreeMap, HashMap, HashSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use crate::memory::mem_store::MemStore;

/// MVCC 引擎（单进程内存）
pub struct MVCC {
    store: Arc<Mutex<MemStore>>,
    active_txn: Arc<Mutex<HashMap<u64, Vec<Vec<u8>>>>>,
    next_version: Arc<AtomicU64>,
}

/// 普通 MVCC 事务
pub struct Transaction {
    pub(crate) store: Arc<Mutex<MemStore>>,
    pub(crate) active_txn: Arc<Mutex<HashMap<u64, Vec<Vec<u8>>>>>,
    version: u64,
    active_xid: HashSet<u64>,
}

/// 批量导入：无冲突检测、不登记活跃表（单写者假定）
pub struct BulkLoader {
    store: Arc<Mutex<MemStore>>,
    next_version: Arc<AtomicU64>,
    version: u64,
    finished: bool,
}

/// 逻辑键 + 版本（调试用）
#[derive(Debug, Clone, PartialEq, Eq)]
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

/// vacuum 结果
#[derive(Debug, Clone)]
pub struct VacuumStats {
    pub xmin: u64,
    pub versions_removed: usize,
}

/// 导出一条逻辑记录
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportRecord {
    pub key: Vec<u8>,
    /// `None` 表示 tombstone（仅 include_deleted 时出现）
    pub value: Option<Vec<u8>>,
}

impl MVCC {
    /// 新建空库
    pub fn new() -> Self {
        Self {
            store: Arc::new(Mutex::new(MemStore::new())),
            active_txn: Arc::new(Mutex::new(HashMap::new())),
            next_version: Arc::new(AtomicU64::new(1)),
        }
    }

    pub fn begin_transaction(&self) -> Transaction {
        Transaction::begin(
            self.store.clone(),
            self.active_txn.clone(),
            self.next_version.clone(),
        )
    }

    /// bulk load：单写者、无冲突检测。结束必须 `finish()`。
    pub fn begin_bulk(&self) -> BulkLoader {
        let version = self.next_version.fetch_add(1, Ordering::SeqCst);
        BulkLoader {
            store: self.store.clone(),
            next_version: self.next_version.clone(),
            version,
            finished: false,
        }
    }

    /// 导出当前快照下每个逻辑 key 的最新可见版本
    pub fn export_latest_visible(&self, include_deleted: bool) -> Vec<ExportRecord> {
        let tx = self.begin_transaction();
        let records = tx.export_latest_visible(include_deleted);
        tx.commit();
        records
    }

    /// 版本回收：每个逻辑 key 只保留「最后一个 version < xmin」+ 全部 version >= xmin；
    /// 若 xmin 之前最终是 tombstone 且无更新版本 → 整 key 删除。
    pub fn vacuum(&self) -> VacuumStats {
        let active = self.active_txn.lock().unwrap();
        let xmin = if active.is_empty() {
            self.next_version.load(Ordering::SeqCst)
        } else {
            *active.keys().min().unwrap()
        };
        drop(active);

        let mut store = self.store.lock().unwrap();
        let all: Vec<(Vec<u8>, Option<Vec<u8>>)> = store.iter().collect();
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
            let _ = store.delete(enc);
        }

        VacuumStats {
            xmin,
            versions_removed: removed,
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
        let enc = encode_key(key, self.version);
        self.store.lock().unwrap().insert(enc, Some(value));
    }

    pub fn put_batch(&mut self, items: &[(Vec<u8>, Vec<u8>)]) {
        let mut store = self.store.lock().unwrap();
        for (key, value) in items {
            let enc = encode_key(key, self.version);
            store.insert(enc, Some(value.clone()));
        }
    }

    pub fn put_batch_owned(&mut self, items: Vec<(Vec<u8>, Vec<u8>)>) {
        let mut store = self.store.lock().unwrap();
        for (key, value) in items {
            let enc = encode_key(&key, self.version);
            store.insert(enc, Some(value));
        }
    }

    pub fn delete(&mut self, key: &[u8]) {
        let enc = encode_key(key, self.version);
        self.store.lock().unwrap().insert(enc, None);
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let store = self.store.lock().unwrap();
        let low = encode_key(key, 0);
        let high = encode_key(key, u64::MAX);
        let mut best: Option<Vec<u8>> = None;
        for (enc, row) in store.range_scan(&low, &high) {
            if let Some(vk) = decode_key(&enc) {
                if vk.raw_key.as_slice() == key && vk.version <= self.version {
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
        self.finish_inner();
    }

    fn finish_inner(&mut self) {
        if self.finished {
            return;
        }
        self.finished = true;
        // 确保 next_version 已越过本 bulk version（begin 时已 fetch_add）
        let _ = self.next_version.load(Ordering::SeqCst);
    }
}

impl Drop for BulkLoader {
    fn drop(&mut self) {
        if !self.finished {
            self.finish_inner();
        }
    }
}

impl Transaction {
    pub fn begin(
        store: Arc<Mutex<MemStore>>,
        active_txn: Arc<Mutex<HashMap<u64, Vec<Vec<u8>>>>>,
        next_version: Arc<AtomicU64>,
    ) -> Self {
        let version = next_version.fetch_add(1, Ordering::SeqCst);
        let active_txn_arc = Arc::clone(&active_txn);

        let mut guard = active_txn.lock().unwrap();
        let active_xid: HashSet<u64> = guard.keys().cloned().collect();
        guard.insert(version, Vec::new());
        drop(guard);

        Transaction {
            store,
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
        let mut store = self.store.lock().unwrap();

        if let Some(latest_version) = Self::latest_version_of(&store, key) {
            if !self.is_visible(latest_version) {
                return false;
            }
        }

        let enc_key = encode_key(key, self.version);

        let writes = active_txn.entry(self.version).or_default();
        if !writes.iter().any(|k| k == key) {
            writes.push(key.to_vec());
        }

        store.insert(enc_key, value);
        true
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let store = self.store.lock().unwrap();
        let mut best: Option<(u64, Option<Vec<u8>>)> = None;

        for (enc, row) in Self::scan_key_versions(&store, key) {
            let Some(vk) = decode_key(&enc) else {
                continue;
            };
            if vk.raw_key.as_slice() != key {
                continue;
            }
            if self.is_visible(vk.version) {
                best = Some((vk.version, row));
            }
        }
        best.and_then(|(_, v)| v.and_then(crate::memory::kv_ops::logical_to_user))
    }

    /// 最新可见逻辑行（含 TTL 信封；tombstone 为 None）
    pub(crate) fn latest_visible_raw(&self, key: &[u8]) -> Option<(u64, Option<Vec<u8>>)> {
        let store = self.store.lock().unwrap();
        let mut best: Option<(u64, Option<Vec<u8>>)> = None;
        for (enc, row) in Self::scan_key_versions(&store, key) {
            let Some(vk) = decode_key(&enc) else {
                continue;
            };
            if vk.raw_key.as_slice() != key {
                continue;
            }
            if self.is_visible(vk.version) {
                best = Some((vk.version, row));
            }
        }
        best
    }

    /// 全部最新可见逻辑行（key 有序）
    pub(crate) fn collect_latest_raw(
        &self,
        include_deleted: bool,
    ) -> Vec<(Vec<u8>, u64, Option<Vec<u8>>)> {
        let mut latest: BTreeMap<Vec<u8>, (u64, Option<Vec<u8>>)> = BTreeMap::new();
        let store = self.store.lock().unwrap();
        for (enc, row) in store.iter() {
            let Some(vk) = decode_key(&enc) else {
                continue;
            };
            if !self.is_visible(vk.version) {
                continue;
            }
            latest.insert(vk.raw_key, (vk.version, row));
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
        let mut active_txn = self.active_txn.lock().unwrap();
        active_txn.remove(&self.version);
    }

    pub fn rollback(&self) {
        let mut active_txn = self.active_txn.lock().unwrap();
        let keys = active_txn.remove(&self.version).unwrap_or_default();

        if !keys.is_empty() {
            let mut store = self.store.lock().unwrap();
            for k in keys {
                let enc_key = encode_key(&k, self.version);
                let _ = store.delete(&enc_key);
            }
        }
    }

    fn is_visible(&self, version: u64) -> bool {
        if self.active_xid.contains(&version) {
            return false;
        }
        version <= self.version
    }

    fn scan_key_versions(store: &MemStore, key: &[u8]) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        let low = encode_key(key, 0);
        let high = encode_key(key, u64::MAX);
        store
            .range_scan(&low, &high)
            .into_iter()
            .filter(|(enc, _)| {
                decode_key(enc)
                    .map(|vk| vk.raw_key.as_slice() == key)
                    .unwrap_or(false)
            })
            .collect()
    }

    fn latest_version_of(store: &MemStore, key: &[u8]) -> Option<u64> {
        Self::scan_key_versions(store, key)
            .into_iter()
            .filter_map(|(enc, _)| decode_key(&enc).map(|vk| vk.version))
            .max()
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
}
