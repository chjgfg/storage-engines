//! 高级 KV 操作（独立模块）：批量、范围扫描、模糊搜索分页、元数据、TTL。
//!
//! 全部建立在 [`crate::mvcc::Transaction`] 的快照可见性之上。
//! API 与 bplus-tree 的 `kv_ops` 对齐，便于对照学习。
//!
//! ## 能力一览
//!
//! | 类别 | API |
//! |------|-----|
//! | 批量 | `batch_set` / `batch_put` / `batch_delete` / `multi_get` |
//! | 计数器 | `incr` / `decr` / `incr_with_ttl` |
//! | 范围 | `scan` / `prefix_scan` / `reverse_scan` / `seek` / `seek_prev` |
//! | 搜索分页 | `search_keys`（`SearchQuery` / `KeyMatchMode` / `SearchPage`） |
//! | 元数据 | `exists` / `get_meta` / `key_count` |
//! | TTL | `set_with_ttl` / `refresh_ttl` / `get_ttl` / `persist` / `purge_expired` |
//!
//! ## TTL 存储约定
//!
//! ```text
//! 永久： [0x00] user_payload...
//! 带过期：[0x01] expire_unix_secs:u64 LE | user_payload...
//! ```
//!
//! 旧数据无 tag 时按「永久裸 value」兼容。

use std::time::Duration;

use crate::common::codec::parse_i64_user;
use crate::memory::memory_mvcc::{MVCC, Transaction};

/// 公共类型与编解码工具现由 [`crate::common`] 统一定义。
///
/// 这里按原路径重导出，保证 `memory::kv_ops::pack_plain` 等既有调用点不变。
pub use crate::common::{
    decode_value_meta, key_matches, logical_to_user, now_unix_secs, pack_plain, pack_ttl,
    prefix_upper_bound, ExportRecord, IncrError, KeyMatchMode, KeyMeta, SearchPage, SearchQuery,
    ValueMeta,
};

// ─── Transaction 扩展 ───────────────────────────────────────────────────────

impl Transaction {
    // ── 批量写 ──

    pub fn batch_set(&self, items: &[(Vec<u8>, Vec<u8>)]) -> Result<(), Vec<u8>> {
        let mut written: Vec<Vec<u8>> = Vec::new();
        for (k, v) in items {
            let packed = pack_plain(v);
            if !self.set(k, packed) {
                self.rollback_keys(&written);
                return Err(k.clone());
            }
            written.push(k.clone());
        }
        Ok(())
    }

    pub fn batch_put(&self, items: &[(Vec<u8>, Vec<u8>)]) -> Result<(), Vec<u8>> {
        self.batch_set(items)
    }

    pub fn batch_delete(&self, keys: &[Vec<u8>]) -> Result<(), Vec<u8>> {
        let mut written: Vec<Vec<u8>> = Vec::new();
        for k in keys {
            if !self.delete(k) {
                self.rollback_keys(&written);
                return Err(k.clone());
            }
            written.push(k.clone());
        }
        Ok(())
    }

    fn rollback_keys(&self, keys: &[Vec<u8>]) {
        use crate::memory::memory_mvcc::encode_key;
        if keys.is_empty() {
            return;
        }
        let ver = self.version();
        let mut store = self.store.lock().unwrap();
        for k in keys {
            let _ = store.delete(&encode_key(k, ver));
        }
        if let Ok(mut active) = self.active_txn.lock() {
            if let Some(writes) = active.get_mut(&ver) {
                writes.retain(|w| !keys.iter().any(|k| k == w));
            }
        }
    }

    // ── multi_get ──

    pub fn multi_get(&self, keys: &[Vec<u8>]) -> Vec<Option<Vec<u8>>> {
        keys.iter().map(|k| self.get(k)).collect()
    }

    // ── 计数器 ──

    pub fn incr(&self, key: &[u8], delta: i64) -> Result<i64, IncrError> {
        self.incr_inner(key, delta, None)
    }

    pub fn decr(&self, key: &[u8], delta: i64) -> Result<i64, IncrError> {
        self.incr(key, delta.checked_neg().ok_or(IncrError::Overflow)?)
    }

    pub fn incr_with_ttl(
        &self,
        key: &[u8],
        delta: i64,
        ttl: Duration,
    ) -> Result<i64, IncrError> {
        let expire = now_unix_secs().saturating_add(ttl.as_secs().max(1));
        self.incr_inner(key, delta, Some(expire))
    }

    fn incr_inner(
        &self,
        key: &[u8],
        delta: i64,
        expire_unix: Option<u64>,
    ) -> Result<i64, IncrError> {
        let now = now_unix_secs();
        let current = match self.latest_visible_raw(key) {
            Some((_ver, Some(raw))) => {
                let m = decode_value_meta(&raw);
                if m.is_expired_at(now) {
                    0i64
                } else {
                    parse_i64_user(m.user())?
                }
            }
            _ => 0i64,
        };
        let new_val = current.checked_add(delta).ok_or(IncrError::Overflow)?;
        let user = new_val.to_string().into_bytes();
        let packed = match expire_unix {
            Some(e) => pack_ttl(&user, e),
            None => pack_plain(&user),
        };
        if !self.set(key, packed) {
            return Err(IncrError::WriteConflict);
        }
        Ok(new_val)
    }

    // ── 范围遍历 ──

    /// 区间扫描 **[start, end)**：`start ≤ key < end`；`end=None` 表示无上界。
    ///
    /// 注意：会物化结果 `Vec`。大数据量请用 [`Transaction::scan_foreach`] 避免 OOM。
    pub fn scan(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Vec<ExportRecord> {
        let mut out = Vec::new();
        self.scan_foreach(start, end, |rec| {
            out.push(rec);
            true
        });
        out
    }

    /// 流式扫描：对每条存活记录调用 `f`；`f` 返回 `false` 提前结束。
    pub fn scan_foreach(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        mut f: impl FnMut(ExportRecord) -> bool,
    ) {
        let now = now_unix_secs();
        // 界下推给折叠：窗口扫的代价必须随窗口宽度缩，不是随全库存活 key 数缩
        for (key, _ver, raw) in self.collect_latest_raw_in(false, start, end) {
            if let Some(s) = start {
                if key.as_slice() < s {
                    continue;
                }
            }
            if let Some(e) = end {
                if key.as_slice() >= e {
                    break; // key 有序，可提前停
                }
            }
            let Some(bytes) = raw else { continue };
            let m = decode_value_meta(&bytes);
            if m.is_expired_at(now) {
                continue;
            }
            let rec = ExportRecord {
                key,
                value: Some(m.user().to_vec()),
            };
            if !f(rec) {
                break;
            }
        }
    }

    /// 前缀流式扫描
    pub fn prefix_scan_foreach(&self, prefix: &[u8], f: impl FnMut(ExportRecord) -> bool) {
        let end = prefix_upper_bound(prefix);
        self.scan_foreach(Some(prefix), end.as_deref(), f);
    }

    pub fn prefix_scan(&self, prefix: &[u8]) -> Vec<ExportRecord> {
        let end = prefix_upper_bound(prefix);
        self.scan(Some(prefix), end.as_deref())
    }

    pub fn reverse_scan(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Vec<ExportRecord> {
        let mut v = self.scan(start, end);
        v.reverse();
        v
    }

    pub fn seek(&self, key: &[u8]) -> Option<ExportRecord> {
        self.scan(Some(key), None).into_iter().next()
    }

    pub fn seek_prev(&self, key: &[u8]) -> Option<ExportRecord> {
        let mut upper = key.to_vec();
        upper.push(0);
        let mut v = self.scan(None, Some(&upper));
        v.retain(|r| r.key.as_slice() <= key);
        v.pop()
    }

    // ── 模糊 / 前缀搜索 + 分页 ──

    pub fn search_keys(&self, query: &SearchQuery) -> SearchPage {
        // 分页核只有一份：[`crate::common::search_page_from`]。这里唯一的分支是"要不要
        // 精确 total"—— 不要的时候折叠可以提前收手，代价从 O(全库) 变成 O(窗口宽)。
        let page_size = query.page_size.max(1);
        let limit = if query.need_total {
            None
        } else {
            // 多取一条：它就是 `has_next` 的证据，代价 = 一条记录
            Some(query.page.saturating_mul(page_size) + page_size + 1)
        };
        let matched = self.collect_for_search(
            query.include_deleted,
            Some(query.pattern.as_slice()),
            query.mode,
            limit,
        );
        crate::common::search_page_from(matched, query)
    }
    fn collect_for_search(
        &self,
        include_deleted: bool,
        pattern: Option<&[u8]>,
        mode: KeyMatchMode,
        limit: Option<usize>,
    ) -> Vec<ExportRecord> {
        // 前缀匹配的键是有界的 ⇒ 界下推给折叠。Contains 要逐键比子串，全库是它的本质代价，
        // 但**只要窗口那么宽的结果**就不必走到底 —— 于是 `limit` 与 `accept` 都交给驱动。
        let (lo, hi) = match mode {
            KeyMatchMode::Prefix => (
                if pattern.unwrap_or_default().is_empty() {
                    None
                } else {
                    pattern
                },
                prefix_upper_bound(pattern.unwrap_or_default()),
            ),
            KeyMatchMode::Contains => (None, None),
        };
        let accept = |k: &[u8]| pattern.is_none_or(|pat| key_matches(k, pat, mode));
        crate::common::fold_window(
            limit,
            |after, budget| self.scan_candidates(lo, hi.as_deref(), Some(&accept), after, budget),
            |cand| match cand.row {
                // 墓碑：只在 include_deleted 时出现（与旧实现同口径）
                None => {
                    if include_deleted {
                        Some(ExportRecord { key: cand.key, value: None })
                    } else {
                        None
                    }
                }
                Some(bytes) => match logical_to_user(bytes) {
                    Some(user) => Some(ExportRecord {
                        key: cand.key,
                        value: Some(user),
                    }),
                    // 过期与墓碑同待：算"取不到值"，不静默当成有效行
                    None => {
                        if include_deleted {
                            Some(ExportRecord { key: cand.key, value: None })
                        } else {
                            None
                        }
                    }
                },
            },
        )
    }
    // ── 元数据 ──

    pub fn exists(&self, key: &[u8]) -> bool {
        match self.latest_visible_raw(key) {
            Some((_ver, Some(raw))) => {
                let m = decode_value_meta(&raw);
                !m.is_expired_at(now_unix_secs())
            }
            _ => false,
        }
    }

    pub fn get_meta(&self, key: &[u8]) -> Option<KeyMeta> {
        let (version, raw) = self.latest_visible_raw(key)?;
        match raw {
            None => Some(KeyMeta {
                key: key.to_vec(),
                value_len: None,
                version,
                expire_unix_secs: None,
                expired: false,
                deleted: true,
            }),
            Some(bytes) => {
                let m = decode_value_meta(&bytes);
                let expired = m.is_expired_at(now_unix_secs());
                Some(KeyMeta {
                    key: key.to_vec(),
                    value_len: if expired {
                        None
                    } else {
                        Some(m.user().len())
                    },
                    version,
                    expire_unix_secs: m.expire_unix_secs(),
                    expired,
                    deleted: false,
                })
            }
        }
    }

    pub fn key_count(&self, prefix: &[u8]) -> usize {
        self.prefix_scan(prefix).len()
    }

    // ── TTL ──

    pub fn set_with_ttl(&self, key: &[u8], value: Vec<u8>, ttl: Duration) -> bool {
        let expire = now_unix_secs().saturating_add(ttl.as_secs().max(1));
        let packed = pack_ttl(&value, expire);
        self.set(key, packed)
    }

    pub fn refresh_ttl(&self, key: &[u8], ttl: Duration) -> bool {
        let Some((_ver, Some(raw))) = self.latest_visible_raw(key) else {
            return false;
        };
        let m = decode_value_meta(&raw);
        if m.is_expired_at(now_unix_secs()) {
            return false;
        }
        let expire = now_unix_secs().saturating_add(ttl.as_secs().max(1));
        let packed = pack_ttl(m.user(), expire);
        self.set(key, packed)
    }

    pub fn get_ttl(&self, key: &[u8]) -> Option<Option<Duration>> {
        let (_ver, raw) = self.latest_visible_raw(key)?;
        let raw = raw?;
        let m = decode_value_meta(&raw);
        let now = now_unix_secs();
        if m.is_expired_at(now) {
            return None;
        }
        match m.expire_unix_secs() {
            None => Some(None),
            Some(e) => {
                let left = e.saturating_sub(now);
                Some(Some(Duration::from_secs(left)))
            }
        }
    }

    pub fn persist(&self, key: &[u8]) -> bool {
        let Some((_ver, Some(raw))) = self.latest_visible_raw(key) else {
            return false;
        };
        let m = decode_value_meta(&raw);
        if m.is_expired_at(now_unix_secs()) {
            return false;
        }
        let packed = pack_plain(m.user());
        self.set(key, packed)
    }

    pub fn purge_expired(&self) -> usize {
        let now = now_unix_secs();
        let expired_keys: Vec<Vec<u8>> = self
            .collect_latest_raw(false)
            .into_iter()
            .filter_map(|(key, _ver, raw)| {
                let raw = raw?;
                let m = decode_value_meta(&raw);
                if m.is_expired_at(now) {
                    Some(key)
                } else {
                    None
                }
            })
            .collect();
        let mut n = 0usize;
        for k in expired_keys {
            if self.delete(&k) {
                n += 1;
            }
        }
        n
    }
}

// ─── MVCC 便捷包装 ──────────────────────────────────────────────────────────

impl MVCC {
    pub fn search_keys(&self, query: &SearchQuery) -> SearchPage {
        let tx = self.begin_transaction();
        let page = tx.search_keys(query);
        tx.commit();
        page
    }
}

// ─── 测试 ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::memory_mvcc::fold_keys;

    #[test]
    fn test_value_meta_roundtrip() {
        let p = pack_plain(b"hi");
        match decode_value_meta(&p) {
            ValueMeta::Plain { user } => assert_eq!(user, b"hi"),
            _ => panic!(),
        }
        let t = pack_ttl(b"x", 1000);
        match decode_value_meta(&t) {
            ValueMeta::Ttl {
                expire_unix_secs,
                user,
            } => {
                assert_eq!(expire_unix_secs, 1000);
                assert_eq!(user, b"x");
            }
            _ => panic!(),
        }
        assert_eq!(
            decode_value_meta(b"raw"),
            ValueMeta::Plain {
                user: b"raw".to_vec()
            }
        );
    }

    #[test]
    fn test_prefix_upper_bound() {
        assert_eq!(prefix_upper_bound(b"ab"), Some(b"ac".to_vec()));
        assert_eq!(prefix_upper_bound(b"a\xff"), Some(b"b".to_vec()));
        assert_eq!(prefix_upper_bound(b"\xff\xff"), None);
    }

    #[test]
    fn test_key_matches_helpers() {
        assert!(key_matches(b"hello", b"ell", KeyMatchMode::Contains));
        assert!(!key_matches(b"hello", b"xyz", KeyMatchMode::Contains));
        assert!(key_matches(b"hello", b"hel", KeyMatchMode::Prefix));
        assert!(!key_matches(b"hello", b"ello", KeyMatchMode::Prefix));
        assert!(key_matches(b"any", b"", KeyMatchMode::Contains));
        assert!(key_matches(b"any", b"", KeyMatchMode::Prefix));
    }

    #[test]
    fn test_batch_and_multi_get() {
        let mvcc = MVCC::new();
        let tx = mvcc.begin_transaction();
        let items = vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
        ];
        assert!(tx.batch_set(&items).is_ok());
        assert!(tx.batch_delete(&[b"b".to_vec()]).is_ok());
        let got = tx.multi_get(&[b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        assert_eq!(got[0], Some(b"1".to_vec()));
        assert_eq!(got[1], None);
        assert_eq!(got[2], Some(b"3".to_vec()));
        tx.commit();
    }

    #[test]
    fn test_scan_prefix_seek() {
        let mvcc = MVCC::new();
        let tx = mvcc.begin_transaction();
        let rows: &[(&[u8], &[u8])] = &[
            (b"a1", b"1"),
            (b"a2", b"2"),
            (b"b1", b"3"),
            (b"order_01", b"o1"),
            (b"order_02", b"o2"),
            (b"z", b"9"),
        ];
        for &(k, v) in rows {
            assert!(tx.set(k, pack_plain(v)));
        }
        tx.commit();

        let tx = mvcc.begin_transaction();
        let range = tx.scan(Some(b"a1"), Some(b"b1"));
        let keys: Vec<_> = range.iter().map(|r| r.key.as_slice()).collect();
        assert_eq!(keys, vec![b"a1".as_slice(), b"a2"]);

        let pref = tx.prefix_scan(b"order_");
        assert_eq!(pref.len(), 2);
        assert_eq!(pref[0].key, b"order_01");
        assert_eq!(pref[1].key, b"order_02");

        let rev = tx.reverse_scan(Some(b"a1"), Some(b"b2"));
        assert_eq!(rev[0].key, b"b1");

        let s = tx.seek(b"a2").unwrap();
        assert_eq!(s.key, b"a2");
        let s2 = tx.seek(b"a15").unwrap();
        assert_eq!(s2.key, b"a2");

        let p = tx.seek_prev(b"a2").unwrap();
        assert_eq!(p.key, b"a2");
        let p2 = tx.seek_prev(b"a15").unwrap();
        assert_eq!(p2.key, b"a1");

        assert!(tx.exists(b"order_01"));
        assert!(!tx.exists(b"nope"));
        assert_eq!(tx.key_count(b"order_"), 2);

        let meta = tx.get_meta(b"a1").unwrap();
        assert_eq!(meta.value_len, Some(1));
        assert!(!meta.deleted);
        assert!(!meta.expired);
        tx.commit();
    }

    #[test]
    fn test_incr_and_incr_with_ttl() {
        let mvcc = MVCC::new();
        let tx = mvcc.begin_transaction();

        assert_eq!(tx.incr(b"cnt", 1).unwrap(), 1);
        assert_eq!(tx.incr(b"cnt", 5).unwrap(), 6);
        assert_eq!(tx.decr(b"cnt", 2).unwrap(), 4);
        assert_eq!(tx.get(b"cnt"), Some(b"4".to_vec()));

        assert!(tx.set(b"s", pack_plain(b"abc")));
        assert_eq!(tx.incr(b"s", 1), Err(IncrError::NotInteger));

        assert_eq!(
            tx.incr_with_ttl(b"hits", 1, Duration::from_secs(90)).unwrap(),
            1
        );
        assert_eq!(
            tx.incr_with_ttl(b"hits", 2, Duration::from_secs(90)).unwrap(),
            3
        );
        assert_eq!(tx.get(b"hits"), Some(b"3".to_vec()));
        match tx.get_ttl(b"hits") {
            Some(Some(d)) => assert!(d.as_secs() > 0 && d.as_secs() <= 90),
            other => panic!("hits should have ttl, got {other:?}"),
        }

        let expired = pack_ttl(b"10", now_unix_secs().saturating_sub(5));
        assert!(tx.set(b"oldc", expired));
        assert_eq!(tx.get(b"oldc"), None);
        assert_eq!(tx.incr(b"oldc", 1).unwrap(), 1);

        tx.commit();
    }

    #[test]
    fn test_search_keys_fuzzy_and_paginate() {
        let mvcc = MVCC::new();
        let tx = mvcc.begin_transaction();
        let rows: &[(&[u8], &[u8])] = &[
            (b"apple", b"1"),
            (b"apply", b"2"),
            (b"banana", b"3"),
            (b"grape", b"4"),
            (b"user:1", b"u1"),
            (b"user:2", b"u2"),
            (b"user:10", b"u10"),
            (b"zebra", b"z"),
        ];
        for &(k, v) in rows {
            assert!(tx.set(k, v.to_vec()));
        }
        assert!(tx.delete(b"grape"));
        tx.commit();

        let p0 = mvcc.search_keys(&SearchQuery::contains(b"pp", 0, 10));
        assert_eq!(p0.total, Some(2));
        assert_eq!(p0.total_pages, Some(1));
        assert_eq!(p0.items.len(), 2);
        assert_eq!(p0.items[0].key, b"apple");
        assert_eq!(p0.items[1].key, b"apply");
        assert!(!p0.has_next());

        let pref = mvcc.search_keys(&SearchQuery::prefix(b"user:", 0, 10));
        assert_eq!(pref.total, Some(3));
        let keys: Vec<_> = pref.items.iter().map(|r| r.key.as_slice()).collect();
        assert_eq!(keys, vec![b"user:1".as_slice(), b"user:10", b"user:2"]);

        let all = mvcc.search_keys(&SearchQuery::contains(Vec::<u8>::new(), 0, 3));
        assert_eq!(all.total, Some(7));
        assert_eq!(all.total_pages, Some(3));
        assert_eq!(all.items.len(), 3);
        assert!(all.has_next());
        assert!(!all.has_prev());

        let p1 = mvcc.search_keys(&SearchQuery::contains(Vec::<u8>::new(), 1, 3));
        assert_eq!(p1.items.len(), 3);
        assert!(p1.has_next());
        assert!(p1.has_prev());

        let p2 = mvcc.search_keys(&SearchQuery::contains(Vec::<u8>::new(), 2, 3));
        assert_eq!(p2.items.len(), 1);
        assert!(!p2.has_next());
        assert_eq!(p2.page, 2);

        let p9 = mvcc.search_keys(&SearchQuery::contains(b"user:", 99, 2));
        assert_eq!(p9.total, Some(3));
        assert!(p9.items.is_empty());

        let with_del =
            mvcc.search_keys(&SearchQuery::contains(b"rape", 0, 10).with_deleted(true));
        assert_eq!(with_del.total, Some(1));
        assert!(with_del.items[0].value.is_none());

        let t2 = mvcc.begin_transaction();
        assert!(t2.set(b"user:draft", b"d".to_vec()));
        let mine = t2.search_keys(&SearchQuery::prefix(b"user:", 0, 20));
        assert_eq!(mine.total, Some(4));
        assert!(mine.items.iter().any(|r| r.key == b"user:draft"));
        t2.rollback();
    }

    /// 不要精确 total 的翻页，代价必须只看**窗口宽**，不看库有多大（M15 的验收）。
    ///
    /// 以前 `search_keys` 为了报一个 `total` 把整库折一遍 ⇒ 首页与末页一样贵
    /// （实测 20000 键时首页也要 100 ms 量级）。尺子是本引擎的折叠计数 `fold_keys()`。
    /// 顺序是**先量窗口、再量全量**：后跑的那次缓存更热，把偏差留给"全量更贵"那一侧，
    /// 这条断言只会因此更难通过，不会反过来骗人。
    #[test]
    fn test_search_window_costs_the_window_not_the_library() {
        const N: usize = 2000;
        const PAGE: usize = 20;

        let mvcc = MVCC::new();
        let tx = mvcc.begin_transaction();
        for i in 0..N {
            assert!(tx.set(format!("user{i:05}").as_bytes(), b"x".to_vec()));
        }
        tx.commit();

        let base = fold_keys();
        let w = mvcc.search_keys(&SearchQuery::contains(b"user", 0, PAGE).without_total());
        let window_keys = fold_keys() - base;

        let base = fold_keys();
        let full = mvcc.search_keys(&SearchQuery::contains(b"user", 0, PAGE));
        let full_keys = fold_keys() - base;

        assert_eq!(full.total, Some(N), "要 total 时语义与改前完全一致");
        assert_eq!(w.total, None, "没数就不许报一个数出来");
        assert_eq!(w.total_pages, None);
        assert_eq!(w.items.len(), PAGE);
        assert_eq!(
            w.items, full.items,
            "窗口必须是全量结果的前缀：分页语义一个字节都不许变"
        );
        assert!(w.has_next(), "后面还有 {} 条，必须报得出来", N - PAGE);
        eprintln!(
            "[search 窗口] 折了 {window_keys} 把键就拿回首页；要精确 total 折了 {full_keys} 把（N={N}）"
        );
        assert!(
            window_keys * 2 <= full_keys,
            "首页折了 {window_keys} 把 / 全量 {full_keys} 把 ⇒ 还在走全库"
        );

        // 最后一面：探不到"多出来的那一条"就必须如实报"没有下一页"（既不许把"没数"
        // 说成"到底了"，也不许反过来吓唬人）
        let last = N / PAGE - 1;
        let w2 = mvcc.search_keys(&SearchQuery::contains(b"user", last, PAGE).without_total());
        let f2 = mvcc.search_keys(&SearchQuery::contains(b"user", last, PAGE));
        assert_eq!(w2.items.len(), PAGE);
        assert_eq!(w2.items, f2.items, "同一页的内容与要不要 total 无关");
        assert!(!w2.has_next(), "最后一页之后没有东西了");
    }

    #[test]
    fn test_ttl_and_purge() {
        let mvcc = MVCC::new();
        let tx = mvcc.begin_transaction();
        assert!(tx.set_with_ttl(b"temp", b"v".to_vec(), Duration::from_secs(60)));
        assert!(tx.set(b"forever", pack_plain(b"x")));
        let past = pack_ttl(b"old", now_unix_secs().saturating_sub(10));
        assert!(tx.set(b"stale", past));

        assert_eq!(tx.get(b"temp"), Some(b"v".to_vec()));
        assert_eq!(tx.get(b"stale"), None);
        assert!(tx.exists(b"temp"));
        assert!(!tx.exists(b"stale"));

        match tx.get_ttl(b"temp") {
            Some(Some(d)) => assert!(d.as_secs() > 0 && d.as_secs() <= 60),
            other => panic!("expected remaining ttl, got {other:?}"),
        }
        assert_eq!(tx.get_ttl(b"forever"), Some(None));
        assert_eq!(tx.get_ttl(b"stale"), None);

        assert!(tx.refresh_ttl(b"temp", Duration::from_secs(120)));
        assert!(tx.persist(b"temp"));
        assert_eq!(tx.get_ttl(b"temp"), Some(None));

        let n = tx.purge_expired();
        assert!(n >= 1, "应清理 stale, n={n}");
        assert!(!tx.exists(b"stale"));
        tx.commit();
    }

    #[test]
    fn test_batch_conflict_rolls_back_batch() {
        let mvcc = MVCC::new();
        let t1 = mvcc.begin_transaction();
        let t2 = mvcc.begin_transaction();
        assert!(t1.set(b"k", pack_plain(b"1")));
        t1.commit();
        let items = vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"k".to_vec(), b"2".to_vec()),
        ];
        let err = t2.batch_set(&items);
        assert!(err.is_err());
        assert_eq!(t2.get(b"a"), None);
        t2.rollback();
    }

    #[test]
    fn test_scan_foreach_early_stop() {
        let mvcc = MVCC::new();
        let tx = mvcc.begin_transaction();
        for &(k, v) in &[
            (&b"k1"[..], &b"1"[..]),
            (&b"k2"[..], &b"2"[..]),
            (&b"k3"[..], &b"3"[..]),
        ] {
            assert!(tx.set(k, pack_plain(v)));
        }
        tx.commit();

        let tx = mvcc.begin_transaction();
        let mut seen: Vec<Vec<u8>> = Vec::new();
        tx.scan_foreach(None, None, |rec| {
            seen.push(rec.key);
            seen.len() < 2
        });
        assert_eq!(seen, vec![b"k1".to_vec(), b"k2".to_vec()]);

        let mut pref: Vec<Vec<u8>> = Vec::new();
        tx.prefix_scan_foreach(b"k", |rec| {
            pref.push(rec.key);
            true
        });
        assert_eq!(pref.len(), 3);
        tx.commit();
    }
}
