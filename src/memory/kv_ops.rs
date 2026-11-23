//! 高级 KV 操作（独立模块）：批量、范围扫描、模糊搜索分页、元数据、TTL。
//!
//! 全部建立在 [`crate::memory::memory_mvcc::Transaction`] 的快照可见性之上。
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
use std::{debug_assert_eq, unreachable};

use crate::common::codec::parse_i64_user;
use crate::memory::memory_mvcc::{MVCC, Transaction};

/// 公共类型与编解码工具现由 [`crate::common`] 统一定义。
///
/// 这里按原路径重导出，保证 `memory::kv_ops::pack_plain` 等既有调用点不变。
pub use crate::common::{
    ExportRecord, IncrError, KeyMatchMode, KeyMeta, SearchPage, SearchQuery, ValueMeta,
    decode_value_meta, key_matches, logical_to_user, now_unix_secs, pack_plain, pack_ttl,
    prefix_upper_bound,
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
        // 撤行 + 摘写集那一段在 [`crate::common::TxnCore::undo`]（四引擎共用的一份）
        self.undo(keys);
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

    pub fn incr_with_ttl(&self, key: &[u8], delta: i64, ttl: Duration) -> Result<i64, IncrError> {
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

    /// 游标式有界扫描（实现见 [`crate::common::scan_page`]）：`after` 之后最多 `limit` 行，
    /// 返回 `(行, 下一页游标)`，游标 `None` = 到底。翻页/迁移用它，别用 `scan(start, None)`。
    pub fn scan_page(
        &self,
        after: Option<&[u8]>,
        limit: usize,
    ) -> (Vec<ExportRecord>, Option<Vec<u8>>) {
        crate::common::scan_page(self, after, limit)
    }
    pub fn scan_foreach(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        f: impl FnMut(ExportRecord) -> bool,
    ) {
        // 界下推给折叠：窗口扫的代价必须随窗口宽度缩，不是随全库存活 key 数缩
        //
        // ⚠️ 逐行**流式**（M27）：这条路径原先写的是 `for … in collect_latest_raw_in(...)`,
        //    那一句会先把界内的 `(键, 版本, 存储字节)` 全攒成一个 `Vec` —— 于是本函数文档里
        //    "不收集全库到单一大 Vec"那句是假的（200k 键实测中间那份元组头 ≈ 13 MB，与结果
        //    `Vec` 同时活着），而且 `f` 提前叫停也省不到"折"的那一段。
        //    `fold_rows_streaming` 才是那句文档的兑现：折一行交一行，叫停之后的键不再折。
        //
        // M35：折叠 + 剥壳 + 过期判定那四行，四家各抄过一遍，现在收进
        // [`crate::common::scan_live_records`]（判据只剩 `live_record_of_logical` 一处）。
        crate::common::scan_live_records(self, start, end, f);
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

    /// 反向一步：编码键 `≤ bound` 的最大那一行属于哪把逻辑键
    // 先取严格物理前驱，逻辑顺序的重排由 common::order 统一处理。
    fn prev_encoded(&self, bound: &[u8]) -> Option<Vec<u8>> {
        let store = self.store.lock().unwrap();
        store.find_key_back(b"", bound, |enc| enc < bound)
    }

    fn live_of(&self, raw: &[u8]) -> Option<ExportRecord> {
        let logical = self.latest_visible_raw(raw).and_then(|(_, v)| v);
        crate::common::live_record_of_logical(raw.to_vec(), logical, now_unix_secs())
    }

    pub fn seek_prev(&self, key: &[u8]) -> Option<ExportRecord> {
        use crate::common::order::{ReverseOutcome, range_end, reverse_window};
        match reverse_window(
            1,
            usize::MAX,
            Some(range_end(key, self.core.version, true)),
            Some(key),
            |bound| self.prev_encoded(bound),
            |raw| self.live_of(raw),
        ) {
            ReverseOutcome::Complete(mut rows) => rows.pop(),
            ReverseOutcome::BudgetExceeded { .. } => unreachable!("unlimited reverse walk"),
        }
    }

    /// 反向物理扫描按逻辑键排序；二进制前缀和长历史都计入8192步预算，回退只保留一页结果。
    pub fn search_last_page(&self, page_size: usize) -> SearchPage {
        use crate::common::order::{ReverseOutcome, reverse_tail_bounded, reverse_window};
        const MAX_STEPS: usize = 8 * 1024;
        let bound = (self.store.lock().unwrap().last_key()).map(|mut enc| {
            enc.push(0);
            enc
        });
        let items = match reverse_window(
            page_size,
            MAX_STEPS,
            bound.clone(),
            None,
            |bound| self.prev_encoded(bound),
            |raw| self.live_of(raw),
        ) {
            ReverseOutcome::Complete(rows) => rows,
            ReverseOutcome::BudgetExceeded { physical_steps } => {
                debug_assert_eq!(physical_steps, MAX_STEPS);
                reverse_tail_bounded(
                    page_size,
                    bound,
                    |bound| self.prev_encoded(bound),
                    |raw| self.live_of(raw),
                )
            }
        };
        SearchPage {
            items,
            total: None,
            page: 0,
            page_size,
            total_pages: None,
            has_next: false,
        }
    }

    pub fn search_keys(&self, query: &SearchQuery) -> SearchPage {
        crate::common::search_page(self, query)
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
                    value_len: if expired { None } else { Some(m.user().len()) },
                    version,
                    expire_unix_secs: m.expire_unix_secs(),
                    expired,
                    deleted: false,
                })
            }
        }
    }

    /// 前缀下存活 key 数量。
    ///
    /// M35：走 [`crate::common::count_window`]（**只数不物化**）。旧写法是
    /// `prefix_scan(prefix).len()` —— 为数一个数把界内每行的键与值都物化成 `ExportRecord`
    /// 再丢掉（600k 档实测 **2.36 s / 峰值 +123.7 MB**）。判据一字未动：墓碑与过期行都不算。
    pub fn key_count(&self, prefix: &[u8]) -> usize {
        crate::common::count_window(self, false, Some(prefix), KeyMatchMode::Prefix)
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
        // 只需要"哪几把键过期了"⇒ 逐行**流式**（M27）：原先 `collect_latest_raw(false)`
        // 会把全库的 `(键, 版本, 字节)` 先攒成一个 `Vec`，那笔瞬时与最终要删的条数根本不成比例。
        // ⚠️ 删除仍然必须在折叠**之后**做：折叠期间是"拿着 kv 锁走元数据"，边折边删会改到
        //    正在走的键流（这条顺序是原实现就有的，别顺手合掉）。
        let mut expired_keys: Vec<Vec<u8>> = Vec::new();
        crate::common::fold_rows_streaming(self, false, None, None, |key, _ver, value| {
            if let crate::common::RowValue::Value(bytes) = value {
                if decode_value_meta(&bytes).is_expired_at(now) {
                    expired_keys.push(key);
                }
            }
            true
        });
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
mod tests;

// 按职责组织的内部实现；公开类型与原模块路径保持一致。
