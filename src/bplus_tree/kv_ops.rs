//! 高级 KV 操作（独立模块）：批量、范围扫描、模糊搜索分页、元数据、TTL。
//!
//! 全部建立在 [`crate::bplus_tree::bplus_tree_mvcc::Transaction`] 的快照可见性之上。
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
//! 逻辑 value（进 blob 编码之前）可带信封：
//!
//! ```text
//! 永久： [0x00] user_payload...
//! 带过期：[0x01] expire_unix_secs:u64 LE | user_payload...
//! ```
//!
//! 旧数据无 tag 时按「永久裸 value」兼容。
//! 读路径（`get` / scan / multi_get / search）自动剥信封并丢弃已过期条目。

use std::time::Duration;
use std::{debug_assert_eq, unreachable};

use crate::bplus_tree::bplus_tree_mvcc::{MVCC, Transaction};
use crate::common::codec::parse_i64_user;

/// 公共类型与编解码工具现由 [`crate::common`] 统一定义。
///
/// 这里按原路径重导出，保证 `bplus_tree::kv_ops::pack_plain` 等既有调用点不变。
pub use crate::common::{
    ExportRecord, IncrError, KeyMatchMode, KeyMeta, SearchPage, SearchQuery, ValueMeta,
    decode_value_meta, key_matches, logical_to_user, now_unix_secs, pack_plain, pack_ttl,
    prefix_upper_bound,
};

// ─── Transaction 扩展 ───────────────────────────────────────────────────────

impl Transaction {
    // ── 批量写 ──

    /// 批量写入多条 K/V（原子：全部成功才算成功；任一条冲突则整批失败并回滚本事务已写部分）。
    ///
    /// 注意：与「独立事务批量」不同——这里共用**当前事务**版本；
    /// 调用方应在成功后自行 `commit()`，失败时 `rollback()`。
    /// 返回 `Ok(())` 表示本批全部写入树；`Err(failed_key)` 表示冲突/失败的第一条 key。
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

    /// `batch_set` 别名
    pub fn batch_put(&self, items: &[(Vec<u8>, Vec<u8>)]) -> Result<(), Vec<u8>> {
        self.batch_set(items)
    }

    /// 批量删除；全部成功返回 Ok，否则 Err(首个失败 key) 并回滚本批删除
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

    /// 回滚本事务对给定 keys 的当前 version 写入（不 Abort 整个事务）
    fn rollback_keys(&self, keys: &[Vec<u8>]) {
        // 撤行 + 摘写集那一段在 [`crate::common::TxnCore::undo`]（四引擎共用的一份）
        self.undo(keys);
    }

    // ── multi_get ──

    /// 一次查询多个 key；返回与 `keys` 等长，不存在/过期为 None
    pub fn multi_get(&self, keys: &[Vec<u8>]) -> Vec<Option<Vec<u8>>> {
        keys.iter().map(|k| self.get(k)).collect()
    }

    // ── 计数器 incr / decr ──

    /// 原子自增（在本事务内：读最新可见 → 加 delta → 写回）。
    ///
    /// - key **不存在 / 已过期 / tombstone**：从 **0** 起算（`0 + delta`）
    /// - 已有 value：按 **十进制 ASCII** 解析为 `i64`（可负）；非法格式 → `Err(IncrError::NotInteger)`
    /// - 写冲突 / 写失败 → `Err(IncrError::WriteConflict)`
    /// - 溢出（`checked_add` 失败）→ `Err(IncrError::Overflow)`
    /// - 成功返回 **自增后** 的新值
    ///
    /// 持久化格式：用户 value = `format!("{}", new)` 再 `pack_plain`（永久计数器）。
    /// 调用方仍须 `commit()` 提交事务。
    pub fn incr(&self, key: &[u8], delta: i64) -> Result<i64, IncrError> {
        self.incr_inner(key, delta, None)
    }

    /// 自减：`incr(key, -delta)`（`delta` 为要减去的量，通常 ≥ 0，但任意 i64 均可）
    pub fn decr(&self, key: &[u8], delta: i64) -> Result<i64, IncrError> {
        self.incr(key, delta.checked_neg().ok_or(IncrError::Overflow)?)
    }

    /// 带 TTL 的自增：逻辑同 [`Self::incr`]，写入时用 `pack_ttl`。
    ///
    /// - 若 key **已存在且未过期并带 TTL**：续写时 **刷新** 为 `now + ttl`（与常见缓存计数器一致）
    /// - 若 key 不存在 / 已过期：从 0 起算并设 TTL
    /// - 若 key 存在且为**永久**整数：自增后变为带 TTL（附上过期）
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
    /// 不收集全库到单一大 Vec（内部按有序 latest 迭代）。

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
        // 界下推给折叠：窗口扫的代价必须随窗口宽度缩（页数），不是随全库页数缩
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

    /// 前缀扫描：所有 `key.starts_with(prefix)` 的最新可见存活项
    pub fn prefix_scan(&self, prefix: &[u8]) -> Vec<ExportRecord> {
        let end = prefix_upper_bound(prefix);
        self.scan(Some(prefix), end.as_deref())
    }

    /// 逆序扫描 [start, end) 后反转结果（教学实现：先正序再 reverse）
    pub fn reverse_scan(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Vec<ExportRecord> {
        let mut v = self.scan(start, end);
        v.reverse();
        v
    }

    /// 定位 **≥ key** 的第一条存活记录
    pub fn seek(&self, key: &[u8]) -> Option<ExportRecord> {
        self.scan(Some(key), None).into_iter().next()
    }

    /// 定位 **≤ key** 的最后一条存活记录
    /// 反向找"≤ key 的最大一把活着的键"：**逐步退，不物化前缀**（M20）。
    ///
    /// 循环与判定在 [`crate::common::seek_prev_with`]（四引擎共用）；反向一步走
    /// [`BPlusTree::find_back`]（本来就是点读的热路径，跨叶反向有进度守卫），单键可见行走
    /// `latest_visible_raw`。改之前是 `scan(None, Some(key‖0))` + `retain` + `pop`。
    /// 反向一步：编码键 `≤ bound` 的最大那一行属于哪把逻辑键
    // 先取严格物理前驱，逻辑顺序的重排由 common::order 统一处理。
    fn prev_encoded(&self, bound: &[u8]) -> Option<Vec<u8>> {
        self.kv
            .lock()
            .unwrap()
            .find_back(b"", bound, |enc| enc < bound)
            .map(|(enc, _)| enc)
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
        let bound = (self.kv.lock().unwrap().last_key()).map(|mut enc| {
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
        // M35：整条实现收进 [`crate::common::search_page`]（四家一份）。要精确 total 时
        // 不再把界内命中全物化成 `Vec`，改成"只数一遍 + 只取那一页"。
        crate::common::search_page(self, query)
    }

    // ── 元数据 ──

    /// key 是否存在（最新可见且未删除且未过期）
    pub fn exists(&self, key: &[u8]) -> bool {
        match self.latest_visible_raw(key) {
            Some((_ver, Some(raw))) => {
                let m = decode_value_meta(&raw);
                !m.is_expired_at(now_unix_secs())
            }
            _ => false,
        }
    }

    /// 元数据：长度、版本、过期时间、是否过期/删除
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

    /// 带 TTL 写入：`ttl` 为从现在起的存活时长
    pub fn set_with_ttl(&self, key: &[u8], value: Vec<u8>, ttl: Duration) -> bool {
        let expire = now_unix_secs().saturating_add(ttl.as_secs().max(1));
        let packed = pack_ttl(&value, expire);
        self.set(key, packed)
    }

    /// 续期：保留原用户 value，刷新过期时间；不存在或已删返回 false
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

    /// 剩余 TTL：
    /// - `None`：不存在 / tombstone / 已过期
    /// - `Some(None)`：存在且永久
    /// - `Some(Some(d))`：存在且剩余时长 d
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

    /// 去掉 TTL，改为永久保存
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

    /// 主动清理：把「最新可见且已过期」的 key 写成 tombstone。返回清理条数。
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

// ─── MVCC 便捷包装（开事务 → 搜索 → commit） ───────────────────────────────

impl MVCC {
    /// 按 key **模糊/前缀**匹配，并 **分页**返回最新可见记录。
    ///
    /// 见 [`Transaction::search_keys`] / [`SearchQuery`]。
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
