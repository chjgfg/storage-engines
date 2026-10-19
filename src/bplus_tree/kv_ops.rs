//! 高级 KV 操作（独立模块）：批量、范围扫描、模糊搜索分页、元数据、TTL。
//!
//! 全部建立在 [`crate::mvcc::Transaction`] 的快照可见性之上。
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

use crate::bplus_tree::bplus_tree_mvcc::{MVCC, Transaction};
use crate::common::codec::parse_i64_user;

/// 公共类型与编解码工具现由 [`crate::common`] 统一定义。
///
/// 这里按原路径重导出，保证 `bplus_tree::kv_ops::pack_plain` 等既有调用点不变。
pub use crate::common::{
    decode_value_meta, key_matches, logical_to_user, now_unix_secs, pack_plain, pack_ttl,
    prefix_upper_bound, ExportRecord, IncrError, KeyMatchMode, KeyMeta, SearchPage, SearchQuery,
    ValueMeta,
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

    /// 带 TTL 的自增：逻辑同 [`incr`]，写入时用 `pack_ttl`。
    ///
    /// - 若 key **已存在且未过期并带 TTL**：续写时 **刷新** 为 `now + ttl`（与常见缓存计数器一致）
    /// - 若 key 不存在 / 已过期：从 0 起算并设 TTL
    /// - 若 key 存在且为**永久**整数：自增后变为带 TTL（附上过期）
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
    /// 不收集全库到单一大 Vec（内部按有序 latest 迭代）。

    /// 游标式有界扫描（实现见 [`crate::common::scan_page`]）：`after` 之后最多 `limit` 行，
    /// 返回 `(行, 下一页游标)`，游标 `None` = 到底。翻页/迁移用它，别用 `scan(start, None)`。
    pub fn scan_page(&self, after: Option<&[u8]>, limit: usize) -> (Vec<ExportRecord>, Option<Vec<u8>>) {
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
    pub fn seek_prev(&self, key: &[u8]) -> Option<ExportRecord> {
        crate::common::seek_prev_with(
            key,
            |bound| {
                let mut kv = self.kv.lock().unwrap();
                kv.find_back(b"", bound, |_| true)
                    .and_then(|(enc, _)| crate::common::decode_key(&enc).map(|vk| vk.raw_key))
            },
            |raw| {
                let logical = self.latest_visible_raw(raw).and_then(|(_, v)| v);
                crate::common::live_record_of_logical(
                    raw.to_vec(),
                    logical,
                    now_unix_secs(),
                )
            },
        )
    }

    // ── 模糊 / 前缀搜索 + 分页 ──

    /// 按 key **模糊/前缀**搜索，并分页。
    ///
    /// - 在「最新可见」逻辑键上过滤（与 `export_latest_visible` 同一可见性）
    /// - `KeyMatchMode::Contains`：子串匹配（模糊）
    /// - `KeyMatchMode::Prefix`：前缀匹配
    /// - `page` 从 0 起；结果按 key 字典序
    /// - 会剥 TTL；过期默认不可见（`include_deleted` 时以 tombstone 给出）
    /// - 当前实现扫全树后过滤（教学清晰；大规模可再做索引/前缀 range 优化）
    ///
    /// ```ignore
    /// let page = tx.search_keys(&SearchQuery::contains(b"user:", 0, 20));
    /// for rec in &page.items { ... }
    /// ```
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
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};

    fn tmp(tag: &str) -> PathBuf {
        crate::common::tmp_test_path("kv_ops", tag, ".db")
    }

    fn cleanup(path: &Path) {
        for suf in ["", ".wal", ".dblwr", ".freelist", ".lock", ".blob"] {
            let p = if suf.is_empty() {
                path.to_path_buf()
            } else {
                PathBuf::from(format!("{}{suf}", path.display()))
            };
            let _ = std::fs::remove_file(p);
        }
    }

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
        let path = tmp("batch");
        {
            let mvcc = MVCC::open(&path, 16, 32);
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
        cleanup(&path);
    }

    #[test]
    fn test_scan_prefix_seek() {
        let path = tmp("scan");
        {
            let mvcc = MVCC::open(&path, 16, 32);
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
        cleanup(&path);
    }

    #[test]
    fn test_incr_and_incr_with_ttl() {
        let path = tmp("incr");
        {
            let mvcc = MVCC::open(&path, 16, 32);
            let tx = mvcc.begin_transaction();

            // 不存在 → 从 0 起
            assert_eq!(tx.incr(b"cnt", 1).unwrap(), 1);
            assert_eq!(tx.incr(b"cnt", 5).unwrap(), 6);
            assert_eq!(tx.decr(b"cnt", 2).unwrap(), 4);
            assert_eq!(tx.get(b"cnt"), Some(b"4".to_vec()));

            // 非整数
            assert!(tx.set(b"s", pack_plain(b"abc")));
            assert_eq!(tx.incr(b"s", 1), Err(IncrError::NotInteger));

            // 带 TTL 计数器
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

            // 过期 key 再 incr 视为 0
            let expired = pack_ttl(b"10", now_unix_secs().saturating_sub(5));
            assert!(tx.set(b"oldc", expired));
            assert_eq!(tx.get(b"oldc"), None);
            assert_eq!(tx.incr(b"oldc", 1).unwrap(), 1);

            tx.commit();
        }
        // 重启仍在
        {
            let mvcc = MVCC::open(&path, 16, 32);
            let tx = mvcc.begin_transaction();
            assert_eq!(tx.get(b"cnt"), Some(b"4".to_vec()));
            assert_eq!(tx.incr(b"cnt", 1).unwrap(), 5);
            tx.commit();
        }
        cleanup(&path);
    }


    /// 不要精确 total 的翻页，代价必须只看**窗口宽**，不看库有多大（M15 的验收）。
    ///
    /// 以前 `search_keys` 为了报一个 `total` 把整库折一遍 ⇒ 首页与末页一样贵
    /// （实测 20000 键时首页也要 100 ms 量级）。尺子是本引擎自己的代价单位。
    /// 顺序是**先量窗口、再量全量**：后跑那次缓存更热，偏差只会让断言更难通过，不会骗人。
    /// M21 守卫：**翻页的代价要跟着页宽走，不能跟着库大小走** —— 而且必须测"后面的页"。
    ///
    /// 首页从库头起，游标 `after` 就算没下推它也一样便宜 ⇒ 只测首页会一路绿灯（bitcask 那条
    /// `scan_page_costs_the_page_and_walks_the_whole_library_once` 量的就是首页，这个洞在它眼皮
    /// 底下活了一整个版本）。所以这里量**最后一页**，并要求它与首页同阶。
    /// 尺子用本引擎自己的（`page_reads()`（读了多少次页）），判完代价还判**等价**：最后一页的起点必须正好是全库
    /// 第 `(pages-1)×PAGE` 把键 —— 只测便宜不测内容，就是拿错数据换性能。
    #[test]
    fn test_scan_page_later_pages_cost_the_page_not_the_library() {
        const N: usize = 2000;
        const PAGE: usize = 50;
        let path = tmp("scan_page_later");
        {
            let mvcc = MVCC::open(&path, 64, 1024);
            {
                let tx = mvcc.begin_transaction();
                for i in 0..N {
                    assert!(tx.set(format!("k{i:05}").as_bytes(), format!("v{i}").into_bytes()));
                }
                tx.commit();
            }
            let _ = mvcc.flush();
            let b0 = crate::bplus_tree::page::page_reads();
            let (_first, mut cur) = {
                let tx = mvcc.begin_transaction();
                let p = tx.scan_page(None, PAGE);
                tx.commit();
                p
            };
            let first_cost = crate::bplus_tree::page::page_reads() - b0;

            let mut pages = 1usize;
            let mut last_rows = Vec::new();
            // **只量最后一页**：把整段翻页的总和当一页看，会把 39 页的成本当成一页去断言
            let mut last_cost = 0u64;
            while let Some(c) = cur {
                let b1 = crate::bplus_tree::page::page_reads();
                let tx = mvcc.begin_transaction();
                let (rows, next) = tx.scan_page(Some(&c), PAGE);
                tx.commit();
                last_cost = crate::bplus_tree::page::page_reads() - b1;
                last_rows = rows;
                pages += 1;
                cur = next;
                assert!(pages <= N / PAGE + 2, "游标不前进？已 {pages} 页");
            }

            let want = (pages - 1) * PAGE;
            assert!(!last_rows.is_empty(), "最后一页不能是空的");
            {
                let tx = mvcc.begin_transaction();
                let all = tx.scan(None, None);
                tx.commit();
                assert_eq!(all.len(), N, "全库应折出 {N} 行");
                assert_eq!(
                    last_rows[0].key, all[want].key,
                    "第 {pages} 页的起点不是全库第 {want} 把键 ⇒ 翻页漏行或重行"
                );
                assert_eq!(
                    all[want + last_rows.len() - 1].key,
                    last_rows.last().unwrap().key,
                    "最后一页的终点也对不上"
                );
            }
            eprintln!(
                "[scan_page 翻页] 首页 {first_cost} / 最后一页 {last_cost}（页宽 {PAGE}，共 {pages} 页，N={N}）"
            );
            // 本引擎的尺子数的是**页**不是键：一页 50 行只跨 ~2 片叶子，余量必须按页给
            // （照抄通用界 `2*(first+PAGE)` 会把病灶整个盖住 —— 实测在"游标不下推"的版本上照样绿）
            assert!(
                last_cost <= 2 * first_cost + 8,
                "最后一页付了 {last_cost}，首页只 {first_cost} ⇒ 游标 `after` 没下推成迭代起点，\
                 每页都从库头重走（翻完整库 = O(库²/页宽)）"
            );
            cleanup(&path);
        }
    }

    #[test]
    fn test_search_window_costs_the_window_not_the_library() {
        const N: usize = 2000;
        const PAGE: usize = 20;
        let path = tmp("search_window.db");
        {
            let mvcc = MVCC::open(&path, 64, 64);
            let tx = mvcc.begin_transaction();
            for i in 0..N {
                assert!(tx.set(format!("user{i:05}").as_bytes(), pack_plain(b"x")), "第 {i} 行没写进去");
            }
            tx.commit();

            let base = crate::bplus_tree::page::page_reads();
            let w = mvcc.search_keys(&SearchQuery::contains(b"user", 0, PAGE).without_total());
            let window_cost = crate::bplus_tree::page::page_reads() - base;

            let base = crate::bplus_tree::page::page_reads();
            let full = mvcc.search_keys(&SearchQuery::contains(b"user", 0, PAGE));
            let full_cost = crate::bplus_tree::page::page_reads() - base;

            assert_eq!(full.total, Some(N), "要 total 时语义与改前完全一致");
            assert_eq!(w.total, None, "没数就不许报一个数出来");
            assert_eq!(w.total_pages, None);
            assert_eq!(w.items.len(), PAGE);
            assert_eq!(w.items, full.items, "窗口必须是同一页的全量结果：分页语义一个字节都不许变");
            assert!(w.has_next(), "后面还有 {} 条，必须报得出来", N - PAGE);
            eprintln!("[search 窗口] 首页付了 {window_cost} unit；要精确 total 付了 {full_cost} unit（N={N}）");
            assert!(
                full_cost > 0 && window_cost * 2 <= full_cost,
                "首页付了 {window_cost} / 全量 {full_cost} ⇒ 还在走全库"
            );

            let last = N / PAGE - 1;
            let w2 = mvcc.search_keys(&SearchQuery::contains(b"user", last, PAGE).without_total());
            let f2 = mvcc.search_keys(&SearchQuery::contains(b"user", last, PAGE));
            assert_eq!(w2.items.len(), PAGE);
            assert_eq!(w2.items, f2.items, "同一页的内容与要不要 total 无关");
            assert!(!w2.has_next(), "最后一页之后没有东西了");
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_search_keys_fuzzy_and_paginate() {
        let path = tmp("search");
        {
            let mvcc = MVCC::open(&path, 16, 32);
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

            let with_del = mvcc.search_keys(
                &SearchQuery::contains(b"rape", 0, 10).with_deleted(true),
            );
            assert_eq!(with_del.total, Some(1));
            assert!(with_del.items[0].value.is_none());

            let t2 = mvcc.begin_transaction();
            assert!(t2.set(b"user:draft", b"d".to_vec()));
            let mine = t2.search_keys(&SearchQuery::prefix(b"user:", 0, 20));
            assert_eq!(mine.total, Some(4));
            assert!(mine.items.iter().any(|r| r.key == b"user:draft"));
            t2.rollback();
        }
        cleanup(&path);
    }

    #[test]
    fn test_ttl_and_purge() {
        let path = tmp("ttl");
        {
            let mvcc = MVCC::open(&path, 16, 32);
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
        cleanup(&path);
    }

    #[test]
    fn test_batch_conflict_rolls_back_batch() {        let path = tmp("bconf");
        {
            let mvcc = MVCC::open(&path, 16, 32);
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
        cleanup(&path);
    }

    #[test]
    fn test_scan_foreach_early_stop() {
        let path = tmp("foreach");
        {
            let mvcc = MVCC::open(&path, 16, 32);
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
        cleanup(&path);
    }
}
