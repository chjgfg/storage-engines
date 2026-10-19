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
//! 逻辑 value 可带信封：
//!
//! ```text
//! 永久： [0x00] user_payload...
//! 带过期：[0x01] expire_unix_secs:u64 LE | user_payload...
//! ```
//!
//! 旧数据无 tag 时按「永久裸 value」兼容。
//! 读路径（`get` / scan / multi_get / search）自动剥信封并丢弃已过期条目。

use std::time::Duration;

use crate::common::codec::parse_i64_user;
use crate::lsm_tree::lsm_tree_mvcc::{MVCC, Transaction, TxnSide};

/// 公共类型与编解码工具现由 [`crate::common`] 统一定义。
///
/// 这里按原路径重导出，保证 `lsm_tree::kv_ops::pack_plain` 等既有调用点不变。
pub use crate::common::{
    decode_value_meta, key_matches, logical_to_user, now_unix_secs, pack_plain, pack_ttl,
    prefix_upper_bound, ExportRecord, IncrError, KeyMatchMode, KeyMeta, SearchPage, SearchQuery,
    ValueMeta,
};


// ─── Transaction 扩展 ───────────────────────────────────────────────────────

impl Transaction {
    // ── 批量写 ──

    /// 批量写入多条 K/V。
    ///
    /// 共用**当前事务**版本；全部成功返回 Ok，任一条冲突则回滚本批写入并
    /// `Err(failed_key)`。调用方成功后自行 `commit()`。
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
        let mut reg = self.core.lock_registry();
        let mut wal = self.wal.lock().unwrap();
        let mut kv = self.kv.lock().unwrap();
        self.core
            .undo(&mut reg, &mut TxnSide { wal: &mut wal, kv: &mut kv }, keys);
    }

    // ── multi_get ──

    /// 一次查询多个 key；返回与 `keys` 等长，不存在/过期为 None
    pub fn multi_get(&self, keys: &[Vec<u8>]) -> Vec<Option<Vec<u8>>> {
        keys.iter().map(|k| self.get(k)).collect()
    }

    // ── 计数器 incr / decr ──

    /// 原子自增（本事务内：读 → 加 → 写）。
    ///
    /// - key 不存在 / 过期 / tombstone：从 0 起算
    /// - 已有 value：十进制 ASCII `i64`
    pub fn incr(&self, key: &[u8], delta: i64) -> Result<i64, IncrError> {
        self.incr_inner(key, delta, None)
    }

    /// 自减：`incr(key, -delta)`
    pub fn decr(&self, key: &[u8], delta: i64) -> Result<i64, IncrError> {
        self.incr(key, delta.checked_neg().ok_or(IncrError::Overflow)?)
    }

    /// 带 TTL 的自增
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
    /// 注意：结果本身仍是物化 `Vec`（API 就是这样）。要**边折边处理**或提前收手，用
    /// [`Self::scan_foreach`] —— 它从 M27 起是真流式：折一行交一行，叫停之后的键不再折、
    /// 值也不再整库取一遍（原先它会先把界内每行折成元组，等于假装流式）。
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
        // 界下推到每一个源：窗口扫的代价必须随窗口宽度缩（块读次数），不是随全库缩
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
    pub fn prefix_scan_foreach(
        &self,
        prefix: &[u8],
        f: impl FnMut(ExportRecord) -> bool,
    ) {
        let end = prefix_upper_bound(prefix);
        self.scan_foreach(Some(prefix), end.as_deref(), f);
    }

    /// 前缀扫描：所有 `key.starts_with(prefix)`
    pub fn prefix_scan(&self, prefix: &[u8]) -> Vec<ExportRecord> {
        let end = prefix_upper_bound(prefix);
        self.scan(Some(prefix), end.as_deref())
    }

    /// 逆序扫描 [start, end)
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
    /// [`LSMTree::find_back`]（逐源、命中即停），单键可见行走 `latest_visible_raw`。
    /// ⚠️ 这里**不能**用整段 `scan`：LSM 的 scan 要跨源归并，一次 seek_prev 会把窗口内所有源
    /// 都拖一遍（200k 实测单次 610 ms，四家最贵）。
    pub fn seek_prev(&self, key: &[u8]) -> Option<ExportRecord> {
        crate::common::seek_prev_with(
            key,
            |bound| {
                let kv = self.kv.lock().unwrap();
                kv.find_back(b"", bound, u64::MAX, |_, _| true)
                    .map(|(enc, _)| enc)
                    .and_then(|enc| crate::common::decode_key(&enc).map(|vk| vk.raw_key))
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

    /// 按 key 模糊/前缀搜索并分页。
    ///
    /// M35：整条实现收进 [`crate::common::search_page`]（四家一份，含"要不要精确 total"那条
    /// 分支 —— 要 total 时改成"只数一遍 + 只取那一页"，不再把界内命中全物化）。
    pub fn search_keys(&self, query: &SearchQuery) -> SearchPage {
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

    /// 续期：保留原用户 value，刷新过期时间
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

// ─── MVCC 便捷包装 ──────────────────────────────────────────────────────────

impl MVCC {
    /// 按 key 模糊/前缀匹配，分页返回最新可见记录
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
    use std::path::PathBuf;

    fn tmp(tag: &str) -> PathBuf {
        crate::common::tmp_test_path("lsm_kv_ops", tag, "")
    }

    fn cleanup(path: &PathBuf) {
        let _ = std::fs::remove_dir_all(path);
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
    fn test_batch_and_multi_get() {
        let path = tmp("batch");
        {
            let mvcc = MVCC::open(&path);
            let tx = mvcc.begin_transaction();
            let items = vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec()),
                (b"c".to_vec(), b"3".to_vec()),
            ];
            tx.batch_set(&items).unwrap();
            let got = tx.multi_get(&[b"a".to_vec(), b"b".to_vec(), b"z".to_vec()]);
            assert_eq!(got[0], Some(b"1".to_vec()));
            assert_eq!(got[1], Some(b"2".to_vec()));
            assert_eq!(got[2], None);
            tx.batch_delete(&[b"b".to_vec()]).unwrap();
            assert!(!tx.exists(b"b"));
            tx.commit();
        }
        cleanup(&path);
    }

    #[test]
    fn test_scan_prefix_seek() {
        let path = tmp("scan");
        {
            let mvcc = MVCC::open(&path);
            let tx = mvcc.begin_transaction();
            for (k, v) in [
                (&b"order:01"[..], &b"a"[..]),
                (&b"order:02"[..], &b"b"[..]),
                (&b"order:10"[..], &b"c"[..]),
                (&b"user:1"[..], &b"u"[..]),
            ] {
                assert!(tx.set(k, pack_plain(v)));
            }
            tx.commit();

            let tx = mvcc.begin_transaction();
            let s = tx.prefix_scan(b"order:");
            assert_eq!(s.len(), 3);
            assert_eq!(s[0].key, b"order:01");
            let one = tx.seek(b"order:05").unwrap();
            assert_eq!(one.key, b"order:10");
            let prev = tx.seek_prev(b"order:05").unwrap();
            assert_eq!(prev.key, b"order:02");
            assert_eq!(tx.key_count(b"order:"), 3);
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
    /// 尺子用本引擎自己的（`light_visits()`（归并吐出多少把键）），判完代价还判**等价**：最后一页的起点必须正好是全库
    /// 第 `(pages-1)×PAGE` 把键 —— 只测便宜不测内容，就是拿错数据换性能。
    #[test]
    fn test_scan_page_later_pages_cost_the_page_not_the_library() {
        const N: usize = 2000;
        const PAGE: usize = 50;
        let path = tmp("scan_page_later");
        {
            let mvcc = MVCC::open(&path);
            {
                let tx = mvcc.begin_transaction();
                for i in 0..N {
                    assert!(tx.set(format!("k{i:05}").as_bytes(), pack_plain(b"v")));
                }
                tx.commit();
            }
            let _ = mvcc.flush();
            let b0 = crate::lsm_tree::sst::light_visits();
            let (_first, mut cur) = {
                let tx = mvcc.begin_transaction();
                let p = tx.scan_page(None, PAGE);
                tx.commit();
                p
            };
            let first_cost = crate::lsm_tree::sst::light_visits() - b0;

            let mut pages = 1usize;
            let mut last_rows = Vec::new();
            // **只量最后一页**：把整段翻页的总和当一页看，会把 39 页的成本当成一页去断言
            let mut last_cost = 0u64;
            while let Some(c) = cur {
                let b1 = crate::lsm_tree::sst::light_visits();
                let tx = mvcc.begin_transaction();
                let (rows, next) = tx.scan_page(Some(&c), PAGE);
                tx.commit();
                last_cost = crate::lsm_tree::sst::light_visits() - b1;
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
            assert!(
                last_cost <= 2 * (first_cost + PAGE as u64),
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
        let path = tmp("search_window");
        {
            let mvcc = MVCC::open(&path);
            let tx = mvcc.begin_transaction();
            for i in 0..N {
                assert!(tx.set(format!("user{i:05}").as_bytes(), pack_plain(b"x")), "第 {i} 行没写进去");
            }
            tx.commit();
            let _ = mvcc.flush();

            let vb = crate::lsm_tree::sst::light_visits();
            let bb = crate::lsm_tree::sst::block_reads();
            let w = mvcc.search_keys(&SearchQuery::contains(b"user", 0, PAGE).without_total());
            let window_cost = crate::lsm_tree::sst::light_visits() - vb;
            let window_blocks = crate::lsm_tree::sst::block_reads() - bb;

            let vb = crate::lsm_tree::sst::light_visits();
            let bb = crate::lsm_tree::sst::block_reads();
            let full = mvcc.search_keys(&SearchQuery::contains(b"user", 0, PAGE));
            let full_cost = crate::lsm_tree::sst::light_visits() - vb;
            let full_blocks = crate::lsm_tree::sst::block_reads() - bb;

            assert_eq!(full.total, Some(N), "要 total 时语义与改前完全一致");
            assert_eq!(w.total, None, "没数就不许报一个数出来");
            assert_eq!(w.total_pages, None);
            assert_eq!(w.items.len(), PAGE);
            assert_eq!(w.items, full.items, "窗口必须是同一页的全量结果：分页语义一个字节都不许变");
            assert!(w.has_next(), "后面还有 {} 条，必须报得出来", N - PAGE);
            eprintln!(
                "[search 窗口] 首页 {window_cost} 把键 / {window_blocks} 块；全量 {full_cost} 把 / {full_blocks} 块（N={N}）"
            );

            assert!(
                full_cost > 0 && window_cost * 2 <= full_cost,
                "首页付了 {window_cost} / 全量 {full_cost} ⇒ 还在走全库"
            );
            // 块读也要跟着窗口走。**这条才是 M15b 的守卫**：上一行只数"归并吐出多少键",
            // 而旧的 `scan_latest_light_range` 先把每个源整篇物化 ⇒ 键数看着对了,块却读满全库
            // （实测首页 53 块,而这库总共才 32 块）。判据取"每交付一把键最多两块"——
            // 内含每个源启动一块的余量,改回 eager 就会红。
            assert!(
                window_blocks <= 2 * (window_cost + 1),
                "首页交付 {window_cost} 把键却读了 {window_blocks} 个块 ⇒ 源被整篇物化了，                 不是惰性拉取（改回 `scan_latest_light_range` 正是这个形状）"
            );

            let last = N / PAGE - 1;
            let w2 = mvcc.search_keys(&SearchQuery::contains(b"user", last, PAGE).without_total());
            let f2 = mvcc.search_keys(&SearchQuery::contains(b"user", last, PAGE));
            assert_eq!(w2.items.len(), PAGE);
            assert_eq!(w2.items, f2.items, "同一页的内容与要不要 total 无关");
            assert!(!w2.has_next(), "最后一页之后没有东西了");
        }
        cleanup(&path);
    }

    #[test]
    fn test_search_pagination() {
        let path = tmp("search");
        {
            let mvcc = MVCC::open(&path);
            let tx = mvcc.begin_transaction();
            for k in [&b"apple"[..], &b"apply"[..], &b"banana"[..], &b"user:1"[..]] {
                assert!(tx.set(k, pack_plain(b"v")));
            }
            tx.commit();

            let page = mvcc.search_keys(&SearchQuery::contains(b"pp", 0, 10));
            assert_eq!(page.total, Some(2));
            assert_eq!(page.items.len(), 2);

            let p0 = mvcc.search_keys(&SearchQuery::prefix(b"user:", 0, 1));
            assert_eq!(p0.total, Some(1));
            assert!(!p0.has_next());
        }
        cleanup(&path);
    }

    #[test]
    fn test_incr_and_ttl() {
        let path = tmp("incr");
        {
            let mvcc = MVCC::open(&path);
            let tx = mvcc.begin_transaction();
            assert_eq!(tx.incr(b"n", 1).unwrap(), 1);
            assert_eq!(tx.incr(b"n", 9).unwrap(), 10);
            assert_eq!(tx.decr(b"n", 3).unwrap(), 7);
            assert_eq!(tx.get(b"n"), Some(b"7".to_vec()));

            assert_eq!(
                tx.incr_with_ttl(b"hits", 1, Duration::from_secs(60))
                    .unwrap(),
                1
            );
            assert!(matches!(tx.get_ttl(b"hits"), Some(Some(_))));

            assert!(tx.set_with_ttl(b"tmp", b"x".to_vec(), Duration::from_secs(120)));
            assert!(tx.exists(b"tmp"));
            assert!(tx.persist(b"tmp"));
            assert_eq!(tx.get_ttl(b"tmp"), Some(None));

            // 过期清理
            let stale = pack_ttl(b"old", now_unix_secs().saturating_sub(5));
            assert!(tx.set(b"gone", stale));
            assert!(!tx.exists(b"gone"));
            let n = tx.purge_expired();
            assert!(n >= 1);
            tx.commit();
        }
        cleanup(&path);
    }

    #[test]
    fn test_meta_and_exists() {
        let path = tmp("meta");
        {
            let mvcc = MVCC::open(&path);
            let tx = mvcc.begin_transaction();
            assert!(tx.set(b"k", pack_plain(b"hello")));
            assert!(tx.exists(b"k"));
            let m = tx.get_meta(b"k").unwrap();
            assert_eq!(m.value_len, Some(5));
            assert!(!m.deleted);
            assert!(!m.expired);
            assert!(tx.delete(b"k"));
            assert!(!tx.exists(b"k"));
            tx.commit();
        }
        cleanup(&path);
    }

    #[test]
    fn test_prefix_upper_bound() {
        assert_eq!(
            prefix_upper_bound(b"ab"),
            Some(b"ac".to_vec())
        );
        assert_eq!(prefix_upper_bound(b"a\xff"), Some(b"b".to_vec()));
    }
}
