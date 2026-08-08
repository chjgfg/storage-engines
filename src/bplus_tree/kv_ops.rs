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

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::bplus_tree::mvcc::{ExportRecord, MVCC, Transaction};

// ─── TTL 信封 ───────────────────────────────────────────────────────────────

const VAL_META_PLAIN: u8 = 0x00;
const VAL_META_TTL: u8 = 0x01;

/// 当前 Unix 秒
pub fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// 用户 value → 永久逻辑 value
pub fn pack_plain(user: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + user.len());
    out.push(VAL_META_PLAIN);
    out.extend_from_slice(user);
    out
}

/// 用户 value + 过期时间戳 → 带 TTL 逻辑 value
pub fn pack_ttl(user: &[u8], expire_unix_secs: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 8 + user.len());
    out.push(VAL_META_TTL);
    out.extend_from_slice(&expire_unix_secs.to_le_bytes());
    out.extend_from_slice(user);
    out
}

/// 解析逻辑 value 信封
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueMeta {
    /// 永久（或旧格式裸 value）
    Plain { user: Vec<u8> },
    /// 带绝对过期时间
    Ttl {
        expire_unix_secs: u64,
        user: Vec<u8>,
    },
}

impl ValueMeta {
    pub fn user(&self) -> &[u8] {
        match self {
            ValueMeta::Plain { user } => user,
            ValueMeta::Ttl { user, .. } => user,
        }
    }

    pub fn expire_unix_secs(&self) -> Option<u64> {
        match self {
            ValueMeta::Plain { .. } => None,
            ValueMeta::Ttl {
                expire_unix_secs, ..
            } => Some(*expire_unix_secs),
        }
    }

    pub fn is_expired_at(&self, now: u64) -> bool {
        match self.expire_unix_secs() {
            Some(e) => now >= e,
            None => false,
        }
    }
}

/// 解码逻辑 value 字节（叶子 decode 之后、给用户之前）
pub fn decode_value_meta(logical: &[u8]) -> ValueMeta {
    if logical.is_empty() {
        return ValueMeta::Plain { user: Vec::new() };
    }
    match logical[0] {
        VAL_META_PLAIN => ValueMeta::Plain {
            user: logical[1..].to_vec(),
        },
        VAL_META_TTL if logical.len() >= 9 => {
            let expire = u64::from_le_bytes(logical[1..9].try_into().unwrap());
            ValueMeta::Ttl {
                expire_unix_secs: expire,
                user: logical[9..].to_vec(),
            }
        }
        // 旧数据 / 无 tag：整段当用户 value
        _ => ValueMeta::Plain {
            user: logical.to_vec(),
        },
    }
}

/// 逻辑 value → 用户可见 value；过期返回 None
pub fn logical_to_user(logical: Vec<u8>) -> Option<Vec<u8>> {
    let meta = decode_value_meta(&logical);
    if meta.is_expired_at(now_unix_secs()) {
        None
    } else {
        Some(meta.user().to_vec())
    }
}

// ─── 元数据 ─────────────────────────────────────────────────────────────────

/// `get_meta` 返回结构
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyMeta {
    pub key: Vec<u8>,
    /// 用户 value 字节长度（过期或不存在则无）
    pub value_len: Option<usize>,
    /// 写入该版本的事务 id / 版本号
    pub version: u64,
    /// 绝对过期 Unix 秒；None = 永久
    pub expire_unix_secs: Option<u64>,
    /// 是否已过期（存在但 TTL 到期）
    pub expired: bool,
    /// 是否逻辑删除（tombstone）
    pub deleted: bool,
}

/// 计数器自增错误
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IncrError {
    /// 当前 value 不是合法十进制整数
    NotInteger,
    /// `i64` 加减溢出
    Overflow,
    /// MVCC 写冲突或 set 失败
    WriteConflict,
}

impl std::fmt::Display for IncrError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IncrError::NotInteger => write!(f, "value is not a decimal integer"),
            IncrError::Overflow => write!(f, "i64 overflow"),
            IncrError::WriteConflict => write!(f, "write conflict"),
        }
    }
}

impl std::error::Error for IncrError {}

/// 解析用户 value 为 i64（十进制 ASCII，允许前导 `+`/`-`）
fn parse_i64_user(user: &[u8]) -> Result<i64, IncrError> {
    let s = std::str::from_utf8(user).map_err(|_| IncrError::NotInteger)?;
    let s = s.trim();
    if s.is_empty() {
        return Err(IncrError::NotInteger);
    }
    s.parse::<i64>().map_err(|_| IncrError::NotInteger)
}

// ─── 模糊 / 前缀搜索 + 分页 ─────────────────────────────────────────────────

/// key 匹配模式
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum KeyMatchMode {
    /// 子串匹配：`pattern` 出现在 key 任意位置（默认「模糊」）
    #[default]
    Contains,
    /// 前缀匹配：`key.starts_with(pattern)`
    Prefix,
}

/// 模糊搜索 + 分页查询参数
#[derive(Debug, Clone)]
pub struct SearchQuery {
    /// 匹配模式用的字节串；**空切片表示不过滤（列出全部可见 key）**
    pub pattern: Vec<u8>,
    /// 页码，**从 0 开始**
    pub page: usize,
    /// 每页条数，至少为 1（若传入 0 会按 1 处理）
    pub page_size: usize,
    pub mode: KeyMatchMode,
    /// 是否包含 tombstone（`value=None`）
    pub include_deleted: bool,
}

impl SearchQuery {
    /// 子串模糊 + 分页（最常用）
    pub fn contains(pattern: impl Into<Vec<u8>>, page: usize, page_size: usize) -> Self {
        Self {
            pattern: pattern.into(),
            page,
            page_size,
            mode: KeyMatchMode::Contains,
            include_deleted: false,
        }
    }

    /// 前缀匹配 + 分页
    pub fn prefix(pattern: impl Into<Vec<u8>>, page: usize, page_size: usize) -> Self {
        Self {
            pattern: pattern.into(),
            page,
            page_size,
            mode: KeyMatchMode::Prefix,
            include_deleted: false,
        }
    }

    pub fn with_deleted(mut self, include: bool) -> Self {
        self.include_deleted = include;
        self
    }
}

/// 一页搜索结果
#[derive(Debug, Clone)]
pub struct SearchPage {
    /// 本页记录（按 key 字节序升序）
    pub items: Vec<ExportRecord>,
    /// 匹配总数（未分页前）
    pub total: usize,
    /// 请求的页码（从 0）
    pub page: usize,
    /// 每页大小
    pub page_size: usize,
    /// 总页数（total=0 时为 0）
    pub total_pages: usize,
}

impl SearchPage {
    pub fn has_next(&self) -> bool {
        self.total_pages > 0 && self.page + 1 < self.total_pages
    }

    pub fn has_prev(&self) -> bool {
        self.page > 0 && self.total > 0
    }
}

/// 字节级 key 是否匹配 pattern
pub fn key_matches(key: &[u8], pattern: &[u8], mode: KeyMatchMode) -> bool {
    if pattern.is_empty() {
        return true;
    }
    match mode {
        KeyMatchMode::Contains => {
            if pattern.len() > key.len() {
                return false;
            }
            key.windows(pattern.len()).any(|w| w == pattern)
        }
        KeyMatchMode::Prefix => key.starts_with(pattern),
    }
}

// ─── 前缀上界（用于 [prefix, next_prefix)） ───────────────────────────────

/// 计算「严格大于所有 prefix* 的最小键」；无法进位时返回 None（表示扫到末尾）
pub fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    if prefix.is_empty() {
        return None;
    }
    let mut u = prefix.to_vec();
    while let Some(last) = u.last_mut() {
        if *last < 0xFF {
            *last += 1;
            return Some(u);
        }
        u.pop();
    }
    None
}

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
        use crate::bplus_tree::mvcc::encode_key;
        if keys.is_empty() {
            return;
        }
        let ver = self.version();
        let mut kv = self.kv.lock().unwrap();
        for k in keys {
            let _ = kv.delete(encode_key(k, ver));
        }
        if let Ok(mut active) = self.active_txn.lock() {
            if let Some(writes) = active.get_mut(&ver) {
                writes.retain(|w| !keys.iter().any(|k| k == w));
            }
        }
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

    /// 区间扫描 **[start, end)**：`start ≤ key < end`；`end=None` 表示无上界
    pub fn scan(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Vec<ExportRecord> {
        let now = now_unix_secs();
        self.collect_latest_raw(false)
            .into_iter()
            .filter_map(|(key, _ver, raw)| {
                if let Some(s) = start {
                    if key.as_slice() < s {
                        return None;
                    }
                }
                if let Some(e) = end {
                    if key.as_slice() >= e {
                        return None;
                    }
                }
                let user = raw.and_then(|b| {
                    let m = decode_value_meta(&b);
                    if m.is_expired_at(now) {
                        None
                    } else {
                        Some(m.user().to_vec())
                    }
                })?;
                Some(ExportRecord {
                    key,
                    value: Some(user),
                })
            })
            .collect()
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
    pub fn seek_prev(&self, key: &[u8]) -> Option<ExportRecord> {
        let mut upper = key.to_vec();
        upper.push(0);
        let mut v = self.scan(None, Some(&upper));
        v.retain(|r| r.key.as_slice() <= key);
        v.pop()
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
        let page_size = query.page_size.max(1);
        let page = query.page;
        let matched = self.collect_for_search(
            query.include_deleted,
            Some(query.pattern.as_slice()),
            query.mode,
        );
        let total = matched.len();
        let total_pages = if total == 0 {
            0
        } else {
            (total + page_size - 1) / page_size
        };
        let start = page.saturating_mul(page_size);
        let items = if start >= total {
            Vec::new()
        } else {
            let end = (start + page_size).min(total);
            matched[start..end].to_vec()
        };
        SearchPage {
            items,
            total,
            page,
            page_size,
            total_pages,
        }
    }

    /// 收集最新可见逻辑 KV；可选按 pattern 过滤；剥 TTL。
    fn collect_for_search(
        &self,
        include_deleted: bool,
        pattern: Option<&[u8]>,
        mode: KeyMatchMode,
    ) -> Vec<ExportRecord> {
        self.collect_latest_raw(true)
            .into_iter()
            .filter_map(|(key, _ver, raw)| {
                if let Some(pat) = pattern {
                    if !key_matches(&key, pat, mode) {
                        return None;
                    }
                }
                match raw {
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
                    Some(bytes) => match logical_to_user(bytes) {
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
                }
            })
            .collect()
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

    /// 前缀下存活 key 数量
    pub fn key_count(&self, prefix: &[u8]) -> usize {
        self.prefix_scan(prefix).len()
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
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "kv_ops_{tag}_{}_{}.db",
            std::process::id(),
            nanos
        ))
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
            assert_eq!(p0.total, 2);
            assert_eq!(p0.total_pages, 1);
            assert_eq!(p0.items.len(), 2);
            assert_eq!(p0.items[0].key, b"apple");
            assert_eq!(p0.items[1].key, b"apply");
            assert!(!p0.has_next());

            let pref = mvcc.search_keys(&SearchQuery::prefix(b"user:", 0, 10));
            assert_eq!(pref.total, 3);
            let keys: Vec<_> = pref.items.iter().map(|r| r.key.as_slice()).collect();
            assert_eq!(keys, vec![b"user:1".as_slice(), b"user:10", b"user:2"]);

            let all = mvcc.search_keys(&SearchQuery::contains(Vec::<u8>::new(), 0, 3));
            assert_eq!(all.total, 7);
            assert_eq!(all.total_pages, 3);
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
            assert_eq!(p9.total, 3);
            assert!(p9.items.is_empty());

            let with_del = mvcc.search_keys(
                &SearchQuery::contains(b"rape", 0, 10).with_deleted(true),
            );
            assert_eq!(with_del.total, 1);
            assert!(with_del.items[0].value.is_none());

            let t2 = mvcc.begin_transaction();
            assert!(t2.set(b"user:draft", b"d".to_vec()));
            let mine = t2.search_keys(&SearchQuery::prefix(b"user:", 0, 20));
            assert_eq!(mine.total, 4);
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
    fn test_batch_conflict_rolls_back_batch() {
        let path = tmp("bconf");
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
}
