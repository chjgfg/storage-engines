//! TTL 信封编解码 + key 匹配 / 前缀上界（唯一定义，四个引擎共享）。
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

#[cfg(test)]
use std::thread_local;
use std::time::{SystemTime, UNIX_EPOCH};

use super::types::{IncrError, KeyMatchMode, ValueMeta};

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

/// 解码逻辑 value 字节
pub fn decode_value_meta(logical: &[u8]) -> ValueMeta {
    let (offset, expire) = envelope(logical);
    let user = logical[offset..].to_vec();
    match expire {
        Some(expire_unix_secs) => ValueMeta::Ttl { expire_unix_secs, user },
        None => ValueMeta::Plain { user },
    }
}

// One parser for owned decoding, borrowed metadata and in-place unsealing.
fn envelope(logical: &[u8]) -> (usize, Option<u64>) {
    match logical.first() {
        Some(&VAL_META_PLAIN) => (1, None),
        Some(&VAL_META_TTL) if logical.len() >= 9 =>
            (9, Some(u64::from_le_bytes(logical[1..9].try_into().unwrap()))),
        _ => (0, None),
    }
}

pub(crate) fn logical_expiry(logical: &[u8]) -> Option<u64> { envelope(logical).1 }

/// 逻辑 value → 用户可见 value；过期返回 None
pub fn logical_to_user(mut logical: Vec<u8>) -> Option<Vec<u8>> {
    let (offset, expire) = envelope(&logical);
    if expire.is_some_and(|e| now_unix_secs() >= e) {
        None
    } else {
        #[cfg(test)]
        note_user_value_copied();
        logical.copy_within(offset.., 0);
        logical.truncate(logical.len() - offset);
        Some(logical)
    }
}

/// 只判「这行还活着吗」，**一个字节都不拷**（M35）。
///
/// 为什么不能直接用 [`decode_value_meta`]：它把 user 段 `to_vec()` 出来了，而"数一个数"
/// 要付的应当只有这一判。判据与它**逐条对齐**（空 → 永久；`0x00` → 永久；`0x01` 且长度够
/// → 看 8 字节小端 `expire`，`now >= expire` 算过期；其余按旧数据的裸 value = 永久）。
pub fn logical_is_live(logical: &[u8]) -> bool {
    logical_expiry(logical).is_none_or(|expire| now_unix_secs() < expire)
}

// 测试专用（M35 的尺子）：**用户字节被拷出来过几次**。
//
// 为什么钉这个：`key_count` / `search` 的精确 total 要的只是一个数，而旧形状为了那一个数
// 把界内每行的键与值都物化成 `ExportRecord` 攒进 `Vec`（600k 档实测峰值 **+123.7 MB**、
// `key_count` 2.36 s）。改"只数不物化"之后，计数那条路上应当**一次都不拷**。
// 两个拷贝入口都记在这一把尺子上：[`logical_to_user`]（search 的剥壳）与
// `store::live_record_of_logical`（scan / `key_count` 旧路径的剥壳）。
// ⚠️ 线程内计数 ⇒ 断言取差值（与 M27/M33/M36 同一理由：进程级分配计数会被并行用例污染）。
#[cfg(test)]
thread_local! {
    static USER_VALUE_COPIES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn note_user_value_copied() {
    USER_VALUE_COPIES.with(|c| c.set(c.get() + 1));
}

/// 当前线程拷出来的用户 value 条数（测试探针用）
#[cfg(test)]
pub(crate) fn user_value_copies() -> u64 {
    USER_VALUE_COPIES.with(|c| c.get())
}

#[cfg(test)]
#[test]
fn owned_unseal_reuses_allocation() {
    use std::assert_eq;
    for mut bytes in [pack_plain(b"payload"), pack_ttl(b"payload", u64::MAX), b"payload".to_vec()] {
        bytes.reserve(64);
        let pointer = bytes.as_ptr();
        let user = logical_to_user(bytes).unwrap();
        assert_eq!(user, b"payload");
        assert_eq!(user.as_ptr(), pointer, "unsealing an owned buffer must reuse its allocation");
    }
}

#[cfg(test)]
#[test]
fn unseal_keeps_legacy_tags_and_exact_expiry() {
    use std::{assert_eq, vec};
    for (logical, expected) in [
        (vec![], vec![]), (vec![0], vec![]), (vec![0, 0, 1], vec![0, 1]),
        (vec![1, 2], vec![1, 2]), (vec![2, 0, 1], vec![2, 0, 1]),
        (vec![1, 255, 255, 255, 255, 255, 255, 255, 255, 0, 1], vec![0, 1]),
    ] {
        assert_eq!(decode_value_meta(&logical).user(), expected.as_slice());
        assert_eq!(logical_to_user(logical), Some(expected));
    }
    let expired = vec![1, 0, 0, 0, 0, 0, 0, 0, 0, 42];
    assert_eq!(logical_expiry(&expired), Some(0));
    assert_eq!(logical_to_user(expired), None);
}

/// 解析用户 value 为 `i64`（计数器用）
pub(crate) fn parse_i64_user(user: &[u8]) -> Result<i64, IncrError> {
    let s = std::str::from_utf8(user).map_err(|_| IncrError::NotInteger)?;
    let s = s.trim();
    if s.is_empty() {
        return Err(IncrError::NotInteger);
    }
    s.parse::<i64>().map_err(|_| IncrError::NotInteger)
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

/// 计算「严格大于所有 prefix* 的最小键」；无法进位时返回 None
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
