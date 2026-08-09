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
