//! 教学用 Bloom filter（double hashing）。
//!
//! 不追求最优误判率；固定 ~10 bits/key，适合 SST 点查短路。

use crate::lsm_tree::storage::crc32;

/// 可序列化的 bloom
#[derive(Debug, Clone)]
pub struct Bloom {
    bits: Vec<u8>,
    n_hashes: u32,
}

impl Bloom {
    /// 按预期元素个数建空表；`bits_per_key` 典型 10
    pub fn new(expected_items: usize, bits_per_key: usize) -> Self {
        let n_bits = expected_items.saturating_mul(bits_per_key).max(64);
        let n_bytes = (n_bits + 7) / 8;
        // hashes ≈ ln2 * bits_per_key
        let n_hashes = ((bits_per_key as f64) * 0.693).ceil().max(1.0).min(8.0) as u32;
        Self {
            bits: vec![0u8; n_bytes],
            n_hashes,
        }
    }

    pub fn empty() -> Self {
        Self {
            bits: Vec::new(),
            n_hashes: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.bits.is_empty() || self.n_hashes == 0
    }

    pub fn insert(&mut self, key: &[u8]) {
        if self.is_empty() {
            return;
        }
        let (h1, h2) = Self::hash_pair(key);
        let nbits = self.bits.len() * 8;
        for i in 0..self.n_hashes {
            let bit = h1.wrapping_add((i as u64).wrapping_mul(h2)) as usize % nbits;
            self.bits[bit / 8] |= 1 << (bit % 8);
        }
    }

    /// true = 可能存在；false = 一定不存在
    pub fn may_contain(&self, key: &[u8]) -> bool {
        if self.is_empty() {
            return true; // 无 filter 时不拦截
        }
        let (h1, h2) = Self::hash_pair(key);
        let nbits = self.bits.len() * 8;
        for i in 0..self.n_hashes {
            let bit = h1.wrapping_add((i as u64).wrapping_mul(h2)) as usize % nbits;
            if self.bits[bit / 8] & (1 << (bit % 8)) == 0 {
                return false;
            }
        }
        true
    }

    fn hash_pair(key: &[u8]) -> (u64, u64) {
        let c1 = crc32(key);
        // 二次哈希：扰动
        let mut buf = key.to_vec();
        buf.push(0xA5);
        let c2 = crc32(&buf);
        (c1 as u64 | 1, (c2 as u64) | 1) // 保证奇数，周期更好
    }

    /// 序列化：n_hashes u32 + len u32 + bits
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(8 + self.bits.len());
        out.extend_from_slice(&self.n_hashes.to_le_bytes());
        out.extend_from_slice(&(self.bits.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.bits);
        out
    }

    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < 8 {
            return None;
        }
        let n_hashes = u32::from_le_bytes(data[0..4].try_into().ok()?);
        let blen = u32::from_le_bytes(data[4..8].try_into().ok()?) as usize;
        if data.len() < 8 + blen {
            return None;
        }
        Some(Self {
            n_hashes,
            bits: data[8..8 + blen].to_vec(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_basic() {
        let mut b = Bloom::new(100, 10);
        b.insert(b"hello");
        b.insert(b"world");
        assert!(b.may_contain(b"hello"));
        assert!(b.may_contain(b"world"));
        // 负例大概率 false（不保证）
        let neg = b.may_contain(b"not-present-xyz-999");
        // 只断言编码往返
        let enc = b.encode();
        let b2 = Bloom::decode(&enc).unwrap();
        assert!(b2.may_contain(b"hello"));
        let _ = neg;
    }
}
