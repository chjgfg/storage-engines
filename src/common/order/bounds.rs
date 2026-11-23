//! 逻辑与物理键区间边界。

use super::*;

/// 求不小于 enc 的编码键可能表示的最小逻辑键。小于 enc 的逻辑键只能是它的前缀，其最大编码是该前缀加 max_version。
#[cfg(test)]
pub(crate) fn logical_floor(enc: &[u8], max_version: u64) -> &[u8] {
    let ceiling = max_version.to_be_bytes();
    for split in 0..enc.len() {
        // 普通键的大多数字节高于版本首字节，通常为零；省去逐字节切片比较，但精确边界不变。
        if enc[split] > ceiling[0] {
            continue;
        }
        if ceiling.as_slice() >= &enc[split..] {
            return &enc[..split];
        }
    }
    enc
}

/// 只检查可能不大于待定键的前缀；超过其与 enc 共用字节的前缀更大，不会阻止该键完成。
pub(crate) fn window_before(first: &[u8], enc: &[u8], max_version: u64) -> bool {
    if first >= enc {
        return false;
    }
    let common = first.iter().zip(enc).take_while(|(a, b)| a == b).count();
    let ceiling = max_version.to_be_bytes();
    for split in 0..=common {
        if enc.get(split).is_some_and(|&b| b > ceiling[0]) {
            continue;
        }
        if ceiling.as_slice() >= &enc[split..] {
            return false;
        }
    }
    true
}

/// 构造覆盖逻辑键小于 end 或不大于 end 的物理排他上界。
pub(crate) fn range_end(end: &[u8], max_version: u64, inclusive: bool) -> Vec<u8> {
    let ceiling = max_version.to_be_bytes();
    let mut bound = end.to_vec();
    for split in 0..end.len() {
        if ceiling.as_slice() >= &end[split..] {
            let mut candidate = encode_key(&end[..split], max_version);
            candidate.push(0);
            bound = bound.max(candidate);
        }
    }
    if inclusive {
        let mut candidate = encode_key(end, max_version);
        candidate.push(0);
        bound = bound.max(candidate);
    }
    bound
}
