//! 反向候选查找与有界末页。

use super::*;

/// 向前查找物理前驱，只保留最大的 n 个存活逻辑键。二进制前缀物理交错，完成判定使用逻辑序。内存随 n 条记录和当前键增长，但时间可能包含全库物理遍历及重复点查；点查必须有界，连续版本共用一次查找。
pub(crate) fn reverse_tail_bounded(
    n: usize,
    mut bound: Option<Vec<u8>>,
    mut prev_encoded: impl FnMut(&[u8]) -> Option<Vec<u8>>,
    mut live: impl FnMut(&[u8]) -> Option<super::super::ExportRecord>,
) -> Vec<super::super::ExportRecord> {
    let mut tail = BTreeMap::<Vec<u8>, super::super::ExportRecord>::new();
    let mut last_checked = None;
    if n == 0 {
        return Vec::new();
    }
    while let Some(enc) = bound.as_deref().and_then(&mut prev_encoded) {
        // 剩余原始键是其编码键的前缀，因此不大于该编码键。
        if tail.len() == n && tail.first_key_value().is_some_and(|(key, _)| &enc <= key) {
            break;
        }
        if let Some(vk) = decode_key(&enc) {
            let raw = vk.raw_key;
            let eligible =
                tail.len() < n || tail.first_key_value().is_some_and(|(key, _)| &raw > key);
            if eligible && !tail.contains_key(&raw) && last_checked.as_ref() != Some(&raw) {
                if let Some(record) = live(&raw) {
                    if tail.len() == n {
                        tail.pop_first();
                    }
                    tail.insert(raw.clone(), record);
                    #[cfg(test)]
                    note_tail_buffer(tail.len());
                }
                last_checked = Some(raw);
            }
        }
        bound = Some(enc);
    }
    tail.into_values().collect()
}

pub(crate) fn reverse_window(
    n: usize,
    max_steps: usize,
    mut bound: Option<Vec<u8>>,
    at_or_below: Option<&[u8]>,
    mut prev_encoded: impl FnMut(&[u8]) -> Option<Vec<u8>>,
    mut live: impl FnMut(&[u8]) -> Option<super::super::ExportRecord>,
) -> ReverseOutcome {
    let mut pending = BTreeSet::<Vec<u8>>::new();
    let mut out = Vec::with_capacity(n.min(4096));
    let mut steps = 0usize;
    while out.len() < n {
        let enc = bound.as_deref().and_then(&mut prev_encoded);
        // 剩余物理键都小于 enc，其逻辑键是自身物理键的前缀；因此不小于 enc 的逻辑组已经完整。
        while pending
            .last()
            .is_some_and(|key| enc.as_ref().is_none_or(|e| key >= e))
        {
            let key = pending.pop_last().expect("checked nonempty");
            if let Some(record) = live(&key) {
                out.push(record);
            }
            if out.len() == n {
                break;
            }
        }
        if out.len() == n || enc.is_none() {
            break;
        }
        let enc = enc.expect("checked Some");
        if steps >= max_steps {
            #[cfg(test)]
            super::super::store::note_reverse_fallback();
            return ReverseOutcome::BudgetExceeded {
                physical_steps: steps,
            };
        }
        steps += 1;
        if let Some(vk) = decode_key(&enc) {
            if at_or_below.is_none_or(|end| vk.raw_key.as_slice() <= end)
                && pending.insert(vk.raw_key)
            {
                #[cfg(test)]
                super::super::store::note_reverse_candidate();
            }
        }
        bound = Some(enc);
    }
    out.reverse();
    ReverseOutcome::Complete(out)
}
