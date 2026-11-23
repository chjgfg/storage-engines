//! 本文件负责顺序、反向与维护扫描。

use super::*;

/// 经典扫描迭代器：只返回存活键（跳过软 tombstone）
///
/// 教学实现：先物化匹配的 KeyLoc，再按需从多分片读 value。
pub struct ScanIterator<'a> {
    items: Vec<(Vec<u8>, KeyLoc)>,
    idx: usize,
    rev: bool,
    eng: &'a mut Bitcask,
}

impl<'a> ScanIterator<'a> {
    pub(super) fn read_item(
        &mut self,
        key: Vec<u8>,
        loc: KeyLoc,
    ) -> Option<<Self as Iterator>::Item> {
        match loc {
            KeyLoc::SoftTombstone { .. } => None,
            KeyLoc::Live { file_id, pos, len } => {
                let value = self.eng.read_live(file_id, pos, len);
                Some(value.map(|v| (key, v)))
            }
        }
    }
}

impl<'a> Iterator for ScanIterator<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.rev {
            // 若混用 next/next_back，按当前位置前进
            while self.idx < self.items.len() {
                let (key, loc) = self.items[self.idx].clone();
                self.idx += 1;
                if let Some(mapped) = self.read_item(key, loc) {
                    return Some(mapped);
                }
            }
            return None;
        }
        while self.idx < self.items.len() {
            let (key, loc) = self.items[self.idx].clone();
            self.idx += 1;
            if let Some(mapped) = self.read_item(key, loc) {
                return Some(mapped);
            }
        }
        None
    }
}

impl<'a> DoubleEndedIterator for ScanIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.rev = true;
        while !self.items.is_empty() && self.idx < self.items.len() {
            // 从末尾弹：items[idx..len] 为尚未消费区间
            let last = self.items.len() - 1;
            if last < self.idx {
                break;
            }
            let (key, loc) = self.items.remove(last);
            if let Some(mapped) = self.read_item(key, loc) {
                return Some(mapped);
            }
        }
        None
    }
}

impl Bitcask {
    pub fn scan(&mut self, range: impl std::ops::RangeBounds<Vec<u8>>) -> ScanIterator<'_> {
        let _ = self.active.flush_buf();
        let items: Vec<(Vec<u8>, KeyLoc)> = self
            .key_dir
            .range(range)
            .map(|(k, loc)| (k.clone(), loc.clone()))
            .collect();
        ScanIterator {
            items,
            idx: 0,
            rev: false,
            eng: self,
        }
    }

    pub fn scan_prefix(&mut self, prefix: &[u8]) -> ScanIterator<'_> {
        let start = Bound::Included(prefix.to_vec());
        let mut bound_prefix = prefix.to_vec();
        if let Some(last) = bound_prefix.iter_mut().last() {
            *last = last.saturating_add(1);
        }
        let end = Bound::Excluded(bound_prefix);
        self.scan((start, end))
    }

    /// 闭区间 `[low, high]` 扫描（含软 tombstone）
    pub fn range_scan(
        &mut self,
        low: Vec<u8>,
        high: Vec<u8>,
    ) -> Result<Vec<(Vec<u8>, Option<Vec<u8>>)>> {
        self.usable()?;
        self.active.flush_buf()?;
        let keys: Vec<(Vec<u8>, KeyLoc)> = self
            .key_dir
            .range(low..=high)
            .map(|(k, loc)| (k.clone(), loc.clone()))
            .collect();
        let mut out = Vec::with_capacity(keys.len());
        for (k, loc) in keys {
            match loc {
                KeyLoc::SoftTombstone { .. } => out.push((k, None)),
                KeyLoc::Live { file_id, pos, len } => {
                    let v = self.read_live(file_id, pos, len)?;
                    out.push((k, Some(v)));
                }
            }
        }
        Ok(out)
    }

    /// 闭区间 `[low, high]` 内**从大到小**最多取 `max` 条索引项，**不读 value**。
    ///
    /// 返回条数 `< max` 说明已走到区间下界 —— 调用方由此可以断定「整段都看过了」，
    /// 于是在只需要区间末尾几条时提前退出，而不必像 [`Self::range_scan`] 那样物化整段并逐行读盘。
    pub fn range_meta_back(&self, low: &[u8], high: &[u8], max: usize) -> Vec<(Vec<u8>, KeyLoc)> {
        self.key_dir
            .range(low.to_vec()..=high.to_vec())
            .rev()
            .take(max)
            .map(|(k, loc)| (k.clone(), loc.clone()))
            .collect()
    }

    /// 遍历版本范围时借用 KeyDir，只保留胜者的键副本。
    pub(crate) fn find_meta_back(
        &self,
        low: &[u8],
        high: &[u8],
        mut accept: impl FnMut(&[u8]) -> bool,
    ) -> Option<Vec<u8>> {
        use std::ops::Bound;
        self.key_dir
            .range::<[u8], _>((Bound::Included(low), Bound::Included(high)))
            .rev()
            .find_map(|(key, _)| accept(key).then(|| key.clone()))
    }

    /// 全量扫描（含软 tombstone）
    pub fn iter(&mut self) -> Result<Vec<(Vec<u8>, Option<Vec<u8>>)>> {
        self.usable()?;
        self.active.flush_buf()?;
        let keys: Vec<(Vec<u8>, KeyLoc)> = self
            .key_dir
            .iter()
            .map(|(k, loc)| (k.clone(), loc.clone()))
            .collect();
        let mut out = Vec::with_capacity(keys.len());
        for (k, loc) in keys {
            match loc {
                KeyLoc::SoftTombstone { .. } => out.push((k, None)),
                KeyLoc::Live { file_id, pos, len } => {
                    let v = self.read_live(file_id, pos, len)?;
                    out.push((k, Some(v)));
                }
            }
        }
        Ok(out)
    }

    /// 只扫 KeyDir 元数据，不读 value（vacuum / 统计用）
    pub fn iter_meta(&self) -> impl Iterator<Item = (&Vec<u8>, &KeyLoc)> {
        self.key_dir.iter()
    }

    /// KeyDir 里**最大的一把编码键**（给"从库尾反向"当起点用，M82）。
    ///
    /// KeyDir 是 `BTreeMap` ⇒ `next_back` 是 O(log n)，不扫库。为什么不给调用方一个"足够大的
    /// 哨兵"当上界：任意字节空间没有最大值，哨兵长度不够就会把更长的尾键整批漏掉。
    pub fn last_meta_key(&self) -> Option<Vec<u8>> {
        self.key_dir.keys().next_back().cloned()
    }

    /// 从 `from`（**含**）起的 KeyDir 元数据升序视图，不读 value。
    ///
    /// 下界落在 enc 域（`raw_key ‖ version`），不是逻辑 key 域：以 `from` 为真前缀
    /// 的其它 key 可能排在 `from` 之前，所以想「跳过某个逻辑 key 的整组」必须由调用方
    /// 自己按 `raw_key > from` 过滤，光靠这个下界会漏行。
    pub fn iter_meta_from(&self, from: Option<&[u8]>) -> impl Iterator<Item = (&Vec<u8>, &KeyLoc)> {
        let lo = match from {
            Some(k) => std::ops::Bound::Included(k),
            None => std::ops::Bound::Unbounded,
        };
        self.key_dir
            .range::<[u8], _>((lo, std::ops::Bound::Unbounded))
    }

    pub fn len(&self) -> usize {
        self.key_dir.len()
    }

    pub fn is_empty(&self) -> bool {
        self.key_dir.is_empty()
    }
}
