//! 本文件负责点查、索引过滤与反向定位。

use super::*;

impl SstFile {
    pub fn get(&self, key: &[u8]) -> Option<StoredEntry> {
        self.get_at(key, u64::MAX)
    }

    /// 在 snapshot `snap_seq` 下查找（取 `seq <= snap_seq` 的最新）
    pub fn get_at(&self, key: &[u8], snap_seq: u64) -> Option<StoredEntry> {
        #[cfg(test)]
        if point_read_reference::enabled() {
            return self.get_at_reference(key, snap_seq);
        }
        self.get_at_project(key, snap_seq, |e| e.owned())
    }

    pub(crate) fn get_light_at(&self, key: &[u8], snap_seq: u64) -> Option<LightEntry> {
        self.get_at_project(key, snap_seq, |e| e.light())
    }
    /// 先按索引和序号定位候选，在共享缓存块中借用记录做投影。
    /// 只复制最终胜出记录的值；同序号比较和快照可见性规则不能因投影而省略。

    /// 遍历键区间内全部匹配序号，只投影最终胜者；持有共享块使加载旧块时胜者仍有效。
    pub(super) fn get_at_project<T>(
        &self,
        key: &[u8],
        snap_seq: u64,
        project: impl Fn(EntryRef<'_>) -> T,
    ) -> Option<T> {
        #[cfg(test)]
        if point_read_reference::enabled() {
            return self
                .get_at_reference(key, snap_seq)
                .as_ref()
                .map(|e| project(EntryRef::of(e)));
        }
        if !self.may_overlap(key, key) {
            return None;
        }
        if !self.bloom.may_contain(key) {
            return None;
        }
        #[cfg(test)]
        note_sst_probe();
        if let Some(e) = &self.entries_cache {
            // 可能同 key 多版本：取 seq 最大且 <= snap
            let lo = e.partition_point(|(k, _)| k.as_slice() < key);
            let hi = e.partition_point(|(k, _)| k.as_slice() <= key);
            let mut best: Option<&StoredEntry> = None;
            for (_, se) in e[lo..hi].iter().rev() {
                if se.visible_at(snap_seq) && best.is_none_or(|b| se.seq > b.seq) {
                    best = Some(se);
                }
            }
            #[cfg(test)]
            note_sst_entry_walks((hi - lo) as u64);
            return best.map(|e| project(EntryRef::of(e)));
        }
        let mut bi = self.find_block(key)?;
        let mut best: Option<(CachedBlock, usize)> = None;
        loop {
            let block = self.load_point_block(bi).ok()?;
            let lo = block.partition(key, false);
            let hi = block.partition(key, true);
            for i in (lo..hi).rev() {
                let se = block.entry(i).1;
                if se.seq <= snap_seq
                    && best
                        .as_ref()
                        .is_none_or(|(b, j)| se.seq > b.entry(*j).1.seq)
                {
                    best = Some((block.clone(), i));
                }
            }
            #[cfg(test)]
            note_sst_entry_walks((hi - lo) as u64);
            // 同键的 seq 可横跨任意多块，甚至不按 seq 排；不能只看 find_block 的最后一块。
            // 本块首键更小则之前全小；首键等于 key 时前块尾部仍可能有同键。
            if bi == 0 || self.index[bi].first_key.as_slice() < key {
                break;
            }
            bi -= 1;
        }
        best.map(|(block, i)| project(block.entry(i).1))
    }

    #[cfg(test)]
    pub(super) fn get_at_reference(&self, key: &[u8], snap_seq: u64) -> Option<StoredEntry> {
        if !self.may_overlap(key, key) || !self.bloom.may_contain(key) {
            return None;
        }
        note_sst_probe();
        if let Some(e) = &self.entries_cache {
            let mut best: Option<StoredEntry> = None;
            for (k, se) in e {
                if k.as_slice() == key
                    && se.visible_at(snap_seq)
                    && best.as_ref().is_none_or(|b| se.seq >= b.seq)
                {
                    best = Some(se.clone());
                }
            }
            note_sst_entry_walks(e.len() as u64);
            return best;
        }
        let mut bi = self.find_block(key)?;
        let mut best: Option<StoredEntry> = None;
        loop {
            let block = self.load_block(bi).ok()?;
            for (k, se) in block.entries.iter().rev() {
                if k.as_slice() == key
                    && se.visible_at(snap_seq)
                    && best.as_ref().is_none_or(|b| se.seq > b.seq)
                {
                    best = Some(se.clone());
                }
            }
            note_sst_entry_walks(block.entries.len() as u64);
            if bi == 0 || self.index[bi].first_key.as_slice() < key {
                break;
            }
            bi -= 1;
        }
        best
    }

    /// 在闭区间 `[low, high]` 内反向查找。稀疏索引定位 high，完整折叠同键的所有可见 seq
    /// 后才交给 accept；版本组可跨块，seq 不要求有序。拒绝一次后不再复活该键的旧值。
    pub fn find_back(
        &self,
        low: &[u8],
        high: &[u8],
        snap_seq: u64,
        accept: impl FnMut(&[u8], &StoredEntry) -> bool,
    ) -> Option<(Vec<u8>, StoredEntry)> {
        self.find_back_project(low, high, snap_seq, |e| e.owned(), accept)
    }

    pub(crate) fn find_back_light(
        &self,
        low: &[u8],
        high: &[u8],
        snap_seq: u64,
        accept: impl FnMut(&[u8], &LightEntry) -> bool,
    ) -> Option<(Vec<u8>, LightEntry)> {
        self.find_back_project(low, high, snap_seq, |e| e.light(), accept)
    }

    pub(super) fn find_back_project<T>(
        &self,
        low: &[u8],
        high: &[u8],
        snap_seq: u64,
        project: impl Fn(EntryRef<'_>) -> T,
        mut accept: impl FnMut(&[u8], &T) -> bool,
    ) -> Option<(Vec<u8>, T)> {
        if !self.may_overlap(low, high) {
            return None;
        }

        let mut pending: Option<(Vec<u8>, u64, T)> = None;
        // None 表示越界/文件结束，收口最后一个组。反向只用 > 替换，保留同 seq 时
        // 正向 range_at 取最后一条的规则。pending 跨块保留，不能每块重置。
        let mut feed = |entry: Option<(&[u8], EntryRef<'_>)>| {
            if pending
                .as_ref()
                .is_some_and(|(pk, _, _)| entry.is_none_or(|(k, _)| k != pk.as_slice()))
            {
                let (k, _, se) = pending.take().unwrap();
                if accept(&k, &se) {
                    return Some((k, se));
                }
            }
            if let Some((k, se)) = entry {
                if se.seq <= snap_seq && pending.as_ref().is_none_or(|(_, seq, _)| se.seq > *seq) {
                    pending = Some((k.to_vec(), se.seq, project(se)));
                }
            }
            None
        };

        if let Some(all) = &self.entries_cache {
            for (k, se) in all.iter().rev() {
                if k.as_slice() > high {
                    continue;
                }
                if k.as_slice() < low {
                    break;
                }
                if let Some(hit) = feed(Some((k, EntryRef::of(se)))) {
                    return Some(hit);
                }
            }
            return feed(None);
        }

        let mut block_i = self.find_block(high)?;
        loop {
            let block = self.load_point_block(block_i).ok()?;
            #[cfg(test)]
            let old = point_read_reference::enabled();
            #[cfg(not(test))]
            let old = false;
            let hi = if old {
                block.len()
            } else {
                block.partition(high, true)
            };
            for i in (0..hi).rev() {
                let (k, se) = block.entry(i);
                if k > high {
                    continue;
                }
                if k < low {
                    return feed(None);
                }
                if let Some(hit) = feed(Some((k, se))) {
                    return Some(hit);
                }
            }
            if block_i == 0 {
                return feed(None);
            }
            block_i -= 1;
        }
    }

    pub(super) fn first_block_for_key(&self, key: &[u8]) -> usize {
        self.index
            .partition_point(|entry| {
                #[cfg(test)]
                INDEX_PROBES.with(|c| c.set(c.get() + 1));
                entry.first_key.as_slice() < key
            })
            .saturating_sub(1)
    }

    pub(super) fn find_block(&self, key: &[u8]) -> Option<usize> {
        if self.index.is_empty() {
            return None;
        }
        // 索引按 `first_key` 升序 ⇒ 要找的是「最后一个 first_key <= key」。
        // 手写二分而不是 `binary_search_by`：守卫要的是**比了几次索引**这个计数，闭包里数不干净。
        // 原先这里是线性扫（`for (i, ie) in index.enumerate()` + 遇大于 key 才 break），
        // 代价 O(块数) = O(库内行数 / 64) —— 点查路径上唯一一处随库总量变贵的纯 CPU。
        let mut lo = 0usize;
        let mut hi = self.index.len();
        let mut found: Option<usize> = None;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            #[cfg(test)]
            INDEX_PROBES.with(|c| c.set(c.get() + 1));
            if self.index[mid].first_key.as_slice() <= key {
                found = Some(mid);
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        found
    }
}
