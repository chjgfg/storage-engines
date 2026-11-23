//! 本文件负责点查、索引过滤与反向定位。

use super::*;

impl L0CandidateOrder {
    pub(super) fn by_max_key(files: &[SstFile]) -> Self {
        let sort = |indexes: &mut [usize]| {
            indexes.sort_unstable_by(|a, b| {
                files[*b]
                    .max_key
                    .cmp(&files[*a].max_key)
                    .then_with(|| b.cmp(a))
            });
        };
        if files.len() <= INLINE_L0_ORDER {
            let mut indexes = [0; INLINE_L0_ORDER];
            for (index, slot) in indexes[..files.len()].iter_mut().enumerate() {
                *slot = index;
            }
            sort(&mut indexes[..files.len()]);
            Self::Inline {
                indexes,
                len: files.len(),
            }
        } else {
            let mut indexes: Vec<_> = (0..files.len()).collect();
            sort(&mut indexes);
            Self::Heap(indexes)
        }
    }

    pub(super) fn as_slice(&self) -> &[usize] {
        match self {
            Self::Inline { indexes, len } => &indexes[..*len],
            Self::Heap(indexes) => indexes,
        }
    }
}

impl LSMTree {
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.deref_value(self.get_at_seq(key, u64::MAX))
    }

    pub fn get_at(&self, snap: &Snapshot, key: &[u8]) -> Option<Vec<u8>> {
        self.deref_value(self.get_at_seq(key, snap.seq))
    }

    pub(super) fn get_at_seq(&self, key: &[u8], snap_seq: u64) -> Option<Vec<u8>> {
        if self.is_ttl_expired(key) {
            return None;
        }
        match self.get_entry_at(key, snap_seq) {
            Some(se) => match se.value {
                EntryValue::Put(Some(v)) => Some(v),
                _ => None,
            },
            None => None,
        }
    }

    pub(super) fn is_ttl_expired(&self, key: &[u8]) -> bool {
        if self.opts.default_ttl_secs.is_none() && self.ttl_meta.is_empty() {
            return false;
        }
        if let Some(&exp) = self.ttl_meta.get(key) {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            return now > exp;
        }
        false
    }

    pub fn get_raw(&self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        match self.get_entry_at(key, u64::MAX) {
            Some(se) => match se.value {
                EntryValue::Put(v) => Some(v),
                EntryValue::Drop => None,
            },
            None => None,
        }
    }

    pub(super) fn get_entry(&self, key: &[u8]) -> Option<StoredEntry> {
        self.get_entry_at(key, u64::MAX)
    }

    /// 测试专用：SST 块总数（自动档的容量凭据）
    #[cfg(test)]
    pub(crate) fn sst_blocks_for_test(&self) -> usize {
        self.sst_block_count()
    }

    /// 测试专用：块缓存里**当前真的住着几块**（M32 用它证明"抬容量不重建缓存"）
    #[cfg(test)]
    pub(crate) fn cached_blocks_for_test(&self) -> usize {
        self.block_cache.lock().unwrap().len()
    }

    /// 测试专用：第 0 层的文件句柄（M30 的耗时拆分要从这里取压实输入）
    #[cfg(test)]
    pub(crate) fn l0_for_test(&self) -> &[SstFile] {
        self.levels.first().map(|l| l.as_slice()).unwrap_or(&[])
    }

    /// 测试专用：把一批**已按键升序**的条目直接写成一个 SST 挂进某一层。
    ///
    /// 为什么要它：`repair_level_overlaps` 要收拾的那个形状（同层两个范围重叠的文件）
    /// **正常压实造不出来** —— L0 是整层并入、同层输出按键有序。它是**崩溃恢复**的产物：
    /// 按 M26 的正确顺序（先记 MANIFEST 再删旧文件），崩在那两步之间就会留下"新+旧同时在层里"。
    /// 手工造那个状态比工程化一次真崩溃便宜，而且造的正是恢复后内存里会出现的那份形状。
    #[cfg(test)]
    pub(crate) fn inject_sst_for_test(
        &mut self,
        level: usize,
        entries: Vec<(Vec<u8>, StoredEntry)>,
    ) -> std::io::Result<()> {
        self.inject_sst_stream_for_test(level, entries)
    }

    #[cfg(test)]
    pub(crate) fn inject_sst_stream_for_test(
        &mut self,
        level: usize,
        entries: impl IntoIterator<Item = (Vec<u8>, StoredEntry)>,
    ) -> std::io::Result<()> {
        let id = self.next_sst_id.fetch_add(1, Ordering::SeqCst);
        let path = self.sst_path(id);
        let mut b = SstBuilder::with_options(
            id,
            &path,
            self.opts.sst_block_entries,
            self.opts.enable_bloom,
        )
        .with_bloom_bits(self.opts.bloom_bits_per_key);
        for (k, se) in entries {
            b.add(k, se);
        }
        let mut sst = b.finish()?;
        sst.set_cache_ns(Self::dir_cache_ns(&self.dir));
        sst.set_block_cache(self.block_cache.clone());
        while self.levels.len() <= level {
            self.levels.push(Vec::new());
        }
        self.levels[level].push(sst);
        self.refresh_level_ranges(level);
        self.stats_dirty = true;
        self.write_manifest()
    }

    pub(super) fn refresh_level_ranges(&mut self, level: usize) {
        self.level_ranges_ordered.resize(self.levels.len(), false);
        self.level_ranges_ordered[level] = self.levels[level]
            .windows(2)
            .all(|pair| pair[0].max_key <= pair[1].min_key);
    }

    /// 返回闭区间可能相交的连续片段，保持原源序（同 seq 的取胜次序不变）。
    /// 正常 L1+ 的 min/max 都单调，可二分两头；端点相等要保留所有相邻文件。
    /// L0 及恢复得到的乱序/重叠层不能二分，仍交全片段给调用方逐篇检查。
    pub(super) fn sst_candidates(&self, level: usize, low: &[u8], high: &[u8]) -> &[SstFile] {
        let range = self.sst_candidate_range(level, low, high);
        &self.levels[level][range]
    }

    pub(super) fn sst_candidate_range(
        &self,
        level: usize,
        low: &[u8],
        high: &[u8],
    ) -> std::ops::Range<usize> {
        let files = &self.levels[level];
        if low > high {
            return 0..0;
        }
        if level == 0 || !self.level_ranges_ordered[level] {
            return 0..files.len();
        }
        let end = files.partition_point(|sst| {
            #[cfg(test)]
            LEVEL_RANGE_PROBES.with(|c| c.set(c.get() + 1));
            sst.min_key.as_slice() <= high
        });
        let start = files[..end].partition_point(|sst| {
            #[cfg(test)]
            LEVEL_RANGE_PROBES.with(|c| c.set(c.get() + 1));
            sst.max_key.as_slice() < low
        });
        start..end
    }

    /// 跨源点查：**每个可能含键的源都要比较，取最大可见 seq**，不能在第一层命中就收手。
    ///
    /// L0 那一段看着确实可以收手（flush 单调：后建的文件只装更晚的 seq），但 L1+ 不行：
    /// `merge_to_ssts` 只按 `target_bytes` 切文件，切的粒度是**编码键**，而同一把 raw key 的
    /// 各版本是各不相同的编码键 ⇒ 一个版本组可以被劈成相邻两个文件（`sst.rs` 里那段
    /// `if b.estimated_bytes() >= target_bytes`）。层内又按 `min_key` 排序，而版本键大端有序
    /// ⇒ **同一个 raw key 的新版本住在后面那个文件**。先命中即停会读回旧版本，而且是静默读回。
    pub(super) fn get_entry_at(&self, key: &[u8], snap_seq: u64) -> Option<StoredEntry> {
        #[cfg(test)]
        vacuum_delete_probe::read();
        let mut best: Option<StoredEntry> = None;
        let consider = |best: &mut Option<StoredEntry>, se: StoredEntry| {
            if !se.visible_at(snap_seq) {
                return;
            }
            if best.as_ref().map(|b| se.seq >= b.seq).unwrap_or(true) {
                *best = Some(se);
            }
        };
        if let Some(se) = Self::mem_get_at(&self.mem, key, snap_seq) {
            consider(&mut best, se);
        }
        for imm in &self.immutables {
            if let Some(se) = Self::mem_get_at(imm, key, snap_seq) {
                consider(&mut best, se);
            }
        }
        if let Some(l0) = self.levels.first() {
            for sst in l0.iter().rev() {
                if let Some(v) = sst.get_at(key, snap_seq) {
                    consider(&mut best, v);
                }
            }
        }
        for level in 1..self.levels.len() {
            for sst in self.sst_candidates(level, key, key) {
                if let Some(v) = sst.get_at(key, snap_seq) {
                    consider(&mut best, v);
                }
            }
        }
        best
    }

    /// 在闭区间 `[low, high]` 内**从大到小**找第一个满足 `accept(key, value)` 的键。
    ///
    /// MVCC 用它取版本链的链尾：版本键 `raw_key ‖ version.to_be_bytes()` 大端有序 ⇒
    /// 同一逻辑 key 的**最新版本排在区间末尾**，各源只需定位自己那一份、命中即停，
    /// 不必像 [`Self::range_scan`] 那样把整段（连 value）物化出来再挑。
    ///
    /// **遮蔽语义必须与 `range_scan` 一致**（这点是被测试逼出来的，不是想当然）：
    /// 一个 key 由跨源最大可见 seq 定论；若定论成 `Drop`，该 key 就整体不可见，不能回退
    /// 到更旧源的 `Put`。候选阶段只找最大键，不按层决定 value；随后由 `get_value_at` 定论。
    ///
    /// 按 SST 的 max_key 降序探测；只要后续源的上界不大于当前候选，便无需读它的数据块。
    /// 正常 L1+ 先二分定位范围再反向走，L0/异常重叠层只排序引用，不改真实源序。
    /// 被拒的 key 进 `excluded` 后重找。若拒绝数超过 `BACK_SCAN_FALLBACK`，回退到同一
    /// snapshot 的一次 `range_scan_at_seq`，避免前缀交错等退化形状反复扫描。
    /// 库里真实存在的**最大键**（KV 层的键 = MVCC 那侧的编码键），只读常驻元数据：
    /// mem/immutables 的 BTreeMap 尾键 + 每篇 SST 的 `max_key`。**一次块 I/O 都不做**
    /// （min/max 从 M26 起驻留在 `SstFile` 上）。
    ///
    /// 为什么要有它：[`LSMTree::find_back`] 的上界是**含端点**的字节界，而任意字节空间里造不出
    /// "最大值"哨兵 —— `\xFF×N` 那种写法会把以它为前缀的更长键整片跳掉（方案第 4 项点过这一条）。
    /// 要"从库尾往回走"就得拿一个**真存在的键**当起点。代价 = O(源数) 的字段比较，无 I/O。
    pub(crate) fn store_max_key(&self) -> Option<Vec<u8>> {
        let mem_max = std::iter::once(&self.mem)
            .chain(self.immutables.iter())
            .filter_map(|m| m.keys().next_back())
            .max();
        let sst_max = self.levels.iter().flatten().map(|s| &s.max_key).max();
        mem_max.into_iter().chain(sst_max).max().cloned()
    }

    pub fn find_back(
        &self,
        low: &[u8],
        high: &[u8],
        snap_seq: u64,
        accept: impl Fn(&[u8], &Option<Vec<u8>>) -> bool,
    ) -> Option<(Vec<u8>, Option<Vec<u8>>)> {
        const BACK_SCAN_FALLBACK: usize = 4096;
        if low > high {
            return None;
        }
        #[cfg(all(test, feature = "lsm-allocation-reference"))]
        if allocation_reference::enabled() {
            return self.find_back_reference(low, high, snap_seq, accept);
        }
        #[cfg(all(test, feature = "lsm-allocation-reference"))]
        if crate::lsm_tree::sst::point_read_reference::enabled() {
            return self.find_back_reference(low, high, snap_seq, accept);
        }

        let mut excluded: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
        loop {
            let Some((key, entry)) = self.max_entry_excluding(low, high, snap_seq, &excluded)
            else {
                return None;
            };
            if excluded.len() >= BACK_SCAN_FALLBACK {
                // 退化形状：回退成一次正向全扫再反向挑（代价与旧实现同阶）
                return self
                    .range_scan_at_seq(low, high, snap_seq)
                    .into_iter()
                    .rev()
                    .find(|(k, v)| accept(k.as_slice(), v));
            }
            // 候选定位时已按 seq 合并所有源，直接用胜出值，省掉一次完整跨 SST 点查。
            let val = match entry.value {
                EntryValue::Put(value) => self.deref_value(value),
                EntryValue::Drop => {
                    excluded.insert(key);
                    continue;
                }
            };
            if accept(&key, &val) {
                return Some((key, val));
            }
            excluded.insert(key);
        }
    }

    #[cfg(all(test, feature = "lsm-allocation-reference"))]
    pub(super) fn find_back_reference(
        &self,
        low: &[u8],
        high: &[u8],
        snap_seq: u64,
        accept: impl Fn(&[u8], &Option<Vec<u8>>) -> bool,
    ) -> Option<(Vec<u8>, Option<Vec<u8>>)> {
        const BACK_SCAN_FALLBACK: usize = 4096;
        let mut excluded = HashSet::new();
        loop {
            let Some(key) = self.max_key_excluding(low, high, snap_seq, &excluded) else {
                return None;
            };
            if excluded.len() >= BACK_SCAN_FALLBACK {
                return self
                    .range_scan_at_seq(low, high, snap_seq)
                    .into_iter()
                    .rev()
                    .find(|(key, value)| accept(key, value));
            }
            excluded.insert(key.clone());
            let Some(value) = self.get_value_at(&key, snap_seq) else {
                excluded.insert(key);
                continue;
            };
            if accept(&key, &value) {
                return Some((key, value));
            }
            excluded.insert(key);
        }
    }

    /// 输入必须按 max_key 降序。等于 cand 的源也可跳过：这里只决定键，值另作跨源点查。
    pub(super) fn sst_max_key<'a>(
        ssts: impl Iterator<Item = &'a SstFile>,
        low: &[u8],
        high: &[u8],
        snap_seq: u64,
        excluded: &HashSet<Vec<u8>>,
        cand: &mut Option<Vec<u8>>,
    ) {
        for sst in ssts {
            if cand
                .as_ref()
                .is_some_and(|k| k.as_slice() >= high || sst.max_key.as_slice() <= k.as_slice())
            {
                break;
            }
            #[cfg(test)]
            let old = crate::lsm_tree::sst::point_read_reference::enabled();
            #[cfg(not(test))]
            let old = false;
            let candidate = if old {
                sst.find_back(low, high, snap_seq, |k, _| !excluded.contains(k))
                    .map(|(key, _)| key)
            } else {
                sst.find_back_light(low, high, snap_seq, |k, _| !excluded.contains(k))
                    .map(|(key, _)| key)
            };
            if let Some(k) = candidate {
                if cand.as_ref().is_none_or(|best| k > *best) {
                    *cand = Some(k);
                }
            }
        }
    }

    /// 某 memtable 在 `[low, high]` 内的最大键（跳过已定论的键）
    pub(super) fn mem_max_key(
        map: &MemMap,
        low: &[u8],
        high: &[u8],
        snap_seq: u64,
        excluded: &std::collections::HashSet<Vec<u8>>,
    ) -> Option<Vec<u8>> {
        #[cfg(all(test, feature = "lsm-allocation-reference"))]
        if allocation_reference::enabled() {
            return map
                .range(low.to_vec()..=high.to_vec())
                .rev()
                .filter(|(k, _)| !excluded.contains(k.as_slice()))
                .find(|(_, vers)| vers.iter().rev().any(|(s, _)| *s <= snap_seq))
                .map(|(k, _)| k.clone());
        }
        map.range::<[u8], _>((
            std::ops::Bound::Included(low),
            std::ops::Bound::Included(high),
        ))
        .rev()
        .filter(|(k, _)| !excluded.contains(k.as_slice()))
        .find(|(_, vers)| vers.iter().rev().any(|(s, _)| *s <= snap_seq))
        .map(|(k, _)| k.clone())
    }

    pub(super) fn mem_max_entry(
        map: &MemMap,
        low: &[u8],
        high: &[u8],
        snap_seq: u64,
        excluded: &HashSet<Vec<u8>>,
    ) -> Option<(Vec<u8>, StoredEntry)> {
        map.range::<[u8], _>((
            std::ops::Bound::Included(low),
            std::ops::Bound::Included(high),
        ))
        .rev()
        .filter(|(key, _)| !excluded.contains(key.as_slice()))
        .find_map(|(key, versions)| {
            versions.range(..=snap_seq).next_back().map(|(seq, value)| {
                (
                    key.clone(),
                    StoredEntry {
                        seq: *seq,
                        value: value.clone(),
                    },
                )
            })
        })
    }

    pub(super) fn consider_back_entry(
        best: &mut Option<(Vec<u8>, StoredEntry, usize)>,
        key: Vec<u8>,
        entry: StoredEntry,
        source_order: usize,
    ) {
        let replace = best.as_ref().is_none_or(|(old_key, old_entry, old_order)| {
            match key.as_slice().cmp(old_key.as_slice()) {
                std::cmp::Ordering::Greater => true,
                std::cmp::Ordering::Less => false,
                std::cmp::Ordering::Equal => {
                    entry.seq > old_entry.seq
                        || (entry.seq == old_entry.seq && source_order >= *old_order)
                }
            }
        });
        if replace {
            *best = Some((key, entry, source_order));
        }
    }

    pub(super) fn consider_sst_back_entry(
        sst: &SstFile,
        low: &[u8],
        high: &[u8],
        snap_seq: u64,
        excluded: &HashSet<Vec<u8>>,
        source_order: usize,
        best: &mut Option<(Vec<u8>, StoredEntry, usize)>,
    ) {
        if let Some((key, entry)) =
            sst.find_back(low, high, snap_seq, |key, _| !excluded.contains(key))
        {
            Self::consider_back_entry(best, key, entry, source_order);
        }
    }

    /// 逐源比较反向候选，并保留候选 SST 已读出的胜出 value，省掉二次跨源点查。
    pub(super) fn max_entry_excluding(
        &self,
        low: &[u8],
        high: &[u8],
        snap_seq: u64,
        excluded: &HashSet<Vec<u8>>,
    ) -> Option<(Vec<u8>, StoredEntry)> {
        let mut best: Option<(Vec<u8>, StoredEntry, usize)> = None;
        let mut source_order = 0usize;
        for map in std::iter::once(&self.mem).chain(self.immutables.iter()) {
            if let Some((key, entry)) = Self::mem_max_entry(map, low, high, snap_seq, excluded) {
                Self::consider_back_entry(&mut best, key, entry, source_order);
            }
            source_order += 1;
        }

        if let Some(l0) = self.levels.first() {
            let by_max = L0CandidateOrder::by_max_key(l0);
            for &index in by_max.as_slice() {
                let sst = &l0[index];
                if sst.may_overlap(low, high)
                    && best
                        .as_ref()
                        .is_none_or(|(key, _, _)| sst.max_key.as_slice() >= key.as_slice())
                {
                    Self::consider_sst_back_entry(
                        sst,
                        low,
                        high,
                        snap_seq,
                        excluded,
                        source_order + l0.len() - 1 - index,
                        &mut best,
                    );
                } else if best
                    .as_ref()
                    .is_some_and(|(key, _, _)| sst.max_key.as_slice() < key.as_slice())
                {
                    break;
                }
            }
            source_order += l0.len();
        }

        for level in 1..self.levels.len() {
            let range = self.sst_candidate_range(level, low, high);
            if level < self.level_ranges_ordered.len() && self.level_ranges_ordered[level] {
                for index in range.rev() {
                    let sst = &self.levels[level][index];
                    if best
                        .as_ref()
                        .is_some_and(|(key, _, _)| sst.max_key.as_slice() < key.as_slice())
                    {
                        break;
                    }
                    Self::consider_sst_back_entry(
                        sst,
                        low,
                        high,
                        snap_seq,
                        excluded,
                        source_order + index,
                        &mut best,
                    );
                }
            } else {
                let mut indexes: Vec<usize> = range.collect();
                indexes.sort_unstable_by(|a, b| {
                    self.levels[level][*b]
                        .max_key
                        .cmp(&self.levels[level][*a].max_key)
                });
                for index in indexes {
                    let sst = &self.levels[level][index];
                    if best
                        .as_ref()
                        .is_some_and(|(key, _, _)| sst.max_key.as_slice() < key.as_slice())
                    {
                        break;
                    }
                    Self::consider_sst_back_entry(
                        sst,
                        low,
                        high,
                        snap_seq,
                        excluded,
                        source_order + index,
                        &mut best,
                    );
                }
            }
            source_order += self.levels[level].len();
        }
        let (key, entry, _) = best?;
        Some((key, entry))
    }

    /// 冲突检查只需要已有 Put 的键和序号，不需要值；物理 Drop 先屏蔽旧 Put，再判断逻辑键是否被接收。
    pub(crate) fn find_key_back(
        &self,
        low: &[u8],
        high: &[u8],
        snap_seq: u64,
        accept: impl Fn(&[u8]) -> bool,
    ) -> KeyBackResult {
        #[cfg(test)]
        if crate::lsm_tree::sst::point_read_reference::enabled() {
            return KeyBackResult::Indexed(
                self.find_back(low, high, snap_seq, |k, _| accept(k))
                    .map(|(k, _)| k),
            );
        }
        if low > high {
            return KeyBackResult::Indexed(None);
        }
        let mut excluded = HashSet::new();
        loop {
            let Some(key) = self.max_key_excluding(low, high, snap_seq, &excluded) else {
                return KeyBackResult::Indexed(None);
            };
            if excluded.len() >= 4096 {
                // 在相同字节区间内只做一次正向归并；接收键不一定是首个原始前缀匹配，序号也不能按层推断。
                let mut found = None;
                for (key, row) in self.light_iter_range(Some(low), None, snap_seq) {
                    if key.as_slice() > high {
                        break;
                    }
                    if row.kind != 2 && accept(&key) {
                        found = Some(key);
                    }
                }
                return KeyBackResult::FullScan(found);
            }
            #[cfg(all(test, feature = "lsm-allocation-reference"))]
            if allocation_reference::enabled() {
                excluded.insert(key.clone());
            }
            if self
                .get_light_entry_at(&key, snap_seq)
                .is_some_and(|row| row.kind != 2)
                && accept(&key)
            {
                return KeyBackResult::Indexed(Some(key));
            }
            #[cfg(all(test, feature = "lsm-allocation-reference"))]
            if allocation_reference::enabled() {
                continue;
            }
            excluded.insert(key);
        }
    }

    pub(super) fn max_key_excluding(
        &self,
        low: &[u8],
        high: &[u8],
        snap_seq: u64,
        excluded: &HashSet<Vec<u8>>,
    ) -> Option<Vec<u8>> {
        let mut candidate = Self::mem_max_key(&self.mem, low, high, snap_seq, excluded);
        for imm in &self.immutables {
            if let Some(key) = Self::mem_max_key(imm, low, high, snap_seq, excluded) {
                if candidate.as_ref().is_none_or(|best| key > *best) {
                    candidate = Some(key);
                }
            }
        }
        for level in 0..self.levels.len() {
            let files = self.sst_candidates(level, low, high);
            if level > 0 && self.level_ranges_ordered[level] {
                Self::sst_max_key(
                    files.iter().rev(),
                    low,
                    high,
                    snap_seq,
                    excluded,
                    &mut candidate,
                );
            } else {
                // L0 和恢复后存在重叠的层不能按文件顺序推断序号顺序。
                let mut by_max: Vec<_> =
                    files.iter().filter(|s| s.may_overlap(low, high)).collect();
                by_max.sort_unstable_by(|a, b| b.max_key.cmp(&a.max_key));
                Self::sst_max_key(
                    by_max.into_iter(),
                    low,
                    high,
                    snap_seq,
                    excluded,
                    &mut candidate,
                );
            }
        }
        candidate
    }

    pub(super) fn get_light_entry_at(&self, key: &[u8], snap_seq: u64) -> Option<LightEntry> {
        let mut best: Option<LightEntry> = None;
        let mut consider = |row: LightEntry| {
            if best.as_ref().is_none_or(|old| row.seq >= old.seq) {
                best = Some(row);
            }
        };
        for mem in std::iter::once(&self.mem).chain(self.immutables.iter()) {
            if let Some((seq, value)) = mem
                .get(key)
                .and_then(|rows| rows.range(..=snap_seq).next_back())
            {
                consider(LightEntry {
                    seq: *seq,
                    kind: value.kind_byte(),
                });
            }
        }
        if let Some(l0) = self.levels.first() {
            for sst in l0.iter().rev() {
                if let Some(row) = sst.get_light_at(key, snap_seq) {
                    consider(row);
                }
            }
        }
        for level in 1..self.levels.len() {
            for sst in self.sst_candidates(level, key, key) {
                if let Some(row) = sst.get_light_at(key, snap_seq) {
                    consider(row);
                }
            }
        }
        best
    }

    /// `get_raw` 的按 seq 版本：跨源取胜后给出该 key 的逻辑 value。
    ///
    /// 外层 `None` = `Drop` 或根本不存在（该 key 不可见）；
    /// `Some(None)` = `Put(None)`，即「行存在但逻辑 value 为 None」—— 与 `range_scan` 的
    /// `(k, None)` 表示一致，两者不能混。
    pub(crate) fn get_raw_key_at(&self, key: &[u8], snap_seq: u64) -> Option<Option<Vec<u8>>> {
        match self.get_entry_at(key, snap_seq)? {
            StoredEntry {
                value: EntryValue::Put(v),
                ..
            } => Some(v),
            StoredEntry {
                value: EntryValue::Drop,
                ..
            } => None,
        }
    }

    /// 把「引擎里存着的字节」解成调用方该看到的字节：大 value 的 blob 指针 → 读回真值。
    ///
    /// ⚠️ 这条是**唯一的解析口**：凡是把存储值交给调用方的路径都必须过这里，过一次就够。
    /// 原来只有 `get`/`get_at` 解析，而 MVCC 的读走的是 `find_back` / `range_scan` / `iter`
    /// 那一族 ⇒ ≥ `blob_threshold`（默认 4 KiB）的值以 9 字节指针回到上层，**不报错**，
    /// 只是变成一个小乱码值（守卫：`test_big_value_roundtrips_through_every_read_path`）。
    /// Bitcask / B+ 树没有这一类洞，因为它们的 blob 分层在 MVCC 之上（storage.rs 的
    /// `decode_stored_value` 是所有读法的必经之路），而 LSM 的分层在引擎**里面**。
    pub(super) fn deref_value(&self, v: Option<Vec<u8>>) -> Option<Vec<u8>> {
        let v = v?;
        Some(Self::resolve_blob(&v, &self.dir).unwrap_or(v))
    }

    /// 用户可见取值：跨源取胜 → 解析 blob 指针。外层 `None` = 不可见。
    ///
    /// 与 [`Self::get_raw_key_at`] 的差别只有一个：这里解析指针。blob GC 必须用**不解析**
    /// 的那一个 —— 它就是靠指针字节认"哪几个 blob 还有人引用"，解析过后反而会把
    /// 幸存的 blob 误判成没人引用。
    pub(crate) fn get_value_at(&self, key: &[u8], snap_seq: u64) -> Option<Option<Vec<u8>>> {
        let stored = self.get_raw_key_at(key, snap_seq)?;
        Some(self.deref_value(stored))
    }
}
