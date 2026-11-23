//! 本文件负责顺序、反向与维护扫描。

use super::*;

impl InlineRow {
    pub(crate) fn materialize(self) -> crate::common::RowValue {
        let value = match self.0 {
            InlineStorage::Owned(value) => value,
            InlineStorage::Block(block, pos) => block.entry(pos).1.owned().value,
        };
        match value {
            EntryValue::Put(Some(value)) => crate::common::RowValue::Value(value),
            EntryValue::Put(None) => crate::common::RowValue::Tombstone,
            EntryValue::Drop => crate::common::RowValue::Missing,
        }
    }
}
impl<'a> SourceRow<'a> {
    pub(crate) fn borrowed(seq: u64, value: &'a EntryValue) -> Self {
        Self {
            seq,
            value: SourceValue::Borrowed(value),
            ambiguous: false,
        }
    }
    pub(super) fn entry(&self) -> EntryRef<'_> {
        match &self.value {
            SourceValue::Borrowed(value) => EntryRef {
                seq: self.seq,
                kind: value.kind_byte(),
                value: match value {
                    EntryValue::Put(Some(v)) => v,
                    _ => &[],
                },
            },
            SourceValue::Block(block, pos) => block.entry(*pos).1,
        }
    }
    pub(crate) fn kind(&self) -> u8 {
        self.entry().kind
    }
    pub(crate) fn select_search(
        self,
        retention: &mut crate::common::store::SearchRetention,
        enc: impl FnOnce() -> Vec<u8>,
    ) -> SearchSourceRow {
        let entry = self.entry();
        if self.ambiguous
            || (entry.kind == 0
                && crate::lsm_tree::lsm_tree::LSMTree::blob_ptr_seq(entry.value).is_some())
        {
            return SearchSourceRow::Lookup(self.freeze(enc));
        }
        let logical = if entry.kind == 0 {
            Some(entry.value)
        } else {
            None
        };
        let selection = retention.select(logical);
        SearchSourceRow::Classified(crate::common::store::retain_search_row(
            selection,
            || match self.entry().owned().value {
                EntryValue::Put(value) => value,
                EntryValue::Drop => unreachable!("dropped sources never enter the logical fold"),
            },
        ))
    }
    pub(crate) fn freeze(self, enc: impl FnOnce() -> Vec<u8>) -> ScanRow {
        if self.ambiguous {
            return ScanRow::AmbiguousLookup(enc());
        }
        let entry = self.entry();
        if entry.kind == 0 {
            if crate::lsm_tree::lsm_tree::LSMTree::blob_ptr_seq(entry.value).is_some() {
                return ScanRow::BlobLookup(enc());
            }
        }
        ScanRow::Inline(InlineRow(match self.value {
            SourceValue::Borrowed(value) => InlineStorage::Owned(value.clone()),
            SourceValue::Block(block, pos) => InlineStorage::Block(block, pos),
        }))
    }
}

impl SstFile {
    /// 维护拥有解码块；旧格式驻留行借用值。
    pub(crate) fn vacuum_light_iter<'a>(
        &'a self,
        from: Option<&'a [u8]>,
        to: Option<&'a [u8]>,
        snap_seq: u64,
    ) -> VacuumLightInput<'a> {
        if !self.may_overlap_half_open(from, to) {
            return VacuumLightInput::Empty;
        }
        if self.entries_cache.is_some() {
            return VacuumLightInput::LegacyShared(Box::new(
                self.light_iter(from, to, snap_seq).map(|row| {
                    #[cfg(test)]
                    crate::common::maintenance_probe::note_lsm_vacuum_row(false);
                    Ok(row)
                }),
            ));
        }
        VacuumLightInput::OwnedBlocks(VacuumLightRows {
            sst: self,
            block: from.and_then(|f| self.find_block(f)).unwrap_or(0),
            rows: VacuumBlockRows::Metadata(Vec::new().into_iter()),
            from,
            to,
            snap_seq,
            done: false,
        })
    }

    pub fn range(&self, low: &[u8], high: &[u8]) -> Vec<(Vec<u8>, StoredEntry)> {
        self.range_at(low, high, u64::MAX)
    }

    pub fn range_at(&self, low: &[u8], high: &[u8], snap_seq: u64) -> Vec<(Vec<u8>, StoredEntry)> {
        if !self.may_overlap(low, high) {
            return Vec::new();
        }
        let mut out = Vec::new();
        // 同 key 可能多 seq：按 key 升序扫，每个 key 保留最大可见 seq
        let mut pending: Option<(Vec<u8>, StoredEntry)> = None;
        let flush = |pending: &mut Option<(Vec<u8>, StoredEntry)>,
                     out: &mut Vec<(Vec<u8>, StoredEntry)>| {
            if let Some(p) = pending.take() {
                out.push(p);
            }
        };

        for (k, se) in self.iter_from_key(low) {
            if k.as_slice() < low {
                continue;
            }
            if k.as_slice() > high {
                break;
            }
            if !se.visible_at(snap_seq) {
                continue;
            }
            match &pending {
                Some((pk, pe)) if pk == &k => {
                    if se.seq >= pe.seq {
                        pending = Some((k, se));
                    }
                }
                Some(_) => {
                    flush(&mut pending, &mut out);
                    pending = Some((k, se));
                }
                None => pending = Some((k, se)),
            }
        }
        flush(&mut pending, &mut out);
        out
    }

    /// 从**可能包含 `low` 的数据块**开始流式迭代（升序），用来给区间扫描定位起点。
    ///
    /// 旧 `range_at` 从第 0 块开始逐条 `continue` 到 `low`，于是区间查询的代价与
    /// **整个文件**成正比而不是与命中行数成正比 —— MVCC 点读放大因此变成随库总量增长。
    /// 起点取第一个 first_key >= low 的前一块，保留任意长的跨块同键版本组。
    /// v1 文件整份在 `entries_cache` 里，没有索引，保持从头扫。
    pub(super) fn iter_from_key(&self, low: &[u8]) -> SstEntryIter<'_> {
        let block_i = self.first_block_for_key(low);
        SstEntryIter {
            sst: self,
            block_i,
            entry_i: 0,
            cur: None,
            cache_i: 0,
        }
    }

    /// 本文件在 `snap_seq` 下的**轻量视图**：每个键一条 `(key, LightEntry{seq, kind})`，
    /// 按引用读块，**不克隆任何 value**。
    ///
    /// 磁盘顺序即 key 非降序 ⇒ 相邻同键折叠即可；可见性规则与 [`Self::range_at`] 一致
    /// （同键取 `seq <= snap_seq` 中最大的那个）。
    pub fn light_view(&self, snap_seq: u64) -> Vec<(Vec<u8>, LightEntry)> {
        let mut out: Vec<(Vec<u8>, LightEntry)> = Vec::new();
        let mut feed = |k: &[u8], se: &StoredEntry| {
            if !se.visible_at(snap_seq) {
                return;
            }
            let le = LightEntry::of(se);
            match out.last_mut() {
                Some((pk, p)) if pk.as_slice() == k => {
                    if le.seq >= p.seq {
                        *p = le;
                    }
                }
                _ => out.push((k.to_vec(), le)),
            }
        };

        if let Some(all) = &self.entries_cache {
            for (k, se) in all {
                feed(k, se);
            }
        } else {
            for i in 0..self.index.len() {
                match self.load_block(i) {
                    Ok(b) => {
                        for (k, se) in &b.entries {
                            feed(k, se);
                        }
                    }
                    // 与 iter_all 一致：坏块即止
                    Err(_) => break,
                }
            }
        }
        // 计数尺（M39）：这条路把**整个文件**折进一个 Vec，谁在惰性路径上调用它就必须看得见。
        #[cfg(test)]
        note_light_view_materialized(out.len() as u64);
        out
    }

    /// `[from, to)` 内的轻量视图：**起点靠稀疏索引定位，键一旦越过上界就收手**，
    /// 后面的块一篇都不读。
    ///
    /// 存在的理由是：`light_view` 是把整个文件读完再筛，于是 10 行的窗口要付整文件的块读代价
    /// （大库基线实测：LSM 窗口 scan 405 µs/行 vs 全量 scan 4.0 µs/行）。折叠规则与
    /// `light_view` 完全一致。
    ///
    /// ⚠️ M29 之前这里还有一位 `light_view_break_from`：按「行数预算」切给有界回收用。
    /// 它已删除 —— 有界回收改走本函数（按**键的区间**切，代价与文件里的行数无关），
    /// 而"每源各取满预算"那种切法正是 M29 那条单价随库涨的根因。
    /// **惰性**的轻量视图：一次只往前拉一条，块是一块一块读的。
    ///
    /// 为什么要有它（M15b）：[`Self::light_view_range`] 先把整篇范围 `Vec` 化才交给归并，
    /// 于是"只要前 20 把键"的调用也得把**这个源的所有块**读完 —— 实测 2000 行的库
    /// 首页付 53 次块读，其中 ~32 次就是全库的块数。跨源归并改成惰性之后，
    /// 首页的块读只跟"覆盖到那 20 把键"有关（见 `LSMTree::light_iter_range`）。
    ///
    /// 语义与 `light_view_range` 逐条一致：只交 `visible_at(snap)` 的条目、`[from, to)` 定界、
    /// 同一个 enc key 只可能出现在一个块里（键升序），所以不需要 lookahead 收口。
    pub fn light_iter(
        &self,
        from: Option<&[u8]>,
        to: Option<&[u8]>,
        snap_seq: u64,
    ) -> impl Iterator<Item = (Vec<u8>, LightEntry)> + '_ {
        self.projected_iter(from, to, snap_seq, LightEntry::of, |block, pos| {
            block.entry(pos).1.light()
        })
    }

    pub(crate) fn row_iter<'a>(
        &'a self,
        from: Option<&[u8]>,
        to: Option<&[u8]>,
        snap_seq: u64,
    ) -> Box<dyn Iterator<Item = (Vec<u8>, SourceRow<'a>)> + 'a> {
        self.projected_iter(
            from,
            to,
            snap_seq,
            |entry| SourceRow::borrowed(entry.seq, &entry.value),
            |block, pos| SourceRow {
                seq: block.entry(pos).1.seq,
                value: SourceValue::Block(block.clone(), pos),
                ambiguous: false,
            },
        )
    }

    pub(super) fn projected_iter<'a, R: 'a>(
        &'a self,
        from: Option<&[u8]>,
        to: Option<&[u8]>,
        snap_seq: u64,
        legacy: fn(&'a StoredEntry) -> R,
        project: fn(&CachedBlock, usize) -> R,
    ) -> Box<dyn Iterator<Item = (Vec<u8>, R)> + 'a> {
        if !self.may_overlap_half_open(from, to) {
            return Box::new(std::iter::empty()) as Box<dyn Iterator<Item = _>>;
        }
        let filter = move |(k, se): &'a (Vec<u8>, StoredEntry)| -> Option<(Vec<u8>, R)> {
            if !se.visible_at(snap_seq) {
                return None;
            }
            Some((k.clone(), legacy(se)))
        };
        if let Some(all) = &self.entries_cache {
            // v1 / 小文件：条目本来就在内存里，切片扫一遍即可（无块可读）
            let (lo, hi) = (from.map(|k| k.to_vec()), to.map(|k| k.to_vec()));
            return Box::new(
                all.iter()
                    .skip_while(move |(k, _)| lo.as_deref().is_some_and(|f| k.as_slice() < f))
                    .take_while(move |(k, _)| hi.as_deref().is_none_or(|t| k.as_slice() < t))
                    .filter_map(filter),
            ) as Box<dyn Iterator<Item = _>>;
        }
        // 起点定位到含 `from` 的那个块：前面的块一块都不读。
        // ⚠️ `from` 比第 0 块首键还小 ⇒ 从第 0 块开始（`unwrap_or(0)`，不是 `index.len()`，
        // 那是 M11e 踩过的"真前缀扫直接返回空"）
        let mut start_block = match from {
            Some(f) => self.find_block(f).unwrap_or(0),
            None => 0,
        };
        // find_block 返回起始键不大于 from 的最后一块；同一键可能跨多块，最大序号可能在其中任意块。
        if let Some(from) = from {
            while start_block > 0 && self.index[start_block].first_key.as_slice() >= from {
                start_block -= 1;
            }
        }
        Box::new(SstLightIter {
            sst: self,
            block: start_block,
            pos: 0,
            cur: None,
            from: from.map(|k| k.to_vec()),
            to: to.map(|k| k.to_vec()),
            snap_seq,
            project,
        }) as Box<dyn Iterator<Item = _>>
    }

    pub fn light_view_range(
        &self,
        from: Option<&[u8]>,
        to: Option<&[u8]>,
        snap_seq: u64,
    ) -> Vec<(Vec<u8>, LightEntry)> {
        let mut out: Vec<(Vec<u8>, LightEntry)> = Vec::new();
        let mut feed = |k: &[u8], se: &StoredEntry| {
            if !se.visible_at(snap_seq) {
                return;
            }
            let le = LightEntry::of(se);
            match out.last_mut() {
                Some((pk, p)) if pk.as_slice() == k => {
                    if le.seq >= p.seq {
                        *p = le;
                    }
                }
                _ => out.push((k.to_vec(), le)),
            }
        };

        if let Some(all) = &self.entries_cache {
            for (k, se) in all {
                if to.is_some_and(|t| k.as_slice() >= t) {
                    break;
                }
                if from.is_some_and(|f| k.as_slice() < f) {
                    continue;
                }
                feed(k, se);
            }
        } else {
            // 起点定位到含 `from` 的那个块；前面的块一块不读。
            // ⚠️ 找不到（`from` 比第 0 块的首键还小，比如 `prefix_scan("0")` 这种
            // "前缀是所有键的真前缀"的形状）必须**从第 0 块开始**，不能跳过整篇 ——
            // 我第一版写成 `unwrap_or(index.len())`，于是这类前缀扫直接返回空。
            let start_block = match from {
                Some(f) => self.find_block(f).unwrap_or(0),
                None => 0,
            };
            'blocks: for i in start_block..self.index.len() {
                match self.load_block(i) {
                    Ok(b) => {
                        for (k, se) in &b.entries {
                            if to.is_some_and(|t| k.as_slice() >= t) {
                                break 'blocks; // 磁盘顺序即 key 非降序 ⇒ 后面全在界外
                            }
                            if from.is_some_and(|f| k.as_slice() < f) {
                                continue;
                            }
                            feed(k, se);
                        }
                    }
                    // 与 iter_all / light_view 一致：坏块即止
                    Err(_) => break,
                }
            }
        }
        out
    }

    /// 流式迭代全部 entry（按磁盘顺序）
    pub fn iter_all(&self) -> SstEntryIter<'_> {
        SstEntryIter {
            sst: self,
            block_i: 0,
            entry_i: 0,
            cur: None,
            cache_i: 0,
        }
    }

    pub(crate) fn compaction_input(&self) -> CompactionRows<'_> {
        #[cfg(test)]
        if compaction_reference::enabled() {
            return CompactionRows::CachedReference(self.iter_all());
        }
        match &self.entries_cache {
            Some(entries) => CompactionRows::LegacyShared(entries.iter()),
            None => CompactionRows::OwnedBlocks {
                sst: self,
                block: 0,
                rows: Vec::new().into_iter(),
            },
        }
    }

    /// 把**整个文件**读进一个 `Vec`。
    ///
    /// ⚠️ 只有"文件本来就在内存里（v1 / 小文件）"或"调用方确实要随机访问这份 `Vec`"时该用它。
    /// **压实与层内重叠修复的输入不许用它**（M33）：那些路径现在收 `MergeInput`（一路一个
    /// 迭代器），用这条会把瞬时内存推到"所有输入文件之和"—— 实测 600k 行 61.7 MB 的输入
    /// 在这一步峰值涨到 **154 MB**。尺子是 `merge_inputs_materialized()`（测试专用）。
    pub fn iter_entries(&self) -> Vec<(Vec<u8>, StoredEntry)> {
        let v: Vec<(Vec<u8>, StoredEntry)> = self.iter_all().collect();
        #[cfg(test)]
        note_merge_input_materialized(v.len() as u64);
        v
    }

    pub fn for_each_entry<F>(&self, mut f: F) -> std::io::Result<()>
    where
        F: FnMut(&[u8], &StoredEntry),
    {
        for (k, se) in self.iter_all() {
            f(&k, &se);
        }
        Ok(())
    }
}

impl Iterator for CompactionRows<'_> {
    type Item = std::io::Result<(Vec<u8>, StoredEntry)>;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::LegacyShared(rows) => rows.next().map(|row| {
                #[cfg(test)]
                crate::lsm_tree::lsm_tree::maintenance_probe::input_row(1);
                Ok(row.clone())
            }),
            #[cfg(test)]
            Self::CachedReference(rows) => rows.next().map(|row| {
                crate::lsm_tree::lsm_tree::maintenance_probe::input_row(2);
                Ok(row)
            }),
            Self::OwnedBlocks { sst, block, rows } => loop {
                if let Some(row) = rows.next() {
                    #[cfg(test)]
                    crate::lsm_tree::lsm_tree::maintenance_probe::input_row(0);
                    return Some(Ok(row));
                }
                let ie = sst.index.get(*block)?;
                *block += 1;
                #[cfg(test)]
                crate::lsm_tree::lsm_tree::maintenance_probe::input_block();
                match sst
                    .read_slice(ie.offset, ie.len as usize)
                    .and_then(|raw| decode_block(&raw, sst.has_seq))
                {
                    Ok(decoded) => *rows = decoded.entries.into_iter(),
                    Err(error) => {
                        *block = sst.index.len();
                        return Some(Err(error));
                    }
                }
            },
        }
    }
}

impl Iterator for SstEntryIter<'_> {
    type Item = (Vec<u8>, StoredEntry);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(cache) = &self.sst.entries_cache {
            if self.cache_i >= cache.len() {
                return None;
            }
            #[cfg(test)]
            note_value_materialization();
            let item = cache[self.cache_i].clone();
            self.cache_i += 1;
            return Some(item);
        }
        loop {
            if let Some(block) = &self.cur {
                if self.entry_i < block.entries.len() {
                    #[cfg(test)]
                    note_value_materialization();
                    let item = block.entries[self.entry_i].clone();
                    self.entry_i += 1;
                    return Some(item);
                }
            }
            if self.block_i >= self.sst.index.len() {
                return None;
            }
            match self.sst.load_block(self.block_i) {
                Ok(b) => {
                    self.cur = Some(b);
                    self.entry_i = 0;
                    self.block_i += 1;
                }
                Err(_) => return None,
            }
        }
    }
}
impl Iterator for VacuumLightInput<'_> {
    type Item = std::io::Result<(Vec<u8>, LightEntry)>;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Empty => None,
            Self::OwnedBlocks(rows) => rows.next(),
            Self::LegacyShared(rows) => rows.next(),
        }
    }
}
impl VacuumBlockRows {
    pub(super) fn next(&mut self) -> Option<(Vec<u8>, LightEntry)> {
        match self {
            Self::Metadata(rows) => rows.next(),
            #[cfg(all(test, feature = "lsm-allocation-reference"))]
            Self::Payload(rows) => rows
                .next()
                .map(|(key, entry)| (key, LightEntry::of(&entry))),
        }
    }
    /// 检查字节边界后建立结构化视图。
    /// 借用视图只能活在原缓冲区生命周期内，格式无效时不得按默认值继续读取。
    pub(super) fn decode(raw: &[u8], with_seq: bool) -> std::io::Result<Self> {
        #[cfg(all(test, feature = "lsm-allocation-reference"))]
        if crate::lsm_tree::lsm_tree::allocation_reference::enabled() {
            return decode_block(raw, with_seq).map(|b| Self::Payload(b.entries.into_iter()));
        }
        decode_light_block(raw, with_seq).map(|rows| Self::Metadata(rows.into_iter()))
    }
}

impl Iterator for VacuumLightRows<'_> {
    type Item = std::io::Result<(Vec<u8>, LightEntry)>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        loop {
            if let Some((key, entry)) = self.rows.next() {
                if self.to.is_some_and(|to| key.as_slice() >= to) {
                    self.done = true;
                    return None;
                }
                if self.from.is_some_and(|from| key.as_slice() < from) || entry.seq > self.snap_seq
                {
                    continue;
                }
                #[cfg(test)]
                crate::common::maintenance_probe::note_lsm_vacuum_row(true);
                return Some(Ok((key, entry)));
            }
            let index = self.sst.index.get(self.block)?;
            self.block += 1;
            #[cfg(test)]
            crate::common::maintenance_probe::note_lsm_vacuum_block();
            match self
                .sst
                .read_slice(index.offset, index.len as usize)
                .and_then(|raw| VacuumBlockRows::decode(&raw, self.sst.has_seq))
            {
                Ok(rows) => self.rows = rows,
                Err(error) => {
                    self.done = true;
                    return Some(Err(error));
                }
            }
        }
    }
}

impl<R> Iterator for SstLightIter<'_, R> {
    type Item = (Vec<u8>, R);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.block >= self.sst.index.len() {
                return None;
            }
            if self.cur.is_none() {
                self.cur = self.sst.load_scan_block(self.block).ok();
                self.pos = 0;
                if self.cur.is_none() {
                    // 坏块：跳过这一块而不是整篇放弃（与 eager 版 `match load_block(i)` 同口径）
                    self.block += 1;
                    continue;
                }
            }
            let block = self.cur.as_ref().expect("上面保证有块");
            while self.pos < block.len() {
                let (k, se) = block.entry(self.pos);
                self.pos += 1;
                if self.to.as_deref().is_some_and(|t| k >= t) {
                    return None; // 越界：后面的块只会更大
                }
                if self.from.as_deref().is_some_and(|f| k < f) {
                    continue;
                }
                if se.seq > self.snap_seq {
                    continue;
                }
                return Some((k.to_vec(), (self.project)(block, self.pos - 1)));
            }
            self.cur = None;
            self.block += 1;
        }
    }
}
