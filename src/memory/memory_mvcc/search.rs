//! 本文件负责可见版本候选与搜索。

use super::*;

impl Transaction {
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        // 转发给 latest_visible_raw：以前这里复制过一份同样的循环，
        // 于是「改了事务层却漏了点读」成为必然 —— 结构与 bitcask 保持一致。
        self.latest_visible_raw(key)
            .and_then(|(_, v)| v)
            .and_then(crate::memory::kv_ops::logical_to_user)
    }
    /// 从物理版本中选出当前事务可见的逻辑胜者。
    /// 墓碑和过期值仍可能是屏障，不能跳过它们再返回更旧的值。

    /// 最新可见逻辑行（含 TTL 信封；tombstone 为 None）
    pub(crate) fn latest_visible_raw(&self, key: &[u8]) -> Option<(u64, Option<Vec<u8>>)> {
        let store = self.store.lock().unwrap();
        // 从链尾往回走，第一个可见版本就是答案
        let enc = Self::find_version_back(&store, key, |v| self.is_visible(v))?;
        let version = decode_key(&enc)?.version;
        Some((version, store.get(&enc).cloned()?))
    }
    /// 读取选中的物理版本并还原用户值。
    /// 返回的墓碑、缺失和过期状态交给共享可见性判据处理。

    /// 取一组的值：折叠时已经把行从 map 里带出来了（内存引擎没有"另一处"），
    /// 所以凭据本身就是值，`None` 即墓碑。
    pub(super) fn read_row(row: Option<Vec<u8>>) -> crate::common::RowValue {
        match row {
            Some(bytes) => crate::common::RowValue::Value(bytes),
            None => crate::common::RowValue::Tombstone,
        }
    }

    /// 折出**候选组**（一把逻辑 key 一组，值已在手里）：按 enc 升序，组内最后一次覆盖
    /// 就是最大可见版本。`after` 是续跑游标（上一轮最后一个键），`accept` 是键级过滤
    /// —— 放在这里而不是放上层，是为了让"整库折完再筛"变成"不命中的键一个值都不取"。
    ///
    /// ⚠️ 交出一组只能在**组边界**：一组的胜出行要等看见下一组的第一行才算收齐
    /// （版本大端接在 key 后面 ⇒ 组内升序）。在半截组上收手会把旧版本当成最新行。
    /// 返回的 `bool` = 是否已走到界尾（`false` = 是被 `budget` 拦下的，还能续跑）。
    pub(super) fn scan_candidate_refs<'a>(
        &self,
        store: &'a MemStore,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        accept: Option<&dyn Fn(&[u8]) -> bool>,
        after: Option<&[u8]>,
        budget: usize,
    ) -> (
        Vec<crate::common::WinCandidate<&'a Option<Vec<u8>>, &'a [u8]>>,
        bool,
    ) {
        let hi = end.map(|e| crate::common::order::range_end(e, self.core.version, false));
        let lo = crate::common::fold_lower_bound(start, after);
        let mut fold = crate::common::order::CandidateFold::new(
            start,
            end,
            after,
            accept,
            self.core.version,
            budget,
        );
        for (enc, value) in store.iter_entries_in(lo, hi.as_deref()) {
            #[cfg(test)]
            FOLD_KEYS.with(|c| c.set(c.get() + 1));
            if fold.push_borrowed(enc, |v| self.is_visible(v), || value) {
                break;
            }
        }
        fold.finish()
    }
    /// 按物理键范围读取一片候选，再按逻辑键和事务快照折叠版本。
    /// 批尾可能跨越同一逻辑键；游标必须推进，不能因未返回用户值而重复扫描。

    pub(crate) fn scan_candidates(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        accept: Option<&dyn Fn(&[u8]) -> bool>,
        after: Option<&[u8]>,
        budget: usize,
    ) -> (Vec<crate::common::WinCandidate<Option<Vec<u8>>>>, bool) {
        #[cfg(test)]
        if scan_reference::enabled() {
            return self.scan_candidates_reference(start, end, accept, after, budget);
        }
        let store = self.store.lock().unwrap();
        let (rows, complete) = self.scan_candidate_refs(&store, start, end, accept, after, budget);
        (
            rows.into_iter()
                .map(|c| crate::common::WinCandidate {
                    key: c.key.to_vec(),
                    version: c.version,
                    row: {
                        #[cfg(test)]
                        if c.row.is_some() {
                            search_copy_reference::note_copy();
                        }
                        c.row.clone()
                    },
                })
                .collect(),
            complete,
        )
    }

    #[cfg(test)]
    pub(super) fn scan_candidates_reference(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        accept: Option<&dyn Fn(&[u8]) -> bool>,
        after: Option<&[u8]>,
        budget: usize,
    ) -> (Vec<crate::common::WinCandidate<Option<Vec<u8>>>>, bool) {
        let hi = end.map(|e| crate::common::order::range_end(e, self.core.version, false));
        let lo = crate::common::fold_lower_bound(start, after);
        let mut fold = crate::common::order::CandidateFold::new(
            start,
            end,
            after,
            accept,
            self.core.version,
            budget,
        );
        let store = self.store.lock().unwrap();
        for enc in store.iter_keys_in(lo, hi.as_deref()) {
            #[cfg(test)]
            FOLD_KEYS.with(|c| c.set(c.get() + 1));
            if fold.push(enc, |v| self.is_visible(v), || enc.clone()) {
                break;
            }
        }
        let (rows, complete) = fold.finish();
        let rows = rows
            .into_iter()
            .filter_map(|c| {
                store
                    .get(&c.row)
                    .cloned()
                    .map(|row| crate::common::WinCandidate {
                        key: c.key,
                        version: c.version,
                        row,
                    })
            })
            .collect();
        (rows, complete)
    }

    pub fn print_all(&self) -> BTreeMap<Vec<u8>, Option<Vec<u8>>> {
        let mut records = BTreeMap::new();
        for rec in self.export_latest_visible(true) {
            records.insert(rec.key, rec.value);
        }
        records
    }

    /// 导出本事务快照下每个逻辑 key 的最新可见版本（剥 TTL）
    pub fn export_latest_visible(&self, include_deleted: bool) -> Vec<ExportRecord> {
        // M44：这条改成 [`Self::export_latest_visible_for_each`] 的**收集壳** ——
        // 墓碑/过期/读不到那三份判据从此全库只剩一份实现（在
        // [`crate::common::export_rows_streaming`]），"导出"这件事不再有两条会各自漂移的路径。
        // ⚠️ 返回整库的 `Vec` 就是这条的合同：600k 档实测一次 `export 全库(物化)` 峰值
        // **+123.7 MB**。只要"把行交出去"的人（导出到文件、搬几家、数个数）都该走流式那条。
        let mut out: Vec<ExportRecord> = Vec::new();
        self.export_latest_visible_for_each(include_deleted, |rec| {
            out.push(rec);
            true
        });
        out
    }

    /// 流式导出（M44）：折一行、判一行、立刻交给 `on`，**一行都不攒**；`on` 返回 `false`
    /// 就当场收手，后面的键一个都不再折。返回实际交出去的行数。
    ///
    /// 存在的理由：折叠侧从 M27 起就是流式的（[`crate::common::fold_rows_streaming`]），
    /// 可出口这一头只有 `export_latest_visible` —— 它返回 `Vec<ExportRecord>`，于是
    /// "导一个 600k 键的库到文件"要在内存里先把 600k 行摆好，等着一行一行写出去。
    /// 判据（墓碑/过期坏壳/读不到）住在共用件里，这一家只供"逻辑字节 → 用户字节"那一步。
    pub fn export_latest_visible_for_each(
        &self,
        include_deleted: bool,
        mut on: impl FnMut(ExportRecord) -> bool,
    ) -> usize {
        crate::common::export_rows_streaming(
            self,
            include_deleted,
            crate::memory::kv_ops::logical_to_user,
            &mut on,
        )
    }

    pub(super) fn is_visible(&self, version: u64) -> bool {
        // 判据在 [`crate::common::visible_at`]（四引擎共用的一份）
        self.core.is_visible(version)
    }

    /// 从链尾往回找本 key 第一个被 `accept` 的版本，返回它的版本键。
    ///
    /// 同一 key 的编码按版本号升序，但可能被二进制前缀交错；借用反向迭代只复制命中键。
    /// `raw_key` 仍是 `key` 前缀的其它逻辑 key 会落进同一个字节区间（版本号首字节 0x00
    /// 小于任何 ASCII 字节），所以只能**跳过**而不是「见到别的 key 就停」。
    pub(super) fn find_version_back(
        store: &MemStore,
        key: &[u8],
        accept: impl Fn(u64) -> bool,
    ) -> Option<Vec<u8>> {
        let low = encode_key(key, 0);
        let high = encode_key(key, u64::MAX);
        store.find_key_back(&low, &high, |enc| {
            decode_key(enc).is_some_and(|vk| vk.raw_key.as_slice() == key && accept(vk.version))
        })
    }
}

impl crate::common::VersionedStore for Transaction {
    ///
    type Row = Option<Vec<u8>>;

    fn scan_candidates(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        accept: Option<&dyn Fn(&[u8]) -> bool>,
        after: Option<&[u8]>,
        budget: usize,
    ) -> (Vec<crate::common::WinCandidate<Option<Vec<u8>>>>, bool) {
        // 走本类型上的同名 inherent 方法（inherent 优先于 trait ⇒ 不会自我递归）
        self.scan_candidates(start, end, accept, after, budget)
    }

    fn scan_search_candidates(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        accept: Option<&dyn Fn(&[u8]) -> bool>,
        after: Option<&[u8]>,
        budget: usize,
        mut retention: crate::common::store::SearchRetention,
    ) -> (
        Vec<crate::common::WinCandidate<crate::common::store::SearchRow<Self::Row>>>,
        bool,
    ) {
        #[cfg(test)]
        if search_copy_reference::enabled() || scan_reference::enabled() {
            return crate::common::store::owned_search_candidates(
                self, start, end, accept, after, budget, retention,
            );
        }
        let store = self.store.lock().unwrap();
        let (rows, complete) = self.scan_candidate_refs(&store, start, end, accept, after, budget);
        let last = rows.len().saturating_sub(1);
        let rows = rows
            .into_iter()
            .enumerate()
            .map(|(index, c)| {
                let selection = retention.select(c.row.as_deref());
                let row = crate::common::store::retain_search_row(selection, || {
                    #[cfg(test)]
                    if c.row.is_some() {
                        search_copy_reference::note_copy();
                    }
                    c.row.clone()
                });
                #[cfg(test)]
                let copy_all = search_key_reference::enabled();
                #[cfg(not(test))]
                let copy_all = false;
                let key = if copy_all
                    || matches!(&row, crate::common::store::SearchRow::Retained(_))
                    || (!complete && index == last)
                {
                    #[cfg(test)]
                    search_key_reference::note_copy();
                    c.key.to_vec()
                } else {
                    Vec::new()
                };
                crate::common::WinCandidate {
                    key,
                    version: c.version,
                    row,
                }
            })
            .collect();
        (rows, complete)
    }

    fn search_walk(
        &self,
        include_deleted: bool,
        pattern: Option<&[u8]>,
        mode: crate::common::KeyMatchMode,
        skip: usize,
        page_size: usize,
        want_total: bool,
    ) -> crate::common::store::SearchWalkOutcome {
        use crate::common::store::{SearchRetention, SearchSelection, SearchWalkOutcome};
        #[cfg(test)]
        if search_stream_reference::enabled()
            || search_copy_reference::enabled()
            || search_key_reference::enabled()
            || scan_reference::enabled()
        {
            return SearchWalkOutcome::UseDefault;
        }
        let (start, end) = crate::common::search_bounds(pattern, mode);
        let hi = end
            .as_deref()
            .map(|e| crate::common::order::range_end(e, self.core.version, false));
        let accept = |k: &[u8]| pattern.is_none_or(|p| crate::common::key_matches(k, p, mode));
        let mut retention = SearchRetention::new(include_deleted, skip, page_size);
        let mut items = Vec::with_capacity(page_size.min(crate::common::kernel::FOLD_CHUNK));
        let mut total = 0usize;
        let mut has_more = false;
        let mut after: Option<Vec<u8>> = None;
        loop {
            // 保持原切片锁边界，只直接消费已完成组，不再填充两份逐行中间数组。
            let store = self.store.lock().unwrap();
            let lo = crate::common::fold_lower_bound(start, after.as_deref());
            let mut fold = crate::common::order::CandidateFold::new(
                start,
                end.as_deref(),
                after.as_deref(),
                Some(&accept),
                self.core.version,
                crate::common::kernel::FOLD_CHUNK,
            );
            let mut last: Option<&[u8]> = None;
            let mut consume = |c: crate::common::WinCandidate<&Option<Vec<u8>>, _>| {
                last = Some(c.key);
                let selection = retention.select(c.row.as_deref());
                if matches!(selection, SearchSelection::Excluded) {
                    return true;
                }
                total += 1;
                match selection {
                    SearchSelection::Value | SearchSelection::Deleted => {
                        #[cfg(test)]
                        search_key_reference::note_copy();
                        let logical = if matches!(selection, SearchSelection::Value) {
                            #[cfg(test)]
                            search_copy_reference::note_copy();
                            c.row.clone()
                        } else {
                            None
                        };
                        items.push(ExportRecord {
                            key: c.key.to_vec(),
                            expire_unix_secs: logical
                                .as_deref()
                                .and_then(crate::common::codec::logical_expiry),
                            value: logical.and_then(crate::common::logical_to_user),
                        });
                    }
                    SearchSelection::Counted if total > skip.saturating_add(page_size) => {
                        has_more = true;
                        return want_total;
                    }
                    _ => {}
                }
                true
            };
            for (enc, value) in store.iter_entries_in(lo, hi.as_deref()) {
                #[cfg(test)]
                FOLD_KEYS.with(|c| c.set(c.get() + 1));
                let stop = fold.push_borrowed(enc, |v| self.is_visible(v), || value);
                for c in fold.drain_ready() {
                    if !consume(c) {
                        return SearchWalkOutcome::Completed {
                            items,
                            has_more,
                            total,
                        };
                    }
                }
                if stop {
                    break;
                }
            }
            let (tail, complete) = fold.finish();
            for c in tail {
                if !consume(c) {
                    return SearchWalkOutcome::Completed {
                        items,
                        has_more,
                        total,
                    };
                }
            }
            if complete {
                return SearchWalkOutcome::Completed {
                    items,
                    has_more,
                    total,
                };
            }
            let Some(last) = last else {
                unreachable!("incomplete chunk must have a finalized cursor");
            };
            #[cfg(test)]
            search_key_reference::note_copy();
            after = Some(last.to_vec());
        }
    }

    fn materialize(&self, row: Option<Vec<u8>>) -> crate::common::RowValue {
        Self::read_row(row)
    }
}
