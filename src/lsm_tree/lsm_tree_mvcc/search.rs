//! 本文件负责可见版本候选与搜索。

use super::*;

impl Transaction {
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.latest_visible_raw(key)
            .and_then(|(_, v)| v)
            .and_then(crate::lsm_tree::kv_ops::logical_to_user)
    }
    /// 从物理版本中选出当前事务可见的逻辑胜者。
    /// 墓碑和过期值仍可能是屏障，不能跳过它们再返回更旧的值。

    /// 最新可见逻辑行（含 TTL 信封原始字节；tombstone 为 None）。
    /// 供 [`crate::kv_ops`] 使用，不剥用户 value。
    pub(crate) fn latest_visible_raw(&self, key: &[u8]) -> Option<(u64, Option<Vec<u8>>)> {
        let kvengine = self.kv.lock().unwrap();
        // 从链尾往回走，第一个可见版本就是答案（旧实现把整条链读出来再取最后一个）
        let (enc, val) = Self::find_version_back(&kvengine, key, |v| self.is_visible(v))?;
        #[cfg(test)]
        if crate::lsm_tree::sst::point_read_reference::enabled() {
            return Some((decode_key(&enc)?.version, val));
        }
        Some((crate::common::version::decode_key_ref(&enc)?.1, val))
    }
    /// 读取选中的物理版本并还原用户值。
    /// 返回的墓碑、缺失和过期状态交给共享可见性判据处理。

    /// 取一组的值：跨源点查（`get_value_at` 而不是 `get_raw_key_at` —— 大 value 存的是 blob
    /// 指针，交给调用方之前必须解回真值；GC 那种"要看指针本身"的读法才用 raw 版）。
    /// 外层 `None` = 这一行已经不在值存储里 ⇒ `Missing`（整行不参与，与改动前一致）；
    /// 内层 `None` = 墓碑。这两种情况在折叠侧必须分开，所以 `RowValue` 是三态。
    pub(super) fn read_row(&self, enc: &[u8]) -> crate::common::RowValue {
        use crate::common::RowValue;
        let kvengine = self.kv.lock().unwrap();
        match kvengine.get_value_at(enc, u64::MAX) {
            None => RowValue::Missing,
            Some(None) => RowValue::Tombstone,
            Some(Some(bytes)) => RowValue::Value(bytes),
        }
    }
    /// 按物理键范围读取一片候选，再按逻辑键和事务快照折叠版本。
    /// 批尾可能跨越同一逻辑键；游标必须推进，不能因未返回用户值而重复扫描。

    /// 折出**候选组**（一把逻辑 key 一组，值还没取）。
    ///
    /// ① 共享视图：归并带出胜者的 seq、种类和块引用，SST 值在交付时才复制。
    ///    mem/旧格式借用在逻辑版本折叠结束后仅拥有化胜者；blob 与同 seq 歧义保留显式点查回退。
    ///    旧实现走 `iter()`，而它会先把每个源（含每个 SST 的每行）连 value 一起
    ///    `collect()` 进内存 ⇒ 一次全库折叠的峰值内存约等于整个库。
    ///    kind==2 即 `EntryValue::Drop`：与 `iter()` 的 filter 一致，整行不可见
    ///    （注意是「该 key 不可见」，不是「回退到更旧源里同 key 的 Put」）。
    ///
    /// `after` 是续跑游标、`accept` 是键级过滤、`budget` 是组数上限；返回的 `bool` =
    /// 已走到界尾（`false` = 被预算拦下，还能续）。⚠️ 交出一组只能在组边界。
    pub(crate) fn scan_candidates(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        accept: Option<&dyn Fn(&[u8]) -> bool>,
        after: Option<&[u8]>,
        budget: usize,
    ) -> (
        Vec<crate::common::WinCandidate<crate::lsm_tree::sst::ScanRow>>,
        bool,
    ) {
        let hi = end.map(|e| crate::common::order::range_end(e, self.core.version, false));
        let lo = crate::common::fold_lower_bound(start, after);
        let kv = self.kv.lock().unwrap();
        #[cfg(test)]
        if scan_row_reference::enabled() {
            let mut fold = crate::common::order::CandidateFold::new(
                start,
                end,
                after,
                accept,
                self.core.version,
                budget,
            );
            for (enc, row) in kv.light_iter_range(lo, hi.as_deref(), u64::MAX) {
                crate::lsm_tree::sst::note_light_visit();
                if row.kind == 2 {
                    continue;
                }
                if fold.push(
                    &enc,
                    |v| self.is_visible(v),
                    || crate::lsm_tree::sst::ScanRow::ReferenceLookup(enc.clone()),
                ) {
                    break;
                }
            }
            return fold.finish();
        }
        let mut fold = crate::common::order::CandidateFold::new(
            start,
            end,
            after,
            accept,
            self.core.version,
            budget,
        );
        for (enc, row) in kv.row_iter_range(lo, hi.as_deref(), u64::MAX) {
            #[cfg(test)]
            crate::lsm_tree::sst::note_light_visit();
            if row.kind() == 2 {
                continue;
            }
            if fold.push(&enc, |v| self.is_visible(v), || row) {
                break;
            }
        }
        // 内存表和旧格式值在树锁内借用并完成逻辑折叠；只让最终胜者拥有值，被替换版本不复制。
        let (rows, complete) = fold.finish();
        (
            rows.into_iter()
                .map(|candidate| {
                    let row = candidate
                        .row
                        .freeze(|| encode_key(&candidate.key, candidate.version));
                    crate::common::WinCandidate {
                        key: candidate.key,
                        version: candidate.version,
                        row,
                    }
                })
                .collect(),
            complete,
        )
    }

    pub fn print_all(&self) -> BTreeMap<Vec<u8>, Option<Vec<u8>>> {
        let mut records = BTreeMap::new();
        for rec in self.export_latest_visible(true) {
            records.insert(rec.key, rec.value);
        }
        records
    }

    /// 导出本事务快照下每个逻辑 key 的最新可见版本（已剥 TTL）。
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
            crate::lsm_tree::kv_ops::logical_to_user,
            &mut on,
        )
    }

    pub(super) fn is_visible(&self, version: u64) -> bool {
        // 判据在 [`crate::common::visible_at`]（四引擎共用的一份）
        self.core.is_visible(version)
    }

    /// 从链尾往回找本 key 第一个被 `accept` 的版本，返回它的版本键与逻辑 value。
    ///
    /// 定位交给 [`LSMTree::find_back`]：逐源（新→旧）命中即停，不再把整条版本链连 value
    /// 一起物化出来再挑最后一个。前缀交错的坑照旧 —— 以 `key` 为前缀的更长 key 会落进
    /// 同一个字节区间，所以必须逐条核对 `raw_key == key`。
    pub(super) fn find_version_back(
        kv: &LSMTree,
        key: &[u8],
        accept: impl Fn(u64) -> bool,
    ) -> Option<(Vec<u8>, Option<Vec<u8>>)> {
        // 两个边界只有尾部版本不同；合并成一次分配，避免每次点读多分配一块。
        let bound_len = key.len() + 8;
        let mut bounds = Vec::with_capacity(bound_len * 2);
        bounds.extend_from_slice(key);
        bounds.extend_from_slice(&0_u64.to_be_bytes());
        bounds.extend_from_slice(key);
        bounds.extend_from_slice(&u64::MAX.to_be_bytes());
        let (low, high) = bounds.split_at(bound_len);
        kv.find_back(low, high, u64::MAX, |enc, _val| {
            #[cfg(test)]
            if crate::lsm_tree::sst::point_read_reference::enabled() {
                return decode_key(enc)
                    .is_some_and(|vk| vk.raw_key.as_slice() == key && accept(vk.version));
            }
            crate::common::version::decode_key_ref(enc)
                .is_some_and(|(raw, version)| raw == key && accept(version))
        })
    }

    #[cfg(test)]
    pub(super) fn find_version_back_reference(
        kv: &LSMTree,
        key: &[u8],
        accept: impl Fn(u64) -> bool,
    ) -> Option<(Vec<u8>, Option<Vec<u8>>)> {
        let low = encode_key(key, 0);
        let high = encode_key(key, u64::MAX);
        kv.find_back(&low, &high, u64::MAX, |enc, _val| {
            crate::common::version::decode_key_ref(enc)
                .is_some_and(|(raw, version)| raw == key && accept(version))
        })
    }
}

impl crate::common::VersionedStore for Transaction {
    /// 内联行共享原存储块，blob 和同序号歧义的点查成本用不同类型表达。
    type Row = crate::lsm_tree::sst::ScanRow;

    fn scan_candidates(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        accept: Option<&dyn Fn(&[u8]) -> bool>,
        after: Option<&[u8]>,
        budget: usize,
    ) -> (Vec<crate::common::WinCandidate<Self::Row>>, bool) {
        // 走本类型上的同名 inherent 方法（inherent 优先于 trait ⇒ 不会自我递归）
        self.scan_candidates(start, end, accept, after, budget)
    }

    fn materialize(&self, row: Self::Row) -> crate::common::RowValue {
        use crate::lsm_tree::sst::ScanRow;
        match row {
            ScanRow::Inline(row) => row.materialize(),
            ScanRow::BlobLookup(enc) | ScanRow::AmbiguousLookup(enc) => self.read_row(&enc),
            #[cfg(test)]
            ScanRow::ReferenceLookup(enc) => self.read_row(&enc),
        }
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
        if search_borrow_reference::enabled()
            || crate::common::store::search_materialize_reference::enabled()
            || scan_row_reference::enabled()
        {
            return crate::common::store::owned_search_candidates(
                self, start, end, accept, after, budget, retention,
            );
        }
        let hi = end.map(|e| crate::common::order::range_end(e, self.core.version, false));
        let lo = crate::common::fold_lower_bound(start, after);
        let kv = self.kv.lock().unwrap();
        let mut fold = crate::common::order::CandidateFold::new(
            start,
            end,
            after,
            accept,
            self.core.version,
            budget,
        );
        for (enc, row) in kv.row_iter_range(lo, hi.as_deref(), u64::MAX) {
            #[cfg(test)]
            crate::lsm_tree::sst::note_light_visit();
            if row.kind() == 2 {
                continue;
            }
            if fold.push(&enc, |v| self.is_visible(v), || row) {
                break;
            }
        }
        let (rows, complete) = fold.finish();
        let rows = rows
            .into_iter()
            .map(|c| {
                use crate::lsm_tree::sst::{ScanRow, SearchSourceRow};
                let row = match c
                    .row
                    .select_search(&mut retention, || encode_key(&c.key, c.version))
                {
                    SearchSourceRow::Classified(row) => row,
                    SearchSourceRow::Lookup(row) => {
                        let enc = match row {
                            ScanRow::BlobLookup(enc) | ScanRow::AmbiguousLookup(enc) => enc,
                            _ => unreachable!("only blob and ambiguous records need locked lookup"),
                        };
                        let value = match kv.get_value_at(&enc, u64::MAX) {
                            None => crate::common::RowValue::Missing,
                            Some(None) => crate::common::RowValue::Tombstone,
                            Some(Some(bytes)) => crate::common::RowValue::Value(bytes),
                        };
                        crate::common::store::classify_search_value(value, &mut retention)
                    }
                };
                crate::common::WinCandidate {
                    key: c.key,
                    version: c.version,
                    row,
                }
            })
            .collect();
        (rows, complete)
    }
}
