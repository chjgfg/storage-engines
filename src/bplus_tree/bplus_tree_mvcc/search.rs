//! 本文件负责可见版本候选与搜索。

use super::*;

impl Transaction {
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        // 转发给 latest_visible_raw：以前这里复制过一份同样的循环，正是
        // 「改了事务层却漏掉点读」的形状。与 bitcask 保持一致。
        self.latest_visible_raw(key)
            .and_then(|(_, v)| v)
            .and_then(|raw| crate::bplus_tree::kv_ops::logical_to_user(raw))
    }
    /// 从物理版本中选出当前事务可见的逻辑胜者。
    /// 墓碑和过期值仍可能是屏障，不能跳过它们再返回更旧的值。

    /// 最新可见逻辑行（含 TTL 信封原始字节；tombstone 为 None）
    /// 供 [`crate::kv_ops`] 做 exists / meta / scan，不剥用户 value。
    pub(crate) fn latest_visible_raw(&self, key: &[u8]) -> Option<(u64, Option<Vec<u8>>)> {
        let mut kvengine = self.kv.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();
        // 从链尾往回走，第一个可见版本就是答案；value 随叶子一起到手，不再二次读页
        let (enc, row) = Self::find_version_back(&mut kvengine, key, |v| self.is_visible(v))?;
        let version = decode_key(&enc)?.version;
        let logical = match &row.0 {
            None => None,
            Some(stored) => decode_stored_value(&mut blob, stored).ok().flatten(),
        };
        Some((version, logical))
    }
    /// 读取选中的物理版本并还原用户值。
    /// 返回的墓碑、缺失和过期状态交给共享可见性判据处理。

    /// 取一组的值：叶子条目里带出来的就是存储字节，只有超过 blob 阈值的那部分要再解一次
    /// （内联行的"无 tag"与解码失败都按墓碑算，与改动前同口径）。
    pub(super) fn read_row(&self, row: &Option<Vec<u8>>) -> crate::common::RowValue {
        use crate::common::RowValue;
        match row {
            None => RowValue::Tombstone,
            Some(stored) => {
                let mut blob = self.blob.lock().unwrap();
                match decode_stored_value(&mut blob, stored).ok().flatten() {
                    Some(bytes) => RowValue::Value(bytes),
                    None => RowValue::Tombstone,
                }
            }
        }
    }
    /// 按物理键范围读取一片候选，再按逻辑键和事务快照折叠版本。
    /// 批尾可能跨越同一逻辑键；游标必须推进，不能因未返回用户值而重复扫描。

    /// 折出**候选组**（一把逻辑 key 一组，页里的原始字节顺手带走）。
    ///
    /// 为什么值要在这里就带上：B+ 树的 value 就住在叶子条目里，遍历的时候已经在手上。
    /// 原先抄 bitcask 的两段式（① 只折键 ② 按胜出行回树取值）对 bitcask 对 ——
    /// 它的 KeyDir 只有元数据、值在日志另一处；对 B+ 树就是**每个存活 key 多付
    /// 3~4 次页访问**：实测一次全量 scan 3.06 页/行、36.2 µs/行。
    ///
    /// `after` 是续跑游标、`accept` 是键级过滤（不命中的键连字节都不带）、
    /// `budget` 是组数上限；返回的 `bool` = 已走到界尾（`false` = 被预算拦下，还能续）。
    /// ⚠️ 交出一组只能在组边界：全表按 enc 升序 ⇒ 同一 raw_key 的版本块连续，
    /// 最后一次覆盖才是最大可见版本；在半截组上收手会把旧版本当新的交出去。
    pub(crate) fn scan_candidates(
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
        let mut kv = self.kv.lock().unwrap();
        kv.scan_entries_break_from(lo, |enc, row| {
            if hi.as_deref().is_some_and(|h| enc >= h) {
                return true;
            }
            fold.push(enc, |v| self.is_visible(v), || row.clone())
        });
        fold.finish()
    }

    pub fn print_all(&self) -> BTreeMap<Vec<u8>, RowId> {
        let mut records = BTreeMap::new();
        for rec in self.export_latest_visible(true) {
            records.insert(rec.key, RowId(rec.value));
        }
        records
    }

    /// 导出本事务快照下每个逻辑 key 的最新可见版本（已解码 blob / 剥 TTL）。
    ///
    /// 同 key 多版本按 version 升序扫描，后者覆盖前者 → 最终为最新可见。
    /// 模糊搜索与分页见 [`crate::bplus_tree::bplus_tree_mvcc::Transaction::search_keys`]。
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
            crate::bplus_tree::kv_ops::logical_to_user,
            &mut on,
        )
    }

    pub(super) fn is_visible(&self, version: u64) -> bool {
        // 判据在 [`crate::common::visible_at`]（四引擎共用的一份）
        self.core.is_visible(version)
    }

    /// 从链尾往回找本 key 第一个被 `accept` 的版本，返回它的版本键与行。
    ///
    /// 游走本身在存储层 [`BPlusTree::find_back`] 里做（只有单向 `next_leaf`，跨叶要重新
    /// 下降）。这里负责 MVCC 的两个判据：`raw_key` 必须精确等于 `key` —— 以 `key` 为前缀的
    /// 更长 key 会落进同一个字节区间（版本号首字节 0x00 小于任何 ASCII 字节）；以及可见性。
    pub(super) fn find_version_back(
        kv: &mut BPlusTree,
        key: &[u8],
        accept: impl Fn(u64) -> bool,
    ) -> Option<(Vec<u8>, RowId)> {
        let low = encode_key(key, 0);
        let high = encode_key(key, u64::MAX);
        kv.find_back(&low, &high, |enc| {
            decode_key(enc)
                .map(|vk| vk.raw_key.as_slice() == key && accept(vk.version))
                .unwrap_or(false)
        })
    }

    pub(super) fn latest_version_of(kv: &mut BPlusTree, key: &[u8]) -> Option<u64> {
        Self::find_version_back(kv, key, |_| true)
            .and_then(|(enc, _)| decode_key(&enc).map(|vk| vk.version))
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

    fn materialize(&self, row: Option<Vec<u8>>) -> crate::common::RowValue {
        self.read_row(&row)
    }
}
