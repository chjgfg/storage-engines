//! 本文件负责可见版本候选与搜索。

use super::*;

impl Transaction {
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.latest_visible_raw(key)
            .and_then(|(_, v)| v)
            .and_then(crate::bitcask::kv_ops::logical_to_user)
    }
    /// 从物理版本中选出当前事务可见的逻辑胜者。
    /// 墓碑和过期值仍可能是屏障，不能跳过它们再返回更旧的值。

    /// 最新可见逻辑行（含 TTL 信封原始字节；tombstone 为 None）。
    /// 供 [`crate::kv_ops`] 使用，不剥用户 value。
    pub(crate) fn latest_visible_raw(&self, key: &[u8]) -> Option<(u64, Option<Vec<u8>>)> {
        let mut kvengine = self.kv.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();
        // 从链尾往回走，第一个可见版本就是答案 —— 只对那一行读盘解码
        let enc = Self::find_version_back(&mut kvengine, key, |v| self.is_visible(v))?;
        let version = decode_key(&enc)?.version;
        // 外层 None：索引里没有这行；内层 None：软墓碑（逻辑值为 None）
        let stored = kvengine.get_entry(&enc).ok()??;
        let logical = match stored {
            None => None,
            Some(s) => decode_stored_value(&mut blob, &s).ok().flatten(),
        };
        Some((version, logical))
    }
    /// 读取选中的物理版本并还原用户值。
    /// 返回的墓碑、缺失和过期状态交给共享可见性判据处理。

    /// 取一组的值：KeyDir 里只有元数据 ⇒ 凭据是编码键，值要按位置回日志读
    /// （大 value 还要过 blob 旁路）。这正是"布局相关的那一半"，各家不同。
    pub(super) fn read_row(&self, enc: &[u8]) -> crate::common::RowValue {
        use crate::common::RowValue;
        let mut kvengine = self.kv.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();
        match kvengine.get_entry(enc) {
            // 索引里查无此键 / IO 出错：与"墓碑"不是一回事，整行不参与结果
            Ok(None) | Err(_) => RowValue::Missing,
            Ok(Some(None)) => RowValue::Tombstone,
            Ok(Some(Some(stored))) => {
                match decode_stored_value(&mut blob, &stored).ok().flatten() {
                    Some(bytes) => RowValue::Value(bytes),
                    None => RowValue::Tombstone,
                }
            }
        }
    }
    /// 按物理键范围读取一片候选，再按逻辑键和事务快照折叠版本。
    /// 批尾可能跨越同一逻辑键；游标必须推进，不能因未返回用户值而重复扫描。

    /// 折出**候选组**（一把逻辑 key 一组，值还没读）。
    ///
    /// 只折键、不碰日志：KeyDir 里就是元数据，`iter_meta_from` 按 enc 升序 ⇒ 同一
    /// 公共 CandidateFold 等待完整逻辑组，组内最后一次覆盖就是最大可见版本。值留到 `keep` 里读，
    /// 于是"命中几条读几条"，而不是"全库折完再筛"。
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
    ) -> (Vec<crate::common::WinCandidate<Vec<u8>>>, bool) {
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
        let kv = self.kv.lock().unwrap();
        for (enc, _) in kv.iter_meta_from(lo) {
            #[cfg(test)]
            crate::bitcask::bitcask::note_keydir_visit();
            if hi.as_deref().is_some_and(|h| enc.as_slice() >= h) {
                break;
            }
            if fold.push(enc, |v| self.is_visible(v), || enc.clone()) {
                break;
            }
        }
        fold.finish()
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
            crate::bitcask::kv_ops::logical_to_user,
            &mut on,
        )
    }

    pub(super) fn is_visible(&self, version: u64) -> bool {
        // 判据在 [`crate::common::visible_at`]（四引擎共用的一份）
        self.core.is_visible(version)
    }

    /// 从链尾往回找本 key 第一个被 `accept` 的版本，返回它的版本键（原始 enc 字节）。
    ///
    /// 版本键是 `raw_key ‖ version.to_be_bytes()`，大端 ⇒ 同一 key 的所有版本在字节序里
    /// 相邻且按版本号升序，所以**链尾就是最新版本**：往回走第一个满足条件的即可停。
    ///
    /// `raw_key` 仍是 `key` 前缀的其它逻辑 key 会落进同一个字节区间，因此必须逐条核对
    /// `raw_key == key`（旧实现靠 `filter` 做这件事，这里保留同样的判定）。
    pub(super) fn find_version_back(
        kv: &mut Bitcask,
        key: &[u8],
        accept: impl Fn(u64) -> bool,
    ) -> Option<Vec<u8>> {
        let low = encode_key(key, 0);
        let high = encode_key(key, u64::MAX);
        kv.find_meta_back(&low, &high, |enc| {
            decode_key(enc).is_some_and(|vk| vk.raw_key.as_slice() == key && accept(vk.version))
        })
    }

    pub(super) fn latest_version_of(kv: &mut Bitcask, key: &[u8]) -> Option<u64> {
        Self::find_version_back(kv, key, |_| true)
            .and_then(|enc| decode_key(&enc).map(|vk| vk.version))
    }
}

impl crate::common::VersionedStore for Transaction {
    /// KeyDir 只有元数据 ⇒ 凭据是编码键，值在日志里另一处。
    type Row = Vec<u8>;

    fn scan_candidates(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        accept: Option<&dyn Fn(&[u8]) -> bool>,
        after: Option<&[u8]>,
        budget: usize,
    ) -> (Vec<crate::common::WinCandidate<Vec<u8>>>, bool) {
        // 走的是本类型上的同名 inherent 方法（inherent 优先于 trait ⇒ 不会自我递归）
        self.scan_candidates(start, end, accept, after, budget)
    }

    fn materialize(&self, row: Vec<u8>) -> crate::common::RowValue {
        self.read_row(&row)
    }
}
