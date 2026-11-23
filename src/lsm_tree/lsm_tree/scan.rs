//! 本文件负责顺序、反向与维护扫描。

use super::*;

/// 一路可被"当场收手"的源（M46）：收到停止信号之后一律吐 `None`。
///
/// 为什么要它：`merge_streaming` 是压实/修复共用的公开件，把它的 `emit` 改成返回 `bool`
/// 是给下游的破坏性变更。而"收手"真正需要的是**各源不再往下拉** —— 源一空，堆就空，
/// 归并按它自己的逻辑结束（剩下 O(源数) 次弹出，不再读盘、不再克隆 value）。
pub(super) struct Stoppeable<I> {
    pub(super) it: I,
    pub(super) stopped: std::rc::Rc<std::cell::Cell<bool>>,
}

impl<I: Iterator> Iterator for Stoppeable<I> {
    type Item = I::Item;
    fn next(&mut self) -> Option<I::Item> {
        if self.stopped.get() {
            return None;
        }
        self.it.next()
    }
}

impl LSMTree {
    /// 归并的**源**（惰性，M46）：mem → immutables → L0 新→旧 → 更高层。
    ///
    /// 归并本身从 M33 起就吃 `Box<dyn Iterator>`（[`merge_streaming`]），坏的一直是这里：
    /// 改前每个 memtable 走 `mem_view_at`（整表折进 `Vec`，M46 起删除）、每个 SST 走
    /// `iter_all().filter(...).collect()`（整篇折进 `Vec`）⇒ **所有源同时活着**，
    /// 于是"全库折叠"的峰值 = 一份全库重量视图 + 归并输出。这正是 M39 在 MVCC 那道门
    /// （`scan_latest_light`）上治过的同一个形状，只是裸引擎这道门当时没走过。
    ///
    /// 源顺序与"每源按键升序"这两条是归并取胜的前提，一字没改；`vec_inputs` 那条兼容入口
    /// 也留着（压实的输入本来就在内存里，换个类型而已）。
    pub(super) fn merge_inputs_at(&self, snap_seq: u64) -> Vec<MergeInput<'_, StoredEntry>> {
        let mut sources: Vec<MergeInput<'_, StoredEntry>> = Vec::new();
        if !self.mem.is_empty() {
            sources.push(Box::new(Self::mem_iter_at(&self.mem, snap_seq)));
        }
        for imm in &self.immutables {
            sources.push(Box::new(Self::mem_iter_at(imm, snap_seq)));
        }
        if let Some(l0) = self.levels.first() {
            for sst in l0.iter().rev() {
                sources.push(Box::new(
                    sst.iter_all()
                        .filter(move |(_, se)| se.visible_at(snap_seq)),
                ));
            }
        }
        for level in self.levels.iter().skip(1) {
            for sst in level {
                sources.push(Box::new(
                    sst.iter_all()
                        .filter(move |(_, se)| se.visible_at(snap_seq)),
                ));
            }
        }
        sources
    }

    /// 全库跨源归并的**流式**版（M46）：折一行交一行，`on` 返回 `false` 就收手 ——
    /// 收手之后各源不再往下拉。返回实际交出去的行数。
    ///
    /// 为什么"收手"要套一层 [`Stoppeable`] 而不是改 [`merge_streaming`] 的签名：那条是
    /// 压实/修复共用的公开件，把它的 `emit` 改成返回 `bool` 是给下游的破坏性变更；
    /// 而"收手"要的只是让各源尽快吐 `None`，堆一空归并自然结束（剩下的收尾是 O(源数) 次弹出）。
    pub(crate) fn merge_latest_at(
        &self,
        snap_seq: u64,
        on: &mut dyn FnMut(Vec<u8>, StoredEntry) -> bool,
    ) -> usize {
        let stopped = std::rc::Rc::new(std::cell::Cell::new(false));
        let mut handed = 0usize;
        let sources: Vec<MergeInput<'_, StoredEntry>> = self
            .merge_inputs_at(snap_seq)
            .into_iter()
            .map(|it| {
                Box::new(Stoppeable {
                    it,
                    stopped: stopped.clone(),
                }) as MergeInput<'_, StoredEntry>
            })
            .collect();
        merge_streaming(
            sources,
            |_, _| true,
            |k, se| {
                if stopped.get() {
                    return;
                }
                if on(k, se) {
                    handed += 1;
                } else {
                    stopped.set(true);
                }
            },
        );
        handed
    }

    /// 全库「每个 enc key 的胜者」的**轻量**视图（跨源取胜规则与 [`Self::iter`] 一致：
    /// 源新→旧优先，同 src 取更大 seq）。
    ///
    /// 与 `iter()` 的差别就在**不克隆 value**：需要 value 的调用方对少数胜出行再点查
    /// `Self::get_value_at`（会解析 blob 指针），语义与归并取胜一致。
    ///
    /// M39 起这份实现就是 [`Self::light_iter_range`] 的**收集壳**：以前它自己把每个 memtable
    /// `mem_view_light` 整表遍历、每个 SST `light_view` 整篇折进 `Vec`，再把所有源同时握着做归并
    /// ⇒ 峰值 = 「2 × 全库轻量视图」。600k 档那笔 **瞬时 +126.1 MB** 就是这么来的
    /// （`health()` 当时是它唯一的生产调用方，现已改走惰性迭代器）。
    /// 签名保留给"确实要一次拿到整份快照"的调用方：它仍然返回 `Vec`，但中途不再另握一份源副本。
    pub fn scan_latest_light(&self, snap_seq: u64) -> Vec<(Vec<u8>, LightEntry)> {
        self.light_iter_range(None, None, snap_seq).collect()
    }

    /// `[from, to)` 的跨源轻量视图。取胜规则与 [`Self::scan_latest_light`] 一字不差，
    /// 区别只有一个：**界下推到每一个源**。
    ///
    /// 为什么必须有它：`scan_latest_light` 把每个 memtable 整表遍历、每个 SST 整篇块读完
    /// 再交给归并，于是"10 行的窗口"付的是"全库"的块读代价（大库基线实测：LSM 窗口 scan
    /// 405 µs/行 vs 一次全量 scan 4.0 µs/行）。memtable 走 `range`，SST 从 `from` 所在块
    /// 起读、键越过 `to` 立即收手，先用 min/max 挡掉整篇不相干的文件。
    /// 跨源归并的**惰性**版本（M15b）：一次只往前拉一个键，所以"只要前 N 把键"的调用
    /// 不会把每个源整篇读完。
    ///
    /// 老的 `scan_latest_light_range` 先把每个源 `Vec` 化再归并 ⇒ 首页也要读满全库的块
    /// （实测 2000 行的库首页 53 次块读，其中 ~32 次就是全库块数）。改成惰性之后
    /// 首页只读"覆盖到那几把键"需要的块，`scan_page` / 窗口搜索 / 有界 vacuum 一起受益。
    ///
    /// ⚠️ M15b 那次只把**每个源内部**改成惰性，源本身还是建堆时一个个拉首键 ⇒ 无上界的
    /// 首页仍付"库里 SST 篇数"次块读（5M 实测 315 篇 = 15.66 ms）。M81 第 1 项补上
    /// **源级延迟激活**（见 [`LightMerge`] 的六条判据），首页代价从此与篇数脱钩。
    ///
    /// 取胜规则与 `merge_streaming` 逐条一致：源序号小 = 更新（mem → immutables → L0 新→旧 →
    /// 更高层），同一个键只输出一行、由更新的源定论；同键同行的旧 seq 不复活。
    pub fn light_iter_range<'a>(
        &'a self,
        from: Option<&'a [u8]>,
        to: Option<&'a [u8]>,
        snap_seq: u64,
    ) -> LightMerge<'a> {
        self.projected_iter_range(
            from,
            to,
            snap_seq,
            |seq, value| LightEntry {
                seq,
                kind: value.kind_byte(),
            },
            |sst, from, to, snap| Box::new(sst.light_iter(from, to, snap)),
        )
    }

    pub(crate) fn row_iter_range<'a>(
        &'a self,
        from: Option<&'a [u8]>,
        to: Option<&'a [u8]>,
        snap_seq: u64,
    ) -> LightMerge<'a, SourceRow<'a>> {
        self.projected_iter_range(
            from,
            to,
            snap_seq,
            SourceRow::borrowed,
            |sst, from, to, snap| sst.row_iter(from, to, snap),
        )
    }

    pub(super) fn projected_iter_range<'a, R: MergeRow + 'a>(
        &'a self,
        from: Option<&'a [u8]>,
        to: Option<&'a [u8]>,
        snap_seq: u64,
        mem: fn(u64, &'a EntryValue) -> R,
        sst_rows: impl Fn(
            &'a SstFile,
            Option<&'a [u8]>,
            Option<&'a [u8]>,
            u64,
        ) -> Box<dyn Iterator<Item = (Vec<u8>, R)> + 'a>,
    ) -> LightMerge<'a, R> {
        let mut sources: Vec<Box<dyn Iterator<Item = (Vec<u8>, R)> + '_>> = Vec::new();
        // 与 sources 一一对应的驻留键下界（`None` = 建堆当场激活）。见 `LightMerge` 的六条判据。
        let mut bound: Vec<Option<&'a [u8]>> = Vec::new();
        if matches!((from, to), (Some(lo), Some(hi)) if lo >= hi) {
            return LightMerge::build(sources, bound);
        }
        // memtable 不排队：它没有"读块"这笔代价，拉首键只是 BTreeMap 上的一次 seek，
        // 而它的下界要靠同一趟 seek 才拿得到 —— 排队省不下任何东西，只会让归并多一层状态。
        if !self.mem.is_empty() {
            sources.push(Box::new(Self::mem_projected_iter(
                &self.mem, from, to, snap_seq, mem,
            )));
            bound.push(None);
        }
        for imm in &self.immutables {
            sources.push(Box::new(Self::mem_projected_iter(
                imm, from, to, snap_seq, mem,
            )));
            bound.push(None);
        }
        // 源顺序必须与 merge_all_at / scan_latest_light 一致：levels[0] 旧→新，反转成新→旧。
        // 先把"有哪些 SST、按什么优先级"排成一个 Vec，再统一建迭代器 ——
        // 用闭包 push 会把两个不同的生命周期搅在一起（编译器拒绝得有道理）。
        let mut ssts: Vec<&SstFile> = Vec::new();
        if let Some(l0) = self.levels.first() {
            ssts.extend(l0.iter().rev());
        }
        for level in 1..self.levels.len() {
            let files = match to {
                Some(hi) => self.sst_candidates(level, from.unwrap_or(&[]), hi),
                None => &self.levels[level],
            };
            ssts.extend(files.iter());
        }
        for sst in ssts {
            // 无上界不造哨兵；有上界是半开区间，min_key == to 的源初始化也不应读块。
            if !sst.may_overlap_half_open(from, to) {
                continue;
            }
            // 这个源**可能**交出的最小键 = max(文件 min_key, from)：两个都是从元数据/调用方
            // 手里直接读到的，不花一次块读。⚠️ 它是下界而不是真首键 —— 快照过滤会让真首键更大，
            // 所以激活之后还要拿真实键重新比一轮（判据⑤）。
            bound.push(Some(match from {
                Some(lo) if lo >= sst.min_key.as_slice() => lo,
                _ => sst.min_key.as_slice(),
            }));
            sources.push(sst_rows(sst, from, to, snap_seq));
        }
        LightMerge::build(sources, bound)
    }

    // 严格维护读取不能用不完整扫描授权回收。
    pub(crate) fn vacuum_iter_range<'a>(
        &'a self,
        from: Option<&'a [u8]>,
        to: Option<&'a [u8]>,
        snap_seq: u64,
    ) -> VacuumMerge<'a> {
        let mut sources: Vec<
            Box<dyn Iterator<Item = std::io::Result<(Vec<u8>, LightEntry)>> + '_>,
        > = Vec::new();
        // 与 sources 一一对应的驻留键下界（`None` = 建堆当场激活）。见 `LightMerge` 的六条判据。
        let mut bound: Vec<Option<&'a [u8]>> = Vec::new();
        if matches!((from, to), (Some(lo), Some(hi)) if lo >= hi) {
            return VacuumMerge::build(sources, bound);
        }
        // memtable 不排队：它没有"读块"这笔代价，拉首键只是 BTreeMap 上的一次 seek，
        // 而它的下界要靠同一趟 seek 才拿得到 —— 排队省不下任何东西，只会让归并多一层状态。
        if !self.mem.is_empty() {
            sources.push(Box::new(
                Self::mem_light_iter(&self.mem, from, to, snap_seq).map(Ok),
            ));
            bound.push(None);
        }
        for imm in &self.immutables {
            sources.push(Box::new(
                Self::mem_light_iter(imm, from, to, snap_seq).map(Ok),
            ));
            bound.push(None);
        }
        // 源顺序必须与 merge_all_at / scan_latest_light 一致：levels[0] 旧→新，反转成新→旧。
        // 先把"有哪些 SST、按什么优先级"排成一个 Vec，再统一建迭代器 ——
        // 用闭包 push 会把两个不同的生命周期搅在一起（编译器拒绝得有道理）。
        let mut ssts: Vec<&SstFile> = Vec::new();
        if let Some(l0) = self.levels.first() {
            ssts.extend(l0.iter().rev());
        }
        for level in 1..self.levels.len() {
            let files = match to {
                Some(hi) => self.sst_candidates(level, from.unwrap_or(&[]), hi),
                None => &self.levels[level],
            };
            ssts.extend(files.iter());
        }
        for sst in ssts {
            // 无上界不造哨兵；有上界是半开区间，min_key == to 的源初始化也不应读块。
            if !sst.may_overlap_half_open(from, to) {
                continue;
            }
            // 这个源**可能**交出的最小键 = max(文件 min_key, from)：两个都是从元数据/调用方
            // 手里直接读到的，不花一次块读。⚠️ 它是下界而不是真首键 —— 快照过滤会让真首键更大，
            // 所以激活之后还要拿真实键重新比一轮（判据⑤）。
            bound.push(Some(match from {
                Some(lo) if lo >= sst.min_key.as_slice() => lo,
                _ => sst.min_key.as_slice(),
            }));
            sources.push(Box::new(sst.vacuum_light_iter(from, to, snap_seq)));
        }
        VacuumMerge::build(sources, bound)
    }

    pub fn range_scan(&self, low: Vec<u8>, high: Vec<u8>) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        self.range_scan_at_seq(&low, &high, u64::MAX)
    }

    pub fn range_scan_at(
        &self,
        snap: &Snapshot,
        low: Vec<u8>,
        high: Vec<u8>,
    ) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        self.range_scan_at_seq(&low, &high, snap.seq)
    }

    pub(super) fn range_scan_at_seq(
        &self,
        low: &[u8],
        high: &[u8],
        snap_seq: u64,
    ) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        self.merge_range_at(low, high, snap_seq)
            .into_iter()
            .filter_map(|(k, se)| match se.value {
                EntryValue::Put(val) => Some((k, self.deref_value(val))),
                EntryValue::Drop => None,
            })
            .collect()
    }

    pub fn iter(&self) -> impl Iterator<Item = (Vec<u8>, Option<Vec<u8>>)> {
        self.merge_all_at(u64::MAX)
            .into_iter()
            .filter_map(move |(k, se)| match se.value {
                EntryValue::Put(val) => Some((k, self.deref_value(val))),
                EntryValue::Drop => None,
            })
    }

    pub fn iter_at(&self, snap: &Snapshot) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        self.merge_all_at(snap.seq)
            .into_iter()
            .filter_map(|(k, se)| match se.value {
                EntryValue::Put(val) => Some((k, self.deref_value(val))),
                EntryValue::Drop => None,
            })
            .collect()
    }

    // ─── merge views ─────────────────────────────────────────────────────────

    pub(super) fn merge_range_at(
        &self,
        low: &[u8],
        high: &[u8],
        snap_seq: u64,
    ) -> Vec<(Vec<u8>, StoredEntry)> {
        let mut sources: Vec<Vec<(Vec<u8>, StoredEntry)>> = Vec::new();
        let mem_part = Self::mem_range_at(&self.mem, low, high, snap_seq);
        if !mem_part.is_empty() {
            sources.push(mem_part);
        }
        for imm in &self.immutables {
            let part = Self::mem_range_at(imm, low, high, snap_seq);
            if !part.is_empty() {
                sources.push(part);
            }
        }
        if let Some(l0) = self.levels.first() {
            for sst in l0.iter().rev() {
                let part = sst.range_at(low, high, snap_seq);
                if !part.is_empty() {
                    sources.push(part);
                }
            }
        }
        for level in self.levels.iter().skip(1) {
            for sst in level {
                let part = sst.range_at(low, high, snap_seq);
                if !part.is_empty() {
                    sources.push(part);
                }
            }
        }
        merge_latest_streaming(vec_inputs(sources), |_, _| true)
    }

    pub(super) fn merge_all_at(&self, snap_seq: u64) -> Vec<(Vec<u8>, StoredEntry)> {
        // M46：这条改成 [`Self::merge_latest_at`] 的**收集壳** —— 归并取胜的判据只剩一份实现，
        // "整库快照"与"逐行流式"不再是两条会各自漂移的路。省下的是**源**那一头：
        // 原先每个 memtable 与每个 SST 都先整篇折进各自的 `Vec` 才开始归并（所有源同时活着）。
        // ⚠️ 它仍然返回全库的 `Vec`（那是调用方要的形状）；只要"逐行"的人请直接用流式那条。
        let mut out: Vec<(Vec<u8>, StoredEntry)> = Vec::new();
        self.merge_latest_at(snap_seq, &mut |k, se| {
            out.push((k, se));
            true
        });
        out
    }

    pub fn export_records(&self, include_deleted: bool) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        self.export_records_with_seq(include_deleted)
            .into_iter()
            .map(|(k, v, _seq)| (k, v))
            .collect()
    }

    /// 导出逻辑 KV + 写序号。
    ///
    /// M46 起这条是 [`Self::export_records_with_seq_for_each`] 的**收集壳**：取胜与"墓碑要不要参与"
    /// 的判据只剩一份实现。⚠️ 它返回整库的 `Vec`（那是调用方要的形状）—— 只要"把行交出去"的人
    /// （导出到文件）请直接用流式那条。
    pub fn export_records_with_seq(
        &self,
        include_deleted: bool,
    ) -> Vec<(Vec<u8>, Option<Vec<u8>>, u64)> {
        let mut out: Vec<(Vec<u8>, Option<Vec<u8>>, u64)> = Vec::new();
        self.export_records_with_seq_for_each(include_deleted, &mut |k, v, seq| {
            out.push((k, v, seq));
            true
        });
        out
    }

    /// **流式**导出（M46）：归并一行、判一行、立刻交给 `on`，返回交出去的行数；
    /// `on` 返回 `false` 当场收手，后面的键不再折、不再读盘。
    ///
    /// 为什么裸引擎也要给一条：M44 给门面加了流式出口之后，四家 CLI 里只剩
    /// `lsm_tree/bin/export_kv.rs` 还在物化整库 —— 因为它走的是这条**裸引擎**入口
    /// （`merge_all_at`），当时归并的**源**每个都先整篇折进 `Vec`（M46 修的就是这个）。
    pub fn export_records_with_seq_for_each(
        &self,
        include_deleted: bool,
        on: &mut dyn FnMut(Vec<u8>, Option<Vec<u8>>, u64) -> bool,
    ) -> usize {
        let mut handed = 0usize;
        self.merge_latest_at(u64::MAX, &mut |k, se| match se.value {
            EntryValue::Put(val) => {
                if val.is_none() && !include_deleted {
                    return true; // 墓碑不参与，但也不算"交出去一行"：继续走
                }
                let keep = on(k, val, se.seq);
                if keep {
                    handed += 1;
                }
                keep
            }
            // 硬删（Drop）不参与导出 —— 与改前那份 filter_map 一字不差
            EntryValue::Drop => true,
        });
        handed
    }
}
