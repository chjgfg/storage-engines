//! 本文件负责统计与计量。

use super::*;

impl LSMTree {
    /// 跨源去重之后的**键数**（三种赢家行都算：有值、空值、`Drop` 墓碑）。
    ///
    /// M47：只数不物化 —— 改前是 `merge_all_at(u64::MAX).len()`，为一个数把整库连 value 折出来
    /// （与 M35 那刀里 `key_count` 同一个形状）。判据复用 `Self::count_kinds_at`。
    pub fn entry_count(&self) -> usize {
        let (put, empty, drop) = self.count_kinds_at(u64::MAX);
        put + empty + drop
    }

    /// 三类赢家行的条数：`(Put(Some), Put(None), Drop)`。
    ///
    /// 为什么一条就够：`entry_count`、`len` 的 dirty 路径、`refresh_stats_cache` 问的其实都是
    /// "每一类各多少条"，只是各自取不同的一撮 ⇒ 三份"为一个数把整库折出来"合成一份
    /// （M5a/M5b 一路的做法：同形状的判定不许留三份）。
    /// 判据（谁算一个键、跨源怎么选赢家）走 M39 那条惰性轻量归并，与重量归并一字不差；
    /// 轻量视图只带 `seq` 与 `kind` 字节 ⇒ **一行 value 都不克隆**
    /// （守卫拿 `sst::value_materializations()` 钉这一格必须是 0）。
    pub(super) fn count_kinds_at(&self, snap_seq: u64) -> (usize, usize, usize) {
        let (mut put, mut empty, mut drop) = (0usize, 0usize, 0usize);
        for (_k, le) in self.light_iter_range(None, None, snap_seq) {
            match le.kind {
                0 => put += 1,
                1 => empty += 1,
                2 => drop += 1,
                // 种类字节只有这三种；走到这里说明两边口径分了家。
                // ⚠️ 宁可少算，也不要把一个不认识的种类当成存活键（那是假数，不是崩溃）。
                _ => {}
            }
        }
        (put, empty, drop)
    }

    /// 存活 key 数。优先返回缓存；`stats_dirty` 时会走查全库（O(全库条目)，M47 起一行 value 都不克隆）。
    /// 刚 `open` 后若未写过，缓存为 0——需要精确值请用 [`Self::stats_exact`]。
    pub fn len(&self) -> usize {
        if !self.stats_dirty {
            return self.cached_live;
        }
        // M47：只数不物化（改前是 `merge_all_at(...).iter().filter(Put(Some)).count()`）
        self.count_kinds_at(u64::MAX).0
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 廉价 stats：**不**全量 merge。
    ///
    /// - `level_files` / `level_bytes` / `l0_files`：始终准确
    /// - `live_keys` / `tombstones`：写路径增量缓存，**可能为 0 或近似**
    ///   （`open` 不扫盘；bulk/`insert_fast` 不读旧值）
    ///
    /// 需要精确 live 时用 [`Self::stats_exact`]。
    pub fn stats(&self) -> LsmStats {
        let level_files: Vec<usize> = self.levels.iter().map(|l| l.len()).collect();
        let level_bytes: Vec<u64> = self
            .levels
            .iter()
            .map(|l| l.iter().map(|s| s.size_bytes).sum())
            .collect();
        let total_sst: usize = level_files.iter().sum();
        LsmStats {
            live_keys: self.cached_live,
            tombstones: self.cached_tomb,
            // 与 flush 判定同一个来源（增量计数器），所以 `stats()` 也是 O(层数) 而不是 O(memtable)
            mem_entries: self.mem_entries,
            mem_bytes: self.mem_bytes,
            l0_files: self.levels.first().map(|l| l.len()).unwrap_or(0),
            l1_files: self.levels.get(1).map(|l| l.len()).unwrap_or(0),
            total_sst,
            level_files,
            level_bytes,
            oldest_snapshot: self.oldest_snapshot_seq(),
            dir: self.dir.clone(),
        }
    }

    /// 走查全库后刷新 `cached_live`/`cached_tomb`，返回精确 stats。
    /// 大库 O(全库条目)，仅对账 / 测试使用。⚠️ M47 起这一趟**不折 value**（只数种类）。
    pub fn stats_exact(&mut self) -> LsmStats {
        self.refresh_stats_cache();
        self.stats()
    }

    /// 仅在 `stats_dirty` 时全量 merge 刷新 live/tomb
    pub fn refresh_stats_if_needed(&mut self) {
        if self.stats_dirty {
            self.refresh_stats_cache();
        }
    }

    pub(super) fn refresh_stats_cache(&mut self) {
        // M47：只数不物化。改前这里是 `merge_all_at(u64::MAX)` 把整库连 value 折出来，
        // 再逐条 match —— 与 `entry_count`/`len` 是同一个形状的第三份复制。
        // 口径一字未动：`Put(Some)` → live、`Put(None)` → tomb、`Drop` 两处都不计。
        let (put, empty, _drop) = self.count_kinds_at(u64::MAX);
        self.cached_live = put;
        self.cached_tomb = empty;
        self.stats_dirty = false;
    }
}
