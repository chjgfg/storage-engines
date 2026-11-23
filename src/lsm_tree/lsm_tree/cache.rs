//! 本文件负责缓存预算、钉住与淘汰。

use super::*;

impl LSMTree {
    pub(super) fn dir_cache_ns(dir: &Path) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        dir.hash(&mut h);
        h.finish()
    }

    pub(super) fn attach_cache_to_levels(&mut self) {
        let cache = self.block_cache.clone();
        let use_mmap = self.opts.use_mmap;
        let ns = Self::dir_cache_ns(&self.dir);
        for level in &mut self.levels {
            for sst in level {
                sst.set_cache_ns(ns);
                sst.set_block_cache(cache.clone());
                if use_mmap {
                    let _ = sst.enable_mmap();
                }
            }
        }
    }

    /// SST 里需要缓存槽位的块总数（v1 文件记 0，见 [`SstFile::blocks`]）
    ///
    /// 公开它是为了让调用方能算**覆盖率**：`tuning()` 里的 `block_cache_capacity` 是分子，
    /// 这个数是分母（M32）。只看生效容量的人分不清"顶已经撞上了"和"库本来就小"。
    pub fn sst_block_count(&self) -> usize {
        self.levels.iter().flatten().map(|s| s.blocks()).sum()
    }

    /// 自动档的**生效**容量（不是配置值）。关掉自动档就等于 `block_cache_blocks`。
    pub fn block_cache_capacity(&self) -> usize {
        self.block_cache.lock().unwrap().capacity()
    }

    /// 文件集变了之后重算块缓存容量；**不重建缓存**（见 [`BlockCache::set_capacity`]）。
    ///
    /// 为什么挂在文件集变化处：块数是"要装多少"的唯一凭据，而 flush / compact 是唯一改它的两处。
    /// 放在读路径上会让一次点读顺带改容量，那不可预测；放在写路径末尾则一次库的代价只有 O(文件数)。
    pub(super) fn reevaluate_cache_capacity(&mut self) {
        let floor = self.opts.block_cache_blocks;
        let ceil = self.opts.block_cache_auto_max;
        let base = if ceil == 0 || floor == 0 {
            // 关自动档：配置值就是生效值。地板 0 = 显式关缓存，上限不参与
            // （否则"关"会被自动档悄悄打开）
            floor
        } else if self.cache_shared {
            // 共享缓存（CF 模式）：每棵树只看得见自己的块数，谁后算谁赢 ⇒ 自动档不动，
            // 容量由装配方（`cf.rs`）定。
            return;
        } else {
            self.sst_block_count().clamp(floor, ceil)
        };
        // 预算档（M76）：**替换自动档那道 `clamp`，不是叠在它下面** ——
        // `生效容量 = min(想装的块数, 预算 ÷ 每槽真实常驻)`。拿 `clamp(块数, 地板, 顶)` 去夹预算，
        // "给我 64 MB"会被默认的 1024 块顶悄悄变成 7 MB。
        // 调用方显式关了缓存（地板 0）时预算**不把缓存打开**；显式给过容量（顶 0）时预算只当天花板。
        let target = if floor == 0 {
            0
        } else if self.cache_budget_bytes > 0 {
            let budget_blocks = (self.cache_budget_bytes / BLOCK_SLOT_RESIDENT_BYTES) as usize;
            let want = if ceil == 0 {
                floor
            } else {
                self.sst_block_count()
            };
            want.min(budget_blocks).max(1)
        } else {
            base
        };
        self.block_cache.lock().unwrap().set_capacity(target);
    }

    /// 按**内存预算**给块缓存档（字节）；返回换算后的生效块数。`0` = 撤掉预算档（回到改前那条）。
    ///
    /// 换算价签见 `BLOCK_SLOT_RESIDENT_BYTES`（实测每槽真实常驻，不是块的名义大小）。
    /// 设了预算之后**自动档那道 `clamp` 不再参与**（预算替换它，见 `Self::reevaluate_cache_capacity`），
    /// 而 flush / compact 之后的重估继续按预算封顶。
    pub fn set_cache_budget_bytes(&mut self, bytes: u64) -> usize {
        self.cache_budget_bytes = bytes;
        self.reevaluate_cache_capacity();
        self.block_cache.lock().unwrap().capacity()
    }

    /// 调用方给的那一档字节（生效块数看 [`Self::block_cache_capacity`]）
    pub fn cache_budget_bytes(&self) -> u64 {
        self.cache_budget_bytes
    }

    /// 把块缓存自动档的上限改成 `blocks`，并**就地**重估容量（`usize::MAX` = 不设顶）。
    ///
    /// 为什么要有这条：默认的 1024 块 ≈ 7.1 MB 是**预算**，而库里块数远大于它时覆盖率就掉下来
    /// —— 实测 40000 键（2681 块）装不下、600k 键更明显，每次点读多付一次未命中（≈54~94 µs 的
    /// 随机读价）。抬顶等于"把库往 RAM 里复制"，这个决定归调用方，不归库默认。
    /// `0` 仍是"关自动档"（生效值 = 配置值），语义与 [`LSMTreeOptions::block_cache_auto_max`] 一致。
    pub fn set_block_cache_auto_max(&mut self, blocks: usize) {
        self.opts.block_cache_auto_max = blocks;
        self.reevaluate_cache_capacity();
    }

    /// 安装共享 block cache（多 CF 复用）
    pub fn install_shared_block_cache(&mut self, cache: Arc<Mutex<BlockCache>>) {
        self.block_cache = cache.clone();
        self.cache_shared = true;
        self.attach_cache_to_levels();
    }
}
