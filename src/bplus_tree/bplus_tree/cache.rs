//! 本文件负责缓存预算、钉住与淘汰。

use super::*;

/// 自动档的容量：按库当前的**实际页数**给，夹在 [`AUTO_MIN_PAGES`] 与 [`AUTO_MAX_PAGES`] 之间。
///
/// 为什么按页数而不是按行数：代价的载体是页（一次点读 = 沿路每层一页），覆盖率才是决定
/// "这次点读要不要读盘 + 重解析"的量 —— 实测同一个默认 1024 页的缓存，
/// 20k 行（1210 页）覆盖率 84.6% ⇒ 11.9 µs/次，200k 行（12129 页）覆盖率 8.4% ⇒ 45.2 µs/次。
pub(super) fn auto_cache_pages(total_pages: u64, max_pages: usize) -> usize {
    (total_pages as usize).clamp(AUTO_MIN_PAGES, max_pages.max(AUTO_MIN_PAGES))
}

impl BPlusTree {
    /// 重算页缓存的**生效容量**：`min(自动档/显式档, 预算换算出的槽数)`，就地改上限、不重建缓存。
    ///
    /// 三处都走这一个函数（`flush_all`、抬自动档顶、设/撤预算），因为"预算"与"跟着库长的自动档"
    /// 是两个正交的约束：只在其中一处应用，另一处一跑就会把预算冲掉（M22/M32 那条"设了没生效"）。
    pub(super) fn reevaluate_cache_capacity(&mut self) {
        let base = if self.auto_cache {
            auto_cache_pages(self.page_manager.next_page_id(), self.auto_max_pages)
        } else {
            self.explicit_pages
        };
        // 预算档（M76）：**它替换自动档那道 `clamp`，不是叠在它下面**。
        // 语义 = "能装多少装多少，但一口字节都不许多"：`生效容量 = min(想装的槽数, 预算 ÷ 每槽真实常驻)`。
        // 为什么不是 `min(自动档, 预算槽数)`：自动档那道夹是"没人给约束时"的默认政策（地板 1024、
        // 顶 2048），调用方既然报了预算，那他就是那个约束 —— 拿 2048 页去夹会把"给我 64 MB"
        // 悄悄变成"还是 8 MB"，而按价签那份预算明明装得下 9142 页。
        // 反过来，调用方**显式**给过容量（非自动档）时预算只当天花板：两个都是他定的，不许抬。
        let cap = if self.cache_budget_bytes > 0 {
            let budget_pages = (self.cache_budget_bytes / CACHE_PAGE_RESIDENT_BYTES) as usize;
            let want = if self.auto_cache {
                self.page_manager.next_page_id() as usize
            } else {
                self.explicit_pages
            };
            want.min(budget_pages).max(1)
        } else {
            base
        };
        self.page_manager.set_cache_capacity(cap);
    }

    /// 按**内存预算**给缓存档（字节）。返回换算后的生效槽数，便于调用方当场核对。
    ///
    /// `0` = 撤掉预算档，回到自动档/显式档那条（与改前一字不差）。
    /// 换算价签见 `CACHE_PAGE_RESIDENT_BYTES`：那是**实测的每槽真实常驻**，不是页的名义大小。
    pub fn set_cache_budget_bytes(&mut self, bytes: u64) -> usize {
        self.cache_budget_bytes = bytes;
        self.reevaluate_cache_capacity();
        self.page_manager.cache_capacity()
    }

    /// 调用方给的那一档字节（生效槽数看 [`BPlusTree::max_cache_pages`]）
    pub fn cache_budget_bytes(&self) -> u64 {
        self.cache_budget_bytes
    }

    /// 缓存策略读数：`(生效容量, 钉住的上层页数, 是否自动档)`
    ///
    /// 给 `tuning()` 与守卫用 —— 自动档必须**看得见生效值**，否则"设了没生效"和
    /// "生效了但不知道是多少"这两种情况在下游日志里长得一模一样。
    /// 把自动档的上限改成 `pages`（**就地改容量**，不重建缓存）。
    ///
    /// `usize::MAX` = 不设顶 ⇒ 自动档就等于"跟着库长"（代价：常驻 ≈ 数据文件本身）。
    /// 只在自动档下有意义：调用方显式给了 `max_cache_pages != 0` 时，本来就没有顶可言。
    /// ⚠️ 与 M23 同一条纪律：**改容量不许重建缓存**，否则"把旋钮调大"这个动作会先把已经
    /// 焐热的页全丢掉，自动档就变成"每次重估都自己清一次缓存"。
    pub fn set_cache_auto_max(&mut self, pages: usize) {
        self.auto_max_pages = pages.max(AUTO_MIN_PAGES);
        self.reevaluate_cache_capacity();
    }

    /// 当前自动档上限（非自动档时它也返回，但不起作用 —— 与 `cache_policy()` 的口径一致）
    pub fn cache_auto_max(&self) -> usize {
        self.auto_max_pages
    }

    /// 钉住预算（M49）：把"上层页可以占多少容量"这一档的决定权交给调用方。
    ///
    /// 病灶（M32 记下、当时没碰的第二点）：抬顶之后叶子**拿不到整个容量** —— 实测 2048 页的顶下
    /// 住着 1986 页，其中**钉住 1024 页全是上层**，留给叶子 962 页，而那份库有 1300 片叶子
    /// ⇒ 一轮随机点读缺 1621 页。上层值多少由键长决定（35 B 键时占 3%，3.3 KB 键时占 50%），
    /// 库替调用方做不了这个决定，所以这一档改成可配；默认仍是 1024 页
    /// （`page::DEFAULT_PIN_BUDGET_PAGES`），**不调就和改前一字不差**。
    ///
    /// ⚠️ 生效值还要与**容量的一半**取小（`min(预算, 容量/2)`）：那条底线不许被这个旋钮突破，
    /// 否则"抬顶"+"调大钉住预算"合起来能把整份缓存钉死、叶子一页装不下 —— 比不钉还糟。
    pub fn set_pin_budget_pages(&mut self, pages: usize) {
        self.page_manager.set_pin_budget(pages);
    }

    /// 调用方给的那一档（生效值看 [`Self::pin_limit_pages`]）
    pub fn pin_budget_pages(&self) -> usize {
        self.page_manager.pin_budget()
    }

    /// 生效的钉住上限 = `min(预算, 生效容量 / 2)`
    pub fn pin_limit_pages(&self) -> usize {
        self.page_manager.effective_pin_limit()
    }

    pub fn cache_policy(&self) -> (usize, usize, bool) {
        (
            self.page_manager.cache_capacity(),
            self.page_manager.pinned_pages(),
            self.auto_cache,
        )
    }

    /// 测试专用：页缓存里**当前真的住着几页**（M32 用它证明"抬容量不重建缓存"）
    #[cfg(test)]
    pub(crate) fn cached_pages_for_test(&self) -> usize {
        self.page_manager.stats().cache_pages
    }
}

impl BPlusTree {
    /// 只读路径取节点：**优先用解析过的**，未命中才读页 + bincode 解析。
    ///
    /// 为什么必须有它：归因量过，B+ 树点读的 41 µs 里几乎全是 CPU —— 每次点读要走 4 个页，
    /// 每个页都要 4 KB 拷贝 + 解析整节点（34~64 条 ⇒ 上百次堆分配）；缓存开大也没用。
    /// 校验凭 `PageManager::write_count` 按页做，所以写点不需要手工失效。
    ///
    /// M34 之后**写路径的下降也走它**（`insert_recursive`/`update_recursive`/`read_node`）：
    /// 以前写一侧是裸 `read_page` + 解析 ⇒ 每行把整条路径重解析一遍（实测 2.94 页读/行、
    /// 3 次整节点解析/行，而这把尺子当时根本数不到它们 —— 解析计数只挂在 `node_of` 上）。
    pub(super) fn node_of(&mut self, page_id: PageId) -> Option<std::sync::Arc<BTreeNode>> {
        let seen = self.page_manager.write_count(page_id);
        if let Some((at, node)) = self.node_cache.get(&page_id) {
            if *at == seen {
                match node {
                    CachedNode::Owned(node) => return Some(node.clone()),
                    CachedNode::Internal(packed) => {
                        let node = std::sync::Arc::new(
                            bincode::deserialize::<BTreeNode>(&packed.payload).ok()?,
                        );
                        #[cfg(test)]
                        {
                            NODE_PARSES.with(|c| c.set(c.get() + 1));
                            POINT_INTERNAL_PROMOTIONS.with(|c| c.set(c.get() + 1));
                        }
                        return self.memoize_node(page_id, seen, node);
                    }
                }
            }
        }
        let data = self.page_manager.read_page(page_id)?;
        #[cfg(test)]
        NODE_PARSES.with(|c| c.set(c.get() + 1));
        let node = std::sync::Arc::new(bincode::deserialize::<BTreeNode>(&data).ok()?);
        self.memoize_node(page_id, seen, node)
    }

    /// 把一份节点内容放进解析缓存，顺带执行两条纪律：上层页钉住、超上限丢没钉住的。
    ///
    /// `seen` 必须是**这次放进去的内容所对应的那次写**的 `write_count` —— `node_of` 拿它判陈旧。
    /// 两条来路都满足：`node_of` 读页之后、`insert` 写完页之后（见 [`Self::cache_written`]）。
    pub(super) fn memoize_node(
        &mut self,
        page_id: PageId,
        seen: u64,
        node: std::sync::Arc<BTreeNode>,
    ) -> Option<std::sync::Arc<BTreeNode>> {
        self.memoize_cached(page_id, seen, CachedNode::Owned(node.clone()));
        Some(node)
    }

    pub(super) fn memoize_cached(&mut self, page_id: PageId, seen: u64, node: CachedNode) {
        // **上层页钉住**（M22）：根与内部节点是结构性小的一小撮（实测占全库 3.0%），而每一次
        // 点读、每一次反向游走都要用它们。不钉住时，全库走查把 LRU 转几圈就把它们挤出去 ——
        // 实测 200k 行的库里"走查后一次随机点读"因此从 45.2 µs 涨到 58.6 µs（+30%）。
        // 只钉 `Internal`：叶子占 97% 的页，钉叶子等于把整个库再复制一份进内存。
        if match &node {
            CachedNode::Owned(node) => matches!(&**node, BTreeNode::Internal { .. }),
            CachedNode::Internal(_) => true,
        } {
            self.page_manager.pin(page_id);
        }
        if self.node_cache.len() >= self.page_manager.cache_capacity() {
            #[cfg(test)]
            {
                NODE_CACHE_SWEEPS.with(|c| c.set(c.get() + 1));
                NODE_CACHE_VISITED.with(|c| c.set(c.get() + self.node_cache.len() as u64));
            }
            // 上限跟页缓存同源；超了丢**没钉住的**，留下钉住的。
            // 以前这里是整表 `clear()`：那等于每装满 cap 个节点就把上层页的解析结果白丢一次，
            // 而下一趟下降立刻要用它们 —— 走查之后点读变贵的那 30% 就是这笔账。
            let pm = &self.page_manager;
            self.node_cache.retain(|id, _| pm.is_pinned(*id));
        }
        self.node_cache.insert(page_id, (seen, node));
    }

    /// 刚落盘的页**直接回填**解析缓存。
    ///
    /// 为什么这是免费的：写入的内容此刻就在调用方手上（`insert` 那条 `Vec<(PageId, BTreeNode)>`），
    /// 搬进 `Arc` 只要一次 `Arc` 分配 —— 不回填的话下一条行的下降要把这 4 KB 重新拷出来、
    /// 再把整节点解析一遍（上百次小堆分配）。实测顺序灌行每行 3 次整节点解析，其中 2 次
    /// （根 + 中间层）本来就几乎没变，另 1 次（叶子）是上一条自己写脏的。
    ///
    /// `write_page` 只在**写成功**之后才该回填：写失败时页内容还是旧的，缓存里放新内容
    /// 就是让下一次下降读到盘上并没有的东西。
    pub(super) fn cache_written(&mut self, page_id: PageId, node: BTreeNode) {
        let seen = self.page_manager.write_count(page_id);
        self.memoize_node(page_id, seen, std::sync::Arc::new(node));
    }
}
