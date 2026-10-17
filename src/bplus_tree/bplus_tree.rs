use std::{fmt, path::Path, write};

use serde::{Deserialize, Serialize};

use crate::bplus_tree::page::{MAX_PAYLOAD, PageId, PageManager};

// 测试专用：本线程「把一页字节 bincode 解析成节点」的次数。
//
// 这是 B+ 树点读真正的 CPU 单位：归因量过 —— 冷 41.8 µs / 热 41.3 µs、命中率 95%、
// 把页缓存从 1024 开到 65536 仍然 37.8 µs ⇒ 钱不是花在读盘，是花在**每次点读 4 个页、
// 每个页都要拷 4 KB + 解析整个节点**。这条计数用来证明解析缓存确实省掉了那 4 次解析。
#[cfg(test)]
thread_local! {
    static NODE_PARSES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

// 测试专用：有多少次插入因为"这一条装不进任何一页"而**一行没写**。
// 正常形状下必须为 0；它不是错误计数器，是"库允许多大的键"这条边界的读数。
#[cfg(test)]
thread_local! {
    static SPLIT_DROPS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn split_drops() -> u64 {
    SPLIT_DROPS.with(|c| c.get())
}

#[cfg(test)]
pub(crate) fn node_parses() -> u64 {
    NODE_PARSES.with(|c| c.get())
}

/// 行ID：指向数据在磁盘/内存中的位置
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
// pub struct RowId(pub u64);
pub struct RowId(pub Option<Vec<u8>>);

/// B+ 树索引键
// pub type Key = u64;
pub type Key = Vec<u8>;

/// B+ 树节点类型
#[derive(Debug, Clone, Serialize, Deserialize)]
enum BTreeNode {
    /// 叶子结点
    Leaf {
        /// kv结构
        entries: Vec<(Key, RowId)>,
        /// 下一个叶子结点的页ID
        next_leaf: Option<PageId>,
    },
    /// 非叶子结点
    Internal {
        /// 索引
        keys: Vec<Key>,
        /// 子节点页ID
        children: Vec<PageId>,
    },
}

/// 相邻一对节点重排的三种结局（`handle_underflow` 据此决定是否继续向上传下溢）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PairReorg {
    /// union 装得下一页 ⇒ 并成一页并**释放**另一页：父节点少一个子节点，可能连锁下溢
    Merged,
    /// union 装不下 ⇒ 按字节**重切**成两个各装得下的半页：不释放页，只换父节点那把分隔键
    Resplit,
    /// 一个字节都没动（找不到合法分界，或父子形状对不上）
    Nothing,
}

/// 一趟下降插入的结果：`writes` 是**还没落盘**的一串（页号, 内容），`alloc` 是途中占下的页
/// （整条路径作废时由 [`BPlusTree::insert`] 回收）。为什么要攒着一起写，见 `insert` 的注释。
struct InsertStep {
    landed: bool,
    /// 需要给父节点上提的分隔键 + 新建的右半页
    promote: Option<(Key, PageId)>,
    writes: Vec<(PageId, BTreeNode)>,
    alloc: Vec<PageId>,
}

impl InsertStep {
    fn written(writes: Vec<(PageId, BTreeNode)>) -> Self {
        Self {
            landed: true,
            promote: None,
            writes,
            alloc: Vec::new(),
        }
    }

    fn dropped() -> Self {
        Self {
            landed: false,
            promote: None,
            writes: Vec::new(),
            alloc: Vec::new(),
        }
    }
}

/// 带页管理的B+树
pub struct BPlusTree {
    /// 页管理器
    page_manager: PageManager,
    /// 根节点页ID
    root: PageId,
    /// 阶数
    order: usize,
    /// **解析过**的节点：`PageId → (这页当时的写入次数, 节点)`。
    ///
    /// 只服务只读路径（点读下降、反向找链尾）。校验靠 `PageManager::write_count` ——
    /// 写入按页计数，所以不需要在每个写点手工失效（漏一处就是读到脏节点）；页被回收再
    /// 复用时 `allocate_page` 也会 bump，不会把旧内容当成新内容。
    /// 上限跟着 `max_cache_pages` 走：超了就把**没钉住的**整批丢掉（解析缓存的意义是让点读
    /// 不重解析，而不是再维护一个无限增长的堆）。⚠️ 不许退回"整表 clear"：那会把上层页的
    /// 解析结果一起丢掉，而每一次点读都要用它们（见 [`BPlusTree::node_of`]）。
    node_cache: std::collections::HashMap<PageId, (u64, std::sync::Arc<BTreeNode>)>,
    /// 容量是不是**自动档**（调用方传的 `max_cache_pages == 0`）
    auto_cache: bool,
}

/// 自动档的下限：与旧的写死默认值一致 ⇒ 从"没旋钮"换到"自动档"不会让谁的工作集变小
const AUTO_MIN_PAGES: usize = 1024;
/// 自动档的上限（一页 4 KB ⇒ 最多 ≈ 8 MiB 常驻）。
///
/// 为什么必须夹住：B+ 树把整库都缓存住的常驻 ≈ 数据文件本身的大小（实测 200k 行 = 12129 页
/// ≈ 47 MB），那已经不是"缓存一点热数据"而是"把库再复制一份到内存"。
/// 这个基座要给单核 6 GB 的机器和 RSS ≤ 80 MB 的下游用，所以**默认只到 8 MiB，
/// 再多是调用方的决定**（显式传 `max_cache_pages`），不是库替你做的决定。
const AUTO_MAX_PAGES: usize = 2048;

/// 自动档的容量：按库当前的**实际页数**给，夹在 [`AUTO_MIN_PAGES`] 与 [`AUTO_MAX_PAGES`] 之间。
///
/// 为什么按页数而不是按行数：代价的载体是页（一次点读 = 沿路每层一页），覆盖率才是决定
/// "这次点读要不要读盘 + 重解析"的量 —— 实测同一个默认 1024 页的缓存，
/// 20k 行（1210 页）覆盖率 84.6% ⇒ 11.9 µs/次，200k 行（12129 页）覆盖率 8.4% ⇒ 45.2 µs/次。
fn auto_cache_pages(total_pages: u64) -> usize {
    (total_pages as usize).clamp(AUTO_MIN_PAGES, AUTO_MAX_PAGES)
}

impl BPlusTree {
    /// 打开或创建磁盘上的 B+ 树（返回 Result，便于上层处理锁冲突等）
    ///
    /// `max_cache_pages == 0` = **自动档**（M22）：容量按库当前的实际页数给，夹在
    /// [`AUTO_MIN_PAGES`] 与 [`AUTO_MAX_PAGES`] 之间，并在每次 `flush_all`（= 各家 checkpoint /
    /// bulk 收尾都会走到）之后重新评估 —— 库是会长大的，只在 open 时定一次容量就等于
    /// "导入完就再也没调过旋钮"。传非 0 就是显式容量，自动档完全不参与，行为与以前一致。
    pub fn try_open(
        path: impl AsRef<Path>,
        order: usize,
        max_cache_pages: usize,
    ) -> std::io::Result<Self> {
        assert!(order >= 3, "B+树阶数至少3阶");
        let auto = max_cache_pages == 0;
        let ask = if auto { AUTO_MIN_PAGES } else { max_cache_pages };
        let mut page_manager = PageManager::open(path, ask)?;

        let root = if let Some(root) = page_manager.root_page_id() {
            root
        } else {
            let root_page_id = page_manager.allocate_page();
            let root_node = BTreeNode::Leaf {
                entries: Vec::new(),
                next_leaf: None,
            };
            let node_data = bincode::serialize(&root_node)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
            page_manager.write_page(root_page_id, &node_data);
            page_manager.set_root_page_id(root_page_id);
            page_manager.flush_all();
            root_page_id
        };

        // 自动档：容量按这份库现在的页数来（meta 已加载 ⇒ next_page_id 是真的）
        if auto {
            let cap = auto_cache_pages(page_manager.next_page_id());
            page_manager.set_cache_capacity(cap);
        }

        Ok(Self {
            page_manager,
            root,
            order,
            node_cache: std::collections::HashMap::new(),
            auto_cache: auto,
        })
    }

    /// 打开或创建磁盘上的 B+ 树
    ///
    /// - 文件不存在 / 空文件：创建新树（空叶子根）
    /// - 已有合法文件：从 meta 恢复 root，数据跨进程保留
    pub fn open(path: impl AsRef<Path>, order: usize, max_cache_pages: usize) -> Self {
        Self::try_open(path, order, max_cache_pages)
            .map_err(|e| {
                format!(
                    "打开数据文件失败: {e}\n\
    提示: 若错误为 WouldBlock/锁被占用, 请确保同进程内已 drop 全部 MVCC 与 Transaction,\
    且没有其它进程占用 data.db.lock。"
                )
            })
            .unwrap()
    }

    /// 兼容旧接口：在系统临时目录创建匿名数据文件
    pub fn new(order: usize, max_cache_pages: usize) -> Self {
        // ⚠️ 路径必须带**进程内单调序号**，不能只靠时间戳：Windows 的 SystemTime 粒度较粗，
        // 并发测试里两个线程可以拿到同一个 nanos ⇒ 第二个会在 `.lock` 上吃到 WouldBlock，
        // 表现为随机失败的"打开数据文件失败"。同一个坑在 engine.rs / 各家 tmp_db 上都出现过。
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let path = std::env::temp_dir().join(format!(
            "bplus_anon_{}_{}_{}.db",
            std::process::id(),
            nanos,
            n
        ));
        Self::open(path, order, max_cache_pages)
    }

    /// 根页变化时同步到 meta（分裂升高 / 删除降层）
    fn set_root(&mut self, new_root: PageId) {
        self.root = new_root;
        self.page_manager.set_root_page_id(new_root);
    }

    /// 刷盘所有脏页 + meta，并 fsync
    pub fn flush_all(&mut self) {
        self.page_manager.set_root_page_id(self.root);
        self.page_manager.flush_all();
        // 自动档要在每次刷盘后重新评估：库是会长大的，只在 open 时定一次容量，
        // 等于"导入完就再也没调过旋钮"—— 而那正是这次要修的那个洞。
        if self.auto_cache {
            let cap = auto_cache_pages(self.page_manager.next_page_id());
            self.page_manager.set_cache_capacity(cap);
        }
    }

    /// 缓存策略读数：`(生效容量, 钉住的上层页数, 是否自动档)`
    ///
    /// 给 `tuning()` 与守卫用 —— 自动档必须**看得见生效值**，否则"设了没生效"和
    /// "生效了但不知道是多少"这两种情况在下游日志里长得一模一样。
    pub fn cache_policy(&self) -> (usize, usize, bool) {
        (
            self.page_manager.cache_capacity(),
            self.page_manager.pinned_pages(),
            self.auto_cache,
        )
    }

    /// 当前根页（WAL checkpoint 使用）
    pub fn root_page_id(&self) -> PageId {
        self.root
    }

    /// 下一个将分配的页号（WAL checkpoint 使用）
    pub fn next_page_id(&self) -> u64 {
        self.page_manager.next_page_id()
    }

    /// meta 中的 next_version（旧库无 NVER 时为 None）
    pub fn next_version(&self) -> Option<u64> {
        self.page_manager.next_version()
    }

    /// 持久化 next_version 到 meta（open/checkpoint 时调用）
    pub fn set_next_version(&mut self, v: u64) {
        self.page_manager.set_next_version(v);
    }

    /// 打开/关闭 bulk 模式（委托 PageManager）
    pub fn set_bulk_mode(&mut self, on: bool) {
        self.page_manager.set_bulk_mode(on);
    }

    pub fn bulk_mode(&self) -> bool {
        self.page_manager.bulk_mode()
    }
}

impl BPlusTree {
    /// 当前生效的阶数（门面读回用：证明"设进去的值真到了引擎里"）
    pub fn order(&self) -> usize {
        self.order
    }

    /// 页缓存容量（同上）
    pub fn max_cache_pages(&self) -> usize {
        self.page_manager.cache_capacity()
    }

    /// 树的形状（**测试/台架专用**）：`(树高, 内部节点数, 叶子数, 文件里的总页数)`。
    ///
    /// M22 需要先知道"一次随机点读要走几层、上层页占全库多少"才能定缓存策略 ——
    /// 上层是**结构性小**（叶子数 / order 量级）还是**跟库一起长**，决定了两件完全不同的事：
    /// 前者该常驻（钉住，代价与库大小无关），后者才需要谈容量与覆盖率。
    /// 走的是解析缓存，会计入 `page_reads` / `node_parses` ⇒ 台架里要在打点**之前**调它。
    #[cfg(test)]
    pub fn shape(&mut self) -> (u64, usize, usize, u64) {
        fn go(t: &mut BPlusTree, id: PageId, depth: u64, st: &mut (u64, usize, usize)) {
            st.0 = st.0.max(depth);
            match t.node_of(id) {
                Some(node) => match &*node {
                    BTreeNode::Internal { keys, children } => {
                        st.1 += 1;
                        let _ = keys;
                        for c in children.iter() {
                            go(t, *c, depth + 1, st);
                        }
                    }
                    BTreeNode::Leaf { .. } => st.2 += 1,
                },
                None => {}
            }
        }
        let mut st = (0u64, 0usize, 0usize);
        let root = self.root;
        go(self, root, 1, &mut st);
        (st.0, st.1, st.2, self.page_manager.next_page_id())
    }

    /// 只读路径取节点：**优先用解析过的**，未命中才读页 + bincode 解析。
    ///
    /// 为什么必须有它：归因量过，B+ 树点读的 41 µs 里几乎全是 CPU —— 每次点读要走 4 个页，
    /// 每个页都要 4 KB 拷贝 + 解析整节点（34~64 条 ⇒ 上百次堆分配）；缓存开大也没用。
    /// 校验凭 `PageManager::write_count` 按页做，所以写点不需要手工失效。
    fn node_of(&mut self, page_id: PageId) -> Option<std::sync::Arc<BTreeNode>> {
        let seen = self.page_manager.write_count(page_id);
        if let Some((at, node)) = self.node_cache.get(&page_id) {
            if *at == seen {
                return Some(node.clone());
            }
        }
        let data = self.page_manager.read_page(page_id)?;
        #[cfg(test)]
        NODE_PARSES.with(|c| c.set(c.get() + 1));
        let node = std::sync::Arc::new(bincode::deserialize::<BTreeNode>(&data).ok()?);
        // **上层页钉住**（M22）：根与内部节点是结构性小的一小撮（实测占全库 3.0%），而每一次
        // 点读、每一次反向游走都要用它们。不钉住时，全库走查把 LRU 转几圈就把它们挤出去 ——
        // 实测 200k 行的库里"走查后一次随机点读"因此从 45.2 µs 涨到 58.6 µs（+30%）。
        // 只钉 `Internal`：叶子占 97% 的页，钉叶子等于把整个库再复制一份进内存。
        if matches!(&*node, BTreeNode::Internal { .. }) {
            self.page_manager.pin(page_id);
        }
        if self.node_cache.len() >= self.page_manager.cache_capacity() {
            // 上限跟页缓存同源；超了丢**没钉住的**，留下钉住的。
            // 以前这里是整表 `clear()`：那等于每装满 cap 个节点就把上层页的解析结果白丢一次，
            // 而下一趟下降立刻要用它们 —— 走查之后点读变贵的那 30% 就是这笔账。
            let pm = &self.page_manager;
            self.node_cache.retain(|id, _| pm.is_pinned(*id));
        }
        self.node_cache.insert(page_id, (seen, node.clone()));
        Some(node)
    }

    /// 节点序列化后能不能装进一页。
    ///
    /// **只能问序列化结果，不能问条数**：条目是变长的（键可达几十字节、RowId 带 flag 与
    /// offset），`order=64` 在 40B 键 + 60B 值的真实形状下是 7.5 KB，而一页载荷只有 4080 B。
    /// 原先只有插入路径问过字节，delete 的借键/合并完全按条数走 ⇒ 见
    /// `test_reorg_never_drops_oversized_pages`。
    fn node_fits(node: &BTreeNode) -> bool {
        match bincode::serialize(node) {
            Ok(d) => d.len() <= MAX_PAYLOAD,
            Err(_) => false,
        }
    }

    /// 一条条目在序列化结果里占多少字节。
    ///
    /// bincode 把 `Vec<T>` 编成「长度 + 逐条」⇒「空 vec 的字节」减掉之后每条**可加**，
    /// 所以「节点字节 = 空节点字节 + Σ 每条字节」是精确值而不是估算（由
    /// `test_reorg_size_formula_matches_bincode` 逐档与真实序列化对拍钉住）。
    /// 为什么不直接 `serialized_size(&候选节点)` 逐档试：那要克隆上百条 `(Vec<u8>, RowId)`，
    /// 一次删除重探十几档 ⇒ 4000 次删除就是几百万次堆分配，**量尺自己变成瓶颈**。
    /// 为什么不让手算常量代替：`RowId` 是 `Option<Vec<u8>>`，键与值都变长，常量错一次
    /// 就是"按错的尺寸写坏一页"。
    fn vec_item_bytes<T: serde::Serialize>(item: &T) -> usize {
        let empty = bincode::serialized_size(&Vec::<T>::new()).unwrap_or(0) as i64;
        let one = bincode::serialized_size(&[item][..]).unwrap_or(0) as i64;
        (one - empty).max(0) as usize
    }

    /// 只有 `next_leaf` 的叶子（条目为空）序列化后的字节数
    fn leaf_base(next_leaf: Option<PageId>) -> i64 {
        let probe = BTreeNode::Leaf {
            entries: Vec::new(),
            next_leaf,
        };
        bincode::serialized_size(&probe).unwrap_or(i64::MAX as u64) as i64
    }

    /// 空内部节点（无键无子）的字节数，与「一个子节点占的字节」——子节点是定宽 `PageId`，
    /// 所以键数与子节点数可以分开加（同样由对拍测试钉住）
    fn internal_base() -> i64 {
        let probe = BTreeNode::Internal {
            keys: Vec::new(),
            children: Vec::new(),
        };
        bincode::serialized_size(&probe).unwrap_or(i64::MAX as u64) as i64
    }

    /// 两半**都装得下**的分界区间 `(lo, hi)`；`None` = 没有任何合法分界 ⇒ 调用方一字都不能写。
    ///
    /// 前缀随 k 变胖、后缀随 k 变瘦 ⇒ 可行档是一段连续区间，一次线性扫把两端都记住就行
    /// （不必二分：`k` 的上界就是阶数，一趟 O(order)）。
    /// 为什么不用 `serialized_size(&候选节点)` 逐档试：重组与分裂都在写路径上，一次十几档
    /// 每档克隆上百条 ⇒ delete 密集时**量尺自己变成瓶颈**；而 bincode 的 `Vec` 就是
    /// 「长度 + 逐条」，所以逐条字节相加是精确值（由 `test_reorg_size_formula_matches_bincode`
    /// 与真实序列化逐档对拍钉住 —— 公式偏了只会退回"什么都不动"，不红测试、静默不优化）。
    fn fitting_split(
        k_from: usize,
        k_to: usize,
        cap: i64,
        bytes: &dyn Fn(usize) -> (i64, i64),
    ) -> Option<(usize, usize)> {
        if k_from > k_to {
            return None;
        }
        let mut lo = None;
        let mut hi = None;
        for k in k_from..=k_to {
            let (left, right) = bytes(k);
            if left > cap {
                break; // 再往右只会更胖，后缀那边不用看了
            }
            if right <= cap {
                if lo.is_none() {
                    lo = Some(k);
                }
                hi = Some(k);
            }
        }
        Some((lo?, hi?))
    }

    /// 一批叶子条目 + `next_leaf` 序列化后的**精确**字节数（可加性见 `fitting_split` 的注释）
    fn leaf_total_bytes(entries: &[(Key, RowId)], next_leaf: Option<PageId>) -> i64 {
        Self::leaf_base(next_leaf)
            + entries
                .iter()
                .map(|e| Self::vec_item_bytes(e) as i64)
                .sum::<i64>()
    }

    /// 内部节点的精确字节数：键是变长的、子节点是定宽 `PageId` ⇒ 两项分开加
    fn internal_total_bytes(keys: &[Key], child_count: usize) -> i64 {
        Self::internal_base()
            + keys
                .iter()
                .map(|k| Self::vec_item_bytes(k) as i64)
                .sum::<i64>()
            + child_count as i64 * Self::internal_child_unit()
    }

    /// 前缀字节表：`pre[i]` = 前 i 条的字节和 ⇒ 任意半页的字节 O(1) 拿到
    fn prefix_bytes(per: &[i64]) -> Vec<i64> {
        let mut pre = Vec::with_capacity(per.len() + 1);
        pre.push(0i64);
        for b in per {
            pre.push(pre.last().unwrap() + b);
        }
        pre
    }

    /// 叶子的分裂点（左半留几条）：**取可行区间的中点**。
    ///
    /// 分裂与重组的目标不同，所以分界取法也不同 —— 分裂是把一页变两页，两边都得留增长空间，
    /// 取中点；重组是要把空位挤成一小片、好让它下一轮合并成功，取最满的一档
    /// （见 `rebalance_leaf_pair`）。两处照抄都会静默地更差，所以各写各的。
    fn leaf_split_point(
        entries: &[(Key, RowId)],
        next_leaf: Option<PageId>,
        order: usize,
    ) -> Option<usize> {
        let n = entries.len();
        let per: Vec<i64> = entries.iter().map(|e| Self::vec_item_bytes(e) as i64).collect();
        let pre = Self::prefix_bytes(&per);
        // 左半的下一叶是"某个 Some"，与具体页号无关（PageId 定宽）
        let base_left = Self::leaf_base(Some(PageId(0)));
        let base_right = Self::leaf_base(next_leaf);
        // 两半条数都必须 < order：否则分裂完谁也插不进去，下一次写必然超载
        let k_from = 1usize.max(n.saturating_sub(order - 1));
        let k_to = (order - 1).min(n.saturating_sub(1));
        let (lo, hi) = Self::fitting_split(k_from, k_to, MAX_PAYLOAD as i64, &|k| {
            (base_left + pre[k], base_right + (pre[n] - pre[k]))
        })?;
        Some((lo + hi) / 2)
    }

    /// 内部节点的分裂点（上提键的下标 `k`：左半拿 `keys[..k]`，右半拿 `keys[k+1..]`）
    fn internal_split_point(keys: &[Key], order: usize) -> Option<usize> {
        let n = keys.len();
        let per: Vec<i64> = keys.iter().map(|k| Self::vec_item_bytes(k) as i64).collect();
        let pre = Self::prefix_bytes(&per);
        let base = Self::internal_base();
        let cu = Self::internal_child_unit();
        // 左半 k 把键 + k+1 个子；右半 n-k-1 把键 + n-k 个子
        let k_from = 1usize.max(n.saturating_sub(order));
        let k_to = (order - 1).min(n.saturating_sub(1));
        let (lo, hi) = Self::fitting_split(k_from, k_to, MAX_PAYLOAD as i64, &|k| {
            (
                base + pre[k] + (k as i64 + 1) * cu,
                base + (pre[n] - pre[k + 1]) + (n as i64 - k as i64) * cu,
            )
        })?;
        Some((lo + hi) / 2)
    }

    fn internal_child_unit() -> i64 {
        let one = BTreeNode::Internal {
            keys: Vec::new(),
            children: vec![PageId(1)],
        };
        bincode::serialized_size(&one).unwrap_or(0) as i64 - Self::internal_base()
    }

    /// 这一行**住得住一页吗** —— 给上层在动 WAL 之前先问一句。
    ///
    /// 为什么要有这个谓词而不是等 `insert` 返回 `false`：事务层是「先 append WAL、再写树」，
    /// 树拒收之后 WAL 里就留下一条"提交了但这行不存在"的记录，恢复时按它重放只会再拒一次，
    /// 表现成"WAL 说有、树里查无此键"。所以拒绝必须发生在写 WAL **之前**。
    /// 判据与分裂用的是同一套字节账（`leaf_total_bytes`），不留两边不一致。
    pub fn row_fits(&self, key: &[u8], row: &Option<Vec<u8>>) -> bool {
        Self::leaf_total_bytes(&[(key.to_vec(), RowId(row.clone()))], None) <= MAX_PAYLOAD as i64
    }

    /// 插入一行。返回 `false` = **这一行没进去**，只有一种成因：这一条比一整页还大，
    /// 分裂也切不出「两半都装得下」的分界。此时磁盘上一个字节都没动 ⇒ 已有的行不受影响。
    ///
    /// 写法是**下降一趟全在内存里算，到顶才统一落盘**（`InsertStep::writes`）。老写法边算边写，
    /// 于是「孩子已经分裂写完、父亲却放不下」这一格会把刚分出去的那一半变成**没人引用的页** ——
    /// 和 M11f 那个静默丢页是同一个形状：`write_page` 超载只打一行日志，返回的 `false` 被调用链丢掉。
    pub fn insert(&mut self, key: Key, row_id: RowId) -> bool {
        let step = self.insert_recursive(self.root, key, row_id);
        if !step.landed {
            // 一个字节都没写 ⇒ 途中占下的页谁也不引用，回收掉别留孤儿
            for page in step.alloc {
                self.page_manager.free_page(page);
            }
            #[cfg(test)]
            SPLIT_DROPS.with(|c| c.set(c.get() + 1));
            return false;
        }
        for (page_id, node) in &step.writes {
            // 到这里的每个节点都逐档量过字节 ⇒ 写失败只剩「磁盘坏了」这一种解释
            let data = bincode::serialize(node).expect("serialize node");
            self.page_manager.write_page(*page_id, &data);
        }
        if let Some((promote_key, new_page)) = step.promote {
            let new_root_page_id = self.page_manager.allocate_page();
            let new_root = BTreeNode::Internal {
                keys: vec![promote_key],
                children: vec![self.root, new_page],
            };
            let root_data = bincode::serialize(&new_root).expect("serialize new root");
            self.page_manager.write_page(new_root_page_id, &root_data);
            self.set_root(new_root_page_id);
        }
        true
    }

    /// 一趟下降：返回「还没落盘的一串（页号, 内容）」与要不要往上升一层。
    /// `landed = false` 时 `writes` 必为空，`alloc` 继续往上传，由 [`Self::insert`] 统一回收。
    fn insert_recursive(&mut self, node_page_id: PageId, key: Key, row_id: RowId) -> InsertStep {
        let node_data = self.page_manager.read_page(node_page_id).unwrap();
        let node: BTreeNode = bincode::deserialize(&node_data).unwrap();
        let order = self.order;
        let cap = MAX_PAYLOAD as i64;

        match node {
            BTreeNode::Leaf {
                mut entries,
                next_leaf,
            } => {
                match entries.binary_search_by(|(k, _)| k.cmp(&key)) {
                    Ok(idx) => entries[idx].1 = row_id,
                    Err(idx) => entries.insert(idx, (key, row_id)),
                }
                if entries.len() < order && Self::leaf_total_bytes(&entries, next_leaf) <= cap {
                    return InsertStep::written(vec![(
                        node_page_id,
                        BTreeNode::Leaf {
                            entries,
                            next_leaf,
                        },
                    )]);
                }
                let Some(i) = Self::leaf_split_point(&entries, next_leaf, order) else {
                    return InsertStep::dropped();
                };
                let promote_key = entries[i].0.clone();
                let right_entries = entries.split_off(i);
                let new_page = self.page_manager.allocate_page();
                let left = BTreeNode::Leaf {
                    entries,
                    next_leaf: Some(new_page),
                };
                let right = BTreeNode::Leaf {
                    entries: right_entries,
                    next_leaf,
                };
                InsertStep {
                    landed: true,
                    promote: Some((promote_key, new_page)),
                    writes: vec![(node_page_id, left), (new_page, right)],
                    alloc: vec![new_page],
                }
            }
            BTreeNode::Internal {
                mut keys,
                mut children,
            } => {
                let child_idx = keys.iter().position(|k| key < *k).unwrap_or(keys.len());
                let child_page_id = children[child_idx];
                let child = self.insert_recursive(child_page_id, key, row_id);
                if !child.landed {
                    return child; // 孩子的 writes 本来就空；alloc 继续往上传
                }
                let Some((promote_key, new_page)) = child.promote else {
                    return child; // 下面没分裂 ⇒ 孩子那条写入原样上交
                };
                keys.insert(child_idx, promote_key);
                children.insert(child_idx + 1, new_page);
                if keys.len() < order && Self::internal_total_bytes(&keys, children.len()) <= cap {
                    let mut writes = child.writes;
                    writes.push((node_page_id, BTreeNode::Internal { keys, children }));
                    return InsertStep {
                        landed: true,
                        promote: None,
                        writes,
                        alloc: child.alloc,
                    };
                }
                let Some(k) = Self::internal_split_point(&keys, order) else {
                    // 塞不下又切不开 ⇒ **整条路径作废**：孩子的写入也丢掉。它们从没落过盘，
                    // 所以盘上还是各自原来的内容 —— 这比"写了一半才发现不行"强得多。
                    return InsertStep {
                        landed: false,
                        promote: None,
                        writes: Vec::new(),
                        alloc: child.alloc,
                    };
                };
                let up_key = keys[k].clone();
                let right_children = children.split_off(k + 1);
                // keys[k] 就是上提那把：先切走 [k+1..]，再把 keys[k] 从剩下的尾巴上摘掉
                let right_keys = keys.split_off(k + 1);
                keys.pop();
                let new_page = self.page_manager.allocate_page();
                let left = BTreeNode::Internal { keys, children };
                let right = BTreeNode::Internal {
                    keys: right_keys,
                    children: right_children,
                };
                let mut writes = child.writes;
                writes.push((node_page_id, left));
                writes.push((new_page, right));
                let mut alloc = child.alloc;
                alloc.push(new_page);
                InsertStep {
                    landed: true,
                    promote: Some((up_key, new_page)),
                    writes,
                    alloc,
                }
            }
        }
    }
}

impl BPlusTree {
    pub fn get(&mut self, key: Key) -> Option<RowId> {
        self.get_recursive(self.root, key)
    }

    pub fn get_recursive(&mut self, node_page_id: PageId, key: Key) -> Option<RowId> {
        // 走解析缓存：点读的代价大头是"每层都重新解析整个节点"，不是读盘
        let node = self.node_of(node_page_id)?;
        match &*node {
            BTreeNode::Leaf { entries, .. } => entries
                .binary_search_by(|(k, _)| k.cmp(&key))
                .ok()
                .map(|idx| entries[idx].1.clone()),
            BTreeNode::Internal { keys, children } => {
                let child_idx = keys
                    .iter()
                    .position(|k| key < k.clone())
                    .unwrap_or(keys.len());
                let next = children[child_idx];
                self.get_recursive(next, key)
            }
        }
    }
}

impl BPlusTree {
    pub fn update(&mut self, key: Key, new_row_id: RowId) -> bool {
        self.update_recursive(self.root, key, new_row_id)
    }

    fn update_recursive(&mut self, node_page_id: PageId, key: Key, new_row_id: RowId) -> bool {
        // 读取节点
        let node_data = self.page_manager.read_page(node_page_id).unwrap();
        let mut node: BTreeNode = bincode::deserialize(&node_data).unwrap();
        match &mut node {
            BTreeNode::Leaf { entries, .. } => {
                // 查找key
                if let Ok(idx) = entries.binary_search_by(|(k, _)| k.cmp(&key)) {
                    // 更新RowId
                    entries[idx].1 = new_row_id;
                    // 写回页：RowId 变长可能让整页超载 ⇒ 必须量过字节，装不下就报失败，
                    // 不能再走"write_page 打印一句、返回值丢掉"那条静默路径
                    return self.write_node(node_page_id, &node);
                }
                return false;
            }
            BTreeNode::Internal { keys, children } => {
                // 找到子节点
                let child_idx = keys
                    .iter()
                    .position(|k| key < k.clone())
                    .unwrap_or(keys.len());
                let child_page_id = children[child_idx];
                // 递归更新子节点
                self.update_recursive(child_page_id, key, new_row_id)
            }
        }
    }
}

impl BPlusTree {
    /// 删除：移除指定 key，必要时借键/合并并处理根降层
    pub fn delete(&mut self, key: Key) -> bool {
        let deleted = self.delete_recursive(self.root, key, None);

        // 根是内部节点且只剩一个子节点时降层
        if deleted {
            self.shrink_root_if_needed();
        }
        deleted
    }

    /// 最小填充数（⌈order/2⌉ - 1）；order=4 → min=1
    fn min_entries(&self) -> usize {
        (self.order + 1) / 2 - 1
    }

    fn read_node(&mut self, page_id: PageId) -> BTreeNode {
        let data = self.page_manager.read_page(page_id).expect("read page");
        bincode::deserialize(&data).expect("deserialize node")
    }

    /// 写回节点。**装不下就一个字节都不动**，并把失败告诉调用方。
    ///
    /// 为什么必须检查：`PageManager::write_page` 遇到超长 payload 只是打印 + 返回 false，
    /// 而这条链原先把那个 bool 丢了 ⇒ 页保持合并前的旧内容，父节点却已经删掉分隔键、
    /// `free_page` 掉死页 ⇒ 那批行不可达（静默丢数据），而且叶子的 `next_leaf` 还指向已释放页。
    fn write_node(&mut self, page_id: PageId, node: &BTreeNode) -> bool {
        if !Self::node_fits(node) {
            return false;
        }
        let data = bincode::serialize(node).expect("serialize node");
        self.page_manager.write_page(page_id, &data)
    }

    fn node_key_count(node: &BTreeNode) -> usize {
        match node {
            BTreeNode::Leaf { entries, .. } => entries.len(),
            BTreeNode::Internal { keys, .. } => keys.len(),
        }
    }

    fn is_underflow(&self, node: &BTreeNode) -> bool {
        Self::node_key_count(node) < self.min_entries()
    }

    /// 在父节点中查找 child 的下标
    fn get_parent_index(&mut self, parent_page_id: PageId, child_page_id: PageId) -> Option<usize> {
        match self.read_node(parent_page_id) {
            BTreeNode::Internal { children, .. } => {
                children.iter().position(|&pid| pid == child_page_id)
            }
            _ => None,
        }
    }

    /// 优先右兄弟，否则左兄弟；返回 (sibling_id, is_left_sibling)
    fn get_sibling(&mut self, parent_page_id: PageId, child_idx: usize) -> (Option<PageId>, bool) {
        match self.read_node(parent_page_id) {
            BTreeNode::Internal { children, .. } => {
                if child_idx + 1 < children.len() {
                    (Some(children[child_idx + 1]), false)
                } else if child_idx > 0 {
                    (Some(children[child_idx - 1]), true)
                } else {
                    (None, false)
                }
            }
            _ => (None, false),
        }
    }

    /// DFS 查找 target 的父节点（正确性优先，教学实现）
    fn find_parent_of(&mut self, target: PageId) -> Option<PageId> {
        if target == self.root {
            return None;
        }
        self.find_parent_of_rec(self.root, target)
    }

    fn find_parent_of_rec(&mut self, current: PageId, target: PageId) -> Option<PageId> {
        match self.read_node(current) {
            BTreeNode::Leaf { .. } => None,
            BTreeNode::Internal { children, .. } => {
                if children.iter().any(|&c| c == target) {
                    return Some(current);
                }
                // 克隆 children 避免与后续递归的可变借用冲突
                for child in children {
                    if let Some(p) = self.find_parent_of_rec(child, target) {
                        return Some(p);
                    }
                }
                None
            }
        }
    }

    fn shrink_root_if_needed(&mut self) {
        match self.read_node(self.root) {
            BTreeNode::Internal { children, .. } if children.len() == 1 => {
                let old_root = self.root;
                self.set_root(children[0]);
                self.page_manager.free_page(old_root);
            }
            _ => {}
        }
    }

    /// 叶子：向兄弟借一条记录；不改动 next_leaf 链表
    fn leaf_borrow_key(
        &mut self,
        node_page_id: PageId,
        sibling_page_id: PageId,
        is_left_sibling: bool,
        parent_page_id: PageId,
        child_idx: usize,
    )  -> bool {
        let mut node = self.read_node(node_page_id);
        let mut sibling = self.read_node(sibling_page_id);
        let mut parent = self.read_node(parent_page_id);

        match (&mut node, &mut sibling, &mut parent) {
            (
                BTreeNode::Leaf {
                    entries: node_entries,
                    ..
                },
                BTreeNode::Leaf {
                    entries: sib_entries,
                    ..
                },
                BTreeNode::Internal {
                    keys: parent_keys, ..
                },
            ) => {
                if is_left_sibling {
                    // 左兄弟借最后一条 → 成为当前节点最小键
                    let entry = sib_entries.pop().expect("left sibling empty");
                    parent_keys[child_idx - 1] = entry.0.clone();
                    node_entries.insert(0, entry);
                } else {
                    // 右兄弟借第一条；父分隔键应更新为右兄弟新的最小键
                    let entry = sib_entries.remove(0);
                    node_entries.push(entry);
                    if !sib_entries.is_empty() {
                        parent_keys[child_idx] = sib_entries[0].0.clone();
                    }
                }
            }
            _ => return false,
        }

        // 借完之后三个节点都必须还装得下一页，否则**这一步作废**（locals 丢弃、盘不动），
        // 由调用方退到整对重排。下溢本身不是错误，只是填充率低一点；写失败的静默丢页才是。
        if !Self::node_fits(&node) || !Self::node_fits(&sibling) || !Self::node_fits(&parent) {
            return false;
        }
        self.write_node(node_page_id, &node);
        self.write_node(sibling_page_id, &sibling);
        self.write_node(parent_page_id, &parent);
        true
    }

    /// 相邻两叶重排：union 装得下一页就合并，装不下就按字节**重切成两个各装得下的半页**。
    ///
    /// 为什么必须有"重切"这一支：`order=64` 在 40B 键 + 60B 值下两个半页的 union 就是
    /// 7.5 KB > 4080 B ⇒ 原先只有「合并」与「装不下就整次作废」两种结局，于是 delete 密集之后
    /// 下溢的叶子永远留着、填充率一路掉（实测 `vacuum()` 206 → 466 ms）。重切不释放页、
    /// 不动父子结构，只把父节点那把分隔键换成新右半页的最小键 ⇒ 行数一个不少，填充率回到 ~50%。
    ///
    /// 落盘前一律用 `node_fits`（真实序列化）复核：字节公式只用来**选分界**，
    /// 公式与真实不符时宁可留下溢，也不写一个装不下的页。
    fn rebalance_leaf_pair(
        &mut self,
        left_page: PageId,
        right_page: PageId,
        parent_page_id: PageId,
        sep_idx: usize,
    ) -> PairReorg {
        let (left_entries, left_next, right_entries, right_next) =
            match (self.read_node(left_page), self.read_node(right_page)) {
                (
                    BTreeNode::Leaf {
                        entries: a,
                        next_leaf: l,
                    },
                    BTreeNode::Leaf {
                        entries: b,
                        next_leaf: r,
                    },
                ) => (a, l, b, r),
                _ => return PairReorg::Nothing,
            };
        let (mut parent_keys, parent_children) = match self.read_node(parent_page_id) {
            BTreeNode::Internal { keys, children } => (keys, children),
            _ => return PairReorg::Nothing,
        };
        // 形状核对：这对页必须是父里相邻的两个子节点，**且叶子链也这么认为**。
        // 三样里任何一样对不上就什么都不动 —— 照着错结构改盘，比留下溢严重得多。
        if left_next != Some(right_page)
            || parent_children.len() != parent_keys.len() + 1
            || parent_children.get(sep_idx) != Some(&left_page)
            || parent_children.get(sep_idx + 1) != Some(&right_page)
        {
            return PairReorg::Nothing;
        }

        let tail = right_next;
        let mut union = left_entries;
        union.extend(right_entries);
        // 逐条问 bincode：`RowId` 是 `Option<Vec<u8>>`（变长），手算常量错一次就是写坏页
        let per: Vec<i64> = union.iter().map(|e| Self::vec_item_bytes(e) as i64).collect();
        let cap = MAX_PAYLOAD as i64;
        // 左半页的下一叶就是右半页，右半页接原本右页的下一叶 ⇒ 两个 base 分开算
        let base_left = Self::leaf_base(Some(right_page));
        let base_right = Self::leaf_base(tail);
        // 前缀和：`prefix[i]` = 前 i 条的字节和 ⇒ 任意半页的字节都是 O(1) 拿到
        let mut prefix = Vec::with_capacity(union.len() + 1);
        prefix.push(0i64);
        for b in &per {
            prefix.push(prefix.last().unwrap() + b);
        }
        let left_bytes = |i: usize| -> i64 { base_left + prefix[i] };
        let right_bytes = |i: usize| -> i64 { base_right + (prefix[union.len()] - prefix[i]) };

        // ① union 整个装得下一页 ⇒ 真合并（释放右页）。合并后的 next_leaf 就是 tail ⇒
        //    用 base_right 算，别用 base_left（差一个 Option 就是差一个字节档）
        if union.len() < self.order && base_right + prefix[union.len()] <= cap {
            let merged = BTreeNode::Leaf {
                entries: union,
                next_leaf: tail,
            };
            let mut children = parent_children;
            parent_keys.remove(sep_idx);
            children.remove(sep_idx + 1);
            let parent = BTreeNode::Internal {
                keys: parent_keys,
                children,
            };
            if !Self::node_fits(&merged) || !Self::node_fits(&parent) {
                return PairReorg::Nothing;
            }
            self.write_node(left_page, &merged);
            self.write_node(parent_page_id, &parent);
            self.page_manager.free_page(right_page);
            return PairReorg::Merged;
        }

        // ② 装不下 ⇒ 找"两半都装得下"的分界。前缀随 i 变胖、后缀随 i 变瘦 ⇒ 可行档位是
        //    一段**连续区间**，一路扫过去记住"最后一个后缀也装得下的档"就是它的最优端。
        let n = union.len();
        // 两半的条数都必须 < order：那是"以后还分裂得掉"的前提，不只是经典 B+ 树规矩
        // （`write_node` 只看字节，条数超了之后任何一次插入都分裂不开 ⇒ 必然超载写页）。
        let k_from = 1usize.max(n.saturating_sub(self.order - 1));
        let k_to = (self.order - 1).min(n - 1);
        let (_lo, hi) = match Self::fitting_split(k_from, k_to, cap, &|k| {
            (left_bytes(k), right_bytes(k))
        }) {
            Some(range) => range,
            None => return PairReorg::Nothing,
        };
        // 取**填得最满的那一档**（`hi`），不是中点：中点切法把两片都停在半满，
        // 页数量永远降不下来（这正是 466 ms 那张单的来源）。把左页压到底，剩下的
        // 挤成一小片 ⇒ 它下一次就能和**自己的**右兄弟合并成功 ⇒ 释放一页。
        // 空位不会丢：一路往库尾排，最后合并掉的就是最右那几片。
        let i = hi;
        let right_side = union.split_off(i);
        let sep_key = right_side[0].0.clone();
        let left_node = BTreeNode::Leaf {
            entries: union,
            next_leaf: Some(right_page),
        };
        let right_node = BTreeNode::Leaf {
            entries: right_side,
            next_leaf: tail,
        };
        let children = parent_children;
        parent_keys[sep_idx] = sep_key;
        let parent = BTreeNode::Internal {
            keys: parent_keys,
            children,
        };
        if !Self::node_fits(&left_node)
            || !Self::node_fits(&right_node)
            || !Self::node_fits(&parent)
        {
            return PairReorg::Nothing;
        }
        self.write_node(left_page, &left_node);
        self.write_node(right_page, &right_node);
        self.write_node(parent_page_id, &parent);
        PairReorg::Resplit
    }

    /// 内部节点：向兄弟借键（经典旋转）
    fn internal_borrow_key(
        &mut self,
        node_page_id: PageId,
        sibling_page_id: PageId,
        is_left_sibling: bool,
        parent_page_id: PageId,
        child_idx: usize,
    )  -> bool {
        let mut node = self.read_node(node_page_id);
        let mut sibling = self.read_node(sibling_page_id);
        let mut parent = self.read_node(parent_page_id);

        match (&mut node, &mut sibling, &mut parent) {
            (
                BTreeNode::Internal {
                    keys: node_keys,
                    children: node_children,
                },
                BTreeNode::Internal {
                    keys: sib_keys,
                    children: sib_children,
                },
                BTreeNode::Internal {
                    keys: parent_keys, ..
                },
            ) => {
                let parent_key_idx = if is_left_sibling {
                    child_idx - 1
                } else {
                    child_idx
                };
                let parent_key = parent_keys[parent_key_idx].clone();

                if is_left_sibling {
                    // 左兄弟最后的 key 上提到父，父原 key 下沉到当前最左
                    let borrowed_key = sib_keys.pop().expect("sib keys");
                    let borrowed_child = sib_children.pop().expect("sib children");
                    parent_keys[parent_key_idx] = borrowed_key;
                    node_keys.insert(0, parent_key);
                    node_children.insert(0, borrowed_child);
                } else {
                    // 右兄弟第一个 key 上提到父，父原 key 下沉到当前最右
                    let borrowed_key = sib_keys.remove(0);
                    let borrowed_child = sib_children.remove(0);
                    parent_keys[parent_key_idx] = borrowed_key;
                    node_keys.push(parent_key);
                    node_children.push(borrowed_child);
                }
            }
            _ => return false,
        }

        // 借完之后三个节点都必须还装得下一页，否则**这一步作废**（locals 丢弃、盘不动），
        // 由调用方退到整对重排。下溢本身不是错误，只是填充率低一点；写失败的静默丢页才是。
        if !Self::node_fits(&node) || !Self::node_fits(&sibling) || !Self::node_fits(&parent) {
            return false;
        }
        self.write_node(node_page_id, &node);
        self.write_node(sibling_page_id, &sibling);
        self.write_node(parent_page_id, &parent);
        true
    }

    /// 相邻两个内部节点重排：union 装得下一页就合并，装不下就按字节重切成两页。
    ///
    /// 与叶子的区别只在"分隔键是搬下来的、不是复制的"：内部节点的分隔键住在节点里，所以
    /// union 要把父那把键**拉下来**一起排，重切时再把新边界那把键**提回**父 —— 全程系统的
    /// 键数与子节点数都守恒（`k + 1 + m` 把键变 `i + 1 + (k+m-i)` 把）。
    /// 两侧都要求**至少一把键**（`i ∈ [1, n-2]`）：留一个空键的内部节点会让 `shrink_root_if_needed`
    /// 和下降逻辑多出没必要的分支。
    fn rebalance_internal_pair(
        &mut self,
        left_page: PageId,
        right_page: PageId,
        parent_page_id: PageId,
        sep_idx: usize,
    ) -> PairReorg {
        let (left_keys, left_children) = match self.read_node(left_page) {
            BTreeNode::Internal { keys, children } => (keys, children),
            _ => return PairReorg::Nothing,
        };
        let (right_keys, right_children) = match self.read_node(right_page) {
            BTreeNode::Internal { keys, children } => (keys, children),
            _ => return PairReorg::Nothing,
        };
        let (mut parent_keys, parent_children) = match self.read_node(parent_page_id) {
            BTreeNode::Internal { keys, children } => (keys, children),
            _ => return PairReorg::Nothing,
        };
        if parent_children.len() != parent_keys.len() + 1
            || parent_children.get(sep_idx) != Some(&left_page)
            || parent_children.get(sep_idx + 1) != Some(&right_page)
        {
            return PairReorg::Nothing;
        }

        let sep = parent_keys[sep_idx].clone();
        let mut union_keys = left_keys;
        let mut union_children = left_children;
        // 父那把键大于左半全部、小于右半全部 ⇒ 接在左半末尾，再拼上右半
        union_keys.push(sep);
        union_keys.extend(right_keys);
        union_children.extend(right_children);

        let base = Self::internal_base();
        let cu = Self::internal_child_unit();
        let cap = MAX_PAYLOAD as i64;
        let per: Vec<i64> = union_keys
            .iter()
            .map(|k| Self::vec_item_bytes(k) as i64)
            .collect();
        let n = union_keys.len();
        // 左半拿 keys[..i] + children[..i+1]，右半拿 keys[i+1..] + children[i+1..]
        // （keys[i] 本身提回父当分隔键 ⇒ 不进任何一半）
        let left_bytes = |i: usize| -> i64 { base + per[..i].iter().sum::<i64>() + (i as i64 + 1) * cu };
        let right_bytes = |i: usize| -> i64 {
            base + per[i + 1..].iter().sum::<i64>() + (n as i64 - i as i64) * cu
        };

        // ① union 整个装得下一页 ⇒ 真合并（父丢一把键与一个子节点，右页释放）
        //    合并后的节点就是 n 把键 + n+1 个子，正好等于 `left_bytes(n)`
        if n < self.order && left_bytes(n) <= cap {
            let merged = BTreeNode::Internal {
                keys: union_keys,
                children: union_children,
            };
            let mut children = parent_children;
            parent_keys.remove(sep_idx);
            children.remove(sep_idx + 1);
            let parent = BTreeNode::Internal {
                keys: parent_keys,
                children,
            };
            if !Self::node_fits(&merged) || !Self::node_fits(&parent) {
                return PairReorg::Nothing;
            }
            self.write_node(left_page, &merged);
            self.write_node(parent_page_id, &parent);
            self.page_manager.free_page(right_page);
            return PairReorg::Merged;
        }

        // ② 装不下 ⇒ 两半都装得下的 i 是一段连续区间（左随 i 变胖、右随 i 变瘦），取中点
        if n < 3 {
            // 至少要"左 1 把 + 提回父 1 把 + 右 1 把"才切得开
            return PairReorg::Nothing;
        }
        // 两侧都至少留一把键 ⇒ k ∈ [1, n-2]；条数上限同叶子（分裂后插不进）
        let k_from = 1usize.max(n.saturating_sub(self.order));
        let k_to = (self.order - 1).min(n.saturating_sub(2));
        let (_lo, hi) = match Self::fitting_split(k_from, k_to, cap, &|k| {
            (left_bytes(k), right_bytes(k))
        }) {
            Some(range) => range,
            None => return PairReorg::Nothing,
        };
        // 同叶子：取填得最满的那一档，把剩余挤成一小片好让后续合并成功
        let i = hi;

        let new_sep = union_keys[i].clone();
        let right_children = union_children.split_off(i + 1);
        let left_keys = union_keys[..i].to_vec();
        let right_keys = union_keys[i + 1..].to_vec();
        let left_node = BTreeNode::Internal {
            keys: left_keys,
            children: union_children,
        };
        let right_node = BTreeNode::Internal {
            keys: right_keys,
            children: right_children,
        };
        let children = parent_children;
        parent_keys[sep_idx] = new_sep;
        let parent = BTreeNode::Internal {
            keys: parent_keys,
            children,
        };
        if !Self::node_fits(&left_node)
            || !Self::node_fits(&right_node)
            || !Self::node_fits(&parent)
        {
            return PairReorg::Nothing;
        }
        self.write_node(left_page, &left_node);
        self.write_node(right_page, &right_node);
        self.write_node(parent_page_id, &parent);
        PairReorg::Resplit
    }

    /// 处理下溢：能借则借，否则整对重排（合并或按字节重切）；只有释放了页才递归处理父节点下溢
    fn handle_underflow(&mut self, node_page_id: PageId, parent_page_id: PageId) {
        if node_page_id == self.root {
            return;
        }

        let node = self.read_node(node_page_id);
        if !self.is_underflow(&node) {
            return;
        }

        let Some(child_idx) = self.get_parent_index(parent_page_id, node_page_id) else {
            return;
        };
        let (sibling_opt, is_left_sibling) = self.get_sibling(parent_page_id, child_idx);
        let Some(sibling_page_id) = sibling_opt else {
            return;
        };

        let sibling = self.read_node(sibling_page_id);
        let sibling_has_extra = Self::node_key_count(&sibling) > self.min_entries();

        // 先试最便宜的一招：只动两页 + 一把分隔键。借不动（兄弟条数不够）或**借完装不下**，
        // 都退到整对重排 —— 原先借完装不下就整次作废，等于"宁可一直下溢也不重排"。
        let is_leaf = matches!(&node, BTreeNode::Leaf { .. });
        if sibling_has_extra {
            let borrowed = if is_leaf {
                self.leaf_borrow_key(
                    node_page_id,
                    sibling_page_id,
                    is_left_sibling,
                    parent_page_id,
                    child_idx,
                )
            } else {
                self.internal_borrow_key(
                    node_page_id,
                    sibling_page_id,
                    is_left_sibling,
                    parent_page_id,
                    child_idx,
                )
            };
            // 借不释放页 ⇒ 父节点的子数不变 ⇒ 不会连锁下溢
            if borrowed {
                return;
            }
        }

        // 相邻两页按 key 顺序是「左在右之前」，重排只需要这一对页号 + 它们之间那把分隔键
        let (left_page, right_page, sep_idx) = if is_left_sibling {
            (sibling_page_id, node_page_id, child_idx - 1)
        } else {
            (node_page_id, sibling_page_id, child_idx)
        };
        let reorg = if is_leaf {
            self.rebalance_leaf_pair(left_page, right_page, parent_page_id, sep_idx)
        } else {
            self.rebalance_internal_pair(left_page, right_page, parent_page_id, sep_idx)
        };

        // 只有真的**释放了一页**，父节点才可能因此下溢：用 DFS 找祖父，正确向上传播
        if reorg == PairReorg::Merged {
            if parent_page_id != self.root {
                if let Some(grandparent) = self.find_parent_of(parent_page_id) {
                    self.handle_underflow(parent_page_id, grandparent);
                }
            } else {
                self.shrink_root_if_needed();
            }
        }
    }

    /// 递归删除；parent_page_id 在下降时传入，避免错误的父节点查找
    fn delete_recursive(
        &mut self,
        node_page_id: PageId,
        key: Key,
        parent_page_id: Option<PageId>,
    ) -> bool {
        let mut node = self.read_node(node_page_id);

        match &mut node {
            BTreeNode::Leaf { entries, .. } => {
                let Ok(idx) = entries.binary_search_by(|(k, _)| k.cmp(&key)) else {
                    return false;
                };
                entries.remove(idx);
                self.write_node(node_page_id, &node);

                // 非根叶子下溢 → 再平衡
                if node_page_id != self.root {
                    if let Some(parent) = parent_page_id {
                        if self.is_underflow(&node) {
                            self.handle_underflow(node_page_id, parent);
                        }
                    }
                }
                true
            }
            BTreeNode::Internal { keys, children } => {
                let child_idx = keys.iter().position(|k| key < *k).unwrap_or(keys.len());
                let child_page_id = children[child_idx];
                let deleted = self.delete_recursive(child_page_id, key, Some(node_page_id));
                if !deleted {
                    return false;
                }

                // 子树删除后，若分隔键对应子树最小键变化则更新
                // keys[i] 是 children[i+1] 子树的下界 → 更新 keys[child_idx - 1]
                if child_idx > 0 {
                    if let Some(min_key) = self.subtree_min_key(child_page_id) {
                        // 重新读取：子路径可能已合并/借键，本节点可能已变
                        // 若本节点在合并中被释放则跳过
                        if self.page_manager.read_page(node_page_id).is_some() {
                            let mut cur = self.read_node(node_page_id);
                            if let BTreeNode::Internal {
                                keys: ks,
                                children: ch,
                            } = &mut cur
                            {
                                // child 可能已变下标，重新定位
                                if let Some(new_idx) = ch.iter().position(|&c| c == child_page_id) {
                                    if new_idx > 0 && new_idx - 1 < ks.len() {
                                        ks[new_idx - 1] = min_key;
                                        self.write_node(node_page_id, &cur);
                                    }
                                }
                            }
                        }
                    }
                }

                // 检查本节点是否下溢（合并子节点后）
                if node_page_id != self.root {
                    if let Some(parent) = parent_page_id {
                        if self.page_manager.read_page(node_page_id).is_some() {
                            let cur = self.read_node(node_page_id);
                            if self.is_underflow(&cur) {
                                self.handle_underflow(node_page_id, parent);
                            }
                        }
                    }
                } else {
                    self.shrink_root_if_needed();
                }

                true
            }
        }
    }

    /// 子树最小键（一直向左走到叶子）
    fn subtree_min_key(&mut self, node_page_id: PageId) -> Option<Key> {
        // 节点可能在合并中被释放
        let data = self.page_manager.read_page(node_page_id)?;
        let node: BTreeNode = bincode::deserialize(&data).ok()?;
        match node {
            BTreeNode::Leaf { entries, .. } => entries.first().map(|(k, _)| k.clone()),
            BTreeNode::Internal { children, .. } => children
                .first()
                .copied()
                .and_then(|c| self.subtree_min_key(c)),
        }
    }
}

impl BPlusTree {
    /// 范围查询：[low, high]
    pub fn range_scan(&mut self, low: Key, high: Key) -> Vec<(Key, RowId)> {
        let mut result = Vec::new();
        let mut current_leaf_id = self.find_leaf_for_key(self.root, low.clone());
        while let Some(node_id) = current_leaf_id {
            // 读取叶子结点
            let node_data = self.page_manager.read_page(node_id).unwrap();
            let node: BTreeNode = bincode::deserialize(&node_data).unwrap();
            if let BTreeNode::Leaf { entries, next_leaf } = node {
                // 遍历当前叶子节点的entries
                for entry in &entries {
                    let (k, rid) = entry;
                    if k > &high {
                        // 超出上限，直接返回
                        return result;
                    }
                    if k >= &low {
                        result.push((k.clone(), rid.clone()));
                    }
                }

                // 移动到下一个叶子节点
                current_leaf_id = next_leaf;
            } else {
                // 不是叶子节点，终止
                break;
            }
        }
        result
    }

    /// 找到包含指定key的叶子节点ID
    /// 在闭区间 `[low, high]` 内**从大到小**找第一个被 `accept` 接受的行。
    ///
    /// 版本键 `raw_key ‖ version.to_be_bytes()` 大端有序 ⇒ 同一逻辑 key 的最新版本排在
    /// 区间末尾，所以「从高界往回走、第一个满足条件的就停」通常一次下降就命中，
    /// 代价只和树高有关、和链长无关。
    ///
    /// 本树的叶子之间只有单向 `next_leaf`，没有 `prev_leaf`，所以「再往前一片」不是走链表
    /// 而是**重新下降**：取本叶最小键 `m`，用它的真前缀 `m[..m.len()-1]`（一定小于 `m`，
    /// enc 键至少 8 字节所以前缀必然存在）作新上界。但「小于 m」并不保证落到更左的叶子——
    /// 已经走到最左叶子时会又回到同一片，所以额外用 `prev_leaf_min` 做**进度守卫**：
    /// 下一跳的叶最小键若不严格小于上一跳的，就说明没有更左的叶子了，直接收尾。
    /// 宁可少走，也不能原地打转。
    ///
    /// 扫描中一旦遇到 `k < low` 就返回 `None`：叶子按键升序，再往左全部落在区间之外。
    pub fn find_back(
        &mut self,
        low: &[u8],
        high: &[u8],
        mut accept: impl FnMut(&[u8]) -> bool,
    ) -> Option<(Key, RowId)> {
        let mut bound = high.to_vec();
        // 上一跳那片叶子的最小键。每跳都必须严格小于它，否则说明下降又回到了同一片叶子
        // （典型情形：已经走到最左叶子，它下面没有叶子了）—— 此时区间内再无未考察的行。
        let mut prev_leaf_min: Option<Key> = None;
        loop {
            let leaf_id = self.find_leaf_for_key(self.root, bound.clone())?;
            // 这条也是**点读的热路径**（MVCC 取版本链尾就走它）⇒ 同样走解析缓存
            let node = self.node_of(leaf_id)?;
            let BTreeNode::Leaf { entries, .. } = node.as_ref() else {
                log::warn!("find_back: 期望叶子却拿到内部节点，停止反向游走");
                return None;
            };

            let mut leaf_min: Option<Key> = None;
            for (k, rid) in entries.iter().rev() {
                leaf_min = Some(k.clone());
                // 首跳那片叶子可能延伸到高界之外；那些行不属于本区间
                if k.as_slice() > high {
                    continue;
                }
                // 叶子按键升序：反向走到这里，再往左全部落在区间之外
                if k.as_slice() < low {
                    return None;
                }
                if accept(k.as_slice()) {
                    return Some((k.clone(), rid.clone()));
                }
            }

            // 本叶没有命中：以更小的上界重新下降到左边的叶子
            let leaf_min = leaf_min?;
            if let Some(prev) = &prev_leaf_min {
                if leaf_min.as_slice() >= prev.as_slice() {
                    // 没有左移 ⇒ 已在最左叶子，区间内的行全部考察过了
                    return None;
                }
            }
            prev_leaf_min = Some(leaf_min.clone());
            if leaf_min.len() <= 1 {
                return None;
            }
            let mut next_bound = leaf_min;
            next_bound.truncate(next_bound.len() - 1);
            bound = next_bound;
        }
    }

    fn find_leaf_for_key(&mut self, node_page_id: PageId, key: Key) -> Option<PageId> {
        // 下降只用来定位，不改内容 ⇒ 走解析缓存（点读、反向找链尾、区间起点都靠它）
        let node = self.node_of(node_page_id)?;
        match &*node {
            BTreeNode::Leaf { .. } => Some(node_page_id),
            BTreeNode::Internal { keys, children } => {
                let child_idx = keys
                    .iter()
                    .position(|k| key < k.clone())
                    .unwrap_or(keys.len());
                let next = children[child_idx];
                self.find_leaf_for_key(next, key)
            }
        }
    }
}

pub struct BPlusTreeIterator<'a> {
    tree: &'a mut BPlusTree,
    current_page_id: Option<PageId>,
    current_idx: usize,
}

impl BPlusTree {
    /// 全表流式遍历，按**引用**把 `(键, value)` 交给回调 —— 不克隆任何 value。
    ///
    /// B+ 树的 value 就住在叶子条目里，没有 Bitcask `KeyDir` 那种独立键索引，所以
    /// 「过每一行、把整页反序列化出来」这一遍省不掉（页读取次数不变）—— 这点和
    /// Bitcask 不一样，别指望一样。真正能省的是**把每行 value 克隆出来**：
    /// `iter()` 每行克隆一次，本方法一次都不克隆。全库折叠与 vacuum 都改用它。
    pub fn scan_entries_with(&mut self, mut f: impl FnMut(&[u8], &Option<Vec<u8>>)) {
        // 先下降到最左叶子
        let mut current = self.root;
        loop {
            let Some(data) = self.page_manager.read_page(current) else {
                return;
            };
            match bincode::deserialize::<BTreeNode>(&data) {
                Ok(BTreeNode::Internal { children, .. }) => {
                    let Some(&first) = children.first() else {
                        return;
                    };
                    current = first;
                }
                Ok(BTreeNode::Leaf { .. }) => break,
                Err(_) => return,
            }
        }

        // 再沿单向 next_leaf 链表一路向右。读页/反序列化失败就止步（与现有 iter 一致地宽容）
        // 走查用 `read_page_for_walk`：每片叶子只碰一次，回填它不会有下一次命中，
        // 却会把点读焐热的页挤出去（实测一次全库走查换出 1600 页、顺带 flush 765 页脏页）。
        let mut leaf = Some(current);
        while let Some(id) = leaf {
            let Some(data) = self.page_manager.read_page_for_walk(id) else {
                break;
            };
            let Ok(node) = bincode::deserialize::<BTreeNode>(&data) else {
                break;
            };
            if let BTreeNode::Leaf { entries, next_leaf } = node {
                for (k, rid) in &entries {
                    f(k, &rid.0);
                }
                leaf = next_leaf;
            } else {
                break;
            }
        }
    }

    /// 只要键的场合（代价与理由见 [`Self::scan_entries_with`]）。
    pub fn scan_keys_with(&mut self, mut f: impl FnMut(&[u8])) {
        self.scan_entries_with(|k, _row| f(k));
    }

    /// 从 `from`（**含**，落在键的字节域上）起的流式遍历，按引用交给 `(键, value)`；
    /// 回调返回 `true` 表示「够了，止步于此」。
    ///
    /// 与 [`Self::scan_entries_with`] 的差别只有两件事：**起点**（下降到Containing叶子，
    /// 前面的叶子一篇都不读）和**止步**（回调喊停就不再往右）。起点是键域下界而不是逻辑
    /// key，所以调用方要「跳过某个逻辑 key 的整组」得自己按 `raw_key > from` 过滤。
    ///
    /// 存在的理由是锁：`BPlusTree` 挂在 `Mutex` 后面，全表遍历会把这把锁按到整库扫完，
    /// 期间点读点写全部排队。有界回收（`vacuum_bounded`）靠它把每一轮按住一小段。
    pub fn scan_entries_break_from(
        &mut self,
        from: Option<&[u8]>,
        mut f: impl FnMut(&[u8], &Option<Vec<u8>>) -> bool,
    ) {
        // 空键 ≤ 一切 ⇒ 没有起点时下降到的就是最左叶子
        let mut leaf = self.find_leaf_for_key(self.root, from.unwrap_or(&[]).to_vec());
        while let Some(id) = leaf {
            // 同上：一次性走查不回填、不换出，见 `scan_entries_with`
            let Some(data) = self.page_manager.read_page_for_walk(id) else {
                break;
            };
            let Ok(node) = bincode::deserialize::<BTreeNode>(&data) else {
                break;
            };
            match node {
                BTreeNode::Leaf { entries, next_leaf } => {
                    for (k, rid) in &entries {
                        if f(k, &rid.0) {
                            return;
                        }
                    }
                    leaf = next_leaf;
                }
                _ => break,
            }
        }
    }

    pub fn iter(&mut self) -> BPlusTreeIterator<'_> {
        // 找到最左边的叶子节点
        let mut current = self.root;
        loop {
            let node_data = self.page_manager.read_page(current).expect("读取页失败");
            let node: BTreeNode = bincode::deserialize(&node_data).expect("反序列化失败");

            match node {
                BTreeNode::Internal { children, .. } => {
                    current = children[0]; // 始终向左走
                }
                BTreeNode::Leaf { .. } => break,
            }
        }

        BPlusTreeIterator {
            tree: self,
            current_page_id: Some(current),
            current_idx: 0,
        }
    }
}

impl<'a> Iterator for BPlusTreeIterator<'a> {
    type Item = (Key, RowId);

    fn next(&mut self) -> Option<Self::Item> {
        let page_id = self.current_page_id?;

        // 读取当前节点（叶子链上的走查 ⇒ 不回填、不把点读的热页挤出去）
        let node_data = self.tree.page_manager.read_page_for_walk(page_id)?;
        let node: BTreeNode = bincode::deserialize(&node_data).ok()?;

        if let BTreeNode::Leaf { entries, next_leaf } = node {
            if self.current_idx < entries.len() {
                // 返回当前条目并自增索引
                #[cfg(test)]
                crate::bplus_tree::page::note_value_clone();
                let item = entries[self.current_idx].clone();
                self.current_idx += 1;
                Some(item)
            } else {
                // 当前页读完了，跳转到下一个叶子节点
                self.current_page_id = next_leaf;
                self.current_idx = 0;
                self.next() // 递归调用以获取下一页的第一个元素
            }
        } else {
            None
        }
    }
}

/// 测试专用：叶子层形状画像。**重组有没有真的把空位收掉，只能按页数得出来** ——
/// 行数在任何一种结局下都不变，所以"没丢数据"那类断言对这件事完全不敏感。
#[cfg(test)]
impl BPlusTree {
    /// `(叶子片数, 行数, 单片最少条数, 单片最多条数)`；沿 `next_leaf` 走到底
    pub(crate) fn leaf_profile(&mut self) -> (usize, usize, usize, usize) {
        let mut cur = self.root;
        let (mut leaves, mut rows, mut min, mut max) = (0usize, 0usize, usize::MAX, 0usize);
        loop {
            let next = match self.read_node(cur) {
                BTreeNode::Internal { children, .. } => {
                    cur = *children.first().expect("内部节点没有子节点");
                    continue;
                }
                BTreeNode::Leaf {
                    entries, next_leaf, ..
                } => {
                    leaves += 1;
                    rows += entries.len();
                    min = min.min(entries.len());
                    max = max.max(entries.len());
                    next_leaf
                }
            };
            match next {
                Some(p) => cur = p,
                None => break,
            }
        }
        (leaves, rows, if min == usize::MAX { 0 } else { min }, max)
    }
}

// 为BTreeNode添加as_ref方法（方便获取叶子节点的next_leaf）
impl AsRef<BTreeNode> for BTreeNode {
    fn as_ref(&self) -> &Self {
        self
    }
}

// 格式化输出
impl fmt::Display for BPlusTree {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "B+Tree(order={}, root_page_id={})",
            self.order, self.root.0
        )
    }
}

// ===================== 测试代码 =====================
#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;

    fn rid(v: u8) -> RowId {
        RowId(Some(vec![v]))
    }

    fn key(n: u8) -> Key {
        vec![n]
    }

    fn collect_all(tree: &mut BPlusTree) -> Vec<u8> {
        tree.iter().map(|(k, _)| k[0]).collect()
    }

    #[test]
    fn test_insert_get_update_range() {
        let mut tree = BPlusTree::new(4, 16);
        for n in [5u8, 10, 15, 20, 25, 30, 35] {
            tree.insert(key(n), rid(n));
        }

        assert_eq!(tree.get(key(10)).unwrap().0, Some(vec![10]));
        assert!(tree.get(key(99)).is_none());

        assert!(tree.update(key(10), rid(99)));
        assert_eq!(tree.get(key(10)).unwrap().0, Some(vec![99]));

        let range: Vec<u8> = tree
            .range_scan(key(5), key(20))
            .into_iter()
            .map(|(k, _)| k[0])
            .collect();
        assert_eq!(range, vec![5, 10, 15, 20]);
    }

    #[test]
    fn test_delete_and_rebalance() {
        let mut tree = BPlusTree::new(4, 32);
        // 插入足够多以触发多层
        let keys: Vec<u8> = (1..=30).collect();
        for &n in &keys {
            tree.insert(key(n), rid(n));
        }
        assert_eq!(collect_all(&mut tree), keys);

        // 删除中间键，触发借键/合并
        for n in [10u8, 11, 12, 5, 6, 7, 8, 9, 15, 16, 17, 18, 19, 20] {
            assert!(tree.delete(key(n)), "delete {n} should succeed");
            assert!(tree.get(key(n)).is_none(), "key {n} should be gone");
        }

        let remaining: Vec<u8> = (1..=30u8)
            .filter(|n| ![10, 11, 12, 5, 6, 7, 8, 9, 15, 16, 17, 18, 19, 20].contains(n))
            .collect();
        assert_eq!(collect_all(&mut tree), remaining);

        // 再删光
        for n in remaining {
            assert!(tree.delete(key(n)));
        }
        assert!(collect_all(&mut tree).is_empty());
    }

    #[test]
    fn test_delete_missing_key() {
        let mut tree = BPlusTree::new(4, 8);
        tree.insert(key(1), rid(1));
        assert!(!tree.delete(key(2)));
        assert!(tree.delete(key(1)));
        assert!(!tree.delete(key(1)));
    }

    #[test]
    fn test_iter_order() {
        let mut tree = BPlusTree::new(4, 16);
        for n in [35u8, 10, 25, 5, 30, 15, 20] {
            tree.insert(key(n), rid(n));
        }
        assert_eq!(collect_all(&mut tree), vec![5, 10, 15, 20, 25, 30, 35]);
        tree.flush_all();
    }

    #[test]
    fn test_overwrite_on_insert() {
        let mut tree = BPlusTree::new(4, 8);
        tree.insert(key(1), rid(1));
        tree.insert(key(1), rid(2));
        assert_eq!(tree.get(key(1)).unwrap().0, Some(vec![2]));
        assert_eq!(collect_all(&mut tree).len(), 1);
    }

    /// 真实条目大小下的 delete：借键/合并不许动一页装不下的写，也不许丢行。
    ///
    /// 旧实现**按条数**判断（`order/2` 对半、`min_entries` 比较），从不量序列化字节：
    /// 40B 键 + 60B 值时两个半页合并 = 7.5 KB > 4080 ⇒ `write_page` 打印一句"数据超过页载荷
    /// 限制"并返回 false，而调用链把那个 bool 丢了 ⇒ 幸存页保持**合并前**的内容，父节点却
    /// 已经删掉分隔键并 `free_page(死页)` ⇒ 那批行同时失去两份副本 = 静默不可达。
    /// 大库基线台架跑 bplus 时刷出的几万行那条消息，走的就是这里
    /// （而且 eprintln 本身把被测耗时污染了 —— `search 首页` 911ms 里有它一份）。
    #[test]
    fn test_reorg_never_drops_oversized_pages() {
        const N: usize = 4000;
        // 台架同款形状：键 ≈ 35 B、值 60 B ⇒ order=64 的一页装不下 64 条
        fn big_key(i: usize) -> Key {
            format!("{i:08}#/worldquant/datafields/x.json").into_bytes()
        }
        fn big_rid() -> RowId {
            RowId(Some(vec![b'x'; 60]))
        }

        let mut tree = BPlusTree::new(64, 256);
        for i in 0..N {
            assert!(tree.insert(big_key(i), big_rid()), "插入第 {i} 个键失败");
        }
        // 隔一个删一个 ⇒ 大量下溢，逼着走借键与合并
        for i in (0..N).step_by(2) {
            assert!(tree.delete(big_key(i)), "删除第 {i} 个键失败");
        }

        let oversized = crate::bplus_tree::page::oversized_writes();
        assert_eq!(oversized, 0, "一次超载写都不该尝试：重组前必须先量字节");

        // 剩下的键必须一个不少、全部读得回来（丢页的表现就是键突然不可达）
        let mut alive = 0usize;
        for i in (1..N).step_by(2) {
            assert!(tree.get(big_key(i)).is_some(), "第 {i} 个键在重组后读不回来了");
            alive += 1;
        }
        assert_eq!(alive, (N + 1) / 2);
        assert_eq!(
            tree.iter().count(),
            alive,
            "叶子链表的行数必须等于存活键数（链表指向已释放页会在这里露出来）"
        );
    }

    /// 一条**比整页还大**的行：分裂切不出"两半都装得下"的分界 ⇒ 必须**一字不写并报告失败**，
    /// 已有的行一条不少。老写法是"按条数对半切、两半都硬写"，写不进的那一半只留下一行日志，
    /// 于是它变成**没人引用的页** = 静默丢数据（和 M11f 那个洞同一个形状，只是在插入这一侧）。
    #[test]
    fn test_oversized_row_is_refused_not_dropped_silently() {
        const N: usize = 300;
        fn k(i: usize) -> Key {
            format!("k{i:06}").into_bytes()
        }
        let mut tree = BPlusTree::new(64, 512);
        for i in 0..N {
            assert!(tree.insert(k(i), RowId(Some(vec![b'v'; 60]))), "第 {i} 行没进去");
        }
        let before = tree.iter().count();
        let drops_before = split_drops();
        // 谓词与分裂必须用同一套字节账：否则上层按 `row_fits` 放行、树却按另一套判据拒收
        assert!(
            !tree.row_fits(&k(N), &Some(vec![b'H'; 8 * 1024])),
            "8 KB 的行必须被 `row_fits` 判成装不下"
        );
        assert!(tree.row_fits(&k(N), &Some(vec![b'v'; 60])), "正常行不能被误判");

        // 8 KB 比一页载荷(4080 B)还大 ⇒ 无论怎么切都有一侧装不下
        assert!(
            !tree.insert(k(N), RowId(Some(vec![b'H'; 8 * 1024]))),
            "装不下的行必须报告失败，不能回一句「成功」"
        );
        assert_eq!(
            split_drops() - drops_before,
            1,
            "要走的是「一行都没写」那条路，而不是别的失败"
        );
        assert_eq!(tree.iter().count(), before, "拒绝一行不许牵连已有的行");
        for i in 0..before {
            assert!(tree.get(k(i)).is_some(), "第 {i} 行在读的时候不见了");
        }
        assert_eq!(
            crate::bplus_tree::page::oversized_writes(),
            0,
            "连一次超载写都不该尝试：分裂前就要量得出两半"
        );
    }

    /// 重切的**分界**由字节公式选出，所以公式必须与真实序列化逐档相等。
    ///
    /// 为什么单开一条：公式偏了，落盘前的 `node_fits` 只会让它退回
    /// "什么都不动" ⇒ 数据一条不丢、测试全绿，但优化**静默地没发生**。这种失败只有对拍抓得到。
    #[test]
    fn test_reorg_size_formula_matches_bincode() {
        fn item(i: usize, pad: usize) -> (Key, RowId) {
            (
                format!("k{i:06}{}", "#".repeat(pad)).into_bytes(),
                RowId(Some(vec![b'v'; 12 + pad])),
            )
        }
        for pad in [0usize, 7, 40, 120, 300] {
            let items: Vec<(Key, RowId)> = (0..50).map(|i| item(i, pad)).collect();
            for next_leaf in [None, Some(PageId(7))] {
                let node = BTreeNode::Leaf {
                    entries: items.clone(),
                    next_leaf,
                };
                let real = bincode::serialize(&node).unwrap().len() as i64;
                let formula = BPlusTree::leaf_base(next_leaf)
                    + items
                        .iter()
                        .map(|e| BPlusTree::vec_item_bytes(e) as i64)
                        .sum::<i64>();
                assert_eq!(formula, real, "叶子字节公式不等于真实序列化（pad={pad}）");
            }

            let keys: Vec<Key> = (0..40).map(|i| item(i, pad).0).collect();
            let children: Vec<PageId> = (0..keys.len() + 1).map(|c| PageId(c as u64)).collect();
            let node = BTreeNode::Internal {
                keys: keys.clone(),
                children,
            };
            let real = bincode::serialize(&node).unwrap().len() as i64;
            let formula = BPlusTree::internal_base()
                + keys.iter().map(|k| BPlusTree::vec_item_bytes(k) as i64).sum::<i64>()
                + (keys.len() as i64 + 1) * BPlusTree::internal_child_unit();
            assert_eq!(formula, real, "内部节点字节公式不等于真实序列化（pad={pad}）");
        }
    }

    /// delete 密集之后**页数必须收回来到 rows/每页容量** —— 这条才是"合并后再分裂"的验收。
    ///
    /// 旧结局是"union 装不下 ⇒ 整次作废"：行数一条不少、一条测试都不红，但按页走的调用
    /// 一直是双倍代价（实测 `vacuum()` 206 → 466 ms）。把重切那一支临时改成"什么都不动"，
    /// 这条会红（页数停在删之前的量级）。
    #[test]
    fn test_reorg_packs_leaves_instead_of_leaving_them_half_empty() {
        const N: usize = 4000;
        fn big_key(i: usize) -> Key {
            format!("{i:08}#/worldquant/datafields/x.json").into_bytes()
        }
        fn big_rid() -> RowId {
            RowId(Some(vec![b'x'; 60]))
        }

        let mut tree = BPlusTree::new(64, 4096);
        for i in 0..N {
            assert!(tree.insert(big_key(i), big_rid()), "插入第 {i} 个键失败");
        }
        let (leaves_full, rows_full, ..) = tree.leaf_profile();
        assert_eq!(rows_full, N);

        for i in (0..N).step_by(2) {
            assert!(tree.delete(big_key(i)), "删除第 {i} 个键失败");
        }
        let (leaves, rows, min, max) = tree.leaf_profile();
        let alive = (N + 1) / 2;
        let item = |i: usize| (big_key(i), big_rid());
        assert_eq!(rows, alive, "行数一条不许少");
        // 删一半之后仍然可达 + 叶子链没断
        for i in (1..N).step_by(2) {
            assert!(tree.get(big_key(i)).is_some(), "第 {i} 个键在重组后读不回来了");
        }
        assert_eq!(
            crate::bplus_tree::page::oversized_writes(),
            0,
            "重组前必须先量字节"
        );

        // 尺子必须问"一页**装得下**几条"，不能问插入时的观察值：插入路径按条数分裂，
        // 每页只填到 ~22 条（`maxf`），而这批键的真实字节容量是 ~35 条 —— 拿 22 当上界
        // 会把"重组其实更满"误判成异常。这里逐条加到装不下为止，直接量出容量。
        let cap = (1usize..)
            .take_while(|&k| {
                let probe = BTreeNode::Leaf {
                    entries: (0..k).map(item).collect(),
                    next_leaf: Some(PageId(999)),
                };
                BPlusTree::node_fits(&probe)
            })
            .last()
            .unwrap_or(1);
        let ideal = alive.div_ceil(cap);
        eprintln!(
            "[reorg] 每页字节容量 {cap} 条；满树 {leaves_full} 叶 / {rows_full} 行 ⇒ 删一半              {leaves} 叶 / {rows} 行（单页 {min}~{max} 条），理想页数 {ideal}（= rows/{cap}）"
        );
        assert!(
            leaves <= ideal + ideal / 2,
            "页数 {leaves} 超出理想 {ideal} 的 1.5× ⇒ 空位没被收掉，按页走的调用              （health / vacuum / 全量 scan）一直在双倍代价（单页 {min}~{max} 条）"
        );
    }

    /// 解析缓存**一次都不许服务旧内容**。
    ///
    /// 失效凭的是"按页写入计数"（`PageManager::write_count`），不是在每个写点手工清 ——
    /// 所以这条真正盯的是**哪条路径改了页内容却没 bump 计数**：覆盖写、分裂写回两片页、
    /// 合并后 free 再 allocate 复用同一个页号，都在这条里走一遍。
    #[test]
    fn test_node_cache_never_serves_stale_content() {
        let mut tree = BPlusTree::new(4, 16);
        // ① 边插边读：分裂会不断改写既有页
        for n in 0..200u8 {
            assert!(tree.insert(key(n), rid(n)), "插入 {n} 失败");
            assert_eq!(
                tree.get(key(n)).unwrap().0,
                Some(vec![n]),
                "写完立刻读第 {n} 个拿到了旧内容"
            );
        }
        // ② 覆盖写：同一个 key 换 RowId
        for n in 0..200u8 {
            let new = n.wrapping_add(1);
            assert!(tree.update(key(n), rid(new)), "update {n} 失败");
            assert_eq!(
                tree.get(key(n)).unwrap().0,
                Some(vec![new]),
                "覆盖写之后第 {n} 个仍是旧值 = 服务了旧节点"
            );
        }
        // ③ 删掉再插回：页被 free 后会被 allocate 复用，计数必须 bump
        for n in (0..200u8).step_by(3) {
            assert!(tree.delete(key(n)), "删除 {n} 失败");
            assert!(tree.get(key(n)).is_none(), "删掉 {n} 之后还读得到 = 旧节点还在被服务");
            assert!(tree.insert(key(n), rid(n)), "复用位置插回 {n} 失败");
            assert_eq!(
                tree.get(key(n)).unwrap().0,
                Some(vec![n]),
                "复用页之后读回的是上一个占用者的内容"
            );
        }
    }
}
