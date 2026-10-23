use std::vec;
use std::format;
use std::matches;
use std::{fmt, path::Path, write};

use serde::{Deserialize, Serialize};

use crate::bplus_tree::page::{
    MAX_PAYLOAD, PageId, PageManager, PHASE_BACKTRACK, PHASE_DESCENT, PHASE_OTHER,
    PHASE_RESTRUCTURE, PHASE_ROOT_CHECK, PHASE_SEP_REFRESH, set_page_phase,
};

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

// 测试专用：`node_cache` 为了腾地方**走过多少个条目**、走**几趟**（M31 的尺子）。
//
// 为什么要单独数：M22 把整表 `clear()` 换成 `retain(只留钉住的)`，那次改动是对的（不再白丢上层
// 页的解析结果），但它留在**每次插入**上 ⇒ 表装满之后每插一条都要走一遍整表（O(容量)），
// 而随机点读恰好一直把它装满。这笔钱与读盘无关，所以 hits/misses 两把尺子都看不见它。
#[cfg(test)]
thread_local! {
    static NODE_CACHE_SWEEPS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static NODE_CACHE_VISITED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn node_cache_sweeps() -> u64 {
    NODE_CACHE_SWEEPS.with(|c| c.get())
}

#[cfg(test)]
pub(crate) fn node_cache_visited() -> u64 {
    NODE_CACHE_VISITED.with(|c| c.get())
}

// 测试专用：下降**定位了几次**、这几次一共**比较了几把分隔键**（M34 的尺子）。
//
// 为什么要单独数：`child_index` 从线性 `position` 换成二分之后，省下的是"每层看多少把键"，
// 这件事在 µs 上会被本机跨构建漂移（1.3~1.9×）盖住，但它是个**纯计数**的事实 ——
// 顺序灌行时目标永远是最后一个子 ⇒ 线性扫必然走满整页键数，二分只走 log2(扇出)。
// 数法是把计数器放在真正的比较回调里，所以量的是"实际发生的比较"，不是公式估算。
#[cfg(test)]
thread_local! {
    static DESCENT_LOOKUPS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static DESCENT_KEY_COMPS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static DESCENT_MAX_COMPS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn descent_lookups() -> u64 {
    DESCENT_LOOKUPS.with(|c| c.get())
}

#[cfg(test)]
pub(crate) fn descent_key_comps() -> u64 {
    DESCENT_KEY_COMPS.with(|c| c.get())
}

#[cfg(test)]
pub(crate) fn descent_max_comps() -> u64 {
    DESCENT_MAX_COMPS.with(|c| c.get())
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
    /// 自动档的**上限**，初值 = [`AUTO_MAX_PAGES`]；M32 让它可改。
    ///
    /// 为什么要能改：上限是**预算**不是正确性，而"默认 2048 页 ≈ 8 MiB"这一档在 600k 行的库上
    /// 只剩 5.6% 覆盖率 ⇒ 每次点读都付一次未命中。调用方要么认这个预算、要么自己把顶抬起来
    /// （抬到 ≥ 总页数 = 把库复制进 RAM，价签在 README 里写着），但那个决定不该由库替他做。
    auto_max_pages: usize,
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
fn auto_cache_pages(total_pages: u64, max_pages: usize) -> usize {
    (total_pages as usize).clamp(AUTO_MIN_PAGES, max_pages.max(AUTO_MIN_PAGES))
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
            let cap = auto_cache_pages(page_manager.next_page_id(), AUTO_MAX_PAGES);
            page_manager.set_cache_capacity(cap);
        }

        Ok(Self {
            page_manager,
            root,
            order,
            node_cache: std::collections::HashMap::new(),
            auto_cache: auto,
            auto_max_pages: AUTO_MAX_PAGES,
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
            let cap = auto_cache_pages(self.page_manager.next_page_id(), self.auto_max_pages);
            self.page_manager.set_cache_capacity(cap);
        }
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
        if self.auto_cache {
            let cap = auto_cache_pages(self.page_manager.next_page_id(), self.auto_max_pages);
            self.page_manager.set_cache_capacity(cap);
        }
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

/// 字节账的常量：全进程量一次，之后每条只两次加法（M34）。
///
/// 为什么可以只量一次：bincode 用定宽整数编码，`Vec<u8>` = 「长度前缀 + 原样字节」、
/// `Option<T>` = 「判别 + 有值时再接一个 T」⇒ 一个节点的字节对「每条的键长、值长」是**线性**的，
/// 于是"逐条问 bincode"退化成"逐条做两次加法"。逐条问一次要做两次 `serialized_size`，
/// 而插一条要问整页 —— 这正是 M34 在写路径上量出来的那一刀。
/// 为什么常量还是探针量出来、不写死数字：编码宽度是 bincode 版本与配置的事，写错一次就是
/// "按错的尺寸写坏一页"。探针与真实序列化的逐档对拍由
/// `test_reorg_size_formula_matches_bincode` 钉住。
#[derive(Clone, Copy)]
struct SizeConsts {
    /// 只有 `next_leaf`（条目为空）的叶子
    leaf_base_none: i64,
    /// 同上但 `next_leaf` 是 `Some`（`Option` 的判别 + 定宽 `PageId`）
    leaf_base_some: i64,
    /// 空内部节点（无键无子）
    internal_base: i64,
    /// 一个子节点占的字节（定宽 `PageId` ⇒ 可乘）
    internal_child: i64,
    /// 一条叶子条目：空键 + `RowId(None)`
    entry_base: i64,
    /// `RowId` 从 `None` 变 `Some(空值)` 多出来的字节
    entry_some: i64,
    /// 一把空键占的字节（= `Vec<u8>` 的长度前缀）
    key_unit: i64,
}

impl SizeConsts {
    fn measure() -> SizeConsts {
        let size = |n: &BTreeNode| bincode::serialized_size(n).unwrap_or(u64::MAX) as i64;
        let leaf = |next_leaf: Option<PageId>| {
            size(&BTreeNode::Leaf {
                entries: Vec::new(),
                next_leaf,
            })
        };
        let empty_internal = size(&BTreeNode::Internal {
            keys: Vec::new(),
            children: Vec::new(),
        });
        let one_child = size(&BTreeNode::Internal {
            keys: Vec::new(),
            children: vec![PageId(1)],
        });
        let empty_entry = bincode::serialized_size(&(Vec::<u8>::new(), RowId(None)))
            .unwrap_or(u64::MAX) as i64;
        let null_entry = bincode::serialized_size(&(Vec::<u8>::new(), RowId(Some(Vec::new()))))
            .unwrap_or(u64::MAX) as i64;
        SizeConsts {
            leaf_base_none: leaf(None),
            leaf_base_some: leaf(Some(PageId(0))),
            internal_base: empty_internal,
            internal_child: one_child - empty_internal,
            entry_base: empty_entry,
            entry_some: null_entry - empty_entry,
            key_unit: bincode::serialized_size(&Vec::<u8>::new()).unwrap_or(u64::MAX) as i64,
        }
    }
}

fn size_consts() -> &'static SizeConsts {
    static CONSTS: std::sync::OnceLock<SizeConsts> = std::sync::OnceLock::new();
    CONSTS.get_or_init(SizeConsts::measure)
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
    ///
    /// M34 之后**写路径的下降也走它**（`insert_recursive`/`update_recursive`/`read_node`）：
    /// 以前写一侧是裸 `read_page` + 解析 ⇒ 每行把整条路径重解析一遍（实测 2.94 页读/行、
    /// 3 次整节点解析/行，而这把尺子当时根本数不到它们 —— 解析计数只挂在 `node_of` 上）。
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
        self.memoize_node(page_id, seen, node)
    }

    /// 把一份节点内容放进解析缓存，顺带执行两条纪律：上层页钉住、超上限丢没钉住的。
    ///
    /// `seen` 必须是**这次放进去的内容所对应的那次写**的 `write_count` —— `node_of` 拿它判陈旧。
    /// 两条来路都满足：`node_of` 读页之后、`insert` 写完页之后（见 [`Self::cache_written`]）。
    fn memoize_node(
        &mut self,
        page_id: PageId,
        seen: u64,
        node: std::sync::Arc<BTreeNode>,
    ) -> Option<std::sync::Arc<BTreeNode>> {
        // **上层页钉住**（M22）：根与内部节点是结构性小的一小撮（实测占全库 3.0%），而每一次
        // 点读、每一次反向游走都要用它们。不钉住时，全库走查把 LRU 转几圈就把它们挤出去 ——
        // 实测 200k 行的库里"走查后一次随机点读"因此从 45.2 µs 涨到 58.6 µs（+30%）。
        // 只钉 `Internal`：叶子占 97% 的页，钉叶子等于把整个库再复制一份进内存。
        if matches!(&*node, BTreeNode::Internal { .. }) {
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
        self.node_cache.insert(page_id, (seen, node.clone()));
        Some(node)
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
    fn cache_written(&mut self, page_id: PageId, node: BTreeNode) {
        let seen = self.page_manager.write_count(page_id);
        self.memoize_node(page_id, seen, std::sync::Arc::new(node));
    }

    /// 内部节点里该走哪个子：第一把**严格大于** `key` 的键的下标（键相等要走右边 —— 分隔键
    /// 本身归左半，`key == keys[i]` 时目标在 `i` 右边）。
    ///
    /// 为什么二分而不是 `position` 线性扫：顺序灌行时目标**永远是最后一个子**，线性扫每层
    /// 都要把整页分隔键走一遍 ⇒ 下降的代价从 O(log 扇出) 变成 O(扇出)。M34 量写路径单价时
    /// 这条是读、写两侧的公共开销（点读、`latest_version`、插入、删除、反向定位都走它）。
    fn child_index(keys: &[Key], key: &Key) -> usize {
        #[cfg(test)]
        let mut comps = 0u64;
        let one = |k: &Key| {
            #[cfg(test)]
            {
                comps += 1;
            }
            k.as_slice().cmp(key.as_slice())
        };
        let idx = match keys.binary_search_by(one) {
            Ok(i) => i + 1,
            Err(i) => i,
        };
        #[cfg(test)]
        {
            DESCENT_LOOKUPS.with(|c| c.set(c.get() + 1));
            DESCENT_KEY_COMPS.with(|c| c.set(c.get() + comps));
            DESCENT_MAX_COMPS.with(|m| {
                if comps > m.get() {
                    m.set(comps);
                }
            });
        }
        idx
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

    /// 一条叶子条目在序列化结果里占多少字节 —— 常量见 [`SizeConsts`]，O(1) 且**精确**。
    fn entry_bytes(item: &(Key, RowId)) -> i64 {
        let RowId(row) = &item.1;
        Self::entry_bytes_borrowed(&item.0, row)
    }

    /// 同上，但接**借用**：`row_fits` 是每条写都要问的一句，不能为量一次尺寸先把键与值各克隆
    /// 一份（那是事务路径上每行两次堆分配，只为了算一个数）。
    fn entry_bytes_borrowed(key: &[u8], row: &Option<Vec<u8>>) -> i64 {
        let c = size_consts();
        let mut n = c.entry_base + key.len() as i64;
        if let Some(v) = row {
            n += c.entry_some + v.len() as i64;
        }
        n
    }

    /// 一把键（内部节点的分隔键）占多少字节
    fn key_bytes(item: &Key) -> i64 {
        size_consts().key_unit + item.len() as i64
    }

    /// 只有 `next_leaf` 的叶子（条目为空）序列化后的字节数
    fn leaf_base(next_leaf: Option<PageId>) -> i64 {
        if next_leaf.is_some() {
            size_consts().leaf_base_some
        } else {
            size_consts().leaf_base_none
        }
    }

    /// 空内部节点（无键无子）的字节数，与「一个子节点占的字节」——子节点是定宽 `PageId`，
    /// 所以键数与子节点数可以分开加（同样由对拍测试钉住）
    fn internal_base() -> i64 {
        size_consts().internal_base
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
            + entries.iter().map(Self::entry_bytes).sum::<i64>()
    }

    /// 内部节点的精确字节数：键是变长的、子节点是定宽 `PageId` ⇒ 两项分开加
    fn internal_total_bytes(keys: &[Key], child_count: usize) -> i64 {
        Self::internal_base()
            + keys.iter().map(Self::key_bytes).sum::<i64>()
            + child_count as i64 * Self::internal_child_unit()
    }

    /// 前缀字节表：`pre[i]` = 前 i 条的字节和 ⇒ 任意半页的字节 O(1) 拿到
    fn prefix_bytes<T>(items: &[T], size: fn(&T) -> i64) -> Vec<i64> {
        let mut pre = Vec::with_capacity(items.len() + 1);
        pre.push(0i64);
        for it in items {
            pre.push(pre.last().unwrap() + size(it));
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
        let pre = Self::prefix_bytes(entries, Self::entry_bytes);
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
        let pre = Self::prefix_bytes(keys, Self::key_bytes);
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
        size_consts().internal_child
    }

    /// 这一行**住得住一页吗** —— 给上层在动 WAL 之前先问一句。
    ///
    /// 为什么要有这个谓词而不是等 `insert` 返回 `false`：事务层是「先 append WAL、再写树」，
    /// 树拒收之后 WAL 里就留下一条"提交了但这行不存在"的记录，恢复时按它重放只会再拒一次，
    /// 表现成"WAL 说有、树里查无此键"。所以拒绝必须发生在写 WAL **之前**。
    /// 判据与分裂用的是同一套字节账（`leaf_base` + `entry_bytes_borrowed`，与 `leaf_total_bytes`
    /// 逐项相同），不留两边不一致。
    ///
    /// 为什么不走 `leaf_total_bytes(&[(key.to_vec(), RowId(row.clone()))])`：那是每条写都要问
    /// 的一句，为了算一个数先把键与值各克隆一份 —— 事务路径上每行两次堆分配白花在量尺上。
    pub fn row_fits(&self, key: &[u8], row: &Option<Vec<u8>>) -> bool {
        Self::leaf_base(None) + Self::entry_bytes_borrowed(key, row) <= MAX_PAYLOAD as i64
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
        for (page_id, node) in step.writes {
            // 到这里的每个节点都逐档量过字节 ⇒ 写失败只剩「磁盘坏了」这一种解释
            let data = bincode::serialize(&node).expect("serialize node");
            if self.page_manager.write_page(page_id, &data) {
                // 写成功才回填：内容与盘上不一致时，缓存里的旧那份比没有那份更坏
                self.cache_written(page_id, node);
            }
        }
        if let Some((promote_key, new_page)) = step.promote {
            let new_root_page_id = self.page_manager.allocate_page();
            let new_root = BTreeNode::Internal {
                keys: vec![promote_key],
                children: vec![self.root, new_page],
            };
            let root_data = bincode::serialize(&new_root).expect("serialize new root");
            if self.page_manager.write_page(new_root_page_id, &root_data) {
                self.cache_written(new_root_page_id, new_root);
            }
            self.set_root(new_root_page_id);
        }
        true
    }

    /// 一趟下降：返回「还没落盘的一串（页号, 内容）」与要不要往上升一层。
    /// `landed = false` 时 `writes` 必为空，`alloc` 继续往上传，由 [`Self::insert`] 统一回收。
    fn insert_recursive(&mut self, node_page_id: PageId, key: Key, row_id: RowId) -> InsertStep {
        // 下降走解析缓存（M34），并且**只有真要改这一层时才克隆键/子数组**：
        // 顺序灌行时内部节点 40 次里只有 1 次真的变（下面分裂了），另外 39 次连一次堆分配都不必花。
        //
        // ⚠️ 这里必须是 `expect` 不能是"当失败处理"：`insert` 返回 `false` 的含义被上层读成
        // "这一行装不下"（`row_fits` 已经过了 ⇒ WAL 也已经写了）⇒ 页读不出来却回一句 false，
        // 盘上就留下一条"提交了但树里没有"的记录，正是 `row_fits` 注释里要避免的那个形状。
        let node = self.node_of(node_page_id).expect("下降取节点");
        let order = self.order;
        let cap = MAX_PAYLOAD as i64;

        match &*node {
            BTreeNode::Leaf {
                entries,
                next_leaf,
            } => {
                let next_leaf = *next_leaf;
                let mut entries = entries.clone();
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
            BTreeNode::Internal { keys, children } => {
                let child_idx = Self::child_index(keys, &key);
                let child_page_id = children[child_idx];
                let child = self.insert_recursive(child_page_id, key, row_id);
                if !child.landed {
                    return child; // 孩子的 writes 本来就空；alloc 继续往上传
                }
                let Some((promote_key, new_page)) = child.promote else {
                    return child; // 下面没分裂 ⇒ 孩子那条写入原样上交
                };
                // 走到这里才需要 owned 副本（这一层要插一把上提键、挂一个新子）
                let (mut keys, mut children) = (keys.clone(), children.clone());
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
                let child_idx = Self::child_index(keys, &key);
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
                let child_idx = Self::child_index(keys, &key);
                let child_page_id = children[child_idx];
                // 递归更新子节点
                self.update_recursive(child_page_id, key, new_row_id)
            }
        }
    }
}

// 测试专用尺子（M54b）：删除的回溯段里"为刷新分隔键而往左下走一趟"做了几次。
//
// 为什么钉这一格：M54 用台架数出来"每删一行读 16 页（小库、全缓存命中）/ 64 页（600k 档）"，
// 可修的那一半就是**无条件**去走这趟左下到底（`subtree_min_key` 逐页读 + 逐页反序列化）。
//
// 为什么是 thread_local：和 [`crate::bplus_tree::page`] 那几把尺子同一个理由 ——
// `cargo test` 并行跑，全局原子量会把别人家的删除算进本用例的分母。
#[cfg(test)]
thread_local! {
    static SEP_REFRESHES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn sep_refreshes() -> u64 {
    SEP_REFRESHES.with(|c| c.get())
}

#[cfg(test)]
pub(crate) fn sep_refreshes_clear() {
    SEP_REFRESHES.with(|c| c.set(0));
}

// 测试专用尺子（M56）：删除路径里**重组**那一段到底发生了多少次、走的哪一招。
//
// 为什么钉这一格：M54b 之后 20k 档"每删除行仍读 9.37 页"，而 `下降/删除行 = 2.04`（树高 2）
// ⇒ 一次下降约 4.6 页，多出来的页不在下降里。候选就是 `handle_underflow` 这条路：它为了查一次
// 下溢，会把**同一批页**（本页、父页、兄弟页）反复 `read_node` 好几遍，合并时还要从根再走一趟整树 DFS
// 去找祖父（那一条 M56b 已删，见 `页·找祖父` 桶）。是这些重复读还是别的，得先数出来才能决定下一刀切哪。
//
// 三个数各自的意思：
// - `underflow`：进到 `handle_underflow` 且**真的下溢**的次数（不是"调用次数"—— 每行删除都会问一句）
// - `borrow`：借位成功的次数
// - `merge`：整对重排并**真释放了一页**的次数（`PairReorg::Merged`）—— M56b 起它还是"要不要问根降层"的唯一触发条件
#[cfg(test)]
thread_local! {
    static UNDERFLOW_CALLS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static BORROWS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static MERGES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static ROOT_CHECKS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// `(真下溢次数, 借位, 合并)`
#[cfg(test)]
pub(crate) fn reorg_stats() -> (u64, u64, u64) {
    UNDERFLOW_CALLS.with(|c| {
        (
            c.get(),
            BORROWS.with(|b| b.get()),
            MERGES.with(|m| m.get()),
        )
    })
}

#[cfg(test)]
pub(crate) fn reorg_stats_clear() {
    UNDERFLOW_CALLS.with(|c| c.set(0));
    BORROWS.with(|c| c.set(0));
    MERGES.with(|c| c.set(0));
    ROOT_CHECKS.with(|c| c.set(0));
}

/// 每行删除问了几次"根要不要降层"（每次都读一遍根页）
#[cfg(test)]
pub(crate) fn root_checks() -> u64 {
    ROOT_CHECKS.with(|c| c.get())
}

#[cfg(test)]
macro_rules! note_underflow {
    () => {
        UNDERFLOW_CALLS.with(|c| c.set(c.get() + 1))
    };
}
#[cfg(test)]
macro_rules! note_borrow {
    () => {
        BORROWS.with(|c| c.set(c.get() + 1))
    };
}
#[cfg(test)]
macro_rules! note_merge {
    () => {
        MERGES.with(|c| c.set(c.get() + 1))
    };
}
#[cfg(test)]
macro_rules! note_root_check {
    () => {
        ROOT_CHECKS.with(|c| c.set(c.get() + 1))
    };
}

// 测试专用尺子（M61 的**先量**）：删除路径**落到的叶子游程数**，也就是"每行都重下一趟全树"这件事的理论上限。
//
// 为什么钉这一格才动代码：M56b 之后 200k 档每删除行仍读 下降 4.00 + 回溯 3.00 + 重组 2.43 页，
// 而"合批同叶多行"这一刀的收益**完全取决于一次删除之后,下一行还在不在同一片叶子里** ——
// 比值 1（每行一片新叶子）⇒ 这一刀一分钱买不到，比值 k ⇒ 下降次数能降到 1/k。
// 这个 k 不读就只能猜，而 M56 那一轮已经证明"猜哪一段花钱"会把因果读反。
//
// 为什么数**游程**而不是"不同叶子数"：删除按升序走 ⇒ 同一叶子的连续行必然连着来（键序就是叶子序），
// 所以游程数 == 不同叶子数，而游程只要留"上一页号"一个格子、不用维护集合。
// ⚠️ 乱序删除时这个等式不成立（游程会大于不同叶子数）—— 那一支不是合批的现场，读数里会照实报。
#[cfg(test)]
thread_local! {
    static DELETE_LEAF_LAST: std::cell::Cell<Option<PageId>> = const { std::cell::Cell::new(None) };
    static DELETE_LEAF_RUNS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static DELETE_ROWS_SEEN: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn note_delete_leaf(leaf: PageId) {
    DELETE_ROWS_SEEN.with(|r| r.set(r.get() + 1));
    DELETE_LEAF_LAST.with(|last| {
        if last.get() != Some(leaf) {
            last.set(Some(leaf));
            DELETE_LEAF_RUNS.with(|c| c.set(c.get() + 1));
        }
    });
}

/// `(累计删除行数, 累计叶子游程数)` ⇒ 两者之比 = 合批后"每几行共用一趟下降"的上限。
/// ⚠️ 读数用**差值**（台架与守卫都按 `p1 − p0` 取，跟 `reorg_stats` 同一口径，不就地清零）。
#[cfg(test)]
pub(crate) fn delete_leaf_runs() -> (u64, u64) {
    (
        DELETE_ROWS_SEEN.with(|c| c.get()),
        DELETE_LEAF_RUNS.with(|c| c.get()),
    )
}

/// 删除的返回值（M54b / M56b）。
///
/// - `min_changed`：**本次删除可能改变了该子树的最小键** —— 分隔键就是子树最小键，所以父节点只有在这一格
///   为真时才需要把最小键重新找一遍（那趟要一路向左下走到叶子、逐页反序列化）。判据：删掉的键若不是本子树
///   最左那片叶子的最小键，子树下界不可能变；而**借位/合并**会挪下界，所以只要做过重组就一律报真
///   （保守方向是"多做一次刷新"，不是"漏做"）。
/// - `merged`（M56b）：**这一趟删除里真释放过一页**。父节点少了一个孩子 ⇒ 上层只需把这个事实报给
///   `delete()` 收尾用一次（根要不要降层）。改前 `delete()` 与回溯段每行删除各问一次"根要不要降层"，
///   每次都要把根页读一遍并重解析 ⇒ 200k 档固定 2.00 页/删除行；现在只有合并过的那几行问一次。
///
/// M61b 起它多带一格 `removed`，并且**不再有 `NotFound` 变体**：批量删除一段键里可能只有几把在树里，
/// "整段什么都没动"用 `removed == 0 && !min_changed && !merged` 表达（[`Del::NOOP`]），
/// 上层看到这格就跳过回溯 —— 与改前 `Del::NotFound` 一路直接返回**花的页读完全相同**（守卫钉着）。
#[derive(Clone, Copy)]
struct Del {
    /// 这一次（批）里**真删掉的行数**。0 且另两格也假 ⇒ 整条路径什么都没动。
    removed: usize,
    min_changed: bool,
    merged: bool,
}

impl Del {
    const NOOP: Del = Del {
        removed: 0,
        min_changed: false,
        merged: false,
    };

    /// 什么都没动 ⇒ 祖先各层都不必回溯、也不必问根降层
    fn is_noop(&self) -> bool {
        self.removed == 0 && !self.min_changed && !self.merged
    }
}

impl BPlusTree {
    /// 删除：移除指定 key，必要时借键/合并并处理根降层
    pub fn delete(&mut self, key: Key) -> bool {
        let (res, _) = self.delete_recursive(self.root, std::slice::from_ref(&key), None);

        // 根是内部节点且只剩一个子节点时降层 —— **只在本次删除真合并过一页时才问**（M56b）。
        // 没合并过 ⇒ 根的孩子数不可能变 ⇒ 那一次根页读与解析纯属白付。
        if res.merged {
            set_page_phase(PHASE_RESTRUCTURE);
            self.shrink_root_if_needed();
        }
        // M56：一趟删除结束把阶段清空 ⇒ 下一趟的桶不会串到这一趟上
        set_page_phase(PHASE_OTHER);
        res.removed > 0
    }

    /// 批量删除（M61b）：`keys` **必须按升序给出**，同一叶子里的连续多行共用一趟下降。
    ///
    /// 为什么只在 vacuum 那条路上用：M61 读到的上限是 **k = 16.5~17.9 行/叶子**（可删行是"被更新过的
    /// 那段连续键"留下的旧版本，天然成块），而日常随机删除 k = 1 ⇒ 一行都省不到。
    /// 所以这一刀的收益**只在成块删除的形状里**，单键 [`Self::delete`] 走的是同一条递归
    /// （`run` 只有一个元素）⇒ 行为与页读账与改前一字不差。
    ///
    /// 返回真删掉的行数；不存在的键不计、也不报错（与 `delete` 返回 `false` 同口径）。
    /// ⚠️ 乱序输入不会损坏数据（每片叶子里都按序二分），但会退化成"每行一趟下降"—— 白跑一趟树，
    ///   所以顺序是调用方的义务；这里不 assert（release 里 assert 也要钱），由守卫 `delete_leaf_runs` 的 k 读数盯住。
    pub fn delete_batch(&mut self, keys: &[Key]) -> usize {
        let mut i = 0usize;
        let mut removed = 0usize;
        while i < keys.len() {
            let (res, used) = self.delete_recursive(self.root, &keys[i..], None);
            removed += res.removed;
            // 合并过 ⇒ 根的孩子数可能变：**每片叶子问一次**，不是每行问一次
            if res.merged {
                set_page_phase(PHASE_RESTRUCTURE);
                self.shrink_root_if_needed();
            }
            // 递归承诺 `used ≥ 1`（哪怕整段都不在树里也会推进），所以这里不可能死循环
            i += used.max(1);
        }
        set_page_phase(PHASE_OTHER);
        removed
    }

    /// 最小填充数（⌈order/2⌉ - 1）；order=4 → min=1
    fn min_entries(&self) -> usize {
        (self.order + 1) / 2 - 1
    }

    fn read_node(&mut self, page_id: PageId) -> BTreeNode {
        // M34 试过把这里改成走 `node_of`（解析缓存），**复测把它退回来了**：600k 档
        // `vacuum()` 全库 12.27 s → **15.12 s**，而同一天没被碰过的 bitcask 那条从 5.79 s 变成
        // 4.52 s（噪声朝反方向）⇒ 比值 2.12× → 3.34× 是这一处带来的。
        // 为什么删除/重组那条路和插入不同：它是**走查** —— 每页基本只碰一次，回填解析缓存换不到
        // 下一次命中，只多付了 `Arc` 分配、HashMap 插入、`pin` 检查与装满之后的整表 `retain`
        // （和 [`crate::bplus_tree::page::PageManager::read_page_for_walk`] 当年"装得下才回填"
        // 是同一个理由）。插入那条路的页会被连续几百行反复用到，所以它值得（见 `insert_recursive`）。
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
    // M56b：**这里原本有一个 `find_parent_of` / `find_parent_of_rec`** —— 从根开始、把整棵树
    // 逐页读一遍去找"某个节点的父节点"。200k 档实测它每次合并都要扫 2820 页、占每删除行页读的 77%，
    // 而合并只让"回溯栈上的下一帧"少一个孩子 ⇒ 那一帧自己就会查下溢，`delete()` 收尾管根降层。
    // 删掉它 ⇒ `页·找祖父` 那一只桶从此恒为 0（台架与守卫都对着这一格）。

    fn shrink_root_if_needed(&mut self) {
        // 单独一只桶（M56）：这一句问的是"根要不要降层"—— 每行删除都会被叫到，而它每次都把根页
        // 读一遍。这与"真的在做重组"不是一回事，所以单独记账、单独归段。
        #[cfg(test)]
        note_root_check!();
        set_page_phase(PHASE_ROOT_CHECK);
        match self.read_node(self.root) {
            BTreeNode::Internal { children, .. } if children.len() == 1 => {
                let old_root = self.root;
                self.set_root(children[0]);
                self.page_manager.free_page(old_root);
            }
            _ => {}
        }
        set_page_phase(PHASE_BACKTRACK);
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
        // 每条的字节由探针常量导出（`RowId` 是 `Option<Vec<u8>>` ⇒ 变长，手算硬编码错一次就是
        // 写坏页，所以常量本身是量出来的，并由逐档对拍钉住）
        let cap = MAX_PAYLOAD as i64;
        // 左半页的下一叶就是右半页，右半页接原本右页的下一叶 ⇒ 两个 base 分开算
        let base_left = Self::leaf_base(Some(right_page));
        let base_right = Self::leaf_base(tail);
        // 前缀和：`prefix[i]` = 前 i 条的字节和 ⇒ 任意半页的字节都是 O(1) 拿到
        let prefix = Self::prefix_bytes(&union, Self::entry_bytes);
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
        let n = union_keys.len();
        let pre = Self::prefix_bytes(&union_keys, Self::key_bytes);
        // 左半拿 keys[..i] + children[..i+1]，右半拿 keys[i+1..] + children[i+1..]
        // （keys[i] 本身提回父当分隔键 ⇒ 不进任何一半）
        let left_bytes = |i: usize| -> i64 { base + pre[i] + (i as i64 + 1) * cu };
        let right_bytes = |i: usize| -> i64 {
            base + (pre[n] - pre[i + 1]) + (n as i64 - i as i64) * cu
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

    /// 处理下溢：能借则借，否则整对重排（合并或按字节重切）。
    ///
    /// 返回**是否真释放了一页**（`PairReorg::Merged`）。M56b 起这件事只往上"报告"，不再自己往上处理：
    /// - 改前它合并完会 `find_parent_of(parent)` —— **从根走一趟整棵树的 DFS** —— 再对父节点做下溢处理。
    ///   200k 档实测这一格占**每删除行 39.69 页（77%）**，每趟 DFS 扫 2820 页，而该档的缺页(38.76)就是它扫出来的；
    /// - 而那是冗余的：合并只让"**这一层的父节点**"少一个孩子，而那个父节点正是 `delete_recursive`
    ///   回溯栈上的下一帧 —— 它紧接着就做自己那一层的下溢检查（`is_underflow(&cur)`），
    ///   一路到根；根降层由 `delete()` 收尾。⇒ DFS 归零，`真下溢/借位/合并` 三个计数一字不差。
    fn handle_underflow(&mut self, node_page_id: PageId, parent_page_id: PageId) -> bool {
        if node_page_id == self.root {
            return false;
        }

        let node = self.read_node(node_page_id);
        if !self.is_underflow(&node) {
            return false;
        }
        // 走到这里才是"真的要做一次重组"（M56 的数：每行删除都会问一句下溢，但只有这里动手）
        #[cfg(test)]
        note_underflow!();

        let Some(child_idx) = self.get_parent_index(parent_page_id, node_page_id) else {
            return false;
        };
        let (sibling_opt, is_left_sibling) = self.get_sibling(parent_page_id, child_idx);
        let Some(sibling_page_id) = sibling_opt else {
            return false;
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
                #[cfg(test)]
                note_borrow!();
                return false;
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
        let merged = reorg == PairReorg::Merged;
        #[cfg(test)]
        if merged {
            note_merge!();
        }
        // 合并过 ⇒ 父节点少了一个孩子：往上**报告**，由父节点自己那一帧去查下溢、由 `delete()` 收尾降根
        merged
    }

    /// 递归删除**一段升序键**；`parent_page_id` 在下降时传入，避免错误的父节点查找。
    ///
    /// 返回 `(Del, 这一段里被本层处理掉的键数)`：
    /// - M54b 起 `Del.min_changed` 让父节点**只在这格为真时**才去刷新分隔键。改前是无条件刷新 ——
    ///   那件事是 `subtree_min_key(child)`：**从该子树一路向左下走到叶子**，逐页读、逐页
    ///   `bincode::deserialize`（这一支不走解析缓存），而且**每一层都走一遍**。600k 档实测
    ///   "每删一行读 64.5 页"里可修的那一半就是它（台架 `bplus_vacuum_cost_attribution`）。
    /// - M56b 起 `Del.merged` 是"要不要问根降层"的唯一触发条件。
    /// - M61b 起入参从"一把键"变成"一段升序键"：**同一片叶子里的连续多行共用一趟下降**，
    ///   返回值多带 `removed` 与该层实际处理的键数（用于上层继续推进 / 提前收尾）。
    ///
    /// ⚠️ 两条不变量（守卫 `batched_deletes_really_restructure_and_stay_correct` 对着）：
    /// ① 返回的 `used ≥ 1` —— 一段里第一把键一定会被这一层处理掉（哪怕它不在树里），否则调用方死循环；
    /// ② 本层一旦做过借位/合并（结构变了），**剩下的键交回调用方从根重下一趟**，绝不拿旧的 `children` 快照继续路由。
    fn delete_recursive(
        &mut self,
        node_page_id: PageId,
        run: &[Key],
        parent_page_id: Option<PageId>,
    ) -> (Del, usize) {
        // M56 的段标注：下降那一次读记到"下降"，返回之后本层的所有读记到"回溯"
        set_page_phase(PHASE_DESCENT);
        let mut node = self.read_node(node_page_id);

        match &mut node {
            BTreeNode::Leaf { entries, .. } => {
                // 父层已经按分隔键把这一段切到"只可能在这片叶子里" ⇒ 这里一定把整段吃掉，used = run.len()
                let mut removed = 0usize;
                let mut touched_min = false;
                for key in run {
                    if let Ok(idx) = entries.binary_search_by(|(k, _)| k.cmp(key)) {
                        // 删的是本片最小键 ⇒ 本片所属子树的下界可能变了；删别的键一定没变
                        touched_min |= idx == 0;
                        entries.remove(idx);
                        removed += 1;
                        #[cfg(test)]
                        note_delete_leaf(node_page_id);
                    }
                }
                if removed == 0 {
                    return (Del::NOOP, run.len());
                }
                let mut min_changed = touched_min;
                self.write_node(node_page_id, &node);

                // 非根叶子下溢 → 再平衡（**一片叶子一次**，不是每行一次）
                let mut merged = false;
                if node_page_id != self.root {
                    if let Some(parent) = parent_page_id {
                        if self.is_underflow(&node) {
                            set_page_phase(PHASE_RESTRUCTURE);
                            merged = self.handle_underflow(node_page_id, parent);
                            set_page_phase(PHASE_BACKTRACK);
                            // 借位/合并会挪分隔键与本层下界 ⇒ 一律上报"可能变了"，让上层去核对
                            min_changed = true;
                        }
                    }
                }
                (
                    Del {
                        removed,
                        min_changed,
                        merged,
                    },
                    run.len(),
                )
            }
            BTreeNode::Internal { keys, children } => {
                // ⚠️ 路由用的分隔键/孩子表**每轮都刷新**：上一个孩子可能已经把自己合并进兄弟
                // （本层的孩子表就少了一格，`chs` 里那一页也已被释放）—— 拿旧表继续路由就会去读
                // 已释放的页。第一次用下降那一趟读到的内容（不再多读一次），之后每轮从
                // "回溯那一趟读"的结果里拿 ⇒ 每层每孩子的页读次数与改前"每行一趟"同量级，
                // 省下来的还是**下降**那一段（一趟下降带一整片叶子）。
                let mut snap = (keys.clone(), children.clone());
                let mut i = 0usize;
                let mut removed = 0usize;
                let mut merged = false;
                let mut min_changed_left = false;
                while i < run.len() {
                    let (ks, chs) = &snap;
                    let child_idx = Self::child_index(ks, &run[i]);
                    // 孩子 `c` 的键区间是 [keys[c-1], keys[c])；最后一个孩子无上界
                    let hi = ks.get(child_idx).cloned();
                    let mut j = i + 1;
                    while j < run.len() && hi.as_ref().map_or(true, |h| run[j].as_slice() < h.as_slice()) {
                        j += 1;
                    }
                    let child_page_id = chs[child_idx];
                    let (res, used) =
                        self.delete_recursive(child_page_id, &run[i..j], Some(node_page_id));
                    // 递归承诺 `used ≥ 1` ⇒ 一定推进，不会死循环
                    i += used.max(1).min(j - i);
                    if res.is_noop() {
                        // 那一片子里一把键都不在（真空区间）⇒ 本层什么都没变，继续下一个孩子
                        continue;
                    }
                    removed += res.removed;
                    merged |= res.merged;
                    min_changed_left |= res.min_changed && child_idx == 0;

                    set_page_phase(PHASE_BACKTRACK);
                    // 回溯段一次读就够（M54b：改前是"先 `read_page` 问在不在、再 `read_node` 读一遍"，
                    // 下溢检查那里**又**两遍 ⇒ 每层 4 次页读、3 次重复）。页已被合掉而读不到 ⇒ 保守上报。
                    let Some(mut cur) = self.try_read_node(node_page_id) else {
                        return (
                            Del {
                                removed,
                                min_changed: true,
                                merged: true,
                            },
                            i,
                        );
                    };

                    // 只有"该子树的最小键可能变了"才去把它重新找出来写进分隔键
                    if res.min_changed && child_idx > 0 {
                        #[cfg(test)]
                        SEP_REFRESHES.with(|c| c.set(c.get() + 1));
                        // 这一趟要一路向左下走到叶子 ⇒ 单独一个桶（M56），别混进"回溯"里
                        set_page_phase(PHASE_SEP_REFRESH);
                        let min_key = self.subtree_min_key(child_page_id);
                        set_page_phase(PHASE_BACKTRACK);
                        if let Some(min_key) = min_key {
                            if let BTreeNode::Internal {
                                keys: ks2,
                                children: ch2,
                            } = &mut cur
                            {
                                // child 可能已变下标（兄弟间挪过），重新定位
                                if let Some(new_idx) = ch2.iter().position(|&c| c == child_page_id) {
                                    if new_idx > 0 && new_idx - 1 < ks2.len() && ks2[new_idx - 1] != min_key
                                    {
                                        ks2[new_idx - 1] = min_key;
                                        self.write_node(node_page_id, &cur);
                                    }
                                }
                            }
                        }
                    }

                    // 检查本节点是否下溢（合并子节点后）
                    let mut restructured = false;
                    if node_page_id != self.root {
                        if let Some(parent) = parent_page_id {
                            if self.is_underflow(&cur) {
                                set_page_phase(PHASE_RESTRUCTURE);
                                merged |= self.handle_underflow(node_page_id, parent);
                                set_page_phase(PHASE_BACKTRACK);
                                restructured = true;
                            }
                        }
                    }
                    if restructured {
                        // 本层被借/合并过 ⇒ 连"这一层是谁"都可能不再是手里这一页（父层的孩子表也变了）
                        // ⇒ 剩下的键交回上层从根重下一趟，绝不拿旧快照继续路由。
                        return (
                            Del {
                                removed,
                                min_changed: true,
                                merged,
                            },
                            i,
                        );
                    }
                    // 用回溯那一趟读到的新鲜内容继续路由下一个孩子（不再多读一次页）
                    let BTreeNode::Internal {
                        keys: ks3,
                        children: ch3,
                    } = &cur
                    else {
                        // 本层变成了叶子 ⇒ 结构变了，同上：交回上层重下
                        return (
                            Del {
                                removed,
                                min_changed: true,
                                merged: true,
                            },
                            i,
                        );
                    };
                    snap = (ks3.clone(), ch3.clone());
                    // 本层是根时**不在这里**问"要不要降层"（M56b）：`delete()`/`delete_batch()` 收尾按 `merged` 问一次。
                }
                // 往上只报"本子树的下界可能变了"：动的是**最左那棵子树**、或本节点自己借/合并过。
                (
                    Del {
                        removed,
                        min_changed: min_changed_left,
                        merged,
                    },
                    i,
                )
            }
        }
    }

    /// `read_node` 的"页可能已经被合并掉"版本（M54b：把回溯段那两遍"先问在不在、再读一遍"合成一次）
    fn try_read_node(&mut self, page_id: PageId) -> Option<BTreeNode> {
        let data = self.page_manager.read_page(page_id)?;
        bincode::deserialize(&data).ok()
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
                let child_idx = Self::child_index(keys, &key);
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
    /// M34 把"逐条问 bincode"换成"逐条两次加法 + 一组探针常量" ⇒ 对拍必须额外覆盖
    /// **同一页里键长不一、`RowId` 有 `None` 有 `Some`** 的形状 —— 线性假设只在这些地方会破。
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
                    + items.iter().map(BPlusTree::entry_bytes).sum::<i64>();
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
                + keys.iter().map(BPlusTree::key_bytes).sum::<i64>()
                + (keys.len() as i64 + 1) * BPlusTree::internal_child_unit();
            assert_eq!(formula, real, "内部节点字节公式不等于真实序列化（pad={pad}）");
        }

        // 混合形状：键长每档不同 + 每三条有一条 `RowId(None)`
        let mixed: Vec<(Key, RowId)> = (0..60)
            .map(|i| {
                (
                    format!("{:04}{}", i, "@".repeat(i % 17)).into_bytes(),
                    if i % 3 == 0 {
                        RowId(None)
                    } else {
                        RowId(Some(vec![b'v'; i % 29]))
                    },
                )
            })
            .collect();
        for next_leaf in [None, Some(PageId(3))] {
            let node = BTreeNode::Leaf {
                entries: mixed.clone(),
                next_leaf,
            };
            let real = bincode::serialize(&node).unwrap().len() as i64;
            let formula = BPlusTree::leaf_base(next_leaf)
                + mixed.iter().map(BPlusTree::entry_bytes).sum::<i64>();
            assert_eq!(
                formula, real,
                "混合键长 + 空 RowId 的叶子公式与真实序列化不等"
            );
        }
        // 逐条单独对拍：常量的线性假设若错，这里就是第一处红（不必等到整页级）
        let vec_base = bincode::serialized_size(&Vec::<(Key, RowId)>::new()).unwrap() as i64;
        for e in &mixed {
            let probe = bincode::serialized_size(&[e][..]).unwrap() as i64 - vec_base;
            assert_eq!(
                BPlusTree::entry_bytes(e),
                probe,
                "单条字节与探针不等（键 {} 字节）",
                e.0.len()
            );
        }
    }

    /// 顺序灌行时**下降一趟都不许摸盘**：写完的页必须直接回填解析缓存（M34）。
    ///
    /// 判据：灌 N 行期间 `page_reads()` 平均 < 0.1 次/行。这把尺子在两种回归下都会响：
    /// ① 下降退回裸 `read_page` + 解析 ⇒ 实测 **2.94 次/行**（每层一次，M34 修前的读数）；
    /// ② 走了缓存但**不回填** ⇒ 每行 ≥ 1 次（上一条自己刚写脏的那一页，下一条马上要用）。
    /// 为什么不用"解析次数"当判据：退回 ① 的时候解析发生在 `node_of` 之外，那把尺子读到的是
    /// **0** —— 一个"看起来更省"的空转读数，抓不到回归（M34 第一版就是被这个 0 骗了一次）。
    ///
    /// 变异验证：删掉 `insert` 写回循环里那句 `cache_written` ⇒ 每行 1.00 次，红。
    #[test]
    fn test_sequential_insert_descends_without_touching_pages() {
        const N: usize = 8000;
        let mut tree = BPlusTree::new(64, 4096);
        let k = |i: usize| -> Key {
            format!("{i:08}#/worldquant/datafields/x.json").into_bytes()
        };
        let r0 = crate::bplus_tree::page::page_reads();
        for i in 0..N {
            assert!(
                tree.insert(k(i), RowId(Some(vec![b'v'; 47]))),
                "插入第 {i} 行失败"
            );
        }
        let reads = crate::bplus_tree::page::page_reads() - r0;
        assert!(
            reads * 10 <= N as u64,
            "顺序灌行平均每行摸 {:.3} 次页 ⇒ 下降还在从页缓存之外取节点，或者写完的页没回填解析缓存\
             （N={N}，页读={reads}；修前实测 2.94 次/行）",
            reads as f64 / N as f64
        );
        // 上面那条只证明"没摸盘"，还得证明"数据真在里面"——缓存回填错了也会一个字节都不读。
        assert_eq!(tree.iter().count(), N, "行数一条不许少");
        for i in (0..N).step_by(317) {
            assert!(tree.get(k(i)).is_some(), "第 {i} 行读不回来");
        }
    }

    /// 定位一个子节点**不许线性扫整页分隔键**（M34 的第二刀）。
    ///
    /// 判据取**单次定位最多比较几把键**，界 = log2(order) + 2（`order=64` ⇒ 8；二分最坏 6）。
    /// 为什么不用平均值：平均值被树形摊薄 —— 第一版用"平均 ≤ 2·log2"时，退回线性扫只把平均
    /// 推到 **11.0**（界 12），变异抓不住；而同一次变异下"最坏那一次"是**整页键数**（实测 40+）。
    /// 为什么这条值得钉成断言：顺序灌行时目标**永远是最后一个子**，线性扫每层都要走满整页键数，
    /// 而这笔钱在 µs 上会被本机的跨构建漂移（1.3~1.9×）整个盖住 —— 只有计数看得见。
    ///
    /// 变异验证：把 `child_index` 换回线性扫 ⇒ 最坏一次涨到整页键数，红。
    #[test]
    fn test_child_location_is_binary_not_linear() {
        const N: usize = 6000;
        let mut tree = BPlusTree::new(64, 4096);
        let k = |i: usize| -> Key {
            format!("{i:08}#/worldquant/datafields/x.json").into_bytes()
        };
        for i in 0..N {
            assert!(
                tree.insert(k(i), RowId(Some(vec![b'v'; 47]))),
                "插入第 {i} 行失败"
            );
        }
        // 升序读：这是线性扫的**最坏**情况（每次都扫到页尾），也是灌行的实际形状
        let l0 = descent_lookups();
        let c0 = descent_key_comps();
        for i in 0..N {
            assert!(tree.get(k(i)).is_some(), "第 {i} 行读不回来");
        }
        let lookups = descent_lookups() - l0;
        let comps = descent_key_comps() - c0;
        assert!(lookups > 0, "这把尺子没数到定位次数 ⇒ 空转");
        let per = comps as f64 / lookups as f64;
        let worst = descent_max_comps();
        let bound = (64f64).log2() as u64 + 2;
        eprintln!(
            "[descent] 定位 {lookups} 次 / 平均 {per:.1} 把 / 最坏一次 {worst} 把（界 {bound}，order=64）"
        );
        assert!(
            worst >= 4,
            "最坏一次只比较 {worst} 把 ⇒ 这棵树的内部节点窄到没东西可量，这条判据在空转"
        );
        assert!(
            worst <= bound,
            "单次定位最多比较 {worst} 把分隔键（界 {bound}）⇒ 下降退回线性扫"
        );
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

    /// 守卫（M54b）：删除回溯段只为"这棵子树的下界**可能**动了"去走那趟左下到底，
    /// 而且每一层只读**一次**本页。
    ///
    /// 四臂各红在哪（界都是从这两臂的实测数中间定的）：
    /// - 改后的形状：0.163 趟刷新/每次删除、9.33 页/每次删除 ⇒ 绿；
    /// - μA（退回改前的"无条件刷新"）：**2.372** 趟、**13.58** 页 ⇒ 两条界一起红（这一格随树高涨，
    ///   600k 档付的是磁盘）；
    /// - μC（把"先 `read_page` 问一句还在不在、再 `read_node` 读一遍"的两遍读抄回来）：页读 **15.33** ⇒ 红；
    /// - μB（**从不**刷新）：刷新数 0 ⇒ 只有空转哨兵红。**正确性扫抓不到它** —— 实测过：900 把键照样
    ///   全读得回。μE（借位/合并**不**向父层上报）**同样绿，而且两把尺子与改后一字不差（21 趟 / 9.33 页）
    ///   ⇒ 这个形状根本没触发过重组（order=12、只删 129/900 把键，叶子没下溢），**"重组也上报"这一半
    ///   还没被任何形状检验过** —— 下一轮要先造一个真会合并的形状，别把这一半读成"已经守住"。
    ///   这一格的"保守方向"只有**代价**证据、没有可达性证据：分隔键停在已被删掉的键上仍是合法分界
    ///   （左子树全 < 它 < 右子树剩下的键）。⇒ 判据只能挂在代价尺子上，不许把它读成"刷新是可达性守卫"。
    ///   扫自己不是装饰：μF（刷新时写一个越过右子树多数键的分隔键）在它这里红 —— 键走错子树，
    ///   连"删掉第 49 个键"都做不成。
    #[test]
    fn separator_refresh_only_happens_when_the_subtree_minimum_moves() {
        // order=12：叶子满约 11 条 ⇒ 900 键有 ~8 成在 3~4 层树上（两层的树每行最多刷 1 次，
        // 那时无条件/有条件两臂的差被树高吃掉了，界就量不出东西）
        const N: usize = 900;
        const BOUND: f64 = 1.0;
        const READ_BOUND: f64 = 12.0;
        fn k(n: usize) -> Key {
            format!("{n:08}").into_bytes()
        }
        fn r(n: usize) -> RowId {
            RowId(Some((n as u32).to_le_bytes().to_vec()))
        }

        let mut tree = BPlusTree::new(12, 4096);
        for i in 0..N {
            assert!(tree.insert(k(i), r(i)), "插入第 {i} 个键失败");
        }
        // 先把页请进缓存，让下面量的是"结构上的重复刷新/重复读"而不是冷缓存
        for i in 0..N {
            assert!(tree.get(k(i)).is_some(), "第 {i} 个键写完读不回来");
        }

        sep_refreshes_clear();
        let reads0 = crate::bplus_tree::page::page_reads();
        let mut dels = 0usize;
        for i in (0..N).step_by(7) {
            assert!(tree.delete(k(i)), "删除第 {i} 个键失败");
            dels += 1;
        }
        let refreshes = sep_refreshes() as usize;
        let reads = crate::bplus_tree::page::page_reads() - reads0;
        let per_del = refreshes as f64 / dels as f64;
        let per_del_reads = reads as f64 / dels as f64;
        eprintln!(
            "[sep_refresh] {dels} 次删除 → {refreshes} 趟左下到底（{per_del:.3} 趟/次，界 {BOUND}）\
             · 页读 {reads}（{per_del_reads:.2} 页/次，界 {READ_BOUND}）"
        );

        // 尺子不能空转：删的是最左键/触发过合并的那些必须刷到
        assert!(
            refreshes > 0,
            "一趟分隔键刷新都没有 ⇒ 这条尺子在空转，界再低也守不住东西"
        );
        assert!(
            per_del <= BOUND,
            "每次删除走 {per_del:.3} 趟左下到底（界 {BOUND}）⇒ 回溯段又变成无条件刷新了：\
             删中间叶子的非最小键不可能改变子树下界"
        );
        assert!(
            per_del_reads <= READ_BOUND,
            "每次删除读 {per_del_reads:.2} 页（界 {READ_BOUND}）⇒ 回溯段又付起\"先问在不在、再读一遍\"的两遍读"
        );

        // 正确性扫：幸存键全部读得回来，删掉的键全部读不到（"从不刷新"在这里红）
        for i in 0..N {
            if i % 7 == 0 {
                assert!(tree.get(k(i)).is_none(), "第 {i} 个键已删，还能读到");
            } else {
                assert!(tree.get(k(i)).is_some(), "第 {i} 个幸存键读不回来 ⇒ 分隔键错了");
            }
        }
    }

    /// 守卫（M56）：**成块删**的形状 —— 上一轮那条守卫用的形状根本没触发过重组（μE 与改后一字不差）。
    ///
    /// 四件事钉在一起：
    /// - **证人先成立**：`借位 + 合并 > 0`（实测 借位 122 / 合并 116）。不然"重组也向父层上报"那一半
    ///   仍然没被任何形状检验过；
    /// - **`页·找祖父` 必须为 0**（M56b）：那趟"每次合并从根走一趟整棵树 DFS"在 200k 档占**每删除行
    ///   39.69 页（77%）**、每趟扫 2820 页，而该档 `缺页 38.76` 就是它造成的 ⇒ 谁把它抄回来这一格就不为 0
    ///   （μG 实测：抄回来 ⇒ 本断言红，页读 10.63 → 15.85 页/删除行）；
    /// - **`问根` 次数 ≤ 合并次数 + 每次删除 1/8**（M56b）：根的孩子数只在真合并过一页时才会变。
    ///   改前每行删除无条件问两次（`delete()` 与回溯段的根分支各一次，每次读+解析一遍根页）
    ///   ⇒ 固定 2.00 页/删除行；μI 把无条件问抄回来 ⇒ 本断言红（600 次 > 界 191）；
    /// - **量具自检**：四个阶段桶之和 == `page_reads()` 的增量 ⇒ 标注漏了一扇门就会当场读出来，
    ///   不会静默少计（桶少算比桶读 0 更危险，它看着像证据）；
    /// - **代价界**：每删除行的页读、以及 `页/次下降`（③ 的尺子：合批下降之前，先看清一次下降花几页）。
    #[test]
    fn batched_deletes_really_restructure_and_stay_correct() {
        const N: usize = 1_200;
        // 三条界都从**两臂实测数中间**定（本形状：改后 10.63 页/删除行、3.54 页/次下降；
        // μG 把整树 DFS 抄回来 15.85 / 5.28；μI 把无条件问根抄回来 11.46 且问根 600 次 > 191 的界）
        const PAGE_BOUND: f64 = 12.5;
        const PER_DESCENT_BOUND: f64 = 4.2;
        fn k(n: usize) -> Key {
            format!("{n:08}").into_bytes()
        }
        fn r(n: usize) -> RowId {
            RowId(Some((n as u64).to_le_bytes().to_vec()))
        }

        // order=12 ⇒ 叶子满约 11 条；成块删一半 ⇒ 叶子成片空掉 ⇒ 必然走到借位/合并
        let mut tree = BPlusTree::new(12, 4096);
        for i in 0..N {
            assert!(tree.insert(k(i), r(i)), "插入第 {i} 个键失败");
        }
        for i in 0..N {
            assert!(tree.get(k(i)).is_some(), "第 {i} 个键写完读不回来");
        }

        let p0 = crate::bplus_tree::page::page_reads();
        let b0 = crate::bplus_tree::page::page_reads_by_phase();
        let (u0, bor0, mg0) = reorg_stats();
        let d0 = descent_lookups();
        let s0 = sep_refreshes();
        reorg_stats_clear();
        sep_refreshes_clear();

        let mut dels = 0usize;
        for i in (0..N).step_by(2) {
            assert!(tree.delete(k(i)), "删除第 {i} 个键失败");
            dels += 1;
        }
        let reads = crate::bplus_tree::page::page_reads() - p0;
        let b1 = crate::bplus_tree::page::page_reads_by_phase();
        let buckets: Vec<u64> = (0..b1.len()).map(|i| b1[i] - b0[i]).collect();
        let (u, bor, m) = reorg_stats();
        let (u, bor, m) = (u - u0, bor - bor0, m - mg0);
        let descents = descent_lookups() - d0;
        let refreshes = sep_refreshes() - s0;
        let per_del = reads as f64 / dels as f64;
        let per_descent = reads as f64 / descents.max(1) as f64;
        let (b_other, b_descent, b_back, b_sep, b_reorg, b_gp, b_rc) = (
            buckets[0],
            buckets[1],
            buckets[2],
            buckets[3],
            buckets[4],
            buckets[5],
            buckets[6],
        );
        let rc = root_checks();
        eprintln!(
            "[del_guard] {dels} 次删除 · 页读 {reads}（{per_del:.2} 页/删除行）· 下降 {descents} · \
             {per_descent:.2} 页/次下降\n  桶：其它 {b_other} 下降 {b_descent} 回溯 {b_back} \
             分隔键 {b_sep} 重组 {b_reorg} 找祖父 {b_gp} 问根 {b_rc}\n\
             重组：真下溢 {u} / 借位 {bor} / 合并 {m} / 问根 {rc} 次 / 分隔键刷新 {refreshes} 趟"
        );

        // ① 量具自检
        assert_eq!(
            buckets.iter().sum::<u64>(),
            reads,
            "阶段桶之和 {} != 页读 {reads} ⇒ 有一扇读页门没被标注，桶的读数全是漏的",
            buckets.iter().sum::<u64>()
        );
        // ②' M56b 的两条硬判据：整树 DFS 不许回来；问根只许在真合并过一页时发生
        assert_eq!(
            b_gp, 0,
            "找祖父桶读出 {b_gp} 页 ⇒ 那趟从根走的整树 DFS 回来了（200k 档它占每删除行 39.69 页 = 77%）"
        );
        assert!(
            rc <= m + dels as u64 / 8,
            "问根 {rc} 次（删除 {dels} 次、合并 {m} 次）⇒ `shrink_root_if_needed` 又变成每行无条件问了：             根的孩子数只在合并过时才会变"
        );
        // ② 证人：这个形状必须真的做过重组
        assert!(
            bor + m > 0,
            "借位与合并都是 0 ⇒ 这个形状还是没触发重组，证人失效（别把\"重组也上报\"读成已检验）"
        );
        // ③ 代价界
        assert!(
            per_del <= PAGE_BOUND,
            "每次删除读 {per_del:.2} 页（界 {PAGE_BOUND}）⇒ 删除路径又长出重复读了；\
             看桶里哪一段涨了就修哪一段（下降/回溯/分隔键/重组）"
        );
        assert!(
            per_descent <= PER_DESCENT_BOUND,
            "每次下降读 {per_descent:.2} 页（界 {PER_DESCENT_BOUND}）⇒ 一趟下降除了路上每层那一页还多读了页"
        );
        // ④ 正确性扫
        for i in 0..N {
            if i % 2 == 0 {
                assert!(tree.get(k(i)).is_none(), "第 {i} 个键已删，还能读到");
            } else {
                assert!(tree.get(k(i)).is_some(), "第 {i} 个幸存键读不回来");
            }
        }
    }

    /// M61b：**批量门真的把同一片叶子里的连续行合成一趟下降**，而且结果与逐行删除逐键一致。
    ///
    /// 为什么是"两臂都在同一个测试里跑"而不是"钉一个绝对界"：界会随形状漂（这片叶子能装几行由
    /// `order` 定），而**比值**是这一刀的定性结论（M61 读到的上限 k = 行数/叶子游程）。
    /// 三件事各自钉住：
    /// ① `k` 两臂必须**完全相同** —— 行数与游程数都由"哪些键被删"决定，与怎么删无关；
    ///    批量臂的 k 若掉到 1，说明它退化成了逐行（那就是变异臂，不需要另写）；
    /// ② 批量臂的 `页·下降/删除行` 必须 ≤ 逐行臂的 1/3（本形状每片叶子有 5~6 行可删 ⇒ 理论 1/5.5，
    ///    留 1/3 是防"批了但每行还是重下一趟"这种假批量）；
    /// ③ 删完之后两臂的**可读性逐键相同**（该没了的读不到、该留的读得到），且桶之和 == 页读照旧。
    #[test]
    fn batch_door_really_shares_one_descent_per_leaf() {
        const N: usize = 1_200;
        fn k(n: usize) -> Key {
            format!("{n:08}").into_bytes()
        }
        fn r(n: usize) -> RowId {
            RowId(Some((n as u64).to_le_bytes().to_vec()))
        }
        // order=12 ⇒ 一片叶子约 11 条；每 2 把删 1 把 ⇒ 同一片叶子里平均 5~6 行连续可删（k ≈ 5.5）
        let build = || {
            let mut t = BPlusTree::new(12, 4096);
            for i in 0..N {
                assert!(t.insert(k(i), r(i)), "插入第 {i} 个键失败");
            }
            t
        };
        let victims: Vec<Key> = (0..N).step_by(2).map(k).collect();

        let arm = |tree: &mut BPlusTree, batch: bool| -> (f64, f64, u64, u64, usize) {
            let p0 = crate::bplus_tree::page::page_reads();
            let b0 = crate::bplus_tree::page::page_reads_by_phase();
            let (r0, g0) = delete_leaf_runs();
            let removed = if batch {
                tree.delete_batch(&victims)
            } else {
                victims
                    .iter()
                    .filter(|key| tree.delete((*key).clone()))
                    .count()
            };
            let reads = crate::bplus_tree::page::page_reads() - p0;
            let b1 = crate::bplus_tree::page::page_reads_by_phase();
            let buckets: Vec<u64> = (0..b1.len()).map(|i| b1[i] - b0[i]).collect();
            let (r1, g1) = delete_leaf_runs();
            let n = removed.max(1) as f64;
            assert_eq!(
                buckets.iter().sum::<u64>(),
                reads,
                "阶段桶之和 {} != 页读 {reads} ⇒ 批量门里有一扇读页门没被标注",
                buckets.iter().sum::<u64>()
            );
            assert_eq!(
                buckets[crate::bplus_tree::page::PHASE_GRANDPARENT as usize],
                0,
                "找祖父桶回来了"
            );
            (
                buckets[PHASE_DESCENT as usize] as f64 / n,
                reads as f64 / n,
                r1 - r0,
                g1 - g0,
                removed,
            )
        };
        let check_readable = |tree: &mut BPlusTree, who: &str| {
            for i in 0..N {
                let got = tree.get(k(i)).is_some();
                assert_eq!(
                    got,
                    i % 2 == 1,
                    "{who}：第 {i} 把键的可读性不对（偶数该被删、奇数该留下）"
                );
            }
        };

        let mut per_row = build();
        let (d_per_row, all_per_row, rows_a, runs_a, removed_a) = arm(&mut per_row, false);
        check_readable(&mut per_row, "逐行臂");

        let mut batched = build();
        let (d_batch, all_batch, rows_b, runs_b, removed_b) = arm(&mut batched, true);
        check_readable(&mut batched, "批量臂");

        eprintln!(
            "[batch door] 逐行臂 下降 {d_per_row:.2} 页/删除行、总 {all_per_row:.2} 页/删除行（{rows_a} 行 / {runs_a} 游程）"
        );
        eprintln!(
            "           批量臂 下降 {d_batch:.2} 页/删除行、总 {all_batch:.2} 页/删除行（{rows_b} 行 / {runs_b} 游程）"
        );
        assert_eq!(rows_a, rows_b, "两臂删的行数不同 ⇒ 有一臂漏删了键");
        // ⚠️ 游程数**允许小幅不同**：合批把"查下溢/借位/合并"的时机从每行一次改成每片叶子一次
        // ⇒ 叶子的划分随之漂移（600k 档实测 20 018 vs 19 419，差 3%）。所以这里钉"同一量级"，
        // 不钉"逐个相等" —— 后者会在别的形状上无故发红（大档 A/B 抓出来的这条就是预警）。
        assert!(
            runs_a > 0 && (runs_a as i64 - runs_b as i64).abs() * 4 <= runs_a as i64,
            "两臂的叶子游程差太多：{runs_a} vs {runs_b} ⇒ 有一臂走错了叶子/路由"
        );
        assert_eq!(removed_a, removed_b, "两臂实删行数不同 ⇒ 有一臂漏删了键");
        assert!(
            runs_a > 0 && rows_a > runs_a * 3,
            "这个形状的 k = {}/{} 本来就不大于 3 ⇒ 证人失效：换不到收益是因为没合批，还是因为没得合批？",
            rows_a,
            runs_a
        );
        assert!(
            d_batch <= d_per_row / 3.0,
            "批量臂每删除行仍读 {d_batch:.2} 页下降（逐行臂 {d_per_row:.2}）⇒ 没合上批：每行还在重下一趟"
        );
    }

    /// M56 的拆段台架（`#[ignore]`，手动 release 单线程跑）：把"每删除行读 9.37 页"**按阶段拆开**
    ///
    /// 两种删法必须一起读：M54 那份归因只给了一个总数。
    /// - **每 3 把删 1 把**（稀疏）：叶子不会成片空 ⇒ 重组少，钱主要在下降 + 回溯；
    /// - **成块删一半**：叶子成片空 ⇒ 借位/合并密集，看重组那只桶涨多少。
    /// ③（同叶合批一次下降）只治"下降"那一段 ⇒ 哪一段占大头，决定下一刀切哪里。
    ///
    /// ```text
    /// BPLUS_DEL_ROWS=20000 cargo test --release --lib -- --ignored --nocapture \
    ///     --test-threads=1 bplus_delete_path_buckets
    /// ```
    #[test]
    #[ignore = "bplus 删除路径按阶段拆段，手动 release 单线程跑：BPLUS_DEL_ROWS=20000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 bplus_delete_path_buckets"]
    fn bplus_delete_path_buckets() {
        let rows: usize = std::env::var("BPLUS_DEL_ROWS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(20_000);
        fn k(n: usize) -> Key {
            format!("{n:08}").into_bytes()
        }
        fn r(n: usize) -> RowId {
            RowId(Some((n as u64).to_le_bytes().to_vec()))
        }

        for (tag, step) in [("每3把删1把", 3usize), ("成块删一半", 2usize)] {
            let mut tree = BPlusTree::new(64, 4096);
            for i in 0..rows {
                assert!(tree.insert(k(i), r(i)), "插入第 {i} 个键失败");
            }
            tree.flush_all();
            // 焐热：先全库点读一遍，让量具读的是"页访问次数"而不是冷缓存的读盘
            for i in 0..rows {
                assert!(tree.get(k(i)).is_some(), "第 {i} 个键写完读不回来");
            }

            let p0 = crate::bplus_tree::page::page_reads();
            let b0 = crate::bplus_tree::page::page_reads_by_phase();
            let m0 = crate::bplus_tree::page::cache_misses();
            let e0 = crate::bplus_tree::page::page_evictions();
            let (u0, bor0, mg0) = reorg_stats();
            let d0 = descent_lookups();
            let s0 = sep_refreshes();
            reorg_stats_clear();
            sep_refreshes_clear();

            let mut dels = 0usize;
            for i in (0..rows).step_by(step) {
                assert!(tree.delete(k(i)), "删除第 {i} 个键失败");
                dels += 1;
            }
            let d = dels as f64;
            let reads = crate::bplus_tree::page::page_reads() - p0;
            let b1 = crate::bplus_tree::page::page_reads_by_phase();
            let bk: Vec<u64> = (0..b1.len()).map(|i| b1[i] - b0[i]).collect();
            let (pu, pb, pbt, ps, pr, pg, prc) = (
                bk[0] as f64 / d,
                bk[1] as f64 / d,
                bk[2] as f64 / d,
                bk[3] as f64 / d,
                bk[4] as f64 / d,
                bk[5] as f64 / d,
                bk[6] as f64 / d,
            );
            let rc = root_checks();
            let miss = crate::bplus_tree::page::cache_misses() - m0;
            let evict = crate::bplus_tree::page::page_evictions() - e0;
            let (u, bor, m) = reorg_stats();
            let (u, bor, m) = (u - u0, bor - bor0, m - mg0);
            let descents = descent_lookups() - d0;
            let refreshes = sep_refreshes() - s0;
            let per_del = reads as f64 / d;
            let per_descent = reads as f64 / descents.max(1) as f64;
            eprintln!(
                "[del_buckets {tag}] rows={rows} 删 {dels} 行 · 页读 {reads}（{per_del:.2} 页/删除行）\
                 · 下降 {descents}（{:.2} 次/删除行）· {per_descent:.2} 页/次下降\n\
                 桶/删除行：其它 {pu:.2} 下降 {pb:.2} 回溯 {pbt:.2} 分隔键 {ps:.2} 重组 {pr:.2} 找祖父 {pg:.2} 问根 {prc:.2}\n\
                 重组：真下溢 {u}（{:.2} 次/删除行）借位 {bor} 合并 {m} 问根 {rc}（{:.2} 次/删除行）\
                 ｜ 分隔键刷新 {refreshes}（{:.3} 趟/删除行）｜ 缺页 {:.2} 换出 {:.2}",
                descents as f64 / d,
                u as f64 / d,
                rc as f64 / d,
                refreshes as f64 / d,
                miss as f64 / d,
                evict as f64 / d,
            );
            assert_eq!(
                bk.iter().sum::<u64>(),
                reads,
                "桶之和 != 页读 ⇒ 标注漏了一扇门，这一行读数全部作废"
            );
            // 只有"成块删一半"这一臂**要求**触发重组（证人）；稀疏那一臂读到 0 是**结论**不是失败 ——
            // 它正是在说"稀疏删几乎不重组，所以 ③ 省下的钱不在重组段"
            if step == 2 {
                assert!(
                    bor + m > 0,
                    "这一档（{tag}, rows={rows}）没触发过重组 ⇒ 证人失效，别把重组段读成\"不花钱\""
                );
            }
            for i in 0..rows {
                if i % step == 0 {
                    assert!(tree.get(k(i)).is_none(), "第 {i} 个键已删还能读到");
                } else {
                    assert!(tree.get(k(i)).is_some(), "第 {i} 个幸存键读不回来");
                }
            }
        }
    }
}
