use std::assert;
use std::format;
use std::matches;
#[cfg(test)]
use std::thread_local;
use std::unreachable;
use std::vec;
use std::{fmt, path::Path, write};

use serde::{Deserialize, Serialize};

use crate::bplus_tree::page::{
    MAX_PAYLOAD, PHASE_BACKTRACK, PHASE_DESCENT, PHASE_OTHER, PHASE_RESTRUCTURE, PHASE_ROOT_CHECK,
    PHASE_SEP_REFRESH, PageId, PageManager, set_page_phase,
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

/// 只读内部节点用一块键区替代逐分隔键分配。两种缓存共用页写入代次和槽位、钉住预算；修改路径显式升级为拥有型。
#[derive(Clone)]
enum CachedNode {
    Owned(std::sync::Arc<BTreeNode>),
    Internal(std::sync::Arc<PointInternal>),
}

struct PointInternal {
    payload: Vec<u8>,
    key_offsets: Vec<(u16, u16)>,
    children_offset: usize,
}

#[cfg(test)]
thread_local! {
    static POINT_INTERNAL_PACKS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static POINT_INTERNAL_PROMOTIONS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MaintenanceEnd {
    Complete,
    Stopped,
}

/// 借用现有 bincode 1 的定长、小端节点格式；必须校验完整节点后，行才能授权回收。
struct NodeBytes<'a>(&'a [u8]);
enum MaintenanceNode<'a> {
    Leaf {
        rows: &'a [u8],
        count: usize,
        next: Option<PageId>,
    },
    Internal {
        keys: &'a [u8],
        count: usize,
        children: &'a [u8],
    },
}

enum PointStep {
    Child(PageId),
    Hit(Key, RowId),
    Previous(Key),
    Missing,
}

enum DeleteRead {
    Owned(BTreeNode),
    PackedLeaf {
        remaining: usize,
        removed: usize,
        min_changed: bool,
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

/// 在树的独占借用期间读取的节点到写入前都有效。借位失败只改变局部副本，后续重平衡应重读原页，不能使用试算副本。
enum RebalanceInput {
    Fresh {
        first: BTreeNode,
        second: BTreeNode,
        parent: BTreeNode,
    },
    Reload,
}

#[cfg(test)]
thread_local! {
    static REBALANCE_REUSES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static REBALANCE_RELOADS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
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

/// 带页管理的B+树
pub struct BPlusTree {
    /// 页管理器
    page_manager: PageManager,
    /// 节点写入共用一份有界序列化缓冲，不把它当作解析缓存。
    node_buffer: Vec<u8>,
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
    node_cache: std::collections::HashMap<PageId, (u64, CachedNode)>,
    /// 容量是不是**自动档**（调用方传的 `max_cache_pages == 0`）
    auto_cache: bool,
    /// 自动档的**上限**，初值 = [`AUTO_MAX_PAGES`]；M32 让它可改。
    ///
    /// 为什么要能改：上限是**预算**不是正确性，而"默认 2048 页 ≈ 8 MiB"这一档在 600k 行的库上
    /// 只剩 5.6% 覆盖率 ⇒ 每次点读都付一次未命中。调用方要么认这个预算、要么自己把顶抬起来
    /// （抬到 ≥ 总页数 = 把库复制进 RAM，价签在 README 里写着），但那个决定不该由库替他做。
    auto_max_pages: usize,
    /// **内存预算档**（字节）：`0` = 不设预算（走 [`Self::auto_max_pages`] 那条）。
    ///
    /// 为什么用字节而不是槽数（M76）：下游给的约束就是字节（"这进程只能再住 32 MB"），
    /// 而换算需要**每槽真实常驻** —— 一页名义 4 KiB，实测一个页缓存槽位连解析缓存一起住
    /// [`CACHE_PAGE_RESIDENT_BYTES`]。换算只在这一处发生，`tuning()` 把"给的字节 / 换来的槽数 /
    /// 生效槽数"三个数一起报回去，调用方不用自己乘价签。
    cache_budget_bytes: u64,
    /// 调用方**显式**给的那一档容量（自动档下它只是初值 [`AUTO_MIN_PAGES`]）。
    ///
    /// 为什么要留着：预算档会把生效容量压到它下面，撤掉预算之后得能回到调用方原本要的那个数 ——
    /// 只读 `page_manager.cache_capacity()` 读到的是"被预算压过的生效值"，撤预算就撤不回去了。
    explicit_pages: usize,
}

/// 一个页缓存槽位的**真实常驻**（字节）：实测 `cache_slot_resident_price`，600k 档、乱序点读姿势，
/// 默认档那一臂 6 758 B/槽、抬顶后的边际 6 838 B/槽（走查姿势 4 264 B/槽 —— 取**大**的那档，
/// 因为预算的含义是"不许超过这么多"，按小的换算会在点读型下游身上超支 1.6 倍）。
///
/// ⚠️ 这个数不是页的名义大小（4 KiB）：页缓存与解析缓存同源、上限一起长（见 [`BPlusTree::node_cache`]），
/// 拿 4 KiB 换算会让常驻到预算的 1.66 倍（守卫 `cache_budget_bytes_*` 钉的就是这一格）。
pub(crate) const CACHE_PAGE_RESIDENT_BYTES: u64 = 7_000;

/// 自动档的下限：与旧的写死默认值一致 ⇒ 从"没旋钮"换到"自动档"不会让谁的工作集变小
const AUTO_MIN_PAGES: usize = 1024;
/// 自动档的上限（一页 4 KB ⇒ 最多 ≈ 8 MiB 常驻）。
///
/// 为什么必须夹住：B+ 树把整库都缓存住的常驻 ≈ 数据文件本身的大小（实测 200k 行 = 12129 页
/// ≈ 47 MB），那已经不是"缓存一点热数据"而是"把库再复制一份到内存"。
/// 这个基座要给单核 6 GB 的机器和 RSS ≤ 80 MB 的下游用，所以**默认只到 8 MiB，
/// 再多是调用方的决定**（显式传 `max_cache_pages`），不是库替你做的决定。
const AUTO_MAX_PAGES: usize = 2048;

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

pub struct BPlusTreeIterator<'a> {
    tree: &'a mut BPlusTree,
    current_page_id: Option<PageId>,
    current_idx: usize,
}

// ===================== 测试代码 =====================
#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) mod delete_read_reference;

#[cfg(test)]
pub(crate) mod rebalance_read_reference;

#[cfg(test)]
pub(crate) mod point_internal_reference;

#[cfg(test)]
pub(crate) mod point_projection_reference;

#[cfg(test)]
pub(crate) mod delete_leaf_reference;

#[cfg(test)]
pub(crate) mod delete_cost_reference;

#[cfg(test)]
pub(crate) mod maintenance_reference;

// 按职责组织的内部实现；公开类型与原模块路径保持一致。
mod cache;
mod delete;
mod insert;
mod lifecycle;
mod node_codec;
mod node_size;
#[cfg(test)]
mod probes;
mod read;
mod rebalance;
mod scan;
mod stats;
use cache::auto_cache_pages;
#[cfg(test)]
pub(crate) use probes::delete_leaf_runs;
#[cfg(test)]
pub(crate) use probes::descent_key_comps;
#[cfg(test)]
pub(crate) use probes::descent_lookups;
#[cfg(test)]
pub(crate) use probes::descent_max_comps;
#[cfg(test)]
pub(crate) use probes::node_cache_sweeps;
#[cfg(test)]
pub(crate) use probes::node_cache_visited;
#[cfg(test)]
pub(crate) use probes::node_parses;
#[cfg(test)]
use probes::note_delete_leaf;
#[cfg(test)]
pub(crate) use probes::point_internal_packs;
#[cfg(test)]
pub(crate) use probes::point_internal_promotions;
#[cfg(test)]
pub(crate) use probes::rebalance_counts;
#[cfg(test)]
pub(crate) use probes::reorg_stats;
#[cfg(test)]
pub(crate) use probes::reorg_stats_clear;
#[cfg(test)]
pub(crate) use probes::root_checks;
#[cfg(test)]
pub(crate) use probes::sep_refreshes;
#[cfg(test)]
pub(crate) use probes::sep_refreshes_clear;
#[cfg(test)]
pub(crate) use probes::split_drops;
