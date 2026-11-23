//! 页管理器：缓冲池 + 持久化
//!
//! - 数据文件：通过 [`crate::bplus_tree::storage::DiskFile`] 读写
//! - 页头 CRC：[`crate::bplus_tree::storage::pack_page`] / [`unpack_page`]
//! - 双写缓冲：刷盘前先写 dblwr 再写主文件，防 torn page

use std::eprintln;
use std::format;
use std::matches;
use std::thread_local;
use std::vec;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::bplus_tree::storage::{
    DiskFile, DoubleWriteBuffer, FileLock, FreelistWriter, PAGE_PAYLOAD_SIZE, PAGE_SIZE,
    dblwr_path, freelist_path, lock_path, pack_page, read_freelist, unpack_page,
};
use crate::common::disk::{SITE_BPLUS_DBLWR, SITE_BPLUS_META, SITE_BPLUS_PAGE_FLUSH};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, PartialOrd, Hash, Eq)]
/// 页号。0 保留为 meta 页。
pub struct PageId(pub u64);

/// 业务载荷上限（页大小去掉页头）
pub const MAX_PAYLOAD: usize = PAGE_PAYLOAD_SIZE;

/// meta 页固定占 page 0；业务数据从 page 1 开始
const META_PAGE_ID: u64 = 0;

/// 文件魔数
const MAGIC: &[u8; 8] = b"BPLUS001";

/// meta payload 布局（写在 pack 后的 payload 区）：
/// ```text
/// 0   8   magic
/// 8   8   next_page_id
/// 16  8   free_count_hint（仅提示；完整 freelist 在 data.db.freelist）
/// 24  8   root_page_id (0 = 未设置)
/// 32  4   "NVER" 魔数（有则 offset 36 为 next_version；兼容旧库）
/// 36  8   next_version（MVCC 下一事务号；避免 open 时扫全树）
/// 44  …   保留
/// ```
///
/// 完整空闲页列表持久化在 sidecar：`{db}.freelist`（见 storage::write_freelist）。
const META_OFF_MAGIC: usize = 0;
const META_OFF_NEXT: usize = 8;
const META_OFF_FREE_CNT: usize = 16;
const META_OFF_ROOT: usize = 24;
const META_OFF_NVER_MAGIC: usize = 32;
const META_OFF_NEXT_VERSION: usize = 36;
const NVER_MAGIC: &[u8; 4] = b"NVER";

/// 页状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageState {
    Clean,
    Dirty,
    Free,
}

/// 内存缓存项（存的是 **payload**，不是整页）
pub struct CacheEntry {
    pub page_id: PageId,
    data: Vec<u8>, // payload，长度 = PAGE_PAYLOAD_SIZE
    state: PageState,
    last_access: u64,
}

/// 换出时攒多少脏页再写盘（并在非 bulk 模式下 fsync 一次）
const EVICT_BATCH_TARGET: usize = 64;

/// 写入失败可能已部分发布页面。只有重开恢复才能解除封锁；重试 checkpoint 不得丢弃恢复 WAL。
enum PageIoState {
    Healthy,
    Failed {
        kind: std::io::ErrorKind,
        message: String,
    },
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ReclamationState {
    Idle,
    Scanning,
    UnpublishedFrees,
}

pub(crate) enum ReclamationPublication {
    DeferredFreelist,
    ExistingBulk,
}

/// 页管理器
pub struct PageManager {
    /// 单写者锁（持有即占用库；Drop 释放）
    _lock: FileLock,
    file: DiskFile,
    dblwr: DoubleWriteBuffer,
    path: PathBuf,
    cache: HashMap<PageId, CacheEntry>,
    lru_queue: VecDeque<PageId>,
    /// 空闲页（FIFO 复用）；权威持久化在 `{db}.freelist`
    free_page_ids: VecDeque<PageId>,
    /// 空闲页集合，O(1) 判断 page_is_live
    free_set: HashSet<u64>,
    next_page_id: u64,
    root_page_id: Option<PageId>,
    /// MVCC 下一事务号（持久化在 meta；`None` 表示旧库尚未写入 NVER）
    next_version: Option<u64>,
    max_cache_pages: usize,
    clock: u64,
    /// meta 是否有未落盘变更
    meta_dirty: bool,
    /// freelist sidecar 是否有未落盘变更
    freelist_dirty: bool,
    freelist_writer: Option<FreelistWriter>,
    /// bulk 模式：换出/刷盘跳过双写与频繁 fsync，只在 flush_all 末尾 sync 一次
    bulk_mode: bool,
    /// bulk 换出后缓存可能已无脏页，但之前的磁盘写入仍需要同步。
    bulk_pending_sync: bool,
    io_state: PageIoState,
    reclamation: ReclamationState,
    /// 每页被写入过几次（`write_page` / `allocate_page` 各算一次）。
    ///
    /// 上层用它验证"解析过的节点还新不新"：数字一变，那份 `BTreeNode` 就是陈旧的，必须重解析。
    /// 做成**按页**计数而不是全局 epoch，否则一次写入就把整棵树的解析全部作废，
    /// 写多的时候缓存等于没有。
    page_writes: HashMap<PageId, u64>,
    /// **钉住的页**：B+ 树的上层页（根 + 内部节点）。
    ///
    /// 为什么单独一档：换出策略再"聪明"也只保证最近用过的页留下，而全库走查（`health()` /
    /// `scan` / `search`）与批量写会让 LRU 转好几圈 —— 实测 200k 行的库里，走查之后的随机点读
    /// 从 45.2 µs 涨到 **58.6 µs（+30%）**，因为下降路上那几层每次都要重新读盘 + 重新解析。
    /// 上层页数量小：35 B 键的库实测 20k 行 36 页 / 200k 行 367 页，都只占全库 **3.0%**
    /// （≈ 叶子数 ÷ 阶数）。⚠️ 但**这个占比不是常数** —— 长键会把内部节点的扇出也压下来
    /// （同一份五千行的库，3.3 KB 键时上层占到 **50%**），所以预算必须跟容量挂钩：
    /// 见 [`PageManager::pin_limit`]，钉住的部分永远不超过容量的一半。
    pinned: HashSet<u64>,
    /// 钉住预算（**可配**，M49）：默认 [`DEFAULT_PIN_BUDGET_PAGES`]，行为与写死那一版一字不差。
    ///
    /// 为什么要能调**小**：钉住的那部分是"不参与换出"的容量，它和叶子抢同一份预算。
    /// 实测 2048 页的顶下住着 1986 页，其中钉住 1024 页全是上层 ⇒ 留给叶子的只有 962 页，
    /// 而那份库有 1300 片叶子 ⇒ 一轮随机点读缺 1621 页。上层值多少由键长决定，
    /// 调用方比库清楚自己那份库的叶子住不住得下 —— 这一档的决定权在这里交出。
    pin_budget: usize,
}

/// 钉住的上层页默认预算（一页 4 KB ⇒ 默认上限 ≈ 4 MB）
pub(crate) const DEFAULT_PIN_BUDGET_PAGES: usize = 1024;

// 测试专用：本线程的页访问次数（含页缓存命中）。
//
// B+ 树没有「读一行」这种细粒度 IO，代价的对应物是读了多少页 —— 用来把
// 「点操作代价不随版本链长度增长」这条不变量钉成断言。做成 thread_local 是为了
// 不和 `cargo test` 的并行用例互相计数。
#[cfg(test)]
thread_local! {
    static PAGE_READS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

// 测试专用：页读**按阶段归桶**（M56）。
//
// 为什么需要它：M54b 之后 20k 档"每删除行仍读 9.37 页"，可 `PAGE_READS` 是一只总桶，
// 它答不出这 9.37 页花在**下降**、**回溯**、**分隔键刷新**还是**重组（借位/合并）**上 ——
// 而这五段的修法完全不同（下降=合批同叶多行、回溯=M54b 已做、分隔键=M54b 已做、重组=去掉对同一页的重复读、
// 找祖父=那趟从根走的整树 DFS —— 它单独一只桶，因为它一发生就是 O(整树页数)，混在"重组"里就看不见它），
// 不拆开就只能猜。阶段由删除路径自己用 [`set_page_phase`] 标注，读页的两扇门
// （`read_page` / `read_page_for_walk`）都把"当前桶 +1" ⇒ **各桶之和应当等于 `PAGE_READS`**，
// 这条本身由守卫对着（桶漏标注会立刻读出来，而不是静默少计）。
// 阶段常量在**两边都存在**：这样删除路径里的标注不必撒 `#[cfg(test)]`，
// 业务代码与量具在两种构建下同形（读桶只在 test 构建里存在）。
pub(crate) const PHASE_OTHER: u8 = 0;
pub(crate) const PHASE_DESCENT: u8 = 1;
pub(crate) const PHASE_BACKTRACK: u8 = 2;
pub(crate) const PHASE_SEP_REFRESH: u8 = 3;
pub(crate) const PHASE_RESTRUCTURE: u8 = 4;
// 这只桶**只有测试构建里有**：M56b 删掉那趟整树 DFS 之后，业务代码再没有一处会往里写，
// 留着它是为了"DFS 回来了就读得见"（守卫断言它恒为 0，μG 变异臂往里写）。
#[cfg(test)]
pub(crate) const PHASE_GRANDPARENT: u8 = 5;
pub(crate) const PHASE_ROOT_CHECK: u8 = 6;
#[cfg(test)]
pub(crate) const PHASE_BUCKETS: usize = 7;
#[cfg(test)]
thread_local! {
    static PAGE_PHASE: std::cell::Cell<u8> = const { std::cell::Cell::new(0) };
    static PAGE_READS_PHASE: [std::cell::Cell<u64>; PHASE_BUCKETS] = const {
        [
            std::cell::Cell::new(0),
            std::cell::Cell::new(0),
            std::cell::Cell::new(0),
            std::cell::Cell::new(0),
            std::cell::Cell::new(0),
            std::cell::Cell::new(0),
            std::cell::Cell::new(0),
        ]
    };
}

// 本线程「试图写一个装不下的页」的次数。
//
// 这个计数存在的理由是**正确性**而不只是噪音：`write_page` 超限只打印并返回 false，
// 而 delete 的借键/合并这条链原先没人检查那个 bool —— 于是页保持**上一次**的内容，
// 父节点却已经删掉分隔键、`free_page` 了死页 ⇒ 那批行不可达（静默丢数据）。
// 真实形状（40B 键 + 60B 值、order=64）下合并两个半页就是 7.5 KB > 4080，必然踩到。
// 现在四家重组前都先量字节，这条应当恒为 0（守卫：`test_reorg_never_drops_oversized_pages`）。
thread_local! {
    static OVERSIZED_WRITES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

// 测试专用：页缓存的命中 / 未命中分开数。
//
// `PAGE_READS` 把两者算成同一个数，所以它只能守"代价不随链长涨"，量不出**为什么慢**：
// 命中是一次 4 KB 拷贝 + 一次 bincode 反序列化，未命中还要多一次真读盘与 CRC。
// bplus 在所有引擎里点读/管理面最贵，必须先分清这两者谁在花钱。
#[cfg(test)]
thread_local! {
    static CACHE_HITS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static CACHE_MISSES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

// 测试专用：换出、LRU 压缩、批量写盘各发生了多少。
//
// 归因 bplus `health()` 时必需的第三把尺子：命中/未命中只说明"有没有读盘"，说明不了
// **走一遍全库时被顺带flush了多少脏页**（换出脏页要写双写缓冲 + sync，一次的成本和一次
// 读盘不是一个量级）。台架复测里 `health()` 从 110 ms 涨到 251 ms，而它走的就是
// `scan_keys_with` 一条链 —— 到底是"每页更贵"还是"页更多"，得靠这三个计数分开。
#[cfg(test)]
thread_local! {
    static PAGE_EVICTIONS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static LRU_COMPACTIONS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static BATCH_FLUSHED_PAGES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

// 测试专用：本线程「把某行的 value 克隆出来」的次数。
//
// B+ 树没有独立键索引，全库走一遍的**页读取**省不掉；能省的是把每一行的 value
// 克隆出来（`iter()` 会做，按引用的 `scan_entries_with` 不会）。所以 vacuum 与
// 全库折叠这两条都用这个量来守。
#[cfg(test)]
thread_local! {
    static VALUE_CLONES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// 页管理器快照统计
#[derive(Debug, Clone)]
pub struct StorageStats {
    pub next_page_id: u64,
    pub free_pages: usize,
    pub cache_pages: usize,
    pub max_cache_pages: usize,
    pub root_page_id: Option<u64>,
    pub bulk_mode: bool,
    pub file_path: String,
}

#[cfg(test)]
pub(crate) mod eviction_reference;

#[cfg(test)]
#[cfg(test)]
pub(crate) mod reclamation_reference;

#[cfg(test)]
pub(crate) mod reclamation_batch_reference;

#[cfg(test)]
mod tests;

// 按职责组织的内部实现；公开类型与原模块路径保持一致。
mod allocation;
mod cache;
mod lifecycle;
mod metadata;
mod page_io;
mod probes;
#[cfg(test)]
pub(crate) use probes::batch_flushed_pages;
#[cfg(test)]
pub(crate) use probes::cache_hits;
#[cfg(test)]
pub(crate) use probes::cache_misses;
#[cfg(test)]
pub(crate) use probes::lru_compactions;
#[cfg(test)]
use probes::note_phase_read;
#[cfg(test)]
pub(crate) use probes::note_value_clone;
pub use probes::oversized_writes;
#[cfg(test)]
pub(crate) use probes::page_evictions;
#[cfg(test)]
pub(crate) use probes::page_reads;
#[cfg(test)]
pub(crate) use probes::page_reads_by_phase;
pub(crate) use probes::set_page_phase;
#[cfg(test)]
pub(crate) use probes::value_clones;
