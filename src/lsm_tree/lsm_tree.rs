//! LSM 引擎核心：MemTable + SST 分层 + flush / leveled compaction。
//!
//! 读路径（新→旧）：mem → immutables → L0 → L1 → …
//! 写路径：insert →（可选 mem.wal）→ mem；超阈值 flush → L0；再 leveled compact。
//!
//! 版本 GC 由上层 `MVCC::vacuum` 负责；compact 仅在没有外部旧来源时丢弃删除屏障。

#[cfg(test)]
use std::assert;
use std::format;
use std::matches;
#[cfg(test)]
use std::thread_local;
use std::write;
use std::writeln;
use std::{
    collections::{BTreeMap, BinaryHeap, HashMap, HashSet},
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
    vec,
};

use crate::lsm_tree::mem_wal::{MemWal, MemWalRecord};

/// 只用于测试的 M147 之前分配路径，不切换持久性和可见性。
#[cfg(all(test, feature = "lsm-allocation-reference"))]
pub(crate) mod allocation_reference;
use crate::lsm_tree::sst::{
    BlockCache, CompactionAction, EntryValue, LightEntry, MergeInput, MergeRow, SourceRow,
    SstBuilder, SstFile, StoredEntry, TryMergeInput, UnpublishedSsts, merge_latest_streaming,
    merge_streaming, merge_to_ssts_with_action, vec_inputs,
};

/// 调用方持有树锁时，由严格维护读取确认的 Put 或墓碑胜者才可授权删除；Drop 记录不能授权第二次删除。
pub(crate) struct VacuumDelete {
    key: Vec<u8>,
    live: bool,
}

/// 全范围回退与索引查找的资源成本分别表达。
pub(crate) enum KeyBackResult {
    Indexed(Option<Vec<u8>>),
    FullScan(Option<Vec<u8>>),
}
impl KeyBackResult {
    pub(crate) fn into_key(self) -> Option<Vec<u8>> {
        match self {
            Self::Indexed(key) | Self::FullScan(key) => key,
        }
    }
}
impl VacuumDelete {
    pub(crate) fn observed(key: Vec<u8>, kind: u8) -> std::io::Result<Self> {
        match kind {
            0 | 1 => Ok(Self {
                key,
                live: kind == 0,
            }),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "vacuum cannot delete an unobserved Put",
            )),
        }
    }
}

#[cfg(test)]
pub(crate) mod vacuum_delete_probe;

/// 有界切片不能证明全库引用关系。只有确认目录为空或不存在，全库回收才可省去引用读取。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BlobGcPlan {
    SkipBounded,
    NoFiles,
    ScanReferences,
}

#[cfg(test)]
pub(crate) mod blob_reference;

#[cfg(test)]
pub(crate) mod manifest_reference;

// ─── Types ───────────────────────────────────────────────────────────────────

/// user_key -> (seq -> value)；同 key 多版本以支持 Snapshot
type MemMap = BTreeMap<Vec<u8>, BTreeMap<u64, EntryValue>>;

/// M41b：**一条 memtable 条目在堆上的结构开销**（载荷账之外的部分）。
///
/// 为什么需要它：`mem_bytes` 数的是**载荷**（`key.len() + StoredEntry::estimate_bytes()`），
/// 一条 16 B 编码键的墓碑因此只算 **25 B**。触发判定直接读它 ⇒ `mem_threshold_bytes: 32 MB`
/// 这一句在 MVCC 档上**永不成立**：600k 条墓碑的载荷账 15 MB，兜底的其实是条数阈值 256k，
/// 而那 256k 条在堆上是 ~155 MB（`lsm_vacuum_stage_split` 的 ① 段实测）。
///
/// 常数取校准台架 `lsm_memtable_bytes_per_entry` 的实测：真堆占用 **476 B/条**，其中载荷账
/// 只记 25 B ⇒ 结构 **451 B**（外层 `BTreeMap` 节点槽 + 内层 `BTreeMap` 的盒子与节点 + 键的
/// `Vec`）。n=100k 与 n=400k 两档都读到同一个 476 ⇒ 这条是线性的，不是规模凑出来的。
///
/// ⚠️ 两个已知边界：① 校准时**一把键一条版本**（正是 MVCC 的形状 —— 编码键自带版本号），
/// 同一把键堆多个版本时真实开销低于 `条数 × 本常数`，于是判定会**偏保守**（提前 flush），
/// 不会反过来低估；② 键更长时开销也涨（节点装得更少），那部分由载荷项顶着，同样是保守方向。
const MEM_ENTRY_OVERHEAD: usize = 451;

// 常规 MVCC 点查的 L0 文件数通常低于此值；恢复或批量写入产生更大 L0 时走堆分配兜底。
const INLINE_L0_ORDER: usize = 128;

enum L0CandidateOrder {
    Inline {
        indexes: [usize; INLINE_L0_ORDER],
        len: usize,
    },
    Heap(Vec<usize>),
}

// 测试专用：本线程「为了数 memtable 条目而走过的条目数」。
//
// M19 的复杂度尺子。它数的不是" flush 判定的结果对不对"，而是**判定本身花了多少步**：
// 旧写法每次 insert 都为"要不要 flush"把整表数一遍 ⇒ 灌 N 行是 O(N²)（200k 大规模基线抓到：
// 裸 insert 每行 89 µs → 1429 µs，与 memtable 规模同步）。改成增量计数器之后这个数**不该再涨**；
// 对照（同一条用例里主动调一次慢口径）证明这把尺子本身不是空转。
#[cfg(test)]
thread_local! {
    static MEM_SCAN_STEPS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    // 层范围二分的比较次数；块访问计数看不见逐篇枚举的纯 CPU 代价。
    static LEVEL_RANGE_PROBES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// compaction filter：返回 `true` 保留，`false` 丢弃。
pub type CompactionFilter = Arc<dyn Fn(&[u8], &StoredEntry) -> bool + Send + Sync + 'static>;

/// 读快照：仅可见 `seq <= snap_seq` 的写入
#[derive(Debug, Clone, Copy)]
pub struct Snapshot {
    pub seq: u64,
}

enum MoveOutcome {
    Moved,
    Rewrite,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum VacuumWriteSchedule {
    Immediate,
    Coalesced,
}

#[cfg(test)]
pub(crate) mod vacuum_compaction_reference;

/// LSM 存储引擎
pub struct LSMTree {
    dir: PathBuf,
    opts: LSMTreeOptions,
    mem: MemMap,
    /// memtable 里的**版本条数**（Σ 每个键的版本数），与 `scan_mem_entry_count` 同口径。
    ///
    /// ⚠️ 必须是增量维护的计数，不许现算：2026-10-17 的 200k 大规模基线抓到旧写法在这里
    /// 每次插入都把整个 memtable 数一遍（只为回答"要不要 flush"），于是灌 N 行是 O(N²) ——
    /// 裸 `insert`（不经 MVCC）每行代价从 89 µs 涨到 1429 µs，与灌入曲线重合。
    mem_entries: usize,
    mem_bytes: usize,
    immutables: Vec<MemMap>,
    levels: Vec<Vec<SstFile>>,
    /// L1+ 的范围是否有序且仅可能端点相接。恢复/测试注入可带任意重叠，不能盲目二分。
    /// 只在文件集变化时验证；读路径不再为确认不变量而逐篇扫元数据。
    level_ranges_ordered: Vec<bool>,
    next_sst_id: AtomicU64,
    write_seq: AtomicU64,
    dirty: bool,
    mem_wal: Option<MemWal>,
    block_cache: Arc<Mutex<BlockCache>>,
    cached_live: usize,
    cached_tomb: usize,
    stats_dirty: bool,
    compaction_filter: Option<CompactionFilter>,
    /// pinned snapshot seq -> refcount
    active_snapshots: BTreeMap<u64, usize>,
    /// 键到 Unix 秒级过期时间的映射，独立于值字节。
    ttl_meta: BTreeMap<Vec<u8>, u64>,
    manifest_edit_id: u64,
    bg: Option<BgCompactCtrl>,
    /// bulk 导入：flush 不触发同步 compact，stall 放宽
    bulk_mode: bool,
    vacuum_write_schedule: VacuumWriteSchedule,
    /// 块缓存是不是别人的（CF 模式共享一份）。true ⇒ **自动档不再动容量**：
    /// 一棵树按自己的块数改上限，会把同进程里另一棵树的驻留块换出去，那是净负收益。
    /// 共享缓存的容量由装配方（`cf.rs`）自己定。
    cache_shared: bool,
    /// **内存预算档**（字节）；`0` = 不设预算（走 `block_cache_blocks` / `block_cache_auto_max` 那条）。
    ///
    /// 为什么用字节（M76）：下游的约束从来是"这进程还能住多少"，而此前两个旋钮都以**块数**计，
    /// 按内存定档要自己乘价签 —— 价签是实测的 10.1 KB/块，不是块的名义大小（`sst_block_entries`
    /// × 条目大小会变）。换算只在这一处发生，`tuning()` 把"给的字节 / 生效块数 / 分母"一起报回去。
    cache_budget_bytes: u64,
}

/// 一个块缓存槽位的**真实常驻**（字节）：实测 `cache_slot_resident_price`，600k 档，
/// 走查 10 130 B/槽、乱序点读 10 192 B/槽（两种姿势一字不差 ⇒ 块缓存没有 bplus 那种"解析缓存"
/// 第二份账），向上取整到 10 400 留余量。
///
/// ⚠️ 预算的语义是"不许超过这么多"，所以换算宁少不多：`槽数 = 预算 ÷ 本值`。
pub(crate) const BLOCK_SLOT_RESIDENT_BYTES: u64 = 10_400;

struct BgCompactCtrl {
    stop: Arc<AtomicBool>,
    notify: Arc<(Mutex<bool>, Condvar)>,
    handle: Option<JoinHandle<()>>,
}

/// 引擎状态快照（见 [`LSMTree::stats`] / [`LSMTree::stats_exact`]）。
///
/// # `live_keys` / `tombstones` 语义
///
/// - **`stats()`**：返回写路径维护的 **缓存近似值**（`cached_live` / `cached_tomb`）。
///   - `open` **不会**全量 merge，故刚打开时 `live_keys` 常为 **0**（或仅含本会话写入增量）。
///   - `insert_fast` / bulk 路径为吞吐会跳过读旧值，live 可能偏高。
///   - 文件层计数（`l0_files` / `level_*`）始终准确。
/// - **`stats_exact()`**：全量 merge 后刷新缓存，**精确** live/tomb（大库昂贵）。
///
/// 监控仪表盘用 `stats()`；对账 / 测试断言用 `stats_exact()`。
#[derive(Debug, Clone)]
pub struct LsmStats {
    /// 存活 Put(Some) 条数（见结构体文档：可能是近似值）
    pub live_keys: usize,
    /// tombstone（Put(None)）条数（近似，语义同 `live_keys`）
    pub tombstones: usize,
    pub mem_entries: usize,
    pub mem_bytes: usize,
    pub l0_files: usize,
    pub l1_files: usize,
    pub total_sst: usize,
    pub level_files: Vec<usize>,
    pub level_bytes: Vec<u64>,
    pub oldest_snapshot: Option<u64>,
    pub dir: PathBuf,
}

/// 测试显式启用真实维护入口的计时，生产路径无该开销。
#[cfg(test)]
pub(crate) mod maintenance_probe;

enum MaintenanceManifest {
    PublishedByCompaction,
    PublishedHere,
}

#[cfg(test)]
mod manifest_repeat_reference;

#[cfg(test)]
mod tests;

// 测试专用：本线程一共**延迟激活**过多少个源（= 拉过多少个源的首键）。
//
// 这是第 1 项的尺子。墙钟在本地跑间能差 2 倍，而"首页付了几篇的块读"是精确的工作量计数：
// eager 形状下它恒等于源数（随库里 SST 篇数涨），lazy 形状下它只等于"下界压得住前几把键"
// 的那几篇。守卫用它，不用墙钟。
#[cfg(test)]
thread_local! {
    static SOURCE_ACTIVATIONS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) mod disjoint_move_reference;

#[cfg(test)]
mod trivial_move_probe;

// 按职责组织的内部实现；公开类型与原模块路径保持一致。
mod background;
mod blob;
mod bulk;
mod cache;
mod compaction;
mod flush;
mod lifecycle;
mod manifest;
mod memtable;
mod merge;
mod options;
#[cfg(test)]
mod probes;
mod read;
mod scan;
mod stats;
pub use background::spawn_bg_compactor;
#[cfg(test)]
use compaction::ranges_overlap;
#[cfg(test)]
use manifest::manifest_publishes;
#[cfg(test)]
use manifest::manifest_publishes_reference;
pub use merge::LightMerge;
pub(crate) use merge::VacuumMerge;
pub use options::LSMTreeOptions;
#[cfg(test)]
pub(crate) use probes::mem_scan_steps;
#[cfg(test)]
pub(crate) use probes::merge_activations;
#[cfg(test)]
pub(crate) use probes::note_mem_scan_steps;
#[cfg(test)]
use probes::note_source_activation;
