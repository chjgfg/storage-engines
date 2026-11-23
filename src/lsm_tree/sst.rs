//! SSTable：不可变有序文件。
//!
//! ## 格式
//! - **v1** `LSMSST01`：扁平 entries（只读兼容，seq 视为 0）
//! - **v2** `LSMSST02`：data blocks + 稀疏索引 + bloom + CRC；entry 含 `seq u64`
//! - **v3** 同 v2 布局，entry 强制写 seq（写路径只用 v3 magic 在 footer 区分；实现上统一用 v2 布局 + seq）
//!
//! 读：bloom → index → block（mmap 或 pread + LRU cache）
//! compact：[`SstEntryIter`] 流式 + `merge_to_builder`

#[cfg(test)]
use std::thread_local;
use std::{
    cmp::Ordering as CmpOrdering,
    collections::{BinaryHeap, HashMap},
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    vec,
};
use std::{debug_assert, unreachable};

use memmap2::Mmap;

use crate::lsm_tree::bloom::Bloom;
use crate::lsm_tree::storage::crc32;

#[cfg(test)]
pub(crate) mod positioned_read_reference;

const SST_MAGIC_V1: &[u8; 8] = b"LSMSST01";
const SST_MAGIC_V2: &[u8; 8] = b"LSMSST02";

/// 逻辑值类型
#[derive(Debug, PartialEq, Eq)]
pub enum EntryValue {
    Put(Option<Vec<u8>>),
    Drop,
}

// M41a 的尺子：值被**复制**（而不是从 memtable 搬进 SST builder）了几次。
//
// 为什么钉这个而不是钉内存：memtable → SST 那条路本来一条都不用复制（`MemMap` 里的条目按值
// 进循环、直接搬走），旧形状是"整张 memtable 连键带值克隆进一个 `Vec` 再 sort"。复制次数是
// 从真实程序状态数出来的 ⇒ 退回旧形状它当场涨，不像进程级内存那样会被并行用例污染（M27/M36
// 各栽过一次）。
#[cfg(test)]
thread_local! {
    static ENTRY_VALUE_CLONES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// 带写序号的存储条目（snapshot：仅 `seq <= snap_seq` 可见）
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredEntry {
    pub seq: u64,
    pub value: EntryValue,
}

// ─── Block cache ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct DataBlock {
    entries: Vec<(Vec<u8>, StoredEntry)>,
}

#[derive(Debug)]
struct PackedRow {
    key: std::ops::Range<u32>,
    value: std::ops::Range<u32>,
    seq: u64,
    kind: u8,
}

#[derive(Debug)]
struct PackedBlock {
    raw: Vec<u8>,
    rows: Vec<PackedRow>,
}

#[derive(Clone, Copy)]
struct EntryRef<'a> {
    seq: u64,
    kind: u8,
    value: &'a [u8],
}

#[derive(Debug, Clone)]
enum CachedBlock {
    Decoded(Arc<DataBlock>),
    Packed(Arc<PackedBlock>),
}

/// 候选持有共享块，使释放树锁后仍无需复制 SST 值；blob 指针保留原有锁内点查，因为释放锁后其文件可能被回收。
pub enum ScanRow {
    Inline(InlineRow),
    BlobLookup(Vec<u8>),
    /// 历史元数据同序号时优先首个来源，点查值却优先最后来源；必须分别保留原契约，不能凭单行推测。
    AmbiguousLookup(Vec<u8>),
    #[cfg(test)]
    ReferenceLookup(Vec<u8>),
}

pub struct InlineRow(InlineStorage);
enum InlineStorage {
    Owned(EntryValue),
    Block(CachedBlock, usize),
}

pub(crate) struct SourceRow<'a> {
    pub(crate) seq: u64,
    value: SourceValue<'a>,
    ambiguous: bool,
}
enum SourceValue<'a> {
    Borrowed(&'a EntryValue),
    Block(CachedBlock, usize),
}

pub(crate) enum SearchSourceRow {
    Classified(crate::common::store::SearchRow<ScanRow>),
    Lookup(ScanRow),
}

/// 配对探针冻结 M123 之前的块遍历和复制值点查。
#[cfg(test)]
pub(crate) mod point_read_reference;

#[cfg(test)]
pub(crate) mod packed_scan_reference;
#[cfg(test)]
thread_local! { static PACKED_PROMOTIONS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) }; }

// 测试专用：本线程的块访问次数（含块缓存命中）。
//
// LSM 的代价对应物是「读了多少个数据块」—— 用来把「点操作代价不随版本链长度/库总量
// 增长」这条不变量钉成断言。做成 thread_local 是为了不和 `cargo test` 的并行用例互相计数。
#[cfg(test)]
thread_local! {
    static BLOCK_READS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

// 测试专用：本线程「块缓存未命中、真的按 offset 去读文件」的次数。
//
// `block_reads` 把命中和未命中算成同一个数，所以它守的是「代价不随链长/库总量涨」，
// 却**看不见墙钟时间的来源**。下游实测单次点查 0.86ms，而块访问只有个位数 —— 差额就在
// 未命中这几步真读上：改造前每次未命中还要额外 `File::open`（本机实测一次 open ≈ 167µs，
// 见 `test_lsm_point_read_baseline_5000_keys` 的"乱序 vs 有序"之差）。所以量这个，不量 open 数。
#[cfg(test)]
thread_local! {
    static BLOCK_MISSES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

// 测试专用：本线程在稀疏索引上比了几次（= `find_block` 的循环次数）。
//
// 这是"点查随库总量变贵"的另一条通道：块访问数看不出一个块内部干了什么，而索引若是线性扫，
// 每探一个块就要走上 ~块数 次比较 ⇒ O(行数/64)。二分成对数之后这个计数应当只跟 log2(块数)
// 同阶，守卫就按这个断言，不看墙钟。
#[cfg(test)]
thread_local! {
    static INDEX_PROBES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

// 测试专用：窗口折叠里**看了多少把编码键**（轻量视图的遍历量，不含取值）。
// 与 bitcask 的 `keydir_visits` 同一角色：只数取值的尺子抓不到"键流照样走到底"，
// 第一版守卫就是量 `block_reads()`，把预算整个忽略掉它照样绿。
#[cfg(test)]
thread_local! {
    static LIGHT_VISITS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

// 测试专用：有多少条目被 `SstFile::iter_entries()` **整篇物化**进内存（M33）。
//
// 为什么还要一把计数尺：改流式源之后"没有整篇读进来"这件事必须能被断言。只看输出正确性
// 永远抓不到"照样先 collect 再归并"的退化版；只看内存又必须单线程跑（分配计数是进程级的，
// 并行用例会互相抬高峰值 —— M27 就是栽在这里）。这条计数把"整篇读进 Vec"直接数出来。
// ⚠️ 单位 = **条目数**，不是"调了几次 `iter_entries()`"：要抓的是"搬进内存多少行"。
// 与 `note_light_visit` 同为**线程内**计数 ⇒ 断言一律取差值。
#[cfg(test)]
thread_local! {
    static MERGE_INPUTS_MATERIALIZED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

// 测试专用：有多少条被 `SstFile::light_view()` **整篇物化**（M39）。
//
// 与上一条同形，但盯的是**轻量视图**那条路：`health()` 以前走 `scan_latest_light`，而它
// 把每个 SST 用 `light_view()` 整篇读进 `Vec`（所有源同时活着）⇒ 600k 档一次 `health()`
// 瞬时 **+126.1 MB**（旧内存量具报 +0.0，看不见 —— M38 修好尺子才露出来）。
// ⚠️ 单位 = 条目数。改走惰性 `light_iter_range` 之后这里必须始终是 0。
#[cfg(test)]
thread_local! {
    static LIGHT_VIEWS_MATERIALIZED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

// 测试专用：一次点读在 SST 里**比较过多少个条目键**，以及探过多少个 SST 文件。
//
// 为什么必须分开数：bloom 通过之后，取那一行有两条路 —— 稀疏索引二分 + 定位块（`find_block`），
// 或者在 `entries_cache` 那份"整文件条目"上从头扫。两者差着文件大小的量级，而"探了几个文件"
// 与"读了几块"都看不见这笔账（M23 归因时就是被这一格卡住：5002 键与 40000 键两档的块/次都稳定在
// 4.00、条目比较/次 128，而乱序点读是 27.9 → 117.3 µs/次 ⇒ 钱既不在访问次数也不在比较，
// 只在"其中几次真去读盘"上）。
#[cfg(test)]
thread_local! {
    static SST_ENTRY_WALKS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static SST_PROBES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

// 测试专用：本线程「把带 value 的条目整个物化出来」的次数。
//
// 这是全库折叠那条通道真正的代价单位：重量路径（`iter_all` / `mem_iter_at`）每行都要
// 克隆 value；轻量路径（`light_view` / `mem_view_light`）只带 seq 与种类。所以 ② 用这个
// 量来守，而不是用块读取次数（块是省不掉的，value 才省得掉）。
#[cfg(test)]
thread_local! {
    static VALUE_MATERIALIZATIONS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
struct CacheKey {
    /// 区分不同 CF / 目录（避免共享 cache 时 sst_id 碰撞）
    ns: u64,
    sst_id: u64,
    offset: u64,
}

// 测试专用：本线程「动一次块缓存时检查过几个槽位」。
//
// M23 自己引入的回归留下的账：旧实现在**每一次命中**里做 `order.iter().position()` ⇒ 代价是
// O(驻留块数)，而自动档正是去加大驻留块数的那一刀，于是"缓存越大、每次命中越贵"，收益被自己的
// LRU 吃回去（40000 键那一档容量 256→1024，未命中次数不动 0.15/次，有序点读 22.2→36.0 µs/次）。
// 改成定长槽 + 双向链表之后每次命中只碰常数个槽，这条尺子才看得见区别。
#[cfg(test)]
thread_local! {
    static LRU_INSPECTIONS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

const NIL: usize = usize::MAX;

/// 一个驻留块 + 它在 LRU 链表里的前后位置（槽下标，`NIL` = 无）
#[derive(Debug)]
struct Slot {
    key: CacheKey,
    block: CachedBlock,
    prev: usize,
    next: usize,
}

#[derive(Debug)]
pub struct BlockCache {
    cap: usize,
    /// key → 槽下标
    map: HashMap<CacheKey, usize>,
    /// 定长槽位表（只长不缩：一个槽 ≈ 48 B，上限 4096 槽也就 200 KB，比反复回收省心）
    slots: Vec<Option<Slot>>,
    /// 空闲槽下标栈
    free: Vec<usize>,
    /// LRU 链表的头（最久未用）与尾（刚用过）
    head: usize,
    tail: usize,
}

#[derive(Debug, Clone)]
struct IndexEntry {
    first_key: Vec<u8>,
    offset: u64,
    len: u32,
}

// ─── SstFile ─────────────────────────────────────────────────────────────────

/// 只有观察过每条记录的构造器才能证明无需重写即可迁层。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MoveEligibility {
    Unknown,
    RewriteRequired,
    Ready,
}

pub struct SstFile {
    pub id: u64,
    pub path: PathBuf,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
    pub size_bytes: u64,
    move_eligibility: MoveEligibility,
    entries_cache: Option<Vec<(Vec<u8>, StoredEntry)>>,
    index: Vec<IndexEntry>,
    bloom: Bloom,
    is_v2: bool,
    /// entry 是否带 seq 字段（v2 写路径 true；v1 false）
    has_seq: bool,
    block_cache: Option<Arc<Mutex<BlockCache>>>,
    mmap: Option<Mmap>,
    /// 常驻的只读句柄，**懒开**：第一次真读时 `File::open` 一次，之后按 offset 读不再开文件。
    ///
    /// 为什么必须有它：SST 是不可变的、路径按 id 唯一，所以句柄可以一直用到对象销毁；而没有它，
    /// 每次块缓存未命中都要重开一次文件 —— 本机实测一次 `File::open` ≈ 167µs，比读那 4KB 字节
    /// 贵一个量级，于是"块访问次数只有个位数"的点查照样能慢到 0.86ms（下游现场就是这个数）。
    /// Windows 下 std 打开文件带 `FILE_SHARE_DELETE`，所以 compact 之后删旧 SST 仍然成功，
    /// 这个句柄不需要显式关闭。
    handle: Mutex<Option<File>>,
    /// block cache 命名空间（CF 隔离）
    cache_ns: u64,
}

/// 流式 SST 迭代器（按 block 推进，不一次载入全表）
pub struct SstEntryIter<'a> {
    sst: &'a SstFile,
    block_i: usize,
    entry_i: usize,
    cur: Option<Arc<DataBlock>>,
    cache_i: usize,
}

/// 新格式块只解码一次再移动；v1 驻留记录显式共享回退。压实不填充点查缓存。
pub(crate) enum CompactionRows<'a> {
    OwnedBlocks {
        sst: &'a SstFile,
        block: usize,
        rows: std::vec::IntoIter<(Vec<u8>, StoredEntry)>,
    },
    LegacyShared(std::slice::Iter<'a, (Vec<u8>, StoredEntry)>),
    #[cfg(test)]
    CachedReference(SstEntryIter<'a>),
}

#[cfg(test)]
pub(crate) mod compaction_reference;

// ─── SstBuilder ──────────────────────────────────────────────────────────────

#[cfg(test)]
pub(crate) mod packed_point_reference;

pub struct SstBuilder {
    path: PathBuf,
    id: u64,
    block_entries: usize,
    enable_bloom: bool,
    bloom_bits_per_key: usize,
    /// **只有一块**的量：攒到 `block_entries` 就编块落盘（M42）。
    ///
    /// 为什么必须拆：改前这里是"整张表的条目"，而一张表切多大由 `target_sst_bytes` 说话
    /// ⇒ 压实与 flush 剩下的那条内存下限由它定。`compaction_cost_split` 600k 档实测
    /// 单个 builder 最多同时握住 **18 333 条**，按同一批条目量出来的 269 B/条 算 ≈ **4.9 MB**，
    /// 而那一段的总峰值是 6.3 MB ⇒ **78% 就是这块缓冲**（条目载荷本身只 1.9 MB）。
    pending: Vec<(Vec<u8>, StoredEntry)>,
    /// 每条 **8 B** 的哈希对 `(h1, h2)`（两个都是 crc32 值）。
    ///
    /// ⚠️ 为什么还留着这一份而不是做到纯 O(块)：布隆的位图宽度 `m = 总条数 × bits_per_key`
    /// 要等总条数知道才能定，而位位置是 `h % m` ⇒ 边加边设位就会与已写的格式对不上。
    /// 存哈希而不是存键值：18 333 条 = 143 KB，改前那一整张表是 MB 级。
    /// `finish` 里按最终的 `m` 逐对设位，位位置与原来"逐键 `insert`"**一字不差**。
    hashes: Vec<(u32, u32)>,
    /// 每块一条，随块落盘一起追加（块数 = 条目数 ÷ 块宽 ⇒ O(文件) 但每 64 条才一条）
    index: Vec<IndexEntry>,
    /// 第一个**满块**写出时才建文件；不足一块的表与改前一样，到 `finish` 才 create
    writer: Option<std::io::BufWriter<File>>,
    /// 已经写进文件的字节数（自己累计 ⇒ 不必每块 `stream_position()` 把缓冲冲一次）
    written: u64,
    /// 每块尾键的克隆（**一块一次**，不是每条一次）：footer 的 `max_key` 与有序断言都从它拿
    tail_key: Option<Vec<u8>>,
    count: usize,
    /// `pending` 的字节账，**增量维护**（M30）。
    ///
    /// 为什么必须是计数器而不是现算：`merge_to_ssts` 每交出一行就问一次
    /// `estimated_bytes() >= target_bytes`，而旧实现是 `entries.iter().map(...).sum()` ——
    /// 于是**每个输出文件**内部是 O(表内行数²)。600k 行压实的实测拆分：整条 18.0 s，其中
    /// "归并+写盘"占 17.5 s（94%），而"物化输入"只有 0.6 s、"纯归并"0.5 s ⇒
    /// 每输入行摊到 ≈30 µs，正好是"一张表 ~19 000 行、每行重求和半表"的常数量级。
    /// 这条与 M19（`flush` 判定每插一行数一遍 memtable）是同一个病的第二次出现，
    /// 所以口径那句 (`k.len() + se.estimate_bytes() + 8`) 一字不动地搬进增量里，
    /// 并留 `sum_entries_bytes` 当对拍对照 + 一把"为判定走过多少条目"的尺子。
    est_bytes: usize,
    /// `add` 里没有地方返回 IO 错误（`pub fn add(&mut self, …)` 四处调用方在用，改签名是破坏性的）
    /// ⇒ 第一个错误存下来，`finish()` 如实抛出。**不吞错，也不改公开签名。**
    io_err: Option<std::io::Error>,
    move_eligibility: MoveEligibility,
}

// 测试专用尺子：一次"要不要切文件"的判定**走过了多少个条目**。
// 生产路径必须是 0；`estimated_bytes_slow`（对拍用）负责证明这把尺子不是空转。
#[cfg(test)]
thread_local! {
    static BUILDER_SIZE_WALKS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

// 测试专用尺子（M42 的先量）：**一个 builder 同时握住多少条还没落盘的条目**的历史最大值。
//
// 为什么要先量它：M33 把压实的**输入**改成流式源之后，输出侧仍然"攒满一整张表才在 `finish()`
// 里编块写盘" —— 于是 flush 与压实各自还剩一条"1×"下限，是这块缓冲定的，不是流式源能顺手带走的。
// 要不要拆它、拆完能省多少，看的是这一个数（条目数 × 每条字节），不是猜。
#[cfg(test)]
thread_local! {
    static BUILDER_HELD_MAX: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

// 测试专用尺子（M42 剩下的那一小截）：布隆哈希表最长有多少条。
//
// 为什么单独一把：块是"攒满就落盘"了，但布隆的 `m` 要等总条数 ⇒ 每条还留 8 B 哈希。
// 这一份是 **O(文件)** 的残留（18 333 条 = 143 KB，而不是改前那一整张表的 MB 级），
// 把它和"未落盘条目数"分开钉，是为了下次谁再动这块时能一眼看见"还剩多少、为什么留"。
#[cfg(test)]
thread_local! {
    static BUILDER_HASHED_MAX: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// 轻量归并项：**只有 seq 与种类字节，不带 value 字节**。
///
/// `kind` 与 [`EntryValue::kind_byte`] 对齐：`0=Put(Some)`、`1=Put(None)`、`2=Drop`。
/// 全库折叠（`collect_latest_raw` 那一类）只需要选出「每个键的胜者」，
/// value 留给少数胜出行再点查 —— 于是一次扫描的峰值内存从 O(库字节) 降到 O(键字节)。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LightEntry {
    pub seq: u64,
    pub kind: u8,
}

/// 多路归并：**同键取 `seq` 最大者**，seq 相同才按源序（sources 传进来时新→旧）。
///
/// 为什么主判据是 seq 而不是源序（M26 改的，原先是"源序优先，同序才比 seq"）：
/// 跨层压实之后，**源序回答不了"谁是新版"** —— L1→L2 的压实会用**新 id** 写出**老数据**，
/// 而同层两个范围重叠的文件里同一把键的先后更是没有元数据可依据（层内只按 `min_key` 排序）。
/// seq 才是那份权威：它和跨源点查 `LSMTree::get_entry_at`（取所有源里最大的可见 seq）用的是
/// 同一个判据 ⇒ **折叠的答案 == 逐把点查的答案**。正常形状里两者本来就一致（后建的 L0 文件
/// 只会装更晚的 seq、内存表比任何已落盘的都新），所以这次改动对既有路径是**语义不变**；
/// 变的是重叠/崩溃恢复那两种以前会给出错误答案的形状。
/// 归并的一路输入：**一个按键升序的迭代器**（M33）。
///
/// 为什么不是 `Vec<(键, 条目)>`：上一版的签名逼着每个调用点先把**整个输入文件**读进内存
/// （`iter_entries()` = `iter_all().collect()`），于是一次压实的瞬时内存 ≈ 所有输入文件之和 ——
/// 实测 600k 行 / 61.7 MB 的输入，物化那一步峰值涨到 **154 MB**（每行摊 269 B，而条目本身
/// 只有 108 B：多出来的是每行两个堆分配 + `Vec` 扩容翻倍），下游那台 1 核 6 GB 的机器
/// 给的 RSS 预算是 80 MB ⇒ 一次压实自己就把预算吃穿了。
/// 改成一路一个迭代器之后，同一轮压实只需要"每源当前那一块 + 一个 builder"。
///
/// 用 `Box<dyn>` 而不是泛型参数：源的形状本来就是混合的（内存表切出来是 `Vec`、
/// SST 是 `SstEntryIter`、读路径还带 `filter`），泛型要么要求同构要么逼每个调用点写类型名；
/// 而"一路一个盒子"的开销 = 输入文件数（个位数到几十），相比每行的克隆可以忽略。
pub type MergeInput<'a, T> = Box<dyn Iterator<Item = (Vec<u8>, T)> + 'a>;
pub(crate) type TryMergeInput<'a, T, E> = Box<dyn Iterator<Item = Result<(Vec<u8>, T), E>> + 'a>;

/// 只有未参与归并的来源不可能暴露旧值时，才可真正丢弃胜者；否则保留其序号作为删除屏障。
pub(crate) enum CompactionAction {
    Keep,
    Discard,
    RetainDeletion,
}

/// 发生错误时，清理全部已完成但尚未发布的输出及其句柄。
pub(crate) struct UnpublishedSsts(pub(crate) Vec<SstFile>);

#[cfg(test)]
mod tests;

/// [`SstFile::light_iter`] 的状态机：一块一块读、一条一条吐。
///
/// ⚠️ 只前进不回退：块内条目按 key 升序 ⇒ 越过 `to` 就 `None` 收尾，后面的块不再读。
struct SstLightIter<'a, R> {
    sst: &'a SstFile,
    block: usize,
    pos: usize,
    cur: Option<CachedBlock>,
    from: Option<Vec<u8>>,
    to: Option<Vec<u8>>,
    snap_seq: u64,
    project: fn(&CachedBlock, usize) -> R,
}

pub(crate) enum VacuumLightInput<'a> {
    Empty,
    OwnedBlocks(VacuumLightRows<'a>),
    LegacyShared(Box<dyn Iterator<Item = std::io::Result<(Vec<u8>, LightEntry)>> + 'a>),
}

pub(crate) struct VacuumLightRows<'a> {
    sst: &'a SstFile,
    block: usize,
    rows: VacuumBlockRows,
    from: Option<&'a [u8]>,
    to: Option<&'a [u8]>,
    snap_seq: u64,
    done: bool,
}

/// 两条分支都拥有键，只有对照分支会物化值载荷。
enum VacuumBlockRows {
    Metadata(std::vec::IntoIter<(Vec<u8>, LightEntry)>),
    #[cfg(all(test, feature = "lsm-allocation-reference"))]
    Payload(std::vec::IntoIter<(Vec<u8>, StoredEntry)>),
}

// 按职责组织的内部实现；公开类型与原模块路径保持一致。
mod block_cache;
mod builder;
mod codec;
mod entry;
mod file_io;
mod merge;
mod packed_block;
#[cfg(test)]
mod probes;
mod read;
mod scan;
#[cfg(test)]
use builder::sum_entries_bytes;
use codec::decode_block;
use codec::decode_light_block;
use codec::encode_block;
use codec::encode_index;
use codec::open_v1;
use codec::open_v2_from_footer;
#[cfg(any(unix, windows))]
#[cfg(test)]
use file_io::read_exact_positioned;
pub use merge::MergeRow;
pub use merge::MergeSeq;
pub use merge::merge_latest;
pub use merge::merge_latest_streaming;
pub use merge::merge_streaming;
pub use merge::merge_to_ssts;
pub(crate) use merge::merge_to_ssts_with_action;
pub use merge::vec_inputs;
#[cfg(test)]
pub(crate) use probes::block_misses;
#[cfg(test)]
pub(crate) use probes::block_reads;
#[cfg(test)]
pub(crate) use probes::builder_hashed_max;
#[cfg(test)]
pub(crate) use probes::builder_held_max;
#[cfg(test)]
pub(crate) use probes::builder_size_walks;
#[cfg(test)]
pub(crate) use probes::entry_value_clones;
#[cfg(test)]
pub(crate) use probes::index_probes;
#[cfg(test)]
pub(crate) use probes::light_view_materialized;
#[cfg(test)]
pub(crate) use probes::light_visits;
#[cfg(test)]
pub(crate) use probes::lru_inspections;
#[cfg(test)]
pub(crate) use probes::merge_inputs_materialized;
#[cfg(test)]
pub(crate) use probes::note_light_view_materialized;
#[cfg(test)]
pub(crate) use probes::note_light_visit;
#[cfg(test)]
use probes::note_lru_inspection;
#[cfg(test)]
pub(crate) use probes::note_merge_input_materialized;
#[cfg(test)]
pub(crate) use probes::note_sst_entry_walks;
#[cfg(test)]
pub(crate) use probes::note_sst_probe;
#[cfg(test)]
pub(crate) use probes::note_value_materialization;
#[cfg(test)]
pub(crate) use probes::packed_promotions;
#[cfg(test)]
pub(crate) use probes::reset_builder_hashed_max;
#[cfg(test)]
pub(crate) use probes::reset_builder_held_max;
#[cfg(test)]
pub(crate) use probes::sst_entry_walks;
#[cfg(test)]
pub(crate) use probes::sst_probes;
#[cfg(test)]
pub(crate) use probes::value_materializations;
