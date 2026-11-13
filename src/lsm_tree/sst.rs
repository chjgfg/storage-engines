//! SSTable：不可变有序文件。
//!
//! ## 格式
//! - **v1** `LSMSST01`：扁平 entries（只读兼容，seq 视为 0）
//! - **v2** `LSMSST02`：data blocks + 稀疏索引 + bloom + CRC；entry 含 `seq u64`
//! - **v3** 同 v2 布局，entry 强制写 seq（写路径只用 v3 magic 在 footer 区分；实现上统一用 v2 布局 + seq）
//!
//! 读：bloom → index → block（mmap 或 pread + LRU cache）
//! compact：[`SstEntryIter`] 流式 + [`merge_to_builder`]

use std::debug_assert;
#[cfg(test)]
use std::thread_local;
use std::{
    cmp::Ordering as CmpOrdering, collections::{BinaryHeap, HashMap}, fs::File, io::{Read, Seek, SeekFrom, Write}, path::{Path, PathBuf}, sync::{Arc, Mutex}, vec,
};

use memmap2::Mmap;

use crate::lsm_tree::bloom::Bloom;
use crate::lsm_tree::storage::crc32;

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

/// 当前线程复制过的 `EntryValue` 条数（测试探针用）
#[cfg(test)]
pub(crate) fn entry_value_clones() -> u64 {
    ENTRY_VALUE_CLONES.with(|c| c.get())
}

// `Clone` 手写而不是 `#[derive]`，就为了在这一个地方把上面那把尺子记上 ——
// 派生实现照样会逐字段复制，只是数不出来。
impl Clone for EntryValue {
    fn clone(&self) -> Self {
        #[cfg(test)]
        ENTRY_VALUE_CLONES.with(|c| c.set(c.get() + 1));
        match self {
            EntryValue::Put(v) => EntryValue::Put(v.clone()),
            EntryValue::Drop => EntryValue::Drop,
        }
    }
}

impl EntryValue {
    pub fn kind_byte(&self) -> u8 {
        match self {
            EntryValue::Put(Some(_)) => 0,
            EntryValue::Put(None) => 1,
            EntryValue::Drop => 2,
        }
    }

    pub fn from_parts(kind: u8, val: Option<Vec<u8>>) -> Option<Self> {
        match kind {
            0 => Some(EntryValue::Put(val)),
            1 => Some(EntryValue::Put(None)),
            2 => Some(EntryValue::Drop),
            _ => None,
        }
    }

    pub fn estimate_bytes(&self) -> usize {
        match self {
            EntryValue::Put(Some(v)) => v.len() + 1,
            EntryValue::Put(None) | EntryValue::Drop => 1,
        }
    }
}

/// 带写序号的存储条目（snapshot：仅 `seq <= snap_seq` 可见）
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredEntry {
    pub seq: u64,
    pub value: EntryValue,
}

impl StoredEntry {
    pub fn put(seq: u64, v: Option<Vec<u8>>) -> Self {
        Self {
            seq,
            value: EntryValue::Put(v),
        }
    }

    pub fn drop_at(seq: u64) -> Self {
        Self {
            seq,
            value: EntryValue::Drop,
        }
    }

    pub fn estimate_bytes(&self) -> usize {
        8 + self.value.estimate_bytes()
    }

    pub fn visible_at(&self, snap_seq: u64) -> bool {
        self.seq <= snap_seq
    }
}

// ─── Block cache ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct DataBlock {
    entries: Vec<(Vec<u8>, StoredEntry)>,
}

/// Freeze the pre-M123 block walk and value-copying lookup for paired probes.
#[cfg(test)]
pub(crate) mod point_read_reference {
    use std::{assert, cell::Cell, thread_local};
    thread_local! { static OLD: Cell<bool> = const { Cell::new(false) }; }
    pub(crate) fn enabled() -> bool { OLD.with(Cell::get) }
    pub(crate) struct Scope;
    impl Scope {
        pub(crate) fn start() -> Self {
            OLD.with(|old| assert!(!old.replace(true)));
            Self
        }
    }
    impl Drop for Scope { fn drop(&mut self) { OLD.with(|old| old.set(false)); } }
}

// 测试专用：本线程的块访问次数（含块缓存命中）。
//
// LSM 的代价对应物是「读了多少个数据块」—— 用来把「点操作代价不随版本链长度/库总量
// 增长」这条不变量钉成断言。做成 thread_local 是为了不和 `cargo test` 的并行用例互相计数。
#[cfg(test)]
thread_local! {
    static BLOCK_READS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// 当前线程已发生的数据块访问次数（测试探针用）
#[cfg(test)]
pub(crate) fn block_reads() -> u64 {
    BLOCK_READS.with(|c| c.get())
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

/// 当前线程的 SST 块缓存未命中次数（测试探针用）
#[cfg(test)]
pub(crate) fn block_misses() -> u64 {
    BLOCK_MISSES.with(|c| c.get())
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

/// 当前线程在 SST 稀疏索引上做过的比较次数（测试探针用）
#[cfg(test)]
pub(crate) fn index_probes() -> u64 {
    INDEX_PROBES.with(|c| c.get())
}

// 测试专用：窗口折叠里**看了多少把编码键**（轻量视图的遍历量，不含取值）。
// 与 bitcask 的 `keydir_visits` 同一角色：只数取值的尺子抓不到"键流照样走到底"，
// 第一版守卫就是量 `block_reads()`，把预算整个忽略掉它照样绿。
#[cfg(test)]
thread_local! {
    static LIGHT_VISITS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn note_light_visit() {
    LIGHT_VISITS.with(|c| c.set(c.get() + 1));
}

/// 当前线程折候选时检查过的编码键数（测试探针用）
#[cfg(test)]
pub(crate) fn light_visits() -> u64 {
    LIGHT_VISITS.with(|c| c.get())
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

#[cfg(test)]
pub(crate) fn note_merge_input_materialized(n: u64) {
    MERGE_INPUTS_MATERIALIZED.with(|c| c.set(c.get() + n));
}

/// 当前线程被整篇物化进内存的条目数（测试探针用）
#[cfg(test)]
pub(crate) fn merge_inputs_materialized() -> u64 {
    MERGE_INPUTS_MATERIALIZED.with(|c| c.get())
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

#[cfg(test)]
pub(crate) fn note_light_view_materialized(n: u64) {
    LIGHT_VIEWS_MATERIALIZED.with(|c| c.set(c.get() + n));
}

#[cfg(test)]
pub(crate) fn light_view_materialized() -> u64 {
    LIGHT_VIEWS_MATERIALIZED.with(|c| c.get())
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

/// 记一次「点读真的进了这个 SST 文件」（范围 + bloom 都过了才算；被 bloom 挡掉的不算代价）
#[cfg(test)]
pub(crate) fn note_sst_probe() {
    SST_PROBES.with(|c| c.set(c.get() + 1));
}

/// 记 n 次「条目键比较」
#[cfg(test)]
pub(crate) fn note_sst_entry_walks(n: u64) {
    SST_ENTRY_WALKS.with(|c| c.set(c.get() + n));
}

/// 当前线程点读探过的 SST 文件数（测试探针用）
#[cfg(test)]
pub(crate) fn sst_probes() -> u64 {
    SST_PROBES.with(|c| c.get())
}

/// 当前线程点读比较过的 SST 条目键数（测试探针用）
#[cfg(test)]
pub(crate) fn sst_entry_walks() -> u64 {
    SST_ENTRY_WALKS.with(|c| c.get())
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

/// 当前线程已物化过多少个带 value 的条目（测试探针用）
#[cfg(test)]
pub(crate) fn value_materializations() -> u64 {
    VALUE_MATERIALIZATIONS.with(|c| c.get())
}

/// 记一次「带 value 的条目被物化」；轻量路径不该调用它
#[cfg(test)]
pub(crate) fn note_value_materialization() {
    VALUE_MATERIALIZATIONS.with(|c| c.set(c.get() + 1));
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

#[cfg(test)]
fn note_lru_inspection() {
    LRU_INSPECTIONS.with(|c| c.set(c.get() + 1));
}

/// 当前线程检查过的缓存槽位数（测试探针用）
#[cfg(test)]
pub(crate) fn lru_inspections() -> u64 {
    LRU_INSPECTIONS.with(|c| c.get())
}

const NIL: usize = usize::MAX;

/// 一个驻留块 + 它在 LRU 链表里的前后位置（槽下标，`NIL` = 无）
#[derive(Debug)]
struct Slot {
    key: CacheKey,
    block: Arc<DataBlock>,
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

impl BlockCache {
    pub fn new(cap: usize) -> Self {
        Self {
            cap,
            map: HashMap::new(),
            slots: Vec::new(),
            free: Vec::new(),
            head: NIL,
            tail: NIL,
        }
    }

    #[inline]
    fn slot(&self, i: usize) -> &Slot {
        #[cfg(test)]
        note_lru_inspection();
        debug_assert!(i != NIL && i < self.slots.len());
        self.slots[i].as_ref().expect("槽位在用却为空")
    }

    #[inline]
    fn slot_mut(&mut self, i: usize) -> &mut Slot {
        #[cfg(test)]
        note_lru_inspection();
        debug_assert!(i != NIL && i < self.slots.len());
        self.slots[i].as_mut().expect("槽位在用却为空")
    }

    /// 把槽 i 从链表摘下来（不动内容）
    fn detach(&mut self, i: usize) {
        let (p, n) = {
            let s = self.slot(i);
            (s.prev, s.next)
        };
        match p {
            NIL => self.head = n,
            p => self.slot_mut(p).next = n,
        }
        match n {
            NIL => self.tail = p,
            n => self.slot_mut(n).prev = p,
        }
        let s = self.slot_mut(i);
        s.prev = NIL;
        s.next = NIL;
    }

    /// 把槽 i 挂到链表尾（= 最近用过）
    fn attach_back(&mut self, i: usize) {
        // 先把 tail 抄出来：`slot_mut` 借走 self 之后不能再读 self.tail
        let t = self.tail;
        {
            let s = self.slot_mut(i);
            s.prev = t;
            s.next = NIL;
        }
        if t == NIL {
            self.head = i;
        } else {
            self.slot_mut(t).next = i;
        }
        self.tail = i;
    }

    /// 换出最久未用的那块
    fn evict_lru(&mut self) {
        let i = self.head;
        if i == NIL {
            return;
        }
        let key = self.slot(i).key.clone();
        self.detach(i);
        self.slots[i] = None;
        self.map.remove(&key);
        self.free.push(i);
    }

    /// 取一个可用槽：先把表长到容量，再用回收来的空闲槽
    fn take_slot(&mut self) -> usize {
        if self.slots.len() < self.cap {
            self.slots.push(None);
            return self.slots.len() - 1;
        }
        self.free
            .pop()
            .expect("未满的缓存里必须有空闲槽（不变量：slots.len()==cap=map.len()+free.len()）")
    }

    fn get(&mut self, ns: u64, sst_id: u64, offset: u64) -> Option<Arc<DataBlock>> {
        let k = CacheKey { ns, sst_id, offset };
        let i = *self.map.get(&k)?;
        self.detach(i);
        self.attach_back(i);
        Some(self.slot(i).block.clone())
    }

    fn put(&mut self, ns: u64, sst_id: u64, offset: u64, block: Arc<DataBlock>) {
        if self.cap == 0 {
            return;
        }
        let k = CacheKey { ns, sst_id, offset };
        if let Some(&i) = self.map.get(&k) {
            self.slot_mut(i).block = block;
            self.detach(i);
            self.attach_back(i);
            return;
        }
        while self.map.len() >= self.cap {
            self.evict_lru();
        }
        let i = self.take_slot();
        self.slots[i] = Some(Slot {
            key: k.clone(),
            block,
            prev: NIL,
            next: NIL,
        });
        self.map.insert(k, i);
        self.attach_back(i);
    }

    /// 改容量：**不重建缓存**，只换上限。
    ///
    /// 调大不用做事（下一次回填自然装得下）；调小按 LRU 顺序换出到上限为止。
    /// 必须有这条路，否则"跟着库长"的自动档只能靠 `BlockCache::new` 把已装进来的块全丢掉 ——
    /// 那等于每写满一次 memtable 就把缓存清空，自动档反而让点读更贵。
    pub fn set_capacity(&mut self, cap: usize) {
        self.cap = cap;
        while self.map.len() > cap {
            self.evict_lru();
        }
    }

    /// 当前上限（块数）。自动档的守卫读的是这个生效值，不是配置值。
    pub fn capacity(&self) -> usize {
        self.cap
    }

    /// 当前驻留块数
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// 与 [`Self::len`] 配对（clippy 要求 `len` 必须有 `is_empty`）
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

#[derive(Debug, Clone)]
struct IndexEntry {
    first_key: Vec<u8>,
    offset: u64,
    len: u32,
}

// ─── SstFile ─────────────────────────────────────────────────────────────────

/// Only builders that observed every entry can certify a no-rewrite move.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MoveEligibility { Unknown, RewriteRequired, Ready }

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

// Manual Debug without dumping mmap
impl std::fmt::Debug for SstFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SstFile")
            .field("id", &self.id)
            .field("path", &self.path)
            .field("size_bytes", &self.size_bytes)
            .field("is_v2", &self.is_v2)
            .field("has_seq", &self.has_seq)
            .field("mmap", &self.mmap.is_some())
            .finish()
    }
}

impl SstFile {
    /// Maintenance owns decoded blocks; legacy resident rows borrow their value.
    pub(crate) fn vacuum_light_iter<'a>(&'a self, from: Option<&'a [u8]>, to: Option<&'a [u8]>, snap_seq: u64)
        -> VacuumLightInput<'a> {
        if !self.may_overlap_half_open(from, to) { return VacuumLightInput::Empty; }
        if self.entries_cache.is_some() {
            return VacuumLightInput::LegacyShared(Box::new(self.light_iter(from, to, snap_seq).map(|row| {
                #[cfg(test)]
                crate::common::maintenance_probe::note_lsm_vacuum_row(false);
                Ok(row)
            })));
        }
        VacuumLightInput::OwnedBlocks(VacuumLightRows { sst: self, block: from.and_then(|f| self.find_block(f)).unwrap_or(0),
            rows: Vec::new().into_iter(), from, to, snap_seq, done: false })
    }

    pub(crate) fn move_eligibility(&self) -> MoveEligibility { self.move_eligibility }

    pub fn num_entries(&self) -> usize {
        if let Some(e) = &self.entries_cache {
            return e.len();
        }
        self.iter_all().count()
    }

    /// 这个文件里**需要缓存槽位**的数据块数（自动档用来跟着库长）。
    ///
    /// v1 文件整份常驻在 `entries_cache` 里（没有块索引、也永不按 offset 读盘）⇒ 记 0：
    /// 给它留槽位等于凭空把上限抬高，而它根本不参与块缓存。
    pub fn blocks(&self) -> usize {
        if self.entries_cache.is_some() {
            0
        } else {
            self.index.len()
        }
    }

    fn has_entries(&self) -> bool {
        self.entries_cache.as_ref().map_or(!self.index.is_empty(), |entries| !entries.is_empty())
    }

    pub fn may_overlap(&self, low: &[u8], high: &[u8]) -> bool {
        // 空字节串也是真键，不能靠 min/max 都为空来判空文件。
        self.has_entries() && low <= high
            && self.min_key.as_slice() <= high && self.max_key.as_slice() >= low
    }

    pub(crate) fn may_overlap_half_open(&self, from: Option<&[u8]>, to: Option<&[u8]>) -> bool {
        self.has_entries()
            && !matches!((from, to), (Some(lo), Some(hi)) if lo >= hi)
            && from.is_none_or(|lo| self.max_key.as_slice() >= lo)
            && to.is_none_or(|hi| self.min_key.as_slice() < hi)
    }

    pub fn get(&self, key: &[u8]) -> Option<StoredEntry> {
        self.get_at(key, u64::MAX)
    }

    /// 在 snapshot `snap_seq` 下查找（取 `seq <= snap_seq` 的最新）
    pub fn get_at(&self, key: &[u8], snap_seq: u64) -> Option<StoredEntry> {
        #[cfg(test)]
        if point_read_reference::enabled() { return self.get_at_reference(key, snap_seq); }
        self.get_at_project(key, snap_seq, Clone::clone)
    }

    pub(crate) fn get_light_at(&self, key: &[u8], snap_seq: u64) -> Option<LightEntry> {
        self.get_at_project(key, snap_seq, LightEntry::of)
    }

    /// Search the key interval, inspect every matching seq, project only its winner.
    /// Holding the block Arc keeps a winner valid while older blocks are loaded.
    fn get_at_project<T>(&self, key: &[u8], snap_seq: u64,
        project: impl Fn(&StoredEntry) -> T) -> Option<T> {
        #[cfg(test)]
        if point_read_reference::enabled() {
            return self.get_at_reference(key, snap_seq).as_ref().map(project);
        }
        if !self.may_overlap(key, key) {
            return None;
        }
        if !self.bloom.may_contain(key) {
            return None;
        }
        #[cfg(test)]
        note_sst_probe();
        if let Some(e) = &self.entries_cache {
            // 可能同 key 多版本：取 seq 最大且 <= snap
            let lo = e.partition_point(|(k, _)| k.as_slice() < key);
            let hi = e.partition_point(|(k, _)| k.as_slice() <= key);
            let mut best: Option<&StoredEntry> = None;
            for (_, se) in e[lo..hi].iter().rev() {
                if se.visible_at(snap_seq) && best.is_none_or(|b| se.seq > b.seq) { best = Some(se); }
            }
            #[cfg(test)]
            note_sst_entry_walks((hi - lo) as u64);
            return best.map(project);
        }
        let mut bi = self.find_block(key)?;
        let mut best: Option<(Arc<DataBlock>, usize)> = None;
        loop {
            let block = self.load_block(bi).ok()?;
            let lo = block.entries.partition_point(|(k, _)| k.as_slice() < key);
            let hi = block.entries.partition_point(|(k, _)| k.as_slice() <= key);
            for i in (lo..hi).rev() {
                let se = &block.entries[i].1;
                if se.visible_at(snap_seq)
                    && best.as_ref().is_none_or(|(b, j)| se.seq > b.entries[*j].1.seq) {
                    best = Some((block.clone(), i));
                }
            }
            #[cfg(test)]
            note_sst_entry_walks((hi - lo) as u64);
            // 同键的 seq 可横跨任意多块，甚至不按 seq 排；不能只看 find_block 的最后一块。
            // 本块首键更小则之前全小；首键等于 key 时前块尾部仍可能有同键。
            if bi == 0 || self.index[bi].first_key.as_slice() < key {
                break;
            }
            bi -= 1;
        }
        best.map(|(block, i)| project(&block.entries[i].1))
    }

    #[cfg(test)]
    fn get_at_reference(&self, key: &[u8], snap_seq: u64) -> Option<StoredEntry> {
        if !self.may_overlap(key, key) || !self.bloom.may_contain(key) { return None; }
        note_sst_probe();
        if let Some(e) = &self.entries_cache {
            let mut best: Option<StoredEntry> = None;
            for (k, se) in e {
                if k.as_slice() == key && se.visible_at(snap_seq)
                    && best.as_ref().is_none_or(|b| se.seq >= b.seq) { best = Some(se.clone()); }
            }
            note_sst_entry_walks(e.len() as u64);
            return best;
        }
        let mut bi = self.find_block(key)?;
        let mut best: Option<StoredEntry> = None;
        loop {
            let block = self.load_block(bi).ok()?;
            for (k, se) in block.entries.iter().rev() {
                if k.as_slice() == key && se.visible_at(snap_seq)
                    && best.as_ref().is_none_or(|b| se.seq > b.seq) { best = Some(se.clone()); }
            }
            note_sst_entry_walks(block.entries.len() as u64);
            if bi == 0 || self.index[bi].first_key.as_slice() < key { break; }
            bi -= 1;
        }
        best
    }

    pub fn range(&self, low: &[u8], high: &[u8]) -> Vec<(Vec<u8>, StoredEntry)> {
        self.range_at(low, high, u64::MAX)
    }

    pub fn range_at(
        &self,
        low: &[u8],
        high: &[u8],
        snap_seq: u64,
    ) -> Vec<(Vec<u8>, StoredEntry)> {
        if !self.may_overlap(low, high) {
            return Vec::new();
        }
        let mut out = Vec::new();
        // 同 key 可能多 seq：按 key 升序扫，每个 key 保留最大可见 seq
        let mut pending: Option<(Vec<u8>, StoredEntry)> = None;
        let flush = |pending: &mut Option<(Vec<u8>, StoredEntry)>,
                     out: &mut Vec<(Vec<u8>, StoredEntry)>| {
            if let Some(p) = pending.take() {
                out.push(p);
            }
        };

        for (k, se) in self.iter_from_key(low) {
            if k.as_slice() < low {
                continue;
            }
            if k.as_slice() > high {
                break;
            }
            if !se.visible_at(snap_seq) {
                continue;
            }
            match &pending {
                Some((pk, pe)) if pk == &k => {
                    if se.seq >= pe.seq {
                        pending = Some((k, se));
                    }
                }
                Some(_) => {
                    flush(&mut pending, &mut out);
                    pending = Some((k, se));
                }
                None => pending = Some((k, se)),
            }
        }
        flush(&mut pending, &mut out);
        out
    }

    /// 从**可能包含 `low` 的数据块**开始流式迭代（升序），用来给区间扫描定位起点。
    ///
    /// 旧 `range_at` 从第 0 块开始逐条 `continue` 到 `low`，于是区间查询的代价与
    /// **整个文件**成正比而不是与命中行数成正比 —— MVCC 点读放大因此变成随库总量增长。
    /// 起点取第一个 first_key >= low 的前一块，保留任意长的跨块同键版本组。
    /// v1 文件整份在 `entries_cache` 里，没有索引，保持从头扫。
    fn iter_from_key(&self, low: &[u8]) -> SstEntryIter<'_> {
        let block_i = self.first_block_for_key(low);
        SstEntryIter {
            sst: self,
            block_i,
            entry_i: 0,
            cur: None,
            cache_i: 0,
        }
    }

    /// 在闭区间 `[low, high]` 内反向查找。稀疏索引定位 high，完整折叠同键的所有可见 seq
    /// 后才交给 accept；版本组可跨块，seq 不要求有序。拒绝一次后不再复活该键的旧值。
    pub fn find_back(
        &self,
        low: &[u8],
        high: &[u8],
        snap_seq: u64,
        accept: impl FnMut(&[u8], &StoredEntry) -> bool,
    ) -> Option<(Vec<u8>, StoredEntry)> {
        self.find_back_project(low, high, snap_seq, Clone::clone, accept)
    }

    pub(crate) fn find_back_light(&self, low: &[u8], high: &[u8], snap_seq: u64,
        accept: impl FnMut(&[u8], &LightEntry) -> bool) -> Option<(Vec<u8>, LightEntry)> {
        self.find_back_project(low, high, snap_seq, LightEntry::of, accept)
    }

    fn find_back_project<T>(&self, low: &[u8], high: &[u8], snap_seq: u64,
        project: impl Fn(&StoredEntry) -> T,
        mut accept: impl FnMut(&[u8], &T) -> bool) -> Option<(Vec<u8>, T)> {
        if !self.may_overlap(low, high) {
            return None;
        }

        let mut pending: Option<(Vec<u8>, u64, T)> = None;
        // None 表示越界/文件结束，收口最后一个组。反向只用 > 替换，保留同 seq 时
        // 正向 range_at 取最后一条的规则。pending 跨块保留，不能每块重置。
        let mut feed = |entry: Option<(&[u8], &StoredEntry)>| {
            if pending.as_ref().is_some_and(|(pk, _, _)| {
                entry.is_none_or(|(k, _)| k != pk.as_slice())
            }) {
                let (k, _, se) = pending.take().unwrap();
                if accept(&k, &se) {
                    return Some((k, se));
                }
            }
            if let Some((k, se)) = entry {
                if se.visible_at(snap_seq)
                    && pending.as_ref().is_none_or(|(_, seq, _)| se.seq > *seq)
                {
                    pending = Some((k.to_vec(), se.seq, project(se)));
                }
            }
            None
        };

        if let Some(all) = &self.entries_cache {
            for (k, se) in all.iter().rev() {
                if k.as_slice() > high {
                    continue;
                }
                if k.as_slice() < low {
                    break;
                }
                if let Some(hit) = feed(Some((k, se))) {
                    return Some(hit);
                }
            }
            return feed(None);
        }

        let mut block_i = self.find_block(high)?;
        loop {
            let block = self.load_block(block_i).ok()?;
            #[cfg(test)]
            let old = point_read_reference::enabled();
            #[cfg(not(test))]
            let old = false;
            let hi = if old { block.entries.len() }
                else { block.entries.partition_point(|(k, _)| k.as_slice() <= high) };
            for (k, se) in block.entries[..hi].iter().rev() {
                if k.as_slice() > high {
                    continue;
                }
                if k.as_slice() < low {
                    return feed(None);
                }
                if let Some(hit) = feed(Some((k, se))) {
                    return Some(hit);
                }
            }
            if block_i == 0 {
                return feed(None);
            }
            block_i -= 1;
        }
    }

    /// 本文件在 `snap_seq` 下的**轻量视图**：每个键一条 `(key, LightEntry{seq, kind})`，
    /// 按引用读块，**不克隆任何 value**。
    ///
    /// 磁盘顺序即 key 非降序 ⇒ 相邻同键折叠即可；可见性规则与 [`Self::range_at`] 一致
    /// （同键取 `seq <= snap_seq` 中最大的那个）。
    pub fn light_view(&self, snap_seq: u64) -> Vec<(Vec<u8>, LightEntry)> {
        let mut out: Vec<(Vec<u8>, LightEntry)> = Vec::new();
        let mut feed = |k: &[u8], se: &StoredEntry| {
            if !se.visible_at(snap_seq) {
                return;
            }
            let le = LightEntry::of(se);
            match out.last_mut() {
                Some((pk, p)) if pk.as_slice() == k => {
                    if le.seq >= p.seq {
                        *p = le;
                    }
                }
                _ => out.push((k.to_vec(), le)),
            }
        };

        if let Some(all) = &self.entries_cache {
            for (k, se) in all {
                feed(k, se);
            }
        } else {
            for i in 0..self.index.len() {
                match self.load_block(i) {
                    Ok(b) => {
                        for (k, se) in &b.entries {
                            feed(k, se);
                        }
                    }
                    // 与 iter_all 一致：坏块即止
                    Err(_) => break,
                }
            }
        }
        // 计数尺（M39）：这条路把**整个文件**折进一个 Vec，谁在惰性路径上调用它就必须看得见。
        #[cfg(test)]
        note_light_view_materialized(out.len() as u64);
        out
    }

    /// `[from, to)` 内的轻量视图：**起点靠稀疏索引定位，键一旦越过上界就收手**，
    /// 后面的块一篇都不读。
    ///
    /// 存在的理由是：`light_view` 是把整个文件读完再筛，于是 10 行的窗口要付整文件的块读代价
    /// （大库基线实测：LSM 窗口 scan 405 µs/行 vs 全量 scan 4.0 µs/行）。折叠规则与
    /// `light_view` 完全一致。
    ///
    /// ⚠️ M29 之前这里还有一位 `light_view_break_from`：按「行数预算」切给有界回收用。
    /// 它已删除 —— 有界回收改走本函数（按**键的区间**切，代价与文件里的行数无关），
    /// 而"每源各取满预算"那种切法正是 M29 那条单价随库涨的根因。
    /// **惰性**的轻量视图：一次只往前拉一条，块是一块一块读的。
    ///
    /// 为什么要有它（M15b）：[`Self::light_view_range`] 先把整篇范围 `Vec` 化才交给归并，
    /// 于是"只要前 20 把键"的调用也得把**这个源的所有块**读完 —— 实测 2000 行的库
    /// 首页付 53 次块读，其中 ~32 次就是全库的块数。跨源归并改成惰性之后，
    /// 首页的块读只跟"覆盖到那 20 把键"有关（见 `LSMTree::light_iter_range`）。
    ///
    /// 语义与 `light_view_range` 逐条一致：只交 `visible_at(snap)` 的条目、`[from, to)` 定界、
    /// 同一个 enc key 只可能出现在一个块里（键升序），所以不需要 lookahead 收口。
    pub fn light_iter(
        &self,
        from: Option<&[u8]>,
        to: Option<&[u8]>,
        snap_seq: u64,
    ) -> impl Iterator<Item = (Vec<u8>, LightEntry)> + '_ {
        if !self.may_overlap_half_open(from, to) {
            return Box::new(std::iter::empty()) as Box<dyn Iterator<Item = _>>;
        }
        let filter = move |(k, se): &(Vec<u8>, StoredEntry)| -> Option<(Vec<u8>, LightEntry)> {
            if !se.visible_at(snap_seq) {
                return None;
            }
            Some((k.clone(), LightEntry::of(se)))
        };
        if let Some(all) = &self.entries_cache {
            // v1 / 小文件：条目本来就在内存里，切片扫一遍即可（无块可读）
            let (lo, hi) = (from.map(|k| k.to_vec()), to.map(|k| k.to_vec()));
            return Box::new(
                all.iter()
                    .skip_while(move |(k, _)| lo.as_deref().is_some_and(|f| k.as_slice() < f))
                    .take_while(move |(k, _)| hi.as_deref().is_none_or(|t| k.as_slice() < t))
                    .filter_map(filter),
            ) as Box<dyn Iterator<Item = _>>;
        }
        // 起点定位到含 `from` 的那个块：前面的块一块都不读。
        // ⚠️ `from` 比第 0 块首键还小 ⇒ 从第 0 块开始（`unwrap_or(0)`，不是 `index.len()`，
        // 那是 M11e 踩过的"真前缀扫直接返回空"）
        let mut start_block = match from {
            Some(f) => self.find_block(f).unwrap_or(0),
            None => 0,
        };
        // find_block returns the last block starting at or below from. A key can
        // span several blocks, with its greatest seq in any of them.
        if let Some(from) = from {
            while start_block > 0 && self.index[start_block].first_key.as_slice() >= from {
                start_block -= 1;
            }
        }
        Box::new(SstLightIter {
            sst: self,
            block: start_block,
            pos: 0,
            cur: None,
            from: from.map(|k| k.to_vec()),
            to: to.map(|k| k.to_vec()),
            snap_seq,
        }) as Box<dyn Iterator<Item = _>>
    }

    pub fn light_view_range(
        &self,
        from: Option<&[u8]>,
        to: Option<&[u8]>,
        snap_seq: u64,
    ) -> Vec<(Vec<u8>, LightEntry)> {
        let mut out: Vec<(Vec<u8>, LightEntry)> = Vec::new();
        let mut feed = |k: &[u8], se: &StoredEntry| {
            if !se.visible_at(snap_seq) {
                return;
            }
            let le = LightEntry::of(se);
            match out.last_mut() {
                Some((pk, p)) if pk.as_slice() == k => {
                    if le.seq >= p.seq {
                        *p = le;
                    }
                }
                _ => out.push((k.to_vec(), le)),
            }
        };

        if let Some(all) = &self.entries_cache {
            for (k, se) in all {
                if to.is_some_and(|t| k.as_slice() >= t) {
                    break;
                }
                if from.is_some_and(|f| k.as_slice() < f) {
                    continue;
                }
                feed(k, se);
            }
        } else {
            // 起点定位到含 `from` 的那个块；前面的块一块不读。
            // ⚠️ 找不到（`from` 比第 0 块的首键还小，比如 `prefix_scan("0")` 这种
            // "前缀是所有键的真前缀"的形状）必须**从第 0 块开始**，不能跳过整篇 ——
            // 我第一版写成 `unwrap_or(index.len())`，于是这类前缀扫直接返回空。
            let start_block = match from {
                Some(f) => self.find_block(f).unwrap_or(0),
                None => 0,
            };
            'blocks: for i in start_block..self.index.len() {
                match self.load_block(i) {
                    Ok(b) => {
                        for (k, se) in &b.entries {
                            if to.is_some_and(|t| k.as_slice() >= t) {
                                break 'blocks; // 磁盘顺序即 key 非降序 ⇒ 后面全在界外
                            }
                            if from.is_some_and(|f| k.as_slice() < f) {
                                continue;
                            }
                            feed(k, se);
                        }
                    }
                    // 与 iter_all / light_view 一致：坏块即止
                    Err(_) => break,
                }
            }
        }
        out
    }

    /// 流式迭代全部 entry（按磁盘顺序）
    pub fn iter_all(&self) -> SstEntryIter<'_> {
        SstEntryIter {
            sst: self,
            block_i: 0,
            entry_i: 0,
            cur: None,
            cache_i: 0,
        }
    }

    pub(crate) fn compaction_input(&self) -> CompactionRows<'_> {
        #[cfg(test)]
        if compaction_reference::enabled() { return CompactionRows::CachedReference(self.iter_all()); }
        match &self.entries_cache {
            Some(entries) => CompactionRows::LegacyShared(entries.iter()),
            None => CompactionRows::OwnedBlocks { sst: self, block: 0, rows: Vec::new().into_iter() },
        }
    }

    /// 把**整个文件**读进一个 `Vec`。
    ///
    /// ⚠️ 只有"文件本来就在内存里（v1 / 小文件）"或"调用方确实要随机访问这份 `Vec`"时该用它。
    /// **压实与层内重叠修复的输入不许用它**（M33）：那些路径现在收 `MergeInput`（一路一个
    /// 迭代器），用这条会把瞬时内存推到"所有输入文件之和"—— 实测 600k 行 61.7 MB 的输入
    /// 在这一步峰值涨到 **154 MB**。尺子是 `merge_inputs_materialized()`（测试专用）。
    pub fn iter_entries(&self) -> Vec<(Vec<u8>, StoredEntry)> {
        let v: Vec<(Vec<u8>, StoredEntry)> = self.iter_all().collect();
        #[cfg(test)]
        note_merge_input_materialized(v.len() as u64);
        v
    }

    pub fn for_each_entry<F>(&self, mut f: F) -> std::io::Result<()>
    where
        F: FnMut(&[u8], &StoredEntry),
    {
        for (k, se) in self.iter_all() {
            f(&k, &se);
        }
        Ok(())
    }

    fn first_block_for_key(&self, key: &[u8]) -> usize {
        self.index.partition_point(|entry| {
            #[cfg(test)]
            INDEX_PROBES.with(|c| c.set(c.get() + 1));
            entry.first_key.as_slice() < key
        }).saturating_sub(1)
    }

    fn find_block(&self, key: &[u8]) -> Option<usize> {
        if self.index.is_empty() {
            return None;
        }
        // 索引按 `first_key` 升序 ⇒ 要找的是「最后一个 first_key <= key」。
        // 手写二分而不是 `binary_search_by`：守卫要的是**比了几次索引**这个计数，闭包里数不干净。
        // 原先这里是线性扫（`for (i, ie) in index.enumerate()` + 遇大于 key 才 break），
        // 代价 O(块数) = O(库内行数 / 64) —— 点查路径上唯一一处随库总量变贵的纯 CPU。
        let mut lo = 0usize;
        let mut hi = self.index.len();
        let mut found: Option<usize> = None;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            #[cfg(test)]
            INDEX_PROBES.with(|c| c.set(c.get() + 1));
            if self.index[mid].first_key.as_slice() <= key {
                found = Some(mid);
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        found
    }

    fn load_block(&self, index_i: usize) -> std::io::Result<Arc<DataBlock>> {
        #[cfg(test)]
        BLOCK_READS.with(|c| c.set(c.get() + 1));
        let ie = &self.index[index_i];
        if let Some(cache) = &self.block_cache {
            let mut c = cache.lock().unwrap();
            if let Some(b) = c.get(self.cache_ns, self.id, ie.offset) {
                return Ok(b);
            }
        }
        let raw = self.read_slice(ie.offset, ie.len as usize)?;
        let block = Arc::new(decode_block(&raw, self.has_seq)?);
        if let Some(cache) = &self.block_cache {
            cache
                .lock()
                .unwrap()
                .put(self.cache_ns, self.id, ie.offset, block.clone());
        }
        Ok(block)
    }

    fn read_slice(&self, offset: u64, len: usize) -> std::io::Result<Vec<u8>> {
        if let Some(m) = &self.mmap {
            let start = offset as usize;
            let end = start + len;
            if end > m.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "mmap 越界",
                ));
            }
            return Ok(m[start..end].to_vec());
        }
        #[cfg(test)]
        BLOCK_MISSES.with(|c| c.set(c.get() + 1));
        // 常驻句柄：懒开一次，之后只 seek + read。这里省掉的正是那次与读字节同量级、
        // 却贵一个数量级的 File::open。
        let mut guard = self.handle.lock().unwrap();
        if guard.is_none() {
            *guard = Some(File::open(&self.path)?);
        }
        let f = guard.as_mut().unwrap();
        f.seek(SeekFrom::Start(offset))?;
        let mut raw: Vec<u8> = vec![0u8; len];
        f.read_exact(&mut raw)?;
        Ok(raw)
    }

    pub fn set_block_cache(&mut self, cache: Arc<Mutex<BlockCache>>) {
        self.block_cache = Some(cache);
    }

    pub fn set_cache_ns(&mut self, ns: u64) {
        self.cache_ns = ns;
    }

    /// 用真实 mmap 映射文件（跨平台 memmap2）
    pub fn enable_mmap(&mut self) -> std::io::Result<()> {
        let f = File::open(&self.path)?;
        // SAFETY: 文件在引擎生命周期内视为不可变 SST
        let mmap = unsafe { Mmap::map(&f)? };
        self.mmap = Some(mmap);
        Ok(())
    }

    pub fn has_mmap(&self) -> bool {
        self.mmap.is_some()
    }

    pub fn open(id: u64, path: impl AsRef<Path>) -> std::io::Result<Self> {
        Self::open_with(id, path, false)
    }

    pub fn open_with(id: u64, path: impl AsRef<Path>, use_mmap: bool) -> std::io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let meta = std::fs::metadata(&path)?;
        let size_bytes = meta.len();
        let mut f = File::open(&path)?;
        let mut magic = [0u8; 8];
        f.read_exact(&mut magic)?;
        let mut sst = if &magic == SST_MAGIC_V1 {
            open_v1(id, path, size_bytes, &mut f)?
        } else {
            open_v2_from_footer(id, path, size_bytes)?
        };
        if use_mmap {
            sst.enable_mmap()?;
        }
        Ok(sst)
    }
}

/// 流式 SST 迭代器（按 block 推进，不一次载入全表）
pub struct SstEntryIter<'a> {
    sst: &'a SstFile,
    block_i: usize,
    entry_i: usize,
    cur: Option<Arc<DataBlock>>,
    cache_i: usize,
}

/// Modern blocks are decoded once and moved. V1's resident entries require an
/// explicit shared fallback; compaction never fills the point-read cache.
pub(crate) enum CompactionRows<'a> {
    OwnedBlocks { sst: &'a SstFile, block: usize, rows: std::vec::IntoIter<(Vec<u8>, StoredEntry)> },
    LegacyShared(std::slice::Iter<'a, (Vec<u8>, StoredEntry)>),
    #[cfg(test)]
    CachedReference(SstEntryIter<'a>),
}

impl Iterator for CompactionRows<'_> {
    type Item = std::io::Result<(Vec<u8>, StoredEntry)>;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::LegacyShared(rows) => rows.next().map(|row| {
                #[cfg(test)]
                crate::lsm_tree::lsm_tree::maintenance_probe::input_row(1);
                Ok(row.clone())
            }),
            #[cfg(test)]
            Self::CachedReference(rows) => rows.next().map(|row| {
                crate::lsm_tree::lsm_tree::maintenance_probe::input_row(2);
                Ok(row)
            }),
            Self::OwnedBlocks { sst, block, rows } => loop {
                if let Some(row) = rows.next() {
                    #[cfg(test)]
                    crate::lsm_tree::lsm_tree::maintenance_probe::input_row(0);
                    return Some(Ok(row));
                }
                let ie = sst.index.get(*block)?;
                *block += 1;
                #[cfg(test)]
                crate::lsm_tree::lsm_tree::maintenance_probe::input_block();
                match sst.read_slice(ie.offset, ie.len as usize).and_then(|raw| decode_block(&raw, sst.has_seq)) {
                    Ok(decoded) => *rows = decoded.entries.into_iter(),
                    Err(error) => { *block = sst.index.len(); return Some(Err(error)); }
                }
            },
        }
    }
}

#[cfg(test)]
pub(crate) mod compaction_reference {
    use std::{assert, cell::Cell, thread_local};
    thread_local! { static ENABLED: Cell<bool> = const { Cell::new(false) }; }
    pub(crate) fn enabled() -> bool { ENABLED.with(|c| c.get()) }
    pub(crate) struct Scope;
    impl Scope { pub fn start() -> Self { ENABLED.with(|c| assert!(!c.replace(true))); Self } }
    impl Drop for Scope { fn drop(&mut self) { ENABLED.with(|c| c.set(false)); } }
}

impl Iterator for SstEntryIter<'_> {
    type Item = (Vec<u8>, StoredEntry);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(cache) = &self.sst.entries_cache {
            if self.cache_i >= cache.len() {
                return None;
            }
            #[cfg(test)]
            note_value_materialization();
            let item = cache[self.cache_i].clone();
            self.cache_i += 1;
            return Some(item);
        }
        loop {
            if let Some(block) = &self.cur {
                if self.entry_i < block.entries.len() {
                    #[cfg(test)]
                    note_value_materialization();
                    let item = block.entries[self.entry_i].clone();
                    self.entry_i += 1;
                    return Some(item);
                }
            }
            if self.block_i >= self.sst.index.len() {
                return None;
            }
            match self.sst.load_block(self.block_i) {
                Ok(b) => {
                    self.cur = Some(b);
                    self.entry_i = 0;
                    self.block_i += 1;
                }
                Err(_) => return None,
            }
        }
    }
}

fn open_v1(
    id: u64,
    path: PathBuf,
    size_bytes: u64,
    f: &mut File,
) -> std::io::Result<SstFile> {
    let mut buf8 = [0u8; 8];
    f.read_exact(&mut buf8)?;
    let count = u64::from_le_bytes(buf8) as usize;
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let (k, v) = read_entry_v1(f)?;
        entries.push((
            k,
            StoredEntry {
                seq: 0,
                value: v,
            },
        ));
    }
    let (min_key, max_key) = min_max_keys_stored(&entries);
    Ok(SstFile {
        id,
        path,
        min_key,
        max_key,
        size_bytes,
        move_eligibility: MoveEligibility::Unknown,
        entries_cache: Some(entries),
        index: Vec::new(),
        bloom: Bloom::empty(),
        is_v2: false,
        has_seq: false,
        block_cache: None,
        mmap: None,
        // v1 整份在 entries_cache 里，永不按 offset 读，所以不需要句柄
        handle: Mutex::new(None),
        cache_ns: 0,
    })
}

fn open_v2_from_footer(id: u64, path: PathBuf, size_bytes: u64) -> std::io::Result<SstFile> {
    let mut f = File::open(&path)?;
    if size_bytes < 4 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "SST 过短",
        ));
    }
    f.seek(SeekFrom::End(-4))?;
    let mut fl = [0u8; 4];
    f.read_exact(&mut fl)?;
    let footer_len = u32::from_le_bytes(fl) as u64;
    if footer_len + 4 > size_bytes {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "footer 长度非法",
        ));
    }
    f.seek(SeekFrom::End(-(4 + footer_len as i64)))?;
    let mut footer = vec![0u8; footer_len as usize];
    f.read_exact(&mut footer)?;
    if footer.len() < 12 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "footer 过短",
        ));
    }
    let crc_stored = u32::from_le_bytes(footer[footer.len() - 4..].try_into().unwrap());
    let body = &footer[..footer.len() - 4];
    if crc32(body) != crc_stored {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "footer CRC 失败",
        ));
    }
    if body.len() < 8 || &body[body.len() - 8..] != SST_MAGIC_V2 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "SST v2 魔数错误",
        ));
    }
    let meta = &body[..body.len() - 8];
    let mut off = 0usize;
    let index_off = read_u64(meta, &mut off)?;
    let index_len = read_u32(meta, &mut off)? as usize;
    let bloom_off = read_u64(meta, &mut off)?;
    let bloom_len = read_u32(meta, &mut off)? as usize;
    let _entry_count = read_u64(meta, &mut off)?;
    let min_key = read_bytes(meta, &mut off)?;
    let max_key = read_bytes(meta, &mut off)?;
    // 可选 flags：has_seq（1 字节）；旧 footer 无此字段则默认 true（本仓库写路径一直有 seq 后加）
    let has_seq = if off < meta.len() {
        meta[off] != 0
    } else {
        true
    };

    let mut index_buf = vec![0u8; index_len];
    f.seek(SeekFrom::Start(index_off))?;
    f.read_exact(&mut index_buf)?;
    let index = decode_index(&index_buf)?;

    let mut bloom_buf = vec![0u8; bloom_len];
    if bloom_len > 0 {
        f.seek(SeekFrom::Start(bloom_off))?;
        f.read_exact(&mut bloom_buf)?;
    }
    let bloom = if bloom_len == 0 {
        Bloom::empty()
    } else {
        Bloom::decode(&bloom_buf).unwrap_or_else(Bloom::empty)
    };

    Ok(SstFile {
        id,
        path,
        min_key,
        max_key,
        size_bytes,
        move_eligibility: MoveEligibility::Unknown,
        entries_cache: None,
        index,
        bloom,
        is_v2: true,
        has_seq,
        block_cache: None,
        mmap: None,
        // 打开页脚/索引/bloom 用的这个句柄直接留下：之后每次按 offset 读都省掉一次 File::open
        handle: Mutex::new(Some(f)),
        cache_ns: 0,
    })
}

fn read_u64(buf: &[u8], off: &mut usize) -> std::io::Result<u64> {
    if *off + 8 > buf.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "eof",
        ));
    }
    let v = u64::from_le_bytes(buf[*off..*off + 8].try_into().unwrap());
    *off += 8;
    Ok(v)
}

fn read_u32(buf: &[u8], off: &mut usize) -> std::io::Result<u32> {
    if *off + 4 > buf.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "eof",
        ));
    }
    let v = u32::from_le_bytes(buf[*off..*off + 4].try_into().unwrap());
    *off += 4;
    Ok(v)
}

fn read_bytes(buf: &[u8], off: &mut usize) -> std::io::Result<Vec<u8>> {
    let n = read_u32(buf, off)? as usize;
    if *off + n > buf.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "eof",
        ));
    }
    let v = buf[*off..*off + n].to_vec();
    *off += n;
    Ok(v)
}

fn decode_index(buf: &[u8]) -> std::io::Result<Vec<IndexEntry>> {
    let mut off = 0usize;
    let count = read_u32(buf, &mut off)? as usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let first_key = read_bytes(buf, &mut off)?;
        let offset = read_u64(buf, &mut off)?;
        let len = read_u32(buf, &mut off)?;
        out.push(IndexEntry {
            first_key,
            offset,
            len,
        });
    }
    Ok(out)
}

fn encode_index(index: &[IndexEntry]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(index.len() as u32).to_le_bytes());
    for ie in index {
        out.extend_from_slice(&(ie.first_key.len() as u32).to_le_bytes());
        out.extend_from_slice(&ie.first_key);
        out.extend_from_slice(&ie.offset.to_le_bytes());
        out.extend_from_slice(&ie.len.to_le_bytes());
    }
    out
}

fn encode_entry(k: &[u8], se: &StoredEntry, with_seq: bool) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(k.len() as u32).to_le_bytes());
    out.extend_from_slice(k);
    out.push(se.value.kind_byte());
    if with_seq {
        out.extend_from_slice(&se.seq.to_le_bytes());
    }
    if let EntryValue::Put(Some(val)) = &se.value {
        out.extend_from_slice(&(val.len() as u32).to_le_bytes());
        out.extend_from_slice(val);
    }
    out
}

fn read_entry_from_slice(
    data: &[u8],
    off: &mut usize,
    with_seq: bool,
) -> std::io::Result<(Vec<u8>, StoredEntry)> {
    if *off + 4 > data.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "entry",
        ));
    }
    let kl = u32::from_le_bytes(data[*off..*off + 4].try_into().unwrap()) as usize;
    *off += 4;
    if *off + kl + 1 > data.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "key",
        ));
    }
    let key = data[*off..*off + kl].to_vec();
    *off += kl;
    let kind = data[*off];
    *off += 1;
    let seq = if with_seq {
        if *off + 8 > data.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "seq",
            ));
        }
        let s = u64::from_le_bytes(data[*off..*off + 8].try_into().unwrap());
        *off += 8;
        s
    } else {
        0
    };
    let val = if kind == 0 {
        if *off + 4 > data.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "val len",
            ));
        }
        let vl = u32::from_le_bytes(data[*off..*off + 4].try_into().unwrap()) as usize;
        *off += 4;
        if *off + vl > data.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "val",
            ));
        }
        let v = data[*off..*off + vl].to_vec();
        *off += vl;
        Some(v)
    } else {
        None
    };
    let value = EntryValue::from_parts(kind, val).ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "未知 kind")
    })?;
    Ok((key, StoredEntry { seq, value }))
}

fn read_entry_v1(f: &mut File) -> std::io::Result<(Vec<u8>, EntryValue)> {
    let mut buf4 = [0u8; 4];
    f.read_exact(&mut buf4)?;
    let kl = u32::from_le_bytes(buf4) as usize;
    let mut key: Vec<u8> = vec![0u8; kl];
    f.read_exact(&mut key)?;
    let mut kind = [0u8; 1];
    f.read_exact(&mut kind)?;
    let val: Option<Vec<u8>> = if kind[0] == 0 {
        f.read_exact(&mut buf4)?;
        let vl = u32::from_le_bytes(buf4) as usize;
        let mut v: Vec<u8> = vec![0u8; vl];
        f.read_exact(&mut v)?;
        Some(v)
    } else {
        None
    };
    EntryValue::from_parts(kind[0], val)
        .map(|ev| (key, ev))
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "kind"))
}

fn decode_block(raw: &[u8], with_seq: bool) -> std::io::Result<DataBlock> {
    if raw.len() < 8 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "block 过短",
        ));
    }
    let count = u32::from_le_bytes(raw[0..4].try_into().unwrap()) as usize;
    let crc_stored = u32::from_le_bytes(raw[raw.len() - 4..].try_into().unwrap());
    let body = &raw[4..raw.len() - 4];
    if crc32(body) != crc_stored {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "block CRC",
        ));
    }
    let mut off = 0usize;
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        entries.push(read_entry_from_slice(body, &mut off, with_seq)?);
    }
    Ok(DataBlock { entries })
}

fn encode_block(entries: &[(Vec<u8>, StoredEntry)], with_seq: bool) -> Vec<u8> {
    let mut body = Vec::new();
    for (k, se) in entries {
        body.extend_from_slice(&encode_entry(k, se, with_seq));
    }
    let checksum = crc32(&body);
    let mut out = Vec::with_capacity(8 + body.len());
    out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    out.extend_from_slice(&body);
    out.extend_from_slice(&checksum.to_le_bytes());
    out
}

fn min_max_keys_stored(entries: &[(Vec<u8>, StoredEntry)]) -> (Vec<u8>, Vec<u8>) {
    if entries.is_empty() {
        (Vec::new(), Vec::new())
    } else {
        (
            entries.first().unwrap().0.clone(),
            entries.last().unwrap().0.clone(),
        )
    }
}

// ─── SstBuilder ──────────────────────────────────────────────────────────────

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

#[cfg(test)]
pub(crate) fn builder_size_walks() -> u64 {
    BUILDER_SIZE_WALKS.with(|c| c.get())
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

#[cfg(test)]
pub(crate) fn builder_held_max() -> u64 {
    BUILDER_HELD_MAX.with(|c| c.get())
}

#[cfg(test)]
pub(crate) fn reset_builder_held_max() {
    BUILDER_HELD_MAX.with(|c| c.set(0));
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

#[cfg(test)]
pub(crate) fn builder_hashed_max() -> u64 {
    BUILDER_HASHED_MAX.with(|c| c.get())
}

#[cfg(test)]
pub(crate) fn reset_builder_hashed_max() {
    BUILDER_HASHED_MAX.with(|c| c.set(0));
}

/// `entries` 的字节数（旧算法，留着当对拍与尺子；生产路径一步都不该走它）
#[cfg(test)]
fn sum_entries_bytes(entries: &[(Vec<u8>, StoredEntry)]) -> usize {
    BUILDER_SIZE_WALKS.with(|c| c.set(c.get() + entries.len() as u64));
    entries
        .iter()
        .map(|(k, se)| k.len() + se.estimate_bytes() + 8)
        .sum()
}

impl SstBuilder {
    pub fn new(id: u64, path: impl AsRef<Path>) -> Self {
        Self::with_options(id, path, 64, true)
    }

    pub fn with_bloom_bits(mut self, bits: usize) -> Self {
        self.bloom_bits_per_key = bits.max(4);
        self
    }

    pub fn with_options(
        id: u64,
        path: impl AsRef<Path>,
        block_entries: usize,
        enable_bloom: bool,
    ) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            id,
            block_entries: block_entries.max(1),
            enable_bloom,
            bloom_bits_per_key: 10,
            pending: Vec::with_capacity(block_entries.max(1)),
            hashes: Vec::new(),
            index: Vec::new(),
            writer: None,
            written: 0,
            tail_key: None,
            count: 0,
            est_bytes: 0,
            io_err: None,
            move_eligibility: MoveEligibility::Ready,
        }
    }

    pub fn add(&mut self, key: Vec<u8>, se: StoredEntry) {
        let previous = self.pending.last().map(|(k, _)| k.as_slice()).or(self.tail_key.as_deref());
        if matches!(se.value, EntryValue::Drop) || previous.is_some_and(|k| k >= key.as_slice()) {
            self.move_eligibility = MoveEligibility::RewriteRequired;
        }
        // 有序断言：上一键可能在还没落盘的那半块里，也可能已经是上一块的尾键
        debug_assert!(
            match self.pending.last() {
                Some((k, _)) => k.as_slice() <= key.as_slice(),
                None => self.tail_key.as_deref().is_none_or(|k| k <= key.as_slice()),
            },
            "SST keys must be non-decreasing"
        );
        // 口径与 `sum_entries_bytes` 里那句一字不差（对拍守卫钉着这两处）
        self.est_bytes += key.len() + se.estimate_bytes() + 8;
        self.hashes.push(Bloom::hash_pair(&key));
        self.count += 1;
        self.pending.push((key, se));
        #[cfg(test)]
        BUILDER_HELD_MAX.with(|c| {
            let n = self.pending.len() as u64;
            if n > c.get() {
                c.set(n);
            }
        });
        #[cfg(test)]
        BUILDER_HASHED_MAX.with(|c| {
            let n = self.hashes.len() as u64;
            if n > c.get() {
                c.set(n);
            }
        });
        if self.pending.len() >= self.block_entries {
            if let Err(e) = self.emit_block() {
                // 只留第一个错误：后面再失败是同一件事的重复，而 `finish` 必须如实抛出那一个
                if self.io_err.is_none() {
                    self.io_err = Some(e);
                }
            }
        }
    }

    /// 把攒满的一块编码并写盘，同时追加索引与尾键。
    ///
    /// ⚠️ 块的**分组方式与偏移算法与改前逐字一致**（改前是 `finish` 里 `while i < len` 按
    /// `block_entries` 切片；这里是攒满即写）⇒ 同一个文件字节相同，由
    /// `test_sst_builder_byteprint_is_stable` 钉住。写失败时**不**清 `pending`（数据还在手里，
    /// 错误由 `add` 存下、`finish` 抛出）。
    fn emit_block(&mut self) -> std::io::Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let block_raw = encode_block(&self.pending, true);
        self.ensure_writer()?;
        let offset = self.written;
        // `written` 自己累计 ⇒ 不必每块 `stream_position()` 把 1 MiB 缓冲冲一次
        if let Some(f) = self.writer.as_mut() {
            f.write_all(&block_raw)?;
        }
        self.written += block_raw.len() as u64;
        let first_key = self.pending[0].0.clone();
        self.tail_key = Some(self.pending[self.pending.len() - 1].0.clone());
        self.index.push(IndexEntry {
            first_key,
            offset,
            len: block_raw.len() as u32,
        });
        self.pending.clear();
        Ok(())
    }

    /// 建文件（一次）。路径仍是 `<path>.sst.tmp` → `finish` 末尾 rename，与改前同一套落盘顺序。
    fn ensure_writer(&mut self) -> std::io::Result<()> {
        if self.writer.is_some() {
            return Ok(());
        }
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = self.path.with_extension("sst.tmp");
        let f = File::create(tmp)?;
        self.writer = Some(std::io::BufWriter::with_capacity(1024 * 1024, f));
        Ok(())
    }

    pub fn add_put(&mut self, key: Vec<u8>, seq: u64, value: Option<Vec<u8>>) {
        self.add(key, StoredEntry::put(seq, value));
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// O(1)：读增量账。**不许**改回 `entries.iter().sum()` —— 这里每被调一次都要走一遍整表的话，
    /// `merge_to_ssts` 的"每行问一次"就把压实变成 O(表内行数²)（实测 600k 行 18.0 s，94% 在这一步）。
    pub fn estimated_bytes(&self) -> usize {
        self.est_bytes
    }

    pub fn finish(self) -> std::io::Result<SstFile> {
        self.finish_with_sync(true)
    }

    /// `sync=false`：不 fsync（bulk 导入用，结束时统一落盘）
    pub fn finish_with_sync(mut self, sync: bool) -> std::io::Result<SstFile> {
        #[cfg(test)]
        let _timer = crate::lsm_tree::lsm_tree::maintenance_probe::Timer::start(crate::lsm_tree::lsm_tree::maintenance_probe::Kind::SstFinish);
        // 最后那半块
        if !self.pending.is_empty() {
            self.emit_block()?;
        }
        if let Some(e) = self.io_err.take() {
            return Err(e);
        }
        // 一条都没写过的空表：与改前一样，到这一步才 create
        self.ensure_writer()?;
        let mut f = self.writer.take().expect("ensure_writer 之后一定有");
        let index = std::mem::take(&mut self.index);
        let mut bloom = if self.enable_bloom {
            Bloom::new(self.count.max(1), self.bloom_bits_per_key)
        } else {
            Bloom::empty()
        };
        for &(h1, h2) in &self.hashes {
            bloom.set_pair(h1, h2);
        }
        let index_blob = encode_index(&index);
        let index_off = self.written;
        f.write_all(&index_blob)?;
        self.written += index_blob.len() as u64;
        let bloom_blob = if self.enable_bloom {
            bloom.encode()
        } else {
            Vec::new()
        };
        let bloom_off = self.written;
        if !bloom_blob.is_empty() {
            f.write_all(&bloom_blob)?;
            self.written += bloom_blob.len() as u64;
        }
        // min/max 与条目数从增量账拿：`index[0].first_key` 就是第一块的首键，
        // `tail_key` 就是最后一块的尾键 ⇒ 与改前 `min_max_keys_stored(&entries)` 同一个值
        let min_key = index.first().map(|e| e.first_key.clone()).unwrap_or_default();
        let max_key = self.tail_key.clone().unwrap_or_default();
        let mut meta = Vec::new();
        meta.extend_from_slice(&index_off.to_le_bytes());
        meta.extend_from_slice(&(index_blob.len() as u32).to_le_bytes());
        meta.extend_from_slice(&bloom_off.to_le_bytes());
        meta.extend_from_slice(&(bloom_blob.len() as u32).to_le_bytes());
        meta.extend_from_slice(&(self.count as u64).to_le_bytes());
        meta.extend_from_slice(&(min_key.len() as u32).to_le_bytes());
        meta.extend_from_slice(&min_key);
        meta.extend_from_slice(&(max_key.len() as u32).to_le_bytes());
        meta.extend_from_slice(&max_key);
        meta.push(1u8); // has_seq flag
        meta.extend_from_slice(SST_MAGIC_V2);
        let fc = crc32(&meta);
        let mut footer = meta;
        footer.extend_from_slice(&fc.to_le_bytes());
        let footer_len = footer.len() as u32;
        f.write_all(&footer)?;
        f.write_all(&footer_len.to_le_bytes())?;
        f.flush()?;
        if sync {
            #[cfg(test)]
            let _timer = crate::lsm_tree::lsm_tree::maintenance_probe::Timer::start(crate::lsm_tree::lsm_tree::maintenance_probe::Kind::SstSync);
            f.get_mut().sync_all()?;
        }
        let _ = std::fs::remove_file(&self.path);
        std::fs::rename(self.path.with_extension("sst.tmp"), &self.path)?;
        let mut sst = SstFile::open(self.id, &self.path)?;
        // Unknown for a disk-opened file; no format change and no hidden full scan.
        if sync && self.count > 0 { sst.move_eligibility = self.move_eligibility; }
        Ok(sst)
    }
}

/// 没走到 `finish` 的 builder（`merge_to_ssts` 中途 IO 失败，或调用方提前把它丢了）现在会留下
/// 一个半截的 `<id>.sst.tmp` —— 改前是"什么都没写，所以什么都不留"。目录扫描不认这个扩展名、
/// 加载以 MANIFEST 为准，所以它不影响正确性，但**会一直占盘**：长跑进程里这类垃圾是要攒的。
/// `finish` 成功之后 rename 已经把那个名字移走 ⇒ 这里是一次失败的 `remove_file`，被 `let _ =` 吃掉。
impl Drop for SstBuilder {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(self.path.with_extension("sst.tmp"));
    }
}


// ─── Streaming merge ─────────────────────────────────────────────────────────

/// 归并核心只用到「键 + 序号 + 谁赢」，不需要知道 value 长什么样。
///
/// 所以把归并泛型化：`StoredEntry`（带 value 字节）和 [`LightEntry`]（只有 seq 与种类）
/// 走**同一份**实现。这样"只想按键折叠全库"不必再抄一份归并 —— 抄一份就是给
/// 「四份复制」那种病再添一份。
pub trait MergeSeq: Clone {
    fn seq(&self) -> u64;
}

impl MergeSeq for StoredEntry {
    fn seq(&self) -> u64 {
        self.seq
    }
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

impl MergeSeq for LightEntry {
    fn seq(&self) -> u64 {
        self.seq
    }
}

impl LightEntry {
    fn of(se: &StoredEntry) -> Self {
        LightEntry {
            seq: se.seq,
            kind: se.value.kind_byte(),
        }
    }
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

/// 把"本来就物化在内存里"的源接到 `MergeInput` 上（M33）。
///
/// 内存表切片（`mem_range_at` / `range_at` 返回的就是 `Vec`）走这条 —— **只是换个类型**，
/// 不复制条目。要避开的物化是 `SstFile::iter_entries()`：那是把磁盘上的整个文件读进 `Vec`，
/// 而压实/修复的输入恰好是磁盘文件。
pub fn vec_inputs<T: 'static>(srcs: Vec<Vec<(Vec<u8>, T)>>) -> Vec<MergeInput<'static, T>> {
    srcs.into_iter()
        .map(|s| Box::new(s.into_iter()) as MergeInput<T>)
        .collect()
}

pub fn merge_streaming<'a, T, F, K>(sources: Vec<MergeInput<'a, T>>, keep: K, mut emit: F)
where T: MergeSeq + 'a, F: FnMut(Vec<u8>, T), K: FnMut(&[u8], &T) -> bool {
    use std::convert::Infallible;
    let sources = sources.into_iter().map(|s| Box::new(s.map(Ok::<_, Infallible>)) as TryMergeInput<'a, T, Infallible>).collect();
    match try_merge_streaming(sources, keep, |key, row| { emit(key, row); Ok(()) }) {
        Ok(()) => {}, Err(impossible) => match impossible {},
    }
}

fn try_merge_streaming<T, E, F, K>(mut sources: Vec<TryMergeInput<'_, T, E>>, mut keep: K, mut emit: F) -> Result<(), E>
where
    T: MergeSeq,
    F: FnMut(Vec<u8>, T) -> Result<(), E>,
    K: FnMut(&[u8], &T) -> bool,
{
    struct Node<T> {
        key: Vec<u8>,
        se: T,
        src: usize,
    }
    impl<T: MergeSeq> Ord for Node<T> {
        fn cmp(&self, other: &Self) -> CmpOrdering {
            match other.key.cmp(&self.key) {
                CmpOrdering::Equal => match self.se.seq().cmp(&other.se.seq()) {
                    // 同键：**seq 大的先出**；seq 也相同才回到源序（小 = 更新）
                    CmpOrdering::Equal => other.src.cmp(&self.src),
                    o => o,
                },
                o => o,
            }
        }
    }
    impl<T: MergeSeq> PartialOrd for Node<T> {
        fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
            Some(self.cmp(other))
        }
    }
    impl<T: MergeSeq> PartialEq for Node<T> {
        fn eq(&self, other: &Self) -> bool {
            self.cmp(other) == CmpOrdering::Equal
        }
    }
    impl<T: MergeSeq> Eq for Node<T> {}

    // The heap owns each consumed head. Advancing moves the next row directly,
    // preserving maximum seq / earliest source ties without cloning payloads.
    let heap_row = |key: Vec<u8>, row: T| {
        #[cfg(test)]
        if compaction_reference::enabled() { return (key.clone(), row.clone()); }
        (key, row)
    };
    let mut heap = BinaryHeap::new();
    for (s, source) in sources.iter_mut().enumerate() {
        if let Some(row) = source.next() {
            let (key, se) = row?;
            let (key, se) = heap_row(key, se);
            heap.push(Node { key, se, src: s });
        }
    }
    let advance = |srcs: &mut [TryMergeInput<T, E>],
                   heap: &mut BinaryHeap<Node<T>>,
                   s: usize| -> Result<(), E> {
        if let Some(row) = srcs[s].next() {
            let (key, se) = row?;
            let (key, se) = heap_row(key, se);
            heap.push(Node { key, se, src: s });
        }
        Ok(())
    };
    while let Some(n) = heap.pop() {
        advance(&mut sources, &mut heap, n.src)?;

        // 收集所有同 key 条目，选最优：**seq 大者赢**；seq 相同才回到"源序小者赢"。
        let key = n.key;
        let mut best_src = n.src;
        let mut best = n.se;
        while heap.peek().map(|p| p.key == key).unwrap_or(false) {
            let m = heap.pop().unwrap();
            advance(&mut sources, &mut heap, m.src)?;
            if m.se.seq() > best.seq() || (m.se.seq() == best.seq() && m.src < best_src) {
                best_src = m.src;
                best = m.se;
            }
        }

        if keep(&key, &best) {
            #[cfg(test)]
            let key = if compaction_reference::enabled() { key.clone() } else { key };
            emit(key, best)?;
        }
    }
    Ok(())
}

/// 兼容：归并到 Vec
pub fn merge_latest_streaming<F>(
    sources: Vec<MergeInput<'_, StoredEntry>>,
    mut keep: F,
) -> Vec<(Vec<u8>, StoredEntry)>
where
    F: FnMut(&[u8], &StoredEntry) -> bool,
{
    let mut out = Vec::new();
    merge_streaming(sources, &mut keep, |k, se| out.push((k, se)));
    out
}

/// `Vec<Vec<…>>` 版的便捷入口（内存表切片、以及测试用）
pub fn merge_latest(
    sources: Vec<Vec<(Vec<u8>, StoredEntry)>>,
) -> Vec<(Vec<u8>, StoredEntry)> {
    merge_latest_streaming(vec_inputs(sources), |_, _| true)
}


/// **真流式**归并写入多个 SST：边 merge 边 `add`，达到 `target_bytes` 就 `finish` 落盘，
/// 不先物化全量 `Vec`（单 builder 缓冲不超过约一个 SST）。
pub fn merge_to_ssts<F>(
    sources: Vec<MergeInput<'_, StoredEntry>>,
    mut keep: F,
    new_builder: impl FnMut() -> SstBuilder,
    target_bytes: usize,
) -> std::io::Result<Vec<SstFile>>
where
    F: FnMut(&[u8], &StoredEntry) -> bool,
{
    let sources = sources.into_iter().map(|s| Box::new(s.map(Ok)) as TryMergeInput<'_, StoredEntry, std::io::Error>).collect();
    merge_to_ssts_with_action(sources, |key, entry| {
        if keep(key, entry) { CompactionAction::Keep } else { CompactionAction::Discard }
    }, new_builder, target_bytes)
}

/// Dropping a winner is safe only when no unmerged source can reveal an older
/// value. Otherwise keep its sequence number as a deletion barrier.
pub(crate) enum CompactionAction { Keep, Discard, RetainDeletion }

/// Errors discard all completed unpublished outputs, including their handles.
pub(crate) struct UnpublishedSsts(pub(crate) Vec<SstFile>);
impl Drop for UnpublishedSsts {
    fn drop(&mut self) {
        for file in self.0.drain(..) {
            let path = file.path.clone(); drop(file);
            let _ = std::fs::remove_file(path);
        }
    }
}

pub(crate) fn merge_to_ssts_with_action(
    sources: Vec<TryMergeInput<'_, StoredEntry, std::io::Error>>,
    mut action: impl FnMut(&[u8], &StoredEntry) -> CompactionAction,
    mut new_builder: impl FnMut() -> SstBuilder,
    target_bytes: usize,
) -> std::io::Result<Vec<SstFile>> {
    let mut out = UnpublishedSsts(Vec::new());
    let mut builder: Option<SstBuilder> = None;
    try_merge_streaming(sources, |_, _| true, |k, mut se| {
        match action(&k, &se) {
            CompactionAction::Keep => {},
            CompactionAction::Discard => return Ok(()),
            CompactionAction::RetainDeletion => se.value = EntryValue::Drop,
        }
        if builder.is_none() {
            builder = Some(new_builder());
        }
        let b = builder.as_mut().unwrap();
        b.add(k, se);
        if b.estimated_bytes() >= target_bytes {
            out.0.push(builder.take().unwrap().finish()?);
        }
        Ok(())
    })?;
    if let Some(b) = builder {
        if !b.is_empty() {
            out.0.push(b.finish()?);
        }
    }
    Ok(std::mem::take(&mut out.0))
}


#[cfg(test)]
mod tests {
    #[test]
    fn vacuum_light_owned_blocks_match_cached_ranges_and_surface_bad_blocks() {
        let path = tmp("vacuum_owned");
        let mut builder = SstBuilder::with_options(1, &path, 3, false);
        for i in 0..12_u64 { builder.add(i.to_be_bytes().to_vec(), StoredEntry::put(i, Some(vec![7; 32]))); }
        let mut sst = builder.finish().unwrap();
        let cache = Arc::new(Mutex::new(BlockCache::new(2)));
        sst.set_block_cache(cache.clone());
        for (lo, hi, snap) in [(0_u64, 12_u64, u64::MAX), (4, 9, 7), (3, 3, 4)] {
            let (lo, hi) = (lo.to_be_bytes(), hi.to_be_bytes());
            let expected: Vec<_> = sst.light_iter(Some(&lo), Some(&hi), snap).collect();
            let resident = cache.lock().unwrap().len();
            let clones = entry_value_clones();
            let actual = sst.vacuum_light_iter(Some(&lo), Some(&hi), snap).collect::<std::io::Result<Vec<_>>>().unwrap();
            assert_eq!(actual, expected);
            assert_eq!(entry_value_clones(), clones, "metadata scan must never clone values");
            assert_eq!(cache.lock().unwrap().len(), resident, "maintenance cannot refill point cache");
        }
        let mut raw = std::fs::read(&path).unwrap();
        raw[sst.index[1].offset as usize + 4] ^= 1;
        std::fs::write(&path, raw).unwrap();
        let mut rows = sst.vacuum_light_iter(None, None, u64::MAX);
        for _ in 0..3 { assert!(rows.next().unwrap().is_ok()); }
        assert_eq!(rows.next().unwrap().unwrap_err().kind(), std::io::ErrorKind::InvalidData);
        assert!(rows.next().is_none());
    }
    use super::*;
    use std::assert;
    use std::assert_eq;
    use std::panic;
    use std::eprintln;
    use std::format;
    use std::matches;

    #[test]
    fn compaction_owns_blocks_and_counts_legacy_fallback() {
        use crate::lsm_tree::lsm_tree::maintenance_probe;
        let path = tmp("owned_compaction");
        let mut builder = SstBuilder::with_options(1, &path, 2, false);
        for i in 0..9_u64 { builder.add(i.to_be_bytes().to_vec(), StoredEntry::put(i, Some(vec![i as u8; 11]))); }
        let mut sst = builder.finish().unwrap();
        let cache = Arc::new(Mutex::new(BlockCache::new(2)));
        sst.set_block_cache(cache.clone());
        let scope = maintenance_probe::Scope::start();
        let clones = entry_value_clones();
        let owned: Vec<_> = sst.compaction_input().collect::<std::io::Result<_>>().unwrap();
        assert_eq!(entry_value_clones(), clones, "owned block input must not clone values");
        assert_eq!(scope.stats().owned_input_rows, 9);
        assert_eq!(scope.stats().compaction_blocks, 5);
        assert!(cache.lock().unwrap().is_empty(), "compaction must not fill the point cache");
        drop(scope);
        assert_eq!(sst.iter_entries(), owned);
        assert_eq!(cache.lock().unwrap().len(), 2);
        let resident = cache.lock().unwrap().len();
        let clones = entry_value_clones();
        assert_eq!(sst.compaction_input().collect::<std::io::Result<Vec<_>>>().unwrap(), owned);
        assert_eq!(entry_value_clones(), clones);
        assert_eq!(cache.lock().unwrap().len(), resident);
        sst.entries_cache = Some(owned);
        assert!(matches!(sst.compaction_input(), CompactionRows::LegacyShared(_)));
        let scope = maintenance_probe::Scope::start();
        let clones = entry_value_clones();
        assert_eq!(sst.compaction_input().collect::<std::io::Result<Vec<_>>>().unwrap().len(), 9);
        assert_eq!(entry_value_clones() - clones, 9);
        assert_eq!(scope.stats().legacy_input_clones, 9);
        assert_eq!(scope.stats().owned_input_rows, 0);
    }

    #[test]
    fn compaction_corruption_is_an_error_instead_of_eof() {
        let path = tmp("strict_input");
        let mut builder = SstBuilder::with_options(1, &path, 3, false);
        for i in 0..9_u64 { builder.add(i.to_be_bytes().to_vec(), StoredEntry::put(i, Some(vec![7]))); }
        let sst = builder.finish().unwrap();
        let mut raw = std::fs::read(&path).unwrap();
        raw[sst.index[1].offset as usize + 4] ^= 1;
        std::fs::write(&path, raw).unwrap();
        assert_eq!(sst.iter_all().count(), 3, "old read iterator silently truncates at the bad block");
        let mut strict = sst.compaction_input();
        for _ in 0..3 { assert!(strict.next().unwrap().is_ok()); }
        assert_eq!(strict.next().unwrap().unwrap_err().kind(), std::io::ErrorKind::InvalidData);
        assert!(strict.next().is_none());
    }

    #[test]
    fn merge_heap_moves_values_and_preserves_seq_source_ties() {
        let sources = || vec![
            vec![(b"a".to_vec(), StoredEntry::put(1, Some(vec![1]))),
                (b"a".to_vec(), StoredEntry::put(3, Some(vec![3]))), (b"b".to_vec(), StoredEntry::put(4, None))],
            vec![(b"a".to_vec(), StoredEntry::put(3, Some(vec![9]))),
                (b"b".to_vec(), StoredEntry::put(2, Some(vec![2]))), (b"c".to_vec(), StoredEntry::drop_at(5))],
        ];
        let clones = entry_value_clones();
        let rows = merge_latest(sources());
        assert_eq!(entry_value_clones(), clones, "heap must move every consumed head");
        assert_eq!(rows, vec![(b"a".to_vec(), StoredEntry::put(3, Some(vec![3]))),
            (b"b".to_vec(), StoredEntry::put(4, None)), (b"c".to_vec(), StoredEntry::drop_at(5))]);
        let _reference = compaction_reference::Scope::start();
        let clones = entry_value_clones();
        assert_eq!(merge_latest(sources()), rows);
        assert_eq!(entry_value_clones() - clones, 6, "old heap would fail the zero-clone guard");
    }

    /// 返回 **RAII 句柄**（M82）：把它声明在引擎之前，用例结束（含 panic 展开）自动删，
    /// 不必再指望人记得写 `cleanup` —— 而"引擎还开着就删"在 Windows 上是静默失败。
    fn tmp(tag: &str) -> crate::common::ScratchPath {
        crate::common::ScratchPath::new("sst", tag, ".sst")
    }

    /// M42 的**格式钉子**：同一批条目必须产出**逐字节相同**的 SST。
    ///
    /// 为什么需要它：这一刀改的是 builder 的内部形状（"攒满一整张表才编块"→"攒满一块就落盘"），
    /// 那种改动出错的方式不是崩，而是**悄悄换了文件格式** —— 块的切法、index 的偏移、bloom 的
    /// 位图宽度，任何一个动了，旧文件还能读、新文件读得回，但两边对不上同一份口径。
    /// 所以钉两个数：文件长度与全文件 crc32。
    ///
    /// ⚠️ 这两个常数是在**改前**的实现上跑出来的（下面断言里写的是当时的实测值）⇒
    /// 改完之后红就代表格式被动过，不是测试写错了。
    #[test]
    fn test_sst_builder_byteprint_is_stable() {
        let path = tmp("byteprint");
        {
            let mut b = SstBuilder::with_options(7, &path, 64, true).with_bloom_bits(10);
            for i in 0..1000usize {
                b.add(
                    format!("k{i:06}").into_bytes(),
                    StoredEntry::put(i as u64, Some(vec![(i % 251) as u8; i % 13])),
                );
            }
            let sst = b.finish().unwrap();
            // 回读必须与写入逐条一致（这一半不需要 golden 也成立）
            let back = sst.iter_entries();
            assert_eq!(back.len(), 1000, "读回来的条目数不对");
            assert_eq!(back[0].0, b"k000000".to_vec());
            assert_eq!(back[999].0, b"k000999".to_vec());
        }
        let bytes = std::fs::read(&path).unwrap();
        let fc = crc32(&bytes);
        eprintln!("[byteprint] 长度={} crc32={fc:08x}", bytes.len());
        assert_eq!(bytes.len(), 31_823, "SST 字节长度变了 ⇒ 块切法/索引/布隆的口径被动过");
        assert_eq!(fc, 0x7911_d92d, "SST 内容 crc32 变了 ⇒ 写出去的字节与改前不一致");
        let _ = std::fs::remove_file(&path);
    }

    /// 增量账必须与旧算法**逐字节同口径**（M30）。
    ///
    /// 这条不是装饰：`est_bytes` 是把一句求和手搬进 `add` 里维护的，搬掉那个 `+8`（每行一条的
    /// 帧头）就会让"什么时候切文件"悄悄变 —— 输出文件的数量与大小跟着变，而在压测表上那只
    /// 表现为"慢了 3%"，没人会往这里查。
    #[test]
    fn test_builder_estimated_bytes_matches_slow_scan() {
        let scratch = tmp("est_parity");
        let mut b = SstBuilder::with_options(1, scratch.to_path_buf(), 64, false);
        assert_eq!(b.estimated_bytes(), 0, "空表两边都该是 0");
        // M42 之后 builder 不再握着整张表（攒满一块就落盘）⇒ 慢口径改在**调用方自己那份输入**上算。
        // 对拍的性质一点没变：同一个口径、同一批条目，比的还是"增量账 vs 逐条慢算"。
        let fed: Vec<(Vec<u8>, StoredEntry)> = (0..500usize)
            .map(|i| {
                (
                    format!("k{i:05}").into_bytes(),
                    StoredEntry::put(i as u64, Some(vec![b'v'; i % 37])),
                )
            })
            .collect();
        for (k, se) in fed.iter().cloned() {
            b.add(k, se);
        }
        assert_eq!(
            b.estimated_bytes(),
            sum_entries_bytes(&fed),
            "增量账与慢口径不一致 ⇒ 切文件的时机变了"
        );
        // 空 value 与墓碑也得同口径（那句 `+8` 是行头，不是常数 8）
        let tomb: Vec<(Vec<u8>, StoredEntry)> = vec![
            (b"a".to_vec(), StoredEntry::put(1, None)),
            (b"b".to_vec(), StoredEntry::drop_at(2)),
            (b"cccc".to_vec(), StoredEntry::put(3, Some(b"xyz".to_vec()))),
        ];
        let scratch = tmp("est_tomb");
        let mut t = SstBuilder::with_options(2, scratch.to_path_buf(), 64, false);
        for (k, se) in tomb.iter().cloned() {
            t.add(k, se);
        }
        assert_eq!(t.estimated_bytes(), sum_entries_bytes(&tomb));
    }

    /// "要不要切文件"这个判定**不许走整表**（M30）。
    ///
    /// 触发它的测量：压实的耗时拆分台架 `compaction_cost_split` 显示 600k 行那一趟 18.0 s 里
    /// **94% 在"归并+写盘"**，而"物化输入"只 0.62 s、"纯归并"只 0.48 s ⇒ 病**不在**我原先点名的
    /// "整篇物化"，在 `merge_to_ssts` 每交出一行都问一次 `estimated_bytes()`，而旧实现把当前表
    /// 从头求和一遍 ⇒ 每个输出文件 O(表内行数²)。这是 M19（flush 判定每插一行数一遍 memtable）
    /// 同一个病的第二次出现，这次在 SST 写侧。
    ///
    /// 尺子 = **为判定走过的条目数**，生产路径必须是 0；同一条用例里主动慢算一次，
    /// 证明这把尺子确实数得到（不然上面那条断言是空转）。
    #[test]
    fn test_builder_size_check_does_not_walk_entries_per_add() {
        const N: usize = 4000;
        let scratch = tmp("est_hot");
        let mut b = SstBuilder::with_options(1, scratch.to_path_buf(), 64, false);
        let s0 = builder_size_walks();
        // M42：builder 不再握着整张表 ⇒ 慢口径改在调用方自己那份输入上算（对拍性质不变）
        let mut fed: Vec<(Vec<u8>, StoredEntry)> = Vec::with_capacity(N);
        for i in 0..N {
            let e: (Vec<u8>, StoredEntry) = (
                format!("k{i:05}").into_bytes(),
                StoredEntry::put(i as u64, Some(vec![b'x'; 40])),
            );
            b.add(e.0.clone(), e.1.clone());
            fed.push(e);
            // 照 `merge_to_ssts` 的形状每行问一次"够不够切一个文件"
            if b.estimated_bytes() >= usize::MAX {
                panic!("哨兵条件不该命中：这条用例只关心判定走过多少条目");
            }
        }
        let walks = builder_size_walks() - s0;
        assert_eq!(
            walks, 0,
            "灌 {N} 行为回答「该不该切文件」走了 {walks} 个条目 ⇒ 判定又变回每行重求和一遍整表\
             （旧形状 ≈ {} 步）",
            (N * (N - 1) / 2) as u64
        );
        // 对照：主动慢算一次，探针必须数得到 —— 注意**单位**：尺子数的是"走过的条目数"，
        // 而 `sum_entries_bytes` 返回的是字节数，两者不能直接比。
        let w1 = builder_size_walks();
        let slow_bytes = sum_entries_bytes(&fed);
        let control_steps = builder_size_walks() - w1;
        assert!(
            control_steps >= N as u64,
            "探针空转：慢口径只走了 {control_steps} 个条目，对照必须数到 ≥{N}"
        );
        assert!(
            slow_bytes > 0,
            "慢口径返回 0 ⇒ 这条对照本身没建立，上面的 0 断言不算数"
        );
        assert_eq!(
            b.estimated_bytes(),
            slow_bytes,
            "增量账与慢口径不一致 ⇒ 上面那条 0 的判据也算不上数"
        );
        assert_eq!(b.len(), N, "`len()` 不再是「加了多少条」⇒ M42 的计数账被改动了");
    }

    /// M42：builder 必须**攒满一块就落盘**，不许在 `finish()` 之前握住整张表。
    ///
    /// 判据（单位=条）：灌 N 条、块宽 B ⇒ "同时在手的未落盘条目"最多 **B** 条
    /// （`builder_held_max() ≤ B`）。旧形状这里读到的就是 **N**（`compaction_cost_split` 在
    /// 600k 档读到单文件 18 333 条 ≈ 4.9 MB，占那一段峰值 6.3 MB 的 78% —— 就是要拆的东西）。
    ///
    /// 另钉一条"还剩多少、为什么留"：布隆的位图宽度 `m = 条数 × bits_per_key` 要等总条数知道
    /// 才能定 ⇒ 每条还留 8 B 哈希（`builder_hashed_max()` 必须正好 = N）。这一小截是**故意**留的，
    /// 写死在这里是为了让下次动它的人看见自己动的是什么（要清掉它得让调用方预估条数，
    /// 那是另一个取舍：布隆的 m 一改，写出去的位图与现在就不是同一份口径了）。
    ///
    /// 语义一起钉：文件读回来逐条一致、footer 里的 min/max 是第一把与最后一把键。
    /// （**格式逐字节不变**由 `test_sst_builder_byteprint_is_stable` 那条 golden 钉着。）
    ///
    /// 变异验证：把 `add` 末尾那句"攒满就 `emit_block`"去掉（回到攒到 finish 才写）⇒ 红在
    /// "单块握住 4000 条未落盘条目（块宽 64）"。
    #[test]
    fn test_builder_emits_blocks_incrementally() {
        const N: usize = 4000;
        const BLOCK: usize = 64;
        let path = tmp("incr_emit");
        reset_builder_held_max();
        reset_builder_hashed_max();
        {
            let mut b = SstBuilder::with_options(3, &path, BLOCK, true);
            for i in 0..N {
                b.add(
                    format!("k{i:06}").into_bytes(),
                    StoredEntry::put(i as u64, Some(vec![b'v'; 20])),
                );
            }
            assert_eq!(b.len(), N, "`len()` 该是加进去的条数");
            let sst = b.finish().unwrap();
            let back = sst.iter_entries();
            assert_eq!(back.len(), N, "落盘的条目数不对 ⇒ 分块写丢行了");
            assert_eq!(back.first().unwrap().0, b"k000000".to_vec());
            assert_eq!(
                back.last().unwrap().0,
                format!("k{:06}", N - 1).into_bytes()
            );
            assert_eq!(sst.min_key, b"k000000".to_vec(), "footer 的 min_key 不对");
            assert_eq!(
                sst.max_key,
                format!("k{:06}", N - 1).into_bytes(),
                "footer 的 max_key 不对"
            );
        }
        let held = builder_held_max();
        let hashed = builder_hashed_max();
        eprintln!(
            "[incr emit] N={N} 块宽={BLOCK} ⇒ 未落盘条目最多 {held} 条、布隆哈希最多 {hashed} 条"
        );
        assert!(
            held <= BLOCK as u64,
            "单块握住 {held} 条未落盘条目（块宽 {BLOCK}）⇒ 又变回攒到 finish 才写"
        );
        assert_eq!(
            hashed,
            N as u64,
            "布隆哈希表不是每条一份 ⇒ 位图口径动了（那是格式变更，另说）"
        );
        let _ = std::fs::remove_file(&path);
    }

    /// 真调用点上这条账也必须是 0（M30）：上面那条量的是形状，这条量的是 `merge_to_ssts` 本身。
    #[test]
    fn test_merge_to_ssts_does_not_walk_entries_for_size_check() {
        const PER: usize = 3000;
        let sources: Vec<Vec<(Vec<u8>, StoredEntry)>> = vec![
            (0..PER)
                .map(|i| {
                    (
                        format!("a{i:06}").into_bytes(),
                        StoredEntry::put(i as u64, Some(vec![1u8; 60])),
                    )
                })
                .collect(),
            (0..PER)
                .map(|i| {
                    (
                        format!("b{i:06}").into_bytes(),
                        StoredEntry::put(i as u64, Some(vec![1u8; 60])),
                    )
                })
                .collect(),
        ];
        let mut id = 0u64;
        let s0 = builder_size_walks();
        // 压出来的 SST 由守卫持有：`with_options` 收的是**路径**，文件要到压实时才写出来，
        // 守卫必须活过 `merge_to_ssts` 那一趟（先声明、再借进闭包）
        let mut sst_guards: Vec<crate::common::ScratchPath> = Vec::new();
        // target 给 64 KiB ⇒ 6000 行该切出好几个文件（"每行问一次判定"这一步真的被走到）
        let files = merge_to_ssts(vec_inputs(sources), |_, _| true, || {
            id += 1;
            let p = tmp(&format!("est_merge_{id}"));
            let owned = p.to_path_buf();
            sst_guards.push(p);
            SstBuilder::with_options(id, owned, 64, false)
        }, 64 * 1024)
        .unwrap();
        let walks = builder_size_walks() - s0;
        assert!(
            files.len() >= 3,
            "形状没造出来：target 64 KiB 下 6000 行只切了 {} 个文件 ⇒ 判定可能一次都没问过",
            files.len()
        );
        assert_eq!(walks, 0, "{} 行 × {} 个文件的压实里，尺寸判定走了 {walks} 个条目", PER * 2, files.len());
        drop(files);
    }

    /// 命中一次的代价**不随驻留块数涨**（M23 自己引入的回归，留下的守卫）。
    ///
    /// 旧实现每次命中都要在 `VecDeque<CacheKey>` 里 `position()` 找那个键 ⇒ O(驻留块数)。
    /// 而自动档恰恰是去加大驻留块数的那一刀：40000 键那一档容量 256→1024，**未命中次数一动不动
    /// （0.15/次），有序点读却从 22.2 µs/次 涨到 36.0 µs/次** —— 缓存越大每次命中越贵，省下来的
    /// 未命中被自己的 LRU 吃了回去。所以这条尺子量的是"检查过几个槽位"，不看墙钟。
    #[test]
    fn lru_touch_cost_does_not_scale_with_residency() {
        // 返回：每次命中检查的槽位数 × 100（整数比，避免浮点）
        fn per_get(resident: usize, cap: usize) -> u64 {
            let mut c = BlockCache::new(cap);
            let blk = Arc::new(DataBlock {
                entries: Vec::new(),
            });
            for i in 0..resident {
                c.put(1, i as u64, 0, blk.clone());
            }
            assert_eq!(c.len(), resident, "必须全都驻留，否则量的就不是命中");

            let before = lru_inspections();
            for _ in 0..4 {
                for i in 0..resident {
                    assert!(c.get(1, i as u64, 0).is_some(), "读第 {i} 块落空（不该未命中）");
                }
            }
            (lru_inspections() - before) * 100 / (resident as u64 * 4)
        }

        let small = per_get(64, 128);
        let big = per_get(1024, 2048);
        eprintln!("[LRU] 每次命中检查的槽位数×100：驻留 64 → {small}，驻留 1024 → {big}");
        assert!(small > 0, "尺子在空转：一次命中都没检查过槽位");
        assert!(
            big * 100 <= small * 150 + 100,
            "驻留 64→1024（16 倍）时每次命中的代价不该跟着涨：{small} vs {big}"
        );
    }

    /// `range_at` 改成用稀疏索引定位起始块之后，命中结果必须与「扫全文件再筛」逐条一致，
    /// 且不能再把整个文件读一遍。
    ///
    /// 这条对拍是改动自带的安全网：全仓原本**没有**任何针对 `range_at` 的正确性测试。
    #[test]
    fn test_range_at_window_equals_full_scan() {
        let path = tmp("window");
        let mut b = SstBuilder::with_options(1, &path, 3, true);
        // 40 个 key × 2 个 seq，块容量 3 条 ⇒ 十幾块；同 key 多 seq 用来验「取最大可见 seq」
        for i in 0..40u32 {
            let k = format!("k{i:03}").into_bytes();
            b.add(k.clone(), StoredEntry::put(i as u64 * 2, Some(b"v".to_vec())));
            b.add(k.clone(), StoredEntry::put(i as u64 * 2 + 1, Some(b"w".to_vec())));
        }
        let sst = b.finish().unwrap();

        // 全文件区间 = 暴力参照
        let all = sst.range_at(b"", &[0xffu8; 8], u64::MAX);
        assert_eq!(all.len(), 40, "每个 key 只应出一条（最大可见 seq）");
        assert!(
            all.iter().all(|(_, se)| se.value == EntryValue::Put(Some(b"w".to_vec()))),
            "同 key 应取最大 seq 的那条"
        );

        let windows: Vec<(Vec<u8>, Vec<u8>)> = vec![
            (b"k007".to_vec(), b"k013".to_vec()),
            (b"k000".to_vec(), b"k000".to_vec()),
            (b"k039".to_vec(), b"k039".to_vec()),
            (b"k020".to_vec(), b"zzz".to_vec()),
            (b"k000".to_vec(), b"k039".to_vec()),
            (b"aaa".to_vec(), b"zzz".to_vec()),
            (Vec::new(), Vec::new()), // 双端皆空 ⇒ high 小于一切，区间为空
            (b"k015".to_vec(), b"k005".to_vec()), // 上下界颠倒 ⇒ 空
        ];
        for (low, high) in windows {
            let want: Vec<(Vec<u8>, StoredEntry)> = all
                .iter()
                .filter(|(k, _)| {
                    k.as_slice() >= low.as_slice() && k.as_slice() <= high.as_slice()
                })
                .cloned()
                .collect();
            let before = block_reads();
            let got = sst.range_at(&low, &high, u64::MAX);
            let used = block_reads() - before;
            assert_eq!(
                got, want,
                "区间 [{low:?}, {high:?}] 的 range_at 与全扫筛出的不一致"
            );
            if want.is_empty() || got.len() <= 3 {
                assert!(
                    used <= 6,
                    "窄区间不该读全文件（[{low:?}, {high:?}] 读了 {used} 块）"
                );
            }
        }
        let _ = std::fs::remove_file(&path);
    }

    /// `find_back` 必须给出 `range_at` 结果里的**最后一条**（区间内最大 key），
    /// 谓词不满足时要继续往更小的 key 退，并且只碰区间末尾附近的块。
    #[test]
    fn test_find_back_matches_range_at_tail() {
        let path = tmp("back");
        let mut b = SstBuilder::with_options(1, &path, 3, true);
        for i in 0..40u32 {
            let k = format!("k{i:03}").into_bytes();
            b.add(k.clone(), StoredEntry::put(i as u64, Some(b"w".to_vec())));
        }
        // 末尾再放一条 Drop，验证「每 key 定论后交给 accept，而不是跳过该 key 回看更旧 seq」
        b.add(b"k040".to_vec(), StoredEntry::drop_at(99));
        let sst = b.finish().unwrap();

        let low = b"k010".to_vec();
        let high = b"k025".to_vec();
        let rows = sst.range_at(&low, &high, u64::MAX);
        assert_eq!(rows.len(), 16);

        let before = block_reads();
        let hit = sst.find_back(&low, &high, u64::MAX, |_, _| true);
        let used = block_reads() - before;
        assert_eq!(
            hit.map(|(k, _)| k),
            rows.last().map(|(k, _)| k.clone()),
            "find_back 应当命中区间内最大 key"
        );
        assert!(
            used <= 3,
            "反向定位只应碰区间末尾附近的块，实际读了 {used} 块（全文件 {} 块）",
            14
        );

        // 谓词只接受 k014 ⇒ 应从 k025 一路退到 k014，返回对的 key
        let hit2 = sst.find_back(&low, &high, u64::MAX, |k, _| k == b"k014");
        assert_eq!(hit2.map(|(k, _)| k), Some(b"k014".to_vec()));

        // 区间内全是 Drop ⇒ 谓词按 value 拒绝，必须一路走到区间下界外并返回 None
        let none = sst.find_back(b"k040", b"k040", u64::MAX, |_, se| {
            !matches!(se.value, EntryValue::Drop)
        });
        assert_eq!(none.map(|(k, _)| k), None, "区间内无可接受的条目时应返回 None");

        // 空区间 / 越界区间
        assert!(sst.find_back(&low, &b"k005".to_vec(), u64::MAX, |_, _| true).is_none());
        assert!(sst.find_back(b"zzz", b"zzzz", u64::MAX, |_, _| true).is_none());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_sst_pruning_back_and_get_keep_cross_block_versions() {
        use std::collections::BTreeMap;
        let path = tmp("pruning_versions");
        let mut builder = SstBuilder::with_options(1, &path, 2, false);
        builder.add(b"a".to_vec(), StoredEntry::put(1, Some(b"a".to_vec())));
        // 只要求键有序，不假设 seq 有序；b 的版本组跨五块。
        for seq in [5, 2, 9, 1, 7, 3, 11, 10] {
            let value = if seq == 10 {
                StoredEntry::drop_at(seq)
            } else {
                StoredEntry::put(seq, Some(vec![seq as u8]))
            };
            builder.add(b"b".to_vec(), value);
        }
        builder.add(b"c".to_vec(), StoredEntry::put(20, Some(b"c".to_vec())));
        let mut sst = builder.finish().unwrap();
        let all: Vec<_> = sst.iter_all().collect();
        for memory_backed in [false, true] {
            if memory_backed {
                // 同一折叠逻辑也覆盖旧格式的内存条目路径。
                sst.entries_cache = Some(all.clone());
            }
            for snap in [0, 1, 4, 7, 9, 10, 11, 20] {
                let mut expected = BTreeMap::<Vec<u8>, StoredEntry>::new();
                for (k, se) in &all {
                    if se.visible_at(snap) && expected.get(k).is_none_or(|old| se.seq >= old.seq) {
                        expected.insert(k.clone(), se.clone());
                    }
                }
                assert_eq!(sst.get_at(b"b", snap), expected.get(b"b".as_slice()).cloned());
                let scanned = sst.light_iter(Some(b"b"), Some(b"c"), snap).max_by_key(|(_, row)| row.seq);
                assert_eq!(scanned.map(|(_, row)| (row.seq, row.kind)),
                    expected.get(b"b".as_slice()).map(|row| (row.seq, row.value.kind_byte())),
                    "forward fallback must include the entire key across block boundaries");
                assert_eq!(sst.range_at(b"b", b"b", snap), expected.iter()
                    .filter(|(k, _)| k.as_slice() == b"b").map(|(k, se)| (k.clone(), se.clone())).collect::<Vec<_>>());
                let want = expected.iter().rev().find(|(_, se)| !matches!(se.value, EntryValue::Drop))
                    .map(|(k, se)| (k.clone(), se.clone()));
                assert_eq!(sst.find_back(b"a", b"c", snap, |_, se| !matches!(se.value, EntryValue::Drop)), want);
                let mut called = Vec::new();
                assert!(sst.find_back(b"a", b"c", snap, |k, _| {
                    called.push(k.to_vec());
                    false
                }).is_none());
                assert_eq!(called, expected.keys().rev().cloned().collect::<Vec<_>>(), "每键只允许拒绝一次");
            }
        }
        drop(sst);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn point_get_projects_only_matching_rows_and_winning_value() {
        let path = tmp("point_key_interval");
        let mut builder = SstBuilder::with_options(1, &path, 128, false);
        for i in 0..100 { builder.add(format!("k{i:03}").into_bytes(), StoredEntry::put(1, Some(vec![i as u8; 64]))); }
        let mut sst = builder.finish().unwrap();
        let all: Vec<_> = sst.iter_all().collect();
        for resident in [false, true] {
            if resident { sst.entries_cache = Some(all.clone()); }
            assert!(sst.get(b"k050").is_some()); // Warm both arms before counting.
            for old in [true, false] {
                let mode = old.then(point_read_reference::Scope::start);
                let walks = sst_entry_walks();
                let clones = entry_value_clones();
                let light = sst.get_light_at(b"k050", 1).unwrap();
                assert_eq!((light.seq, light.kind), (1, 0));
                assert_eq!(sst_entry_walks() - walks, if old { 100 } else { 1 });
                assert_eq!(entry_value_clones() - clones, u64::from(old), "metadata lookup must not copy payload");
                let clones = entry_value_clones();
                assert_eq!(sst.get_at(b"k050", 1).unwrap().value, EntryValue::Put(Some(vec![50; 64])));
                assert_eq!(entry_value_clones() - clones, 1, "reference must not clone the winning payload a second time");
                drop(mode);
            }
        }
    }

    #[test]
    fn test_sst_pruning_half_open_bounds_and_empty_key() {
        let path = tmp("pruning_bounds");
        let mut builder = SstBuilder::with_options(1, &path, 1, false);
        builder.add(b"m".to_vec(), StoredEntry::put(1, Some(vec![1])));
        let sst = builder.finish().unwrap();
        let before = block_reads();
        assert_eq!(sst.light_iter(None, Some(b"m"), 1).count(), 0);
        assert_eq!(sst.light_iter(Some(b"n"), None, 1).count(), 0);
        assert_eq!(sst.light_iter(Some(b"z"), Some(b"a"), 1).count(), 0);
        assert_eq!(block_reads() - before, 0, "界外源不能为了初始化先读一块");
        assert_eq!(sst.light_iter(Some(b"m"), None, 1).count(), 1);
        assert!(block_reads() > before, "相交源是计数正控");
        drop(sst);
        let _ = std::fs::remove_file(&path);

        let path = tmp("pruning_empty_key");
        let mut builder = SstBuilder::with_options(2, &path, 1, false);
        builder.add(Vec::new(), StoredEntry::put(1, Some(vec![2])));
        let sst = builder.finish().unwrap();
        assert!(sst.may_overlap(b"", b""));
        assert_eq!(sst.get_at(b"", 1), Some(StoredEntry::put(1, Some(vec![2]))));
        assert_eq!(sst.find_back(b"", b"", 1, |_, _| true).unwrap().0, Vec::<u8>::new());
        assert_eq!(sst.light_iter(None, None, 1).count(), 1);
        assert_eq!(sst.light_iter(None, Some(b""), 1).count(), 0);
        drop(sst);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_write_read_get_v2() {
        let path = tmp("basic");
        let mut b = SstBuilder::new(1, &path);
        b.add(b"a".to_vec(), StoredEntry::put(1, Some(b"1".to_vec())));
        b.add(b"b".to_vec(), StoredEntry::put(2, None));
        b.add(b"c".to_vec(), StoredEntry::drop_at(3));
        let sst = b.finish().unwrap();
        assert_eq!(
            sst.get(b"a").map(|e| e.value),
            Some(EntryValue::Put(Some(b"1".to_vec())))
        );
        assert_eq!(sst.get(b"a").unwrap().seq, 1);
        assert_eq!(sst.get(b"b").map(|e| e.value), Some(EntryValue::Put(None)));
        assert_eq!(sst.get(b"c").map(|e| e.value), Some(EntryValue::Drop));
        assert!(sst.is_v2);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_snapshot_hides_newer_seq() {
        let path = tmp("snap");
        let mut b = SstBuilder::new(1, &path);
        b.add(b"k".to_vec(), StoredEntry::put(5, Some(b"old".to_vec())));
        b.add(b"k".to_vec(), StoredEntry::put(10, Some(b"new".to_vec())));
        let sst = b.finish().unwrap();
        assert_eq!(
            sst.get_at(b"k", 7).map(|e| e.value),
            Some(EntryValue::Put(Some(b"old".to_vec())))
        );
        assert_eq!(
            sst.get_at(b"k", 10).map(|e| e.value),
            Some(EntryValue::Put(Some(b"new".to_vec())))
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_mmap() {
        let path = tmp("mmap");
        let mut b = SstBuilder::new(1, &path);
        b.add(b"x".to_vec(), StoredEntry::put(1, Some(b"y".to_vec())));
        let mut sst = b.finish().unwrap();
        sst.enable_mmap().unwrap();
        assert!(sst.has_mmap());
        assert_eq!(
            sst.get(b"x").map(|e| e.value),
            Some(EntryValue::Put(Some(b"y".to_vec())))
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_merge_latest() {
        let older = vec![
            (
                b"a".to_vec(),
                StoredEntry::put(1, Some(b"old".to_vec())),
            ),
            (b"b".to_vec(), StoredEntry::put(1, Some(b"b".to_vec()))),
        ];
        let newer = vec![
            (
                b"a".to_vec(),
                StoredEntry::put(2, Some(b"new".to_vec())),
            ),
            (b"c".to_vec(), StoredEntry::drop_at(2)),
        ];
        let m = merge_latest(vec![newer, older]);
        assert_eq!(m.len(), 3);
        assert_eq!(m[0].1.value, EntryValue::Put(Some(b"new".to_vec())));
    }

    #[test]
    fn test_stream_iter() {
        let path = tmp("iter");
        let mut b = SstBuilder::with_options(2, &path, 3, true);
        for i in 0..20u32 {
            b.add(
                format!("k{i:04}").into_bytes(),
                StoredEntry::put(i as u64, Some(format!("v{i}").into_bytes())),
            );
        }
        let sst = b.finish().unwrap();
        assert_eq!(sst.iter_all().count(), 20);
        let _ = std::fs::remove_file(&path);
    }

    /// `find_block` 改成二分之后必须同时满足两件事：
    /// ① 与线性扫**逐 key 给出同一个块**（块内/块边界/两块之间/文件内外都算）；
    /// ② 定位代价只能是 log 阶 —— 原先线性扫平均要走 ~块数/2 次比较，那就是点查路径上
    ///    唯一一处随**库内行数**变贵的纯 CPU（行数 20 万 ⇒ 每探一个块白走 ~1500 次）。
    #[test]
    fn test_find_block_is_binary_and_agrees_with_linear() {
        let path = tmp("find_block");
        let mut b = SstBuilder::with_options(1, &path, 8, true);
        for i in 0..2000u32 {
            b.add(
                format!("k{i:05}").into_bytes(),
                StoredEntry::put(i as u64, Some(b"v".to_vec())),
            );
        }
        let sst = b.finish().unwrap();
        let blocks = sst.index.len();
        assert!(
            blocks >= 200,
            "样本要有足够多的块才分得出线性与二分（实际 {blocks}）"
        );

        let linear = |key: &[u8]| -> Option<usize> {
            let mut best = None;
            for (i, ie) in sst.index.iter().enumerate() {
                if ie.first_key.as_slice() <= key {
                    best = Some(i);
                } else {
                    break;
                }
            }
            best
        };

        let mut probes: Vec<Vec<u8>> =
            (0..2000u32).step_by(37).map(|i| format!("k{i:05}").into_bytes()).collect();
        // 每个块的下界本身、以及块与块之间、文件外两侧
        for i in (0..blocks).step_by(11) {
            probes.push(sst.index[i].first_key.clone());
        }
        probes.push(b"k00000".to_vec());
        probes.push(b"k01999".to_vec());
        probes.push(b"aaa".to_vec());
        probes.push(vec![0xffu8; 4]);
        for k in &probes {
            assert_eq!(sst.find_block(k), linear(k), "find_block 与线性扫不一致: {k:?}");
        }

        let log2 = usize::BITS - blocks.leading_zeros();
        let before = index_probes();
        const N: u64 = 200;
        for _ in 0..N {
            assert!(sst.find_block(b"k01999").is_some());
        }
        let per = (index_probes() - before) / N;
        eprintln!(
            "二分后每次定位比较 {per} 次（块数 {blocks}，线性参照 ≈ {} 次）",
            blocks / 2
        );
        assert!(
            per as u32 <= 2 + log2,
            "定位代价应当只与 log2(块数) 同阶（块数 {blocks}，实测 {per} 次）"
        );
        let _ = std::fs::remove_file(&path);
    }
}

/// [`SstFile::light_iter`] 的状态机：一块一块读、一条一条吐。
///
/// ⚠️ 只前进不回退：块内条目按 key 升序 ⇒ 越过 `to` 就 `None` 收尾，后面的块不再读。
struct SstLightIter<'a> {
    sst: &'a SstFile,
    block: usize,
    pos: usize,
    cur: Option<Arc<DataBlock>>,
    from: Option<Vec<u8>>,
    to: Option<Vec<u8>>,
    snap_seq: u64,
}

pub(crate) enum VacuumLightInput<'a> {
    Empty,
    OwnedBlocks(VacuumLightRows<'a>),
    LegacyShared(Box<dyn Iterator<Item = std::io::Result<(Vec<u8>, LightEntry)>> + 'a>),
}
impl Iterator for VacuumLightInput<'_> {
    type Item = std::io::Result<(Vec<u8>, LightEntry)>;
    fn next(&mut self) -> Option<Self::Item> {
        match self { Self::Empty => None, Self::OwnedBlocks(rows) => rows.next(), Self::LegacyShared(rows) => rows.next() }
    }
}

pub(crate) struct VacuumLightRows<'a> {
    sst: &'a SstFile,
    block: usize,
    rows: std::vec::IntoIter<(Vec<u8>, StoredEntry)>,
    from: Option<&'a [u8]>,
    to: Option<&'a [u8]>,
    snap_seq: u64,
    done: bool,
}

impl Iterator for VacuumLightRows<'_> {
    type Item = std::io::Result<(Vec<u8>, LightEntry)>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.done { return None; }
        loop {
            if let Some((key, entry)) = self.rows.next() {
                if self.to.is_some_and(|to| key.as_slice() >= to) { self.done = true; return None; }
                if self.from.is_some_and(|from| key.as_slice() < from) || !entry.visible_at(self.snap_seq) { continue; }
                #[cfg(test)]
                crate::common::maintenance_probe::note_lsm_vacuum_row(true);
                return Some(Ok((key, LightEntry::of(&entry))));
            }
            let index = self.sst.index.get(self.block)?;
            self.block += 1;
            #[cfg(test)]
            crate::common::maintenance_probe::note_lsm_vacuum_block();
            match self.sst.read_slice(index.offset, index.len as usize).and_then(|raw| decode_block(&raw, self.sst.has_seq)) {
                Ok(block) => self.rows = block.entries.into_iter(),
                Err(error) => { self.done = true; return Some(Err(error)); }
            }
        }
    }
}

impl<'a> Iterator for SstLightIter<'a> {
    type Item = (Vec<u8>, LightEntry);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.block >= self.sst.index.len() {
                return None;
            }
            if self.cur.is_none() {
                self.cur = self.sst.load_block(self.block).ok();
                self.pos = 0;
                if self.cur.is_none() {
                    // 坏块：跳过这一块而不是整篇放弃（与 eager 版 `match load_block(i)` 同口径）
                    self.block += 1;
                    continue;
                }
            }
            let block = self.cur.as_ref().expect("上面保证有块");
            while self.pos < block.entries.len() {
                let (k, se) = &block.entries[self.pos];
                self.pos += 1;
                if self.to.as_deref().is_some_and(|t| k.as_slice() >= t) {
                    return None; // 越界：后面的块只会更大
                }
                if self.from.as_deref().is_some_and(|f| k.as_slice() < f) {
                    continue;
                }
                if !se.visible_at(self.snap_seq) {
                    continue;
                }
                return Some((k.clone(), LightEntry::of(se)));
            }
            self.cur = None;
            self.block += 1;
        }
    }
}

