//! SSTable：不可变有序文件。
//!
//! ## 格式
//! - **v1** `LSMSST01`：扁平 entries（只读兼容，seq 视为 0）
//! - **v2** `LSMSST02`：data blocks + 稀疏索引 + bloom + CRC；entry 含 `seq u64`
//! - **v3** 同 v2 布局，entry 强制写 seq（写路径只用 v3 magic 在 footer 区分；实现上统一用 v2 布局 + seq）
//!
//! 读：bloom → index → block（mmap 或 pread + LRU cache）
//! compact：[`SstEntryIter`] 流式 + [`merge_to_builder`]

use std::{
    cmp::Ordering as CmpOrdering, collections::{BinaryHeap, HashMap, VecDeque}, fs::File, io::{Read, Seek, SeekFrom, Write}, path::{Path, PathBuf}, sync::{Arc, Mutex}, vec,
};

use memmap2::Mmap;

use crate::lsm_tree::bloom::Bloom;
use crate::lsm_tree::storage::crc32;

const SST_MAGIC_V1: &[u8; 8] = b"LSMSST01";
const SST_MAGIC_V2: &[u8; 8] = b"LSMSST02";

/// 逻辑值类型
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntryValue {
    Put(Option<Vec<u8>>),
    Drop,
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

// 测试专用：本线程「把带 value 的条目整个物化出来」的次数。
//
// 这是全库折叠那条通道真正的代价单位：重量路径（`iter_all` / `mem_view_at`）每行都要
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

#[derive(Debug)]
pub struct BlockCache {
    cap: usize,
    map: HashMap<CacheKey, Arc<DataBlock>>,
    order: VecDeque<CacheKey>,
}

impl BlockCache {
    pub fn new(cap: usize) -> Self {
        Self {
            cap,
            map: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    fn get(&mut self, ns: u64, sst_id: u64, offset: u64) -> Option<Arc<DataBlock>> {
        let k = CacheKey { ns, sst_id, offset };
        if let Some(b) = self.map.get(&k) {
            if let Some(pos) = self.order.iter().position(|x| x == &k) {
                self.order.remove(pos);
                self.order.push_back(k);
            }
            return Some(b.clone());
        }
        None
    }

    fn put(&mut self, ns: u64, sst_id: u64, offset: u64, block: Arc<DataBlock>) {
        if self.cap == 0 {
            return;
        }
        let k = CacheKey { ns, sst_id, offset };
        if self.map.contains_key(&k) {
            self.map.insert(k, block);
            return;
        }
        while self.map.len() >= self.cap {
            if let Some(old) = self.order.pop_front() {
                self.map.remove(&old);
            } else {
                break;
            }
        }
        self.map.insert(k.clone(), block);
        self.order.push_back(k);
    }
}

#[derive(Debug, Clone)]
struct IndexEntry {
    first_key: Vec<u8>,
    offset: u64,
    len: u32,
}

// ─── SstFile ─────────────────────────────────────────────────────────────────

pub struct SstFile {
    pub id: u64,
    pub path: PathBuf,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
    pub size_bytes: u64,
    entries_cache: Option<Vec<(Vec<u8>, StoredEntry)>>,
    index: Vec<IndexEntry>,
    bloom: Bloom,
    is_v2: bool,
    /// entry 是否带 seq 字段（v2 写路径 true；v1 false）
    has_seq: bool,
    block_cache: Option<Arc<Mutex<BlockCache>>>,
    mmap: Option<Mmap>,
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
    pub fn num_entries(&self) -> usize {
        if let Some(e) = &self.entries_cache {
            return e.len();
        }
        self.iter_all().count()
    }

    pub fn may_overlap(&self, low: &[u8], high: &[u8]) -> bool {
        if self.min_key.is_empty() && self.max_key.is_empty() {
            return false;
        }
        self.min_key.as_slice() <= high && self.max_key.as_slice() >= low
    }

    pub fn get(&self, key: &[u8]) -> Option<StoredEntry> {
        self.get_at(key, u64::MAX)
    }

    /// 在 snapshot `snap_seq` 下查找（取 `seq <= snap_seq` 的最新）
    pub fn get_at(&self, key: &[u8], snap_seq: u64) -> Option<StoredEntry> {
        if !self.may_overlap(key, key) {
            return None;
        }
        if !self.bloom.may_contain(key) {
            return None;
        }
        if let Some(e) = &self.entries_cache {
            // 可能同 key 多版本：取 seq 最大且 <= snap
            let mut best: Option<StoredEntry> = None;
            for (k, se) in e {
                if k.as_slice() == key && se.visible_at(snap_seq) {
                    if best.as_ref().map(|b| se.seq >= b.seq).unwrap_or(true) {
                        best = Some(se.clone());
                    }
                }
            }
            return best;
        }
        let bi = self.find_block(key)?;
        let block = self.load_block(bi).ok()?;
        let mut best: Option<StoredEntry> = None;
        for (k, se) in &block.entries {
            if k.as_slice() == key && se.visible_at(snap_seq) {
                if best.as_ref().map(|b| se.seq >= b.seq).unwrap_or(true) {
                    best = Some(se.clone());
                }
            }
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
    /// 起点额外回退一块：块是按**固定条数**切分的，同一个 key 的多个 seq 可能跨块边界，
    /// 少回退一块就可能漏掉该 key 更旧的 seq（`snap_seq` 截断较新 seq 时会取错）。
    /// v1 文件整份在 `entries_cache` 里，没有索引，保持从头扫。
    fn iter_from_key(&self, low: &[u8]) -> SstEntryIter<'_> {
        let block_i = self
            .find_block(low)
            .map(|i| i.saturating_sub(1))
            .unwrap_or(0);
        SstEntryIter {
            sst: self,
            block_i,
            entry_i: 0,
            cur: None,
            cache_i: 0,
        }
    }

    /// 在闭区间 `[low, high]` 内**从大到小**找第一个被 `accept` 接受的 key。
    ///
    /// 用稀疏索引直接定位 `high` 所在块，块内反向扫，一块扫完退到前一块；一旦某块里
    /// 出现 `k < low` 即可收尾 —— 块内与块间都按 key 升序，再往前全部落在区间之外。
    ///
    /// 与 [`Self::range_at`] 保持同一套「每个 key 只取最大可见 seq」的语义：反向遇到某
    /// key 的第一条若 `seq > snap_seq` 不可见，会继续在该 key 的更旧条目里找次大可见 seq；
    /// 一旦该 key 定论（拿到可见条目并交给 `accept`），同 key 更旧的 seq 不再回看。
    ///
    /// 已知边界：若同一个 key 的条目**跨块**、且较新的那条落在后一块而 `snap_seq` 把它截断，
    /// 这里取到的是「本块内最大可见 seq」，与 `range_at` 扫全文件相比可能偏旧。MVCC 调用
    /// 传 `snap_seq = u64::MAX`（全可见），不落入该边界。
    pub fn find_back(
        &self,
        low: &[u8],
        high: &[u8],
        snap_seq: u64,
        mut accept: impl FnMut(&[u8], &StoredEntry) -> bool,
    ) -> Option<(Vec<u8>, StoredEntry)> {
        if !self.may_overlap(low, high) {
            return None;
        }

        // v1：整份在内存里，反向线性扫即可
        if let Some(all) = &self.entries_cache {
            for (k, se) in all.iter().rev() {
                let ks = k.as_slice();
                if ks > high {
                    continue;
                }
                if ks < low {
                    return None;
                }
                if accept(ks, se) {
                    return Some((k.clone(), se.clone()));
                }
            }
            return None;
        }

        let mut block_i = self.find_block(high)?;
        loop {
            let block = self.load_block(block_i).ok()?;
            // 已经定论的 key：同 key 更旧的 seq 不再考虑（对齐 range_at 的每 key 取胜）
            let mut decided: Option<Vec<u8>> = None;
            let mut idx = block.entries.len();
            while idx > 0 {
                idx -= 1;
                let (k, se) = &block.entries[idx];
                let ks = k.as_slice();
                if ks > high {
                    continue;
                }
                if ks < low {
                    return None;
                }
                if decided.as_deref() == Some(ks) {
                    continue;
                }
                if !se.visible_at(snap_seq) {
                    continue;
                }
                decided = Some(k.clone());
                if accept(ks, se) {
                    return Some((k.clone(), se.clone()));
                }
            }

            if block_i == 0 {
                return None;
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
        out
    }

    /// 从 `from`（**含**）起取轻量条目，**够 `max_rows` 行就准备收手，但一定把当前这一组读完
    /// 才停**；返回 `(条目, 是否被截断)`，`truncated = true` 表示本文件在后面还有行。
    ///
    /// 起点靠稀疏索引下降到所在块，**之前的块一篇都不读**。
    ///
    /// 为什么要「按组收手」而不是按行：组 = 去掉尾部 `group_suffix` 字节后的键，也就是
    /// MVCC 的版本链（`raw_key ‖ version`）。半组交给上层等于让上层无法判断「留哪一版」，
    /// 所以预算只能在组边界生效 —— 一行的偏差就是一条链的长度，这是有界回收明确接受的代价。
    /// `group_suffix = 0` 时每个键自成一界，等同于纯按行封顶。
    ///
    /// 有界回收还需要它：跨源归并只有「每个源都读满了这个窗口」时才能安全定出截断点，
    /// 所以窗口必须先按源封顶，而不是按归并结果封顶。
    pub fn light_view_break_from(
        &self,
        from: Option<&[u8]>,
        snap_seq: u64,
        max_rows: usize,
        group_suffix: usize,
    ) -> (Vec<(Vec<u8>, LightEntry)>, bool) {
        let mut out: Vec<(Vec<u8>, LightEntry)> = Vec::new();
        // 折叠规则与 light_view 一致；写成局部函数而不是闭包，调用方才能在外面读 out.len()
        fn feed(
            out: &mut Vec<(Vec<u8>, LightEntry)>,
            k: &[u8],
            se: &StoredEntry,
            snap_seq: u64,
        ) {
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
        }

        // 预算已满且下一条换了组 → 就地收手
        let mut over_budget = false;

        if let Some(all) = &self.entries_cache {
            // v1 文件整份在内存里，没有索引：二分定位起点后顺序取
            let start = match from {
                Some(f) => all.partition_point(|(k, _)| k.as_slice() < f),
                None => 0,
            };
            for (k, se) in &all[start..] {
                if over_budget && !out.last().is_some_and(|(pk, _)| same_group(pk, k, group_suffix))
                {
                    return (out, true);
                }
                feed(&mut out, k, se, snap_seq);
                if out.len() >= max_rows {
                    over_budget = true;
                }
            }
            return (out, false);
        }

        let start_block = match from {
            Some(f) => match self.find_block(f) {
                Some(i) => i,
                // 比本文件最小键还小 ⇒ 从第 0 块起
                None => 0,
            },
            None => 0,
        };
        for i in start_block..self.index.len() {
            match self.load_block(i) {
                Ok(b) => {
                    for (k, se) in &b.entries {
                        if let Some(f) = from {
                            if k.as_slice() < f {
                                continue;
                            }
                        }
                        if over_budget
                            && !out.last().is_some_and(|(pk, _)| same_group(pk, k, group_suffix))
                        {
                            return (out, true);
                        }
                        feed(&mut out, k, se, snap_seq);
                        if out.len() >= max_rows {
                            over_budget = true;
                        }
                    }
                }
                // 与 iter_all 一致：坏块即止
                Err(_) => break,
            }
        }
        (out, false)
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

    pub fn iter_entries(&self) -> Vec<(Vec<u8>, StoredEntry)> {
        self.iter_all().collect()
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

    fn find_block(&self, key: &[u8]) -> Option<usize> {
        if self.index.is_empty() {
            return None;
        }
        let mut best = None;
        for (i, ie) in self.index.iter().enumerate() {
            if ie.first_key.as_slice() <= key {
                best = Some(i);
            } else {
                break;
            }
        }
        best
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
        let mut f = File::open(&self.path)?;
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
        entries_cache: Some(entries),
        index: Vec::new(),
        bloom: Bloom::empty(),
        is_v2: false,
        has_seq: false,
        block_cache: None,
        mmap: None,
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
        entries_cache: None,
        index,
        bloom,
        is_v2: true,
        has_seq,
        block_cache: None,
        mmap: None,
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
    entries: Vec<(Vec<u8>, StoredEntry)>,
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
            entries: Vec::new(),
        }
    }

    pub fn add(&mut self, key: Vec<u8>, se: StoredEntry) {
        if let Some((last_k, _)) = self.entries.last() {
            debug_assert!(
                last_k.as_slice() <= key.as_slice(),
                "SST keys must be non-decreasing"
            );
        }
        self.entries.push((key, se));
    }

    pub fn add_put(&mut self, key: Vec<u8>, seq: u64, value: Option<Vec<u8>>) {
        self.add(key, StoredEntry::put(seq, value));
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn estimated_bytes(&self) -> usize {
        self.entries
            .iter()
            .map(|(k, se)| k.len() + se.estimate_bytes() + 8)
            .sum()
    }

    pub fn finish(self) -> std::io::Result<SstFile> {
        self.finish_with_sync(true)
    }

    /// `sync=false`：不 fsync（bulk 导入用，结束时统一落盘）
    pub fn finish_with_sync(self, sync: bool) -> std::io::Result<SstFile> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = self.path.with_extension("sst.tmp");
        {
            let f = File::create(&tmp)?;
            let mut f = std::io::BufWriter::with_capacity(1024 * 1024, f);
            let mut index = Vec::new();
            let mut bloom = if self.enable_bloom {
                Bloom::new(self.entries.len().max(1), self.bloom_bits_per_key)
            } else {
                Bloom::empty()
            };
            let mut i = 0;
            while i < self.entries.len() {
                let end = (i + self.block_entries).min(self.entries.len());
                let chunk = &self.entries[i..end];
                let first_key = chunk[0].0.clone();
                for (k, _) in chunk {
                    bloom.insert(k);
                }
                let block_raw = encode_block(chunk, true);
                let offset = f.stream_position()?;
                f.write_all(&block_raw)?;
                index.push(IndexEntry {
                    first_key,
                    offset,
                    len: block_raw.len() as u32,
                });
                i = end;
            }
            let index_blob = encode_index(&index);
            let index_off = f.stream_position()?;
            f.write_all(&index_blob)?;
            let bloom_blob = if self.enable_bloom {
                bloom.encode()
            } else {
                Vec::new()
            };
            let bloom_off = f.stream_position()?;
            if !bloom_blob.is_empty() {
                f.write_all(&bloom_blob)?;
            }
            let (min_key, max_key) = min_max_keys_stored(&self.entries);
            let mut meta = Vec::new();
            meta.extend_from_slice(&index_off.to_le_bytes());
            meta.extend_from_slice(&(index_blob.len() as u32).to_le_bytes());
            meta.extend_from_slice(&bloom_off.to_le_bytes());
            meta.extend_from_slice(&(bloom_blob.len() as u32).to_le_bytes());
            meta.extend_from_slice(&(self.entries.len() as u64).to_le_bytes());
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
                f.get_mut().sync_all()?;
            }
        }
        let _ = std::fs::remove_file(&self.path);
        std::fs::rename(&tmp, &self.path)?;
        SstFile::open(self.id, &self.path)
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

/// 判断两个键是否属于同一「组」：组 = 去掉尾部 `suffix` 字节后的键。
///
/// MVCC 层用 `raw_key ‖ version(8 字节大端)` 做版本键，所以 `suffix = 8` 时一组就是
/// 一把逻辑 key 的整条版本链。有界遍历必须只在组边界收手（半条链交给上层没法判
/// 「留哪一版」），而 SST 这层本身不认识 MVCC，所以组的宽度由调用方给。
/// `suffix = 0` ⇒ 每个键自成一界。
pub(crate) fn same_group(a: &[u8], b: &[u8], suffix: usize) -> bool {
    if a.len() < suffix || b.len() < suffix {
        return a == b;
    }
    a[..a.len() - suffix] == b[..b.len() - suffix]
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

/// 多路归并：sources 新→旧；同 key 取最高优先级中 `seq` 最大者；流式回调。
pub fn merge_streaming<T, F, K>(sources: Vec<Vec<(Vec<u8>, T)>>, mut keep: K, mut emit: F)
where
    T: MergeSeq,
    F: FnMut(Vec<u8>, T),
    K: FnMut(&[u8], &T) -> bool,
{
    struct Node<T> {
        key: Vec<u8>,
        se: T,
        src: usize,
        idx: usize,
    }
    impl<T: MergeSeq> Ord for Node<T> {
        fn cmp(&self, other: &Self) -> CmpOrdering {
            match other.key.cmp(&self.key) {
                CmpOrdering::Equal => match other.src.cmp(&self.src) {
                    CmpOrdering::Equal => self.se.seq().cmp(&other.se.seq()), // 同 src 更大 seq 优先
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

    let mut heap = BinaryHeap::new();
    for (s, src) in sources.iter().enumerate() {
        if let Some((k, se)) = src.first() {
            heap.push(Node {
                key: k.clone(),
                se: se.clone(),
                src: s,
                idx: 0,
            });
        }
    }
    let mut last_key: Option<Vec<u8>> = None;
    while let Some(n) = heap.pop() {
        let push_next = |heap: &mut BinaryHeap<Node<T>>,
                         sources: &[Vec<(Vec<u8>, T)>],
                         src: usize,
                         idx: usize| {
            let next_idx = idx + 1;
            if let Some(s) = sources.get(src) {
                if next_idx < s.len() {
                    heap.push(Node {
                        key: s[next_idx].0.clone(),
                        se: s[next_idx].1.clone(),
                        src,
                        idx: next_idx,
                    });
                }
            }
        };
        push_next(&mut heap, &sources, n.src, n.idx);

        if last_key.as_ref().map(|k| k == &n.key).unwrap_or(false) {
            continue;
        }

        // 收集所有同 key 条目，选最优：src 更小（更新）优先，同 src 取更大 seq
        let key = n.key.clone();
        let mut best_src = n.src;
        let mut best = n.se.clone();
        while heap.peek().map(|p| p.key == key).unwrap_or(false) {
            let m = heap.pop().unwrap();
            push_next(&mut heap, &sources, m.src, m.idx);
            if m.src < best_src || (m.src == best_src && m.se.seq() > best.seq()) {
                best_src = m.src;
                best = m.se.clone();
            }
        }
        // 也包括首个 n 与后续比较（n 已是堆顶，src 应最小；同 src 需比 seq）
        // 重新扫：n 已在 best；上面循环处理其余

        if keep(&key, &best) {
            emit(key.clone(), best);
        }
        last_key = Some(key);
    }
}

/// 兼容：归并到 Vec
pub fn merge_latest_streaming<F>(
    sources: Vec<Vec<(Vec<u8>, StoredEntry)>>,
    mut keep: F,
) -> Vec<(Vec<u8>, StoredEntry)>
where
    F: FnMut(&[u8], &StoredEntry) -> bool,
{
    let mut out = Vec::new();
    merge_streaming(sources, &mut keep, |k, se| out.push((k, se)));
    out
}

pub fn merge_latest(
    sources: Vec<Vec<(Vec<u8>, StoredEntry)>>,
) -> Vec<(Vec<u8>, StoredEntry)> {
    merge_latest_streaming(sources, |_, _| true)
}


/// **真流式**归并写入多个 SST：边 merge 边 `add`，达到 `target_bytes` 就 `finish` 落盘，
/// 不先物化全量 `Vec`（单 builder 缓冲不超过约一个 SST）。
pub fn merge_to_ssts<F>(
    sources: Vec<Vec<(Vec<u8>, StoredEntry)>>,
    mut keep: F,
    mut new_builder: impl FnMut() -> SstBuilder,
    target_bytes: usize,
) -> std::io::Result<Vec<SstFile>>
where
    F: FnMut(&[u8], &StoredEntry) -> bool,
{
    let mut out = Vec::new();
    let mut builder: Option<SstBuilder> = None;
    let mut err: Option<std::io::Error> = None;
    merge_streaming(sources, &mut keep, |k, se| {
        if err.is_some() {
            return;
        }
        if builder.is_none() {
            builder = Some(new_builder());
        }
        let b = builder.as_mut().unwrap();
        b.add(k, se);
        if b.estimated_bytes() >= target_bytes {
            match builder.take().unwrap().finish() {
                Ok(sst) => out.push(sst),
                Err(e) => err = Some(e),
            }
        }
    });
    if let Some(e) = err {
        return Err(e);
    }
    if let Some(b) = builder {
        if !b.is_empty() {
            out.push(b.finish()?);
        }
    }
    Ok(out)
}


#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(tag: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("sst_{tag}_{nanos}.sst"))
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
}
