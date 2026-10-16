//! 页管理器：缓冲池 + 持久化
//!
//! - 数据文件：通过 [`crate::storage::DiskFile`] 读写
//! - 页头 CRC：[`crate::storage::pack_page`] / [`unpack_page`]
//! - 双写缓冲：刷盘前先写 dblwr 再写主文件，防 torn page

use std::{
    collections::{HashMap, HashSet, VecDeque},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::bplus_tree::storage::{
    dblwr_path, freelist_path, lock_path, pack_page, read_freelist, unpack_page, write_freelist, DiskFile,
    PAGE_SIZE,
    DoubleWriteBuffer, FileLock, PAGE_PAYLOAD_SIZE,
};

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
    /// bulk 模式：换出/刷盘跳过双写与频繁 fsync，只在 flush_all 末尾 sync 一次
    bulk_mode: bool,
}

// 测试专用：本线程的页访问次数（含页缓存命中）。
//
// B+ 树没有「读一行」这种细粒度 IO，代价的对应物是读了多少页 —— 用来把
// 「点操作代价不随版本链长度增长」这条不变量钉成断言。做成 thread_local 是为了
// 不和 `cargo test` 的并行用例互相计数。
#[cfg(test)]
thread_local! {
    static PAGE_READS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// 当前线程已发生的页访问次数（测试探针用）
#[cfg(test)]
pub(crate) fn page_reads() -> u64 {
    PAGE_READS.with(|c| c.get())
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

/// 本线程发生的超载写次数（诊断与守卫用）
pub fn oversized_writes() -> u64 {
    OVERSIZED_WRITES.with(|c| c.get())
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

/// 当前线程已发生的 value 克隆次数（测试探针用）
#[cfg(test)]
pub(crate) fn value_clones() -> u64 {
    VALUE_CLONES.with(|c| c.get())
}

/// 记一次 value 克隆（重量路径调用；轻量路径不该调用）
#[cfg(test)]
pub(crate) fn note_value_clone() {
    VALUE_CLONES.with(|c| c.set(c.get() + 1));
}

impl PageManager {
    /// 打开或创建数据文件（同时获取单写者锁、打开 dblwr，并做 torn-page 恢复）
    pub fn open(path: impl AsRef<Path>, max_cache_pages: usize) -> std::io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        // 先抢锁，避免两个进程同时恢复/写入
        let lock = FileLock::try_acquire_at(&lock_path(&path))?;
        // 主数据文件是定宽页布局 ⇒ 声明块大小，read_page/write_page 才算得对偏移
        let mut file = DiskFile::open_paged(&path, PAGE_SIZE)?;
        let mut dblwr = DoubleWriteBuffer::open(dblwr_path(&path))?;

        // 1) 双写恢复：把 dblwr 中 CRC 合法的页覆盖回主文件
        Self::recover_from_dblwr(&mut file, &mut dblwr)?;

        let file_len = file.len()?;
        let mut pm = Self {
            _lock: lock,
            file,
            dblwr,
            path,
            cache: HashMap::new(),
            lru_queue: VecDeque::new(),
            free_page_ids: VecDeque::new(),
            free_set: HashSet::new(),
            next_page_id: 1,
            root_page_id: None,
            next_version: None,
            max_cache_pages,
            clock: 0,
            meta_dirty: false,
            freelist_dirty: false,
            bulk_mode: false,
        };

        if file_len == 0 {
            pm.meta_dirty = true;
            pm.persist_meta_via_dblwr()?;
            pm.persist_freelist()?;
        } else {
            pm.load_meta()?;
            pm.load_freelist()?;
        }
        Ok(pm)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn root_page_id(&self) -> Option<PageId> {
        self.root_page_id
    }

    pub fn set_root_page_id(&mut self, root: PageId) {
        if self.root_page_id != Some(root) {
            self.root_page_id = Some(root);
            self.meta_dirty = true;
        }
    }

    pub fn next_page_id(&self) -> u64 {
        self.next_page_id
    }

    /// meta 中持久化的 `next_version`（旧库无 NVER 魔数时为 `None`）
    pub fn next_version(&self) -> Option<u64> {
        self.next_version
    }

    /// 更新 meta 中的 next_version（checkpoint / open 结束时写入）
    pub fn set_next_version(&mut self, v: u64) {
        if self.next_version != Some(v) {
            self.next_version = Some(v);
            self.meta_dirty = true;
        }
    }

    /// bulk 导入模式：跳过双写，换出时只 write 不 fsync，flush_all 末尾统一 sync
    pub fn set_bulk_mode(&mut self, on: bool) {
        self.bulk_mode = on;
    }

    pub fn bulk_mode(&self) -> bool {
        self.bulk_mode
    }

    /// 崩溃恢复：用 dblwr 中的完整页修复主文件
    fn recover_from_dblwr(
        file: &mut DiskFile,
        dblwr: &mut DoubleWriteBuffer,
    ) -> std::io::Result<()> {
        let batch = dblwr.read_batch()?;
        if batch.is_empty() {
            return Ok(());
        }
        for (page_id, raw) in &batch {
            // 主文件对应页若 CRC 损坏或与 dblwr 不一致 → 用 dblwr 覆盖
            let main = file.read_page(*page_id)?;
            let main_ok = unpack_page(&main).is_ok();
            if !main_ok || main != *raw {
                file.write_page(*page_id, raw)?;
            }
        }
        file.sync()?;
        dblwr.clear()?;
        Ok(())
    }

    fn load_meta(&mut self) -> std::io::Result<()> {
        let raw = self.file.read_page(META_PAGE_ID)?;
        let (_id, payload) = unpack_page(&raw).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, format!("meta 损坏: {e}"))
        })?;
        if &payload[META_OFF_MAGIC..META_OFF_MAGIC + 8] != MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("不是合法的 bplus 数据文件: {:?}", self.path),
            ));
        }
        self.next_page_id =
            u64::from_le_bytes(payload[META_OFF_NEXT..META_OFF_NEXT + 8].try_into().unwrap());
        let root =
            u64::from_le_bytes(payload[META_OFF_ROOT..META_OFF_ROOT + 8].try_into().unwrap());
        self.root_page_id = if root == 0 {
            None
        } else {
            Some(PageId(root))
        };
        // next_version：有 "NVER" 魔数才读（兼容旧库 offset 32 起是 free 列表）
        self.next_version = if payload.len() >= META_OFF_NEXT_VERSION + 8
            && &payload[META_OFF_NVER_MAGIC..META_OFF_NVER_MAGIC + 4] == NVER_MAGIC
        {
            Some(u64::from_le_bytes(
                payload[META_OFF_NEXT_VERSION..META_OFF_NEXT_VERSION + 8]
                    .try_into()
                    .unwrap(),
            ))
        } else {
            None
        };
        // free 列表不再从 meta 内嵌字段加载（见 load_freelist）
        self.meta_dirty = false;
        Ok(())
    }

    /// 从 `{db}.freelist` 加载完整空闲页；若无 sidecar 则尝试从旧 meta 内嵌列表迁移
    fn load_freelist(&mut self) -> std::io::Result<()> {
        self.free_page_ids.clear();
        self.free_set.clear();
        let fl_path = freelist_path(&self.path);

        if let Some(ids) = read_freelist(&fl_path)? {
            for id in ids {
                if id > META_PAGE_ID && id < self.next_page_id && self.free_set.insert(id) {
                    self.free_page_ids.push_back(PageId(id));
                }
            }
            self.freelist_dirty = false;
            return Ok(());
        }

        // 兼容旧库：meta payload offset 32 起可能有内嵌 free 列表
        if let Ok(raw) = self.file.read_page(META_PAGE_ID) {
            if let Ok((_id, payload)) = unpack_page(&raw) {
                let free_cnt = u64::from_le_bytes(
                    payload[META_OFF_FREE_CNT..META_OFF_FREE_CNT + 8]
                        .try_into()
                        .unwrap(),
                ) as usize;
                let max_old = (PAGE_PAYLOAD_SIZE - 32) / 8;
                let free_cnt = free_cnt.min(max_old);
                for i in 0..free_cnt {
                    let off = 32 + i * 8;
                    if off + 8 > payload.len() {
                        break;
                    }
                    let id = u64::from_le_bytes(payload[off..off + 8].try_into().unwrap());
                    if id > META_PAGE_ID && id < self.next_page_id && self.free_set.insert(id) {
                        self.free_page_ids.push_back(PageId(id));
                    }
                }
            }
        }

        // 若从旧 meta 迁出了 free 页，立刻写出 sidecar
        if !self.free_page_ids.is_empty() {
            self.freelist_dirty = true;
            self.persist_freelist()?;
        } else {
            self.freelist_dirty = false;
        }
        Ok(())
    }

    fn build_meta_payload(&self) -> Vec<u8> {
        let mut payload = vec![0u8; PAGE_PAYLOAD_SIZE];
        payload[META_OFF_MAGIC..META_OFF_MAGIC + 8].copy_from_slice(MAGIC);
        payload[META_OFF_NEXT..META_OFF_NEXT + 8]
            .copy_from_slice(&self.next_page_id.to_le_bytes());
        // free_count 仅作提示（监控用），权威在 freelist 文件
        payload[META_OFF_FREE_CNT..META_OFF_FREE_CNT + 8]
            .copy_from_slice(&(self.free_page_ids.len() as u64).to_le_bytes());
        let root = self.root_page_id.map(|p| p.0).unwrap_or(0);
        payload[META_OFF_ROOT..META_OFF_ROOT + 8].copy_from_slice(&root.to_le_bytes());
        // 仅在明确 set 过 next_version 后写 NVER，避免旧库被误写成 0 而跳过全树扫描
        if let Some(nv) = self.next_version {
            payload[META_OFF_NVER_MAGIC..META_OFF_NVER_MAGIC + 4].copy_from_slice(NVER_MAGIC);
            payload[META_OFF_NEXT_VERSION..META_OFF_NEXT_VERSION + 8]
                .copy_from_slice(&nv.to_le_bytes());
        }
        payload
    }

    fn persist_freelist(&mut self) -> std::io::Result<()> {
        if !self.freelist_dirty {
            return Ok(());
        }
        let ids: Vec<u64> = self.free_page_ids.iter().map(|p| p.0).collect();
        write_freelist(&freelist_path(&self.path), &ids)?;
        self.freelist_dirty = false;
        Ok(())
    }

    /// 写 meta（bulk 下直接写+可选 sync；正常路径走双写）
    fn persist_meta_via_dblwr(&mut self) -> std::io::Result<()> {
        let payload = self.build_meta_payload();
        let packed = pack_page(META_PAGE_ID, &payload);
        if self.bulk_mode {
            self.file.write_page(META_PAGE_ID, &packed)?;
            self.file.sync()?;
        } else {
            self.dblwr
                .write_batch(&[(META_PAGE_ID, packed.clone())])?;
            self.file.write_page(META_PAGE_ID, &packed)?;
            self.file.sync()?;
            self.dblwr.clear()?;
        }
        self.meta_dirty = false;
        Ok(())
    }

    fn page_is_live(&self, page_id: PageId) -> bool {
        page_id.0 > META_PAGE_ID
            && page_id.0 < self.next_page_id
            && !self.free_set.contains(&page_id.0)
    }

    /// 读磁盘页 → 校验 CRC → 返回 payload
    fn read_disk_payload(&mut self, page_id: PageId) -> std::io::Result<Vec<u8>> {
        let raw = self.file.read_page(page_id.0)?;
        match unpack_page(&raw) {
            Ok((_id, payload)) => Ok(payload),
            Err(e) => {
                // 尝试从 dblwr 恢复单页
                let batch = self.dblwr.read_batch().unwrap_or_default();
                if let Some((_, raw)) = batch.into_iter().find(|(id, _)| *id == page_id.0) {
                    if let Ok((_id, payload)) = unpack_page(&raw) {
                        // 修好主文件
                        let _ = self.file.write_page(page_id.0, &raw);
                        return Ok(payload);
                    }
                }
                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("读页 {} CRC 失败: {e}", page_id.0),
                ))
            }
        }
    }

    /// 把一批脏页写到主文件。
    /// - `do_sync=true`：写完 fsync（checkpoint / 非 bulk 换出）
    /// - bulk 模式：跳过双写，仅 write；是否 sync 由调用方决定
    fn flush_pages_batch(&mut self, page_ids: &[PageId], do_sync: bool) -> std::io::Result<()> {
        if page_ids.is_empty() && !self.meta_dirty && !self.freelist_dirty {
            return Ok(());
        }

        let mut batch: Vec<(u64, Vec<u8>)> = Vec::with_capacity(page_ids.len() + 1);
        for &pid in page_ids {
            if let Some(entry) = self.cache.get(&pid) {
                if entry.state == PageState::Dirty {
                    batch.push((pid.0, pack_page(pid.0, &entry.data)));
                }
            }
        }
        if self.meta_dirty {
            let payload = self.build_meta_payload();
            batch.push((META_PAGE_ID, pack_page(META_PAGE_ID, &payload)));
        }
        if batch.is_empty() {
            // 可能只有 freelist 变更
            self.persist_freelist()?;
            return Ok(());
        }

        if self.bulk_mode {
            // 快速路径：直接写主文件，不做双写；sync 可选
            for (pid, raw) in &batch {
                self.file.write_page(*pid, raw)?;
            }
            if do_sync {
                self.file.sync()?;
            }
        } else {
            for chunk in batch.chunks(crate::bplus_tree::storage::DBLWR_MAX_PAGES) {
                let owned: Vec<(u64, Vec<u8>)> = chunk.to_vec();
                self.dblwr.write_batch(&owned)?;
                for (pid, raw) in &owned {
                    self.file.write_page(*pid, raw)?;
                }
                if do_sync {
                    self.file.sync()?;
                }
                self.dblwr.clear()?;
            }
        }

        for &pid in page_ids {
            if let Some(entry) = self.cache.get_mut(&pid) {
                if entry.state == PageState::Dirty {
                    entry.state = PageState::Clean;
                }
            }
        }
        self.meta_dirty = false;
        // freelist 与 meta 同事务落盘
        self.persist_freelist()?;
        Ok(())
    }

    fn flush_pages_via_dblwr(&mut self, page_ids: &[PageId]) -> std::io::Result<()> {
        self.flush_pages_batch(page_ids, true)
    }

    /// 分配新页
    ///
    /// 优先复用 freelist；只进缓存并标 Dirty，**不**立刻 fsync。
    pub fn allocate_page(&mut self) -> PageId {
        self.clock += 1;
        self.evict_if_needed_for_insert();

        let empty = vec![0u8; PAGE_PAYLOAD_SIZE];
        let page_id = if let Some(page_id) = self.free_page_ids.pop_front() {
            self.free_set.remove(&page_id.0);
            self.freelist_dirty = true;
            self.meta_dirty = true;
            page_id
        } else {
            let id = PageId(self.next_page_id);
            self.next_page_id += 1;
            self.meta_dirty = true;
            id
        };

        self.cache.insert(
            page_id,
            CacheEntry {
                page_id,
                data: empty,
                state: PageState::Dirty,
                last_access: self.clock,
            },
        );
        self.lru_queue.push_back(page_id);
        page_id
    }

    /// 读页 payload
    pub fn read_page(&mut self, page_id: PageId) -> Option<Vec<u8>> {
        // 测试专用：页访问次数（含页缓存命中）—— 用来断言点操作的代价不随版本链长度增长
        #[cfg(test)]
        PAGE_READS.with(|c| c.set(c.get() + 1));
        if !self.page_is_live(page_id) {
            return None;
        }
        self.clock += 1;

        if self.cache.contains_key(&page_id) {
            self.update_lru(page_id);
            let entry = self.cache.get_mut(&page_id).unwrap();
            entry.last_access = self.clock;
            return Some(entry.data.clone());
        }

        let payload = match self.read_disk_payload(page_id) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("读盘失败 page={}: {}", page_id.0, e);
                return None;
            }
        };
        self.evict_if_needed();
        self.cache.insert(
            page_id,
            CacheEntry {
                page_id,
                data: payload.clone(),
                state: PageState::Clean,
                last_access: self.clock,
            },
        );
        self.lru_queue.push_back(page_id);
        Some(payload)
    }

    /// 写页 payload（标记 Dirty，真正落盘在 flush / 换出时）
    pub fn write_page(&mut self, page_id: PageId, data: &[u8]) -> bool {
        if data.len() > PAGE_PAYLOAD_SIZE {
            // 超载写必须**很少见到**：调用方（借键/合并/插入分裂）都要先量过字节再动盘。
            // 这里保留计数 + 首次告警：以前每条都 eprintln，真实形状（40B 键 + 60B 值）下
            // 一次 vacuum 能刷出几万行，把被测的耗时整个污染掉（台架复测时 bplus 的
            // `search 首页` 911ms 里就有它）。
            OVERSIZED_WRITES.with(|c| c.set(c.get() + 1));
            if OVERSIZED_WRITES.with(|c| c.get()) <= 3 {
                eprintln!(
                    "数据超过页载荷限制 {} > {}（同类告警只报前 3 条，其余计数）",
                    data.len(),
                    PAGE_PAYLOAD_SIZE
                );
            }
            return false;
        }
        if !self.page_is_live(page_id) {
            eprintln!("页 {} 不存在", page_id.0);
            return false;
        }
        self.clock += 1;

        if !self.cache.contains_key(&page_id) {
            self.evict_if_needed_for_insert();
            let disk_data = self
                .read_disk_payload(page_id)
                .unwrap_or_else(|_| vec![0u8; PAGE_PAYLOAD_SIZE]);
            self.cache.insert(
                page_id,
                CacheEntry {
                    page_id,
                    data: disk_data,
                    state: PageState::Clean,
                    last_access: self.clock,
                },
            );
            self.lru_queue.push_back(page_id);
        }

        let entry = self.cache.get_mut(&page_id).unwrap();
        entry.data.clear();
        entry.data.extend_from_slice(data);
        entry.data.resize(PAGE_PAYLOAD_SIZE, 0);
        entry.state = PageState::Dirty;
        entry.last_access = self.clock;
        self.update_lru(page_id);
        true
    }

    /// 刷单个脏页
    pub fn flush_page(&mut self, page_id: PageId) -> bool {
        let is_dirty = self
            .cache
            .get(&page_id)
            .map(|e| e.state == PageState::Dirty)
            .unwrap_or(false);
        if !is_dirty && !self.meta_dirty {
            return true;
        }
        if let Err(e) = self.flush_pages_via_dblwr(&[page_id]) {
            eprintln!("flush_page 失败: {e}");
            return false;
        }
        true
    }

    /// 刷全部脏页 + meta + freelist。
    pub fn flush_all(&mut self) {
        let dirty: Vec<PageId> = self
            .cache
            .iter()
            .filter(|(_, e)| e.state == PageState::Dirty)
            .map(|(id, _)| *id)
            .collect();
        if let Err(e) = self.flush_pages_batch(&dirty, true) {
            eprintln!("flush_all 失败: {e}");
        }
        // 即使没有脏页，meta/freelist 也可能脏
        if self.meta_dirty {
            if let Err(e) = self.persist_meta_via_dblwr() {
                eprintln!("persist meta 失败: {e}");
            }
        }
        if let Err(e) = self.persist_freelist() {
            eprintln!("persist freelist 失败: {e}");
        }
    }

    /// 释放页：归还 freelist（文件不收缩，槽位可复用）
    pub fn free_page(&mut self, page_id: PageId) -> bool {
        if !self.page_is_live(page_id) {
            return false;
        }
        self.cache.remove(&page_id);
        if self.free_set.insert(page_id.0) {
            self.free_page_ids.push_back(page_id);
            self.freelist_dirty = true;
            self.meta_dirty = true;
        }
        true
    }

    pub fn get_page_state(&mut self, page_id: PageId) -> Option<PageState> {
        self.cache.get(&page_id).map(|e| e.state)
    }

    /// 存储统计（监控 / 调试）
    pub fn stats(&self) -> StorageStats {
        StorageStats {
            next_page_id: self.next_page_id,
            free_pages: self.free_page_ids.len(),
            cache_pages: self.cache.len(),
            max_cache_pages: self.max_cache_pages,
            root_page_id: self.root_page_id.map(|p| p.0),
            bulk_mode: self.bulk_mode,
            file_path: self.path.display().to_string(),
        }
    }

    fn evict_if_needed_for_insert(&mut self) {
        self.evict_batch_until_room();
    }

    fn evict_if_needed(&mut self) {
        self.evict_batch_until_room();
    }

    /// 缓存满时一次换出多页：脏页凑一批再写盘，bulk 下不 fsync
    fn evict_batch_until_room(&mut self) {
        if self.cache.len() < self.max_cache_pages {
            return;
        }
        // 目标：腾出至少 1/4 缓存或 EVICT_BATCH_TARGET 页
        let target_free = (self.max_cache_pages / 4).max(1).min(EVICT_BATCH_TARGET);
        let mut victims: Vec<PageId> = Vec::with_capacity(target_free);
        let mut dirty_victims: Vec<PageId> = Vec::new();

        while victims.len() < target_free {
            let Some(pid) = self.pop_lru_valid() else {
                break;
            };
            if self
                .cache
                .get(&pid)
                .map(|e| e.state == PageState::Dirty)
                .unwrap_or(false)
            {
                dirty_victims.push(pid);
            }
            victims.push(pid);
        }

        if !dirty_victims.is_empty() {
            // bulk：只 write 不 sync；正常：批量双写 + 一次 sync
            let sync = !self.bulk_mode;
            let _ = self.flush_pages_batch(&dirty_victims, sync);
        }
        for pid in victims {
            self.cache.remove(&pid);
        }
    }

    /// 从队首弹出仍在 cache 中的页（跳过已释放/已换出的陈旧 id）
    fn pop_lru_valid(&mut self) -> Option<PageId> {
        while let Some(pid) = self.lru_queue.pop_front() {
            if self.cache.contains_key(&pid) {
                return Some(pid);
            }
        }
        None
    }

    /// O(1) 近似 LRU：只 push，不 retain；陈旧项在换出时跳过
    fn update_lru(&mut self, page_id: PageId) {
        self.lru_queue.push_back(page_id);
        // 防止队列无限膨胀：偶尔压缩
        if self.lru_queue.len() > self.max_cache_pages * 4 {
            self.compact_lru();
        }
    }

    fn compact_lru(&mut self) {
        let mut seen = HashMap::with_capacity(self.cache.len());
        let mut new_q = VecDeque::with_capacity(self.cache.len());
        // 从队尾往队首扫，保留每个 page 最后一次出现（最近）
        for pid in self.lru_queue.drain(..).rev() {
            if self.cache.contains_key(&pid) && !seen.contains_key(&pid) {
                seen.insert(pid, ());
                new_q.push_front(pid);
            }
        }
        self.lru_queue = new_q;
    }
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

impl Drop for PageManager {
    fn drop(&mut self) {
        self.flush_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_db(tag: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("bplus_{tag}_{nanos}.db"))
    }

    fn cleanup(path: &Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(dblwr_path(path));
        let _ = std::fs::remove_file(crate::bplus_tree::storage::wal_path(path));
        let _ = std::fs::remove_file(freelist_path(path));
        let _ = std::fs::remove_file(crate::bplus_tree::storage::lock_path(path));
        let _ = std::fs::remove_file(crate::bplus_tree::storage::blob_path(path));
    }

    #[test]
    fn test_page_manager_disk() {
        let path = temp_db("basic");
        let page_id = {
            let mut pm = PageManager::open(&path, 4).unwrap();
            let page_id = pm.allocate_page();
            assert_eq!(page_id, PageId(1));
            assert!(pm.write_page(page_id, &[1, 2, 3]));
            pm.flush_all();
            let data = pm.read_page(page_id).unwrap();
            assert_eq!(&data[..3], &[1, 2, 3]);
            assert_eq!(pm.get_page_state(page_id), Some(PageState::Clean));
            page_id
        };

        {
            let mut pm = PageManager::open(&path, 4).unwrap();
            let data = pm.read_page(page_id).unwrap();
            assert_eq!(&data[..3], &[1, 2, 3]);
        }
        cleanup(&path);
    }

    #[test]
    fn test_lru_eviction_writes_through() {
        let path = temp_db("lru");
        {
            let mut pm = PageManager::open(&path, 2).unwrap();
            let p1 = pm.allocate_page();
            let _p2 = pm.allocate_page();
            assert!(pm.write_page(p1, &[9]));
            let _p3 = pm.allocate_page();
            assert!(pm.cache.len() <= 2);
            let data = pm.read_page(p1).unwrap();
            assert_eq!(data[0], 9);
            pm.flush_all();
        }
        cleanup(&path);
    }

    #[test]
    fn test_root_persists() {
        let path = temp_db("root");
        {
            let mut pm = PageManager::open(&path, 4).unwrap();
            let root = pm.allocate_page();
            pm.set_root_page_id(root);
            pm.flush_all();
        }
        {
            let pm = PageManager::open(&path, 4).unwrap();
            assert_eq!(pm.root_page_id(), Some(PageId(1)));
        }
        cleanup(&path);
    }

    #[test]
    fn test_free_and_reuse() {
        let path = temp_db("free");
        {
            let mut pm = PageManager::open(&path, 8).unwrap();
            let p1 = pm.allocate_page();
            assert!(pm.free_page(p1));
            let p2 = pm.allocate_page();
            assert_eq!(p1, p2);
            pm.flush_all();
        }
        cleanup(&path);
    }

    #[test]
    fn test_crc_on_disk() {
        let path = temp_db("crc");
        {
            let mut pm = PageManager::open(&path, 4).unwrap();
            let p = pm.allocate_page();
            pm.write_page(p, b"crc-test");
            pm.flush_all();
        }
        // 直接读文件，确认页头有 BPG1 魔数
        let raw = std::fs::read(&path).unwrap();
        // page 1 在 offset 4096
        assert_eq!(&raw[4096..4100], b"BPG1");
        cleanup(&path);
    }

    #[test]
    fn test_dblwr_recovers_torn_page() {
        let path = temp_db("torn");
        let page_id = {
            let mut pm = PageManager::open(&path, 4).unwrap();
            let p = pm.allocate_page();
            pm.write_page(p, b"good-data");
            pm.flush_all();
            p
        };

        // 模拟 torn page：破坏主文件 page 1 的 payload，但在 dblwr 放一份好页
        {
            use crate::bplus_tree::storage::{pack_page, DoubleWriteBuffer, PAGE_SIZE};
            // 必须用 open_paged：主文件是定宽页布局，共享的 DiskFile 在没声明块大小时
            // 会直接拒绝 read_page/write_page（猜一个页大小 = 把数据写到错误偏移上）
            let mut file = crate::bplus_tree::storage::DiskFile::open_paged(&path, PAGE_SIZE)
                .unwrap();
            let mut bad = file.read_page(page_id.0).unwrap();
            // 破坏 payload
            if bad.len() > 100 {
                bad[100] ^= 0xFF;
            }
            file.write_page(page_id.0, &bad).unwrap();
            file.sync().unwrap();

            // dblwr 放正确页
            let good_payload = {
                let mut p = vec![0u8; PAGE_PAYLOAD_SIZE];
                p[..9].copy_from_slice(b"good-data");
                p
            };
            let packed = pack_page(page_id.0, &good_payload);
            let mut dw = DoubleWriteBuffer::open(dblwr_path(&path)).unwrap();
            dw.write_batch(&[(page_id.0, packed)]).unwrap();
        }

        // 重新打开应自动从 dblwr 恢复
        {
            let mut pm = PageManager::open(&path, 4).unwrap();
            let data = pm.read_page(page_id).unwrap();
            assert_eq!(&data[..9], b"good-data");
        }
        cleanup(&path);
    }

    /// 大量 free 后 reopen：freelist sidecar 必须完整恢复（旧 meta 只能塞 ~500 个）
    #[test]
    fn test_many_free_pages_persist_across_reopen() {
        let path = temp_db("manyfree");
        let n = 800usize; // > 旧 meta 容量 (~506)
        {
            let mut pm = PageManager::open(&path, 64).unwrap();
            let mut ids = Vec::new();
            for _ in 0..n {
                ids.push(pm.allocate_page());
            }
            for id in &ids {
                assert!(pm.free_page(*id));
            }
            assert_eq!(pm.stats().free_pages, n);
            pm.flush_all();
            // sidecar 文件应存在
            assert!(freelist_path(&path).exists());
        }
        {
            let mut pm = PageManager::open(&path, 64).unwrap();
            assert_eq!(pm.stats().free_pages, n, "reopen 后 free 数量应完整");
            // 复用不应抬高 next_page_id 到 1+2n
            let next_before = pm.stats().next_page_id;
            for _ in 0..n {
                let _ = pm.allocate_page();
            }
            assert_eq!(
                pm.stats().next_page_id,
                next_before,
                "应完全从 freelist 复用，不分配新页号"
            );
            assert_eq!(pm.stats().free_pages, 0);
            pm.flush_all();
        }
        cleanup(&path);
    }

    #[test]
    fn test_single_writer_lock_rejects_second_open() {
        let path = temp_db("lock");
        let pm1 = PageManager::open(&path, 4).unwrap();
        let err = PageManager::open(&path, 4).err();
        assert!(err.is_some(), "第二进程/句柄应拿不到锁");
        let e = err.unwrap();
        // WouldBlock 或 PermissionDenied 均可
        assert!(
            e.kind() == std::io::ErrorKind::WouldBlock
                || e.kind() == std::io::ErrorKind::PermissionDenied
                || e.to_string().contains("锁")
                || e.to_string().contains("占用"),
            "unexpected error: {e:?}"
        );
        drop(pm1);
        // 释放后应能再 open
        let pm2 = PageManager::open(&path, 4).unwrap();
        drop(pm2);
        cleanup(&path);
    }
}
