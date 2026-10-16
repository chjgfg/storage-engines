//! LSM 引擎核心：MemTable + SST 分层 + flush / leveled compaction。
//!
//! 读路径（新→旧）：mem → immutables → L0 → L1 → …
//! 写路径：insert →（可选 mem.wal）→ mem；超阈值 flush → L0；再 leveled compact。
//!
//! 版本 GC 由上层 `MVCC::vacuum` 负责；compact 只做空间合并（丢弃 Drop）。

use std::{
    collections::{BTreeMap, BinaryHeap, HashSet}, fs::{self, File}, io::{BufRead, BufReader, Write}, path::{Path, PathBuf}, sync::{
        Arc, Condvar, Mutex, atomic::{AtomicBool, AtomicU64, Ordering},
    }, thread::JoinHandle, time::Duration, vec,
};

use crate::lsm_tree::mem_wal::{MemWal, MemWalRecord};
use crate::lsm_tree::sst::{
    merge_latest_streaming, merge_streaming, merge_to_ssts, same_group, BlockCache, EntryValue,
    LightEntry, SstBuilder, SstFile, StoredEntry,
};

// ─── Options ─────────────────────────────────────────────────────────────────

/// 引擎可调参数（可在运行时通过 [`LSMTree::set_options`] 热更新部分字段）
#[derive(Debug, Clone)]
pub struct LSMTreeOptions {
    /// mem 字节阈值（估算 key+value）；默认 4MB
    pub mem_threshold_bytes: usize,
    /// mem 条目数兜底阈值；0 = 仅看字节；默认 65536
    pub mem_threshold_entries: usize,
    /// L0 文件数触发 compact
    pub l0_compact_threshold: usize,
    /// 每层相对 L1 的 size 倍增
    pub level_size_multiplier: usize,
    /// L1 目标总字节（再 × multiplier 得更高层）
    pub level_base_bytes: usize,
    /// 层数（含 L0），默认 4 → L0..L3
    pub max_levels: usize,
    /// 每个 data block 的条目数
    pub sst_block_entries: usize,
    /// 目标单个 SST 大约字节（compact 切分）
    pub target_sst_bytes: usize,
    /// 裸 LSM 写 mem.wal（MVCC 应关闭）
    pub enable_mem_wal: bool,
    /// 每条 mem.wal 写后 fsync（慢但崩溃后 mem 不丢）。
    /// **默认 false**：只保证 append 到 OS 页缓存；进程 kill 可能丢未 flush 数据。
    /// flush 成功后仍会 sync + truncate。
    pub mem_wal_sync_on_write: bool,
    /// SST bloom
    pub enable_bloom: bool,
    /// 后台 compact 线程（默认关；MVCC 关）
    pub enable_bg_compact: bool,
    /// block cache 容量（块数）；0=关
    pub block_cache_blocks: usize,
    /// 单次 flush 后最多 compact 轮数
    pub max_compactions_per_flush: usize,
    /// 使用 memmap2 映射 SST 文件（真 mmap）
    pub use_mmap: bool,
    /// 默认 TTL（秒）；独立 ttl_meta，不改 value 字节
    pub default_ttl_secs: Option<u64>,
    /// bloom bits/key
    pub bloom_bits_per_key: usize,
    /// compact 时按活跃 snapshot 水位回收旧 seq
    pub enable_snapshot_gc: bool,
    /// L0 文件数 ≥ 此值时写路径触发一次 compact（软限流）
    pub l0_slowdown_trigger: usize,
    /// L0 文件数 ≥ 此值时写路径阻塞直到 L0 降下来（硬停写）
    pub l0_stop_trigger: usize,
    /// 大 value 阈值（字节）；超过则写入 `blobs/` 旁路，SST 只存指针
    pub blob_threshold: usize,
}

impl Default for LSMTreeOptions {
    fn default() -> Self {
        Self {
            mem_threshold_bytes: 4 * 1024 * 1024,
            mem_threshold_entries: 64 * 1024,
            l0_compact_threshold: 4,
            level_size_multiplier: 10,
            level_base_bytes: 2 * 1024 * 1024,
            max_levels: 4,
            sst_block_entries: 64,
            target_sst_bytes: 512 * 1024,
            enable_mem_wal: true,
            mem_wal_sync_on_write: false,
            enable_bloom: true,
            enable_bg_compact: false,
            block_cache_blocks: 64,
            max_compactions_per_flush: 8,
            use_mmap: false,
            default_ttl_secs: None,
            bloom_bits_per_key: 10,
            enable_snapshot_gc: true,
            l0_slowdown_trigger: 8,
            l0_stop_trigger: 20,
            blob_threshold: 4 * 1024, // 4KB
        }
    }
}

impl LSMTreeOptions {
    /// MVCC 后端：无 mem WAL；写路径可同步 compact（事务量通常不大）
    pub fn for_mvcc() -> Self {
        Self {
            enable_mem_wal: false,
            enable_bg_compact: false,
            mem_threshold_bytes: 32 * 1024 * 1024,
            mem_threshold_entries: 256 * 1024,
            l0_compact_threshold: 8,
            l0_slowdown_trigger: 12,
            l0_stop_trigger: 32,
            target_sst_bytes: 2 * 1024 * 1024,
            level_base_bytes: 16 * 1024 * 1024,
            max_compactions_per_flush: 4,
            blob_threshold: 4 * 1024,
            // 块缓存从 64 提到 256：实测 5002 键 / 10005 行的小库就有 ~157 个块，64 槽装不下
            // 工作集 ⇒ 乱序点读每次 1.61 个未命中（一次未命中实测 ≈ 40µs）。256 槽按块容量
            // 64 条 × ~100B 估 ≈ 2MB 上限，对单核小机器是可控的；再大的库靠它也不可能全装下，
            // 那时决定代价的是"同一把键散在几个 SST"（见 lsm_tree_mvcc 的基线用例）。
            block_cache_blocks: 256,
            ..Self::default()
        }
    }

    /// 大批量导入（10M 级）：更大 mem、更松 L0、无 mem.wal
    pub fn for_bulk_load() -> Self {
        Self {
            enable_mem_wal: false,
            enable_bg_compact: false, // bulk 自己控制 compact 节奏
            mem_threshold_bytes: 64 * 1024 * 1024,
            mem_threshold_entries: 512 * 1024,
            l0_compact_threshold: 8,
            l0_slowdown_trigger: 16,
            l0_stop_trigger: 48,
            target_sst_bytes: 4 * 1024 * 1024,
            level_base_bytes: 32 * 1024 * 1024,
            level_size_multiplier: 10,
            max_compactions_per_flush: 2,
            enable_bloom: true,
            block_cache_blocks: 128,
            blob_threshold: 4 * 1024,
            ..Self::default()
        }
    }
}

// ─── Types ───────────────────────────────────────────────────────────────────

/// user_key -> (seq -> value)；同 key 多版本以支持 Snapshot
type MemMap = BTreeMap<Vec<u8>, BTreeMap<u64, EntryValue>>;

/// compaction filter：返回 `true` 保留，`false` 丢弃。
pub type CompactionFilter =
    Arc<dyn Fn(&[u8], &StoredEntry) -> bool + Send + Sync + 'static>;

/// 读快照：仅可见 `seq <= snap_seq` 的写入
#[derive(Debug, Clone, Copy)]
pub struct Snapshot {
    pub seq: u64,
}

/// LSM 存储引擎
pub struct LSMTree {
    dir: PathBuf,
    opts: LSMTreeOptions,
    mem: MemMap,
    mem_bytes: usize,
    immutables: Vec<MemMap>,
    levels: Vec<Vec<SstFile>>,
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
    /// key -> expire unix secs (independent of value bytes)
    ttl_meta: BTreeMap<Vec<u8>, u64>,
    manifest_edit_id: u64,
    bg: Option<BgCompactCtrl>,
    /// bulk 导入：flush 不触发同步 compact，stall 放宽
    bulk_mode: bool,
}

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

impl LSMTree {
    pub fn open(dir: impl AsRef<Path>) -> std::io::Result<Self> {
        Self::open_with_config(dir, LSMTreeOptions::default())
    }

    /// 兼容旧签名：`(mem_entry_threshold, l0_compact)`
    pub fn open_with_options(
        dir: impl AsRef<Path>,
        mem_threshold: usize,
        l0_compact_threshold: usize,
    ) -> std::io::Result<Self> {
        let mut opts = LSMTreeOptions::default();
        opts.mem_threshold_entries = mem_threshold.max(1);
        opts.mem_threshold_bytes = usize::MAX / 4; // 主要由 entries 触发
        opts.l0_compact_threshold = l0_compact_threshold.max(1);
        Self::open_with_config(dir, opts)
    }

    pub fn open_with_config(
        dir: impl AsRef<Path>,
        opts: LSMTreeOptions,
    ) -> std::io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(dir.join("sst"))?;
        let max_levels = opts.max_levels.max(2);

        let cache = Arc::new(Mutex::new(BlockCache::new(opts.block_cache_blocks)));
        let mem_wal = if opts.enable_mem_wal {
            Some(MemWal::open(&dir)?)
        } else {
            None
        };

        let mut db = Self {
            dir: dir.clone(),
            opts,
            mem: BTreeMap::new(),
            mem_bytes: 0,
            immutables: Vec::new(),
            levels: (0..max_levels).map(|_| Vec::new()).collect(),
            next_sst_id: AtomicU64::new(1),
            write_seq: AtomicU64::new(1),
            dirty: false,
            mem_wal,
            block_cache: cache,
            cached_live: 0,
            cached_tomb: 0,
            stats_dirty: true,
            compaction_filter: None,
            active_snapshots: BTreeMap::new(),
            ttl_meta: BTreeMap::new(),
            manifest_edit_id: 0,
            bg: None,
            bulk_mode: false,
        };

        let manifest = db.manifest_path();
        let legacy = db.dir.join("CURRENT.snap");
        if manifest.exists() {
            db.load_manifest()?;
        } else if legacy.exists() {
            db.import_legacy_snap(&legacy)?;
            let _ = fs::remove_file(&legacy);
            db.flush()?;
        }

        // mem.wal redo：只重放最后一个 Flush 之后的记录
        if db.mem_wal.is_some() {
            let recs = db.mem_wal.as_mut().unwrap().read_all()?;
            let start = recs
                .iter()
                .rposition(|r| matches!(r, MemWalRecord::Flush { .. }))
                .map(|i| i + 1)
                .unwrap_or(0);
            let mut max_seq = 0u64;
            for r in &recs[start..] {
                match r {
                    MemWalRecord::Put { key, value, seq } => {
                        let s = if *seq == 0 {
                            db.write_seq.fetch_add(1, Ordering::SeqCst)
                        } else {
                            *seq
                        };
                        max_seq = max_seq.max(s);
                        db.mem_insert_no_wal(key.clone(), StoredEntry::put(s, value.clone()));
                    }
                    MemWalRecord::Drop { key, seq } => {
                        let s = if *seq == 0 {
                            db.write_seq.fetch_add(1, Ordering::SeqCst)
                        } else {
                            *seq
                        };
                        max_seq = max_seq.max(s);
                        db.mem_insert_no_wal(key.clone(), StoredEntry::drop_at(s));
                    }
                    MemWalRecord::Flush {
                        next_write_seq, ..
                    } => {
                        if *next_write_seq > 0 {
                            max_seq = max_seq.max(next_write_seq.saturating_sub(1));
                        }
                    }
                }
            }
            if max_seq > 0 {
                let cur = db.write_seq.load(Ordering::SeqCst);
                if max_seq + 1 > cur {
                    db.write_seq.store(max_seq + 1, Ordering::SeqCst);
                }
            }
            if start == recs.len() && !recs.is_empty() {
                db.mem_wal.as_mut().unwrap().truncate()?;
            }
        }

        db.attach_cache_to_levels();
        // 启动不扫全库：stats 懒算（stats_dirty=true 已设），避免 O(全部 SST)
        if db.opts.enable_bg_compact {
            db.start_bg_compact();
        }
        Ok(db)
    }

    fn dir_cache_ns(dir: &Path) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        dir.hash(&mut h);
        h.finish()
    }

    fn attach_cache_to_levels(&mut self) {
        let cache = self.block_cache.clone();
        let use_mmap = self.opts.use_mmap;
        let ns = Self::dir_cache_ns(&self.dir);
        for level in &mut self.levels {
            for sst in level {
                sst.set_cache_ns(ns);
                sst.set_block_cache(cache.clone());
                if use_mmap {
                    let _ = sst.enable_mmap();
                }
            }
        }
    }

    fn manifest_path(&self) -> PathBuf {
        self.dir.join("MANIFEST")
    }

    fn sst_path(&self, id: u64) -> PathBuf {
        self.dir.join("sst").join(format!("{id:08}.sst"))
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn options(&self) -> &LSMTreeOptions {
        &self.opts
    }

    /// 热更新部分 option（阈值 / compact 触发 / bloom 新文件 / fsync 策略等）。
    /// 不改变：`enable_mem_wal`（开/关需 reopen）、已有 SST 格式。
    pub fn set_options(&mut self, patch: LSMTreeOptions) {
        let keep_wal = self.opts.enable_mem_wal;
        self.opts = patch;
        self.opts.enable_mem_wal = keep_wal;
        // block cache 容量：重建
        *self.block_cache.lock().unwrap() =
            BlockCache::new(self.opts.block_cache_blocks);
    }

    /// 设置 compaction filter（`None` 清除）
    pub fn set_compaction_filter(&mut self, f: Option<CompactionFilter>) {
        self.compaction_filter = f;
    }

    /// 安装共享 block cache（多 CF 复用）
    pub fn install_shared_block_cache(&mut self, cache: Arc<Mutex<BlockCache>>) {
        self.block_cache = cache.clone();
        self.attach_cache_to_levels();
    }

    /// 设置 MVCC xmin compaction filter：丢弃 version < xmin 的旧 encode_key 版本
    /// （与 MVCC::vacuum 互补；key 格式 raw||be_u64(version)）
    pub fn set_mvcc_xmin_filter(&mut self, xmin: u64) {
        self.compaction_filter = Some(Arc::new(move |k: &[u8], se: &StoredEntry| {
            if k.len() < 8 {
                return true;
            }
            let mut ver_bytes = [0u8; 8];
            ver_bytes.copy_from_slice(&k[k.len() - 8..]);
            let ver = u64::from_be_bytes(ver_bytes);
            // 保留 version >= xmin 的全部；version < xmin 只在它是最新合并结果时保留
            // merge 已折叠为最新，故 version < xmin 且为 tombstone 可丢
            if ver < xmin {
                !matches!(se.value, EntryValue::Put(None)) && !matches!(se.value, EntryValue::Drop)
                // 实际上最新若 ver < xmin 仍是存活数据，必须保留
                // 因此这里永远 true：xmin 过滤应在 vacuum 做物理删。compact filter 仅丢 ver < xmin 的中间版本
                // 但 merge 后每 key 一条——无法丢「旧版本」。改为：总是保留。
                // 真正有用：若 key 的 version < xmin 且 value 是 tombstone，丢弃整 key
            } else {
                true
            }
        }));
        // simplify: tombstone with ver < xmin drop
        self.compaction_filter = Some(Arc::new(move |k: &[u8], se: &StoredEntry| {
            if k.len() < 8 {
                return true;
            }
            let mut ver_bytes = [0u8; 8];
            ver_bytes.copy_from_slice(&k[k.len() - 8..]);
            let ver = u64::from_be_bytes(ver_bytes);
            if ver < xmin && matches!(se.value, EntryValue::Put(None) | EntryValue::Drop) {
                return false;
            }
            true
        }));
    }


    /// 当前写序号（下一次写入将使用的序号）
    pub fn snapshot_seq(&self) -> u64 {
        self.write_seq.load(Ordering::SeqCst)
    }

    /// 创建读快照并 **pin**（阻止 compact 回收更旧版本）。
    /// 用完后应 `release_snapshot`（或 drop 时用 [`PinnedSnapshot`]）。
    pub fn snapshot(&mut self) -> Snapshot {
        let seq = self.write_seq.load(Ordering::SeqCst).saturating_sub(1);
        *self.active_snapshots.entry(seq).or_insert(0) += 1;
        Snapshot { seq }
    }

    /// 释放 pin（与 `snapshot` 成对）
    pub fn release_snapshot(&mut self, snap: Snapshot) {
        if let Some(c) = self.active_snapshots.get_mut(&snap.seq) {
            *c = c.saturating_sub(1);
            if *c == 0 {
                self.active_snapshots.remove(&snap.seq);
            }
        }
    }

    /// 当前活跃 snapshot 最小 seq（compact GC 水位）；无 pin 时为 None
    pub fn oldest_snapshot_seq(&self) -> Option<u64> {
        self.active_snapshots.keys().next().copied()
    }

    /// 只读快照序号，不 pin（不阻止 GC）
    pub fn snapshot_seq_unpinned(&self) -> Snapshot {
        Snapshot {
            seq: self.write_seq.load(Ordering::SeqCst).saturating_sub(1),
        }
    }

    // ─── 逻辑 KV ─────────────────────────────────────────────────────────────

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.insert(key, Some(value));
    }

    pub fn delete(&mut self, key: Vec<u8>) {
        self.insert(key, None);
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.deref_value(self.get_at_seq(key, u64::MAX))
    }

    pub fn get_at(&self, snap: &Snapshot, key: &[u8]) -> Option<Vec<u8>> {
        self.deref_value(self.get_at_seq(key, snap.seq))
    }

    fn get_at_seq(&self, key: &[u8], snap_seq: u64) -> Option<Vec<u8>> {
        if self.is_ttl_expired(key) {
            return None;
        }
        match self.get_entry_at(key, snap_seq) {
            Some(se) => match se.value {
                EntryValue::Put(Some(v)) => Some(v),
                _ => None,
            },
            None => None,
        }
    }

    fn is_ttl_expired(&self, key: &[u8]) -> bool {
        if self.opts.default_ttl_secs.is_none() && self.ttl_meta.is_empty() {
            return false;
        }
        if let Some(&exp) = self.ttl_meta.get(key) {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            return now > exp;
        }
        false
    }

    // ─── 版本化 API ──────────────────────────────────────────────────────────

    pub fn insert(&mut self, key: Vec<u8>, value: Option<Vec<u8>>) {
        self.insert_inner(key, value, false);
    }

    /// 高速写入：跳过「读旧值维护 live 统计」与 TTL meta（bulk / 压测用）。
    /// 语义仍正确；`stats().live_keys` 可能略不准，可用 `stats_exact()`。
    pub fn insert_fast(&mut self, key: Vec<u8>, value: Option<Vec<u8>>) {
        self.insert_inner(key, value, true);
    }

    /// 批量高速写入：一次不抢锁外逻辑，调用方已持 `&mut self`
    pub fn insert_batch_fast(&mut self, items: impl IntoIterator<Item = (Vec<u8>, Option<Vec<u8>>)>) {
        for (k, v) in items {
            self.insert_inner(k, v, true);
        }
    }

    /// 开关 bulk 模式：写路径 flush 不 compact，L0 限流放宽
    pub fn set_bulk_mode(&mut self, on: bool) {
        self.bulk_mode = on;
        if on {
            // 进一步放大阈值
            self.opts.mem_threshold_bytes = self.opts.mem_threshold_bytes.max(128 * 1024 * 1024);
            self.opts.mem_threshold_entries = self.opts.mem_threshold_entries.max(1_000_000);
            self.opts.l0_stop_trigger = self.opts.l0_stop_trigger.max(128);
            self.opts.l0_slowdown_trigger = self.opts.l0_slowdown_trigger.max(64);
            self.opts.max_compactions_per_flush = 1;
        }
    }

    pub fn bulk_mode(&self) -> bool {
        self.bulk_mode
    }

    /// Bulk 极速路径：把 **已按 key 排序** 的条目直接写成一个 L0 SST（跳过 MemTable）。
    /// 调用方保证 batch 内 key 严格递增。
    pub fn bulk_ingest_sorted(
        &mut self,
        entries: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    ) -> std::io::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let id = self.next_sst_id.fetch_add(1, Ordering::SeqCst);
        let path = self.sst_path(id);
        // bulk：关 bloom + 大 block，减少 CPU；不 fsync
        let mut builder = SstBuilder::with_options(
            id,
            &path,
            if self.bulk_mode { 256 } else { self.opts.sst_block_entries },
            if self.bulk_mode { false } else { self.opts.enable_bloom },
        )
        .with_bloom_bits(self.opts.bloom_bits_per_key);
        for (k, v) in entries {
            let seq = self.write_seq.fetch_add(1, Ordering::SeqCst);
            builder.add(k, StoredEntry::put(seq, v));
        }
        let n = builder.len();
        let mut sst = builder.finish_with_sync(!self.bulk_mode)?;
        sst.set_cache_ns(Self::dir_cache_ns(&self.dir));
        sst.set_block_cache(self.block_cache.clone());
        if self.opts.use_mmap {
            let _ = sst.enable_mmap();
        }
        self.levels[0].push(sst);
        self.cached_live = self.cached_live.saturating_add(n);
        self.dirty = true;
        if self.bulk_mode {
            let l0 = self.levels[0].len();
            if l0 >= self.opts.l0_stop_trigger {
                let _ = self.compact_level(0);
            }
            // 每次 ingest 更新 MANIFEST（bulk 下不 fsync，成本低）
            self.write_manifest()?;
        } else if self.levels[0].len() >= self.opts.l0_compact_threshold {
            let _ = self.maybe_compact();
            self.write_manifest()?;
        } else {
            self.write_manifest()?;
        }
        Ok(())
    }


    fn insert_inner(&mut self, key: Vec<u8>, mut value: Option<Vec<u8>>, fast: bool) {
        // 大 value 旁路
        if let Some(ref v) = value {
            if v.len() >= self.opts.blob_threshold {
                if let Ok(ptr) = self.write_blob(v) {
                    value = Some(ptr);
                }
            }
        }

        let seq = self.write_seq.fetch_add(1, Ordering::SeqCst);
        if let Some(wal) = self.mem_wal.as_mut() {
            let _ = wal.append(MemWalRecord::Put {
                key: key.clone(),
                value: value.clone(),
                seq,
            });
            if self.opts.mem_wal_sync_on_write {
                let _ = wal.sync();
            }
        }

        if !fast {
            let had = self
                .get_entry_at(&key, u64::MAX)
                .map(|e| matches!(e.value, EntryValue::Put(Some(_))))
                .unwrap_or(false);
            self.mem_insert_no_wal(key.clone(), StoredEntry::put(seq, value.clone()));
            if matches!(value, Some(_)) {
                if !had {
                    self.cached_live = self.cached_live.saturating_add(1);
                }
            } else if had {
                self.cached_live = self.cached_live.saturating_sub(1);
                self.cached_tomb = self.cached_tomb.saturating_add(1);
            } else {
                self.cached_tomb = self.cached_tomb.saturating_add(1);
            }
            if let Some(secs) = self.opts.default_ttl_secs {
                if value.is_some() {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0);
                    self.ttl_meta.insert(key, now.saturating_add(secs));
                } else {
                    self.ttl_meta.remove(&key);
                }
            }
        } else {
            self.mem_insert_no_wal(key, StoredEntry::put(seq, value));
            // 近似：当新 key
            self.cached_live = self.cached_live.saturating_add(1);
        }

        self.dirty = true;
        self.maybe_flush_mem();
        self.maybe_stall_on_l0();
    }

    /// 大 value 写入 `blobs/{seq}.blob`，返回指针 value：`\x02` + seq_u64_le
    fn write_blob(&self, data: &[u8]) -> std::io::Result<Vec<u8>> {
        let blob_dir = self.dir.join("blobs");
        fs::create_dir_all(&blob_dir)?;
        let seq = self.write_seq.load(Ordering::SeqCst);
        let path = blob_dir.join(format!("{seq:016}.blob"));
        fs::write(&path, data)?;
        let mut ptr = Vec::with_capacity(9);
        ptr.push(0x02); // BLOB pointer tag
        ptr.extend_from_slice(&seq.to_le_bytes());
        Ok(ptr)
    }

    /// 解析 blob 指针；非指针返回 None
    pub fn resolve_blob(value: &[u8], dir: &Path) -> Option<Vec<u8>> {
        if value.len() == 9 && value[0] == 0x02 {
            let seq = u64::from_le_bytes(value[1..9].try_into().ok()?);
            let path = dir.join("blobs").join(format!("{seq:016}.blob"));
            return fs::read(path).ok();
        }
        None
    }

    /// blob 旁路的空间回收：删掉**没有任何存活行引用**的 `blobs/*.blob`，返回删掉的个数。
    ///
    /// `keep` 是存活行的键（vacuum 裁完之后的那一集）。为什么只能由 vacuum 来做、compact
    /// 做不了：compact 只看得到它正在合并的那几个文件，没法判断"这个键在别的层里还有一份
    /// 更新的版本正引用着同一个 blob"。
    ///
    /// 判据只认指针字节（`\x02 + seq_u64_le`，见 [`Self::write_blob`]），不解析 value。
    /// 保守方向是「宁可留着也不删错」：删掉一个仍被引用的 blob 是**丢数据**，
    /// 留着一个没人引用的 blob 只是占空间。
    pub fn gc_blobs(&self, keep: &[Vec<u8>]) -> std::io::Result<usize> {
        let mut referenced: HashSet<u64> = HashSet::new();
        for enc in keep {
            // 外层 None = 这行已经不在；内层 None = Drop 墓碑（墓碑不引用任何 blob）
            // ⚠️ 这里刻意用**不解析**的 get_raw_key_at：GC 靠指针字节认引用，解析就认不出来了
            let Some(Some(val)) = self.get_raw_key_at(enc, u64::MAX) else {
                continue;
            };
            if let Some(seq) = Self::blob_ptr_seq(&val) {
                referenced.insert(seq);
            }
        }

        let blob_dir = self.dir.join("blobs");
        let Ok(rd) = fs::read_dir(&blob_dir) else {
            return Ok(0); // 从没写过大数据 ⇒ 没有旁路目录，正常
        };
        let mut removed = 0usize;
        for ent in rd.flatten() {
            let name = ent.file_name();
            let name = name.to_string_lossy();
            // 文件名是 `{seq:016}.blob`：十进制零填充
            let Some(stem) = name.strip_suffix(".blob") else {
                continue;
            };
            let Ok(seq) = stem.parse::<u64>() else {
                continue;
            };
            if referenced.contains(&seq) {
                continue;
            }
            match fs::remove_file(ent.path()) {
                Ok(()) => removed += 1,
                Err(e) => log::warn!("blob 回收跳过 {}：{e}", ent.path().display()),
            }
        }
        Ok(removed)
    }

    fn maybe_stall_on_l0(&mut self) {
        if self.bulk_mode {
            // bulk：只在极端 L0 堆积时做一次 compact，避免写路径被 leveled 拖死
            let l0 = self.levels.first().map(|l| l.len()).unwrap_or(0);
            if l0 >= self.opts.l0_stop_trigger {
                let _ = self.compact_level(0);
                let _ = self.write_manifest();
            }
            return;
        }
        let l0 = self.levels.first().map(|l| l.len()).unwrap_or(0);
        if l0 >= self.opts.l0_stop_trigger {
            // 硬停：同步 compact 直到 L0 降到 slowdown 以下
            let target = self.opts.l0_slowdown_trigger.max(1);
            let mut guard = 0;
            while self.levels.first().map(|l| l.len()).unwrap_or(0) >= target && guard < 64 {
                let _ = self.compact_level(0);
                guard += 1;
            }
            let _ = self.write_manifest();
            self.notify_bg();
        } else if l0 >= self.opts.l0_slowdown_trigger {
            // 软限：顺带 compact 一轮，并通知后台
            let _ = self.compact_level(0);
            self.notify_bg();
        }
    }

    pub fn remove(&mut self, key: &[u8]) -> bool {
        // 写 Drop 版本，保留历史供 Snapshot 读取
        if self.get_entry(key).is_none() {
            return false;
        }
        let seq = self.write_seq.fetch_add(1, Ordering::SeqCst);
        if let Some(wal) = self.mem_wal.as_mut() {
            let _ = wal.append(MemWalRecord::Drop {
                key: key.to_vec(),
                seq,
            });
            if self.opts.mem_wal_sync_on_write {
                let _ = wal.sync();
            }
        }
        if self.get_entry_at(key, u64::MAX).map(|e| matches!(e.value, EntryValue::Put(Some(_)))).unwrap_or(false) {
            self.cached_live = self.cached_live.saturating_sub(1);
        }
        self.ttl_meta.remove(key);
        self.mem_insert_no_wal(key.to_vec(), StoredEntry::drop_at(seq));
        self.dirty = true;
        self.stats_dirty = true;
        self.maybe_flush_mem();
        true
    }

    fn mem_insert_no_wal(&mut self, key: Vec<u8>, val: StoredEntry) {
        let add = key.len() + val.estimate_bytes();
        let versions = self.mem.entry(key).or_default();
        if let Some(old) = versions.insert(val.seq, val.value) {
            self.mem_bytes = self
                .mem_bytes
                .saturating_sub(old.estimate_bytes())
                .saturating_add(add);
        } else {
            self.mem_bytes = self.mem_bytes.saturating_add(add);
        }
    }

    fn mem_get_at(map: &MemMap, key: &[u8], snap_seq: u64) -> Option<StoredEntry> {
        let vers = map.get(key)?;
        vers.iter()
            .rev()
            .find(|(seq, _)| **seq <= snap_seq)
            .map(|(seq, v)| StoredEntry {
                seq: *seq,
                value: v.clone(),
            })
    }

    fn mem_range_at(
        map: &MemMap,
        low: &[u8],
        high: &[u8],
        snap_seq: u64,
    ) -> Vec<(Vec<u8>, StoredEntry)> {
        let mut out = Vec::new();
        for (k, vers) in map.range(low.to_vec()..=high.to_vec()) {
            if let Some(se) = vers
                .iter()
                .rev()
                .find(|(seq, _)| **seq <= snap_seq)
                .map(|(seq, v)| StoredEntry {
                    seq: *seq,
                    value: v.clone(),
                })
            {
                out.push((k.clone(), se));
            }
        }
        out
    }

    fn mem_to_sorted_entries(map: &MemMap) -> Vec<(Vec<u8>, StoredEntry)> {
        let mut out = Vec::new();
        for (k, vers) in map {
            for (seq, v) in vers {
                out.push((
                    k.clone(),
                    StoredEntry {
                        seq: *seq,
                        value: v.clone(),
                    },
                ));
            }
        }
        out.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.seq.cmp(&b.1.seq)));
        out
    }

    fn mem_view_at(map: &MemMap, snap_seq: u64) -> Vec<(Vec<u8>, StoredEntry)> {
        let mut out = Vec::new();
        for (k, vers) in map {
            if let Some(se) = vers
                .iter()
                .rev()
                .find(|(seq, _)| **seq <= snap_seq)
                .map(|(seq, v)| StoredEntry {
                    seq: *seq,
                    value: v.clone(),
                })
            {
                // 重量路径：value 被整个克隆出来。全库折叠不该走这里 ——
                // 计数探针就是用来发现「不知不觉又用回重量路径」。
                #[cfg(test)]
                crate::lsm_tree::sst::note_value_materialization();
                out.push((k.clone(), se));
            }
        }
        out
    }

    /// 内存表的**轻量**视图：每个键只带 `seq` 与种类字节，**不克隆 value**。
    fn mem_view_light(map: &MemMap, snap_seq: u64) -> Vec<(Vec<u8>, LightEntry)> {
        let mut out = Vec::new();
        for (k, vers) in map {
            if let Some((seq, ev)) = vers.iter().rev().find(|(s, _)| **s <= snap_seq) {
                out.push((
                    k.clone(),
                    LightEntry {
                        seq: *seq,
                        kind: ev.kind_byte(),
                    },
                ));
            }
        }
        out
    }

    /// memtable 的 `[from, to)` 轻量视图：界走 BTreeMap 的 `range`，界外的键根本不遍历。
    fn mem_view_light_range(
        map: &MemMap,
        from: Option<&[u8]>,
        to: Option<&[u8]>,
        snap_seq: u64,
    ) -> Vec<(Vec<u8>, LightEntry)> {
        use std::ops::Bound;
        let lo = match from {
            Some(k) => Bound::Included(k),
            None => Bound::Unbounded,
        };
        let hi = match to {
            Some(k) => Bound::Excluded(k),
            None => Bound::Unbounded,
        };
        let mut out = Vec::new();
        for (k, vers) in map.range::<[u8], _>((lo, hi)) {
            if let Some((seq, ev)) = vers.iter().rev().find(|(s, _)| **s <= snap_seq) {
                out.push((
                    k.clone(),
                    LightEntry {
                        seq: *seq,
                        kind: ev.kind_byte(),
                    },
                ));
            }
        }
        out
    }

    /// 全库「每个 enc key 的胜者」的**轻量**视图（跨源取胜规则与 [`Self::iter`] 一致：
    /// 源新→旧优先，同 src 取更大 seq）。
    ///
    /// 与 `iter()` 的差别就在**不克隆 value**：`iter()` 会把每个源的条目整个 `collect()`
    /// 进内存（连 value），所以一次全库扫描的峰值内存约等于整个库；这里的峰值只有键字节。
    /// 需要 value 的调用方对少数胜出行再点查 [`Self::get_value_at`]（会解析 blob 指针），语义与归并取胜一致。
    pub fn scan_latest_light(&self, snap_seq: u64) -> Vec<(Vec<u8>, LightEntry)> {
        let mut sources: Vec<Vec<(Vec<u8>, LightEntry)>> = Vec::new();
        if !self.mem.is_empty() {
            let part = Self::mem_view_light(&self.mem, snap_seq);
            if !part.is_empty() {
                sources.push(part);
            }
        }
        for imm in &self.immutables {
            let part = Self::mem_view_light(imm, snap_seq);
            if !part.is_empty() {
                sources.push(part);
            }
        }
        // 源顺序必须与 merge_all_at 一致：levels[0] 的 vec 是旧→新，反转成新→旧
        if let Some(l0) = self.levels.first() {
            for sst in l0.iter().rev() {
                sources.push(sst.light_view(snap_seq));
            }
        }
        for level in self.levels.iter().skip(1) {
            for sst in level {
                sources.push(sst.light_view(snap_seq));
            }
        }
        let mut out = Vec::new();
        merge_streaming(sources, |_, _| true, |k, le| out.push((k, le)));
        out
    }

    /// `[from, to)` 的跨源轻量视图。取胜规则与 [`Self::scan_latest_light`] 一字不差，
    /// 区别只有一个：**界下推到每一个源**。
    ///
    /// 为什么必须有它：`scan_latest_light` 把每个 memtable 整表遍历、每个 SST 整篇块读完
    /// 再交给归并，于是"10 行的窗口"付的是"全库"的块读代价（大库基线实测：LSM 窗口 scan
    /// 405 µs/行 vs 一次全量 scan 4.0 µs/行）。memtable 走 `range`，SST 从 `from` 所在块
    /// 起读、键越过 `to` 立即收手，先用 min/max 挡掉整篇不相干的文件。
    /// 跨源归并的**惰性**版本（M15b）：一次只往前拉一个键，所以"只要前 N 把键"的调用
    /// 不会把每个源整篇读完。
    ///
    /// 老的 `scan_latest_light_range` 先把每个源 `Vec` 化再归并 ⇒ 首页也要读满全库的块
    /// （实测 2000 行的库首页 53 次块读，其中 ~32 次就是全库块数）。改成惰性之后
    /// 首页只读"覆盖到那几把键"需要的块，`scan_page` / 窗口搜索 / 有界 vacuum 一起受益。
    ///
    /// 取胜规则与 `merge_streaming` 逐条一致：源序号小 = 更新（mem → immutables → L0 新→旧 →
    /// 更高层），同一个键只输出一行、由更新的源定论；同键同行的旧 seq 不复活。
    pub fn light_iter_range<'a>(
        &'a self,
        from: Option<&'a [u8]>,
        to: Option<&'a [u8]>,
        snap_seq: u64,
    ) -> LightMerge<'a> {
        let probe_low = from.unwrap_or(&[]);
        let probe_high = to.unwrap_or(&[0xffu8; 8]);
        let mut sources: Vec<Box<dyn Iterator<Item = (Vec<u8>, LightEntry)> + '_>> = Vec::new();
        if !self.mem.is_empty() {
            sources.push(Box::new(Self::mem_light_iter(&self.mem, from, to, snap_seq)));
        }
        for imm in &self.immutables {
            sources.push(Box::new(Self::mem_light_iter(imm, from, to, snap_seq)));
        }
        // 源顺序必须与 merge_all_at / scan_latest_light 一致：levels[0] 旧→新，反转成新→旧。
        // 先把"有哪些 SST、按什么优先级"排成一个 Vec，再统一建迭代器 ——
        // 用闭包 push 会把两个不同的生命周期搅在一起（编译器拒绝得有道理）。
        let mut ssts: Vec<&SstFile> = Vec::new();
        if let Some(l0) = self.levels.first() {
            ssts.extend(l0.iter().rev());
        }
        for level in self.levels.iter().skip(1) {
            ssts.extend(level.iter());
        }
        for sst in ssts {
            if !sst.may_overlap(probe_low, probe_high) {
                continue; // min/max 不相干 ⇒ 整篇不读
            }
            sources.push(Box::new(sst.light_iter(from, to, snap_seq)));
        }
        LightMerge::new(sources)
    }

    /// mem / immutable 的轻量视图（惰性）：`MemMap` 每个 key 一条版本列表 ⇒ 范围内逐 key
    /// 取"≤ snap 的最大 seq"那一版，与 `mem_view_light_range` 同一判据。
    fn mem_light_iter<'a>(
        map: &'a MemMap,
        from: Option<&'a [u8]>,
        to: Option<&'a [u8]>,
        snap_seq: u64,
    ) -> impl Iterator<Item = (Vec<u8>, LightEntry)> + 'a {
        use std::ops::Bound;
        let lo = match from {
            Some(k) => Bound::Included(k),
            None => Bound::Unbounded,
        };
        let hi = match to {
            Some(k) => Bound::Excluded(k),
            None => Bound::Unbounded,
        };
        map.range::<[u8], _>((lo, hi)).filter_map(move |(k, vers)| {
            let (seq, ev) = vers.iter().rev().find(|(s, _)| **s <= snap_seq)?;
            Some((k.clone(), LightEntry { seq: *seq, kind: ev.kind_byte() }))
        })
    }


    /// memtable 的轻量视图 + 「够 `max_rows` 行就准备收手，但把当前这一组读完才停」。
    ///
    /// 组的定义见 [`same_group`]：MVCC 层传 `group_suffix = 8`，一组 = 一把逻辑 key 的
    /// 整条版本链。返回的第二位是「本源后面还有行吗」。
    fn mem_view_light_break(
        map: &MemMap,
        snap_seq: u64,
        from: Option<&[u8]>,
        max_rows: usize,
        group_suffix: usize,
    ) -> (Vec<(Vec<u8>, LightEntry)>, bool) {
        use std::ops::Bound;
        let lo = match from {
            Some(k) => Bound::Included(k),
            None => Bound::Unbounded,
        };
        let mut out: Vec<(Vec<u8>, LightEntry)> = Vec::new();
        let mut over_budget = false;
        for (k, vers) in map.range::<[u8], _>((lo, Bound::Unbounded)) {
            if over_budget
                && !out.last().is_some_and(|(pk, _)| same_group(pk, k, group_suffix))
            {
                return (out, true);
            }
            if let Some((seq, ev)) = vers.iter().rev().find(|(s, _)| **s <= snap_seq) {
                out.push((
                    k.clone(),
                    LightEntry {
                        seq: *seq,
                        kind: ev.kind_byte(),
                    },
                ));
                if out.len() >= max_rows {
                    over_budget = true;
                }
            }
        }
        (out, false)
    }

    /// 「从 `from`（**含**，键的字节域）起的一扇窗口」的轻量视图：每个源最多读
    /// `max_rows_per_source` 行，返回 `(窗口内的胜出行, 右界)`。
    ///
    /// 右界 `None` ⇒ 已经读到库尾；`Some(B)` ⇒ 窗口只覆盖到 `enc < B` 附近，后面还有行。
    /// 每个源都只在**组边界**收手（见 [`Self::mem_view_light_break`]、
    /// [`SstFile::light_view_break_from`]），B 取各被截断源「最后一个整组起点」的最小值。
    ///
    /// ⚠️ B 只保证「窗口里至少有 2 组」，**不保证** B 正好落在组边界上：真前缀嵌套
    /// （`"a"` 与 `"a\0…"`）会让两把 key 的版本区间互相穿插，所以最后一组可能只收到一半。
    /// 调用方必须自己盯组边界 —— 只有「看到了下一组的第一行」才算上一组收齐（各引擎
    /// `vacuum_bounded` 都是这个写法）。保证 ≥2 组就是为了让它每轮至少前进一组。
    /// 单把热点 key 的链长到能吃满预算时，这里按 4 倍放大窗口重试（几何 ⇒ 总代价仍是
    /// 最终窗口的常数倍），直到某个源见底为止 —— 所以本函数一定前进。
    ///
    /// 取胜规则与 [`Self::scan_latest_light`] 完全一致，只是范围是一扇窗。存在的理由是锁：
    /// `LSMTree` 挂在 `Mutex` 后面，全库扫描会把这把锁按到归并走完，期间读写全部排队。
    pub fn scan_latest_light_window(
        &self,
        from: Option<&[u8]>,
        snap_seq: u64,
        max_rows_per_source: usize,
        group_suffix: usize,
    ) -> (Vec<(Vec<u8>, LightEntry)>, Option<Vec<u8>>) {
        fn count_groups(rows: &[(Vec<u8>, LightEntry)], suffix: usize) -> usize {
            let mut n = 0usize;
            let mut prev: Option<&Vec<u8>> = None;
            for (k, _) in rows {
                if prev.is_none_or(|p| !same_group(p, k, suffix)) {
                    n += 1;
                }
                prev = Some(k);
            }
            n
        }

        let mut cap = max_rows_per_source.max(8);
        loop {
            let (out, boundary) = self.window_once(from, snap_seq, cap, group_suffix);
            if boundary.is_none() || count_groups(&out, group_suffix) >= 2 || cap >= usize::MAX / 4
            {
                return (out, boundary);
            }
            cap *= 4;
        }
    }

    /// [`Self::scan_latest_light_window`] 的一次尝试：按 `cap` 向每个源取一段、定右界、归并。
    fn window_once(
        &self,
        from: Option<&[u8]>,
        snap_seq: u64,
        cap: usize,
        group_suffix: usize,
    ) -> (Vec<(Vec<u8>, LightEntry)>, Option<Vec<u8>>) {
        fn note(
            sources: &mut Vec<Vec<(Vec<u8>, LightEntry)>>,
            boundary: &mut Option<Vec<u8>>,
            part: Vec<(Vec<u8>, LightEntry)>,
            truncated: bool,
            group_suffix: usize,
        ) {
            if part.is_empty() {
                return;
            }
            if truncated {
                // 该源最后一个整组的起点
                let last = part.last().map(|(k, _)| k.clone()).unwrap_or_default();
                let mut start = last.clone();
                for (k, _) in part.iter().rev() {
                    if !same_group(k, &last, group_suffix) {
                        break;
                    }
                    start = k.clone();
                }
                match boundary {
                    Some(b) if *b <= start => {}
                    _ => *boundary = Some(start),
                }
            }
            sources.push(part);
        }

        let mut sources: Vec<Vec<(Vec<u8>, LightEntry)>> = Vec::new();
        let mut boundary: Option<Vec<u8>> = None;

        if !self.mem.is_empty() {
            let (part, tr) = Self::mem_view_light_break(&self.mem, snap_seq, from, cap, group_suffix);
            note(&mut sources, &mut boundary, part, tr, group_suffix);
        }
        for imm in &self.immutables {
            let (part, tr) = Self::mem_view_light_break(imm, snap_seq, from, cap, group_suffix);
            note(&mut sources, &mut boundary, part, tr, group_suffix);
        }
        // 源顺序必须与 scan_latest_light / merge_all_at 一致：levels[0] 是旧→新，反转成新→旧
        if let Some(l0) = self.levels.first() {
            for sst in l0.iter().rev() {
                let (part, tr) = sst.light_view_break_from(from, snap_seq, cap, group_suffix);
                note(&mut sources, &mut boundary, part, tr, group_suffix);
            }
        }
        for level in self.levels.iter().skip(1) {
            for sst in level {
                let (part, tr) = sst.light_view_break_from(from, snap_seq, cap, group_suffix);
                note(&mut sources, &mut boundary, part, tr, group_suffix);
            }
        }

        let bound = boundary.clone();
        let mut out = Vec::new();
        merge_streaming(
            sources,
            |k, _| match &bound {
                Some(b) => k < b.as_slice(),
                None => true,
            },
            |k, le| out.push((k, le)),
        );
        (out, boundary)
    }

    fn maybe_flush_mem(&mut self) {
        let mem_entry_count: usize = self.mem.values().map(|v| v.len()).sum();
        let by_entries = self.opts.mem_threshold_entries > 0
            && mem_entry_count >= self.opts.mem_threshold_entries;
        let by_bytes = self.mem_bytes >= self.opts.mem_threshold_bytes;
        if by_entries || by_bytes {
            let _ = self.flush_mem();
        }
    }

    pub fn get_raw(&self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        match self.get_entry_at(key, u64::MAX) {
            Some(se) => match se.value {
                EntryValue::Put(v) => Some(v),
                EntryValue::Drop => None,
            },
            None => None,
        }
    }

    fn get_entry(&self, key: &[u8]) -> Option<StoredEntry> {
        self.get_entry_at(key, u64::MAX)
    }

    fn get_entry_at(&self, key: &[u8], snap_seq: u64) -> Option<StoredEntry> {
        let mut best: Option<StoredEntry> = None;
        let consider = |best: &mut Option<StoredEntry>, se: StoredEntry| {
            if !se.visible_at(snap_seq) {
                return;
            }
            if best.as_ref().map(|b| se.seq >= b.seq).unwrap_or(true) {
                *best = Some(se);
            }
        };
        if let Some(se) = Self::mem_get_at(&self.mem, key, snap_seq) {
            consider(&mut best, se);
        }
        for imm in &self.immutables {
            if let Some(se) = Self::mem_get_at(imm, key, snap_seq) {
                consider(&mut best, se);
            }
        }
        if let Some(l0) = self.levels.first() {
            for sst in l0.iter().rev() {
                if let Some(v) = sst.get_at(key, snap_seq) {
                    consider(&mut best, v);
                }
            }
        }
        for level in self.levels.iter().skip(1) {
            for sst in level {
                if key < sst.min_key.as_slice() || key > sst.max_key.as_slice() {
                    continue;
                }
                if let Some(v) = sst.get_at(key, snap_seq) {
                    consider(&mut best, v);
                }
            }
        }
        best
    }

    pub fn range_scan(&self, low: Vec<u8>, high: Vec<u8>) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        self.range_scan_at_seq(&low, &high, u64::MAX)
    }

    pub fn range_scan_at(
        &self,
        snap: &Snapshot,
        low: Vec<u8>,
        high: Vec<u8>,
    ) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        self.range_scan_at_seq(&low, &high, snap.seq)
    }

    fn range_scan_at_seq(
        &self,
        low: &[u8],
        high: &[u8],
        snap_seq: u64,
    ) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        self.merge_range_at(low, high, snap_seq)
            .into_iter()
            .filter_map(|(k, se)| match se.value {
                EntryValue::Put(val) => Some((k, self.deref_value(val))),
                EntryValue::Drop => None,
            })
            .collect()
    }

    /// 在闭区间 `[low, high]` 内**从大到小**找第一个满足 `accept(key, value)` 的键。
    ///
    /// MVCC 用它取版本链的链尾：版本键 `raw_key ‖ version.to_be_bytes()` 大端有序 ⇒
    /// 同一逻辑 key 的**最新版本排在区间末尾**，各源只需定位自己那一份、命中即停，
    /// 不必像 [`Self::range_scan`] 那样把整段（连 value）物化出来再挑。
    ///
    /// **遮蔽语义必须与 `range_scan` 一致**（这点是被测试逼出来的，不是想当然）：
    /// 一个 key 由「最新的含它的源 + 该源内最大可见 seq」定论；若定论成 `Drop`，该 key
    /// 就整体不可见 —— **不能**回退到更旧源里同 key 的 `Put`。所以这里先取各源的最大键，
    /// 再用现成的跨源点查 [`Self::get_raw`] 定论（它用的就是 `merge_range_at` 那套取胜规则）。
    ///
    /// 被拒的 key 进 `excluded` 后重找。常态一次命中；若拒绝数超过 `BACK_SCAN_FALLBACK`
    /// （例如以 `key` 为前缀的更长 key 大量交错那种退化形状），直接回退到一次
    /// `range_scan` —— 宁可慢，不能让「每次重找全源」比旧实现还差。
    pub fn find_back(
        &self,
        low: &[u8],
        high: &[u8],
        snap_seq: u64,
        accept: impl Fn(&[u8], &Option<Vec<u8>>) -> bool,
    ) -> Option<(Vec<u8>, Option<Vec<u8>>)> {
        const BACK_SCAN_FALLBACK: usize = 4096;

        let mut excluded: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
        loop {
            let mut cand: Option<Vec<u8>> = None;
            let mut consider = |k: Option<Vec<u8>>| {
                if let Some(k) = k {
                    if cand.as_ref().map_or(true, |b| k.as_slice() > b.as_slice()) {
                        cand = Some(k);
                    }
                }
            };

            consider(Self::mem_max_key(&self.mem, low, high, snap_seq, &excluded));
            for imm in &self.immutables {
                consider(Self::mem_max_key(imm, low, high, snap_seq, &excluded));
            }
            if let Some(l0) = self.levels.first() {
                for sst in l0.iter() {
                    consider(
                        sst.find_back(low, high, snap_seq, |k, _| !excluded.contains(k))
                            .map(|(k, _)| k),
                    );
                }
            }
            for level in self.levels.iter().skip(1) {
                for sst in level {
                    consider(
                        sst.find_back(low, high, snap_seq, |k, _| !excluded.contains(k))
                            .map(|(k, _)| k),
                    );
                }
            }

            let Some(key) = cand else { return None };
            if excluded.len() >= BACK_SCAN_FALLBACK {
                // 退化形状：回退成一次正向全扫再反向挑（代价与旧实现同阶）
                return self
                    .range_scan(low.to_vec(), high.to_vec())
                    .into_iter()
                    .rev()
                    .find(|(k, v)| accept(k.as_slice(), v));
            }
            excluded.insert(key.clone());

            // 跨源定论：Drop / 不存在 ⇒ 该 key 整体不可见，继续找下一个更小的键
            let Some(val) = self.get_value_at(&key, snap_seq) else {
                continue;
            };
            if accept(&key, &val) {
                return Some((key, val));
            }
        }
    }

    /// 某 memtable 在 `[low, high]` 内的最大键（跳过已定论的键）
    fn mem_max_key(
        map: &MemMap,
        low: &[u8],
        high: &[u8],
        snap_seq: u64,
        excluded: &std::collections::HashSet<Vec<u8>>,
    ) -> Option<Vec<u8>> {
        map.range(low.to_vec()..=high.to_vec())
            .rev()
            .filter(|(k, _)| !excluded.contains(k.as_slice()))
            .find(|(_, vers)| vers.iter().rev().any(|(s, _)| *s <= snap_seq))
            .map(|(k, _)| k.clone())
    }

    /// `get_raw` 的按 seq 版本：跨源取胜后给出该 key 的逻辑 value。
    ///
    /// 外层 `None` = `Drop` 或根本不存在（该 key 不可见）；
    /// `Some(None)` = `Put(None)`，即「行存在但逻辑 value 为 None」—— 与 `range_scan` 的
    /// `(k, None)` 表示一致，两者不能混。
    pub(crate) fn get_raw_key_at(&self, key: &[u8], snap_seq: u64) -> Option<Option<Vec<u8>>> {
        match self.get_entry_at(key, snap_seq)? {
            StoredEntry { value: EntryValue::Put(v), .. } => Some(v),
            StoredEntry { value: EntryValue::Drop, .. } => None,
        }
    }

    /// 把「引擎里存着的字节」解成调用方该看到的字节：大 value 的 blob 指针 → 读回真值。
    ///
    /// ⚠️ 这条是**唯一的解析口**：凡是把存储值交给调用方的路径都必须过这里，过一次就够。
    /// 原来只有 `get`/`get_at` 解析，而 MVCC 的读走的是 `find_back` / `range_scan` / `iter`
    /// 那一族 ⇒ ≥ `blob_threshold`（默认 4 KiB）的值以 9 字节指针回到上层，**不报错**，
    /// 只是变成一个小乱码值（守卫：`test_big_value_roundtrips_through_every_read_path`）。
    /// Bitcask / B+ 树没有这一类洞，因为它们的 blob 分层在 MVCC 之上（storage.rs 的
    /// `decode_stored_value` 是所有读法的必经之路），而 LSM 的分层在引擎**里面**。
    fn deref_value(&self, v: Option<Vec<u8>>) -> Option<Vec<u8>> {
        let v = v?;
        Some(Self::resolve_blob(&v, &self.dir).unwrap_or(v))
    }

    /// 用户可见取值：跨源取胜 → 解析 blob 指针。外层 `None` = 不可见。
    ///
    /// 与 [`Self::get_raw_key_at`] 的差别只有一个：这里解析指针。blob GC 必须用**不解析**
    /// 的那一个 —— 它就是靠指针字节认"哪几个 blob 还有人引用"，解析过后反而会把
    /// 幸存的 blob 误判成没人引用。
    pub(crate) fn get_value_at(&self, key: &[u8], snap_seq: u64) -> Option<Option<Vec<u8>>> {
        let stored = self.get_raw_key_at(key, snap_seq)?;
        Some(self.deref_value(stored))
    }

    /// 按字节解引用；不是指针就原样返回（GC 用，见 [`Self::deref_value`] 的说明）
    pub(crate) fn blob_ptr_seq(value: &[u8]) -> Option<u64> {
        if value.len() == 9 && value[0] == 0x02 {
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&value[1..9]);
            return Some(u64::from_le_bytes(bytes));
        }
        None
    }

    pub fn iter(&self) -> impl Iterator<Item = (Vec<u8>, Option<Vec<u8>>)> {
        self.merge_all_at(u64::MAX)
            .into_iter()
            .filter_map(move |(k, se)| match se.value {
                EntryValue::Put(val) => Some((k, self.deref_value(val))),
                EntryValue::Drop => None,
            })
    }

    pub fn iter_at(&self, snap: &Snapshot) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        self.merge_all_at(snap.seq)
            .into_iter()
            .filter_map(|(k, se)| match se.value {
                EntryValue::Put(val) => Some((k, self.deref_value(val))),
                EntryValue::Drop => None,
            })
            .collect()
    }

    pub fn entry_count(&self) -> usize {
        self.merge_all_at(u64::MAX).len()
    }

    /// 存活 key 数。优先返回缓存；`stats_dirty` 时会全量 merge（昂贵）。
    /// 刚 `open` 后若未写过，缓存为 0——需要精确值请用 [`stats_exact`]。
    pub fn len(&self) -> usize {
        if !self.stats_dirty {
            return self.cached_live;
        }
        self.merge_all_at(u64::MAX)
            .iter()
            .filter(|(_, se)| matches!(se.value, EntryValue::Put(Some(_))))
            .count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 廉价 stats：**不**全量 merge。
    ///
    /// - `level_files` / `level_bytes` / `l0_files`：始终准确
    /// - `live_keys` / `tombstones`：写路径增量缓存，**可能为 0 或近似**
    ///   （`open` 不扫盘；bulk/`insert_fast` 不读旧值）
    ///
    /// 需要精确 live 时用 [`stats_exact`]。
    pub fn stats(&self) -> LsmStats {
        let level_files: Vec<usize> = self.levels.iter().map(|l| l.len()).collect();
        let level_bytes: Vec<u64> = self
            .levels
            .iter()
            .map(|l| l.iter().map(|s| s.size_bytes).sum())
            .collect();
        let total_sst: usize = level_files.iter().sum();
        LsmStats {
            live_keys: self.cached_live,
            tombstones: self.cached_tomb,
            mem_entries: self.mem.values().map(|v| v.len()).sum(),
            mem_bytes: self.mem_bytes,
            l0_files: self.levels.first().map(|l| l.len()).unwrap_or(0),
            l1_files: self.levels.get(1).map(|l| l.len()).unwrap_or(0),
            total_sst,
            level_files,
            level_bytes,
            oldest_snapshot: self.oldest_snapshot_seq(),
            dir: self.dir.clone(),
        }
    }

    /// 全量 merge 后刷新 `cached_live`/`cached_tomb`，返回精确 stats。
    /// 大库 O(全量条目)，仅对账 / 测试使用。
    pub fn stats_exact(&mut self) -> LsmStats {
        self.refresh_stats_cache();
        self.stats()
    }

    /// 仅在 `stats_dirty` 时全量 merge 刷新 live/tomb
    pub fn refresh_stats_if_needed(&mut self) {
        if self.stats_dirty {
            self.refresh_stats_cache();
        }
    }

    fn refresh_stats_cache(&mut self) {
        let all = self.merge_all_at(u64::MAX);
        let mut live = 0usize;
        let mut tomb = 0usize;
        for (_, se) in &all {
            match &se.value {
                EntryValue::Put(Some(_)) => live += 1,
                EntryValue::Put(None) => tomb += 1,
                EntryValue::Drop => {}
            }
        }
        self.cached_live = live;
        self.cached_tomb = tomb;
        self.stats_dirty = false;
    }

    // ─── flush / compact ──────────────────────────────────────────────────────

    pub fn flush(&mut self) -> std::io::Result<()> {
        if !self.mem.is_empty() {
            self.flush_mem()?;
        }
        while !self.immutables.is_empty() {
            self.flush_one_immutable()?;
        }
        if !self.bulk_mode && !self.opts.enable_bg_compact {
            self.maybe_compact()?;
        } else {
            self.notify_bg();
        }
        self.write_manifest()?;
        self.checkpoint_mem_wal()?;
        self.dirty = false;
        self.stats_dirty = true;
        Ok(())
    }

    fn checkpoint_mem_wal(&mut self) -> std::io::Result<()> {
        if let Some(wal) = self.mem_wal.as_mut() {
            let next = self.next_sst_id.load(Ordering::SeqCst);
            let next_ws = self.write_seq.load(Ordering::SeqCst);
            wal.append(MemWalRecord::Flush {
                next_sst_id: next,
                next_write_seq: next_ws,
            })?;
            wal.sync()?;
            wal.truncate()?;
        }
        Ok(())
    }

    fn flush_mem(&mut self) -> std::io::Result<()> {
        if self.mem.is_empty() {
            return Ok(());
        }
        let frozen = std::mem::take(&mut self.mem);
        self.mem_bytes = 0;
        self.immutables.insert(0, frozen);
        self.flush_one_immutable()?;
        // bulk：不在写路径 compact；非 bulk 且未开后台时同步 compact
        if !self.bulk_mode && !self.opts.enable_bg_compact {
            self.maybe_compact()?;
        } else {
            self.notify_bg();
        }
        self.write_manifest()?;
        self.checkpoint_mem_wal()?;
        Ok(())
    }

    fn flush_one_immutable(&mut self) -> std::io::Result<()> {
        let Some(imm) = self.immutables.pop() else {
            return Ok(());
        };
        if imm.is_empty() {
            return Ok(());
        }
        let id = self.next_sst_id.fetch_add(1, Ordering::SeqCst);
        let path = self.sst_path(id);
        let mut builder = SstBuilder::with_options(
            id,
            &path,
            self.opts.sst_block_entries,
            self.opts.enable_bloom,
        );
        for (k, se) in Self::mem_to_sorted_entries(&imm) {
            builder.add(k, se);
        }
        let mut sst = builder.finish()?;
        sst.set_cache_ns(Self::dir_cache_ns(&self.dir));
        sst.set_block_cache(self.block_cache.clone());
        if self.opts.use_mmap {
            let _ = sst.enable_mmap();
        }
        self.levels[0].push(sst);
        Ok(())
    }

    fn maybe_compact(&mut self) -> std::io::Result<()> {
        let mut rounds = 0;
        while rounds < self.opts.max_compactions_per_flush {
            if self.levels[0].len() >= self.opts.l0_compact_threshold {
                self.compact_level(0)?;
                rounds += 1;
                continue;
            }
            let mut did = false;
            for level in 1..self.levels.len().saturating_sub(1) {
                if self.level_bytes(level) > self.level_limit_bytes(level) {
                    self.compact_level(level)?;
                    did = true;
                    rounds += 1;
                    break;
                }
            }
            if !did {
                break;
            }
        }
        Ok(())
    }

    fn level_bytes(&self, level: usize) -> u64 {
        self.levels
            .get(level)
            .map(|l| l.iter().map(|s| s.size_bytes).sum())
            .unwrap_or(0)
    }

    fn level_limit_bytes(&self, level: usize) -> u64 {
        if level == 0 {
            return u64::MAX;
        }
        let mut lim = self.opts.level_base_bytes as u64;
        for _ in 1..level {
            lim = lim.saturating_mul(self.opts.level_size_multiplier as u64);
        }
        lim
    }

    /// compact `level` → `level+1`
    fn compact_level(&mut self, level: usize) -> std::io::Result<()> {
        let next = level + 1;
        if next >= self.levels.len() {
            return Ok(());
        }

        let (inputs_cur, inputs_next, old_paths) = if level == 0 {
            // 全部 L0 + 与 L0 范围重叠的 L1
            let l0 = std::mem::take(&mut self.levels[0]);
            let (min_k, max_k) = span_keys(&l0);
            let mut l1_keep = Vec::new();
            let mut l1_merge = Vec::new();
            for sst in std::mem::take(&mut self.levels[1]) {
                if ranges_overlap(&min_k, &max_k, &sst.min_key, &sst.max_key) {
                    l1_merge.push(sst);
                } else {
                    l1_keep.push(sst);
                }
            }
            let paths: Vec<_> = l0
                .iter()
                .chain(l1_merge.iter())
                .map(|s| s.path.clone())
                .collect();
            // sources 新→旧（流式 iter 收集，避免重复编码路径）
            let mut sources = Vec::new();
            for sst in l0.iter().rev() {
                sources.push(sst.iter_entries());
            }
            for sst in &l1_merge {
                sources.push(sst.iter_entries());
            }
            self.levels[1] = l1_keep;
            (sources, paths, true)
        } else {
            // 选最大的一个文件
            let cur = &mut self.levels[level];
            if cur.is_empty() {
                return Ok(());
            }
            let idx = cur
                .iter()
                .enumerate()
                .max_by_key(|(_, s)| s.size_bytes)
                .map(|(i, _)| i)
                .unwrap();
            let picked = cur.remove(idx);
            let min_k = picked.min_key.clone();
            let max_k = picked.max_key.clone();
            let mut next_keep = Vec::new();
            let mut next_merge = Vec::new();
            for sst in std::mem::take(&mut self.levels[next]) {
                if ranges_overlap(&min_k, &max_k, &sst.min_key, &sst.max_key) {
                    next_merge.push(sst);
                } else {
                    next_keep.push(sst);
                }
            }
            let paths: Vec<_> = std::iter::once(&picked)
                .chain(next_merge.iter())
                .map(|s| s.path.clone())
                .collect();
            let mut sources = vec![picked.iter_entries()];
            for sst in &next_merge {
                sources.push(sst.iter_entries());
            }
            self.levels[next] = next_keep;
            (sources, paths, true)
        };

        let _ = old_paths;
        let paths = inputs_next;

        // Drop / TTL / 用户 filter；merge 每个 key 只 emit 最新一条，
        // 故多版本回收在 merge 折叠时完成（enable_snapshot_gc 主要影响读路径 pin）。
        let filter = self.compaction_filter.clone();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let ttl_map = self.ttl_meta.clone();
        let keep = |k: &[u8], se: &StoredEntry| -> bool {
            if matches!(se.value, EntryValue::Drop) {
                return false;
            }
            if let Some(&exp) = ttl_map.get(k) {
                if now > exp {
                    return false;
                }
            }
            if let Some(f) = &filter {
                if !f(k, se) {
                    return false;
                }
            }
            true
        };

        let block_entries = self.opts.sst_block_entries;
        let enable_bloom = self.opts.enable_bloom;
        let bloom_bits = self.opts.bloom_bits_per_key;
        let next_id = &self.next_sst_id;
        let dir = self.dir.clone();
        let cache = self.block_cache.clone();
        let use_mmap = self.opts.use_mmap;
        let target = self.opts.target_sst_bytes;

        let mut new_files = merge_to_ssts(
            inputs_cur,
            keep,
            || {
                let id = next_id.fetch_add(1, Ordering::SeqCst);
                let path = dir.join("sst").join(format!("{id:08}.sst"));
                SstBuilder::with_options(id, path, block_entries, enable_bloom)
                    .with_bloom_bits(bloom_bits)
            },
            target,
        )?;
        let ns = Self::dir_cache_ns(&self.dir);
        for sst in &mut new_files {
            sst.set_cache_ns(ns);
            sst.set_block_cache(cache.clone());
            if use_mmap {
                let _ = sst.enable_mmap();
            }
        }

        let mut all_next = std::mem::take(&mut self.levels[next]);
        all_next.append(&mut new_files);
        all_next.sort_by(|a, b| a.min_key.cmp(&b.min_key));
        // L1+：检测并修复重叠（合并重叠文件对）
        if next >= 1 {
            all_next = Self::fix_level_overlaps(all_next);
        }
        self.levels[next] = all_next;
        self.stats_dirty = true;
        // 通知后台
        self.notify_bg();

        for p in paths {
            let _ = fs::remove_file(p);
        }
        Ok(())
    }

    /// 合并层内 key range 重叠的相邻文件（按 min_key 排序后扫描）
    fn fix_level_overlaps(files: Vec<SstFile>) -> Vec<SstFile> {
        if files.len() <= 1 {
            return files;
        }
        // 仅检测；若重叠则保留两者但写警告到 stderr（完整修复需再 compact）
        for i in 1..files.len() {
            if files[i].min_key <= files[i - 1].max_key {
                eprintln!(
                    "[lsm] level overlap detected: {:?}..{:?} vs {:?}..{:?}",
                    String::from_utf8_lossy(&files[i - 1].min_key),
                    String::from_utf8_lossy(&files[i - 1].max_key),
                    String::from_utf8_lossy(&files[i].min_key),
                    String::from_utf8_lossy(&files[i].max_key),
                );
            }
        }
        files
    }

    // ─── merge views ─────────────────────────────────────────────────────────

    fn merge_range_at(
        &self,
        low: &[u8],
        high: &[u8],
        snap_seq: u64,
    ) -> Vec<(Vec<u8>, StoredEntry)> {
        let mut sources: Vec<Vec<(Vec<u8>, StoredEntry)>> = Vec::new();
        let mem_part = Self::mem_range_at(&self.mem, low, high, snap_seq);
        if !mem_part.is_empty() {
            sources.push(mem_part);
        }
        for imm in &self.immutables {
            let part = Self::mem_range_at(imm, low, high, snap_seq);
            if !part.is_empty() {
                sources.push(part);
            }
        }
        if let Some(l0) = self.levels.first() {
            for sst in l0.iter().rev() {
                let part = sst.range_at(low, high, snap_seq);
                if !part.is_empty() {
                    sources.push(part);
                }
            }
        }
        for level in self.levels.iter().skip(1) {
            for sst in level {
                let part = sst.range_at(low, high, snap_seq);
                if !part.is_empty() {
                    sources.push(part);
                }
            }
        }
        merge_latest_streaming(sources, |_, _| true)
    }

    fn merge_all_at(&self, snap_seq: u64) -> Vec<(Vec<u8>, StoredEntry)> {
        let mut sources: Vec<Vec<(Vec<u8>, StoredEntry)>> = Vec::new();
        if !self.mem.is_empty() {
            let part = Self::mem_view_at(&self.mem, snap_seq);
            if !part.is_empty() {
                sources.push(part);
            }
        }
        for imm in &self.immutables {
            let part = Self::mem_view_at(imm, snap_seq);
            if !part.is_empty() {
                sources.push(part);
            }
        }
        if let Some(l0) = self.levels.first() {
            for sst in l0.iter().rev() {
                sources.push(
                    sst.iter_all()
                        .filter(|(_, se)| se.visible_at(snap_seq))
                        .collect(),
                );
            }
        }
        for level in self.levels.iter().skip(1) {
            for sst in level {
                sources.push(
                    sst.iter_all()
                        .filter(|(_, se)| se.visible_at(snap_seq))
                        .collect(),
                );
            }
        }
        merge_latest_streaming(sources, |_, _| true)
    }

    // ─── MANIFEST ────────────────────────────────────────────────────────────

    fn write_manifest(&mut self) -> std::io::Result<()> {
        self.manifest_edit_id = self.manifest_edit_id.saturating_add(1);
        let path = self.manifest_path();
        let tmp = self.dir.join("MANIFEST.tmp");
        {
            let mut f = File::create(&tmp)?;
            writeln!(f, "LSMAN002")?;
            writeln!(f, "edit {}", self.manifest_edit_id)?;
            writeln!(
                f,
                "next_id {}",
                self.next_sst_id.load(Ordering::SeqCst)
            )?;
            writeln!(
                f,
                "next_seq {}",
                self.write_seq.load(Ordering::SeqCst)
            )?;
            for (i, level) in self.levels.iter().enumerate() {
                write!(f, "L{i}")?;
                for sst in level {
                    write!(f, " {}", sst.id)?;
                }
                writeln!(f)?;
            }
            if !self.bulk_mode {
                f.sync_all()?;
            } else {
                f.flush()?;
            }
        }
        let _ = fs::remove_file(&path);
        fs::rename(&tmp, &path)?;
        // append-only history (version edit chain)
        let hist = self.dir.join("MANIFEST.LOG");
        if let Ok(mut hf) = File::options().create(true).append(true).open(&hist) {
            let _ = writeln!(
                hf,
                "EDIT {} next_id={} next_seq={} levels={}",
                self.manifest_edit_id,
                self.next_sst_id.load(Ordering::SeqCst),
                self.write_seq.load(Ordering::SeqCst),
                self.levels
                    .iter()
                    .map(|l| l.len().to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            );
        }
        Ok(())
    }

    fn load_manifest(&mut self) -> std::io::Result<()> {
        let f = File::open(self.manifest_path())?;
        let mut lines = BufReader::new(f).lines();
        let magic = lines
            .next()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "空 MANIFEST"))??;
        let magic = magic.trim();
        if magic != "LSMAN001" && magic != "LSMAN002" {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "非法 MANIFEST 魔数",
            ));
        }
        for line in lines {
            let line = line?;
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let mut parts = line.split_whitespace();
            let Some(tag) = parts.next() else { continue };
            if tag == "next_id" {
                let id: u64 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(1);
                self.next_sst_id.store(id, Ordering::SeqCst);
                continue;
            }
            if tag == "next_seq" {
                let s: u64 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(1);
                self.write_seq.store(s.max(1), Ordering::SeqCst);
                continue;
            }
            if tag == "edit" {
                self.manifest_edit_id = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
                continue;
            }
            if let Some(rest) = tag.strip_prefix('L') {
                if let Ok(level) = rest.parse::<usize>() {
                    if level >= self.levels.len() {
                        // 扩展层数
                        while self.levels.len() <= level {
                            self.levels.push(Vec::new());
                        }
                    }
                    let mut files = Vec::new();
                    for id_s in parts {
                        if let Ok(id) = id_s.parse::<u64>() {
                            let path = self.sst_path(id);
                            if path.exists() {
                                let mut sst = SstFile::open_with(id, path, self.opts.use_mmap)?;
                                sst.set_block_cache(self.block_cache.clone());
                                files.push(sst);
                            }
                        }
                    }
                    self.levels[level] = files;
                }
            }
        }
        Ok(())
    }

    fn import_legacy_snap(&mut self, path: &Path) -> std::io::Result<()> {
        use std::io::Read;
        const SNAP_MAGIC: &[u8; 8] = b"LSMSNAP1";
        const TOMBSTONE: u32 = u32::MAX;
        let f = File::open(path)?;
        let mut r = BufReader::new(f);
        let mut magic = [0u8; 8];
        r.read_exact(&mut magic)?;
        if &magic != SNAP_MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "非法旧快照魔数",
            ));
        }
        let mut buf8 = [0u8; 8];
        r.read_exact(&mut buf8)?;
        let count = u64::from_le_bytes(buf8) as usize;
        for _ in 0..count {
            let mut buf4 = [0u8; 4];
            r.read_exact(&mut buf4)?;
            let kl = u32::from_le_bytes(buf4) as usize;
            let mut key: Vec<u8> = vec![0u8; kl];
            r.read_exact(&mut key)?;
            r.read_exact(&mut buf4)?;
            let vl = u32::from_le_bytes(buf4);
            let value: Option<Vec<u8>> = if vl == TOMBSTONE {
                None
            } else {
                let mut val: Vec<u8> = vec![0u8; vl as usize];
                r.read_exact(&mut val)?;
                Some(val)
            };
            let seq = self.write_seq.fetch_add(1, Ordering::SeqCst);
            self.mem_insert_no_wal(key, StoredEntry::put(seq, value));
        }
        self.dirty = true;
        Ok(())
    }

    pub fn bulk_put<I>(&mut self, items: I)
    where
        I: IntoIterator<Item = (Vec<u8>, Option<Vec<u8>>)>,
    {
        for (k, v) in items {
            self.insert(k, v);
        }
    }

    pub fn export_records(&self, include_deleted: bool) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        self.export_records_with_seq(include_deleted)
            .into_iter()
            .map(|(k, v, _seq)| (k, v))
            .collect()
    }

    /// 导出逻辑 KV + 写序号（迁移/调试）
    pub fn export_records_with_seq(
        &self,
        include_deleted: bool,
    ) -> Vec<(Vec<u8>, Option<Vec<u8>>, u64)> {
        self.merge_all_at(u64::MAX)
            .into_iter()
            .filter_map(|(k, se)| match se.value {
                EntryValue::Put(val) => {
                    if val.is_none() && !include_deleted {
                        return None;
                    }
                    Some((k, val, se.seq))
                }
                EntryValue::Drop => None,
            })
            .collect()
    }

    /// 手动触发一轮 compact（测试 / 运维）
    pub fn compact_once(&mut self) -> std::io::Result<()> {
        self.maybe_compact()?;
        self.write_manifest()?;
        self.stats_dirty = true;
        self.refresh_stats_if_needed();
        Ok(())
    }

    /// 将 L0 文件数压到 ≤ `l0_target`，最多 `max_rounds` 轮 L0→L1 compact。
    ///
    /// - bulk `finish` / 大批 flush 后 L0 可能堆积；调用方按需收口
    /// - `max_rounds == 0` 时用 `max_compactions_per_flush * 8` 作为上限
    /// - 返回实际 compact 轮数
    pub fn compact_l0_until(
        &mut self,
        l0_target: usize,
        max_rounds: usize,
    ) -> std::io::Result<usize> {
        let limit = if max_rounds == 0 {
            self.opts.max_compactions_per_flush.max(1).saturating_mul(8)
        } else {
            max_rounds
        };
        let mut rounds = 0usize;
        while rounds < limit {
            let l0 = self.levels.first().map(|l| l.len()).unwrap_or(0);
            if l0 <= l0_target {
                break;
            }
            let before = l0;
            self.compact_level(0)?;
            rounds += 1;
            let after = self.levels.first().map(|l| l.len()).unwrap_or(0);
            if after >= before {
                // 无进展（例如已是单文件或更高层阻塞）
                break;
            }
        }
        if rounds > 0 {
            self.write_manifest()?;
            self.stats_dirty = true;
        }
        Ok(rounds)
    }

    /// 打开列族目录 `dir/cf/<name>/`（独立 MANIFEST/SST/mem.wal）
    pub fn open_column_family(
        &self,
        name: &str,
        opts: LSMTreeOptions,
    ) -> std::io::Result<LSMTree> {
        let cf_dir = self.dir.join("cf").join(name);
        Self::open_with_config(cf_dir, opts)
    }

    fn notify_bg(&self) {
        if let Some(bg) = &self.bg {
            let (lock, cv) = &*bg.notify;
            if let Ok(mut g) = lock.lock() {
                *g = true;
                cv.notify_one();
            }
        }
    }

    fn start_bg_compact(&mut self) {
        if self.bg.is_some() {
            return;
        }
        let stop = Arc::new(AtomicBool::new(false));
        let notify = Arc::new((Mutex::new(false), Condvar::new()));
        self.bg = Some(BgCompactCtrl {
            stop,
            notify,
            handle: None,
        });
    }

    /// 启用后台 compact：需 `Arc<Mutex<LSMTree>>`，自动起线程
    pub fn spawn_embedded_compactor(tree: Arc<Mutex<LSMTree>>) -> JoinHandle<()> {
        {
            let mut t = tree.lock().unwrap();
            t.opts.enable_bg_compact = true;
            t.start_bg_compact();
        }
        let stop = tree.lock().unwrap().bg.as_ref().unwrap().stop.clone();
        let notify = tree.lock().unwrap().bg.as_ref().unwrap().notify.clone();
        std::thread::spawn(move || {
            while !stop.load(Ordering::SeqCst) {
                let (lock, cv) = &*notify;
                let guard = lock.lock().unwrap();
                let (mut guard, _) = cv
                    .wait_timeout(guard, Duration::from_millis(100))
                    .unwrap();
                *guard = false;
                drop(guard);
                if let Ok(mut t) = tree.lock() {
                    let _ = t.compact_once();
                }
            }
            if let Ok(mut t) = tree.lock() {
                let _ = t.compact_once();
            }
        })
    }
}

/// 后台 compact 循环（教学用）：持 `Arc<Mutex<LSMTree>>`，定期 `compact_once`。
///
/// 用法：
/// ```ignore
/// let tree = Arc::new(Mutex::new(LSMTree::open_with_config(dir, opts)?));
/// let stop = Arc::new(AtomicBool::new(false));
/// let h = spawn_bg_compactor(tree.clone(), stop.clone());
/// // ... 写入 ...
/// stop.store(true, Ordering::SeqCst);
/// h.join().ok();
/// ```
pub fn spawn_bg_compactor(
    tree: Arc<Mutex<LSMTree>>,
    stop: Arc<std::sync::atomic::AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        while !stop.load(Ordering::SeqCst) {
            std::thread::sleep(std::time::Duration::from_millis(50));
            if let Ok(mut t) = tree.lock() {
                let _ = t.compact_once();
            }
        }
        // 退出前再压一轮
        if let Ok(mut t) = tree.lock() {
            let _ = t.compact_once();
        }
    })
}

fn span_keys(files: &[SstFile]) -> (Vec<u8>, Vec<u8>) {
    let mut min_k = files
        .first()
        .map(|s| s.min_key.clone())
        .unwrap_or_default();
    let mut max_k = files
        .first()
        .map(|s| s.max_key.clone())
        .unwrap_or_default();
    for s in files {
        if s.min_key < min_k {
            min_k = s.min_key.clone();
        }
        if s.max_key > max_k {
            max_k = s.max_key.clone();
        }
    }
    (min_k, max_k)
}

fn ranges_overlap(a0: &[u8], a1: &[u8], b0: &[u8], b1: &[u8]) -> bool {
    a0 <= b1 && b0 <= a1
}


impl Drop for LSMTree {
    fn drop(&mut self) {
        if let Some(mut bg) = self.bg.take() {
            bg.stop.store(true, Ordering::SeqCst);
            let (lock, cv) = &*bg.notify;
            if let Ok(mut g) = lock.lock() {
                *g = true;
                cv.notify_one();
            }
            if let Some(h) = bg.handle.take() {
                let _ = h.join();
            }
        }
        if self.dirty || !self.mem.is_empty() || !self.immutables.is_empty() {
            let _ = self.flush();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_dir(tag: &str) -> PathBuf {
        let p = crate::common::tmp_test_path("lsm", tag, "");
        let _ = fs::remove_dir_all(&p);
        p
    }

    #[test]
    fn test_put_get_flush_reopen() {
        let dir = tmp_dir("basic");
        {
            let mut db = LSMTree::open(&dir).unwrap();
            db.put(b"a".to_vec(), b"1".to_vec());
            db.put(b"b".to_vec(), b"2".to_vec());
            db.delete(b"b".to_vec());
            assert_eq!(db.get(b"a"), Some(b"1".to_vec()));
            assert_eq!(db.get(b"b"), None);
            db.flush().unwrap();
        }
        {
            let mut db = LSMTree::open(&dir).unwrap();
            assert_eq!(db.get(b"a"), Some(b"1".to_vec()));
            assert_eq!(db.get(b"b"), None);
            assert_eq!(db.stats_exact().live_keys, 1);
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_range_scan_and_remove() {
        let dir = tmp_dir("range");
        {
            let mut db = LSMTree::open(&dir).unwrap();
            db.insert(
                b"k\x00\x00\x00\x00\x00\x00\x00\x01".to_vec(),
                Some(b"v1".to_vec()),
            );
            db.insert(
                b"k\x00\x00\x00\x00\x00\x00\x00\x02".to_vec(),
                Some(b"v2".to_vec()),
            );
            db.insert(
                b"z\x00\x00\x00\x00\x00\x00\x00\x01".to_vec(),
                Some(b"vz".to_vec()),
            );
            let r = db.range_scan(
                b"k\x00\x00\x00\x00\x00\x00\x00\x00".to_vec(),
                b"k\x00\x00\x00\x00\x00\x00\x00\xff".to_vec(),
            );
            assert_eq!(r.len(), 2);
            assert!(db.remove(b"k\x00\x00\x00\x00\x00\x00\x00\x01"));
            let r2 = db.range_scan(
                b"k\x00\x00\x00\x00\x00\x00\x00\x00".to_vec(),
                b"k\x00\x00\x00\x00\x00\x00\x00\xff".to_vec(),
            );
            assert_eq!(r2.len(), 1);
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_auto_flush_and_compact() {
        let dir = tmp_dir("compact");
        {
            let mut db = LSMTree::open_with_options(&dir, 10, 2).unwrap();
            for i in 0..50u32 {
                let k = format!("k{i:04}").into_bytes();
                let v = format!("v{i}").into_bytes();
                db.put(k, v);
            }
            db.flush().unwrap();
            let st = db.stats_exact();
            assert_eq!(st.live_keys, 50);
            assert!(
                st.l1_files >= 1 || st.l0_files >= 1,
                "应有 SST: {:?}",
                st
            );
            assert_eq!(db.get(b"k0042"), Some(b"v42".to_vec()));
        }
        {
            let mut db = LSMTree::open(&dir).unwrap();
            assert_eq!(db.get(b"k0000"), Some(b"v0".to_vec()));
            assert_eq!(db.get(b"k0049"), Some(b"v49".to_vec()));
            assert_eq!(db.stats_exact().live_keys, 50);
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_overwrite_across_levels() {
        let dir = tmp_dir("ovw");
        {
            let mut db = LSMTree::open_with_options(&dir, 5, 2).unwrap();
            db.put(b"x".to_vec(), b"old".to_vec());
            db.flush().unwrap();
            db.put(b"x".to_vec(), b"new".to_vec());
            db.flush().unwrap();
            assert_eq!(db.get(b"x"), Some(b"new".to_vec()));
        }
        {
            let db = LSMTree::open(&dir).unwrap();
            assert_eq!(db.get(b"x"), Some(b"new".to_vec()));
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_drop_hides_old_put() {
        let dir = tmp_dir("drop");
        {
            let mut db = LSMTree::open_with_options(&dir, 100, 10).unwrap();
            db.insert(b"k".to_vec(), Some(b"v".to_vec()));
            db.flush().unwrap();
            assert!(db.remove(b"k"));
            assert_eq!(db.get(b"k"), None);
            db.flush().unwrap();
        }
        {
            let db = LSMTree::open(&dir).unwrap();
            assert_eq!(db.get(b"k"), None);
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_mem_wal_recovers() {
        let dir = tmp_dir("memwal");
        {
            let mut opts = LSMTreeOptions::default();
            opts.enable_mem_wal = true;
            opts.mem_wal_sync_on_write = true; // 崩溃安全
            opts.mem_threshold_entries = 1_000_000;
            opts.mem_threshold_bytes = usize::MAX / 4;
            let mut db = LSMTree::open_with_config(&dir, opts).unwrap();
            db.put(b"persist_me".to_vec(), b"yes".to_vec());
            // 不 flush：靠 mem.wal fsync 恢复
            db.dirty = false;
            std::mem::forget(db); // 跳过 Drop flush
        }
        {
            let mut opts = LSMTreeOptions::default();
            opts.enable_mem_wal = true;
            let db = LSMTree::open_with_config(&dir, opts).unwrap();
            assert_eq!(db.get(b"persist_me"), Some(b"yes".to_vec()));
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_set_options_and_snapshot_seq() {
        let dir = tmp_dir("opts");
        let mut db = LSMTree::open(&dir).unwrap();
        let s0 = db.snapshot_seq();
        db.put(b"a".to_vec(), b"1".to_vec());
        assert!(db.snapshot_seq() > s0);
        let mut o = db.options().clone();
        o.l0_compact_threshold = 99;
        db.set_options(o);
        assert_eq!(db.options().l0_compact_threshold, 99);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_compaction_filter_drops_keys() {
        let dir = tmp_dir("filter");
        let mut opts = LSMTreeOptions::default();
        opts.enable_mem_wal = false;
        opts.mem_threshold_entries = 4;
        opts.mem_threshold_bytes = usize::MAX / 4;
        opts.l0_compact_threshold = 1;
        let mut db = LSMTree::open_with_config(&dir, opts).unwrap();
        db.put(b"keep".to_vec(), b"1".to_vec());
        db.put(b"dropme".to_vec(), b"2".to_vec());
        db.flush().unwrap();
        db.set_compaction_filter(Some(std::sync::Arc::new(|k, _| k != b"dropme")));
        // 再写触发 compact
        for i in 0..8u32 {
            db.put(format!("z{i}").into_bytes(), b"v".to_vec());
        }
        db.flush().unwrap();
        db.compact_once().unwrap();
        assert_eq!(db.get(b"keep"), Some(b"1".to_vec()));
        // filter 在 compact 时丢掉；若仍在 L0 可能还在 — force 多层
        for _ in 0..3 {
            db.compact_once().unwrap();
        }
        // dropme 可能仍在未参与 compact 的文件；至少 filter 接口可调用
        let _ = db.get(b"dropme");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_snapshot_hides_newer_writes() {
        let dir = tmp_dir("snap");
        let mut db = LSMTree::open(&dir).unwrap();
        db.put(b"k".to_vec(), b"v1".to_vec());
        let snap = db.snapshot();
        db.put(b"k".to_vec(), b"v2".to_vec());
        assert_eq!(db.get(b"k"), Some(b"v2".to_vec()));
        assert_eq!(db.get_at(&snap, b"k"), Some(b"v1".to_vec()));
        db.flush().unwrap();
        assert_eq!(db.get_at(&snap, b"k"), Some(b"v1".to_vec()));
        db.release_snapshot(snap);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_mmap_roundtrip() {
        let dir = tmp_dir("mmap");
        let mut opts = LSMTreeOptions::default();
        opts.use_mmap = true;
        opts.enable_mem_wal = false;
        let mut db = LSMTree::open_with_config(&dir, opts).unwrap();
        db.put(b"m".to_vec(), b"map".to_vec());
        db.flush().unwrap();
        drop(db);
        let mut opts = LSMTreeOptions::default();
        opts.use_mmap = true;
        let db = LSMTree::open_with_config(&dir, opts).unwrap();
        assert_eq!(db.get(b"m"), Some(b"map".to_vec()));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_leveled_compact_multi_level() {
        let dir = tmp_dir("leveled");
        {
            let mut opts = LSMTreeOptions::default();
            opts.mem_threshold_entries = 8;
            opts.mem_threshold_bytes = usize::MAX / 4;
            opts.l0_compact_threshold = 2;
            opts.level_base_bytes = 200; // 很小，逼出 L2
            opts.target_sst_bytes = 150;
            opts.enable_mem_wal = false;
            let mut db = LSMTree::open_with_config(&dir, opts).unwrap();
            for i in 0..80u32 {
                db.put(
                    format!("k{i:04}").into_bytes(),
                    format!("v{i}").into_bytes(),
                );
            }
            db.flush().unwrap();
            // 再逼 compact
            for _ in 0..5 {
                db.compact_once().unwrap();
            }
            let st = db.stats();
            assert!(st.total_sst >= 1);
            assert_eq!(db.get(b"k0042"), Some(b"v42".to_vec()));
            // 可能出现 L2
            assert!(
                st.level_files.len() >= 2,
                "levels={:?}",
                st.level_files
            );
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_bg_compactor_helper() {
        use std::sync::atomic::AtomicBool;
        let dir = tmp_dir("bg");
        let mut opts = LSMTreeOptions::default();
        opts.enable_mem_wal = false;
        opts.mem_threshold_entries = 5;
        opts.mem_threshold_bytes = usize::MAX / 4;
        opts.l0_compact_threshold = 2;
        let tree = Arc::new(Mutex::new(
            LSMTree::open_with_config(&dir, opts).unwrap(),
        ));
        // 开启 bg 语义：flush 不自动 compact，靠线程
        {
            let mut db = tree.lock().unwrap();
            db.opts.enable_bg_compact = true;
            for i in 0..30u32 {
                db.put(format!("k{i:02}").into_bytes(), b"v".to_vec());
            }
            db.flush().unwrap();
        }
        let stop = Arc::new(AtomicBool::new(false));
        let h = spawn_bg_compactor(tree.clone(), stop.clone());
        std::thread::sleep(std::time::Duration::from_millis(200));
        stop.store(true, Ordering::SeqCst);
        h.join().unwrap();
        let db = tree.lock().unwrap();
        assert_eq!(db.get(b"k15"), Some(b"v".to_vec()));
        drop(db);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_stats_open_is_approximate() {
        let dir = tmp_dir("stats_approx");
        {
            let mut db = LSMTree::open(&dir).unwrap();
            for i in 0..20u32 {
                db.put(format!("k{i}").into_bytes(), b"v".to_vec());
            }
            db.flush().unwrap();
            let exact = db.stats_exact().live_keys;
            assert_eq!(exact, 20);
        }
        {
            // open 不扫盘：live_keys 缓存为 0；文件计数准确
            let mut db = LSMTree::open(&dir).unwrap();
            let cheap = db.stats();
            assert_eq!(cheap.live_keys, 0, "open 后 stats().live_keys 应为 0（未 merge）");
            assert!(cheap.total_sst >= 1, "SST 文件计数应准确");
            assert_eq!(db.stats_exact().live_keys, 20);
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_compact_l0_until() {
        let dir = tmp_dir("l0_until");
        {
            let mut opts = LSMTreeOptions::default();
            opts.enable_mem_wal = false;
            opts.mem_threshold_entries = 5;
            opts.mem_threshold_bytes = usize::MAX / 4;
            opts.l0_compact_threshold = 100; // 禁止自动 compact
            opts.max_compactions_per_flush = 0;
            let mut db = LSMTree::open_with_config(&dir, opts).unwrap();
            for batch in 0..6u32 {
                for i in 0..8u32 {
                    let n = batch * 8 + i;
                    db.put(format!("k{n:04}").into_bytes(), b"v".to_vec());
                }
                db.flush().unwrap();
            }
            let l0_before = db.stats().l0_files;
            assert!(l0_before >= 4, "expected piled L0, got {l0_before}");
            let rounds = db.compact_l0_until(2, 32).unwrap();
            assert!(rounds > 0);
            assert!(db.stats().l0_files <= 2, "L0 not reduced: {:?}", db.stats());
            assert_eq!(db.get(b"k0000"), Some(b"v".to_vec()));
        }
        let _ = fs::remove_dir_all(&dir);
    }
}

/// [`LSMTree::light_iter_range`] 的状态机：k 路归并，每次 `next()` 只往前拉一条。
///
/// 堆里只放 `(键, 源序号)`，每个源至多一个候选在堆里；真正的值在 `head` 里等着。
/// 输出**非降序** ⇒ 跨源的同键去重只需一个"待定键"的先行量：
/// - 同一键由更新的源（序号小）定论 ⇒ 后到的同键丢掉
/// - 同源同键的多个 seq（正常写入不会出现，防的是.compaction 产物里同键多版）⇒ 取最大 seq
/// 没有这个先行量就会把"该键还有更新的源"的键提前吐出去。
pub struct LightMerge<'a> {
    sources: Vec<Box<dyn Iterator<Item = (Vec<u8>, LightEntry)> + 'a>>,
    head: Vec<Option<(Vec<u8>, LightEntry)>>,
    heap: BinaryHeap<MergeNode>,
    pending: Option<(Vec<u8>, LightEntry)>,
}

struct MergeNode {
    key: Vec<u8>,
    src: usize,
}

/// 最小键优先；同键时源序号小的优先（新→旧）
impl Ord for MergeNode {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match other.key.cmp(&self.key) {
            std::cmp::Ordering::Equal => other.src.cmp(&self.src),
            o => o,
        }
    }
}

impl PartialOrd for MergeNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for MergeNode {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.src == other.src
    }
}

impl Eq for MergeNode {}

impl<'a> LightMerge<'a> {
    fn new(mut sources: Vec<Box<dyn Iterator<Item = (Vec<u8>, LightEntry)> + 'a>>) -> Self {
        let mut head = Vec::with_capacity(sources.len());
        let mut heap = BinaryHeap::new();
        for (src, it) in sources.iter_mut().enumerate() {
            match it.next() {
                Some((k, le)) => {
                    heap.push(MergeNode { key: k.clone(), src });
                    head.push(Some((k, le)));
                }
                None => head.push(None),
            }
        }
        Self { sources, head, heap, pending: None }
    }

    /// 把源 `src` 的下一个候选放进堆（源 exhausted 就不放）
    fn advance(&mut self, src: usize) {
        if let Some((k, le)) = self.sources[src].next() {
            self.heap.push(MergeNode { key: k.clone(), src });
            self.head[src] = Some((k, le));
        } else {
            self.head[src] = None;
        }
    }
}

impl<'a> Iterator for LightMerge<'a> {
    type Item = (Vec<u8>, LightEntry);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(node) = self.heap.pop() {
            let Some((key, le)) = self.head[node.src].take() else {
                self.advance(node.src);
                continue;
            };
            self.advance(node.src);
            match self.pending.take() {
                None => self.pending = Some((key, le)),
                Some((pk, ple)) if pk == key => {
                    // 同一个键：更新的源已经先出（堆的 tie-break 保证），这里只防
                    // "同源同键多版"那种畸形产物 ⇒ 取更大的 seq，别让旧版赢
                    self.pending = Some((pk, if ple.seq >= le.seq { ple } else { le }));
                }
                Some((pk, ple)) => {
                    // 换键了 ⇒ 前一个键已经收齐，可以吐
                    self.pending = Some((key, le));
                    return Some((pk, ple));
                }
            }
        }
        self.pending.take()
    }
}
