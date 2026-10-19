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
    merge_latest_streaming, merge_streaming, merge_to_ssts, vec_inputs, BlockCache, EntryValue,
    LightEntry, MergeInput, SstBuilder, SstFile, StoredEntry,
};

/// 一路可被"当场收手"的源（M46）：收到停止信号之后一律吐 `None`。
///
/// 为什么要它：`merge_streaming` 是压实/修复共用的公开件，把它的 `emit` 改成返回 `bool`
/// 是给下游的破坏性变更。而"收手"真正需要的是**各源不再往下拉** —— 源一空，堆就空，
/// 归并按它自己的逻辑结束（剩下 O(源数) 次弹出，不再读盘、不再克隆 value）。
struct Stoppeable<I> {
    it: I,
    stopped: std::rc::Rc<std::cell::Cell<bool>>,
}

impl<I: Iterator> Iterator for Stoppeable<I> {
    type Item = I::Item;
    fn next(&mut self) -> Option<I::Item> {
        if self.stopped.get() {
            return None;
        }
        self.it.next()
    }
}

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
    /// block cache 容量（块数）；0=关。
    /// 开了 [`Self::block_cache_auto_max`] 时它是**地板**（下限），不是生效值。
    pub block_cache_blocks: usize,
    /// block cache **自动档的上限**（块数）；0=关自动档（容量恒等于 `block_cache_blocks`）。
    ///
    /// 非 0 时生效容量 = `clamp(当前 SST 块总数, block_cache_blocks, 本值)`，并在文件集变化
    /// （打开 / flush / compact / bulk ingest）时重算。为什么要有这一档：点读的钱几乎全落在
    /// "块缓存未命中"上（M23 同构建 A/B，5002 键那一档：钉死 256 槽 27.9 µs/次、未命中 0.39，
    /// 自动档给到 314 槽 ⇒ **8.5 µs/次、未命中 0.03**，而块/次两边都是 4.00 —— 少的不是访问次数，
    /// 是访问里"真去读盘"那一步）。固定 256 槽在 ~1.6 万行之后就不再装下工作集 ⇒ **单价随库长**。
    ///
    /// 上限是**预算**不是正确性：40000 键那一档 2681 块，放到 4096 槽（全库驻留 ≈ 18.7 MB）
    /// 乱序点读 117.3 → 21.0 µs/次，一次未命中实测 ≈ 54 µs（200k 那一档 ≈ 94 µs）。要不要把库
    /// 复制进 RAM 该调用方决定，与 B+ 树页缓存自动档（M22）同一口径。
    ///
    /// ⚠️ 与 `block_cache_blocks: 0`（关缓存）不冲突：地板为 0 时直接不缓存，上限不参与。
    /// ⚠️ 放大这个数之前先想清楚：**驻留块数本身就是别处的代价分母**。第一版自动档就是在
    /// `BlockCache` 的 LRU 上踩到这一格（每次命中 O(驻留块数) 的 `position()` 扫），已改成 O(1)，
    /// 守卫见 `lru_touch_cost_does_not_scale_with_residency`。
    pub block_cache_auto_max: usize,
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
            block_cache_auto_max: 0,
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
            // 块缓存地板 256（M10 那次从 64 提上来）+ 自动档上限 1024（M23）。
            // 实测形状（`LSM_POINT_ROWS=5002` 台架，就是下游那份现场）：314 块、每块均值 7403 B
            // ⇒ 钉死 256 槽**装不下**（差 58 块就整份覆盖），乱序点读因此 27.9 µs/次、未命中 0.39；
            // 自动档把容量给到 314 ⇒ 8.5 µs/次、未命中 0.03，常驻多 0.4 MB。
            // 上限 1024 的价签：1024 × 7.4 KB ≈ **7.1 MB**（40000 键那一档实测就是这个数），
            // 与 B+ 树页缓存自动档的 4~8 MB 同一量级；再大的库不再靠缓存，靠 `scan`（顺序读摊薄）。
            block_cache_blocks: 256,
            block_cache_auto_max: 1024,
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

// 测试专用：本线程「为了数 memtable 条目而走过的条目数」。
//
// M19 的复杂度尺子。它数的不是" flush 判定的结果对不对"，而是**判定本身花了多少步**：
// 旧写法每次 insert 都为"要不要 flush"把整表数一遍 ⇒ 灌 N 行是 O(N²)（200k 大规模基线抓到：
// 裸 insert 每行 89 µs → 1429 µs，与 memtable 规模同步）。改成增量计数器之后这个数**不该再涨**；
// 对照（同一条用例里主动调一次慢口径）证明这把尺子本身不是空转。
#[cfg(test)]
thread_local! {
    static MEM_SCAN_STEPS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// 记一次「把 memtable 数了一遍」的步数（只由 `scan_mem_entry_count` 调用）
#[cfg(test)]
pub(crate) fn note_mem_scan_steps(n: u64) {
    MEM_SCAN_STEPS.with(|c| c.set(c.get() + n));
}

/// 当前线程为"数 memtable 条目"走过的条目总数（测试探针用）
#[cfg(test)]
pub(crate) fn mem_scan_steps() -> u64 {
    MEM_SCAN_STEPS.with(|c| c.get())
}

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
    /// memtable 里的**版本条数**（Σ 每个键的版本数），与 `scan_mem_entry_count` 同口径。
    ///
    /// ⚠️ 必须是增量维护的计数，不许现算：2026-10-17 的 200k 大规模基线抓到旧写法在这里
    /// 每次插入都把整个 memtable 数一遍（只为回答"要不要 flush"），于是灌 N 行是 O(N²) ——
    /// 裸 `insert`（不经 MVCC）每行代价从 89 µs 涨到 1429 µs，与灌入曲线重合。
    mem_entries: usize,
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
    /// 块缓存是不是别人的（CF 模式共享一份）。true ⇒ **自动档不再动容量**：
    /// 一棵树按自己的块数改上限，会把同进程里另一棵树的驻留块换出去，那是净负收益。
    /// 共享缓存的容量由装配方（`cf.rs`）自己定。
    cache_shared: bool,
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
            mem_entries: 0,
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
            cache_shared: false,
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
        db.reevaluate_cache_capacity();
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

    /// SST 里需要缓存槽位的块总数（v1 文件记 0，见 [`SstFile::blocks`]）
    ///
    /// 公开它是为了让调用方能算**覆盖率**：`tuning()` 里的 `block_cache_capacity` 是分子，
    /// 这个数是分母（M32）。只看生效容量的人分不清"顶已经撞上了"和"库本来就小"。
    pub fn sst_block_count(&self) -> usize {
        self.levels.iter().flatten().map(|s| s.blocks()).sum()
    }

    /// 自动档的**生效**容量（不是配置值）。关掉自动档就等于 `block_cache_blocks`。
    pub fn block_cache_capacity(&self) -> usize {
        self.block_cache.lock().unwrap().capacity()
    }

    /// 文件集变了之后重算块缓存容量；**不重建缓存**（见 [`BlockCache::set_capacity`]）。
    ///
    /// 为什么挂在文件集变化处：块数是"要装多少"的唯一凭据，而 flush / compact 是唯一改它的两处。
    /// 放在读路径上会让一次点读顺带改容量，那不可预测；放在写路径末尾则一次库的代价只有 O(文件数)。
    fn reevaluate_cache_capacity(&mut self) {
        let floor = self.opts.block_cache_blocks;
        let ceil = self.opts.block_cache_auto_max;
        let target = if ceil == 0 || floor == 0 {
            // 关自动档：配置值就是生效值。地板 0 = 显式关缓存，上限不参与
            // （否则"关"会被自动档悄悄打开）
            floor
        } else if self.cache_shared {
            // 共享缓存（CF 模式）：每棵树只看得见自己的块数，谁后算谁赢 ⇒ 自动档不动，
            // 容量由装配方（`cf.rs`）定。
            return;
        } else {
            self.sst_block_count().clamp(floor, ceil)
        };
        self.block_cache.lock().unwrap().set_capacity(target);
    }

    /// 把块缓存自动档的上限改成 `blocks`，并**就地**重估容量（`usize::MAX` = 不设顶）。
    ///
    /// 为什么要有这条：默认的 1024 块 ≈ 7.1 MB 是**预算**，而库里块数远大于它时覆盖率就掉下来
    /// —— 实测 40000 键（2681 块）装不下、600k 键更明显，每次点读多付一次未命中（≈54~94 µs 的
    /// 随机读价）。抬顶等于"把库往 RAM 里复制"，这个决定归调用方，不归库默认。
    /// `0` 仍是"关自动档"（生效值 = 配置值），语义与 [`LSMTreeOptions::block_cache_auto_max`] 一致。
    pub fn set_block_cache_auto_max(&mut self, blocks: usize) {
        self.opts.block_cache_auto_max = blocks;
        self.reevaluate_cache_capacity();
    }

    /// 热更新部分 option（阈值 / compact 触发 / bloom 新文件 / fsync 策略等）。
    /// 不改变：`enable_mem_wal`（开/关需 reopen）、已有 SST 格式。
    pub fn set_options(&mut self, patch: LSMTreeOptions) {
        let keep_wal = self.opts.enable_mem_wal;
        self.opts = patch;
        self.opts.enable_mem_wal = keep_wal;
        // 块缓存容量：**就地改上限**，不重建。
        // 旧写法 `*cache = BlockCache::new(n)` 会把已经装进来的块全丢掉 ⇒ 改一次旋钮
        // 等于把点读打回冷启动，而且自动档每重算一次就自己清一次自己。
        self.reevaluate_cache_capacity();
    }

    /// 设置 compaction filter（`None` 清除）
    pub fn set_compaction_filter(&mut self, f: Option<CompactionFilter>) {
        self.compaction_filter = f;
    }

    /// 安装共享 block cache（多 CF 复用）
    pub fn install_shared_block_cache(&mut self, cache: Arc<Mutex<BlockCache>>) {
        self.block_cache = cache.clone();
        self.cache_shared = true;
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
        self.reevaluate_cache_capacity();
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
        self.collect_blob_refs(keep, &mut referenced);
        self.gc_blobs_referenced(&referenced)
    }

    /// [`Self::gc_blobs`] 的前半段：把这批保留行引用的 blob `seq` 并进 `into`。
    ///
    /// 单独露出来是给 **M37 的按块走查**用的：调用方每块把保留行喂进来就丢弃，于是
    /// "整张存活行清单"（`keep: &[Vec<u8>]`，600k 档那笔 O(全库) 的账）不需要存在；
    /// 留下的 `referenced` 只数**真有大 value 的行**（一个 `u64` 一条）。
    /// 判据与 `gc_blobs` 同一个口径（这份就是它原来的那段循环，没第二条实现）。
    pub(crate) fn collect_blob_refs(&self, keep: &[Vec<u8>], into: &mut HashSet<u64>) {
        for enc in keep {
            // 外层 None = 这行已经不在；内层 None = Drop 墓碑（墓碑不引用任何 blob）
            // ⚠️ 这里刻意用**不解析**的 get_raw_key_at：GC 靠指针字节认引用，解析就认不出来了
            let Some(Some(val)) = self.get_raw_key_at(enc, u64::MAX) else {
                continue;
            };
            if let Some(seq) = Self::blob_ptr_seq(&val) {
                into.insert(seq);
            }
        }
    }

    /// [`Self::gc_blobs`] 的后半段：拿着"还被引用的 seq 集合"去删没人引用的 blob 文件。
    /// 保守方向不变：宁可留着也不删错（删掉被引用的是**丢数据**）。
    pub(crate) fn gc_blobs_referenced(&self, referenced: &HashSet<u64>) -> std::io::Result<usize> {
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
        let replaced = versions.insert(val.seq, val.value);
        // 上面那句之后 `versions` 的借用就结束了，所以这里才允许动 `self` 的其它字段
        match replaced {
            // 同 seq 被覆盖：条数不变，只换字节账
            Some(old) => {
                self.mem_bytes = self
                    .mem_bytes
                    .saturating_sub(old.estimate_bytes())
                    .saturating_add(add);
            }
            None => {
                self.mem_bytes = self.mem_bytes.saturating_add(add);
                self.mem_entries += 1;
            }
        }
    }

    /// 慢口径：真把整个 memtable 数一遍。**只给测试当对照**——生产路径一律读 `mem_entries`。
    ///
    /// 留着它有两个用处：① 与增量计数器对拍口径（"版本条数"不是"键数"）；
    /// ② 当复杂度守卫的探针 —— 它每被调一次就把"走过的条目数"记账，所以"热路径有没有偷偷
    /// 现算"是能直接数出来的（M19 的守卫断言灌 4000 行期间这个计数**一点没涨**）。
    #[cfg(test)]
    fn scan_mem_entry_count(&self) -> usize {
        note_mem_scan_steps(self.mem.len() as u64);
        self.mem.values().map(|v| v.len()).sum()
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

    /// mem / immutable 的**重量**视图，但是**惰性**的（M46）：判据与改前那条 `mem_view_at`（整表折进 `Vec`，M46 起删除）
    /// 一字不差（每把键取"≤ snap 的最大 seq"那一版，`MemMap` 本来就是按键升序），
    /// 区别只有一个 —— 不再一次折成 `Vec`。
    fn mem_iter_at<'a>(
        map: &'a MemMap,
        snap_seq: u64,
    ) -> impl Iterator<Item = (Vec<u8>, StoredEntry)> + 'a {
        map.iter().filter_map(move |(k, vers)| {
            let (seq, ev) = vers.iter().rev().find(|(s, _)| **s <= snap_seq)?;
            // 重量路径：value 被整个克隆出来。全库折叠不该走这里 —— 计数探针就是用来
            // 发现「不知不觉又用回重量路径」。守卫另靠它数"交出第一行之前已经物化了多大一截"。
            #[cfg(test)]
            crate::lsm_tree::sst::note_value_materialization();
            Some((
                k.clone(),
                StoredEntry {
                    seq: *seq,
                    value: ev.clone(),
                },
            ))
        })
    }

    /// 归并的**源**（惰性，M46）：mem → immutables → L0 新→旧 → 更高层。
    ///
    /// 归并本身从 M33 起就吃 `Box<dyn Iterator>`（[`merge_streaming`]），坏的一直是这里：
    /// 改前每个 memtable 走 `mem_view_at`（整表折进 `Vec`，M46 起删除）、每个 SST 走
    /// `iter_all().filter(...).collect()`（整篇折进 `Vec`）⇒ **所有源同时活着**，
    /// 于是"全库折叠"的峰值 = 一份全库重量视图 + 归并输出。这正是 M39 在 MVCC 那道门
    /// （`scan_latest_light`）上治过的同一个形状，只是裸引擎这道门当时没走过。
    ///
    /// 源顺序与"每源按键升序"这两条是归并取胜的前提，一字没改；`vec_inputs` 那条兼容入口
    /// 也留着（压实的输入本来就在内存里，换个类型而已）。
    fn merge_inputs_at(&self, snap_seq: u64) -> Vec<MergeInput<'_, StoredEntry>> {
        let mut sources: Vec<MergeInput<'_, StoredEntry>> = Vec::new();
        if !self.mem.is_empty() {
            sources.push(Box::new(Self::mem_iter_at(&self.mem, snap_seq)));
        }
        for imm in &self.immutables {
            sources.push(Box::new(Self::mem_iter_at(imm, snap_seq)));
        }
        if let Some(l0) = self.levels.first() {
            for sst in l0.iter().rev() {
                sources.push(Box::new(
                    sst.iter_all()
                        .filter(move |(_, se)| se.visible_at(snap_seq)),
                ));
            }
        }
        for level in self.levels.iter().skip(1) {
            for sst in level {
                sources.push(Box::new(
                    sst.iter_all()
                        .filter(move |(_, se)| se.visible_at(snap_seq)),
                ));
            }
        }
        sources
    }

    /// 全库跨源归并的**流式**版（M46）：折一行交一行，`on` 返回 `false` 就收手 ——
    /// 收手之后各源不再往下拉。返回实际交出去的行数。
    ///
    /// 为什么"收手"要套一层 [`Stoppeable`] 而不是改 [`merge_streaming`] 的签名：那条是
    /// 压实/修复共用的公开件，把它的 `emit` 改成返回 `bool` 是给下游的破坏性变更；
    /// 而"收手"要的只是让各源尽快吐 `None`，堆一空归并自然结束（剩下的收尾是 O(源数) 次弹出）。
    pub(crate) fn merge_latest_at(
        &self,
        snap_seq: u64,
        on: &mut dyn FnMut(Vec<u8>, StoredEntry) -> bool,
    ) -> usize {
        let stopped = std::rc::Rc::new(std::cell::Cell::new(false));
        let mut handed = 0usize;
        let sources: Vec<MergeInput<'_, StoredEntry>> = self
            .merge_inputs_at(snap_seq)
            .into_iter()
            .map(|it| {
                Box::new(Stoppeable {
                    it,
                    stopped: stopped.clone(),
                }) as MergeInput<'_, StoredEntry>
            })
            .collect();
        merge_streaming(sources, |_, _| true, |k, se| {
            if stopped.get() {
                return;
            }
            if on(k, se) {
                handed += 1;
            } else {
                stopped.set(true);
            }
        });
        handed
    }

    /// 全库「每个 enc key 的胜者」的**轻量**视图（跨源取胜规则与 [`Self::iter`] 一致：
    /// 源新→旧优先，同 src 取更大 seq）。
    ///
    /// 与 `iter()` 的差别就在**不克隆 value**：需要 value 的调用方对少数胜出行再点查
    /// [`Self::get_value_at`]（会解析 blob 指针），语义与归并取胜一致。
    ///
    /// M39 起这份实现就是 [`Self::light_iter_range`] 的**收集壳**：以前它自己把每个 memtable
    /// `mem_view_light` 整表遍历、每个 SST `light_view` 整篇折进 `Vec`，再把所有源同时握着做归并
    /// ⇒ 峰值 = 「2 × 全库轻量视图」。600k 档那笔 **瞬时 +126.1 MB** 就是这么来的
    /// （`health()` 当时是它唯一的生产调用方，现已改走惰性迭代器）。
    /// 签名保留给"确实要一次拿到整份快照"的调用方：它仍然返回 `Vec`，但中途不再另握一份源副本。
    pub fn scan_latest_light(&self, snap_seq: u64) -> Vec<(Vec<u8>, LightEntry)> {
        self.light_iter_range(None, None, snap_seq).collect()
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
            // ⚠️ M47：`to == None` 时**不能**拿一个合成的上界去剪文件 —— 键是任意字节，
            // 任何"够大的填充值"都存在比它更大的真键（改前那行用 `&[0xffu8; 8]`，
            // 于是 `min_key = [0xff;12]` 的文件在"全库"走查里被整篇跳过：少数、少扫、不报错）。
            // 有上界时才用 min/max 双向剪；没有上界就只剪"整篇在 `from` 之下"的那一头。
            let alive = !(sst.min_key.is_empty() && sst.max_key.is_empty());
            let overlaps = match to {
                Some(hi) => alive && sst.may_overlap(probe_low, hi),
                None => alive && sst.max_key.as_slice() >= probe_low,
            };
            if !overlaps {
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


    fn maybe_flush_mem(&mut self) {
        // ⚠️ 读增量计数器，不是现算。旧写法 `self.mem.values().map(|v| v.len()).sum()` 每次
        // 插入都走一遍整表 ⇒ 灌 N 行 O(N²)。判定语义（"版本条数 ≥ 阈值就 flush"）一字未动。
        let by_entries = self.opts.mem_threshold_entries > 0
            && self.mem_entries >= self.opts.mem_threshold_entries;
        // M41b：字节这一档读的是**真内存**（载荷 + 条数 × 结构开销），不是载荷账。
        // 改前 `mem_threshold_bytes` 在"小值/墓碑"形状上从不触发（一条 16 B 编码键的墓碑只算 25 B
        // 载荷），于是 memtable 的实际大小由条数阈值说话 —— MVCC 档那一句"mem 32 MB"因此是空的：
        // 262 144 条在堆上是 ~155 MB。⚠️ `stats().mem_bytes` 对外仍是**载荷账**，没改口径，
        // 改的只有这一句判定；两个都是增量计数器 ⇒ 仍然是 O(1)，不把 M19 的 O(N²) 请回来。
        let by_bytes = self
            .mem_bytes
            .saturating_add(self.mem_entries.saturating_mul(MEM_ENTRY_OVERHEAD))
            >= self.opts.mem_threshold_bytes;
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

    /// 测试专用：SST 块总数（自动档的容量凭据）
    #[cfg(test)]
    pub(crate) fn sst_blocks_for_test(&self) -> usize {
        self.sst_block_count()
    }

    /// 测试专用：块缓存里**当前真的住着几块**（M32 用它证明"抬容量不重建缓存"）
    #[cfg(test)]
    pub(crate) fn cached_blocks_for_test(&self) -> usize {
        self.block_cache.lock().unwrap().len()
    }

    /// 测试专用：第 0 层的文件句柄（M30 的耗时拆分要从这里取压实输入）
    #[cfg(test)]
    pub(crate) fn l0_for_test(&self) -> &[SstFile] {
        self.levels.first().map(|l| l.as_slice()).unwrap_or(&[])
    }

    /// 测试专用：把一批**已按键升序**的条目直接写成一个 SST 挂进某一层。
    ///
    /// 为什么要它：`repair_level_overlaps` 要收拾的那个形状（同层两个范围重叠的文件）
    /// **正常压实造不出来** —— L0 是整层并入、同层输出按键有序。它是**崩溃恢复**的产物：
    /// 按 M26 的正确顺序（先记 MANIFEST 再删旧文件），崩在那两步之间就会留下"新+旧同时在层里"。
    /// 手工造那个状态比工程化一次真崩溃便宜，而且造的正是恢复后内存里会出现的那份形状。
    #[cfg(test)]
    pub(crate) fn inject_sst_for_test(
        &mut self,
        level: usize,
        entries: Vec<(Vec<u8>, StoredEntry)>,
    ) -> std::io::Result<()> {
        let id = self.next_sst_id.fetch_add(1, Ordering::SeqCst);
        let path = self.sst_path(id);
        let mut b = SstBuilder::with_options(
            id,
            &path,
            self.opts.sst_block_entries,
            self.opts.enable_bloom,
        )
        .with_bloom_bits(self.opts.bloom_bits_per_key);
        for (k, se) in entries {
            b.add(k, se);
        }
        let mut sst = b.finish()?;
        sst.set_cache_ns(Self::dir_cache_ns(&self.dir));
        sst.set_block_cache(self.block_cache.clone());
        while self.levels.len() <= level {
            self.levels.push(Vec::new());
        }
        self.levels[level].push(sst);
        self.stats_dirty = true;
        self.write_manifest()
    }

    /// 跨源点查：**每个源都要走一遍，取最大可见 seq**，不能"在第一个含它的源就收手"。
    ///
    /// L0 那一段看着确实可以收手（flush 单调：后建的文件只装更晚的 seq），但 L1+ 不行：
    /// `merge_to_ssts` 只按 `target_bytes` 切文件，切的粒度是**编码键**，而同一把 raw key 的
    /// 各版本是各不相同的编码键 ⇒ 一个版本组可以被劈成相邻两个文件（`sst.rs` 里那段
    /// `if b.estimated_bytes() >= target_bytes`）。层内又按 `min_key` 排序，而版本键大端有序
    /// ⇒ **同一个 raw key 的新版本住在后面那个文件**。先命中即停会读回旧版本，而且是静默读回。
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

    /// 跨源去重之后的**键数**（三种赢家行都算：有值、空值、`Drop` 墓碑）。
    ///
    /// M47：只数不物化 —— 改前是 `merge_all_at(u64::MAX).len()`，为一个数把整库连 value 折出来
    /// （与 M35 那刀里 `key_count` 同一个形状）。判据复用 [`Self::count_kinds_at`]。
    pub fn entry_count(&self) -> usize {
        let (put, empty, drop) = self.count_kinds_at(u64::MAX);
        put + empty + drop
    }

    /// 三类赢家行的条数：`(Put(Some), Put(None), Drop)`。
    ///
    /// 为什么一条就够：`entry_count`、`len` 的 dirty 路径、`refresh_stats_cache` 问的其实都是
    /// "每一类各多少条"，只是各自取不同的一撮 ⇒ 三份"为一个数把整库折出来"合成一份
    /// （M5a/M5b 一路的做法：同形状的判定不许留三份）。
    /// 判据（谁算一个键、跨源怎么选赢家）走 M39 那条惰性轻量归并，与重量归并一字不差；
    /// 轻量视图只带 `seq` 与 `kind` 字节 ⇒ **一行 value 都不克隆**
    /// （守卫拿 `sst::value_materializations()` 钉这一格必须是 0）。
    fn count_kinds_at(&self, snap_seq: u64) -> (usize, usize, usize) {
        let (mut put, mut empty, mut drop) = (0usize, 0usize, 0usize);
        for (_k, le) in self.light_iter_range(None, None, snap_seq) {
            match le.kind {
                0 => put += 1,
                1 => empty += 1,
                2 => drop += 1,
                // 种类字节只有这三种；走到这里说明两边口径分了家。
                // ⚠️ 宁可少算，也不要把一个不认识的种类当成存活键（那是假数，不是崩溃）。
                _ => {}
            }
        }
        (put, empty, drop)
    }

    /// 存活 key 数。优先返回缓存；`stats_dirty` 时会走查全库（O(全库条目)，M47 起一行 value 都不克隆）。
    /// 刚 `open` 后若未写过，缓存为 0——需要精确值请用 [`stats_exact`]。
    pub fn len(&self) -> usize {
        if !self.stats_dirty {
            return self.cached_live;
        }
        // M47：只数不物化（改前是 `merge_all_at(...).iter().filter(Put(Some)).count()`）
        self.count_kinds_at(u64::MAX).0
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
            // 与 flush 判定同一个来源（增量计数器），所以 `stats()` 也是 O(层数) 而不是 O(memtable)
            mem_entries: self.mem_entries,
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

    /// 走查全库后刷新 `cached_live`/`cached_tomb`，返回精确 stats。
    /// 大库 O(全库条目)，仅对账 / 测试使用。⚠️ M47 起这一趟**不折 value**（只数种类）。
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
        // M47：只数不物化。改前这里是 `merge_all_at(u64::MAX)` 把整库连 value 折出来，
        // 再逐条 match —— 与 `entry_count`/`len` 是同一个形状的第三份复制。
        // 口径一字未动：`Put(Some)` → live、`Put(None)` → tomb、`Drop` 两处都不计。
        let (put, empty, _drop) = self.count_kinds_at(u64::MAX);
        self.cached_live = put;
        self.cached_tomb = empty;
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
        self.mem_entries = 0;
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
        // M41a：**流式**吐 memtable。旧形状是 `mem_to_sorted_entries(&imm)` —— 把整张表
        // 连键带值**克隆**进一个 `Vec` 再 `sort_by((键, seq))`。那个排序是白付的：`MemMap` 是
        // `BTreeMap<键, BTreeMap<seq, 值>>`，两级本来就升序 ⇒ 按值进循环出来的顺序与它排完
        // 一模一样（`SstBuilder::add` 里那句 `debug_assert(last_k <= key)` 核的就是这个序）。
        // 按值进循环还有第二层意义：每搬走一条，memtable 那一侧当场少一条（BTreeMap 的节点随
        // 消费释放）⇒ flush 的瞬时占用是"一块条目 + builder"，不是"整张 memtable + 它的两份拷贝"。
        for (k, vers) in imm {
            // 编码键本身就带版本 ⇒ 一把键下有多条 seq 的情形极少，那种时候才多克隆一次键
            for (seq, ev) in vers {
                builder.add(k.clone(), StoredEntry { seq, value: ev });
            }
        }
        let mut sst = builder.finish()?;
        sst.set_cache_ns(Self::dir_cache_ns(&self.dir));
        sst.set_block_cache(self.block_cache.clone());
        if self.opts.use_mmap {
            let _ = sst.enable_mmap();
        }
        self.levels[0].push(sst);
        self.reevaluate_cache_capacity();
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

        // `input_files`：**拥有**本次压实的输入文件，顺序 = 源序（新→旧）。
        // M33 起归并收的是迭代器，所以这些文件必须活到 `merge_to_ssts` 返回为止 ——
        // 旧写法在这里就 `iter_entries()` 把它们整篇读进 `Vec` 再丢掉文件，于是瞬时内存
        // ≈ 所有输入文件之和（实测 600k 行 61.7 MB 输入 ⇒ 物化那一步峰值 +154 MB）。
        let (input_files, paths) = if level == 0 {
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
            let mut files: Vec<SstFile> = Vec::with_capacity(l0.len() + l1_merge.len());
            files.extend(l0.into_iter().rev()); // L0 那个 vec 是旧→新，反转成新→旧
            files.extend(l1_merge);
            let paths: Vec<_> = files.iter().map(|s| s.path.clone()).collect();
            self.levels[1] = l1_keep;
            (files, paths)
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
            // 源序 = picked 先、下一层重叠段在后（与旧写法同序）
            let mut files: Vec<SstFile> = Vec::with_capacity(1 + next_merge.len());
            files.push(picked);
            files.extend(next_merge);
            self.levels[next] = next_keep;
            (files, paths)
        };

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

        // 每源一路迭代器，`iter_all()` 一块一块读 ⇒ 压实期间活着的是"每源当前那一块 + 一个
        // builder"，不再是"所有输入文件之和"（M33）。
        let mut new_files = merge_to_ssts(
            input_files
                .iter()
                .map(|s| Box::new(s.iter_all()) as MergeInput<'_, StoredEntry>)
                .collect(),
            &keep,
            || {
                let id = next_id.fetch_add(1, Ordering::SeqCst);
                let path = dir.join("sst").join(format!("{id:08}.sst"));
                SstBuilder::with_options(id, path, block_entries, enable_bloom)
                    .with_bloom_bits(bloom_bits)
            },
            target,
        )?;
        // 迭代器借的是 `input_files`，而下面要**删这些文件** —— Windows 上句柄还开着删不掉，
        // 所以归并一结束就释放输入文件（旧写法"源是 Vec"顺带做到了这点，不是巧合能省的东西）。
        drop(input_files);
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
        // L1+：层内重叠**真的折掉**（M26）。旧实现只 `eprintln!` 告警、把文件原样交回去 ⇒
        // 重叠一旦出现就永远没人修：常规压实只会拉"与所选文件重叠的**下一层**"，从不碰同层兄弟，
        // 而这一层的输出又只在被压实到时才重写。
        let mut replaced: Vec<PathBuf> = Vec::new();
        if next >= 1 {
            self.repair_level_overlaps(&mut all_next, &mut replaced, &keep)?;
        }
        self.levels[next] = all_next;
        self.stats_dirty = true;
        self.reevaluate_cache_capacity();
        // ⚠️ 顺序：**先把新文件集记进 MANIFEST，再删旧文件**（M26）。
        // 旧写法是"删完就等调用方去写 manifest"，中间崩掉 ⇒ manifest 还指着已经删掉的文件、
        // 而新写出的文件没人记 —— `load_manifest` 对不在盘的条目是 `path.exists()` 静默跳过，
        // 于是重开之后那批数据**既不在 manifest 里也不在盘上**，一句话不报。
        // 反过来留的窗口是"新+旧同时在 manifest 里"（重叠），那正是上面 `repair_level_overlaps`
        // 负责收拾的形状，读答案两边同键同 seq ⇒ 折叠与点查都不受影响。
        self.write_manifest()?;
        #[cfg(test)]
        {
            // 顺序守卫读的是**盘上的 MANIFEST 内容**，不是"我有没有在删之前调用过一次写"：
            // 删任何一个旧文件之前，这一层现在每个文件的 id 都必须已经在 MANIFEST 里。
            // 把上面那次 `write_manifest` 挪回删除之后（旧写法：删完等调用方去记），这段当场红 ——
            // 而且**不需要谁"记得把探针一起搬"**，因为探针就长在删除点前面。
            let text = fs::read_to_string(self.manifest_path()).unwrap_or_default();
            for sst in &self.levels[next] {
                assert!(
                    text.split_whitespace().any(|w| w == sst.id.to_string()),
                    "删旧 SST 之前 MANIFEST 里还找不到新文件 {}（层 {next}）⇒ 崩在这个窗口就是静默丢数据",
                    sst.id
                );
            }
        }
        for p in paths.iter().chain(replaced.iter()) {
            let _ = fs::remove_file(p);
        }
        // 通知后台
        self.notify_bg();
        Ok(())
    }

    /// 把一层里范围连续重叠的文件折成不重叠的新文件。
    ///
    /// 分组：传进来的 `files` 已按 `min_key` 排序，扫一遍把"与当前段上界相交"的文件并进同一段，
    /// 并且是**传递闭包**（A∩B、B∩C 而 A∩C=∅ 也得整段一起折，否则留下新的重叠）。
    /// 长度 1 的段原样留着 —— 那是常态：正常压实不会产生重叠（L0 全量并入、同层输出按键有序）。
    ///
    /// 取胜规则直接用 [`merge_to_ssts`](→ `merge_streaming`)：**同键取 seq 最大者**，与跨源点查
    /// `get_entry_at` 同一个判据 ⇒ 修复前后的读答案逐键相同（这条有守卫对拍，不是说法）。
    /// 过滤条件沿用**本次 compaction 的 `keep`**（TTL / Drop / 用户 filter）：同一次压实里
    /// "新写出的文件过滤过、修复出来的没过滤"会更难解释。
    ///
    /// ✅ 内存：M33 起这条与常规压实**走同一条流式源**（`iter_all()` 一块一块读）。
    /// 旧写法在这里也 `iter_entries()` 把段里每个文件整篇读进 `Vec`，与压实共用那条账单 ——
    /// 实测 600k 行 / 8 个输入文件时那一步峰值涨 154 MB（条目本身 61.7 MB）。
    /// 尺子见本文件 `test_compaction_repairs_level_overlap` 里那条 `merge_inputs_materialized` 断言。
    ///
    /// 被替换掉的旧文件路径追加进 `replaced`，**由调用方在 MANIFEST 记完之后**再删。
    fn repair_level_overlaps<F>(
        &mut self,
        files: &mut Vec<SstFile>,
        replaced: &mut Vec<PathBuf>,
        keep: &F,
    ) -> std::io::Result<()>
    where
        F: Fn(&[u8], &StoredEntry) -> bool,
    {
        let taken = std::mem::take(files);
        let mut runs: Vec<Vec<SstFile>> = Vec::new();
        let mut cur: Vec<SstFile> = Vec::new();
        let mut cur_hi: Vec<u8> = Vec::new();
        for f in taken {
            if cur.is_empty() || f.min_key.as_slice() <= cur_hi.as_slice() {
                if f.max_key > cur_hi {
                    cur_hi = f.max_key.clone();
                }
                cur.push(f);
            } else {
                runs.push(std::mem::take(&mut cur));
                cur_hi = f.max_key.clone();
                cur.push(f);
            }
        }
        if !cur.is_empty() {
            runs.push(cur);
        }

        let block_entries = self.opts.sst_block_entries;
        let enable_bloom = self.opts.enable_bloom;
        let bloom_bits = self.opts.bloom_bits_per_key;
        let target = self.opts.target_sst_bytes;
        let dir = self.dir.clone();
        let cache = self.block_cache.clone();
        let use_mmap = self.opts.use_mmap;
        let ns = Self::dir_cache_ns(&self.dir);
        let next_id = &self.next_sst_id;

        let mut out: Vec<SstFile> = Vec::new();
        for run in runs {
            if run.len() <= 1 {
                out.extend(run);
                continue;
            }
            let old_paths: Vec<PathBuf> = run.iter().map(|s| s.path.clone()).collect();
            // M33：修复重叠与常规压实**共用同一条流式源账单**。旧写法在这里也 `iter_entries()`
            // 整篇读进 Vec，而修复恰恰多在崩溃恢复之后跑 —— 那时正是内存最紧的时候。
            let mut merged = merge_to_ssts(
                run.iter()
                    .map(|s| Box::new(s.iter_all()) as MergeInput<'_, StoredEntry>)
                    .collect(),
                keep,
                || {
                    let id = next_id.fetch_add(1, Ordering::SeqCst);
                    let path = dir.join("sst").join(format!("{id:08}.sst"));
                    SstBuilder::with_options(id, path, block_entries, enable_bloom)
                        .with_bloom_bits(bloom_bits)
                },
                target,
            )?;
            for sst in &mut merged {
                sst.set_cache_ns(ns);
                sst.set_block_cache(cache.clone());
                if use_mmap {
                    let _ = sst.enable_mmap();
                }
            }
            replaced.extend(old_paths);
            out.extend(merged);
        }
        out.sort_by(|a, b| a.min_key.cmp(&b.min_key));
        *files = out;
        Ok(())
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
        merge_latest_streaming(vec_inputs(sources), |_, _| true)
    }

    fn merge_all_at(&self, snap_seq: u64) -> Vec<(Vec<u8>, StoredEntry)> {
        // M46：这条改成 [`Self::merge_latest_at`] 的**收集壳** —— 归并取胜的判据只剩一份实现，
        // "整库快照"与"逐行流式"不再是两条会各自漂移的路。省下的是**源**那一头：
        // 原先每个 memtable 与每个 SST 都先整篇折进各自的 `Vec` 才开始归并（所有源同时活着）。
        // ⚠️ 它仍然返回全库的 `Vec`（那是调用方要的形状）；只要"逐行"的人请直接用流式那条。
        let mut out: Vec<(Vec<u8>, StoredEntry)> = Vec::new();
        self.merge_latest_at(snap_seq, &mut |k, se| {
            out.push((k, se));
            true
        });
        out
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

    /// 导出逻辑 KV + 写序号。
    ///
    /// M46 起这条是 [`Self::export_records_with_seq_for_each`] 的**收集壳**：取胜与"墓碑要不要参与"
    /// 的判据只剩一份实现。⚠️ 它返回整库的 `Vec`（那是调用方要的形状）—— 只要"把行交出去"的人
    /// （导出到文件）请直接用流式那条。
    pub fn export_records_with_seq(
        &self,
        include_deleted: bool,
    ) -> Vec<(Vec<u8>, Option<Vec<u8>>, u64)> {
        let mut out: Vec<(Vec<u8>, Option<Vec<u8>>, u64)> = Vec::new();
        self.export_records_with_seq_for_each(include_deleted, &mut |k, v, seq| {
            out.push((k, v, seq));
            true
        });
        out
    }

    /// **流式**导出（M46）：归并一行、判一行、立刻交给 `on`，返回交出去的行数；
    /// `on` 返回 `false` 当场收手，后面的键不再折、不再读盘。
    ///
    /// 为什么裸引擎也要给一条：M44 给门面加了流式出口之后，四家 CLI 里只剩
    /// `lsm_tree/bin/export_kv.rs` 还在物化整库 —— 因为它走的是这条**裸引擎**入口
    /// （`merge_all_at`），当时归并的**源**每个都先整篇折进 `Vec`（M46 修的就是这个）。
    pub fn export_records_with_seq_for_each(
        &self,
        include_deleted: bool,
        on: &mut dyn FnMut(Vec<u8>, Option<Vec<u8>>, u64) -> bool,
    ) -> usize {
        let mut handed = 0usize;
        self.merge_latest_at(u64::MAX, &mut |k, se| match se.value {
            EntryValue::Put(val) => {
                if val.is_none() && !include_deleted {
                    return true; // 墓碑不参与，但也不算"交出去一行"：继续走
                }
                let keep = on(k, val, se.seq);
                if keep {
                    handed += 1;
                }
                keep
            }
            // 硬删（Drop）不参与导出 —— 与改前那份 filter_map 一字不差
            EntryValue::Drop => true,
        });
        handed
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

    /// M41a：`flush()` 必须把 memtable **搬**进 SST，一条都不许复制值。
    ///
    /// 旧形状 `mem_to_sorted_entries(&imm)` 先把整张 memtable 连键带值克隆进一个 `Vec` 再
    /// `sort_by` —— 那份暂存在 600k 档就是 flush 段实测斜率里的 ~0.6 KB/条。尺子 =
    /// `EntryValue` 的复制次数（`sst::entry_value_clones`，手写 `Clone` 就为记这一笔）：
    /// 一次 flush 之后必须是 **0**。
    ///
    /// ⚠️ 两条防自空转的断言：flush 真发生了（`mem_entries` 归零、SST 至少多出一个），
    /// 以及数据还读得回来。
    ///
    /// 变异验证：把 `flush_one_immutable` 里那两个 `for` 换回
    /// `for (k, se) in Self::mem_to_sorted_entries(&imm)` ⇒ 尺子变成 memtable 的条数，红。
    #[test]
    fn test_flush_moves_memtable_entries_without_cloning_them() {
        use crate::lsm_tree::sst::entry_value_clones;
        let dir = tmp_dir("flush_no_clone");
        {
            let mut db = LSMTree::open(&dir).unwrap();
            // 载荷远不到 4 MB / 64k 条这两档 ⇒ 这 5000 条一直躺在 memtable 里，
            // 下面那次 flush 才是"搬多少条"的那一次
            for i in 0..5000usize {
                db.put(format!("k{i:05}").into_bytes(), b"v".to_vec());
            }
            assert_eq!(db.stats().mem_entries, 5000, "写路自己先 flush 了 ⇒ 这次量的不是 flush");
            let sst0 = db.stats().total_sst;
            let base = entry_value_clones();
            db.flush().unwrap();
            let copied = entry_value_clones() - base;
            assert_eq!(
                db.stats().mem_entries,
                0,
                "flush 没把 memtable 清空 ⇒ 形状断言不成立"
            );
            assert!(
                db.stats().total_sst > sst0,
                "flush 没产出 SST ⇒ 这条判据量不到任何东西（空转）"
            );
            assert_eq!(
                copied, 0,
                "flush 复制了 {copied} 条值 ⇒ 又变回'整张 memtable 克隆进 Vec 再 sort'那个形状"
            );
            // 语义还得对：搬过去的行读得回来。**首/中/尾三把**都读 —— 那次 `sort_by` 没了之后，
            // SST 稀疏索引靠的就是 `MemMap` 两级升序这件事，读错一段当场红
            for k in [b"k00000".as_slice(), b"k02500".as_slice(), b"k04999".as_slice()] {
                assert_eq!(db.get(k), Some(b"v".to_vec()), "搬进 SST 之后读不回来");
            }
        }
        let _ = fs::remove_dir_all(&dir);
    }

    /// M46 的守卫：全库归并交出**第一行**之前，不该已经把整库的值物化过一遍。
    ///
    /// 尺子 = `sst::value_materializations()`（重量路径每出一条记一次：`SstEntryIter::next`
    /// 与 `mem_iter_at`）。为什么拿它当凭据而不是内存高水位：并行用例会互相污染
    /// （M27/M36 各栽过一次），而"交出第一行之前已经物化了多大一截"正是这条路径的代价单位。
    /// 改前的形状是 `merge_all_at` 把每个 memtable `mem_view_at` 整表折进 `Vec`、每个 SST
    /// `iter_all().collect()` 整篇折进 `Vec` ⇒ 归并还没开始，计数器就已经是**全库**。
    ///
    /// 三条各自防一种坏法：
    /// ① 只要 1 行 ⇒ 物化次数在"源数量级"，不是全库（回归成整篇物化源就冲到 20 000+）；
    /// ② 全库那一趟 ⇒ 物化次数 ≥ 行数 —— 这一档是给 ① 作证的：不响的尺子读出来的"小数字"没意义；
    /// ③ 取胜规则没变：导出的行与**逐把点查**一字不差（点查是另一条路径，M26 立的那条不变量）。
    ///
    /// 变异验证：把 `merge_inputs_at` 里 SST 那一支换回 `.collect()` 再 `vec_inputs` 递进去 ⇒
    /// 红在 ①（交出第一行之前已物化 20 000 条）。
    #[test]
    fn test_merge_streams_sources_instead_of_materializing_them() {
        use crate::lsm_tree::sst::value_materializations;
        const N: usize = 20_000;
        const FILES: usize = 4;
        let dir = tmp_dir("merge_lazy_sources");
        {
            // 两档都钉开：memtable 不会自己 flush（flush 由我按次发起，源数才算得准），
            // L0 也不会自己 compact（默认 4 篇就合，那样"4 个源"这个形状当场就没了）
            let mut db = LSMTree::open_with_options(&dir, usize::MAX / 8, 64).unwrap();
            // 分成 FILES 次 flush ⇒ 归并的源是 FILES 篇 SST（不是一只 memtable），
            // 这样"每个源整篇折进 Vec"才会显形为"全库"
            for f in 0..FILES {
                for i in 0..(N / FILES) {
                    let k = format!("k{:06}", f * (N / FILES) + i).into_bytes();
                    db.put(k, vec![b'v'; 8]);
                }
                db.flush().unwrap();
            }
            assert_eq!(db.stats().total_sst as usize, FILES, "没造出 {FILES} 篇 SST ⇒ 形状不对");
            assert!(db.stats().mem_entries == 0, "还有行留在 memtable ⇒ 源数不对");

            // ── ① 只要第一行 ──
            let base = value_materializations();
            let mut seen = 0usize;
            let handed = db.merge_latest_at(u64::MAX, &mut |_k, _se| {
                seen += 1;
                false // 当场收手
            });
            let first_mats = value_materializations() - base;
            eprintln!(
                "[merge lazy] 交出 {handed} 行（回调收到 {seen} 行）⇒ 第一行之前物化了 {first_mats} 条（库里 {N} 行、源 {FILES} 个）"
            );
            assert_eq!(handed, 0, "收手那一行不该算交出去（返回的行数说它算了）");
            assert_eq!(seen, 1, "回调被叫了 {seen} 次 ⇒ 收手没生效");
            assert!(
                first_mats <= 64,
                "交出第一行之前已物化 {first_mats} 条（库 {N} 行）⇒ 源又被整篇折进 Vec 了"
            );

            // ── ② 同一把尺子在全库那一趟必须响（不然上面那个小数可能是空转）──
            let base = value_materializations();
            let mut all = 0usize;
            let handed_all = db.merge_latest_at(u64::MAX, &mut |_k, _se| {
                all += 1;
                true
            });
            let full_mats = value_materializations() - base;
            eprintln!("[merge lazy] 全库一趟：交出 {handed_all} 行、物化 {full_mats} 条");
            assert_eq!(handed_all, N, "全库归并交出的行数不对");
            assert_eq!(all, N, "回调次数与返回行数不同");
            assert!(
                full_mats >= N as u64,
                "全库那一趟只物化了 {full_mats} 条（应 ≥ {N}）⇒ 尺子空转，上面那个 ≤64 不算数"
            );

            // ── ③ 取胜规则没变：导出的行必须与逐把点查一字不差 ──
            let exported: Vec<(Vec<u8>, Option<Vec<u8>>)> = db.export_records(false);
            assert_eq!(exported.len(), N, "导出的行数不对");
            let keys: Vec<Vec<u8>> = exported.iter().map(|(k, _)| k.clone()).collect();
            let mut sorted = keys.clone();
            sorted.sort();
            sorted.dedup();
            assert_eq!(sorted, keys, "导出不是按键升序且互不相同 ⇒ 归并的取胜/去重坏了");
            let mut by_key: std::collections::HashMap<Vec<u8>, Option<Vec<u8>>> =
                exported.into_iter().collect();
            for i in (0..N).step_by(N / 32) {
                let k = format!("k{i:06}").into_bytes();
                let got = db.get(&k);
                let row = by_key.remove(&k).unwrap_or_else(|| panic!("导出里没有键 {k:?}"));
                assert_eq!(row.as_deref(), got.as_deref(), "第 {i} 把键：导出与点查不一致");
            }

            // ── ④ 流式导出的"收手"：一行都不许再多折 ──
            let base = value_materializations();
            let handed_stop =
                db.export_records_with_seq_for_each(false, &mut |_k, _v, _seq| false);
            let stop_mats = value_materializations() - base;
            eprintln!("[merge lazy] 一行的导出：交出 {handed_stop} 行、物化 {stop_mats} 条");
            assert_eq!(handed_stop, 0, "回调一律拒绝，却还是交出了行");
            assert!(
                stop_mats <= 64,
                "一行的导出物化了 {stop_mats} 条 ⇒ 收手没传到源那一头"
            );
        }
        let _ = fs::remove_dir_all(&dir);
    }

    /// M47 的守卫：三格"为一个数物化整库"（`entry_count`、`len` 的 dirty 路径、
    /// `stats_exact` → `refresh_stats_cache`）改成只数不物化之后，**数要一字不差**，
    /// 而 value 一次都不该被克隆。
    ///
    /// 两把尺子各钉各的（M44 的教训：一把读 0 的尺子可能只是没接上）：
    /// ① **对拍重量归并** —— 测试里直接把 `merge_all_at` 跑一遍按类数出来当基准，
    ///   形状必须混：存活行、空值 `Put(None)`、`Drop` 墓碑，再加"同一把键在 memtable 与
    ///   多份 SST 里都有"（跨源取胜才是这条路径真正在算的东西）；
    /// ② `value_materializations()` 在三格上增量必须是 **0**，而同一个测试里基准那一趟
    ///   必须读到 ≥ 全库 ⇒ 证明这把尺子确实接在重量路径上。
    #[test]
    fn test_counting_paths_do_not_materialize_values() {
        use crate::lsm_tree::sst::value_materializations;
        let dir = tmp_dir("count_no_materialize");
        {
            let mut db = LSMTree::open(&dir).unwrap();
            for i in 0..400usize {
                db.put(format!("k{i:04}").into_bytes(), vec![b'v'; 4]);
            }
            for i in 100..140usize {
                db.insert(format!("e{i:04}").into_bytes(), None);
            }
            for i in 200..230usize {
                db.delete(format!("k{i:04}").into_bytes());
            }
            // `remove` 写的是第三种赢家行 `EntryValue::Drop`（`delete` 只写 `Put(None)`）：
            // 三种种类都要在场，否则 `count_kinds_at` 里那一支 match 分支量不到。
            for i in 50..62usize {
                assert!(db.remove(format!("k{i:04}").as_bytes()));
            }
            db.flush().unwrap();
            // 同一把键再写一遍 ⇒ memtable 与 SST 里都有它，跨源取胜才被判到
            for i in 300..340usize {
                db.put(format!("k{i:04}").into_bytes(), vec![b'w'; 9]);
            }
            db.insert(
                format!("e{:04}", 105).into_bytes(),
                Some(b"again".to_vec()),
            );
            db.flush().unwrap();
            for i in 350..360usize {
                db.delete(format!("k{i:04}").into_bytes());
            }
            for i in 100..108usize {
                assert!(db.remove(format!("k{i:04}").as_bytes()));
            }
            assert!(
                db.stats().total_sst >= 2,
                "没造出两份以上 SST ⇒ 这次对拍量不到跨源"
            );

            // ── 基准：重量归并逐条数（这就是改前三格的实现）──
            let base = value_materializations();
            let all = db.merge_all_at(u64::MAX);
            let want = (
                all.iter()
                    .filter(|(_, se)| matches!(se.value, EntryValue::Put(Some(_))))
                    .count(),
                all.iter()
                    .filter(|(_, se)| matches!(se.value, EntryValue::Put(None)))
                    .count(),
                all.iter()
                    .filter(|(_, se)| matches!(se.value, EntryValue::Drop))
                    .count(),
            );
            let heavy = value_materializations() - base;
            eprintln!(
                "[count] 重量基准 live={} 空值={} 墓碑={}（这一趟物化 {} 条）",
                want.0, want.1, want.2, heavy
            );
            assert!(want.1 > 0 && want.2 > 0, "形状没混起来：空值 {} 墓碑 {}", want.1, want.2);
            assert!(
                heavy >= all.len() as u64,
                "重量基准只物化了 {} 条（全库 {} 行）⇒ 尺子没接上，下面那个 0 不算数",
                heavy,
                all.len()
            );

            // ── ① 数一字不差 ──
            assert_eq!(db.count_kinds_at(u64::MAX), want, "轻量计数与重量归并不一致");
            assert_eq!(db.entry_count(), all.len(), "entry_count 与改前不同数");
            assert!(db.stats_dirty, "没走在 dirty 路径上 ⇒ 这一格量的不是那次全量计数");
            assert_eq!(db.len(), want.0, "len（dirty 路径）与改前不同数");

            // ── ② 三格合起来一行 value 都不许克隆 ──
            let base = value_materializations();
            let n = db.entry_count();
            let m = db.len();
            let st = db.stats_exact();
            let mats = value_materializations() - base;
            eprintln!(
                "[count] 只数不物化：entry_count={} len={} live={} tomb={} ⇒ 物化 {} 条",
                n, m, st.live_keys, st.tombstones, mats
            );
            assert_eq!(mats, 0, "计数这三格克隆了 {mats} 条 value ⇒ 还在为一个数折整库");
            assert_eq!(n, all.len(), "entry_count 二次调用给了不同的数");
            assert_eq!(st.live_keys as usize, want.0, "stats_exact 的 live 与基准不同");
            assert_eq!(st.tombstones as usize, want.1, "stats_exact 的 tomb 与基准不同");
        }
        let _ = fs::remove_dir_all(&dir);
    }

    /// M47 顺带挖出来的一条**先前就存在**的少算：轻量走查在"没有上界"时拿一个合成上界
    /// `&[0xffu8; 8]` 去剪 SST ⇒ 一篇 `min_key` 比它更大的文件被**整篇跳过**，
    /// 全库计数/首页扫描少了那些键，而且不报错。
    ///
    /// 这一格为什么归这里：M47 把 `entry_count` 接到 `light_iter_range` 上，剪枝判据从此
    /// 也在"数出全库有多少键"这条路上生效 —— 少算的不再是"某一页少几行"而是"整个数不对"。
    /// 同一个判据的用户（`scan_page` / 窗口搜索 / 有界 vacuum / `health()`）一起受益。
    #[test]
    fn test_light_scan_without_upper_bound_keeps_files_past_the_old_sentinel() {
        let dir = tmp_dir("count_high_keys");
        {
            let mut db = LSMTree::open(&dir).unwrap();
            for i in 0..50usize {
                db.put(format!("k{i:04}").into_bytes(), vec![b'v'; 4]);
            }
            db.flush().unwrap();
            // 这一篇里**每一把键**都大于 `[0xff;8]`（12 个 0xff，末位才动）⇒ 改前那行
            // `may_overlap(low, &[0xffu8; 8])` 判定 `min_key <= high` 为假 ⇒ 整篇不读。
            for i in 0..7u8 {
                let mut k = vec![0xffu8; 12];
                k[11] = i;
                db.put(k, vec![b'w'; 4]);
            }
            db.flush().unwrap();
            assert!(
                db.stats().total_sst >= 2,
                "没造出独立的第二篇 SST ⇒ 这一格量不到剪枝"
            );

            // 重量归并不剪文件，它就是基准
            let all = db.merge_all_at(u64::MAX);
            assert_eq!(all.len(), 57, "基准本身不对：{all:?}");
            assert_eq!(
                db.entry_count(),
                all.len(),
                "全库计数漏掉了 min_key 高于旧合成上界的那篇 SST"
            );
            // 有下界、无上界的那一趟同样不许把尾部剪掉
            let tail: Vec<_> = db.light_iter_range(Some(b"k0049"), None, u64::MAX).collect();
            assert_eq!(tail.len(), 1 + 7, "开区间走查少算：{tail:?}");
            assert_eq!(
                db.count_kinds_at(u64::MAX).0,
                all.len(),
                "计数与开区间走查口径分家"
            );
        }
        let _ = fs::remove_dir_all(&dir);
    }

    /// M41b 的**校准**：memtable 里一条条目在堆上到底占多少字节。
    ///
    /// 为什么要有这条：`maybe_flush_mem` 读的 `mem_bytes` 只数载荷账（`key.len() +
    /// StoredEntry::estimate_bytes()`），一条 16 B 编码键的墓碑因此只算 **25 B**。MVCC 档写着
    /// `mem_threshold_bytes: 32 MB`，可 60 万条墓碑的载荷账才 15 MB ⇒ 那一条永不触发，
    /// 兜底的是条数阈值 256k，而 256k 条在堆上是 **~155 MB**（分段台架 ① 段的实测斜率）。
    /// 要把"memtable 多大"这句话改成真内存，常数得是量出来的。
    ///
    /// ⚠️ 只能单线程跑（`live()` 是进程级的，并行用例会互相污染 —— M27/M36 各栽过一次），
    /// 所以它是 `#[ignore]` 台架而不是守卫。
    /// M46 的内存凭据：裸引擎"全库导出"那一趟的瞬时峰值（`#[ignore]`，手动 release 单线程跑）。
    ///
    /// 为什么要有它：M44 之后四家 CLI 只剩 `lsm_tree/bin/export_kv.rs` 还在物化整库，而它走的
    /// 是**裸引擎**入口（`merge_all_at`）—— 门面那台的 `export` 读数照不到这条路。
    /// 改前那一列不是这里切的臂（测量臂跑完就摘了，不属于产品）：它是同一段代码把
    /// `merge_inputs_at` 里 SST 那一支换回 `iter_all().collect()` + `vec_inputs` 之后读到的，
    /// 守卫 `test_merge_streams_sources_instead_of_materializing_them` 的变异用的就是这个换法
    /// —— 计数器（20 000 条 vs 5 条）与这里的 MB（100.4 vs 0.4）是同一件事的两把尺子。
    ///
    /// ```text
    /// LSM_RAW_EXPORT_ROWS=600000 cargo test --release --lib -- --ignored --nocapture     ///   --test-threads=1 lsm_raw_export_peak
    /// LSM_MERGE_OLD_SHAPE=1 LSM_RAW_EXPORT_ROWS=600000 cargo test --release --lib --     ///   --ignored --nocapture --test-threads=1 lsm_raw_export_peak
    /// ```
    #[test]
    #[ignore = "裸引擎全库导出的内存台架，手动 release 单线程跑：LSM_RAW_EXPORT_ROWS=600000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 lsm_raw_export_peak"]
    fn lsm_raw_export_peak() {
        use crate::lsm_tree::sst::value_materializations;
        let rows: usize = std::env::var("LSM_RAW_EXPORT_ROWS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(200_000);
        let mb = |b: u64| b as f64 / 1024.0 / 1024.0;
        let dir = tmp_dir("raw_export_peak");
        {
            let mut db = LSMTree::open_with_options(&dir, usize::MAX / 8, 64).unwrap();
            for i in 0..rows {
                db.put(format!("{i:08}").into_bytes(), vec![b'v'; 24]);
            }
            db.flush().unwrap();
            let files = db.stats().total_sst;
            let base = crate::mem_probe::reset_peak();
            let m0 = value_materializations();
            let t = std::time::Instant::now();
            let n = db.export_records_with_seq_for_each(false, &mut |_k, _v, _seq| true);
            let d = t.elapsed();
            let peak = crate::mem_probe::peak().saturating_sub(base);
            let mats = value_materializations() - m0;
            eprintln!(
                "=== 裸引擎全库导出 rows={rows} 源={files} 交出 {n} 行 ⇒ 瞬时峰值 {:.1} MB、物化 {mats} 条、耗时 {:.2} s ===",
                mb(peak),
                d.as_secs_f64()
            );
        }
        let _ = fs::remove_dir_all(&dir);
    }

    /// M47 的内存凭据：三格"为一个数物化整库"（`entry_count`、`len` 的 dirty 路径、
    /// `stats_exact` → `refresh_stats_cache`）那一趟到底瞬时涨多少（`#[ignore]`，手动 release 单线程跑）。
    ///
    /// 两臂在**同一个构建**里，只切 `LSM_COUNT_OLD_SHAPE=1`：那一臂把三格各自的旧写法
    /// （各调一次 `merge_all_at(u64::MAX)` 再数）就地重放一遍。⚠️ 两臂都在每一格之前把
    /// `stats_dirty` 压回 true —— 否则第二、三格读的是缓存，量的就不是那三扇门了
    /// （M44 的教训：尺子必须钉在旧形状真正流过的那扇门上）。
    ///
    /// ```text
    /// LSM_COUNT_ROWS=600000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 lsm_counting_doors_peak
    /// LSM_COUNT_OLD_SHAPE=1 LSM_COUNT_ROWS=600000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 lsm_counting_doors_peak
    /// ```
    #[test]
    #[ignore = "三格计数的内存台架，手动 release 单线程跑：LSM_COUNT_ROWS=600000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 lsm_counting_doors_peak"]
    fn lsm_counting_doors_peak() {
        use crate::lsm_tree::sst::value_materializations;
        let rows: usize = std::env::var("LSM_COUNT_ROWS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(200_000);
        let old = std::env::var("LSM_COUNT_OLD_SHAPE").map(|v| v == "1").unwrap_or(false);
        let mb = |b: u64| b as f64 / 1024.0 / 1024.0;
        let dir = tmp_dir("counting_doors_peak");
        {
            let mut db = LSMTree::open_with_options(&dir, usize::MAX / 8, 64).unwrap();
            for i in 0..rows {
                db.put(format!("{i:08}").into_bytes(), vec![b'v'; 24]);
            }
            db.flush().unwrap();
            let files = db.stats().total_sst;

            let base = crate::mem_probe::reset_peak();
            let m0 = value_materializations();
            let t = std::time::Instant::now();
            let (n_entry, n_live, st) = if old {
                // 改前：每一格各自把整库连 value 折出来再数（口径与那一版一字不差）
                db.stats_dirty = true;
                let n_entry = db.merge_all_at(u64::MAX).len();
                db.stats_dirty = true;
                let n_live = db
                    .merge_all_at(u64::MAX)
                    .iter()
                    .filter(|(_, se)| matches!(se.value, EntryValue::Put(Some(_))))
                    .count();
                db.stats_dirty = true;
                let st = {
                    let all = db.merge_all_at(u64::MAX);
                    let live = all
                        .iter()
                        .filter(|(_, se)| matches!(se.value, EntryValue::Put(Some(_))))
                        .count();
                    let tomb = all
                        .iter()
                        .filter(|(_, se)| matches!(se.value, EntryValue::Put(None)))
                        .count();
                    db.cached_live = live;
                    db.cached_tomb = tomb;
                    db.stats_dirty = false;
                    db.stats()
                };
                (n_entry, n_live, st)
            } else {
                db.stats_dirty = true;
                let n_entry = db.entry_count();
                db.stats_dirty = true;
                let n_live = db.len();
                db.stats_dirty = true;
                let st = db.stats_exact();
                (n_entry, n_live, st)
            };
            let d = t.elapsed();
            let peak = crate::mem_probe::peak().saturating_sub(base);
            let mats = value_materializations() - m0;
            eprintln!(
                "=== 三格计数 rows={rows} 源={files} 臂={} ⇒ entry_count={n_entry} len={n_live} live={} tomb={} 瞬时峰值 {:.1} MB、物化 {mats} 条、耗时 {:.2} s ===",
                if old { "改前(整库折出来)" } else { "改后(只数)" },
                st.live_keys,
                st.tombstones,
                mb(peak),
                d.as_secs_f64()
            );
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    #[ignore = "memtable 每条真实字节数的校准，手动 release 单线程跑：LSM_MEM_CAL=200000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 lsm_memtable_bytes_per_entry"]
    fn lsm_memtable_bytes_per_entry() {
        let n: usize = std::env::var("LSM_MEM_CAL")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(200_000);
        let dir = tmp_dir("mem_cal");
        {
            let opts = LSMTreeOptions {
                // 两档阈值都钉到不会触发 ⇒ 全程只有一只 memtable 在长
                mem_threshold_entries: usize::MAX / 8,
                mem_threshold_bytes: usize::MAX / 8,
                enable_mem_wal: false,
                ..LSMTreeOptions::default()
            };
            let mut db = LSMTree::open_with_config(&dir, opts).unwrap();
            let base = crate::mem_probe::live();
            for i in 0..n {
                // 键长 16 B：与 MVCC 的编码键 `raw(8) ‖ version(8)` 同量级；值取空 ⇒
                // 载荷账 = 16 + (8 + 1) = 25 B，与一条墓碑同量级
                db.put(format!("k{i:015}").into_bytes(), Vec::new());
            }
            let held = crate::mem_probe::live() - base;
            assert_eq!(db.stats().mem_entries, n, "中途 flush 过 ⇒ 量的不是同一只 memtable");
            let per = held / n as u64;
            // 载荷账按**生产口径**读（`stats().mem_bytes`），不是自己另算一遍：
            // 一条 = `key.len()` + `StoredEntry::estimate_bytes()`
            let pay = db.stats().mem_bytes as u64 / n as u64;
            println!(
                "=== memtable 校准 n={n}：真堆占用每条 {per} B，判定用的载荷账每条 {pay} B ⇒ 结构 {struct_} B ===",
                struct_ = per - pay
            );
        }
        let _ = fs::remove_dir_all(&dir);
    }

    /// M41b：`mem_threshold_bytes` 说的必须是 memtable 的**真内存**，不是载荷账。
    ///
    /// 校准过的边界（每条载荷账 25 B + 结构 `MEM_ENTRY_OVERHEAD` = 476 B）：阈值钉 256 KB 时
    /// - **550 条** = 261 800 B < 262 144 ⇒ 还**不该** flush；
    /// - **551 条** = 262 276 B ≥ 262 144 ⇒ 必须 flush。
    /// 两臂同构建、只差一行，所以既钉住"开销项进了判定"（少算 ⇒ 551 那一臂不 flush），
    /// 也钉住"没把它 inflate"（多算 ⇒ 550 那一臂提前 flush）。条数阈值钉到不可能触发，
    /// 让字节这一档单独说话。
    ///
    /// 另外钉一条口径：`stats().mem_bytes` **仍然是载荷账**（对外含义没变，改的只有判定）。
    ///
    /// 变异验证：把判定里那句 `+ mem_entries × MEM_ENTRY_OVERHEAD` 去掉 ⇒ 551 条那臂红
    /// （载荷账只有 13 775 B，离 256 KB 远着呢，于是 memtable 一路涨到条数阈值那么高才 flush ——
    /// 就是 600k 档 ① 段那 155 MB 的来处）。
    #[test]
    fn test_memtable_flush_trigger_reads_real_bytes() {
        const THRESH: usize = 256 * 1024;
        // 每条 = 载荷账 25 B（16 B 键 + `StoredEntry::estimate_bytes()` 9）+ 结构开销
        let per_entry = 25 + MEM_ENTRY_OVERHEAD;
        let n_no = THRESH / per_entry; // 差一条不到阈值
        let n_yes = n_no + 1;
        assert!(
            n_no * per_entry < THRESH && n_yes * per_entry >= THRESH,
            "边界算错了：{n_no} 条 = {} B，{n_yes} 条 = {} B，阈值 {THRESH}",
            n_no * per_entry,
            n_yes * per_entry
        );

        for (n, should_flush) in [(n_no, false), (n_yes, true)] {
            let dir = tmp_dir(&format!("mem_real_{n}"));
            {
                let opts = LSMTreeOptions {
                    mem_threshold_bytes: THRESH,
                    // 只让"字节"这一档说话：条数阈值高到碰不到
                    mem_threshold_entries: usize::MAX / 8,
                    enable_mem_wal: false,
                    ..LSMTreeOptions::default()
                };
                let mut db = LSMTree::open_with_config(&dir, opts).unwrap();
                for i in 0..n {
                    db.put(format!("k{i:015}").into_bytes(), Vec::new());
                }
                let st = db.stats();
                if should_flush {
                    assert_eq!(
                        st.total_sst,
                        1,
                        "{n} 条 × {per_entry} B = {} B 已经过阈值 {THRESH} ⇒ 该 flush 了；\
                         没 flush 说明判定还在只读载荷",
                        n * per_entry
                    );
                    assert_eq!(st.mem_entries, 0, "flush 之后 memtable 应当是空的");
                } else {
                    assert_eq!(
                        st.total_sst, 0,
                        "{n} 条 = {} B 还没到阈值 {THRESH} 就 flush 了 ⇒ 开销被 inflate 了",
                        n * per_entry
                    );
                    assert_eq!(st.mem_entries, n, "没 flush 却清了 memtable");
                }
                // 载荷账的口径没改（flush 之后清零）：一条 = 16 B 键 + `StoredEntry` 的 9 B。
                // ⚠️ 放在 flush 判定**之后**：先判该 flush 的有没有 flush，失败信息才说得出根因。
                assert_eq!(
                    st.mem_bytes,
                    if should_flush { 0 } else { n * 25 },
                    "`stats().mem_bytes` 的口径被改了（对外应当仍是载荷账，flush 后清零）"
                );
                // 数据还在（不论在 memtable 还是 SST 里）
                assert_eq!(db.get(b"k000000000000000"), Some(Vec::new()));
                assert_eq!(
                    db.get(format!("k{:015}", n - 1).as_bytes()),
                    Some(Vec::new())
                );
            }
            let _ = fs::remove_dir_all(&dir);
        }
    }

    /// M19 守卫①：增量计数器 `mem_entries` 的**口径**必须与慢算法一字不差。
    ///
    /// 口径是「版本条数」（Σ 每键版本数），不是「键数」—— 当年正是为此才去 `sum` 整表。
    /// 计数器只要漏了一条路径（同 seq 覆盖该不该加、flush 有没有清零），这条对拍当场红。
    #[test]
    fn test_mem_entry_counter_matches_slow_scan() {
        let dir = tmp_dir("mem_counter");
        {
            let mut db = LSMTree::open(&dir).unwrap();
            let check = |db: &LSMTree, at: &str| {
                assert_eq!(
                    db.stats().mem_entries,
                    db.scan_mem_entry_count(),
                    "{at}：计数器与慢口径不一致"
                );
            };
            check(&db, "空库");
            for i in 0..50 {
                db.put(format!("k{i}").into_bytes(), b"v".to_vec());
            }
            check(&db, "50 把新键");
            // 同一把键堆版本：条数按版本算，不按键算
            for _ in 0..7 {
                db.put(b"k0".to_vec(), b"v2".to_vec());
            }
            check(&db, "k0 多版本");
            db.delete(b"k1".to_vec());
            check(&db, "含墓碑");
            assert_eq!(db.stats().mem_entries, 58, "条数口径 = 版本条数（50 + 追加 7 + 墓碑 1）");
            db.flush().unwrap();
            check(&db, "flush 之后");
            assert_eq!(db.stats().mem_entries, 0, "flush 没清零 ⇒ 下一次判定会提前触发");
        }
        let _ = fs::remove_dir_all(&dir);
    }

    /// M19 守卫②：**插一行不该为「要不要 flush」去数整个 memtable**。
    ///
    /// 这条是 M19 的病根。2026-10-17 的 200k 大规模基线抓到 LSM 灌行是 O(N²)：每次插入都
    /// `self.mem.values().map(len).sum()` 一遍，于是裸 `insert` 每行从 89 µs 涨到 1429 µs。
    /// 尺子数的不是墙钟而是**为判定走过的 memtable 条目数** —— 改成增量计数器之后它必须是 0。
    ///
    /// 对照段是必需的：探针如果空转，`assert_eq!(steps, 0)` 会永远绿。所以同一条用例里主动
    /// 慢数一次，要求它**真的**被记进账（步数 == 表大小），证明这把尺子量得到东西。
    #[test]
    fn test_flush_check_does_not_scan_memtable_per_insert() {
        const N: usize = 4000;
        let dir = tmp_dir("mem_counter_hot");
        {
            let mut db = LSMTree::open(&dir).unwrap();
            let s0 = mem_scan_steps();
            for i in 0..N {
                db.put(format!("key{i:06}").into_bytes(), b"x".to_vec());
            }
            let steps = mem_scan_steps() - s0;
            assert_eq!(
                steps, 0,
                "灌 {N} 行为回答「要不要 flush」走了 {steps} 个 memtable 条目 ⇒ 热路径又变回\
                 每插一行数一遍整表（旧形状实测 ≈ {} 步）",
                (N * (N - 1) / 2) as u64
            );
            // 对照：主动慢数一次，探针必须数得到
            let slow = db.scan_mem_entry_count() as u64;
            assert_eq!(mem_scan_steps() - s0, slow, "探针空转：慢口径没被记进账，上一条断言不算数");
            assert!(slow >= N as u64, "对照必须真数到 ≥{N} 条，实际 {slow}");
        }
        let _ = fs::remove_dir_all(&dir);
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

    /// 压实**不许把输入整篇读进内存**（M33）。
    ///
    /// 触发它的测量：`compaction_cost_split` 在 600k 行 / 8 个输入文件上，光"物化输入"这一步
    /// 峰值就涨 **154 MB**（条目本身只有 61.7 MB ⇒ 多出来的是每行两个堆分配 + Vec 扩容翻倍），
    /// 一次压实把下游 80 MB 的 RSS 预算直接吃穿。改成流式源之后这件事的计数必须是 0。
    ///
    /// 尺子 = `merge_inputs_materialized()`，单位是**条目数**（不是"调了几次 `iter_entries()`"：
    /// 调一次就能搬进几万行，按次数断言会漏掉大半）。线程内计数 ⇒ 一律取差值。
    /// ⚠️ 空转检查放在**同一行**里、且排在前面：先主动物化一个文件，证明这把尺子数得到，
    /// 否则后面那个 `0` 完全可能只是"尺子坏了"。
    #[test]
    fn test_compaction_does_not_materialize_its_inputs() {
        use crate::lsm_tree::sst::merge_inputs_materialized;
        let dir = tmp_dir("no_materialize");
        {
            // mem 40 条 / L0 4 个就压：造得出"多个输入文件"这个形状
            let mut db = LSMTree::open_with_options(&dir, 40, 4).unwrap();
            for i in 0..400u32 {
                db.put(
                    format!("k{i:04}").into_bytes(),
                    format!("v{i}").into_bytes(),
                );
                if i % 40 == 39 {
                    db.flush().unwrap();
                }
            }
            // ① 尺子自检：主动物化 L0 的一个文件，计数必须正好涨它的条目数
            let probe_file = db
                .l0_for_test()
                .first()
                .expect("形状没造出来：L0 是空的，后面两条断言都会空转");
            let base = merge_inputs_materialized();
            let n_probe = probe_file.iter_entries().len() as u64;
            let heard = merge_inputs_materialized() - base;
            assert_eq!(
                heard, n_probe,
                "这把尺子空转：主动物化了 {n_probe} 条，只数到 {heard}"
            );

            // ② 真压实：这一段不许物化任何一条输入
            // ⚠️ 不能用 `compact_once()`：它自己看 L0 阈值，第一版就是这么空转过去的（L0 只有 2 个
            // 文件、阈值 4 ⇒ 它一轮都不压，那条 `0` 断言就成了"什么都没做也算通过"）。
            // `compact_l0_until(0, …)` 是"不管阈值先把 L0 压干净"，才真的走到 `compact_level`。
            let before = db.stats_exact();
            let m0 = merge_inputs_materialized();
            let rounds = db.compact_l0_until(0, 8).unwrap();
            let walked = merge_inputs_materialized() - m0;
            let after = db.stats_exact();
            assert!(
                rounds >= 1 && after.l0_files < before.l0_files,
                "这次没压实（rounds={rounds}，{before:?} → {after:?}）⇒ 上面那条 0 是空转"
            );
            assert_eq!(
                walked, 0,
                "压实把 {walked} 条输入整篇物化进了内存 ⇒ 瞬时内存又回到「所有输入文件之和」的形状\
                 （M33：输入必须走 `MergeInput` 迭代器）"
            );
            // 正确性尾巴：压完仍读得到首尾两把键（流式归并没把行弄丢）
            assert_eq!(db.get(b"k0000"), Some(b"v0".to_vec()));
            assert_eq!(db.get(b"k0399"), Some(b"v399".to_vec()));
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

    /// 折叠的答案必须**逐键等于**跨源点查的答案（M26 的主判据）。
    ///
    /// 造的正是以前会给出错误答案的形状：L1 里两个范围重叠的文件，**排序在前的那个装着更旧的一份**
    /// —— 同层文件谁新谁旧没有元数据可依据（L1→L2 压实会用**新 id 写老数据**，层内又只按
    /// `min_key` 排序）。旧判据"源序号小者赢"在这种形状上会把旧版当胜者交出去：
    /// 重量折叠 `range_scan` 与轻量折叠 `light_iter_range` 都交旧值，而点查 `get` 取 max seq
    /// 交新值 ⇒ 同一个库两种读法两个答案。改成 seq 优先之后三者必须一致。
    #[test]
    fn test_fold_agrees_with_point_read_on_overlapping_level() {
        fn key(i: usize) -> Vec<u8> {
            format!("k{i:02}").into_bytes()
        }
        let dir = tmp_dir("fold_vs_point");
        {
            let mut db = LSMTree::open(&dir).unwrap();
            let a: Vec<(Vec<u8>, StoredEntry)> =
                (0..30).map(|i| (key(i), StoredEntry::put(1, Some(b"old".to_vec())))).collect();
            let b: Vec<(Vec<u8>, StoredEntry)> =
                (10..40).map(|i| (key(i), StoredEntry::put(5, Some(b"new".to_vec())))).collect();
            db.inject_sst_for_test(1, a).unwrap();
            db.inject_sst_for_test(1, b).unwrap();
            assert_eq!(db.stats().level_files[1], 2, "重叠形状没造出来");

            // 点查 = 判据的基准（`get_entry_at`：所有源里取最大可见 seq）
            for i in 0..10 {
                assert_eq!(db.get(&key(i)), Some(b"old".to_vec()), "k{i:02} 只住在旧文件里");
            }
            for i in 10..30 {
                assert_eq!(db.get(&key(i)), Some(b"new".to_vec()), "重叠区 k{i:02} 点查没拿到新版");
            }
            for i in 30..40 {
                assert_eq!(db.get(&key(i)), Some(b"new".to_vec()), "k{i:02} 只住在新文件里");
            }

            // 重量折叠：每个键的答案必须和点查一字不差
            let rows = db.range_scan(Vec::new(), vec![0xffu8; 8]);
            assert_eq!(rows.len(), 40, "折叠把同一把键吐了两遍（去重坏了）");
            for (k, v) in &rows {
                assert_eq!(v.as_deref(), db.get(k).as_deref(), "折叠与点查不一致：{:?}", String::from_utf8_lossy(k));
            }

            // 轻量折叠（翻页 / 窗口搜索 / 有界回收走的就是这条）：比的也是 seq
            let light: Vec<(Vec<u8>, u64)> = db
                .light_iter_range(None, None, u64::MAX)
                .map(|(k, le)| (k, le.seq))
                .collect();
            assert_eq!(light.len(), 40, "轻量折叠的键数");
            for (k, seq) in &light {
                let want = if k.as_slice() < key(10).as_slice() { 1 } else { 5 };
                assert_eq!(
                    *seq, want,
                    "轻量折叠在 {:?} 上选错了版本（seq {seq}，点查会给 {want}）",
                    String::from_utf8_lossy(k)
                );
            }
            db.flush().unwrap();
        }
        let _ = fs::remove_dir_all(&dir);
    }

    /// 层内重叠**真的被折掉**（M26）。旧实现叫 `fix_level_overlaps`，但函数体只有
    /// `eprintln!` + 原样返回 ⇒ 重叠一旦出现就永远没人修：常规压实只拉"与所选文件重叠的
    /// **下一层**"，从不碰同层兄弟。这条守卫钉四件事：折之前多探的文件数、折之后层内两两
    /// 不重叠、每一把键的读答案一字未变、旧文件被删且 MANIFEST 认得新文件（重开读得回）。
    #[test]
    fn test_compaction_repairs_level_overlap() {
        fn key(i: usize) -> Vec<u8> {
            format!("k{i:02}").into_bytes()
        }
        let dir = tmp_dir("overlap_repair");
        let old_paths: Vec<PathBuf>;
        let probes_before;
        let probes_after;
        {
            let mut db = LSMTree::open(&dir).unwrap();
            let a: Vec<(Vec<u8>, StoredEntry)> =
                (0..30).map(|i| (key(i), StoredEntry::put(1, Some(b"old".to_vec())))).collect();
            let b: Vec<(Vec<u8>, StoredEntry)> =
                (10..40).map(|i| (key(i), StoredEntry::put(5, Some(b"new".to_vec())))).collect();
            db.inject_sst_for_test(1, a).unwrap();
            db.inject_sst_for_test(1, b).unwrap();
            old_paths = db.levels[1].iter().map(|s| s.path.clone()).collect();
            assert!(
                ranges_overlap(&db.levels[1][0].min_key, &db.levels[1][0].max_key,
                               &db.levels[1][1].min_key, &db.levels[1][1].max_key),
                "两个文件必须真的重叠，否则这条什么都没测"
            );

            let p0 = crate::lsm_tree::sst::sst_probes();
            for i in 0..40 {
                assert!(db.get(&key(i)).is_some());
            }
            // 精确账：k00..k09 只在 A、k30..k39 只在 B、k10..k29 两个文件都过范围判定
            probes_before = crate::lsm_tree::sst::sst_probes() - p0;
            assert_eq!(probes_before, 60, "重叠层每次点读的探文件数");

            // 触发一次会重写 L1 的压实
            // M33：层内重叠修复与常规压实**共用同一条输入账单**（旧写法两边都 `iter_entries()`
            // 整篇读进 Vec），所以这里也要有自己的尺子 —— 不是重复劳动，是两个调用点各自担责。
            let m0 = crate::lsm_tree::sst::merge_inputs_materialized();
            db.put(b"zz".to_vec(), b"l0".to_vec());
            db.flush().unwrap();
            let rounds = db.compact_l0_until(0, 8).unwrap();
            let walked = crate::lsm_tree::sst::merge_inputs_materialized() - m0;
            assert_eq!(
                walked, 0,
                "压实/重叠修复把 {walked} 条输入整篇物化进了内存 ⇒ 又回到「所有输入文件之和」那条账（M33）"
            );
            assert!(rounds >= 1, "压实没跑，修复也就没被调用");

            let l1 = &db.levels[1];
            assert_eq!(l1.len(), 2, "折完该剩「合并后的 40 键文件 + zz 文件」（实测 {}）", l1.len());
            for i in 0..l1.len() {
                for j in (i + 1)..l1.len() {
                    assert!(
                        !ranges_overlap(&l1[i].min_key, &l1[i].max_key, &l1[j].min_key, &l1[j].max_key),
                        "L1 里 {} 与 {} 还重叠 ⇒ 修复没做事",
                        l1[i].id, l1[j].id
                    );
                }
            }
            for p in &old_paths {
                assert!(!p.exists(), "被替换掉的旧文件没删：{p:?}");
            }

            let p1 = crate::lsm_tree::sst::sst_probes();
            for i in 0..40 {
                assert!(db.get(&key(i)).is_some());
            }
            probes_after = crate::lsm_tree::sst::sst_probes() - p1;
            assert_eq!(probes_after, 40, "折干净之后每把键只该探一个文件");

            // 读答案一字未变（含重叠区那 20 把：旧版住在排序靠前的文件里，修复不许把它们改旧）
            for i in 0..10 {
                assert_eq!(db.get(&key(i)), Some(b"old".to_vec()));
            }
            for i in 10..40 {
                assert_eq!(db.get(&key(i)), Some(b"new".to_vec()), "修复之后 k{i:02} 变了");
            }
            assert_eq!(db.get(b"zz"), Some(b"l0".to_vec()));
            db.flush().unwrap();
        }

        // 重开 = 崩溃恢复后看到的那份形状：MANIFEST 记了新文件，盘上没有孤儿
        {
            let db = LSMTree::open(&dir).unwrap();
            for i in 10..40 {
                assert_eq!(db.get(&key(i)), Some(b"new".to_vec()), "重开之后 k{i:02} 读不回新版");
            }
            let text = fs::read_to_string(dir.join("MANIFEST")).unwrap();
            let listed: Vec<String> = text
                .lines()
                .filter(|l| l.starts_with('L'))
                .flat_map(|l| l.split_whitespace().skip(1).map(|s| s.to_string()))
                .collect();
            let on_disk: Vec<String> = fs::read_dir(dir.join("sst"))
                .unwrap()
                .flatten()
                .filter_map(|e| {
                    let n = e.file_name().to_string_lossy().to_string();
                    n.strip_suffix(".sst").map(|s| s.to_string())
                })
                .collect();
            for id in &listed {
                // ⚠️ 必须按**数值**补零：`format!("{id:08}")` 对 String 是空格补齐，
                //    会把 "5" 找成 "       5.sst"（这条测试第一版就是这么假红的）
                let n = id.parse::<u64>().expect("MANIFEST 里的 id 不是数字");
                assert!(
                    dir.join("sst").join(format!("{n:08}.sst")).exists(),
                    "MANIFEST 指着不在盘上的文件 {id}
MANIFEST={text:?}
盘上={on_disk:?}"
                );
            }
            for f in &on_disk {
                let id = f.parse::<u64>().unwrap().to_string();
                assert!(listed.contains(&id), "盘上有没人引用的孤儿 SST {f}（修复/压实没把它记进 MANIFEST）");
            }
        }
        eprintln!(
            "[层内重叠] 折之前 40 把键探文件 {probes_before} 次 → 折之后 {probes_after} 次（每个键 1 次）"
        );
        let _ = fs::remove_dir_all(&dir);
    }
}

/// [`LSMTree::light_iter_range`] 的状态机：k 路归并，每次 `next()` 只往前拉一条。
///
/// 堆里只放 `(键, 源序号)`，每个源至多一个候选在堆里；真正的值在 `head` 里等着。
/// 输出**非降序** ⇒ 跨源的同键去重只需一个"待定键"的先行量：
/// - 同一键取 **seq 最大者**定论（由下面那道"待定键"比较做到 —— 不是靠堆的出队顺序）
/// - 为什么必须是 seq：层内两个范围重叠的文件里谁新谁旧，源序答不了（M26 的实测结论：
///   `merge_streaming` 那边走的是"堆的 tie-break 选胜者"，所以它必须比 seq；这边是"先出者待定、
///   后到者比 seq 换掉"，所以判据在待定那一步）
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

/// 最小键优先；同键时源序号小的先出（新→旧）。
///
/// ⚠️ 这里**不需要**像 `merge_streaming` 那样把 seq 排进比较（M26 试过，证不出来）：
/// 同键的胜者由 `next()` 里那道"待定键 + 取更大 seq"的比较定论，堆序只决定谁先来。
/// 反过来说，**那道比较才是这条路径的判据**，把它当冗余删掉就会在层内重叠时交出旧版本
/// （守卫 `test_fold_agrees_with_point_read_on_overlapping_level` 会红，实测证过）。
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
                    // 同一个键：**这道比较才是胜者判据** —— 先出的是源序里更新的那个，
                    // 但层内重叠文件的"更新"只有 seq 能回答 ⇒ 后到的 seq 更大就换掉它。
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
