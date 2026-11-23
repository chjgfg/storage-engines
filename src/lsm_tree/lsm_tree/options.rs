//! 本文件负责参数默认值与配置策略。

// ─── Options ─────────────────────────────────────────────────────────────────

/// 引擎可调参数（可在运行时通过 [`crate::lsm_tree::lsm_tree::LSMTree::set_options`] 热更新部分字段）
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
