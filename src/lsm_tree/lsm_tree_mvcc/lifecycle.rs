//! 本文件负责打开、配置、关闭及资源所有权。

use super::*;

impl MVCC {
    /// 打开/创建磁盘上的 MVCC 引擎（含 WAL 恢复）。失败时 panic（兼容旧代码）。
    pub fn open(dir: impl AsRef<Path>) -> Self {
        Self::try_open(dir)
            .map_err(|e| format!("MVCC::open 失败: {e}"))
            .unwrap()
    }

    /// 打开/创建，错误以 `io::Result` 返回（锁冲突、坏文件、IO 等）。
    pub fn try_open(dir: impl AsRef<Path>) -> std::io::Result<Self> {
        Self::try_open_with(dir, None)
    }

    /// 打开并覆盖 MVCC 档的旋钮；`None` = 全用 [`LSMTreeOptions::for_mvcc`]。
    ///
    /// 传进来的是**整份** options（不是差量），所以常规写法是用引擎自己的档当基线做
    /// struct-update，改哪个字段写在哪个字段上：
    ///
    /// ```no_run
    /// use storage_engines::lsm_tree::{MVCC, LSMTreeOptions};
    /// # use std::path::Path;
    /// let opts = LSMTreeOptions {
    ///     block_cache_blocks: 1024,
    ///     ..LSMTreeOptions::for_mvcc()
    /// };
    /// let db = MVCC::try_open_with(Path::new("./data/lsm_tree"), Some(opts)).unwrap();
    /// ```
    ///
    /// ⚠️ `for_mvcc` 档开着块缓存**自动档**（`block_cache_auto_max: 1024`），所以
    /// `block_cache_blocks` 在这里是**地板**而不是生效值：
    /// `生效容量 = clamp(库的 SST 块数, block_cache_blocks, block_cache_auto_max)`。
    /// 上面那个例子读作"至少 1024 块、最多 1024 块"= 钉死 1024。想要钉死别的数就把上限
    /// 一起改（`block_cache_auto_max: 0` = 关自动档，回到"配置值即生效值"）。
    ///
    /// 两个字段**传了也不算**，恒被钉回 `false`（见 [`MVCC::set_options`] 同处的理由）：
    /// `enable_mem_wal`、`enable_bg_compact`。
    pub fn try_open_with(
        dir: impl AsRef<Path>,
        overrides: Option<LSMTreeOptions>,
    ) -> std::io::Result<Self> {
        let db_dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&db_dir)?;

        // 锁文件路径规则是各引擎自己的（lsm: `dir/lsm.lock`），锁的**语义**在
        // [`crate::common::disk::FileLock`]（四引擎共用的一份实现）
        let lock = FileLock::try_acquire_at(&lock_path(&db_dir))?;
        let opts = pin_mvcc_invariants(overrides.unwrap_or_else(LSMTreeOptions::for_mvcc));
        let mut tree = LSMTree::open_with_config(&db_dir, opts)?;
        let mut wal = Wal::open(wal_path(&db_dir))?;

        let entries = wal.read_all()?;
        let plan = analyze_recovery(&entries);
        // 「有没有东西要重放」必须在插进 memtable 之前判 —— 之后 memtable 非空就再也分不出
        // 是 redo 撑的还是别人撑的（`pin_mvcc_invariants` 已把 LSM 自己的 mem WAL 关掉，
        // 所以空 plan ⇒ 空 memtable 这条推理在这里是成立的）。
        let has_redo = !plan.committed_writes.is_empty() || !plan.uncommitted_writes.is_empty();

        for (_xid, key, value) in &plan.committed_writes {
            tree.insert(key.clone(), value.clone());
        }
        for (_xid, key) in &plan.uncommitted_writes {
            let _ = tree.remove(key);
        }

        // 不再全表 scan_max_version：用 checkpoint / WAL xid / redo key / 持久化元数据
        let start_ver = compute_start_version(&db_dir, &plan);

        // **只有 redo 非空才 flush**。理由不是"省一次 fsync"：`LSMTree::flush()` 末尾无条件
        // 走 `maybe_compact()`，而压实是**无界的写路径工作**。实测（`reopen_shape_probe`，
        // release，600k 键 + bulk 收尾）：bulk 的 `finish()` 把 WAL 收干净了（redo 0 条），
        // 却把 L0 停在 `l0_compact_threshold`(8) 上 ⇒ 这次 open 自己把 85 MB 从 L0 搬进
        // L1/L2、写出 37 个文件。同一轮剖面 A/B（`uncond_flush` vs `cond_flush`）：总
        // **13 947 618.8 µs vs 8 362.3 µs = 1668×**，其中 flush 段 13 938 766.2 vs 0.0；
        // 门面台架同档跨轮读数是 17 487 030.8 → 9 106.7 µs。同一形状在 200k 只有 4 个 L0
        // 文件（<8）⇒ 不压 ⇒ 两臂同价 —— 这就是那条 1700× 悬崖的来源：**开关是 L0 文件数
        // 是否越线，跟"恢复"这件事本身没关系**。
        //
        // redo 非空时仍然必须 flush：`seal_wal_checkpoint` 随后就把 WAL 截断了，那些重放进
        // memtable 的行只有落了盘才不会跟着 WAL 一起消失（这条由
        // `test_open_persists_redo_before_truncating_wal` 钉着）。
        if has_redo {
            tree.flush()?;
        }
        // 先截断再写回 Checkpoint（原先 append→truncate 会把刚写的记录抹掉）
        seal_wal_checkpoint(&mut wal, start_ver)?;
        persist_mvcc_next(&db_dir, start_ver);

        Ok(Self {
            kv: Arc::new(Mutex::new(tree)),
            wal: Arc::new(Mutex::new(wal)),
            active_txn: Arc::new(Mutex::new(HashMap::new())),
            next_version: Arc::new(AtomicU64::new(start_ver)),
            wal_limit: Arc::new(AtomicU64::new(MVCC::WAL_CHECKPOINT_BYTES_DEFAULT)),
            vac: Arc::new(VacuumScheduler::new(MVCC::VACUUM_AUTO_SLICE_DEFAULT)),
            db_dir: Arc::new(db_dir),
            _lock: lock,
        })
    }

    /// 在系统临时目录创建匿名库（测试用）
    pub fn new() -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("lsm_mvcc_anon_{}_{}", std::process::id(), nanos));
        Self::open(path)
    }

    pub fn begin_transaction(&self) -> Transaction {
        self.tick_auto_vacuum();
        Transaction::begin(
            self.kv.clone(),
            self.wal.clone(),
            self.active_txn.clone(),
            self.next_version.clone(),
            self.wal_limit.clone(),
            self.vac.clone(),
            self.db_dir.clone(),
        )
    }

    /// 默认切片宽度 = 16 Ki 行；按完整键组结束，所以超长链可超过预算。
    /// 多源归并仍有源状态和定位成本；这是行预算，不是 IO/内存/时间硬上限。
    pub const VACUUM_AUTO_SLICE_DEFAULT: u64 = 16 * 1024;

    /// 开启 bulk load：单写者、无 WAL、无冲突检测。
    ///
    /// **崩溃不安全**：finish 之前进程挂掉可能丢数据。
    /// 导入结束后必须 `finish()` / `finish_with_compact(...)`。
    ///
    /// 为 10M 级吞吐：`put_batch_owned` 排序后直接写 L0 SST（跳过 MemTable）。
    pub fn begin_bulk(&self) -> BulkLoader {
        {
            let mut kv = self.kv.lock().unwrap();
            let mut o = kv.options().clone();
            o.mem_threshold_bytes = 128 * 1024 * 1024;
            o.mem_threshold_entries = 1_000_000;
            o.l0_compact_threshold = 16;
            o.l0_slowdown_trigger = 32;
            o.l0_stop_trigger = 64;
            o.target_sst_bytes = 8 * 1024 * 1024;
            o.level_base_bytes = 64 * 1024 * 1024;
            o.enable_bg_compact = false;
            o.enable_mem_wal = false;
            o.max_compactions_per_flush = 1;
            kv.set_options(o);
            kv.set_bulk_mode(true);
        }
        BulkLoader {
            kv: self.kv.clone(),
            wal: self.wal.clone(),
            core: BulkCore::begin(self.next_version.clone()),
        }
    }

    /// 默认 WAL 阈值：16 MiB。现场见过 WAL 长到几十 MB 仍无人截断，而恢复是**全量扫描**
    /// WAL（`analyze_recovery`）⇒ 不截断等于启动一次比一次慢。
    pub const WAL_CHECKPOINT_BYTES_DEFAULT: u64 = 16 << 20;

    /// WAL 字节阈值可配置项：增长到这么多字节就在**下一次提交后**自动 checkpoint。
    /// 设 0 关闭自动触发（只能手动调 `checkpoint()`）。
    pub fn set_wal_checkpoint_limit(&self, bytes: u64) {
        self.wal_limit.store(bytes, Ordering::SeqCst);
    }

    pub fn wal_checkpoint_limit(&self) -> u64 {
        self.wal_limit.load(Ordering::SeqCst)
    }

    /// 当前生效的引擎旋钮（读回用，与 [`LSMTree::options`] 同一份）。
    pub fn options(&self) -> LSMTreeOptions {
        self.kv.lock().unwrap().options().clone()
    }

    /// 热更新旋钮（打开之后也能改，不用重开库）。
    ///
    /// `enable_mem_wal` / `enable_bg_compact` 会被钉回 `false`，理由见
    /// `pin_mvcc_invariants`。其余字段立刻生效：块缓存**就地改容量**（不清空已驻留的块），
    /// compact/l0 阈值影响下一次写路径。
    pub fn set_options(&self, patch: LSMTreeOptions) {
        let mut kv = self.kv.lock().unwrap();
        kv.set_options(pin_mvcc_invariants(patch));
    }

    /// 本引擎能被调的东西 —— 清单在这里，不在门面。
    pub fn tuning(&self) -> EngineTuning {
        let mut t = EngineTuning::default();
        t.push_shared(self.vacuum_auto_slice(), Some(self.wal_checkpoint_limit()));
        let kv = self.kv.lock().unwrap();
        let o = kv.options().clone();
        t.push("mem_threshold_bytes", o.mem_threshold_bytes);
        t.push("mem_threshold_entries", o.mem_threshold_entries);
        t.push("block_cache_blocks", o.block_cache_blocks);
        t.push("block_cache_auto_max", o.block_cache_auto_max);
        // 生效值，不是配置值：自动档下 `block_cache_blocks` 只是地板
        t.push("block_cache_capacity", kv.block_cache_capacity());
        // 分母（M32）：没有它，调用方看见"容量 1024"分不清是"顶撞上了"还是"库就这么点块"
        t.push("sst_blocks", kv.sst_block_count());
        // 预算档（M76）：**给的字节**要报 —— 生效块数已经在 `block_cache_capacity` 那一格里了，
        // 两个数同时在场，调用方才看得出"预算换算成了多少槽"以及"是不是被库自己的块数封顶了"。
        t.push("cache_budget_bytes", kv.cache_budget_bytes());
        drop(kv);
        t.push("use_mmap", o.use_mmap);
        t.push("l0_compact_threshold", o.l0_compact_threshold);
        t.push("l0_slowdown_trigger", o.l0_slowdown_trigger);
        t.push("l0_stop_trigger", o.l0_stop_trigger);
        t.push("target_sst_bytes", o.target_sst_bytes);
        t.push("blob_threshold", o.blob_threshold);
        t
    }

    /// 块缓存自动档的上限（`0` = 关自动档，`usize::MAX` = 不设顶）。见
    /// [`LSMTree::set_block_cache_auto_max`]。
    pub fn set_cache_auto_max(&self, blocks: usize) {
        self.kv.lock().unwrap().set_block_cache_auto_max(blocks);
    }

    pub fn cache_auto_max(&self) -> usize {
        self.kv.lock().unwrap().options().block_cache_auto_max
    }

    /// 按**内存预算**给块缓存档（M76）：`bytes ÷ 每槽真实常驻` = 槽数上限，返回换算后的生效块数。
    ///
    /// `0` = 撤掉预算档（回到 `block_cache_blocks` / `block_cache_auto_max` 那条，与改前一字不差）。
    /// 它与"抬顶"是两道正交的约束（取小），预算**允许压到地板以下**，且在 flush/compact 之后的
    /// 重估里继续生效。存在的理由见 [`LSMTree::cache_budget_bytes`]。
    pub fn set_cache_budget_bytes(&self, bytes: u64) -> usize {
        self.kv.lock().unwrap().set_cache_budget_bytes(bytes)
    }

    /// 调用方给的那一档字节（生效块数 = `tuning()["block_cache_capacity"]`）
    pub fn cache_budget_bytes(&self) -> u64 {
        self.kv.lock().unwrap().cache_budget_bytes()
    }

    pub fn checkpoint(&self) {
        if let Err(e) = self.try_checkpoint() {
            log::error!("checkpoint failed: {e}");
        }
    }

    pub fn try_checkpoint(&self) -> std::io::Result<()> {
        let active = self.active_txn.lock().unwrap();
        if !active.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "active transactions prevent checkpoint",
            ));
        }
        run_checkpoint(&self.kv, &self.wal, &self.next_version, &self.db_dir)
    }

    pub fn flush(&self) {
        if let Err(e) = self.try_flush() {
            log::error!("flush failed: {e}");
        }
    }

    pub fn try_flush(&self) -> std::io::Result<()> {
        self.kv.lock().unwrap().flush()
    }

    pub fn db_dir(&self) -> &Path {
        &self.db_dir
    }

    /// 崩溃安全画像。SST 块带 CRC32、MANIFEST 是「写 tmp 再改名」的原子换，所以
    /// compaction 中途崩掉不会丢数据（旧文件集仍然有效）—— 但会留下没人引用的孤儿 SST
    /// 占空间，这条 [`MVCC::vacuum`] / compact 都不收，属于已知代价。
    ///
    /// 两条要说清的缺口：
    /// - `blobs/*.blob` 是 `fs::write` 的**裸文件、没有校验和** ⇒ 大 value 的静默损坏发现不了
    ///   （SST 里那条指针本身在块里、有 CRC，所以"指针没坏、内容坏了"这种最难查）。
    /// - MVCC 路径**关掉了 mem.wal**（`LSMTreeOptions::for_mvcc` 里 `enable_mem_wal = false`）：
    ///   持久化完全靠上面这层事务 WAL 重放，不是"两层都开着"。
    pub fn durability_profile(&self) -> DurabilityProfile {
        DurabilityProfile {
            engine: "lsm_tree",
            transaction_wal: true,
            fsync_on_commit: true,
            data_checksum: true,
            blob_checksum: false,
            // SST 不可变，只有追加式写 ⇒ 没有 torn page 这条能力
            in_place_page_writes: false,
            torn_page_guard: false,
            merge_artifact_recovery: true,
        }
    }
}

impl Default for MVCC {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for MVCC {
    fn drop(&mut self) {
        // 进程正常退出时落盘 next_version，避免下次 open 回退
        let next = self.next_version.load(Ordering::SeqCst);
        persist_mvcc_next(&self.db_dir, next);
    }
}
