//! 本文件负责打开、配置、关闭及资源所有权。

use super::*;

impl MVCC {
    /// 打开/创建磁盘上的 MVCC 引擎（含 WAL 恢复）。失败时 panic（兼容旧代码）。
    pub fn open(path: impl AsRef<Path>, order: usize, max_cache_pages: usize) -> Self {
        Self::try_open(path, order, max_cache_pages)
            .map_err(|e| format!("MVCC::open 失败: {e}"))
            .unwrap()
    }

    /// 打开/创建，错误以 `io::Result` 返回（锁冲突、坏文件、IO 等）。
    ///
    /// **冷启动路径**（千万级 / GB 库）：
    /// - 优先用 meta 中的 `next_version`（checkpoint 时写入），**不扫全树**
    /// - 其次用 WAL 里最后一条 Checkpoint / max_xid
    /// - 仅旧库（meta 无 NVER 且 WAL 无 checkpoint）才 `scan_max_version` 全叶扫描
    pub fn try_open(
        path: impl AsRef<Path>,
        order: usize,
        max_cache_pages: usize,
    ) -> std::io::Result<Self> {
        let db_path = path.as_ref().to_path_buf();
        let mut tree = BPlusTree::try_open(&db_path, order, max_cache_pages)?;
        let blob = BlobStore::open(&db_path)?;
        let mut wal = Wal::open(wal_path(&db_path))?;

        let entries = wal.read_all()?;
        let plan = analyze_recovery(&entries);

        // REDO 已提交 / UNDO 未提交（仅当 WAL 非空有内容时）
        for (_xid, key, value) in &plan.committed_writes {
            insert_or_shout(&mut tree, key.clone(), RowId(value.clone()), "WAL REDO");
        }
        for (_xid, key) in &plan.uncommitted_writes {
            let _ = tree.delete(key.clone());
        }

        let start_ver = resolve_start_version(&mut tree, &plan);

        // 把 next_version 写入 meta，下次干净 open 可跳过全树扫描
        tree.set_next_version(start_ver);
        // **必须无条件 flush**：上面那句 `set_next_version` 只改了内存里的页，是这次 flush
        // 把它落到 meta 页 —— 与 redo 是否为空无关。所以 LSM 在 M28 上的那条「有 redo 才
        // flush」在这里不适用（那边的 flush 在空 redo 时只剩一次 fsync 外加无界压实）。
        tree.try_flush_all()?;
        let root = tree.root_page_id();
        let next_page = tree.next_page_id();
        // 先截断、再把 Checkpoint 写成新 WAL 的第一条。原先的顺序是 append→sync→truncate，
        // 而 truncate 只留 header ⇒ 刚写的 Checkpoint 被自己抹掉，下次 open 想靠它拿起点也拿不到。
        seal_wal_checkpoint(&mut wal, start_ver, root.0, next_page)?;

        Ok(Self {
            kv: Arc::new(Mutex::new(tree)),
            blob: Arc::new(Mutex::new(blob)),
            wal: Arc::new(Mutex::new(wal)),
            active_txn: Arc::new(Mutex::new(HashMap::new())),
            next_version: Arc::new(AtomicU64::new(start_ver)),
            wal_limit: Arc::new(AtomicU64::new(MVCC::WAL_CHECKPOINT_BYTES_DEFAULT)),
            vac: Arc::new(VacuumScheduler::new(MVCC::VACUUM_AUTO_SLICE_DEFAULT)),
            db_path,
        })
    }

    pub fn new(order: usize, max_cache_pages: usize) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("mvcc_anon_{}_{}.db", std::process::id(), nanos));
        Self::open(path, order, max_cache_pages)
    }

    pub fn begin_transaction(&self) -> Transaction {
        self.tick_auto_vacuum();
        Transaction::begin(
            self.kv.clone(),
            self.blob.clone(),
            self.wal.clone(),
            self.active_txn.clone(),
            self.next_version.clone(),
            self.wal_limit.clone(),
            self.vac.clone(),
        )
    }

    /// 默认切片宽度 = 16 Ki 行。
    ///
    /// bplus 的一片要读叶子页（不像 Bitcask 只在内存 KeyDir 上走），页数 ≈ 行数 / order；
    /// 设这么小是为了「每次只挡一小下」，不是省总开销。事故现场那条链有 23.8 万行 ——
    /// 默认开着的话它活不到那么长。
    pub const VACUUM_AUTO_SLICE_DEFAULT: u64 = 16 * 1024;

    /// 开启 bulk load：单写者、无 WAL、无冲突检测。
    ///
    /// **崩溃不安全**：finish 之前进程挂掉可能丢数据。
    /// 导入结束后必须 `finish()`。
    pub fn begin_bulk(&self) -> BulkLoader {
        {
            let mut kv = self.kv.lock().unwrap();
            kv.set_bulk_mode(true);
        }
        BulkLoader {
            kv: self.kv.clone(),
            blob: self.blob.clone(),
            wal: self.wal.clone(),
            core: BulkCore::begin(self.next_version.clone()),
        }
    }

    /// 默认 WAL 阈值：16 MiB。现场见过 WAL 长到几十 MB 仍无人截断，而恢复是**全量扫描**
    /// WAL ⇒ 不截断等于启动一次比一次慢。
    pub const WAL_CHECKPOINT_BYTES_DEFAULT: u64 = 16 << 20;

    /// WAL 字节阈值可配置项：增长到这么多字节就在**下一次提交后**自动 checkpoint。
    /// 设 0 关闭自动触发（只能手动调 `checkpoint()`）。
    pub fn set_wal_checkpoint_limit(&self, bytes: u64) {
        self.wal_limit.store(bytes, Ordering::SeqCst);
    }

    pub fn wal_checkpoint_limit(&self) -> u64 {
        self.wal_limit.load(Ordering::SeqCst)
    }

    /// 生效的 B+ 树阶数（每个节点的子节点上限）。
    ///
    /// 只在**打开时**可设：阶数决定已落盘节点的形状，开着库改它等于让新旧页用两套规则。
    pub fn order(&self) -> usize {
        self.kv.lock().unwrap().order()
    }

    /// 生效的页缓存容量（页数）。
    ///
    /// 这是全库走查（scan / health / search / vacuum）与工作集能否焐热之间的那颗旋钮：
    /// 页数超过容量时走查只"装得下才缓存"（`read_page_for_walk`），不再挤掉 OLTP 的热页。
    pub fn max_cache_pages(&self) -> usize {
        self.kv.lock().unwrap().max_cache_pages()
    }

    /// 缓存策略读数：`(生效容量, 钉住的上层页数, 是否自动档)`。
    ///
    /// 自动档必须**看得见生效值**："设了没生效"和"生效了但不知道是多少"在下游日志里
    /// 长得一模一样，只有读回来才分得开。
    pub fn cache_policy(&self) -> (usize, usize, bool) {
        self.kv.lock().unwrap().cache_policy()
    }

    /// 页缓存自动档的上限（`usize::MAX` = 不设顶，见 [`BPlusTree::set_cache_auto_max`]）。
    pub fn set_cache_auto_max(&self, pages: usize) {
        self.kv.lock().unwrap().set_cache_auto_max(pages);
    }

    pub fn cache_auto_max(&self) -> usize {
        self.kv.lock().unwrap().cache_auto_max()
    }

    /// 按**内存预算**给页缓存档（M76）：`bytes ÷ 每槽真实常驻` = 槽数上限，返回换算后的生效页数。
    ///
    /// 存在的理由：下游的约束一直是字节（"这进程只能再住 32 MB"），而此前能调的两个旋钮都以
    /// **槽数**计（`max_cache_pages`、`cache_auto_max`），要按内存定档就得自己乘价签 ——
    /// 而价签是实测的 6.8 KB/槽（点读姿势），不是页的名义 4 KiB；按 4 KiB 乘会把常驻买到 1.66 倍。
    ///
    /// `0` = 撤掉预算档（回到自动档/显式档，与改前一字不差）。预算**允许压到自动档地板 1024 以下**，
    /// 且在每次刷盘后的重估里继续生效（`BPlusTree::reevaluate_cache_capacity` 是三个入口共用的那一个）。
    pub fn set_cache_budget_bytes(&self, bytes: u64) -> usize {
        self.kv.lock().unwrap().set_cache_budget_bytes(bytes)
    }

    /// 调用方给的那一档字节（生效页数 = [`Self::max_cache_pages`]）
    pub fn cache_budget_bytes(&self) -> u64 {
        self.kv.lock().unwrap().cache_budget_bytes()
    }

    /// 钉住预算（M49）：上层页可以占多少容量。`0` = 一页都不钉，叶子拿到全部容量。
    /// 生效值还要与容量的一半取小，见 [`BPlusTree::set_pin_budget_pages`]。
    pub fn set_pin_budget_pages(&self, pages: usize) {
        self.kv.lock().unwrap().set_pin_budget_pages(pages);
    }

    pub fn pin_budget_pages(&self) -> usize {
        self.kv.lock().unwrap().pin_budget_pages()
    }

    pub fn pin_limit_pages(&self) -> usize {
        self.kv.lock().unwrap().pin_limit_pages()
    }

    /// 本引擎能被调的东西 —— 清单在这里，不在门面。
    pub fn tuning(&self) -> EngineTuning {
        let mut t = EngineTuning::default();
        t.push_shared(self.vacuum_auto_slice(), Some(self.wal_checkpoint_limit()));
        t.push("order", self.order());
        let (cap, pinned, auto) = self.cache_policy();
        // `max_cache_pages` 报的是**生效值**：传 0（自动档）时这里是算出来的那个数
        t.push("max_cache_pages", cap);
        t.push("cache_pinned_pages", pinned);
        t.push("cache_auto", if auto { 1 } else { 0 });
        // 自动档的顶与分母（M32）：只有 (生效容量, 总页数) 同时给出来，调用方才知道
        // "覆盖率 5.6%" 这件事，也才知道抬顶要付出多少常驻。
        // 钉住预算这一档（M49）：给的与生效的**都要报** —— 生效值还要与容量一半取小，
        // 只报"给了多少"就又是一次"设了没生效"与"生效了但不知道是多少"长得一模一样。
        // ⚠️ 一次锁取四个数：`Mutex` 不可重入，同一条作用域里第二次 `lock()` 就是自死锁
        //（M32 那条守卫就是这么冻住过一次，三个现象全指向错的地方）。
        let tree = self.kv.lock().unwrap();
        t.push("page_cache_auto_max", tree.cache_auto_max());
        t.push("total_pages", tree.next_page_id() as usize);
        t.push("cache_pin_budget", tree.pin_budget_pages());
        t.push("cache_pin_limit", tree.pin_limit_pages());
        // 预算档（M76）：**给的字节**与**换算出的槽数**都要报 —— 只报其中一个，调用方就分不清
        // "预算太小装不下几页"与"价签乘错了"。`0` = 没设预算（走自动档/显式档）。
        t.push("cache_budget_bytes", tree.cache_budget_bytes());
        drop(tree);
        t
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
        run_checkpoint(&self.kv, &self.wal, &self.next_version)
    }

    pub fn flush(&self) {
        if let Err(e) = self.try_flush() {
            log::error!("flush failed: {e}");
        }
    }

    pub fn try_flush(&self) -> std::io::Result<()> {
        self.kv.lock().unwrap().try_flush_all()
    }

    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    /// 崩溃安全画像 —— 四家里构件最全的一个，但"最全"不等于"没问题"：
    /// 页有 magic + CRC32、原地页写有双写缓冲兜底（防半页撕裂），freelist/blob 各自成文件。
    /// `merge_artifact_recovery` 这里**不适用**（设 false 不是缺陷）：B+ 树没有全库重写式
    /// merge，回收是原地页操作，被 dblwr 覆盖。
    pub fn durability_profile(&self) -> DurabilityProfile {
        DurabilityProfile {
            engine: "bplus_tree",
            transaction_wal: true,
            fsync_on_commit: true,
            // 页头带 magic + CRC32（pack_page）
            data_checksum: true,
            blob_checksum: true,
            in_place_page_writes: true,
            torn_page_guard: true,
            merge_artifact_recovery: false,
        }
    }
}

impl Drop for MVCC {
    /// 正常退出且无活跃事务时 checkpoint：写 meta.next_version + 截断 WAL。
    /// - 仍有未 commit/rollback 的事务时**不**截断 WAL，避免丢掉 UNDO 信息
    /// - 崩溃/kill -9 不会走到这里，仍靠 WAL REDO + meta/max_xid 恢复
    fn drop(&mut self) {
        // 忽略锁中毒与 IO 错误，避免 drop 时二次 panic
        let has_active = self
            .active_txn
            .lock()
            .map(|g| !g.is_empty())
            .unwrap_or(true);
        if has_active {
            return;
        }
        let next_ver = self.next_version.load(Ordering::SeqCst);
        if let (Ok(mut kv), Ok(mut wal)) = (self.kv.lock(), self.wal.lock()) {
            kv.set_next_version(next_ver);
            if kv.try_flush_all().is_err() {
                return;
            }
            let root = kv.root_page_id();
            let next_page = kv.next_page_id();
            // M52：`Drop` 里同一个洞 ⇒ 同一份收口（先截断再写回）
            let _ = seal_wal_checkpoint(&mut wal, next_ver, root.0, next_page);
        }
        if let Ok(mut blob) = self.blob.lock() {
            let _ = blob.sync();
        }
    }
}
