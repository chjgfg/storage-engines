//! 本文件负责打开、配置、关闭及资源所有权。

use super::*;

impl MVCC {
    /// 崩溃安全画像 —— 本引擎**如实**的构件清单，跨引擎不等价（见 [`MvccHealth`] 同层的
    /// [`DurabilityProfile`] 说明）。
    ///
    /// Bitcask 唯一的缺口是主日志记录没有校验和：`data.log` 的裸记录是
    /// `key_len | flag | key | value`，坏一个字节不会报错，只会读出一个错值。
    /// 补 CRC 要改磁盘格式 —— 本仓库明确不采纳破坏性格式改动，所以把它**说清楚**而不是藏起来。
    pub fn durability_profile(&self) -> DurabilityProfile {
        DurabilityProfile {
            engine: "bitcask",
            transaction_wal: true,
            fsync_on_commit: true,
            // ⚠️ 见上面的说明：data.log 裸记录
            data_checksum: false,
            // blob 记录是 `len | crc32 | data`
            blob_checksum: true,
            // 追加式布局：没有原地页写 ⇒ 不存在 torn page 这条能力
            in_place_page_writes: false,
            torn_page_guard: false,
            // M6a：open 会认回崩在改名之前的 data.merge / 清掉残骸
            merge_artifact_recovery: true,
        }
    }

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

    /// 打开并允许覆盖分片滚动阈值（`max_file_bytes = None` ⇒ 用引擎默认）。
    ///
    /// 门面层 `EngineConfig::bitcask` 走的就是这里 —— 目的是让下游**不必绕开门面自己构造
    /// `Bitcask`**。基座被依赖而不是被复制，缺一个入口就多一层复制。
    pub fn try_open_with(
        dir: impl AsRef<Path>,
        max_file_bytes: Option<u64>,
    ) -> std::io::Result<Self> {
        let db_dir = dir.as_ref().to_path_buf();
        // ── M48 冷启动**分段耗时**标尺（门控 `OPEN_STAGE_SPLIT`，默认完全 no-op）──
        // 这里是最外面那一层：基准由这里的 `open_stage_t0()` 放，`Bitcask::with_max_file_bytes`
        // 只往里落槽。八段：
        //   0 = 门面前置（`create_dir_all` + 拿文件锁）
        //   1 = Bitcask 前置（认回 merge 残骸 + 发现密封分片 + hint 路径）
        //   2 = **hint 反序列化**（`read_hint_file`：整份 KeyDir 从盘上折出来）
        //   3 = **建 KeyDir**（hint 命中就只增量扫 `active_size` 之后；回退则全量扫所有分片）
        //   4 = 门面三件（`BlobStore::open` + `Wal::open`）
        //   5 = WAL `read_all` + `analyze_recovery` + 重放（已提交/未提交两趟）
        //   6 = 冷启动起始版本号（走 1/2/3 哪一档；⚠️ 第三档是 `scan_max_version` = O(全库 KeyDir)）
        //   7 = `tree.flush()` + `seal_wal_checkpoint`（这两次落盘）
        #[cfg(test)]
        crate::mem_probe::open_stage_t0();
        std::fs::create_dir_all(&db_dir)?;

        // 锁文件路径规则是各引擎自己的（bitcask: `dir/bitcask.lock`），锁的**语义**在
        // [`crate::common::disk::FileLock`]（四引擎共用的一份实现）
        let lock = FileLock::try_acquire_at(&lock_path(&db_dir))?;
        #[cfg(test)]
        crate::mem_probe::open_stage_mark(0);
        let data_log = data_path(&db_dir);
        let mut tree = match max_file_bytes {
            Some(n) => Bitcask::with_max_file_bytes(data_log, n)?,
            None => Bitcask::new(data_log)?,
        };
        let blob = BlobStore::open(&db_dir)?;
        let mut wal = Wal::open(wal_path(&db_dir))?;
        #[cfg(test)]
        crate::mem_probe::open_stage_mark(4);

        let entries = wal.read_all()?;
        let plan = analyze_recovery(&entries);

        for (_xid, key, value) in &plan.committed_writes {
            tree.insert(key.clone(), value.clone())?;
        }
        for (_xid, key) in &plan.uncommitted_writes {
            let _ = tree.remove(key);
        }
        #[cfg(test)]
        crate::mem_probe::open_stage_mark(5);

        // 冷启动的起始版本号，三级优先（照 bplus 的口径，别让同一个优化在三处有三种成熟度）：
        // 1. WAL 里最后一个 Checkpoint 的 next_version —— checkpoint 先 flush 数据再截断 WAL，
        //    所以它之前的版本行都已落盘、编号必然小于它，它之后的记录才是新增写入；
        // 2. 没有 Checkpoint 就用 WAL 见过的最大 xid；
        // 3. 两者都没有（首次打开/老库）才退化到全库扫 KeyDir 求最大版本。
        //
        // 走 1 才让「库越大 open 越慢」这条通道断开：`scan_max_version` 是 O(总行数)。
        // 注意这里省的是**启动开销**而不是正确性 —— 即便退化到 3，扫出来的最大值也不会
        // 小于某个仍存活的行，所以不会发生版本号复用。
        let ckpt_nv = match plan.last_checkpoint.as_ref() {
            Some(WalRecord::Checkpoint { next_version, .. }) => Some(*next_version),
            _ => None,
        };
        let start_ver = match ckpt_nv {
            Some(nv) => nv.max(plan.max_xid.saturating_add(1)).max(1),
            None => {
                let disk_max = scan_max_version(&mut tree)?;
                disk_max.max(plan.max_xid).saturating_add(1).max(1)
            }
        };
        #[cfg(test)]
        crate::mem_probe::open_stage_mark(6);

        // 这一家**保持无条件**：`Bitcask::flush` = 写缓冲空就直接返回 + 一次 fsync，是有界工作。
        // LSM 那一档在 M28 改成了「有 redo 才 flush」，因为它的 `flush()` 末尾挂着无界的
        // `maybe_compact()` —— 600k 实测那次悬崖 13.9 s 全花在那儿。若将来这里的 flush 长出
        // 随库涨的活（文件滚动、合并），照 `test_open_does_not_compact_when_nothing_to_redo`
        // 那把尺子（层文件数不动）改，别在这里加一条只会空转的断言。
        tree.flush()?;
        // 同样要「先截断再写 Checkpoint」，否则这次 open 留下的起点信息会被截断抹掉，
        // 下次 open 只能再全库扫一遍（首次打开的库尤其需要这条快路）。
        seal_wal_checkpoint(&mut wal, start_ver)?;
        #[cfg(test)]
        crate::mem_probe::open_stage_mark(7);

        Ok(Self {
            kv: Arc::new(Mutex::new(tree)),
            blob: Arc::new(Mutex::new(blob)),
            wal: Arc::new(Mutex::new(wal)),
            active_txn: Arc::new(Mutex::new(HashMap::new())),
            next_version: Arc::new(AtomicU64::new(start_ver)),
            wal_limit: Arc::new(AtomicU64::new(MVCC::WAL_CHECKPOINT_BYTES_DEFAULT)),
            vac: Arc::new(VacuumScheduler::new(MVCC::VACUUM_AUTO_SLICE_DEFAULT)),
            db_dir,
            _lock: lock,
        })
    }

    /// 在系统临时目录创建匿名库（测试用）
    pub fn new() -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "bitcask_mvcc_anon_{}_{}",
            std::process::id(),
            nanos
        ));
        Self::open(path)
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
    /// 依据：一片的代价是读 16 Ki 行元数据（KeyDir 内存遍历，不读 value），单核机器上
    /// 亚毫秒级；而事故现场那条链有 23.8 万行 —— 默认值开着他就会在自己被需要之前
    /// 被裁掉。设这么小是为了「每次只挡一小下」，不是省总开销。
    pub const VACUUM_AUTO_SLICE_DEFAULT: u64 = 16 * 1024;

    /// 开启 bulk load：单写者、无 WAL、无冲突检测。
    ///
    /// **崩溃不安全**：finish 之前进程挂掉可能丢数据。
    /// 导入结束后必须 `finish()`。
    pub fn begin_bulk(&self) -> BulkLoader {
        BulkLoader {
            kv: self.kv.clone(),
            blob: self.blob.clone(),
            wal: self.wal.clone(),
            core: BulkCore::begin(self.next_version.clone()),
        }
    }

    /// 默认 WAL 阈值：16 MiB。
    ///
    /// 现场见过 WAL 长到 55MB 仍无人截断，而恢复路径是**全量扫描** WAL（`analyze_recovery`）
    /// —— 不截断等于启动一次比一次慢，连「重启止血」这个动作本身都在退化。
    pub const WAL_CHECKPOINT_BYTES_DEFAULT: u64 = 16 << 20;

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
        run_checkpoint(
            &self.kv,
            &self.blob,
            &self.wal,
            &self.next_version,
            crate::bitcask::bitcask::HintPolicy::Force,
        )
    }

    /// WAL 字节阈值可配置项：增长到这么多字节就在**下一次提交后**自动 checkpoint。
    /// 设 0 关闭自动触发（只能手动调 `checkpoint()`）。
    pub fn set_wal_checkpoint_limit(&self, bytes: u64) {
        self.wal_limit.store(bytes, Ordering::SeqCst);
    }

    pub fn wal_checkpoint_limit(&self) -> u64 {
        self.wal_limit.load(Ordering::SeqCst)
    }

    /// 当前生效的 active 分片软上限（字节），写满后 rotate 到下一个 `data.{id}.log`。
    pub fn max_file_bytes(&self) -> u64 {
        self.kv.lock().unwrap().max_file_bytes()
    }

    /// 本引擎能被调的东西 —— 清单在这里，不在门面。
    pub fn tuning(&self) -> EngineTuning {
        let mut t = EngineTuning::default();
        t.push_shared(self.vacuum_auto_slice(), Some(self.wal_checkpoint_limit()));
        t.push("max_file_bytes", self.max_file_bytes());
        t
    }

    pub fn flush(&self) {
        if let Err(e) = self.try_flush() {
            log::error!("flush failed: {e}");
        }
    }

    pub fn try_flush(&self) -> std::io::Result<()> {
        let mut kv = self.kv.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();
        kv.flush()?;
        blob.sync()
    }

    /// 压缩数据文件 + 写出 hint
    ///
    /// ⚠️ M43 起 `merge()` 内部有一段"索引重扫"：它先把旧的内存索引腾掉，再原地重扫新的
    /// `data.log`。正常那一次没人看得见（这段代码全程持 `kv` 锁，并发请求只是阻塞在锁上）。
    /// 但如果**重扫失败**，本方法返回 `Err` 而引擎进入隔离态：此后每一扇能报错的门
    /// （`get` / `scan_page` 背后的取值 / 写路径 / `dump_hint`）都返回错，**不会再回"键不存在"**。
    /// 磁盘上的数据是完整的，出路只有重新 `open`（那里会全量扫日志重建索引）。
    pub fn merge(&self) -> std::io::Result<()> {
        self.kv.lock().unwrap().merge()
    }

    /// 主动写出 KeyDir hint（不 merge）
    pub fn dump_hint(&self) -> std::io::Result<()> {
        self.kv.lock().unwrap().dump_hint()
    }

    pub fn db_dir(&self) -> &Path {
        &self.db_dir
    }
}

impl Default for MVCC {
    fn default() -> Self {
        Self::new()
    }
}
