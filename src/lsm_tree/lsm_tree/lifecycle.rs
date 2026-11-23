//! 本文件负责打开、配置、关闭及资源所有权。

use super::*;

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

    pub fn open_with_config(dir: impl AsRef<Path>, opts: LSMTreeOptions) -> std::io::Result<Self> {
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
            level_ranges_ordered: vec![true; max_levels],
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
            vacuum_write_schedule: VacuumWriteSchedule::Immediate,
            cache_shared: false,
            cache_budget_bytes: 0,
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
                    MemWalRecord::Flush { next_write_seq, .. } => {
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
        // 块缓存容量：**就地改上限**，不重建。
        // 旧写法 `*cache = BlockCache::new(n)` 会把已经装进来的块全丢掉 ⇒ 改一次旋钮
        // 等于把点读打回冷启动，而且自动档每重算一次就自己清一次自己。
        self.reevaluate_cache_capacity();
    }

    /// 设置 compaction filter（`None` 清除）
    pub fn set_compaction_filter(&mut self, f: Option<CompactionFilter>) {
        self.compaction_filter = f;
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
        // 简化规则：版本小于 xmin 的墓碑可在满足删除屏障条件后丢弃。
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
    /// 用完后调用 `release_snapshot` 释放版本保留条件，避免旧版本一直无法回收。
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
