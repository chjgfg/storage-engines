//! 本文件负责内存表刷盘与维护写入调度。
//! 模块边界仅组织代码：锁顺序、版本可见性、磁盘格式和发布失败的处理均沿用原契约。

use super::*;

impl LSMTree {
    pub(super) fn maybe_flush_mem(&mut self) {
        let _ = self.maybe_flush_mem_checked();
    }

    pub(super) fn maybe_flush_mem_checked(&mut self) -> std::io::Result<()> {
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
            #[cfg(test)]
            let _flush = maintenance_probe::Timer::start(maintenance_probe::Kind::ThresholdFlush);
            self.flush_mem()?;
        }
        Ok(())
    }

    // ─── flush / compact ──────────────────────────────────────────────────────

    pub fn flush(&mut self) -> std::io::Result<()> {
        let mut before_publication = self.manifest_edit_id;
        if !self.mem.is_empty() {
            self.flush_mem()?;
        }
        while !self.immutables.is_empty() {
            // 每追加一个不可变表，都会改变此前已发布的文件集合。
            before_publication = self.manifest_edit_id;
            self.flush_one_immutable()?;
        }
        if !self.bulk_mode && !self.opts.enable_bg_compact {
            self.maybe_compact()?;
        } else {
            self.notify_bg();
        }
        match self.publish_after_maintenance(before_publication)? {
            MaintenanceManifest::PublishedByCompaction | MaintenanceManifest::PublishedHere => {}
        }
        self.checkpoint_mem_wal()?;
        self.dirty = false;
        self.stats_dirty = true;
        Ok(())
    }

    pub(super) fn checkpoint_mem_wal(&mut self) -> std::io::Result<()> {
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
    /// 全库回收期间把阈值刷盘产生的删除文件集中压实。
    /// 每个阈值 SST 和清单仍持久化，L0 停止阈值仍生效；作用域退出时恢复普通调度，包括返回错误和 panic 展开。

    /// 全库回收已持有存储锁；每次阈值 SST 和清单仍持久化，只把删除文件归并推迟到最终刷盘。成功、错误和异常展开都恢复普通调度。
    pub(crate) fn with_vacuum_writes<T>(
        &mut self,
        full_sweep: bool,
        run: impl FnOnce(&mut Self) -> std::io::Result<T>,
    ) -> std::io::Result<T> {
        struct ScheduleScope<'a> {
            tree: &'a mut LSMTree,
            old: VacuumWriteSchedule,
        }
        impl Drop for ScheduleScope<'_> {
            fn drop(&mut self) {
                self.tree.vacuum_write_schedule = self.old;
            }
        }
        #[cfg(test)]
        let full_sweep = full_sweep && !vacuum_compaction_reference::enabled();
        let old = self.vacuum_write_schedule;
        if full_sweep {
            self.vacuum_write_schedule = VacuumWriteSchedule::Coalesced;
        }
        let scope = ScheduleScope { tree: self, old };
        run(&mut *scope.tree)
    }
    /// 把当前内存表转换为持久 SST，再发布当前文件集合。
    /// 刷盘与压实的清单发布状态必须明确区分，不能省掉尚未完成的持久化屏障。

    pub(super) fn flush_mem(&mut self) -> std::io::Result<()> {
        if self.mem.is_empty() {
            return Ok(());
        }
        let frozen = std::mem::take(&mut self.mem);
        self.mem_entries = 0;
        self.mem_bytes = 0;
        self.immutables.insert(0, frozen);
        self.flush_one_immutable()?;
        let before_publication = self.manifest_edit_id;
        // bulk：不在写路径 compact；非 bulk 且未开后台时同步 compact
        if !self.bulk_mode && !self.opts.enable_bg_compact {
            if self.vacuum_write_schedule == VacuumWriteSchedule::Immediate
                || self.levels[0].len() >= self.opts.l0_stop_trigger.max(1)
            {
                self.maybe_compact()?;
            }
        } else {
            self.notify_bg();
        }
        match self.publish_after_maintenance(before_publication)? {
            MaintenanceManifest::PublishedByCompaction | MaintenanceManifest::PublishedHere => {}
        }
        self.checkpoint_mem_wal()?;
        Ok(())
    }
    /// 将一个不可变内存表写入 SST 并纳入分层文件集合。
    /// 只有输出完成后才更新文件集合；后续清单发布决定恢复时哪些文件可见。

    pub(super) fn flush_one_immutable(&mut self) -> std::io::Result<()> {
        let Some(imm) = self.immutables.pop() else {
            return Ok(());
        };
        if imm.is_empty() {
            return Ok(());
        }
        #[cfg(test)]
        let _probe = maintenance_probe::Timer::start(maintenance_probe::Kind::Flush);
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
}
