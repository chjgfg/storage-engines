//! 本文件负责历史版本选择与回收。
//! 模块边界仅组织代码：锁顺序、版本可见性、磁盘格式和发布失败的处理均沿用原契约。

use super::*;

impl MVCC {
    /// 自动回收的一小步：攒够一个切片宽度的新版本，就裁同样宽度的**一片**。
    ///
    /// 挂在 `begin_transaction` 而不是 commit 上：这一处还没有本线程的活跃事务，
    /// `vacuum_bounded` 的「有活跃事务就整轮跳过」不会被自己的调用方踩中；而且每个
    /// 事务至少 begin 一次，攒账的节奏和写路径一样。
    ///
    /// 触发量 = 切片宽度 ⇒ 攒够一片垃圾才扫一片，扫描量与写入量 **1:1**，`begin()` 不付额外的税
    /// （M82：M81 第 5 项后半那条"一轮开着就每个 begin 推一片"已拿掉 —— 它让写流里每个事务都扫
    /// 一整片，`docs/test2.txt` 五档 `灌`/`更新` 全变慢）。账花完而一轮还在半路时，由
    /// [`crate::maintenance::Maintenance`]（独立游标与间隔）把它推到库尾，不由下一个事务推；
    /// 不跑维护器就等下一次写或手动 `vacuum()`。
    /// 一片的代价 = 走 `slice` 行叶子条目，锁也就按这么久。
    /// 游标回到库尾之后，下一片自动从头开始第二遍。
    pub(super) fn tick_auto_vacuum(&self) {
        if let Err(e) = self.vac.run_pending(|after, rows| {
            #[cfg(test)]
            let _probe = crate::common::maintenance_probe::Timer::start(
                crate::common::maintenance_probe::Stage::AutoVacuum,
            );
            self.vacuum_bounded(after, rows)
        }) {
            log::warn!("自动版本回收失败，保留待回收账: {e}");
        }
    }

    /// 轻量回收状态，不扫描数据库。
    pub fn reclamation_status(&self) -> crate::common::ReclamationStatus {
        self.vac.status(&self.active_txn.lock().unwrap())
    }

    /// 自动回收的切片宽度（行数）：每攒够这么多行新版本，就裁一片这么多行。
    ///
    /// 默认 [`MVCC::VACUUM_AUTO_SLICE_DEFAULT`]；**设 0 关闭自动回收**，那时版本行只增不减，
    /// 得靠手动/后台维护裁版本。点读的链尾早停仍在，但历史页与维护成本会积累。
    pub fn set_vacuum_auto_slice(&self, rows: u64) {
        self.vac.set_slice(rows);
    }

    pub fn vacuum_auto_slice(&self) -> u64 {
        self.vac.slice()
    }
    /// 规划并回收每个逻辑键的历史版本，保护活动快照仍可能读取的版本。
    /// 全库回收与有界切片共用保留判据，不能只凭“不是最新版”就删除。

    /// 版本回收：一次扫完全库。
    ///
    /// 就是 `vacuum_bounded(None, u64::MAX)`；保留规则、跳过条件、游标语义都在
    /// [`MVCC::vacuum_bounded`] 那边。
    pub fn vacuum(&self) -> std::io::Result<VacuumStats> {
        Ok(self.vacuum_bounded(None, u64::MAX)?.stats)
    }
    /// 从上次物理游标继续，按扫描行预算裁出一个回收切片。
    /// 跨片版本组保持连续，删除前完成该组判定；报告实际扫描和删除数供调度记账。

    /// 有界版本回收：从逻辑 key `after`（**不含**）之后开始扫，最多读 `budget_rows` 行。
    ///
    /// 保留规则和 [`MVCC::vacuum`] 完全一致，只是作用范围是一段而不是全库：
    /// - **有任何活跃事务 ⇒ 整轮跳过**（`skipped_active_txn = true`），游标原样退回
    /// - 无活跃事务时 `xmin = next_version`
    /// - 每个逻辑 key：只保留「最后一个 version < xmin」+ 全部 version >= xmin
    /// - 若 xmin 之前最终是 tombstone 且无更新版本 → 整 key 删除
    ///
    /// 截断只发生在逻辑 key 边界（半条链不能收：判「留哪一版」要求整组版本都在手上），
    /// 所以一行的预算偏差 = 那一条链的长度。
    ///
    /// blob 回收只在**全库轮次**（`budget_rows == u64::MAX`）做：它要看到全部存活行才能判断
    /// 「还有没有人引用 blob」。有删除的轮次必须 `flush_all`；空扫不代替 checkpoint。
    ///
    /// 为什么要切片：`kv` 挂在 `Mutex` 后面，[`MVCC::vacuum`] 会把这把锁按到整库叶子链
    /// 走完，期间点读点写全部排队。自动回收要能在单核机器上跑，就得一轮只按一小段。
    ///
    /// 「跳过而不是算精确水位线」的理由与锁序说明见 [`crate::bitcask::MVCC::vacuum_bounded`]
    /// （四引擎同一套语义；旧水位线会裁掉活跃事务快照仍需要的版本 ⇒ 静默读到旧值）。
    pub fn vacuum_bounded(
        &self,
        after: Option<&[u8]>,
        budget_rows: u64,
    ) -> std::io::Result<VacuumRound> {
        #[cfg(test)]
        if vacuum_scan_reference::enabled() {
            return self.vacuum_bounded_reference(after, budget_rows);
        }
        #[cfg(test)]
        let _active_guard = crate::common::maintenance_probe::lock(
            &self.active_txn,
            crate::common::maintenance_probe::LockKind::Registry,
        );
        #[cfg(not(test))]
        let _active_guard = self.active_txn.lock().unwrap();
        if !_active_guard.is_empty() {
            return Ok(VacuumRound {
                stats: VacuumStats {
                    xmin: 0,
                    versions_removed: 0,
                    blob_rewritten: Some(false),
                    skipped_active_txn: true,
                    rows_scanned: 0,
                },
                // 游标原样退回：跳过不丢进度，下一轮从同一处接着扫
                next_cursor: after.map(|k| k.to_vec()),
            });
        }
        let xmin = self.next_version.load(Ordering::SeqCst);

        #[cfg(test)]
        let mut kv = crate::common::maintenance_probe::lock(
            &self.kv,
            crate::common::maintenance_probe::LockKind::Storage,
        );
        #[cfg(not(test))]
        let mut kv = self.kv.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();
        kv.ensure_healthy()?;
        kv.with_reclamation(|kv| {
            // ① 一趟按引用的遍历。判「留哪一版」只需要三样：版本号、这行是不是墓碑、
            //    这行是不是 blob 指针 —— 全都在叶子条目手里就能读到，不必把 value 克隆出来。
            //    旧实现在这里有两趟 `kv.iter()`（每行克隆一次 value），第二趟纯粹为查 blob 引用。
            //    M36：**只攒当前这一把键的那一组**（以前是全库归组 ⇒ vacuum 自己的瞬时内存
            //    = O(全库物理行)，600k 档 +89.4 MB 那个形状），版本键按 `raw ‖ version_be`
            //    升序到达 ⇒ 判完一组就地并进 `to_delete`。
            let budget_rows = budget_rows.max(1);
            let mut groups: crate::common::order::KeyWindow<Vec<(u64, u64, bool, bool)>> =
                crate::common::order::KeyWindow::new();
            let mut spare = Vec::new();
            let mut to_delete: Vec<Vec<u8>> = Vec::new();
            let mut removed = 0usize;
            let mut has_blob_ref = false;
            let mut scanned = 0u64;
            let mut cur_raw: Option<Vec<u8>> = None;
            let mut truncated = false;
            // M37：按块走查（与 bitcask / lsm / memory 同一形状）—— 走查期间 `kv` 被
            // `scan_entries_break_from` 借住，删除只能在块与块之间做 ⇒ `to_delete` 每块清一次，
            // 不再 O(全库)。块间下界 `enc ‖ 0x00`：enc 域的严格后继方向。
            let mut lower: Option<Vec<u8>> = after.map(|k| k.to_vec());
            loop {
                let mut chunk_rows = 0usize;
                let mut next_lower: Option<Vec<u8>> = None;
                let mut done = false;
                // 本块续跑下界：首块 = 逻辑游标 `after`，其后 = 上一块最后一行的 `enc ‖ 0x00`。
                let resume_bound = lower.clone();
                #[cfg(test)]
                let scan_probe = crate::common::maintenance_probe::Timer::start(
                    crate::common::maintenance_probe::Stage::VacuumScan,
                );
                let end = kv.scan_maintenance_from(lower.as_deref(), |enc, row| {
                    // ⚠️ `scan_entries_break_from` 只下降到 containing 叶子，随后**从该片第一条**起遍历
                    // （叶粒度定位，过滤交给调用方——见其文档）。上一块已经消费过的行会被再喂一遍；若某个
                    // 版本组正好骑在块边界上（它最新那版恰是续跑叶子的首条），那一版会**第二次**进 `group`，
                    // 让 `vacuum_plan_group` 排序后把"同一 enc 的较早副本"划进 `last_old` 之前、连同活版本的
                    // enc 一起塞进 `to_delete`（判保留的最新版本被物理删掉 ⇒ 静默丢活键）。
                    // ⇒ 按本块续跑下界显式跳过已消费的行（`resume_bound` 是上一块尾行的严格后继方向）。
                    if resume_bound.as_ref().is_some_and(|b| enc < b.as_slice()) {
                        return false;
                    }
                    let Some((raw_key, version)) = crate::common::version::decode_key_ref(enc)
                    else {
                        return false;
                    };
                    // 下界落在键的字节域，`raw_key ≤ after` 的那些行（含 `after` 自己那一整组）
                    // 还在范围内 —— 显式跳过，游标才是逻辑 key 域的。
                    if after.is_some_and(|a| raw_key <= a) {
                        return false;
                    }
                    while let Some((key, mut group)) = groups.pop_before(&enc, xmin) {
                        vacuum_drain_group(
                            &mut group,
                            xmin,
                            |version| to_delete.push(encode_key(&key, version)),
                            |_, refs_blob| has_blob_ref |= refs_blob,
                        );
                        if group.capacity() > spare.capacity() {
                            spare = group;
                        }
                        cur_raw = Some(key);
                    }
                    // 整个重叠前缀窗口完成判定后才能停止。
                    if groups.is_empty() && scanned >= budget_rows {
                        truncated = true;
                        done = true;
                        return true;
                    }
                    scanned += 1;
                    let is_tomb = row.is_none();
                    let refs_blob = row.is_some_and(|s| {
                        s.first() == Some(&crate::bplus_tree::storage::VAL_TAG_BLOB)
                    });
                    groups
                        .entry_with(raw_key, || {
                            let mut key = cur_raw.take().unwrap_or_default();
                            key.clear();
                            key.extend_from_slice(raw_key);
                            (key, std::mem::take(&mut spare))
                        })
                        .push((version, version, is_tomb, refs_blob));
                    chunk_rows += 1;
                    if chunk_rows >= vac_chunk_limit() {
                        let mut bound = enc.to_vec();
                        bound.push(0u8);
                        next_lower = Some(bound);
                        return true;
                    }
                    false
                })?;
                debug_assert_eq!(
                    end == crate::bplus_tree::bplus_tree::MaintenanceEnd::Complete,
                    !done && next_lower.is_none()
                );
                if done || next_lower.is_none() {
                    while let Some((key, mut group)) = groups.pop_first() {
                        vacuum_drain_group(
                            &mut group,
                            xmin,
                            |version| to_delete.push(encode_key(&key, version)),
                            |_, refs_blob| has_blob_ref |= refs_blob,
                        );
                        if group.capacity() > spare.capacity() {
                            spare = group;
                        }
                        cur_raw = Some(key);
                    }
                }
                #[cfg(test)]
                std::mem::drop(scan_probe);
                #[cfg(test)]
                note_vac_pending(to_delete.len());
                // M61b：`to_delete` 是**升序**的（走查顺序就是编码键顺序：先按 raw_key 升序、组内按版本升序），
                // 而可删行天然成块（M61 实测 k = 16.5~17.9 行/叶子）⇒ 交给批量门，一趟下降带一整片叶子。
                // ⚠️ 对照臂（门控 `BPLUS_BATCH_OLD_SHAPE`，`#[cfg(test)]` ⇒ 不随 crate 发布）逐行删除，
                //   同一份库、同一批键 ⇒ 比值只归因于"合批 vs 不合批"这一件事。
                if !to_delete.is_empty() {
                    #[cfg(test)]
                    let _probe = crate::common::maintenance_probe::Timer::start(
                        crate::common::maintenance_probe::Stage::VacuumDelete,
                    );
                    to_delete.sort();
                    let planned = to_delete.len();
                    #[cfg(test)]
                    let per_row = std::env::var("BPLUS_BATCH_OLD_SHAPE").is_ok();
                    #[cfg(not(test))]
                    let per_row = false;
                    if per_row {
                        for enc in to_delete.drain(..) {
                            let _ = kv.delete(enc);
                        }
                    } else {
                        let _ = kv.delete_batch(&to_delete);
                        to_delete.clear();
                    }
                    kv.ensure_healthy()?;
                    // 口径与改前一字不差：按"计划删除的行数"计（不是按门返回的实删数）
                    removed += planned;
                }
                match next_lower {
                    Some(l) if !done => lower = Some(l),
                    _ => break,
                }
            }
            let next_cursor = if truncated { cur_raw } else { None };

            // Blob GC（教学简化）：
            // - 若已无任何大 value 引用 → clear 文件
            // - 若仍有引用 → 暂不重写（append-only 空间可能残留；完整 compact 后续可做）
            //   这样避免 Windows 上打开句柄重写导致读路径异常。
            //
            // 「有没有人引用 blob」是个全库问题，所以只在这次调用本来就是全库扫描时才处理
            // （判据用 budget 而不是游标：一把 key 的小库任何一片都正好读到库尾，
            //   用游标判断会让自动回收每一片都去动 blob 文件）。
            let blob_rewritten = if budget_rows != u64::MAX {
                false
            } else if !has_blob_ref {
                let old_len = blob.path().metadata().map(|m| m.len()).unwrap_or(0);
                if old_len > 8 {
                    blob.clear_in_place()?;
                    true
                } else {
                    false
                }
            } else {
                false
            };

            // 只读回收不得顺带刷出无关事务的脏页。WAL 和 checkpoint 仍负责持久性；实际删除须发布页面和空闲清单后才能报告回收完成。
            let flush_pages = removed > 0;
            #[cfg(test)]
            let flush_pages = flush_pages || vacuum_sync_reference::enabled();
            if flush_pages {
                kv.set_next_version(self.next_version.load(Ordering::SeqCst));
                #[cfg(test)]
                let _probe = crate::common::maintenance_probe::Timer::start(
                    crate::common::maintenance_probe::Stage::VacuumSync,
                );
                kv.try_flush_all()?;
            }
            // 回收不追加 blob；真正全量重写时 clear_in_place 已同步，因此每个有界回收无需额外无条件同步 blob。
            #[cfg(test)]
            if vacuum_sync_reference::enabled() {
                blob.sync()?;
            }

            Ok(VacuumRound {
                stats: VacuumStats {
                    xmin,
                    versions_removed: removed,
                    blob_rewritten: Some(blob_rewritten),
                    skipped_active_txn: false,
                    rows_scanned: scanned,
                },
                next_cursor,
            })
        })
    }

    // M112 对照采用 ee74ca7 的分配路径，删除入口和磁盘读写入口相同。
    #[cfg(test)]
    pub(super) fn vacuum_bounded_reference(
        &self,
        after: Option<&[u8]>,
        budget_rows: u64,
    ) -> std::io::Result<VacuumRound> {
        let _active_guard = self.active_txn.lock().unwrap();
        if !_active_guard.is_empty() {
            return Ok(VacuumRound {
                stats: VacuumStats {
                    xmin: 0,
                    versions_removed: 0,
                    blob_rewritten: Some(false),
                    skipped_active_txn: true,
                    rows_scanned: 0,
                },
                // 游标原样退回：跳过不丢进度，下一轮从同一处接着扫
                next_cursor: after.map(|k| k.to_vec()),
            });
        }
        let xmin = self.next_version.load(Ordering::SeqCst);

        let mut kv = self.kv.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();
        kv.ensure_healthy()?;

        // ① 一趟按引用的遍历。判「留哪一版」只需要三样：版本号、这行是不是墓碑、
        //    这行是不是 blob 指针 —— 全都在叶子条目手里就能读到，不必把 value 克隆出来。
        //    旧实现在这里有两趟 `kv.iter()`（每行克隆一次 value），第二趟纯粹为查 blob 引用。
        //    M36：**只攒当前这一把键的那一组**（以前是全库归组 ⇒ vacuum 自己的瞬时内存
        //    = O(全库物理行)，600k 档 +89.4 MB 那个形状），版本键按 `raw ‖ version_be`
        //    升序到达 ⇒ 判完一组就地并进 `to_delete`。
        let budget_rows = budget_rows.max(1);
        let mut groups: crate::common::order::KeyWindow<Vec<(u64, Vec<u8>, bool, bool)>> =
            crate::common::order::KeyWindow::new();
        let mut to_delete: Vec<Vec<u8>> = Vec::new();
        let mut removed = 0usize;
        let mut has_blob_ref = false;
        let mut scanned = 0u64;
        let mut cur_raw: Option<Vec<u8>> = None;
        let mut truncated = false;
        // M37：按块走查（与 bitcask / lsm / memory 同一形状）—— 走查期间 `kv` 被
        // `scan_entries_break_from` 借住，删除只能在块与块之间做 ⇒ `to_delete` 每块清一次，
        // 不再 O(全库)。块间下界 `enc ‖ 0x00`：enc 域的严格后继方向。
        let mut lower: Option<Vec<u8>> = after.map(|k| k.to_vec());
        loop {
            let mut chunk_rows = 0usize;
            let mut next_lower: Option<Vec<u8>> = None;
            let mut done = false;
            // 本块续跑下界：首块 = 逻辑游标 `after`，其后 = 上一块最后一行的 `enc ‖ 0x00`。
            let resume_bound = lower.clone();
            #[cfg(test)]
            let scan_probe = crate::common::maintenance_probe::Timer::start(
                crate::common::maintenance_probe::Stage::VacuumScan,
            );
            kv.scan_entries_break_from(lower.as_deref(), |enc, row| {
                // ⚠️ `scan_entries_break_from` 只下降到 containing 叶子，随后**从该片第一条**起遍历
                // （叶粒度定位，过滤交给调用方——见其文档）。上一块已经消费过的行会被再喂一遍；若某个
                // 版本组正好骑在块边界上（它最新那版恰是续跑叶子的首条），那一版会**第二次**进 `group`，
                // 让 `vacuum_plan_group` 排序后把"同一 enc 的较早副本"划进 `last_old` 之前、连同活版本的
                // enc 一起塞进 `to_delete`（判保留的最新版本被物理删掉 ⇒ 静默丢活键）。
                // ⇒ 按本块续跑下界显式跳过已消费的行（`resume_bound` 是上一块尾行的严格后继方向）。
                if resume_bound.as_ref().is_some_and(|b| enc < b.as_slice()) {
                    return false;
                }
                let Some(vk) = decode_key(enc) else {
                    return false;
                };
                // 下界落在键的字节域，`raw_key ≤ after` 的那些行（含 `after` 自己那一整组）
                // 还在范围内 —— 显式跳过，游标才是逻辑 key 域的。
                if after.is_some_and(|a| vk.raw_key.as_slice() <= a) {
                    return false;
                }
                while let Some((key, mut group)) = groups.pop_before(&enc, xmin) {
                    vacuum_drain_group(
                        &mut group,
                        xmin,
                        |row| to_delete.push(row),
                        |_, refs_blob| has_blob_ref |= refs_blob,
                    );
                    cur_raw = Some(key);
                }
                // 整个重叠前缀窗口完成判定后才能停止。
                if groups.is_empty() && scanned >= budget_rows {
                    truncated = true;
                    done = true;
                    return true;
                }
                scanned += 1;
                let is_tomb = row.is_none();
                let refs_blob = row
                    .as_ref()
                    .is_some_and(|s| s.first() == Some(&crate::bplus_tree::storage::VAL_TAG_BLOB));
                groups
                    .entry(vk.raw_key)
                    .push((vk.version, enc.to_vec(), is_tomb, refs_blob));
                chunk_rows += 1;
                if chunk_rows >= vac_chunk_limit() {
                    let mut bound = enc.to_vec();
                    bound.push(0u8);
                    next_lower = Some(bound);
                    return true;
                }
                false
            });
            if done || next_lower.is_none() {
                while let Some((key, mut group)) = groups.pop_first() {
                    vacuum_drain_group(
                        &mut group,
                        xmin,
                        |row| to_delete.push(row),
                        |_, refs_blob| has_blob_ref |= refs_blob,
                    );
                    cur_raw = Some(key);
                }
            }
            #[cfg(test)]
            std::mem::drop(scan_probe);
            #[cfg(test)]
            note_vac_pending(to_delete.len());
            // M61b：`to_delete` 是**升序**的（走查顺序就是编码键顺序：先按 raw_key 升序、组内按版本升序），
            // 而可删行天然成块（M61 实测 k = 16.5~17.9 行/叶子）⇒ 交给批量门，一趟下降带一整片叶子。
            // ⚠️ 对照臂（门控 `BPLUS_BATCH_OLD_SHAPE`，`#[cfg(test)]` ⇒ 不随 crate 发布）逐行删除，
            //   同一份库、同一批键 ⇒ 比值只归因于"合批 vs 不合批"这一件事。
            if !to_delete.is_empty() {
                #[cfg(test)]
                let _probe = crate::common::maintenance_probe::Timer::start(
                    crate::common::maintenance_probe::Stage::VacuumDelete,
                );
                to_delete.sort();
                let planned = to_delete.len();
                #[cfg(test)]
                let per_row = std::env::var("BPLUS_BATCH_OLD_SHAPE").is_ok();
                #[cfg(not(test))]
                let per_row = false;
                if per_row {
                    for enc in to_delete.drain(..) {
                        let _ = kv.delete(enc);
                    }
                } else {
                    let _ = kv.delete_batch(&to_delete);
                    to_delete.clear();
                }
                kv.ensure_healthy()?;
                // 口径与改前一字不差：按"计划删除的行数"计（不是按门返回的实删数）
                removed += planned;
            }
            match next_lower {
                Some(l) if !done => lower = Some(l),
                _ => break,
            }
        }
        let next_cursor = if truncated { cur_raw } else { None };

        // Blob GC（教学简化）：
        // - 若已无任何大 value 引用 → clear 文件
        // - 若仍有引用 → 暂不重写（append-only 空间可能残留；完整 compact 后续可做）
        //   这样避免 Windows 上打开句柄重写导致读路径异常。
        //
        // 「有没有人引用 blob」是个全库问题，所以只在这次调用本来就是全库扫描时才处理
        // （判据用 budget 而不是游标：一把 key 的小库任何一片都正好读到库尾，
        //   用游标判断会让自动回收每一片都去动 blob 文件）。
        let blob_rewritten = if budget_rows != u64::MAX {
            false
        } else if !has_blob_ref {
            let old_len = blob.path().metadata().map(|m| m.len()).unwrap_or(0);
            if old_len > 8 {
                blob.clear_in_place()?;
                true
            } else {
                false
            }
        } else {
            false
        };

        // 把当前 next_version 一并写入 meta，避免下次 open 因 meta 过旧依赖大 WAL
        let next_ver = self.next_version.load(Ordering::SeqCst);
        kv.set_next_version(next_ver);
        #[cfg(test)]
        let _probe = crate::common::maintenance_probe::Timer::start(
            crate::common::maintenance_probe::Stage::VacuumSync,
        );
        kv.try_flush_all()?;
        if vacuum_sync_reference::enabled() {
            blob.sync()?;
        }

        Ok(VacuumRound {
            stats: VacuumStats {
                xmin,
                versions_removed: removed,
                blob_rewritten: Some(blob_rewritten),
                skipped_active_txn: false,
                rows_scanned: scanned,
            },
            next_cursor,
        })
    }
}
