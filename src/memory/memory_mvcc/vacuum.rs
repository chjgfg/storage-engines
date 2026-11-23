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
    pub(super) fn tick_auto_vacuum(&self) {
        let _: Result<(), std::convert::Infallible> = self.vac.run_pending(|after, rows| {
            #[cfg(test)]
            let _probe = crate::common::maintenance_probe::Timer::start(
                crate::common::maintenance_probe::Stage::AutoVacuum,
            );
            Ok(self.vacuum_bounded(after, rows))
        });
    }

    /// 轻量回收状态，不扫描数据库。
    pub fn reclamation_status(&self) -> crate::common::ReclamationStatus {
        self.vac.status(&self.active_txn.lock().unwrap())
    }

    /// 自动回收的切片宽度（行数）：每攒够这么多行新版本，就裁一片这么多行。
    ///
    /// 默认 [`MVCC::VACUUM_AUTO_SLICE_DEFAULT`]；**设 0 关闭自动回收**。内存引擎没有磁盘
    /// 空间问题，但旧版本仍占堆内存。点读早停与版本回收是两件独立的事。
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
    /// 就是 `vacuum_bounded(None, u64::MAX)`；保留规则、「有活跃事务就整轮跳过」的理由和
    /// 游标语义都在 [`MVCC::vacuum_bounded`] 那边。
    pub fn vacuum(&self) -> VacuumStats {
        self.vacuum_bounded(None, u64::MAX).stats
    }
    /// 从上次物理游标继续，按扫描行预算裁出一个回收切片。
    /// 跨片版本组保持连续，删除前完成该组判定；报告实际扫描和删除数供调度记账。

    /// 有界版本回收：从逻辑 key `after`（**不含**）之后开始扫，最多读 `budget_rows` 行。
    ///
    /// 规则与 [`MVCC::vacuum`] 一致：每个逻辑 key 只保留「最后一个 version < xmin」+
    /// 全部 version >= xmin；若 xmin 之前最终是 tombstone 且无更新版本 → 整 key 删除。
    /// **有任何活跃事务 ⇒ 整轮跳过**（`skipped_active_txn = true`），此时游标原样退回，
    /// 下一轮从同一处接着扫。理由见 [`crate::bitcask::MVCC::vacuum_bounded`]：旧的
    /// `min(活跃事务 id)` 会裁掉活跃事务快照仍需要的版本 ⇒ 静默读到旧值。
    ///
    /// 截断只发生在逻辑 key 边界（半条链不能收：判「留哪一版」要求整组版本都在手上），
    /// 所以一行的预算偏差 = 那一条链的长度。本引擎无 WAL/blob，也就没有 blob 回收和磁盘压缩。
    pub fn vacuum_bounded(&self, after: Option<&[u8]>, budget_rows: u64) -> VacuumRound {
        #[cfg(test)]
        if vacuum_cost_reference::enabled() {
            return self.vacuum_bounded_reference(after, budget_rows);
        }
        use crate::common::version::decode_key_ref;
        #[cfg(test)]
        let _active_guard = crate::common::maintenance_probe::lock(
            &self.active_txn,
            crate::common::maintenance_probe::LockKind::Registry,
        );
        #[cfg(not(test))]
        let _active_guard = self.active_txn.lock().unwrap();
        if !_active_guard.is_empty() {
            return VacuumRound {
                stats: VacuumStats {
                    xmin: 0,
                    versions_removed: 0,
                    blob_rewritten: None,
                    skipped_active_txn: true,
                    rows_scanned: 0,
                },
                next_cursor: after.map(|k| k.to_vec()),
            };
        }
        let xmin = self.next_version.load(Ordering::SeqCst);

        // 分组按引用走表：判「留哪一版」只需要版本号 + 是不是墓碑，
        // 不需要把每行 value 克隆出来（旧实现是 `store.iter().collect()`）。
        //
        // M36：**只攒当前这一把键的那一组**，看到后继就把上一组交出去判 ⇒ 瞬时内存从
        // O(全库物理行)（600k 档实测 +89.4 MB，比要裁的东西大两个数量级）降到
        // O(最长那条链)。版本键按 `raw ‖ version_be` 升序到达，同一把键的行可能被二进制前缀打断，按公共 KeyWindow 等待完整组。
        // M37：再按块走 —— 走查持读锁、删除要写锁 ⇒ 每块读完就**放掉读锁**再删这一块，
        // 于是 `to_delete` 从 O(全库) 变成 O(一块)（这一家实测基数小：600k 档 `vacuum()`
        // 瞬时 4.0 MB，改完是 O(块)；改它的理由是"同根因不留三家"，不是那 4 MB）。
        // 块间下界 `enc ‖ 0x00`：enc 域的严格后继方向，与 bitcask/lsm 同一个理由。
        let budget_rows = budget_rows.max(1);
        let mut groups: crate::common::order::KeyWindow<Vec<(u64, u64, bool, ())>> =
            crate::common::order::KeyWindow::new();
        let mut spare = Vec::new();
        let mut to_delete: Vec<Vec<u8>> = Vec::new();
        let mut removed = 0usize;
        let mut scanned = 0u64;
        let mut cur_raw: Option<Vec<u8>> = None;
        let mut truncated = false;
        let mut lower: Option<Vec<u8>> = after.map(|k| k.to_vec());
        loop {
            let mut chunk_rows = 0usize;
            let mut next_lower: Option<Vec<u8>> = None;
            let mut done = false;
            #[cfg(test)]
            let scan_probe = crate::common::maintenance_probe::Timer::start(
                crate::common::maintenance_probe::Stage::VacuumScan,
            );
            {
                #[cfg(test)]
                let store = crate::common::maintenance_probe::lock(
                    &self.store,
                    crate::common::maintenance_probe::LockKind::Storage,
                );
                #[cfg(not(test))]
                let store = self.store.lock().unwrap();
                store.scan_entries_from(lower.as_deref(), |enc, val| {
                    let Some((raw_key, version)) = decode_key_ref(enc) else {
                        return false;
                    };
                    if after.is_some_and(|a| raw_key <= a) {
                        return false;
                    }
                    while let Some((key, mut group)) = groups.pop_before(&enc, xmin) {
                        vacuum_drain_group(
                            &mut group,
                            xmin,
                            |version| to_delete.push(encode_key(&key, version)),
                            |_, _| {},
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
                    groups
                        .entry_with(raw_key, || {
                            let mut owned = cur_raw.take().unwrap_or_default();
                            owned.clear();
                            owned.extend_from_slice(raw_key);
                            (owned, std::mem::take(&mut spare))
                        })
                        .push((version, version, val.is_none(), ()));
                    chunk_rows += 1;
                    if chunk_rows >= VAC_CHUNK_ROWS {
                        let mut bound = enc.to_vec();
                        bound.push(0u8);
                        next_lower = Some(bound);
                        return true;
                    }
                    false
                });
            }
            if done || next_lower.is_none() {
                while let Some((key, mut group)) = groups.pop_first() {
                    vacuum_drain_group(
                        &mut group,
                        xmin,
                        |version| to_delete.push(encode_key(&key, version)),
                        |_, _| {},
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
            // 读锁已经放掉 ⇒ 这一块单独拿写锁删（以前是"整库攒完再删一次"）
            {
                #[cfg(test)]
                let _probe = crate::common::maintenance_probe::Timer::start(
                    crate::common::maintenance_probe::Stage::VacuumDelete,
                );
                #[cfg(test)]
                let mut store = crate::common::maintenance_probe::lock(
                    &self.store,
                    crate::common::maintenance_probe::LockKind::Storage,
                );
                #[cfg(not(test))]
                let mut store = self.store.lock().unwrap();
                for enc in to_delete.drain(..) {
                    let _ = store.delete(&enc);
                    removed += 1;
                }
            }
            match next_lower {
                Some(l) if !done => lower = Some(l),
                _ => break,
            }
        }
        let next_cursor = if truncated { cur_raw } else { None };

        VacuumRound {
            stats: VacuumStats {
                xmin,
                versions_removed: removed,
                blob_rewritten: None,
                skipped_active_txn: false,
                rows_scanned: scanned,
            },
            next_cursor,
        }
    }

    #[cfg(test)]
    pub(super) fn vacuum_bounded_reference(
        &self,
        after: Option<&[u8]>,
        budget_rows: u64,
    ) -> VacuumRound {
        let _active_guard = self.active_txn.lock().unwrap();
        if !_active_guard.is_empty() {
            return VacuumRound {
                stats: VacuumStats {
                    xmin: 0,
                    versions_removed: 0,
                    blob_rewritten: None,
                    skipped_active_txn: true,
                    rows_scanned: 0,
                },
                next_cursor: after.map(|k| k.to_vec()),
            };
        }
        let xmin = self.next_version.load(Ordering::SeqCst);

        // 分组按引用走表：判「留哪一版」只需要版本号 + 是不是墓碑，
        // 不需要把每行 value 克隆出来（旧实现是 `store.iter().collect()`）。
        //
        // M36：**只攒当前这一把键的那一组**，看到后继就把上一组交出去判 ⇒ 瞬时内存从
        // O(全库物理行)（600k 档实测 +89.4 MB，比要裁的东西大两个数量级）降到
        // O(最长那条链)。版本键按 `raw ‖ version_be` 升序到达，同一把键的行可能被二进制前缀打断，按公共 KeyWindow 等待完整组。
        // M37：再按块走 —— 走查持读锁、删除要写锁 ⇒ 每块读完就**放掉读锁**再删这一块，
        // 于是 `to_delete` 从 O(全库) 变成 O(一块)（这一家实测基数小：600k 档 `vacuum()`
        // 瞬时 4.0 MB，改完是 O(块)；改它的理由是"同根因不留三家"，不是那 4 MB）。
        // 块间下界 `enc ‖ 0x00`：enc 域的严格后继方向，与 bitcask/lsm 同一个理由。
        let budget_rows = budget_rows.max(1);
        let mut groups: crate::common::order::KeyWindow<Vec<(u64, Vec<u8>, bool, ())>> =
            crate::common::order::KeyWindow::new();
        let mut to_delete: Vec<Vec<u8>> = Vec::new();
        let mut removed = 0usize;
        let mut scanned = 0u64;
        let mut cur_raw: Option<Vec<u8>> = None;
        let mut truncated = false;
        let mut lower: Option<Vec<u8>> = after.map(|k| k.to_vec());
        loop {
            let mut chunk_rows = 0usize;
            let mut next_lower: Option<Vec<u8>> = None;
            let mut done = false;
            #[cfg(test)]
            let scan_probe = crate::common::maintenance_probe::Timer::start(
                crate::common::maintenance_probe::Stage::VacuumScan,
            );
            {
                let store = self.store.lock().unwrap();
                store.scan_entries_from(lower.as_deref(), |enc, val| {
                    let Some(vk) = decode_key(enc) else {
                        return false;
                    };
                    if after.is_some_and(|a| vk.raw_key.as_slice() <= a) {
                        return false;
                    }
                    while let Some((key, mut group)) = groups.pop_before(&enc, xmin) {
                        vacuum_drain_group(&mut group, xmin, |row| to_delete.push(row), |_, _| {});
                        cur_raw = Some(key);
                    }
                    // 整个重叠前缀窗口完成判定后才能停止。
                    if groups.is_empty() && scanned >= budget_rows {
                        truncated = true;
                        done = true;
                        return true;
                    }
                    scanned += 1;
                    groups
                        .entry(vk.raw_key)
                        .push((vk.version, enc.to_vec(), val.is_none(), ()));
                    chunk_rows += 1;
                    if chunk_rows >= VAC_CHUNK_ROWS {
                        let mut bound = enc.to_vec();
                        bound.push(0u8);
                        next_lower = Some(bound);
                        return true;
                    }
                    false
                });
            }
            if done || next_lower.is_none() {
                while let Some((key, mut group)) = groups.pop_first() {
                    vacuum_drain_group(&mut group, xmin, |row| to_delete.push(row), |_, _| {});
                    cur_raw = Some(key);
                }
            }
            #[cfg(test)]
            std::mem::drop(scan_probe);
            #[cfg(test)]
            note_vac_pending(to_delete.len());
            // 读锁已经放掉 ⇒ 这一块单独拿写锁删（以前是"整库攒完再删一次"）
            {
                #[cfg(test)]
                let _probe = crate::common::maintenance_probe::Timer::start(
                    crate::common::maintenance_probe::Stage::VacuumDelete,
                );
                let mut store = self.store.lock().unwrap();
                for enc in to_delete.drain(..) {
                    let _ = store.delete(&enc);
                    removed += 1;
                }
            }
            match next_lower {
                Some(l) if !done => lower = Some(l),
                _ => break,
            }
        }
        let next_cursor = if truncated { cur_raw } else { None };

        VacuumRound {
            stats: VacuumStats {
                xmin,
                versions_removed: removed,
                blob_rewritten: None,
                skipped_active_txn: false,
                rows_scanned: scanned,
            },
            next_cursor,
        }
    }
}
