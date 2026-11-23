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
    /// LSM 的一片是「每个源各读 slice 行」的窗口，块读取代价由稀疏索引压住。
    pub(super) fn tick_auto_vacuum(&self) {
        if let Err(e) = self.vac.run_pending(|after, rows| {
            #[cfg(test)]
            let _timer = crate::common::maintenance_probe::Timer::start(
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
    /// 得靠手动 `vacuum()`。当年就是 LSM 先撑不住（一天），这条通道必须默认是通的。
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
    /// 保留规则和 [`MVCC::vacuum`] 完全一致：
    /// - **有任何活跃事务 ⇒ 整轮跳过**（`skipped_active_txn = true`），游标原样退回
    /// - 无活跃事务时 `xmin = next_version`
    /// - 每个逻辑 key：只保留「最后一个 version < xmin」+ 全部 version >= xmin
    /// - 若 xmin 之前最终是 tombstone 且无更新版本 → 整 key 删除
    ///
    /// 遍历用 [`LSMTree::light_iter_range`]（惰性 k 路归并，**凑够预算就当场收手**），
    /// 所以一轮的代价与库总量无关 —— M29 之前这里走的是 `scan_latest_light_window`
    /// （**每个源各取满 `预算` 行**再归并），代价 = `源数 × 预算`，于是"一片"的每行单价
    /// 随 SST 文件数涨、也就是随库涨（600k 实测 5.8~11×，凭据见
    /// `vacuum_slice_fold_attribution` 与守卫 `test_vacuum_slice_walks_only_its_budget`）。
    /// `kind == 2`（`EntryValue::Drop`，已被硬删的行）整行不参与，语义与 `iter()` 一致；
    /// `Put(None)` 才是「逻辑删除行」。
    ///
    /// **游标只在组边界上落**：收手的条件是"看到了后继组的第一行"，所以被当游标的那一组
    /// 一定已经收齐；自然走到库尾时最后一组同样收齐。旧窗口实现还要额外防"右界把最后一组
    /// 切成半条链"（那个 `boundary` 与"放大 4 倍重试"的循环都是为它存在的），惰性流从结构上
    /// 不会切在半组上，所以那套补偿一并删掉了。
    ///
    /// `set_mvcc_xmin_filter` 只在**全库轮次**（`budget_rows == u64::MAX`）下发：它影响
    /// compact 丢哪些旧墓碑，语义是「全库已按这个 xmin 裁过」。
    ///
    /// 为什么跳过而不是用旧的 `min(活跃事务 id)`：理由与锁序见
    /// [`crate::bitcask::MVCC::vacuum_bounded`]（四引擎同一套语义）。
    pub fn vacuum_bounded(
        &self,
        after: Option<&[u8]>,
        budget_rows: u64,
    ) -> std::io::Result<VacuumRound> {
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
            return Ok(VacuumRound {
                stats: VacuumStats {
                    xmin: 0,
                    versions_removed: 0,
                    blob_rewritten: None,
                    skipped_active_txn: true,
                    rows_scanned: 0,
                },
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
        #[cfg(test)]
        // M41 的分段标尺（与 bitcask 共用 [`crate::mem_probe`] 那一把）：①走查+删除、
        // ②`kv.flush()`（memtable → SST，顺带压实）、③`set_mvcc_xmin_filter` + blob GC。
        // ⚠️ 带门控（`VACUUM_STAGE_SPLIT`），不设时纯读高水位、不压 —— 常开会把外层台架
        // 对 `vacuum()` 的读数锯短（bitcask 那边实测 171.4 → 47.2 MB）。
        let mut vac_base = crate::mem_probe::stage_base();

        // 分组走 M3a 为轻量视图：判「留哪一版」只需要版本号 + 是不是墓碑。
        // 旧实现用 `kv.iter()`，而它会先把**每个源整个 collect() 进内存（连 value）**
        // ⇒ 一次 vacuum 的峰值内存约等于整个库。
        let budget_rows = budget_rows.max(1);

        // ⚠️ 这里必须用**惰性**归并（`light_iter_range`），不能用旧的
        // `scan_latest_light_window`：那个是「每个源各取 `budget` 行再归并」，所以一片取到的
        // 行数是 `源数 × 预算`（2026-10-18 台架实测：24 个 SST 时一片交出 2048 行却取了
        // 22000 行；48 个文件时干脆等于整库），一片的每行单价因此**随 SST 文件数涨 = 随库涨**，
        // 600k 档那一片 14.7 µs/行就是这么来的（同档全量 scan 才 4.9 µs/行）。
        // 惰性那条每源只先取一行定堆顶，后面的行只有真被合并用到才取 ⇒ 代价 = 走到的行数。
        //
        // 游标语义跟着变简单了：自然走到库尾 ⇒ `next_cursor = None`（这一轮就是最后一片，
        // 最后一组也是收齐的，不用退回）；被预算截断 ⇒ 破圈时已经「看到了下一组的第一行」，
        // 所以 `cur_raw` 那一组收齐了，可以直接当游标。旧那份"窗口太窄就放大 4 倍重试"
        // 整个不需要了 —— 它存在的理由正是窗口右界可能把唯一的完整组切断。
        // M36：**只攒当前这一把键的那一组**（以前是全库归组的 `BTreeMap`，每行两个 `Vec`
        // ⇒ vacuum 自己的瞬时内存 = O(全库物理行)）。归并流的键序是 `raw ‖ version_be` 升序
        // ⇒ 同一把键的行可能被二进制前缀打断，按公共 KeyWindow 等待完整组，判完一组就地交出去。
        // M37：再把**块**加上 —— 每块至多 `VAC_CHUNK_ROWS` 行，出借用（`LightMerge` 按 `&self`
        // 借住 kv，删除要 `&mut kv`）之后立刻删这一块、并把保留行的 blob 引用当场并进 `referenced`
        // 然后丢掉 ⇒ `to_delete` 与"整张存活行清单"（原来交给 `gc_blobs(&keep_enc)`）这两笔
        // O(全库) 的账一起塌成 O(一块)。块间下界用 `enc ‖ 0x00`（严格后继方向，⚠️ 不是
        // `‖ 0xFF×8` 那种右界写法），与 bitcask 那条同源、同一个理由。
        let full_sweep = budget_rows == u64::MAX;
        let blob_plan = kv.blob_gc_plan(full_sweep)?;
        let collect_refs = blob_plan == BlobGcPlan::ScanReferences;
        let mut groups: crate::common::order::KeyWindow<Vec<(u64, (u64, u8), bool, ())>> =
            crate::common::order::KeyWindow::new();
        let mut spare = Vec::new();
        let mut to_delete: Vec<(Vec<u8>, u8)> = Vec::new();
        let mut keep_enc: Vec<Vec<u8>> = Vec::new();
        // 只数"真有大 value 的行"，一条一个 u64 ⇒ 没有大 value 时这一份一分钱不花
        let mut referenced: HashSet<u64> = HashSet::new();
        let mut removed = 0usize;
        let mut scanned = 0u64;
        let mut cur_raw: Option<Vec<u8>> = None;
        let mut budget_cut = false;
        let mut lower: Option<Vec<u8>> = after.map(|k| k.to_vec());
        kv.with_vacuum_writes(full_sweep, |kv| {
            loop {
                #[cfg(test)]
                let scan_timer = crate::common::maintenance_probe::Timer::start(
                    crate::common::maintenance_probe::Stage::VacuumScan,
                );
                let mut chunk_rows = 0usize;
                let mut next_lower: Option<Vec<u8>> = None;
                let mut done = false;
                {
                    let mut it = kv.vacuum_iter_range(lower.as_deref(), None, u64::MAX);
                    while let Some(row) = it.next() {
                        let (enc, le) = row?;
                        // 尺子：这片真的折过多少把编码键（与 M27 那四家守卫同一把）
                        #[cfg(test)]
                        crate::lsm_tree::sst::note_light_visit();
                        let Some((raw_key, version)) = decode_key_ref(&enc) else {
                            continue;
                        };
                        if after.is_some_and(|a| raw_key <= a) {
                            continue;
                        }
                        while let Some((key, mut group)) = groups.pop_before(&enc, xmin) {
                            vacuum_drain_group(
                                &mut group,
                                xmin,
                                |(version, kind)| to_delete.push((encode_key(&key, version), kind)),
                                |(version, _), _| {
                                    if collect_refs {
                                        keep_enc.push(encode_key(&key, version));
                                    }
                                },
                            );
                            if group.capacity() > spare.capacity() {
                                spare = group;
                            }
                            cur_raw = Some(key);
                        }
                        // 整个重叠前缀窗口完成判定后才能停止。
                        if groups.is_empty() && scanned >= budget_rows {
                            budget_cut = true;
                            done = true;
                            break;
                        }
                        scanned += 1;
                        // Drop 行也参与「换组」的判定（它照样推进键序），只是不作为裁剪候选
                        let group = groups.entry_with(raw_key, || {
                            let mut key = cur_raw.take().unwrap_or_default();
                            key.clear();
                            key.extend_from_slice(raw_key);
                            (key, std::mem::take(&mut spare))
                        });
                        if le.kind == 2 {
                            continue;
                        }
                        group.push((version, (version, le.kind), le.kind == 1, ()));
                        chunk_rows += 1;
                        if chunk_rows >= VAC_CHUNK_ROWS {
                            let mut bound = enc;
                            bound.push(0u8);
                            next_lower = Some(bound);
                            break;
                        }
                    }
                }
                if done || next_lower.is_none() {
                    while let Some((key, mut group)) = groups.pop_first() {
                        vacuum_drain_group(
                            &mut group,
                            xmin,
                            |(version, kind)| to_delete.push((encode_key(&key, version), kind)),
                            |(version, _), _| {
                                if collect_refs {
                                    keep_enc.push(encode_key(&key, version));
                                }
                            },
                        );
                        if group.capacity() > spare.capacity() {
                            spare = group;
                        }
                        cur_raw = Some(key);
                    }
                }
                #[cfg(test)]
                note_vac_pending(to_delete.len() + keep_enc.len());
                #[cfg(test)]
                drop(scan_timer);
                #[cfg(test)]
                let delete_timer = crate::common::maintenance_probe::Timer::start(
                    crate::common::maintenance_probe::Stage::VacuumDelete,
                );
                for (enc, kind) in to_delete.drain(..) {
                    kv.remove_vacuum_known(crate::lsm_tree::lsm_tree::VacuumDelete::observed(
                        enc, kind,
                    )?)?;
                    removed += 1;
                }
                #[cfg(test)]
                drop(delete_timer);
                if collect_refs {
                    kv.collect_blob_refs(&keep_enc, &mut referenced);
                    keep_enc.clear();
                }
                match next_lower {
                    Some(l) if !done => lower = Some(l),
                    _ => break,
                }
            }
            Ok(())
        })?;
        let next_cursor = if budget_cut { cur_raw } else { None };
        #[cfg(test)]
        {
            vac_base = crate::mem_probe::stage_mark(0, vac_base);
        }

        // 有界轮次只把删除留在 memtable 里，交给正常的 memtable flush 阈值收口：
        // 每片都 flush 会在 L0 造出一堆只装墓碑的小 SST，压实代价比省下的那点持久性贵。
        // 新事务仍可能看见 version < xmin；删除屏障必须在跨层压实时保留，直到被遮住的
        // 旧来源一起参与合并。不能仅凭版本小于 xmin 就宣称它不可见。
        let mut blob_rewritten = None;
        if full_sweep {
            #[cfg(test)]
            let sync_timer = crate::common::maintenance_probe::Timer::start(
                crate::common::maintenance_probe::Stage::VacuumSync,
            );
            kv.flush()?;
            #[cfg(test)]
            drop(sync_timer);
            #[cfg(test)]
            {
                vac_base = crate::mem_probe::stage_mark(1, vac_base);
            }
            // 后续 compact 丢弃 ver < xmin 的 tombstone 版本键。
            // 只在全库轮次下发：判据用 budget 而不是游标，一把 key 的小库任何一片都读到库尾，
            // 用游标判断会让自动回收每片都改动 compact 的过滤条件。
            kv.set_mvcc_xmin_filter(xmin);

            // blob 旁路的空间回收：必须在删完旧行之后、且只有全库扫完才敢做 ——
            // 「还有没有人在引用这个 blob」是个全库问题。引用集合是上面逐块并进来的，
            // 这里只剩"按集合删孤儿文件"那一半（`gc_blobs` 那个收整张清单的入口仍然在，
            // 它就是这两半的壳）。
            blob_rewritten = Some(match blob_plan {
                BlobGcPlan::ScanReferences => kv.gc_blobs_referenced(&referenced)? > 0,
                BlobGcPlan::NoFiles | BlobGcPlan::SkipBounded => false,
            });
            #[cfg(test)]
            {
                let _ = crate::mem_probe::stage_mark(2, vac_base);
            }
        }

        Ok(VacuumRound {
            stats: VacuumStats {
                xmin,
                versions_removed: removed,
                blob_rewritten,
                skipped_active_txn: false,
                rows_scanned: scanned,
            },
            next_cursor,
        })
    }

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
                    blob_rewritten: None,
                    skipped_active_txn: true,
                    rows_scanned: 0,
                },
                next_cursor: after.map(|k| k.to_vec()),
            });
        }
        let xmin = self.next_version.load(Ordering::SeqCst);

        let mut kv = self.kv.lock().unwrap();
        #[cfg(test)]
        // M41 的分段标尺（与 bitcask 共用 [`crate::mem_probe`] 那一把）：①走查+删除、
        // ②`kv.flush()`（memtable → SST，顺带压实）、③`set_mvcc_xmin_filter` + blob GC。
        // ⚠️ 带门控（`VACUUM_STAGE_SPLIT`），不设时纯读高水位、不压 —— 常开会把外层台架
        // 对 `vacuum()` 的读数锯短（bitcask 那边实测 171.4 → 47.2 MB）。
        let mut vac_base = crate::mem_probe::stage_base();

        // 分组走 M3a 为轻量视图：判「留哪一版」只需要版本号 + 是不是墓碑。
        // 旧实现用 `kv.iter()`，而它会先把**每个源整个 collect() 进内存（连 value）**
        // ⇒ 一次 vacuum 的峰值内存约等于整个库。
        let budget_rows = budget_rows.max(1);

        // ⚠️ 这里必须用**惰性**归并（`light_iter_range`），不能用旧的
        // `scan_latest_light_window`：那个是「每个源各取 `budget` 行再归并」，所以一片取到的
        // 行数是 `源数 × 预算`（2026-10-18 台架实测：24 个 SST 时一片交出 2048 行却取了
        // 22000 行；48 个文件时干脆等于整库），一片的每行单价因此**随 SST 文件数涨 = 随库涨**，
        // 600k 档那一片 14.7 µs/行就是这么来的（同档全量 scan 才 4.9 µs/行）。
        // 惰性那条每源只先取一行定堆顶，后面的行只有真被合并用到才取 ⇒ 代价 = 走到的行数。
        //
        // 游标语义跟着变简单了：自然走到库尾 ⇒ `next_cursor = None`（这一轮就是最后一片，
        // 最后一组也是收齐的，不用退回）；被预算截断 ⇒ 破圈时已经「看到了下一组的第一行」，
        // 所以 `cur_raw` 那一组收齐了，可以直接当游标。旧那份"窗口太窄就放大 4 倍重试"
        // 整个不需要了 —— 它存在的理由正是窗口右界可能把唯一的完整组切断。
        // M36：**只攒当前这一把键的那一组**（以前是全库归组的 `BTreeMap`，每行两个 `Vec`
        // ⇒ vacuum 自己的瞬时内存 = O(全库物理行)）。归并流的键序是 `raw ‖ version_be` 升序
        // ⇒ 同一把键的行可能被二进制前缀打断，按公共 KeyWindow 等待完整组，判完一组就地交出去。
        // M37：再把**块**加上 —— 每块至多 `VAC_CHUNK_ROWS` 行，出借用（`LightMerge` 按 `&self`
        // 借住 kv，删除要 `&mut kv`）之后立刻删这一块、并把保留行的 blob 引用当场并进 `referenced`
        // 然后丢掉 ⇒ `to_delete` 与"整张存活行清单"（原来交给 `gc_blobs(&keep_enc)`）这两笔
        // O(全库) 的账一起塌成 O(一块)。块间下界用 `enc ‖ 0x00`（严格后继方向，⚠️ 不是
        // `‖ 0xFF×8` 那种右界写法），与 bitcask 那条同源、同一个理由。
        let full_sweep = budget_rows == u64::MAX;
        let blob_plan = kv.blob_gc_plan(full_sweep)?;
        let collect_refs = blob_plan == BlobGcPlan::ScanReferences;
        let mut groups: crate::common::order::KeyWindow<Vec<(u64, Vec<u8>, bool, ())>> =
            crate::common::order::KeyWindow::new();
        let mut to_delete: Vec<Vec<u8>> = Vec::new();
        let mut keep_enc: Vec<Vec<u8>> = Vec::new();
        // 只数"真有大 value 的行"，一条一个 u64 ⇒ 没有大 value 时这一份一分钱不花
        let mut referenced: HashSet<u64> = HashSet::new();
        let mut removed = 0usize;
        let mut scanned = 0u64;
        let mut cur_raw: Option<Vec<u8>> = None;
        let mut budget_cut = false;
        let mut lower: Option<Vec<u8>> = after.map(|k| k.to_vec());
        loop {
            #[cfg(test)]
            let scan_timer = crate::common::maintenance_probe::Timer::start(
                crate::common::maintenance_probe::Stage::VacuumScan,
            );
            let mut chunk_rows = 0usize;
            let mut next_lower: Option<Vec<u8>> = None;
            let mut done = false;
            {
                let mut it = kv.light_iter_range(lower.as_deref(), None, u64::MAX);
                while let Some((enc, le)) = it.next() {
                    // 尺子：这片真的折过多少把编码键（与 M27 那四家守卫同一把）
                    #[cfg(test)]
                    crate::lsm_tree::sst::note_light_visit();
                    let Some(vk) = decode_key(&enc) else {
                        continue;
                    };
                    if after.is_some_and(|a| vk.raw_key.as_slice() <= a) {
                        continue;
                    }
                    while let Some((key, mut group)) = groups.pop_before(&enc, xmin) {
                        vacuum_drain_group(
                            &mut group,
                            xmin,
                            |row| to_delete.push(row),
                            |row, _| {
                                if collect_refs {
                                    keep_enc.push(row);
                                }
                            },
                        );
                        cur_raw = Some(key);
                    }
                    // 整个重叠前缀窗口完成判定后才能停止。
                    if groups.is_empty() && scanned >= budget_rows {
                        budget_cut = true;
                        done = true;
                        break;
                    }
                    scanned += 1;
                    // Drop 行也参与「换组」的判定（它照样推进键序），只是不作为裁剪候选
                    if le.kind == 2 {
                        groups.entry(vk.raw_key);
                        continue;
                    }
                    groups
                        .entry(vk.raw_key)
                        .push((vk.version, enc.clone(), le.kind == 1, ()));
                    chunk_rows += 1;
                    if chunk_rows >= VAC_CHUNK_ROWS {
                        let mut bound = enc.clone();
                        bound.push(0u8);
                        next_lower = Some(bound);
                        break;
                    }
                }
            }
            if done || next_lower.is_none() {
                while let Some((key, mut group)) = groups.pop_first() {
                    vacuum_drain_group(
                        &mut group,
                        xmin,
                        |row| to_delete.push(row),
                        |row, _| {
                            if collect_refs {
                                keep_enc.push(row);
                            }
                        },
                    );
                    cur_raw = Some(key);
                }
            }
            #[cfg(test)]
            note_vac_pending(to_delete.len() + keep_enc.len());
            #[cfg(test)]
            drop(scan_timer);
            #[cfg(test)]
            let delete_timer = crate::common::maintenance_probe::Timer::start(
                crate::common::maintenance_probe::Stage::VacuumDelete,
            );
            for enc in to_delete.drain(..) {
                let _ = kv.remove(&enc);
                removed += 1;
            }
            #[cfg(test)]
            drop(delete_timer);
            if collect_refs {
                kv.collect_blob_refs(&keep_enc, &mut referenced);
                keep_enc.clear();
            }
            match next_lower {
                Some(l) if !done => lower = Some(l),
                _ => break,
            }
        }
        let next_cursor = if budget_cut { cur_raw } else { None };
        #[cfg(test)]
        {
            vac_base = crate::mem_probe::stage_mark(0, vac_base);
        }

        // 有界轮次只把删除留在 memtable 里，交给正常的 memtable flush 阈值收口：
        // 每片都 flush 会在 L0 造出一堆只装墓碑的小 SST，压实代价比省下的那点持久性贵。
        // 新事务仍可能看见 version < xmin；删除屏障必须在跨层压实时保留，直到被遮住的
        // 旧来源一起参与合并。不能仅凭版本小于 xmin 就宣称它不可见。
        let mut blob_rewritten = None;
        if full_sweep {
            #[cfg(test)]
            let sync_timer = crate::common::maintenance_probe::Timer::start(
                crate::common::maintenance_probe::Stage::VacuumSync,
            );
            kv.flush()?;
            #[cfg(test)]
            drop(sync_timer);
            #[cfg(test)]
            {
                vac_base = crate::mem_probe::stage_mark(1, vac_base);
            }
            // 后续 compact 丢弃 ver < xmin 的 tombstone 版本键。
            // 只在全库轮次下发：判据用 budget 而不是游标，一把 key 的小库任何一片都读到库尾，
            // 用游标判断会让自动回收每片都改动 compact 的过滤条件。
            kv.set_mvcc_xmin_filter(xmin);

            // blob 旁路的空间回收：必须在删完旧行之后、且只有全库扫完才敢做 ——
            // 「还有没有人在引用这个 blob」是个全库问题。引用集合是上面逐块并进来的，
            // 这里只剩"按集合删孤儿文件"那一半（`gc_blobs` 那个收整张清单的入口仍然在，
            // 它就是这两半的壳）。
            blob_rewritten = Some(match blob_plan {
                BlobGcPlan::ScanReferences => kv.gc_blobs_referenced(&referenced)? > 0,
                BlobGcPlan::NoFiles | BlobGcPlan::SkipBounded => false,
            });
            #[cfg(test)]
            {
                let _ = crate::mem_probe::stage_mark(2, vac_base);
            }
        }

        Ok(VacuumRound {
            stats: VacuumStats {
                xmin,
                versions_removed: removed,
                blob_rewritten,
                skipped_active_txn: false,
                rows_scanned: scanned,
            },
            next_cursor,
        })
    }
}
