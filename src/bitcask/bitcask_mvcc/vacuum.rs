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
    /// 一片的代价 = 读 `slice` 行元数据（不读 value），锁也就按这么久。
    /// 长尾那一片（库尾之后）会自动从头开始下一遍。
    pub(super) fn tick_auto_vacuum(&self) {
        if let Err(e) = self
            .vac
            .run_pending(|after, rows| self.vacuum_bounded(after, rows))
        {
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
    /// 得靠手动/后台维护裁版本。点读的链尾早停仍在，但历史索引、日志与维护成本会积累。
    /// 记账/取账/游标的策略在 [`crate::common::VacuumScheduler`]（四引擎共用）。
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
    /// 锁序说明都在 [`MVCC::vacuum_bounded`] 那边。
    pub fn vacuum(&self) -> std::io::Result<VacuumStats> {
        Ok(self.vacuum_bounded(None, u64::MAX)?.stats)
    }
    /// 从上次物理游标继续，按扫描行预算裁出一个回收切片。
    /// 跨片版本组保持连续，删除前完成该组判定；报告实际扫描和删除数供调度记账。

    /// 有界版本回收：从逻辑 key `after`（**不含**）之后开始扫，最多读 `budget_rows` 行物理行。
    ///
    /// 保留规则和 [`MVCC::vacuum`] 完全一致，只是作用范围是一段而不是全库。
    ///
    /// **截断只发生在逻辑 key 边界**：半条链不能收 —— 「留哪一版」的判据要求整组版本都在手上。
    /// 所以一轮实际读的行数可能超出 `budget_rows`，超出量就是那一条链的长度：某把热点 key
    /// 堆了 23.8 万版时，裁它这一下就得一次读 23.8 万行元数据（走 KeyDir，不读 value）。
    /// 这是本设计明确接受的代价。
    ///
    /// blob 回收与日志压缩只在**全库轮次**（`budget_rows == u64::MAX`，也就是手动
    /// [`MVCC::vacuum`]）做：两者都是全库代价。判据不能是「游标回到 None」—— 一把 key 的
    /// 小库任何一片都正好读到库尾，那样自动回收每轮都会重写一遍库。
    ///
    /// 为什么要切片：自动回收要能在单核机器上跑，而 [`MVCC::vacuum`] 会把 kv 锁按到整库
    /// 扫完 —— 那期间点读点写全在排队。切片后每轮只按住一小段。
    ///
    /// 为什么是「整轮跳过」而不是旧的 `xmin = min(活跃事务 id)`：那个水位线**会裁掉
    /// 活跃事务快照仍需要的版本**。T 的可见判据是「v ≤ T.version 且 v ∉ T.active_xid」，
    /// 若「最后一个 < xmin」那一版本身就属于 T 的并发快照，T 得往前再退一版 ——
    /// 而那一版刚被裁掉 ⇒ T 静默读到更旧的值或 None。裁错版本是**静默数据错误**，
    /// 比多等一轮 vacuum 危险得多（守卫见 `test_vacuum_keeps_versions_needed_by_active_txn`）。
    ///
    /// 活跃表这把锁**全程持有**：确认它为空之后不会有新事务抢先拿到快照。
    /// 锁序 `active_txn → kv → blob` 与 `Transaction::write` 一致（那边是
    /// active_txn → wal → blob → kv，都在真正动手前拿齐，不会与这里交叉等待）。
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
        #[cfg(test)]
        // 分段标尺在 [`crate::mem_probe`]（M41 起 lsm 共用同一把）。⚠️ 它带门控：不设
        // `VACUUM_STAGE_SPLIT` 时这里绝不能压高水位，否则外层大库台架对 `vacuum()` 的读数会被锯短。
        let mut vac_base = crate::mem_probe::stage_base();

        // ① 分组只走 KeyDir 元数据：判「留哪一版」只需要版本号 + 是不是软墓碑，不需要 value。
        //    旧实现是两次 `kv.iter()`（每一行连 value 全部读出来），于是 vacuum 自己成了
        //    全库最重的一条路径 —— 在单核机器上「开自动回收」反而会在回收那一刻制造新事故。
        let budget_rows = budget_rows.max(1);
        //
        // M36：**只攒当前这一把键的那一组**。以前这里攒的是全库归组的
        // `BTreeMap<Vec<u8>, Vec<(u64, Vec<u8>, bool, ())>>`（每行两个 `Vec`：raw_key 与 enc）。
        // ⚠️ 当时报的"600k 档 +89.4 MB"是**坏量具**读数（M38 修好尺子之前，`峰值` 只看得见超过
        // 历史最高点的部分）；修好后同一形状同一档：整段 `vacuum()` 瞬时 **171.4 MB**，其中
        // **① 归组判定 106.0 MB（93 B/扫描行）+ ④ `merge()` 115.0 MB**，②③ 都是 0
        // ⇒ ①那笔现在的主人是 `to_delete` + `keep` 这两个跨全库累加器，不是"当前一组"（M37）。
        // 版本键按 `raw ‖ version_be` 升序到达 ⇒ 同一把键的行可能被二进制前缀打断，按公共 KeyWindow 等待完整组，判完一组就地交出去。
        // 守卫：`all_engines_vacuum_judges_per_group_not_whole_library`（数"判定发生了几次"与
        // "最大一组的行数"，四家一条各核一次）。
        // M37：**按块走查**。`iter_meta_from` 按 `&self` 借住 KeyDir，而删除与读值都要 `&mut kv`
        // ⇒ 一块一块来：块内只克隆 `(版本号, enc, 是不是软墓碑)`（至多 `VAC_CHUNK_ROWS` 行），
        // 出借用之后立刻把这一块判出来的行删掉、把保留行的 blob 引用当场读出来然后丢掉。
        // 于是 M36 之后剩下的那两笔 O(全库) 累加器（分段实测 ①=106.0 MB、93 B/扫描行）
        // 塌成 O(一块)。只有真有大 value 时 `blob_keys` 才随库涨 —— 那是 `rewrite_in_place`
        // 必须一次拿到全部存活 value 的结构，不是这一层的账（守卫：同一条四家守卫 + 分段台架）。
        //
        // 块间下界用「上一块最后一把 `enc ‖ 0x00`」：那是 enc 域里的**严格后继方向**
        // （`enc‖0x00 > enc`，且任何 `K > enc` 都满足 `K ≥ enc‖0x00`）⇒ 一行不漏、一行不重。
        // ⚠️ 反过来 `enc ‖ 0xFF×8`（[`crate::common::version_key_upper_bound`] 那种**右界**用法）
        // 会把"以 `enc` 为前缀的更长键"整把跳掉 —— 同一个坑在 `common::store::fold_lower_bound`
        // 的注释里已经写过一次，这里别再踩。
        // 只有全库轮次要把保留行读出来查 blob 引用（有界片不碰 blob）⇒ 才需要那条当场读的路
        let full_sweep = budget_rows == u64::MAX;
        let inspect_refs = full_sweep
            && std::matches!(
                blob.reference_scan()?,
                crate::bitcask::storage::ReferenceScan::Inspect
            );
        #[cfg(test)]
        if full_sweep {
            crate::common::maintenance_probe::note_bitcask_scan(inspect_refs);
        }
        let mut groups: crate::common::order::KeyWindow<Vec<(u64, u64, bool, ())>> =
            crate::common::order::KeyWindow::new();
        let mut spare = Vec::new();
        let mut to_delete: Vec<Vec<u8>> = Vec::new();
        let mut kept: Vec<Vec<u8>> = Vec::new();
        let mut blob_keys: Vec<(Vec<u8>, Vec<u8>)> = Vec::new(); // (enc, 完整用户值) —— 只有大 value 才进
        let mut removed = 0usize;
        let mut scanned = 0u64;
        let mut cur_raw: Option<Vec<u8>> = None;
        let mut truncated = false;
        // 首块的下界是调用方给的 `after`（那是**逻辑键**域，靠下面那句显式跳过补齐），之后逐块前推
        let mut lower: Option<Vec<u8>> = after.map(|k| k.to_vec());
        loop {
            let mut chunk_rows = 0usize;
            let mut next_lower: Option<Vec<u8>> = None;
            let mut done = false;
            #[cfg(test)]
            let scan_probe = crate::common::maintenance_probe::Timer::start(
                crate::common::maintenance_probe::Stage::VacuumScan,
            );
            for (enc, loc) in kv.iter_meta_from(lower.as_deref()) {
                let Some((raw_key, version)) = decode_key_ref(enc) else {
                    continue;
                };
                // 下界落在 enc 域，而 `raw_key ≤ after` 的那些行（包括 `after` 自己那一整组）
                // 都还在范围内 —— 显式跳过，游标才是逻辑 key 域的。
                if after.is_some_and(|a| raw_key <= a) {
                    continue;
                }
                while let Some((key, mut group)) = groups.pop_before(&enc, xmin) {
                    vacuum_drain_group(
                        &mut group,
                        xmin,
                        |version| to_delete.push(encode_key(&key, version)),
                        |version, _| {
                            if inspect_refs {
                                kept.push(encode_key(&key, version));
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
                    truncated = true;
                    done = true;
                    break;
                }
                scanned += 1;
                groups
                    .entry_with(raw_key, || {
                        let mut owned = cur_raw.take().unwrap_or_default();
                        owned.clear();
                        owned.extend_from_slice(raw_key);
                        (owned, std::mem::take(&mut spare))
                    })
                    .push((version, version, loc.is_soft_tombstone(), ()));
                chunk_rows += 1;
                if chunk_rows >= VAC_CHUNK_ROWS {
                    let mut bound = enc.clone();
                    bound.push(0u8);
                    next_lower = Some(bound);
                    break;
                }
            }
            // ⚠️ 走到这里 `for` 里那个按引用迭代 KeyDir 的迭代器已经 drop ⇒ 才拿得到 `&mut kv`
            if done || next_lower.is_none() {
                while let Some((key, mut group)) = groups.pop_first() {
                    vacuum_drain_group(
                        &mut group,
                        xmin,
                        |version| to_delete.push(encode_key(&key, version)),
                        |version, _| {
                            if inspect_refs {
                                kept.push(encode_key(&key, version));
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
            std::mem::drop(scan_probe);
            #[cfg(test)]
            note_vac_pending(to_delete.len() + kept.len());
            {
                #[cfg(test)]
                let _probe = crate::common::maintenance_probe::Timer::start(
                    crate::common::maintenance_probe::Stage::VacuumDelete,
                );
                for enc in to_delete.drain(..) {
                    kv.remove(&enc)?;
                    removed += 1;
                }
            }
            if inspect_refs {
                #[cfg(test)]
                let _probe = crate::common::maintenance_probe::Timer::start(
                    crate::common::maintenance_probe::Stage::VacuumBlobRefs,
                );
                // 保留行的 blob 引用**当场读、读完就丢**：这是这一刀能不要 `keep` 那张清单的原因
                for enc in kept.drain(..) {
                    #[cfg(test)]
                    crate::common::maintenance_probe::note_bitcask_value_read();
                    let stored = kv.get_entry(&enc)?.ok_or_else(|| {
                        std::io::Error::new(std::io::ErrorKind::InvalidData, "retained row missing")
                    })?;
                    let Some(stored) = stored else {
                        continue;
                    };
                    if stored.first() != Some(&VAL_TAG_BLOB) {
                        continue;
                    }
                    #[cfg(test)]
                    crate::common::maintenance_probe::note_blob_read();
                    if let Some(user) = decode_stored_value(&mut blob, &stored)? {
                        blob_keys.push((enc, user));
                    }
                }
            } else {
                kept.clear();
            }
            match next_lower {
                Some(l) if !done => lower = Some(l),
                _ => break,
            }
        }
        let next_cursor = if truncated { cur_raw } else { None };

        #[cfg(test)]
        {
            vac_base = crate::mem_probe::stage_mark(0, vac_base);
        }
        {
            #[cfg(test)]
            let _probe = crate::common::maintenance_probe::Timer::start(
                crate::common::maintenance_probe::Stage::VacuumSync,
            );
            kv.flush()?;
            blob.sync()?;
        }
        #[cfg(test)]
        {
            vac_base = crate::mem_probe::stage_mark(1, vac_base);
        }

        // Blob GC / 日志压缩都是**全库代价**，只在这一次调用本来就是全库扫描时才做。
        // 判据用 `budget_rows` 而不是「游标回到 None」：一个只有一把 key 的小库，任何
        // 有界轮次都会正好读到库尾 —— 用游标判断会让自动回收每轮都重写一遍库。
        // 也就是说：有界轮次只裁行，全库收尾（blob 重写 + merge）归手动 `vacuum()`。
        //
        // Blob GC：
        // - 无任何 blob 引用 → clear
        // - 仍有引用 → rewrite_in_place 只保留存活大 value，并更新 KeyDir 中的指针
        //
        // `blob_keys` 在上面那个按块的循环里就已经填好了（保留行**当场读、读完就丢**），
        // 所以这里不再有"整张存活行清单"，也不再有第二次全库 `iter()`。
        // ⚠️ `rewrite_in_place` 要一次拿到**全部**存活大 value —— 那是 blob 文件的结构，
        // 不是这一层能省掉的账（没有大 value 时这一段一分钱都不花，M37 分段实测 ③ = 0.0 MB）。
        #[cfg(test)]
        let _gc_probe = crate::common::maintenance_probe::Timer::start(
            crate::common::maintenance_probe::Stage::VacuumBlobGc,
        );
        let blob_rewritten = if !full_sweep {
            false
        } else if blob_keys.is_empty() {
            let old_len = blob.path().metadata()?.len();
            if old_len > 8 {
                blob.clear_in_place()?;
                true
            } else {
                false
            }
        } else {
            let values: Vec<Vec<u8>> = blob_keys.iter().map(|(_, v)| v.clone()).collect();
            let new_offsets = blob.rewrite_in_place(&values)?;
            // 更新主日志里的 blob 指针（覆盖写同一 storage key）
            for ((enc, user), new_off) in blob_keys.into_iter().zip(new_offsets.into_iter()) {
                let mut stored = Vec::with_capacity(1 + 8 + 4);
                stored.push(VAL_TAG_BLOB);
                stored.extend_from_slice(&new_off.to_le_bytes());
                stored.extend_from_slice(&(user.len() as u32).to_le_bytes());
                kv.insert(enc, Some(stored))?;
            }
            true
        };

        // M81（方案第 2 项）：**merge 不再跟着 vacuum 跑**。
        //
        // 改前的形状：全库轮只要有回收就附一次整库重写 ⇒ 600k 台架同一趟付了两遍
        // （`MERGE_TIME_SPLIT` 一手读数 3.564s + 3.216s；20M 档 `docs/test1.txt` 里
        // vacuum 166.9s 内含一次 merge + flush+merge 113.5s = 280.4s）。
        // 拆出后这一家与 bplus/lsm/memory 同形状：vacuum = 裁版本 + blob 重写，不动段文件布局。
        //
        // ⚠️ 语义变更（不是纯性能手术）：**硬删除腾出的空间不再当场归还**，等调用方显式 `merge()`。
        // "长跑不靠调用方记得"那一头由维护器兜：[`crate::maintenance::Maintenance`] 的全库收尾
        // 现在是 `vacuum()` + `merge()` 两步，仍保证日志字节有界。
        // M43 那条"这一句的 Err 不许吞"跟着搬到那两个调用点（`MVCC::merge()` 上那段注释讲的是
        // 同一个隔离态），这里不再有我替调用方咽下的错。
        #[cfg(test)]
        {
            vac_base = crate::mem_probe::stage_mark(2, vac_base);
            let _ = crate::mem_probe::stage_mark(3, vac_base);
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
        #[cfg(test)]
        // 分段标尺在 [`crate::mem_probe`]（M41 起 lsm 共用同一把）。⚠️ 它带门控：不设
        // `VACUUM_STAGE_SPLIT` 时这里绝不能压高水位，否则外层大库台架对 `vacuum()` 的读数会被锯短。
        let mut vac_base = crate::mem_probe::stage_base();

        // ① 分组只走 KeyDir 元数据：判「留哪一版」只需要版本号 + 是不是软墓碑，不需要 value。
        //    旧实现是两次 `kv.iter()`（每一行连 value 全部读出来），于是 vacuum 自己成了
        //    全库最重的一条路径 —— 在单核机器上「开自动回收」反而会在回收那一刻制造新事故。
        let budget_rows = budget_rows.max(1);
        //
        // M36：**只攒当前这一把键的那一组**。以前这里攒的是全库归组的
        // `BTreeMap<Vec<u8>, Vec<(u64, Vec<u8>, bool, ())>>`（每行两个 `Vec`：raw_key 与 enc）。
        // ⚠️ 当时报的"600k 档 +89.4 MB"是**坏量具**读数（M38 修好尺子之前，`峰值` 只看得见超过
        // 历史最高点的部分）；修好后同一形状同一档：整段 `vacuum()` 瞬时 **171.4 MB**，其中
        // **① 归组判定 106.0 MB（93 B/扫描行）+ ④ `merge()` 115.0 MB**，②③ 都是 0
        // ⇒ ①那笔现在的主人是 `to_delete` + `keep` 这两个跨全库累加器，不是"当前一组"（M37）。
        // 版本键按 `raw ‖ version_be` 升序到达 ⇒ 同一把键的行可能被二进制前缀打断，按公共 KeyWindow 等待完整组，判完一组就地交出去。
        // 守卫：`all_engines_vacuum_judges_per_group_not_whole_library`（数"判定发生了几次"与
        // "最大一组的行数"，四家一条各核一次）。
        // M37：**按块走查**。`iter_meta_from` 按 `&self` 借住 KeyDir，而删除与读值都要 `&mut kv`
        // ⇒ 一块一块来：块内只克隆 `(版本号, enc, 是不是软墓碑)`（至多 `VAC_CHUNK_ROWS` 行），
        // 出借用之后立刻把这一块判出来的行删掉、把保留行的 blob 引用当场读出来然后丢掉。
        // 于是 M36 之后剩下的那两笔 O(全库) 累加器（分段实测 ①=106.0 MB、93 B/扫描行）
        // 塌成 O(一块)。只有真有大 value 时 `blob_keys` 才随库涨 —— 那是 `rewrite_in_place`
        // 必须一次拿到全部存活 value 的结构，不是这一层的账（守卫：同一条四家守卫 + 分段台架）。
        //
        // 块间下界用「上一块最后一把 `enc ‖ 0x00`」：那是 enc 域里的**严格后继方向**
        // （`enc‖0x00 > enc`，且任何 `K > enc` 都满足 `K ≥ enc‖0x00`）⇒ 一行不漏、一行不重。
        // ⚠️ 反过来 `enc ‖ 0xFF×8`（[`crate::common::version_key_upper_bound`] 那种**右界**用法）
        // 会把"以 `enc` 为前缀的更长键"整把跳掉 —— 同一个坑在 `common::store::fold_lower_bound`
        // 的注释里已经写过一次，这里别再踩。
        // 只有全库轮次要把保留行读出来查 blob 引用（有界片不碰 blob）⇒ 才需要那条当场读的路
        let full_sweep = budget_rows == u64::MAX;
        let mut groups: crate::common::order::KeyWindow<Vec<(u64, Vec<u8>, bool, ())>> =
            crate::common::order::KeyWindow::new();
        let mut to_delete: Vec<Vec<u8>> = Vec::new();
        let mut kept: Vec<Vec<u8>> = Vec::new();
        let mut blob_keys: Vec<(Vec<u8>, Vec<u8>)> = Vec::new(); // (enc, 完整用户值) —— 只有大 value 才进
        let mut removed = 0usize;
        let mut scanned = 0u64;
        let mut cur_raw: Option<Vec<u8>> = None;
        let mut truncated = false;
        // 首块的下界是调用方给的 `after`（那是**逻辑键**域，靠下面那句显式跳过补齐），之后逐块前推
        let mut lower: Option<Vec<u8>> = after.map(|k| k.to_vec());
        loop {
            let mut chunk_rows = 0usize;
            let mut next_lower: Option<Vec<u8>> = None;
            let mut done = false;
            for (enc, loc) in kv.iter_meta_from(lower.as_deref()) {
                let Some(vk) = decode_key(enc) else {
                    continue;
                };
                // 下界落在 enc 域，而 `raw_key ≤ after` 的那些行（包括 `after` 自己那一整组）
                // 都还在范围内 —— 显式跳过，游标才是逻辑 key 域的。
                if after.is_some_and(|a| vk.raw_key.as_slice() <= a) {
                    continue;
                }
                while let Some((key, mut group)) = groups.pop_before(&enc, xmin) {
                    vacuum_drain_group(
                        &mut group,
                        xmin,
                        |row| to_delete.push(row),
                        |row, _| {
                            if full_sweep {
                                kept.push(row);
                            }
                        },
                    );
                    cur_raw = Some(key);
                }
                // 整个重叠前缀窗口完成判定后才能停止。
                if groups.is_empty() && scanned >= budget_rows {
                    truncated = true;
                    done = true;
                    break;
                }
                scanned += 1;
                groups.entry(vk.raw_key).push((
                    vk.version,
                    enc.clone(),
                    loc.is_soft_tombstone(),
                    (),
                ));
                chunk_rows += 1;
                if chunk_rows >= VAC_CHUNK_ROWS {
                    let mut bound = enc.clone();
                    bound.push(0u8);
                    next_lower = Some(bound);
                    break;
                }
            }
            // ⚠️ 走到这里 `for` 里那个按引用迭代 KeyDir 的迭代器已经 drop ⇒ 才拿得到 `&mut kv`
            if done || next_lower.is_none() {
                while let Some((key, mut group)) = groups.pop_first() {
                    vacuum_drain_group(
                        &mut group,
                        xmin,
                        |row| to_delete.push(row),
                        |row, _| {
                            if full_sweep {
                                kept.push(row);
                            }
                        },
                    );
                    cur_raw = Some(key);
                }
            }
            #[cfg(test)]
            note_vac_pending(to_delete.len() + kept.len());
            for enc in to_delete.drain(..) {
                let _ = kv.remove(&enc);
                removed += 1;
            }
            if full_sweep {
                // 保留行的 blob 引用**当场读、读完就丢**：这是这一刀能不要 `keep` 那张清单的原因
                for enc in kept.drain(..) {
                    let Ok(Some(stored)) = kv.get_entry(&enc) else {
                        continue;
                    };
                    let Some(stored) = stored else {
                        continue; // 软墓碑没有 value
                    };
                    if stored.first() != Some(&VAL_TAG_BLOB) {
                        continue;
                    }
                    match decode_stored_value(&mut blob, &stored) {
                        Ok(Some(user)) => blob_keys.push((enc, user)),
                        Ok(None) => {}
                        Err(e) => {
                            log::warn!("blob decode 失败 key={enc:?}: {e}");
                        }
                    }
                }
            } else {
                kept.clear();
            }
            match next_lower {
                Some(l) if !done => lower = Some(l),
                _ => break,
            }
        }
        let next_cursor = if truncated { cur_raw } else { None };

        #[cfg(test)]
        {
            vac_base = crate::mem_probe::stage_mark(0, vac_base);
        }
        kv.flush()?;
        blob.sync()?;
        #[cfg(test)]
        {
            vac_base = crate::mem_probe::stage_mark(1, vac_base);
        }

        // Blob GC / 日志压缩都是**全库代价**，只在这一次调用本来就是全库扫描时才做。
        // 判据用 `budget_rows` 而不是「游标回到 None」：一个只有一把 key 的小库，任何
        // 有界轮次都会正好读到库尾 —— 用游标判断会让自动回收每轮都重写一遍库。
        // 也就是说：有界轮次只裁行，全库收尾（blob 重写 + merge）归手动 `vacuum()`。
        //
        // Blob GC：
        // - 无任何 blob 引用 → clear
        // - 仍有引用 → rewrite_in_place 只保留存活大 value，并更新 KeyDir 中的指针
        //
        // `blob_keys` 在上面那个按块的循环里就已经填好了（保留行**当场读、读完就丢**），
        // 所以这里不再有"整张存活行清单"，也不再有第二次全库 `iter()`。
        // ⚠️ `rewrite_in_place` 要一次拿到**全部**存活大 value —— 那是 blob 文件的结构，
        // 不是这一层能省掉的账（没有大 value 时这一段一分钱都不花，M37 分段实测 ③ = 0.0 MB）。
        let blob_rewritten = if !full_sweep {
            false
        } else if blob_keys.is_empty() {
            let old_len = blob.path().metadata().map(|m| m.len()).unwrap_or(0);
            if old_len > 8 {
                blob.clear_in_place()?;
                true
            } else {
                false
            }
        } else {
            let values: Vec<Vec<u8>> = blob_keys.iter().map(|(_, v)| v.clone()).collect();
            let new_offsets = blob.rewrite_in_place(&values)?;
            // 更新主日志里的 blob 指针（覆盖写同一 storage key）
            for ((enc, user), new_off) in blob_keys.into_iter().zip(new_offsets.into_iter()) {
                let mut stored = Vec::with_capacity(1 + 8 + 4);
                stored.push(VAL_TAG_BLOB);
                stored.extend_from_slice(&new_off.to_le_bytes());
                stored.extend_from_slice(&(user.len() as u32).to_le_bytes());
                let _ = kv.insert(enc, Some(stored));
            }
            true
        };

        // M81（方案第 2 项）：**merge 不再跟着 vacuum 跑**。
        //
        // 改前的形状：全库轮只要有回收就附一次整库重写 ⇒ 600k 台架同一趟付了两遍
        // （`MERGE_TIME_SPLIT` 一手读数 3.564s + 3.216s；20M 档 `docs/test1.txt` 里
        // vacuum 166.9s 内含一次 merge + flush+merge 113.5s = 280.4s）。
        // 拆出后这一家与 bplus/lsm/memory 同形状：vacuum = 裁版本 + blob 重写，不动段文件布局。
        //
        // ⚠️ 语义变更（不是纯性能手术）：**硬删除腾出的空间不再当场归还**，等调用方显式 `merge()`。
        // "长跑不靠调用方记得"那一头由维护器兜：[`crate::maintenance::Maintenance`] 的全库收尾
        // 现在是 `vacuum()` + `merge()` 两步，仍保证日志字节有界。
        // M43 那条"这一句的 Err 不许吞"跟着搬到那两个调用点（`MVCC::merge()` 上那段注释讲的是
        // 同一个隔离态），这里不再有我替调用方咽下的错。
        #[cfg(test)]
        {
            vac_base = crate::mem_probe::stage_mark(2, vac_base);
            let _ = crate::mem_probe::stage_mark(3, vac_base);
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
