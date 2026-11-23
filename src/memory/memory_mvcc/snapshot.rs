//! 本文件负责快照回放、记录组装及停机发布。
//! 模块边界仅组织代码：锁顺序、版本可见性、磁盘格式和发布失败的处理均沿用原契约。

use super::*;

impl<'a> AutoVacuumOff<'a> {
    pub(super) fn pause(vac: &'a VacuumScheduler) -> Self {
        let saved = vac.slice();
        vac.set_slice(0);
        Self { vac, saved }
    }
}

impl Drop for AutoVacuumOff<'_> {
    fn drop(&mut self) {
        self.vac.set_slice(self.saved);
    }
}

impl MVCC {
    /// 打开一个带启停快照的 memory 库（M81 第 6 项）：`dir` 里放 [`SNAPSHOT_FILE`]。
    /// 文件在就读入重放，不在就从空库开始 —— 两种都算打开成功。
    ///
    /// ⚠️ `snapshot` 字段是**重放成功之后**才赋值的，顺序不能反过来：读中途失败就直接返回 `Err`，
    /// 此时这个半成品对象的 `Drop` 里 `snapshot` 还是 `None` ⇒ 不会把一份好快照覆盖成半截库。
    /// （反过来写就是"打开失败一次，库没了"——比不落盘坏得多。）
    pub fn open_with_snapshot(dir: &std::path::Path) -> std::io::Result<Self> {
        let path = dir.join(SNAPSHOT_FILE);
        let mut mvcc = Self::new();
        mvcc.load_snapshot_from(&path)?;
        mvcc.snapshot = Some(path);
        Ok(mvcc)
    }

    /// 快照路径（门面的 `path()` 用它；`MVCC::new()` 开的库返回 `None`，行为与改前一致）
    pub fn snapshot_path(&self) -> Option<&std::path::Path> {
        self.snapshot.as_deref()
    }

    /// 读入快照并重放。三条硬规矩都是这个仓库一路在收的东西：
    /// ① **已过期的行不许复活**（拿文件里的绝对 expire 与 now 比，不信任"它当时是活的"）；
    /// ② 按页一事务（照 `migrate` 的写侧），不把两千万行攒进一个写集；
    /// ③ TTL 保留文件内绝对到期时刻；不把重放耗时加回剩余寿命。
    pub(super) fn load_snapshot_from(&mut self, path: &std::path::Path) -> std::io::Result<()> {
        #[cfg(test)]
        let _timer = crate::common::kv_snapshot::recovery_probe::Timer::start(
            crate::common::kv_snapshot::recovery_probe::Kind::Total,
        );
        if !path.exists() {
            return Ok(());
        }
        // 重放期间关掉自动回收，收尾把账也丢掉（M82）：重放出来的行每把键只有一版 ⇒ **一行垃圾
        // 都没有**，而重放走的是"每 1000 行一个事务"的写流 ⇒ 不关的话每个事务的 `begin()` 都要
        // 扫一片（1:1 跟着"写"的量走）：600k 档 `冷重开` 1.3 s → 4.4 s、20M 档白扫一整库。
        // ⚠️ 还原走 RAII：下面三条提前返回（读失败、重放被拒收）都不许把自动回收永久关掉。
        let _vac_paused = AutoVacuumOff::pause(&self.vac);
        const PAGE: usize = 1_000;
        let now = crate::common::codec::now_unix_secs();
        let mut batch: Vec<(Vec<u8>, Option<Vec<u8>>, Option<u64>)> = Vec::with_capacity(PAGE);
        let mut expired = 0usize;
        let mut err: Option<std::io::Error> = None;
        let values = crate::common::kv_snapshot::ValueBuffer::PlainEnvelope;
        #[cfg(test)]
        let values = if replay_cost_reference::enabled() {
            crate::common::kv_snapshot::ValueBuffer::Exact
        } else {
            values
        };
        let stats = crate::common::kv_snapshot::read_snapshot_with(&path, values, &mut |rec| {
            // `value = None` = 墓碑行，照样进重放队列（重放成 `delete`）——
            // 丢掉它就是"重启即失忆"：判据①的地基没了，后续同步能复活已删键。
            let value = rec.value;
            if let Some(e) = rec.expire_unix_secs {
                if e <= now {
                    expired += 1;
                    return true;
                }
            }
            batch.push((rec.key, value, rec.expire_unix_secs));
            if batch.len() >= PAGE {
                if let Err(e) = MVCC::replay_page(self, &mut batch) {
                    err = Some(e);
                    return false;
                }
            }
            true
        })?;
        if let Some(e) = err {
            return Err(e);
        }
        MVCC::replay_page(self, &mut batch)?;
        log::info!(
            "memory: 快照读入 {} 行（跳过已过期 {} 行）{}",
            stats.records - expired,
            expired,
            path.display()
        );
        // 收尾把重放攒下的触发账丢掉：那些行不是垃圾（见上面那段注释）。
        // ⚠️ 位置在还原切片宽度**之前**也无所谓（`take_tick` 只看账与游标），但要紧的是：
        // 这个口子只在这儿用 —— 别的地方丢账就是把"该回收"写成"不用回收"。
        let dropped = self.vac.discard_pending();
        if dropped > 0 {
            log::debug!("memory: 快照重放丢弃 {dropped} 行触发账（新写入的行没有旧版本可回收）");
        }
        Ok(())
    }
    /// 按页持有注册表和存储锁，回放这一页完整的版本化记录。
    /// 重复键与版本顺序仍需检查；页内失败必须撤销整个页，不能留下半页恢复结果。

    /// 一页一事务；任何写入/提交失败都返回错误，未提交的页由事务 Drop 回滚。
    /// 读取回调把错误带出，尾页直接使用同一个 Result 入口。
    ///
    /// 为什么单独抽出来：回调里要同时用 `&MVCC`（开事务）与 `&mut batch`，写成内联块会让
    /// 借用检查在两件事之间反复；而"攒满一页就冲"与"收尾把剩的冲掉"必须是**同一份**代码，
    /// 否则尾巴那半页会静默不写（本仓库收过这类账）。
    pub(super) fn replay_page(
        mvcc: &MVCC,
        batch: &mut Vec<(Vec<u8>, Option<Vec<u8>>, Option<u64>)>,
    ) -> std::io::Result<()> {
        #[cfg(test)]
        let _timer = crate::common::kv_snapshot::recovery_probe::Timer::start(
            crate::common::kv_snapshot::recovery_probe::Kind::Replay,
        );
        #[cfg(test)]
        if replay_reference::enabled() {
            return Self::replay_page_reference(mvcc, batch);
        }
        if batch.is_empty() {
            return Ok(());
        }
        #[cfg(test)]
        crate::common::kv_snapshot::recovery_probe::note(|s| {
            s.replay_pages += 1;
            s.replay_input_rows += batch.len() as u64;
        });
        let tx = {
            #[cfg(test)]
            let _setup = crate::common::kv_snapshot::recovery_probe::Timer::start(
                crate::common::kv_snapshot::recovery_probe::Kind::ReplaySetup,
            );
            mvcc.begin_transaction()
        };
        {
            // 锁注册表和存储前先给拥有型页面加信封；回放仍是一笔普通事务，被拒绝的整页要回滚。
            #[cfg(test)]
            let _encode = crate::common::kv_snapshot::recovery_probe::Timer::start(
                crate::common::kv_snapshot::recovery_probe::Kind::ReplayEncode,
            );
            for (_, value, expire) in batch.iter_mut() {
                *value = value.take().map(|v| {
                    #[cfg(test)]
                    if replay_cost_reference::enabled() {
                        return match expire {
                            Some(e) => crate::common::pack_ttl(&v, *e),
                            None => crate::common::pack_plain(&v),
                        };
                    }
                    crate::common::codec::pack_owned(v, *expire).into_bytes()
                });
            }
        }
        {
            // 未对外开放的实例也保留普通冲突检查、注册表先于存储的锁序及有界逐页事务。
            #[cfg(test)]
            let _apply = crate::common::kv_snapshot::recovery_probe::Timer::start(
                crate::common::kv_snapshot::recovery_probe::Kind::ReplayApply,
            );
            let mut reg = tx.core.lock_registry();
            let mut side = mvcc.store.lock().unwrap();
            #[cfg(test)]
            let reference = page_apply_reference::enabled();
            #[cfg(not(test))]
            let reference = false;
            if reference {
                for (key, logical, _) in batch.drain(..) {
                    tx.core
                        .try_write_owned(&mut reg, &mut *side, key, logical)
                        .map_err(|error| {
                            std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                format!("快照重放被引擎拒收: {error:?}"),
                            )
                        })?;
                }
            } else {
                tx.core
                    .try_write_owned_page(
                        &mut reg,
                        &mut *side,
                        batch.drain(..).map(|(key, logical, _)| (key, logical)),
                    )
                    .map_err(|error| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("快照重放被引擎拒收: {error:?}"),
                        )
                    })?;
            }
        }
        #[cfg(test)]
        let _commit = crate::common::kv_snapshot::recovery_probe::Timer::start(
            crate::common::kv_snapshot::recovery_probe::Kind::ReplayCommit,
        );
        tx.try_commit().map_err(|error| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("快照重放提交失败: {error:?}"),
            )
        })
    }

    #[cfg(test)]
    pub(super) fn replay_page_reference(
        mvcc: &MVCC,
        batch: &mut Vec<(Vec<u8>, Option<Vec<u8>>, Option<u64>)>,
    ) -> std::io::Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let _legacy = crate::common::kv_snapshot::recovery_probe::Timer::start(
            crate::common::kv_snapshot::recovery_probe::Kind::ReplayLegacy,
        );
        crate::common::kv_snapshot::recovery_probe::note(|s| {
            s.replay_pages += 1;
            s.replay_input_rows += batch.len() as u64;
        });
        let tx = mvcc.begin_transaction();
        for (key, value, expire) in batch.drain(..) {
            match (value, expire) {
                (None, _) => tx.try_delete(&key),
                (Some(value), Some(expiry)) => {
                    tx.try_set(&key, crate::common::pack_ttl(&value, expiry))
                }
                (Some(value), None) => tx.try_set_raw(&key, value),
            }
            .map_err(|error| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("快照重放被引擎拒收: {error:?}"),
                )
            })?;
        }
        tx.try_commit().map_err(|error| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("快照重放提交失败: {error:?}"),
            )
        })
    }

    /// 写一次带完整性校验的整库快照（`BPEXP004`），同步后原子替换旧文件。
    ///
    /// 三条口径：
    /// - **流式**：走 `export_latest_visible_for_each` 交一行折一行，不把整库摆进 `Vec`
    ///   （M44 那条纪律对快照同样成立；20M 档那一格本来就要 GB 级）。
    /// - **带墓碑**（`include_deleted = true`）：`tools::sync` / `import --skip-existing` 的判据①
    ///   是"目标见过这把键吗（含墓碑）"，快照里没墓碑 ⇒ 重启一次判据的地基就没了。
    /// - TTL 从出口那一份 `ExportRecord::expire_unix_secs` 直接拿（0.5.0 起有这一格），
    ///   文件里存**绝对**时刻 ⇒ 躺多久都不影响这一行还剩多少命。
    ///
    /// 写盘失败**不吞**：一路 `?` 交给 `Drop`，那里降成 `warn`（析构里不能抛）。
    #[cfg(test)]
    pub(super) fn write_snapshot_reference(
        &self,
        path: &std::path::Path,
    ) -> std::io::Result<usize> {
        use crate::common::kv_snapshot::{KvRecord, atomic_write_bin_v4};
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut written = 0usize;
        let mut err: Option<std::io::Error> = None;
        let stats = atomic_write_bin_v4(path, |sink| {
            // `include_deleted = true` ⇒ 墓碑也进快照（`value = None` 那一支）。
            // 为什么必须带上：memory 当 `tools::sync` 的目标时，判据是"目标见过这把键吗"，
            // 而"见过"含墓碑 —— 快照里没墓碑，重启一次之后"这把键被删过"就消失了，
            // 后续同步能把已删键复活（`memory_snapshot_keeps_tombstones_alive` 钉这条）。
            let handed = self.export_latest_visible_for_each(true, &mut |rec: ExportRecord| {
                let mut kr = KvRecord::new(rec.key, rec.value);
                if let Some(e) = rec.expire_unix_secs {
                    kr = kr.with_expire(e);
                }
                match sink.write(&kr) {
                    Ok(()) => {
                        written += 1;
                        true
                    }
                    Err(e) => {
                        err = Some(e);
                        false
                    }
                }
            });
            if let Some(e) = err {
                return Err(e);
            }
            if handed != written {
                // 不拿 assert：这条在 `Drop` 之路上，panic 会在已经 panic 的展开里再炸一次。
                // 两份账不一致 = 有行静默没落盘，必须报出去而不是"看起来成功了"。
                return Err(std::io::Error::other(format!(
                    "memory 快照写出 {written} 行、出口交出 {handed} 行"
                )));
            }
            Ok(())
        })?;
        debug_assert_eq!(stats.records, written);
        Ok(written)
    }
    /// 只有最终独占持有者可以消费内存树；共享句柄仍按借用路径写快照。
    /// 两条分支使用相同版本、TTL 与墓碑判据，最终文件只有完整写入后才替换旧快照。

    /// 共享句柄仍可读；只有最终独占所有者才消费内存树。
    pub(super) fn write_stop_snapshot(&mut self, path: &std::path::Path) -> std::io::Result<usize> {
        #[cfg(test)]
        if stop_reference::enabled() || scan_reference::enabled() {
            let result = self.write_snapshot_reference(path);
            crate::common::kv_snapshot::snapshot_probe::note(|s| s.stop_mode = "reference");
            return result;
        }
        self.tick_auto_vacuum();
        let ownership = match Arc::get_mut(&mut self.store) {
            Some(store) => SnapshotOwnership::Exclusive(store.get_mut().unwrap().take_entries()),
            None => SnapshotOwnership::Shared,
        };
        match ownership {
            SnapshotOwnership::Exclusive(entries) => {
                let ceiling = self.next_version.load(Ordering::SeqCst);
                let rows = entries.map(|(key, value)| (Cow::Owned(key), value.map(Cow::Owned)));
                Self::write_snapshot_rows(path, rows, ceiling, |v| v < ceiling, "exclusive")
            }
            SnapshotOwnership::Shared => {
                let tx = Transaction::begin(
                    self.store.clone(),
                    self.active_txn.clone(),
                    self.next_version.clone(),
                    self.vac.clone(),
                );
                let store = self.store.lock().unwrap();
                let rows = store
                    .borrowed_entries()
                    .map(|(key, value)| (Cow::Borrowed(key), value.map(Cow::Borrowed)));
                let result = Self::write_snapshot_rows(
                    path,
                    rows,
                    tx.version(),
                    |v| tx.core.is_visible(v),
                    "shared",
                );
                drop(store);
                tx.commit();
                result
            }
        }
    }

    #[cfg(test)]
    pub(super) fn write_snapshot_rows_batch<'a>(
        path: &std::path::Path,
        rows: impl Iterator<Item = (Cow<'a, [u8]>, Option<Cow<'a, [u8]>>)>,
        ceiling: u64,
        visible: impl Fn(u64) -> bool,
        mode: &'static str,
    ) -> std::io::Result<usize> {
        use crate::common::{
            kv_snapshot::{BinWriter, RecordEncoding, atomic_write_bin_v4},
            order::KeyWindow,
            version::decode_key_ref,
        };
        #[cfg(test)]
        if stop_rows_reference::enabled() {
            return Self::write_snapshot_rows_reference(path, rows, ceiling, visible, mode);
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut visited = 0_u64;
        #[cfg(test)]
        let mut detail = crate::common::kv_snapshot::snapshot_probe::Stats::default();
        #[cfg(test)]
        let measured = crate::common::kv_snapshot::snapshot_probe::is_active();
        let stats = atomic_write_bin_v4(path, |sink| {
            let mut groups: KeyWindow<(u64, Option<Cow<'a, [u8]>>)> = KeyWindow::new();
            let mut spare = Vec::new();
            let mut rows = rows;
            const INPUT_BATCH: usize = 8192;
            let mut batch = Vec::with_capacity(rows.size_hint().0.min(INPUT_BATCH));
            let emit =
                |sink: &mut BinWriter, key: &[u8], value: Option<&[u8]>| -> std::io::Result<()> {
                    let (user, expire) = match value {
                        Some(bytes) => crate::common::codec::snapshot_value(bytes),
                        None => (None, None),
                    };
                    match sink.write_snapshot_batched(key, user, expire)? {
                        RecordEncoding::Buffered | RecordEncoding::DirectLarge => Ok(()),
                        #[cfg(test)]
                        RecordEncoding::DirectReference => Ok(()),
                    }
                };
            loop {
                #[cfg(test)]
                let pull = measured.then(std::time::Instant::now);
                batch.extend(rows.by_ref().take(INPUT_BATCH));
                #[cfg(test)]
                if let Some(pull) = pull {
                    detail.pull_ns += pull.elapsed().as_nanos();
                }
                #[cfg(test)]
                {
                    detail.input_buffer_bytes = detail.input_buffer_bytes.max(
                        batch.capacity()
                            * std::mem::size_of::<(Cow<'a, [u8]>, Option<Cow<'a, [u8]>>)>(),
                    );
                }
                if batch.is_empty() {
                    break;
                }
                #[cfg(test)]
                {
                    detail.input_batches += 1;
                }
                #[cfg(test)]
                let fold = measured.then(std::time::Instant::now);
                let batch_rows = batch.len();
                for index in 0..batch_rows {
                    let (enc, value) = &batch[index];
                    visited += 1;
                    while let Some((key, (_, value))) = groups.pop_before(&enc, ceiling) {
                        emit(sink, &key, value.as_deref())?;
                        spare = key;
                    }
                    let Some((raw, version)) = decode_key_ref(enc) else {
                        continue;
                    };
                    if !visible(version) {
                        continue;
                    }
                    #[cfg(test)]
                    let reference = stop_fold_reference::enabled();
                    #[cfg(not(test))]
                    let reference = false;
                    let route = if !reference
                        && groups.is_empty()
                        && batch.get(index + 1).is_some_and(|(next, _)| {
                            crate::common::order::window_before(raw, next, ceiling)
                        }) {
                        SnapshotFoldRoute::Complete
                    } else {
                        SnapshotFoldRoute::Window
                    };
                    match route {
                        SnapshotFoldRoute::Complete => {
                            emit(sink, raw, value.as_deref())?;
                            #[cfg(test)]
                            {
                                detail.fold_direct_rows += 1;
                            }
                            continue;
                        }
                        SnapshotFoldRoute::Window => {
                            #[cfg(test)]
                            {
                                detail.fold_window_rows += 1;
                            }
                        }
                    }
                    let len = raw.len();
                    let (enc, value) =
                        std::mem::replace(&mut batch[index], (Cow::Borrowed(&[]), None));
                    let winner = match enc {
                        Cow::Owned(mut key) => {
                            key.truncate(len);
                            groups.entry(key)
                        }
                        Cow::Borrowed(enc) => groups.entry_with(&enc[..len], || {
                            let mut key = std::mem::take(&mut spare);
                            key.clear();
                            key.extend_from_slice(&enc[..len]);
                            (key, (0, None))
                        }),
                    };
                    if version >= winner.0 {
                        *winner = (version, value);
                    }
                }
                #[cfg(test)]
                if let Some(fold) = fold {
                    detail.fold_emit_ns += fold.elapsed().as_nanos();
                }
                #[cfg(test)]
                let release = measured.then(std::time::Instant::now);
                batch.clear();
                #[cfg(test)]
                {
                    if let Some(release) = release {
                        detail.input_release_ns += release.elapsed().as_nanos();
                    }
                    detail.release_rows += (batch_rows - batch.len()) as u64;
                }
            }
            #[cfg(test)]
            let tail = measured.then(std::time::Instant::now);
            while let Some((key, (_, value))) = groups.pop_first() {
                emit(sink, &key, value.as_deref())?;
            }
            sink.flush_record_batch()?;
            #[cfg(test)]
            if let Some(tail) = tail {
                detail.fold_emit_ns += tail.elapsed().as_nanos();
            }
            Ok(())
        })?;
        #[cfg(test)]
        crate::common::kv_snapshot::snapshot_probe::note(|s| {
            s.stop_mode = mode;
            s.visited_rows = visited;
            s.consumed_rows = if mode == "exclusive" { visited } else { 0 };
            s.pull_ns = detail.pull_ns;
            s.fold_emit_ns = detail.fold_emit_ns;
            s.input_batches = detail.input_batches;
            s.input_buffer_bytes = detail.input_buffer_bytes;
            s.fold_direct_rows = detail.fold_direct_rows;
            s.fold_window_rows = detail.fold_window_rows;
            s.input_release_ns = detail.input_release_ns;
            s.release_rows = detail.release_rows;
        });
        #[cfg(not(test))]
        let _ = (mode, visited);
        Ok(stats.records)
    }

    pub(super) fn write_snapshot_rows_eager<'a>(
        path: &std::path::Path,
        rows: impl Iterator<Item = (Cow<'a, [u8]>, Option<Cow<'a, [u8]>>)>,
        ceiling: u64,
        visible: impl Fn(u64) -> bool,
        mode: &'static str,
    ) -> std::io::Result<usize> {
        use crate::common::{
            kv_snapshot::{BinWriter, RecordEncoding, atomic_write_bin_v4},
            order::KeyWindow,
            version::decode_key_ref,
        };
        #[cfg(test)]
        if stop_rows_reference::enabled() {
            return Self::write_snapshot_rows_reference(path, rows, ceiling, visible, mode);
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut visited = 0_u64;
        #[cfg(test)]
        let mut detail = crate::common::kv_snapshot::snapshot_probe::Stats::default();
        #[cfg(test)]
        let measured = crate::common::kv_snapshot::snapshot_probe::is_active();
        let stats = atomic_write_bin_v4(path, |sink| {
            let mut groups: KeyWindow<(u64, Option<Cow<'a, [u8]>>)> = KeyWindow::new();
            let mut spare = Vec::new();
            let mut rows = rows;
            const INPUT_BATCH: usize = 8192;
            let mut batch = Vec::with_capacity(rows.size_hint().0.min(INPUT_BATCH));
            let emit = |sink: &mut BinWriter,
                        key: &[u8],
                        value: Option<Cow<'a, [u8]>>|
             -> std::io::Result<()> {
                let (user, expire) = match value.as_deref() {
                    Some(bytes) => crate::common::codec::snapshot_value(bytes),
                    None => (None, None),
                };
                match sink.write_snapshot_batched(key, user, expire)? {
                    RecordEncoding::Buffered | RecordEncoding::DirectLarge => Ok(()),
                    #[cfg(test)]
                    RecordEncoding::DirectReference => Ok(()),
                }
            };
            loop {
                #[cfg(test)]
                let pull = measured.then(std::time::Instant::now);
                batch.extend(rows.by_ref().take(INPUT_BATCH));
                #[cfg(test)]
                if let Some(pull) = pull {
                    detail.pull_ns += pull.elapsed().as_nanos();
                }
                #[cfg(test)]
                {
                    detail.input_buffer_bytes = detail.input_buffer_bytes.max(
                        batch.capacity()
                            * std::mem::size_of::<(Cow<'a, [u8]>, Option<Cow<'a, [u8]>>)>(),
                    );
                }
                if batch.is_empty() {
                    break;
                }
                #[cfg(test)]
                {
                    detail.input_batches += 1;
                }
                #[cfg(test)]
                let fold = measured.then(std::time::Instant::now);
                let mut input = batch.drain(..).peekable();
                while let Some((enc, value)) = input.next() {
                    visited += 1;
                    while let Some((key, (_, value))) = groups.pop_before(&enc, ceiling) {
                        emit(sink, &key, value)?;
                        spare = key;
                    }
                    let Some((raw, version)) = decode_key_ref(&enc) else {
                        continue;
                    };
                    if !visible(version) {
                        continue;
                    }
                    #[cfg(test)]
                    let reference = stop_fold_reference::enabled();
                    #[cfg(not(test))]
                    let reference = false;
                    let route = if !reference
                        && groups.is_empty()
                        && input.peek().is_some_and(|(next, _)| {
                            crate::common::order::window_before(raw, next, ceiling)
                        }) {
                        SnapshotFoldRoute::Complete
                    } else {
                        SnapshotFoldRoute::Window
                    };
                    match route {
                        SnapshotFoldRoute::Complete => {
                            emit(sink, raw, value)?;
                            #[cfg(test)]
                            {
                                detail.fold_direct_rows += 1;
                            }
                            continue;
                        }
                        SnapshotFoldRoute::Window => {
                            #[cfg(test)]
                            {
                                detail.fold_window_rows += 1;
                            }
                        }
                    }
                    let len = raw.len();
                    let winner = match enc {
                        Cow::Owned(mut key) => {
                            key.truncate(len);
                            groups.entry(key)
                        }
                        Cow::Borrowed(enc) => groups.entry_with(&enc[..len], || {
                            let mut key = std::mem::take(&mut spare);
                            key.clear();
                            key.extend_from_slice(&enc[..len]);
                            (key, (0, None))
                        }),
                    };
                    if version >= winner.0 {
                        *winner = (version, value);
                    }
                }
                #[cfg(test)]
                if let Some(fold) = fold {
                    detail.fold_emit_ns += fold.elapsed().as_nanos();
                }
            }
            #[cfg(test)]
            let tail = measured.then(std::time::Instant::now);
            while let Some((key, (_, value))) = groups.pop_first() {
                emit(sink, &key, value)?;
            }
            sink.flush_record_batch()?;
            #[cfg(test)]
            if let Some(tail) = tail {
                detail.fold_emit_ns += tail.elapsed().as_nanos();
            }
            Ok(())
        })?;
        #[cfg(test)]
        crate::common::kv_snapshot::snapshot_probe::note(|s| {
            s.stop_mode = mode;
            s.visited_rows = visited;
            s.consumed_rows = if mode == "exclusive" { visited } else { 0 };
            s.pull_ns = detail.pull_ns;
            s.fold_emit_ns = detail.fold_emit_ns;
            s.input_batches = detail.input_batches;
            s.input_buffer_bytes = detail.input_buffer_bytes;
            s.fold_direct_rows = detail.fold_direct_rows;
            s.fold_window_rows = detail.fold_window_rows;
        });
        #[cfg(not(test))]
        let _ = (mode, visited);
        Ok(stats.records)
    }

    pub(super) fn write_snapshot_rows<'a>(
        path: &std::path::Path,
        rows: impl Iterator<Item = (Cow<'a, [u8]>, Option<Cow<'a, [u8]>>)>,
        ceiling: u64,
        visible: impl Fn(u64) -> bool,
        mode: &'static str,
    ) -> std::io::Result<usize> {
        #[cfg(test)]
        if stop_batch_release::enabled() {
            return Self::write_snapshot_rows_batch(path, rows, ceiling, visible, mode);
        }
        Self::write_snapshot_rows_eager(path, rows, ceiling, visible, mode)
    }

    #[cfg(test)]
    pub(super) fn write_snapshot_rows_reference<'a>(
        path: &std::path::Path,
        rows: impl Iterator<Item = (Cow<'a, [u8]>, Option<Cow<'a, [u8]>>)>,
        ceiling: u64,
        visible: impl Fn(u64) -> bool,
        mode: &'static str,
    ) -> std::io::Result<usize> {
        use crate::common::{
            kv_snapshot::{BinWriter, atomic_write_bin_v4},
            order::KeyWindow,
            version::decode_key_ref,
        };
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut visited = 0_u64;
        let stats = atomic_write_bin_v4(path, |sink| {
            let mut groups: KeyWindow<(u64, Option<Cow<'a, [u8]>>)> = KeyWindow::new();
            let mut spare = Vec::new();
            let emit = |sink: &mut BinWriter, key: &[u8], value: Option<Cow<'a, [u8]>>| {
                let (user, expire) = match value.as_deref() {
                    Some(bytes) => crate::common::codec::snapshot_value(bytes),
                    None => (None, None),
                };
                sink.write_borrowed(key, user, None, expire)
            };
            for (enc, value) in rows {
                visited += 1;
                while let Some((key, (_, value))) = groups.pop_before(&enc, ceiling) {
                    emit(sink, &key, value)?;
                    spare = key;
                }
                let Some((raw, version)) = decode_key_ref(&enc) else {
                    continue;
                };
                if !visible(version) {
                    continue;
                }
                let len = raw.len();
                let winner = match enc {
                    Cow::Owned(mut key) => {
                        key.truncate(len);
                        groups.entry(key)
                    }
                    Cow::Borrowed(enc) => groups.entry_with(&enc[..len], || {
                        let mut key = std::mem::take(&mut spare);
                        key.clear();
                        key.extend_from_slice(&enc[..len]);
                        (key, (0, None))
                    }),
                };
                if version >= winner.0 {
                    *winner = (version, value);
                }
            }
            while let Some((key, (_, value))) = groups.pop_first() {
                emit(sink, &key, value)?;
            }
            Ok(())
        })?;
        crate::common::kv_snapshot::snapshot_probe::note(|s| {
            s.stop_mode = mode;
            s.visited_rows = visited;
            s.consumed_rows = if mode == "exclusive" { visited } else { 0 };
        });
        Ok(stats.records)
    }
}
