// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// 在临时目录里跑一遍所有磁盘引擎 + memory
///
/// 唯一性交给 [`crate::common::ScratchPath`]（pid + 纳秒 + **进程内序号**）：Windows 上
/// `SystemTime` 的粒度约 15.6ms，只靠 tag 会让并发跑的两个同 tag 用例拿到同一个目录、
/// 同一把 `.lock`，后来者 `WouldBlock`（表现为随机的偶发红）。
///
/// 返回的是 **RAII 句柄**（M82）：用例把它声明在引擎**之前**即可 —— 析构按反向声明序走，
/// 引擎先关文件、scratch 后删目录；正常收尾与 panic 展开都会删（此前靠人记得写 `cleanup`，
/// 而"引擎还开着就删目录"在 Windows 上静默失败 ⇒ 一次全量跑往 `%TEMP%` 漏 158 个条目）。
fn tmp_dir(tag: &str) -> crate::common::ScratchPath {
    crate::common::ScratchPath::new("se_engine", tag, "")
}

/// 四个引擎（含 memory）。第三条是 scratch 守卫：**元组的字段按 0→1→2 析构**，
/// 所以 `db` 一定先于 `dir` 落地 —— 别把这个元组拆成 `let (a, b, c)`（那样按反向声明序
/// 走，守卫会先跑，Windows 上文件还开着就删不掉）。
fn open_all() -> Vec<(
    &'static str,
    Box<dyn KvEngine>,
    Option<crate::common::ScratchPath>,
)> {
    let mut out: Vec<(
        &'static str,
        Box<dyn KvEngine>,
        Option<crate::common::ScratchPath>,
    )> = Vec::new();
    for name in ["bitcask", "bplus_tree", "lsm_tree"] {
        let dir = tmp_dir(name);
        let db = open_engine(name, Some(&dir)).expect("open disk engine");
        out.push((name, db, Some(dir)));
    }
    out.push(("memory", open_engine("memory", None).unwrap(), None));
    out
}

fn cleanup(dirs: Vec<Option<crate::common::ScratchPath>>) {
    for d in dirs.into_iter().flatten() {
        let _ = std::fs::remove_dir_all(&d);
    }
}

fn binary_key_regression(name: &str) {
    let dir = tmp_dir(name);
    let db = open_engine(name, (name != "memory").then_some(dir.as_ref())).unwrap();
    db.set_vacuum_auto_slice(0);
    let nested = [b'a', 0, 0, 0, 0, 0, 0, 0, 2];
    {
        let tx = db.begin();
        assert!(tx.set_raw(b"a", b"old".to_vec()));
        tx.commit();
    }
    {
        let tx = db.begin();
        assert!(tx.set_raw(&nested, b"other".to_vec()));
        tx.commit();
    }
    {
        let tx = db.begin();
        assert!(tx.delete(b"a"));
        tx.commit();
    }
    {
        let tx = db.begin();
        assert_eq!(tx.get(b"a"), None);
        let keys: Vec<_> = tx.scan(None, None).into_iter().map(|r| r.key).collect();
        assert_eq!(
            keys,
            vec![nested.to_vec()],
            "{name}: scan resurrected a deleted version"
        );
        tx.commit();
    }
    assert_eq!(db.health().live_keys, 2, "{name}: logical key count");
    assert_eq!(db.health().max_chain, 2, "{name}: version chain count");
    db.vacuum().unwrap();
    let tx = db.begin();
    assert_eq!(
        tx.get(b"a"),
        None,
        "{name}: vacuum resurrected a deleted version"
    );
    assert_eq!(tx.get(&nested), Some(b"other".to_vec()));
    tx.commit();
    drop(tx);
    db.checkpoint().unwrap();
    drop(db);
    if name != "memory" {
        let reopened = open_engine(name, Some(dir.as_ref())).unwrap();
        let tx = reopened.begin();
        assert_eq!(
            tx.get(b"a"),
            None,
            "{name}: reopen resurrected a deleted version"
        );
        assert_eq!(tx.get(&nested), Some(b"other".to_vec()));
        tx.commit();
    }
}

/// `Box<dyn KvEngine>` 没有 `Debug`，`unwrap_err()` 用不了，手动取错误
fn expect_err(r: Result<Box<dyn KvEngine>>) -> EngineError {
    match r {
        Ok(_) => panic!("expected an error, got an engine"),
        Err(e) => e,
    }
}

fn memory_stop_fold_or_emit_cost(fold_only: bool, release_only: bool) {
    let _run = crate::scale_probe::RunGuard::acquire("memory_stop_fold_or_emit_cost");
    use crate::common::kv_snapshot::{
        RECORD_BATCH_BYTES, ValueBuffer, read_snapshot_with, snapshot_emit_reference,
        snapshot_probe,
    };
    use std::io::{Read, Seek, SeekFrom};
    let rows: usize = std::env::var("SNAPSHOT_ROWS")
        .map(|s| s.parse().unwrap())
        .unwrap_or(600_000);
    assert!(rows >= 1000);
    let shape = std::env::var("SNAPSHOT_SHAPE").unwrap_or_else(|_| "all".into());
    assert!(matches!(shape.as_str(), "all" | "first" | "bulk"));
    for appended in [false, true] {
        if (shape == "first" && appended) || (shape == "bulk" && !appended) {
            continue;
        }
        let total_rows = rows + if appended { rows / 10 } else { 0 };
        let mut golden_footer = None;
        let mut golden_bytes = None;
        for (round, reference) in [true, false, false, true].into_iter().enumerate() {
            let dir = ScaleScratch::new("stop_emit");
            let mut db = crate::memory::MVCC::open_with_snapshot(&dir.0).unwrap();
            db.set_vacuum_auto_slice(0);
            for base in (0..rows).step_by(1000) {
                let tx = db.begin_transaction();
                for i in base..(base + 1000).min(rows) {
                    assert!(tx.set_raw(&scale_key(i), scale_val(i)));
                }
                tx.try_commit().unwrap();
            }
            if appended {
                drop(db);
                db = crate::memory::MVCC::open_with_snapshot(&dir.0).unwrap();
                db.set_vacuum_auto_slice(0);
                if release_only {
                    for base in (0..rows / 10).step_by(1000) {
                        let tx = db.begin_transaction();
                        for i in base..(base + 1000).min(rows / 10) {
                            assert!(tx.set_raw(&scale_key(i), scale_val(i + rows)));
                        }
                        tx.try_commit().unwrap();
                    }
                    db.vacuum();
                    let migrated = crate::memory::MVCC::new();
                    let mut bulk = migrated.begin_bulk();
                    db.export_latest_visible_for_each(false, |r| {
                        bulk.put(&r.key, crate::common::pack_plain(&r.value.unwrap()));
                        true
                    });
                    bulk.finish();
                    drop(migrated);
                }
                let mut bulk = db.begin_bulk();
                for i in rows..total_rows {
                    bulk.put(&scale_key(i), crate::common::pack_plain(&scale_val(i)));
                }
                bulk.finish();
            }
            let mode = (reference && !fold_only && !release_only)
                .then(snapshot_emit_reference::Scope::start);
            let fold_mode = (reference && fold_only)
                .then(crate::memory::memory_mvcc::stop_fold_reference::Scope::start);
            // 早期隔离对照开关保留消费输入所有权的原路径。
            let release_mode = (release_only && !reference)
                .then(crate::memory::memory_mvcc::stop_batch_release::Scope::start);
            let scope = snapshot_probe::Scope::start(false);
            let before = crate::mem_probe::allocations();
            let start = std::time::Instant::now();
            drop(db);
            let seconds = start.elapsed().as_secs_f64();
            let allocations = crate::mem_probe::allocations() - before;
            let stats = scope.stats();
            assert_eq!(stats.stop_mode, "exclusive");
            assert_eq!(stats.consumed_rows, total_rows as u64);
            assert_eq!(stats.input_batches, total_rows.div_ceil(8192) as u64);
            assert!(stats.pull_ns + stats.fold_emit_ns <= stats.body_ns);
            if reference && !fold_only && !release_only {
                assert_eq!(stats.record_reference_rows, total_rows as u64);
                assert_eq!(stats.record_buffer_bytes, 0);
                assert_eq!(stats.record_flushes, 0);
            } else {
                assert_eq!(stats.record_buffer_bytes, RECORD_BATCH_BYTES);
                assert_eq!(stats.record_buffered_rows, total_rows as u64);
                assert_eq!(stats.record_direct_large_rows, 0);
                assert_eq!(stats.record_reference_rows, 0);
                assert!(stats.record_flushes > 0 && stats.record_flushes < total_rows as u64);
                assert!(stats.record_flush_ns <= stats.fold_emit_ns);
            }
            if fold_only {
                assert_eq!(
                    stats.fold_direct_rows,
                    if reference {
                        0
                    } else {
                        (total_rows - total_rows.div_ceil(8192)) as u64
                    }
                );
                assert_eq!(
                    stats.fold_window_rows,
                    if reference {
                        total_rows as u64
                    } else {
                        total_rows.div_ceil(8192) as u64
                    }
                );
            }
            if release_only {
                assert_eq!(
                    stats.release_rows,
                    if reference { 0 } else { total_rows as u64 }
                );
                if reference {
                    assert_eq!(stats.input_release_ns, 0);
                }
                assert!(
                    stats.pull_ns + stats.fold_emit_ns + stats.input_release_ns <= stats.body_ns
                );
            }
            eprintln!(
                "MEMORY_STOP_EMIT fold_only={fold_only} release_only={release_only} round={round} reference={reference} appended={appended} rows={total_rows} seconds={seconds:.6} allocations={allocations} stages={stats:?}"
            );
            drop(scope);
            drop(mode);
            drop(fold_mode);
            drop(release_mode);
            let path = dir.0.join("snapshot.bxp");
            let mut file = std::fs::File::open(&path).unwrap();
            file.seek(SeekFrom::End(-16)).unwrap();
            let mut footer = [0_u8; 16];
            file.read_exact(&mut footer).unwrap();
            drop(file);
            if let Some(expected) = &golden_footer {
                assert_eq!(&footer, expected);
            } else {
                golden_footer = Some(footer);
            }
            if rows <= 5000 {
                let bytes = std::fs::read(&path).unwrap();
                if let Some(expected) = &golden_bytes {
                    assert_eq!(&bytes, expected);
                } else {
                    golden_bytes = Some(bytes);
                }
            }
            let mut seen = 0;
            read_snapshot_with(&path, ValueBuffer::Exact, &mut |r| {
                assert_eq!(r.key, scale_key(seen));
                assert_eq!(
                    r.value,
                    Some(scale_val(if release_only && appended && seen < rows / 10 {
                        seen + rows
                    } else {
                        seen
                    }))
                );
                assert_eq!(r.expire_unix_secs, None);
                seen += 1;
                true
            })
            .unwrap();
            assert_eq!(seen, total_rows);
            let reopened = crate::memory::MVCC::open_with_snapshot(&dir.0).unwrap();
            let mut seen = 0;
            reopened.export_latest_visible_for_each(false, |r| {
                assert_eq!(r.key, scale_key(seen));
                assert_eq!(
                    r.value,
                    Some(scale_val(if release_only && appended && seen < rows / 10 {
                        seen + rows
                    } else {
                        seen
                    }))
                );
                assert_eq!(r.expire_unix_secs, None);
                seen += 1;
                true
            });
            assert_eq!(seen, total_rows);
            eprintln!("MEMORY_STOP_EMIT_VERIFIED round={round} appended={appended} rows={seen}");
        }
    }
}

fn bplus_delete_cost(read_projection: bool, leaf_projection: bool, sync_only: bool) {
    let _run = crate::scale_probe::RunGuard::acquire("bplus_delete_cost");
    use crate::common::maintenance_probe::{Scope, Stage};
    let rows: usize = std::env::var("BPLUS_DEL_ROWS")
        .map(|s| s.parse().unwrap())
        .unwrap_or(600_000);
    let mut costs = Vec::new();
    let mut pack_buffers = Vec::new();
    // 使用已经关闭的种子，为每个删除对照臂保留相同物理树，
    // 避免为四个对照臂重复执行完整的大库插入与更新。
    let seed = (leaf_projection || sync_only).then(|| ScaleScratch::new("bplus_delete_seed"));
    if let Some(seed) = &seed {
        eprintln!("BPLUS_DELETE_SEED rows={rows}");
        let db = crate::bplus_tree::MVCC::open(seed.0.join("bplus.db"), 64, 1024);
        db.set_vacuum_auto_slice(0);
        let mut bulk = db.begin_bulk();
        for i in 0..rows {
            bulk.put(&scale_key(i), crate::common::pack_plain(&scale_val(i)));
        }
        bulk.try_finish().unwrap();
        for base in (0..rows / 10).step_by(1000) {
            let tx = db.begin_transaction();
            for i in base..(base + 1000).min(rows / 10) {
                assert!(tx.set_raw(&scale_key(i), scale_val(i + rows)));
            }
            tx.try_commit().unwrap();
        }
    }
    for (round, reference) in [true, false, false, true].into_iter().enumerate() {
        let dir = ScaleScratch::new("bplus_delete_buffers");
        if let Some(seed) = &seed {
            for entry in std::fs::read_dir(&seed.0).unwrap() {
                let entry = entry.unwrap();
                assert!(entry.file_type().unwrap().is_file());
                std::fs::copy(entry.path(), dir.0.join(entry.file_name())).unwrap();
            }
        }
        let path = dir.0.join("bplus.db");
        let db = crate::bplus_tree::MVCC::open(&path, 64, 1024);
        db.set_vacuum_auto_slice(0);
        if seed.is_none() {
            let mut bulk = db.begin_bulk();
            for i in 0..rows {
                bulk.put(&scale_key(i), crate::common::pack_plain(&scale_val(i)));
            }
            bulk.try_finish().unwrap();
            for base in (0..rows / 10).step_by(1000) {
                let tx = db.begin_transaction();
                for i in base..(base + 1000).min(rows / 10) {
                    assert!(tx.set_raw(&scale_key(i), scale_val(i + rows)));
                }
                tx.try_commit().unwrap();
            }
        }
        let mode = (reference && !read_projection && !leaf_projection && !sync_only)
            .then(crate::bplus_tree::bplus_tree::delete_cost_reference::Scope::start);
        let read_mode = (reference && read_projection)
            .then(crate::bplus_tree::bplus_tree::delete_read_reference::Scope::start);
        let leaf_mode = (reference && leaf_projection)
            .then(crate::bplus_tree::bplus_tree::delete_leaf_reference::Scope::start);
        let freeze_leaf = (!leaf_projection && !reference && !sync_only)
            .then(crate::bplus_tree::bplus_tree::delete_leaf_reference::Scope::start);
        let sync_mode = (sync_only && reference)
            .then(crate::bplus_tree::bplus_tree_mvcc::vacuum_sync_reference::Scope::start);
        let blob_syncs = crate::common::disk::fsync_counts()[crate::common::disk::SITE_BLOB];
        let scope = Scope::start();
        let phase = crate::scale_probe::Phase::begin("bplus", "vacuum_abba");
        let start = std::time::Instant::now();
        let result = db.vacuum().unwrap();
        let seconds = start.elapsed().as_secs_f64();
        let stats = scope.stats();
        phase.finish("bplus", "vacuum_abba");
        costs.push(stats.allocations[Stage::VacuumDelete as usize]);
        pack_buffers.push(stats.bplus_pack_buffers);
        assert_eq!(result.versions_removed, rows / 10);
        assert_eq!(result.rows_scanned, (rows + rows / 10) as u64);
        if sync_only {
            assert_eq!(
                crate::common::disk::fsync_counts()[crate::common::disk::SITE_BLOB] - blob_syncs,
                u64::from(reference)
            );
        }
        eprintln!(
            "BPLUS_DELETE_BUFFER read_projection={read_projection} leaf_projection={leaf_projection} sync_only={sync_only} round={round} reference={reference} rows={rows} seconds={seconds:.6} delete_allocations={} stages={stats:?}",
            stats.allocations[Stage::VacuumDelete as usize]
        );
        drop(scope);
        drop(mode);
        drop(read_mode);
        drop(leaf_mode);
        drop(freeze_leaf);
        drop(sync_mode);
        drop(db);
        let db = crate::bplus_tree::MVCC::open(&path, 64, 1024);
        let mut seen = 0;
        db.export_latest_visible_for_each(false, |r| {
            assert_eq!(r.key, scale_key(seen));
            assert_eq!(
                r.value,
                Some(scale_val(if seen < rows / 10 { seen + rows } else { seen }))
            );
            seen += 1;
            true
        });
        assert_eq!(seen, rows);
        eprintln!("BPLUS_DELETE_VERIFIED round={round} rows={seen}");
    }
    if sync_only {
        return;
    } // This gate removes a sync, not deletion allocations.
    assert!(costs[0] > costs[1]);
    assert!(costs[3] > costs[2]);
    assert!(pack_buffers[0] > 0 && pack_buffers[3] > 0);
    if read_projection || leaf_projection {
        assert_eq!(pack_buffers[0], 1);
        assert_eq!(pack_buffers[3], 1);
    }
    assert_eq!(pack_buffers[1], 1);
    assert_eq!(pack_buffers[2], 1);
}

impl ScaleScratch {
    fn new(tag: &str) -> Self {
        let root = std::env::var_os("SCALE_SCRATCH_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        std::fs::create_dir_all(&root).expect("创建台架 scratch 根目录");
        let unique = crate::common::tmp_test_path("se_engine", tag, "");
        let path = root.join(unique.file_name().unwrap());
        // create_dir 而不是清旧目录：碰撞宁可失败，绝不删除别次台架的数据。
        std::fs::create_dir(&path).expect("创建本次台架 scratch 子目录");
        Self(path)
    }
}

impl Drop for ScaleScratch {
    fn drop(&mut self) {
        if let Err(e) = std::fs::remove_dir_all(&self.0) {
            if e.kind() != std::io::ErrorKind::NotFound {
                eprintln!("台架 scratch 清理失败 {}: {e}", self.0.display());
            }
        }
    }
}
