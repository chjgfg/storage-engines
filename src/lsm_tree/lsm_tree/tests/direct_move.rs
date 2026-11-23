// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn disjoint_l0_move_preserves_files_and_reopen_with_reference_control() {
    for reference in [true, false] {
        let dir = tmp_dir("disjoint_l0");
        let mut db = LSMTree::open(&dir).unwrap();
        // L0 插入顺序与键顺序不同，目标文件位于键范围的空隙中。
        for key in [b"e", b"a", b"c"] {
            db.inject_sst_for_test(
                0,
                vec![(key.to_vec(), StoredEntry::put(1, Some(key.to_vec())))],
            )
            .unwrap();
        }
        db.inject_sst_for_test(1, vec![(b"b".to_vec(), StoredEntry::put(1, None))])
            .unwrap();
        db.write_manifest().unwrap();
        let originals: Vec<_> = db
            .levels
            .iter()
            .flatten()
            .map(|f| (f.id, f.path.clone(), fs::read(&f.path).unwrap()))
            .collect();
        let mode = reference.then(disjoint_move_reference::Scope::start);
        let scope = maintenance_probe::Scope::start();
        db.compact_level(0).unwrap();
        let stats = scope.stats();
        assert_eq!(stats.trivial_moves, if reference { 0 } else { 3 });
        assert_eq!(stats.owned_input_rows, if reference { 4 } else { 0 });
        // 只统计实际生成的 SST 文件，复用文件和输入字节另计。
        assert_eq!(
            stats.output_files,
            if reference {
                db.levels[1].len() as u64
            } else {
                0
            }
        );
        assert_eq!(
            stats.output_bytes,
            if reference {
                db.levels[1]
                    .iter()
                    .map(|file| fs::metadata(&file.path).unwrap().len())
                    .sum()
            } else {
                0
            }
        );
        if !reference {
            assert_eq!(stats.merge_ns, 0);
            assert_eq!(stats.sst_sync_ns, 0);
            for (id, path, bytes) in &originals {
                assert!(db.levels[1].iter().any(|f| f.id == *id));
                assert_eq!(fs::read(path).unwrap(), *bytes);
            }
        }
        drop(scope);
        drop(mode);
        drop(db);
        let db = LSMTree::open(&dir).unwrap();
        for key in [b"a", b"c", b"e"] {
            assert_eq!(db.get(key), Some(key.to_vec()));
        }
        assert_eq!(db.get(b"b"), None);
        assert!(db.levels[0].is_empty());
    }
}

#[test]
fn disjoint_l0_move_failure_restores_source_order_and_target() {
    let dir = tmp_dir("disjoint_publish_failure");
    let mut db = LSMTree::open(&dir).unwrap();
    for key in [b"e", b"a", b"c"] {
        db.inject_sst_for_test(
            0,
            vec![(key.to_vec(), StoredEntry::put(1, Some(key.to_vec())))],
        )
        .unwrap();
    }
    db.inject_sst_for_test(
        1,
        vec![(b"b".to_vec(), StoredEntry::put(1, Some(b"b".to_vec())))],
    )
    .unwrap();
    db.write_manifest().unwrap();
    let ids = |db: &LSMTree| {
        db.levels
            .iter()
            .map(|l| l.iter().map(|f| f.id).collect::<Vec<_>>())
            .collect::<Vec<_>>()
    };
    let before = ids(&db);
    let manifest = fs::read(db.manifest_path()).unwrap();
    fs::create_dir(db.dir.join("MANIFEST.tmp")).unwrap();
    assert!(db.compact_level(0).is_err());
    assert_eq!(ids(&db), before);
    assert_eq!(fs::read(db.manifest_path()).unwrap(), manifest);
    for key in [b"a", b"b", b"c", b"e"] {
        assert_eq!(db.get(key), Some(key.to_vec()));
    }
    fs::remove_dir(db.dir.join("MANIFEST.tmp")).unwrap();
    db.compact_level(0).unwrap();
    drop(db);
    let db = LSMTree::open(&dir).unwrap();
    for key in [b"a", b"b", b"c", b"e"] {
        assert_eq!(db.get(key), Some(key.to_vec()));
    }
}

#[test]
fn disjoint_l0_move_fallbacks_match_reference() {
    for mode in 0..9 {
        let mut answers = Vec::new();
        for reference in [true, false] {
            let dir = tmp_dir("disjoint_fallback");
            let mut db = LSMTree::open(&dir).unwrap();
            let first = if mode == 2 {
                StoredEntry::drop_at(3)
            } else {
                StoredEntry::put(3, Some(b"new".to_vec()))
            };
            let mut entries = vec![(b"a".to_vec(), first)];
            if mode == 3 {
                entries.insert(
                    0,
                    (b"a".to_vec(), StoredEntry::put(1, Some(b"old".to_vec()))),
                );
            }
            db.inject_sst_for_test(0, entries).unwrap();
            db.inject_sst_for_test(
                0,
                vec![(
                    if mode == 0 {
                        b"a".to_vec()
                    } else {
                        b"z".to_vec()
                    },
                    StoredEntry::put(4, None),
                )],
            )
            .unwrap();
            if mode == 1 {
                db.set_compaction_filter(Some(Arc::new(|_, _| false)));
            }
            if mode == 2 {
                db.inject_sst_for_test(
                    3,
                    vec![(b"a".to_vec(), StoredEntry::put(1, Some(b"old".to_vec())))],
                )
                .unwrap();
            }
            if mode == 4 {
                db.ttl_meta.insert(b"a".to_vec(), 1);
            }
            if mode == 5 {
                db.inject_sst_for_test(
                    1,
                    vec![(b"a".to_vec(), StoredEntry::put(1, Some(b"old".to_vec())))],
                )
                .unwrap();
            }
            if mode == 6 {
                let f = &db.levels[0][0];
                db.levels[0][0] = SstFile::open(f.id, &f.path).unwrap();
            }
            if mode == 7 {
                for seq in [1, 2] {
                    db.inject_sst_for_test(
                        1,
                        vec![(b"m".to_vec(), StoredEntry::put(seq, Some(vec![seq as u8])))],
                    )
                    .unwrap();
                }
            }
            if mode == 8 {
                db.bulk_mode = true;
            }
            let control = reference.then(disjoint_move_reference::Scope::start);
            let scope = maintenance_probe::Scope::start();
            db.compact_level(0).unwrap();
            assert_eq!(scope.stats().trivial_moves, 0, "mode={mode}");
            assert!(scope.stats().owned_input_rows > 0, "mode={mode}");
            drop(scope);
            drop(control);
            db.bulk_mode = false;
            drop(db);
            let db = LSMTree::open(&dir).unwrap();
            answers.push([db.get(b"a"), db.get(b"m"), db.get(b"z")]);
        }
        assert_eq!(answers[0], answers[1], "mode={mode}");
        if mode == 2 {
            assert_eq!(answers[1][0], None, "deleted value resurrected");
        }
    }
}

#[test]
fn trivial_move_reference_rewrites_and_fallbacks_preserve_semantics() {
    for mode in 0..8 {
        let dir = tmp_dir("trivial_fallback");
        let mut db = LSMTree::open(&dir).unwrap();
        let entries = if mode == 3 {
            vec![
                (b"a".to_vec(), StoredEntry::put(1, Some(b"old".to_vec()))),
                (b"a".to_vec(), StoredEntry::put(2, Some(b"new".to_vec()))),
            ]
        } else {
            vec![(
                b"a".to_vec(),
                if mode == 2 {
                    StoredEntry::drop_at(2)
                } else {
                    StoredEntry::put(2, Some(b"new".to_vec()))
                },
            )]
        };
        db.inject_sst_for_test(1, entries).unwrap();
        let id = db.levels[1][0].id;
        if mode == 1 {
            db.set_compaction_filter(Some(Arc::new(|_, _| false)));
        }
        if mode == 2 {
            db.inject_sst_for_test(
                3,
                vec![(b"a".to_vec(), StoredEntry::put(1, Some(b"old".to_vec())))],
            )
            .unwrap();
        }
        if mode == 4 {
            db.ttl_meta.insert(b"a".to_vec(), 1);
        }
        if mode == 5 {
            db.inject_sst_for_test(
                2,
                vec![(b"a".to_vec(), StoredEntry::put(1, Some(b"old".to_vec())))],
            )
            .unwrap();
        }
        if mode == 6 {
            let path = db.levels[1][0].path.clone();
            db.levels[1][0] = SstFile::open(id, path).unwrap(); // Unknown certificate after reopen.
        }
        if mode == 7 {
            db.inject_sst_for_test(
                1,
                vec![(b"a".to_vec(), StoredEntry::put(1, Some(b"old".to_vec())))],
            )
            .unwrap();
        }
        let reference = (mode == 0).then(trivial_move_probe::Scope::start);
        let scope = maintenance_probe::Scope::start();
        db.compact_level(1).unwrap();
        assert_eq!(scope.stats().trivial_moves, 0, "mode={mode}");
        assert!(db.levels[2].iter().all(|file| file.id != id));
        if mode == 1 || mode == 4 {
            assert!(db.levels[2].is_empty());
        } else {
            let value = &db.levels[2][0].get(b"a").unwrap().value;
            if mode == 2 {
                assert!(matches!(value, EntryValue::Drop));
            } else if mode == 7 {
                // 源文件同样大时选择最后一个，记录其原来的
                // 旧值向下层迁移后，全局归并仍应选择更新的源值。
                assert_eq!(value, &EntryValue::Put(Some(b"old".to_vec())));
                assert_eq!(db.get(b"a"), Some(b"new".to_vec()));
            } else {
                assert_eq!(value, &EntryValue::Put(Some(b"new".to_vec())));
            }
        }
        drop(scope);
        drop(reference);
    }
}

#[test]
#[ignore = "same-build rewrite/move comparison; SCALE_ROWS controls input size"]
fn trivial_move_cost_comparison() {
    let rows: usize = std::env::var("SCALE_ROWS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100_000);
    for (round, reference) in [true, false, false, true].into_iter().enumerate() {
        let dir = tmp_dir("trivial_compare");
        let mut db = LSMTree::open(&dir).unwrap();
        db.opts.target_sst_bytes = usize::MAX;
        let id = db.next_sst_id.fetch_add(1, Ordering::SeqCst);
        let mut builder = SstBuilder::with_options(id, db.sst_path(id), 128, true);
        for i in 0..rows {
            builder.add(
                (i as u64).to_be_bytes().to_vec(),
                StoredEntry::put(i as u64 + 1, Some(vec![7; 64])),
            );
        }
        db.levels[1].push(builder.finish().unwrap());
        db.write_manifest().unwrap();
        let reference_scope = reference.then(trivial_move_probe::Scope::start);
        let scope = maintenance_probe::Scope::start();
        let start = std::time::Instant::now();
        db.compact_level(1).unwrap();
        let seconds = start.elapsed().as_secs_f64();
        let stats = scope.stats();
        assert_eq!(stats.trivial_moves, if reference { 0 } else { 1 });
        if !reference {
            assert_eq!(stats.merge_ns, 0);
            assert_eq!(stats.sst_sync_ns, 0);
        }
        drop(scope);
        drop(reference_scope);
        let mut seen = 0;
        for file in &db.levels[2] {
            for (key, value) in file.iter_all() {
                assert_eq!(key, (seen as u64).to_be_bytes());
                assert_eq!(value, StoredEntry::put(seen as u64 + 1, Some(vec![7; 64])));
                seen += 1;
            }
        }
        assert_eq!(seen, rows);
        eprintln!(
            "LSM_MOVE_COMPARE round={round} reference={reference} rows={rows} seconds={seconds:.6} stats={stats:?}"
        );
    }
}

#[test]
fn trivial_move_preserves_file_identity_and_bytes() {
    let dir = tmp_dir("trivial_move");
    let mut db = LSMTree::open(&dir).unwrap();
    db.inject_sst_for_test(
        1,
        vec![
            (b"a".to_vec(), StoredEntry::put(1, Some(b"one".to_vec()))),
            (b"b".to_vec(), StoredEntry::put(2, None)),
        ],
    )
    .unwrap();
    let id = db.levels[1][0].id;
    let path = db.levels[1][0].path.clone();
    let before = fs::read(&path).unwrap();
    let scope = maintenance_probe::Scope::start();
    db.compact_level(1).unwrap();
    assert_eq!(scope.stats().trivial_moves, 1);
    assert_eq!(scope.stats().merge_ns, 0);
    assert_eq!(scope.stats().sst_sync_ns, 0);
    drop(scope);
    assert_eq!(
        db.levels[2][0].id, id,
        "single safe input must retain its file"
    );
    assert_eq!(fs::read(&path).unwrap(), before);
    assert_eq!(
        db.levels[2][0].get(b"a").unwrap(),
        StoredEntry::put(1, Some(b"one".to_vec()))
    );
    assert_eq!(
        db.levels[2][0].get(b"b").unwrap(),
        StoredEntry::put(2, None)
    );
    assert!(manifest_publishes(
        &fs::read_to_string(db.manifest_path()).unwrap(),
        2,
        &[id]
    ));
    drop(db);
    let db = LSMTree::open(&dir).unwrap();
    assert_eq!(db.levels[2][0].id, id);
    assert_eq!(
        db.levels[2][0].get(b"a").unwrap(),
        StoredEntry::put(1, Some(b"one".to_vec()))
    );
}
