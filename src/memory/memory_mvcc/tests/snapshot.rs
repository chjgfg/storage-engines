// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn search_retention_preserves_binary_snapshots_ttl_and_rejected_chunk_progress() {
    let db = MVCC::new();
    db.set_vacuum_auto_slice(0);
    let tx = db.begin_transaction();
    let mut keys: Vec<Vec<u8>> = (0..9000u64).map(|i| i.to_be_bytes().to_vec()).collect();
    keys.extend([
        vec![],
        b"a".to_vec(),
        b"a\0".to_vec(),
        b"a\0\0".to_vec(),
        b"a\xff".to_vec(),
        vec![255],
    ]);
    for (i, key) in keys.iter().enumerate() {
        let value = if i < 5000 {
            crate::common::codec::pack_ttl(b"expired", 0)
        } else if i % 3 == 0 {
            crate::common::codec::pack_ttl(b"future", u64::MAX)
        } else if i % 3 == 1 {
            vec![1, 2]
        }
        // 长度不足的 TTL 标记按旧版原始值处理。
        else {
            crate::common::codec::pack_plain(b"live")
        };
        assert!(tx.set(key, value));
    }
    tx.commit();
    let snapshot = db.begin_transaction();
    let writer = db.begin_transaction();
    for key in keys.iter().skip(5000).step_by(3) {
        assert!(writer.delete(key));
    }
    let before_commit = db.begin_transaction();
    let own = &writer;
    fn verify(tx: &Transaction) {
        for mode in [
            crate::common::KeyMatchMode::Contains,
            crate::common::KeyMatchMode::Prefix,
        ] {
            for pattern in [b"".as_slice(), b"a".as_slice(), &[0], &[255]] {
                for deleted in [false, true] {
                    let expected =
                        crate::common::search_window(tx, deleted, Some(pattern), mode, None);
                    assert_eq!(
                        crate::common::count_window(tx, deleted, Some(pattern), mode),
                        expected.len()
                    );
                    for page in [0, 1, 241, 500] {
                        for total in [false, true] {
                            let q = crate::common::SearchQuery {
                                pattern: pattern.to_vec(),
                                mode,
                                include_deleted: deleted,
                                page,
                                page_size: 17,
                                need_total: total,
                            };
                            let actual = tx.search_keys(&q);
                            let skip = page * 17;
                            assert_eq!(
                                actual.items,
                                expected
                                    .iter()
                                    .skip(skip)
                                    .take(17)
                                    .cloned()
                                    .collect::<Vec<_>>()
                            );
                            assert_eq!(actual.total, total.then_some(expected.len()));
                            assert_eq!(actual.has_next, expected.len() > skip + 17);
                        }
                    }
                }
            }
        }
    }
    verify(own);
    verify(&before_commit);
    writer.commit();
    verify(&snapshot);
    verify(&before_commit);
    let latest = db.begin_transaction();
    verify(&latest);
    let page = latest.search_keys(&crate::common::SearchQuery::contains(b"", 0, 0));
    assert_eq!(page.page_size, 1);
    assert_eq!(page.items.len(), 1);
    eprintln!(
        "SEARCH_SEMANTICS_VERIFIED snapshots=5 binary_prefixes=true expired_chunk=5000 zero_page_size=true"
    );
}

#[test]
fn snapshot_completed_rows_bypass_window_with_real_reference() {
    use crate::common::{encode_key, kv_snapshot::snapshot_probe, pack_plain};
    let rows: Vec<_> = (0..8200_u64)
        .map(|i| {
            (
                encode_key(format!("k{i:08}").as_bytes(), 1),
                Some(pack_plain(&i.to_le_bytes())),
            )
        })
        .collect();
    let mut expected = None;
    for borrowed in [false, true] {
        for old in [true, false] {
            let path = crate::common::ScratchPath::new("memory", "completed_snapshot", ".bxp");
            let mode = old.then(stop_fold_reference::Scope::start);
            let scope = snapshot_probe::Scope::start(false);
            let input = rows.iter().map(|(key, value)| {
                if borrowed {
                    (
                        Cow::Borrowed(key.as_slice()),
                        value.as_deref().map(Cow::Borrowed),
                    )
                } else {
                    (Cow::Owned(key.clone()), value.clone().map(Cow::Owned))
                }
            });
            assert_eq!(
                MVCC::write_snapshot_rows(
                    &path,
                    input,
                    2,
                    |v| v < 2,
                    if borrowed { "shared" } else { "exclusive" }
                )
                .unwrap(),
                8200
            );
            let stats = scope.stats();
            assert_eq!(stats.fold_direct_rows, if old { 0 } else { 8198 });
            assert_eq!(stats.fold_window_rows, if old { 8200 } else { 2 });
            assert_eq!(stats.record_buffered_rows, 8200);
            assert_eq!(stats.input_batches, 2);
            drop(scope);
            drop(mode);
            let bytes = std::fs::read(&path).unwrap();
            if let Some(expected) = &expected {
                assert_eq!(&bytes, expected);
            } else {
                expected = Some(bytes);
            }
        }
    }
}

#[test]
fn snapshot_batches_keep_cross_batch_versions_and_prefixes() {
    use crate::common::{
        encode_key,
        kv_snapshot::{ValueBuffer, read_snapshot_with, snapshot_probe},
        pack_plain, pack_ttl,
    };
    let mut rows = Vec::new();
    for version in 1..=9000 {
        rows.push((
            encode_key(b"k", version),
            Some(pack_plain(&version.to_le_bytes())),
        ));
    }
    rows.push((encode_key(b"", 1), None));
    rows.push((encode_key(b"k\0", 1), Some(pack_ttl(b"future", u64::MAX))));
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    let mut golden = None;
    for (reference, old_fold) in [(true, false), (false, true), (false, false)] {
        let path = crate::common::ScratchPath::new("memory", "snapshot_batches", ".bxp");
        let _mode = reference.then(stop_rows_reference::Scope::start);
        let _fold_mode = old_fold.then(stop_fold_reference::Scope::start);
        let scope = snapshot_probe::Scope::start(false);
        let input = rows
            .clone()
            .into_iter()
            .map(|(k, v)| (Cow::Owned(k), v.map(Cow::Owned)));
        assert_eq!(
            MVCC::write_snapshot_rows(&path, input, 9001, |v| v <= 9000, "exclusive").unwrap(),
            3
        );
        let stats = scope.stats();
        assert_eq!(stats.consumed_rows, 9002);
        assert_eq!(stats.input_batches, if reference { 0 } else { 2 });
        drop(scope);
        let bytes = std::fs::read(&path).unwrap();
        if let Some(expected) = &golden {
            assert_eq!(&bytes, expected);
        } else {
            golden = Some(bytes);
        }
        let mut records = Vec::new();
        read_snapshot_with(&path, ValueBuffer::Exact, &mut |r| {
            records.push(r);
            true
        })
        .unwrap();
        assert_eq!(
            records[0],
            crate::common::kv_snapshot::KvRecord::new(vec![], None)
        );
        assert_eq!(records[1].value, Some(9000_u64.to_le_bytes().to_vec()));
        assert_eq!(records[2].key, b"k\0");
        assert_eq!(records[2].value, Some(b"future".to_vec()));
        assert_eq!(records[2].expire_unix_secs, Some(u64::MAX));
    }
}

#[test]
fn snapshot_stop_modes_preserve_bytes_and_shared_transactions() {
    use crate::common::kv_snapshot::{ValueBuffer, read_snapshot_with, snapshot_probe};
    let mut golden = None;
    for old_release in [true, false] {
        for (mode, old_rows, old_emit) in [
            ("reference", false, true),
            ("exclusive", true, true),
            ("exclusive", false, true),
            ("exclusive", false, false),
            ("shared", true, true),
            ("shared", false, true),
            ("shared", false, false),
        ] {
            let path = crate::common::ScratchPath::new("memory", "stop_modes", ".bxp");
            let mut db = MVCC::new();
            db.set_vacuum_auto_slice(0);
            let keys = [
                vec![],
                b"a".to_vec(),
                b"a\0".to_vec(),
                b"a\0\0".to_vec(),
                vec![255],
                vec![255, 0],
            ];
            let tx = db.begin_transaction();
            for key in &keys {
                assert!(tx.set_raw(key, vec![0, 1, 255]));
            }
            tx.try_commit().unwrap();
            drop(tx);
            let tx = db.begin_transaction();
            assert!(tx.delete(&keys[0]));
            assert!(tx.set(&keys[1], crate::common::pack_ttl(b"future", u64::MAX)));
            assert!(tx.set(&keys[2], crate::common::pack_ttl(b"expired", 0)));
            assert!(tx.set_raw(&keys[3], vec![]));
            tx.try_commit().unwrap();
            drop(tx);
            let held = (mode == "shared").then(|| db.begin_transaction());
            if let Some(tx) = &held {
                assert!(tx.set_raw(b"uncommitted", vec![7]));
            }
            let reference = (mode == "reference").then(stop_reference::Scope::start);
            let rows_reference = old_rows.then(stop_rows_reference::Scope::start);
            let emit_reference =
                old_emit.then(crate::common::kv_snapshot::snapshot_emit_reference::Scope::start);
            let release_reference = (!old_release).then(stop_batch_release::Scope::start);
            let scope = snapshot_probe::Scope::start(false);
            let clones = crate::common::codec::user_value_copies();
            assert_eq!(db.write_stop_snapshot(&path).unwrap(), 6);
            let stats = scope.stats();
            assert_eq!(stats.stop_mode, mode);
            if mode != "reference" {
                assert_eq!(crate::common::codec::user_value_copies(), clones);
            }
            if mode == "exclusive" {
                assert_eq!(stats.consumed_rows, 10);
                assert_eq!(
                    stats.release_rows,
                    if old_release || old_rows { 0 } else { 10 }
                );
                assert_eq!(
                    db.store.lock().unwrap().iter_keys().count(),
                    0,
                    "exclusive snapshot must consume its input tree"
                );
            } else {
                assert!(db.store.lock().unwrap().iter_keys().count() >= 10);
            }
            if mode != "reference" && !old_rows {
                assert_eq!(stats.input_batches, 1);
                assert_eq!(
                    stats.input_buffer_bytes,
                    stats.visited_rows.min(8192) as usize
                        * std::mem::size_of::<(Cow<'_, [u8]>, Option<Cow<'_, [u8]>>)>()
                );
                assert!(stats.fold_direct_rows + stats.fold_window_rows > 0);
                assert!(
                    stats.pull_ns + stats.fold_emit_ns + stats.input_release_ns <= stats.body_ns
                );
            } else {
                assert_eq!(stats.input_batches, 0);
            }
            if mode != "reference" && !old_rows && !old_emit {
                assert_eq!(stats.record_buffered_rows, 6);
                assert_eq!(stats.record_reference_rows, 0);
                assert_eq!(stats.record_flushes, 1);
            } else {
                assert_eq!(stats.record_buffered_rows, 0);
            }
            drop(scope);
            drop(reference);
            drop(rows_reference);
            drop(emit_reference);
            drop(release_reference);
            let bytes = std::fs::read(&path).unwrap();
            if let Some(expected) = &golden {
                assert_eq!(&bytes, expected);
            } else {
                golden = Some(bytes);
            }
            let mut records = Vec::new();
            read_snapshot_with(&path, ValueBuffer::Exact, &mut |rec| {
                records.push(rec);
                true
            })
            .unwrap();
            assert_eq!(records[0].value, None);
            assert_eq!(records[1].value, Some(b"future".to_vec()));
            assert_eq!(records[1].expire_unix_secs, Some(u64::MAX));
            assert_eq!(records[2].value, None);
            assert_eq!(records[3].value, Some(vec![]));
            if let Some(tx) = held {
                assert_eq!(tx.get(b"uncommitted"), Some(vec![7]));
                assert_eq!(tx.get(&keys[4]), Some(vec![0, 1, 255]));
                tx.rollback();
            }
            let mut reopened = MVCC::new();
            reopened.load_snapshot_from(&path).unwrap();
            assert_eq!(reopened.export_latest_visible(true).len(), 6);
        }
    }
}

#[test]
fn replay_owned_page_uses_one_registry_lookup_and_preserves_duplicate_keys() {
    for reference in [true, false] {
        let mode = reference.then(page_apply_reference::Scope::start);
        let db = MVCC::new();
        let mut page = vec![
            (b"a".to_vec(), Some(vec![1]), None),
            (b"a\0".to_vec(), Some(vec![2]), Some(u64::MAX)),
            (b"a".to_vec(), Some(vec![3]), None),
            (b"tomb".to_vec(), None, None),
        ];
        let before = crate::common::txn::write_registry_lookups();
        MVCC::replay_page(&db, &mut page).unwrap();
        assert_eq!(
            crate::common::txn::write_registry_lookups() - before,
            if reference { 4 } else { 1 }
        );
        assert!(page.is_empty());
        assert_eq!(db.health().total_rows, 3);
        let tx = db.begin_transaction();
        assert_eq!(tx.get(b"a"), Some(vec![3]));
        assert_eq!(tx.get(b"a\0"), Some(vec![2]));
        assert_eq!(tx.get(b"tomb"), None);
        assert_eq!(
            db.export_latest_visible(true)
                .into_iter()
                .find(|r| r.key == b"a\0")
                .unwrap()
                .expire_unix_secs,
            Some(u64::MAX)
        );
        tx.try_commit().unwrap();
        assert!(db.active_txn.lock().unwrap().is_empty());
        drop(mode);
    }
}

#[test]
fn replay_failure_rolls_back_entire_page() {
    for reference in [true, false] {
        let _mode = reference.then(page_apply_reference::Scope::start);
        let db = MVCC::new();
        let held = db.begin_transaction();
        assert!(held.set_raw(b"locked", vec![7]));
        let mut batch = vec![
            (b"fresh".to_vec(), Some(vec![1]), None),
            (b"locked".to_vec(), Some(vec![2]), None),
            (b"unread".to_vec(), Some(vec![3]), None),
        ];
        let scope = crate::common::kv_snapshot::recovery_probe::Scope::start();
        let error = MVCC::replay_page(&db, &mut batch);
        let stages = scope.stats();
        assert_eq!(stages.replay_input_rows, 3);
        assert_eq!(stages.replay_pages, 1);
        assert!(stages.replay_encode_ns > 0 && stages.replay_apply_ns > 0);
        assert_eq!(
            stages.replay_commit_ns, 0,
            "a rejected page must never reach commit"
        );
        assert!(error.is_err());
        assert!(batch.is_empty());
        assert_eq!(
            db.health().total_rows,
            1,
            "a rejected page must not publish its earlier rows"
        );
        assert_eq!(held.get(b"locked"), Some(vec![7]));
        assert_eq!(db.active_txn.lock().unwrap().len(), 1);
        held.rollback();
    }
}

#[test]
fn snapshot_owned_replay_preserves_duplicates_prefixes_and_ttl() {
    use crate::common::kv_snapshot::{KvRecord, atomic_write_bin_v4};
    let path = crate::common::ScratchPath::new("memory", "owned_replay", ".bxp");
    let expiry = crate::common::now_unix_secs() + 3600;
    atomic_write_bin_v4(&path, |writer| {
        for i in 0..1003u64 {
            writer.write(&KvRecord::new(
                i.to_be_bytes().to_vec(),
                Some(i.to_le_bytes().to_vec()),
            ))?;
        }
        writer.write(&KvRecord::new(0u64.to_be_bytes().to_vec(), Some(vec![99])))?;
        writer.write(&KvRecord::new(1u64.to_be_bytes().to_vec(), None))?;
        writer.write(&KvRecord::new(vec![], Some(vec![0, 1])))?;
        writer.write(&KvRecord::new(vec![0], Some(vec![1, 0])).with_expire(expiry))?;
        writer.write(&KvRecord::new(b"expired".to_vec(), Some(vec![8])).with_expire(1))?;
        Ok(())
    })
    .unwrap();
    let mut expected = BTreeMap::new();
    for i in 0..1003u64 {
        expected.insert(
            i.to_be_bytes().to_vec(),
            (Some(i.to_le_bytes().to_vec()), None),
        );
    }
    expected.insert(0u64.to_be_bytes().to_vec(), (Some(vec![99]), None));
    expected.insert(1u64.to_be_bytes().to_vec(), (None, None));
    expected.insert(vec![], (Some(vec![0, 1]), None));
    expected.insert(vec![0], (Some(vec![1, 0]), Some(expiry)));
    for (reference, cost_reference) in [(true, true), (true, false), (false, true), (false, false)]
    {
        let _mode = reference.then(crate::common::txn::owned_reference::Scope::start);
        let _cost_mode = cost_reference.then(replay_cost_reference::Scope::start);
        let recovery = crate::common::kv_snapshot::recovery_probe::Scope::start();
        let mut db = MVCC::new();
        db.load_snapshot_from(&path).unwrap();
        let stages = recovery.stats();
        assert!(stages.verify_ns > 0 && stages.decode_ns > 0 && stages.replay_ns > 0);
        assert!(stages.total_ns >= stages.verify_ns + stages.decode_ns + stages.replay_ns);
        assert_eq!(stages.replay_input_rows, 1007);
        assert_eq!(stages.replay_pages, 2);
        assert!(
            stages.replay_setup_ns > 0
                && stages.replay_encode_ns > 0
                && stages.replay_apply_ns > 0
                && stages.replay_commit_ns > 0
        );
        assert!(
            stages.replay_ns
                >= stages.replay_setup_ns
                    + stages.replay_encode_ns
                    + stages.replay_apply_ns
                    + stages.replay_commit_ns
        );
        drop(recovery);
        assert!(
            crate::common::kv_snapshot::recovery_probe::Timer::start(
                crate::common::kv_snapshot::recovery_probe::Kind::Total
            )
            .is_none()
        );
        let actual: BTreeMap<_, _> = db
            .export_latest_visible(true)
            .into_iter()
            .map(|r| (r.key, (r.value, r.expire_unix_secs)))
            .collect();
        assert_eq!(actual, expected);
        assert!(db.active_txn.lock().unwrap().is_empty());
    }
}

#[test]
fn test_snapshot_isolation() {
    let mvcc = MVCC::new();
    let t1 = mvcc.begin_transaction();
    assert!(t1.set(b"k", b"v1".to_vec()));
    t1.commit();
    let t2 = mvcc.begin_transaction();
    let t3 = mvcc.begin_transaction();
    assert!(t2.set(b"k", b"v2".to_vec()));
    assert_eq!(t3.get(b"k"), Some(b"v1".to_vec()));
    t2.commit();
    assert_eq!(t3.get(b"k"), Some(b"v1".to_vec()));
    let t4 = mvcc.begin_transaction();
    assert_eq!(t4.get(b"k"), Some(b"v2".to_vec()));
    t4.commit();
    t3.commit();
}
