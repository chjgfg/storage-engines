// 该文件集中验证对应职责，保留原测试名称和字面值。

/// 同一 raw key 的编码版本组分散到更多不重叠 SST：反向定位的数据块不随篇数增长。
/// 全源逐 SST seek 是旧形状的计数正控，range_scan 是独立的结果对照。
#[test]
fn test_sst_pruning_seek_blocks_do_not_scale_with_files() {
    use crate::lsm_tree::sst::block_reads;
    for count in [8usize, 64, 256] {
        let dir = tmp_dir(&format!("sst_pruning_{count}"));
        let opts = pruning_options();
        {
            let mut db = LSMTree::open_with_config(&dir, opts.clone()).unwrap();
            db.bulk_mode = true;
            for file in 0..count {
                let entries = (0..4)
                    .map(|i| {
                        let n = (file * 4 + i) as u64;
                        (
                            version_key(n),
                            StoredEntry::put(1, Some(n.to_be_bytes().to_vec())),
                        )
                    })
                    .collect();
                db.inject_sst_for_test(1, entries).unwrap();
            }
            assert!(db.level_ranges_ordered[1]);
        }
        // 冷重开也必须重建范围不变量；不是只在注入路径有效。
        {
            let db = LSMTree::open_with_config(&dir, opts).unwrap();
            assert!(db.level_ranges_ordered[1]);
            for n in [(count * 4 - 1) as u64, (count * 2 - 1) as u64] {
                let mut high = version_key(n);
                high.push(0); // 不存在、但位于 n 与 n+1 之间的前缀扩展
                let low = version_key(0);
                let before = block_reads();
                let hit = db.find_back(&low, &high, 1, |_, _| true);
                let used = block_reads() - before;
                assert_eq!(hit.as_ref().map(|(k, _)| k), Some(&version_key(n)));
                assert!(
                    used > 0 && used <= 3,
                    "{count} SST 的一次 seek 访问 {used} 块"
                );
                assert_eq!(hit, db.range_scan_at_seq(&low, &high, 1).pop());
            }
            let low = version_key(0);
            let high = version_key(u64::MAX);
            let before = block_reads();
            for sst in &db.levels[1] {
                assert!(sst.find_back(&low, &high, 1, |_, _| true).is_some());
            }
            assert!(
                block_reads() - before >= count as u64,
                "全源正控必须随 SST 数增长"
            );

            let key = version_key((count * 2 - 1) as u64);
            let before = LEVEL_RANGE_PROBES.with(|c| c.get());
            assert!(db.get_entry_at(&key, 1).is_some());
            let probes = LEVEL_RANGE_PROBES.with(|c| c.get()) - before;
            assert!(probes > 0 && probes <= 24, "点查范围定位比较了 {probes} 次");
            assert_eq!(db.sst_candidates(1, &key, &key).len(), 1);
        }
        let _ = fs::remove_dir_all(&dir);
    }
}

#[test]
fn test_sst_pruning_keeps_l0_overlap_and_source_seq_rules() {
    let dir = tmp_dir("sst_pruning_semantics");
    let opts = pruning_options();
    {
        let mut db = LSMTree::open_with_config(&dir, opts.clone()).unwrap();
        db.inject_sst_for_test(
            1,
            vec![
                (b"a".to_vec(), StoredEntry::put(50, Some(b"l1".to_vec()))),
                (b"a\0".to_vec(), StoredEntry::put(8, Some(b"old".to_vec()))),
            ],
        )
        .unwrap();
        db.inject_sst_for_test(
            1,
            vec![
                (b"a\0".to_vec(), StoredEntry::drop_at(20)),
                (
                    b"a\0\0".to_vec(),
                    StoredEntry::put(12, Some(b"middle".to_vec())),
                ),
            ],
        )
        .unwrap();
        assert!(db.level_ranges_ordered[1], "相接端点应保留二分路径");
        assert_eq!(db.sst_candidates(1, b"a\0", b"a\0").len(), 2);
        db.inject_sst_for_test(
            2,
            vec![
                (
                    b"a".to_vec(),
                    StoredEntry::put(60, Some(b"higher-seq".to_vec())),
                ),
                (
                    b"a\0".to_vec(),
                    StoredEntry::put(6, Some(b"older".to_vec())),
                ),
                (b"a\0\0".to_vec(), StoredEntry::drop_at(30)),
                (b"b".to_vec(), StoredEntry::put(90, Some(b"last".to_vec()))),
            ],
        )
        .unwrap();
        db.inject_sst_for_test(
            0,
            vec![
                (b"a".to_vec(), StoredEntry::put(2, Some(b"l0".to_vec()))),
                (
                    b"a\0".to_vec(),
                    StoredEntry::put(40, Some(b"revived".to_vec())),
                ),
                (
                    b"a\0\x01".to_vec(),
                    StoredEntry::put(4, Some(b"nested".to_vec())),
                ),
            ],
        )
        .unwrap();
        db.inject_sst_for_test(
            0,
            vec![
                (
                    b"a\0".to_vec(),
                    StoredEntry::put(3, Some(b"new-file-old-seq".to_vec())),
                ),
                (b"a\0\0".to_vec(), StoredEntry::put(5, None)),
            ],
        )
        .unwrap();
    }
    for abnormal in [false, true] {
        let mut db = LSMTree::open_with_config(&dir, opts.clone()).unwrap();
        if abnormal {
            // 尾部插一个 min 更小且包住多个文件的范围，模拟恢复后乱序/嵌套重叠。
            db.inject_sst_for_test(
                1,
                vec![
                    (
                        b"a".to_vec(),
                        StoredEntry::put(70, Some(b"recovered".to_vec())),
                    ),
                    (b"a\0".to_vec(), StoredEntry::drop_at(45)),
                    (b"b".to_vec(), StoredEntry::put(7, Some(b"early".to_vec()))),
                ],
            )
            .unwrap();
            drop(db);
            db = LSMTree::open_with_config(&dir, opts.clone()).unwrap();
            assert!(!db.level_ranges_ordered[1]);
        }
        db.mem_insert_no_wal(
            b"a\0\x01".to_vec(),
            StoredEntry::put(3, Some(b"mem-old".to_vec())),
        );
        db.mem_insert_no_wal(b"b".to_vec(), StoredEntry::put(10, None));
        let mut imm = MemMap::new();
        imm.entry(b"a\x01".to_vec())
            .or_default()
            .insert(18, EntryValue::Put(Some(b"imm".to_vec())));
        db.immutables.push(imm);
        for snap in [0, 3, 7, 15, 25, 35, 50, 100] {
            for (lo, hi) in [
                (b"a".as_slice(), b"b".as_slice()),
                (b"a\0".as_slice(), b"a\x01".as_slice()),
            ] {
                let rows = db.range_scan_at_seq(lo, hi, snap);
                for mode in 0..4 {
                    let accept = |key: &[u8], value: &Option<Vec<u8>>| match mode {
                        0 => true,
                        1 => key.len() == 2, // 拒绝更长的前缀键，继续退
                        2 => value.is_some(),
                        _ => key == b"a\0", // Drop 必须遮住同键旧 Put
                    };
                    let want = rows.iter().rev().find(|(k, v)| accept(k, v)).cloned();
                    assert_eq!(
                        db.find_back(lo, hi, snap, accept),
                        want,
                        "snap={snap}, mode={mode}, abnormal={abnormal}"
                    );
                    #[cfg(feature = "lsm-allocation-reference")]
                    {
                        let _reference = crate::lsm_tree::sst::point_read_reference::Scope::start();
                        assert_eq!(
                            db.find_back(lo, hi, snap, |key, value| accept(key, value)),
                            want,
                            "reference snap={snap}, mode={mode}, abnormal={abnormal}"
                        );
                    }
                    if mode != 2 {
                        let key_accept = |key: &[u8]| accept(key, &None);
                        assert_eq!(
                            db.find_key_back(lo, hi, snap, key_accept).into_key(),
                            want.as_ref().map(|(k, _)| k.clone()),
                            "key-only gate must preserve Drop/seq rules: snap={snap}, mode={mode}, abnormal={abnormal}"
                        );
                    }
                }
            }
        }
        assert!(db.find_back(b"z", b"a", 100, |_, _| true).is_none());
        // 不把本轮 mem/immutable 写回磁盘，下一轮只考察原文件集。
        db.mem.clear();
        db.immutables.clear();
    }
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_sst_pruning_l0_descending_files_and_light_bounds() {
    use crate::lsm_tree::sst::block_reads;
    use crate::lsm_tree::sst::entry_value_clones;
    let dir = tmp_dir("sst_pruning_l0_bounds");
    {
        let mut db = LSMTree::open_with_config(&dir, pruning_options()).unwrap();
        db.bulk_mode = true;
        for n in (0..64).rev() {
            db.inject_sst_for_test(
                0,
                vec![(version_key(n), StoredEntry::put(1, Some(vec![1])))],
            )
            .unwrap();
        }
        let before = block_reads();
        let clones_before = entry_value_clones();
        let range_low = version_key(0);
        let range_high = version_key(u64::MAX);
        let hit = db
            .find_back(&range_low, &range_high, 1, |_, _| true)
            .unwrap();
        assert_eq!(hit.0, version_key(63));
        assert_eq!(
            block_reads() - before,
            1,
            "候选块已包含胜者，不应再做第二次点查"
        );
        assert_eq!(
            entry_value_clones() - clones_before,
            1,
            "新路径只拥有化胜出 value 一次"
        );
        #[cfg(feature = "lsm-allocation-reference")]
        {
            let _reference = crate::lsm_tree::sst::point_read_reference::Scope::start();
            let clones_before = entry_value_clones();
            let reference = db
                .find_back(&range_low, &range_high, 1, |_, _| true)
                .unwrap();
            assert_eq!(reference, hit);
            assert_eq!(
                entry_value_clones() - clones_before,
                2,
                "旧式反向候选再点查会复制胜出 value 两次"
            );
        }

        let from = version_key(10);
        let to = version_key(11);
        let before = block_reads();
        let rows: Vec<_> = db.light_iter_range(Some(&from), Some(&to), 1).collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, from);
        assert_eq!(
            block_reads() - before,
            1,
            "min_key == to 的文件不该初始化读块"
        );
        let before = block_reads();
        assert_eq!(db.light_iter_range(Some(&to), Some(&from), 1).count(), 0);
        assert_eq!(db.light_iter_range(Some(&from), Some(&from), 1).count(), 0);
        assert_eq!(block_reads() - before, 0);
    }
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_sst_pruning_refreshes_ranges_after_compaction() {
    let dir = tmp_dir("sst_pruning_compaction");
    let mut opts = pruning_options();
    opts.target_sst_bytes = 128;
    {
        let mut db = LSMTree::open_with_config(&dir, opts.clone()).unwrap();
        for start in [0, 12] {
            db.inject_sst_for_test(
                0,
                (start..start + 12)
                    .map(|n| (version_key(n), StoredEntry::put(1, Some(vec![1]))))
                    .collect(),
            )
            .unwrap();
        }
        let low = version_key(0);
        let high = version_key(23);
        let want = db.range_scan(low.clone(), high.clone());
        db.compact_level(0).unwrap();
        assert!(db.levels[1].len() > 1);
        assert!(db.level_ranges_ordered[1]);
        db.compact_level(1).unwrap();
        assert!(!db.levels[2].is_empty());
        assert!(db.level_ranges_ordered[1] && db.level_ranges_ordered[2]);
        assert_eq!(db.range_scan(low.clone(), high.clone()), want);
        for (key, value) in &want {
            assert_eq!(db.get_value_at(key, 1), Some(value.clone()));
        }
        assert_eq!(
            db.find_back(&low, &high, 1, |_, _| true),
            want.last().cloned()
        );
    }
    {
        let db = LSMTree::open_with_config(&dir, opts).unwrap();
        assert!(db.level_ranges_ordered[1] && db.level_ranges_ordered[2]);
        assert_eq!(
            db.find_back(&version_key(0), &version_key(23), 1, |_, _| true)
                .unwrap()
                .0,
            version_key(23)
        );
    }
    let _ = fs::remove_dir_all(&dir);
}
