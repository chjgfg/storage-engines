// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
#[cfg(any(unix, windows))]
fn positioned_sst_read_retries_short_reads_and_interrupts() {
    let data = b"0123456789";
    let mut output = [0; 5];
    let mut calls = 0;
    read_exact_positioned(3, &mut output, |buffer, offset| {
        calls += 1;
        if calls == 1 {
            return Err(std::io::ErrorKind::Interrupted.into());
        }
        let count = buffer.len().min(2);
        buffer[..count].copy_from_slice(&data[offset as usize..offset as usize + count]);
        Ok(count)
    })
    .unwrap();
    assert_eq!(&output, b"34567");
    assert_eq!(calls, 4);
    assert_eq!(
        read_exact_positioned(0, &mut output, |_, _| Ok(0))
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::UnexpectedEof
    );
    assert_eq!(
        read_exact_positioned(u64::MAX, &mut output, |_, _| Ok(1))
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::InvalidInput
    );
}

#[test]
#[cfg(any(unix, windows))]
fn positioned_sst_point_reads_match_seek_reads() {
    let path = tmp("positioned_sst");
    let mut builder = SstBuilder::with_options(1, &path, 64, false);
    for key in 0..64_u64 {
        builder.add(
            key.to_be_bytes().to_vec(),
            StoredEntry::put(key, Some(vec![key as u8; 16])),
        );
    }
    let sst = builder.finish().unwrap();
    for reference in [true, false] {
        let _mode = reference.then(positioned_read_reference::Scope::start);
        let before = positioned_read_reference::seeks();
        for key in 0..64_u64 {
            assert_eq!(
                sst.get_at(&key.to_be_bytes(), u64::MAX),
                Some(StoredEntry::put(key, Some(vec![key as u8; 16])))
            );
        }
        let seeks = positioned_read_reference::seeks() - before;
        assert!(if reference { seeks > 0 } else { seeks == 0 });
    }
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[sst.index[0].offset as usize + 4] ^= 1;
    std::fs::write(&path, bytes).unwrap();
    assert_eq!(
        sst.load_point_block(0).unwrap_err().kind(),
        std::io::ErrorKind::InvalidData
    );
}

#[test]
fn compaction_owns_blocks_and_counts_legacy_fallback() {
    use crate::lsm_tree::lsm_tree::maintenance_probe;
    let path = tmp("owned_compaction");
    let mut builder = SstBuilder::with_options(1, &path, 2, false);
    for i in 0..9_u64 {
        builder.add(
            i.to_be_bytes().to_vec(),
            StoredEntry::put(i, Some(vec![i as u8; 11])),
        );
    }
    let mut sst = builder.finish().unwrap();
    let cache = Arc::new(Mutex::new(BlockCache::new(2)));
    sst.set_block_cache(cache.clone());
    let scope = maintenance_probe::Scope::start();
    let clones = entry_value_clones();
    let owned: Vec<_> = sst
        .compaction_input()
        .collect::<std::io::Result<_>>()
        .unwrap();
    assert_eq!(
        entry_value_clones(),
        clones,
        "owned block input must not clone values"
    );
    assert_eq!(scope.stats().owned_input_rows, 9);
    assert_eq!(scope.stats().compaction_blocks, 5);
    assert!(
        cache.lock().unwrap().is_empty(),
        "compaction must not fill the point cache"
    );
    drop(scope);
    assert_eq!(sst.iter_entries(), owned);
    assert_eq!(cache.lock().unwrap().len(), 2);
    let resident = cache.lock().unwrap().len();
    let clones = entry_value_clones();
    assert_eq!(
        sst.compaction_input()
            .collect::<std::io::Result<Vec<_>>>()
            .unwrap(),
        owned
    );
    assert_eq!(entry_value_clones(), clones);
    assert_eq!(cache.lock().unwrap().len(), resident);
    sst.entries_cache = Some(owned);
    assert!(matches!(
        sst.compaction_input(),
        CompactionRows::LegacyShared(_)
    ));
    let scope = maintenance_probe::Scope::start();
    let clones = entry_value_clones();
    assert_eq!(
        sst.compaction_input()
            .collect::<std::io::Result<Vec<_>>>()
            .unwrap()
            .len(),
        9
    );
    assert_eq!(entry_value_clones() - clones, 9);
    assert_eq!(scope.stats().legacy_input_clones, 9);
    assert_eq!(scope.stats().owned_input_rows, 0);
}

#[test]
fn compaction_corruption_is_an_error_instead_of_eof() {
    let path = tmp("strict_input");
    let mut builder = SstBuilder::with_options(1, &path, 3, false);
    for i in 0..9_u64 {
        builder.add(i.to_be_bytes().to_vec(), StoredEntry::put(i, Some(vec![7])));
    }
    let sst = builder.finish().unwrap();
    let mut raw = std::fs::read(&path).unwrap();
    raw[sst.index[1].offset as usize + 4] ^= 1;
    std::fs::write(&path, raw).unwrap();
    assert_eq!(
        sst.iter_all().count(),
        3,
        "old read iterator silently truncates at the bad block"
    );
    let mut strict = sst.compaction_input();
    for _ in 0..3 {
        assert!(strict.next().unwrap().is_ok());
    }
    assert_eq!(
        strict.next().unwrap().unwrap_err().kind(),
        std::io::ErrorKind::InvalidData
    );
    assert!(strict.next().is_none());
}

#[test]
fn merge_heap_moves_values_and_preserves_seq_source_ties() {
    let sources = || {
        vec![
            vec![
                (b"a".to_vec(), StoredEntry::put(1, Some(vec![1]))),
                (b"a".to_vec(), StoredEntry::put(3, Some(vec![3]))),
                (b"b".to_vec(), StoredEntry::put(4, None)),
            ],
            vec![
                (b"a".to_vec(), StoredEntry::put(3, Some(vec![9]))),
                (b"b".to_vec(), StoredEntry::put(2, Some(vec![2]))),
                (b"c".to_vec(), StoredEntry::drop_at(5)),
            ],
        ]
    };
    let clones = entry_value_clones();
    let rows = merge_latest(sources());
    assert_eq!(
        entry_value_clones(),
        clones,
        "heap must move every consumed head"
    );
    assert_eq!(
        rows,
        vec![
            (b"a".to_vec(), StoredEntry::put(3, Some(vec![3]))),
            (b"b".to_vec(), StoredEntry::put(4, None)),
            (b"c".to_vec(), StoredEntry::drop_at(5))
        ]
    );
    let _reference = compaction_reference::Scope::start();
    let clones = entry_value_clones();
    assert_eq!(merge_latest(sources()), rows);
    assert_eq!(
        entry_value_clones() - clones,
        6,
        "old heap would fail the zero-clone guard"
    );
}

/// M42 的**格式钉子**：同一批条目必须产出**逐字节相同**的 SST。
///
/// 为什么需要它：这一刀改的是 builder 的内部形状（"攒满一整张表才编块"→"攒满一块就落盘"），
/// 那种改动出错的方式不是崩，而是**悄悄换了文件格式** —— 块的切法、index 的偏移、bloom 的
/// 位图宽度，任何一个动了，旧文件还能读、新文件读得回，但两边对不上同一份口径。
/// 所以钉两个数：文件长度与全文件 crc32。
///
/// ⚠️ 这两个常数是在**改前**的实现上跑出来的（下面断言里写的是当时的实测值）⇒
/// 改完之后红就代表格式被动过，不是测试写错了。
#[test]
fn test_sst_builder_byteprint_is_stable() {
    let path = tmp("byteprint");
    {
        let mut b = SstBuilder::with_options(7, &path, 64, true).with_bloom_bits(10);
        for i in 0..1000usize {
            b.add(
                format!("k{i:06}").into_bytes(),
                StoredEntry::put(i as u64, Some(vec![(i % 251) as u8; i % 13])),
            );
        }
        let sst = b.finish().unwrap();
        // 回读必须与写入逐条一致（这一半不需要 golden 也成立）
        let back = sst.iter_entries();
        assert_eq!(back.len(), 1000, "读回来的条目数不对");
        assert_eq!(back[0].0, b"k000000".to_vec());
        assert_eq!(back[999].0, b"k000999".to_vec());
    }
    let bytes = std::fs::read(&path).unwrap();
    let fc = crc32(&bytes);
    eprintln!("[byteprint] 长度={} crc32={fc:08x}", bytes.len());
    assert_eq!(
        bytes.len(),
        31_823,
        "SST 字节长度变了 ⇒ 块切法/索引/布隆的口径被动过"
    );
    assert_eq!(
        fc, 0x7911_d92d,
        "SST 内容 crc32 变了 ⇒ 写出去的字节与改前不一致"
    );
    let _ = std::fs::remove_file(&path);
}

/// 真调用点上这条账也必须是 0（M30）：上面那条量的是形状，这条量的是 `merge_to_ssts` 本身。
#[test]
fn test_merge_to_ssts_does_not_walk_entries_for_size_check() {
    const PER: usize = 3000;
    let sources: Vec<Vec<(Vec<u8>, StoredEntry)>> = vec![
        (0..PER)
            .map(|i| {
                (
                    format!("a{i:06}").into_bytes(),
                    StoredEntry::put(i as u64, Some(vec![1u8; 60])),
                )
            })
            .collect(),
        (0..PER)
            .map(|i| {
                (
                    format!("b{i:06}").into_bytes(),
                    StoredEntry::put(i as u64, Some(vec![1u8; 60])),
                )
            })
            .collect(),
    ];
    let mut id = 0u64;
    let s0 = builder_size_walks();
    // 压出来的 SST 由守卫持有：`with_options` 收的是**路径**，文件要到压实时才写出来，
    // 守卫必须活过 `merge_to_ssts` 那一趟（先声明、再借进闭包）
    let mut sst_guards: Vec<crate::common::ScratchPath> = Vec::new();
    // target 给 64 KiB ⇒ 6000 行该切出好几个文件（"每行问一次判定"这一步真的被走到）
    let files = merge_to_ssts(
        vec_inputs(sources),
        |_, _| true,
        || {
            id += 1;
            let p = tmp(&format!("est_merge_{id}"));
            let owned = p.to_path_buf();
            sst_guards.push(p);
            SstBuilder::with_options(id, owned, 64, false)
        },
        64 * 1024,
    )
    .unwrap();
    let walks = builder_size_walks() - s0;
    assert!(
        files.len() >= 3,
        "形状没造出来：target 64 KiB 下 6000 行只切了 {} 个文件 ⇒ 判定可能一次都没问过",
        files.len()
    );
    assert_eq!(
        walks,
        0,
        "{} 行 × {} 个文件的压实里，尺寸判定走了 {walks} 个条目",
        PER * 2,
        files.len()
    );
    drop(files);
}

#[test]
fn test_sst_pruning_back_and_get_keep_cross_block_versions() {
    use std::collections::BTreeMap;
    let path = tmp("pruning_versions");
    let mut builder = SstBuilder::with_options(1, &path, 2, false);
    builder.add(b"a".to_vec(), StoredEntry::put(1, Some(b"a".to_vec())));
    // 只要求键有序，不假设 seq 有序；b 的版本组跨五块。
    for seq in [5, 2, 9, 1, 7, 3, 11, 10] {
        let value = if seq == 10 {
            StoredEntry::drop_at(seq)
        } else {
            StoredEntry::put(seq, Some(vec![seq as u8]))
        };
        builder.add(b"b".to_vec(), value);
    }
    builder.add(b"c".to_vec(), StoredEntry::put(20, Some(b"c".to_vec())));
    let mut sst = builder.finish().unwrap();
    let all: Vec<_> = sst.iter_all().collect();
    for memory_backed in [false, true] {
        if memory_backed {
            // 同一折叠逻辑也覆盖旧格式的内存条目路径。
            sst.entries_cache = Some(all.clone());
        }
        for snap in [0, 1, 4, 7, 9, 10, 11, 20] {
            let mut expected = BTreeMap::<Vec<u8>, StoredEntry>::new();
            for (k, se) in &all {
                if se.visible_at(snap) && expected.get(k).is_none_or(|old| se.seq >= old.seq) {
                    expected.insert(k.clone(), se.clone());
                }
            }
            assert_eq!(
                sst.get_at(b"b", snap),
                expected.get(b"b".as_slice()).cloned()
            );
            let scanned = sst
                .light_iter(Some(b"b"), Some(b"c"), snap)
                .max_by_key(|(_, row)| row.seq);
            assert_eq!(
                scanned.map(|(_, row)| (row.seq, row.kind)),
                expected
                    .get(b"b".as_slice())
                    .map(|row| (row.seq, row.value.kind_byte())),
                "forward fallback must include the entire key across block boundaries"
            );
            assert_eq!(
                sst.range_at(b"b", b"b", snap),
                expected
                    .iter()
                    .filter(|(k, _)| k.as_slice() == b"b")
                    .map(|(k, se)| (k.clone(), se.clone()))
                    .collect::<Vec<_>>()
            );
            let want = expected
                .iter()
                .rev()
                .find(|(_, se)| !matches!(se.value, EntryValue::Drop))
                .map(|(k, se)| (k.clone(), se.clone()));
            assert_eq!(
                sst.find_back(b"a", b"c", snap, |_, se| !matches!(
                    se.value,
                    EntryValue::Drop
                )),
                want
            );
            let mut called = Vec::new();
            assert!(
                sst.find_back(b"a", b"c", snap, |k, _| {
                    called.push(k.to_vec());
                    false
                })
                .is_none()
            );
            assert_eq!(
                called,
                expected.keys().rev().cloned().collect::<Vec<_>>(),
                "每键只允许拒绝一次"
            );
        }
    }
    drop(sst);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_sst_pruning_half_open_bounds_and_empty_key() {
    let path = tmp("pruning_bounds");
    let mut builder = SstBuilder::with_options(1, &path, 1, false);
    builder.add(b"m".to_vec(), StoredEntry::put(1, Some(vec![1])));
    let sst = builder.finish().unwrap();
    let before = block_reads();
    assert_eq!(sst.light_iter(None, Some(b"m"), 1).count(), 0);
    assert_eq!(sst.light_iter(Some(b"n"), None, 1).count(), 0);
    assert_eq!(sst.light_iter(Some(b"z"), Some(b"a"), 1).count(), 0);
    assert_eq!(block_reads() - before, 0, "界外源不能为了初始化先读一块");
    assert_eq!(sst.light_iter(Some(b"m"), None, 1).count(), 1);
    assert!(block_reads() > before, "相交源是计数正控");
    drop(sst);
    let _ = std::fs::remove_file(&path);

    let path = tmp("pruning_empty_key");
    let mut builder = SstBuilder::with_options(2, &path, 1, false);
    builder.add(Vec::new(), StoredEntry::put(1, Some(vec![2])));
    let sst = builder.finish().unwrap();
    assert!(sst.may_overlap(b"", b""));
    assert_eq!(sst.get_at(b"", 1), Some(StoredEntry::put(1, Some(vec![2]))));
    assert_eq!(
        sst.find_back(b"", b"", 1, |_, _| true).unwrap().0,
        Vec::<u8>::new()
    );
    assert_eq!(sst.light_iter(None, None, 1).count(), 1);
    assert_eq!(sst.light_iter(None, Some(b""), 1).count(), 0);
    drop(sst);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_merge_latest() {
    let older = vec![
        (b"a".to_vec(), StoredEntry::put(1, Some(b"old".to_vec()))),
        (b"b".to_vec(), StoredEntry::put(1, Some(b"b".to_vec()))),
    ];
    let newer = vec![
        (b"a".to_vec(), StoredEntry::put(2, Some(b"new".to_vec()))),
        (b"c".to_vec(), StoredEntry::drop_at(2)),
    ];
    let m = merge_latest(vec![newer, older]);
    assert_eq!(m.len(), 3);
    assert_eq!(m[0].1.value, EntryValue::Put(Some(b"new".to_vec())));
}
