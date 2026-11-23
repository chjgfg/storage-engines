// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn light_decode_matches_full_and_rejects_the_same_bad_frames() {
    let entries = vec![
        (b"a".to_vec(), StoredEntry::put(2, Some(vec![7; 8192]))),
        (b"b".to_vec(), StoredEntry::put(3, Some(Vec::new()))),
        (b"c".to_vec(), StoredEntry::put(4, None)),
        (
            b"d".to_vec(),
            StoredEntry {
                seq: 5,
                value: EntryValue::Drop,
            },
        ),
    ];
    for with_seq in [false, true] {
        let raw = encode_block(&entries, with_seq);
        let expected: Vec<_> = decode_block(&raw, with_seq)
            .unwrap()
            .entries
            .into_iter()
            .map(|(key, value)| (key, LightEntry::of(&value)))
            .collect();
        assert_eq!(decode_light_block(&raw, with_seq).unwrap(), expected);
        for len in 0..raw.len() {
            assert!(decode_light_block(&raw[..len], with_seq).is_err());
        }
        // 重算 CRC，让非法长度或类型通过校验并进入字段解析器。
        for (at, byte) in [(9, 255), (4, 255), (0, 5)] {
            let mut bad = raw.clone();
            bad[at] = byte;
            let end = bad.len() - 4;
            let checksum = crc32(&bad[4..end]);
            bad[end..].copy_from_slice(&checksum.to_le_bytes());
            let full = decode_block(&bad, with_seq).err().map(|e| e.kind());
            let light = decode_light_block(&bad, with_seq).err().map(|e| e.kind());
            assert!(
                full.is_some(),
                "malformed fixture must fail the original parser"
            );
            assert_eq!(light, full);
        }
    }
}

/// "要不要切文件"这个判定**不许走整表**（M30）。
///
/// 触发它的测量：压实的耗时拆分台架 `compaction_cost_split` 显示 600k 行那一趟 18.0 s 里
/// **94% 在"归并+写盘"**，而"物化输入"只 0.62 s、"纯归并"只 0.48 s ⇒ 病**不在**我原先点名的
/// "整篇物化"，在 `merge_to_ssts` 每交出一行都问一次 `estimated_bytes()`，而旧实现把当前表
/// 从头求和一遍 ⇒ 每个输出文件 O(表内行数²)。这是 M19（flush 判定每插一行数一遍 memtable）
/// 同一个病的第二次出现，这次在 SST 写侧。
///
/// 尺子 = **为判定走过的条目数**，生产路径必须是 0；同一条用例里主动慢算一次，
/// 证明这把尺子确实数得到（不然上面那条断言是空转）。
#[test]
fn test_builder_size_check_does_not_walk_entries_per_add() {
    const N: usize = 4000;
    let scratch = tmp("est_hot");
    let mut b = SstBuilder::with_options(1, scratch.to_path_buf(), 64, false);
    let s0 = builder_size_walks();
    // M42：builder 不再握着整张表 ⇒ 慢口径改在调用方自己那份输入上算（对拍性质不变）
    let mut fed: Vec<(Vec<u8>, StoredEntry)> = Vec::with_capacity(N);
    for i in 0..N {
        let e: (Vec<u8>, StoredEntry) = (
            format!("k{i:05}").into_bytes(),
            StoredEntry::put(i as u64, Some(vec![b'x'; 40])),
        );
        b.add(e.0.clone(), e.1.clone());
        fed.push(e);
        // 照 `merge_to_ssts` 的形状每行问一次"够不够切一个文件"
        if b.estimated_bytes() >= usize::MAX {
            panic!("哨兵条件不该命中：这条用例只关心判定走过多少条目");
        }
    }
    let walks = builder_size_walks() - s0;
    assert_eq!(
        walks,
        0,
        "灌 {N} 行为回答「该不该切文件」走了 {walks} 个条目 ⇒ 判定又变回每行重求和一遍整表\
             （旧形状 ≈ {} 步）",
        (N * (N - 1) / 2) as u64
    );
    // 对照：主动慢算一次，探针必须数得到 —— 注意**单位**：尺子数的是"走过的条目数"，
    // 而 `sum_entries_bytes` 返回的是字节数，两者不能直接比。
    let w1 = builder_size_walks();
    let slow_bytes = sum_entries_bytes(&fed);
    let control_steps = builder_size_walks() - w1;
    assert!(
        control_steps >= N as u64,
        "探针空转：慢口径只走了 {control_steps} 个条目，对照必须数到 ≥{N}"
    );
    assert!(
        slow_bytes > 0,
        "慢口径返回 0 ⇒ 这条对照本身没建立，上面的 0 断言不算数"
    );
    assert_eq!(
        b.estimated_bytes(),
        slow_bytes,
        "增量账与慢口径不一致 ⇒ 上面那条 0 的判据也算不上数"
    );
    assert_eq!(
        b.len(),
        N,
        "`len()` 不再是「加了多少条」⇒ M42 的计数账被改动了"
    );
}

#[test]
fn point_get_projects_only_matching_rows_and_winning_value() {
    let path = tmp("point_key_interval");
    let mut builder = SstBuilder::with_options(1, &path, 128, false);
    for i in 0..100 {
        builder.add(
            format!("k{i:03}").into_bytes(),
            StoredEntry::put(1, Some(vec![i as u8; 64])),
        );
    }
    let mut sst = builder.finish().unwrap();
    let all: Vec<_> = sst.iter_all().collect();
    for resident in [false, true] {
        if resident {
            sst.entries_cache = Some(all.clone());
        }
        assert!(sst.get(b"k050").is_some()); // Warm both arms before counting.
        for old in [true, false] {
            let mode = old.then(point_read_reference::Scope::start);
            let walks = sst_entry_walks();
            let clones = entry_value_clones();
            let light = sst.get_light_at(b"k050", 1).unwrap();
            assert_eq!((light.seq, light.kind), (1, 0));
            assert_eq!(sst_entry_walks() - walks, if old { 100 } else { 1 });
            assert_eq!(
                entry_value_clones() - clones,
                u64::from(old),
                "metadata lookup must not copy payload"
            );
            let clones = entry_value_clones();
            assert_eq!(
                sst.get_at(b"k050", 1).unwrap().value,
                EntryValue::Put(Some(vec![50; 64]))
            );
            assert_eq!(
                entry_value_clones() - clones,
                1,
                "reference must not clone the winning payload a second time"
            );
            drop(mode);
        }
    }
}

#[test]
fn test_mmap() {
    let path = tmp("mmap");
    let mut b = SstBuilder::new(1, &path);
    b.add(b"x".to_vec(), StoredEntry::put(1, Some(b"y".to_vec())));
    let mut sst = b.finish().unwrap();
    sst.enable_mmap().unwrap();
    assert!(sst.has_mmap());
    assert_eq!(
        sst.get(b"x").map(|e| e.value),
        Some(EntryValue::Put(Some(b"y".to_vec())))
    );
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_stream_iter() {
    let path = tmp("iter");
    let mut b = SstBuilder::with_options(2, &path, 3, true);
    for i in 0..20u32 {
        b.add(
            format!("k{i:04}").into_bytes(),
            StoredEntry::put(i as u64, Some(format!("v{i}").into_bytes())),
        );
    }
    let sst = b.finish().unwrap();
    assert_eq!(sst.iter_all().count(), 20);
    let _ = std::fs::remove_file(&path);
}
