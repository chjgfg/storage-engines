// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn packed_point_blocks_preserve_records_crc_and_reduce_miss_allocations() {
    let entries = vec![
        (vec![], StoredEntry::put(0, Some(vec![]))),
        (b"a".to_vec(), StoredEntry::put(9, Some(vec![3; 1024]))),
        (b"a".to_vec(), StoredEntry::put(2, None)),
        (b"a\0".to_vec(), StoredEntry::drop_at(5)),
    ];
    for seq in [false, true] {
        let raw = encode_block(&entries, seq);
        let packed = PackedBlock::decode(raw.clone(), seq).unwrap();
        let decoded = decode_block(&raw, seq).unwrap();
        for (i, (k, v)) in decoded.entries.iter().enumerate() {
            assert_eq!(packed.entry(i).0, k);
            assert_eq!(packed.entry(i).1.owned(), *v);
        }
        for len in 0..raw.len() {
            assert!(PackedBlock::decode(raw[..len].to_vec(), seq).is_err());
        }
        let mut bad = raw.clone();
        bad[8] ^= 1;
        assert!(PackedBlock::decode(bad, seq).is_err());
        let mut bad = raw.clone();
        bad[..4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(PackedBlock::decode(bad, seq).is_err());
    }
    let path = tmp("packed_point");
    let mut builder = SstBuilder::with_options(1, &path, 128, false);
    for i in 0..100 {
        builder.add(
            format!("k{i:03}").into_bytes(),
            StoredEntry::put(1, Some(vec![i as u8; 64])),
        );
    }
    let mut sst = builder.finish().unwrap();
    let mut costs = Vec::new();
    for old in [true, false] {
        let cache = Arc::new(Mutex::new(BlockCache::new(1)));
        sst.set_block_cache(cache.clone());
        let _mode = old.then(packed_point_reference::Scope::start);
        let (got, allocations) =
            crate::mem_probe::count_thread_allocations(|| sst.get_at(b"k050", 1).unwrap());
        costs.push(allocations);
        assert_eq!(got, StoredEntry::put(1, Some(vec![50; 64])));
        let block = cache
            .lock()
            .unwrap()
            .get(0, 1, sst.index[0].offset)
            .unwrap();
        assert_eq!(matches!(block, CachedBlock::Decoded(_)), old);
        assert_eq!(sst.get_light_at(b"k050", 1).unwrap().kind, 0);
        assert_eq!(
            sst.find_back(b"k000", b"k099", 1, |k, _| k == b"k049")
                .unwrap()
                .0,
            b"k049"
        );
        assert_eq!(
            sst.iter_all().count(),
            100,
            "packed cache must promote correctly for scans"
        );
        assert!(matches!(
            cache.lock().unwrap().get(0, 1, sst.index[0].offset),
            Some(CachedBlock::Decoded(_))
        ));
    }
    assert!(
        costs[0] >= costs[1] + 190,
        "old decode must allocate each key/value, not just an output row"
    );
}

/// `find_block` 改成二分之后必须同时满足两件事：
/// ① 与线性扫**逐 key 给出同一个块**（块内/块边界/两块之间/文件内外都算）；
/// ② 定位代价只能是 log 阶 —— 原先线性扫平均要走 ~块数/2 次比较，那就是点查路径上
///    唯一一处随**库内行数**变贵的纯 CPU（行数 20 万 ⇒ 每探一个块白走 ~1500 次）。
#[test]
fn test_find_block_is_binary_and_agrees_with_linear() {
    let path = tmp("find_block");
    let mut b = SstBuilder::with_options(1, &path, 8, true);
    for i in 0..2000u32 {
        b.add(
            format!("k{i:05}").into_bytes(),
            StoredEntry::put(i as u64, Some(b"v".to_vec())),
        );
    }
    let sst = b.finish().unwrap();
    let blocks = sst.index.len();
    assert!(
        blocks >= 200,
        "样本要有足够多的块才分得出线性与二分（实际 {blocks}）"
    );

    let linear = |key: &[u8]| -> Option<usize> {
        let mut best = None;
        for (i, ie) in sst.index.iter().enumerate() {
            if ie.first_key.as_slice() <= key {
                best = Some(i);
            } else {
                break;
            }
        }
        best
    };

    let mut probes: Vec<Vec<u8>> = (0..2000u32)
        .step_by(37)
        .map(|i| format!("k{i:05}").into_bytes())
        .collect();
    // 每个块的下界本身、以及块与块之间、文件外两侧
    for i in (0..blocks).step_by(11) {
        probes.push(sst.index[i].first_key.clone());
    }
    probes.push(b"k00000".to_vec());
    probes.push(b"k01999".to_vec());
    probes.push(b"aaa".to_vec());
    probes.push(vec![0xffu8; 4]);
    for k in &probes {
        assert_eq!(
            sst.find_block(k),
            linear(k),
            "find_block 与线性扫不一致: {k:?}"
        );
    }

    let log2 = usize::BITS - blocks.leading_zeros();
    let before = index_probes();
    const N: u64 = 200;
    for _ in 0..N {
        assert!(sst.find_block(b"k01999").is_some());
    }
    let per = (index_probes() - before) / N;
    eprintln!(
        "二分后每次定位比较 {per} 次（块数 {blocks}，线性参照 ≈ {} 次）",
        blocks / 2
    );
    assert!(
        per as u32 <= 2 + log2,
        "定位代价应当只与 log2(块数) 同阶（块数 {blocks}，实测 {per} 次）"
    );
    let _ = std::fs::remove_file(&path);
}
