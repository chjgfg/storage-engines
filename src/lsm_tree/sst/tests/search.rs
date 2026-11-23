// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// 增量账必须与旧算法**逐字节同口径**（M30）。
///
/// 这条不是装饰：`est_bytes` 是把一句求和手搬进 `add` 里维护的，搬掉那个 `+8`（每行一条的
/// 帧头）就会让"什么时候切文件"悄悄变 —— 输出文件的数量与大小跟着变，而在压测表上那只
/// 表现为"慢了 3%"，没人会往这里查。
#[test]
fn test_builder_estimated_bytes_matches_slow_scan() {
    let scratch = tmp("est_parity");
    let mut b = SstBuilder::with_options(1, scratch.to_path_buf(), 64, false);
    assert_eq!(b.estimated_bytes(), 0, "空表两边都该是 0");
    // M42 之后 builder 不再握着整张表（攒满一块就落盘）⇒ 慢口径改在**调用方自己那份输入**上算。
    // 对拍的性质一点没变：同一个口径、同一批条目，比的还是"增量账 vs 逐条慢算"。
    let fed: Vec<(Vec<u8>, StoredEntry)> = (0..500usize)
        .map(|i| {
            (
                format!("k{i:05}").into_bytes(),
                StoredEntry::put(i as u64, Some(vec![b'v'; i % 37])),
            )
        })
        .collect();
    for (k, se) in fed.iter().cloned() {
        b.add(k, se);
    }
    assert_eq!(
        b.estimated_bytes(),
        sum_entries_bytes(&fed),
        "增量账与慢口径不一致 ⇒ 切文件的时机变了"
    );
    // 空 value 与墓碑也得同口径（那句 `+8` 是行头，不是常数 8）
    let tomb: Vec<(Vec<u8>, StoredEntry)> = vec![
        (b"a".to_vec(), StoredEntry::put(1, None)),
        (b"b".to_vec(), StoredEntry::drop_at(2)),
        (b"cccc".to_vec(), StoredEntry::put(3, Some(b"xyz".to_vec()))),
    ];
    let scratch = tmp("est_tomb");
    let mut t = SstBuilder::with_options(2, scratch.to_path_buf(), 64, false);
    for (k, se) in tomb.iter().cloned() {
        t.add(k, se);
    }
    assert_eq!(t.estimated_bytes(), sum_entries_bytes(&tomb));
}

/// `range_at` 改成用稀疏索引定位起始块之后，命中结果必须与「扫全文件再筛」逐条一致，
/// 且不能再把整个文件读一遍。
///
/// 这条对拍是改动自带的安全网：全仓原本**没有**任何针对 `range_at` 的正确性测试。
#[test]
fn test_range_at_window_equals_full_scan() {
    let path = tmp("window");
    let mut b = SstBuilder::with_options(1, &path, 3, true);
    // 40 个 key × 2 个 seq，块容量 3 条 ⇒ 十幾块；同 key 多 seq 用来验「取最大可见 seq」
    for i in 0..40u32 {
        let k = format!("k{i:03}").into_bytes();
        b.add(
            k.clone(),
            StoredEntry::put(i as u64 * 2, Some(b"v".to_vec())),
        );
        b.add(
            k.clone(),
            StoredEntry::put(i as u64 * 2 + 1, Some(b"w".to_vec())),
        );
    }
    let sst = b.finish().unwrap();

    // 全文件区间 = 暴力参照
    let all = sst.range_at(b"", &[0xffu8; 8], u64::MAX);
    assert_eq!(all.len(), 40, "每个 key 只应出一条（最大可见 seq）");
    assert!(
        all.iter()
            .all(|(_, se)| se.value == EntryValue::Put(Some(b"w".to_vec()))),
        "同 key 应取最大 seq 的那条"
    );

    let windows: Vec<(Vec<u8>, Vec<u8>)> = vec![
        (b"k007".to_vec(), b"k013".to_vec()),
        (b"k000".to_vec(), b"k000".to_vec()),
        (b"k039".to_vec(), b"k039".to_vec()),
        (b"k020".to_vec(), b"zzz".to_vec()),
        (b"k000".to_vec(), b"k039".to_vec()),
        (b"aaa".to_vec(), b"zzz".to_vec()),
        (Vec::new(), Vec::new()), // 双端皆空 ⇒ high 小于一切，区间为空
        (b"k015".to_vec(), b"k005".to_vec()), // 上下界颠倒 ⇒ 空
    ];
    for (low, high) in windows {
        let want: Vec<(Vec<u8>, StoredEntry)> = all
            .iter()
            .filter(|(k, _)| k.as_slice() >= low.as_slice() && k.as_slice() <= high.as_slice())
            .cloned()
            .collect();
        let before = block_reads();
        let got = sst.range_at(&low, &high, u64::MAX);
        let used = block_reads() - before;
        assert_eq!(
            got, want,
            "区间 [{low:?}, {high:?}] 的 range_at 与全扫筛出的不一致"
        );
        if want.is_empty() || got.len() <= 3 {
            assert!(
                used <= 6,
                "窄区间不该读全文件（[{low:?}, {high:?}] 读了 {used} 块）"
            );
        }
    }
    let _ = std::fs::remove_file(&path);
}

/// `find_back` 必须给出 `range_at` 结果里的**最后一条**（区间内最大 key），
/// 谓词不满足时要继续往更小的 key 退，并且只碰区间末尾附近的块。
#[test]
fn test_find_back_matches_range_at_tail() {
    let path = tmp("back");
    let mut b = SstBuilder::with_options(1, &path, 3, true);
    for i in 0..40u32 {
        let k = format!("k{i:03}").into_bytes();
        b.add(k.clone(), StoredEntry::put(i as u64, Some(b"w".to_vec())));
    }
    // 末尾再放一条 Drop，验证「每 key 定论后交给 accept，而不是跳过该 key 回看更旧 seq」
    b.add(b"k040".to_vec(), StoredEntry::drop_at(99));
    let sst = b.finish().unwrap();

    let low = b"k010".to_vec();
    let high = b"k025".to_vec();
    let rows = sst.range_at(&low, &high, u64::MAX);
    assert_eq!(rows.len(), 16);

    let before = block_reads();
    let hit = sst.find_back(&low, &high, u64::MAX, |_, _| true);
    let used = block_reads() - before;
    assert_eq!(
        hit.map(|(k, _)| k),
        rows.last().map(|(k, _)| k.clone()),
        "find_back 应当命中区间内最大 key"
    );
    assert!(
        used <= 3,
        "反向定位只应碰区间末尾附近的块，实际读了 {used} 块（全文件 {} 块）",
        14
    );

    // 谓词只接受 k014 ⇒ 应从 k025 一路退到 k014，返回对的 key
    let hit2 = sst.find_back(&low, &high, u64::MAX, |k, _| k == b"k014");
    assert_eq!(hit2.map(|(k, _)| k), Some(b"k014".to_vec()));

    // 区间内全是 Drop ⇒ 谓词按 value 拒绝，必须一路走到区间下界外并返回 None
    let none = sst.find_back(b"k040", b"k040", u64::MAX, |_, se| {
        !matches!(se.value, EntryValue::Drop)
    });
    assert_eq!(
        none.map(|(k, _)| k),
        None,
        "区间内无可接受的条目时应返回 None"
    );

    // 空区间 / 越界区间
    assert!(
        sst.find_back(&low, &b"k005".to_vec(), u64::MAX, |_, _| true)
            .is_none()
    );
    assert!(
        sst.find_back(b"zzz", b"zzzz", u64::MAX, |_, _| true)
            .is_none()
    );
    let _ = std::fs::remove_file(&path);
}
