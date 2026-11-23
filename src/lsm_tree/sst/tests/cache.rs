// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// 命中一次的代价**不随驻留块数涨**（M23 自己引入的回归，留下的守卫）。
///
/// 旧实现每次命中都要在 `VecDeque<CacheKey>` 里 `position()` 找那个键 ⇒ O(驻留块数)。
/// 而自动档恰恰是去加大驻留块数的那一刀：40000 键那一档容量 256→1024，**未命中次数一动不动
/// （0.15/次），有序点读却从 22.2 µs/次 涨到 36.0 µs/次** —— 缓存越大每次命中越贵，省下来的
/// 未命中被自己的 LRU 吃了回去。所以这条尺子量的是"检查过几个槽位"，不看墙钟。
#[test]
fn lru_touch_cost_does_not_scale_with_residency() {
    // 返回：每次命中检查的槽位数 × 100（整数比，避免浮点）
    fn per_get(resident: usize, cap: usize) -> u64 {
        let mut c = BlockCache::new(cap);
        let blk = Arc::new(DataBlock {
            entries: Vec::new(),
        });
        for i in 0..resident {
            c.put(1, i as u64, 0, blk.clone());
        }
        assert_eq!(c.len(), resident, "必须全都驻留，否则量的就不是命中");

        let before = lru_inspections();
        for _ in 0..4 {
            for i in 0..resident {
                assert!(
                    c.get(1, i as u64, 0).is_some(),
                    "读第 {i} 块落空（不该未命中）"
                );
            }
        }
        (lru_inspections() - before) * 100 / (resident as u64 * 4)
    }

    let small = per_get(64, 128);
    let big = per_get(1024, 2048);
    eprintln!("[LRU] 每次命中检查的槽位数×100：驻留 64 → {small}，驻留 1024 → {big}");
    assert!(small > 0, "尺子在空转：一次命中都没检查过槽位");
    assert!(
        big * 100 <= small * 150 + 100,
        "驻留 64→1024（16 倍）时每次命中的代价不该跟着涨：{small} vs {big}"
    );
}

#[test]
fn packed_scan_reuses_point_blocks_and_keeps_rows_alive_after_eviction() {
    let path = tmp("packed_mixed_scan");
    let mut builder = SstBuilder::with_options(1, &path, 128, false);
    for i in 0..100 {
        builder.add(
            format!("k{i:03}").into_bytes(),
            StoredEntry::put(1, Some(vec![i as u8; 64])),
        );
    }
    let mut sst = builder.finish().unwrap();
    let mut costs = Vec::new();
    for reference in [true, false] {
        let cache = Arc::new(Mutex::new(BlockCache::new(1)));
        sst.set_block_cache(cache.clone());
        assert!(sst.get_at(b"k050", 1).is_some());
        let mode = reference.then(packed_scan_reference::Scope::start);
        let promotions = packed_promotions();
        let (rows, allocations) = crate::mem_probe::count_thread_allocations(|| {
            sst.row_iter(None, None, 1)
                .map(|(key, row)| {
                    let frozen = row.freeze(|| key.clone());
                    (key, frozen)
                })
                .collect::<Vec<_>>()
        });
        costs.push(allocations);
        assert_eq!(packed_promotions() - promotions, u64::from(reference));
        assert_eq!(rows.len(), 100);
        cache.lock().unwrap().evict_lru();
        assert!(cache.lock().unwrap().map.is_empty());
        let cold = sst.load_scan_block(0).unwrap();
        assert!(
            matches!(cold, CachedBlock::Decoded(_)),
            "cold scans must retain decoded blocks"
        );
        for (i, (key, row)) in rows.into_iter().enumerate() {
            assert_eq!(key, format!("k{i:03}").as_bytes());
            let ScanRow::Inline(row) = row else {
                panic!("ordinary row must stay inline");
            };
            assert_eq!(
                row.materialize(),
                crate::common::RowValue::Value(vec![i as u8; 64])
            );
        }
        drop(mode);
    }
    assert!(
        costs[0] >= costs[1] + 200,
        "reference must own every cached key/value again"
    );
}
