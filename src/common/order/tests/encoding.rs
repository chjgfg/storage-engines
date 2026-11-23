// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn logical_health_preserves_binary_overlap_and_group_transitions() {
    let mut raw_keys = vec![vec![], vec![0], vec![255], b"a".to_vec(), b"b".to_vec()];
    for a in [0, 1, 97, 255] {
        for b in [0, 1, 97, 255] {
            raw_keys.push(vec![a, b]);
            raw_keys.push(vec![a, b, 0]);
            raw_keys.push(vec![a, b, 0, 0, 0, 0, 0, 0, 1]);
        }
    }
    raw_keys.sort();
    raw_keys.dedup();
    for ceiling in [65_536, u64::MAX] {
        let mut physical = Vec::new();
        let mut expected = BTreeMap::<Vec<u8>, u64>::new();
        for (i, raw) in raw_keys.iter().enumerate() {
            for version in [0, 1, 256, 65_536].into_iter().take(1 + i % 4) {
                physical.push(encode_key(raw, version));
                *expected.entry(raw.clone()).or_default() += 1;
            }
        }
        physical.sort();
        let mut health = LogicalHealth::new(ceiling);
        for enc in &physical {
            health.push(enc);
        }
        let got = health.finish(0);
        assert_eq!(got.total_rows, physical.len() as u64);
        assert_eq!(got.live_keys, expected.len() as u64);
        assert_eq!(got.max_chain, *expected.values().max().unwrap());
        assert_eq!(got, reference_health(&physical, ceiling));
    }
}

#[test]
fn binary_order_bounds_cover_prefixes_and_all_version_bytes() {
    let keys = vec![
        vec![],
        vec![0],
        vec![0, 0],
        vec![1],
        vec![97],
        vec![97, 0],
        vec![97, 0, 0, 0, 0, 0, 0, 0, 2],
        vec![255],
    ];
    for ceiling in [3, 256, 1 << 56, u64::MAX] {
        for end in &keys {
            let bound = range_end(end, ceiling, false);
            for raw in &keys {
                for version in [0, 1, ceiling] {
                    let enc = encode_key(raw, version);
                    if raw < end {
                        assert!(enc < bound, "{raw:?} < {end:?}");
                    }
                    assert!(logical_floor(&enc, ceiling) <= raw.as_slice());
                }
            }
        }
    }
    let enc = encode_key(&[97, 0, 0, 0, 0, 0, 0, 0, 2], 2);
    assert_eq!(logical_floor(&enc, 3), b"a");
}
