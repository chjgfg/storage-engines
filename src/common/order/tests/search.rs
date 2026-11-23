// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn window_bound_matches_original_floor_exhaustively() {
    let mut keys = vec![vec![]];
    let mut level = vec![vec![]];
    for _ in 0..4 {
        let mut next = Vec::new();
        for key in level {
            for byte in [0, 1, 255] {
                let mut key = key.clone();
                key.push(byte);
                next.push(key);
            }
        }
        keys.extend(next.clone());
        level = next;
    }
    for ceiling in [0, 1, 255, 256, u64::MAX] {
        for first in &keys {
            for enc in &keys {
                assert_eq!(
                    window_before(first, enc, ceiling),
                    first.as_slice() < logical_floor(enc, ceiling),
                    "first={first:?} enc={enc:?} ceiling={ceiling}"
                );
                let enc = encode_key(enc, ceiling);
                assert_eq!(
                    window_before(first, &enc, ceiling),
                    first.as_slice() < logical_floor(&enc, ceiling)
                );
            }
        }
    }
}

#[test]
fn bounded_tail_handles_interleaved_versions_and_empty_answers() {
    let mut physical = BTreeMap::new();
    let mut logical = BTreeMap::new();
    for i in 0u16..256 {
        let mut raw = vec![97];
        raw.extend_from_slice(&i.to_be_bytes());
        for version in [1, 2, 255, 256, u64::MAX] {
            physical.insert(encode_key(&raw, version), ());
        }
        logical.insert(
            raw.clone(),
            super::super::ExportRecord::new(raw, Some(vec![i as u8])),
        );
    }
    for raw in [vec![], vec![97], vec![97, 0], vec![255]] {
        for version in [0, 1, 256, u64::MAX] {
            physical.insert(encode_key(&raw, version), ());
        }
        logical.insert(
            raw.clone(),
            super::super::ExportRecord::new(raw, Some(vec![0])),
        );
    }
    let bound = physical.last_key_value().map(|(key, _)| {
        let mut b = key.clone();
        b.push(0);
        b
    });
    for n in [0, 1, 7, 300] {
        take_tail_buffer_peak();
        let rows = reverse_tail_bounded(
            n,
            bound.clone(),
            |b| {
                physical
                    .range::<[u8], _>((std::ops::Bound::Unbounded, std::ops::Bound::Excluded(b)))
                    .next_back()
                    .map(|(key, _)| key.clone())
            },
            |raw| logical.get(raw).cloned(),
        );
        let want: Vec<_> = logical.values().rev().take(n).rev().cloned().collect();
        assert_eq!(rows, want);
        assert!(take_tail_buffer_peak() <= n);
    }
    assert!(
        reverse_tail_bounded(
            7,
            bound,
            |b| physical
                .range::<[u8], _>((std::ops::Bound::Unbounded, std::ops::Bound::Excluded(b)))
                .next_back()
                .map(|(key, _)| key.clone()),
            |_| None
        )
        .is_empty()
    );
}
