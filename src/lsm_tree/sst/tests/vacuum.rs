// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn vacuum_light_owned_blocks_match_cached_ranges_and_surface_bad_blocks() {
    let path = tmp("vacuum_owned");
    let mut builder = SstBuilder::with_options(1, &path, 3, false);
    for i in 0..12_u64 {
        builder.add(
            i.to_be_bytes().to_vec(),
            StoredEntry::put(i, Some(vec![7; 32])),
        );
    }
    let mut sst = builder.finish().unwrap();
    let cache = Arc::new(Mutex::new(BlockCache::new(2)));
    sst.set_block_cache(cache.clone());
    for (lo, hi, snap) in [(0_u64, 12_u64, u64::MAX), (4, 9, 7), (3, 3, 4)] {
        let (lo, hi) = (lo.to_be_bytes(), hi.to_be_bytes());
        let expected: Vec<_> = sst.light_iter(Some(&lo), Some(&hi), snap).collect();
        let resident = cache.lock().unwrap().len();
        let clones = entry_value_clones();
        let actual = sst
            .vacuum_light_iter(Some(&lo), Some(&hi), snap)
            .collect::<std::io::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(actual, expected);
        assert_eq!(
            entry_value_clones(),
            clones,
            "metadata scan must never clone values"
        );
        assert_eq!(
            cache.lock().unwrap().len(),
            resident,
            "maintenance cannot refill point cache"
        );
    }
    let mut raw = std::fs::read(&path).unwrap();
    raw[sst.index[1].offset as usize + 4] ^= 1;
    std::fs::write(&path, raw).unwrap();
    let mut rows = sst.vacuum_light_iter(None, None, u64::MAX);
    for _ in 0..3 {
        assert!(rows.next().unwrap().is_ok());
    }
    assert_eq!(
        rows.next().unwrap().unwrap_err().kind(),
        std::io::ErrorKind::InvalidData
    );
    assert!(rows.next().is_none());
}
