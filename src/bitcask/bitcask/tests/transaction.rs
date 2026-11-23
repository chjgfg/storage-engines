// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// M60：批量成树与逐条施加**必须给出同一把索引**（含重复键"后到者胜"、软墓碑、以及硬删除的退回支）。
///
/// 为什么单独钉这一条：`BTreeMap` 的有序批量填充走的是"排序 + 去重"那条内部路，
/// 而 `apply_log_record` 是"后写覆盖先写 + 硬删就 remove"。两条路等价**不是显然的**，
/// 而 merge 每次都要靠它 ⇒ 一旦不等价，症状是"某把键在 merge 之后静默换成旧版本"。
/// 对照对象选 `load_index`（同一条日志、逐条施加的那扇门）⇒ 两把索引逐键逐字节比。
///
/// ⚠️ 退回那一支（扫到硬删除）也得有形状能跑到它 —— 不然它就是死代码，下次改坏没人知道。
#[test]
fn bulk_sorted_rebuild_agrees_with_the_insert_rebuild() {
    // ① 干净形状：升序、中间夹一把重复键（后到者胜）与一把软墓碑
    let path = tmp_log("bulk_agree");
    {
        let mut log = Log::new(path.to_path_buf()).unwrap();
        for i in 0..600u32 {
            let k = format!("k{i:04}");
            log.write_entry(k.as_bytes(), Some(b"first")).unwrap();
            if i % 5 == 0 {
                // 同一把键再写一版：批量那一侧必须认"后到者胜"
                log.write_entry(k.as_bytes(), Some(b"second-longer"))
                    .unwrap();
            }
            if i % 11 == 0 {
                log.write_soft_tombstone(k.as_bytes()).unwrap();
            }
        }
        log.flush_buf().unwrap();
        let by_bulk = log.load_keydir_sorted(0).unwrap();
        let by_insert = log.load_index(0).unwrap();
        assert_eq!(
            by_bulk, by_insert,
            "同一份升序日志：批量成树与逐条施加给出的索引不一样 ⇒ 后到者胜/软墓碑有一条对不上"
        );
        assert!(
            by_bulk.contains_key(&b"k0000"[..]),
            "对照对象本身是空的 ⇒ 这份台架什么都没比"
        );
        assert_eq!(merge_bulk_fallbacks(), 0, "干净形状不该退回逐条施加");
    }
    cleanup(&path);

    // ② 带硬删除的形状：必须整份退回逐条施加，并且结果与逐条那扇门一致
    let path2 = tmp_log("bulk_fallback");
    {
        let mut log = Log::new(path2.to_path_buf()).unwrap();
        for i in 0..200u32 {
            let k = format!("k{i:04}");
            log.write_entry(k.as_bytes(), Some(b"v")).unwrap();
        }
        log.write_hard_delete(b"k00100").unwrap();
        log.flush_buf().unwrap();
        let by_bulk = log.load_keydir_sorted(0).unwrap();
        let by_insert = log.load_index(0).unwrap();
        assert_eq!(
            by_bulk, by_insert,
            "扫到硬删除之后，退回那一支给出的索引与逐条施加不一样 ⇒ 那一步丢掉了一次删除"
        );
        assert!(
            !by_bulk.contains_key(&b"k00100"[..]),
            "硬删过的那把键还在索引里 ⇒ 退回时丢了 remove"
        );
        assert_eq!(
            merge_bulk_fallbacks(),
            1,
            "有硬删除却没退回逐条施加 ⇒ 批量那一侧的升序前提被静默违反了"
        );
    }
    cleanup(&path2);
}

/// 写缓冲必须是**懒分配**：打开一个空库不该先占 4 MiB。
///
/// 大库基线台架量到 `open 空库` 就"常驻 8.4 MB"，来源正是数据日志缓冲与 blob 缓冲各
/// 一次 `Vec::with_capacity(4 MiB)` —— 库里一个键都没有也要先占 8 MiB（下游那台单核机器
/// 同时只跑得起几百 MB 预算）。改成"首次写入才一步到位"之后：
/// ① 打开时容量为 0；② 第一次写入就涨满，所以"攒够 4 MiB 才落盘"的 syscall 优化不变。
#[test]
fn test_write_buffer_is_allocated_lazily() {
    let path = tmp_log("lazy_buf");
    {
        let mut log = Log::new(path.to_path_buf()).unwrap();
        assert_eq!(log.buf.capacity(), 0, "打开空库不该预留写缓冲");
        log.append_entry(b"k", Some(b"v"), false).unwrap();
        assert!(
            log.buf.capacity() >= WRITE_BUF_TARGET,
            "首次写入后应一步涨到目标大小（实际 {}）",
            log.buf.capacity()
        );
    }
    cleanup(&path);

    // blob 旁路同一个规矩（这里的 4 MiB 与 storage.rs 的 BLOB_BUF_TARGET 同值）
    let dir = tmp_log("lazy_blob_dir");
    {
        std::fs::create_dir_all(&dir).unwrap();
        let mut blob = crate::bitcask::storage::BlobStore::open(&dir).unwrap();
        assert_eq!(
            blob.buf_capacity_for_test(),
            0,
            "打开空库不该预留 blob 缓冲"
        );
        blob.append(b"payload").unwrap();
        assert!(
            blob.buf_capacity_for_test() >= 4 * 1024 * 1024,
            "首次写入后应一步涨到目标大小（实际 {}）",
            blob.buf_capacity_for_test()
        );
    }
    cleanup(&dir);
}
