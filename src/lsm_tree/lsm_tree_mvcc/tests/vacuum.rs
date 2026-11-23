// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn vacuum_compaction_coalesces_real_deletion_files() {
    compare_vacuum_compaction_schedule(1024, true);
}

#[test]
fn vacuum_delete_stage_accounting_observes_real_threshold_flush() {
    measure_vacuum_delete_stages(1024, true);
}

/// vacuum 的分组阶段不该再把每行的 value 物化出来。
///
/// 量尺用「带 value 的条目物化次数」（挂在 `SstEntryIter::next` / `mem_iter_at` 上）。
/// 注意 vacuum 尾部的 flush→compact 本来就要把**存活行**读一遍重写 SST，那部分是
/// compaction 的本职、不是 vacuum 的浪费 —— 所以界按存活行数量级给（`KEYS × 8`），
/// 不按历史行数。实测 0 次（未修前是 1200 次）。
#[test]
fn test_vacuum_does_not_materialize_values() {
    const CHAIN: u64 = 300;
    const KEYS: u64 = 4;

    let path = tmp_db("vacuum_mat");
    {
        let mvcc = MVCC::open(&path);
        for i in 0..CHAIN {
            for k in 0..KEYS {
                let key = format!("k{k}");
                let tx = mvcc.begin_transaction();
                assert!(tx.set(key.as_bytes(), format!("v{i}").into_bytes()));
                tx.commit();
            }
        }
        // 落到 SST，让重量路径有机会被走到
        mvcc.flush();

        let base = crate::lsm_tree::sst::value_materializations();
        let stats = mvcc.vacuum().unwrap();
        let used = crate::lsm_tree::sst::value_materializations() - base;
        eprintln!(
            "vacuum 期间带 value 条目物化次数：{used}（裁掉 {} 行，历史行数 {}）",
            stats.versions_removed,
            CHAIN * KEYS
        );
        assert_eq!(
            stats.versions_removed,
            ((CHAIN - 1) * KEYS) as usize,
            "每 key 留一版，其余都该裁掉"
        );
        assert!(
            used <= (KEYS * 8) as u64,
            "vacuum 分组不该物化每行 value：实际 {used} 次，而历史行数是 {}",
            CHAIN * KEYS
        );
    }
    cleanup(&path);
}

#[test]
fn test_vacuum_removes_old_versions() {
    let path = tmp_db("vac");
    {
        let mvcc = MVCC::open(&path);
        let t = mvcc.begin_transaction();
        assert!(t.set(b"a", b"1".to_vec()));
        assert!(t.set(b"b", b"old".to_vec()));
        t.commit();
        let t = mvcc.begin_transaction();
        assert!(t.set(b"a", b"2".to_vec()));
        assert!(t.delete(b"b"));
        t.commit();

        let st = mvcc.vacuum().unwrap();
        assert!(
            st.versions_removed >= 2,
            "应删除 a 旧版与 b 的历史, removed={}",
            st.versions_removed
        );

        let tx = mvcc.begin_transaction();
        assert_eq!(tx.get(b"a"), Some(b"2".to_vec()));
        assert_eq!(tx.get(b"b"), None);
        tx.commit();
    }
    cleanup(&path);
}

/// blob 旁路（`blobs/`，大 value 写外面）过去**从来没人回收**：compact 只看得到自己
/// 正在合并的那几个文件，判断不了"这个键在别的层里还有更新的版本正引用同一个 blob"。
///
/// 现在由 vacuum 的全库轮次收：先裁掉旧版本行，再删没人引用的 blob 文件。
/// 断言的是**文件个数**，不是"跑完没报错"—— 这条通道原来的症状就是文件只增不减。
#[test]
fn test_vacuum_reclaims_unreferenced_blobs() {
    fn blob_count(mvcc: &MVCC) -> usize {
        let dir = mvcc.db_dir().join("blobs");
        std::fs::read_dir(dir)
            .map(|rd| rd.flatten().count())
            .unwrap_or(0)
    }

    const CHAIN: u64 = 20;
    let path = tmp_db("blob_gc");
    {
        let mvcc = MVCC::open(&path);
        // LSM 的 blob_threshold 默认 4 KiB ⇒ 8 KiB 的 value 一定走旁路
        for i in 0..CHAIN {
            let tx = mvcc.begin_transaction();
            assert!(tx.set(b"big", vec![i as u8; 8 * 1024]));
            tx.commit();
        }
        assert_eq!(
            blob_count(&mvcc),
            CHAIN as usize,
            "每版都该是一个独立的 blob 文件"
        );

        mvcc.try_checkpoint().unwrap();
        drop(mvcc);
        let mvcc = MVCC::open(&path);
        let probe = crate::common::maintenance_probe::Scope::start();

        // 有界轮次不动 blob 文件：全库收尾只属于 vacuum()
        let slice = mvcc.vacuum_bounded(None, 4).unwrap();
        assert_eq!(
            probe.stats().blob_point_reads,
            0,
            "bounded vacuum must not scan blob references"
        );
        assert!(slice.next_cursor.is_some() || slice.stats.versions_removed > 0);
        assert_eq!(
            blob_count(&mvcc),
            CHAIN as usize,
            "有界片不该去动 blob —— 引用关系只有全库才看得全"
        );

        let stats = mvcc.vacuum().unwrap();
        assert!(
            probe.stats().blob_point_reads > 0,
            "existing blob files must keep reference collection"
        );
        // 上一片可能已经裁掉过几版，所以按两轮之和核总账
        assert!(
            slice.stats.versions_removed + stats.versions_removed >= CHAIN as usize - 1,
            "版本行没被裁干净：{} + {}",
            slice.stats.versions_removed,
            stats.versions_removed
        );
        assert_eq!(
            stats.blob_rewritten,
            Some(true),
            "全库 vacuum 必须报它收了 blob"
        );
        assert_eq!(blob_count(&mvcc), 1, "只剩存活那一版引用的 blob");
        assert_eq!(
            mvcc.health().total_rows,
            1,
            "全库 vacuum 之后每把 key 只剩一行"
        );

        // 活着的 value 必须还读得出来（回收不能把可见版本的 blob 删了）
        let tx = mvcc.begin_transaction();
        assert_eq!(tx.get(b"big"), Some(vec![(CHAIN - 1) as u8; 8 * 1024]));
        tx.commit();
    }
    cleanup(&path);
}

#[test]
fn vacuum_without_blobs_does_not_point_read_survivors() {
    let path = tmp_db("vac_no_blob");
    let mvcc = MVCC::open(&path);
    mvcc.set_vacuum_auto_slice(0);
    for version in 0..2 {
        let tx = mvcc.begin_transaction();
        for i in 0..100 {
            assert!(tx.set_raw(format!("k{i:04}").as_bytes(), vec![version; 24]));
        }
        tx.commit();
    }
    mvcc.flush();
    let probe = crate::common::maintenance_probe::Scope::start();
    let stats = mvcc.vacuum().unwrap();
    assert_eq!(stats.versions_removed, 100);
    assert_eq!(stats.blob_rewritten, Some(false));
    assert_eq!(
        probe.stats().blob_point_reads,
        0,
        "no blob files means no references to collect"
    );
    {
        let _reference = crate::lsm_tree::lsm_tree::blob_reference::Scope::start();
        mvcc.vacuum().unwrap();
    }
    assert_eq!(
        probe.stats().blob_point_reads,
        100,
        "old path must still exercise every survivor lookup"
    );
    std::fs::create_dir(path.join("blobs")).unwrap();
    mvcc.vacuum().unwrap();
    let keep: Vec<_> = (0..100)
        .map(|i| encode_key(format!("k{i:04}").as_bytes(), 1))
        .collect();
    assert_eq!(mvcc.kv.lock().unwrap().gc_blobs(&keep).unwrap(), 0);
    assert_eq!(
        probe.stats().blob_point_reads,
        100,
        "empty directory and public GC must also skip reads"
    );
    let tx = mvcc.begin_transaction();
    for i in 0..100 {
        assert_eq!(tx.get(format!("k{i:04}").as_bytes()), Some(vec![1; 24]));
    }
    tx.commit();
}
