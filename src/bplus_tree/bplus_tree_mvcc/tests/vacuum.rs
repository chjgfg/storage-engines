// 该文件集中验证对应职责，保留原测试名称和字面值。

#[test]
fn vacuum_sync_skips_unchanged_blobs_but_persists_reclamation() {
    use crate::common::disk::{SITE_BLOB, fsync_counts};
    for reference in [true, false] {
        let path = tmp_db("vacuum_blob_sync");
        let db = MVCC::open(&path, 64, 128);
        db.set_vacuum_auto_slice(0);
        let tx = db.begin_transaction();
        assert!(tx.set_raw(b"large", vec![7; 4096]));
        assert!(tx.set_raw(b"small", vec![8; 4]));
        tx.try_commit().unwrap();
        drop(tx);
        let mode = reference.then(vacuum_sync_reference::Scope::start);
        for _ in 0..2 {
            let before = fsync_counts()[SITE_BLOB];
            let round = db.vacuum_bounded(None, 1).unwrap();
            assert!(!round.stats.blob_rewritten.unwrap());
            assert_eq!(fsync_counts()[SITE_BLOB] - before, u64::from(reference));
        }
        let tx = db.begin_transaction();
        assert_eq!(tx.get(b"large"), Some(vec![7; 4096]));
        assert!(tx.delete(b"large"));
        tx.try_commit().unwrap();
        drop(tx);
        let before = fsync_counts()[SITE_BLOB];
        let round = db.vacuum_bounded(None, u64::MAX).unwrap();
        assert!(round.stats.blob_rewritten.unwrap());
        assert_eq!(fsync_counts()[SITE_BLOB] - before, 1 + u64::from(reference));
        let before = fsync_counts()[SITE_BLOB];
        db.vacuum_bounded(None, u64::MAX).unwrap();
        assert_eq!(fsync_counts()[SITE_BLOB] - before, u64::from(reference));
        drop(mode);
        drop(db);
        let db = MVCC::open(&path, 64, 128);
        let tx = db.begin_transaction();
        assert_eq!(tx.get(b"large"), None);
        assert_eq!(tx.get(b"small"), Some(vec![8; 4]));
        tx.commit();
    }
}

/// 回归：版本组**跨块边界**时，块续跑不能把已消费的行再喂进 `group`。
///
/// `scan_entries_break_from` 只下降到 containing 叶子，然后**从该片第一条**起遍历（叶粒度定位，
/// 过滤交给调用方）。`vacuum_bounded` 续跑下界是上一块最后一行的 `enc ‖ 0x00`，而回调原先只按
/// 逻辑游标 `after` 过滤、**没按这个续跑下界过滤** ⇒ 续跑那块会把上一块尾部的行重新扫一遍。
/// 若某把键的版本组正好骑在块边界上（它最新那版恰是续跑叶子的第一条），那一版会**第二次**进
/// `group`；`vacuum_plan_group` 排序后把"同一 enc 的较早副本"划进 `last_old` 之前 ⇒ 连同活版本
/// 的 enc 一起进 `to_delete` ⇒ 判保留的最新版本被物理删掉（静默丢活键）。
///
/// 自然块 8192 行要几十万行才碰得到一次这种对齐，所以这里用 `order=4`（叶子只装得下 ~3 行）
/// + 块大小压到 4，让"组骑边界、且最新那版是续跑叶首条"在小库上**确定性**发生。
/// 判据：回收后每把键的最新版本都还得读得回来（丢任何一把 = 红）。摘掉续跑下界过滤这条就红。
#[test]
fn vacuum_chunk_resume_must_not_readjudicate_carried_group() {
    let path = tmp_db("vac_chunk_resume");
    cleanup(&path);
    // order=4 ⇒ 叶子最多 3 行；两版一把键 ⇒ 每 1.5 把键换一片叶子，叶首条常是某键的最新那版。
    let mvcc = MVCC::open(&path, 4, 0);
    let n_keys = 64usize;
    let key = |i: usize| format!("k{i:04}").into_bytes();
    // 第一遍：每把键写 v1（基线）。
    let tx = mvcc.begin_transaction();
    for i in 0..n_keys {
        assert!(
            tx.set(&key(i), format!("base{i}").into_bytes()),
            "灌基线失败"
        );
    }
    tx.commit();
    // 第二遍：每把键写 v2（更新）⇒ 每把键都有 [v1(死), v2(活)] 两版，v2 必须留下。
    let tx = mvcc.begin_transaction();
    for i in 0..n_keys {
        assert!(
            tx.set(&key(i), format!("upd{i}").into_bytes()),
            "灌更新失败"
        );
    }
    tx.commit();

    // 块大小压到 4：让版本组骑在块边界上（自然块 8192 在小库上根本不分块 ⇒ 复现不到）。
    set_vac_chunk_override(4);
    let _guard = VacChunkGuard;
    mvcc.vacuum().expect("vacuum 失败");
    drop(_guard);

    // 每把键的最新版本（upd{i}）都得读得回来；丢任何一把 = 续跑重复判定把活版本删了。
    let mut lost = Vec::new();
    for i in 0..n_keys {
        let tx = mvcc.begin_transaction();
        let got = tx.get(&key(i));
        tx.commit();
        match got {
            Some(v) if v == format!("upd{i}").into_bytes() => {}
            other => lost.push((i, other.map(|v| String::from_utf8_lossy(&v).to_string()))),
        }
    }
    drop(mvcc);
    cleanup(&path);
    assert!(
        lost.is_empty(),
        "块续跑重复判定过删了活版本：丢 {} 把，首丢 key={:?} 读到={:?}",
        lost.len(),
        lost.first().map(|(i, _)| *i),
        lost.first().map(|(_, v)| v.clone())
    );
}

/// vacuum 期间不该发生任何「把某行的 value 克隆出来」—— 它只需要版本号，
/// 加上「是不是墓碑 / 是不是 blob 指针」两个标志位，而这三样按引用就能读到。
///
/// 量尺选的是 value 克隆次数而不是页读取次数：B+ 树没有独立键索引，走一遍全库的
/// 页读取本来就省不掉，用页计数会量不到这次改动的任何东西。
#[test]
fn test_vacuum_does_not_clone_values() {
    const CHAIN: u64 = 300;
    const KEYS: u64 = 4;

    let path = tmp_db("vacuum_clone");
    {
        let mvcc = MVCC::open(&path, 4, 16);
        for i in 0..CHAIN {
            for k in 0..KEYS {
                let key = format!("k{k}");
                let tx = mvcc.begin_transaction();
                assert!(tx.set(key.as_bytes(), format!("v{i}").into_bytes()));
                tx.commit();
            }
        }

        let base = crate::bplus_tree::page::value_clones();
        let stats = mvcc.vacuum().unwrap();
        let used = crate::bplus_tree::page::value_clones() - base;
        eprintln!(
            "vacuum 期间 value 克隆次数：{used}（裁掉 {} 行，历史行数 {}）",
            stats.versions_removed,
            CHAIN * KEYS
        );
        assert_eq!(
            stats.versions_removed,
            ((CHAIN - 1) * KEYS) as usize,
            "每 key 留一版，其余都该裁掉"
        );
        assert_eq!(used, 0, "vacuum 只需版本号与两个标志位，不该克隆任何 value");
    }
    cleanup(&path);
}

#[test]
fn test_vacuum_removes_old_versions_and_rewrites_blob() {
    let path = tmp_db("vac");
    let big = vec![9u8; 1000];
    {
        let mvcc = MVCC::open(&path, 16, 64);
        let t = mvcc.begin_transaction();
        assert!(t.set(b"a", b"1".to_vec()));
        assert!(t.set(b"b", b"old".to_vec()));
        t.commit();
        let t = mvcc.begin_transaction();
        assert!(t.set(b"a", b"2".to_vec()));
        assert!(t.delete(b"b"));
        assert!(t.set(b"big", big.clone()));
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
        assert_eq!(tx.get(b"big"), Some(big.clone()));
        tx.commit();
    }
    {
        let mvcc = MVCC::open(&path, 16, 64);
        let tx = mvcc.begin_transaction();
        assert_eq!(tx.get(b"a"), Some(b"2".to_vec()));
        assert_eq!(tx.get(b"big"), Some(big));
        tx.commit();

        // 删除唯一大 value 后再 vacuum → blob 应被清空
        let t = mvcc.begin_transaction();
        assert!(t.delete(b"big"));
        t.commit();
        let st = mvcc.vacuum().unwrap();
        assert!(st.versions_removed >= 1);
        // blob 无引用时应收缩
        let blob = crate::bplus_tree::storage::blob_path(&path);
        if blob.exists() {
            let len = std::fs::metadata(&blob).unwrap().len();
            assert!(len <= 16, "无 blob 引用时文件应接近仅魔数, len={len}");
        }
    }
    cleanup(&path);
}
