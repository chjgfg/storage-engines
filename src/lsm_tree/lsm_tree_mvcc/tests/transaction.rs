// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn conflict_key_lookup_does_not_copy_values_and_keeps_visibility() {
    use crate::lsm_tree::sst::{entry_value_clones, point_read_reference};
    let path = tmp_db("conflict_metadata");
    let mv = MVCC::open(&path);
    mv.set_vacuum_auto_slice(0);
    let tx = mv.begin_transaction();
    assert!(tx.set_raw(b"k", vec![7; 256]));
    tx.commit();
    mv.flush();
    // 预热真实 SST 读取缓存；解码分配不等同于载荷复制。
    let reader = mv.begin_transaction();
    assert_eq!(reader.get(b"k"), Some(vec![7; 256]));
    reader.commit();
    for old in [true, false] {
        let mode = old.then(point_read_reference::Scope::start);
        let tx = mv.begin_transaction();
        let clones = entry_value_clones();
        assert!(tx.set_raw(b"k", vec![8; 256]));
        let used = entry_value_clones() - clones;
        if old {
            assert!(used > 0, "old conflict gate is the negative control");
        } else {
            assert_eq!(used, 0, "conflict detection only needs keys/kinds");
        }
        tx.rollback();
        drop(mode);
    }
    let old_reader = mv.begin_transaction();
    let newer = mv.begin_transaction();
    assert!(newer.set_raw(b"k", b"new".to_vec()));
    assert!(!old_reader.set_raw(b"k", b"conflict".to_vec()));
    assert_eq!(old_reader.get(b"k"), Some(vec![7; 256]));
    newer.rollback();
    old_reader.rollback();
    drop(mv);
    let reopened = MVCC::open(&path);
    let tx = reopened.begin_transaction();
    assert_eq!(tx.get(b"k"), Some(vec![7; 256]));
    tx.commit();
}

/// 探针：一次点读 / 一次点写访问了多少个 SST 数据块。
///
/// 旧实现是 `range_scan(encode(key,0), encode(key,MAX))` 跨 memtable 与每个重叠 SST，
/// 而 `SstFile::range_at` 又是从文件**第 0 块**开始逐条 `continue` 到 low ——
/// 于是 LSM 的代价不是随链长涨，而是随**库总量**涨。这正是同一份数据在 LSM 上一天
/// 就顶不住、换到 Bitcask 能撑一个多月的差别来源。
/// M52：`BulkLoader` 收尾必须给 WAL 留下**一条还能用的 Checkpoint**。
///
/// 洞（M48 那 89 ms 的真因）：`finish_inner` 原来是 `append(Checkpoint) -> sync -> truncate()`，
/// 而 `truncate()` 只留 header —— **刚写的那条记录被自己抹掉**。下一次 open 因此拿不到起点，
/// 只能退回 O(全库 KeyDir) 的 `scan_max_version`（bitcask 600k 档实测多花 89 ms）。
/// 正确的收口顺序在 `seal_wal_checkpoint` 里本来就有一份（先截断再写回）——
/// 这次把抄写的几处全部接回那一份：bitcask / bplus / lsm 的 bulk 收尾，加 bplus 的 `Drop`。
///
/// 判据不看墙钟：收口之后**直接读 WAL** —— 应当恰好只剩一条 `Checkpoint`；
/// 再重开一趟，数据必须一行不少（起点拿小了真正危险的是版本号复用，不只是慢）。
#[test]
fn test_bulk_finish_leaves_a_usable_wal_checkpoint() {
    use crate::lsm_tree::storage::wal_path;
    use crate::lsm_tree::wal::{Wal, WalRecord};
    let path = tmp_db("bulk_wal");
    {
        let mvcc = MVCC::open(&path);
        let tx = mvcc.begin_transaction();
        for i in 0..200u64 {
            assert!(
                tx.set(format!("k{i:06}").as_bytes(), b"v".to_vec()),
                "灌一把键失败"
            );
        }
        tx.commit();
        let mut bulk = mvcc.begin_bulk();
        for i in 200..260u64 {
            bulk.put(format!("k{i:06}").as_bytes(), b"w".to_vec());
        }
        bulk.finish();
        let mut wal = Wal::open(wal_path(&path)).unwrap();
        let entries = wal.read_all().unwrap();
        let cps: Vec<u64> = entries
            .iter()
            .filter_map(|e| match &e.record {
                WalRecord::Checkpoint { next_version, .. } => Some(*next_version),
                _ => None,
            })
            .collect();
        eprintln!(
            "[bulk 收口] lsm WAL 剩 {} 条，Checkpoint = {:?}",
            entries.len(),
            cps
        );
        assert_eq!(
            cps.len(),
            1,
            "bulk 收尾没留下可用的 Checkpoint => 又退回 append -> truncate（自己抹掉自己）"
        );
        assert_eq!(
            entries.len(),
            1,
            "截断没生效：WAL 里还剩 {} 条",
            entries.len()
        );
    }
    let mvcc = MVCC::open(&path);
    let tx = mvcc.begin_transaction();
    assert_eq!(
        tx.key_count(b"k"),
        260,
        "重开之后行数不对 => 收口把数据弄坏了"
    );
    tx.commit();
    drop(mvcc);
    cleanup(&path);
}

#[test]
fn test_write_write_conflict() {
    let path = tmp_db("ww");
    {
        let mvcc = MVCC::open(&path);
        let t1 = mvcc.begin_transaction();
        let t2 = mvcc.begin_transaction();
        assert!(t1.set(b"k", b"v1".to_vec()));
        assert!(!t2.set(b"k", b"v2".to_vec()));
        t1.commit();
        t2.rollback();
    }
    cleanup(&path);
}

#[test]
fn test_rollback_discards_writes() {
    let path = tmp_db("rb");
    {
        let mvcc = MVCC::open(&path);
        let t1 = mvcc.begin_transaction();
        assert!(t1.set(b"x", b"1".to_vec()));
        t1.rollback();
        let t2 = mvcc.begin_transaction();
        assert_eq!(t2.get(b"x"), None);
        t2.commit();
    }
    cleanup(&path);
}

#[test]
fn test_self_write_visible() {
    let path = tmp_db("self");
    {
        let mvcc = MVCC::open(&path);
        let tx = mvcc.begin_transaction();
        assert!(tx.set(b"k", b"v".to_vec()));
        assert_eq!(tx.get(b"k"), Some(b"v".to_vec()));
        tx.commit();
    }
    cleanup(&path);
}

#[test]
fn test_conflict_after_other_commits() {
    let path = tmp_db("cmt");
    {
        let mvcc = MVCC::open(&path);
        let t1 = mvcc.begin_transaction();
        let t2 = mvcc.begin_transaction();
        assert!(t1.set(b"k", b"1".to_vec()));
        t1.commit();
        assert!(!t2.set(b"k", b"2".to_vec()));
        t2.rollback();
        let t3 = mvcc.begin_transaction();
        assert_eq!(t3.get(b"k"), Some(b"1".to_vec()));
        t3.commit();
    }
    cleanup(&path);
}

#[test]
fn test_wal_redo_after_commit_without_page_flush() {
    let path = tmp_db("redo");
    {
        cleanup(&path);
        std::fs::create_dir_all(&path).unwrap();
        // 空快照
        let mut tree = LSMTree::open(&path).unwrap();
        tree.flush().unwrap();
        drop(tree);

        let mut wal = Wal::open(wal_path(&path)).unwrap();
        let xid = 1u64;
        let enc = encode_key(b"recover", xid);
        // 老日志的形状：M25 起写侧不再落 `Begin`，这里手写一份**带 Begin** 的日志，
        // 恢复必须照样是对的那份（读侧兼容由 `plan_is_identical_with_or_without_begin` 钉死）
        wal.append(WalRecord::Begin { xid }).unwrap();
        wal.append(WalRecord::Write {
            xid,
            key: enc,
            value: Some(b"me".to_vec()),
        })
        .unwrap();
        wal.append(WalRecord::Commit { xid }).unwrap();
        wal.sync().unwrap();
    }
    {
        let mvcc = MVCC::open(&path);
        let tx = mvcc.begin_transaction();
        assert_eq!(tx.get(b"recover"), Some(b"me".to_vec()));
        tx.commit();
    }
    cleanup(&path);
}

#[test]
fn test_wal_undo_uncommitted() {
    let path = tmp_db("undo");
    {
        {
            let mvcc = MVCC::open(&path);
            drop(mvcc);
        }
        // 模拟：脏写入已进快照，但 WAL 只有 Begin+Write 无 Commit
        let mut wal = Wal::open(wal_path(&path)).unwrap();
        let xid = 1u64;
        {
            let mut tree = LSMTree::open(&path).unwrap();
            let enc = encode_key(b"ghost", xid);
            tree.insert(enc.clone(), Some(b"should-vanish".to_vec()));
            tree.flush().unwrap();
            // 同上：带 `Begin` 的老日志形状
            wal.append(WalRecord::Begin { xid }).unwrap();
            wal.append(WalRecord::Write {
                xid,
                key: enc,
                value: Some(b"should-vanish".to_vec()),
            })
            .unwrap();
            wal.sync().unwrap();
        }
    }
    {
        let mvcc = MVCC::open(&path);
        let tx = mvcc.begin_transaction();
        assert_eq!(tx.get(b"ghost"), None);
        tx.commit();
    }
    cleanup(&path);
}

#[test]
fn test_bulk_load_and_reopen() {
    let path = tmp_db("bulk");
    {
        let mvcc = MVCC::open(&path);
        let mut bulk = mvcc.begin_bulk();
        for i in 0..1000u32 {
            let k = format!("k{i:06}").into_bytes();
            let v = format!("v{i}").into_bytes();
            bulk.put(&k, v);
        }
        assert_eq!(bulk.get(b"k000042").as_deref(), Some(b"v42".as_slice()));
        bulk.finish();
    }
    {
        let mvcc = MVCC::open(&path);
        let tx = mvcc.begin_transaction();
        assert_eq!(tx.get(b"k000042"), Some(b"v42".to_vec()));
        assert_eq!(tx.get(b"k000999"), Some(b"v999".to_vec()));
        tx.commit();
    }
    cleanup(&path);
}

#[test]
fn test_try_open_lock_conflict() {
    let path = tmp_db("lock2");
    let m1 = MVCC::try_open(&path).unwrap();
    let err = MVCC::try_open(&path).err();
    assert!(err.is_some());
    drop(m1);
    let m2 = MVCC::try_open(&path).unwrap();
    drop(m2);
    cleanup(&path);
}

#[test]
fn blob_gc_rechecks_after_writes_and_rejects_directory_errors() {
    let path = tmp_db("blob_recheck");
    let mvcc = MVCC::open(&path);
    mvcc.set_vacuum_auto_slice(0);
    assert_eq!(mvcc.vacuum().unwrap().blob_rewritten, Some(false));
    for version in 0..2 {
        let tx = mvcc.begin_transaction();
        assert!(tx.set_raw(b"small", vec![version; 24]));
        tx.commit();
    }
    // 用普通文件占据目录路径，在两个受支持的平台上
    // 都能确定性触发 read_dir 失败；管理员权限下的权限位不可靠。
    std::fs::write(path.join("blobs"), b"not a directory").unwrap();
    assert!(mvcc.vacuum().is_err());
    assert_eq!(
        mvcc.health().total_rows,
        2,
        "probe failure must precede deletion"
    );
    assert!(mvcc.kv.lock().unwrap().gc_blobs(&[]).is_err());
    let slice = mvcc.vacuum_bounded(None, 1).unwrap();
    assert_eq!(slice.stats.blob_rewritten, None);
    std::fs::remove_file(path.join("blobs")).unwrap();

    let tx = mvcc.begin_transaction();
    assert!(tx.set_raw(b"big", vec![7; 8192]));
    tx.commit();
    std::fs::write(
        path.join("blobs").join("18446744073709551615.blob"),
        b"orphan",
    )
    .unwrap();
    let probe = crate::common::maintenance_probe::Scope::start();
    assert_eq!(mvcc.vacuum().unwrap().blob_rewritten, Some(true));
    assert_eq!(
        probe.stats().blob_point_reads,
        2,
        "previously empty does not mean always empty"
    );
    assert_eq!(std::fs::read_dir(path.join("blobs")).unwrap().count(), 1);
    let tx = mvcc.begin_transaction();
    assert_eq!(tx.get(b"big"), Some(vec![7; 8192]));
    assert_eq!(tx.get(b"small"), Some(vec![1; 24]));
    tx.commit();
}

/// 只读事务不许往 WAL 落一个字节，也不许付 fsync 的代价。
///
/// 反向那半同样重要：**写过就必须有 `Commit` 记录 + fsync** —— 不然这条优化会悄悄
/// 把"提交的持久化点"一起省掉，那比慢一千倍严重。
#[test]
fn test_read_only_transaction_touches_no_wal() {
    const READS: u64 = 200;

    let path = tmp_db("ro_wal");
    {
        let mvcc = MVCC::open(&path);
        let tx = mvcc.begin_transaction();
        assert!(tx.set(b"k", b"v".to_vec()));
        tx.commit();

        let wal = wal_path(&path);
        let size0 = std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0);
        for _ in 0..READS {
            let tx = mvcc.begin_transaction();
            assert_eq!(tx.get(b"k"), Some(b"v".to_vec()));
            tx.commit();
        }
        let size1 = std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0);
        assert_eq!(
            size1,
            size0,
            "{READS} 次只读事务往 WAL 落了 {} 字节",
            size1 - size0
        );

        // 写过的事务照旧进 WAL
        let tx = mvcc.begin_transaction();
        assert!(tx.set(b"k", b"w".to_vec()));
        tx.commit();
        let size2 = std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0);
        assert!(
            size2 > size1,
            "写入必须留下 Write/Commit 记录，持久化点不能被省掉"
        );

        // 字节数当尺子**不够**：只落一条 `Commit` 也能让体积涨，于是"`Write` 记录整个没写"
        // 这种形状照样绿（M18d 实测：把 `wal_write` 改成不 append 直接返回成功，bitcask 那条
        // 一路通过）。判据必须精确到记录类型 —— 一次写要在 WAL 里留下一条 `Write`。
        {
            let mut reader = Wal::open(&wal).expect("打开 WAL 读记录");
            let records = reader.read_all().expect("读 WAL");
            let writes = records
                .iter()
                .filter(|e| matches!(&e.record, WalRecord::Write { .. }))
                .count();
            assert_eq!(
                writes, 2,
                "两次真写要在 WAL 里各留一条 Write，实际 {writes} 条 \
                     （只数 Begin/Commit 的尺子抓不到丢了的 Write）"
            );
            // M25：写侧不再落 `Begin`（恢复不读它）。老日志**还能读**那半边由
            // `plan_is_identical_with_or_without_begin` 钉住，两边缺一条都是洞。
            let begins = records
                .iter()
                .filter(|e| matches!(&e.record, WalRecord::Begin { .. }))
                .count();
            assert_eq!(begins, 0, "写侧不再落 Begin，实际留下 {begins} 条");
        }

        // 只读事务也不能把数据读坏（版本号照旧推进，快照语义不变）
        let tx = mvcc.begin_transaction();
        assert_eq!(tx.get(b"k"), Some(b"w".to_vec()));
        tx.commit();
    }
    cleanup(&path);
}
