// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// 探针：一次点读 / 一次点写到底读了几行盘。
///
/// 版本键 `raw_key ‖ version.to_be_bytes()` 大端有序 ⇒ 最新版本必然在链尾，
/// 定位到它就够 —— 所以**读盘次数不该随链长增长**。
/// 当前实现走 `scan_key_versions` → `range_scan` → 每行一次 `read_live`，
/// 于是链长 N 就付 N 次 pread；点写更冤：`latest_version_of` 只要一个版本号，
/// 却把 N 个 value 全读出来丢掉。
///
/// 这条断言在当前代码上**必然失败**，失败信息里的两个数字就是这个 bug 的量级证明。
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
    use crate::bitcask::storage::wal_path;
    use crate::bitcask::wal::{Wal, WalRecord};
    let path = tmp_db("bulk_wal");
    let c0 = scan_max_calls();
    {
        // 全新库的第一次 open：WAL 里没有 checkpoint ⇒ 退路必然被走一次（给下面那个 0 作证）
        let mvcc = MVCC::open(&path);
        assert!(
            scan_max_calls() > c0,
            "`scan_max_calls` 没数到全新库那次 open 的退路 ⇒ 尺子没接上，下面那个 0 不算数"
        );
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
            "[bulk 收口] bitcask WAL 剩 {} 条，Checkpoint = {:?}",
            entries.len(),
            cps
        );
        assert_eq!(
            cps.len(),
            1,
            "bulk 收尾没留下可用的 Checkpoint => 顺序又退回 append -> truncate（自己抹掉自己）"
        );
        assert_eq!(
            entries.len(),
            1,
            "截断没生效：WAL 里还剩 {} 条",
            entries.len()
        );
    }
    // 重开：起点接上了 => 那条 O(全库) 的退路必须不再被走
    let c1 = scan_max_calls();
    let mvcc = MVCC::open(&path);
    let used = scan_max_calls() - c1;
    assert_eq!(
        used, 0,
        "bulk 之后的 open 又走了 `scan_max_version` => WAL 里的起点没被用上（M48 实测 89 ms）"
    );
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

/// 每笔"单行事务"在 WAL 里到底留下多少字节、几条记录（`#[ignore]`，手动 release 跑）。
///
/// 存在的理由有两个：
/// 1. **M25 的代价数字**。交易进程的常态是"一笔事务就改一两把键"，所以 WAL 的**每条记录**
///    都要算钱：`wal_checkpoint_bytes` 那个阈值什么时候到、恢复要重放多少条，都按记录数走。
/// 2. 挑 WAL 收口阈值的人需要"多少笔事务撑满一 MiB"这种换算，而不是只有一个总字节数。
///
/// ```text
/// WAL_COST_TXNS=20000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 wal_cost_per_single_write_txn
/// ```
#[test]
#[ignore = "WAL 单价台架，手动 release 单线程跑：WAL_COST_TXNS=20000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 wal_cost_per_single_write_txn"]
fn wal_cost_per_single_write_txn() {
    let txns: u64 = std::env::var("WAL_COST_TXNS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20000);
    let path = tmp_db("wal_cost");
    cleanup(&path);
    std::fs::create_dir_all(&path).unwrap();
    let bytes;
    let counts;
    {
        let mvcc = MVCC::open(&path);
        let wal = crate::bitcask::storage::wal_path(&path);
        // 基线从**空 WAL** 起算：`checkpoint()` 会把 WAL 截断并把 `Checkpoint` 写成第一条，
        // 所以先写一笔、再 checkpoint，之后量到的字节就只剩"每笔事务的价"。
        {
            let tx = mvcc.begin_transaction();
            assert!(tx.set(b"seed", b"v0".to_vec()));
            tx.commit();
        }
        mvcc.checkpoint();
        let base = std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0);
        for i in 0..txns {
            let tx = mvcc.begin_transaction();
            assert!(tx.set(b"hot", format!("v{i}").into_bytes()));
            tx.commit();
        }
        bytes = std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0) - base;
        let mut reader = Wal::open(&wal).expect("打开 WAL 读记录");
        let records = reader.read_all().expect("读 WAL");
        let of = |kind: u8| {
            records
                .iter()
                .filter(|e| {
                    let tag = match &e.record {
                        WalRecord::Begin { .. } => 0,
                        WalRecord::Write { .. } => 1,
                        WalRecord::Commit { .. } => 2,
                        WalRecord::Abort { .. } => 3,
                        WalRecord::Checkpoint { .. } => 4,
                    };
                    tag == kind
                })
                .count()
        };
        counts = (of(0), of(1), of(2), records.len());
    }
    eprintln!(
        "[WAL 单价] {txns} 笔单行事务：{} B 合计 / {:.1} B 每笔 —— \
             Begin {} 条、Write {} 条、Commit {} 条、总 {} 条（每笔 {:.2} 条）",
        bytes,
        bytes as f64 / txns as f64,
        counts.0,
        counts.1,
        counts.2,
        counts.3,
        counts.3 as f64 / txns as f64
    );
    assert_eq!(counts.1, txns as usize, "每笔写必须留下一条 Write");
    assert_eq!(counts.2, txns as usize, "每笔写必须留下一条 Commit");
    assert_eq!(counts.0, 0, "M25 起写侧不再落 Begin");
    cleanup(&path);
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

        let wal = crate::bitcask::storage::wal_path(&path);
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
        // 这种形状照样绿（实测把 `wal_write` 改成不 append 直接返回成功，本函数一路通过）。
        // 判据必须精确到记录类型 —— 一次写要在 WAL 里留下一条 `Write`。
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
            // M25：写侧不再落 `Begin` —— 恢复要的信息全在 Write/Commit/Abort 里，
            // 而"只留一条 Begin 就崩了"的那种事务本来一行都没写。
            // ⚠️ 这条断言守的是"别再写它"；老日志**还能读**那半边由
            // `plan_is_identical_with_or_without_begin` 钉住，两边缺一条都是洞。
            let begins = records
                .iter()
                .filter(|e| matches!(&e.record, WalRecord::Begin { .. }))
                .count();
            assert_eq!(begins, 0, "写侧不再落 Begin，实际留下 {begins} 条");
        }

        let tx = mvcc.begin_transaction();
        assert_eq!(tx.get(b"k"), Some(b"w".to_vec()));
        tx.commit();
    }
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
fn test_wal_redo_after_commit_without_data_flush() {
    let path = tmp_db("redo");
    {
        cleanup(&path);
        std::fs::create_dir_all(&path).unwrap();
        // 空数据文件
        let mut tree = Bitcask::new(data_path(&path)).unwrap();
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
        // 模拟：脏写入已进数据文件，但 WAL 只有 Begin+Write 无 Commit
        let mut wal = Wal::open(wal_path(&path)).unwrap();
        let xid = 1u64;
        {
            let mut tree = Bitcask::new(data_path(&path)).unwrap();
            let enc = encode_key(b"ghost", xid);
            tree.insert(enc.clone(), Some(b"should-vanish".to_vec()))
                .unwrap();
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
