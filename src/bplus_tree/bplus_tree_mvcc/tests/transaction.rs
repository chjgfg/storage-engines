// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
#[ignore = "same closed seed ABBA for reclamation dirty batch coalescing"]
fn reclamation_batch_same_seed_abba() {
    use crate::bplus_tree::page::reclamation_batch_reference;
    use crate::common::disk::{
        SITE_BPLUS_DBLWR, SITE_BPLUS_FREELIST, SITE_BPLUS_PAGE_FLUSH, fsync_counts,
    };
    use std::{assert, assert_eq, format, vec};
    fn key(i: usize) -> Vec<u8> {
        format!("key:{i:08}").into_bytes()
    }
    fn value(i: usize, round: usize) -> Vec<u8> {
        let mut out = vec![round as u8; 64];
        out[..8].copy_from_slice(&(i as u64).to_le_bytes());
        out
    }
    fn sidecar(path: &std::path::Path, suffix: &str) -> std::path::PathBuf {
        let mut name = path.as_os_str().to_os_string();
        name.push(suffix);
        name.into()
    }
    let rows: usize = std::env::var("BPLUS_BATCH_ROWS")
        .map(|s| s.parse().unwrap())
        .unwrap_or(12_000);
    let seed = crate::common::ScratchPath::new("bplus", "reclamation_batch_seed", ".db");
    {
        let db = MVCC::open(&seed, 64, 128);
        db.set_vacuum_auto_slice(0);
        for round in 0..2 {
            for base in (0..rows).step_by(500) {
                let tx = db.begin_transaction();
                for i in base..(base + 500).min(rows) {
                    assert!(tx.set_raw(&key(i), value(i, round)));
                }
                tx.commit();
            }
        }
        {
            let tx = db.begin_transaction();
            for i in 0..rows {
                assert_eq!(tx.get(&key(i)), Some(value(i, 1)));
            }
        }
        db.checkpoint();
    }
    let mut measured = Vec::new();
    for (arm, improved) in [false, true, true, false].into_iter().enumerate() {
        let target = crate::common::ScratchPath::new("bplus", "reclamation_batch_arm", ".db");
        for suffix in ["", ".wal", ".dblwr", ".freelist", ".blob"] {
            let source = sidecar(&seed, suffix);
            if source.exists() {
                std::fs::copy(&source, sidecar(&target, suffix)).unwrap();
            }
        }
        let db = MVCC::open(&target, 64, 128);
        db.set_vacuum_auto_slice(0);
        let reference = (!improved).then(reclamation_batch_reference::Scope::start);
        let before = fsync_counts();
        let t = std::time::Instant::now();
        let stats = db.vacuum().unwrap();
        let ms = t.elapsed().as_secs_f64() * 1000.0;
        let after = fsync_counts();
        drop(reference);
        let freelist = after[SITE_BPLUS_FREELIST] - before[SITE_BPLUS_FREELIST];
        assert_eq!(stats.versions_removed, rows);
        {
            let tx = db.begin_transaction();
            for i in 0..rows {
                assert_eq!(tx.get(&key(i)), Some(value(i, 1)));
            }
        }
        drop(db);
        let db = MVCC::open(&target, 64, 128);
        {
            let tx = db.begin_transaction();
            for i in 0..rows {
                assert_eq!(tx.get(&key(i)), Some(value(i, 1)));
            }
        }
        drop(db);
        std::eprintln!(
            "RECLAMATION_BATCH_ABBA arm={arm} improved={improved} rows={rows} ms={ms:.3} freelist_sync={freelist} page_sync={} dblwr_sync={} removed={} full_values_verified={rows} reopened_values_verified={rows}",
            after[SITE_BPLUS_PAGE_FLUSH] - before[SITE_BPLUS_PAGE_FLUSH],
            after[SITE_BPLUS_DBLWR] - before[SITE_BPLUS_DBLWR],
            stats.versions_removed
        );
        measured.push((
            improved,
            ms,
            after[SITE_BPLUS_PAGE_FLUSH] - before[SITE_BPLUS_PAGE_FLUSH],
        ));
    }
    let old: u64 = measured.iter().filter(|x| !x.0).map(|x| x.2).sum();
    let new: u64 = measured.iter().filter(|x| x.0).map(|x| x.2).sum();
    assert!(
        new < old,
        "coalescing must reduce actual tree page syncs: old={old} new={new}"
    );
}

#[test]
fn rollback_page_failure_keeps_remaining_versions_invisible() {
    let path = tmp_db("rollback_page_failure");
    let db = MVCC::open(&path, 4, 2);
    db.set_vacuum_auto_slice(0);
    let tx = db.begin_transaction();
    // 先构造回滚负载，初始化时的同步不计入回滚计量。
    db.kv.lock().unwrap().set_bulk_mode(true);
    for i in 0u32..20 {
        tx.try_set_raw(&i.to_be_bytes(), vec![7; 512]).unwrap();
    }
    db.kv.lock().unwrap().set_bulk_mode(false);
    let _fault = crate::bplus_tree::storage::dblwr_probe::Scope::fault(4);
    tx.rollback();
    assert!(
        db.kv.lock().unwrap().ensure_healthy().is_err(),
        "rollback must reach the fault"
    );
    assert!(
        db.active_txn.lock().unwrap().contains_key(&tx.version()),
        "failed rollback must keep its remaining versions invisible"
    );
    assert!(tx.try_commit().is_err());
}

#[test]
fn bulk_finish_propagates_page_sync_failure_without_sealing_wal() {
    let path = tmp_db("bulk_sync_failure");
    let db = MVCC::open(&path, 4, 16);
    let tx = db.begin_transaction();
    assert!(tx.set_raw(b"committed", b"keep".to_vec()));
    tx.try_commit().unwrap();
    drop(tx);
    let mut bulk = db.begin_bulk();
    bulk.put(b"bulk", crate::common::pack_plain(b"pending"));
    let before = std::fs::read(wal_path(&path)).unwrap();
    let _fault = crate::bplus_tree::storage::dblwr_probe::Scope::fault(5);
    assert!(bulk.try_finish().is_err());
    assert_eq!(std::fs::read(wal_path(&path)).unwrap(), before);
    assert!(db.try_checkpoint().is_err());
    assert!(db.vacuum().is_err());
    drop(db);
    assert_eq!(std::fs::read(wal_path(&path)).unwrap(), before);
    let db = MVCC::open(&path, 4, 16);
    let tx = db.begin_transaction();
    assert_eq!(tx.get(b"committed"), Some(b"keep".to_vec()));
    tx.try_commit().unwrap();
}

#[test]
fn bulk_finish_freelist_failure_does_not_seal_wal() {
    for fault in [6, 7] {
        let path = tmp_db("bulk_freelist_failure");
        let db = MVCC::open(&path, 4, 128);
        db.set_vacuum_auto_slice(0);
        let tx = db.begin_transaction();
        for i in 0..32_u32 {
            assert!(tx.set_raw(&i.to_be_bytes(), vec![1; 16]));
        }
        tx.try_commit().unwrap();
        drop(tx);
        let tx = db.begin_transaction();
        for i in 0..32_u32 {
            assert!(tx.delete(&i.to_be_bytes()));
        }
        tx.try_commit().unwrap();
        drop(tx);
        assert!(db.vacuum().unwrap().versions_removed > 0);
        db.try_checkpoint().unwrap();
        let tx = db.begin_transaction();
        assert!(tx.set_raw(b"committed", b"keep".to_vec()));
        tx.try_commit().unwrap();
        drop(tx);
        let mut bulk = db.begin_bulk();
        for i in 32..64_u32 {
            bulk.put(&i.to_be_bytes(), crate::common::pack_plain(b"bulk"));
        }
        let before = std::fs::read(wal_path(&path)).unwrap();
        let injection = crate::bplus_tree::storage::dblwr_probe::Scope::fault(fault);
        assert!(
            bulk.try_finish().is_err(),
            "must reach freelist fault {fault}"
        );
        assert!(db.try_checkpoint().is_err());
        assert!(db.vacuum().is_err());
        assert_eq!(std::fs::read(wal_path(&path)).unwrap(), before);
        drop(db);
        drop(injection);
        assert_eq!(std::fs::read(wal_path(&path)).unwrap(), before);
        // begin_bulk 在 finish 成功前不提供崩溃恢复保证；
        // 即使 WAL 还保留着，也不能据此认定
        // 部分发布的批量树在旁路文件失败后仍一定能重开。
    }
}

#[test]
fn page_failure_without_active_transaction_keeps_checkpoint_wal() {
    let path = tmp_db("checkpoint_page_failure");
    let db = MVCC::open(&path, 4, 16);
    let tx = db.begin_transaction();
    assert!(tx.set_raw(b"committed", b"keep".to_vec()));
    tx.try_commit().unwrap();
    let before = std::fs::read(wal_path(&path)).unwrap();
    let _fault = crate::bplus_tree::storage::dblwr_probe::Scope::fault(4);
    assert!(db.try_flush().is_err());
    tx.try_commit().unwrap(); // A previously committed result remains committed.
    drop(tx);
    assert!(db.try_checkpoint().is_err());
    assert_eq!(std::fs::read(wal_path(&path)).unwrap(), before);
    drop(db);
    assert_eq!(std::fs::read(wal_path(&path)).unwrap(), before);
    let db = MVCC::open(&path, 4, 16);
    let tx = db.begin_transaction();
    assert_eq!(tx.get(b"committed"), Some(b"keep".to_vec()));
    tx.try_commit().unwrap();
}

/// 探针：一次点读 / 一次点写访问了多少页。
///
/// 版本键大端有序 ⇒ 最新版本必然在链尾 ⇒ 下降到高界再往回一步就该停。
/// 修复前是从 `low` 下降后沿 `next_leaf` 正向走完整个区间：N 行版本横跨
/// N/order 片叶子，每片都要 read_page + 整节点 bincode 反序列化。
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
    use crate::bplus_tree::wal::{Wal, WalRecord};
    let path = tmp_db("bulk_wal");
    {
        let mvcc = MVCC::open(&path, 64, 0);
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
            "[bulk 收口] bplus WAL 剩 {} 条，Checkpoint = {:?}",
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
    let mvcc = MVCC::open(&path, 64, 0);
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
        let mvcc = MVCC::open(&path, 4, 16);
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
        let mvcc = MVCC::open(&path, 4, 16);
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
        let mvcc = MVCC::open(&path, 4, 8);
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
        let mvcc = MVCC::open(&path, 4, 8);
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
        use crate::bplus_tree::wal::{Wal, WalRecord};
        cleanup(&path);
        let mut tree = crate::bplus_tree::BPlusTree::open(&path, 4, 16);
        tree.flush_all();
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
        let mvcc = MVCC::open(&path, 4, 16);
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
        use crate::bplus_tree::wal::{Wal, WalRecord};
        {
            let mvcc = MVCC::open(&path, 4, 16);
            drop(mvcc);
        }
        let mut wal = Wal::open(wal_path(&path)).unwrap();
        let xid = 1u64;
        {
            let mut tree = crate::bplus_tree::BPlusTree::open(&path, 4, 16);
            let enc = encode_key(b"ghost", xid);
            tree.insert(enc.clone(), RowId(Some(b"should-vanish".to_vec())));
            tree.flush_all();
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
        let mvcc = MVCC::open(&path, 4, 16);
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
        let mvcc = MVCC::open(&path, 64, 256);
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
        let mvcc = MVCC::open(&path, 64, 256);
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
    let m1 = MVCC::try_open(&path, 8, 16).unwrap();
    let err = MVCC::try_open(&path, 8, 16).err();
    assert!(err.is_some());
    drop(m1);
    let m2 = MVCC::try_open(&path, 8, 16).unwrap();
    drop(m2);
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
        let mvcc = MVCC::open(&path, 16, 32);
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

/// bplus 写路径的每行 / 每事务各花在哪（M34 的先量，`#[ignore]`）。
///
/// 触发它的测量：三档基线 600k 档 `bplus 灌 600000 行(事务)` = **114.2 µs/行**，同一形状
/// bitcask 16.3、lsm 25.2、memory 4.3 ⇒ 贵 **7~26 倍**；而 20k→600k 只有 1.49×
/// （**线性但绝对值贵**）⇒ 不是复杂度，是每行 / 每事务的固定开销。
/// `commit_with_options` 默认不刷页（`flush_pages = false`），所以嫌疑集中在逐行 `insert`。
///
/// 五臂各自一份新库（同一批键形状：键 40 B / 值 47 B，与门面大库台架同一形状）：
/// - **A** 裸 `insert_or_shout`（不经 MVCC、不经 WAL）⇒ 纯树 + 页缓存那份账
/// - **E** 与 A 完全同形，只多一句 `set_cache_auto_max(usize::MAX)` ⇒ **A/B/C 里哪一笔是
///   "缓存顶不住 ⇒ 脏页换出（双写 + fsync）"，看 `E − A`**（页数超过顶的那些档才有差值）
/// - **B** 事务内 `set_raw` ×N **不 commit** ⇒ 再加版本键编码 + 逻辑值编码 + WAL 追加
/// - **C** 完整 begin / set×1000 / commit ⇒ 就是台架那一格
/// - **D** 空事务 begin/commit ×(N/1000) ⇒ 每事务的固定开销（WAL fsync、阈值判断）
///
/// 判读（只看差值，绝对墙钟在本机不作因果凭据）：`B − A` = MVCC/WAL 追加的每行账；
/// `C − B` = 每事务固定摊到每行的部分；`D × (N/1000)` 应当与 `C − B` 对得上，
/// 对不上就说明 commit 之外还有别的事在按行收费。
/// 每臂还带六把计数尺（解析/页读/缺页/清扫/换出/刷脏，都是"次/行"）——
/// µs 只能看趋势，**"钱在哪一层"要靠次数定案**：第一版就是靠"解析 0 次/行"才发现
/// 写侧下降当时完全绕过解析缓存，那把尺子数不到它。
///
/// ```text
/// BPLUS_WRITE_ROWS=200000 cargo test --release --lib -- --ignored --nocapture \
///     --test-threads=1 bplus_write_cost_attribution
/// ```
#[test]
#[ignore = "写路径归因，手动 release 单线程跑：BPLUS_WRITE_ROWS=200000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 bplus_write_cost_attribution"]
fn bplus_write_cost_attribution() {
    use std::time::Instant;
    let rows: usize = std::env::var("BPLUS_WRITE_ROWS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(20_000);
    fn key(i: usize) -> Vec<u8> {
        format!("{i:08}#/worldquant/datafields/x.json").into_bytes()
    }
    fn val(i: usize) -> Vec<u8> {
        format!("{{\"i\":{i},\"h\":\"{:0<28}\",\"v\":1}}", i * 7).into_bytes()
    }
    let us = |d: std::time::Duration| d.as_secs_f64() * 1e6 / rows as f64;
    let txns = (rows / 1000).max(1);
    // 计数尺：µs 在本机只能看趋势，"钱在哪一层"要靠次数定案
    let probe = || {
        (
            crate::bplus_tree::bplus_tree::node_parses(),
            crate::bplus_tree::page::page_reads(),
            crate::bplus_tree::page::cache_misses(),
            crate::bplus_tree::bplus_tree::node_cache_sweeps(),
            crate::bplus_tree::page::page_evictions(),
            crate::bplus_tree::page::batch_flushed_pages(),
        )
    };
    let mut per = Vec::new();

    // A 裸 insert（不经 MVCC / 不经 WAL）
    let dir_a = tmp_db("w_attrib_raw");
    let t_a = {
        let mvcc = MVCC::open(&dir_a, 64, 0);
        let p0 = probe();
        let t = Instant::now();
        {
            let mut kv = mvcc.kv.lock().unwrap();
            for i in 0..rows {
                insert_or_shout(
                    &mut kv,
                    encode_key(&key(i), i as u64 + 1),
                    RowId(Some(val(i))),
                    "归因 A",
                );
            }
        }
        let d = t.elapsed();
        let p1 = probe();
        per.push((
            "A 裸 insert",
            p1.0 - p0.0,
            p1.1 - p0.1,
            p1.2 - p0.2,
            p1.3 - p0.3,
            p1.4 - p0.4,
            p1.5 - p0.5,
        ));
        d
    };

    // E 与 A 同一形状，但先把页缓存的顶**抬到不设顶**（M32 那个旋钮）
    // ⇒ `E − A` = "缓存顶不住 ⇒ 脏页换出 + 双写 + fsync" 这一笔的价签
    let dir_e = tmp_db("w_attrib_raised");
    let t_e = {
        let mvcc = MVCC::open(&dir_e, 64, 0);
        let p0 = probe();
        let t = Instant::now();
        {
            let mut kv = mvcc.kv.lock().unwrap();
            kv.set_cache_auto_max(usize::MAX);
            for i in 0..rows {
                insert_or_shout(
                    &mut kv,
                    encode_key(&key(i), i as u64 + 1),
                    RowId(Some(val(i))),
                    "归因 E",
                );
            }
        }
        let d = t.elapsed();
        let p1 = probe();
        per.push((
            "E 抬顶裸 insert",
            p1.0 - p0.0,
            p1.1 - p0.1,
            p1.2 - p0.2,
            p1.3 - p0.3,
            p1.4 - p0.4,
            p1.5 - p0.5,
        ));
        d
    };

    // B 事务内写，不 commit（回滚排在计时之外）
    let dir_b = tmp_db("w_attrib_txn");
    let t_b = {
        let mvcc = MVCC::open(&dir_b, 64, 0);
        let tx = mvcc.begin_transaction();
        let p0 = probe();
        let t = Instant::now();
        for i in 0..rows {
            assert!(tx.set_raw(&key(i), val(i)), "第 {i} 行被拒收");
        }
        let d = t.elapsed();
        let p1 = probe();
        per.push((
            "B 写不提交",
            p1.0 - p0.0,
            p1.1 - p0.1,
            p1.2 - p0.2,
            p1.3 - p0.3,
            p1.4 - p0.4,
            p1.5 - p0.5,
        ));
        drop(tx);
        d
    };

    // C 完整事务（每 1000 行一事务）= 台架那一格
    let dir_c = tmp_db("w_attrib_full");
    let t_c = {
        let mvcc = MVCC::open(&dir_c, 64, 0);
        let p0 = probe();
        let t = Instant::now();
        for base in (0..rows).step_by(1000) {
            let end = (base + 1000).min(rows);
            let tx = mvcc.begin_transaction();
            for i in base..end {
                assert!(tx.set_raw(&key(i), val(i)), "第 {i} 行被拒收");
            }
            tx.commit();
        }
        let d = t.elapsed();
        let p1 = probe();
        per.push((
            "C 完整事务",
            p1.0 - p0.0,
            p1.1 - p0.1,
            p1.2 - p0.2,
            p1.3 - p0.3,
            p1.4 - p0.4,
            p1.5 - p0.5,
        ));
        d
    };

    // D 空事务：每事务的固定开销
    let dir_d = tmp_db("w_attrib_empty");
    let t_d = {
        let mvcc = MVCC::open(&dir_d, 64, 0);
        let t = Instant::now();
        for _ in 0..txns {
            let tx = mvcc.begin_transaction();
            tx.commit();
        }
        t.elapsed()
    };

    let per_txn_empty = t_d.as_secs_f64() * 1e6 / txns as f64;
    println!(
        "=== bplus 写路径归因 rows={rows} 事务数={txns}（µs/行，同一份构建）===\n  \
             A 裸 insert {a:>8.1} | E 抬顶裸 insert {e:>8.1}（E−A = {ea:>6.1} ⇒ 换出+双写+fsync 那一笔）| \
             B 事务内写不提交 {b:>8.1}（B−A = {ba:>6.1}）| \
             C 完整事务 {c:>8.1}（C−B = {cb:>6.1}）\n  \
             D 空事务 {d:>8.1} µs/事务 ⇒ 摊到每行 {dp:>6.3} µs | C 实测每事务 {cpt:>9.1} ms",
        a = us(t_a),
        e = us(t_e),
        ea = us(t_e) - us(t_a),
        b = us(t_b),
        ba = us(t_b) - us(t_a),
        c = us(t_c),
        cb = us(t_c) - us(t_b),
        d = per_txn_empty,
        dp = per_txn_empty / 1000.0,
        cpt = t_c.as_secs_f64() * 1e3 / txns as f64,
    );
    for (name, parses, reads, misses, sweeps, evictions, flushed) in &per {
        println!(
            "  {name:<16} 每行：解析 {:>6.2} 页读 {:>6.2} 缺 {:>6.2} 换出 {:>6.2} 刷脏 {:>6.2} | 解析缓存整表清扫 {} 次",
            *parses as f64 / rows as f64,
            *reads as f64 / rows as f64,
            *misses as f64 / rows as f64,
            *evictions as f64 / rows as f64,
            *flushed as f64 / rows as f64,
            sweeps,
        );
    }
    for d in [dir_a, dir_e, dir_b, dir_c, dir_d] {
        cleanup(&d);
    }
}
