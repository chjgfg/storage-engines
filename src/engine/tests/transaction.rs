// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn checked_commit_quarantines_failures_and_recovers() {
    for name in ["bitcask", "bplus_tree", "lsm_tree"] {
        for stage in [0, 1, 2] {
            let dir = tmp_dir(name);
            let db = open_engine(name, Some(dir.as_ref())).unwrap();
            db.set_vacuum_auto_slice(0);
            let tx = db.begin();
            tx.try_set_raw(b"k", b"value".to_vec()).unwrap();
            crate::common::wal::fail_next_commit(stage);
            let result = tx.try_commit();
            if stage == 0 {
                result.unwrap();
                assert_eq!(
                    tx.try_set_raw(b"late", vec![]),
                    Err(crate::WriteError::TransactionClosed)
                );
            } else {
                assert!(
                    matches!(result, Err(crate::CommitError::Indeterminate { .. })),
                    "{name}: {result:?}"
                );
                assert!(matches!(
                    tx.try_commit(),
                    Err(crate::CommitError::Indeterminate { .. })
                ));
                tx.rollback(); // Must not contradict an uncertain Commit with Abort.
            }
            drop(tx);
            let read = db.begin();
            assert_eq!(
                read.get(b"k"),
                (stage == 0).then(|| b"value".to_vec()),
                "{name} stage {stage}"
            );
            read.try_commit().unwrap();
            drop(read);
            if stage != 0 {
                assert!(db.vacuum().unwrap().skipped_active_txn);
                let next = db.begin();
                assert_eq!(
                    next.try_set_raw(b"next", vec![]),
                    Err(crate::WriteError::WalWriteFailed)
                );
                assert!(matches!(
                    next.try_commit(),
                    Err(crate::CommitError::WriteFailed(_))
                ));
                next.rollback();
            }
            // 即使显式执行 checkpoint，也必须保留结果不确定的 WAL 记录。
            if stage == 0 {
                db.checkpoint().unwrap();
            } else {
                assert!(db.checkpoint().is_err());
            }
            drop(db);
            let reopened = open_engine(name, Some(dir.as_ref())).unwrap();
            let read = reopened.begin();
            // 阶段 2 在注入同步失败前已写完整 Commit；
            // 正常进程重开可能恢复这条记录，但这不能作为断电持久性的证据。
            assert_eq!(
                read.get(b"k"),
                (stage != 1).then(|| b"value".to_vec()),
                "{name} recovered stage {stage}"
            );
            read.try_commit().unwrap();
        }
    }
}

#[test]
fn checked_checkpoint_preserves_active_transaction_log() {
    for name in ["bitcask", "bplus_tree", "lsm_tree"] {
        let dir = tmp_dir(name);
        let db = open_engine(name, Some(dir.as_ref())).unwrap();
        let tx = db.begin();
        tx.try_set_raw(b"pending", vec![1]).unwrap();
        assert!(db.checkpoint().is_err(), "{name}: must retain active WAL");
        let other = db.begin();
        other.try_set_raw(b"committed", vec![2]).unwrap();
        other.try_commit().unwrap();
        tx.rollback();
        drop(tx);
        drop(other);
        db.checkpoint().unwrap();
        drop(db);
        let reopened = open_engine(name, Some(dir.as_ref())).unwrap();
        let read = reopened.begin();
        assert_eq!(read.get(b"pending"), None, "{name}");
        assert_eq!(read.get(b"committed"), Some(vec![2]), "{name}");
    }
}

#[test]
fn checked_writes_distinguish_conflict_capacity_and_closed() {
    for entry in open_all() {
        let (name, db) = (entry.0, &*entry.1);
        let old = db.begin();
        let writer = db.begin();
        writer.try_set_raw(b"k", vec![1]).unwrap();
        writer.try_commit().unwrap();
        assert_eq!(
            old.try_set_raw(b"k", vec![2]),
            Err(crate::WriteError::Conflict)
        );
        old.rollback();
        assert_eq!(
            old.try_delete(b"k"),
            Err(crate::WriteError::TransactionClosed)
        );
        if name == "bplus_tree" {
            let tx = db.begin();
            assert_eq!(
                tx.try_set_raw(&vec![b'x'; 8192], vec![]),
                Err(crate::WriteError::RowTooLarge)
            );
            tx.try_commit().unwrap();
        }
    }
}

#[test]
fn all_engines_detect_write_conflict() {
    let engines = open_all();
    let mut dirs = Vec::new();
    for (name, db, dir) in engines {
        let t1 = db.begin();
        let t2 = db.begin();
        assert!(t1.set(b"k", b"1".to_vec()), "{name}: t1 set");
        assert!(!t2.set(b"k", b"2".to_vec()), "{name}: t2 must conflict");
        t1.commit();
        t2.rollback();
        dirs.push(dir);
    }
    cleanup(dirs);
}

/// 事务内核（M18b）的跨引擎对拍：同一段事务脚本在四家跑出**同一个结果**。
///
/// 为什么必须有它：`memory` 一家搬上共用内核时，另三家还各留一份自己的实现，
/// 而每家原有的测试只测自家那一份 —— 内核写错、或搬的时候漏了一条语义，
/// 单引擎测试全都发现不了（各家的断言本来就是对各家实现写的）。这条按"脚本 + 终态"对拍，
/// 覆盖内核的全部四条通道：
/// ①冲突拒写（且被拒的那一行**没落库**）②回滚撤销自己写过的行 ③批量写撞墙时的部分撤销
/// ④未收尾即丢弃 ⇒ 自动回滚且不漏活跃事务。终态逐字节比对，四家必须一模一样。
#[test]
fn txn_kernel_agrees_across_engines() {
    let mut finals: Vec<(&str, Vec<(Vec<u8>, Option<Vec<u8>>)>)> = Vec::new();
    let mut actives: Vec<(&str, u64)> = Vec::new();
    for entry in open_all() {
        // 整条 `entry` 不拆开：元组字段按 0→1→2 析构 ⇒ 引擎先落地、scratch 守卫后删目录。
        // 拆成 `for (name, db, dir)` 会按反向声明序走、守卫先跑 —— 而 Windows 上引擎还开着的
        // `.log`/`.lock` 那时删不掉，目录就白留在 `%TEMP%` 里（M82）。
        let (name, db) = (entry.0, &*entry.1);
        // ① 两条事务抢同一把 key：后来者拒写，且那一行不许留在库里
        let t1 = db.begin();
        let t2 = db.begin();
        assert!(t1.set(b"hot", b"v1".to_vec()), "{name}: t1 写 hot");
        assert!(
            !t2.set(b"hot", b"v2".to_vec()),
            "{name}: t2 写 hot 必须是写冲突"
        );
        // ② t2 另一把 key 写得成，回滚后要消失
        assert!(t2.set(b"mine", b"v3".to_vec()), "{name}: t2 写 mine");
        assert!(t2.get(b"mine").is_some(), "{name}: t2 自己得看得见刚写的行");
        assert!(
            t1.get(b"mine").is_none(),
            "{name}: t1 不该看见 t2 未提交的行"
        );
        t1.commit();
        t2.rollback();

        // ③ 批量写撞墙：已落的那几条要退回去（部分撤销），整笔事务随后回滚
        let t3 = db.begin();
        let t4 = db.begin();
        assert!(t3.set(b"hot", b"v5".to_vec()), "{name}: t3 抢 hot");
        let err = t4
            .batch_set(&[
                (b"first".to_vec(), b"v6".to_vec()),
                (b"hot".to_vec(), b"v7".to_vec()),
            ])
            .expect_err("{name}: batch_set 必须在 hot 上冲突");
        assert_eq!(err, b"hot".to_vec(), "{name}: 冲突键");
        assert!(
            t4.get(b"first").is_none(),
            "{name}: 部分撤销必须把 first 也退回"
        );
        t3.rollback();
        t4.rollback();

        // ④ 未收尾即丢弃 ⇒ 自动回滚，且活跃表不许漏下这一笔
        let t5 = db.begin();
        t5.set(b"leak", b"v8".to_vec());
        let leaked_version = t5.version();
        drop(t5);
        let after_drop = db.begin();
        assert!(
            after_drop.get(b"leak").is_none(),
            "{name}: 丢弃的事务必须等于没写"
        );
        after_drop.commit();
        let _ = leaked_version;

        let tx = db.begin();
        let mut rows: Vec<(Vec<u8>, Option<Vec<u8>>)> = tx
            .scan(None, None)
            .into_iter()
            .map(|r| (r.key, r.value))
            .collect();
        rows.sort();
        tx.commit();
        finals.push((name, rows));
        actives.push((name, db.health().active_txns));
    }
    let want = &finals[0];
    for got in &finals[1..] {
        assert_eq!(
            want.1, got.1,
            "终态不一致：{} vs {}（内核搬完必须逐家对得上）",
            want.0, got.0
        );
    }
    for (name, n) in actives {
        assert_eq!(
            n, 0,
            "{name}: 一轮脚本跑完不许留下活跃事务（漏网会永久停掉回收）"
        );
    }
    // 脚本本身也钉住：四家终态都是这一把 committed 的行
    assert_eq!(finals[0].1, vec![(b"hot".to_vec(), Some(b"v1".to_vec()))]);
}

#[test]
fn all_engines_bulk_load() {
    let engines = open_all();
    let mut dirs = Vec::new();
    for (name, db, dir) in engines {
        {
            let mut bulk = db.begin_bulk();
            bulk.put(b"a", b"1".to_vec());
            bulk.put_batch(&[(b"b".to_vec(), b"2".to_vec())]);
            bulk.put_batch_owned(vec![(b"c".to_vec(), b"3".to_vec())]);
            bulk.finish();
        }
        let tx = db.begin();
        assert_eq!(tx.get(b"a"), Some(b"1".to_vec()), "{name}: bulk a");
        assert_eq!(tx.get(b"b"), Some(b"2".to_vec()), "{name}: bulk b");
        assert_eq!(tx.get(b"c"), Some(b"3".to_vec()), "{name}: bulk c");
        tx.commit();
        dirs.push(dir);
    }
    cleanup(dirs);
}
