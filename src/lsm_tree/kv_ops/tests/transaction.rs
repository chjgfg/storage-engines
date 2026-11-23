// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn test_batch_and_multi_get() {
    let path = tmp("batch");
    {
        let mvcc = MVCC::open(&path);
        let tx = mvcc.begin_transaction();
        let items = vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
        ];
        tx.batch_set(&items).unwrap();
        let got = tx.multi_get(&[b"a".to_vec(), b"b".to_vec(), b"z".to_vec()]);
        assert_eq!(got[0], Some(b"1".to_vec()));
        assert_eq!(got[1], Some(b"2".to_vec()));
        assert_eq!(got[2], None);
        tx.batch_delete(&[b"b".to_vec()]).unwrap();
        assert!(!tx.exists(b"b"));
        tx.commit();
    }
    cleanup(&path);
}

#[test]
fn batch_failure_undoes_only_successful_prefix_and_survives_reopen() {
    #[cfg(feature = "lsm-allocation-reference")]
    let references = [true, false];
    #[cfg(not(feature = "lsm-allocation-reference"))]
    let references = [false];
    for _reference in references {
        #[cfg(feature = "lsm-allocation-reference")]
        let mode = _reference.then(super::super::lsm_tree::allocation_reference::Scope::start);
        let path = tmp("batch_failure_prefix");
        {
            let mvcc = MVCC::open(&path);
            let seed = mvcc.begin_transaction();
            seed.batch_set(&[
                (b"a".to_vec(), b"old".to_vec()),
                (b"hot".to_vec(), b"original".to_vec()),
            ])
            .unwrap();
            seed.commit();
            let holder = mvcc.begin_transaction();
            assert!(holder.set(b"hot", pack_plain(b"locked")));
            let tx = mvcc.begin_transaction();
            assert!(tx.batch_set(&[]).is_ok());
            assert!(tx.batch_delete(&[]).is_ok());
            assert!(tx.set(b"outside", pack_plain(b"keep")));
            let items = [
                (b"new".to_vec(), b"one".to_vec()),
                (b"new".to_vec(), b"two".to_vec()),
                (b"hot".to_vec(), b"rejected".to_vec()),
                (b"untouched".to_vec(), b"never".to_vec()),
            ];
            let lookups_before = crate::common::txn::write_registry_lookups();
            assert_eq!(tx.batch_set(&items), Err(b"hot".to_vec()));
            assert_eq!(
                crate::common::txn::write_registry_lookups() - lookups_before,
                if _reference { 2 } else { 1 },
                "the reference resolves the write registry for each successful row; the batch path resolves it once"
            );
            assert_eq!(tx.get(b"new"), None);
            assert_eq!(tx.get(b"untouched"), None);
            assert_eq!(
                tx.batch_delete(&[b"a".to_vec(), b"a".to_vec(), b"hot".to_vec()]),
                Err(b"hot".to_vec())
            );
            assert_eq!(tx.get(b"a"), Some(b"old".to_vec()));
            assert_eq!(
                tx.batch_delete(&[b"hot".to_vec(), b"outside".to_vec()]),
                Err(b"hot".to_vec())
            );
            assert_eq!(tx.get(b"outside"), Some(b"keep".to_vec()));
            // KvTxn 的失败契约要求中止整个事务；
            // 部分撤销不是可继续提交的持久化保存点。
            tx.rollback();
            holder.rollback();
        }
        {
            let mvcc = MVCC::open(&path);
            let tx = mvcc.begin_transaction();
            assert_eq!(tx.get(b"a"), Some(b"old".to_vec()));
            assert_eq!(tx.get(b"hot"), Some(b"original".to_vec()));
            assert_eq!(tx.get(b"outside"), None);
            assert_eq!(tx.get(b"new"), None);
            assert_eq!(tx.get(b"untouched"), None);
            tx.commit();
        }
        #[cfg(feature = "lsm-allocation-reference")]
        drop(mode);
    }
}

#[test]
fn batch_set_rolls_back_prefix_across_lock_pages() {
    let path = tmp("batch_set_lock_pages");
    let mvcc = MVCC::open(&path);
    let holder = mvcc.begin_transaction();
    assert!(holder.set(b"locked", pack_plain(b"held")));
    let tx = mvcc.begin_transaction();
    let items: Vec<_> = (0..BATCH_SET_PAGE_ROWS + 2)
        .map(|index| {
            if index == BATCH_SET_PAGE_ROWS {
                (b"locked".to_vec(), b"rejected".to_vec())
            } else {
                (index.to_be_bytes().to_vec(), b"value".to_vec())
            }
        })
        .collect();

    let lookups_before = crate::common::txn::write_registry_lookups();
    assert_eq!(tx.batch_set(&items), Err(b"locked".to_vec()));
    assert_eq!(
        crate::common::txn::write_registry_lookups() - lookups_before,
        2,
        "the write registry is resolved once for each attempted lock page"
    );
    assert_eq!(tx.get(&0usize.to_be_bytes()), None);
    assert_eq!(tx.get(&1023usize.to_be_bytes()), None);
    assert_eq!(tx.get(&1025usize.to_be_bytes()), None);

    tx.rollback();
    holder.rollback();
    drop(tx);
    drop(holder);
    drop(mvcc);

    let reopened = MVCC::open(&path);
    let check = reopened.begin_transaction();
    assert_eq!(check.get(&0usize.to_be_bytes()), None);
    assert_eq!(check.get(&1023usize.to_be_bytes()), None);
    assert_eq!(check.get(&1025usize.to_be_bytes()), None);
    check.commit();
    cleanup(&path);
}
