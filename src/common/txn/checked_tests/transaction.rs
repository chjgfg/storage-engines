// transaction：checked_tests 同作用域中的职责测试分片。

#[test]
fn checked_partial_storage_write_cannot_commit_and_is_rolled_back() {
    #[derive(Default)]
    struct PartialStore {
        rows: HashMap<Vec<u8>, Option<Vec<u8>>>,
    }
    impl WriteSide for PartialStore {
        fn latest_version(&mut self, _: &[u8]) -> Option<u64> {
            None
        }
        fn put_versioned(&mut self, key: &[u8], value: Option<Vec<u8>>) -> bool {
            self.rows.insert(key.to_vec(), value);
            false // A storage operation can fail after partial mutation.
        }
        fn remove_versioned(&mut self, key: &[u8]) {
            self.rows.remove(key);
        }
    }
    impl WalTail for PartialStore {
        fn try_wal_commit(&mut self, _: u64) -> std::io::Result<()> {
            Ok(())
        }
    }
    for mode in 0..3 {
        let reg = Arc::new(Mutex::new(Registry::new()));
        let tx = TxnCore::begin(
            reg,
            &AtomicU64::new(1),
            Arc::new(VacuumScheduler::default()),
        );
        let mut guard = tx.lock_registry();
        let mut store = PartialStore::default();
        let result = if mode == 2 {
            tx.try_write_owned_page(
                &mut guard,
                &mut store,
                vec![(b"k".to_vec(), Some(vec![1]))].into_iter(),
            )
        } else if mode == 1 {
            tx.try_write_owned(&mut guard, &mut store, b"k".to_vec(), Some(vec![1]))
        } else {
            tx.try_write(&mut guard, &mut store, b"k", Some(vec![1]))
        };
        assert_eq!(result, Err(WriteError::StorageWriteFailed));
        assert_eq!(
            store.rows.len(),
            1,
            "must actually mutate before reporting failure"
        );
        assert!(matches!(
            tx.try_commit(&mut guard, &mut store),
            Err(CommitError::WriteFailed(WriteError::StorageWriteFailed))
        ));
        tx.rollback(&mut guard, &mut store);
        assert!(
            store.rows.is_empty(),
            "rollback must clean a partially written row"
        );
        assert!(guard.is_empty());
    }
}
