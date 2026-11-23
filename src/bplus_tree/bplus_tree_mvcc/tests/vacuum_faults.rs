// 该文件集中验证对应职责，保留原测试名称和字面值。

#[test]
fn vacuum_rejects_truncated_tree_before_blob_clear() {
    for reference in [true, false] {
        let path = tmp_db("vacuum_truncated_tree");
        {
            let db = MVCC::open(&path, 4, 2);
            db.set_vacuum_auto_slice(0);
            let tx = db.begin_transaction();
            for i in 0..32 {
                assert!(tx.set_raw(format!("k{i:04}").as_bytes(), vec![b'x'; 32]));
            }
            assert!(tx.set_raw(b"z-blob", vec![b'b'; 1024]));
            tx.try_commit().unwrap();
            db.try_flush().unwrap();
        }
        let db = MVCC::open(&path, 4, 2);
        db.set_vacuum_auto_slice(0);
        let blob_path = crate::bplus_tree::storage::blob_path(&path);
        let before = std::fs::read(&blob_path).unwrap();
        assert!(before.len() > 1024);
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(crate::bplus_tree::storage::PAGE_SIZE as u64)
            .unwrap();
        let mode =
            reference.then(crate::bplus_tree::bplus_tree::maintenance_reference::Scope::start);
        let result = db.vacuum();
        let after = std::fs::read(&blob_path).unwrap();
        eprintln!(
            "BPLUS_BAD_TREE reference={reference} result={result:?} blob_before={} blob_after={}",
            before.len(),
            after.len()
        );
        if reference {
            assert!(
                result.is_ok(),
                "the frozen old reader must expose the unsafe success"
            );
            assert_eq!(
                after.len(),
                8,
                "the old incomplete walk really authorizes blob clear"
            );
        } else {
            assert!(
                result.is_err(),
                "an unreadable tree must not authorize full-sweep GC"
            );
            assert_eq!(
                before, after,
                "partial maintenance must preserve blob bytes"
            );
            assert!(
                db.try_checkpoint().is_err(),
                "an unreadable maintenance source must not truncate recovery WAL"
            );
        }
        drop(mode);
    }
}

#[test]
fn vacuum_bad_crc_or_valid_crc_bad_node_preserves_blob_and_wal() {
    for malformed in [false, true] {
        let path = tmp_db("vacuum_bad_node");
        {
            let db = MVCC::open(&path, 4, 2);
            db.set_vacuum_auto_slice(0);
            let tx = db.begin_transaction();
            for i in 0..32 {
                assert!(tx.set_raw(format!("k{i:04}").as_bytes(), vec![b'x'; 32]));
            }
            assert!(tx.set_raw(b"z-blob", vec![b'b'; 1024]));
            tx.try_commit().unwrap();
            db.try_flush().unwrap();
        }
        let db = MVCC::open(&path, 4, 2);
        db.set_vacuum_auto_slice(0);
        let root = db.kv.lock().unwrap().root_page_id().0 as usize;
        let mut bytes = std::fs::read(&path).unwrap();
        let offset = root * crate::bplus_tree::storage::PAGE_SIZE;
        bytes[offset + crate::bplus_tree::storage::PAGE_HEADER_SIZE] = 255;
        if malformed {
            let crc = crate::common::disk::crc32(&bytes[offset + 16..offset + 4096]);
            bytes[offset + 8..offset + 12].copy_from_slice(&crc.to_le_bytes());
        }
        std::fs::write(&path, &bytes).unwrap();
        let blob_path = crate::bplus_tree::storage::blob_path(&path);
        let before_blob = std::fs::read(&blob_path).unwrap();
        let before_wal = std::fs::read(wal_path(&path)).unwrap();
        assert!(db.vacuum().is_err());
        assert!(db.try_checkpoint().is_err());
        assert_eq!(std::fs::read(&blob_path).unwrap(), before_blob);
        drop(db);
        assert_eq!(std::fs::read(wal_path(&path)).unwrap(), before_wal);
    }
}
