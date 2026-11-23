// 该文件集中验证对应职责，保留原测试名称和字面值。

#[test]
fn test_basic_set_get_delete() {
    let path = tmp_db("basic");
    {
        let mvcc = MVCC::open(&path);
        let tx = mvcc.begin_transaction();
        assert!(tx.set(b"a", b"1".to_vec()));
        assert_eq!(tx.get(b"a"), Some(b"1".to_vec()));
        assert!(tx.set(b"a", b"2".to_vec()));
        assert_eq!(tx.get(b"a"), Some(b"2".to_vec()));
        assert!(tx.delete(b"a"));
        assert_eq!(tx.get(b"a"), None);
        tx.commit();
    }
    cleanup(&path);
}

#[test]
fn test_print_all_latest_visible() {
    let path = tmp_db("pa");
    {
        let mvcc = MVCC::open(&path);
        let t1 = mvcc.begin_transaction();
        assert!(t1.set(b"a", b"1".to_vec()));
        assert!(t1.set(b"b", b"2".to_vec()));
        assert!(t1.set(b"a", b"3".to_vec()));
        let all = t1.print_all();
        assert_eq!(all.get(&b"a".to_vec()).unwrap().as_ref().unwrap(), b"3");
        assert_eq!(all.get(&b"b".to_vec()).unwrap().as_ref().unwrap(), b"2");
        t1.commit();
    }
    cleanup(&path);
}

#[test]
fn test_instances_isolated() {
    let p1 = tmp_db("i1");
    let p2 = tmp_db("i2");
    {
        let m1 = MVCC::open(&p1);
        let m2 = MVCC::open(&p2);
        let t1 = m1.begin_transaction();
        let t2 = m2.begin_transaction();
        assert!(t1.set(b"k", b"1".to_vec()));
        assert!(t2.set(b"k", b"2".to_vec()));
        assert_eq!(t1.get(b"k"), Some(b"1".to_vec()));
        assert_eq!(t2.get(b"k"), Some(b"2".to_vec()));
        t1.commit();
        t2.commit();
    }
    cleanup(&p1);
    cleanup(&p2);
}

/// 大 value（≥ blob_threshold 4 KiB）走 LSM 自己的 blob 旁路，SST/memtable 里存的是指针。
///
/// 这条用例先只证明一件事：**没 vacuum、没 compact，纯 set→get 也必须拿回原值**。
/// 引擎里只有 `get`/`get_at` 会解析指针，而 MVCC 读走的是 `find_version_back`/
/// `collect_latest_raw`/`range_scan`/`iter` 那一族 ⇒ 全都在往外发 9 字节的指针。
/// Bitcask / B+ 树没这个洞，因为它们的 blob 分层在 MVCC 之上（storage.rs 里
/// `decode_stored_value`），而 LSM 的分层在引擎**里面** —— 任何不走 `get` 的读都漏。
#[test]
fn test_big_value_roundtrips_through_every_read_path() {
    let path = tmp_db("blob_read");
    {
        let mvcc = MVCC::open(&path);
        let big = vec![0x5Au8; 8 * 1024];
        let tx = mvcc.begin_transaction();
        assert!(tx.set(b"big", big.clone()));
        assert!(tx.set(b"small", b"tiny".to_vec()));
        tx.commit();

        let tx = mvcc.begin_transaction();
        assert_eq!(
            tx.get(b"big"),
            Some(big.clone()),
            "点读拿到的是指针而不是值"
        );
        assert_eq!(tx.get(b"small"), Some(b"tiny".to_vec()));
        tx.commit();

        // 折叠类读法（export / scan）同一条路，也要给回值
        let exported = mvcc.export_latest_visible(false);
        let got = exported
            .iter()
            .find(|r| r.key == b"big")
            .and_then(|r| r.value.clone());
        assert_eq!(got, Some(big.clone()), "export 拿到的是指针而不是值");

        // 落一次 SST：指针解析不能只在 memtable 里成立
        mvcc.flush();
        let tx = mvcc.begin_transaction();
        assert_eq!(tx.get(b"big"), Some(big));
        tx.commit();
    }
    cleanup(&path);
}
