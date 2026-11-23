// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn test_value_meta_roundtrip() {
    let p = pack_plain(b"hi");
    match decode_value_meta(&p) {
        ValueMeta::Plain { user } => assert_eq!(user, b"hi"),
        _ => panic!(),
    }
    let t = pack_ttl(b"x", 1000);
    match decode_value_meta(&t) {
        ValueMeta::Ttl {
            expire_unix_secs,
            user,
        } => {
            assert_eq!(expire_unix_secs, 1000);
            assert_eq!(user, b"x");
        }
        _ => panic!(),
    }
    assert_eq!(
        decode_value_meta(b"raw"),
        ValueMeta::Plain {
            user: b"raw".to_vec()
        }
    );
}

#[test]
fn test_incr_and_incr_with_ttl() {
    let path = tmp("incr");
    {
        let mvcc = MVCC::open(&path, 16, 32);
        let tx = mvcc.begin_transaction();

        // 不存在 → 从 0 起
        assert_eq!(tx.incr(b"cnt", 1).unwrap(), 1);
        assert_eq!(tx.incr(b"cnt", 5).unwrap(), 6);
        assert_eq!(tx.decr(b"cnt", 2).unwrap(), 4);
        assert_eq!(tx.get(b"cnt"), Some(b"4".to_vec()));

        // 非整数
        assert!(tx.set(b"s", pack_plain(b"abc")));
        assert_eq!(tx.incr(b"s", 1), Err(IncrError::NotInteger));

        // 带 TTL 计数器
        assert_eq!(
            tx.incr_with_ttl(b"hits", 1, Duration::from_secs(90))
                .unwrap(),
            1
        );
        assert_eq!(
            tx.incr_with_ttl(b"hits", 2, Duration::from_secs(90))
                .unwrap(),
            3
        );
        assert_eq!(tx.get(b"hits"), Some(b"3".to_vec()));
        match tx.get_ttl(b"hits") {
            Some(Some(d)) => assert!(d.as_secs() > 0 && d.as_secs() <= 90),
            other => panic!("hits should have ttl, got {other:?}"),
        }

        // 过期 key 再 incr 视为 0
        let expired = pack_ttl(b"10", now_unix_secs().saturating_sub(5));
        assert!(tx.set(b"oldc", expired));
        assert_eq!(tx.get(b"oldc"), None);
        assert_eq!(tx.incr(b"oldc", 1).unwrap(), 1);

        tx.commit();
    }
    // 重启仍在
    {
        let mvcc = MVCC::open(&path, 16, 32);
        let tx = mvcc.begin_transaction();
        assert_eq!(tx.get(b"cnt"), Some(b"4".to_vec()));
        assert_eq!(tx.incr(b"cnt", 1).unwrap(), 5);
        tx.commit();
    }
    cleanup(&path);
}

#[test]
fn test_ttl_and_purge() {
    let path = tmp("ttl");
    {
        let mvcc = MVCC::open(&path, 16, 32);
        let tx = mvcc.begin_transaction();
        assert!(tx.set_with_ttl(b"temp", b"v".to_vec(), Duration::from_secs(60)));
        assert!(tx.set(b"forever", pack_plain(b"x")));
        let past = pack_ttl(b"old", now_unix_secs().saturating_sub(10));
        assert!(tx.set(b"stale", past));

        assert_eq!(tx.get(b"temp"), Some(b"v".to_vec()));
        assert_eq!(tx.get(b"stale"), None);
        assert!(tx.exists(b"temp"));
        assert!(!tx.exists(b"stale"));

        match tx.get_ttl(b"temp") {
            Some(Some(d)) => assert!(d.as_secs() > 0 && d.as_secs() <= 60),
            other => panic!("expected remaining ttl, got {other:?}"),
        }
        assert_eq!(tx.get_ttl(b"forever"), Some(None));
        assert_eq!(tx.get_ttl(b"stale"), None);

        assert!(tx.refresh_ttl(b"temp", Duration::from_secs(120)));
        assert!(tx.persist(b"temp"));
        assert_eq!(tx.get_ttl(b"temp"), Some(None));

        let n = tx.purge_expired();
        assert!(n >= 1, "应清理 stale, n={n}");
        assert!(!tx.exists(b"stale"));
        tx.commit();
    }
    cleanup(&path);
}
