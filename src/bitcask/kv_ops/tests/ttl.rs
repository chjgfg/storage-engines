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
fn test_incr_and_ttl() {
    let path = tmp("incr");
    {
        let mvcc = MVCC::open(&path);
        let tx = mvcc.begin_transaction();
        assert_eq!(tx.incr(b"n", 1).unwrap(), 1);
        assert_eq!(tx.incr(b"n", 9).unwrap(), 10);
        assert_eq!(tx.decr(b"n", 3).unwrap(), 7);
        assert_eq!(tx.get(b"n"), Some(b"7".to_vec()));

        assert_eq!(
            tx.incr_with_ttl(b"hits", 1, Duration::from_secs(60))
                .unwrap(),
            1
        );
        assert!(matches!(tx.get_ttl(b"hits"), Some(Some(_))));

        assert!(tx.set_with_ttl(b"tmp", b"x".to_vec(), Duration::from_secs(120)));
        assert!(tx.exists(b"tmp"));
        assert!(tx.persist(b"tmp"));
        assert_eq!(tx.get_ttl(b"tmp"), Some(None));

        // 过期清理
        let stale = pack_ttl(b"old", now_unix_secs().saturating_sub(5));
        assert!(tx.set(b"gone", stale));
        assert!(!tx.exists(b"gone"));
        let n = tx.purge_expired();
        assert!(n >= 1);
        tx.commit();
    }
    cleanup(&path);
}

#[test]
fn test_meta_and_exists() {
    let path = tmp("meta");
    {
        let mvcc = MVCC::open(&path);
        let tx = mvcc.begin_transaction();
        assert!(tx.set(b"k", pack_plain(b"hello")));
        assert!(tx.exists(b"k"));
        let m = tx.get_meta(b"k").unwrap();
        assert_eq!(m.value_len, Some(5));
        assert!(!m.deleted);
        assert!(!m.expired);
        assert!(tx.delete(b"k"));
        assert!(!tx.exists(b"k"));
        tx.commit();
    }
    cleanup(&path);
}
