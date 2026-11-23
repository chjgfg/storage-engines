// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn all_engines_ttl_and_meta() {
    let engines = open_all();
    let mut dirs = Vec::new();
    for (name, db, dir) in engines {
        let tx = db.begin();
        assert!(tx.set_with_ttl(b"t", b"v".to_vec(), Duration::from_secs(60)));
        let ttl = tx.get_ttl(b"t").expect("{name}: key exists");
        assert!(ttl.is_some(), "{name}: should have a TTL");

        assert!(tx.persist_key(b"t"), "{name}: persist");
        assert_eq!(tx.get_ttl(b"t"), Some(None), "{name}: now permanent");

        let meta = tx.get_meta(b"t").expect("{name}: meta");
        assert_eq!(meta.key, b"t".to_vec());
        assert!(!meta.deleted, "{name}: not deleted");
        tx.commit();
        dirs.push(dir);
    }
    cleanup(dirs);
}
