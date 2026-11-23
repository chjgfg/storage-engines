// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn test_hint_roundtrip_and_fast_open() {
    let path = tmp_log("rt");
    {
        let mut eng = Bitcask::new(path.to_path_buf()).unwrap();
        eng.set(b"a", b"1".to_vec()).unwrap();
        eng.set(b"b", b"2".to_vec()).unwrap();
        eng.set(b"c", b"3".to_vec()).unwrap();
        eng.delete(b"b").unwrap();
        eng.flush().unwrap();
        eng.dump_hint().unwrap();
        assert!(eng.hint_path().exists());
        assert_eq!(eng.len(), 2);
    }
    // 再 open：应走 hint 快路径
    {
        let mut eng = Bitcask::new(path.to_path_buf()).unwrap();
        assert_eq!(eng.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(eng.get(b"b").unwrap(), None);
        assert_eq!(eng.get(b"c").unwrap(), Some(b"3".to_vec()));
        assert_eq!(eng.len(), 2);
    }
    cleanup(&path);
}

#[test]
fn test_rotate_and_reopen() {
    let path = tmp_log("rotate");
    {
        // 很小上限强制 rotate
        let mut eng = Bitcask::with_max_file_bytes(path.to_path_buf(), 256).unwrap();
        for i in 0..40u32 {
            let k = format!("k{i:03}");
            let v = format!("value-padding-{:03}-xxxxxxxx", i);
            eng.set(k.as_bytes(), v.into_bytes()).unwrap();
        }
        eng.flush().unwrap();
        eng.dump_hint().unwrap();
        assert!(
            eng.active_id() > 0,
            "expected rotate, active_id={}",
            eng.active_id()
        );
        // 密封文件应存在
        let sealed = discover_sealed_ids(&path);
        assert!(!sealed.is_empty(), "expected sealed segments");
        assert_eq!(
            eng.get(b"k000").unwrap(),
            Some(b"value-padding-000-xxxxxxxx".to_vec())
        );
        assert_eq!(
            eng.get(b"k039").unwrap(),
            Some(b"value-padding-039-xxxxxxxx".to_vec())
        );
    }
    {
        let mut eng = Bitcask::new(path.to_path_buf()).unwrap();
        assert_eq!(
            eng.get(b"k000").unwrap(),
            Some(b"value-padding-000-xxxxxxxx".to_vec())
        );
        assert_eq!(
            eng.get(b"k020").unwrap(),
            Some(b"value-padding-020-xxxxxxxx".to_vec())
        );
        assert_eq!(
            eng.get(b"k039").unwrap(),
            Some(b"value-padding-039-xxxxxxxx".to_vec())
        );
        assert_eq!(eng.len(), 40);
    }
    // merge 后应只剩 active
    {
        let mut eng = Bitcask::new(path.to_path_buf()).unwrap();
        eng.merge().unwrap();
        assert_eq!(eng.active_id(), 0);
        assert!(discover_sealed_ids(&path).is_empty());
        assert_eq!(
            eng.get(b"k010").unwrap(),
            Some(b"value-padding-010-xxxxxxxx".to_vec())
        );
    }
    cleanup(&path);
}
