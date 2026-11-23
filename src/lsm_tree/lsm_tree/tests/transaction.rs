// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn test_overwrite_across_levels() {
    let dir = tmp_dir("ovw");
    {
        let mut db = LSMTree::open_with_options(&dir, 5, 2).unwrap();
        db.put(b"x".to_vec(), b"old".to_vec());
        db.flush().unwrap();
        db.put(b"x".to_vec(), b"new".to_vec());
        db.flush().unwrap();
        assert_eq!(db.get(b"x"), Some(b"new".to_vec()));
    }
    {
        let db = LSMTree::open(&dir).unwrap();
        assert_eq!(db.get(b"x"), Some(b"new".to_vec()));
    }
    let _ = fs::remove_dir_all(&dir);
}
