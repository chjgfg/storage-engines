// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

fn temp_db(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("bplus_{tag}_{nanos}.db"))
}

fn cleanup(path: &Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(dblwr_path(path));
    let _ = std::fs::remove_file(crate::bplus_tree::storage::wal_path(path));
    let _ = std::fs::remove_file(freelist_path(path));
    let _ = std::fs::remove_file(crate::bplus_tree::storage::lock_path(path));
    let _ = std::fs::remove_file(crate::bplus_tree::storage::blob_path(path));
}
