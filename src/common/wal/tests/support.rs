// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

fn tmp_wal(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "se_wal_{tag}_{}_{}.wal",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}
