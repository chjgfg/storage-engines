//! v4 快照完整写入后的原子替换。

use super::*;
/// 写入临时快照、完成校验和同步，再原子替换正式文件。
/// 编码或发布失败保留旧快照；读取方只接受完整发布的版本格式。

/// 在同一文件系统完整发布 memory 快照；重命名前发生错误保留旧文件，Unix 还同步父目录。
pub(crate) fn atomic_write_bin_v4(
    path: &Path,
    write: impl FnOnce(&mut BinWriter) -> std::io::Result<()>,
) -> std::io::Result<IoStats> {
    #[cfg(test)]
    let measured = snapshot_probe::options().is_some();
    #[cfg(test)]
    let total = measured.then(std::time::Instant::now);
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    struct Staging(std::path::PathBuf);
    impl Drop for Staging {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }
    let (staging, file) = loop {
        let mut name = path.as_os_str().to_os_string();
        name.push(format!(
            ".{}.{}.tmp",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let staging = std::path::PathBuf::from(name);
        match File::options().write(true).create_new(true).open(&staging) {
            Ok(file) => break (Staging(staging), file),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e),
        }
    };
    let mut writer = BinWriter::from_file(file, BIN_MAGIC_V4, false, true, true)?;
    #[cfg(test)]
    let body = measured.then(std::time::Instant::now);
    write(&mut writer)?;
    #[cfg(test)]
    let body_ns = body.map(|s| s.elapsed().as_nanos()).unwrap_or(0);
    let stats = writer.finish()?;
    #[cfg(test)]
    let publish = measured.then(std::time::Instant::now);
    // 不要先删除目标文件，重命名本身完成替换。
    std::fs::rename(&staging.0, path)?;
    #[cfg(unix)]
    File::open(
        path.parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new(".")),
    )?
    .sync_all()?;
    #[cfg(test)]
    if let (Some(total), Some(publish)) = (total, publish) {
        snapshot_probe::note(|s| {
            s.body_ns = body_ns;
            s.publish_ns = publish.elapsed().as_nanos();
            s.total_ns = total.elapsed().as_nanos();
        });
    }
    Ok(stats)
}
