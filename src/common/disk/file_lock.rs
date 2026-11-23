//! 单写者文件锁和异常释放。

use super::*;

impl FileLock {
    /// 获取 `lock_file` 上的独占锁。各引擎自己决定锁文件路径（约定不同）。
    pub fn try_acquire_at(lock_file: &Path) -> std::io::Result<Self> {
        let path = lock_file.to_path_buf();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                let _ = std::fs::create_dir_all(parent);
            }
        }

        #[cfg(windows)]
        let file = {
            use std::os::windows::fs::OpenOptionsExt;
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .share_mode(0) // 禁止其它进程共享读写
                .open(&path)
                .map_err(|e| {
                    if e.kind() == std::io::ErrorKind::PermissionDenied
                        || e.raw_os_error() == Some(32)
                    {
                        std::io::Error::new(
                            std::io::ErrorKind::WouldBlock,
                            format!(
                                "数据库锁被占用: {} — 可能原因：\
(1) 同进程内仍有未 drop 的 MVCC/Transaction（Transaction 持有 Arc 会保住锁）; \
(2) 另一个进程正在使用该库。请先释放全部句柄再 open。",
                                path.display()
                            ),
                        )
                    } else {
                        e
                    }
                })?
        };

        #[cfg(unix)]
        let file = {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&path)?;
            use std::os::unix::io::AsRawFd;
            // LOCK_EX=2, LOCK_NB=4
            let rc = unsafe { libc_flock(file.as_raw_fd(), 2 | 4) };
            if rc != 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    format!("数据库已被其它进程打开（无法获取锁）: {}", path.display()),
                ));
            }
            file
        };

        #[cfg(not(any(windows, unix)))]
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        #[cfg(test)]
        note_file_open();
        // 写入 pid 便于排查
        // ⚠️ 包成 `DiskFile` 而不是留裸 `File`：fsync 的记账点只在 `DiskFile::sync*` 这一扇门上，
        // 裸句柄偷偷 `sync_all()` 是数不到的 —— 而这一格恰恰是 M53 砍掉的那笔启动税，
        // 守卫要能抓住"有人把 fsync 加回来"才算判据（见 `file_lock_hint_is_written_without_an_fsync`）。
        let mut f = DiskFile::from_opened(file, &path);
        let _ = f.set_len(0);
        let _ = f.write_all_at(0, format!("pid={}\n", std::process::id()).as_bytes());
        // M53：**这里不再 fsync**（改前每次 open 都要付一次，四家都付）。
        // 这把锁的安全性来自句柄本身（Windows 的 `share_mode(0)` / unix 的 `flock`）：进程一死内核就释放，
        // 与 pid 那行字落没落盘无关。所以落盘的只是"排障时看得见谁占着库"这一条提示 ——
        // 它不崩数据、不崩锁，最多是掉电后那个提示文件是空的（`path()` 与错误消息照样能用）。
        // ⚠️ 反向的那半不能一起省：`set_len`/`write_all` 留着，否则连活着的读者都看不见 pid。
        Ok(Self { file: f, path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            // LOCK_UN = 8
            unsafe {
                let _ = libc_flock(self.file.inner().as_raw_fd(), 8);
            }
        }
        // Windows：关闭句柄即释放 share_mode=0 独占
        let _ = &self.file;
    }
}
