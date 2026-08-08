//! 底层文件操作 + CRC（供 WAL 使用）
//!
//! 从 bplus-tree 精简移植：只保留 DiskFile / crc32 / 路径辅助，
//! 不含页式双写、Blob（LSM 侧 value 先内联）。

use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

// ─── CRC32 (IEEE / Ethernet, poly 0xEDB88320) ───────────────────────────────

/// 计算 CRC-32（IEEE 802.3）
pub fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &b in data {
        let idx = ((crc ^ u32::from(b)) & 0xFF) as usize;
        crc = CRC32_TABLE[idx] ^ (crc >> 8);
    }
    !crc
}

const CRC32_TABLE: [u32; 256] = make_crc32_table();

const fn make_crc32_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut n = 0;
    while n < 256 {
        let mut c = n as u32;
        let mut k = 0;
        while k < 8 {
            if c & 1 != 0 {
                c = 0xEDB8_8320 ^ (c >> 1);
            } else {
                c >>= 1;
            }
            k += 1;
        }
        table[n] = c;
        n += 1;
    }
    table
}

// ─── 底层文件封装 ───────────────────────────────────────────────────────────

/// 可定位读写的磁盘文件封装
pub struct DiskFile {
    file: File,
    path: PathBuf,
}

impl DiskFile {
    /// 打开或创建文件（读写）
    pub fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                let _ = std::fs::create_dir_all(parent);
            }
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;
        Ok(Self { file, path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn len(&self) -> std::io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    pub fn set_len(&mut self, len: u64) -> std::io::Result<()> {
        self.file.set_len(len)
    }

    /// 从 offset 读满 buf
    pub fn read_exact_at(&mut self, offset: u64, buf: &mut [u8]) -> std::io::Result<()> {
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(buf)
    }

    /// 从 offset 写满 data
    pub fn write_all_at(&mut self, offset: u64, data: &[u8]) -> std::io::Result<()> {
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(data)
    }

    /// 追加写，返回写入前的 offset
    pub fn append(&mut self, data: &[u8]) -> std::io::Result<u64> {
        let offset = self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(data)?;
        Ok(offset)
    }

    /// fsync：数据 + 元数据落盘
    pub fn sync(&mut self) -> std::io::Result<()> {
        self.file.sync_all()
    }
}

// ─── 路径辅助 ───────────────────────────────────────────────────────────────

/// 数据目录下的 WAL：`dir/lsm.wal`
pub fn wal_path(dir: &Path) -> PathBuf {
    dir.join("lsm.wal")
}

/// 数据目录下的锁文件：`dir/lsm.lock`
pub fn lock_path(dir: &Path) -> PathBuf {
    dir.join("lsm.lock")
}

/// 数据目录下的 MemTable WAL：`dir/mem.wal`（裸 LSM 用；MVCC 关闭）
pub fn mem_wal_path(dir: &Path) -> PathBuf {
    dir.join("mem.wal")
}

// ─── 单写者文件锁 ───────────────────────────────────────────────────────────

/// 独占锁（进程级）。持有期间阻止其它 `MVCC::try_open` 打开同一目录。
pub struct FileLock {
    file: File,
    path: PathBuf,
}

impl FileLock {
    /// 尝试获取独占锁；失败返回错误（库已被占用）
    pub fn try_acquire(dir: &Path) -> std::io::Result<Self> {
        let path = lock_path(dir);
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
(1) 同进程内仍有未 drop 的 MVCC/Transaction; \
(2) 另一个进程正在使用该库。",
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
                    format!(
                        "数据库已被其它进程打开（无法获取锁）: {}",
                        path.display()
                    ),
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

        // 写入 pid 便于排查
        let mut f = file;
        let _ = f.set_len(0);
        let _ = f.write_all(format!("pid={}\n", std::process::id()).as_bytes());
        let _ = f.sync_all();
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
            let _ = unsafe { libc_flock(self.file.as_raw_fd(), 8) };
        }
        // Windows: 关闭句柄即释放 share_mode=0 锁
        let _ = &self.file;
    }
}

#[cfg(unix)]
unsafe fn libc_flock(fd: i32, op: i32) -> i32 {
    extern "C" {
        fn flock(fd: i32, op: i32) -> i32;
    }
    flock(fd, op)
}
