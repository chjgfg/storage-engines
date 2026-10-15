//! 字节级文件工具：`DiskFile` / `FileLock` / `crc32`。
//!
//! 这三样原本在 bitcask / bplus_tree / lsm_tree 的 `storage.rs` 里各有一份**逐字重复**的
//! 实现（`diff` 实测：bitcask 与 lsm 的 FileLock 差 0 行，crc32 差 0 行）。README 的
//! 「已知局限」里那条"三份近乎逐字重复的实现"说的就是它们。
//!
//! 为什么值得合：这三样都是**崩溃语义**的载体 —— `sync()` 到底是不是 `sync_all`、
//! Windows 的 `share_mode(0)` 与 unix 的 `flock(LOCK_EX|LOCK_NB)` 谁在挡第二个进程、
//! 锁文件里写不写 pid。这类细节修一处漏两处，正是"一个 bug 四个实例"的来源。
//!
//! 引擎侧仍然各留自己的**路径规则**（`lock_path` / `data.log` / `<stem>.lock` 各家不同），
//! 所以 `FileLock::try_acquire_at` 收的是锁文件的完整路径，由各引擎自己算。

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

// ─── CRC32 ──────────────────────────────────────────────────────────────────

const CRC32_TABLE: [u32; 256] = make_crc32_table();

const fn make_crc32_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut j = 0;
        while j < 8 {
            crc = if crc & 1 != 0 { (crc >> 1) ^ 0xEDB8_8320 } else { crc >> 1 };
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

/// CRC32（IEEE，多项式 0xEDB88320）。WAL 帧、SST 数据块、blob 记录都用它。
pub fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &b in data {
        crc = (crc >> 8) ^ CRC32_TABLE[((crc ^ b as u32) & 0xFF) as usize];
    }
    !crc
}

// ─── DiskFile ───────────────────────────────────────────────────────────────

/// 定宽/变宽记录共用的文件句柄：定位读写 + 追加 + fsync。
///
/// `block_size` 只在**固定槽位布局**（B+ 树的 4 KiB 页）的文件上设置：那样才能按
/// `page_index` 读写整页。没设置（0）就调用 `read_page` / `write_page` 会明确报错，
/// 不会静默按某个猜测的页大小算错偏移 —— 那种错会写成一片看起来正常的坏数据。
#[derive(Debug)]
pub struct DiskFile {
    file: File,
    path: PathBuf,
    block_size: usize,
}

impl DiskFile {
    /// 打开或创建（读写）。变宽/追加式布局用它。
    pub fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        Self::open_sized(path, 0)
    }

    /// 打开或创建，并声明这是**定宽块**文件（`block_size` 字节一块）。
    pub fn open_paged(path: impl AsRef<Path>, block_size: usize) -> std::io::Result<Self> {
        Self::open_sized(path, block_size)
    }

    fn open_sized(path: impl AsRef<Path>, block_size: usize) -> std::io::Result<Self> {
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
        Ok(Self { file, path, block_size })
    }

    /// 仅当文件已存在时打开（不存在返回 `None`）
    pub fn open_existing(path: impl AsRef<Path>) -> std::io::Result<Option<Self>> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(None);
        }
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        Ok(Some(Self {
            file,
            path: path.to_path_buf(),
            block_size: 0,
        }))
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

    /// 读一整块。越界返回全零块（B+ 树把"还没写过的槽位"当空页看）。
    pub fn read_page(&mut self, page_index: u64) -> std::io::Result<Vec<u8>> {
        let size = self.page_size()?;
        let offset = page_index * size as u64;
        let file_len = self.len()?;
        let mut buf = vec![0u8; size];
        if offset >= file_len {
            return Ok(buf);
        }
        let available = (file_len - offset).min(size as u64) as usize;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(&mut buf[..available])?;
        Ok(buf)
    }

    /// 写一整块（必须正好一块）
    pub fn write_page(&mut self, page_index: u64, data: &[u8]) -> std::io::Result<()> {
        let size = self.page_size()?;
        if data.len() != size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("write_page: 需要正好 {size} 字节，收到 {}", data.len()),
            ));
        }
        self.write_all_at(page_index * size as u64, data)
    }

    fn page_size(&self) -> std::io::Result<usize> {
        if self.block_size == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "该文件没有用 DiskFile::open_paged 声明块大小，不能按 page_index 读写",
            ));
        }
        Ok(self.block_size)
    }
}

// ─── 单写者文件锁 ───────────────────────────────────────────────────────────

/// 独占文件锁：持有期间保证「一个库只有一个写者」。
///
/// 平台做法不同但**语义必须一致**，这也是把三份重复实现合成一份的主要原因：
/// - Windows：`share_mode(0)` 让第二个进程 open 直接失败（权限拒绝 / 32 号错），
///   释放靠关闭句柄 —— 所以 `Drop` 不需要额外动作。
/// - unix：`flock(LOCK_EX | LOCK_NB)`，拿不到就立刻返回 `WouldBlock`，不阻塞。
/// - 其它平台：退化成无保护（如实记录在 README 的已知局限里）。
///
/// 拿不到时统一返回 `ErrorKind::WouldBlock`，调用方（`MVCC::try_open`）据此区分
/// "库被占用"与"磁盘坏了"。
pub struct FileLock {
    file: File,
    path: PathBuf,
}

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
            unsafe {
                let _ = libc_flock(self.file.as_raw_fd(), 8);
            }
        }
        // Windows：关闭句柄即释放 share_mode=0 独占
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

#[cfg(test)]
mod tests {
    use super::*;

    /// crc32 的已知值：`"123456789"` 的 IEEE CRC32 是 `0xCBF43926`。
    /// 三份复制实现合成一份之后，这个交叉校验只需要写一次。
    #[test]
    fn crc32_matches_known_vector() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(crc32(b""), 0);
        assert_eq!(crc32(b"a"), crc32(b"a"));
        assert_ne!(crc32(b"a"), crc32(b"b"));
    }

    #[test]
    fn disk_file_round_trips_offsets() {
        let path = std::env::temp_dir().join(format!("se_disk_{}.bin", std::process::id()));
        let mut f = DiskFile::open(&path).unwrap();
        let off = f.append(b"hello").unwrap();
        assert_eq!(off, 0);
        let off2 = f.append(b"world").unwrap();
        assert_eq!(off2, 5);
        assert_eq!(f.len().unwrap(), 10);
        let mut buf = [0u8; 5];
        f.read_exact_at(off2, &mut buf).unwrap();
        assert_eq!(&buf, b"world");
        f.write_all_at(0, b"HELLO").unwrap();
        f.read_exact_at(0, &mut buf).unwrap();
        assert_eq!(&buf, b"HELLO");
        f.sync().unwrap();
        drop(f);
        let _ = std::fs::remove_file(&path);
    }

    /// 没声明块大小就按 page_index 读写 ⇒ 必须**明确报错**，
    /// 不能猜一个页大小然后把数据写到错误的偏移上（那会是一片看起来正常的坏数据）。
    #[test]
    fn page_ops_require_declared_block_size() {
        let path = std::env::temp_dir().join(format!("se_disk_pg_{}.bin", std::process::id()));
        let mut f = DiskFile::open(&path).unwrap();
        let err = f.read_page(0).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        let err = f.write_page(0, &[0u8; 16]).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        drop(f);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn paged_file_reads_and_writes_whole_blocks() {
        const BS: usize = 64;
        let path = std::env::temp_dir().join(format!("se_disk_blk_{}.bin", std::process::id()));
        {
            let mut f = DiskFile::open_paged(&path, BS).unwrap();
            // 没写过的槽位 = 全零
            assert_eq!(f.read_page(3).unwrap(), vec![0u8; BS]);
            f.write_page(1, &vec![7u8; BS]).unwrap();
            assert_eq!(f.read_page(1).unwrap(), vec![7u8; BS]);
            assert_eq!(f.len().unwrap(), 2 * BS as u64);
            // 长度不对的写入必须被挡住
            assert!(f.write_page(0, &[0u8; BS - 1]).is_err());
        }
        let _ = std::fs::remove_file(&path);
    }

    /// 同一把锁的第二个持有者要拿到 `WouldBlock`（不是 panic、不是阻塞成功）。
    #[test]
    fn file_lock_blocks_second_holder() {
        let dir = std::env::temp_dir().join(format!("se_lock_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let lock_file = dir.join("db.lock");
        let _hold = FileLock::try_acquire_at(&lock_file).unwrap();
        let second = FileLock::try_acquire_at(&lock_file);
        assert!(second.is_err(), "第二个持有者不该拿到锁");
        assert_eq!(
            second.err().map(|e| e.kind()),
            Some(std::io::ErrorKind::WouldBlock),
            "错误类型必须是 WouldBlock，调用方靠它区分「被占用」和「盘坏了」"
        );
        // 释放之后同一把锁可以再拿
        drop(_hold);
        let again = FileLock::try_acquire_at(&lock_file);
        assert!(again.is_ok());
        drop(again);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
