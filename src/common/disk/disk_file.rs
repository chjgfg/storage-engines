//! 磁盘定长读写、长度缓存与同步。

use super::*;

impl DiskFile {
    /// 打开或创建（读写）。变宽/追加式布局用它。
    pub fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        Self::open_sized(path, 0)
    }

    /// 打开或创建，并声明这是**定宽块**文件（`block_size` 字节一块）。
    pub fn open_paged(path: impl AsRef<Path>, block_size: usize) -> std::io::Result<Self> {
        Self::open_sized(path, block_size)
    }

    pub(super) fn open_sized(path: impl AsRef<Path>, block_size: usize) -> std::io::Result<Self> {
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
        #[cfg(test)]
        note_file_open();
        Ok(Self {
            file,
            path,
            block_size,
        })
    }

    /// 接管一个**已经按平台专属方式打开**的句柄（锁文件：Windows 要 `share_mode(0)`、
    /// unix 要 `flock`）。存在的意义就是把 fsync 拉回**唯一的门**上 —— `DiskFile::sync` 是记账点，
    /// 裸 `File::sync_all()` 不是：锁这里要是退回"为了一行 pid 付一次 fsync"，
    /// 只有包在同一只门里才数得到（守卫 `file_lock_hint_is_written_without_an_fsync` 判的是总次数为 0）。
    pub(crate) fn from_opened(file: File, path: impl AsRef<Path>) -> Self {
        Self {
            file,
            path: path.as_ref().to_path_buf(),
            block_size: 0,
        }
    }

    /// 裸句柄（只给 `FileLock` 在 unix 上取 `as_raw_fd` 解锁用；Windows 关句柄即释放，
    /// 所以这个方法在 Windows 构建里没人调 —— 用 `cfg(unix)` 而不是 `allow(dead_code)`，
    /// 免得一个"看起来在用"的方法挂在公共门上）。
    #[cfg(unix)]
    pub(crate) fn inner(&self) -> &File {
        &self.file
    }

    /// 仅当文件已存在时打开（不存在返回 `None`）
    pub fn open_existing(path: impl AsRef<Path>) -> std::io::Result<Option<Self>> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(None);
        }
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        #[cfg(test)]
        note_file_open();
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
        // 测试专用尺子：谁在热路径上问文件长度（M31 就是靠这把尺子抓到 `read_page` 的）
        #[cfg(test)]
        LEN_CALLS.with(|c| c.set(c.get() + 1));
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
    /// 只补目标尚缺的可见键，并保留现有目标记录及其 TTL。
    /// 墓碑也属于已观察状态；该接口不是删除双向对账协议，不能保证回收后的删除标记可继续传播。

    /// fsync：数据 + 元数据落盘
    pub fn sync(&mut self) -> std::io::Result<()> {
        #[cfg(test)]
        note_fsync(SITE_OTHER);
        self.file.sync_all()
    }

    /// 同上，但把这一次记到**指定站点**的账上（测试尺子 M53；发布构建里除了 `sync_all` 什么都不多做的）。
    pub fn sync_at(&mut self, site: usize) -> std::io::Result<()> {
        #[cfg(test)]
        note_fsync(site);
        #[cfg(not(test))]
        let _ = site;
        self.file.sync_all()
    }

    /// 读一整块。**越界返回全零块**（B+ 树把"还没写过的槽位"当空页看）。
    ///
    /// ⚠️ 这里**不许**再问 `self.len()`（M31）：旧写法每次读页都先 `File::metadata()` 一趟，
    /// 只为判断"这页在不在 EOF 之外"，而本机裸测那一问要 **6.2~8.9 µs**，整次定位读才
    /// 15~17 µs —— 一半的价花在一句可以不要的问题上。改成"seek 一次 + 读到 EOF 为止"之后
    /// 结论完全等价：
    /// - 整页在文件内 ⇒ 一次 `read` 就填满（与旧的 `read_exact` 同）；
    /// - 起始就在 EOF 之外 ⇒ 第一次 `read` 返回 0 ⇒ 整块保持零（旧写法靠 `offset >= file_len`）；
    /// - 尾部半页 ⇒ 读到 0 为止，剩下保持零（旧写法靠 `available` 截断）。
    /// 语义一字未动，少的是一次系统调用；这条由 `test_read_page_does_not_ask_file_length`
    /// 与 `test_read_page_tail_shapes_match_the_old_length_guarded_form` 两头守着。
    pub fn read_page(&mut self, page_index: u64) -> std::io::Result<Vec<u8>> {
        let mut buf = vec![0u8; self.page_size()?];
        self.read_page_into(page_index, &mut buf)?;
        Ok(buf)
    }

    /// 保持补零尾部协议，使用调用方拥有的有界缓冲。
    pub(crate) fn read_page_into(
        &mut self,
        page_index: u64,
        buf: &mut [u8],
    ) -> std::io::Result<()> {
        #[cfg(test)]
        DISK_PAGE_READS.with(|c| c.set(c.get() + 1));
        let size = self.page_size()?;
        if buf.len() != size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "page buffer has the wrong size",
            ));
        }
        let offset = page_index.checked_mul(size as u64).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "page offset overflow")
        })?;
        buf.fill(0);
        self.file.seek(SeekFrom::Start(offset))?;
        let mut filled = 0usize;
        while filled < size {
            match self.file.read(&mut buf[filled..]) {
                // 0 字节 = 到 EOF ⇒ 其余保持零（等价于旧的两条 EOF 分支）
                Ok(0) => break,
                Ok(n) => filled += n,
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }
        Ok(())
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

    pub(super) fn page_size(&self) -> std::io::Result<usize> {
        if self.block_size == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "该文件没有用 DiskFile::open_paged 声明块大小，不能按 page_index 读写",
            ));
        }
        Ok(self.block_size)
    }
}
