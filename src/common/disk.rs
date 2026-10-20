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

/// slice-by-8 表：`SLICES[0]` 就是上面那张逐字节表，`SLICES[k]` 由它递推出来。
///
/// 为什么值得多这 8 KB：逐字节版的 8 次查表是**串行依赖**的（第 k 次要用第 k−1 次的结果），
/// CPU 没法重叠；slice-by-8 一次吃 8 个字节，8 次查表彼此独立 ⇒ 流水线能并行跑。
/// 乘法次数一样，快的是指令级并行。实测见 `bplus_miss_price_probe` 里的**同轮配对 A/B**
/// （同一批缓冲、同一个进程里前后各跑一遍，跨轮噪声抵掉）。
const CRC32_SLICES: [[u32; 256]; 8] = make_crc32_slices();

const fn make_crc32_slices() -> [[u32; 256]; 8] {
    let base = make_crc32_table();
    let mut out = [[0u32; 256]; 8];
    out[0] = base;
    let mut k = 1;
    while k < 8 {
        let mut i = 0;
        while i < 256 {
            let prev = out[k - 1][i];
            out[k][i] = (prev >> 8) ^ base[(prev & 0xFF) as usize];
            i += 1;
        }
        k += 1;
    }
    out
}

/// CRC32（IEEE，多项式 0xEDB88320）。WAL 帧、SST 数据块、blob 记录都用它。
///
/// 与逐字节版（[`crc32_bytewise`]，留着当参照实现）**逐位相同**，这条由
/// `crc32_slice_by_8_matches_bytewise_everywhere` 按多种长度与随机内容钉住 ——
/// 校验和算错不是"慢一点"，是每一页每个块都判成坏页。
pub fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    let mut chunks = data.chunks_exact(8);
    for c in &mut chunks {
        // 小端加载：与逐字节版"先吃低位字节"的顺序一致，所以两者可比
        let a = crc ^ u32::from_le_bytes([c[0], c[1], c[2], c[3]]);
        let b = u32::from_le_bytes([c[4], c[5], c[6], c[7]]);
        let s = &CRC32_SLICES;
        crc = s[7][(a & 0xFF) as usize]
            ^ s[6][((a >> 8) & 0xFF) as usize]
            ^ s[5][((a >> 16) & 0xFF) as usize]
            ^ s[4][((a >> 24) & 0xFF) as usize]
            ^ s[3][(b & 0xFF) as usize]
            ^ s[2][((b >> 8) & 0xFF) as usize]
            ^ s[1][((b >> 16) & 0xFF) as usize]
            ^ s[0][((b >> 24) & 0xFF) as usize];
    }
    for &b in chunks.remainder() {
        crc = (crc >> 8) ^ CRC32_TABLE[((crc ^ b as u32) & 0xFF) as usize];
    }
    !crc
}

/// 逐字节参照实现（串行依赖链，慢的那个）。留着有两个用处：对拍、以及让 A/B 有另一半。
#[cfg(test)]
pub(crate) fn crc32_bytewise(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &b in data {
        crc = (crc >> 8) ^ CRC32_TABLE[((crc ^ b as u32) & 0xFF) as usize];
    }
    !crc
}

// 测试专用尺子①：本线程调过多少次 `DiskFile::len()`（= 一次 `File::metadata()` 系统调用）。
//
// M31 的凭据就挂在这上面：`read_page` 原先每次读都问一遍文件长度，只为判断"这页在不在 EOF
// 之外"，而本机裸测这一次问价 **6.2~8.9 µs**，整次定位读也才 15~17 µs ⇒ 四到五成花在一句
// 可以不要的问题上。守卫 `test_read_page_does_not_ask_file_length` 要求读一页时这个数是 0，
// 并且同一条用例里主动问一次 `len()` 证明这把尺子数得到（不然 0 是空转）。
#[cfg(test)]
thread_local! {
    static LEN_CALLS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn len_calls() -> u64 {
    LEN_CALLS.with(|c| c.get())
}

// 测试专用尺子②：本线程**真的落盘读了几页**（M32）。
//
// 为什么"全驻留"只能在磁盘这一层判：页缓存的命中数回答的是"有没有走到页缓存"，而
// `BPlusTree::node_of` 在**上一层**还有一份解析结果缓存 —— 那份命中时连页缓存都不问，
// 所以"页缓存零未命中"并不等于"没读盘"（结构上可能是两份尺子读数差很多）。实测这个形状里
// 两者恰好相等（一轮点读落盘 1621 = 页缓存未命中 1621，因为未命中必然落盘），但判据要选
// 那个**不会说谎**的层：抬到不设顶之后一轮随机点读的磁盘读次数 = 0，才是"库住进内存了"。
// 守卫见 `test_cache_auto_max_is_raisable_and_misses_go_to_zero`。
#[cfg(test)]
thread_local! {
    static DISK_PAGE_READS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// 本线程落盘读过的页数（每次 `DiskFile::read_page` 记一次；它在最底层，无从命中/未命中之分）
#[cfg(test)]
pub(crate) fn disk_page_reads() -> u64 {
    DISK_PAGE_READS.with(|c| c.get())
}

// 测试专用尺子③（M53）：**fsync 落在哪几处**，按站点分格计数。
//
// 为什么拿"次数"而不是毫秒当判据：本机 `C:` 一次 fsync ≈ 2.6 ms、`D:` ≈ 85.8 ms ⇒ 同一行代码
// 换个盘型就差 30 倍。"启动税是几毫秒"这句话没有可比性，"**一次 open 付几次 fsync、在哪几处**"
// 才是盘型无关的账（下游那台单核机器上把次数 ×14 就是他的代价）。
//
// 站点表就是这一刀的读者最需要的东西：每一次 fsync 都得能回答"它保证什么"，答不出保证什么的
// 那一格才是可修的（比如锁文件只为写个 pid 就 sync 一次）。
#[cfg(test)]
pub(crate) const FSYNC_SITES: &[&str] = &[
    "lock_pid",
    "wal_header(新建)",
    "wal_truncate(改后应为 0)",
    "wal_sync(提交/收口)",
    "bplus_meta",
    "bplus_freelist",
    "bplus_page_flush",
    "bplus_dblwr",
    "bitcask_log_flush",
    "blob(新建/同步)",
    "bitcask_hint",
    "lsm_manifest",
    "lsm_mem_wal",
    "other",
];
#[allow(dead_code)] // 有些站点只在 `#[cfg(test)]` 的记账里被引用（发布构建不该带任何东西）
pub(crate) const SITE_LOCK_PID: usize = 0;
#[allow(dead_code)] // 有些站点只在 `#[cfg(test)]` 的记账里被引用（发布构建不该带任何东西）
pub(crate) const SITE_WAL_HEADER: usize = 1;
#[allow(dead_code)] // 有些站点只在 `#[cfg(test)]` 的记账里被引用（发布构建不该带任何东西）
pub(crate) const SITE_WAL_TRUNCATE: usize = 2;
#[allow(dead_code)] // 有些站点只在 `#[cfg(test)]` 的记账里被引用（发布构建不该带任何东西）
pub(crate) const SITE_WAL_SYNC: usize = 3;
#[allow(dead_code)] // 有些站点只在 `#[cfg(test)]` 的记账里被引用（发布构建不该带任何东西）
pub(crate) const SITE_BPLUS_META: usize = 4;
#[allow(dead_code)] // 有些站点只在 `#[cfg(test)]` 的记账里被引用（发布构建不该带任何东西）
pub(crate) const SITE_BPLUS_FREELIST: usize = 5;
#[allow(dead_code)] // 有些站点只在 `#[cfg(test)]` 的记账里被引用（发布构建不该带任何东西）
pub(crate) const SITE_BPLUS_PAGE_FLUSH: usize = 6;
#[allow(dead_code)] // 有些站点只在 `#[cfg(test)]` 的记账里被引用（发布构建不该带任何东西）
pub(crate) const SITE_BPLUS_DBLWR: usize = 7;
#[allow(dead_code)] // 有些站点只在 `#[cfg(test)]` 的记账里被引用（发布构建不该带任何东西）
pub(crate) const SITE_BITCASK_LOG_FLUSH: usize = 8;
#[allow(dead_code)] // 有些站点只在 `#[cfg(test)]` 的记账里被引用（发布构建不该带任何东西）
pub(crate) const SITE_BLOB: usize = 9;
#[allow(dead_code)] // 有些站点只在 `#[cfg(test)]` 的记账里被引用（发布构建不该带任何东西）
pub(crate) const SITE_BITCASK_HINT: usize = 10;
#[allow(dead_code)] // 有些站点只在 `#[cfg(test)]` 的记账里被引用（发布构建不该带任何东西）
pub(crate) const SITE_LSM_MANIFEST: usize = 11;
#[allow(dead_code)] // 有些站点只在 `#[cfg(test)]` 的记账里被引用（发布构建不该带任何东西）
pub(crate) const SITE_LSM_MEM_WAL: usize = 12;
#[allow(dead_code)] // 有些站点只在 `#[cfg(test)]` 的记账里被引用（发布构建不该带任何东西）
pub(crate) const SITE_OTHER: usize = 13;

#[cfg(test)]
thread_local! {
    // ⚠️ **必须线程局部**（M27/M36 那条：并行用例里进程级计数会互相污染，别的用例正在收口 WAL
    // 就会把这一档读高）。台架一律 `--test-threads=1`，单元守卫则在本线程里做完再读。
    static FSYNC_COUNTS: std::cell::Cell<[u64; 14]> =
        const { std::cell::Cell::new([0u64; 14]) };
}

/// 记一次 fsync。**用 `#[cfg(test)]` 的原子自增，不用墙钟分段**：M48 那条教训照旧 ——
/// 常开的计时插桩会锯短外层 `timed()` 的读数，而一次原子加加减减在这条路径上是噪声级。
#[cfg(test)]
pub(crate) fn note_fsync(site: usize) {
    if site >= FSYNC_SITES.len() {
        return;
    }
    FSYNC_COUNTS.with(|c| {
        // `Cell<数组>` 没有内部可变性 ⇒ 取出、改一格、放回去（同一线程内没有并发）
        let mut arr = c.get();
        arr[site] += 1;
        c.set(arr);
    });
}

#[cfg(test)]
pub(crate) fn fsync_counts() -> [u64; 14] {
    FSYNC_COUNTS.with(|c| c.get())
}

/// ⚠️ 与 M48 那条"槽不会自己清"同一课：读之前必须清，否则第二家会把第一家原样复述一遍。
#[cfg(test)]
pub(crate) fn fsync_counts_clear() {
    FSYNC_COUNTS.with(|c| c.set([0u64; 14]));
}

// 测试专用尺子④（M53）：一次 open 打开了几个文件句柄（`create` 与 `open` 都算 ——
// 每个句柄至少一次内核往返，且新建文件还要给目录项落盘）。
#[cfg(test)]
thread_local! {
    static FILE_OPENS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn file_opens() -> u64 {
    FILE_OPENS.with(|c| c.get())
}

#[cfg(test)]
pub(crate) fn file_opens_clear() {
    FILE_OPENS.with(|c| c.set(0));
}

#[cfg(test)]
pub(crate) fn note_file_open() {
    FILE_OPENS.with(|c| c.set(c.get() + 1));
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
        #[cfg(test)]
        note_file_open();
        Ok(Self { file, path, block_size })
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

    /// 裸句柄（只给 `FileLock` 在 unix 上取 `as_raw_fd` 解锁用）。
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
        #[cfg(test)]
        DISK_PAGE_READS.with(|c| c.set(c.get() + 1));
        let size = self.page_size()?;
        let offset = page_index * size as u64;
        let mut buf = vec![0u8; size];
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
    /// ⚠️ 是 `DiskFile` 而不是裸 `File`：这样"锁文件上有没有 fsync"才是**数得到**的（见 `from_opened`）
    file: DiskFile,
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

    /// M53 的守卫：**拿一把锁不许付 fsync**（改前每次 open 都要为"把 pid 写进锁文件"付一次）。
    ///
    /// 为什么这一格可以省：锁的安全性来自**句柄本身**（Windows 的 `share_mode(0)`、unix 的
    /// `flock`）—— 进程一死内核就释放，与那行 pid 有没有落盘无关。省掉的只有 fsync，
    /// `set_len` + `write_all` 留着：活着的读者仍然看得见 pid（那是这条存在的唯一理由）。
    /// ⚠️ 反向那半也要钉：锁的语义不许被一起带走（第二个 `try_acquire` 必须还是 `WouldBlock`）。
    /// 证人：同线程里主动 `DiskFile::sync()` 一次，尺子必须数得到。
    #[test]
    fn file_lock_hint_is_written_without_an_fsync() {
        use crate::common::disk::{
            fsync_counts, fsync_counts_clear, SITE_LOCK_PID, SITE_OTHER,
        };
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("se_disk_lock_fsync_{}_{}", std::process::id(), nanos));
        let lock_file = dir.join("db.lock");

        fsync_counts_clear();
        let _hold = FileLock::try_acquire_at(&lock_file).unwrap();
        let c = fsync_counts();
        assert_eq!(c[SITE_LOCK_PID], 0, "锁文件又自己 fsync 了 ⇒ 每次 open 多付一次（启动税那一格）");
        assert_eq!(c.iter().sum::<u64>(), 0, "拿一把锁不该产生任何 fsync，现在 {:?}", c);

        // 提示到底写没写进去，只能在**放开之后**读：Windows 这一侧的锁是 `share_mode(0)`，
        // 握着句柄时连本进程都打不开这个文件（实测 error 32 共享冲突 —— 那正是锁的本意）。
        let pid_line = format!("pid={}\n", std::process::id());

        let second = FileLock::try_acquire_at(&lock_file);
        let kind = match second {
            // `FileLock` 没有 Debug（持着句柄，derive 不出来）⇒ 只能自己 match
            Ok(_) => panic!("锁的语义被一起带走了 ⇒ 第二个持有者现在能同时开同一个库"),
            Err(e) => e.kind(),
        };
        assert_eq!(
            kind,
            std::io::ErrorKind::WouldBlock,
            "锁的语义被一起带走了 ⇒ 第二个进程现在能同时开同一个库"
        );

        drop(_hold);
        let txt = std::fs::read_to_string(&lock_file).unwrap();
        assert!(
            txt.contains(&pid_line),
            "pid 提示没写进锁文件（省的是 fsync，不是那次写）：{txt:?}"
        );

        // 证人：尺子在这条路上数得到 fsync
        fsync_counts_clear();
        let mut f = DiskFile::open(dir.join("witness.dat")).unwrap();
        f.sync().unwrap();
        assert_eq!(
            fsync_counts()[SITE_OTHER],
            1,
            "证人失败：主动 sync 一次都没数到 ⇒ 上面那个 0 是量不到，不是不花钱"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// crc32 的已知值：`"123456789"` 的 IEEE CRC32 是 `0xCBF43926`。
    /// 三份复制实现合成一份之后，这个交叉校验只需要写一次。
    #[test]
    fn crc32_matches_known_vector() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(crc32(b""), 0);
        assert_eq!(crc32(b"a"), crc32(b"a"));
        assert_ne!(crc32(b"a"), crc32(b"b"));
    }

    /// slice-by-8 必须与逐字节参照实现**逐位相同**（M31）。
    ///
    /// 校验和算错不是"慢一点"，是**每一页、每一个 SST 块、每一条 WAL 帧都判成坏数据** ——
    /// 静默数据损坏级别的后果，所以对拍要覆盖每一种长度形状：8 的整数倍、不整除的尾巴、
    /// 整页 4096、一页零头 4097；内容也不能只有一种（全零/递增看不到某些进位错）。
    #[test]
    fn crc32_slice_by_8_matches_bytewise_everywhere() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926, "新实现也得过那条已知向量");
        let mut buf: Vec<u8> = (0..=255u8).cycle().take(90_000).collect();
        for n in 0..=41usize {
            assert_eq!(crc32(&buf[..n]), crc32_bytewise(&buf[..n]), "长度 {n} 不一致");
        }
        for n in [63usize, 64, 65, 511, 512, 513, 4095, 4096, 4097, 8192, 70_000] {
            assert_eq!(crc32(&buf[..n]), crc32_bytewise(&buf[..n]), "长度 {n} 不一致");
        }
        // 换一份伪随机内容再对一遍
        let mut s = 0x2545_F491_4F6C_DD1Du64;
        for v in buf.iter_mut() {
            s = s
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            *v = (s >> 56) as u8;
        }
        for n in [1usize, 7, 8, 9, 1000, 4096, 8191] {
            assert_eq!(
                crc32(&buf[..n]),
                crc32_bytewise(&buf[..n]),
                "随机内容长度 {n} 不一致"
            );
        }
        // 判别力：翻一个字节必须变（否则"两个实现一致但都算错"这种共同错误抓不住）
        let before = crc32(&buf[..4096]);
        buf[2000] ^= 0xFF;
        assert_ne!(before, crc32(&buf[..4096]), "单字节翻转必须改变校验和");
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

    /// 读一页**不许问文件长度**（M31）。
    ///
    /// 旧写法 `read_page` 里那句 `self.len()` 是每次读页一趟 `File::metadata()`，本机裸测
    /// 6.2~8.9 µs/次，而整次定位读才 15~17 µs ⇒ 四到五成的页读代价花在一句可以不要的问题上
    /// （判断"这页在不在 EOF 之外"完全可以交给 `read` 自己走到 EOF）。
    ///
    /// ⚠️ 与 M19/M30 同一个写法：**同一条用例里必须主动问一次 `len()` 证明这把尺子数得到**，
    /// 否则"0"可以是空转出来的。
    #[test]
    fn test_read_page_does_not_ask_file_length() {
        const BS: usize = 64;
        let path = std::env::temp_dir().join(format!("se_disk_lenhot_{}.bin", std::process::id()));
        let mut f = DiskFile::open_paged(&path, BS).unwrap();
        for i in 0..8u64 {
            f.write_page(i, &vec![0xA0u8; BS]).unwrap();
        }
        let base = len_calls();
        for i in 0..8u64 {
            let p = f.read_page(i).unwrap();
            assert_eq!(p.len(), BS);
        }
        let hot = len_calls() - base;
        assert_eq!(
            hot, 0,
            "读 8 页过程中问了 {hot} 次文件长度 ⇒ 每页多一次 metadata() 系统调用\
             （本机裸测 6~9 µs/次，而整次定位读才 15~17 µs）"
        );
        // 对照：主动问一次，尺子必须数得到
        let _ = f.len().unwrap();
        assert_eq!(
            len_calls() - base,
            1,
            "尺子空转：主动问一次 len() 都没被记进账，上面那条 0 不算数"
        );
        drop(f);
        let _ = std::fs::remove_file(&path);
    }

    /// 去掉那句 `len()` 之后，**三种页形状的结果必须与旧写法逐字节相同**（M31）。
    ///
    /// 这条是上一条的另一半：上一条管"少了一次系统调用"，这一条管"语义没动"。
    /// 旧写法靠 `offset >= file_len` 与 `available` 两个分支表达"整页在外的页读回全零"和
    /// "尾部半页零填充"；新写法靠 `read` 走到 EOF 得到同一个结果 —— 但"得到同一个结果"
    /// 是要逐字节对的，不是靠读代码看出来。
    #[test]
    fn test_read_page_tail_shapes_match_the_old_length_guarded_form() {
        const BS: usize = 64;
        let path = std::env::temp_dir().join(format!("se_disk_tail_{}.bin", std::process::id()));
        let mut f = DiskFile::open_paged(&path, BS).unwrap();
        // 整页两页
        f.write_page(0, &vec![1u8; BS]).unwrap();
        f.write_page(1, &vec![2u8; BS]).unwrap();
        // 第 2 页只写 20 字节 ⇒ 文件长 2*BS + 20，这是一页"尾部半页"
        f.write_all_at(2 * BS as u64, &[3u8; 20]).unwrap();

        let whole = f.read_page(0).unwrap();
        assert_eq!(whole, vec![1u8; BS], "整页必须原样读回");
        let half = f.read_page(2).unwrap();
        assert_eq!(half.len(), BS, "越界/半页也必须返回正好一块");
        assert_eq!(&half[..20], &[3u8; 20][..], "半页的前 20 字节是真数据");
        assert!(
            half[20..].iter().all(|&b| b == 0),
            "半页剩下必须是零（旧写法靠 available 截断得到同一个结果）"
        );
        let beyond = f.read_page(9).unwrap();
        assert!(
            beyond.iter().all(|&b| b == 0),
            "整页在 EOF 之外必须是全零块（旧写法靠 offset >= file_len 那条分支）"
        );
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
