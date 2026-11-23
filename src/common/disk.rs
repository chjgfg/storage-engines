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

use std::format;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::thread_local;
use std::vec;

// ─── CRC32 ──────────────────────────────────────────────────────────────────

const CRC32_TABLE: [u32; 256] = make_crc32_table();

const fn make_crc32_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut j = 0;
        while j < 8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
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

// 测试专用尺子④（M53）：一次 open 打开了几个文件句柄（`create` 与 `open` 都算 ——
// 每个句柄至少一次内核往返，且新建文件还要给目录项落盘）。
#[cfg(test)]
thread_local! {
    static FILE_OPENS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
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

#[cfg(test)]
mod tests;

// 按职责组织的内部实现；公开类型与原模块路径保持一致。
mod crc;
mod disk_file;
mod file_lock;
mod probes;
pub use crc::crc32;
#[cfg(test)]
pub(crate) use crc::crc32_bytewise;
pub(crate) use crc::crc32_extend;
#[cfg(test)]
pub(crate) use probes::disk_page_reads;
#[cfg(test)]
pub(crate) use probes::file_opens;
#[cfg(test)]
pub(crate) use probes::file_opens_clear;
#[cfg(test)]
pub(crate) use probes::fsync_counts;
#[cfg(test)]
pub(crate) use probes::fsync_counts_clear;
#[cfg(test)]
pub(crate) use probes::len_calls;
#[cfg(unix)]
use probes::libc_flock;
#[cfg(test)]
pub(crate) use probes::note_file_open;
#[cfg(test)]
#[cfg(test)]
pub(crate) use probes::note_fsync;
