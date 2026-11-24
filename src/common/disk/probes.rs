//! 本文件负责测试计量与机制对照。

#[cfg(test)]
use super::*;

#[cfg(test)]
pub(crate) fn len_calls() -> u64 {
    LEN_CALLS.with(|c| c.get())
}

/// 本线程落盘读过的页数（每次 `DiskFile::read_page` 记一次；它在最底层，无从命中/未命中之分）
#[cfg(test)]
pub(crate) fn disk_page_reads() -> u64 {
    DISK_PAGE_READS.with(|c| c.get())
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

#[cfg(unix)]
// 父模块需要导入此封装，供文件锁实现获取和释放 Unix 独占锁。
pub(super) unsafe fn libc_flock(fd: i32, op: i32) -> i32 {
    // edition 2024：extern 块必须 unsafe、unsafe fn 体内不再隐式 unsafe。
    unsafe extern "C" {
        pub(super) fn flock(fd: i32, op: i32) -> i32;
    }
    unsafe { flock(fd, op) }
}
