//! LSM 侧的路径辅助（`dir/lsm.wal` / `dir/lsm.lock` / `mem.wal`）。
//!
//! 字节级工具（`DiskFile` / `FileLock` / `crc32`）三份逐字重复的实现已收进
//! [`crate::common::disk`]；这里 re-export 以保持 `crate::lsm_tree::storage::DiskFile`
//! 这类既有路径不断。不含页式双写、Blob（LSM 的大 value 走 `blobs/`，见 `lsm_tree.rs`）。

pub use crate::common::disk::{crc32, DiskFile, FileLock};

use std::path::{Path, PathBuf};

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

