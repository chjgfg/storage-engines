//! B+ 树的磁盘布局：页头 pack/unpack、双写缓冲、路径辅助、blob 旁路、空闲页表。
//!
//! 字节级工具（`DiskFile` / `FileLock` / `crc32`）不在这里 —— 三个引擎各有一份逐字重复的
//! 实现（bitcask 与 lsm 的 FileLock `diff` 差 0 行），而它们恰恰是崩溃语义的载体
//! （sync 是不是 sync_all、谁在挡第二个进程）。已收进 [`crate::common::disk`]，这里 re-export
//! 以保持 `crate::bplus_tree::storage::DiskFile` 这类既有路径不断。
//!
//! 唯一和引擎绑在一起的是**布局**：主数据文件用 `DiskFile::open_paged(path, PAGE_SIZE)`
//! 声明定宽页，`read_page` / `write_page` 才算得对偏移。

use std::assert;
use std::assert_eq;
use std::format;
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::thread_local;
use std::vec;

pub use crate::common::disk::{DiskFile, FileLock, crc32};

// M53 的站点账（不是公开 API，只在 crate 内用于记账）
use crate::common::disk::{SITE_BLOB, SITE_BPLUS_DBLWR, SITE_BPLUS_FREELIST};

/// 页大小（含页头）
pub const PAGE_SIZE: usize = 4096;
/// 页头大小
pub const PAGE_HEADER_SIZE: usize = 16;
/// 有效载荷大小（B+ 树节点序列化数据放这里）
pub const PAGE_PAYLOAD_SIZE: usize = PAGE_SIZE - PAGE_HEADER_SIZE;

/// 页头魔数 "BPG1"
pub const PAGE_MAGIC: u32 = u32::from_le_bytes(*b"BPG1");

// ─── 双写缓冲（Double Write Buffer）────────────────────────────────────────
//
// 防 torn page：先把整页原子地写到 dblwr 文件并 fsync，再写主文件。
// 崩溃恢复时：若 dblwr 中有有效页，用它覆盖主文件中 CRC 损坏的页。

/// 双写文件魔数
const DBLWR_MAGIC: &[u8; 8] = b"DBLWR001";
/// header: magic(8) + count(u32) + reserved(4) = 16
const DBLWR_HEADER_SIZE: usize = 16;
/// 最多缓存的脏页数（一次 flush 批量上限）
pub const DBLWR_MAX_PAGES: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DblwrState {
    Unknown,
    DurableEmpty,
    Published,
}

pub struct DoubleWriteBuffer {
    file: DiskFile,
    state: DblwrState,
}

#[cfg(test)]
pub(crate) mod dblwr_probe;

// ─── 大 value Blob 存储（append-only）──────────────────────────────────────
//
// 文件布局：
// ```text
// 0..8   magic "BLOB0001"
// 之后每条记录：
//   len:u32 LE | crc32(data):u32 LE | data[len]
// ```
//
// 叶子里存引用：`[0x01][offset:u64 LE][len:u32 LE]`
// 内联值：`[0x00][payload...]`  （小 value）

/// 超过此字节数的 value 外置到 blob（叶子只存指针）
pub const BLOB_THRESHOLD: usize = 256;

const BLOB_MAGIC: &[u8; 8] = b"BLOB0001";
/// 存储编码：内联
pub const VAL_TAG_INLINE: u8 = 0x00;
/// 存储编码：blob 引用
pub const VAL_TAG_BLOB: u8 = 0x01;

pub struct BlobStore {
    file: DiskFile,
}

// 测试专用：本线程的 value 解码次数。
//
// B+ 树没有独立键索引，全库折叠的**页读取**次数省不掉；能省的是「每行一次 value 解码」
// —— 内联值是一次 Vec 分配，大 value 是一次 blob 读盘。所以 ② 这条通道用这个量来守。
// 做成 thread_local 是为了不和 `cargo test` 的并行用例互相计数。
#[cfg(test)]
thread_local! {
    static VALUE_DECODES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

// ─── 空闲页列表文件（完整持久化，不受 meta 页容量限制）────────────────────
//
// 布局：
// ```text
// 0..8    magic "FREE0001"
// 8..16   count (u64 LE)
// 16..20  crc32(ids bytes)
// 20..    page_id (u64 LE) × count
// ```

const FREELIST_MAGIC: &[u8; 8] = b"FREE0001";

/// 复用旁文件句柄，每次发布仍以同步结束；FREE0001 格式和 CRC 与原空闲清单读写兼容。
pub(crate) struct FreelistWriter {
    file: DiskFile,
    len: u64,
}

#[cfg(test)]
mod tests;

// 按职责组织的内部实现；公开类型与原模块路径保持一致。
mod blob;
mod double_write;
mod freelist;
mod page_codec;
mod paths;
#[cfg(test)]
mod probes;
mod value_codec;
pub use freelist::read_freelist;
pub use freelist::write_freelist;
pub(crate) use page_codec::maintenance_payload;
pub use page_codec::pack_page;
pub use page_codec::unpack_page;
pub use page_codec::verify_page;
pub use paths::blob_path;
pub use paths::dblwr_path;
pub use paths::freelist_path;
pub use paths::lock_path;
pub use paths::wal_path;
#[cfg(test)]
pub(crate) use probes::value_decodes;
pub use value_codec::decode_stored_value;
pub use value_codec::encode_stored_value;
