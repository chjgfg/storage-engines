//! Bitcask 的路径辅助 + 存储编码（内联 / blob 指针）。
//!
//! 字节级工具（`DiskFile` / `FileLock` / `crc32`）不在这里 —— 三个引擎各有一份逐字重复
//! 的实现正是崩溃语义修一处漏两处的来源，已收进 [`crate::common::disk`]，这里 re-export
//! 以保持 `crate::bitcask::storage::DiskFile` 这类既有路径不断。

use std::vec;
use std::format;
use std::path::{Path, PathBuf};

// M53 的站点账：blob 侧文件的 fsync 不认引擎，四家共用同一格
use crate::common::disk::SITE_BLOB;

pub use crate::common::disk::{crc32, DiskFile, FileLock};


// ─── 路径辅助 ───────────────────────────────────────────────────────────────

/// 数据目录下的 Bitcask 数据文件：`dir/data.log`
pub fn data_path(dir: &Path) -> PathBuf {
    dir.join("data.log")
}

/// 数据目录下的 KeyDir hint：`dir/data.hint`（与 `data.log` 同 stem）
pub fn hint_path(dir: &Path) -> PathBuf {
    dir.join("data.hint")
}

/// 由数据文件路径推导 hint：`data.log` → `data.hint`
pub fn hint_path_for_log(data_log: &Path) -> PathBuf {
    data_log.with_extension("hint")
}

/// 数据目录下的事务 WAL：`dir/bitcask.wal`
pub fn wal_path(dir: &Path) -> PathBuf {
    dir.join("bitcask.wal")
}

/// 数据目录下的锁文件：`dir/bitcask.lock`
pub fn lock_path(dir: &Path) -> PathBuf {
    dir.join("bitcask.lock")
}

/// 数据目录下的大 value blob：`dir/data.blob`
pub fn blob_path(dir: &Path) -> PathBuf {
    dir.join("data.blob")
}

// ─── 大 value Blob 存储（append-only）──────────────────────────────────────
//
// 文件布局：
// ```text
// 0..8   magic "BLOB0001"
// 之后每条记录：
//   len:u32 LE | crc32(data):u32 LE | data[len]
// ```
//
// 主日志里存引用：`[0x01][offset:u64 LE][len:u32 LE]`
// 内联值：`[0x00][payload...]`  （小 value）

/// 超过此字节数的 value 外置到 blob（主日志只存指针）
pub const BLOB_THRESHOLD: usize = 256;

const BLOB_MAGIC: &[u8; 8] = b"BLOB0001";
/// 存储编码：内联
pub const VAL_TAG_INLINE: u8 = 0x00;
/// 存储编码：blob 引用
pub const VAL_TAG_BLOB: u8 = 0x01;

pub struct BlobStore {
    file: DiskFile,
    /// 逻辑文件末尾（含缓冲）
    end_pos: u64,
    buf: Vec<u8>,
}

const BLOB_BUF_TARGET: usize = 4 * 1024 * 1024;

impl BlobStore {
    pub fn open(dir: &Path) -> std::io::Result<Self> {
        let path = blob_path(dir);
        let mut file = DiskFile::open(&path)?;
        let len = file.len()?;
        if len == 0 {
            file.write_all_at(0, BLOB_MAGIC)?;
            // bulk 路径不每条 sync；open 时写一次 header
            file.sync_at(SITE_BLOB)?;
            Ok(Self {
                file,
                end_pos: BLOB_MAGIC.len() as u64,
                buf: Vec::new(), // 首次 append 才按目标大小分配（见 BlobStore::append）
            })
        } else {
            let mut magic = [0u8; 8];
            file.read_exact_at(0, &mut magic)?;
            if &magic != BLOB_MAGIC {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("非法 blob 文件: {}", path.display()),
                ));
            }
            Ok(Self {
                file,
                end_pos: len,
                buf: Vec::new(), // 首次 append 才按目标大小分配（见 BlobStore::append）
            })
        }
    }

    fn flush_buf(&mut self) -> std::io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let offset = self.end_pos - self.buf.len() as u64;
        self.file.write_all_at(offset, &self.buf)?;
        self.buf.clear();
        Ok(())
    }

    /// 测试探针：写缓冲当前的预留容量（"懒分配"那条守卫要看的就是这个）
    #[cfg(test)]
    pub fn buf_capacity_for_test(&self) -> usize {
        self.buf.capacity()
    }

    /// 追加一条 blob，返回文件内 offset（记录起点）
    pub fn append(&mut self, data: &[u8]) -> std::io::Result<u64> {
        let offset = self.end_pos;
        let checksum = crc32(data);
        let rec_len = 8 + data.len();
        if self.buf.capacity() < self.buf.len() + rec_len {
            // 首次就一步涨到目标大小（此后 flush_buf 只 clear、容量保留）：
            // 既不空留 4 MiB，也不让小步 realloc 抖动
            self.buf.reserve(BLOB_BUF_TARGET.saturating_sub(self.buf.len()).max(rec_len));
        }
        self.buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
        self.buf.extend_from_slice(&checksum.to_le_bytes());
        self.buf.extend_from_slice(data);
        self.end_pos += rec_len as u64;
        if self.buf.len() >= BLOB_BUF_TARGET {
            self.flush_buf()?;
        }
        Ok(offset)
    }

    pub fn sync(&mut self) -> std::io::Result<()> {
        self.flush_buf()?;
        self.file.sync_at(SITE_BLOB)
    }

    /// 按 offset 读取 blob 记录
    pub fn read_at(&mut self, offset: u64) -> std::io::Result<Vec<u8>> {
        self.flush_buf()?;
        let mut hdr = [0u8; 8];
        self.file.read_exact_at(offset, &mut hdr)?;
        let len = u32::from_le_bytes(hdr[0..4].try_into().unwrap()) as usize;
        let expect_crc = u32::from_le_bytes(hdr[4..8].try_into().unwrap());
        if len > 64 * 1024 * 1024 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "blob 长度异常",
            ));
        }
        let mut data: Vec<u8> = vec![0u8; len];
        if len > 0 {
            self.file.read_exact_at(offset + 8, &mut data)?;
        }
        if crc32(&data) != expect_crc {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "blob CRC 失败",
            ));
        }
        Ok(data)
    }

    pub fn path(&self) -> &Path {
        self.file.path()
    }

    /// 清空为仅魔数
    pub fn clear_in_place(&mut self) -> std::io::Result<()> {
        self.buf.clear();
        self.file.set_len(0)?;
        self.file.write_all_at(0, BLOB_MAGIC)?;
        self.file.sync_at(SITE_BLOB)?;
        self.end_pos = BLOB_MAGIC.len() as u64;
        Ok(())
    }

    /// 在同一文件上原地重写全部 blob，返回每条新 offset（与 `values` 等长）。
    /// 用于 vacuum compact：只保留仍被引用的大 value。
    pub fn rewrite_in_place(&mut self, values: &[Vec<u8>]) -> std::io::Result<Vec<u64>> {
        self.buf.clear();
        self.file.set_len(0)?;
        self.file.write_all_at(0, BLOB_MAGIC)?;
        let mut offset = BLOB_MAGIC.len() as u64;
        let mut offsets = Vec::with_capacity(values.len());
        for v in values {
            let checksum = crc32(v);
            let mut rec = Vec::with_capacity(8 + v.len());
            rec.extend_from_slice(&(v.len() as u32).to_le_bytes());
            rec.extend_from_slice(&checksum.to_le_bytes());
            rec.extend_from_slice(v);
            self.file.write_all_at(offset, &rec)?;
            offsets.push(offset);
            offset += rec.len() as u64;
        }
        self.file.set_len(offset)?;
        self.file.sync_at(SITE_BLOB)?;
        self.end_pos = offset;
        Ok(offsets)
    }
}

/// 把逻辑 value 编码为存储形式（可能写 blob）
pub fn encode_stored_value(
    blob: &mut BlobStore,
    value: Vec<u8>,
    sync_blob: bool,
) -> std::io::Result<Vec<u8>> {
    if value.len() <= BLOB_THRESHOLD {
        let mut out = Vec::with_capacity(1 + value.len());
        out.push(VAL_TAG_INLINE);
        out.extend_from_slice(&value);
        return Ok(out);
    }
    let offset = blob.append(&value)?;
    if sync_blob {
        blob.sync()?;
    }
    let mut out = Vec::with_capacity(1 + 8 + 4);
    out.push(VAL_TAG_BLOB);
    out.extend_from_slice(&offset.to_le_bytes());
    out.extend_from_slice(&(value.len() as u32).to_le_bytes());
    Ok(out)
}

/// 存储形式 → 逻辑 value
pub fn decode_stored_value(
    blob: &mut BlobStore,
    stored: &[u8],
) -> std::io::Result<Option<Vec<u8>>> {
    if stored.is_empty() {
        return Ok(None);
    }
    match stored[0] {
        VAL_TAG_INLINE => Ok(Some(stored[1..].to_vec())),
        VAL_TAG_BLOB => {
            if stored.len() < 1 + 8 + 4 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "blob 引用截断",
                ));
            }
            let offset = u64::from_le_bytes(stored[1..9].try_into().unwrap());
            let _len = u32::from_le_bytes(stored[9..13].try_into().unwrap());
            Ok(Some(blob.read_at(offset)?))
        }
        // 兼容旧数据：无 tag 的裸 value 当内联
        _ => Ok(Some(stored.to_vec())),
    }
}
