//! B+ 树的磁盘布局：页头 pack/unpack、双写缓冲、路径辅助、blob 旁路、空闲页表。
//!
//! 字节级工具（`DiskFile` / `FileLock` / `crc32`）不在这里 —— 三个引擎各有一份逐字重复的
//! 实现（bitcask 与 lsm 的 FileLock `diff` 差 0 行），而它们恰恰是崩溃语义的载体
//! （sync 是不是 sync_all、谁在挡第二个进程）。已收进 [`crate::common::disk`]，这里 re-export
//! 以保持 `crate::bplus_tree::storage::DiskFile` 这类既有路径不断。
//!
//! 唯一和引擎绑在一起的是**布局**：主数据文件用 `DiskFile::open_paged(path, PAGE_SIZE)`
//! 声明定宽页，`read_page` / `write_page` 才算得对偏移。

use std::vec;
use std::format;
use std::path::{Path, PathBuf};

pub use crate::common::disk::{crc32, DiskFile, FileLock};

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


// ─── 页头 pack / unpack ─────────────────────────────────────────────────────

/// 把 payload 打成带 CRC 的完整页（4KB）
///
/// 布局：
/// ```text
/// 0..4   magic  "BPG1"
/// 4..8   page_id (u32 LE，页号高 32 位必须为 0，教学简化)
/// 8..12  crc32(payload)
/// 12..16 reserved
/// 16..4096 payload（不足补 0）
/// ```
pub fn pack_page(page_id: u64, payload: &[u8]) -> Vec<u8> {
    assert!(payload.len() <= PAGE_PAYLOAD_SIZE, "payload 超过页载荷上限");
    let mut page = vec![0u8; PAGE_SIZE];
    page[0..4].copy_from_slice(&PAGE_MAGIC.to_le_bytes());
    page[4..8].copy_from_slice(&(page_id as u32).to_le_bytes());
    // reserved 先置 0
    let mut body = [0u8; PAGE_PAYLOAD_SIZE];
    body[..payload.len()].copy_from_slice(payload);
    let checksum = crc32(&body);
    page[8..12].copy_from_slice(&checksum.to_le_bytes());
    page[PAGE_HEADER_SIZE..].copy_from_slice(&body);
    page
}

/// 校验并拆页，返回 (page_id, payload_owned)
pub fn unpack_page(raw: &[u8]) -> Result<(u64, Vec<u8>), String> {
    if raw.len() != PAGE_SIZE {
        return Err(format!("页长度错误: {} != {}", raw.len(), PAGE_SIZE));
    }
    let magic = u32::from_le_bytes(raw[0..4].try_into().unwrap());
    if magic != PAGE_MAGIC {
        // 全 0 页视为空页（新分配尚未写有效数据）
        if raw.iter().all(|&b| b == 0) {
            return Ok((0, vec![0u8; PAGE_PAYLOAD_SIZE]));
        }
        return Err(format!("页魔数错误: {magic:#x}"));
    }
    let page_id = u32::from_le_bytes(raw[4..8].try_into().unwrap()) as u64;
    let expect_crc = u32::from_le_bytes(raw[8..12].try_into().unwrap());
    let payload = &raw[PAGE_HEADER_SIZE..];
    let actual_crc = crc32(payload);
    if expect_crc != actual_crc {
        return Err(format!(
            "页 CRC 校验失败 page_id={page_id}: expect={expect_crc:#x} actual={actual_crc:#x}"
        ));
    }
    Ok((page_id, payload.to_vec()))
}

/// 只校验 CRC，不拆 payload
pub fn verify_page(raw: &[u8]) -> bool {
    unpack_page(raw).is_ok()
}


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

pub struct DoubleWriteBuffer {
    file: DiskFile,
}

impl DoubleWriteBuffer {
    pub fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let mut file = DiskFile::open(path)?;
        if file.len()? == 0 {
            // 初始化空 header
            let mut hdr = vec![0u8; DBLWR_HEADER_SIZE];
            hdr[0..8].copy_from_slice(DBLWR_MAGIC);
            file.write_all_at(0, &hdr)?;
            file.sync_at(SITE_BPLUS_DBLWR)?;
        }
        Ok(Self { file })
    }

    /// 把一批**已 pack** 的完整页写入双写区并 fsync
    pub fn write_batch(&mut self, pages: &[(u64, Vec<u8>)]) -> std::io::Result<()> {
        assert!(pages.len() <= DBLWR_MAX_PAGES);
        for (_, p) in pages {
            assert_eq!(p.len(), PAGE_SIZE);
        }

        let mut hdr = vec![0u8; DBLWR_HEADER_SIZE];
        hdr[0..8].copy_from_slice(DBLWR_MAGIC);
        hdr[8..12].copy_from_slice(&(pages.len() as u32).to_le_bytes());

        // 先把 count 写成 0，避免半写入状态被当成有效 batch
        let mut zero_hdr = hdr.clone();
        zero_hdr[8..12].copy_from_slice(&0u32.to_le_bytes());
        self.file.write_all_at(0, &zero_hdr)?;
        self.file.sync_at(SITE_BPLUS_DBLWR)?;

        // 写各页：每槽 = page_id(8) + page(PAGE_SIZE)
        let slot_size = 8 + PAGE_SIZE;
        for (i, (page_id, page)) in pages.iter().enumerate() {
            let offset = (DBLWR_HEADER_SIZE + i * slot_size) as u64;
            let mut slot = vec![0u8; slot_size];
            slot[0..8].copy_from_slice(&page_id.to_le_bytes());
            slot[8..].copy_from_slice(page);
            self.file.write_all_at(offset, &slot)?;
        }
        self.file.sync_at(SITE_BPLUS_DBLWR)?;

        // 最后写真实 count 并 fsync → 标志着 batch 完整
        self.file.write_all_at(0, &hdr)?;
        self.file.sync_at(SITE_BPLUS_DBLWR)?;
        Ok(())
    }

    /// 读取 batch 中的有效页（page_id, raw_page）
    pub fn read_batch(&mut self) -> std::io::Result<Vec<(u64, Vec<u8>)>> {
        let len = self.file.len()?;
        if len < DBLWR_HEADER_SIZE as u64 {
            return Ok(Vec::new());
        }
        let mut hdr = vec![0u8; DBLWR_HEADER_SIZE];
        self.file.read_exact_at(0, &mut hdr)?;
        if &hdr[0..8] != DBLWR_MAGIC {
            return Ok(Vec::new());
        }
        let count = u32::from_le_bytes(hdr[8..12].try_into().unwrap()) as usize;
        if count == 0 || count > DBLWR_MAX_PAGES {
            return Ok(Vec::new());
        }

        let slot_size = 8 + PAGE_SIZE;
        let mut out = Vec::with_capacity(count);
        for i in 0..count {
            let offset = (DBLWR_HEADER_SIZE + i * slot_size) as u64;
            if offset + slot_size as u64 > len {
                break;
            }
            let mut slot = vec![0u8; slot_size];
            self.file.read_exact_at(offset, &mut slot)?;
            let page_id = u64::from_le_bytes(slot[0..8].try_into().unwrap());
            let page = slot[8..].to_vec();
            // 只接受 CRC 合法的页
            if verify_page(&page) {
                out.push((page_id, page));
            }
        }
        Ok(out)
    }

    /// 清空 batch（flush 主文件成功后调用）
    pub fn clear(&mut self) -> std::io::Result<()> {
        let mut hdr = vec![0u8; DBLWR_HEADER_SIZE];
        hdr[0..8].copy_from_slice(DBLWR_MAGIC);
        // count = 0
        self.file.write_all_at(0, &hdr)?;
        self.file.sync_at(SITE_BPLUS_DBLWR)?;
        Ok(())
    }
}

// ─── 路径辅助 ───────────────────────────────────────────────────────────────

/// `data.db` → `data.db.dblwr`
pub fn dblwr_path(db_path: &Path) -> PathBuf {
    let mut s = db_path.as_os_str().to_os_string();
    s.push(".dblwr");
    PathBuf::from(s)
}

/// `data.db` → `data.db.wal`
pub fn wal_path(db_path: &Path) -> PathBuf {
    let mut s = db_path.as_os_str().to_os_string();
    s.push(".wal");
    PathBuf::from(s)
}

/// `data.db` → `data.db.freelist`
pub fn freelist_path(db_path: &Path) -> PathBuf {
    let mut s = db_path.as_os_str().to_os_string();
    s.push(".freelist");
    PathBuf::from(s)
}

/// `data.db` → `data.db.lock`
pub fn lock_path(db_path: &Path) -> PathBuf {
    let mut s = db_path.as_os_str().to_os_string();
    s.push(".lock");
    PathBuf::from(s)
}

/// `data.db` → `data.db.blob`
pub fn blob_path(db_path: &Path) -> PathBuf {
    let mut s = db_path.as_os_str().to_os_string();
    s.push(".blob");
    PathBuf::from(s)
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

impl BlobStore {
    pub fn open(db_path: &Path) -> std::io::Result<Self> {
        let path = blob_path(db_path);
        let mut file = DiskFile::open(&path)?;
        if file.len()? == 0 {
            file.write_all_at(0, BLOB_MAGIC)?;
            file.sync_at(SITE_BLOB)?;
        } else {
            let mut magic = [0u8; 8];
            file.read_exact_at(0, &mut magic)?;
            if &magic != BLOB_MAGIC {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("非法 blob 文件: {}", path.display()),
                ));
            }
        }
        Ok(Self { file })
    }

    /// 追加一条 blob，返回文件内 offset（记录起点）
    pub fn append(&mut self, data: &[u8]) -> std::io::Result<u64> {
        let offset = self.file.len()?;
        let checksum = crc32(data);
        let mut rec = Vec::with_capacity(8 + data.len());
        rec.extend_from_slice(&(data.len() as u32).to_le_bytes());
        rec.extend_from_slice(&checksum.to_le_bytes());
        rec.extend_from_slice(data);
        self.file.write_all_at(offset, &rec)?;
        Ok(offset)
    }

    pub fn sync(&mut self) -> std::io::Result<()> {
        self.file.sync_at(SITE_BLOB)
    }

    /// 按 offset 读取 blob 记录
    pub fn read_at(&mut self, offset: u64) -> std::io::Result<Vec<u8>> {
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

    /// 在**已打开**的同一文件上原地重写全部 blob 内容，返回新 offsets。
    pub fn rewrite_in_place(&mut self, values: &[Vec<u8>]) -> std::io::Result<Vec<u64>> {
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
        // 截断到实际长度（去掉旧文件尾部垃圾）
        self.file.set_len(offset)?;
        self.file.sync_at(SITE_BLOB)?;
        Ok(offsets)
    }

    /// 清空为仅魔数
    pub fn clear_in_place(&mut self) -> std::io::Result<()> {
        self.rewrite_in_place(&[])?;
        Ok(())
    }

    /// 写入全新 blob 文件到 `*.blob.new`（不碰正在打开的旧文件），返回 (new_path, offsets)
    pub fn write_new_blob_file(
        db_path: &Path,
        values: &[Vec<u8>],
    ) -> std::io::Result<(PathBuf, Vec<u64>)> {
        let final_path = blob_path(db_path);
        let mut new_path = final_path.as_os_str().to_os_string();
        new_path.push(".new");
        let new_path = PathBuf::from(new_path);
        let _ = std::fs::remove_file(&new_path);

        let mut file = DiskFile::open(&new_path)?;
        file.set_len(0)?;
        file.write_all_at(0, BLOB_MAGIC)?;
        let mut offsets = Vec::with_capacity(values.len());
        for v in values {
            let offset = file.len()?;
            let checksum = crc32(v);
            let mut rec = Vec::with_capacity(8 + v.len());
            rec.extend_from_slice(&(v.len() as u32).to_le_bytes());
            rec.extend_from_slice(&checksum.to_le_bytes());
            rec.extend_from_slice(v);
            file.write_all_at(offset, &rec)?;
            offsets.push(offset);
        }
        file.sync_at(SITE_BLOB)?;
        drop(file);
        Ok((new_path, offsets))
    }

    /// 关闭旧句柄后，将 `*.blob.new` 替换为 `*.blob` 并重新 open
    pub fn install_new_blob_file(db_path: &Path, new_path: &Path) -> std::io::Result<Self> {
        let final_path = blob_path(db_path);
        if final_path.exists() {
            std::fs::remove_file(&final_path)?;
        }
        std::fs::rename(new_path, &final_path)?;
        BlobStore::open(db_path)
    }

    /// 用新文件替换（调用方须已 drop 旧 BlobStore）
    pub fn replace_with_values(
        db_path: &Path,
        values: &[Vec<u8>],
    ) -> std::io::Result<(Self, Vec<u64>)> {
        let (new_path, offsets) = Self::write_new_blob_file(db_path, values)?;
        let store = Self::install_new_blob_file(db_path, &new_path)?;
        Ok((store, offsets))
    }

    /// 清空并重建为空 blob 文件（无大 value 引用时）
    pub fn reset_empty(db_path: &Path) -> std::io::Result<Self> {
        let path = blob_path(db_path);
        let _ = std::fs::remove_file(&path);
        BlobStore::open(db_path)
    }
}

/// 把逻辑 value 编码为叶子存储形式（可能写 blob）
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
        blob.sync()?; // BlobStore::sync 内部已经记 SITE_BLOB 这一格
    }
    let mut out = Vec::with_capacity(1 + 8 + 4);
    out.push(VAL_TAG_BLOB);
    out.extend_from_slice(&offset.to_le_bytes());
    out.extend_from_slice(&(value.len() as u32).to_le_bytes());
    Ok(out)
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

/// 当前线程已发生的 value 解码次数（测试探针用）
#[cfg(test)]
pub(crate) fn value_decodes() -> u64 {
    VALUE_DECODES.with(|c| c.get())
}

/// 叶子存储形式 → 逻辑 value
pub fn decode_stored_value(
    blob: &mut BlobStore,
    stored: &[u8],
) -> std::io::Result<Option<Vec<u8>>> {
    #[cfg(test)]
    VALUE_DECODES.with(|c| c.set(c.get() + 1));
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

/// 把全部空闲页号写入 sidecar 文件并 fsync
pub fn write_freelist(path: &Path, free_ids: &[u64]) -> std::io::Result<()> {
    let mut body = Vec::with_capacity(free_ids.len() * 8);
    for id in free_ids {
        body.extend_from_slice(&id.to_le_bytes());
    }
    let checksum = crc32(&body);

    let mut buf = Vec::with_capacity(20 + body.len());
    buf.extend_from_slice(FREELIST_MAGIC);
    buf.extend_from_slice(&(free_ids.len() as u64).to_le_bytes());
    buf.extend_from_slice(&checksum.to_le_bytes());
    buf.extend_from_slice(&body);

    let mut file = DiskFile::open(path)?;
    // 先截断再写，避免旧文件更长时残留垃圾
    file.set_len(0)?;
    file.write_all_at(0, &buf)?;
    file.sync_at(SITE_BPLUS_FREELIST)?;
    Ok(())
}

/// 读取 sidecar 空闲页列表；文件不存在返回 Ok(None)
pub fn read_freelist(path: &Path) -> std::io::Result<Option<Vec<u64>>> {
    if !path.exists() {
        return Ok(None);
    }
    let mut file = match DiskFile::open_existing(path)? {
        Some(f) => f,
        None => return Ok(None),
    };
    let len = file.len()?;
    if len < 20 {
        return Ok(None);
    }
    let mut hdr = [0u8; 20];
    file.read_exact_at(0, &mut hdr)?;
    if &hdr[0..8] != FREELIST_MAGIC {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "freelist 魔数错误",
        ));
    }
    let count = u64::from_le_bytes(hdr[8..16].try_into().unwrap()) as usize;
    let expect_crc = u32::from_le_bytes(hdr[16..20].try_into().unwrap());
    let need = 20 + count * 8;
    if len < need as u64 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "freelist 文件截断",
        ));
    }
    let mut body = vec![0u8; count * 8];
    if count > 0 {
        file.read_exact_at(20, &mut body)?;
    }
    if crc32(&body) != expect_crc {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "freelist CRC 校验失败",
        ));
    }
    let mut ids = Vec::with_capacity(count);
    for chunk in body.chunks_exact(8) {
        ids.push(u64::from_le_bytes(chunk.try_into().unwrap()));
    }
    Ok(Some(ids))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc32_known() {
        // "123456789" 的 IEEE CRC32 = 0xCBF43926
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn test_pack_unpack_roundtrip() {
        let payload = b"hello b+tree page";
        let page = pack_page(7, payload);
        assert_eq!(page.len(), PAGE_SIZE);
        let (id, body) = unpack_page(&page).unwrap();
        assert_eq!(id, 7);
        assert_eq!(&body[..payload.len()], payload);
    }

    #[test]
    fn test_crc_detects_corruption() {
        let mut page = pack_page(1, b"data");
        page[100] ^= 0xFF; // 破坏 payload
        assert!(unpack_page(&page).is_err());
    }

    #[test]
    fn test_disk_file_rw() {
        let path = std::env::temp_dir().join(format!(
            "storage_test_{}.bin",
            std::process::id()
        ));
        {
            let mut f = DiskFile::open(&path).unwrap();
            f.write_all_at(0, b"abcdef").unwrap();
            f.sync().unwrap();
            let mut buf = [0u8; 6];
            f.read_exact_at(0, &mut buf).unwrap();
            assert_eq!(&buf, b"abcdef");
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_dblwr_batch() {
        let path = std::env::temp_dir().join(format!(
            "dblwr_test_{}.bin",
            std::process::id()
        ));
        {
            let mut dw = DoubleWriteBuffer::open(&path).unwrap();
            let p1 = pack_page(1, b"aaa");
            let p2 = pack_page(2, b"bbb");
            dw.write_batch(&[(1, p1.clone()), (2, p2.clone())])
                .unwrap();
            let batch = dw.read_batch().unwrap();
            assert_eq!(batch.len(), 2);
            assert_eq!(batch[0].0, 1);
            assert_eq!(batch[1].0, 2);
            dw.clear().unwrap();
            assert!(dw.read_batch().unwrap().is_empty());
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_freelist_roundtrip() {
        let path = std::env::temp_dir().join(format!(
            "freelist_test_{}.bin",
            std::process::id()
        ));
        let ids: Vec<u64> = (1..1000).collect();
        write_freelist(&path, &ids).unwrap();
        let loaded = read_freelist(&path).unwrap().unwrap();
        assert_eq!(loaded, ids);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_blob_store_large_value() {
        let db = std::env::temp_dir().join(format!("blob_db_{}", std::process::id()));
        let _ = std::fs::remove_file(blob_path(&db));
        let mut store = BlobStore::open(&db).unwrap();
        let data = vec![7u8; 5000];
        let off = store.append(&data).unwrap();
        store.sync().unwrap();
        let got = store.read_at(off).unwrap();
        assert_eq!(got, data);

        let stored = encode_stored_value(&mut store, data.clone(), true).unwrap();
        assert_eq!(stored[0], VAL_TAG_BLOB);
        let decoded = decode_stored_value(&mut store, &stored).unwrap().unwrap();
        assert_eq!(decoded, data);

        let small = encode_stored_value(&mut store, b"ok".to_vec(), false).unwrap();
        assert_eq!(small[0], VAL_TAG_INLINE);
        assert_eq!(
            decode_stored_value(&mut store, &small).unwrap().unwrap(),
            b"ok"
        );
        let _ = std::fs::remove_file(blob_path(&db));
    }
}
