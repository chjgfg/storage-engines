//! 本文件负责大值旁文件读写及回收。

use super::*;

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
    /// 只补目标尚缺的可见键，并保留现有目标记录及其 TTL。
    /// 墓碑也属于已观察状态；该接口不是删除双向对账协议，不能保证回收后的删除标记可继续传播。

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
