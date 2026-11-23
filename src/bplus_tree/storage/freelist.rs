//! 空闲页清单读取与原子发布。

use super::*;

/// 把全部空闲页号写入 sidecar 文件并 fsync
pub fn write_freelist(path: &Path, free_ids: &[u64]) -> std::io::Result<()> {
    FreelistWriter::open(path)?.write(free_ids.iter().copied())
}

impl FreelistWriter {
    pub(crate) fn open(path: &Path) -> std::io::Result<Self> {
        #[cfg(test)]
        let _timer = crate::common::maintenance_probe::Timer::start(
            crate::common::maintenance_probe::Stage::FreelistOpen,
        );
        let file = DiskFile::open(path)?;
        let len = file.len()?;
        Ok(Self { file, len })
    }

    pub(crate) fn write(&mut self, ids: impl ExactSizeIterator<Item = u64>) -> std::io::Result<()> {
        #[cfg(test)]
        let encode = crate::common::maintenance_probe::Timer::start(
            crate::common::maintenance_probe::Stage::FreelistEncode,
        );
        let count = ids.len();
        let mut buf = Vec::with_capacity(20 + count * 8);
        buf.extend_from_slice(FREELIST_MAGIC);
        buf.extend_from_slice(&(count as u64).to_le_bytes());
        buf.extend_from_slice(&[0; 4]);
        for id in ids {
            buf.extend_from_slice(&id.to_le_bytes());
        }
        let checksum = crc32(&buf[20..]);
        buf[16..20].copy_from_slice(&checksum.to_le_bytes());
        #[cfg(test)]
        drop(encode);
        let new_len = buf.len() as u64;
        {
            #[cfg(test)]
            let _timer = crate::common::maintenance_probe::Timer::start(
                crate::common::maintenance_probe::Stage::FreelistWrite,
            );
            #[cfg(test)]
            dblwr_probe::fail_at(6)?;
            self.file.write_all_at(0, &buf)?;
            // 先覆盖写入，只截短变小的尾部，不先截断到零。
            if new_len < self.len {
                self.file.set_len(new_len)?;
            }
        }
        {
            #[cfg(test)]
            let _timer = crate::common::maintenance_probe::Timer::start(
                crate::common::maintenance_probe::Stage::FreelistSync,
            );
            #[cfg(test)]
            dblwr_probe::fail_at(7)?;
            self.file.sync_at(SITE_BPLUS_FREELIST)?;
        }
        self.len = new_len;
        Ok(())
    }
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
