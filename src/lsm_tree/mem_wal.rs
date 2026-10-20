//! MemTable 预写日志（裸 `LSMTree` 用）。
//!
//! 与事务层 `lsm.wal` 分离：
//! - 记录为存储键级 Put/Drop，**带 seq**（崩溃恢复后 snapshot 语义可续）
//! - `MVCC` 打开时关闭本 WAL，避免双写
//!
//! 帧格式：`len u32 + crc u32 + bincode body`。
//!
//! - `MEMWAL01`：旧格式无 seq（读时 seq=0）
//! - `MEMWAL02`：Put/Drop 含 seq；Flush 含 next_sst_id + next_write_seq

use serde::{Deserialize, Serialize};
use std::path::Path;

use crate::common::disk::SITE_LSM_MEM_WAL;
use crate::lsm_tree::storage::{crc32, mem_wal_path, DiskFile};

const MAGIC_V1: &[u8; 8] = b"MEMWAL01";
const MAGIC_V2: &[u8; 8] = b"MEMWAL02";
const HEADER_SIZE: usize = 32;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum MemWalRecord {
    Put {
        key: Vec<u8>,
        value: Option<Vec<u8>>,
        /// 写序号；旧记录缺省反序列化为 0
        #[serde(default)]
        seq: u64,
    },
    Drop {
        key: Vec<u8>,
        #[serde(default)]
        seq: u64,
    },
    /// mem 已成功刷成 SST；之后可 truncate
    Flush {
        next_sst_id: u64,
        /// 下一次写入将使用的 write_seq
        #[serde(default)]
        next_write_seq: u64,
    },
}

pub struct MemWal {
    file: DiskFile,
}

impl MemWal {
    pub fn open(dir: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = mem_wal_path(dir.as_ref());
        let mut file = DiskFile::open(path)?;
        if file.len()? == 0 {
            Self::write_header(&mut file)?;
        } else {
            let mut hdr = vec![0u8; HEADER_SIZE];
            file.read_exact_at(0, &mut hdr)?;
            if &hdr[0..8] != MAGIC_V1 && &hdr[0..8] != MAGIC_V2 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "非法 mem.wal 魔数",
                ));
            }
        }
        Ok(Self { file })
    }

    fn write_header(file: &mut DiskFile) -> std::io::Result<()> {
        let mut hdr = vec![0u8; HEADER_SIZE];
        hdr[0..8].copy_from_slice(MAGIC_V2);
        file.write_all_at(0, &hdr)?;
        file.sync_at(SITE_LSM_MEM_WAL)?;
        Ok(())
    }

    pub fn append(&mut self, record: MemWalRecord) -> std::io::Result<()> {
        let body = bincode::serialize(&record).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
        })?;
        let checksum = crc32(&body);
        let mut frame = Vec::with_capacity(8 + body.len());
        frame.extend_from_slice(&(body.len() as u32).to_le_bytes());
        frame.extend_from_slice(&checksum.to_le_bytes());
        frame.extend_from_slice(&body);
        self.file.append(&frame)?;
        Ok(())
    }

    pub fn sync(&mut self) -> std::io::Result<()> {
        self.file.sync_at(SITE_LSM_MEM_WAL)
    }

    pub fn read_all(&mut self) -> std::io::Result<Vec<MemWalRecord>> {
        let len = self.file.len()?;
        if len <= HEADER_SIZE as u64 {
            return Ok(Vec::new());
        }
        let mut offset = HEADER_SIZE as u64;
        let mut out = Vec::new();
        while offset + 8 <= len {
            let mut header = [0u8; 8];
            if self.file.read_exact_at(offset, &mut header).is_err() {
                break;
            }
            let body_len = u32::from_le_bytes(header[0..4].try_into().unwrap()) as u64;
            let expect_crc = u32::from_le_bytes(header[4..8].try_into().unwrap());
            if body_len == 0 || body_len > 16 * 1024 * 1024 {
                break;
            }
            if offset + 8 + body_len > len {
                break;
            }
            let mut body = vec![0u8; body_len as usize];
            if self.file.read_exact_at(offset + 8, &mut body).is_err() {
                break;
            }
            if crc32(&body) != expect_crc {
                break;
            }
            match bincode::deserialize::<MemWalRecord>(&body) {
                Ok(r) => out.push(r),
                Err(_) => break,
            }
            offset += 8 + body_len;
        }
        Ok(out)
    }

    pub fn truncate(&mut self) -> std::io::Result<()> {
        self.file.set_len(HEADER_SIZE as u64)?;
        Self::write_header(&mut self.file)?;
        Ok(())
    }

    pub fn path(&self) -> &Path {
        self.file.path()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_mem_wal_seq_roundtrip() {
        let dir = std::env::temp_dir().join(format!(
            "memwal_seq_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = fs::create_dir_all(&dir);
        {
            let mut w = MemWal::open(&dir).unwrap();
            w.append(MemWalRecord::Put {
                key: b"a".to_vec(),
                value: Some(b"1".to_vec()),
                seq: 42,
            })
            .unwrap();
            w.append(MemWalRecord::Drop {
                key: b"b".to_vec(),
                seq: 43,
            })
            .unwrap();
            w.append(MemWalRecord::Flush {
                next_sst_id: 7,
                next_write_seq: 44,
            })
            .unwrap();
            w.sync().unwrap();
        }
        {
            let mut w = MemWal::open(&dir).unwrap();
            let recs = w.read_all().unwrap();
            assert_eq!(recs.len(), 3);
            match &recs[0] {
                MemWalRecord::Put { seq, .. } => assert_eq!(*seq, 42),
                _ => panic!(),
            }
            match &recs[2] {
                MemWalRecord::Flush {
                    next_write_seq, ..
                } => assert_eq!(*next_write_seq, 44),
                _ => panic!(),
            }
        }
        let _ = fs::remove_dir_all(&dir);
    }
}
