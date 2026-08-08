//! Write-Ahead Log（预写日志）
//!
//! 保证：**Commit 记录 fsync 成功 = 事务持久**，即使数据页尚未刷完。
//! 崩溃恢复时重放已提交事务的 Write，并回滚未提交事务的脏写入。

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::bplus_tree::storage::{crc32, DiskFile};

/// WAL 文件头魔数
const WAL_MAGIC: &[u8; 8] = b"WAL00001";
const WAL_HEADER_SIZE: usize = 32;
// header: magic(8) + next_lsn(8) + reserved(16)

/// 记录帧：len(u32) + crc(u32) + body
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum WalRecord {
    /// 事务开始
    Begin { xid: u64 },
    /// 写入一个版本键（value=None 表示 tombstone 删除）
    Write {
        xid: u64,
        /// 已编码的存储键 encode_key(raw, version)
        key: Vec<u8>,
        value: Option<Vec<u8>>,
    },
    /// 事务提交
    Commit { xid: u64 },
    /// 事务回滚
    Abort { xid: u64 },
    /// 检查点：此 LSN 之前的页已全部落盘，WAL 可截断
    Checkpoint {
        next_version: u64,
        root_page_id: u64,
        next_page_id: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    pub lsn: u64,
    pub record: WalRecord,
}

pub struct Wal {
    file: DiskFile,
    next_lsn: u64,
    /// 是否在本次打开后写过记录（用于判断是否需要恢复）
    dirty: bool,
}

impl Wal {
    /// 打开或创建 WAL
    pub fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let mut file = DiskFile::open(path)?;
        let next_lsn = if file.len()? == 0 {
            Self::write_header(&mut file, 1)?;
            1
        } else {
            let mut hdr = vec![0u8; WAL_HEADER_SIZE];
            file.read_exact_at(0, &mut hdr)?;
            if &hdr[0..8] != WAL_MAGIC {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "非法 WAL 文件魔数",
                ));
            }
            // next_lsn 在 header 中只是提示；真实值以扫描为准
            Self::scan_next_lsn(&mut file)?
        };
        Ok(Self {
            file,
            next_lsn,
            dirty: false,
        })
    }

    fn write_header(file: &mut DiskFile, next_lsn: u64) -> std::io::Result<()> {
        let mut hdr = vec![0u8; WAL_HEADER_SIZE];
        hdr[0..8].copy_from_slice(WAL_MAGIC);
        hdr[8..16].copy_from_slice(&next_lsn.to_le_bytes());
        file.write_all_at(0, &hdr)?;
        file.sync()?;
        Ok(())
    }

    fn scan_next_lsn(file: &mut DiskFile) -> std::io::Result<u64> {
        let entries = Self::read_all_entries(file)?;
        Ok(entries.last().map(|e| e.lsn + 1).unwrap_or(1))
    }

    /// 追加一条记录（**不** fsync；调用方在 commit 时统一 sync）
    pub fn append(&mut self, record: WalRecord) -> std::io::Result<u64> {
        let lsn = self.next_lsn;
        self.next_lsn += 1;
        let entry = WalEntry { lsn, record };
        let body = bincode::serialize(&entry).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
        })?;
        let checksum = crc32(&body);
        let mut frame = Vec::with_capacity(8 + body.len());
        frame.extend_from_slice(&(body.len() as u32).to_le_bytes());
        frame.extend_from_slice(&checksum.to_le_bytes());
        frame.extend_from_slice(&body);
        self.file.append(&frame)?;
        self.dirty = true;
        Ok(lsn)
    }

    /// fsync WAL（commit 的持久化点）
    pub fn sync(&mut self) -> std::io::Result<()> {
        // 更新 header 中的 next_lsn 提示
        let mut hdr = vec![0u8; WAL_HEADER_SIZE];
        hdr[0..8].copy_from_slice(WAL_MAGIC);
        hdr[8..16].copy_from_slice(&self.next_lsn.to_le_bytes());
        self.file.write_all_at(0, &hdr)?;
        self.file.sync()?;
        Ok(())
    }

    /// 读取全部完整记录（损坏的尾部帧丢弃，模拟崩溃截断）
    pub fn read_all(&mut self) -> std::io::Result<Vec<WalEntry>> {
        Self::read_all_entries(&mut self.file)
    }

    fn read_all_entries(file: &mut DiskFile) -> std::io::Result<Vec<WalEntry>> {
        let len = file.len()?;
        if len <= WAL_HEADER_SIZE as u64 {
            return Ok(Vec::new());
        }
        let mut offset = WAL_HEADER_SIZE as u64;
        let mut out = Vec::new();
        while offset + 8 <= len {
            let mut header = [0u8; 8];
            if file.read_exact_at(offset, &mut header).is_err() {
                break;
            }
            let body_len = u32::from_le_bytes(header[0..4].try_into().unwrap()) as u64;
            let expect_crc = u32::from_le_bytes(header[4..8].try_into().unwrap());
            if body_len == 0 || body_len > 16 * 1024 * 1024 {
                break; // 非法
            }
            if offset + 8 + body_len > len {
                break; // 不完整帧（崩溃截断）
            }
            let mut body = vec![0u8; body_len as usize];
            if file.read_exact_at(offset + 8, &mut body).is_err() {
                break;
            }
            if crc32(&body) != expect_crc {
                break; // 损坏帧
            }
            match bincode::deserialize::<WalEntry>(&body) {
                Ok(entry) => out.push(entry),
                Err(_) => break,
            }
            offset += 8 + body_len;
        }
        Ok(out)
    }

    /// 检查点后截断 WAL（只保留 header）
    pub fn truncate(&mut self) -> std::io::Result<()> {
        self.file.set_len(WAL_HEADER_SIZE as u64)?;
        Self::write_header(&mut self.file, self.next_lsn)?;
        self.dirty = false;
        Ok(())
    }

    pub fn next_lsn(&self) -> u64 {
        self.next_lsn
    }

    pub fn path(&self) -> &Path {
        self.file.path()
    }
}

/// 恢复分析结果
#[derive(Debug, Default)]
pub struct RecoveryPlan {
    /// 已提交事务的全部 Write（按 LSN 序）
    pub committed_writes: Vec<(u64, Vec<u8>, Option<Vec<u8>>)>, // (xid, key, value)
    /// 未提交事务的 Write（需要从数据文件撤销）
    pub uncommitted_writes: Vec<(u64, Vec<u8>)>, // (xid, key)
    /// WAL 中见过的最大 xid
    pub max_xid: u64,
    /// 最后一个 Checkpoint（若有）
    pub last_checkpoint: Option<WalRecord>,
}

/// 根据 WAL 记录生成恢复计划
pub fn analyze_recovery(entries: &[WalEntry]) -> RecoveryPlan {
    use std::collections::{HashMap, HashSet};

    let mut plan = RecoveryPlan::default();
    let mut open_writes: HashMap<u64, Vec<(Vec<u8>, Option<Vec<u8>>)>> = HashMap::new();
    let mut committed: HashSet<u64> = HashSet::new();

    for e in entries {
        match &e.record {
            WalRecord::Begin { xid } => {
                plan.max_xid = plan.max_xid.max(*xid);
                open_writes.entry(*xid).or_default();
            }
            WalRecord::Write { xid, key, value } => {
                plan.max_xid = plan.max_xid.max(*xid);
                open_writes
                    .entry(*xid)
                    .or_default()
                    .push((key.clone(), value.clone()));
            }
            WalRecord::Commit { xid } => {
                plan.max_xid = plan.max_xid.max(*xid);
                if let Some(writes) = open_writes.remove(xid) {
                    for (k, v) in writes {
                        plan.committed_writes.push((*xid, k, v));
                    }
                }
                committed.insert(*xid);
            }
            WalRecord::Abort { xid } => {
                plan.max_xid = plan.max_xid.max(*xid);
                if let Some(writes) = open_writes.remove(xid) {
                    for (k, _) in writes {
                        plan.uncommitted_writes.push((*xid, k));
                    }
                }
            }
            WalRecord::Checkpoint { .. } => {
                plan.last_checkpoint = Some(e.record.clone());
            }
        }
    }

    // 仍未结束的事务 → 全部视为需要撤销
    for (xid, writes) in open_writes {
        if !committed.contains(&xid) {
            for (k, _) in writes {
                plan.uncommitted_writes.push((xid, k));
            }
        }
    }
    plan
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_wal(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "wal_{tag}_{}_{}.wal",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[test]
    fn test_wal_append_and_read() {
        let path = tmp_wal("basic");
        {
            let mut wal = Wal::open(&path).unwrap();
            wal.append(WalRecord::Begin { xid: 1 }).unwrap();
            wal.append(WalRecord::Write {
                xid: 1,
                key: b"k".to_vec(),
                value: Some(b"v".to_vec()),
            })
            .unwrap();
            wal.append(WalRecord::Commit { xid: 1 }).unwrap();
            wal.sync().unwrap();
        }
        {
            let mut wal = Wal::open(&path).unwrap();
            let entries = wal.read_all().unwrap();
            assert_eq!(entries.len(), 3);
            let plan = analyze_recovery(&entries);
            assert_eq!(plan.committed_writes.len(), 1);
            assert!(plan.uncommitted_writes.is_empty());
            assert_eq!(plan.max_xid, 1);
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_recovery_undo_uncommitted() {
        let path = tmp_wal("undo");
        {
            let mut wal = Wal::open(&path).unwrap();
            wal.append(WalRecord::Begin { xid: 1 }).unwrap();
            wal.append(WalRecord::Write {
                xid: 1,
                key: b"a".to_vec(),
                value: Some(b"1".to_vec()),
            })
            .unwrap();
            wal.append(WalRecord::Commit { xid: 1 }).unwrap();

            wal.append(WalRecord::Begin { xid: 2 }).unwrap();
            wal.append(WalRecord::Write {
                xid: 2,
                key: b"b".to_vec(),
                value: Some(b"2".to_vec()),
            })
            .unwrap();
            // 无 Commit → 崩溃
            wal.sync().unwrap();
        }
        {
            let mut wal = Wal::open(&path).unwrap();
            let plan = analyze_recovery(&wal.read_all().unwrap());
            assert_eq!(plan.committed_writes.len(), 1);
            assert_eq!(plan.uncommitted_writes.len(), 1);
            assert_eq!(plan.uncommitted_writes[0].1, b"b");
        }
        let _ = std::fs::remove_file(&path);
    }
}
