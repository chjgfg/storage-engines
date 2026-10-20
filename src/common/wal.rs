//! 四引擎共用的 WAL：帧格式带 CRC32，记录类型 `Begin` / `Write` / `Commit` / `Abort` / `Checkpoint`。
//!
//! 保证：**Commit 记录 fsync 成功 = 事务持久**。数据页/日志可以之后慢慢刷；反过来说，
//! WAL 一旦被截断，未落盘的行就再无重放来源 —— 所以 checkpoint 的顺序永远是
//! 「先落数据，最后才收口 WAL」。
//!
//! 原来三个磁盘引擎各有一份 ~335 行的实现，`diff` 实测彼此只差 14–16 行（注释与 import 路径），
//! 连 `WAL_MAGIC` 和帧头都是同一个 —— 也就是说这本来就是同一个格式的三份复制。
//! 崩溃恢复是最不该有三分之一的地方：一处修了截断顺序、另两处继续把刚写的 Checkpoint
//! 自己 truncate 抹掉，这类事故本仓库真的发生过。
//!
//! Checkpoint 记录里的 `root_page_id` / `next_page_id` 只有 B+ 树用得上，
//! Bitcask / LSM 恒写 0 —— 保留同一个布局，不为这个再开一个记录类型。

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::common::disk::{crc32, DiskFile, SITE_WAL_HEADER, SITE_WAL_SYNC};

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
    /// 检查点：此 LSN 之前的数据已全部落盘，WAL 可截断
    Checkpoint {
        next_version: u64,
        /// 页号：只有 B+ 树用得上，Bitcask / LSM 恒为 0（同一个记录布局）
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
    /// 是否在本次打开后写过记录（决定 open 后要不要走恢复）
    dirty: bool,
}

impl Wal {
    /// 打开或创建 WAL
    pub fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let mut file = DiskFile::open(path)?;
        let next_lsn = if file.len()? == 0 {
            Self::write_header(&mut file, 1, SITE_WAL_HEADER)?;
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

    /// 写 WAL 头部并 **fsync**。调用方要说清是哪一格在付（M53 的尺子）：新建时（`open`）是一格。
    /// 收口那条路用 `write_header_bytes`（收缩与新记录由后面那一次 `sync` 一起冲下去）。
    fn write_header(file: &mut DiskFile, next_lsn: u64, site: usize) -> std::io::Result<()> {
        Self::write_header_bytes(file, next_lsn)?;
        file.sync_at(site)
    }

    /// 只写头部，不落盘（`sync` / `sync_at` 负责那一格）
    fn write_header_bytes(file: &mut DiskFile, next_lsn: u64) -> std::io::Result<()> {
        let mut hdr = vec![0u8; WAL_HEADER_SIZE];
        hdr[0..8].copy_from_slice(WAL_MAGIC);
        hdr[8..16].copy_from_slice(&next_lsn.to_le_bytes());
        file.write_all_at(0, &hdr)
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
        self.file.sync_at(SITE_WAL_SYNC)?;
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

    /// 检查点后截断 WAL（只保留 header）—— **不落盘**，调用方必须紧接着 `sync()`。
    ///
    /// ⚠️ M53：改前这里自带一次 fsync，而收口的下一步 `Wal::sync()` 又一次 ⇒ **同一趟收口付两遍**
    /// （一次 open、一次 `Drop`、每次阈值 checkpoint 都付）。合成一次是安全的，理由写在下面这句里：
    /// 收口只在**数据已经 flush 之后**发生（三家的 `run_checkpoint` 都把顺序钉死了）。所以"收缩晚一点
    /// 才落盘"最坏的后果是掉电后 WAL 里还留着那批**已经落进数据文件**的旧记录，下次 open 把它们
    /// 再重放一遍 —— MVCC 的键是 `raw ‖ version_be`，同一批记录重放出来的键与值一字不差 ⇒ 幂等；
    /// 而起点取的是 `max(WAL 里的 Checkpoint, max_xid + 1)`，旧记录的 `xid ≤ Checkpoint.next_version`
    /// ⇒ 起点不会被算低，不会复用版本号。**反过来说：没有"数据已落盘"这条前提就不许用这个口子。**
    /// 需要"只截断、不写任何新记录"又要求持久的调用方，自己补一次 `sync()`。
    pub fn truncate(&mut self) -> std::io::Result<()> {
        self.file.set_len(WAL_HEADER_SIZE as u64)?;
        Self::write_header_bytes(&mut self.file, self.next_lsn)?;
        self.dirty = false;
        Ok(())
    }

    pub fn next_lsn(&self) -> u64 {
        self.next_lsn
    }

    /// 当前 WAL 占用字节数（含 header）。用来判断是否该 checkpoint 截断了 ——
    /// WAL 只有被截断才不会无限增长，而恢复路径是**全量扫描**它。
    pub fn size_bytes(&self) -> std::io::Result<u64> {
        self.file.len()
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
            "se_wal_{tag}_{}_{}.wal",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    /// M53 的守卫：**一次收口只许付一次 fsync**。
    ///
    /// 洞：`Wal::truncate()` 改前在重写头部时自带一次 fsync，而收口的下一步 `sync()` 又一次
    /// ⇒ 一次 open、一次 `Drop`、每次阈值 checkpoint 都白付一遍（大库台架是按站点数出来的：
    /// `wal_header(截断里重写) 1 + wal_sync(收口) 1`）。
    /// 判据是**计数**不是墙钟：本机 `C:`/`D:` 的 fsync 单价差 33 倍，毫秒没有可比性，次数有。
    ///
    /// 证人（尺子自己得先接得上）：同一条路上新建 WAL 必须数到一次 `wal_header` ——
    /// 数不到的话下面那个 `wal_sync == 1` 就只是"量不到"（M38/M48：读数 0 有两种解释）。
    #[test]
    fn wal_seal_fsyncs_exactly_once() {
        use crate::common::disk::{
            fsync_counts, fsync_counts_clear, SITE_WAL_HEADER, SITE_WAL_SYNC, SITE_WAL_TRUNCATE,
        };
        let path = tmp_wal("seal_fsync");
        fsync_counts_clear();
        let mut wal = Wal::open(&path).unwrap();
        let fresh = fsync_counts();
        assert_eq!(
            fresh[SITE_WAL_HEADER],
            1,
            "证人失败：新建 WAL 那一次 fsync 都没数到（{:?}）⇒ 尺子没接在门上",
            fresh
        );

        // 收口的形状：三家的 `seal_wal_checkpoint` 都是这三步
        fsync_counts_clear();
        wal.truncate().unwrap();
        wal.append(WalRecord::Checkpoint {
            next_version: 7,
            root_page_id: 0,
            next_page_id: 0,
        })
        .unwrap();
        wal.sync().unwrap();
        let c = fsync_counts();
        assert_eq!(
            c[SITE_WAL_TRUNCATE],
            0,
            "truncate 又自己 fsync 了 ⇒ 同一趟收口付两遍（M53 改前的形状）"
        );
        assert_eq!(
            c[SITE_WAL_SYNC],
            1,
            "收口那一次 fsync 是唯一的持久化点：现在读到 {} 次（0 = 把持久化点省掉了，比慢更糟）",
            c[SITE_WAL_SYNC]
        );
        assert_eq!(c.iter().sum::<u64>(), 1, "这一趟收口总账：{:?}", c);

        // 语义一字未动：截断之后 WAL 里就剩那条 Checkpoint
        let entries = wal.read_all().unwrap();
        assert_eq!(entries.len(), 1, "截断没生效：还剩 {} 条", entries.len());
        assert!(
            matches!(
                entries[0].record,
                WalRecord::Checkpoint {
                    next_version: 7,
                    ..
                }
            ),
            "收口写进去的那条不是 Checkpoint：{:?}",
            entries[0].record
        );
        let _ = std::fs::remove_file(&path);
    }

    /// M25 守卫：**恢复计划不许依赖 `Begin`**。
    ///
    /// 同一条事务流写两遍 —— 一遍带 `Begin`（0.4.0~M24 的形状，老库里就是这么存的），一遍不带 ——
    /// 再从文件里读回来折 plan，**REDO 与 UNDO 两边必须一字不差**。
    ///
    /// 为什么必须有它：M8 之后 `Begin` 只剩"把 xid 提前登记成一个空条目"这一件事，而 `Write`
    /// 走的也是同一句 `open_writes.entry(xid).or_default()` ⇒ 它承载的信息是零。
    /// **零信息的记录 + 零覆盖的假设** = 下一个人以为"事务开始在日志里有凭据"
    /// （M18d 记在案：把整步作废之后全量测试一条都不红）。写侧从 M25 起不再落 `Begin`，
    /// 而老日志必须还能读 —— 这条就是那半边的守卫，同时也是"变体不能删"的理由。
    ///
    /// ⚠️ 唯一合法的差别是 `max_xid`：**只留下一条 `Begin` 就崩掉**的事务（只有老日志可能有），
    /// 去掉 Begin 之后版本号起点小 1。那一笔一行都没写 ⇒ REDO/UNDO 都不受影响，
    /// 但这条差别必须**看得见**，否则有人会拿它当"Begin 有用"的证据。
    #[test]
    fn plan_is_identical_with_or_without_begin() {
        // 一段真实形状的事务流：1 提交、2 撤销、3 崩在提交之前（只有 Write）、4 只留 Begin 就崩了
        fn script(wal: &mut Wal, with_begin: bool) {
            for xid in 1..=4u64 {
                if with_begin {
                    wal.append(WalRecord::Begin { xid }).unwrap();
                }
                let rec = match xid {
                    1 => WalRecord::Write {
                        xid,
                        key: b"k1".to_vec(),
                        value: Some(b"v1".to_vec()),
                    },
                    2 => WalRecord::Write {
                        xid,
                        key: b"k2".to_vec(),
                        value: None,
                    },
                    3 => WalRecord::Write {
                        xid,
                        key: b"k3".to_vec(),
                        value: Some(b"v3".to_vec()),
                    },
                    // 4 号只有 Begin 就崩了（老日志里那种"补了 Begin 还没写"的形状）
                    _ => continue,
                };
                wal.append(rec).unwrap();
                match xid {
                    1 => {
                        wal.append(WalRecord::Commit { xid }).unwrap();
                    }
                    2 => {
                        wal.append(WalRecord::Abort { xid }).unwrap();
                    }
                    _ => {}
                }
            }
            wal.sync().unwrap();
        }

        fn read_plan(tag: &str, with_begin: bool) -> (usize, RecoveryPlan) {
            let path = tmp_wal(tag);
            {
                let mut wal = Wal::open(&path).unwrap();
                script(&mut wal, with_begin);
            }
            let mut wal = Wal::open(&path).unwrap();
            let entries = wal.read_all().unwrap();
            let plan = analyze_recovery(&entries);
            let _ = std::fs::remove_file(&path);
            (entries.len(), plan)
        }

        let (n_with, with) = read_plan("begin_on", true);
        let (n_without, without) = read_plan("begin_off", false);
        // 带 Begin 的那份必须真读出 Begin（否则这份对拍是空转）
        assert!(
            n_with > n_without,
            "带 Begin 的日志只有 {n_with} 条，不带的有 {n_without} 条 ⇒ Begin 根本没写进去"
        );
        assert_eq!(
            with.committed_writes, without.committed_writes,
            "REDO 列表因为 Begin 的有无而变了 ⇒ 恢复其实依赖它"
        );
        assert_eq!(
            with.uncommitted_writes, without.uncommitted_writes,
            "UNDO 列表因为 Begin 的有无而变了 ⇒ 恢复其实依赖它"
        );
        // 唯一允许的差：那笔"只有 Begin"的事务把 max_xid 抬了一格
        assert_eq!(without.max_xid, 3, "不带 Begin 时日志里最大的 xid 是 3");
        assert_eq!(with.max_xid, 4, "Begin 把 4 号那笔空事务的 xid 也抬进来了");
        assert_eq!(with.committed_writes.len(), 1, "只有 1 号提交");
        assert_eq!(with.uncommitted_writes.len(), 2, "2 号撤销 + 3 号崩在提交前");
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

    /// 崩溃最常留下的现场是**半帧**：写到最后一次 flush 之间被砍断。
    /// 必须丢掉坏尾并保住前面所有完整记录 —— 否则一次断电等于整库恢复失败。
    #[test]
    fn test_corrupt_tail_is_discarded_but_good_frames_survive() {
        let path = tmp_wal("corrupt");
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
            wal.sync().unwrap();
            // 手工接一个"长度合法但 CRC 不对"的残帧，模拟半截写入
            let mut raw = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            use std::io::Write;
            raw.write_all(&[9u8; 4]).unwrap(); // len = 9
            raw.write_all(b"NOT-A-FRAME").unwrap();
            raw.flush().unwrap();
        }
        {
            let mut wal = Wal::open(&path).unwrap();
            let entries = wal.read_all().unwrap();
            assert_eq!(entries.len(), 3, "坏尾之后的完整记录不该被牵连");
            let plan = analyze_recovery(&entries);
            assert_eq!(plan.committed_writes.len(), 1);
            assert_eq!(plan.max_xid, 1);
        }
        let _ = std::fs::remove_file(&path);
    }
}
