//! 事务 WAL 记录追加、提交同步与 checkpoint。

use super::*;

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
            poisoned: false,
        })
    }

    /// 写 WAL 头部并 **fsync**。调用方要说清是哪一格在付（M53 的尺子）：新建时（`open`）是一格。
    /// 收口那条路用 `write_header_bytes`（收缩与新记录由后面那一次 `sync` 一起冲下去）。
    pub(super) fn write_header(
        file: &mut DiskFile,
        next_lsn: u64,
        site: usize,
    ) -> std::io::Result<()> {
        Self::write_header_bytes(file, next_lsn)?;
        file.sync_at(site)
    }

    /// 只写头部，不落盘（`sync` / `sync_at` 负责那一格）
    pub(super) fn write_header_bytes(file: &mut DiskFile, next_lsn: u64) -> std::io::Result<()> {
        let mut hdr = vec![0u8; WAL_HEADER_SIZE];
        hdr[0..8].copy_from_slice(WAL_MAGIC);
        hdr[8..16].copy_from_slice(&next_lsn.to_le_bytes());
        file.write_all_at(0, &hdr)
    }

    pub(super) fn scan_next_lsn(file: &mut DiskFile) -> std::io::Result<u64> {
        let entries = Self::read_all_entries(file)?;
        Ok(entries.last().map(|e| e.lsn + 1).unwrap_or(1))
    }

    /// 追加一条记录（**不** fsync；调用方在 commit 时统一 sync）
    pub fn append(&mut self, record: WalRecord) -> std::io::Result<u64> {
        self.ensure_healthy()?;
        let result = self.append_inner(record);
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    pub(super) fn append_inner(&mut self, record: WalRecord) -> std::io::Result<u64> {
        let lsn = self.next_lsn;
        self.next_lsn += 1;
        let entry = WalEntry { lsn, record };
        let body = bincode::serialize(&entry)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        let checksum = crc32(&body);
        let mut frame = Vec::with_capacity(8 + body.len());
        frame.extend_from_slice(&(body.len() as u32).to_le_bytes());
        frame.extend_from_slice(&checksum.to_le_bytes());
        frame.extend_from_slice(&body);
        self.file.append(&frame)?;
        self.dirty = true;
        Ok(lsn)
    }
    /// 只补目标尚缺的可见键，并保留现有目标记录及其 TTL。
    /// 墓碑也属于已观察状态；该接口不是删除双向对账协议，不能保证回收后的删除标记可继续传播。

    /// fsync WAL（commit 的持久化点）
    pub fn sync(&mut self) -> std::io::Result<()> {
        self.ensure_healthy()?;
        let result = self.sync_inner();
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    pub(super) fn sync_inner(&mut self) -> std::io::Result<()> {
        // 更新 header 中的 next_lsn 提示
        let mut hdr = vec![0u8; WAL_HEADER_SIZE];
        hdr[0..8].copy_from_slice(WAL_MAGIC);
        hdr[8..16].copy_from_slice(&self.next_lsn.to_le_bytes());
        self.file.write_all_at(0, &hdr)?;
        #[cfg(test)]
        let fsync_timer = commit_probe::Timer::start(commit_probe::Stage::Fsync);
        let result = self.file.sync_at(SITE_WAL_SYNC);
        #[cfg(test)]
        drop(fsync_timer);
        result?;
        Ok(())
    }

    /// 读取全部完整记录（损坏的尾部帧丢弃，模拟崩溃截断）
    pub fn read_all(&mut self) -> std::io::Result<Vec<WalEntry>> {
        Self::read_all_entries(&mut self.file)
    }

    pub(super) fn read_all_entries(file: &mut DiskFile) -> std::io::Result<Vec<WalEntry>> {
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
        self.ensure_healthy()?;
        let result = self
            .file
            .set_len(WAL_HEADER_SIZE as u64)
            .and_then(|()| Self::write_header_bytes(&mut self.file, self.next_lsn));
        if let Err(e) = result {
            self.poisoned = true;
            return Err(e);
        }
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

    pub fn ensure_healthy(&self) -> std::io::Result<()> {
        if self.poisoned {
            Err(std::io::Error::other(
                "WAL is quarantined after an I/O failure; reopen required",
            ))
        } else {
            Ok(())
        }
    }
    /// 追加提交记录并同步 WAL，两步成功后事务才能对其它事务可见。
    /// 追加或同步失败会封锁该 WAL；调用方必须保留不确定状态，不能提前销账。

    /// 提交记录追加和同步都成功后，事务才能对其它事务可见。
    pub fn commit_transaction(&mut self, xid: u64) -> std::io::Result<()> {
        self.ensure_healthy()?;
        #[cfg(test)]
        commit_probe::note_attempt();
        #[cfg(test)]
        let fail = FAIL_COMMIT.with(|f| f.replace(0));
        #[cfg(test)]
        if fail == 1 {
            self.poisoned = true;
            return Err(std::io::Error::other("injected Commit append failure"));
        }
        #[cfg(test)]
        let append_timer = commit_probe::Timer::start(commit_probe::Stage::Append);
        self.append(WalRecord::Commit { xid })?;
        #[cfg(test)]
        drop(append_timer);
        #[cfg(test)]
        if fail == 2 {
            self.poisoned = true;
            return Err(std::io::Error::other("injected Commit fsync failure"));
        }
        #[cfg(test)]
        let sync_timer = commit_probe::Timer::start(commit_probe::Stage::Sync);
        let result = self.sync();
        #[cfg(test)]
        drop(sync_timer);
        result
    }
}
