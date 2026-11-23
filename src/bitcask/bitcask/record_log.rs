//! 本文件负责追加日志编码、刷盘和恢复。

use super::*;

impl Drop for Log {
    fn drop(&mut self) {
        let _ = self.flush_buf();
    }
}

impl Log {
    pub fn new(path: PathBuf) -> Result<Self> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;
        // 排他锁，防止并发写同一数据文件
        file.try_lock_exclusive()?;
        let end_pos = file.metadata()?.len();
        Ok(Self {
            path,
            file,
            end_pos,
            // 缓冲**首次写入时**才按目标大小分配：只读/空库不该一打开就留 4 MiB
            // （基线实测：两处各 4 MiB ⇒ `open 空库` 就"常驻 8.4 MB"）
            buf: Vec::new(),
            dirty: false,
        })
    }

    /// 把写缓冲落盘（不 fsync）
    pub fn flush_buf(&mut self) -> Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        // end_pos 已在 append 时推进；实际文件长度 = end_pos - buf.len()
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&self.buf)?;
        self.buf.clear();
        // 字节进了文件、还没 fsync ⇒ 仍然欠一次
        self.dirty = true;
        Ok(())
    }

    /// 从 `from` 扫到 `to`，把每条记录交给 `on_record`。
    ///
    /// **解日志格式的地方全仓库只有这一处**（M60 抽出来的）：逐条施加（open 时的全量扫、hint 增量扫）
    /// 与批量成树（merge 之后的重扫）必须共用同一份解码 —— 两份解码器迟早有一份漏掉某类记录，
    /// 而"漏掉"在这条路上不是慢，是**静默少一把键**。
    /// 记录游标（`pos`）的推进也在这里：活值跳过载荷、墓碑不跳，两份用户都不会再各写一遍。
    pub(super) fn for_each_record(
        &mut self,
        from: u64,
        to: u64,
        mut on_record: impl FnMut(Vec<u8>, u64, i32, u32),
    ) -> Result<()> {
        if from >= to {
            return Ok(());
        }
        let mut len_buf = [0u8; KEY_VAL_HEADER_LEN as usize];
        let mut r = BufReader::new(&mut self.file);
        let mut pos: u64 = r.seek(SeekFrom::Start(from))?;

        while pos < to {
            let read_one = || -> Result<(Vec<u8>, u64, i32, u32)> {
                r.read_exact(&mut len_buf)?;
                let key_len = u32::from_be_bytes(len_buf);
                r.read_exact(&mut len_buf)?;
                let flag = i32::from_be_bytes(len_buf);
                let value_pos = pos + KEY_VAL_HEADER_LEN as u64 * 2 + key_len as u64;

                let mut key: Vec<u8> = vec![0; key_len as usize];
                r.read_exact(&mut key)?;

                let value_len = if flag >= 0 { flag as u32 } else { 0 };
                if flag >= 0 {
                    r.seek_relative(value_len as i64)?;
                }
                Ok((key, value_pos, flag, value_len))
            }();

            match read_one {
                Ok((key, value_pos, flag, value_len)) => {
                    #[cfg(test)]
                    note_merge_rescan_row();
                    on_record(key, value_pos, flag, value_len);
                    if flag >= 0 {
                        pos = value_pos + value_len as u64;
                    } else {
                        pos = value_pos;
                    }
                }
                Err(err) => return Err(err),
            }
        }
        Ok(())
    }

    /// 从 `from` 扫到 `to`，把记录合并进 `key_dir`（后写覆盖先写）
    pub(super) fn scan_log_into(
        &mut self,
        key_dir: &mut KeyDir,
        file_id: u32,
        from: u64,
        to: u64,
    ) -> Result<()> {
        self.for_each_record(from, to, |key, value_pos, flag, value_len| {
            apply_log_record(key_dir, file_id, key, value_pos, flag, value_len)
        })
    }

    /// **升序日志 → 索引，一次成树**（M60，merge 重扫专用）：满节点，免掉 M55 那笔半满税。
    ///
    /// ⚠️ 扫到硬删除（`flag` 既不是活值也不是软墓碑 —— 与 `apply_log_record` 的判据一字不差）就
    /// **整份退回**逐条施加：批量那一侧只认"后到者胜"，删除必须按顺序施加。退回的代价是多扫一遍日志，
    /// 而 merge 的产物本来就写不出硬删除（`merge()` 只写活值与软墓碑）⇒ 这一支是保险，不是常规路径。
    ///
    /// 对照臂（门控 `MERGE_BULK_OLD_SHAPE`，`#[cfg(test)]` ⇒ 不随 crate 发布）由调用方 `Bitcask::merge`
    /// 走老路，这样"批量 vs 逐条"两臂共用同一个 merge、同一份日志 ⇒ 比值只归因于形状。
    pub fn load_keydir_sorted(&mut self, file_id: u32) -> Result<KeyDir> {
        self.flush_buf()?;
        let file_len = self.file.metadata()?.len();
        self.end_pos = file_len;

        let mut bulk = KeyDirBulk::new();
        let mut saw_delete = false;
        self.for_each_record(0, file_len, |key, value_pos, flag, value_len| {
            let loc = if flag >= 0 {
                KeyLoc::Live {
                    file_id,
                    pos: value_pos,
                    len: value_len,
                }
            } else if flag == FLAG_SOFT_TOMBSTONE {
                KeyLoc::SoftTombstone { file_id }
            } else {
                saw_delete = true;
                return;
            };
            bulk.push((key, loc));
        })?;
        if saw_delete {
            #[cfg(test)]
            note_merge_bulk_fallback();
            let mut key_dir = KeyDir::new();
            self.scan_log_into(&mut key_dir, file_id, 0, file_len)?;
            return Ok(key_dir);
        }
        // M62：清单攒满 = 这一段最贵的那个形状在此刻成形（还没开始成树）
        #[cfg(test)]
        MERGE_BULK_LEN.with(|c| c.set(bulk.0.len()));
        #[cfg(test)]
        merge_probe_mark(1);
        #[cfg(test)]
        note_merge_bulk_rebuild();
        Ok(bulk.into_keydir())
    }

    /// 扫描整文件构建 KeyDir（先 flush 缓冲）
    pub fn load_index(&mut self, file_id: u32) -> Result<KeyDir> {
        self.flush_buf()?;
        let file_len = self.file.metadata()?.len();
        self.end_pos = file_len;
        let mut key_dir = KeyDir::new();
        self.scan_log_into(&mut key_dir, file_id, 0, file_len)?;
        Ok(key_dir)
    }

    /// 从 `from_offset` 扫到文件末尾，增量更新 KeyDir
    pub fn load_index_from(
        &mut self,
        key_dir: &mut KeyDir,
        file_id: u32,
        from_offset: u64,
    ) -> Result<()> {
        self.flush_buf()?;
        let file_len = self.file.metadata()?.len();
        self.end_pos = file_len;
        if from_offset > file_len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("hint log_size={from_offset} > data.log len={file_len}"),
            ));
        }
        self.scan_log_into(key_dir, file_id, from_offset, file_len)
    }

    pub(super) fn read_value(&mut self, value_pos: u64, value_len: u32) -> Result<Vec<u8>> {
        #[cfg(test)]
        READ_VALUE_CALLS.with(|c| c.set(c.get() + 1));
        // 读路径前必须 flush，offset 才落在真实文件上
        self.flush_buf()?;
        let mut value: Vec<u8> = vec![0; value_len as usize];
        self.file.seek(SeekFrom::Start(value_pos))?;
        self.file.read_exact(&mut value)?;
        Ok(value)
    }

    /// 编码一条记录到 `out`，返回 (value_pos, value_len_or_0, flag)
    pub(super) fn encode_entry_to(
        out: &mut Vec<u8>,
        base_offset: u64,
        key: &[u8],
        value: Option<&[u8]>,
        hard_delete: bool,
    ) -> (u64, u32, i32) {
        let key_len = key.len() as u32;
        let (flag, value_len, payload): (i32, u32, Option<&[u8]>) = if hard_delete {
            (FLAG_HARD_DELETE, 0, None)
        } else {
            match value {
                Some(v) => (v.len() as i32, v.len() as u32, Some(v)),
                None => (FLAG_SOFT_TOMBSTONE, 0, None),
            }
        };
        let value_pos = base_offset + KEY_VAL_HEADER_LEN as u64 * 2 + key_len as u64;
        out.extend_from_slice(&key_len.to_be_bytes());
        out.extend_from_slice(&flag.to_be_bytes());
        out.extend_from_slice(key);
        if let Some(v) = payload {
            out.extend_from_slice(v);
        }
        (value_pos, value_len, flag)
    }

    /// 追加一条记录到写缓冲。返回 `(value_pos, value_len)`（硬删/软 tomb 时 len=0）
    pub(super) fn append_entry(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        hard_delete: bool,
    ) -> Result<(u64, u32, i32)> {
        let base = self.end_pos;
        let before = self.buf.len();
        // 攒够 WRITE_BUF_TARGET 才落盘的规矩不变，但缓冲改成**首次写入时**才一次到位：
        // 既不让只读/空库白留 4 MiB，也不让大写入反复 realloc（flush_buf 是 clear，容量保留）
        if self.buf.capacity() < WRITE_BUF_TARGET {
            self.buf.reserve(WRITE_BUF_TARGET.saturating_sub(before));
        }
        let (value_pos, value_len, flag) =
            Self::encode_entry_to(&mut self.buf, base, key, value, hard_delete);
        let written = (self.buf.len() - before) as u64;
        self.end_pos += written;
        if written > 0 {
            self.dirty = true;
        }
        if self.buf.len() >= WRITE_BUF_TARGET {
            self.flush_buf()?;
        }
        Ok((value_pos, value_len, flag))
    }

    /// 经典：`value=None` → 硬删除
    pub fn write_entry(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<(u64, u32)> {
        match value {
            Some(v) => {
                let (pos, len, _) = self.append_entry(key, Some(v), false)?;
                // 兼容旧 API：返回 (entry_offset, entry_total_len)
                let entry_len = KEY_VAL_HEADER_LEN * 2 + key.len() as u32 + len;
                let offset = pos - KEY_VAL_HEADER_LEN as u64 * 2 - key.len() as u64;
                Ok((offset, entry_len))
            }
            None => self.write_hard_delete(key),
        }
    }

    pub fn write_hard_delete(&mut self, key: &[u8]) -> Result<(u64, u32)> {
        let (pos, _, _) = self.append_entry(key, None, true)?;
        let entry_len = KEY_VAL_HEADER_LEN * 2 + key.len() as u32;
        let offset = pos - KEY_VAL_HEADER_LEN as u64 * 2 - key.len() as u64;
        Ok((offset, entry_len))
    }

    pub fn write_soft_tombstone(&mut self, key: &[u8]) -> Result<(u64, u32)> {
        let (pos, _, _) = self.append_entry(key, None, false)?;
        let entry_len = KEY_VAL_HEADER_LEN * 2 + key.len() as u32;
        let offset = pos - KEY_VAL_HEADER_LEN as u64 * 2 - key.len() as u64;
        Ok((offset, entry_len))
    }
}
