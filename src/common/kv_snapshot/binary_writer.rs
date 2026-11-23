//! 二进制快照输出、记录合批与完成。

use super::*;

impl BinWriter {
    pub fn create(path: &Path, with_seq: bool) -> std::io::Result<Self> {
        Self::create_shape(path, with_seq, false)
    }

    /// `BPEXP003`：001 的字段序 + 每条 8 字节 `expire_unix_secs`（**0 = 永久**）。M81 第 6 项。
    pub fn create_v3(path: &Path) -> std::io::Result<Self> {
        Self::create_shape(path, false, true)
    }

    pub(super) fn create_shape(
        path: &Path,
        with_seq: bool,
        with_expire: bool,
    ) -> std::io::Result<Self> {
        if with_seq && with_expire {
            // 没有"既带 seq 又带 expire"的版本：那是两条互不相干的元数据叠成一个组合爆炸，
            // 而今天没有任何调用方需要它（统一工具不收 --with-seq，理由见 BIN_MAGIC_V3 注释）。
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "BPEXP 没有同时带 seq 与 expire 的版本（002=seq，003=expire）",
            ));
        }
        let f = File::create(path)?;
        let magic: &[u8; 8] = if with_seq {
            BIN_MAGIC_V2
        } else if with_expire {
            BIN_MAGIC_V3
        } else {
            BIN_MAGIC
        };
        Self::from_file(f, magic, with_seq, with_expire, false)
    }

    pub(super) fn from_file(
        f: File,
        magic: &[u8; 8],
        with_seq: bool,
        with_expire: bool,
        checked: bool,
    ) -> std::io::Result<Self> {
        #[cfg(test)]
        let options = snapshot_probe::options();
        let sink = SnapshotFile {
            file: f,
            checksum: checked.then_some(0),
            #[cfg(test)]
            legacy_fields: options == Some(true),
            #[cfg(test)]
            record_reference: options == Some(true)
                || io_reference::enabled()
                || snapshot_emit_reference::enabled(),
            #[cfg(test)]
            measured: options.is_some(),
            #[cfg(test)]
            stats: snapshot_probe::Stats::default(),
        };
        let mut w = BufWriter::with_capacity(snapshot_buffer_size(checked), sink);
        w.write_all(magic)?;
        #[cfg(test)]
        if w.get_ref().legacy_fields && checked {
            w.get_mut().checksum = Some(crate::common::disk::crc32(magic));
            w.get_mut().stats.crc_calls += 1;
        }
        Ok(Self {
            w,
            stats: IoStats::default(),
            with_seq,
            with_expire,
            record_batch: RecordBatch::default(),
        })
    }

    pub(super) fn bytes(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.w.write_all(bytes)?;
        #[cfg(test)]
        if self.w.get_ref().legacy_fields {
            let sink = self.w.get_mut();
            if let Some(crc) = &mut sink.checksum {
                *crc = crate::common::disk::crc32_extend(*crc, bytes);
                sink.stats.crc_calls += 1;
            }
        }
        Ok(())
    }

    pub fn write(&mut self, rec: &KvRecord) -> std::io::Result<()> {
        self.write_borrowed(
            &rec.key,
            rec.value.as_deref(),
            rec.seq,
            rec.expire_unix_secs,
        )
    }

    pub(crate) fn write_borrowed(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        seq: Option<u64>,
        expire: Option<u64>,
    ) -> std::io::Result<()> {
        record_size(key, value, self.with_seq, self.with_expire)?;
        self.flush_record_batch()?;
        encode_record(
            key,
            value,
            seq,
            expire,
            self.with_seq,
            self.with_expire,
            |bytes| self.bytes(bytes),
        )?;
        self.stats.push_live(value.is_some());
        Ok(())
    }

    pub(crate) fn write_snapshot_batched(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        expire: Option<u64>,
    ) -> std::io::Result<RecordEncoding> {
        if self.w.get_ref().checksum.is_none() || !self.with_expire || self.with_seq {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "record batches require BPEXP004",
            ));
        }
        let size = record_size(key, value, false, true)?;
        #[cfg(test)]
        if self.w.get_ref().record_reference {
            self.write_borrowed(key, value, None, expire)?;
            self.w.get_mut().stats.record_reference_rows += 1;
            return Ok(RecordEncoding::DirectReference);
        }
        if size > RECORD_BATCH_BYTES {
            self.flush_record_batch()?;
            #[cfg(test)]
            let start = self.w.get_ref().measured.then(std::time::Instant::now);
            self.write_borrowed(key, value, None, expire)?;
            #[cfg(test)]
            {
                let stats = &mut self.w.get_mut().stats;
                stats.record_direct_large_rows += 1;
                if let Some(start) = start {
                    stats.record_direct_large_ns += start.elapsed().as_nanos();
                }
            }
            return Ok(RecordEncoding::DirectLarge);
        }
        if self.record_batch.bytes.len() + size > RECORD_BATCH_BYTES {
            self.flush_record_batch()?;
        }
        if self.record_batch.bytes.capacity() == 0 {
            self.record_batch.bytes = Vec::with_capacity(RECORD_BATCH_BYTES);
        }
        encode_record(key, value, None, expire, false, true, |bytes| {
            self.record_batch.bytes.extend_from_slice(bytes);
            Ok(())
        })?;
        self.record_batch.pending.push_live(value.is_some());
        Ok(RecordEncoding::Buffered)
    }

    pub(crate) fn flush_record_batch(&mut self) -> std::io::Result<()> {
        if self.record_batch.bytes.is_empty() {
            return Ok(());
        }
        #[cfg(test)]
        let start = self.w.get_ref().measured.then(std::time::Instant::now);
        self.w.write_all(&self.record_batch.bytes)?;
        // 整批完整写入成功后才累计记录数；部分写入返回发布错误，并保留旧文件。
        let pending = std::mem::take(&mut self.record_batch.pending);
        self.stats.records += pending.records;
        self.stats.live += pending.live;
        self.stats.deleted += pending.deleted;
        #[cfg(test)]
        {
            let stats = &mut self.w.get_mut().stats;
            stats.record_buffer_bytes = self.record_batch.bytes.capacity();
            stats.record_flushes += 1;
            stats.record_buffered_rows += pending.records as u64;
            if let Some(start) = start {
                stats.record_flush_ns += start.elapsed().as_nanos();
            }
        }
        self.record_batch.bytes.clear();
        Ok(())
    }

    pub fn finish(mut self) -> std::io::Result<IoStats> {
        self.flush_record_batch()?;
        // 先刷出整个正文再读取校验和；页脚不参与 CRC，保持原 BPEXP004 格式。
        self.w.flush()?;
        let sink = self.w.get_mut();
        if let Some(crc) = sink.checksum {
            sink.file.write_all(&u32::MAX.to_le_bytes())?;
            sink.file
                .write_all(&(self.stats.records as u64).to_le_bytes())?;
            sink.file.write_all(&crc.to_le_bytes())?;
            #[cfg(test)]
            let start = sink.measured.then(std::time::Instant::now);
            sink.file.sync_all()?;
            #[cfg(test)]
            if let Some(start) = start {
                sink.stats.sync_ns += start.elapsed().as_nanos();
            }
        }
        #[cfg(test)]
        snapshot_probe::note(|s| *s = sink.stats.clone());
        Ok(self.stats)
    }
}
