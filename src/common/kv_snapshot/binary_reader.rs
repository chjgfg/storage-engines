//! 二进制快照分版本读取和损坏判断。

use super::*;

// ─── 读 ─────────────────────────────────────────────────────────────────────

/// **流式**读二进制快照：解出一条就交给 `on`，`on` 返回 `false` 当场停止解码。
/// 004 在回调前用固定缓冲预验整份文件 CRC；001–003 不需要这次预读。
///
/// 存在的理由与 [`crate::KvEngine::export_latest_visible_for_each`] 同一件事的两端：
/// `import_kv` 那侧要的是"边读边写进引擎"，把整份文件先摆进 `Vec` 只是为等着被逐条丢掉
/// （600k 档实测那笔 `Vec` 是 123 MB 的瞬时）。[`read_bin`] 从这一条改成收集壳，
/// **口径一字不变**：同一种截断处理（尾部不完整就停，不报错）、同一套魔数判定。
pub fn read_bin_with(
    path: &Path,
    on: &mut dyn FnMut(KvRecord) -> bool,
) -> std::io::Result<IoStats> {
    read_bin_policy(path, on, false, ValueBuffer::Exact)
}

impl ValueBuffer {
    pub(super) fn spare(self) -> usize {
        match self {
            Self::Exact => 0,
            Self::PlainEnvelope => 1,
        }
    }
}

pub(crate) fn read_snapshot_with(
    path: &Path,
    values: ValueBuffer,
    on: &mut dyn FnMut(KvRecord) -> bool,
) -> std::io::Result<IoStats> {
    read_bin_policy(path, on, true, values)
}

pub(super) fn read_bin_policy(
    path: &Path,
    on: &mut dyn FnMut(KvRecord) -> bool,
    strict: bool,
    values: ValueBuffer,
) -> std::io::Result<IoStats> {
    use std::io::{Read, Seek, SeekFrom};
    let file = File::open(path)?;
    let length = file.metadata()?.len();
    #[cfg(test)]
    let file = recovery_probe::Input(file);
    let mut reader = BufReader::with_capacity(snapshot_buffer_size(strict), file);
    if length < 8 {
        return Err(snapshot_invalid("文件过短"));
    }
    let mut magic = [0u8; 8];
    reader.read_exact(&mut magic)?;
    let (with_seq, with_expire) = match &magic {
        BIN_MAGIC => (false, false),
        BIN_MAGIC_V2 => (true, false),
        BIN_MAGIC_V3 | BIN_MAGIC_V4 => (false, true),
        _ => return Err(snapshot_invalid("非法导出文件魔数")),
    };
    // 严格快照在第一次回调前完成校验；有界预扫描防止导入方先提交损坏文件的前半段。
    let checked = &magic == BIN_MAGIC_V4;
    let (body_len, expected) = if checked {
        #[cfg(test)]
        let _timer = recovery_probe::Timer::start(recovery_probe::Kind::Verify);
        if length < 24 {
            return Err(snapshot_invalid("missing snapshot footer"));
        }
        reader.seek(SeekFrom::Start(0))?;
        let mut remaining = length - 16;
        let mut buffer = [0u8; 64 * 1024];
        let mut crc = 0;
        while remaining > 0 {
            let n = remaining.min(buffer.len() as u64) as usize;
            reader.read_exact(&mut buffer[..n])?;
            crc = crate::common::disk::crc32_extend(crc, &buffer[..n]);
            remaining -= n as u64;
        }
        let mut footer = [0u8; 16];
        reader.read_exact(&mut footer)?;
        if footer[..4] != u32::MAX.to_le_bytes()
            || crc != u32::from_le_bytes(footer[12..].try_into().unwrap())
        {
            return Err(snapshot_invalid("snapshot checksum/footer mismatch"));
        }
        reader.seek(SeekFrom::Start(8))?;
        (
            length - 24,
            Some(u64::from_le_bytes(footer[4..12].try_into().unwrap())),
        )
    } else {
        (length - 8, None)
    };
    #[cfg(test)]
    let _timer = recovery_probe::Timer::start(recovery_probe::Kind::Records);
    read_bin_records(
        &mut reader,
        body_len,
        with_seq,
        with_expire,
        strict || checked,
        expected,
        values,
        on,
    )
}

impl<R: std::io::Read> RecordInput<R> {
    pub(super) fn fixed<const N: usize>(&mut self) -> std::io::Result<Option<[u8; N]>> {
        if self.remaining < N as u64 {
            return Ok(None);
        }
        let mut bytes = [0; N];
        self.reader.read_exact(&mut bytes)?;
        self.remaining -= N as u64;
        Ok(Some(bytes))
    }

    pub(super) fn bytes(&mut self, len: u32, spare: usize) -> std::io::Result<Option<Vec<u8>>> {
        if len as u64 > self.remaining {
            return Ok(None);
        }
        let mut bytes = Vec::new();
        // 空值载荷只在确定实际信封形状时分配一次。
        let spare = if len == 0 { 0 } else { spare };
        let capacity = (len as usize)
            .checked_add(spare)
            .ok_or_else(|| snapshot_invalid("snapshot value capacity overflow"))?;
        bytes
            .try_reserve_exact(capacity)
            .map_err(std::io::Error::other)?;
        bytes.resize(len as usize, 0);
        self.reader.read_exact(&mut bytes)?;
        self.remaining -= len as u64;
        Ok(Some(bytes))
    }

    pub(super) fn record(
        &mut self,
        with_seq: bool,
        with_expire: bool,
    ) -> std::io::Result<RecordRead> {
        if self.remaining == 0 {
            return Ok(RecordRead::End);
        }
        let Some(len) = self.fixed::<4>()? else {
            return Ok(RecordRead::Truncated);
        };
        let Some(key) = self.bytes(u32::from_le_bytes(len), 0)? else {
            return Ok(RecordRead::Truncated);
        };
        let Some(len) = self.fixed::<4>()? else {
            return Ok(RecordRead::Truncated);
        };
        let value = match u32::from_le_bytes(len) {
            TOMBSTONE_LEN => None,
            len => {
                let Some(value) = self.bytes(len, self.values.spare())? else {
                    return Ok(RecordRead::Truncated);
                };
                Some(value)
            }
        };
        let seq = if with_seq {
            let Some(bytes) = self.fixed::<8>()? else {
                return Ok(RecordRead::Truncated);
            };
            Some(u64::from_le_bytes(bytes))
        } else {
            None
        };
        let expire_unix_secs = if with_expire {
            let Some(bytes) = self.fixed::<8>()? else {
                return Ok(RecordRead::Truncated);
            };
            let expire = u64::from_le_bytes(bytes);
            (expire != 0).then_some(expire)
        } else {
            None
        };
        Ok(RecordRead::Record(KvRecord {
            key,
            value,
            seq,
            expire_unix_secs,
        }))
    }
}

pub(super) fn read_bin_records(
    reader: &mut impl std::io::Read,
    length: u64,
    with_seq: bool,
    with_expire: bool,
    strict: bool,
    expected: Option<u64>,
    values: ValueBuffer,
    on: &mut dyn FnMut(KvRecord) -> bool,
) -> std::io::Result<IoStats> {
    let mut input = RecordInput {
        reader,
        remaining: length,
        values,
    };
    let mut stats = IoStats::default();
    loop {
        match input.record(with_seq, with_expire)? {
            RecordRead::End => break,
            RecordRead::Truncated if strict => {
                return Err(snapshot_invalid("incomplete snapshot record"));
            }
            RecordRead::Truncated => break,
            RecordRead::Record(record) => {
                stats.push(&record.value);
                if !on(record) {
                    return Ok(stats);
                }
            }
        }
    }
    if expected.is_some_and(|n| n != stats.records as u64) {
        return Err(snapshot_invalid("snapshot record count mismatch"));
    }
    Ok(stats)
}

pub(super) fn snapshot_invalid(message: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message)
}

pub fn read_bin(path: &Path) -> std::io::Result<(Vec<KvRecord>, IoStats)> {
    let mut out = Vec::new();
    let stats = read_bin_with(path, &mut |rec| {
        out.push(rec);
        true
    })?;
    Ok((out, stats))
}
