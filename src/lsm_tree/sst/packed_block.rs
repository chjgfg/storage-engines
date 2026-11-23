//! 本文件负责紧凑块的借用访问。

use super::*;

impl<'a> EntryRef<'a> {
    pub(super) fn of(entry: &'a StoredEntry) -> Self {
        Self {
            seq: entry.seq,
            kind: entry.value.kind_byte(),
            value: match &entry.value {
                EntryValue::Put(Some(value)) => value,
                _ => &[],
            },
        }
    }
    pub(super) fn light(self) -> LightEntry {
        LightEntry {
            seq: self.seq,
            kind: self.kind,
        }
    }
    pub(super) fn owned(self) -> StoredEntry {
        #[cfg(test)]
        ENTRY_VALUE_CLONES.with(|c| c.set(c.get() + 1));
        StoredEntry {
            seq: self.seq,
            value: match self.kind {
                0 => EntryValue::Put(Some(self.value.to_vec())),
                1 => EntryValue::Put(None),
                _ => EntryValue::Drop,
            },
        }
    }
}

impl PackedBlock {
    /// 检查字节边界后建立结构化视图。
    /// 借用视图只能活在原缓冲区生命周期内，格式无效时不得按默认值继续读取。
    pub(super) fn decode(raw: Vec<u8>, with_seq: bool) -> std::io::Result<Self> {
        let invalid =
            || std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid packed SST block");
        if raw.len() < 8 || raw.len() > u32::MAX as usize {
            return Err(invalid());
        }
        let count = u32::from_le_bytes(raw[..4].try_into().unwrap()) as usize;
        let end = raw.len() - 4;
        if crc32(&raw[4..end]) != u32::from_le_bytes(raw[end..].try_into().unwrap()) {
            return Err(invalid());
        }
        let minimum = if with_seq { 13 } else { 5 };
        if count > (end - 4) / minimum {
            return Err(invalid());
        }
        fn take(
            raw: &[u8],
            pos: &mut usize,
            end: usize,
            size: usize,
        ) -> std::io::Result<std::ops::Range<usize>> {
            let next = pos.checked_add(size).filter(|n| *n <= end).ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "packed SST field")
            })?;
            let range = *pos..next;
            let _ = &raw[range.clone()];
            *pos = next;
            Ok(range)
        }
        let mut pos = 4;
        let mut rows: Vec<PackedRow> = Vec::with_capacity(count);
        for _ in 0..count {
            let size =
                u32::from_le_bytes(raw[take(&raw, &mut pos, end, 4)?].try_into().unwrap()) as usize;
            let key = take(&raw, &mut pos, end, size)?;
            let kind = raw[take(&raw, &mut pos, end, 1)?.start];
            if kind > 2 {
                return Err(invalid());
            }
            let seq = if with_seq {
                u64::from_le_bytes(raw[take(&raw, &mut pos, end, 8)?].try_into().unwrap())
            } else {
                0
            };
            let value = if kind == 0 {
                let size =
                    u32::from_le_bytes(raw[take(&raw, &mut pos, end, 4)?].try_into().unwrap())
                        as usize;
                take(&raw, &mut pos, end, size)?
            } else {
                pos..pos
            };
            if rows.last().is_some_and(|old| {
                raw[old.key.start as usize..old.key.end as usize] > raw[key.clone()]
            }) {
                return Err(invalid());
            }
            rows.push(PackedRow {
                key: key.start as u32..key.end as u32,
                value: value.start as u32..value.end as u32,
                seq,
                kind,
            });
        }
        if pos != end {
            return Err(invalid());
        }
        Ok(Self { raw, rows })
    }
    pub(super) fn entry(&self, i: usize) -> (&[u8], EntryRef<'_>) {
        let row = &self.rows[i];
        (
            &self.raw[row.key.start as usize..row.key.end as usize],
            EntryRef {
                seq: row.seq,
                kind: row.kind,
                value: &self.raw[row.value.start as usize..row.value.end as usize],
            },
        )
    }

    /// 顺扫或压实复用紧凑点查块时，不再解析块头、CRC 与每个字段；首次 decode 入缓存前已校验范围和顺序。
    pub(super) fn to_decoded(&self) -> DataBlock {
        let mut entries = Vec::with_capacity(self.rows.len());
        for row in &self.rows {
            let key = self.raw[row.key.start as usize..row.key.end as usize].to_vec();
            let value = match row.kind {
                0 => EntryValue::Put(Some(
                    self.raw[row.value.start as usize..row.value.end as usize].to_vec(),
                )),
                1 => EntryValue::Put(None),
                _ => EntryValue::Drop,
            };
            entries.push((
                key,
                StoredEntry {
                    seq: row.seq,
                    value,
                },
            ));
        }
        DataBlock { entries }
    }
}
impl CachedBlock {
    pub(super) fn len(&self) -> usize {
        match self {
            Self::Decoded(b) => b.entries.len(),
            Self::Packed(b) => b.rows.len(),
        }
    }
    pub(super) fn entry(&self, i: usize) -> (&[u8], EntryRef<'_>) {
        match self {
            Self::Decoded(b) => (&b.entries[i].0, EntryRef::of(&b.entries[i].1)),
            Self::Packed(b) => b.entry(i),
        }
    }
    pub(super) fn partition(&self, key: &[u8], inclusive: bool) -> usize {
        let mut lo = 0;
        let mut hi = self.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let k = self.entry(mid).0;
            if k < key || (inclusive && k == key) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }
}
