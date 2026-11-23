//! 本文件负责SST 记录及可见性操作。

use super::*;

// `Clone` 手写而不是 `#[derive]`，就为了在这一个地方把上面那把尺子记上 ——
// 派生实现照样会逐字段复制，只是数不出来。
impl Clone for EntryValue {
    fn clone(&self) -> Self {
        #[cfg(test)]
        ENTRY_VALUE_CLONES.with(|c| c.set(c.get() + 1));
        match self {
            EntryValue::Put(v) => EntryValue::Put(v.clone()),
            EntryValue::Drop => EntryValue::Drop,
        }
    }
}

impl EntryValue {
    pub fn kind_byte(&self) -> u8 {
        match self {
            EntryValue::Put(Some(_)) => 0,
            EntryValue::Put(None) => 1,
            EntryValue::Drop => 2,
        }
    }

    pub fn from_parts(kind: u8, val: Option<Vec<u8>>) -> Option<Self> {
        match kind {
            0 => Some(EntryValue::Put(val)),
            1 => Some(EntryValue::Put(None)),
            2 => Some(EntryValue::Drop),
            _ => None,
        }
    }

    pub fn estimate_bytes(&self) -> usize {
        match self {
            EntryValue::Put(Some(v)) => v.len() + 1,
            EntryValue::Put(None) | EntryValue::Drop => 1,
        }
    }
}

impl StoredEntry {
    pub fn put(seq: u64, v: Option<Vec<u8>>) -> Self {
        Self {
            seq,
            value: EntryValue::Put(v),
        }
    }

    pub fn drop_at(seq: u64) -> Self {
        Self {
            seq,
            value: EntryValue::Drop,
        }
    }

    pub fn estimate_bytes(&self) -> usize {
        8 + self.value.estimate_bytes()
    }

    pub fn visible_at(&self, snap_seq: u64) -> bool {
        self.seq <= snap_seq
    }
}

impl LightEntry {
    pub(super) fn of(se: &StoredEntry) -> Self {
        LightEntry {
            seq: se.seq,
            kind: se.value.kind_byte(),
        }
    }
}
