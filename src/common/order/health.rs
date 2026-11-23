//! 多版本链健康统计。

use super::*;

impl LogicalHealth {
    pub(crate) fn new(max_version: u64) -> Self {
        Self {
            groups: KeyWindow::new(),
            max_version,
            total_rows: 0,
            keys: 0,
            max_chain: 0,
        }
    }
    pub(super) fn count(&mut self, count: u64) {
        self.keys += 1;
        self.max_chain = self.max_chain.max(count);
    }
    pub(crate) fn push(&mut self, enc: &[u8]) {
        if enc.len() < 8 {
            return;
        }
        let raw = &enc[..enc.len() - 8];
        self.total_rows += 1;
        if let WindowStorage::Single(key, count) = &mut self.groups.pending {
            if key.as_slice() == raw {
                *count += 1;
                return;
            }
        }
        let mut reusable = Vec::new();
        while let Some((key, count)) = self.groups.pop_before(enc, self.max_version) {
            self.count(count);
            reusable = key;
        }
        match &mut self.groups.pending {
            WindowStorage::Single(key, count) if key.as_slice() == raw => {
                *count += 1;
                return;
            }
            WindowStorage::Overlap(map) => {
                if let Some(count) = map.get_mut(raw) {
                    *count += 1;
                    return;
                }
            }
            _ => {}
        }
        reusable.clear();
        reusable.extend_from_slice(raw);
        self.groups.insert(reusable, 1);
    }
    pub(crate) fn finish(mut self, active_txns: u64) -> super::super::MvccHealth {
        while let Some((_, count)) = self.groups.pop_first() {
            self.count(count);
        }
        super::super::MvccHealth {
            total_rows: self.total_rows,
            live_keys: self.keys,
            max_chain: self.max_chain,
            rows_above_one: self.total_rows.saturating_sub(self.keys),
            active_txns,
        }
    }
}
