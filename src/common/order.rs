//! Logical ordering over the unchanged `raw_key || version_be` disk format.
//!
//! Binary prefixes can interleave physically. A group is complete only when no
//! future encoded key (at or below the version ceiling) can belong to it.
//! Ordinary text keys buffer one group. Overlapping binary prefixes buffer the
//! overlap window; in the worst case that window can span the database.

use std::collections::{BTreeMap, BTreeSet};

use super::{VersionedKey, WinCandidate, decode_key, encode_key};

/// Smallest logical key that an encoded key >= `enc` can represent.
/// A logical key smaller than `enc` must be one of its prefixes; its largest
/// possible encoding is that prefix followed by `max_version`.
pub(crate) fn logical_floor(enc: &[u8], max_version: u64) -> &[u8] {
    let ceiling = max_version.to_be_bytes();
    for split in 0..enc.len() {
        if ceiling.as_slice() >= &enc[split..] {
            return &enc[..split];
        }
    }
    enc
}

/// Exclusive physical bound covering logical keys < end (or <= end).
pub(crate) fn range_end(end: &[u8], max_version: u64, inclusive: bool) -> Vec<u8> {
    let ceiling = max_version.to_be_bytes();
    let mut bound = end.to_vec();
    for split in 0..end.len() {
        if ceiling.as_slice() >= &end[split..] {
            let mut candidate = encode_key(&end[..split], max_version);
            candidate.push(0);
            bound = bound.max(candidate);
        }
    }
    if inclusive {
        let mut candidate = encode_key(end, max_version);
        candidate.push(0);
        bound = bound.max(candidate);
    }
    bound
}

pub(crate) struct KeyWindow<T> {
    pending: BTreeMap<Vec<u8>, T>,
}

impl<T> KeyWindow<T> {
    pub(crate) fn new() -> Self {
        Self {
            pending: BTreeMap::new(),
        }
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
    pub(crate) fn insert(&mut self, key: Vec<u8>, value: T) {
        self.pending.insert(key, value);
    }
    pub(crate) fn pop_first(&mut self) -> Option<(Vec<u8>, T)> {
        self.pending.pop_first()
    }
    pub(crate) fn pop_before(&mut self, enc: &[u8], max_version: u64) -> Option<(Vec<u8>, T)> {
        let floor = logical_floor(enc, max_version);
        if self
            .pending
            .first_key_value()
            .is_some_and(|(key, _)| key.as_slice() < floor)
        {
            self.pending.pop_first()
        } else {
            None
        }
    }
}

impl<T: Default> KeyWindow<T> {
    pub(crate) fn entry(&mut self, key: Vec<u8>) -> &mut T {
        self.pending.entry(key).or_default()
    }
}

/// Shared candidate folding, keeping only the winning row of each pending key.
pub(crate) struct CandidateFold<'a, R> {
    window: KeyWindow<(u64, R)>,
    out: Vec<WinCandidate<R>>,
    start: Option<&'a [u8]>,
    end: Option<&'a [u8]>,
    after: Option<&'a [u8]>,
    accept: Option<&'a dyn Fn(&[u8]) -> bool>,
    max_version: u64,
    budget: usize,
    stopped: bool,
}

impl<'a, R> CandidateFold<'a, R> {
    pub(crate) fn new(
        start: Option<&'a [u8]>,
        end: Option<&'a [u8]>,
        after: Option<&'a [u8]>,
        accept: Option<&'a dyn Fn(&[u8]) -> bool>,
        max_version: u64,
        budget: usize,
    ) -> Self {
        Self {
            window: KeyWindow::new(),
            out: Vec::new(),
            start,
            end,
            after,
            accept,
            max_version,
            budget: budget.max(1),
            stopped: false,
        }
    }

    fn emit(&mut self, key: Vec<u8>, (version, row): (u64, R)) {
        self.out.push(WinCandidate { key, version, row });
    }

    /// Returns true once the output budget is full. The caller may stop its walk.
    pub(crate) fn push(
        &mut self,
        enc: &[u8],
        visible: impl FnOnce(u64) -> bool,
        row: impl FnOnce() -> R,
    ) -> bool {
        while let Some((key, winner)) = self.window.pop_before(enc, self.max_version) {
            self.emit(key, winner);
            if self.out.len() >= self.budget {
                self.stopped = true;
                return true;
            }
        }
        let Some(VersionedKey {
            raw_key: key,
            version,
        }) = decode_key(enc)
        else {
            return false;
        };
        if !visible(version)
            || self.start.is_some_and(|s| key.as_slice() < s)
            || self.end.is_some_and(|e| key.as_slice() >= e)
            || self.after.is_some_and(|a| key.as_slice() <= a)
            || self.accept.is_some_and(|f| !f(&key))
        {
            return false;
        }
        // Equal logical keys have monotonically increasing physical versions.
        self.window.insert(key, (version, row()));
        false
    }

    pub(crate) fn finish(mut self) -> (Vec<WinCandidate<R>>, bool) {
        if !self.stopped {
            while self.out.len() < self.budget {
                let Some((key, winner)) = self.window.pop_first() else {
                    break;
                };
                self.emit(key, winner);
            }
        }
        let complete = !self.stopped && self.window.is_empty();
        (self.out, complete)
    }
}

/// The caller must explicitly handle the O(database) fallback when the physical
/// reverse walk exceeds its budget; an empty answer is not a fallback signal.
pub(crate) enum ReverseOutcome {
    Complete(Vec<super::ExportRecord>),
    BudgetExceeded { physical_steps: usize },
}

pub(crate) struct LogicalHealth {
    groups: KeyWindow<u64>,
    max_version: u64,
    total_rows: u64,
    keys: u64,
    max_chain: u64,
}

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
    fn count(&mut self, count: u64) {
        self.keys += 1;
        self.max_chain = self.max_chain.max(count);
    }
    pub(crate) fn push(&mut self, enc: &[u8]) {
        while let Some((_, count)) = self.groups.pop_before(enc, self.max_version) {
            self.count(count);
        }
        if let Some(vk) = decode_key(enc) {
            self.total_rows += 1;
            *self.groups.entry(vk.raw_key) += 1;
        }
    }
    pub(crate) fn finish(mut self, active_txns: u64) -> super::MvccHealth {
        while let Some((_, count)) = self.groups.pop_first() {
            self.count(count);
        }
        super::MvccHealth {
            total_rows: self.total_rows,
            live_keys: self.keys,
            max_chain: self.max_chain,
            rows_above_one: self.total_rows.saturating_sub(self.keys),
            active_txns,
        }
    }
}

pub(crate) fn reverse_window(
    n: usize,
    max_steps: usize,
    mut bound: Option<Vec<u8>>,
    at_or_below: Option<&[u8]>,
    mut prev_encoded: impl FnMut(&[u8]) -> Option<Vec<u8>>,
    mut live: impl FnMut(&[u8]) -> Option<super::ExportRecord>,
) -> ReverseOutcome {
    let mut pending = BTreeSet::<Vec<u8>>::new();
    let mut out = Vec::with_capacity(n.min(4096));
    let mut steps = 0usize;
    while out.len() < n {
        let enc = bound.as_deref().and_then(&mut prev_encoded);
        // Every remaining physical key is smaller than enc, and its logical
        // key is a prefix of itself. Thus logical keys >= enc are finalized.
        while pending
            .last()
            .is_some_and(|key| enc.as_ref().is_none_or(|e| key >= e))
        {
            let key = pending.pop_last().expect("checked nonempty");
            if let Some(record) = live(&key) {
                out.push(record);
            }
            if out.len() == n {
                break;
            }
        }
        if out.len() == n || enc.is_none() {
            break;
        }
        let enc = enc.expect("checked Some");
        if steps >= max_steps {
            #[cfg(test)]
            super::store::note_reverse_fallback();
            return ReverseOutcome::BudgetExceeded {
                physical_steps: steps,
            };
        }
        steps += 1;
        if let Some(vk) = decode_key(&enc) {
            if at_or_below.is_none_or(|end| vk.raw_key.as_slice() <= end)
                && pending.insert(vk.raw_key)
            {
                #[cfg(test)]
                super::store::note_reverse_candidate();
            }
        }
        bound = Some(enc);
    }
    out.reverse();
    ReverseOutcome::Complete(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{assert, assert_eq, vec};

    #[test]
    fn binary_order_bounds_cover_prefixes_and_all_version_bytes() {
        let keys = vec![
            vec![],
            vec![0],
            vec![0, 0],
            vec![1],
            vec![97],
            vec![97, 0],
            vec![97, 0, 0, 0, 0, 0, 0, 0, 2],
            vec![255],
        ];
        for ceiling in [3, 256, 1 << 56, u64::MAX] {
            for end in &keys {
                let bound = range_end(end, ceiling, false);
                for raw in &keys {
                    for version in [0, 1, ceiling] {
                        let enc = encode_key(raw, version);
                        if raw < end {
                            assert!(enc < bound, "{raw:?} < {end:?}");
                        }
                        assert!(logical_floor(&enc, ceiling) <= raw.as_slice());
                    }
                }
            }
        }
        let enc = encode_key(&[97, 0, 0, 0, 0, 0, 0, 0, 2], 2);
        assert_eq!(logical_floor(&enc, 3), b"a");
    }
}
