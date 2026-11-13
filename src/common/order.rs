//! Logical ordering over the unchanged `raw_key || version_be` disk format.
//!
//! Binary prefixes can interleave physically. A group is complete only when no
//! future encoded key (at or below the version ceiling) can belong to it.
//! Ordinary text keys buffer one group. Overlapping binary prefixes buffer the
//! overlap window; in the worst case that window can span the database.

use std::collections::{BTreeMap, BTreeSet};
use std::{debug_assert_eq, matches, unreachable};

use super::{WinCandidate, decode_key, encode_key};
#[cfg(test)]
use super::VersionedKey;
use super::version::decode_key_ref;

#[cfg(test)]
std::thread_local! {
    static TAIL_BUFFER_PEAK: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
#[cfg(test)]
pub(crate) fn note_tail_buffer(rows: usize) { TAIL_BUFFER_PEAK.with(|c| c.set(c.get().max(rows))); }
#[cfg(test)]
pub(crate) fn take_tail_buffer_peak() -> usize { TAIL_BUFFER_PEAK.with(|c| c.replace(0)) }

/// Smallest logical key that an encoded key >= `enc` can represent.
/// A logical key smaller than `enc` must be one of its prefixes; its largest
/// possible encoding is that prefix followed by `max_version`.
#[cfg(test)]
pub(crate) fn logical_floor(enc: &[u8], max_version: u64) -> &[u8] {
    let ceiling = max_version.to_be_bytes();
    for split in 0..enc.len() {
        // Most ordinary key bytes exceed the first (usually zero) version byte.
        // Avoid a slice comparison for each such byte; the exact bound is unchanged.
        if enc[split] > ceiling[0] { continue; }
        if ceiling.as_slice() >= &enc[split..] {
            return &enc[..split];
        }
    }
    enc
}

/// Test only prefixes that can be <= this pending key. Prefixes beyond its
/// shared bytes with enc are larger, so cannot prevent this key's completion.
pub(crate) fn window_before(first: &[u8], enc: &[u8], max_version: u64) -> bool {
    if first >= enc { return false; }
    let common = first.iter().zip(enc).take_while(|(a, b)| a == b).count();
    let ceiling = max_version.to_be_bytes();
    for split in 0..=common {
        if enc.get(split).is_some_and(|&b| b > ceiling[0]) { continue; }
        if ceiling.as_slice() >= &enc[split..] { return false; }
    }
    true
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
    pending: WindowStorage<T>,
}

/// Ordinary groups need no tree node. Binary prefix overlap explicitly promotes
/// to a sorted window, whose worst-case size remains the full database.
enum WindowStorage<T> {
    Empty,
    Single(Vec<u8>, T),
    Overlap(BTreeMap<Vec<u8>, T>),
}

impl<T> KeyWindow<T> {
    pub(crate) fn new() -> Self {
        Self {
            pending: WindowStorage::Empty,
        }
    }
    pub(crate) fn is_empty(&self) -> bool {
        matches!(self.pending, WindowStorage::Empty)
    }
    pub(crate) fn insert(&mut self, key: Vec<u8>, value: T) {
        match &mut self.pending {
            WindowStorage::Empty => self.pending = WindowStorage::Single(key, value),
            WindowStorage::Single(old, v) if *old == key => *v = value,
            WindowStorage::Single(_, _) => {
                let old = std::mem::replace(&mut self.pending, WindowStorage::Empty);
                let WindowStorage::Single(old_key, old_value) = old else { unreachable!() };
                let mut map = BTreeMap::new();
                map.insert(old_key, old_value);
                map.insert(key, value);
                self.pending = WindowStorage::Overlap(map);
            }
            WindowStorage::Overlap(map) => { map.insert(key, value); }
        }
    }
    pub(crate) fn pop_first(&mut self) -> Option<(Vec<u8>, T)> {
        match std::mem::replace(&mut self.pending, WindowStorage::Empty) {
            WindowStorage::Empty => None,
            WindowStorage::Single(key, value) => Some((key, value)),
            WindowStorage::Overlap(mut map) => {
                let out = map.pop_first();
                self.pending = if map.len() == 1 {
                    let (key, value) = map.pop_first().expect("one group");
                    WindowStorage::Single(key, value)
                } else if map.is_empty() { WindowStorage::Empty }
                else { WindowStorage::Overlap(map) };
                out
            }
        }
    }
    pub(crate) fn pop_before(&mut self, enc: &[u8], max_version: u64) -> Option<(Vec<u8>, T)> {
        let first = match &self.pending {
            WindowStorage::Empty => return None,
            WindowStorage::Single(key, _) => key,
            WindowStorage::Overlap(map) => map.first_key_value().expect("overlap nonempty").0,
        };
        #[cfg(test)]
        let before = if super::kv_snapshot::snapshot_emit_reference::enabled() {
            first.as_slice() < logical_floor(enc, max_version)
        } else { window_before(first, enc, max_version) };
        #[cfg(not(test))]
        let before = window_before(first, enc, max_version);
        if before {
            self.pop_first()
        } else {
            None
        }
    }

    /// Allocate the logical key and group only on first insertion. The factory
    /// may reuse a finalized group's buffers; overlapping binary groups remain owned.
    pub(crate) fn entry_with(&mut self, key: &[u8], make: impl FnOnce() -> (Vec<u8>, T)) -> &mut T {
        if matches!(&self.pending, WindowStorage::Single(old, _) if old.as_slice() == key) {
            return match &mut self.pending {
                WindowStorage::Single(_, value) => value,
                _ => unreachable!(),
            };
        }
        if self.is_empty() {
            let (owned, value) = make();
            debug_assert_eq!(owned.as_slice(), key);
            self.pending = WindowStorage::Single(owned, value);
            return match &mut self.pending {
                WindowStorage::Single(_, value) => value,
                _ => unreachable!(),
            };
        }
        if matches!(&self.pending, WindowStorage::Single(_, _)) {
            let old = std::mem::replace(&mut self.pending, WindowStorage::Empty);
            let WindowStorage::Single(owned, value) = old else { unreachable!() };
            let mut map = BTreeMap::new();
            map.insert(owned, value);
            self.pending = WindowStorage::Overlap(map);
        }
        match &mut self.pending {
            WindowStorage::Overlap(map) => {
                if map.contains_key(key) { return map.get_mut(key).unwrap(); }
                let (owned, value) = make();
                debug_assert_eq!(owned.as_slice(), key);
                map.entry(owned).or_insert(value)
            }
            _ => unreachable!(),
        }
    }
}

impl<T: Default> KeyWindow<T> {
    pub(crate) fn entry(&mut self, key: Vec<u8>) -> &mut T {
        if let WindowStorage::Single(old, _) = &self.pending {
            if *old != key {
                let old = std::mem::replace(&mut self.pending, WindowStorage::Empty);
                let WindowStorage::Single(k, v) = old else { unreachable!() };
                let mut map = BTreeMap::new();
                map.insert(k, v);
                self.pending = WindowStorage::Overlap(map);
            }
        }
        if self.is_empty() {
            self.pending = WindowStorage::Single(key, T::default());
            return match &mut self.pending {
                WindowStorage::Single(_, value) => value,
                _ => unreachable!(),
            };
        }
        match &mut self.pending {
            WindowStorage::Single(_, value) => value,
            WindowStorage::Overlap(map) => map.entry(key).or_default(),
            WindowStorage::Empty => unreachable!(),
        }
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
        #[cfg(test)]
        if super::store::scan_cost_reference::enabled() {
            let Some(VersionedKey { raw_key: key, version }) = decode_key(enc) else { return false; };
            super::store::scan_cost_reference::note_key_copy();
            if !visible(version)
                || self.start.is_some_and(|s| key.as_slice() < s)
                || self.end.is_some_and(|e| key.as_slice() >= e)
                || self.after.is_some_and(|a| key.as_slice() <= a)
                || self.accept.is_some_and(|f| !f(&key)) { return false; }
            self.window.insert(key, (version, row()));
            return false;
        }
        let Some((key, version)) = decode_key_ref(enc)
        else {
            return false;
        };
        if !visible(version)
            || self.start.is_some_and(|s| key < s)
            || self.end.is_some_and(|e| key >= e)
            || self.after.is_some_and(|a| key <= a)
            || self.accept.is_some_and(|f| !f(key))
        {
            return false;
        }
        // Equal logical keys have monotonically increasing physical versions.
        let mut next = Some((version, row()));
        let winner = self.window.entry_with(key, || {
            #[cfg(test)]
            super::store::scan_cost_reference::note_key_copy();
            (key.to_vec(), next.take().unwrap())
        });
        if let Some(next) = next { *winner = next; }
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

/// Scan physical predecessors and retain only the largest n live logical keys.
/// Physical order may interleave binary prefixes, so finalize by logical order.
/// Memory is O(n records + current key); time can include a full physical walk
/// and repeated point lookups for interleaved histories. Callers must supply a
/// bounded-memory point lookup. Consecutive versions share one lookup.
pub(crate) fn reverse_tail_bounded(
    n: usize,
    mut bound: Option<Vec<u8>>,
    mut prev_encoded: impl FnMut(&[u8]) -> Option<Vec<u8>>,
    mut live: impl FnMut(&[u8]) -> Option<super::ExportRecord>,
) -> Vec<super::ExportRecord> {
    let mut tail = BTreeMap::<Vec<u8>, super::ExportRecord>::new();
    let mut last_checked = None;
    if n == 0 { return Vec::new(); }
    while let Some(enc) = bound.as_deref().and_then(&mut prev_encoded) {
        // A remaining raw key is a prefix of its encoded key, hence <= enc.
        if tail.len() == n && tail.first_key_value().is_some_and(|(key, _)| &enc <= key) { break; }
        if let Some(vk) = decode_key(&enc) {
            let raw = vk.raw_key;
            let eligible = tail.len() < n || tail.first_key_value().is_some_and(|(key, _)| &raw > key);
            if eligible && !tail.contains_key(&raw) && last_checked.as_ref() != Some(&raw) {
                if let Some(record) = live(&raw) {
                    if tail.len() == n { tail.pop_first(); }
                    tail.insert(raw.clone(), record);
                    #[cfg(test)]
                    note_tail_buffer(tail.len());
                }
                last_checked = Some(raw);
            }
        }
        bound = Some(enc);
    }
    tail.into_values().collect()
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
        if enc.len() < 8 { return; }
        let raw = &enc[..enc.len() - 8];
        self.total_rows += 1;
        if let WindowStorage::Single(key, count) = &mut self.groups.pending {
            if key.as_slice() == raw { *count += 1; return; }
        }
        let mut reusable = Vec::new();
        while let Some((key, count)) = self.groups.pop_before(enc, self.max_version) {
            self.count(count);
            reusable = key;
        }
        match &mut self.groups.pending {
            WindowStorage::Single(key, count) if key.as_slice() == raw => { *count += 1; return; }
            WindowStorage::Overlap(map) => {
                if let Some(count) = map.get_mut(raw) { *count += 1; return; }
            }
            _ => {},
        }
        reusable.clear();
        reusable.extend_from_slice(raw);
        self.groups.insert(reusable, 1);
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
    fn window_bound_matches_original_floor_exhaustively() {
        let mut keys = vec![vec![]];
        let mut level = vec![vec![]];
        for _ in 0..4 {
            let mut next = Vec::new();
            for key in level {
                for byte in [0, 1, 255] { let mut key = key.clone(); key.push(byte); next.push(key); }
            }
            keys.extend(next.clone()); level = next;
        }
        for ceiling in [0, 1, 255, 256, u64::MAX] {
            for first in &keys {
                for enc in &keys {
                    assert_eq!(window_before(first, enc, ceiling), first.as_slice() < logical_floor(enc, ceiling),
                        "first={first:?} enc={enc:?} ceiling={ceiling}");
                    let enc = encode_key(enc, ceiling);
                    assert_eq!(window_before(first, &enc, ceiling), first.as_slice() < logical_floor(&enc, ceiling));
                }
            }
        }
    }

    fn reference_health(physical: &[Vec<u8>], ceiling: u64) -> super::super::MvccHealth {
        let mut pending = BTreeMap::<Vec<u8>, u64>::new();
        let mut out = super::super::MvccHealth::default();
        for enc in physical {
            // Original generic map path, including its original floor comparison.
            let max = ceiling.to_be_bytes();
            let split = (0..enc.len()).find(|&i| max.as_slice() >= &enc[i..]).unwrap_or(enc.len());
            while pending.first_key_value().is_some_and(|(k, _)| k.as_slice() < &enc[..split]) {
                let (_, count) = pending.pop_first().unwrap();
                out.live_keys += 1;
                out.max_chain = out.max_chain.max(count);
            }
            if let Some(vk) = decode_key(enc) { *pending.entry(vk.raw_key).or_default() += 1; out.total_rows += 1; }
        }
        for count in pending.into_values() { out.live_keys += 1; out.max_chain = out.max_chain.max(count); }
        out.rows_above_one = out.total_rows - out.live_keys;
        out
    }

    #[test]
    fn logical_health_preserves_binary_overlap_and_group_transitions() {
        let mut raw_keys = vec![vec![], vec![0], vec![255], b"a".to_vec(), b"b".to_vec()];
        for a in [0, 1, 97, 255] {
            for b in [0, 1, 97, 255] {
                raw_keys.push(vec![a, b]);
                raw_keys.push(vec![a, b, 0]);
                raw_keys.push(vec![a, b, 0, 0, 0, 0, 0, 0, 1]);
            }
        }
        raw_keys.sort(); raw_keys.dedup();
        for ceiling in [65_536, u64::MAX] {
            let mut physical = Vec::new();
            let mut expected = BTreeMap::<Vec<u8>, u64>::new();
            for (i, raw) in raw_keys.iter().enumerate() {
                for version in [0, 1, 256, 65_536].into_iter().take(1 + i % 4) {
                    physical.push(encode_key(raw, version));
                    *expected.entry(raw.clone()).or_default() += 1;
                }
            }
            physical.sort();
            let mut health = LogicalHealth::new(ceiling);
            for enc in &physical { health.push(enc); }
            let got = health.finish(0);
            assert_eq!(got.total_rows, physical.len() as u64);
            assert_eq!(got.live_keys, expected.len() as u64);
            assert_eq!(got.max_chain, *expected.values().max().unwrap());
            assert_eq!(got, reference_health(&physical, ceiling));
        }
    }

    #[test]
    #[ignore = "same-build logical health allocation/time comparison; release single thread"]
    fn logical_health_cost() {
        use std::{eprintln, format, time::Instant};
        let rows = 600_000;
        let physical: Vec<_> = (0..rows).map(|i| encode_key(format!("{i:08}#/worldquant/datafields/x.json").as_bytes(), 1)).collect();
        let mut expected = None;
        for old in [true, false, false, true] {
            let alloc = crate::mem_probe::allocations();
            let start = Instant::now();
            let result = if old { reference_health(&physical, 1000) } else {
                let mut fold = LogicalHealth::new(1000);
                for enc in &physical { fold.push(enc); }
                fold.finish(0)
            };
            let elapsed = start.elapsed();
            let allocations = crate::mem_probe::allocations() - alloc;
            if let Some(want) = &expected { assert_eq!(&result, want); }
            else { expected = Some(result); }
            if old { assert!(allocations >= rows as u64, "reference must expose per-row allocation"); }
            else { assert!(allocations < 100, "ordinary health must reuse its key buffer"); }
            eprintln!("HEALTH_COST old={old} rows={rows} ms={:.3} allocations={allocations}", elapsed.as_secs_f64() * 1e3);
        }
    }

    #[test]
    fn bounded_tail_handles_interleaved_versions_and_empty_answers() {
        let mut physical = BTreeMap::new();
        let mut logical = BTreeMap::new();
        for i in 0u16..256 {
            let mut raw = vec![97];
            raw.extend_from_slice(&i.to_be_bytes());
            for version in [1, 2, 255, 256, u64::MAX] {
                physical.insert(encode_key(&raw, version), ());
            }
            logical.insert(raw.clone(), super::super::ExportRecord::new(raw, Some(vec![i as u8])));
        }
        for raw in [vec![], vec![97], vec![97, 0], vec![255]] {
            for version in [0, 1, 256, u64::MAX] { physical.insert(encode_key(&raw, version), ()); }
            logical.insert(raw.clone(), super::super::ExportRecord::new(raw, Some(vec![0])));
        }
        let bound = physical.last_key_value().map(|(key, _)| { let mut b = key.clone(); b.push(0); b });
        for n in [0, 1, 7, 300] {
            take_tail_buffer_peak();
            let rows = reverse_tail_bounded(n, bound.clone(),
                |b| physical.range::<[u8], _>((std::ops::Bound::Unbounded, std::ops::Bound::Excluded(b))).next_back().map(|(key, _)| key.clone()),
                |raw| logical.get(raw).cloned());
            let want: Vec<_> = logical.values().rev().take(n).rev().cloned().collect();
            assert_eq!(rows, want);
            assert!(take_tail_buffer_peak() <= n);
        }
        assert!(reverse_tail_bounded(7, bound,
            |b| physical.range::<[u8], _>((std::ops::Bound::Unbounded, std::ops::Bound::Excluded(b))).next_back().map(|(key, _)| key.clone()),
            |_| None).is_empty());
    }

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

#[cfg(test)]
#[test]
fn lazy_window_factory_is_called_only_for_new_groups() {
    use std::{assert_eq, vec};
    let mut window = KeyWindow::<Vec<u64>>::new();
    let mut calls = 0;
    for (key, value) in [(b"a".as_slice(), 1), (b"a".as_slice(), 2), (b"a\0".as_slice(), 3), (b"a".as_slice(), 4)] {
        window.entry_with(key, || { calls += 1; (key.to_vec(), vec![]) }).push(value);
    }
    assert_eq!(calls, 2);
    assert_eq!(window.pop_first(), Some((b"a".to_vec(), vec![1, 2, 4])));
    assert_eq!(window.pop_first(), Some((b"a\0".to_vec(), vec![3])));
    assert_eq!(window.pop_first(), None);
}

#[cfg(test)]
#[test]
fn candidates_copy_only_new_visible_groups_with_old_control() {
    use std::{assert, assert_eq};
    use super::store::scan_cost_reference;
    for reference in [true, false] {
        let _mode = reference.then(scan_cost_reference::Scope::start);
        let before = scan_cost_reference::key_copies();
        let accept = |key: &[u8]| key != b"rejected";
        let mut fold = CandidateFold::new(None, None, None, Some(&accept), 100, 10);
        for version in 1..=100 { assert!(!fold.push(&encode_key(b"key", version), |v| v % 2 == 0, || version)); }
        assert!(!fold.push(&encode_key(b"rejected", 100), |_| true, || 100));
        let copies = scan_cost_reference::key_copies() - before;
        let (out, complete) = fold.finish();
        assert!(complete);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].key, b"key");
        assert_eq!((out[0].version, out[0].row), (100, 100));
        assert_eq!(copies, if reference { 101 } else { 1 });
    }
}
