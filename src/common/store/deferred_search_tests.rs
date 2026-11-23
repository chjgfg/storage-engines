//! deferred_search_tests 的测试对照门与计量。
use super::*;
use std::{assert, assert_eq, cell::Cell, vec};

struct Store {
    calls: Cell<usize>,
    excluded: usize,
    rows: usize,
}
impl VersionedStore for Store {
    type Row = usize;
    fn scan_candidates(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        accept: Option<&dyn Fn(&[u8]) -> bool>,
        after: Option<&[u8]>,
        budget: usize,
    ) -> (Vec<WinCandidate<usize>>, bool) {
        let first = after.map_or(0, |key| {
            u64::from_be_bytes(key.try_into().unwrap()) as usize + 1
        });
        let mut out = Vec::new();
        for i in first..self.rows {
            let key = (i as u64).to_be_bytes();
            if start.is_some_and(|lo| key.as_slice() < lo)
                || end.is_some_and(|hi| key.as_slice() >= hi)
                || accept.is_some_and(|f| !f(&key))
            {
                continue;
            }
            if out.len() == budget {
                return (out, false);
            }
            out.push(WinCandidate {
                key: key.to_vec(),
                version: 1,
                row: i,
            });
        }
        (out, true)
    }
    fn materialize(&self, row: usize) -> RowValue {
        self.calls.set(self.calls.get() + 1);
        if row < self.excluded {
            match row % 3 {
                0 => RowValue::Missing,
                1 => RowValue::Tombstone,
                _ => RowValue::Value(crate::common::codec::pack_ttl(b"expired", 1)),
            }
        } else {
            RowValue::Value(crate::common::codec::pack_plain(&vec![7; 64]))
        }
    }
}

#[test]
fn search_default_materializes_only_reached_rows() {
    let store = Store {
        calls: Cell::new(0),
        excluded: 0,
        rows: 10_000,
    };
    for reference in [true, false] {
        let mode = reference.then(search_materialize_reference::Scope::start);
        store.calls.set(0);
        let (page, more, _) =
            search_page_walk(&store, false, None, KeyMatchMode::Prefix, 0, 100, false);
        assert_eq!(page.len(), 100);
        assert!(more);
        assert_eq!(page[99].key, 99u64.to_be_bytes());
        let guard = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            assert_eq!(store.calls.get(), 101, "no-total must stop materialization")
        }));
        assert_eq!(
            guard.is_err(),
            reference,
            "restored eager arm must fail the same guard"
        );
        assert_eq!(store.calls.get(), if reference { 4096 } else { 101 });
        drop(mode);
    }
}

#[test]
fn search_deferred_keeps_missing_ttl_deleted_and_chunk_cursor_semantics() {
    let store = Store {
        calls: Cell::new(0),
        excluded: 5000,
        rows: 9000,
    };
    for include_deleted in [false, true] {
        let expected = (0..store.rows)
            .filter(|&i| i >= store.excluded || (include_deleted && i % 3 != 0))
            .collect::<Vec<_>>();
        for total in [false, true] {
            for skip in [0, 4100, 8990] {
                let mut results = Vec::new();
                for reference in [true, false] {
                    let mode = reference.then(search_materialize_reference::Scope::start);
                    let result = search_page_walk(
                        &store,
                        include_deleted,
                        None,
                        KeyMatchMode::Contains,
                        skip,
                        17,
                        total,
                    );
                    let keys = result
                        .0
                        .iter()
                        .map(|r| u64::from_be_bytes(r.key.as_slice().try_into().unwrap()) as usize)
                        .collect::<Vec<_>>();
                    assert_eq!(
                        keys,
                        expected
                            .iter()
                            .skip(skip)
                            .take(17)
                            .copied()
                            .collect::<Vec<_>>()
                    );
                    assert_eq!(result.1, expected.len() > skip + 17);
                    if total {
                        assert_eq!(result.2, expected.len());
                    }
                    results.push(result);
                    drop(mode);
                }
                assert_eq!(results[0], results[1]);
            }
        }
        assert_eq!(
            count_window(&store, include_deleted, None, KeyMatchMode::Prefix),
            expected.len()
        );
    }
}
