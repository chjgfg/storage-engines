// 原模块根作用域中的守卫；保留历史测试名称。

#[cfg(test)]
#[test]
fn streaming_cursor_copies_only_chunk_ends_with_rejected_last_rows() {
    use super::store::scan_cost_reference;
    use std::{assert_eq, vec::Vec};
    const ROWS: usize = 9000;
    for reference in [true, false] {
        let _mode = reference.then(scan_cost_reference::Scope::start);
        let before = scan_cost_reference::fold_cursor_copies();
        let mut out = Vec::new();
        let handed = fold_window_emit(
            None,
            |after, budget| {
                let start = after
                    .map(|b| u64::from_be_bytes(b.try_into().unwrap()) as usize + 1)
                    .unwrap_or(0);
                let end = (start + budget).min(ROWS);
                let rows = (start..end)
                    .map(|i| WinCandidate {
                        key: (i as u64).to_be_bytes().to_vec(),
                        version: 1,
                        row: i,
                    })
                    .collect();
                (rows, end == ROWS)
            },
            |c| (c.row % FOLD_CHUNK != FOLD_CHUNK - 1).then_some(c.row),
            |i| {
                out.push(i);
                true
            },
        );
        let expected: Vec<_> = (0..ROWS)
            .filter(|i| i % FOLD_CHUNK != FOLD_CHUNK - 1)
            .collect();
        assert_eq!(handed, expected.len());
        assert_eq!(out, expected);
        assert_eq!(
            scan_cost_reference::fold_cursor_copies() - before,
            if reference { ROWS as u64 } else { 2 }
        );
    }
}
