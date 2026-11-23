// 原模块根作用域中的守卫；保留历史测试名称。

#[cfg(test)]
#[test]
fn lazy_window_factory_is_called_only_for_new_groups() {
    use std::{assert_eq, vec};
    let mut window = KeyWindow::<Vec<u64>>::new();
    let mut calls = 0;
    for (key, value) in [
        (b"a".as_slice(), 1),
        (b"a".as_slice(), 2),
        (b"a\0".as_slice(), 3),
        (b"a".as_slice(), 4),
    ] {
        window
            .entry_with(key, || {
                calls += 1;
                (key.to_vec(), vec![])
            })
            .push(value);
    }
    assert_eq!(calls, 2);
    assert_eq!(window.pop_first(), Some((b"a".to_vec(), vec![1, 2, 4])));
    assert_eq!(window.pop_first(), Some((b"a\0".to_vec(), vec![3])));
    assert_eq!(window.pop_first(), None);
}

#[cfg(test)]
#[test]
fn candidates_copy_only_new_visible_groups_with_old_control() {
    use super::store::scan_cost_reference;
    use std::{assert, assert_eq};
    for reference in [true, false] {
        let _mode = reference.then(scan_cost_reference::Scope::start);
        let before = scan_cost_reference::key_copies();
        let accept = |key: &[u8]| key != b"rejected";
        let mut fold = CandidateFold::new(None, None, None, Some(&accept), 100, 10);
        for version in 1..=100 {
            assert!(!fold.push(&encode_key(b"key", version), |v| v % 2 == 0, || version));
        }
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
