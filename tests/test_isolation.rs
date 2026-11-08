mod support;
use std::{assert, assert_eq, assert_ne, panic};

#[test]
fn owned_directories_are_distinct_and_cleanup_on_unwind() {
    let first = support::TestDir::new("isolation");
    let second = support::TestDir::new("isolation");
    assert_ne!(first.path(), second.path());
    let removed = first.path().to_path_buf();
    std::fs::write(first.join("owned"), b"first").unwrap();
    std::fs::write(second.join("keep"), b"second").unwrap();
    let result = std::panic::catch_unwind(move || {
        let _guard = first;
        panic!("exercise unwind cleanup");
    });
    assert!(result.is_err());
    assert!(!removed.exists());
    assert_eq!(std::fs::read(second.join("keep")).unwrap(), b"second");
}
