// 原模块根作用域中的守卫；保留历史测试名称。

#[cfg(test)]
#[test]
fn live_owned_record_keeps_buffer_and_exact_ttl_with_old_control() {
    use crate::common::codec::{pack_plain, pack_ttl};
    use std::{assert_eq, assert_ne, vec};
    for bytes in [
        vec![],
        vec![0],
        vec![1, 2],
        vec![2, 0, 1],
        pack_plain(&[0, 1, 255]),
        pack_ttl(b"future", 101),
        pack_ttl(b"boundary", 100),
        pack_ttl(b"expired", 99),
    ] {
        let old = {
            let _mode = scan_cost_reference::Scope::start();
            let logical = bytes.clone();
            let pointer = logical.as_ptr();
            let record = live_record_of_logical(vec![0, 255], Some(logical), 100);
            if let Some(record) = &record {
                let value = record.value.as_ref().unwrap();
                if !value.is_empty() {
                    assert_ne!(
                        value.as_ptr(),
                        pointer,
                        "old decoding must allocate a separate user value"
                    );
                }
            }
            record
        };
        let pointer = bytes.as_ptr();
        let new = live_record_of_logical(vec![0, 255], Some(bytes), 100);
        if let Some(record) = &new {
            assert_eq!(record.value.as_ref().unwrap().as_ptr(), pointer);
        }
        assert_eq!(new, old);
    }
    assert_eq!(live_record_of_logical(vec![], None, 100), None);
}
