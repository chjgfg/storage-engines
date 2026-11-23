// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn record_batches_keep_bytes_counts_large_fallback_and_finish_flush() {
    use super::*;
    let path = crate::common::ScratchPath::new("snapshot", "record_batches", ".bxp");
    let records = vec![
        KvRecord::new(vec![], Some(vec![])),
        KvRecord::new(
            b"exact".to_vec(),
            Some(vec![1; RECORD_BATCH_BYTES - 16 - 5]),
        ),
        KvRecord::new(b"large".to_vec(), Some(vec![2; RECORD_BATCH_BYTES])).with_expire(u64::MAX),
        KvRecord::new(b"tomb".to_vec(), None),
        KvRecord::new(b"tail".to_vec(), Some(vec![0, 1, 255])).with_expire(u64::MAX),
    ];
    let mut golden = None;
    for reference in [true, false] {
        let mode = reference.then(snapshot_emit_reference::Scope::start);
        let scope = snapshot_probe::Scope::start(false);
        let counts = atomic_write_bin_v4(&path, |w| {
            for (i, record) in records.iter().enumerate() {
                if i == 3 {
                    w.write(record)?;
                    continue;
                }
                let outcome = w.write_snapshot_batched(
                    &record.key,
                    record.value.as_deref(),
                    record.expire_unix_secs,
                )?;
                assert_eq!(
                    outcome,
                    if reference {
                        RecordEncoding::DirectReference
                    } else if i == 2 {
                        RecordEncoding::DirectLarge
                    } else {
                        RecordEncoding::Buffered
                    }
                );
            }
            // 尾部仍在缓冲区里，BinWriter.finish 必须写出并记入条数。
            if !reference {
                assert_eq!(w.record_batch.pending.records, 1);
                assert_eq!(w.stats.records, 4);
            }
            Ok(())
        })
        .unwrap();
        assert_eq!((counts.records, counts.live, counts.deleted), (5, 4, 1));
        let stats = scope.stats();
        if reference {
            assert_eq!(stats.record_buffer_bytes, 0);
            assert_eq!(stats.record_reference_rows, 4);
        } else {
            assert_eq!(stats.record_buffer_bytes, RECORD_BATCH_BYTES);
            assert_eq!(stats.record_buffered_rows, 3);
            assert_eq!(stats.record_flushes, 3);
            assert_eq!(stats.record_direct_large_rows, 1);
            assert!(stats.record_direct_large_ns > 0);
        }
        drop(scope);
        drop(mode);
        let bytes = std::fs::read(&path).unwrap();
        if let Some(golden) = &golden {
            assert_eq!(&bytes, golden);
        } else {
            golden = Some(bytes);
        }
        assert_eq!(read_bin(&path).unwrap().0, records);
    }
    let scope = snapshot_probe::Scope::start(false);
    atomic_write_bin_v4(&path, |_| Ok(())).unwrap();
    assert_eq!(scope.stats().record_buffer_bytes, 0);
    assert_eq!(scope.stats().record_flushes, 0);
}

#[test]
fn record_batch_flush_failure_preserves_previous_and_unaccepted_counts() {
    use super::*;
    let path = crate::common::ScratchPath::new("snapshot", "record_flush_failure", ".bxp");
    atomic_write_bin_v4(&path, |w| {
        w.write(&KvRecord::new(b"old".to_vec(), Some(vec![7])))
    })
    .unwrap();
    let golden = std::fs::read(&path).unwrap();
    let result = atomic_write_bin_v4(&path, |w| {
        let large = vec![1; RECORD_BATCH_BYTES - 16];
        assert_eq!(
            w.write_snapshot_batched(b"", Some(&large), None)?,
            RecordEncoding::Buffered
        );
        // 使用真实只读文件句柄，确定性触发批量写入失败。
        w.w.get_mut().file = File::open(&path)?;
        let result = w.flush_record_batch();
        assert!(result.is_err());
        assert_eq!(w.stats.records, 0);
        assert_eq!(w.record_batch.pending.records, 1);
        result
    });
    assert!(result.is_err());
    assert_eq!(std::fs::read(&path).unwrap(), golden);
    assert_eq!(read_bin(&path).unwrap().0[0].key, b"old");
}
