// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn snapshot_io_buffer_bounds_real_writes() {
    use super::*;
    let path = crate::common::ScratchPath::new("snapshot", "io_buffer", ".bxp");
    let mut reference_bytes = None;
    for reference in [true, false] {
        let _mode = reference.then(io_reference::Scope::start);
        let scope = snapshot_probe::Scope::start(false);
        atomic_write_bin_v4(&path, |w| {
            for i in 0u64..20_000 {
                w.write(&KvRecord::new(i.to_be_bytes().to_vec(), Some(vec![7; 64])))?;
            }
            Ok(())
        })
        .unwrap();
        let writes = scope.stats().write_calls;
        if reference {
            assert!(writes > 100);
        } else {
            assert!(writes <= 8);
        }
        let bytes = std::fs::read(&path).unwrap();
        if reference {
            reference_bytes = Some(bytes);
        } else {
            assert_eq!(Some(bytes), reference_bytes);
        }
        let recovery = recovery_probe::Scope::start();
        let mut seen = 0u64;
        read_snapshot_with(&path, ValueBuffer::Exact, &mut |row| {
            assert_eq!(row.key, seen.to_be_bytes());
            assert_eq!(row.value, Some(vec![7; 64]));
            seen += 1;
            true
        })
        .unwrap();
        assert_eq!(seen, 20_000);
        let reads = recovery.stats().reads;
        if reference {
            assert!(reads > 100);
        } else {
            assert!(reads < 30);
        }
    }
}

#[test]
fn snapshot_buffered_crc_matches_reference_and_batches_calls() {
    use super::*;
    let path = crate::common::ScratchPath::new("snapshot", "crc_blocks", ".bxp");
    let mut reference = None;
    for legacy in [true, false] {
        let scope = snapshot_probe::Scope::start(legacy);
        atomic_write_bin_v4(&path, |w| {
            for i in 0u64..2000 {
                let len = if i == 123 { 100_003 } else { 61 };
                w.write(&KvRecord::new(
                    i.to_be_bytes().to_vec(),
                    Some(vec![i as u8; len]),
                ))?;
            }
            w.write(&KvRecord::new(b"tombstone".to_vec(), None))?;
            Ok(())
        })
        .unwrap();
        let stats = scope.stats();
        drop(scope);
        let bytes = std::fs::read(&path).unwrap();
        let footer = bytes.len() - 16;
        assert_eq!(
            u32::from_le_bytes(bytes[footer + 12..].try_into().unwrap()),
            crate::common::disk::crc32(&bytes[..footer])
        );
        if legacy {
            assert!(
                stats.crc_calls > 10_000,
                "old per-field shape must exceed the new bound"
            );
            reference = Some(bytes);
        } else {
            assert_eq!(Some(bytes), reference, "BPEXP004 bytes must not change");
            assert!(stats.crc_calls < 100, "CRC was not moved below buffering");
            assert!(stats.sync_ns > 0, "checked snapshot must still sync");
        }
        let mut seen = 0u64;
        read_snapshot_with(&path, ValueBuffer::Exact, &mut |r| {
            if seen < 2000 {
                assert_eq!(r.key, seen.to_be_bytes());
                assert_eq!(
                    r.value,
                    Some(vec![seen as u8; if seen == 123 { 100_003 } else { 61 }])
                );
            } else {
                assert_eq!(r.value, None);
            }
            seen += 1;
            true
        })
        .unwrap();
        assert_eq!(seen, 2001);
    }
}

#[test]
#[ignore = "allocator peak probe; run alone with --test-threads=1"]
fn snapshot_streaming_memory_probe() {
    use super::*;
    let path = crate::common::ScratchPath::new("snapshot", "stream_peak", ".bxp");
    let rows = 200_000;
    atomic_write_bin_v4(&path, |w| {
        for i in 0u64..rows {
            w.write(&KvRecord::new(i.to_be_bytes().to_vec(), Some(vec![0; 64])))?;
        }
        Ok(())
    })
    .unwrap();
    let file_bytes = std::fs::metadata(&path).unwrap().len();
    let measure = |whole_file: bool| {
        let baseline = crate::mem_probe::reset_peak();
        let control = whole_file.then(|| std::fs::read(&path).unwrap());
        let mut seen = 0u64;
        read_snapshot_with(&path, ValueBuffer::Exact, &mut |r| {
            assert_eq!(r.key, seen.to_be_bytes());
            assert_eq!(r.value.as_deref(), Some([0u8; 64].as_slice()));
            seen += 1;
            true
        })
        .unwrap();
        assert_eq!(seen, rows);
        std::hint::black_box(&control);
        crate::mem_probe::peak() - baseline
    };
    let control = measure(true);
    let streaming = measure(false);
    eprintln!(
        "SNAPSHOT_READ rows={rows} file_bytes={file_bytes} whole_file_peak={control} streaming_peak={streaming}"
    );
    assert!(control >= file_bytes, "control did not hold the whole file");
    assert!(
        streaming < 256 * 1024,
        "reader retained more than a buffer and current record"
    );
}

#[test]
fn snapshot_streaming_stops_before_the_next_record() {
    use super::*;
    struct FirstOnly(std::io::Cursor<Vec<u8>>);
    impl std::io::Read for FirstOnly {
        fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
            if self.0.position() == self.0.get_ref().len() as u64 {
                return Err(std::io::Error::other("read past first record"));
            }
            std::io::Read::read(&mut self.0, out)
        }
    }
    let mut body = Vec::new();
    body.extend_from_slice(&1u32.to_le_bytes());
    body.push(b'k');
    body.extend_from_slice(&1u32.to_le_bytes());
    body.push(b'v');
    let length = body.len() as u64 + 1000;
    let mut reader = FirstOnly(std::io::Cursor::new(body.clone()));
    assert_eq!(
        read_bin_records(
            &mut reader,
            length,
            false,
            false,
            false,
            None,
            ValueBuffer::Exact,
            &mut |_| false
        )
        .unwrap()
        .records,
        1
    );
    let mut reader = FirstOnly(std::io::Cursor::new(body));
    assert!(
        read_bin_records(
            &mut reader,
            length,
            false,
            false,
            false,
            None,
            ValueBuffer::Exact,
            &mut |_| true
        )
        .is_err()
    );
}

#[test]
fn atomic_snapshot_preserves_previous_on_write_or_publish_failure() {
    use super::*;
    let dir = crate::common::ScratchPath::new("snapshot", "atomic", "");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("snapshot.bxp");
    let old = KvRecord::new(b"old".to_vec(), Some(b"good".to_vec()));
    atomic_write_bin_v4(&path, |w| w.write(&old)).unwrap();
    let bytes = std::fs::read(&path).unwrap();
    assert!(
        atomic_write_bin_v4(&path, |w| {
            w.write(&KvRecord::new(b"partial".to_vec(), Some(vec![7; 20000])))?;
            Err(std::io::Error::other("injected write failure"))
        })
        .is_err()
    );
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    assert_eq!(read_bin(&path).unwrap().0, vec![old]);
    assert_eq!(
        std::fs::read_dir(&dir).unwrap().count(),
        1,
        "staging leaked"
    );
    let blocked = dir.join("blocked");
    std::fs::create_dir(&blocked).unwrap();
    std::fs::write(blocked.join("keep"), b"keep").unwrap();
    assert!(atomic_write_bin_v4(&blocked, |_| Ok(())).is_err());
    assert_eq!(std::fs::read(blocked.join("keep")).unwrap(), b"keep");
    assert_eq!(
        std::fs::read_dir(&dir).unwrap().count(),
        2,
        "staging leaked"
    );
}

#[test]
fn strict_snapshot_keeps_legacy_formats_and_import_tolerance() {
    use super::*;
    let path = crate::common::ScratchPath::new("snapshot", "legacy", ".bxp");
    for version in 1..=3 {
        let mut record = KvRecord::new(b"k".to_vec(), Some(vec![0, 1, 2]));
        if version == 2 {
            record.seq = Some(42);
        }
        if version == 3 {
            record.expire_unix_secs = Some(123);
        }
        let mut writer = if version == 3 {
            BinWriter::create_v3(&path)
        } else {
            BinWriter::create(&path, version == 2)
        }
        .unwrap();
        writer.write(&record).unwrap();
        writer.finish().unwrap();
        let mut back = Vec::new();
        read_snapshot_with(&path, ValueBuffer::Exact, &mut |r| {
            back.push(r);
            true
        })
        .unwrap();
        assert_eq!(back, vec![record]);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.pop();
        std::fs::write(&path, bytes).unwrap();
        assert!(read_snapshot_with(&path, ValueBuffer::Exact, &mut |_| true).is_err());
        assert_eq!(
            read_bin(&path).unwrap().1.records,
            0,
            "legacy import must tolerate partial tail"
        );
    }
}

#[test]
fn snapshot_crc_chunks_match_whole_file() {
    let bytes: Vec<u8> = (0..=255).cycle().take(1027).collect();
    for chunk in [1, 7, 8, 9, 64, 1024] {
        let mut crc = 0;
        for part in bytes.chunks(chunk) {
            crc = crate::common::disk::crc32_extend(crc, part);
        }
        assert_eq!(crc, crate::common::disk::crc32(&bytes));
    }
}
