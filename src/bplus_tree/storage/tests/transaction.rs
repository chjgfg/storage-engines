// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn test_dblwr_batch() {
    let path = std::env::temp_dir().join(format!("dblwr_test_{}.bin", std::process::id()));
    {
        let mut dw = DoubleWriteBuffer::open(&path).unwrap();
        let p1 = pack_page(1, b"aaa");
        let p2 = pack_page(2, b"bbb");
        dw.write_batch(&[(1, p1.clone()), (2, p2.clone())]).unwrap();
        let batch = dw.read_batch().unwrap();
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].0, 1);
        assert_eq!(batch[1].0, 2);
        dw.clear().unwrap();
        assert!(dw.read_batch().unwrap().is_empty());
    }
    let _ = std::fs::remove_file(&path);
}

#[test]
fn dblwr_reopen_and_interrupted_writes_require_reset() {
    use crate::common::disk::fsync_counts;
    let path = crate::common::ScratchPath::new("bplus", "dblwr_states", ".bin");
    let pages = vec![(7, pack_page(7, b"live page"))];
    {
        let mut dw = DoubleWriteBuffer::open(&path).unwrap();
        dw.write_batch(&pages).unwrap();
    }
    let mut dw = DoubleWriteBuffer::open(&path).unwrap();
    assert_eq!(dw.state, DblwrState::Unknown);
    assert_eq!(
        dw.read_batch().unwrap(),
        pages,
        "reopen must retain recoverable pages"
    );
    let before = fsync_counts()[SITE_BPLUS_DBLWR];
    dw.write_batch(&pages).unwrap();
    assert_eq!(fsync_counts()[SITE_BPLUS_DBLWR] - before, 3);
    // 批次已发布但清除尚未确认时，重开也必须先重置双写状态。
    let before = fsync_counts()[SITE_BPLUS_DBLWR];
    dw.write_batch(&pages).unwrap();
    assert_eq!(fsync_counts()[SITE_BPLUS_DBLWR] - before, 3);
    for fault in 1..=3 {
        dw.clear().unwrap();
        {
            let _fault = dblwr_probe::Scope::fault(fault);
            if fault == 3 {
                dw.write_batch(&pages).unwrap();
                assert!(dw.clear().is_err());
            } else {
                assert!(dw.write_batch(&pages).is_err());
            }
        }
        assert_eq!(dw.state, DblwrState::Unknown);
        if fault == 1 {
            assert!(dw.read_batch().unwrap().is_empty());
        }
        if fault == 2 {
            assert_eq!(dw.read_batch().unwrap(), pages);
        }
        let before = fsync_counts()[SITE_BPLUS_DBLWR];
        dw.write_batch(&pages).unwrap();
        assert_eq!(fsync_counts()[SITE_BPLUS_DBLWR] - before, 3);
        assert_eq!(dw.read_batch().unwrap(), pages);
    }
}

#[test]
fn dblwr_batches_payload_and_preserves_slots() {
    use crate::common::disk::fsync_counts;
    let path = crate::common::ScratchPath::new("bplus", "dblwr_slots", ".bin");
    let mut dw = DoubleWriteBuffer::open(&path).unwrap();
    for reference in [true, false] {
        let _mode = reference.then(dblwr_probe::Scope::start);
        for len in [0, 1, DBLWR_MAX_PAGES] {
            dw.clear().unwrap();
            let pages: Vec<_> = (0..len)
                .map(|i| ((i * 3) as u64, pack_page((i * 3) as u64, &[i as u8; 97])))
                .collect();
            let writes = dblwr_probe::writes();
            let syncs = fsync_counts()[SITE_BPLUS_DBLWR];
            dw.write_batch(&pages).unwrap();
            assert_eq!(dw.read_batch().unwrap(), pages);
            assert_eq!(
                dblwr_probe::writes() - writes,
                if reference {
                    len as u64
                } else {
                    u64::from(len > 0)
                }
            );
            assert_eq!(
                fsync_counts()[SITE_BPLUS_DBLWR] - syncs,
                if reference { 3 } else { 2 }
            );
        }
    }
}

#[test]
fn freelist_writer_reuses_handle_and_preserves_exact_layout() {
    use crate::common::disk::{file_opens, fsync_counts};
    use crate::common::maintenance_probe::{Scope, Stage};
    let path = crate::common::ScratchPath::new("storage", "freelist_writer", ".bin");
    let before = file_opens();
    let probe = Scope::start();
    let mut writer = FreelistWriter::open(&path).unwrap();
    for count in [256_u64, 3, 0, 129, 1] {
        let ids: Vec<u64> = (1..count + 1).collect();
        let syncs = fsync_counts()[SITE_BPLUS_FREELIST];
        let opens = file_opens();
        writer.write(ids.iter().copied()).unwrap();
        assert_eq!(file_opens(), opens);
        assert_eq!(fsync_counts()[SITE_BPLUS_FREELIST] - syncs, 1);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 20 + count * 8);
        assert_eq!(read_freelist(&path).unwrap().unwrap(), ids);
        let bytes = std::fs::read(&path).unwrap();
        let body: Vec<u8> = ids.iter().flat_map(|id| id.to_le_bytes()).collect();
        let mut expected = b"FREE0001".to_vec();
        expected.extend_from_slice(&count.to_le_bytes());
        expected.extend_from_slice(&crc32(&body).to_le_bytes());
        expected.extend_from_slice(&body);
        assert_eq!(bytes, expected);
    }
    assert_eq!(file_opens() - before, 6); // one writer, five independent readers
    let stats = probe.stats();
    assert_eq!(stats.calls[Stage::FreelistOpen as usize], 1);
    assert_eq!(stats.calls[Stage::FreelistSync as usize], 5);
    drop(probe);
    drop(writer);
    write_freelist(&path, &[9, 5]).unwrap();
    assert_eq!(read_freelist(&path).unwrap().unwrap(), vec![9, 5]);
}
