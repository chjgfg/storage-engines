// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn test_pack_unpack_roundtrip() {
    let payload = b"hello b+tree page";
    let page = pack_page(7, payload);
    assert_eq!(page.len(), PAGE_SIZE);
    let (id, body) = unpack_page(&page).unwrap();
    assert_eq!(id, 7);
    assert_eq!(&body[..payload.len()], payload);
}

#[test]
fn test_disk_file_rw() {
    let path = std::env::temp_dir().join(format!("storage_test_{}.bin", std::process::id()));
    {
        let mut f = DiskFile::open(&path).unwrap();
        f.write_all_at(0, b"abcdef").unwrap();
        f.sync().unwrap();
        let mut buf = [0u8; 6];
        f.read_exact_at(0, &mut buf).unwrap();
        assert_eq!(&buf, b"abcdef");
    }
    let _ = std::fs::remove_file(&path);
}

#[test]
fn dblwr_durable_empty_does_not_repeat_invalidation_sync() {
    use crate::common::disk::fsync_counts;
    let path = crate::common::ScratchPath::new("bplus", "dblwr_empty", ".bin");
    let mut dw = DoubleWriteBuffer::open(&path).unwrap();
    let pages = vec![(7, pack_page(7, b"new page"))];
    let before = fsync_counts()[SITE_BPLUS_DBLWR];
    dw.write_batch(&pages).unwrap();
    assert_eq!(dw.read_batch().unwrap(), pages);
    dw.clear().unwrap();
    assert_eq!(
        fsync_counts()[SITE_BPLUS_DBLWR] - before,
        3,
        "durably empty header needs payload/publish/clear sync only"
    );
}

#[test]
fn test_freelist_roundtrip() {
    let path = std::env::temp_dir().join(format!("freelist_test_{}.bin", std::process::id()));
    let ids: Vec<u64> = (1..1000).collect();
    write_freelist(&path, &ids).unwrap();
    let loaded = read_freelist(&path).unwrap().unwrap();
    assert_eq!(loaded, ids);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_blob_store_large_value() {
    let db = std::env::temp_dir().join(format!("blob_db_{}", std::process::id()));
    let _ = std::fs::remove_file(blob_path(&db));
    let mut store = BlobStore::open(&db).unwrap();
    let data = vec![7u8; 5000];
    let off = store.append(&data).unwrap();
    store.sync().unwrap();
    let got = store.read_at(off).unwrap();
    assert_eq!(got, data);

    let stored = encode_stored_value(&mut store, data.clone(), true).unwrap();
    assert_eq!(stored[0], VAL_TAG_BLOB);
    let decoded = decode_stored_value(&mut store, &stored).unwrap().unwrap();
    assert_eq!(decoded, data);

    let small = encode_stored_value(&mut store, b"ok".to_vec(), false).unwrap();
    assert_eq!(small[0], VAL_TAG_INLINE);
    assert_eq!(
        decode_stored_value(&mut store, &small).unwrap().unwrap(),
        b"ok"
    );
    let _ = std::fs::remove_file(blob_path(&db));
}
