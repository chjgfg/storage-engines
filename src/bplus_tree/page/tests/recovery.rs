// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn test_dblwr_recovers_torn_page() {
    let path = temp_db("torn");
    let page_id = {
        let mut pm = PageManager::open(&path, 4).unwrap();
        let p = pm.allocate_page();
        pm.write_page(p, b"good-data");
        pm.flush_all();
        p
    };

    // 模拟 torn page：破坏主文件 page 1 的 payload，但在 dblwr 放一份好页
    {
        use crate::bplus_tree::storage::{DoubleWriteBuffer, PAGE_SIZE, pack_page};
        // 必须用 open_paged：主文件是定宽页布局，共享的 DiskFile 在没声明块大小时
        // 会直接拒绝 read_page/write_page（猜一个页大小 = 把数据写到错误偏移上）
        let mut file = crate::bplus_tree::storage::DiskFile::open_paged(&path, PAGE_SIZE).unwrap();
        let mut bad = file.read_page(page_id.0).unwrap();
        // 破坏 payload
        if bad.len() > 100 {
            bad[100] ^= 0xFF;
        }
        file.write_page(page_id.0, &bad).unwrap();
        file.sync().unwrap();

        // dblwr 放正确页
        let good_payload = {
            let mut p = vec![0u8; PAGE_PAYLOAD_SIZE];
            p[..9].copy_from_slice(b"good-data");
            p
        };
        let packed = pack_page(page_id.0, &good_payload);
        let mut dw = DoubleWriteBuffer::open(dblwr_path(&path)).unwrap();
        dw.write_batch(&[(page_id.0, packed)]).unwrap();
    }

    // 重新打开应自动从 dblwr 恢复
    {
        let mut pm = PageManager::open(&path, 4).unwrap();
        let data = pm.read_page(page_id).unwrap();
        assert_eq!(&data[..9], b"good-data");
    }
    cleanup(&path);
}

/// 大量 free 后 reopen：freelist sidecar 必须完整恢复（旧 meta 只能塞 ~500 个）
#[test]
fn test_many_free_pages_persist_across_reopen() {
    let path = temp_db("manyfree");
    let n = 800usize; // > 旧 meta 容量 (~506)
    {
        let mut pm = PageManager::open(&path, 64).unwrap();
        let mut ids = Vec::new();
        for _ in 0..n {
            ids.push(pm.allocate_page());
        }
        for id in &ids {
            assert!(pm.free_page(*id));
        }
        assert_eq!(pm.stats().free_pages, n);
        pm.flush_all();
        // sidecar 文件应存在
        assert!(freelist_path(&path).exists());
    }
    {
        let mut pm = PageManager::open(&path, 64).unwrap();
        assert_eq!(pm.stats().free_pages, n, "reopen 后 free 数量应完整");
        // 复用不应抬高 next_page_id 到 1+2n
        let next_before = pm.stats().next_page_id;
        for _ in 0..n {
            let _ = pm.allocate_page();
        }
        assert_eq!(
            pm.stats().next_page_id,
            next_before,
            "应完全从 freelist 复用，不分配新页号"
        );
        assert_eq!(pm.stats().free_pages, 0);
        pm.flush_all();
    }
    cleanup(&path);
}
