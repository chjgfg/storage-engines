// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn reclamation_coalesces_dirty_pages_with_failure_and_old_path_control() {
    use crate::common::disk::{SITE_BPLUS_PAGE_FLUSH, fsync_counts};
    for old in [true, false] {
        let path = crate::common::ScratchPath::new("page", "reclaim_coalesce", ".db");
        {
            let mut pm = PageManager::open(&path, 256).unwrap();
            for _ in 0..128 {
                pm.allocate_page();
            }
            pm.try_flush_all().unwrap();
            pm.begin_reclamation().unwrap();
            for i in [1u64, 100] {
                assert!(pm.write_page(PageId(i), &i.to_le_bytes()));
            }
            pm.max_cache_pages = 128;
            let mode = old.then(reclamation_batch_reference::Scope::start);
            let before = fsync_counts()[SITE_BPLUS_PAGE_FLUSH];
            pm.evict_batch_until_room();
            pm.try_flush_all().unwrap();
            pm.finish_reclamation().unwrap();
            let count = fsync_counts()[SITE_BPLUS_PAGE_FLUSH] - before;
            let guard = std::panic::catch_unwind(|| assert_eq!(count, 1));
            assert_eq!(guard.is_err(), old);
            assert_eq!(count, if old { 2 } else { 1 });
            drop(mode);
        }
        let mut pm = PageManager::open(&path, 256).unwrap();
        for i in [1u64, 100] {
            assert_eq!(&pm.read_page(PageId(i)).unwrap()[..8], &i.to_le_bytes());
        }
    }
    for fault in [1, 2, 3, 4, 5] {
        let path = crate::common::ScratchPath::new("page", "reclaim_coalesce_fault", ".db");
        let mut pm = PageManager::open(&path, 256).unwrap();
        for _ in 0..128 {
            pm.allocate_page();
        }
        pm.try_flush_all().unwrap();
        pm.begin_reclamation().unwrap();
        for i in [1u64, 100] {
            assert!(pm.write_page(PageId(i), &i.to_le_bytes()));
        }
        pm.max_cache_pages = 128;
        let _fault = crate::bplus_tree::storage::dblwr_probe::Scope::fault(fault);
        pm.evict_batch_until_room();
        assert!(pm.ensure_healthy().is_err());
        assert_eq!(pm.cache.len(), 128);
        for i in [1u64, 100] {
            assert_eq!(pm.get_page_state(PageId(i)), Some(PageState::Dirty));
        }
        assert!(pm.try_flush_all().is_err());
    }
}

#[test]
fn reclamation_refuses_unpublished_return_and_latches_publication_faults() {
    for fault in [0, 1, 2, 3, 4, 5, 6, 7] {
        let path = crate::common::ScratchPath::new("page", "reclamation_fault", ".db");
        let mut pm = PageManager::open(&path, 64).unwrap();
        for _ in 0..4 {
            pm.allocate_page();
        }
        pm.try_flush_all().unwrap();
        pm.begin_reclamation().unwrap();
        assert!(pm.free_page(PageId(1)));
        assert!(pm.write_page(PageId(2), b"retained"));
        if fault == 0 {
            assert!(pm.finish_reclamation().is_err());
        } else {
            let _fault = crate::bplus_tree::storage::dblwr_probe::Scope::fault(fault);
            assert!(
                pm.try_flush_all().is_err(),
                "fault {fault} must reach its publication door"
            );
            assert!(pm.finish_reclamation().is_err());
        }
        assert!(pm.ensure_healthy().is_err());
        assert!(pm.try_flush_all().is_err());
    }
}

#[test]
fn reclamation_empty_scope_preserves_unrelated_dirty_pages() {
    use crate::common::disk::fsync_counts;
    let path = crate::common::ScratchPath::new("page", "reclamation_empty", ".db");
    let mut pm = PageManager::open(&path, 64).unwrap();
    let pid = pm.allocate_page();
    assert!(pm.write_page(pid, b"ordinary"));
    let before = fsync_counts();
    pm.begin_reclamation().unwrap();
    pm.finish_reclamation().unwrap();
    assert_eq!(fsync_counts(), before);
    assert_eq!(pm.get_page_state(pid), Some(PageState::Dirty));
}

#[test]
fn freelist_publication_failure_keeps_dirty_state_and_refuses_retry() {
    for bulk in [false, true] {
        for fault in [6, 7] {
            let path = crate::common::ScratchPath::new("page", "freelist_failure", ".db");
            let mut pm = PageManager::open(&path, 64).unwrap();
            let pid = pm.allocate_page();
            pm.try_flush_all().unwrap();
            assert!(pm.free_page(pid));
            pm.set_bulk_mode(bulk);
            let _fault = crate::bplus_tree::storage::dblwr_probe::Scope::fault(fault);
            assert!(pm.try_flush_all().is_err());
            assert!(pm.freelist_dirty);
            assert!(pm.meta_dirty);
            assert!(pm.ensure_healthy().is_err());
            assert!(pm.try_flush_all().is_err());
        }
    }
}

#[test]
fn test_page_manager_disk() {
    let path = temp_db("basic");
    let page_id = {
        let mut pm = PageManager::open(&path, 4).unwrap();
        let page_id = pm.allocate_page();
        assert_eq!(page_id, PageId(1));
        assert!(pm.write_page(page_id, &[1, 2, 3]));
        pm.flush_all();
        let data = pm.read_page(page_id).unwrap();
        assert_eq!(&data[..3], &[1, 2, 3]);
        assert_eq!(pm.get_page_state(page_id), Some(PageState::Clean));
        page_id
    };

    {
        let mut pm = PageManager::open(&path, 4).unwrap();
        let data = pm.read_page(page_id).unwrap();
        assert_eq!(&data[..3], &[1, 2, 3]);
    }
    cleanup(&path);
}

#[test]
fn test_root_persists() {
    let path = temp_db("root");
    {
        let mut pm = PageManager::open(&path, 4).unwrap();
        let root = pm.allocate_page();
        pm.set_root_page_id(root);
        pm.flush_all();
    }
    {
        let pm = PageManager::open(&path, 4).unwrap();
        assert_eq!(pm.root_page_id(), Some(PageId(1)));
    }
    cleanup(&path);
}

#[test]
fn test_free_and_reuse() {
    let path = temp_db("free");
    {
        let mut pm = PageManager::open(&path, 8).unwrap();
        let p1 = pm.allocate_page();
        assert!(pm.free_page(p1));
        let p2 = pm.allocate_page();
        assert_eq!(p1, p2);
        pm.flush_all();
    }
    cleanup(&path);
}
