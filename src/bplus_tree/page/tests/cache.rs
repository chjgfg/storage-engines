// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn bulk_eviction_defers_freelist_sync_until_finish() {
    use crate::common::disk::{SITE_BPLUS_FREELIST, fsync_counts};
    let path = crate::common::ScratchPath::new("page", "bulk_freelist", ".db");
    {
        let mut pm = PageManager::open(&path, 64).unwrap();
        for _ in 0..256 {
            pm.allocate_page();
        }
        pm.try_flush_all().unwrap();
        for i in 1..=256 {
            pm.free_page(PageId(i));
        }
        pm.try_flush_all().unwrap();
        pm.set_bulk_mode(true);
        let before = fsync_counts()[SITE_BPLUS_FREELIST];
        for i in 1..=256_u64 {
            let pid = pm.allocate_page();
            assert_eq!(pid, PageId(i));
            assert!(pm.write_page(pid, &i.to_le_bytes()));
        }
        assert_eq!(fsync_counts()[SITE_BPLUS_FREELIST] - before, 0);
        pm.try_flush_all().unwrap();
        assert_eq!(fsync_counts()[SITE_BPLUS_FREELIST] - before, 1);
    }
    let mut pm = PageManager::open(&path, 64).unwrap();
    for i in 1..=256_u64 {
        assert_eq!(&pm.read_page(PageId(i)).unwrap()[..8], &i.to_le_bytes());
    }
    assert_eq!(pm.allocate_page(), PageId(257));
}

#[test]
fn eviction_reserves_metadata_and_preserves_every_page() {
    use crate::common::maintenance_probe::{Scope, Stage};
    for reference in [true, false] {
        let path = crate::common::ScratchPath::new("page", "metadata_batch", ".db");
        {
            let mut pm = PageManager::open(&path, 512).unwrap();
            for i in 1..=256_u64 {
                let pid = pm.allocate_page();
                assert_eq!(pid, PageId(i));
                assert!(pm.write_page(pid, &i.to_le_bytes()));
            }
            pm.set_root_page_id(PageId(1));
            pm.max_cache_pages = 256;
            let reference_mode = reference.then(eviction_reference::Scope::start);
            let scope = Scope::start();
            pm.evict_batch_until_room();
            let stats = scope.stats();
            assert_eq!(
                stats.calls[Stage::DblwrData as usize],
                if reference { 2 } else { 1 }
            );
            assert_eq!(
                stats.calls[Stage::PageSync as usize],
                if reference { 2 } else { 1 }
            );
            assert_eq!(pm.cache.len(), if reference { 224 } else { 193 });
            assert!(!pm.meta_dirty);
            drop(scope);
            drop(reference_mode);
            pm.try_flush_all().unwrap();
        }
        let mut pm = PageManager::open(&path, 512).unwrap();
        assert_eq!(pm.root_page_id(), Some(PageId(1)));
        for i in 1..=256_u64 {
            assert_eq!(&pm.read_page(PageId(i)).unwrap()[..8], &i.to_le_bytes());
        }
        drop(pm);
        cleanup(&path);
    }
}

#[test]
fn eviction_failure_retains_dirty_pages() {
    let path = crate::common::ScratchPath::new("page", "eviction_failure", ".db");
    {
        let mut pm = PageManager::open(&path, 1).unwrap();
        let pid = pm.allocate_page();
        assert!(pm.write_page(pid, b"unsynced"));
        let _fault = crate::bplus_tree::storage::dblwr_probe::Scope::fault(1);
        pm.evict_batch_until_room();
        assert_eq!(
            pm.get_page_state(pid),
            Some(PageState::Dirty),
            "failed eviction must retain the dirty page"
        );
        assert_eq!(&pm.read_page(pid).unwrap()[..8], b"unsynced");
        assert!(pm.ensure_healthy().is_err());
        assert!(
            pm.try_flush_all().is_err(),
            "a later checkpoint must not hide the failure"
        );
        assert!(!pm.flush_page(pid));
    }
    cleanup(&path);
}

#[test]
fn test_lru_eviction_writes_through() {
    let path = temp_db("lru");
    {
        let mut pm = PageManager::open(&path, 2).unwrap();
        let p1 = pm.allocate_page();
        let _p2 = pm.allocate_page();
        assert!(pm.write_page(p1, &[9]));
        let _p3 = pm.allocate_page();
        assert!(pm.cache.len() <= 2);
        let data = pm.read_page(p1).unwrap();
        assert_eq!(data[0], 9);
        pm.flush_all();
    }
    cleanup(&path);
}
