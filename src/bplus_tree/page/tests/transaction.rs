// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn reclamation_defers_freelist_but_keeps_double_write_and_prevents_reuse() {
    use crate::common::disk::{
        SITE_BPLUS_DBLWR, SITE_BPLUS_FREELIST, SITE_BPLUS_PAGE_FLUSH, fsync_counts,
    };
    let path = crate::common::ScratchPath::new("page", "reclamation_publication", ".db");
    {
        let mut pm = PageManager::open(&path, 16).unwrap();
        for i in 1..=128_u64 {
            let pid = pm.allocate_page();
            assert!(pm.write_page(pid, &i.to_le_bytes()));
        }
        pm.try_flush_all().unwrap();
        for i in 1..=8 {
            assert!(pm.free_page(PageId(i)));
        }
        pm.try_flush_all().unwrap();
        pm.begin_reclamation().unwrap();
        let before = fsync_counts();
        for i in 9..=24 {
            assert!(pm.free_page(PageId(i)));
        }
        for i in 25..=128_u64 {
            assert!(pm.write_page(PageId(i), &(i + 1000).to_le_bytes()));
        }
        assert_eq!(
            fsync_counts()[SITE_BPLUS_FREELIST] - before[SITE_BPLUS_FREELIST],
            0
        );
        assert!(fsync_counts()[SITE_BPLUS_DBLWR] > before[SITE_BPLUS_DBLWR]);
        assert!(fsync_counts()[SITE_BPLUS_PAGE_FLUSH] > before[SITE_BPLUS_PAGE_FLUSH]);
        assert_eq!(
            read_freelist(&freelist_path(&path)).unwrap().unwrap(),
            (1..=8).collect::<Vec<u64>>()
        );
        assert_eq!(
            pm.allocate_page(),
            PageId(129),
            "unpublished free pages must not be reused"
        );
        pm.try_flush_all().unwrap();
        pm.finish_reclamation().unwrap();
        assert_eq!(
            fsync_counts()[SITE_BPLUS_FREELIST] - before[SITE_BPLUS_FREELIST],
            1
        );
        pm.try_flush_all().unwrap();
        assert_eq!(
            fsync_counts()[SITE_BPLUS_FREELIST] - before[SITE_BPLUS_FREELIST],
            1
        );
    }
    let mut pm = PageManager::open(&path, 16).unwrap();
    for i in 25..=128_u64 {
        assert_eq!(
            &pm.read_page(PageId(i)).unwrap()[..8],
            &(i + 1000).to_le_bytes()
        );
    }
    for i in 1..=24 {
        assert_eq!(pm.allocate_page(), PageId(i));
    }
    std::eprintln!(
        "RECLAMATION_VERIFIED freelist_sync=1 double_write=retained reused_after_publication=24"
    );
}

#[test]
fn bulk_finish_syncs_evicted_clean_pages_and_latches_failure() {
    use crate::common::disk::{SITE_BPLUS_PAGE_FLUSH, fsync_counts};
    for (switch_mode, fault) in [(false, false), (true, false), (false, true)] {
        let path = crate::common::ScratchPath::new("page", "bulk_pending_sync", ".db");
        {
            let mut pm = PageManager::open(&path, 64).unwrap();
            pm.set_bulk_mode(true);
            let pid = pm.allocate_page();
            assert!(pm.write_page(pid, b"bulk"));
            let before = fsync_counts()[SITE_BPLUS_PAGE_FLUSH];
            pm.flush_pages_batch(&[pid], false).unwrap();
            assert_eq!(pm.get_page_state(pid), Some(PageState::Clean));
            assert!(!pm.meta_dirty);
            pm.cache.clear(); // all remaining cached pages have already been published
            if switch_mode {
                pm.set_bulk_mode(false);
            }
            let injection = fault.then(|| crate::bplus_tree::storage::dblwr_probe::Scope::fault(5));
            let result = pm.try_flush_all();
            assert_eq!(result.is_err(), fault);
            if fault {
                assert!(pm.bulk_pending_sync);
                assert!(pm.ensure_healthy().is_err());
                assert!(pm.try_flush_all().is_err());
            } else {
                assert!(!pm.bulk_pending_sync);
                assert_eq!(fsync_counts()[SITE_BPLUS_PAGE_FLUSH] - before, 1);
                pm.try_flush_all().unwrap();
                assert_eq!(fsync_counts()[SITE_BPLUS_PAGE_FLUSH] - before, 1);
            }
            drop(injection);
        }
        if !fault {
            let mut pm = PageManager::open(&path, 64).unwrap();
            assert_eq!(&pm.read_page(PageId(1)).unwrap()[..4], b"bulk");
        }
    }
}

#[test]
fn test_single_writer_lock_rejects_second_open() {
    let path = temp_db("lock");
    let pm1 = PageManager::open(&path, 4).unwrap();
    let err = PageManager::open(&path, 4).err();
    assert!(err.is_some(), "第二进程/句柄应拿不到锁");
    let e = err.unwrap();
    // WouldBlock 或 PermissionDenied 均可
    assert!(
        e.kind() == std::io::ErrorKind::WouldBlock
            || e.kind() == std::io::ErrorKind::PermissionDenied
            || e.to_string().contains("锁")
            || e.to_string().contains("占用"),
        "unexpected error: {e:?}"
    );
    drop(pm1);
    // 释放后应能再 open
    let pm2 = PageManager::open(&path, 4).unwrap();
    drop(pm2);
    cleanup(&path);
}
