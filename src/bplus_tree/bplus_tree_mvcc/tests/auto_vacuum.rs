// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn empty_auto_vacuum_leaves_page_sync_to_checkpoint() {
    use crate::common::disk::{SITE_BPLUS_PAGE_FLUSH, fsync_counts};
    use crate::common::maintenance_probe::{Scope, Stage};
    let path = tmp_db("empty_auto_vacuum");
    {
        let db = MVCC::open(&path, 64, 128);
        db.set_vacuum_auto_slice(0);
        let tx = db.begin_transaction();
        assert!(tx.set_raw(b"key", b"value".to_vec()));
        tx.try_commit().unwrap();
        drop(tx);
        db.set_vacuum_auto_slice(1);
        let before = fsync_counts()[SITE_BPLUS_PAGE_FLUSH];
        let scope = Scope::start();
        let tx = db.begin_transaction();
        assert_eq!(tx.get(b"key"), Some(b"value".to_vec()));
        tx.try_commit().unwrap();
        drop(tx);
        let stats = scope.stats();
        assert_eq!(stats.calls[Stage::AutoVacuum as usize], 1);
        assert!(stats.bplus_maintenance_rows > 0);
        assert_eq!(fsync_counts()[SITE_BPLUS_PAGE_FLUSH] - before, 0);
        drop(scope);
        db.try_checkpoint().unwrap();
        assert!(fsync_counts()[SITE_BPLUS_PAGE_FLUSH] > before);
    }
    let db = MVCC::open(&path, 64, 128);
    let tx = db.begin_transaction();
    assert_eq!(tx.get(b"key"), Some(b"value".to_vec()));
    tx.try_commit().unwrap();
}

/// 长跑进程不喊 vacuum，版本链也不能无限涨（I2：垃圾必须自己回来）。
///
/// bplus 的一片要真读叶子页，所以这里除了「链被压住」还断言**最新值始终读得到**：
/// 自动裁的是旧版本，不是可见版本 —— 裁错就是静默数据错误。
#[test]
fn test_auto_vacuum_reclaims_without_being_asked() {
    const CHAIN: u64 = 200;
    const SLICE: u64 = 8;

    let path = tmp_db("auto_vac");
    {
        let mvcc = MVCC::open(&path, 16, 32);
        assert_eq!(
            mvcc.vacuum_auto_slice(),
            MVCC::VACUUM_AUTO_SLICE_DEFAULT,
            "自动回收默认必须开着"
        );
        mvcc.set_vacuum_auto_slice(SLICE);

        for i in 0..CHAIN {
            let tx = mvcc.begin_transaction();
            assert!(tx.set(b"hot", format!("v{i}").into_bytes()));
            tx.commit();
        }

        let h = mvcc.health();
        assert!(
            h.max_chain <= SLICE + 2,
            "没人喊 vacuum，链却涨到 {} 行 —— 自动回收没在工作",
            h.max_chain
        );

        let tx = mvcc.begin_transaction();
        assert_eq!(tx.get(b"hot"), Some(format!("v{}", CHAIN - 1).into_bytes()));
        tx.commit();
    }
    cleanup(&path);
}

/// 关掉的开关必须真关掉：自动回收不能是「没法关掉的那种」。
#[test]
fn test_auto_vacuum_off_switch() {
    const CHAIN: u64 = 60;

    let path = tmp_db("auto_off");
    {
        let mvcc = MVCC::open(&path, 16, 32);
        mvcc.set_vacuum_auto_slice(0);
        for i in 0..CHAIN {
            let tx = mvcc.begin_transaction();
            assert!(tx.set(b"hot", format!("v{i}").into_bytes()));
            tx.commit();
        }
        assert_eq!(mvcc.health().max_chain, CHAIN, "关闭后不该裁掉任何一版");
    }
    cleanup(&path);
}
