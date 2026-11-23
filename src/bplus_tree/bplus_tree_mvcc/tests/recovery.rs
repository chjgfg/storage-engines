// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// M52：`Drop` 是**另一个**收口点（与 `finish_inner` 不是一个函数），同一个洞也在那儿。
/// 抄写两份就得各钉一次 —— 正常关闭库时刚 append 的 Checkpoint 会被随后的 truncate 抹掉。
#[test]
fn test_drop_leaves_a_usable_wal_checkpoint() {
    use crate::bplus_tree::wal::{Wal, WalRecord};
    let path = tmp_db("drop_wal");
    {
        let mvcc = MVCC::open(&path, 64, 0);
        let tx = mvcc.begin_transaction();
        for i in 0..50u64 {
            assert!(tx.set(format!("d{i:04}").as_bytes(), b"v".to_vec()));
        }
        tx.commit();
    }
    // mvcc 已离开作用域 => Drop 里的收口跑完了，直接看盘上那份 WAL
    let mut wal = Wal::open(wal_path(&path)).unwrap();
    let entries = wal.read_all().unwrap();
    let n_ckpt = entries
        .iter()
        .filter(|e| matches!(e.record, WalRecord::Checkpoint { .. }))
        .count();
    eprintln!(
        "[Drop 收口] bplus WAL 剩 {} 条、Checkpoint {n_ckpt} 条",
        entries.len()
    );
    assert_eq!(
        n_ckpt, 1,
        "Drop 之后 WAL 里没有可用的 Checkpoint（同一个洞的第二处）"
    );
    let mvcc = MVCC::open(&path, 64, 0);
    let tx = mvcc.begin_transaction();
    assert_eq!(tx.key_count(b"d"), 50, "重开之后行数不对");
    tx.commit();
    drop(mvcc);
    cleanup(&path);
}

/// WAL 阈值触发必须真的把文件截住（对照「关闭自动触发」），且截断之后重开仍能读到
/// 全部已提交数据 —— 后半段守的是「先把 next_version 记进 meta 并 flush 页、
/// 最后才收口 WAL」这个顺序：顺序反了就是**丢已提交写入**。
#[test]
fn test_wal_auto_checkpoint_bounds_file_and_keeps_data() {
    const WRITES: u32 = 40;

    // 注意量测时机：本引擎有 `impl Drop for MVCC`（析构时自带一次 checkpoint），
    // 所以必须在实例还活着的时候量，否则两种配置都会被 drop 截成 header 大小，
    // 断言就变成了空转（第一版我就踩了这个坑：量到 32 对 32）。
    fn wal_bytes_after(tag: &str, limit: u64) -> u64 {
        let path = tmp_db(tag);
        let size;
        {
            let mvcc = MVCC::open(&path, 4, 16);
            mvcc.set_wal_checkpoint_limit(limit);
            for i in 0..WRITES {
                let key = format!("k{i}");
                let tx = mvcc.begin_transaction();
                assert!(tx.set(key.as_bytes(), vec![b'x'; 200]));
                tx.commit();
            }
            size = std::fs::metadata(wal_path(&path))
                .map(|m| m.len())
                .unwrap_or(0);
        }
        cleanup(&path);
        size
    }

    let manual = wal_bytes_after("wal_manual", 0);
    let auto = wal_bytes_after("wal_auto", 1);
    eprintln!("WAL 体积：关闭自动 checkpoint {manual} 字节 vs 阈值触发 {auto} 字节");
    assert!(
        auto * 4 <= manual,
        "阈值触发应把 WAL 截住（自动 {auto}，手动 {manual}）"
    );

    let path = tmp_db("wal_reopen");
    {
        let mvcc = MVCC::open(&path, 4, 16);
        mvcc.set_wal_checkpoint_limit(1);
        for i in 0..WRITES {
            let key = format!("r{i}");
            let tx = mvcc.begin_transaction();
            assert!(tx.set(key.as_bytes(), format!("v{i}").into_bytes()));
            tx.commit();
        }
    }
    {
        let mvcc = MVCC::open(&path, 4, 16);
        let tx = mvcc.begin_transaction();
        for i in 0..WRITES {
            let key = format!("r{i}");
            assert_eq!(
                tx.get(key.as_bytes()),
                Some(format!("v{i}").into_bytes()),
                "第 {i} 条已提交写入在 WAL 被截断后仍应读到"
            );
        }
        tx.commit();
    }
    cleanup(&path);
}

#[test]
fn test_disk_persist_across_reopen() {
    let path = tmp_db("persist");
    {
        let mvcc = MVCC::open(&path, 4, 16);
        let t1 = mvcc.begin_transaction();
        assert!(t1.set(b"hello", b"world".to_vec()));
        t1.commit();
        let t2 = mvcc.begin_transaction();
        assert!(t2.set(b"hello", b"rust".to_vec()));
        assert!(t2.set(b"foo", b"bar".to_vec()));
        t2.commit();
    }
    {
        let mvcc = MVCC::open(&path, 4, 16);
        let tx = mvcc.begin_transaction();
        assert_eq!(tx.get(b"hello"), Some(b"rust".to_vec()));
        assert_eq!(tx.get(b"foo"), Some(b"bar".to_vec()));
        tx.commit();
    }
    cleanup(&path);
}

/// 干净 checkpoint 后重开：meta 持久化 next_version，version 连续且数据可读
#[test]
fn test_reopen_preserves_next_version_after_checkpoint() {
    let path = tmp_db("reopen_nv");
    let v1;
    {
        let mvcc = MVCC::open(&path, 16, 32);
        let t = mvcc.begin_transaction();
        v1 = t.version();
        assert!(t.set(b"k", b"v1".to_vec()));
        t.commit();
        // checkpoint 把 next_version 写入 meta 并截断 WAL
        mvcc.checkpoint();
    }
    {
        let mvcc = MVCC::open(&path, 16, 32);
        let t = mvcc.begin_transaction();
        // 新事务 version 必须严格大于上次已用 version
        assert!(
            t.version() > v1,
            "reopen version={} should > {}",
            t.version(),
            v1
        );
        assert_eq!(t.get(b"k"), Some(b"v1".to_vec()));
        assert!(t.set(b"k", b"v2".to_vec()));
        t.commit();
    }
    {
        let mvcc = MVCC::open(&path, 16, 32);
        let t = mvcc.begin_transaction();
        assert_eq!(t.get(b"k"), Some(b"v2".to_vec()));
        t.commit();
    }
    cleanup(&path);
}

/// 未手动 checkpoint：meta 可能仍是上次 open 写入的旧 next_version，
/// 应靠 WAL 中 max_xid 把起点抬高，保证 version 单调。
#[test]
fn test_reopen_uses_wal_max_xid_without_checkpoint() {
    let path = tmp_db("reopen_wal");
    let mut last_v = 0u64;
    {
        let mvcc = MVCC::open(&path, 16, 32);
        for i in 0..5 {
            let t = mvcc.begin_transaction();
            last_v = t.version();
            assert!(t.set(format!("k{i}").as_bytes(), b"x".to_vec()));
            t.commit();
        }
        // 故意不 checkpoint：WAL 里保留 Write/Commit
    }
    {
        let mvcc = MVCC::open(&path, 16, 32);
        let t = mvcc.begin_transaction();
        assert!(
            t.version() > last_v,
            "version after reopen={} must > last committed {}",
            t.version(),
            last_v
        );
        assert_eq!(t.get(b"k4"), Some(b"x".to_vec()));
        t.commit();
    }
    cleanup(&path);
}
