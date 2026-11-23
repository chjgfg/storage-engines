// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// 记账必须"攒够才取走"：曾经四引擎都用 `swap(0)` 取账，每次 begin 把没攒够的账清零
/// ⇒ 阈值永远到不了，自动回收静默停摆。这条断言就是钉那个反例的。
#[test]
fn scheduler_only_ticks_when_debt_reaches_slice() {
    let s = VacuumScheduler::new(8);
    for _ in 0..7 {
        s.on_commit(1);
        assert!(s.take_tick().is_none(), "没攒够不许取账");
    }
    s.on_commit(1);
    let t = s.take_tick().expect("攒够 8 行必须触发一片");
    assert_eq!((t.slice, t.charged), (8, 8));
    assert!(s.take_tick().is_none(), "取走后账要清零，不许连续空转");

    // 关闭：一律不触发，账也不许被偷偷吃掉
    s.set_slice(0);
    s.on_commit(100);
    assert!(s.take_tick().is_none());
    s.set_slice(8);
    let t = s.take_tick().expect("重新打开后原来的账还在");
    assert_eq!((t.slice, t.charged), (8, 8));
}

/// 一片失败/跳过时归还的必须正好是取走的那 `charged` 行：既不少（账不许被失败吃掉），
/// 也不多（不许凭空造账开出下一轮）。游标在半路时也一样。
#[test]
fn failed_slice_refunds_exactly_what_it_charged() {
    let s = VacuumScheduler::new(8);
    s.set_cursor(Some(b"mid".to_vec()));
    let pending = || s.pending.load(Ordering::SeqCst);
    s.on_commit(8);
    let fail: Result<(), &str> = s.run_pending(|_, _| Err("disk error"));
    assert_eq!(fail, Err("disk error"));
    assert_eq!(pending(), 8, "失败要把取走的 8 行原样退回");
    let _: Result<(), &str> = s.run_pending(|_, _| Err("disk error"));
    assert_eq!(pending(), 8, "连着失败也只是原样退回，不许越堆越高");
    assert_eq!(s.cursor().as_deref(), Some(&b"mid"[..]), "失败不许动游标");
}

#[test]
fn scheduler_refunds_errors_and_panics_and_serializes_cursor() {
    use crate::common::{VacuumRound, VacuumStats};
    let s = VacuumScheduler::new(8);
    s.on_commit(24);
    let status = || s.status(&Registry::new());
    let fail: Result<(), &str> = s.run_pending(|_, _| Err("disk error"));
    assert_eq!(fail, Err("disk error"));
    assert_eq!(status().pending_rows, 24);
    assert_eq!(status().failed_slices, 1);
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _: Result<(), ()> = s.run_pending(|_, _| panic!("probe"));
    }));
    assert!(panic.is_err());
    assert!(!status().running);
    assert_eq!(status().pending_rows, 24);
    let ok: Result<(), ()> = s.run_pending(|after, rows| {
        assert_eq!(after, None);
        assert_eq!(rows, 8);
        // 同一片还没返回时，再入不能取账或读写游标（等价于另一线程撞进来）。
        let nested: Result<(), ()> = s.run_pending(|_, _| panic!("duplicate slice"));
        assert!(nested.is_ok());
        Ok(VacuumRound {
            stats: VacuumStats {
                xmin: 1,
                versions_removed: 3,
                blob_rewritten: None,
                skipped_active_txn: false,
                rows_scanned: 8,
            },
            next_cursor: Some(b"next".to_vec()),
        })
    });
    assert!(ok.is_ok());
    assert_eq!(status().pending_rows, 16);
    assert_eq!(status().committed_rows, 24);
    assert_eq!(status().versions_removed, 3);
    assert_eq!(s.cursor().as_deref(), Some(&b"next"[..]));
}

#[test]
fn cursor_round_trips() {
    let s = VacuumScheduler::new(8);
    assert_eq!(s.cursor(), None);
    s.set_cursor(Some(b"k1".to_vec()));
    assert_eq!(s.cursor().as_deref(), Some(&b"k1"[..]));
    s.set_cursor(None);
    assert_eq!(s.cursor(), None);
}

#[test]
fn unfinished_is_answered_by_membership() {
    let mut ledger: crate::common::txn::Registry = std::collections::HashMap::new();
    ledger.insert(7u64, [b"k".to_vec()].into_iter().collect());
    let active: Mutex<crate::common::txn::Registry> = Mutex::new(ledger);
    assert!(is_unfinished(&active, 7), "还在活跃表里 ⇒ 未终结");
    // 判据必须**只查不删**：删除是引擎 rollback 的活，它要按表里那份 key 列表清行
    assert!(
        is_unfinished(&active, 7),
        "查一次不能把条目吃掉，否则 rollback 拿到空列表、行留在库里"
    );
    assert!(!is_unfinished(&active, 8), "没注册过的事务不该被回滚");
}
