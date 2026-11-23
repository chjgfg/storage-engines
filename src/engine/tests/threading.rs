// 该文件集中验证对应职责，保留原测试名称和字面值。

#[test]
fn engine_is_object_safe_in_collections() {
    // 关键回归：Box<dyn KvEngine> 必须能放进容器并统一驱动
    let dirs = vec![tmp_dir("obj1")];
    let mut engines: Vec<Box<dyn KvEngine>> = Vec::new();
    engines.push(open_engine("memory", None).unwrap());
    engines.push(open_engine("lsm", Some(&dirs[0])).unwrap());

    for db in &engines {
        let tx = db.begin();
        tx.set(b"x", b"y".to_vec());
        tx.commit();
    }
    for db in &engines {
        let tx = db.begin();
        assert_eq!(tx.get(b"x"), Some(b"y".to_vec()), "{}", db.name());
        tx.commit();
    }
    drop(engines);
    cleanup(dirs.into_iter().map(Some).collect());
}

/// 门面必须**能跨线程用**。
///
/// 这不是锦上添花：`Box<dyn KvEngine>` 少了 `Send`/`Sync`，既进不了 `Arc` 也搬不到
/// 别的线程，而交易进程的常态就是"开库线程 + 心跳线程 checkpoint + 工作线程读写"。
/// 下游正是因为这个才在应用里又包了一层 trait —— 那条 wrapper 现在可以删了。
/// 这条断言在 trait 上界被人改掉时立刻编译失败。
#[test]
fn facade_is_usable_across_threads() {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}
    assert_send::<Box<dyn KvEngine>>();
    assert_sync::<Box<dyn KvEngine>>();

    // 1) 事务搬到别的线程去写（scoped：`Box<dyn KvTxn + '_>` 带着对引擎的借用，
    //    普通 spawn 要求 'static，所以这里用 scope —— 要证的还是 Send）
    let dir = tmp_dir("send_txn");
    {
        let db = open_engine("bitcask", Some(&dir)).unwrap();
        std::thread::scope(|sc| {
            let t = db.begin();
            sc.spawn(move || {
                assert!(t.set(b"moved", b"txn".to_vec()));
                t.commit();
            });
        });
        let t = db.begin();
        assert_eq!(t.get(b"moved"), Some(b"txn".to_vec()));
        t.commit();
    }
    let _ = std::fs::remove_dir_all(&dir);

    // 2) 句柄进 Arc，四个线程共用同一个库
    let db = std::sync::Arc::new(open_engine("memory", None).unwrap());
    let mut handles = Vec::new();
    for i in 0..4u8 {
        let db = db.clone();
        handles.push(std::thread::spawn(move || {
            let t = db.begin();
            // 刻意不用 0x00/0x01 打头的值：那会踩到"值信封标签"的坑，
            // 与本条要证的跨线程无关（那个坑单独记在 README 已知局限 + 待决策）
            assert!(t.set(format!("k{i}").as_bytes(), vec![i.max(1) + 0x10; 8]));
            t.commit();
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let t = db.begin();
    for i in 0..4u8 {
        assert_eq!(
            t.get(format!("k{i}").as_bytes()),
            Some(vec![i.max(1) + 0x10; 8])
        );
    }
    t.commit();
    // 引擎层的维护动作也能从别的线程调
    let db2 = db.clone();
    std::thread::spawn(move || {
        db2.flush().unwrap();
        let h = db2.health();
        assert_eq!(h.live_keys, 4);
    })
    .join()
    .unwrap();
}
