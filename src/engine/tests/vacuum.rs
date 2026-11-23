// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn all_engines_vacuum_and_flush() {
    let engines = open_all();
    let mut dirs = Vec::new();
    for (name, db, dir) in engines {
        let t1 = db.begin();
        t1.set(b"k", b"v1".to_vec());
        t1.commit();
        let t2 = db.begin();
        t2.set(b"k", b"v2".to_vec());
        t2.commit();

        db.flush().unwrap_or_else(|e| panic!("{name}: flush: {e}"));
        db.checkpoint()
            .unwrap_or_else(|e| panic!("{name}: checkpoint: {e}"));
        let stats = db
            .vacuum()
            .unwrap_or_else(|e| panic!("{name}: vacuum: {e}"));
        assert!(stats.xmin > 0, "{name}: xmin");

        let tx = db.begin();
        assert_eq!(tx.get(b"k"), Some(b"v2".to_vec()), "{name}: after vacuum");
        tx.commit();
        dirs.push(dir);
    }
    cleanup(dirs);
}

/// 门面层的有界回收：一轮的代价只和 `budget_rows` 有关、游标能接力，
/// **扫完全库后的终态与 `vacuum()` 一致**（同一份保留规则，不是两套）。
#[test]
fn vacuum_bounded_is_bounded_and_ends_where_vacuum_does() {
    const KEYS: u64 = 4;
    const CHAIN: u64 = 40;
    const BUDGET: u64 = 25;
    const TOTAL: u64 = KEYS * CHAIN;

    let engines = open_all();
    let mut dirs = Vec::new();
    for (name, db, dir) in engines {
        dirs.push(dir);
        // 关掉自动回收：本例要的是"我手动一轮一轮推"这个现场
        db.set_vacuum_auto_slice(0);

        for i in 0..CHAIN {
            for k in 0..KEYS {
                let tx = db.begin();
                assert!(tx.set(format!("k{k}").as_bytes(), format!("v{i}").into_bytes()));
                tx.commit();
            }
        }
        let before = db.health();
        assert_eq!(before.total_rows, TOTAL, "{name}: 灌数后的物理行数");
        assert_eq!(before.max_chain, CHAIN, "{name}: 每把链 {CHAIN} 版");

        let mut cursor: Option<Vec<u8>> = None;
        let mut rounds = 0usize;
        let mut max_scanned = 0u64;
        loop {
            let round = db
                .vacuum_bounded(cursor.as_deref(), BUDGET)
                .unwrap_or_else(|e| panic!("{name}: vacuum_bounded 失败 {e}"));
            assert!(
                !round.stats.skipped_active_txn,
                "{name}: 不该有活跃事务挡路"
            );
            // 预算的余量 = 一条链：截断只发生在逻辑 key 边界（半条链不能收，
            // 判"留哪一版"要求整组版本都在手上）。
            max_scanned = max_scanned.max(round.stats.rows_scanned);
            assert!(
                round.stats.rows_scanned <= BUDGET + CHAIN,
                "{name}: 一轮扫了 {} 行，预算 {BUDGET} + 一条链 {CHAIN}",
                round.stats.rows_scanned
            );
            match round.next_cursor {
                None => break,
                Some(next) => {
                    assert!(
                        cursor
                            .as_ref()
                            .is_none_or(|c| next.as_slice() > c.as_slice()),
                        "{name}: 游标必须严格前进，否则每轮重读同一片"
                    );
                    cursor = Some(next);
                }
            }
            rounds += 1;
            assert!(rounds < 64, "{name}: 游标不前进？已 {rounds} 轮");
        }
        assert!(
            rounds > 1 && max_scanned < TOTAL,
            "{name}: 预算 {BUDGET} ≪ 全库 {TOTAL} 行，一轮扫完说明根本没限流（轮数 {rounds}）"
        );
        eprintln!(
            "[vacuum_bounded] {name}: 全库 {TOTAL} 行 / 预算 {BUDGET} ⇒ {rounds} 轮，\
                 单轮最多扫 {max_scanned} 行"
        );
        let after = db.health();
        assert_eq!(after.max_chain, 1, "{name}: 接力扫完应等价于 vacuum()");
        assert_eq!(after.total_rows, KEYS, "{name}: 每个逻辑 key 只剩一行");

        drop(db);
    }
    cleanup(dirs);

    // 一次性大预算 = 手动 `vacuum()`：同一个入口，只是不分片
    let engines = open_all();
    let mut dirs = Vec::new();
    for (name, db, dir) in engines {
        dirs.push(dir);
        db.set_vacuum_auto_slice(0);
        for i in 0..CHAIN {
            for k in 0..KEYS {
                let tx = db.begin();
                assert!(tx.set(format!("k{k}").as_bytes(), format!("v{i}").into_bytes()));
                tx.commit();
            }
        }
        let round = db
            .vacuum_bounded(None, u64::MAX)
            .unwrap_or_else(|e| panic!("{name}: 全库轮次失败 {e}"));
        assert_eq!(round.next_cursor, None, "{name}: 全库轮次应一次到底");
        assert_eq!(
            round.stats.rows_scanned, TOTAL,
            "{name}: 全库轮次要读满 {TOTAL} 行"
        );
        let after = db.health();
        assert_eq!(after.total_rows, KEYS, "{name}: 与 vacuum() 同一份保留规则");
        drop(db);
    }
    cleanup(dirs);
}

/// 自动回收必须**默认开着**，而且四引擎都开着 —— 只有一家开就等于把事故留给换引擎的那天。
///
/// 上层只通过门面打交道，所以这条走的是 `KvEngine::{vacuum_auto_slice,
/// set_vacuum_auto_slice, health}`：不叫 vacuum，链也不能无限涨；关掉的开关要真能关掉。
#[test]
fn all_engines_auto_reclaim_by_default() {
    const CHAIN: u64 = 200;
    const SLICE: u64 = 8;

    let engines = open_all();
    let mut dirs = Vec::new();
    for (name, db, dir) in engines {
        dirs.push(dir);
        assert_eq!(
            db.vacuum_auto_slice(),
            16 * 1024,
            "{name}: 自动回收默认必须开着"
        );
        db.set_vacuum_auto_slice(SLICE);

        for i in 0..CHAIN {
            let tx = db.begin();
            assert!(
                tx.set(b"hot", format!("v{i}").into_bytes()),
                "{name} 第 {i} 次写入被拒"
            );
            tx.commit();
        }
        let h = db.health();
        assert!(
            h.max_chain <= SLICE + 2,
            "{name}: 没人喊 vacuum，链却涨到 {} 行 —— 自动回收没在工作",
            h.max_chain
        );

        // 关掉的开关必须真关掉
        db.set_vacuum_auto_slice(0);
        for i in 0..30 {
            let tx = db.begin();
            assert!(tx.set(b"cold", format!("v{i}").into_bytes()));
            tx.commit();
        }
        let off = db.health();
        assert_eq!(
            off.live_keys,
            h.live_keys + 1,
            "{name}: 应多出 cold 这把 key"
        );
        assert_eq!(
            off.max_chain, 30,
            "{name}: 关闭自动回收后不该裁掉任何一版（链应等于写入次数）"
        );
        drop(db);
    }
    cleanup(dirs);
}

/// 四家的 `vacuum()` 都必须**逐组**判定，不许先把全库归组攒出来（M36）。
///
/// 判据两条（每个引擎各核一次，失败点名是谁）：
/// - `vacuum_group_calls() >= 键数` —— 一把键判一次。旧形状（先把全库归组进
///   `BTreeMap<Vec<u8>, Vec<…>>`、再问一次 `vacuum_plan`）在这里读出 **1** ⇒ 红
/// - `vacuum_group_max_rows() <= 每把键的版本数` —— 任何一次判定握住的行数不得超过一条链；
///   旧形状读出的是**全库物理行** ⇒ 红
///
/// 为什么不用 `mem_probe` 的 peak 当判据：那是**进程级**高水位，并行跑用例时别家分配的字节
/// 会算进来（内存那笔读数一直留在 `--test-threads=1` 的台架里，M27/M33 同口径 —— 600k 那档
/// 改前 **+89.4 MB** 的读数在 README）。这条守卫钉的是**结构**：不再攒全库，内存自然跟着降。
///
/// 变异验证：把任意一家的收组改回 `BTreeMap` + 一次 `vacuum_plan` ⇒ 那一家两条都红、别家绿。
#[test]
fn all_engines_vacuum_judges_per_group_not_whole_library() {
    const KEYS: usize = 800;
    const VERSIONS: usize = 2;

    let engines = open_all();
    let mut dirs = Vec::new();
    for (name, db, dir) in engines {
        dirs.push(dir);
        // 自动回收会先把旧版裁掉一部分，那就不剩账了 —— 这一条要手动 vacuum
        db.set_vacuum_auto_slice(0);
        for _ in 0..VERSIONS {
            let tx = db.begin();
            for i in 0..KEYS {
                let k = format!("k{i:05}");
                assert!(
                    tx.set(k.as_bytes(), vec![b'v'; 24]),
                    "{name}: 第 {i} 把键写失败"
                );
            }
            tx.commit();
        }
        crate::common::version::reset_vacuum_group_counters();
        let st = db
            .vacuum()
            .unwrap_or_else(|e| panic!("{name}: vacuum 失败 {e}"));
        let calls = crate::common::version::vacuum_group_calls();
        let max_rows = crate::common::version::vacuum_group_max_rows();
        eprintln!(
            "[vac groups] {name}: 判定 {calls} 次 / 最大一组 {max_rows} 行 / 裁掉 {} 行、扫 {} 行",
            st.versions_removed, st.rows_scanned
        );
        assert!(
            st.rows_scanned >= (KEYS * VERSIONS) as u64,
            "{name}: 只扫到 {} 行（应有 {} 物理行）⇒ 这个形状没被跑到",
            st.rows_scanned,
            KEYS * VERSIONS
        );
        assert!(st.versions_removed > 0, "{name}: 一轮什么都没裁 ⇒ 判据空转");
        assert!(
            calls as usize >= KEYS,
            "{name}: vacuum 把判定折叠成了 {calls} 次（键有 {KEYS} 把）⇒ 它还是先把全库归组攒出来"
        );
        assert!(
            max_rows as usize <= VERSIONS,
            "{name}: 有一次判定握住了 {max_rows} 行（每把键只有 {VERSIONS} 版）⇒ 归组攒了不止一条链"
        );
        // 裁完必须还读得回最新版
        {
            let tx = db.begin();
            assert_eq!(
                tx.get(b"k00000"),
                Some(vec![b'v'; 24]),
                "{name}: 裁完读不回最新版"
            );
            tx.commit();
        }
        drop(db);
    }
    cleanup(dirs);
}

#[test]
fn vacuum_lock_probe_observes_skip_and_real_storage_holds() {
    use crate::common::maintenance_probe::{Scope, Stage};
    for (name, db, _dir) in open_all() {
        db.set_vacuum_auto_slice(0);
        let tx = db.begin();
        tx.try_set_raw(b"key", b"value".to_vec()).unwrap();
        tx.try_commit().unwrap();
        drop(tx);
        let reader = db.begin();
        let scope = Scope::start();
        assert!(
            db.vacuum_bounded(None, 2048)
                .unwrap()
                .stats
                .skipped_active_txn
        );
        let skipped = scope.stats();
        drop(scope);
        assert_eq!(
            skipped.calls[Stage::VacuumRegistryWait as usize],
            1,
            "{name}"
        );
        assert_eq!(
            skipped.calls[Stage::VacuumRegistryHold as usize],
            1,
            "{name}"
        );
        assert_eq!(
            skipped.calls[Stage::VacuumStorageHold as usize],
            0,
            "{name}"
        );
        drop(reader);
        let scope = Scope::start();
        assert!(
            !db.vacuum_bounded(None, 2048)
                .unwrap()
                .stats
                .skipped_active_txn
        );
        let completed = scope.stats();
        drop(scope);
        assert_eq!(
            completed.calls[Stage::VacuumRegistryHold as usize],
            1,
            "{name}"
        );
        assert_eq!(
            completed.calls[Stage::VacuumStorageWait as usize],
            completed.calls[Stage::VacuumStorageHold as usize],
            "{name}"
        );
        assert_eq!(
            completed.calls[Stage::VacuumStorageHold as usize],
            if name == "memory" { 2 } else { 1 },
            "{name}"
        );
    }
}
