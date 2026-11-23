// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn full_maintenance_waits_for_interval_and_active_reader() {
    let scratch = Scratch::new("physical");
    let db = open_engine("bitcask", Some(&scratch.0.join("store"))).unwrap();
    db.set_vacuum_auto_slice(0);
    for _ in 0..24 {
        let tx = db.begin();
        assert!(tx.set_raw(b"hot", vec![7; 512]));
        tx.commit();
    }
    db.flush().unwrap(); // commit 只持久化 WAL；先排空数据缓冲，才能比较真实日志字节。
    let log_bytes = || -> u64 {
        std::fs::read_dir(scratch.0.join("store"))
            .unwrap()
            .map(|entry| entry.unwrap())
            .filter(|entry| {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                name.starts_with("data") && name.ends_with(".log")
            })
            .map(|entry| entry.metadata().unwrap().len())
            .sum()
    };
    let before = log_bytes();
    let mut driver = Maintenance::new(MaintenanceConfig {
        interval: Duration::from_secs(1),
        slice_rows: 8,
        full_vacuum_interval: Some(Duration::from_secs(60)),
    })
    .unwrap();
    let base = driver.last_full;
    let reader = db.begin();
    driver
        .tick_at(db.as_ref(), base + Duration::from_secs(61))
        .unwrap();
    driver
        .tick_at(db.as_ref(), base + Duration::from_secs(71))
        .unwrap();
    assert_eq!(driver.status().full_runs, 0);
    assert_eq!(driver.status().blocked_for, Duration::from_secs(10));
    reader.commit();
    drop(reader);
    driver
        .tick_at(db.as_ref(), base + Duration::from_secs(72))
        .unwrap();
    assert_eq!(driver.status().full_runs, 1);
    assert_eq!(driver.status().blocked_for, Duration::ZERO);
    let after = log_bytes();
    assert!(
        after < before / 2,
        "全库收尾必须真回收日志，不只是计数: {before} -> {after}"
    );
    driver
        .tick_at(db.as_ref(), base + Duration::from_secs(73))
        .unwrap();
    assert_eq!(driver.status().full_runs, 1, "有界片到尾不许变成全库重写");
    let tx = db.begin();
    assert_eq!(tx.get(b"hot"), Some(vec![7; 512]));
    tx.commit();
}

#[test]
fn maintenance_config_rejects_busy_loop_and_full_slice() {
    for cfg in [
        MaintenanceConfig {
            interval: Duration::ZERO,
            ..MaintenanceConfig::default()
        },
        MaintenanceConfig {
            slice_rows: 0,
            ..MaintenanceConfig::default()
        },
        MaintenanceConfig {
            slice_rows: u64::MAX,
            ..MaintenanceConfig::default()
        },
        MaintenanceConfig {
            full_vacuum_interval: Some(Duration::ZERO),
            ..MaintenanceConfig::default()
        },
    ] {
        assert!(Maintenance::new(cfg).is_err());
    }
    assert!(
        Maintenance::new(MaintenanceConfig {
            full_vacuum_interval: None,
            ..MaintenanceConfig::default()
        })
        .is_ok()
    );
}

#[test]
fn worker_stop_wakes_long_interval_and_joins() {
    let db: Arc<dyn KvEngine> = Arc::from(open_engine("memory", None).unwrap());
    let worker = start_maintenance(
        db.clone(),
        MaintenanceConfig {
            interval: Duration::from_secs(3600),
            ..MaintenanceConfig::default()
        },
    )
    .unwrap();
    let status = worker.stop().unwrap();
    assert_eq!(status.full_runs, 0);
    assert_eq!(
        Arc::strong_count(&db),
        1,
        "stop 必须释放数据库，不能遗留后台线程"
    );
}

/// 方案第 5 项：长读事务挡住自动回收 → 放开之后**排空**，一条活键都不许丢（要规模才测得到）。
///
/// 为什么不并进上面那几条小库测试：bplus 的 `vacuum_bounded` 按 `VAC_CHUNK_ROWS = 8192` 行
/// 一块续跑，而续跑是**叶粒度**定位（降到含游标的叶子，从该片第一条重喂）。一把键的版本组
/// 要**正好骑在块边界上**才走进"同一编码键被第二次判定"那条路径 —— 小库里一块装得下全表，
/// 边界根本不出现。11-02 那条 `vacuum_chunk_resume_must_not_readjudicate_carried_group` 是把
/// 块压到 4 行、在小库上**确定性**复现的；这一条是它的另一侧：**自然块大小 + 真规模**。
///
/// 规模怎么定的（不是随手挑的大数）：200k 把键 × 2 版 = 40 万版本行 ⇒ 一次全库回收天然切出
/// ~49 个块边界，每个边界约一半概率落在某把两版键的中间 ⇒ 期望二十几次"组骑边"，不靠运气。
/// 判据一律走**计数**（逐键读回 + 排空次数），墙钟只打印不作判据。
#[test]
#[ignore = "约 30~90 s：自然块大小下的 bplus 长跑回收守卫（块边界要规模才出现）"]
fn bplus_long_reader_does_not_lose_live_rows_at_scale() {
    const KEYS: usize = 200_000;
    const BATCH: usize = 5_000;
    let scratch = Scratch::new("bplus_scale_vac");
    let db = open_engine("bplus_tree", Some(&scratch.0.join("db"))).unwrap();
    let key = |i: usize| format!("{i:08}#/hot/x.json").into_bytes();
    let v1 = |i: usize| format!("base{i}").into_bytes();
    let v2 = |i: usize| format!("upd{i}").into_bytes();

    // 第一遍：基线，一把键一版。分批事务（一把 20 万键的写集本身就是一份大内存账）。
    for base in (0..KEYS).step_by(BATCH) {
        let end = (base + BATCH).min(KEYS);
        let items: Vec<(Vec<u8>, Vec<u8>)> = (base..end).map(|i| (key(i), v1(i))).collect();
        let tx = db.begin();
        if let Err(bad) = tx.batch_set(&items) {
            panic!("灌基线失败 @ {base}: 冲突 {bad:?}");
        }
        tx.commit();
    }

    // 长读事务挂在这里：它不释放，回收一片都不许裁（`skipped_active_txn` 那一支）。
    let reader = db.begin();
    // 第二遍：每把键更新 ⇒ 上一版的行全变垃圾，待回收账开始攒。
    for base in (0..KEYS).step_by(BATCH) {
        let end = (base + BATCH).min(KEYS);
        let items: Vec<(Vec<u8>, Vec<u8>)> = (base..end).map(|i| (key(i), v2(i))).collect();
        let tx = db.begin();
        if let Err(bad) = tx.batch_set(&items) {
            panic!("灌更新失败 @ {base}: 冲突 {bad:?}");
        }
        tx.commit();
    }
    let blocked = db.reclamation_status().expect("bplus 必须有回收账可读");
    assert!(
        blocked.pending_rows > 0,
        "更新 20 万把键之后必须攒出待回收账"
    );

    reader.commit();
    drop(reader);
    // ⚠️ 方案正文点名的这一条：**排空**，不是一次 begin/commit。一片只裁 `slice` 行，账可能有
    // 几十片。"排空"的终点**不是 `pending_rows == 0`** —— `VacuumScheduler::take_tick`
    // （`common/kernel.rs`）攒够一片才取一片，不足一片的尾巴按设计留着（实测停在
    // 6784 = 400000 − 24×16384，`active_txns: 0`）。
    // M82 的口径：`begin()` 的扫描量与写入量 1:1，账花完就停，**游标可以停在半路**
    // （`sweep_in_progress` 如实报 true）—— 把它推到库尾是下面维护器的活，不是下一个事务的。
    // M81 第 5 项后半曾让 begin() 续推半路的轮次，代价是写流里每个事务扫一整片（守卫
    // `auto_vacuum_scanning_keeps_pace_with_writes_not_with_transactions`），已拿掉。
    let slice = db.vacuum_auto_slice();
    let mut ticks = 0usize;
    let mut prev_removed = db.reclamation_status().unwrap().versions_removed;
    loop {
        db.begin().commit();
        let st = db.reclamation_status().unwrap();
        if st.versions_removed == prev_removed {
            assert!(
                st.pending_rows < slice,
                "回收停在半路：还剩 {} 行账 ≥ 一片 {slice} 行，却没有再裁（active_txns={}）",
                st.pending_rows,
                st.active_txns
            );
            break;
        }
        prev_removed = st.versions_removed;
        ticks += 1;
        assert!(
            ticks < 4_000,
            "回收不收敛：{ticks} 次 tick 还在前进，已裁 {prev_removed} 行"
        );
    }
    assert!(
        ticks > 1,
        "一次 tick 就排空 ⇒ 规模没到位、块边界根本没出现，这一条等于没测: ticks={ticks}"
    );
    let stopped = db.reclamation_status().unwrap();
    println!(
        "自动回收：{ticks} 次 tick 裁掉 {prev_removed} 行（slice={slice}），停在不足一片的尾巴上，剩余账 {}，轮次{}",
        stopped.pending_rows,
        if stopped.sweep_in_progress {
            "停在半路（交给维护器）"
        } else {
            "已走完"
        }
    );

    // 半路的轮次由维护器推到库尾（M82 的分工）：独立游标、从头扫一整遍，直到它的游标回到库头。
    // 这一遍不许被活跃事务挡（reader 已放开），也不许走全库重写（`full_vacuum_interval: None`）。
    let mut driver = Maintenance::new(MaintenanceConfig {
        interval: Duration::from_secs(1),
        slice_rows: slice,
        full_vacuum_interval: None,
    })
    .unwrap();
    let mut maint_ticks = 0usize;
    loop {
        driver.tick(db.as_ref()).unwrap();
        maint_ticks += 1;
        assert!(
            driver.status().blocked_for == Duration::ZERO,
            "维护器被不存在的活跃事务挡住"
        );
        if driver.cursor.is_none() {
            break;
        }
        assert!(maint_ticks < 4_000, "维护器一轮走不完：{maint_ticks} 片");
    }
    assert!(
        driver.status().slice_runs == maint_ticks as u64,
        "维护器每次 tick 都该是一片有界回收（slice_runs={} ticks={maint_ticks}）",
        driver.status().slice_runs
    );
    println!(
        "维护器：{maint_ticks} 片走完一整遍，又裁 {} 行、扫 {} 行",
        driver.status().versions_removed,
        driver.status().rows_scanned
    );

    // 判据一（自动路径 + 维护器收尾）：逐键读回最新值 —— 丢任何一把（或读回旧值）就是静默丢活键。
    let tx = db.begin();
    let mut lost: Vec<usize> = Vec::new();
    for i in 0..KEYS {
        if tx.get(&key(i)).as_deref() != Some(v2(i).as_slice()) && lost.len() < 10 {
            lost.push(i);
        }
    }
    tx.commit();
    assert!(
        lost.is_empty(),
        "自动回收后丢失或读回旧值的键（前 10 把，共 {KEYS} 把）: {lost:?}"
    );

    // 判据二（手动门）：全库 `vacuum()` 必须真把链裁到 1，且仍然一条活键不丢 ——
    // 两条路都要过，因为骑块边那条路径只在"续跑换了下界"时出现，手动全库轮走的是同一套分块。
    db.vacuum().unwrap();
    assert_eq!(db.health().max_chain, 1, "全库 vacuum 后每把键应只剩一版");
    let tx = db.begin();
    let mut lost2: Vec<usize> = Vec::new();
    for i in 0..KEYS {
        if tx.get(&key(i)).as_deref() != Some(v2(i).as_slice()) && lost2.len() < 10 {
            lost2.push(i);
        }
    }
    tx.commit();
    assert!(
        lost2.is_empty(),
        "全库 vacuum 后丢失或读回旧值的键（前 10 把）: {lost2:?}"
    );
}
