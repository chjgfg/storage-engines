// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn all_engines_retry_blocked_auto_vacuum_without_new_writes() {
    for name in crate::engine::ENGINE_NAMES {
        let scratch = Scratch::new(name);
        let path = if *name == "bplus_tree" {
            scratch.0.join("db")
        } else {
            scratch.0.join("store")
        };
        let db = open_engine_with(
            *name,
            Some(&path),
            &EngineConfig {
                vacuum_auto_slice_rows: Some(8),
                ..EngineConfig::default()
            },
        )
        .unwrap();
        let tx = db.begin();
        assert!(tx.set_raw(b"hot", b"original".to_vec()));
        tx.commit();
        drop(tx);
        let reader = db.begin();
        for n in 0..7 {
            let writer = db.begin();
            assert!(writer.set_raw(b"hot", format!("v{n}").into_bytes()));
            writer.commit();
        }
        db.begin().commit(); // 在 reader 挡住回收时，明确触发一次 tick
        let blocked = db.reclamation_status().unwrap();
        assert_eq!(blocked.pending_rows, 8, "{name}: 跳过不许吃掉账");
        assert!(blocked.skipped_active > 0);
        assert_eq!(blocked.oldest_active_version, Some(reader.version()));
        assert_eq!(reader.get(b"hot"), Some(b"original".to_vec()));
        reader.commit();
        drop(reader);
        let reader = db.begin(); // 不再写任何新行，也必须重试
        assert_eq!(reader.get(b"hot"), Some(b"v6".to_vec()));
        reader.commit();
        drop(reader);
        assert_eq!(db.health().max_chain, 1, "{name}: 无新写时不能永久停摆");
        let done = db.reclamation_status().unwrap();
        assert_eq!(done.pending_rows, 0);
        assert_eq!(done.completed_slices, 1);
        assert_eq!(done.versions_removed, 7);
    }
}

/// 自动回收的扫描量必须跟**写入行数**走，不能跟**事务次数**走（M81 第 5 项后半的回归守卫）。
///
/// 形状：先关着自动回收灌 `PREFILL` 行（库 ≫ 一片），再开成 `SLICE` 行/片，然后写 `WRITES` 个
/// **单行**事务 —— 下游 trading-system 逐条 `persist` 就是这个形状，台架的 `灌`/`更新` 是它的
/// 1000 行/500 行版本。
///
/// 判据（计数，不看墙钟）：`completed_slices × SLICE ≤ 流前账 + WRITES + SLICE` ——
/// 写多少行就扫多少行（1:1：预灌时关着自动回收攒下的账也算"写过"，在流里照样 1:1 还掉），
/// 外加一片零头。没有"每个 begin 推一片"的额度：半路的轮次是维护器的活，不是事务的。
///
/// 为什么要这条：`339a559` 让 `take_tick` 在"一轮开着"时对**每次 `begin()`** 都给一片许可
/// （`charged = 0` 那一支），而写流里一轮永远开着（账一直攒、游标一绕完就立刻再开）⇒ 单行事务
/// 每条都扫一整片 = `SLICE` 倍放大。`docs/test2.txt` 五档 20/20 格 `灌`/`更新` 变慢、
/// memory `冷重开` 1.3 s → 4.4 s（重放也是 1000 行/页的写流）都是这笔税。
/// 本条在那份实现上四家各读出 **275 片 ≈ 17 600 行**（上限 4 672）⇒ 红；M82 拿掉那一支后
/// 读出 71 片 ≈ 4 544 行 = 4096 预灌账 + 512 写入，正好 1:1。
#[test]
fn auto_vacuum_scanning_keeps_pace_with_writes_not_with_transactions() {
    const SLICE: u64 = 64;
    const PREFILL: u64 = SLICE * 64;
    const WRITES: u64 = 512;
    let mut red: Vec<String> = Vec::new();
    for name in crate::engine::ENGINE_NAMES {
        let scratch = Scratch::new(name);
        let path = scratch.0.join("db");
        let db = open_engine_with(
            *name,
            Some(&path),
            &EngineConfig {
                vacuum_auto_slice_rows: Some(0),
                ..EngineConfig::default()
            },
        )
        .unwrap();
        let tx = db.begin();
        for i in 0..PREFILL {
            assert!(
                tx.set_raw(format!("p{i:06}").as_bytes(), vec![b'x'; 16]),
                "{name}: 预灌第 {i} 行被拒"
            );
        }
        tx.commit();
        drop(tx);
        db.set_vacuum_auto_slice(SLICE);
        let before = db.reclamation_status().unwrap();
        for i in 0..WRITES {
            let tx = db.begin();
            assert!(
                tx.set_raw(format!("w{i:06}").as_bytes(), vec![b'y'; 16]),
                "{name}: 第 {i} 条单行事务被拒"
            );
            tx.commit();
        }
        let after = db.reclamation_status().unwrap();
        let slices = after.completed_slices - before.completed_slices;
        let scanned = slices * SLICE;
        // 预灌是关着自动回收写的，那 PREFILL 行的账还挂着，流里会按 1:1 还掉 ⇒ 算进上限
        let allowed = before.pending_rows + WRITES + SLICE;
        println!(
            "[auto-vac pace] {name}: 流前账 {} 行 + 写 {WRITES} 行 / {WRITES} 事务 ⇒ 扫 {slices} 片 ≈ {scanned} 行（上限 {allowed}，跳过 {} 片）",
            before.pending_rows,
            after.skipped_active - before.skipped_active
        );
        if scanned > allowed {
            red.push(format!("{name}: 扫了 ~{scanned} 行（{slices} 片）却只写了 {WRITES} 行（含流前账上限 {allowed}）"));
        }
    }
    // 四家跑完再判，一次把所有红的引擎都点出来
    assert!(
        red.is_empty(),
        "自动回收的扫描量跟着事务次数而不是写入行数走：\n  {}",
        red.join("\n  ")
    );
}
