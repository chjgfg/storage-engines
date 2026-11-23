// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// 缓存自动档的**顶**这颗旋钮：两家里调得动、另外两家当场说不行（M32 的门面侧守卫）。
///
/// 为什么门面层还要一条：`set_cache_auto_max` / `cache_auto_max` 走的是 `impl_engine!` 的
/// `@cache_max yes/no` 分支 —— **分支标错不会编译失败**（把 bitcask 也写成 `yes` 只会让它
/// 转发给一个没有这个方法的名字… 而写成 `no`/`yes` 的错配更隐蔽：调用拿到 `Ok(())`，
/// 其实没人听）。下游要的是"要么真抬得动，要么明确说抬不动"，所以两条路（热调 / 带配置打开）
/// 都要过一遍。
///
/// 覆盖率效应（抬顶之后落盘读归零）**不在这里量**：那是各家自己的账，守卫是
/// bplus `test_cache_auto_max_is_raisable_and_misses_go_to_zero` 与
/// lsm `test_block_cache_auto_max_is_raisable_and_coverage_is_readable`。
#[test]
fn cache_auto_max_knob_routes_to_two_engines_and_is_refused_by_the_other_two() {
    // ---- 能调的两家：默认顶读得回来，压/抬之后读回来的是新值，tuning 里连着分母 ----
    for (name, default_ceil) in [("bplus_tree", 2048usize), ("lsm_tree", 1024)] {
        let dir = tmp_dir(&format!("cache_max_{name}"));
        let db = open_engine(name, Some(&dir)).expect("默认档");
        assert_eq!(
            db.cache_auto_max().unwrap(),
            default_ceil,
            "{name}: 默认顶要读得回来"
        );

        db.set_cache_auto_max(default_ceil / 2)
            .expect("压顶不该报错");
        assert_eq!(
            db.cache_auto_max().unwrap(),
            default_ceil / 2,
            "{name}: 压顶没落到引擎里"
        );

        db.set_cache_auto_max(usize::MAX).expect("抬顶不该报错");
        assert_eq!(
            db.cache_auto_max().unwrap(),
            usize::MAX,
            "{name}: 抬顶没落到引擎里"
        );

        // 顶与分母必须同时在清单里：只给"生效容量"没人能判断这是撞顶还是库就这么小
        let t = db.tuning();
        let (ceil_key, denom_key) = if name == "bplus_tree" {
            ("page_cache_auto_max", "total_pages")
        } else {
            ("block_cache_auto_max", "sst_blocks")
        };
        assert_eq!(
            t.get(ceil_key),
            Some(usize::MAX.to_string().as_str()),
            "{t}"
        );
        assert!(t.get(denom_key).is_some(), "{t}: 缺分母 ⇒ 报不出覆盖率");
    }

    // ---- `EngineConfig.cache_auto_max` 那条路：打开时就带上顶 ----
    for (name, ceil) in [("bplus_tree", 4096usize), ("lsm_tree", 3000)] {
        let dir = tmp_dir(&format!("cache_max_cfg_{name}"));
        let db = open_engine_with(
            name,
            Some(&dir),
            &EngineConfig {
                cache_auto_max: Some(ceil),
                ..Default::default()
            },
        )
        .expect("带顶打开");
        assert_eq!(
            db.cache_auto_max().unwrap(),
            ceil,
            "{name}: 配置里的顶没进引擎"
        );
    }

    // ---- 没有"缓存槽数"这个概念的两家：热调与带配置打开都要**当场失败** ----
    for name in ["bitcask", "memory"] {
        let dir = tmp_dir(&format!("cache_max_bad_{name}"));
        let db = open_engine(name, Some(&dir)).expect("默认档");
        let e = db
            .set_cache_auto_max(4096)
            .err()
            .unwrap_or_else(|| panic!("{name}: 抬顶要报错，不能静默返回成功"));
        assert!(
            matches!(e, EngineError::Unsupported { engine, op }
                    if engine == name && op == "cache auto max"),
            "错误要指到具体的引擎与操作：{e}"
        );
        assert!(
            matches!(db.cache_auto_max(), Err(EngineError::Unsupported { .. })),
            "{name}: 读顶也要报同一个错"
        );
        drop(db); // 同一个目录还要再开一次：锁要放开

        // 带配置打开也不能静默忽略（"以为设了顶、其实跑的是默认档"就是这个洞的形状）
        let cfg_dir = tmp_dir(&format!("cache_max_cfg_bad_{name}"));
        let bad = open_engine_with(
            name,
            Some(&cfg_dir),
            &EngineConfig {
                cache_auto_max: Some(4096),
                ..Default::default()
            },
        )
        .err()
        .unwrap_or_else(|| panic!("{name}: 带 cache_auto_max 打开要失败，不能静默忽略"));
        assert!(
            matches!(bad, EngineError::Unsupported { .. }),
            "{name}: 要的是 Unsupported，实际 {bad}"
        );
    }
}

/// 缓存**内存预算**这颗旋钮：两家里调得动、另外两家当场说不行（M76 的门面侧守卫）。
///
/// 为什么门面层还要一条（与 [`cache_auto_max_knob_routes_to_two_engines_and_is_refused_by_the_other_two`]
/// 同一个理由）：`set_cache_budget_bytes` / `cache_budget_bytes` 走的是 `impl_engine!` 的
/// `@cache_max` 分支 + trait 默认实现 —— **分支标错不会编译失败**，而 `bitcask` 一旦走到默认实现
/// 之外拿到 `Ok(())`，下游就以为"我给这进程封了 32 MB"，实际一颗旋钮都没动。
/// 决定 RSS 上限的旋钮，静默失效比报错危险。
///
/// 换算与"封顶被不被执行"**不在这里量**（那是各家自己的账，守卫是 bplus
/// `test_cache_budget_bytes_prices_slots_and_is_enforced` 与 lsm
/// `test_block_cache_budget_bytes_replaces_the_clamp_and_respects_explicit`）；这里只核两条路
/// （热调 / 带配置打开）都真的进了引擎，且读回来的是给的那个字节数。
#[test]
fn cache_budget_knob_routes_to_two_engines_and_is_refused_by_the_other_two() {
    for name in ["bplus_tree", "lsm_tree"] {
        let dir = tmp_dir(&format!("cache_budget_{name}"));
        let db = open_engine(name, Some(&dir)).expect("默认档");
        // 没设过预算 ⇒ 读回 0，而不是"看起来像个数"
        assert_eq!(
            db.cache_budget_bytes().unwrap(),
            0,
            "{name}: 默认档不该像设过预算"
        );

        // 热调：返回的是**换算后的生效槽数**，读回来的是**给的那个字节数**
        let slots = db
            .set_cache_budget_bytes(32_000_000)
            .expect("设预算不该报错");
        assert!(slots > 0, "{name}: 32 MB 换算出 0 槽 ⇒ 价签或除法坏了");
        assert_eq!(
            db.cache_budget_bytes().unwrap(),
            32_000_000,
            "{name}: 预算没落到引擎里"
        );
        let t = db.tuning();
        assert_eq!(
            t.get("cache_budget_bytes"),
            Some("32000000"),
            "{name}: tuning 要看得见给的预算：{t}"
        );
        let cap_key = if name == "bplus_tree" {
            "max_cache_pages"
        } else {
            "block_cache_capacity"
        };
        assert_eq!(
            t.get(cap_key).unwrap().parse::<usize>().unwrap(),
            slots,
            "{name}: 返回的槽数与 tuning 里的生效容量不是同一个数 ⇒ 报的是别的门"
        );

        // 撤掉预算 ⇒ 读回 0（不许停在最后一次换算值上）
        db.set_cache_budget_bytes(0).expect("撤预算不该报错");
        assert_eq!(
            db.cache_budget_bytes().unwrap(),
            0,
            "{name}: 撤掉之后要读回 0"
        );
        drop(db); // 同一个目录还要再开一次：锁要放开

        // 带配置打开：那条路也必须真的进引擎（"以为配过了"是这个洞的形状）
        let cfg_dir = tmp_dir(&format!("cache_budget_cfg_{name}"));
        let db2 = open_engine_with(
            name,
            Some(&cfg_dir),
            &EngineConfig {
                cache_budget_bytes: Some(8_000_000),
                ..Default::default()
            },
        )
        .expect("带预算打开");
        assert_eq!(
            db2.cache_budget_bytes().unwrap(),
            8_000_000,
            "{name}: 配置里的预算没进引擎"
        );
    }

    // 没有"缓存槽数"这个概念的两家：热调与带配置打开都要**当场失败**
    for name in ["bitcask", "memory"] {
        let dir = tmp_dir(&format!("cache_budget_bad_{name}"));
        let db = open_engine(name, Some(&dir)).expect("默认档");
        let e = db
            .set_cache_budget_bytes(8_000_000)
            .err()
            .unwrap_or_else(|| panic!("{name}: 设预算要报错，不能静默返回成功"));
        assert!(
            matches!(e, EngineError::Unsupported { engine, op }
                    if engine == name && op == "cache budget bytes"),
            "错误要指到具体的引擎与操作：{e}"
        );
        assert!(
            matches!(
                db.cache_budget_bytes(),
                Err(EngineError::Unsupported { .. })
            ),
            "{name}: 读预算也要报同一个错"
        );
        assert!(
            db.tuning().get("cache_budget_bytes").is_none(),
            "{name}: 没有这一档就不该出现在清单里"
        );
        drop(db);

        let cfg_dir = tmp_dir(&format!("cache_budget_cfg_bad_{name}"));
        let bad = open_engine_with(
            name,
            Some(&cfg_dir),
            &EngineConfig {
                cache_budget_bytes: Some(8_000_000),
                ..Default::default()
            },
        )
        .err()
        .unwrap_or_else(|| panic!("{name}: 带 cache_budget_bytes 打开要失败，不能静默忽略"));
        assert!(
            matches!(bad, EngineError::Unsupported { .. }),
            "{name}: 要的是 Unsupported，实际 {bad}"
        );
    }
}
