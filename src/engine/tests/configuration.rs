// 该文件集中验证对应职责，保留原测试名称和字面值。

/// #17 的守卫：**写进 [`EngineConfig`] 的旋钮必须在引擎里生效，而且要读得回来**。
///
/// 每条断言都配一条"不设时是多少"的对照。没有对照，这个测试会在两种坏情况下照样绿：
/// ① `tuning()` 的键名和 `EngineConfig` 字段名漂了（`.get()` 一律返回 `None`，
/// 而我若只断言 `!= Some("64")` 就永远成立）；② 引擎根本没接这个旋钮、值是门面自己
/// 编的。对照必须来自**同一条代码路径的默认档**，所以这里既有 `open_engine_with`
/// 也有 `open_engine`。
#[test]
fn engine_config_knobs_reach_the_engine_and_are_readable_back() {
    // ---- bplus_tree：order / max_cache_pages（只有这一家吃）----
    let dir = tmp_dir("knob_bplus");
    let db = open_engine_with(
        "bplus_tree",
        Some(&dir),
        &EngineConfig {
            order: 16,
            max_cache_pages: 32,
            ..Default::default()
        },
    )
    .expect("按配置打开 B+ 树");
    let t = db.tuning();
    assert_eq!(t.get("order"), Some("16"), "{t}");
    assert_eq!(t.get("max_cache_pages"), Some("32"), "{t}");

    let ddir = tmp_dir("default_bplus");
    let dflt = open_engine("bplus_tree", Some(&ddir)).expect("默认档 B+ 树");
    let d = dflt.tuning();
    assert_eq!(
        d.get("order"),
        Some("64"),
        "默认档必须是另一个数，上面的断言才不是空转"
    );
    assert_eq!(d.get("max_cache_pages"), Some("1024"));

    // ---- bitcask：分片上限（open 时就定死了，之后 rotate 按它走）----
    let dir = tmp_dir("knob_bitcask");
    let db = open_engine_with(
        EngineKind::Bitcask,
        Some(&dir),
        &EngineConfig {
            bitcask_max_file_bytes: Some(4096),
            ..Default::default()
        },
    )
    .expect("按配置打开 bitcask");
    assert_eq!(db.tuning().get("max_file_bytes"), Some("4096"));
    // 引擎自己的读回（`tuning()` 就是从这里取的）—— 证明值进的是 Bitcask 结构体，不是门面
    assert_eq!(db.tuning().get("max_file_bytes"), Some("4096"));

    let ddir = tmp_dir("default_bitcask");
    let dflt = open_engine("bitcask", Some(&ddir)).expect("默认档 bitcask");
    assert_eq!(
        dflt.tuning().get("max_file_bytes"),
        Some("67108864"),
        "bitcask 默认分片上限应是 64 MiB"
    );

    // ---- lsm_tree：整份 LSMTreeOptions 补丁 ----
    let dir = tmp_dir("knob_lsm");
    let db = open_engine_with(
        EngineKind::LsmTree,
        Some(&dir),
        &EngineConfig {
            lsm: Some(crate::lsm_tree::LSMTreeOptions {
                block_cache_blocks: 7,
                mem_threshold_bytes: 12345,
                ..crate::lsm_tree::LSMTreeOptions::for_mvcc()
            }),
            ..Default::default()
        },
    )
    .expect("按配置打开 lsm_tree");
    let t = db.tuning();
    assert_eq!(t.get("block_cache_blocks"), Some("7"), "{t}");
    assert_eq!(t.get("mem_threshold_bytes"), Some("12345"), "{t}");
    // 没动的字段留在 MVCC 档上：证明"补丁"不是"整份替换成裸 LSM 默认值"
    assert_eq!(
        t.get("l0_compact_threshold"),
        Some("8"),
        "MVCC 档的 L0 阈值不该被顺手改回 4"
    );

    let ddir = tmp_dir("default_lsm");
    let dflt = open_engine("lsm_tree", Some(&ddir)).expect("默认档 lsm_tree");
    let d = dflt.tuning();
    assert_eq!(
        d.get("block_cache_blocks"),
        Some("256"),
        "for_mvcc 的块缓存是 256"
    );
    assert_eq!(d.get("mem_threshold_bytes"), Some("33554432"));

    // ---- 跨引擎两条：WAL 阈值 + 自动回收切片 ----
    for name in ["bitcask", "bplus_tree", "lsm_tree"] {
        let dir = tmp_dir("knob_wal");
        let db = open_engine_with(
            name,
            Some(&dir),
            &EngineConfig {
                wal_checkpoint_bytes: Some(4096),
                vacuum_auto_slice_rows: Some(0),
                ..Default::default()
            },
        )
        .expect("带 WAL 阈值打开磁盘引擎");
        assert_eq!(
            db.wal_checkpoint_limit().unwrap(),
            4096,
            "{name}: 阈值要真生效"
        );
        assert_eq!(db.tuning().get("wal_checkpoint_bytes"), Some("4096"));
        assert_eq!(
            db.vacuum_auto_slice(),
            0,
            "{name}: 切片设 0 = 关闭自动回收，要读得回 0"
        );
        assert_eq!(db.tuning().get("vacuum_auto_slice"), Some("0"));
    }

    // memory 没有 WAL：给它设阈值必须**当场失败**，不能静默通过（"以为设了、其实没 WAL"
    // 是这次事故的形状之一）。切片它有的是真的。
    let db = open_engine_with(
        EngineKind::Memory,
        None,
        &EngineConfig {
            vacuum_auto_slice_rows: Some(0),
            ..Default::default()
        },
    )
    .expect("内存引擎只需切片");
    assert_eq!(db.vacuum_auto_slice(), 0);
    assert!(
        db.tuning().get("wal_checkpoint_bytes").is_none(),
        "没有 WAL 就不该出现在清单里"
    );
    let e = db
        .set_wal_checkpoint_limit(4096)
        .err()
        .expect("memory 设 WAL 阈值要报错");
    assert!(
        matches!(e, EngineError::Unsupported { engine, op }
                if engine == "memory" && op == "wal checkpoint limit"),
        "错误要指到具体的引擎与操作：{e}"
    );
    let mem_default = open_engine("memory", None).expect("默认档 memory");
    assert_eq!(
        mem_default.vacuum_auto_slice(),
        16 * 1024,
        "四引擎默认切片同一个值"
    );
}
