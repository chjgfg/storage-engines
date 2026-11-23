// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn canonical_name_accepts_aliases() {
    assert_eq!(canonical_name("LSM"), Some("lsm_tree"));
    assert_eq!(canonical_name("bplus-tree"), Some("bplus_tree"));
    assert_eq!(canonical_name(" mem "), Some("memory"));
    assert_eq!(canonical_name("btree"), Some("bplus_tree"));
    assert_eq!(canonical_name("nope"), None);
}

#[test]
fn unknown_engine_is_reported() {
    let err = expect_err(open_engine("redis", None));
    assert!(matches!(err, EngineError::UnknownEngine(n) if n == "redis"));
}

#[test]
fn disk_engines_require_path() {
    for name in ["bitcask", "bplus_tree", "lsm_tree"] {
        let err = expect_err(open_engine(name, None));
        assert!(
            matches!(err, EngineError::PathRequired(_)),
            "{name} should require a path"
        );
    }
}

#[test]
fn unknown_name_is_echoed_verbatim_whatever_the_shape() {
    let s = String::from("redis");
    let errs = [
        expect_err(open_engine("redis", None)),
        expect_err(open_engine(s.clone(), None)),
        expect_err(open_engine(&s, None)),
    ];
    for err in errs {
        assert!(
            matches!(err, EngineError::UnknownEngine(ref n) if n == "redis"),
            "错误里要原样回显传进来的名字，实际: {err}"
        );
    }
}

/// 枚举 + 漏路径 = 归「必须给路径」管，不能被名字解析那一步吃掉
#[test]
fn enum_without_path_is_path_required() {
    for kind in [
        EngineKind::Bitcask,
        EngineKind::BPlusTree,
        EngineKind::LsmTree,
    ] {
        let err = expect_err(open_engine(kind, None));
        assert!(matches!(err, EngineError::PathRequired(_)), "{kind}: {err}");
    }
}

/// 类型化选型：别名解析、"要不要路径"、"能不能上生产"。
#[test]
fn engine_kind_selection_is_typed_and_honest() {
    assert_eq!(EngineKind::parse("LSM"), Some(EngineKind::LsmTree));
    assert_eq!(
        EngineKind::parse(" b-plus-tree "),
        Some(EngineKind::BPlusTree)
    );
    assert_eq!(EngineKind::parse("mem"), Some(EngineKind::Memory));
    assert_eq!(EngineKind::parse("redis"), None);
    for k in [
        EngineKind::Bitcask,
        EngineKind::BPlusTree,
        EngineKind::LsmTree,
        EngineKind::Memory,
    ] {
        assert_eq!(
            EngineKind::parse(k.as_str()),
            Some(k),
            "as_str/parse 必须互逆"
        );
        assert_eq!(k.to_string(), k.as_str());
    }
    // memory 是唯一"没有持久化"的那个，选型时就得能问到
    assert!(!EngineKind::Memory.is_production_ok());
    assert!(!EngineKind::Memory.needs_disk());
    for k in [
        EngineKind::Bitcask,
        EngineKind::BPlusTree,
        EngineKind::LsmTree,
    ] {
        assert!(k.is_production_ok() && k.needs_disk());
    }

    // StorageConfig 自己建目录、按引擎决定"目录 or 库文件"
    let base = tmp_dir("storage_cfg");
    let cfg = StorageConfig {
        kind: EngineKind::BPlusTree,
        data_dir: base.to_path_buf(),
        engine: EngineConfig {
            order: 16,
            max_cache_pages: 32,
            ..Default::default()
        },
    };
    let db = cfg.open().expect("按配置打开 B+ 树");
    assert_eq!(db.name(), "bplus_tree");
    assert!(cfg.engine_dir().exists());
    let t = db.begin();
    assert!(t.set(b"cfg", b"ok".to_vec()));
    assert_eq!(
        t.incr_with_ttl(b"counter", 3, Duration::from_secs(60))
            .unwrap(),
        3
    );
    assert!(
        t.get_ttl(b"counter").is_some_and(|o| o.is_some()),
        "TTL 必须真的设上"
    );
    t.commit();
    let _ = std::fs::remove_dir_all(&base);
}
