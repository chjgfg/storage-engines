// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// 门面入口的几种写法必须落到同一个引擎。
///
/// `EngineKind::as_str` 和 `canonical_name` 是两张表，字符串时代它们只在
/// `open_engine` 内部碰头；现在枚举能直接入参，这张用例就是那个汇合点。
#[test]
fn open_engine_accepts_enum_and_string_in_every_ownership_shape() {
    let mut dirs = Vec::new();
    let disk = [
        (EngineKind::Bitcask, "bitcask"),
        (EngineKind::BPlusTree, "bplus-tree"),
        (EngineKind::LsmTree, "lsm"),
    ];
    for (kind, alias) in disk {
        let dir = tmp_dir("name_kind");
        let want = kind.as_str();
        let owned = alias.to_string();
        let by_kind = open_engine(kind, Some(&dir))
            .expect("枚举入参")
            .name()
            .to_string();
        let by_str = open_engine(alias, Some(&dir))
            .expect("&str 入参")
            .name()
            .to_string();
        let by_string = open_engine(owned.clone(), Some(&dir))
            .expect("String 入参")
            .name()
            .to_string();
        // 泛型位置不会把 &String 自动解引用成 &str —— 这条就是钉住那个 impl
        let by_ref = open_engine(&owned, Some(&dir))
            .expect("&String 入参")
            .name()
            .to_string();
        for (got, how) in [
            (&by_kind, "enum"),
            (&by_str, "&str"),
            (&by_string, "String"),
            (&by_ref, "&String"),
        ] {
            assert_eq!(got, want, "{alias} 走 {how} 这条路");
        }
        dirs.push(dir);
    }
    // memory 不吃路径
    assert_eq!(
        open_engine(EngineKind::Memory, None).unwrap().name(),
        "memory"
    );
    assert_eq!(open_engine("mem", None).unwrap().name(), "memory");
    cleanup(dirs.into_iter().map(Some).collect());
}
