// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn memory_engine_has_no_path_and_no_merge() {
    let db = open_engine("memory", None).unwrap();
    assert!(db.path().is_none());
    assert!(matches!(
        db.merge(),
        Err(EngineError::Unsupported { op: "merge", .. })
    ));
}
