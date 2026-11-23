// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn all_engines_keep_large_commit_debt() {
    for name in crate::engine::ENGINE_NAMES {
        let scratch = Scratch::new(name);
        let path = scratch.0.join("db");
        let db = open_engine_with(
            *name,
            Some(&path),
            &EngineConfig {
                vacuum_auto_slice_rows: Some(8),
                ..EngineConfig::default()
            },
        )
        .unwrap();
        let writer = db.begin();
        for n in 0..32 {
            assert!(writer.set_raw(format!("k{n:03}").as_bytes(), vec![42]));
        }
        writer.commit();
        drop(writer);
        for expected in [24, 16, 8, 0] {
            db.begin().commit();
            assert_eq!(
                db.reclamation_status().unwrap().pending_rows,
                expected,
                "{name}"
            );
        }
    }
}
