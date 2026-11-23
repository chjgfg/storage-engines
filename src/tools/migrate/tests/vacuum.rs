// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// 判据①的**时效**（M82 记一条现状，不是缺陷报告）：墓碑是"见过"的证据，而回收会把**孤墓碑
/// 整键删掉**（四家 `vacuum_plan_group` 的规则）⇒ 回收走过那把键之后"见过"就没了，同一份旧源
/// 再 sync 会把已删键**复活**。
///
/// 这不是 `sync` 的 bug，是两条合同的交点（"墓碑不该永久占位" × "见过要有据可查"）。钉成会翻红的
/// 现状：哪天有人给墓碑加保留策略，这一条要按新语义改写，而不是悄悄变绿。
/// 三条断言各钉一件事：① 前置——回收**之前**墓碑算见过（判据①成立的地基）；
/// ② 机理——`vacuum()` 之后墓碑已被带走（这一条红了说明"孤墓碑会被回收"这个前提变了）；
/// ③ 现状——同一份源再 sync，`inserted == 1` 且已删键读回活值（**这就是时效**）。
#[test]
fn sync_after_vacuum_resurrects_the_deleted_key_characterization() {
    for kind in [
        EngineKind::Bitcask,
        EngineKind::BPlusTree,
        EngineKind::LsmTree,
        EngineKind::Memory,
    ] {
        let tag = kind.as_str();
        let target = root(&format!("sync_gc_target_{tag}"));
        let source = root(&format!("sync_gc_source_{tag}"));
        seed_ttl(kind, &target, &[(b"dead", b"was-here", None)]);
        let db = open_for_sync(kind, &target);
        {
            let tx = db.begin();
            assert!(tx.delete(b"dead"), "{kind}: 删不掉");
            tx.commit();
        }
        {
            let tx = db.begin();
            let meta = tx.get_meta(b"dead");
            tx.commit();
            assert!(
                meta.is_some_and(|m| m.deleted),
                "{kind}: 前置：墓碑此刻必须算'见过'（判据①的地基）"
            );
        }
        db.vacuum()
            .unwrap_or_else(|e| panic!("{kind}: vacuum 失败 {e}"));
        {
            let tx = db.begin();
            let meta = tx.get_meta(b"dead");
            tx.commit();
            assert!(
                meta.is_none(),
                "{kind}: 回收之后墓碑应当已被整键带走 —— 这一条红了说明本测试的机理前提变了，\
                     判据①的时效结论要跟着重写"
            );
        }
        seed_ttl(kind, &source, &[(b"dead", b"resurrect", None)]);
        let src = open_for_sync(kind, &source);
        let st = sync(&*src, &*db).unwrap();
        assert_eq!(
            st.inserted, 1,
            "{kind}: 现状：墓碑被回收之后，同一份源再 sync 会把它当'没见过'插入（账 {st:?}）"
        );
        {
            let tx = db.begin();
            assert_eq!(
                tx.get(b"dead").as_deref(),
                Some(&b"resurrect"[..]),
                "{kind}: 现状：已删键被复活（判据①的时效 —— 见 `sync` 的文档）"
            );
            tx.commit();
        }
        drop(src);
        drop(db);
        let _ = std::fs::remove_dir_all(&source);
        let _ = std::fs::remove_dir_all(&target);
    }
}
