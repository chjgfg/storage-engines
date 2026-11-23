// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// 判据①的地基：**四家一致**把墓碑算成"见过"（`get_meta` 有值、`exists` 是 `false`）。
///
/// 这条不是顺手写的断言，是 skip-existing 选门门的风向标：`exists()` 把墓碑算不存在 ⇒
/// 拿它当判据，同步会把已删键**复活**。所以这里同时钉两面：`get_meta(...).is_some()` 必须真、
/// `exists()` 必须假 —— 少钉后面那半，"换个门也一样"就没人发现得了。
#[test]
fn sync_tombstone_counts_as_seen_in_every_engine() {
    for kind in [
        EngineKind::Bitcask,
        EngineKind::BPlusTree,
        EngineKind::LsmTree,
        EngineKind::Memory,
    ] {
        let r = root(&format!("sync_tomb_{}", kind.as_str()));
        seed_ttl(kind, &r, &[(b"dead", b"was-here", None)]);
        let db = open_for_sync(kind, &r);
        {
            let tx = db.begin();
            assert!(tx.delete(b"dead"));
            tx.commit();
        }
        {
            let tx = db.begin();
            let meta = tx.get_meta(b"dead");
            assert!(
                meta.is_some(),
                "{kind}：墓碑在 get_meta 里必须是'见过'（sync 的判据就用这一门）"
            );
            assert!(meta.unwrap().deleted, "{kind}：那一格要报 deleted = true");
            assert!(
                !tx.exists(b"dead"),
                "{kind}：exists 把墓碑算不存在 ⇒ 拿它当判据就会复活已删键"
            );
            tx.commit();
        }
        // ⚠️ 用**同一个句柄**当目标，不再重开：重开对三家磁盘引擎等价，对 memory 却多一层
        // 快照重启。memory 的启停快照**现在**会带上墓碑（尾巴 B 已做，守卫
        // `memory_snapshot_keeps_tombstones_alive`），但这条判据仍然只用同一个句柄 ——
        // 判"门选错了"不该顺带依赖"重启保真"，两件事混在一起红了分不清是谁。
        // 真跑一次同步：源里这把键是活值 ⇒ 目标必须原样不动（既不复活，也不刷新）
        let rs = root(&format!("sync_tomb_src_{}", kind.as_str()));
        seed_ttl(
            kind,
            &rs,
            &[(b"dead", b"resurrect", None), (b"fresh", b"new", None)],
        );
        let src = open_for_sync(kind, &rs);
        let st = sync(&*src, &*db).unwrap();
        assert_eq!(
            st.skipped_existing, 1,
            "{kind}：墓碑没算进'见过'（账 {st:?}）"
        );
        assert_eq!(st.inserted, 1, "{kind}：只该补进 fresh 一把（账 {st:?}）");
        {
            let tx = db.begin();
            assert_eq!(tx.get(b"dead"), None, "{kind}：同步复活了已删键");
            assert_eq!(tx.get(b"fresh").as_deref(), Some(&b"new"[..]));
            tx.commit();
        }
        drop(src);
        drop(db);
        let _ = std::fs::remove_dir_all(&rs);
        let _ = std::fs::remove_dir_all(&r);
    }
}
