// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// 尾巴 A：文件版补齐 —— 同一份文件，`--skip-existing` 那条门与覆盖那条门**给出两个答案**，
/// 所以两条都要判：覆盖门照旧后到者胜，补齐门"见过的一个字不动"（含墓碑），并带上 TTL。
#[test]
fn import_skip_existing_door_differs_from_the_overwrite_door() {
    let rs = root("sk_src");
    let expire = now_unix_secs() + 3_600;
    // 源：k1 新值、k2 活值、k3 带 TTL、k4 永久、k9 墓碑（只有带墓碑导出才会进文件）
    seed_ttl(
        EngineKind::LsmTree,
        &rs,
        &[
            (b"k1", b"new", None),
            (b"k2", b"alive", None),
            (b"k3", b"ttl", Some(expire)),
            (b"k4", b"four", None),
        ],
    );
    let src = open_for_sync(EngineKind::LsmTree, &rs);
    {
        let tx = src.begin();
        assert!(tx.set_raw(b"k9", b"gone".to_vec()));
        tx.commit();
    }
    {
        let tx = src.begin();
        assert!(tx.delete(b"k9"));
        tx.commit();
    }
    // 导出的门会自己再 open 一次 ⇒ 手上的 src 必须先放掉（LSM 的目录锁是独占的）
    drop(src);
    let snap = rs.join("s.bin");
    // ⚠️ 文件那两侧要的是**引擎目录**（`display_path`），不是 root ——
    // 传错就会对着一个空目录导出，读出 0 行还不报错（本条测试首跑就是这么红的）。
    export_to_dir(
        EngineKind::LsmTree,
        &display_path(EngineKind::LsmTree, &rs),
        &snap,
        SnapshotFormat::Bin,
        true,
    )
    .unwrap();

    // 目标：k1 = 旧值（已有）、k2 = 墓碑（"见过"）
    let rt = root("sk_dst");
    seed_ttl(EngineKind::Bitcask, &rt, &[(b"k1", b"old", None)]);
    let d0 = open_for_sync(EngineKind::Bitcask, &rt);
    {
        let tx = d0.begin();
        assert!(tx.set_raw(b"k2", b"will-die".to_vec()));
        tx.commit();
    }
    {
        let tx = d0.begin();
        assert!(tx.delete(b"k2"));
        tx.commit();
    }
    drop(d0);

    // ① 补齐门
    let dst_dir = display_path(EngineKind::Bitcask, &rt);
    let st =
        import_skip_existing_from_dir(EngineKind::Bitcask, &dst_dir, &snap, SnapshotFormat::Bin)
            .unwrap();
    // 文件里 5 行活值（k1..k4 + k9 是墓碑 ⇒ 不进候选）
    assert_eq!(st.scanned, 4, "候选集合该只数活行（墓碑不进）: {st:?}");
    assert_eq!(
        (st.inserted, st.skipped_existing),
        (2, 2),
        "该补 k3/k4、该挡 k1/k2: {st:?}"
    );
    assert_eq!(st.inserted_with_ttl, 1, "k3 该带着 TTL 插进去: {st:?}");
    {
        let db = open_for_sync(EngineKind::Bitcask, &rt);
        let tx = db.begin();
        assert_eq!(
            tx.get(b"k1").as_deref(),
            Some(&b"old"[..]),
            "补齐门把已有的键覆盖了"
        );
        assert_eq!(tx.get(b"k2"), None, "补齐门复活了已删键");
        assert_eq!(tx.get(b"k3").as_deref(), Some(&b"ttl"[..]));
        let m = tx.get_meta(b"k3").unwrap();
        assert!(
            m.expire_unix_secs.is_some() && m.expire_unix_secs.unwrap() >= expire,
            "k3 的 TTL 没跟过来（{expire} vs {m:?}）"
        );
        assert_eq!(tx.get(b"k4").as_deref(), Some(&b"four"[..]));
        assert!(
            tx.get_meta(b"k9").is_none(),
            "文件里的墓碑不该在目标留下任何痕迹（删除不传播）"
        );
        tx.commit();
        drop(tx);
        drop(db);
        // 幂等：再补齐一次 ⇒ 0 插入（门自己会再 open 一次 ⇒ 手上的句柄得先放掉）
        let again = import_skip_existing_from_dir(
            EngineKind::Bitcask,
            &dst_dir,
            &snap,
            SnapshotFormat::Bin,
        )
        .unwrap();
        assert_eq!(
            (again.scanned, again.inserted, again.skipped_existing),
            (4, 0, 4),
            "第二次不幂等: {again:?}"
        );
    }

    // ② 覆盖门（对照）：同一份文件走老门，k1 必须被改成 new —— 两条门不一样才各自有意义
    let rc = root("sk_overwrite");
    seed_ttl(EngineKind::Bitcask, &rc, &[(b"k1", b"old", None)]);
    import_from_dir(
        EngineKind::Bitcask,
        &display_path(EngineKind::Bitcask, &rc),
        &snap,
        SnapshotFormat::Bin,
        false,
    )
    .unwrap();
    let db = open_for_sync(EngineKind::Bitcask, &rc);
    let tx = db.begin();
    assert_eq!(
        tx.get(b"k1").as_deref(),
        Some(&b"new"[..]),
        "覆盖门的语义不该被顺手改掉"
    );
    tx.commit();
    let _ = std::fs::remove_dir_all(&rs);
    let _ = std::fs::remove_dir_all(&rt);
    let _ = std::fs::remove_dir_all(&rc);
}
