// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// 下游 `bin/migrate_storage.rs` 与 `tests/storage_migrate.rs` 用的就是这一条：换引擎搬一次，
/// 值必须逐字节回来，`MigrateStats` 的字段必须齐（少一个字段下游就编译不过）。
#[test]
fn tools_migrate_carries_values_byte_for_byte() {
    let r = root("carry");
    // 0x00 / 0x01 打头的值是"信封不对称"那条的现形位置：多包一层、少包一层都在这里露
    let rows: &[(&[u8], &[u8])] = &[
        (b"strat:solusdt", br#"{"in_position":true}"#),
        (b"order:1", b"NEW"),
        (b"acct:spot:USDT", b"100.5"),
        (b"zero:lead", &[0x00, 0x01, 0x02]),
        (b"ttl:lead", &[0x01, 0x00, 0x00, 0x7f]),
    ];
    seed(EngineKind::Bitcask, &r, rows);
    let st = migrate(EngineKind::Bitcask, EngineKind::LsmTree, &r, false, false).unwrap();
    assert_eq!(st.records, rows.len());
    assert_eq!(st.live, rows.len());
    assert_eq!(st.deleted, 0);
    assert_eq!(
        st.verify_live, st.live,
        "目标里可见条数必须 == 写进去的存活条数"
    );
    assert_eq!(st.from, EngineKind::Bitcask);
    assert_eq!(st.to, EngineKind::LsmTree);
    assert!(st.export_secs >= 0.0 && st.import_secs >= 0.0);
    assert_eq!(st.source_path, engine_data_path(&r, EngineKind::Bitcask));
    assert_eq!(st.target_path, display_path(EngineKind::LsmTree, &r));
    for (k, v) in rows {
        assert_eq!(
            read_back(EngineKind::LsmTree, &r, k).as_deref(),
            Some(*v),
            "{k:?} 搬完读不回来"
        );
    }
    // 再搬一家：bplus 的库文件名按下游约定（另有专门守卫钉那一格）
    let st2 = migrate(EngineKind::LsmTree, EngineKind::BPlusTree, &r, false, false).unwrap();
    assert_eq!(st2.verify_live, rows.len());
    for (k, v) in rows {
        assert_eq!(
            read_back(EngineKind::BPlusTree, &r, k).as_deref(),
            Some(*v),
            "{k:?} 搬进 bplus 后读不回来"
        );
    }
    let _ = std::fs::remove_dir_all(&r);
}

/// ⚠️ 这条钉的是**会不会双信封**：本 crate 的 `bulk` 自己打值信封，而下游那份是先
/// `pack_plain` 再灌。照抄那一步 ⇒ 读出来带一层假信封（`get` 少剥一层）。
#[test]
fn tools_import_leaves_no_double_envelope() {
    let r = root("envelope");
    let out = r.join("snap.jsonl");
    let user: &[u8] = &[0x00, b'v', 0x01, 0x02];
    std::fs::write(
        &out,
        format!(
            "{{\"key\":\"{}\",\"value\":\"{}\"}}\n",
            b64_encode(b"k1"),
            b64_encode(user)
        ),
    )
    .unwrap();
    import_from_file(EngineKind::Bitcask, &r, &out, SnapshotFormat::Jsonl, true).unwrap();
    let got = read_back(EngineKind::Bitcask, &r, b"k1").expect("导入的键读不回来");
    assert_eq!(
        got,
        user.to_vec(),
        "读回来的必须正好是文件里那份用户字节（多一层/少一层信封都不行）"
    );
    let _ = std::fs::remove_dir_all(&r);
}

/// 下游主进程、运维脚本、它自己的测试都指 `bplus_tree/data.db`；本仓库 `StorageConfig`
/// 早先指 `bplus.db`。选错等于凭空开一个空库 ⇒ "迁移成功、余额没了"。
#[test]
fn tools_migrate_uses_the_downstream_bplus_file_name() {
    let r = root("dbname");
    seed(EngineKind::Bitcask, &r, &[(b"a", b"1")]);
    migrate(EngineKind::Bitcask, EngineKind::BPlusTree, &r, false, false).unwrap();
    let dir = engine_data_path(&r, EngineKind::BPlusTree);
    assert!(
        dir.join("data.db").exists(),
        "新建的 bplus 库必须叫 data.db"
    );
    assert!(
        !dir.join("bplus.db").exists(),
        "同一目录里不能留两个库文件（下一次开库选哪个变成运气）"
    );
    // 反向：源是 bplus 时也要从同一个名字找到，而不是报"源库文件不存在"
    let back = migrate(EngineKind::BPlusTree, EngineKind::LsmTree, &r, false, false);
    assert!(back.is_ok(), "从 data.db 当源迁移失败: {:?}", back.err());
    let _ = std::fs::remove_dir_all(&r);
}

/// 本仓库早先那个名字（`bplus.db`）也不能变孤儿：已存在就必须被认出来。
#[test]
fn tools_still_finds_the_old_bplus_file_name() {
    let r = root("oldname");
    std::fs::create_dir_all(engine_data_path(&r, EngineKind::BPlusTree)).unwrap();
    let legacy = r.join("bplus_tree").join("bplus.db");
    let db = open_engine_with(EngineKind::BPlusTree, Some(&legacy), &migrate_config()).unwrap();
    {
        let tx = db.begin();
        assert!(tx.set(b"legacy", b"v".to_vec()));
        tx.commit();
    }
    db.flush().unwrap();
    db.checkpoint().unwrap();
    drop(db);
    assert_eq!(
        bplus_db_path(&r),
        legacy,
        "只有 bplus.db 存在时必须认它，不能新建 data.db"
    );
    assert_eq!(
        read_back(EngineKind::BPlusTree, &r, b"legacy").as_deref(),
        Some(&b"v"[..]),
        "从旧名字的库里读不回东西 = 迁移会把它当空库"
    );
    let _ = std::fs::remove_dir_all(&r);
}

/// `wipe_target` 要真清干净：老键没了、sidecar（含 blob 重写留下的 `*.blob.new`）也没了。
#[test]
fn tools_wipe_target_removes_old_keys_and_sidecars() {
    let r = root("wipe");
    seed(EngineKind::Bitcask, &r, &[(b"new", b"N")]);
    seed(EngineKind::BPlusTree, &r, &[(b"stale", b"S")]);
    let dir = engine_data_path(&r, EngineKind::BPlusTree);
    let junk = ["data.db.wal", "data.db.dblwr", "data.db.blob.new"];
    for j in junk {
        std::fs::write(dir.join(j), b"junk").unwrap();
    }
    migrate(EngineKind::Bitcask, EngineKind::BPlusTree, &r, false, true).unwrap();
    let db = open_engine_with(
        EngineKind::BPlusTree,
        Some(&dir.join("data.db")),
        &migrate_config(),
    )
    .unwrap();
    {
        let tx = db.begin();
        assert_eq!(tx.get(b"new").as_deref(), Some(&b"N"[..]));
        assert_eq!(
            tx.get(b"stale"),
            None,
            "wipe 之后老键还在 = 目标没清，搬完是个混合库"
        );
    }
    drop(db);
    for j in junk {
        let p = dir.join(j);
        // ⚠️ 不能断"文件不存在"：引擎开库后会自己重建 `.wal`/`.dblwr`。
        // 要断的是**那份垃圾内容不在了**（要么整个没了，要么已被新库改写）。
        let still_junk = std::fs::read(&p).map(|b| b == b"junk").unwrap_or(false);
        assert!(!still_junk, "sidecar {j} 里还是 wipe 之前那份垃圾内容");
    }
    let _ = std::fs::remove_dir_all(&r);
}

/// 箭头三种写法、别名、两条 memory 拒绝、同源拒绝、源目录不存在拒绝。
#[test]
fn tools_arrow_parsing_and_rejections() {
    assert_eq!(
        parse_arrow("bitcask->bplus_tree").unwrap(),
        (EngineKind::Bitcask, EngineKind::BPlusTree)
    );
    assert_eq!(
        parse_arrow("lsm\u{2192}bitcask").unwrap(),
        (EngineKind::LsmTree, EngineKind::Bitcask)
    );
    // 下游 `StorageEngine::parse` 认 `b+tree` 这种写法 ⇒ 这里必须同样认（换库不能悄悄换引擎）
    assert_eq!(
        parse_arrow("b+tree=>lsm").unwrap(),
        (EngineKind::BPlusTree, EngineKind::LsmTree)
    );
    assert!(parse_arrow("bitcask->bitcask").is_err());
    assert!(parse_arrow("nope->bitcask").is_err());

    let r = root("reject");
    seed(EngineKind::Bitcask, &r, &[(b"a", b"1")]);
    let e = migrate(EngineKind::Memory, EngineKind::Bitcask, &r, false, false).unwrap_err();
    assert!(e.contains("不能作为 migrate 源"), "{e}");
    let e = migrate(EngineKind::Bitcask, EngineKind::Memory, &r, false, false).unwrap_err();
    assert!(e.contains("不能作为 migrate 目标"), "{e}");
    let e = migrate(EngineKind::Bitcask, EngineKind::Bitcask, &r, false, false).unwrap_err();
    assert!(e.contains("源与目标相同"), "{e}");
    // 源目录不存在必须报错，不能当空库搬走（那等于把生产库"搬"没了）
    let e = migrate(EngineKind::LsmTree, EngineKind::Bitcask, &r, false, false).unwrap_err();
    assert!(e.contains("源目录不存在"), "{e}");
    let _ = std::fs::remove_dir_all(&r);
}

/// `read_bin` / `read_jsonl` 从"边读边攒"改成"收集壳" ⇒ 壳与流式版必须给同一个答案，
/// 且提前收手时**只**交前 N 条（不钉这条，重构就可能悄悄改了口径）。
#[test]
fn tools_streaming_readers_agree_with_the_collecting_shells() {
    let r = root("readers");
    let recs: Vec<KvRecord> = (0..5u8)
        .map(|i| {
            KvRecord::new(
                format!("k{i}").into_bytes(),
                if i % 2 == 0 { Some(vec![i]) } else { None },
            )
        })
        .collect();
    let p_bin = r.join("a.bin");
    let p_jsonl = r.join("a.jsonl");
    write_bin(&p_bin, &recs).unwrap();
    write_jsonl(&p_jsonl, &recs).unwrap();
    for p in [&p_bin, &p_jsonl] {
        let read: fn(&Path) -> std::io::Result<(Vec<KvRecord>, IoStats)> =
            if p.extension().unwrap() == "bin" {
                read_bin
            } else {
                read_jsonl
            };
        let stream: fn(&Path, &mut dyn FnMut(KvRecord) -> bool) -> std::io::Result<IoStats> =
            if p.extension().unwrap() == "bin" {
                read_bin_with
            } else {
                read_jsonl_with
            };
        let (all, st_all) = read(p).unwrap();
        assert_eq!(all, recs, "{:?}: 收集壳读回来的不等于写出去的", p);
        assert_eq!(st_all.records, 5);
        let mut got = Vec::new();
        let st_two = stream(p, &mut |rec| {
            got.push(rec);
            got.len() < 2
        })
        .unwrap();
        assert_eq!(got.len(), 2, "{:?}: 提前收手还多读了", p);
        assert_eq!(got, recs[..2]);
        assert_eq!(st_two.records, 2, "{:?}: 收口那一条不该计进账", p);
    }
    let _ = std::fs::remove_dir_all(&r);
}

/// 带墓碑的全库往返：`include_deleted` 两边都要认，且不许把删掉的键搬成活的。
#[test]
fn tools_round_trip_keeps_tombstones_dead() {
    let r = root("tomb");
    seed(EngineKind::Bitcask, &r, &[(b"keep", b"K"), (b"drop", b"D")]);
    let src = open_engine_with(
        EngineKind::Bitcask,
        Some(&engine_data_path(&r, EngineKind::Bitcask)),
        &migrate_config(),
    )
    .unwrap();
    {
        let tx = src.begin();
        assert!(tx.delete(b"drop"));
        tx.commit();
    }
    src.flush().unwrap();
    src.checkpoint().unwrap();
    drop(src);
    let snap = r.join("s.bin");
    let st = export_to_file(EngineKind::Bitcask, &r, &snap, SnapshotFormat::Bin, true).unwrap();
    assert!(
        st.deleted >= 1,
        "带墓碑导出里没有墓碑 ⇒ include_deleted 没生效"
    );
    import_from_file(EngineKind::LsmTree, &r, &snap, SnapshotFormat::Bin, true).unwrap();
    assert_eq!(
        read_back(EngineKind::LsmTree, &r, b"keep").as_deref(),
        Some(&b"K"[..])
    );
    assert_eq!(
        read_back(EngineKind::LsmTree, &r, b"drop"),
        None,
        "墓碑被搬成活键了"
    );
    let _ = std::fs::remove_dir_all(&r);
}

/// 交集+差集形状（方案点名的判据）：目标已有的**一个字都不动**，缺的补齐；再同步一次 0 插入。
/// 源里的墓碑不进候选 ⇒ 删除不传播。
#[test]
fn sync_fills_missing_and_is_idempotent() {
    let rt = root("sync_fill_dst");
    let rs = root("sync_fill_src");
    // 目标：k1 = 旧值（已有）、k2 = 墓碑（"见过"）
    seed_ttl(EngineKind::Bitcask, &rt, &[(b"k1", b"old", None)]);
    let dst = open_for_sync(EngineKind::Bitcask, &rt);
    {
        let tx = dst.begin();
        assert!(tx.set_raw(b"k2", b"will-be-deleted".to_vec()));
        tx.commit();
    }
    {
        let tx = dst.begin();
        assert!(tx.delete(b"k2"));
        tx.commit();
    }
    drop(dst);
    // 源：k1 = 新值、k2 = 活值、k3/k4 目标没有、k9 是墓碑
    seed_ttl(
        EngineKind::LsmTree,
        &rs,
        &[
            (b"k1", b"new", None),
            (b"k2", b"alive", None),
            (b"k3", b"three", None),
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
    let dst = open_for_sync(EngineKind::Bitcask, &rt);
    let st = sync(&*src, &*dst).unwrap();
    // 源交出 5 行活值（k1..k4 + k2 墓碑不进 ⇒ 实际是 k1,k2,k3,k4 四行；k9 是墓碑 ⇒ 不进候选）
    assert_eq!(
        st.scanned, 4,
        "候选集合不对（含墓碑的源应当只交活行）: {st:?}"
    );
    assert_eq!(
        (st.inserted, st.skipped_existing),
        (2, 2),
        "该补 k3/k4、该挡 k1/k2: {st:?}"
    );
    let tx = dst.begin();
    assert_eq!(
        tx.get(b"k1").as_deref(),
        Some(&b"old"[..]),
        "已有的键被覆盖了"
    );
    assert_eq!(tx.get(b"k2"), None, "墓碑被复活了");
    assert_eq!(tx.get(b"k3").as_deref(), Some(&b"three"[..]));
    assert_eq!(tx.get(b"k4").as_deref(), Some(&b"four"[..]));
    assert_eq!(
        tx.get(b"k9"),
        None,
        "删除不该传播，源里的墓碑不该在目标留下任何痕迹"
    );
    tx.commit();
    drop(tx);
    // 幂等：同一份源再同步一次 ⇒ 0 插入（第二次的判定全是"见过"）
    let again = sync(&*src, &*dst).unwrap();
    assert_eq!(
        (again.scanned, again.inserted, again.skipped_existing),
        (4, 0, 4),
        "第二次同步不幂等: {again:?}"
    );
    drop(src);
    drop(dst);
    let _ = std::fs::remove_dir_all(&rs);
    let _ = std::fs::remove_dir_all(&rt);
}
