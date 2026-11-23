// 该文件集中验证对应职责，保留原测试名称和字面值。

#[test]
fn all_engines_roundtrip_set_get_delete() {
    let engines = open_all();
    let mut dirs = Vec::new();
    for (name, db, dir) in engines {
        assert_eq!(db.name(), name);

        let tx = db.begin();
        assert!(tx.set(b"a", b"1".to_vec()), "{name}: set");
        assert_eq!(tx.get(b"a"), Some(b"1".to_vec()), "{name}: get");
        assert!(tx.exists(b"a"), "{name}: exists");
        assert!(tx.delete(b"a"), "{name}: delete");
        assert_eq!(tx.get(b"a"), None, "{name}: get after delete");
        tx.commit();

        dirs.push(dir);
    }
    cleanup(dirs);
}

#[test]
fn disk_engines_expose_their_path() {
    let dir = tmp_dir("path");
    let db = open_engine("bitcask", Some(&dir)).unwrap();
    assert_eq!(db.path(), Some(&*dir)); // 守卫只 Deref 到 `Path`，不装 `PathBuf`
    drop(db);
    let _ = std::fs::remove_dir_all(&dir);
}

/// LSM 档里 `enable_mem_wal` / `enable_bg_compact` 传 `true` 也**必须**是 `false`。
///
/// 这两条一旦被下游打开，崩溃恢复就有两个互不知情的提交日志（mem.wal 与事务 WAL），
/// 而 `set_options` 热开 bg compact 更阴：线程只在 open 时创建，热改成 `true` 只会让
/// 写路径以为"有人替我 compact"于是自己不再 compact ⇒ L0 无限涨。
///
/// 走具体类型而不是门面：门面的 `tuning()` 有意**不列**这两条（列了会被读成"能调"）。
#[test]
fn lsm_mvcc_pins_the_two_fields_that_would_break_crash_safety() {
    use crate::lsm_tree::LSMTreeOptions;

    let dir = tmp_dir("lsm_pin");
    let db = crate::lsm_tree::MVCC::try_open_with(
        &dir,
        Some(LSMTreeOptions {
            enable_mem_wal: true,
            enable_bg_compact: true,
            block_cache_blocks: 999,
            ..LSMTreeOptions::for_mvcc()
        }),
    )
    .expect("带补丁打开 lsm_tree");

    let got = db.options();
    assert!(
        !got.enable_mem_wal,
        "事务 WAL 是唯一的持久化点：mem.wal 不能被下游打开"
    );
    assert!(!got.enable_bg_compact, "MVCC 档的 compact 必须同步做");
    assert_eq!(
        got.block_cache_blocks, 999,
        "钉住两条不等于连坐：其余字段要真的生效"
    );

    // 热更新路径同样钉住
    db.set_options(LSMTreeOptions {
        enable_mem_wal: true,
        enable_bg_compact: true,
        ..got.clone()
    });
    let after = db.options();
    assert!(
        !after.enable_mem_wal && !after.enable_bg_compact,
        "set_options 也要钉"
    );
    assert_eq!(after.block_cache_blocks, 999, "热更新不能把补丁读回原样");

    drop(db);
    cleanup(vec![Some(dir)]);
}

/// 崩溃安全差异必须**写在断言里**，不能只写在文档里。
///
/// 选引擎的人最终看的是代码给的答案：文档会过期，这张表不会。哪天 bitcask 补了记录
/// 校验和、或 LSM 给 `blobs/` 加了 CRC，用例会红，逼作者同步更新画像与 CHANGELOG ——
/// 而不是让下游继续照着过期的"已知局限"做取舍。
#[test]
fn durability_profiles_are_honestly_different() {
    let engines = open_all();
    let mut dirs = Vec::new();
    let mut seen: Vec<(&'static str, DurabilityProfile)> = Vec::new();
    for (name, db, dir) in engines {
        dirs.push(dir);
        let p = db.durability_profile();
        assert_eq!(p.engine, name, "画像里的引擎名必须和门面名一致");
        match name {
            // 唯一没有数据记录校验和的一家：data.log 坏一个字节不报错，只读出错值
            "bitcask" => {
                assert!(!p.data_checksum, "bitcask 主日志仍然是裸记录");
                assert!(p.blob_checksum && p.merge_artifact_recovery);
                let text = p.to_string();
                assert!(text.contains("数据记录无校验和"), "缺口必须报出来：{text}");
                assert!(!text.contains("无事务 WAL"), "没缺的不许瞎报：{text}");
            }
            // 唯一做原地页写的一家 ⇒ 也只有它有 torn page 问题，dblwr 兜着
            "bplus_tree" => {
                assert!(p.data_checksum && p.in_place_page_writes && p.torn_page_guard);
            }
            // SST 块有 CRC，blobs/ 却是裸文件
            "lsm_tree" => {
                assert!(p.data_checksum);
                assert!(!p.blob_checksum, "blobs/*.blob 至今是 fs::write 的裸文件");
            }
            // 不持久化是设计，不是疏漏
            "memory" => {
                assert!(!p.transaction_wal && !p.fsync_on_commit);
                assert!(p.to_string().contains("无事务 WAL"));
            }
            other => panic!("未知引擎 {other}"),
        }
        seen.push((name, p));
    }
    // 画像必须两两不同 —— 差异存在却报成一样，才是真问题
    for (i, a) in seen.iter().enumerate() {
        for b in seen.iter().skip(i + 1) {
            assert_ne!(a.1, b.1, "{} 与 {} 的崩溃安全画像不该相同", a.0, b.0);
        }
    }
    cleanup(dirs);
}
