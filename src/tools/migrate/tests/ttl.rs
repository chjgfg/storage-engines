// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// 文件这一侧的 TTL 往返（0.5.0 的那件事）：导出 → 导入 → 到期时刻**逐键不变**。
/// 两种载体各测一次：`BPEXP003`（bin）与 JSONL 的可选 `expire` 字段。
///
/// 判据为什么用绝对时刻而不是剩余时间：剩余时间是读出来的瞬时量，两次读之间就变；
/// 绝对时刻对齐才是"搬一次不改寿命"的说法。永久行也一并钉（`None` 不许变成 `Some`）。
#[test]
fn file_carries_ttl_through_export_and_import() {
    for fmt in [SnapshotFormat::Bin, SnapshotFormat::Jsonl] {
        let r = root("file_ttl");
        let src_dir = r.join("lsm_tree");
        let dst_dir = r.join("bitcask");
        std::fs::create_dir_all(&src_dir).unwrap();
        std::fs::create_dir_all(&dst_dir).unwrap();
        let now = now_unix_secs();
        let rows = [
            (b"a".to_vec(), Some(now + 3_600)),
            (b"b".to_vec(), None),
            (b"c".to_vec(), Some(now + 86_400)),
        ];
        {
            let db =
                open_engine_with(EngineKind::LsmTree, Some(&src_dir), &migrate_config()).unwrap();
            let tx = db.begin();
            for (k, e) in &rows {
                match e {
                    // 直接构造待测的准确截止时间；如果先计算时长，
                    // 再在开库后叠加后来的时钟，截止时间可能偏移一秒。
                    Some(v) => assert!(tx.set(k, pack_ttl(b"v", *v))),
                    None => assert!(tx.set_raw(k, b"v".to_vec())),
                }
            }
            tx.commit();
            db.flush().unwrap();
            db.checkpoint().unwrap();
            let check = db.begin();
            for (k, expiry) in &rows {
                assert_eq!(
                    check.get_meta(k).unwrap().expire_unix_secs,
                    *expiry,
                    "source fixture must have the intended absolute deadline"
                );
            }
        }
        let snap = r.join(match fmt {
            SnapshotFormat::Bin => "s.bin",
            SnapshotFormat::Jsonl => "s.jsonl",
        });
        export_to_dir(EngineKind::LsmTree, &src_dir, &snap, fmt, false).unwrap();
        import_from_dir(EngineKind::Bitcask, &dst_dir, &snap, fmt, false).unwrap();
        let dst = open_engine_with(EngineKind::Bitcask, Some(&dst_dir), &migrate_config()).unwrap();
        let tx = dst.begin();
        for (k, want) in &rows {
            let m = tx
                .get_meta(k)
                .unwrap_or_else(|| panic!("{fmt:?}：键 {:?} 没搬过去", String::from_utf8_lossy(k)));
            let got = m.expire_unix_secs;
            assert_eq!(
                got,
                *want,
                "{fmt:?}：{:?} 的到期时刻变了（要 {want:?}，实得 {got:?}）",
                String::from_utf8_lossy(k)
            );
        }
        tx.commit();
        drop(tx);
        let _ = std::fs::remove_dir_all(&r);
    }
}

/// 判据③：TTL 跟着走（绝对到期时刻对齐），永久行仍永久；而**已存在的键 TTL 不刷新**。
/// 这条同时是"为什么 `sync` 不照 `migrate` 的出口走"的证据：那条出口交的是**用户字节**，
/// TTL 在 `live_record_of_logical` 那一步就被剥掉了。
#[test]
fn sync_carries_ttl_and_does_not_extend_existing() {
    let rt = root("sync_ttl_dst");
    let rs = root("sync_ttl_src");
    let expire = now_unix_secs() + 3_600;
    seed_ttl(
        EngineKind::LsmTree,
        &rs,
        &[
            (b"ttl", b"will-expire", Some(expire)),
            (b"perm", b"forever", None),
        ],
    );
    seed_ttl(
        EngineKind::Bitcask,
        &rt,
        &[(b"ttl", b"old-value", Some(expire + 99_999))],
    );
    let src = open_for_sync(EngineKind::LsmTree, &rs);
    let dst = open_for_sync(EngineKind::Bitcask, &rt);
    let st = sync(&*src, &*dst).unwrap();
    assert_eq!(
        (st.inserted, st.inserted_with_ttl, st.skipped_existing),
        (1, 0, 1),
        "该补的是永久那把 perm（TTL 那把目标已有 ⇒ 整个不动，连 TTL 都不许刷）: {st:?}"
    );
    let tx = dst.begin();
    let m = tx.get_meta(b"ttl").unwrap();
    assert_eq!(
        m.expire_unix_secs,
        Some(expire + 99_999),
        "已有的键 TTL 被刷新了（合同是整个不动）"
    );
    assert_eq!(m.value_len, Some(9), "值也不许动");
    let p = tx.get_meta(b"perm").unwrap();
    assert_eq!(p.expire_unix_secs, None, "永久行同步过去仍须是永久");
    assert_eq!(tx.get(b"perm").as_deref(), Some(&b"forever"[..]));
    tx.commit();
    drop(tx);
    // 反面证据（钉住"为什么不照 migrate 那条门走"）：同一条源用 migrate 搬一次，TTL 会没。
    drop(src);
    drop(dst);
    let rr = root("sync_ttl_probe");
    seed_ttl(EngineKind::LsmTree, &rr, &[(b"probe", b"x", Some(expire))]);
    let src2 = open_for_sync(EngineKind::LsmTree, &rr);
    let dst2 = open_for_sync(EngineKind::Bitcask, &rt);
    let st2 = sync(&*src2, &*dst2).unwrap();
    assert_eq!(
        (st2.inserted, st2.inserted_with_ttl),
        (1, 1),
        "probe 该带着 TTL 插进去: {st2:?}"
    );
    let tx = dst2.begin();
    let m2 = tx.get_meta(b"probe").unwrap();
    assert!(m2.expire_unix_secs.is_some(), "sync 这条路把 TTL 弄丢了");
    // 绝对到期时刻对齐（±5 s 只容忍两侧读时钟的差），钉住"重打信封不是随手给个默认时长"
    let src_tx = src2.begin();
    let want = src_tx.get_meta(b"probe").unwrap().expire_unix_secs;
    src_tx.commit();
    drop(src_tx);
    assert_eq!(
        m2.expire_unix_secs, want,
        "同步过去的到期时刻与源不一致: {want:?} vs {:?}",
        m2.expire_unix_secs
    );
    let got_ttl = tx.get_ttl(b"probe");
    assert!(
        matches!(got_ttl, Some(Some(d)) if d.as_secs() > 3_590 && d.as_secs() < 3_700),
        "新插的 TTL 行剩余时间不对: {got_ttl:?}"
    );
    tx.commit();
    drop(tx);
    drop(src2);
    drop(dst2);
    let _ = std::fs::remove_dir_all(&rs);
    let _ = std::fs::remove_dir_all(&rt);
    let _ = std::fs::remove_dir_all(&rr);
}

/// `migrate` 要保住 TTL（0.5.0 的那件事）。
///
/// 这条**以前是现状记录**：`migrate_currently_drops_ttl_characterization` 钉的是"当前会丢"，
/// 丢的链条只有一处 —— 源侧出口 `live_record_of_logical` 交用户字节（信封已剥）而写侧一律
/// `pack_plain` 重新打，`ExportRecord{key, value}` 里没有放 expire 的位置。
/// 出口加上 `expire_unix_secs` 之后本条按计划翻红 ⇒ 就地改写成"必须保住 TTL"（这正是当初
/// 把它写成断言而不是写成一段说明的目的）。永久行也在里面：`None` 不许被写成 `Some`。
#[test]
fn migrate_carries_ttl_across_engines() {
    let rr = root("mig_ttl_probe_root");
    let src_dir = rr.join("lsm_tree");
    std::fs::create_dir_all(&src_dir).unwrap();
    {
        let db = open_engine_with(EngineKind::LsmTree, Some(&src_dir), &migrate_config()).unwrap();
        let tx = db.begin();
        assert!(tx.set_with_ttl(b"has-ttl", b"v".to_vec(), Duration::from_secs(3_600)));
        tx.commit();
        db.flush().unwrap();
        db.checkpoint().unwrap();
    }
    migrate(EngineKind::LsmTree, EngineKind::Bitcask, &rr, false, false).unwrap();
    let dst = open_engine_with(
        EngineKind::Bitcask,
        Some(&engine_data_path(&rr, EngineKind::Bitcask)),
        &migrate_config(),
    )
    .unwrap();
    let tx = dst.begin();
    let m = tx.get_meta(b"has-ttl").expect("这把键要搬过去");
    assert!(
        m.expire_unix_secs.is_some(),
        "migrate 把 TTL 弄丢了：到期时刻在出口被剥掉 ⇒ 键搬完变永久"
    );
    tx.commit();
    drop(tx);
    let _ = std::fs::remove_dir_all(&rr);
}
