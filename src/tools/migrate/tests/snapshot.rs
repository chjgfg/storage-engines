// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// 反面：老格式文件（001，没有 expire 的位置）读回来一律永久 —— 读侧不许"猜"哪一行有 TTL。
#[test]
fn v1_snapshot_reads_back_as_permanent() {
    use crate::common::ExportRecord;
    let r = root("v1_permanent");
    let dir = r.join("bitcask");
    std::fs::create_dir_all(&dir).unwrap();
    let snap = r.join("old.bin");
    {
        let mut w = ExportWriter::bin(&snap, false).unwrap();
        assert!(w.push(ExportRecord::new(b"k".to_vec(), Some(b"v".to_vec()))));
        w.finish().unwrap();
    }
    import_from_dir(EngineKind::Bitcask, &dir, &snap, SnapshotFormat::Bin, false).unwrap();
    let db = open_engine_with(EngineKind::Bitcask, Some(&dir), &migrate_config()).unwrap();
    let tx = db.begin();
    assert_eq!(
        tx.get_meta(b"k").unwrap().expire_unix_secs,
        None,
        "001 文件里没这个字段，读侧不许凭空造一个到期时刻"
    );
    tx.commit();
    let _ = std::fs::remove_dir_all(&r);
}

/// 尾巴 B（已做）：memory 的启停快照**要**保存墓碑 —— 本条以前是现状记录，今天按计划翻红后改写。
///
/// 为什么这不是"内存引擎的洁癖"：`tools::sync` 与 `import --skip-existing` 的判据①是
/// "目标见过这把键吗（含墓碑）"。快照里没墓碑 ⇒ 重启一次之后"这把键被删过"整个消失 ⇒
/// 判据的地基没了，后续同步能把已删键复活。所以墓碑必须活过重启。
/// 活行那一半边一直是合格的，两条一起判才不空转。
#[test]
fn memory_snapshot_keeps_tombstones_alive() {
    let r = root("mem_snap_tombstone");
    let dir = r.join("memory");
    std::fs::create_dir_all(&dir).unwrap();
    {
        let db = open_engine_with(EngineKind::Memory, Some(&dir), &migrate_config()).unwrap();
        {
            let tx = db.begin();
            assert!(tx.set_raw(b"gone", b"was-here".to_vec()));
            assert!(tx.set_raw(b"stay", b"live".to_vec()));
            tx.commit();
        }
        {
            let tx = db.begin();
            assert!(tx.delete(b"gone"));
            tx.commit();
        }
        // 重启之前：墓碑在（这条是上面那把判据的前提）
        {
            let tx = db.begin();
            assert!(tx.get_meta(b"gone").is_some(), "本条的前提变了");
            tx.commit();
        }
        // Drop 就是"停止落盘"（第 6 项前半钉死的口径）
        drop(db);
    }
    let db = open_engine_with(EngineKind::Memory, Some(&dir), &migrate_config()).unwrap();
    let tx = db.begin();
    assert_eq!(
        tx.get(b"stay").as_deref(),
        Some(&b"live"[..]),
        "活行要活过重启（这半边是合格的）"
    );
    let m = tx.get_meta(b"gone");
    assert!(
        m.is_some(),
        "memory 快照把墓碑丢了 ⇒ 重启后 sync 的判据①没有地基"
    );
    assert!(m.unwrap().deleted, "重放回来的那行必须是墓碑，不是活值");
    assert_eq!(tx.get(b"gone"), None, "墓碑重放成活值了");
    tx.commit();
    drop(tx);
    drop(db);
    let _ = std::fs::remove_dir_all(&r);
}
