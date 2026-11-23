// 该文件集中验证对应职责，保留原测试名称和字面值。

/// 重扫窗口里每一扇**能报错的门**都必须报错，而不是回「键不存在」（M43）。
///
/// 这一条买的是 B1 的那笔代价：为了不把峰值抬到两份索引，旧索引必须在重扫之前丢掉，
/// 而丢掉的这段时间里磁盘上明明有全库、内存里却没人知道有哪些键。让它回 `Ok(None)`
/// 就是宣称「这些键不存在」—— 门面会当真是（`get_entry` 的 `None` 在上层是「这版本没
/// 这一项」）。宁可让调用方拿到一个错。
///
/// hint 那一支单独钉：hint 是索引的**持久化副本**，空索引写成 hint 落盘，下次 open 就把它
/// 当真索引读回来 —— 那是把「一次失败」变成「整库看不见」。open 侧的 `hint_files_exist`
/// 兜不住（空表里挑不出指向缺失分片的条目）。
///
/// 变异验证：① 去掉 `get` 里的 `usable()?` ⇒ 第一条断言红在「回了 Some」；
/// ② 去掉 `insert`/`set` 那两道 ⇒ 红在「被拒的写入还是落盘了」；
/// ③ 去掉 `dump_hint` 那道 ⇒ 红在「隔离期把 hint 落盘了」。
#[test]
fn test_quarantined_engine_errors_instead_of_claiming_keys_missing() {
    let path = tmp_log("quarantine");
    let hint = hint_path_for(&path);
    {
        let mut eng = Bitcask::new(path.to_path_buf()).unwrap();
        eng.set(b"a", b"1".to_vec()).unwrap();
        eng.merge().unwrap(); // 正常路径走完：此时索引完整、可读写
        eng.flush().unwrap();
        let len_before = std::fs::metadata(&path).unwrap().len();

        eng.quarantine_index_for_test();

        // ── 读侧：不许把「索引里没有」说成「键不存在」──
        let g = eng.get(b"a");
        assert!(
            g.is_err(),
            "隔离期 get 回了「{:?}」⇒ 空索引被当成「键都不存在」",
            g.ok()
        );
        let ge = eng.get_entry(b"a");
        assert!(
            ge.is_err(),
            "隔离期 get_entry 回了「{:?}」⇒ 上层会把它当成「这版本没有值」",
            ge.ok()
        );
        assert!(
            eng.range_scan(b"a".to_vec(), b"z".to_vec()).is_err(),
            "隔离期 range_scan 回了 Ok(空)"
        );
        assert!(eng.iter().is_err(), "隔离期 iter 回了 Ok(空)");

        // ── 写侧：一条都不许落盘 ──
        assert!(eng.set(b"b", b"2".to_vec()).is_err(), "隔离期 set 竟然成功");
        assert!(
            eng.insert(b"c".to_vec(), Some(b"3".to_vec())).is_err(),
            "隔离期 insert 竟然成功"
        );
        assert!(
            eng.insert_batch(vec![(b"d".to_vec(), Some(b"4".to_vec()))])
                .is_err(),
            "隔离期 insert_batch 竟然成功"
        );
        assert!(eng.delete(b"a").is_err(), "隔离期 delete 竟然成功");
        assert!(eng.remove(b"a").is_err(), "隔离期 remove 竟然成功");

        // ── hint：不许把空索引写下去 ──
        let _ = std::fs::remove_file(&hint);
        assert!(eng.dump_hint().is_err(), "隔离期 dump_hint 回了 Ok");
        assert!(
            !hint.exists(),
            "隔离期把 hint 落盘了 ⇒ 下次 open 会当真索引读回来"
        );

        eng.flush().unwrap();
        let len_after = std::fs::metadata(&path).unwrap().len();
        assert_eq!(
            len_before,
            len_after,
            "被拒的写入还是落盘了（差 {} 字节）",
            len_after.saturating_sub(len_before)
        );

        // ── 拦下来的是那个**状态**，不是把对象弄坏了 ──
        eng.release_index_for_test();
        eng.set(b"e", b"5".to_vec()).unwrap();
        eng.flush().unwrap();
        assert!(
            std::fs::metadata(&path).unwrap().len() > len_after,
            "解除隔离之后新写入没落盘 ⇒ 守卫动到了文件"
        );
        assert_eq!(
            eng.get(b"e").unwrap(),
            Some(b"5".to_vec()),
            "解除隔离之后同一句柄的新写入读不回来"
        );
        // 旧键此时在内存里确实没有了 —— 窗口里索引本来就是空的，这正是每一扇门都要
        // 拦下来的原因（让它悄悄回 `Ok(None)` 就等于宣称「这键不存在」）。
        assert_eq!(
            eng.get(b"a").unwrap(),
            None,
            "解除隔离之后旧键不该凭空回来：出路只有重新 open"
        );

        // ── 退回隔离，然后让它出作用域：Drop 会 flush + dump_hint，后者必须写不下去 ──
        eng.quarantine_index_for_test();
        eng.flush().unwrap();
    }
    // ── 重新 open 是唯一出路，而它真的救得回来 ──
    assert!(
        !hint.exists(),
        "带着隔离状态 drop 之后 hint 落盘了 ⇒ 下次 open 会把空索引当真"
    );
    {
        let mut eng = Bitcask::new(path.to_path_buf()).unwrap();
        assert_eq!(
            eng.get(b"a").unwrap(),
            Some(b"1".to_vec()),
            "重新 open 读不回旧键 ⇒ 隔离期把日志写坏了"
        );
        assert_eq!(
            eng.get(b"e").unwrap(),
            Some(b"5".to_vec()),
            "重新 open 读不回隔离解除后的新写入"
        );
        assert_eq!(eng.len(), 2);
    }
    cleanup(&path);
}
