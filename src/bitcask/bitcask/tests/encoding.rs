// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// M52 的守卫：hint 读侧从"逐条 `insert`"换成"攒有序 `Vec` 再一次 `collect`"之后，
/// 解出来的 `KeyDir` 必须与**逐条 insert 的那一份一字不差**。
///
/// 为什么不只测"写一遍再读回来对得上"：那只证明**新写新读自洽**（M42 立过这条：格式类改动
/// 的凭据得是独立基准），证明不了换解码形状没动语义。所以基准在测试里**手工逐条 insert** 出来。
///
/// 覆盖面（少一种就有一种退化溜过去）：
/// - 存活行 / 软墓碑行 / **未知种类**的行（改前是静默丢弃 ⇒ 必须继续丢弃，不能变成"当作存活"）
/// - **重复键**（`collect` 与逐条 `insert` 同为"后者胜"，这条不对称一旦不同就是静默改数据）
/// - v1 与 v2 两份格式（v1 没有 `file_id`，一律 0）
/// - **降序**写出的一份 hint：`write_hint_file` 永远不会产出降序，但读侧不该依赖它 ——
///   依赖"有序"只是性能优化，正确性必须与顺序无关。
#[test]
fn test_hint_read_matches_record_by_record_insert() {
    let path = tmp_log("hint_shape");
    let hint = hint_path_for(&path);
    // (键, flag, file_id, pos, len)
    let rows: Vec<(Vec<u8>, i32, u32, u64, u32)> = vec![
        (b"aa".to_vec(), HINT_FLAG_LIVE, 3, 64, 11),
        (b"bb".to_vec(), FLAG_SOFT_TOMBSTONE, 4, 0, 0),
        (b"cc".to_vec(), 99, 5, 128, 7), // 未知种类 ⇒ 两边都不许把它当存活键
        (b"dup".to_vec(), HINT_FLAG_LIVE, 1, 200, 3),
        (b"dup".to_vec(), FLAG_SOFT_TOMBSTONE, 2, 0, 0), // 重复键 ⇒ 后者胜
        (b"ee".to_vec(), HINT_FLAG_LIVE, 6, 300, 9),
    ];
    for (v2, order) in [(true, false), (true, true), (false, false)] {
        let mut feed = rows.clone();
        if order {
            feed.reverse();
        }
        build_hint_file(&hint, v2, &feed, None);
        let (got, active_id, log_size) = read_hint_file(&hint).unwrap_or_else(|e| {
            panic!("读不出自己搭的 hint（v2={v2} 降序={order}）：{e}");
        });
        // 基准：逐条 insert（= 改前的读法）
        let mut want = KeyDir::new();
        for (k, flag, fid, pos, len) in &feed {
            let fid = if v2 { *fid } else { 0 };
            match *flag {
                HINT_FLAG_LIVE => {
                    want.insert(
                        k.clone(),
                        KeyLoc::Live {
                            file_id: fid,
                            pos: *pos,
                            len: *len,
                        },
                    );
                }
                FLAG_SOFT_TOMBSTONE => {
                    want.insert(k.clone(), KeyLoc::SoftTombstone { file_id: fid });
                }
                _ => {}
            }
        }
        assert_eq!(
            got, want,
            "解出来的 KeyDir 与逐条 insert 那份不同（v2={v2} 降序={order}）"
        );
        let want_id = if v2 { 7 } else { 0 };
        assert_eq!(active_id, want_id, "active_id（v2={v2}）");
        assert_eq!(log_size, 4096, "active_log_size（v2={v2}）");
    }
    // 坏 `key_len`：两份格式都必须报错（改前 v1 那份漏了这道检查 ⇒ 现在一并拦下），
    // 调用方据此回退全量扫。先信了荒谬的键长，就会先分配一个大 `Vec` 再去读。
    for v2 in [true, false] {
        build_hint_file(&hint, v2, &rows, Some(600 << 20));
        let err = read_hint_file(&hint).unwrap_err();
        assert_eq!(
            err.kind(),
            std::io::ErrorKind::InvalidData,
            "坏 key_len 没被拦住（v2={v2}）"
        );
    }
    cleanup(&path);
}
