// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// hint 指的是向已经不存在的分片 ⇒ 必须回退全量扫，不能悄悄少键。
///
/// 增量扫只补 active 文件里 `active_size` 之后的新增行；键的位置要是被 hint 记成
/// 「在某个已删掉的分片上」，它就**永远不会**被这次扫描补回来。
/// 真实来源就是崩在 merge 中途：合并后的 data.log 是全的，hint 却还记着合并前的布局。
#[test]
fn test_hint_naming_missing_segment_falls_back_to_full_scan() {
    let path = tmp_log("stale_hint");
    {
        // 小分片上限 ⇒ 会 rotate，active 里是最后几行
        let mut eng = Bitcask::with_max_file_bytes(path.to_path_buf(), 64).unwrap();
        for i in 0..12u8 {
            eng.set(format!("k{i}").as_bytes(), vec![i; 40]).unwrap();
        }
        eng.flush().unwrap();
        eng.dump_hint().unwrap();
    }
    let hint = hint_path_for(&path);
    let (mut kd, active_id, active_size) = read_hint_file(&hint).unwrap();
    // 把最后写入的那个键伪造成"在分片 999 上"：它的真实记录确实在 data.log 里
    let last = format!("k{}", 11u8).into_bytes();
    kd.insert(
        last,
        KeyLoc::Live {
            file_id: 999,
            pos: 0,
            len: 40,
        },
    );
    write_hint_file(&hint, &kd, active_id, active_size).unwrap();

    {
        let mut eng = Bitcask::new(path.to_path_buf()).unwrap();
        for i in 0..12u8 {
            assert_eq!(
                eng.get(format!("k{i}").as_bytes()).unwrap(),
                Some(vec![i; 40]),
                "k{i} 该由全量扫补回来（hint 里有一条指向失踪分片的位置）"
            );
        }
    }
    cleanup(&path);
}

#[test]
fn test_hint_incremental_tail_scan() {
    let path = tmp_log("incr");
    {
        let mut eng = Bitcask::new(path.to_path_buf()).unwrap();
        eng.set(b"k1", b"v1".to_vec()).unwrap();
        eng.set(b"k2", b"v2".to_vec()).unwrap();
        eng.flush().unwrap();
        eng.dump_hint().unwrap();
    }
    let stale = std::fs::read(hint_path_for(&path)).unwrap();
    // 写更多数据但不更新 hint → open 应 hint + 扫尾部
    {
        let mut eng = Bitcask::new(path.to_path_buf()).unwrap();
        eng.set(b"k3", b"v3".to_vec()).unwrap();
        eng.set(b"k1", b"v1b".to_vec()).unwrap(); // 覆盖
        eng.flush().unwrap();
        // 故意不 dump_hint
        drop(eng);
    }
    // Drop 会发布新 hint；恢复旧文件字节，才能实际覆盖重放路径。
    std::fs::write(hint_path_for(&path), stale).unwrap();
    {
        let mut eng = Bitcask::new(path.to_path_buf()).unwrap();
        assert_eq!(eng.get(b"k1").unwrap(), Some(b"v1b".to_vec()));
        assert_eq!(eng.get(b"k2").unwrap(), Some(b"v2".to_vec()));
        assert_eq!(eng.get(b"k3").unwrap(), Some(b"v3".to_vec()));
        assert_eq!(eng.len(), 3);
    }
    cleanup(&path);
}
