// 该文件集中验证对应职责，保留原测试名称和字面值。

/// M53 的守卫：`flush()` 那一刀要**正反两面**都钉住。
///
/// - 正面（省下来的）：账上没有未落盘的写入时不许付 fsync —— 改前每次 open 末尾那句
///   无条件 `tree.flush()` 都要白付一次，干净重开一个没人动过的库也是。
/// - 反面（不许省过头）：**写过就必须 fsync**。M8 那一课是反方向的同一个错：
///   一个"优化"把提交点的 fsync 一起省掉了。所以第三条断言钉 `dirty` 被正确清掉，
///   第四条钉"只 `flush_buf` 不 fsync"之后 `flush()` 必须补上那一次
///   （bulk 路径正是靠这个：字节已经在文件里、还没 fsync，此时 `buf.is_empty()` 是空的、
///   账却不干净 —— 这就是判据用 `dirty` 而不是用 `buf.is_empty()` 的原因）。
#[test]
fn test_flush_fsyncs_only_when_something_was_written() {
    use crate::common::disk::{fsync_counts, fsync_counts_clear};
    let path = tmp_log("flush_fsync");
    let mut kv = Bitcask::new(path.to_path_buf()).unwrap();

    // ① 什么都没写 ⇒ flush 不付 fsync
    fsync_counts_clear();
    kv.flush().unwrap();
    assert_eq!(
        fsync_counts().iter().sum::<u64>(),
        0,
        "空账还 fsync ⇒ 每次 open 白付一次（M53 数出来的那一格）"
    );

    // ② 写过就必须付，而且只付一次
    kv.insert(b"k1".to_vec(), Some(b"v1".to_vec())).unwrap();
    fsync_counts_clear();
    kv.flush().unwrap();
    let after_write = fsync_counts();
    assert!(
        after_write.iter().sum::<u64>() >= 1,
        "写过之后 flush 没有 fsync ⇒ 持久化点被这条\"优化\"省掉了（比慢更糟）"
    );

    // ③ 账已清 ⇒ 再 flush 不重复付
    fsync_counts_clear();
    kv.flush().unwrap();
    assert_eq!(
        fsync_counts().iter().sum::<u64>(),
        0,
        "dirty 没被清 ⇒ 每次 flush 都重付一遍 fsync"
    );

    // ④ 只把缓冲落盘（bulk 中途的做法）不等于 fsync 过：随后 flush() 必须补那一次
    kv.insert(b"k2".to_vec(), Some(b"v2".to_vec())).unwrap();
    kv.flush_buf().unwrap();
    fsync_counts_clear();
    kv.flush().unwrap();
    assert!(
        fsync_counts().iter().sum::<u64>() >= 1,
        "`flush_buf` 写进文件的字节被当成\"已经 fsync\"了 ⇒ 崩溃会丢这批行"
    );
    drop(kv);
    let _ = std::fs::remove_dir_all(&path);
}
