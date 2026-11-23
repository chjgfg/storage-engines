// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// M53 的守卫：**拿一把锁不许付 fsync**（改前每次 open 都要为"把 pid 写进锁文件"付一次）。
///
/// 为什么这一格可以省：锁的安全性来自**句柄本身**（Windows 的 `share_mode(0)`、unix 的
/// `flock`）—— 进程一死内核就释放，与那行 pid 有没有落盘无关。省掉的只有 fsync，
/// `set_len` + `write_all` 留着：活着的读者仍然看得见 pid（那是这条存在的唯一理由）。
/// ⚠️ 反向那半也要钉：锁的语义不许被一起带走（第二个 `try_acquire` 必须还是 `WouldBlock`）。
/// 证人：同线程里主动 `DiskFile::sync()` 一次，尺子必须数得到。
#[test]
fn file_lock_hint_is_written_without_an_fsync() {
    use crate::common::disk::{SITE_LOCK_PID, SITE_OTHER, fsync_counts, fsync_counts_clear};
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "se_disk_lock_fsync_{}_{}",
        std::process::id(),
        nanos
    ));
    let lock_file = dir.join("db.lock");

    fsync_counts_clear();
    let _hold = FileLock::try_acquire_at(&lock_file).unwrap();
    let c = fsync_counts();
    assert_eq!(
        c[SITE_LOCK_PID], 0,
        "锁文件又自己 fsync 了 ⇒ 每次 open 多付一次（启动税那一格）"
    );
    assert_eq!(
        c.iter().sum::<u64>(),
        0,
        "拿一把锁不该产生任何 fsync，现在 {:?}",
        c
    );

    // 提示到底写没写进去，只能在**放开之后**读：Windows 这一侧的锁是 `share_mode(0)`，
    // 握着句柄时连本进程都打不开这个文件（实测 error 32 共享冲突 —— 那正是锁的本意）。
    let pid_line = format!("pid={}\n", std::process::id());

    let second = FileLock::try_acquire_at(&lock_file);
    let kind = match second {
        // `FileLock` 没有 Debug（持着句柄，derive 不出来）⇒ 只能自己 match
        Ok(_) => panic!("锁的语义被一起带走了 ⇒ 第二个持有者现在能同时开同一个库"),
        Err(e) => e.kind(),
    };
    assert_eq!(
        kind,
        std::io::ErrorKind::WouldBlock,
        "锁的语义被一起带走了 ⇒ 第二个进程现在能同时开同一个库"
    );

    drop(_hold);
    let txt = std::fs::read_to_string(&lock_file).unwrap();
    assert!(
        txt.contains(&pid_line),
        "pid 提示没写进锁文件（省的是 fsync，不是那次写）：{txt:?}"
    );

    // 证人：尺子在这条路上数得到 fsync
    fsync_counts_clear();
    let mut f = DiskFile::open(dir.join("witness.dat")).unwrap();
    f.sync().unwrap();
    assert_eq!(
        fsync_counts()[SITE_OTHER],
        1,
        "证人失败：主动 sync 一次都没数到 ⇒ 上面那个 0 是量不到，不是不花钱"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// 没声明块大小就按 page_index 读写 ⇒ 必须**明确报错**，
/// 不能猜一个页大小然后把数据写到错误的偏移上（那会是一片看起来正常的坏数据）。
#[test]
fn page_ops_require_declared_block_size() {
    let path = std::env::temp_dir().join(format!("se_disk_pg_{}.bin", std::process::id()));
    let mut f = DiskFile::open(&path).unwrap();
    let err = f.read_page(0).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    let err = f.write_page(0, &[0u8; 16]).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    drop(f);
    let _ = std::fs::remove_file(&path);
}

/// 同一把锁的第二个持有者要拿到 `WouldBlock`（不是 panic、不是阻塞成功）。
#[test]
fn file_lock_blocks_second_holder() {
    let dir = std::env::temp_dir().join(format!("se_lock_{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let lock_file = dir.join("db.lock");
    let _hold = FileLock::try_acquire_at(&lock_file).unwrap();
    let second = FileLock::try_acquire_at(&lock_file);
    assert!(second.is_err(), "第二个持有者不该拿到锁");
    assert_eq!(
        second.err().map(|e| e.kind()),
        Some(std::io::ErrorKind::WouldBlock),
        "错误类型必须是 WouldBlock，调用方靠它区分「被占用」和「盘坏了」"
    );
    // 释放之后同一把锁可以再拿
    drop(_hold);
    let again = FileLock::try_acquire_at(&lock_file);
    assert!(again.is_ok());
    drop(again);
    let _ = std::fs::remove_dir_all(&dir);
}
