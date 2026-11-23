// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// 崩溃最常留下的现场是**半帧**：写到最后一次 flush 之间被砍断。
/// 必须丢掉坏尾并保住前面所有完整记录 —— 否则一次断电等于整库恢复失败。
#[test]
fn test_corrupt_tail_is_discarded_but_good_frames_survive() {
    let path = tmp_wal("corrupt");
    {
        let mut wal = Wal::open(&path).unwrap();
        wal.append(WalRecord::Begin { xid: 1 }).unwrap();
        wal.append(WalRecord::Write {
            xid: 1,
            key: b"a".to_vec(),
            value: Some(b"1".to_vec()),
        })
        .unwrap();
        wal.append(WalRecord::Commit { xid: 1 }).unwrap();
        wal.sync().unwrap();
        // 手工接一个"长度合法但 CRC 不对"的残帧，模拟半截写入
        let mut raw = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        use std::io::Write;
        raw.write_all(&[9u8; 4]).unwrap(); // len = 9
        raw.write_all(b"NOT-A-FRAME").unwrap();
        raw.flush().unwrap();
    }
    {
        let mut wal = Wal::open(&path).unwrap();
        let entries = wal.read_all().unwrap();
        assert_eq!(entries.len(), 3, "坏尾之后的完整记录不该被牵连");
        let plan = analyze_recovery(&entries);
        assert_eq!(plan.committed_writes.len(), 1);
        assert_eq!(plan.max_xid, 1);
    }
    let _ = std::fs::remove_file(&path);
}
