// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn disk_file_round_trips_offsets() {
    let path = std::env::temp_dir().join(format!("se_disk_{}.bin", std::process::id()));
    let mut f = DiskFile::open(&path).unwrap();
    let off = f.append(b"hello").unwrap();
    assert_eq!(off, 0);
    let off2 = f.append(b"world").unwrap();
    assert_eq!(off2, 5);
    assert_eq!(f.len().unwrap(), 10);
    let mut buf = [0u8; 5];
    f.read_exact_at(off2, &mut buf).unwrap();
    assert_eq!(&buf, b"world");
    f.write_all_at(0, b"HELLO").unwrap();
    f.read_exact_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"HELLO");
    f.sync().unwrap();
    drop(f);
    let _ = std::fs::remove_file(&path);
}

/// 读一页**不许问文件长度**（M31）。
///
/// 旧写法 `read_page` 里那句 `self.len()` 是每次读页一趟 `File::metadata()`，本机裸测
/// 6.2~8.9 µs/次，而整次定位读才 15~17 µs ⇒ 四到五成的页读代价花在一句可以不要的问题上
/// （判断"这页在不在 EOF 之外"完全可以交给 `read` 自己走到 EOF）。
///
/// ⚠️ 与 M19/M30 同一个写法：**同一条用例里必须主动问一次 `len()` 证明这把尺子数得到**，
/// 否则"0"可以是空转出来的。
#[test]
fn test_read_page_does_not_ask_file_length() {
    const BS: usize = 64;
    let path = std::env::temp_dir().join(format!("se_disk_lenhot_{}.bin", std::process::id()));
    let mut f = DiskFile::open_paged(&path, BS).unwrap();
    for i in 0..8u64 {
        f.write_page(i, &vec![0xA0u8; BS]).unwrap();
    }
    let base = len_calls();
    for i in 0..8u64 {
        let p = f.read_page(i).unwrap();
        assert_eq!(p.len(), BS);
    }
    let hot = len_calls() - base;
    assert_eq!(
        hot, 0,
        "读 8 页过程中问了 {hot} 次文件长度 ⇒ 每页多一次 metadata() 系统调用\
             （本机裸测 6~9 µs/次，而整次定位读才 15~17 µs）"
    );
    // 对照：主动问一次，尺子必须数得到
    let _ = f.len().unwrap();
    assert_eq!(
        len_calls() - base,
        1,
        "尺子空转：主动问一次 len() 都没被记进账，上面那条 0 不算数"
    );
    drop(f);
    let _ = std::fs::remove_file(&path);
}
