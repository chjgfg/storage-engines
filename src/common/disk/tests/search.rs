// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// 去掉那句 `len()` 之后，**三种页形状的结果必须与旧写法逐字节相同**（M31）。
///
/// 这条是上一条的另一半：上一条管"少了一次系统调用"，这一条管"语义没动"。
/// 旧写法靠 `offset >= file_len` 与 `available` 两个分支表达"整页在外的页读回全零"和
/// "尾部半页零填充"；新写法靠 `read` 走到 EOF 得到同一个结果 —— 但"得到同一个结果"
/// 是要逐字节对的，不是靠读代码看出来。
#[test]
fn test_read_page_tail_shapes_match_the_old_length_guarded_form() {
    const BS: usize = 64;
    let path = std::env::temp_dir().join(format!("se_disk_tail_{}.bin", std::process::id()));
    let mut f = DiskFile::open_paged(&path, BS).unwrap();
    // 整页两页
    f.write_page(0, &vec![1u8; BS]).unwrap();
    f.write_page(1, &vec![2u8; BS]).unwrap();
    // 第 2 页只写 20 字节 ⇒ 文件长 2*BS + 20，这是一页"尾部半页"
    f.write_all_at(2 * BS as u64, &[3u8; 20]).unwrap();

    let whole = f.read_page(0).unwrap();
    assert_eq!(whole, vec![1u8; BS], "整页必须原样读回");
    let half = f.read_page(2).unwrap();
    assert_eq!(half.len(), BS, "越界/半页也必须返回正好一块");
    assert_eq!(&half[..20], &[3u8; 20][..], "半页的前 20 字节是真数据");
    assert!(
        half[20..].iter().all(|&b| b == 0),
        "半页剩下必须是零（旧写法靠 available 截断得到同一个结果）"
    );
    let beyond = f.read_page(9).unwrap();
    assert!(
        beyond.iter().all(|&b| b == 0),
        "整页在 EOF 之外必须是全零块（旧写法靠 offset >= file_len 那条分支）"
    );
    drop(f);
    let _ = std::fs::remove_file(&path);
}
