// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// 返回 **RAII 句柄**（M82）：把它声明在引擎之前，用例结束（含 panic 展开）自动删，
/// 不必再指望人记得写 `cleanup` —— 而"引擎还开着就删"在 Windows 上是静默失败。
fn tmp_log(tag: &str) -> crate::common::ScratchPath {
    crate::common::ScratchPath::new("bitcask_hint", tag, ".log")
}

fn cleanup(path: &Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(hint_path_for(path));
    let _ = std::fs::remove_file(path.with_extension("merge"));
    let _ = std::fs::remove_file(path.with_extension("merge_hold"));
    let _ = std::fs::remove_file(path.with_extension("rotate_hold"));
    // 密封分片
    for id in discover_sealed_ids(path) {
        let _ = std::fs::remove_file(segment_path(path, id));
    }
}

/// 手工按文档布局搭一份 hint（不经 `write_hint_file`，为的是能造出未知种类 / 重复键 / 降序）。
/// `lie_last_key_len` = 把**最后一条**记录声明的键长改成这个值（造坏 hint 用）。
fn build_hint_file(
    path: &Path,
    v2: bool,
    rows: &[(Vec<u8>, i32, u32, u64, u32)],
    lie_last_key_len: Option<usize>,
) {
    let mut bytes = Vec::new();
    if v2 {
        bytes.extend_from_slice(HINT_MAGIC_V2);
        bytes.extend_from_slice(&7u32.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
        bytes.extend_from_slice(&4096u64.to_le_bytes());
        bytes.extend_from_slice(&(rows.len() as u64).to_le_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes());
    } else {
        bytes.extend_from_slice(HINT_MAGIC_V1);
        bytes.extend_from_slice(&4096u64.to_le_bytes());
        bytes.extend_from_slice(&(rows.len() as u64).to_le_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes());
    }
    for (i, (k, flag, fid, pos, len)) in rows.iter().enumerate() {
        let declared = match lie_last_key_len {
            Some(n) if i + 1 == rows.len() => n,
            _ => k.len(),
        };
        bytes.extend_from_slice(&(declared as u32).to_le_bytes());
        bytes.extend_from_slice(&flag.to_le_bytes());
        if v2 {
            bytes.extend_from_slice(&fid.to_le_bytes());
        }
        bytes.extend_from_slice(&pos.to_le_bytes());
        bytes.extend_from_slice(&len.to_le_bytes());
        bytes.extend_from_slice(k);
    }
    std::fs::write(path, bytes).unwrap();
}

fn check_oversized_hint(measure_heap: bool) {
    // ⑥ 端到端：把头部那个声明值改成 `u64::MAX` 的坏 hint —— 读它必须**报错**，
    //    而不是按声明值先做一次巨大分配（这条就是那条界在真实路径上的样子）
    let path = tmp_log("hint_bound_lie");
    let hint = hint_path_for(&path);
    let rows: Vec<(Vec<u8>, i32, u32, u64, u32)> = (0..3)
        .map(|i| {
            (
                format!("k{i}").into_bytes(),
                HINT_FLAG_LIVE,
                0u32,
                i as u64,
                1u32,
            )
        })
        .collect();
    build_hint_file(&hint, true, &rows, None);
    let mut raw = std::fs::read(&hint).unwrap();
    // 头部布局：magic(8) | active_id(4) | 预留(4) | active_log_size(8) | **entry_count(8)** | 预留(8)
    // ⇒ 声明值在 `[头部大小−16, 头部大小−8)`；最后那 8 字节是空着的 `reserved`
    let off = HINT_HEADER_SIZE_V2 - 16;
    raw[off..off + 8].copy_from_slice(&u64::MAX.to_le_bytes());
    std::fs::write(&hint, &raw).unwrap();
    let before = crate::mem_probe::live() as i64;
    hint_reserve_reset();
    let bad = read_hint_file(&hint)
        .err()
        .expect("条目数被吹大的 hint 要报错");
    if measure_heap {
        let grew = crate::mem_probe::live() as i64 - before;
        assert!(
            grew < 1 << 16,
            "坏 hint 读失败前多占了 {grew} B ⇒ 预留没被文件字节数夹住"
        );
    }
    // 端到端那一半：门自己承认它预留了几条 —— 声明是 `u64::MAX`，界只能是"这文件装得下的 3 条"
    assert!(
        hint_reserve_noted() <= rows.len(),
        "坏 hint 让门预留了 {} 条，而文件只装得下 {} 条 ⇒ 那条界没接上",
        hint_reserve_noted(),
        rows.len()
    );
    let _ = bad;
    let _ = std::fs::remove_file(&hint);
    let _ = std::fs::remove_file(&path);
}
