// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// 返回 **RAII 句柄**（M82）：把它声明在引擎之前，用例结束（含 panic 展开）自动删，
/// 不必再指望人记得写 `cleanup` —— 而"引擎还开着就删"在 Windows 上是静默失败。
fn tmp_db(tag: &str) -> crate::common::ScratchPath {
    crate::common::ScratchPath::new("bitcask_mvcc", tag, "")
}

fn cleanup(path: &Path) {
    let _ = std::fs::remove_dir_all(path);
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(wal_path(path));
    let _ = std::fs::remove_file(lock_path(path));
    let _ = std::fs::remove_file(data_path(path));
    let _ = std::fs::remove_file(blob_path(path));
    let _ = std::fs::remove_file(hint_path(path));
}

/// M62：分段尺子开没开（与 `bitcask::merge_stage_gated` 同一个环境变量，读一次不缓存 —— 台架里改不了它）
fn merge_stage_on() -> bool {
    std::env::var("MERGE_STAGE_SPLIT").is_ok()
}

/// M62：把 `merge()` 内部四段的读数打出来（`净增` = 段末还活着多少 vs 进门时；`尖峰` = 段内最多涨到多少）
fn print_merge_stages(tag: &str) {
    const MB: f64 = 1024.0 * 1024.0;
    const NAMES: [&str; 4] = [
        "写新日志+腾掉旧索引后",
        "清单攒满（还没成树）",
        "成树完成并换上",
        "终态（hint 也落完）",
    ];
    let st = crate::bitcask::bitcask::merge_probe_read();
    println!(
        "    {tag}：清单 {} 条（≈{:.1} MB 格位）",
        crate::bitcask::bitcask::merge_bulk_len(),
        crate::bitcask::bitcask::merge_bulk_len() as f64 * 48.0 / MB
    );
    for (i, (live, peak)) in st.iter().enumerate() {
        println!(
            "      段{i} {:<22} 净增 {:+7.1} MB ｜ 段内尖峰 +{:6.1} MB",
            NAMES[i],
            *live as f64 / MB,
            *peak as f64 / MB
        );
    }
    if st.iter().all(|(l, p)| *l == 0 && *p == 0) {
        println!("      ⚠️ 四段全 0 ⇒ 这一趟压根没走进 merge（或门控没开），别读成\"不花钱\"");
    }
}
