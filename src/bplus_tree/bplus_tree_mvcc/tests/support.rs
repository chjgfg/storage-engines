// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// 返回 **RAII 句柄**（M82）：把它声明在引擎之前，用例结束（含 panic 展开）自动删，
/// 不必再指望人记得写 `cleanup` —— 而"引擎还开着就删"在 Windows 上是静默失败。
fn tmp_db(tag: &str) -> crate::common::ScratchPath {
    crate::common::ScratchPath::new("mvcc", tag, ".db")
}

fn cleanup(path: &Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(dblwr_path(path));
    let _ = std::fs::remove_file(wal_path(path));
    let _ = std::fs::remove_file(crate::bplus_tree::storage::freelist_path(path));
    let _ = std::fs::remove_file(crate::bplus_tree::storage::lock_path(path));
    let _ = std::fs::remove_file(crate::bplus_tree::storage::blob_path(path));
}

/// 把 `vacuum_bounded` 的块大小临时压小的 RAII 守卫：离开作用域（含 panic 展开）一律复位，
/// 免得 thread_local 漏给同线程后跑的别的 vacuum 测试。
struct VacChunkGuard;
impl Drop for VacChunkGuard {
    fn drop(&mut self) {
        set_vac_chunk_override(0);
    }
}
