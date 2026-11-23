// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// 返回 **RAII 句柄**（M82）：把它声明在引擎之前，用例结束（含 panic 展开）自动删，
/// 不必再指望人记得写 `cleanup` —— 而"引擎还开着就删"在 Windows 上是静默失败。
fn tmp(tag: &str) -> crate::common::ScratchPath {
    crate::common::ScratchPath::new("sst", tag, ".sst")
}
