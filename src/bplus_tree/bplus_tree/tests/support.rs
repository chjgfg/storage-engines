// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// `BPlusTree::new` 是"匿名临时库"的兼容接口：路径它自己造、**没人负责删**（公开 API
/// 不能偷偷改成 drop 即删）。测试要用它就得自己拎着守卫 —— 返回值必须**绑在变量上**，
/// 元组按字段序析构 ⇒ 引擎先落地、守卫后删目录（M82）。
fn anon(order: usize, max_cache_pages: usize) -> (crate::common::ScratchPath, BPlusTree) {
    let dir = crate::common::ScratchPath::new("bplus_anon", "test", "");
    let db = BPlusTree::open(dir.join("anon.db"), order, max_cache_pages);
    (dir, db)
}

fn rid(v: u8) -> RowId {
    RowId(Some(vec![v]))
}

fn key(n: u8) -> Key {
    vec![n]
}

fn collect_all(tree: &mut BPlusTree) -> Vec<u8> {
    tree.iter().map(|(k, _)| k[0]).collect()
}
