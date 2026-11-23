// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// 解析缓存**一次都不许服务旧内容**。
///
/// 失效凭的是"按页写入计数"（`PageManager::write_count`），不是在每个写点手工清 ——
/// 所以这条真正盯的是**哪条路径改了页内容却没 bump 计数**：覆盖写、分裂写回两片页、
/// 合并后 free 再 allocate 复用同一个页号，都在这条里走一遍。
#[test]
fn test_node_cache_never_serves_stale_content() {
    let (_s, mut tree) = anon(4, 16);
    // ① 边插边读：分裂会不断改写既有页
    for n in 0..200u8 {
        assert!(tree.insert(key(n), rid(n)), "插入 {n} 失败");
        assert_eq!(
            tree.get(key(n)).unwrap().0,
            Some(vec![n]),
            "写完立刻读第 {n} 个拿到了旧内容"
        );
    }
    // ② 覆盖写：同一个 key 换 RowId
    for n in 0..200u8 {
        let new = n.wrapping_add(1);
        assert!(tree.update(key(n), rid(new)), "update {n} 失败");
        assert_eq!(
            tree.get(key(n)).unwrap().0,
            Some(vec![new]),
            "覆盖写之后第 {n} 个仍是旧值 = 服务了旧节点"
        );
    }
    // ③ 删掉再插回：页被 free 后会被 allocate 复用，计数必须 bump
    for n in (0..200u8).step_by(3) {
        assert!(tree.delete(key(n)), "删除 {n} 失败");
        assert!(
            tree.get(key(n)).is_none(),
            "删掉 {n} 之后还读得到 = 旧节点还在被服务"
        );
        assert!(tree.insert(key(n), rid(n)), "复用位置插回 {n} 失败");
        assert_eq!(
            tree.get(key(n)).unwrap().0,
            Some(vec![n]),
            "复用页之后读回的是上一个占用者的内容"
        );
    }
}
