// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn maintenance_walk_matches_old_dirty_binary_rows_and_stop() {
    let (_scratch, mut tree) = anon(8, 8);
    for i in 0..200 {
        assert!(tree.insert(
            format!("k{i:04}").into_bytes(),
            RowId(Some(vec![i as u8; 15]))
        ));
    }
    for key in [vec![], vec![0], vec![0, 0], vec![0, 255], vec![255]] {
        assert!(tree.insert(key, RowId(None)));
    }
    for bound in [None, Some(b"k0010".as_slice()), Some(b"z")] {
        for limit in [7, usize::MAX] {
            let mut expected = Vec::new();
            let scope = maintenance_reference::Scope::start();
            let old_end = tree
                .scan_maintenance_from(bound, |key, value| {
                    if bound.is_none_or(|b| key >= b) {
                        expected.push((key.to_vec(), value.map(<[u8]>::to_vec)));
                    }
                    expected.len() >= limit
                })
                .unwrap();
            drop(scope);
            let mut actual = Vec::new();
            let end = tree
                .scan_maintenance_from(bound, |key, value| {
                    if bound.is_none_or(|b| key >= b) {
                        actual.push((key.to_vec(), value.map(<[u8]>::to_vec)));
                    }
                    actual.len() >= limit
                })
                .unwrap();
            assert_eq!(actual, expected);
            assert_eq!(end, old_end);
        }
    }
    tree.try_flush_all().unwrap();
    let path = tree.page_manager.path().to_path_buf();
    drop(tree);
    let mut tree = BPlusTree::open(&path, 8, 8);
    let mut rows = 0;
    assert_eq!(
        tree.scan_maintenance_from(None, |_, _| {
            rows += 1;
            false
        })
        .unwrap(),
        MaintenanceEnd::Complete
    );
    assert_eq!(rows, 205);
}

#[test]
fn rebalance_reuses_nodes_and_preserves_tree_bytes() {
    let mut outcomes = Vec::new();
    for old in [true, false] {
        let (_scratch, mut tree) = anon(8, 64);
        for i in 0..1200_u64 {
            assert!(tree.insert(i.to_be_bytes().to_vec(), RowId(Some(vec![i as u8; 24]))));
        }
        let mode = old.then(rebalance_read_reference::Scope::start);
        let before = crate::bplus_tree::page::page_reads();
        let counts = rebalance_counts();
        let stats = reorg_stats();
        let victims: Vec<_> = (0..1000_u64).map(|i| i.to_be_bytes().to_vec()).collect();
        assert_eq!(tree.delete_batch(&victims), 1000);
        let reads = crate::bplus_tree::page::page_reads() - before;
        let now = rebalance_counts();
        let after = reorg_stats();
        let stats = (after.0 - stats.0, after.1 - stats.1, after.2 - stats.2);
        assert!(stats.1 + stats.2 > 0);
        if old {
            assert_eq!(now.0, counts.0);
            assert!(now.1 > counts.1);
        } else {
            assert!(now.0 > counts.0);
        }
        drop(mode);
        let mut pages = Vec::new();
        for id in 1..tree.page_manager.next_page_id() {
            pages.push(tree.page_manager.read_page(PageId(id)));
        }
        outcomes.push((reads, stats, tree.root, pages));
        tree.try_flush_all().unwrap();
        let path = tree.page_manager.path().to_path_buf();
        drop(tree);
        let mut tree = BPlusTree::open(&path, 8, 64);
        for i in 0..1200_u64 {
            assert_eq!(
                tree.get(i.to_be_bytes().to_vec()),
                (i >= 1000).then(|| RowId(Some(vec![i as u8; 24])))
            );
        }
    }
    assert!(
        outcomes[1].0 < outcomes[0].0,
        "reusing nodes must actually remove page reads"
    );
    assert_eq!(
        (&outcomes[0].1, &outcomes[0].2, &outcomes[0].3),
        (&outcomes[1].1, &outcomes[1].2, &outcomes[1].3)
    );
}

#[test]
fn packed_leaf_delete_matches_owned_bytes_and_rebalances() {
    let entries: Vec<_> = (0..30_u64)
        .map(|i| {
            (
                i.to_be_bytes().to_vec(),
                RowId(if i % 3 == 0 {
                    None
                } else {
                    Some(vec![i as u8; 32])
                }),
            )
        })
        .collect();
    for victims in [
        vec![0, 1, 4, 7, 29, 99],
        vec![99],
        (0..30).collect::<Vec<_>>(),
    ] {
        let keys: Vec<_> = victims
            .iter()
            .map(|i| (*i as u64).to_be_bytes().to_vec())
            .collect();
        let payload = bincode::serialize(&BTreeNode::Leaf {
            entries: entries.clone(),
            next_leaf: Some(PageId(77)),
        })
        .unwrap();
        let mut out = Vec::with_capacity(MAX_PAYLOAD);
        let (got, allocations) = crate::mem_probe::count_thread_allocations(|| {
            DeleteRead::project(&payload, &keys, &mut out).unwrap()
        });
        assert_eq!(allocations, 0);
        let expected: Vec<_> = entries
            .iter()
            .filter(|(k, _)| !keys.contains(k))
            .cloned()
            .collect();
        assert!(
            matches!(got, DeleteRead::PackedLeaf { remaining, removed, min_changed }
                if remaining == expected.len() && removed == entries.len() - expected.len() && min_changed == victims.contains(&0))
        );
        assert_eq!(
            out,
            bincode::serialize(&BTreeNode::Leaf {
                entries: expected,
                next_leaf: Some(PageId(77))
            })
            .unwrap()
        );
    }
    let mut costs = Vec::new();
    for old in [true, false] {
        let (_scratch, mut tree) = anon(64, 64);
        for (k, v) in entries.clone() {
            assert!(tree.insert(k, v));
        }
        let victims = vec![4_u64.to_be_bytes().to_vec()];
        let mode = old.then(delete_leaf_reference::Scope::start);
        let (deleted, allocations) =
            crate::mem_probe::count_thread_allocations(|| tree.delete_batch(&victims));
        assert_eq!(deleted, 1);
        costs.push(allocations);
        drop(mode);
    }
    assert!(
        costs[0] >= costs[1] + 30,
        "owned reference must allocate surviving rows"
    );
    for old in [true, false] {
        let (_scratch, mut tree) = anon(8, 64);
        for i in 0..400_u64 {
            assert!(tree.insert(i.to_be_bytes().to_vec(), RowId(Some(vec![i as u8; 32]))));
        }
        let mode = old.then(delete_leaf_reference::Scope::start);
        let victims: Vec<_> = (0..350_u64).map(|i| i.to_be_bytes().to_vec()).collect();
        assert_eq!(tree.delete_batch(&victims), 350);
        drop(mode);
        tree.try_flush_all().unwrap();
        let path = tree.page_manager.path().to_path_buf();
        drop(tree);
        let mut tree = BPlusTree::open(&path, 8, 64);
        for i in 0..400_u64 {
            assert_eq!(
                tree.get(i.to_be_bytes().to_vec()),
                if i < 350 {
                    None
                } else {
                    Some(RowId(Some(vec![i as u8; 32])))
                }
            );
        }
    }
}

#[test]
fn delete_buffers_preserve_serialized_bytes_and_reject_oversized_node() {
    let (_scratch, mut tree) = anon(64, 64);
    let nodes = [
        BTreeNode::Leaf {
            entries: vec![(vec![0, 1, 255], RowId(Some(vec![1; 80])))],
            next_leaf: None,
        },
        BTreeNode::Internal {
            keys: vec![vec![], vec![0, 255]],
            children: vec![PageId(2), PageId(3), PageId(4)],
        },
    ];
    for node in &nodes {
        let expected = bincode::serialize(node).unwrap();
        assert_eq!(
            bincode::serialized_size(node).unwrap(),
            expected.len() as u64
        );
        assert!(tree.write_node(tree.root, node));
        assert_eq!(
            tree.node_buffer, expected,
            "serializer must preserve the original bytes"
        );
        let mut expected_page = expected;
        expected_page.resize(MAX_PAYLOAD, 0);
        assert_eq!(
            tree.page_manager.read_page(tree.root).unwrap(),
            expected_page
        );
    }
    let pointer = tree.node_buffer.as_ptr();
    let before = tree.page_manager.read_page(tree.root).unwrap();
    let oversized = BTreeNode::Leaf {
        entries: vec![(vec![], RowId(Some(vec![1; MAX_PAYLOAD + 1])))],
        next_leaf: None,
    };
    assert!(!tree.write_node(tree.root, &oversized));
    assert_eq!(tree.node_buffer.as_ptr(), pointer);
    assert_eq!(
        tree.page_manager.read_page(tree.root).unwrap(),
        before,
        "oversized writes cannot mutate the page"
    );
}

/// 定位一个子节点**不许线性扫整页分隔键**（M34 的第二刀）。
///
/// 判据取**单次定位最多比较几把键**，界 = log2(order) + 2（`order=64` ⇒ 8；二分最坏 6）。
/// 为什么不用平均值：平均值被树形摊薄 —— 第一版用"平均 ≤ 2·log2"时，退回线性扫只把平均
/// 推到 **11.0**（界 12），变异抓不住；而同一次变异下"最坏那一次"是**整页键数**（实测 40+）。
/// 为什么这条值得钉成断言：顺序灌行时目标**永远是最后一个子**，线性扫每层都要走满整页键数，
/// 而这笔钱在 µs 上会被本机的跨构建漂移（1.3~1.9×）整个盖住 —— 只有计数看得见。
///
/// 变异验证：把 `child_index` 换回线性扫 ⇒ 最坏一次涨到整页键数，红。
#[test]
fn test_child_location_is_binary_not_linear() {
    const N: usize = 6000;
    let (_s, mut tree) = anon(64, 4096);
    let k = |i: usize| -> Key { format!("{i:08}#/worldquant/datafields/x.json").into_bytes() };
    for i in 0..N {
        assert!(
            tree.insert(k(i), RowId(Some(vec![b'v'; 47]))),
            "插入第 {i} 行失败"
        );
    }
    // 升序读：这是线性扫的**最坏**情况（每次都扫到页尾），也是灌行的实际形状
    let l0 = descent_lookups();
    let c0 = descent_key_comps();
    for i in 0..N {
        assert!(tree.get(k(i)).is_some(), "第 {i} 行读不回来");
    }
    let lookups = descent_lookups() - l0;
    let comps = descent_key_comps() - c0;
    assert!(lookups > 0, "这把尺子没数到定位次数 ⇒ 空转");
    let per = comps as f64 / lookups as f64;
    let worst = descent_max_comps();
    let bound = (64f64).log2() as u64 + 2;
    eprintln!(
        "[descent] 定位 {lookups} 次 / 平均 {per:.1} 把 / 最坏一次 {worst} 把（界 {bound}，order=64）"
    );
    assert!(
        worst >= 4,
        "最坏一次只比较 {worst} 把 ⇒ 这棵树的内部节点窄到没东西可量，这条判据在空转"
    );
    assert!(
        worst <= bound,
        "单次定位最多比较 {worst} 把分隔键（界 {bound}）⇒ 下降退回线性扫"
    );
}
