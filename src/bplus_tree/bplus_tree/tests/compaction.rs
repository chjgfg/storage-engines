// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn compact_internal_routes_match_owned_and_reject_invalid_nodes() {
    for count in [0, 1, 16, 63] {
        let keys: Vec<Key> = (0..count)
            .map(|i| format!("{i:04}{}", "x".repeat(i % 19)).into_bytes())
            .collect();
        let children = (0..=count).map(|i| PageId(i as u64 + 1)).collect();
        let node = BTreeNode::Internal {
            keys: keys.clone(),
            children,
        };
        let payload = bincode::serialize(&node).unwrap();
        let packed = PointInternal::decode(&payload).unwrap();
        for key in keys.iter().chain([Vec::new(), vec![255]].iter()) {
            assert_eq!(
                packed.child(key),
                PageId(BPlusTree::child_index(&keys, key) as u64 + 1)
            );
        }
        for end in [0, 4, 11, payload.len() - 1] {
            assert!(PointInternal::decode(&payload[..end]).is_err());
        }
        let mut bad = payload.clone();
        bad.push(1);
        assert!(PointInternal::decode(&bad).is_err());
        let (owned, owned_cost) = crate::mem_probe::count_thread_allocations(|| {
            bincode::deserialize::<BTreeNode>(&payload).unwrap()
        });
        let (compact, compact_cost) =
            crate::mem_probe::count_thread_allocations(|| PointInternal::decode(&payload).unwrap());
        if count > 1 {
            assert!(owned_cost >= compact_cost + count as u64);
        }
        drop((owned, compact));
    }
    let duplicate = BTreeNode::Internal {
        keys: vec![vec![1], vec![1]],
        children: vec![PageId(1); 3],
    };
    assert!(PointInternal::decode(&bincode::serialize(&duplicate).unwrap()).is_err());
    let bad_children = BTreeNode::Internal {
        keys: vec![vec![1]],
        children: vec![PageId(1)],
    };
    assert!(PointInternal::decode(&bincode::serialize(&bad_children).unwrap()).is_err());
}

#[test]
fn compact_internal_cache_promotes_and_invalidates_after_writes() {
    let (_scratch, mut tree) = anon(8, 8);
    for i in 0..600_u64 {
        assert!(tree.insert(i.to_be_bytes().to_vec(), RowId(Some(vec![i as u8; 32]))));
    }
    tree.try_flush_all().unwrap();
    for old in [true, false, false, true] {
        tree.node_cache.clear();
        let _mode = old.then(point_internal_reference::Scope::start);
        let parses = node_parses();
        let packs = point_internal_packs();
        for i in (0..600_u64).map(|i| i * 199 % 600) {
            let key = i.to_be_bytes();
            assert_eq!(
                tree.find_back(&key, &key, |_| true),
                Some((key.to_vec(), RowId(Some(vec![i as u8; 32]))))
            );
        }
        if old {
            assert!(node_parses() > parses);
            assert_eq!(point_internal_packs(), packs);
        } else {
            assert!(node_parses() - parses <= tree.page_manager.effective_pin_limit() as u64);
            assert!(point_internal_packs() > packs);
        }
    }
    tree.node_cache.clear();
    for i in 0..600_u64 {
        let key = i.to_be_bytes();
        tree.find_back(&key, &key, |_| true).unwrap();
    }
    let packed_page = *tree
        .node_cache
        .iter()
        .find(|(_, (_, node))| matches!(node, CachedNode::Internal(_)))
        .unwrap()
        .0;
    let before = point_internal_promotions();
    tree.node_of(packed_page).unwrap();
    assert_eq!(point_internal_promotions() - before, 1);
    for i in 0..600_u64 {
        let key = i.to_be_bytes().to_vec();
        // 在修改或重平衡前预热紧凑祖先缓存，验证更新后不会读到旧路由。
        tree.node_cache.clear();
        tree.find_back(&key, &key, |_| true).unwrap();
        assert!(tree.delete(key.clone()));
        assert!(tree.find_back(&key, &key, |_| true).is_none());
        if i % 3 == 0 {
            assert!(tree.insert(key, RowId(Some(vec![99; 32]))));
        }
    }
    tree.try_flush_all().unwrap();
    let path = tree.page_manager.path().to_path_buf();
    drop(tree);
    let mut tree = BPlusTree::open(&path, 8, 8);
    for i in 0..600_u64 {
        let key = i.to_be_bytes();
        let expected = (i % 3 == 0).then(|| (key.to_vec(), RowId(Some(vec![99; 32]))));
        assert_eq!(tree.find_back(&key, &key, |_| true), expected);
    }
}
