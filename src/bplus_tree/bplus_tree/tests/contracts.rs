// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn maintenance_borrowed_nodes_match_bincode_without_row_allocations() {
    let keys = vec![
        vec![],
        vec![0],
        vec![0, 0],
        vec![0, 255],
        vec![1],
        vec![255],
    ];
    let nodes = [
        BTreeNode::Leaf {
            entries: keys
                .iter()
                .enumerate()
                .map(|(i, k)| {
                    (
                        k.clone(),
                        RowId(if i % 3 == 0 {
                            None
                        } else {
                            Some(vec![i as u8; i * 7])
                        }),
                    )
                })
                .collect(),
            next_leaf: Some(PageId(42)),
        },
        BTreeNode::Internal {
            keys: keys.clone(),
            children: (1..=7).map(PageId).collect(),
        },
    ];
    for expected in nodes {
        let mut bytes = bincode::serialize(&expected).unwrap();
        bytes.resize(MAX_PAYLOAD, 0);
        let actual = MaintenanceNode::decode(&bytes).unwrap();
        match (&expected, actual) {
            (
                BTreeNode::Leaf { entries, next_leaf },
                MaintenanceNode::Leaf { rows, count, next },
            ) => {
                assert_eq!(count, entries.len());
                assert_eq!(next, *next_leaf);
                let mut view = NodeBytes(rows);
                for (key, row) in entries {
                    let (borrowed_key, borrowed_value) = view.row().unwrap();
                    assert_eq!(
                        (borrowed_key, borrowed_value),
                        (key.as_slice(), row.0.as_deref())
                    );
                    assert!(
                        (bytes.as_ptr() as usize..bytes.as_ptr() as usize + bytes.len())
                            .contains(&(borrowed_key.as_ptr() as usize))
                    );
                }
                assert!(view.0.is_empty());
            }
            (
                BTreeNode::Internal { keys, children },
                MaintenanceNode::Internal {
                    keys: raw,
                    count,
                    children: child_bytes,
                },
            ) => {
                for bound in [b"".as_slice(), &[0], &[0, 1], &[255], &[255, 255]] {
                    let index = BPlusTree::child_index(keys, &bound.to_vec());
                    assert_eq!(
                        MaintenanceNode::child(raw, count, child_bytes, bound).unwrap(),
                        children[index]
                    );
                }
            }
            _ => panic!("borrowed node kind changed"),
        }
        // 分配次数放在隔离执行的旧新路径对照中衡量；
        // mem_probe 统计整个进程，也包含测试框架，不能在这里混合计数。
        let old: BTreeNode = bincode::deserialize(&bytes).unwrap();
        assert_eq!(
            bincode::serialize(&old).unwrap(),
            bincode::serialize(&expected).unwrap()
        );
    }
    for bad in [vec![], vec![0; 8], vec![255; 20]] {
        assert!(MaintenanceNode::decode(&bad).is_err());
    }
    let mut bad_option = bincode::serialize(&BTreeNode::Leaf {
        entries: vec![],
        next_leaf: None,
    })
    .unwrap();
    bad_option[12] = 2;
    assert!(MaintenanceNode::decode(&bad_option).is_err());
    let bad_order = BTreeNode::Internal {
        keys: vec![vec![2], vec![1]],
        children: vec![PageId(1), PageId(2), PageId(3)],
    };
    assert!(MaintenanceNode::decode(&bincode::serialize(&bad_order).unwrap()).is_err());
    let bad_children = BTreeNode::Internal {
        keys: vec![vec![1]],
        children: vec![PageId(1)],
    };
    assert!(MaintenanceNode::decode(&bincode::serialize(&bad_children).unwrap()).is_err());
}

#[test]
fn point_projection_matches_reference_and_avoids_leaf_materialization() {
    let (_scratch, mut tree) = anon(64, 64);
    for i in 0..30_u64 {
        assert!(tree.insert(i.to_be_bytes().to_vec(), RowId(Some(vec![i as u8; 32]))));
    }
    let mut costs = Vec::new();
    for old in [true, false] {
        tree.node_cache.clear();
        let _mode = old.then(point_projection_reference::Scope::start);
        let parses = node_parses();
        let (hit, allocations) = crate::mem_probe::count_thread_allocations(|| {
            tree.find_back(&0_u64.to_be_bytes(), &4_u64.to_be_bytes(), |_| true)
        });
        costs.push(allocations);
        assert_eq!(
            hit,
            Some((4_u64.to_be_bytes().to_vec(), RowId(Some(vec![4; 32]))))
        );
        assert_eq!(node_parses() - parses, u64::from(old));
    }
    assert!(
        costs[0] > costs[1] + 30,
        "reference must actually materialize the other leaf rows"
    );
    for i in 30..250_u64 {
        assert!(tree.insert(
            i.to_be_bytes().to_vec(),
            RowId(if i % 7 == 0 {
                None
            } else {
                Some(vec![i as u8; 32])
            })
        ));
    }
    for high in [0_u64, 1, 63, 120, 249, 300] {
        for reject in [false, true] {
            let mut results = Vec::new();
            for old in [true, false] {
                tree.node_cache.clear();
                let _mode = old.then(point_projection_reference::Scope::start);
                let mut seen = Vec::new();
                let hit = tree.find_back(&0_u64.to_be_bytes(), &high.to_be_bytes(), |k| {
                    seen.push(k.to_vec());
                    !reject || k[7] % 11 == 0
                });
                results.push((hit, seen));
            }
            assert_eq!(results[0], results[1]);
        }
    }
    let key = 120_u64.to_be_bytes().to_vec();
    assert!(tree.update(key.clone(), RowId(Some(vec![99]))));
    assert_eq!(
        tree.find_back(&key, &key, |_| true).unwrap().1,
        RowId(Some(vec![99]))
    );
    assert!(tree.delete(key.clone()));
    assert!(tree.find_back(&key, &key, |_| true).is_none());
    tree.try_flush_all().unwrap();
    let path = tree.page_manager.path().to_path_buf();
    drop(tree);
    let mut tree = BPlusTree::open(&path, 64, 64);
    assert!(tree.find_back(&key, &key, |_| true).is_none());
}

#[test]
fn rebalance_failed_borrow_reloads_unmodified_pages() {
    let mut outcomes = Vec::new();
    for old in [true, false] {
        let (_scratch, mut tree) = anon(8, 8);
        let left = tree.root;
        let right = tree.page_manager.allocate_page();
        let parent = tree.page_manager.allocate_page();
        let left_node = BTreeNode::Leaf {
            entries: vec![(vec![0], RowId(Some(vec![9; 3700])))],
            next_leaf: Some(right),
        };
        let right_node = BTreeNode::Leaf {
            entries: (1..=4)
                .map(|i| (vec![i], RowId(Some(vec![i; 500]))))
                .collect(),
            next_leaf: None,
        };
        let parent_node = BTreeNode::Internal {
            keys: vec![vec![1]],
            children: vec![left, right],
        };
        assert!(tree.write_node(left, &left_node));
        assert!(tree.write_node(right, &right_node));
        assert!(tree.write_node(parent, &parent_node));
        tree.set_root(parent);
        let mode = old.then(rebalance_read_reference::Scope::start);
        let before = rebalance_counts();
        assert!(!tree.handle_underflow(left, parent));
        let after = rebalance_counts();
        // 3700+500 字节无法借位到同一页；回退路径
        // 必须重新读取原始页，不能使用借位失败后已被局部修改的对象。
        assert_eq!(after.1 - before.1, if old { 2 } else { 1 });
        assert_eq!(after.0 - before.0, u64::from(!old));
        drop(mode);
        outcomes.push([left, right, parent].map(|p| tree.page_manager.read_page(p)));
        tree.try_flush_all().unwrap();
        let path = tree.page_manager.path().to_path_buf();
        drop(tree);
        let mut tree = BPlusTree::open(&path, 8, 8);
        for i in 0..=4 {
            assert_eq!(
                tree.get(vec![i]),
                Some(RowId(Some(vec![
                    if i == 0 { 9 } else { i };
                    if i == 0 { 3700 } else { 500 }
                ])))
            );
        }
    }
    assert_eq!(outcomes[0], outcomes[1]);
}

#[test]
fn delete_page_projection_preserves_dirty_reads_and_restructure() {
    let mut costs = Vec::new();
    for old in [true, false] {
        let (_scratch, mut tree) = anon(8, 4096);
        for i in 0..2000 {
            assert!(tree.insert(format!("{i:08}").into_bytes(), RowId(Some(vec![7; 16]))));
        }
        let mode = old.then(delete_read_reference::Scope::start);
        let (read, allocations) =
            crate::mem_probe::count_thread_allocations(|| tree.read_node(tree.root));
        costs.push(allocations);
        assert!(matches!(read, BTreeNode::Internal { .. }));
        let ((), min_allocations) = crate::mem_probe::count_thread_allocations(|| {
            assert_eq!(tree.subtree_min_key(tree.root), Some(b"00000000".to_vec()));
        });
        if !old {
            assert_eq!(
                min_allocations, 2,
                "one copied min key plus expected test key; no owned intermediate nodes"
            );
        }
        let victims: Vec<_> = (0..1900).map(|i| format!("{i:08}").into_bytes()).collect();
        assert_eq!(tree.delete_batch(&victims), victims.len());
        drop(mode);
        for i in 0..2000 {
            assert_eq!(
                tree.get(format!("{i:08}").into_bytes()).is_some(),
                i >= 1900
            );
        }
        tree.try_flush_all().unwrap();
        let path = tree.page_manager.path().to_path_buf();
        drop(tree);
        let mut tree = BPlusTree::open(&path, 8, 8);
        for i in 0..2000 {
            assert_eq!(
                tree.get(format!("{i:08}").into_bytes()).is_some(),
                i >= 1900
            );
        }
    }
    assert_eq!(
        costs[0],
        costs[1] + 1,
        "old read must allocate the extra raw payload"
    );
}

#[test]
fn test_insert_get_update_range() {
    let (_s, mut tree) = anon(4, 16);
    for n in [5u8, 10, 15, 20, 25, 30, 35] {
        tree.insert(key(n), rid(n));
    }

    assert_eq!(tree.get(key(10)).unwrap().0, Some(vec![10]));
    assert!(tree.get(key(99)).is_none());

    assert!(tree.update(key(10), rid(99)));
    assert_eq!(tree.get(key(10)).unwrap().0, Some(vec![99]));

    let range: Vec<u8> = tree
        .range_scan(key(5), key(20))
        .into_iter()
        .map(|(k, _)| k[0])
        .collect();
    assert_eq!(range, vec![5, 10, 15, 20]);
}

#[test]
fn test_delete_and_rebalance() {
    let (_s, mut tree) = anon(4, 32);
    // 插入足够多以触发多层
    let keys: Vec<u8> = (1..=30).collect();
    for &n in &keys {
        tree.insert(key(n), rid(n));
    }
    assert_eq!(collect_all(&mut tree), keys);

    // 删除中间键，触发借键/合并
    for n in [10u8, 11, 12, 5, 6, 7, 8, 9, 15, 16, 17, 18, 19, 20] {
        assert!(tree.delete(key(n)), "delete {n} should succeed");
        assert!(tree.get(key(n)).is_none(), "key {n} should be gone");
    }

    let remaining: Vec<u8> = (1..=30u8)
        .filter(|n| ![10, 11, 12, 5, 6, 7, 8, 9, 15, 16, 17, 18, 19, 20].contains(n))
        .collect();
    assert_eq!(collect_all(&mut tree), remaining);

    // 再删光
    for n in remaining {
        assert!(tree.delete(key(n)));
    }
    assert!(collect_all(&mut tree).is_empty());
}

#[test]
fn test_delete_missing_key() {
    let (_s, mut tree) = anon(4, 8);
    tree.insert(key(1), rid(1));
    assert!(!tree.delete(key(2)));
    assert!(tree.delete(key(1)));
    assert!(!tree.delete(key(1)));
}

#[test]
fn test_iter_order() {
    let (_s, mut tree) = anon(4, 16);
    for n in [35u8, 10, 25, 5, 30, 15, 20] {
        tree.insert(key(n), rid(n));
    }
    assert_eq!(collect_all(&mut tree), vec![5, 10, 15, 20, 25, 30, 35]);
    tree.flush_all();
}

/// 真实条目大小下的 delete：借键/合并不许动一页装不下的写，也不许丢行。
///
/// 旧实现**按条数**判断（`order/2` 对半、`min_entries` 比较），从不量序列化字节：
/// 40B 键 + 60B 值时两个半页合并 = 7.5 KB > 4080 ⇒ `write_page` 打印一句"数据超过页载荷
/// 限制"并返回 false，而调用链把那个 bool 丢了 ⇒ 幸存页保持**合并前**的内容，父节点却
/// 已经删掉分隔键并 `free_page(死页)` ⇒ 那批行同时失去两份副本 = 静默不可达。
/// 大库基线台架跑 bplus 时刷出的几万行那条消息，走的就是这里
/// （而且 eprintln 本身把被测耗时污染了 —— `search 首页` 911ms 里有它一份）。
#[test]
fn test_reorg_never_drops_oversized_pages() {
    const N: usize = 4000;
    // 台架同款形状：键 ≈ 35 B、值 60 B ⇒ order=64 的一页装不下 64 条
    fn big_key(i: usize) -> Key {
        format!("{i:08}#/worldquant/datafields/x.json").into_bytes()
    }
    fn big_rid() -> RowId {
        RowId(Some(vec![b'x'; 60]))
    }

    let (_s, mut tree) = anon(64, 256);
    for i in 0..N {
        assert!(tree.insert(big_key(i), big_rid()), "插入第 {i} 个键失败");
    }
    // 隔一个删一个 ⇒ 大量下溢，逼着走借键与合并
    for i in (0..N).step_by(2) {
        assert!(tree.delete(big_key(i)), "删除第 {i} 个键失败");
    }

    let oversized = crate::bplus_tree::page::oversized_writes();
    assert_eq!(oversized, 0, "一次超载写都不该尝试：重组前必须先量字节");

    // 剩下的键必须一个不少、全部读得回来（丢页的表现就是键突然不可达）
    let mut alive = 0usize;
    for i in (1..N).step_by(2) {
        assert!(
            tree.get(big_key(i)).is_some(),
            "第 {i} 个键在重组后读不回来了"
        );
        alive += 1;
    }
    assert_eq!(alive, (N + 1) / 2);
    assert_eq!(
        tree.iter().count(),
        alive,
        "叶子链表的行数必须等于存活键数（链表指向已释放页会在这里露出来）"
    );
}

/// 一条**比整页还大**的行：分裂切不出"两半都装得下"的分界 ⇒ 必须**一字不写并报告失败**，
/// 已有的行一条不少。老写法是"按条数对半切、两半都硬写"，写不进的那一半只留下一行日志，
/// 于是它变成**没人引用的页** = 静默丢数据（和 M11f 那个洞同一个形状，只是在插入这一侧）。
#[test]
fn test_oversized_row_is_refused_not_dropped_silently() {
    const N: usize = 300;
    fn k(i: usize) -> Key {
        format!("k{i:06}").into_bytes()
    }
    let (_s, mut tree) = anon(64, 512);
    for i in 0..N {
        assert!(
            tree.insert(k(i), RowId(Some(vec![b'v'; 60]))),
            "第 {i} 行没进去"
        );
    }
    let before = tree.iter().count();
    let drops_before = split_drops();
    // 谓词与分裂必须用同一套字节账：否则上层按 `row_fits` 放行、树却按另一套判据拒收
    assert!(
        !tree.row_fits(&k(N), &Some(vec![b'H'; 8 * 1024])),
        "8 KB 的行必须被 `row_fits` 判成装不下"
    );
    assert!(
        tree.row_fits(&k(N), &Some(vec![b'v'; 60])),
        "正常行不能被误判"
    );

    // 8 KB 比一页载荷(4080 B)还大 ⇒ 无论怎么切都有一侧装不下
    assert!(
        !tree.insert(k(N), RowId(Some(vec![b'H'; 8 * 1024]))),
        "装不下的行必须报告失败，不能回一句「成功」"
    );
    assert_eq!(
        split_drops() - drops_before,
        1,
        "要走的是「一行都没写」那条路，而不是别的失败"
    );
    assert_eq!(tree.iter().count(), before, "拒绝一行不许牵连已有的行");
    for i in 0..before {
        assert!(tree.get(k(i)).is_some(), "第 {i} 行在读的时候不见了");
    }
    assert_eq!(
        crate::bplus_tree::page::oversized_writes(),
        0,
        "连一次超载写都不该尝试：分裂前就要量得出两半"
    );
}

/// 重切的**分界**由字节公式选出，所以公式必须与真实序列化逐档相等。
///
/// 为什么单开一条：公式偏了，落盘前的 `node_fits` 只会让它退回
/// "什么都不动" ⇒ 数据一条不丢、测试全绿，但优化**静默地没发生**。这种失败只有对拍抓得到。
/// M34 把"逐条问 bincode"换成"逐条两次加法 + 一组探针常量" ⇒ 对拍必须额外覆盖
/// **同一页里键长不一、`RowId` 有 `None` 有 `Some`** 的形状 —— 线性假设只在这些地方会破。
#[test]
fn test_reorg_size_formula_matches_bincode() {
    fn item(i: usize, pad: usize) -> (Key, RowId) {
        (
            format!("k{i:06}{}", "#".repeat(pad)).into_bytes(),
            RowId(Some(vec![b'v'; 12 + pad])),
        )
    }
    for pad in [0usize, 7, 40, 120, 300] {
        let items: Vec<(Key, RowId)> = (0..50).map(|i| item(i, pad)).collect();
        for next_leaf in [None, Some(PageId(7))] {
            let node = BTreeNode::Leaf {
                entries: items.clone(),
                next_leaf,
            };
            let real = bincode::serialize(&node).unwrap().len() as i64;
            let formula = BPlusTree::leaf_base(next_leaf)
                + items.iter().map(BPlusTree::entry_bytes).sum::<i64>();
            assert_eq!(formula, real, "叶子字节公式不等于真实序列化（pad={pad}）");
        }

        let keys: Vec<Key> = (0..40).map(|i| item(i, pad).0).collect();
        let children: Vec<PageId> = (0..keys.len() + 1).map(|c| PageId(c as u64)).collect();
        let node = BTreeNode::Internal {
            keys: keys.clone(),
            children,
        };
        let real = bincode::serialize(&node).unwrap().len() as i64;
        let formula = BPlusTree::internal_base()
            + keys.iter().map(BPlusTree::key_bytes).sum::<i64>()
            + (keys.len() as i64 + 1) * BPlusTree::internal_child_unit();
        assert_eq!(
            formula, real,
            "内部节点字节公式不等于真实序列化（pad={pad}）"
        );
    }

    // 混合形状：键长每档不同 + 每三条有一条 `RowId(None)`
    let mixed: Vec<(Key, RowId)> = (0..60)
        .map(|i| {
            (
                format!("{:04}{}", i, "@".repeat(i % 17)).into_bytes(),
                if i % 3 == 0 {
                    RowId(None)
                } else {
                    RowId(Some(vec![b'v'; i % 29]))
                },
            )
        })
        .collect();
    for next_leaf in [None, Some(PageId(3))] {
        let node = BTreeNode::Leaf {
            entries: mixed.clone(),
            next_leaf,
        };
        let real = bincode::serialize(&node).unwrap().len() as i64;
        let formula =
            BPlusTree::leaf_base(next_leaf) + mixed.iter().map(BPlusTree::entry_bytes).sum::<i64>();
        assert_eq!(
            formula, real,
            "混合键长 + 空 RowId 的叶子公式与真实序列化不等"
        );
    }
    // 逐条单独对拍：常量的线性假设若错，这里就是第一处红（不必等到整页级）
    let vec_base = bincode::serialized_size(&Vec::<(Key, RowId)>::new()).unwrap() as i64;
    for e in &mixed {
        let probe = bincode::serialized_size(&[e][..]).unwrap() as i64 - vec_base;
        assert_eq!(
            BPlusTree::entry_bytes(e),
            probe,
            "单条字节与探针不等（键 {} 字节）",
            e.0.len()
        );
    }
}

/// 顺序灌行时**下降一趟都不许摸盘**：写完的页必须直接回填解析缓存（M34）。
///
/// 判据：灌 N 行期间 `page_reads()` 平均 < 0.1 次/行。这把尺子在两种回归下都会响：
/// ① 下降退回裸 `read_page` + 解析 ⇒ 实测 **2.94 次/行**（每层一次，M34 修前的读数）；
/// ② 走了缓存但**不回填** ⇒ 每行 ≥ 1 次（上一条自己刚写脏的那一页，下一条马上要用）。
/// 为什么不用"解析次数"当判据：退回 ① 的时候解析发生在 `node_of` 之外，那把尺子读到的是
/// **0** —— 一个"看起来更省"的空转读数，抓不到回归（M34 第一版就是被这个 0 骗了一次）。
///
/// 变异验证：删掉 `insert` 写回循环里那句 `cache_written` ⇒ 每行 1.00 次，红。
#[test]
fn test_sequential_insert_descends_without_touching_pages() {
    const N: usize = 8000;
    let (_s, mut tree) = anon(64, 4096);
    let k = |i: usize| -> Key { format!("{i:08}#/worldquant/datafields/x.json").into_bytes() };
    let r0 = crate::bplus_tree::page::page_reads();
    for i in 0..N {
        assert!(
            tree.insert(k(i), RowId(Some(vec![b'v'; 47]))),
            "插入第 {i} 行失败"
        );
    }
    let reads = crate::bplus_tree::page::page_reads() - r0;
    assert!(
        reads * 10 <= N as u64,
        "顺序灌行平均每行摸 {:.3} 次页 ⇒ 下降还在从页缓存之外取节点，或者写完的页没回填解析缓存\
             （N={N}，页读={reads}；修前实测 2.94 次/行）",
        reads as f64 / N as f64
    );
    // 上面那条只证明"没摸盘"，还得证明"数据真在里面"——缓存回填错了也会一个字节都不读。
    assert_eq!(tree.iter().count(), N, "行数一条不许少");
    for i in (0..N).step_by(317) {
        assert!(tree.get(k(i)).is_some(), "第 {i} 行读不回来");
    }
}

/// delete 密集之后**页数必须收回来到 rows/每页容量** —— 这条才是"合并后再分裂"的验收。
///
/// 旧结局是"union 装不下 ⇒ 整次作废"：行数一条不少、一条测试都不红，但按页走的调用
/// 一直是双倍代价（实测 `vacuum()` 206 → 466 ms）。把重切那一支临时改成"什么都不动"，
/// 这条会红（页数停在删之前的量级）。
#[test]
fn test_reorg_packs_leaves_instead_of_leaving_them_half_empty() {
    const N: usize = 4000;
    fn big_key(i: usize) -> Key {
        format!("{i:08}#/worldquant/datafields/x.json").into_bytes()
    }
    fn big_rid() -> RowId {
        RowId(Some(vec![b'x'; 60]))
    }

    let (_s, mut tree) = anon(64, 4096);
    for i in 0..N {
        assert!(tree.insert(big_key(i), big_rid()), "插入第 {i} 个键失败");
    }
    let (leaves_full, rows_full, ..) = tree.leaf_profile();
    assert_eq!(rows_full, N);

    for i in (0..N).step_by(2) {
        assert!(tree.delete(big_key(i)), "删除第 {i} 个键失败");
    }
    let (leaves, rows, min, max) = tree.leaf_profile();
    let alive = (N + 1) / 2;
    let item = |i: usize| (big_key(i), big_rid());
    assert_eq!(rows, alive, "行数一条不许少");
    // 删一半之后仍然可达 + 叶子链没断
    for i in (1..N).step_by(2) {
        assert!(
            tree.get(big_key(i)).is_some(),
            "第 {i} 个键在重组后读不回来了"
        );
    }
    assert_eq!(
        crate::bplus_tree::page::oversized_writes(),
        0,
        "重组前必须先量字节"
    );

    // 尺子必须问"一页**装得下**几条"，不能问插入时的观察值：插入路径按条数分裂，
    // 每页只填到 ~22 条（`maxf`），而这批键的真实字节容量是 ~35 条 —— 拿 22 当上界
    // 会把"重组其实更满"误判成异常。这里逐条加到装不下为止，直接量出容量。
    let cap = (1usize..)
        .take_while(|&k| {
            let probe = BTreeNode::Leaf {
                entries: (0..k).map(item).collect(),
                next_leaf: Some(PageId(999)),
            };
            BPlusTree::node_fits(&probe)
        })
        .last()
        .unwrap_or(1);
    let ideal = alive.div_ceil(cap);
    eprintln!(
        "[reorg] 每页字节容量 {cap} 条；满树 {leaves_full} 叶 / {rows_full} 行 ⇒ 删一半              {leaves} 叶 / {rows} 行（单页 {min}~{max} 条），理想页数 {ideal}（= rows/{cap}）"
    );
    assert!(
        leaves <= ideal + ideal / 2,
        "页数 {leaves} 超出理想 {ideal} 的 1.5× ⇒ 空位没被收掉，按页走的调用              （health / vacuum / 全量 scan）一直在双倍代价（单页 {min}~{max} 条）"
    );
}

/// 守卫（M54b）：删除回溯段只为"这棵子树的下界**可能**动了"去走那趟左下到底，
/// 而且每一层只读**一次**本页。
///
/// 四臂各红在哪（界都是从这两臂的实测数中间定的）：
/// - 改后的形状：0.163 趟刷新/每次删除、9.33 页/每次删除 ⇒ 绿；
/// - μA（退回改前的"无条件刷新"）：**2.372** 趟、**13.58** 页 ⇒ 两条界一起红（这一格随树高涨，
///   600k 档付的是磁盘）；
/// - μC（把"先 `read_page` 问一句还在不在、再 `read_node` 读一遍"的两遍读抄回来）：页读 **15.33** ⇒ 红；
/// - μB（**从不**刷新）：刷新数 0 ⇒ 只有空转哨兵红。**正确性扫抓不到它** —— 实测过：900 把键照样
///   全读得回。μE（借位/合并**不**向父层上报）**同样绿，而且两把尺子与改后一字不差（21 趟 / 9.33 页）
///   ⇒ 这个形状根本没触发过重组（order=12、只删 129/900 把键，叶子没下溢），**"重组也上报"这一半
///   还没被任何形状检验过** —— 下一轮要先造一个真会合并的形状，别把这一半读成"已经守住"。
///   这一格的"保守方向"只有**代价**证据、没有可达性证据：分隔键停在已被删掉的键上仍是合法分界
///   （左子树全 < 它 < 右子树剩下的键）。⇒ 判据只能挂在代价尺子上，不许把它读成"刷新是可达性守卫"。
///   扫自己不是装饰：μF（刷新时写一个越过右子树多数键的分隔键）在它这里红 —— 键走错子树，
///   连"删掉第 49 个键"都做不成。
#[test]
fn separator_refresh_only_happens_when_the_subtree_minimum_moves() {
    // order=12：叶子满约 11 条 ⇒ 900 键有 ~8 成在 3~4 层树上（两层的树每行最多刷 1 次，
    // 那时无条件/有条件两臂的差被树高吃掉了，界就量不出东西）
    const N: usize = 900;
    const BOUND: f64 = 1.0;
    const READ_BOUND: f64 = 12.0;
    fn k(n: usize) -> Key {
        format!("{n:08}").into_bytes()
    }
    fn r(n: usize) -> RowId {
        RowId(Some((n as u32).to_le_bytes().to_vec()))
    }

    let (_s, mut tree) = anon(12, 4096);
    for i in 0..N {
        assert!(tree.insert(k(i), r(i)), "插入第 {i} 个键失败");
    }
    // 先把页请进缓存，让下面量的是"结构上的重复刷新/重复读"而不是冷缓存
    for i in 0..N {
        assert!(tree.get(k(i)).is_some(), "第 {i} 个键写完读不回来");
    }

    sep_refreshes_clear();
    let reads0 = crate::bplus_tree::page::page_reads();
    let mut dels = 0usize;
    for i in (0..N).step_by(7) {
        assert!(tree.delete(k(i)), "删除第 {i} 个键失败");
        dels += 1;
    }
    let refreshes = sep_refreshes() as usize;
    let reads = crate::bplus_tree::page::page_reads() - reads0;
    let per_del = refreshes as f64 / dels as f64;
    let per_del_reads = reads as f64 / dels as f64;
    eprintln!(
        "[sep_refresh] {dels} 次删除 → {refreshes} 趟左下到底（{per_del:.3} 趟/次，界 {BOUND}）\
             · 页读 {reads}（{per_del_reads:.2} 页/次，界 {READ_BOUND}）"
    );

    // 尺子不能空转：删的是最左键/触发过合并的那些必须刷到
    assert!(
        refreshes > 0,
        "一趟分隔键刷新都没有 ⇒ 这条尺子在空转，界再低也守不住东西"
    );
    assert!(
        per_del <= BOUND,
        "每次删除走 {per_del:.3} 趟左下到底（界 {BOUND}）⇒ 回溯段又变成无条件刷新了：\
             删中间叶子的非最小键不可能改变子树下界"
    );
    assert!(
        per_del_reads <= READ_BOUND,
        "每次删除读 {per_del_reads:.2} 页（界 {READ_BOUND}）⇒ 回溯段又付起\"先问在不在、再读一遍\"的两遍读"
    );

    // 正确性扫：幸存键全部读得回来，删掉的键全部读不到（"从不刷新"在这里红）
    for i in 0..N {
        if i % 7 == 0 {
            assert!(tree.get(k(i)).is_none(), "第 {i} 个键已删，还能读到");
        } else {
            assert!(
                tree.get(k(i)).is_some(),
                "第 {i} 个幸存键读不回来 ⇒ 分隔键错了"
            );
        }
    }
}

/// M56 的拆段台架（`#[ignore]`，手动 release 单线程跑）：把"每删除行读 9.37 页"**按阶段拆开**
///
/// 两种删法必须一起读：M54 那份归因只给了一个总数。
/// - **每 3 把删 1 把**（稀疏）：叶子不会成片空 ⇒ 重组少，钱主要在下降 + 回溯；
/// - **成块删一半**：叶子成片空 ⇒ 借位/合并密集，看重组那只桶涨多少。
/// ③（同叶合批一次下降）只治"下降"那一段 ⇒ 哪一段占大头，决定下一刀切哪里。
///
/// ```text
/// BPLUS_DEL_ROWS=20000 cargo test --release --lib -- --ignored --nocapture \
///     --test-threads=1 bplus_delete_path_buckets
/// ```
#[test]
#[ignore = "bplus 删除路径按阶段拆段，手动 release 单线程跑：BPLUS_DEL_ROWS=20000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 bplus_delete_path_buckets"]
fn bplus_delete_path_buckets() {
    let rows: usize = std::env::var("BPLUS_DEL_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20_000);
    fn k(n: usize) -> Key {
        format!("{n:08}").into_bytes()
    }
    fn r(n: usize) -> RowId {
        RowId(Some((n as u64).to_le_bytes().to_vec()))
    }

    for (tag, step) in [("每3把删1把", 3usize), ("成块删一半", 2usize)] {
        let (_s, mut tree) = anon(64, 4096);
        for i in 0..rows {
            assert!(tree.insert(k(i), r(i)), "插入第 {i} 个键失败");
        }
        tree.flush_all();
        // 焐热：先全库点读一遍，让量具读的是"页访问次数"而不是冷缓存的读盘
        for i in 0..rows {
            assert!(tree.get(k(i)).is_some(), "第 {i} 个键写完读不回来");
        }

        let p0 = crate::bplus_tree::page::page_reads();
        let b0 = crate::bplus_tree::page::page_reads_by_phase();
        let m0 = crate::bplus_tree::page::cache_misses();
        let e0 = crate::bplus_tree::page::page_evictions();
        let (u0, bor0, mg0) = reorg_stats();
        let d0 = descent_lookups();
        let s0 = sep_refreshes();
        reorg_stats_clear();
        sep_refreshes_clear();

        let mut dels = 0usize;
        for i in (0..rows).step_by(step) {
            assert!(tree.delete(k(i)), "删除第 {i} 个键失败");
            dels += 1;
        }
        let d = dels as f64;
        let reads = crate::bplus_tree::page::page_reads() - p0;
        let b1 = crate::bplus_tree::page::page_reads_by_phase();
        let bk: Vec<u64> = (0..b1.len()).map(|i| b1[i] - b0[i]).collect();
        let (pu, pb, pbt, ps, pr, pg, prc) = (
            bk[0] as f64 / d,
            bk[1] as f64 / d,
            bk[2] as f64 / d,
            bk[3] as f64 / d,
            bk[4] as f64 / d,
            bk[5] as f64 / d,
            bk[6] as f64 / d,
        );
        let rc = root_checks();
        let miss = crate::bplus_tree::page::cache_misses() - m0;
        let evict = crate::bplus_tree::page::page_evictions() - e0;
        let (u, bor, m) = reorg_stats();
        let (u, bor, m) = (u - u0, bor - bor0, m - mg0);
        let descents = descent_lookups() - d0;
        let refreshes = sep_refreshes() - s0;
        let per_del = reads as f64 / d;
        let per_descent = reads as f64 / descents.max(1) as f64;
        eprintln!(
            "[del_buckets {tag}] rows={rows} 删 {dels} 行 · 页读 {reads}（{per_del:.2} 页/删除行）\
                 · 下降 {descents}（{:.2} 次/删除行）· {per_descent:.2} 页/次下降\n\
                 桶/删除行：其它 {pu:.2} 下降 {pb:.2} 回溯 {pbt:.2} 分隔键 {ps:.2} 重组 {pr:.2} 找祖父 {pg:.2} 问根 {prc:.2}\n\
                 重组：真下溢 {u}（{:.2} 次/删除行）借位 {bor} 合并 {m} 问根 {rc}（{:.2} 次/删除行）\
                 ｜ 分隔键刷新 {refreshes}（{:.3} 趟/删除行）｜ 缺页 {:.2} 换出 {:.2}",
            descents as f64 / d,
            u as f64 / d,
            rc as f64 / d,
            refreshes as f64 / d,
            miss as f64 / d,
            evict as f64 / d,
        );
        assert_eq!(
            bk.iter().sum::<u64>(),
            reads,
            "桶之和 != 页读 ⇒ 标注漏了一扇门，这一行读数全部作废"
        );
        // 只有"成块删一半"这一臂**要求**触发重组（证人）；稀疏那一臂读到 0 是**结论**不是失败 ——
        // 它正是在说"稀疏删几乎不重组，所以 ③ 省下的钱不在重组段"
        if step == 2 {
            assert!(
                bor + m > 0,
                "这一档（{tag}, rows={rows}）没触发过重组 ⇒ 证人失效，别把重组段读成\"不花钱\""
            );
        }
        for i in 0..rows {
            if i % step == 0 {
                assert!(tree.get(k(i)).is_none(), "第 {i} 个键已删还能读到");
            } else {
                assert!(tree.get(k(i)).is_some(), "第 {i} 个幸存键读不回来");
            }
        }
    }
}
