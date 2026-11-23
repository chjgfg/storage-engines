// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn l0_candidate_order_uses_stack_and_keeps_source_tie_order() {
    let dir = tmp_dir("l0_candidate_order");
    let mut db = LSMTree::open(&dir).unwrap();
    for key in [b"c".as_slice(), b"a", b"b", b"b"] {
        db.inject_sst_for_test(
            0,
            vec![(key.to_vec(), StoredEntry::put(1, Some(key.to_vec())))],
        )
        .unwrap();
    }
    let files = db.l0_for_test();
    let (order, allocations) =
        crate::mem_probe::count_thread_allocations(|| L0CandidateOrder::by_max_key(files));
    assert_eq!(
        allocations, 0,
        "ordinary L0 candidate ordering must not allocate"
    );
    assert_eq!(
        order.as_slice(),
        &[0, 3, 2, 1],
        "same-key source priority must be unchanged"
    );
}

/// M81 第 1 项 · **计数**判据：首页（`to = None`）拉过的源数与块读**不随 L1+ 篇数涨**。
///
/// 尺子 = `merge_activations()`（归并真的拉过几个源的首键）＋ `block_reads()`。
/// 为什么不用墙钟：本机同档两趟噪声实测 2.6×，而"首页付几篇的块读"是精确工作量。
/// 对照 = 同一棵树同一范围再走一遍 `activate_all_for_test()`（= M81 之前的旧形状）：
/// 它的激活数恒等于源数，所以随篇数线性涨 ⇒ `eager` 那一侧的"必须涨"断言就是这把尺子的
/// 阳性对照（把它摘掉，这个测试会因为尺子不咬而红，不会假绿）。
#[test]
fn test_lazy_source_activation_first_page_does_not_scale_with_files() {
    use crate::lsm_tree::sst::block_reads;
    const FIRST_PAGE: usize = 20;
    let mut lazy: Vec<(u64, u64)> = Vec::new();
    let mut eager: Vec<(u64, u64)> = Vec::new();
    for count in [8usize, 64, 256] {
        let dir = tmp_dir(&format!("lazy_sources_scale_{count}"));
        {
            let mut db = LSMTree::open_with_config(&dir, pruning_options()).unwrap();
            db.bulk_mode = true;
            // 每篇 4 把键、键区间互不重叠（L1 的常态），min_key 随篇号递增。
            for file in 0..count {
                let entries = (0..4)
                    .map(|i| {
                        let n = (file * 4 + i) as u64;
                        (
                            version_key(n),
                            StoredEntry::put(1, Some(n.to_be_bytes().to_vec())),
                        )
                    })
                    .collect();
                db.inject_sst_for_test(1, entries).unwrap();
            }
            assert!(db.level_ranges_ordered[1]);

            let a0 = merge_activations();
            let b0 = block_reads();
            let got: Vec<_> = db
                .light_iter_range(None, None, u64::MAX)
                .take(FIRST_PAGE)
                .collect();
            lazy.push((merge_activations() - a0, block_reads() - b0));

            let a1 = merge_activations();
            let b1 = block_reads();
            let mut m = db.light_iter_range(None, None, u64::MAX);
            m.activate_all_for_test();
            let want: Vec<_> = m.take(FIRST_PAGE).collect();
            eager.push((merge_activations() - a1, block_reads() - b1));

            assert_eq!(got.len(), FIRST_PAGE, "{count} 篇：首页凑不满 ⇒ 规模没到位");
            assert_eq!(
                got, want,
                "{count} 篇：延迟激活交出的首页与 eager 逐字节不同"
            );
        }
        let _ = fs::remove_dir_all(&dir);
    }
    assert_eq!(
        lazy[0], lazy[2],
        "首页代价随篇数增长（激活数, 块读）：8 篇 {:?} → 256 篇 {:?} ⇒ 延迟激活没生效",
        lazy[0], lazy[2]
    );
    assert_eq!(
        eager[0].0, 8,
        "对照组（eager 旧形状）首页应当正好拉过 8 个源（= 篇数）"
    );
    assert_eq!(eager[1].0, 64, "eager 那一列必须是篇数：{eager:?}");
    assert_eq!(eager[2].0, 256, "eager 那一列必须是篇数：{eager:?}");
    assert!(
        lazy[2].0 <= 6,
        "256 篇里首页为了凑 {FIRST_PAGE} 把键拉了 {} 个源（每篇 4 把键 ⇒ 5 篇就该够）\
             ⇒ 源级延迟没生效（旧形状是 256）",
        lazy[2].0
    );
    assert!(
        lazy[2].0 < eager[2].0,
        "延迟激活没有减少源激活数：lazy {lazy:?} vs eager {eager:?}"
    );
    assert!(
        lazy[0].0 > 0,
        "lazy 一个源都没激活 ⇒ 首页根本没输出，前面的逐字节对拍是空的"
    );
    eprintln!(
        "[延迟激活·首页 {FIRST_PAGE} 键] (激活源数, 块读) 8 篇 {lazy:?} → 256 篇不变；\
             eager 旧形状 {eager:?}（激活数 = 篇数，随库线性涨）"
    );
}

/// M81 第 1 项 · **结果**判据：四种形状下 lazy 与 eager 必须逐字节相同。
///
/// 覆盖的是复核点名的四件事：同键跨源（两个 L0 文件里同一个键、seq 谁大都试过）、
/// 墓碑（`Drop` 参与定论）、快照过滤（`snap_seq` 卡在中间 ⇒ 真首键 > `min_key`，判据⑤）、
/// 全库输出与中途收手（`take(2)`）各自对拍。
/// 重量折叠 vs 轻量归并的等价由 `test_fold_agrees_with_point_read_on_overlapping_level`
/// 等既有守卫守着，这里只答"改了激活时机之后输出变了没有"。
#[test]
fn test_lazy_source_activation_agrees_with_eager_everywhere() {
    let dir = tmp_dir("lazy_sources_differential");
    {
        let mut db = LSMTree::open_with_config(&dir, pruning_options()).unwrap();
        db.bulk_mode = true;
        // L0：两篇范围重叠的文件 ⇒ 同键跨源 + 只有 seq 能定胜负
        db.inject_sst_for_test(
            0,
            vec![
                (version_key(0), StoredEntry::put(1, Some(b"a1".to_vec()))),
                (version_key(2), StoredEntry::put(1, Some(b"c1".to_vec()))),
                (version_key(4), StoredEntry::drop_at(2)),
            ],
        )
        .unwrap();
        db.inject_sst_for_test(
            0,
            vec![
                (version_key(0), StoredEntry::put(2, Some(b"a2".to_vec()))),
                (version_key(2), StoredEntry::put(3, Some(b"c3".to_vec()))),
                (version_key(3), StoredEntry::put(1, Some(b"d1".to_vec()))),
            ],
        )
        .unwrap();
        // L1：不重叠的多篇 ⇒ 下界排队真正生效的形状；其中一篇最小键只在中段可见（判据⑤）
        for entries in [
            vec![
                (version_key(1), StoredEntry::put(1, Some(b"b1".to_vec()))),
                (version_key(5), StoredEntry::put(4, Some(b"e4".to_vec()))),
            ],
            vec![
                (version_key(2), StoredEntry::put(5, Some(b"c5".to_vec()))),
                (version_key(6), StoredEntry::put(1, Some(b"f1".to_vec()))),
            ],
            vec![(version_key(7), StoredEntry::put(9, Some(b"g9".to_vec())))],
        ] {
            db.inject_sst_for_test(1, entries).unwrap();
        }
        // 相接端点（方案二审点名的形状）：`max_key[i] == min_key[i+1]` ⇒ 同一把键住在两篇里，
        // 后一篇那版 seq 更新。判据写成严格"<"就在这一瞬不触发 ⇒ 吐旧值 + 到更大键再吐一遍。
        db.inject_sst_for_test(
            1,
            vec![
                (version_key(20), StoredEntry::put(1, Some(b"h1".to_vec()))),
                (version_key(25), StoredEntry::put(1, Some(b"i1".to_vec()))),
            ],
        )
        .unwrap();
        db.inject_sst_for_test(
            1,
            vec![
                (version_key(25), StoredEntry::put(7, Some(b"i7".to_vec()))),
                (version_key(26), StoredEntry::drop_at(7)),
            ],
        )
        .unwrap();
        // memtable 也放两把：驻留源和当场激活的源混排时的顺序
        db.put(version_key(8), b"mem8".to_vec());
        db.put(version_key(3), b"mem3".to_vec());

        let mut cases = 0usize;
        for snap in [1u64, 2, 3, 5, 8, u64::MAX] {
            for (from, to) in [
                (None, None),
                (Some(version_key(2)), None),
                (None, Some(version_key(4))),
                (Some(version_key(1)), Some(version_key(5))),
                (Some(version_key(7)), Some(version_key(7))),
                (Some(version_key(9)), None),
            ] {
                let f = from.as_deref();
                let t = to.as_deref();
                let mut m = db.light_iter_range(f, t, snap);
                m.activate_all_for_test();
                let eager: Vec<_> = m.collect();
                let lazy: Vec<_> = db.light_iter_range(f, t, snap).collect();
                assert_eq!(
                    lazy, eager,
                    "snap={snap} 窗口={from:?}..{to:?}：全库输出与 eager 不同"
                );
                let lazy_head: Vec<_> = db.light_iter_range(f, t, snap).take(2).collect();
                let mut m2 = db.light_iter_range(f, t, snap);
                m2.activate_all_for_test();
                assert_eq!(
                    lazy_head,
                    m2.take(2).collect::<Vec<_>>(),
                    "snap={snap} 窗口={from:?}..{to:?}：中途收手的前两把键与 eager 不同"
                );
                cases += 1;
            }
        }
        assert!(
            cases >= 30,
            "对拍用例太少（{cases}）⇒ 这一格可能根本没跑到多种形状"
        );
        // 相接端点单独点名（不靠 eager 对拍，独立判一次）：键 25 只能出现一次、
        // 胜者必须是后一篇的 seq 7，且整份输出严格升序（乱序 = 未激活源的键在更大键之后补吐）。
        let all: Vec<_> = db.light_iter_range(None, None, u64::MAX).collect();
        let at25: Vec<_> = all.iter().filter(|(k, _)| *k == version_key(25)).collect();
        assert_eq!(at25.len(), 1, "相接端点那把键被吐了 {} 次", at25.len());
        assert_eq!(
            at25[0].1.seq, 7,
            "相接端点吐的是旧值（seq {}）",
            at25[0].1.seq
        );
        assert!(
            all.windows(2).all(|w| w[0].0 < w[1].0),
            "全库输出非严格升序 ⇒ 延迟激活漏了顺序"
        );
        // 至少证明"延迟"在这些形状里真的动过：全库首页在 snap 卡中段时不该激活全部源
        let a0 = merge_activations();
        let _: Vec<_> = db
            .light_iter_range(None, Some(&version_key(1)), u64::MAX)
            .take(1)
            .collect();
        assert!(merge_activations() - a0 > 0, "一次都没激活 ⇒ 尺子被架空");
    }
    let _ = fs::remove_dir_all(&dir);
}

/// M41a：`flush()` 必须把 memtable **搬**进 SST，一条都不许复制值。
///
/// 旧形状 `mem_to_sorted_entries(&imm)` 先把整张 memtable 连键带值克隆进一个 `Vec` 再
/// `sort_by` —— 那份暂存在 600k 档就是 flush 段实测斜率里的 ~0.6 KB/条。尺子 =
/// `EntryValue` 的复制次数（`sst::entry_value_clones`，手写 `Clone` 就为记这一笔）：
/// 一次 flush 之后必须是 **0**。
///
/// ⚠️ 两条防自空转的断言：flush 真发生了（`mem_entries` 归零、SST 至少多出一个），
/// 以及数据还读得回来。
///
/// 变异验证：把 `flush_one_immutable` 里那两个 `for` 换回
/// `for (k, se) in Self::mem_to_sorted_entries(&imm)` ⇒ 尺子变成 memtable 的条数，红。
#[test]
fn test_flush_moves_memtable_entries_without_cloning_them() {
    use crate::lsm_tree::sst::entry_value_clones;
    let dir = tmp_dir("flush_no_clone");
    {
        let mut db = LSMTree::open(&dir).unwrap();
        // 载荷远不到 4 MB / 64k 条这两档 ⇒ 这 5000 条一直躺在 memtable 里，
        // 下面那次 flush 才是"搬多少条"的那一次
        for i in 0..5000usize {
            db.put(format!("k{i:05}").into_bytes(), b"v".to_vec());
        }
        assert_eq!(
            db.stats().mem_entries,
            5000,
            "写路自己先 flush 了 ⇒ 这次量的不是 flush"
        );
        let sst0 = db.stats().total_sst;
        let base = entry_value_clones();
        db.flush().unwrap();
        let copied = entry_value_clones() - base;
        assert_eq!(
            db.stats().mem_entries,
            0,
            "flush 没把 memtable 清空 ⇒ 形状断言不成立"
        );
        assert!(
            db.stats().total_sst > sst0,
            "flush 没产出 SST ⇒ 这条判据量不到任何东西（空转）"
        );
        assert_eq!(
            copied, 0,
            "flush 复制了 {copied} 条值 ⇒ 又变回'整张 memtable 克隆进 Vec 再 sort'那个形状"
        );
        // 语义还得对：搬过去的行读得回来。**首/中/尾三把**都读 —— 那次 `sort_by` 没了之后，
        // SST 稀疏索引靠的就是 `MemMap` 两级升序这件事，读错一段当场红
        for k in [
            b"k00000".as_slice(),
            b"k02500".as_slice(),
            b"k04999".as_slice(),
        ] {
            assert_eq!(db.get(k), Some(b"v".to_vec()), "搬进 SST 之后读不回来");
        }
    }
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_drop_hides_old_put() {
    let dir = tmp_dir("drop");
    {
        let mut db = LSMTree::open_with_options(&dir, 100, 10).unwrap();
        db.insert(b"k".to_vec(), Some(b"v".to_vec()));
        db.flush().unwrap();
        assert!(db.remove(b"k"));
        assert_eq!(db.get(b"k"), None);
        db.flush().unwrap();
    }
    {
        let db = LSMTree::open(&dir).unwrap();
        assert_eq!(db.get(b"k"), None);
    }
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_mmap_roundtrip() {
    let dir = tmp_dir("mmap");
    let mut opts = LSMTreeOptions::default();
    opts.use_mmap = true;
    opts.enable_mem_wal = false;
    let mut db = LSMTree::open_with_config(&dir, opts).unwrap();
    db.put(b"m".to_vec(), b"map".to_vec());
    db.flush().unwrap();
    drop(db);
    let mut opts = LSMTreeOptions::default();
    opts.use_mmap = true;
    let db = LSMTree::open_with_config(&dir, opts).unwrap();
    assert_eq!(db.get(b"m"), Some(b"map".to_vec()));
    let _ = fs::remove_dir_all(&dir);
}

/// 折叠的答案必须**逐键等于**跨源点查的答案（M26 的主判据）。
///
/// 造的正是以前会给出错误答案的形状：L1 里两个范围重叠的文件，**排序在前的那个装着更旧的一份**
/// —— 同层文件谁新谁旧没有元数据可依据（L1→L2 压实会用**新 id 写老数据**，层内又只按
/// `min_key` 排序）。旧判据"源序号小者赢"在这种形状上会把旧版当胜者交出去：
/// 重量折叠 `range_scan` 与轻量折叠 `light_iter_range` 都交旧值，而点查 `get` 取 max seq
/// 交新值 ⇒ 同一个库两种读法两个答案。改成 seq 优先之后三者必须一致。
#[test]
fn test_fold_agrees_with_point_read_on_overlapping_level() {
    fn key(i: usize) -> Vec<u8> {
        format!("k{i:02}").into_bytes()
    }
    let dir = tmp_dir("fold_vs_point");
    {
        let mut db = LSMTree::open(&dir).unwrap();
        let a: Vec<(Vec<u8>, StoredEntry)> = (0..30)
            .map(|i| (key(i), StoredEntry::put(1, Some(b"old".to_vec()))))
            .collect();
        let b: Vec<(Vec<u8>, StoredEntry)> = (10..40)
            .map(|i| (key(i), StoredEntry::put(5, Some(b"new".to_vec()))))
            .collect();
        db.inject_sst_for_test(1, a).unwrap();
        db.inject_sst_for_test(1, b).unwrap();
        assert_eq!(db.stats().level_files[1], 2, "重叠形状没造出来");

        // 点查 = 判据的基准（`get_entry_at`：所有源里取最大可见 seq）
        for i in 0..10 {
            assert_eq!(
                db.get(&key(i)),
                Some(b"old".to_vec()),
                "k{i:02} 只住在旧文件里"
            );
        }
        for i in 10..30 {
            assert_eq!(
                db.get(&key(i)),
                Some(b"new".to_vec()),
                "重叠区 k{i:02} 点查没拿到新版"
            );
        }
        for i in 30..40 {
            assert_eq!(
                db.get(&key(i)),
                Some(b"new".to_vec()),
                "k{i:02} 只住在新文件里"
            );
        }

        // 重量折叠：每个键的答案必须和点查一字不差
        let rows = db.range_scan(Vec::new(), vec![0xffu8; 8]);
        assert_eq!(rows.len(), 40, "折叠把同一把键吐了两遍（去重坏了）");
        for (k, v) in &rows {
            assert_eq!(
                v.as_deref(),
                db.get(k).as_deref(),
                "折叠与点查不一致：{:?}",
                String::from_utf8_lossy(k)
            );
        }

        // 轻量折叠（翻页 / 窗口搜索 / 有界回收走的就是这条）：比的也是 seq
        let light: Vec<(Vec<u8>, u64)> = db
            .light_iter_range(None, None, u64::MAX)
            .map(|(k, le)| (k, le.seq))
            .collect();
        assert_eq!(light.len(), 40, "轻量折叠的键数");
        for (k, seq) in &light {
            let want = if k.as_slice() < key(10).as_slice() {
                1
            } else {
                5
            };
            assert_eq!(
                *seq,
                want,
                "轻量折叠在 {:?} 上选错了版本（seq {seq}，点查会给 {want}）",
                String::from_utf8_lossy(k)
            );
        }
        db.flush().unwrap();
    }
    let _ = fs::remove_dir_all(&dir);
}
