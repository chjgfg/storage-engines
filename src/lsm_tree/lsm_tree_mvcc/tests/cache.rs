// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// 块缓存自动档：容量跟着库长，而不是让人盯着块数过日子（M23）。
///
/// 归因出自下面那条 `LSM_POINT_ROWS` 台架：200k 键那一档乱序点读 220.9 µs/次，块/次 6.24、
/// **未命中/次 3.42**；同样 6.24 块/次 的**有序**读只要 21.7 µs（未命中 0.11）
/// ⇒ 随规模涨的那一段是"未命中的随机读"（一次 ≈ 60 µs），不是条目比较，也不是文件数本身。
/// 而容量固定 256 槽在 ~1.6 万行之后就不再装得下工作集 —— 于是点读单价随库长，且没人能
/// 提前算出该设多少。
///
/// 钉四件事：① 库比地板小 ⇒ 容量停在地板（不凭空开槽）② 库长在中间 ⇒ 容量 = 实际块数
/// ③ 超过天花板 ⇒ 封顶（内存是有价的，不是想花多少花多少）④ 装得下 ⇒ 第二趟乱序重读
/// **0 未命中**；把自动档关掉、钉回地板，同一趟读**必须**重新出现未命中 —— 最后这半句是
/// 尺子自检，没有它这条测试可以永远绿。
#[test]
fn test_block_cache_capacity_follows_the_library() {
    const BLOCK_ENTRIES: usize = 8; // 故意把块做小：让"块数"在测试里廉价地涨起来
    const FLOOR: usize = 4;
    const CEIL: usize = 16;

    fn opts(ceil: usize) -> LSMTreeOptions {
        LSMTreeOptions {
            sst_block_entries: BLOCK_ENTRIES,
            block_cache_blocks: FLOOR,
            block_cache_auto_max: ceil,
            ..LSMTreeOptions::for_mvcc()
        }
    }

    let path = tmp_db("cache_auto_tier");
    let keys: Vec<Vec<u8>> = (0..200).map(|i| format!("k{i:04}").into_bytes()).collect();
    {
        let mvcc = MVCC::try_open_with(&path, Some(opts(CEIL))).unwrap();
        let write = |range: std::ops::Range<usize>| {
            let items: Vec<(Vec<u8>, Vec<u8>)> = range
                .map(|i| (keys[i].clone(), format!("v{i}").into_bytes()))
                .collect();
            let tx = mvcc.begin_transaction();
            if let Err(bad) = tx.batch_set(&items) {
                panic!("batch_set 冲突: {}", String::from_utf8_lossy(&bad));
            }
            tx.commit();
            mvcc.flush();
        };
        let shape = || {
            let kv = mvcc.kv.lock().unwrap();
            (kv.sst_blocks_for_test(), kv.block_cache_capacity())
        };

        // ① 一小撮键 ⇒ 块数远低于地板 ⇒ 容量就该停在地板
        write(0..3);
        let (blocks_small, cap_small) = shape();
        assert!(
            blocks_small < FLOOR,
            "形状没造出来：{blocks_small} 块不算小"
        );
        assert_eq!(cap_small, FLOOR, "库比地板小 ⇒ 容量该停在地板");

        // ② 长到地板与天花板之间 ⇒ 容量 = 实际块数
        write(3..100);
        let (blocks, cap) = shape();
        assert!(
            blocks > FLOOR && blocks < CEIL,
            "形状没造出来：块数 {blocks} 不在 ({FLOOR},{CEIL}) 里"
        );
        assert_eq!(cap, blocks, "容量该跟着库长（块数 {blocks}，实测 {cap}）");

        // ④ 全都装得下 ⇒ 第二趟乱序重读不该有未命中
        // ⚠️ 读的顺序只覆盖**此刻已经写进去的** 100 把键（后面 100 把是拿来把块数顶过天花板的）
        let mut order: Vec<usize> = (0..100).collect();
        let mut s = 0x853c_49e6_77b9_5b3eu64;
        for i in (1..order.len()).rev() {
            s = s
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let j = (s >> 33) as usize % (i + 1);
            order.swap(i, j);
        }
        let tx = mvcc.begin_transaction();
        for &i in &order {
            assert!(tx.get(&keys[i]).is_some(), "读第 {i} 把键落空");
        }
        let m0 = crate::lsm_tree::sst::block_misses();
        for &i in &order {
            assert!(tx.get(&keys[i]).is_some());
        }
        let warm_misses = crate::lsm_tree::sst::block_misses() - m0;
        drop(tx);
        assert_eq!(
            warm_misses, 0,
            "块数 {blocks} ≤ 容量 {cap} 时，第二趟重读不该有未命中（实测 {warm_misses}）"
        );

        // ③ 再长过天花板 ⇒ 封顶，一槽不多占
        write(100..200);
        let (blocks_big, cap_big) = shape();
        assert!(
            blocks_big > CEIL,
            "形状没造出来：{blocks_big} 块没超过天花板 {CEIL}"
        );
        assert_eq!(
            cap_big, CEIL,
            "容量必须封顶（块数 {blocks_big}，实测 {cap_big}）"
        );

        // ④ 的对照 ⇒ 关掉自动档就回到"配置值即生效值"，同一趟读必须有未命中
        mvcc.set_options(opts(0));
        let (_, cap_pinned) = shape();
        assert_eq!(cap_pinned, FLOOR, "auto_max=0 就该钉死配置值");
        let tx = mvcc.begin_transaction();
        let m0 = crate::lsm_tree::sst::block_misses();
        for &i in order.iter().take(100) {
            assert!(tx.get(&keys[i]).is_some());
        }
        let cold_misses = crate::lsm_tree::sst::block_misses() - m0;
        drop(tx);
        assert!(
            cold_misses > 0,
            "{FLOOR} 槽装 {blocks_big} 块，重读居然一次未命中都没有 ⇒ 这把尺子在空转"
        );
        eprintln!(
            "[块缓存自动档] 地板 {FLOOR} / 天花板 {CEIL}：小块库容量 {cap_small}，\
                 {blocks} 块时容量 {cap}（第二趟未命中 {warm_misses}），{blocks_big} 块时容量 {cap_big}；\
                 关掉自动档钉回 {FLOOR} 槽 ⇒ 同一趟未命中 {cold_misses}"
        );

        // 生效值得看得见：运维调完要知道现在到底是多少
        let t = mvcc.tuning();
        assert_eq!(t.get("block_cache_auto_max"), Some("0"), "{t}");
        assert_eq!(t.get("block_cache_capacity"), Some("4"), "{t}");
    }
    cleanup(&path);
}

/// 天花板本身可以抬，而且**分母要看得见**（M32）。
///
/// 为什么单独一条：`for_mvcc` 的默认上限 1024 块 ≈ 7.1 MB 是**预算**，可 600k 键那档的块数
/// 远不止 1024 ⇒ 覆盖率掉下来，每次点读多付一次随机读（实测 ≈54~94 µs）。默认值不该替调用方
/// 决定"要不要把库复制进 RAM"，但**要能让他决定**：`set_cache_auto_max(usize::MAX)` = 不设顶。
///
/// 钉四件事：⓪ 封顶时**焐热一趟之后仍然缺块**（26 块 / 容量 8 实测一轮缺 26 —— 冷启第一趟
/// 谁都要缺，拿它当"顶绑住了"的证据等于什么都没证，与 bplus 那条守卫同一个口径）
/// ① 抬顶之后容量 = 实际块数（不是"配了没生效"）② `tuning()` 同时给出分子
/// (`block_cache_capacity`) 与分母 (`sst_blocks`)，否则运维看到 1024 分不清"撞上顶"还是"库就这么小"
/// ③ 抬顶**不重建缓存** ⇒ 之前焐热的块还在（M23 的教训：自动档自己清自己的缓存）。
///
/// 变异对照（与 bplus 那两条一起跑的）：A「只记值、不重估容量」⇒ 红在 ① 的
/// `cap_after == blocks`（left 8 / right 26）；B「重建缓存」⇒ 红在 ③ 的"住着几块"
/// （left 0 / right 8）。两条都**不会**被门面的 `cache_auto_max_knob_…` 抓到，效应归引擎。
#[test]
fn test_block_cache_auto_max_is_raisable_and_coverage_is_readable() {
    const BLOCK_ENTRIES: usize = 8;
    const FLOOR: usize = 4;
    // 200 把键、每块 8 条 ⇒ ≈26 块。顶给 8（不是 16）才够"被明显甩开"——
    // 形状没造出来的守卫等于没写。
    const CEIL: usize = 8;
    let path = tmp_db("cache_auto_raise");
    let keys: Vec<Vec<u8>> = (0..200).map(|i| format!("k{i:04}").into_bytes()).collect();
    let opts = LSMTreeOptions {
        sst_block_entries: BLOCK_ENTRIES,
        block_cache_blocks: FLOOR,
        block_cache_auto_max: CEIL,
        ..LSMTreeOptions::for_mvcc()
    };
    {
        let mvcc = MVCC::try_open_with(&path, Some(opts)).unwrap();
        for base in (0..200).step_by(20) {
            let end = (base + 20).min(200);
            let items: Vec<(Vec<u8>, Vec<u8>)> = (base..end)
                .map(|i| (keys[i].clone(), format!("v{i}").into_bytes()))
                .collect();
            let tx = mvcc.begin_transaction();
            if let Err(bad) = tx.batch_set(&items) {
                panic!("batch_set 冲突: {}", String::from_utf8_lossy(&bad));
            }
            tx.commit();
            mvcc.flush();
        }
        let shape = || {
            let kv = mvcc.kv.lock().unwrap();
            (kv.sst_blocks_for_test(), kv.block_cache_capacity())
        };
        let (blocks, cap) = shape();
        assert!(
            blocks > CEIL * 2,
            "形状没造出来：{blocks} 块没把天花板 {CEIL} 甩开"
        );
        assert_eq!(
            cap, CEIL,
            "抬顶之前容量必须封顶（块数 {blocks}，实测 {cap}）"
        );

        // 先在封顶的状态下焐热：容量 8 而库有 26 块 ⇒ 缓存里住着东西，但装不下全部
        let tx = mvcc.begin_transaction();
        for k in &keys {
            assert!(
                tx.get(k).is_some(),
                "读 {} 落空",
                String::from_utf8_lossy(k)
            );
        }
        // ⚠️ 焐热一趟**之后**再量缺页：冷启第一趟谁都要缺，拿它当"顶绑住了"的证据等于什么都没证
        let capped0 = crate::lsm_tree::sst::block_misses();
        for k in &keys {
            assert!(tx.get(k).is_some());
        }
        let capped_misses = crate::lsm_tree::sst::block_misses() - capped0;
        drop(tx);
        assert!(
            capped_misses > 0,
            "容量 {cap} / 全库 {blocks} 块，焐热后第二趟一次都不缺 ⇒ 这个顶没绑住，后面全是空转"
        );
        let resident_before = mvcc.kv.lock().unwrap().cached_blocks_for_test();
        assert!(
            resident_before > 0,
            "形状没造出来：焐过一轮之后缓存里一块都没有 ⇒ 下面那条「没重建」的断言会空转"
        );

        mvcc.set_cache_auto_max(usize::MAX);
        let (blocks2, cap_after) = shape();
        assert_eq!(blocks2, blocks, "抬顶不该改动库的形状");
        assert_eq!(
            cap_after, blocks,
            "顶抬到不设限 ⇒ 容量必须等于实际块数（块数 {blocks}，实测 {cap_after}）"
        );
        assert_eq!(mvcc.cache_auto_max(), usize::MAX, "上限要能读回来");
        // M23 的纪律：**改容量是就地改上限，不许重建**（重建 = 调一次旋钮就把焐热的块全丢掉）
        let resident_after = mvcc.kv.lock().unwrap().cached_blocks_for_test();
        assert_eq!(
            resident_after, resident_before,
            "抬顶把缓存从 {resident_before} 块变成 {resident_after} 块 ⇒ 又走回「重建缓存」那条路"
        );

        // 再跑两趟：第一趟把新容量填满，第二趟才该 0 未命中。
        // ⚠️ 别在抬顶后**立刻**要求 0 未命中：封顶那会儿只装得下 8/26 块，缺的那 18 块是
        //   应有账（第一版这条就是写出假红来的地方）。
        let tx = mvcc.begin_transaction();
        for k in &keys {
            assert!(tx.get(k).is_some());
        }
        let m0 = crate::lsm_tree::sst::block_misses();
        for k in &keys {
            assert!(tx.get(k).is_some());
        }
        let misses = crate::lsm_tree::sst::block_misses() - m0;
        drop(tx);
        assert_eq!(
            misses, 0,
            "容量 = 块数 {blocks} 之后第二趟仍缺 {misses} 块 ⇒ 抬上来的容量没真的生效"
        );

        let t = mvcc.tuning();
        assert_eq!(
            t.get("sst_blocks"),
            Some(blocks.to_string().as_str()),
            "分母必须看得见，否则运维算不出覆盖率：{t}"
        );
        assert_eq!(
            t.get("block_cache_capacity"),
            Some(blocks.to_string().as_str()),
            "{t}"
        );
        eprintln!(
            "[抬顶] 块数 {blocks}：封顶 {cap} 时一轮缺 {capped_misses} 块 ⇒ 抬到不设顶后容量 \
                 {cap_after}，重读未命中 {misses}"
        );
    }
    cleanup(&path);
}

/// M76 的守卫（LSM 一侧）：块缓存能按**内存预算（字节）**给档，而预算真的当约束。
///
/// 与 bplus 那条同形，形状是 LSM 自己的：`sst_block_entries = 8` 造出 ≈26 块的小库，
/// 地板 4 / 顶 8 ⇒ 预算必须**既能压到地板以下**（它替换 `clamp(块数, 地板, 顶)` 那道夹，
/// 不是叠在下面）、**又能长到顶以上**（给够字节就全库驻留），同时
/// ① 不许把调用方**显式关掉**的缓存（地板 0）悄悄打开；
/// ② 不许抬他**显式定过**的容量（顶 0 那条路，预算只当天花板）；
/// ③ 撤预算（`0`）要回到没设过的那个默认封顶；
/// ④ 封顶要被真的执行（走完一轮，住着块数 ≤ 预算槽数）；
/// ⑤ 换算一格不差 —— 价签在这里**重述一遍**（10 400 B/槽），库里的常数被改小便红，
///    而"改小"正是"按名义块大小乘 ⇒ 内存爆炸"那个事故形状。
#[test]
fn test_block_cache_budget_bytes_replaces_the_clamp_and_respects_explicit() {
    const BLOCK_ENTRIES: usize = 8;
    const FLOOR: usize = 4;
    const CEIL: usize = 8;
    const PRICE: u64 = 10_400;
    // 200 把键 × 每块 8 条 ⇒ ≈26 块：地板 4、顶 8 都被它甩开，两个方向都量得动
    const KEYS: usize = 200;
    // 预算取"槽数 × 价签 + 一点余量"，让换算数是整数好核对
    const SMALL: u64 = 20_000; // ⇒ 1 槽（比地板 4 还小）
    const BIG: u64 = 500_000; // ⇒ 48 槽（比这块库的块数还大 ⇒ 全库驻留）

    fn build(
        tag: &str,
        floor: usize,
        ceil: usize,
    ) -> (MVCC, crate::common::ScratchPath, Vec<Vec<u8>>) {
        let path = tmp_db(tag);
        let keys: Vec<Vec<u8>> = (0..KEYS).map(|i| format!("k{i:04}").into_bytes()).collect();
        let opts = LSMTreeOptions {
            sst_block_entries: BLOCK_ENTRIES,
            block_cache_blocks: floor,
            block_cache_auto_max: ceil,
            ..LSMTreeOptions::for_mvcc()
        };
        let mvcc = MVCC::try_open_with(&path, Some(opts)).unwrap();
        for base in (0..KEYS).step_by(20) {
            let end = (base + 20).min(KEYS);
            let items: Vec<(Vec<u8>, Vec<u8>)> = (base..end)
                .map(|i| (keys[i].clone(), format!("v{i}").into_bytes()))
                .collect();
            let tx = mvcc.begin_transaction();
            if let Err(bad) = tx.batch_set(&items) {
                panic!("batch_set 冲突: {}", String::from_utf8_lossy(&bad));
            }
            tx.commit();
            mvcc.flush();
        }
        (mvcc, path, keys)
    }
    let shape = |mvcc: &MVCC| -> (usize, usize) {
        let kv = mvcc.kv.lock().unwrap();
        (kv.sst_blocks_for_test(), kv.block_cache_capacity())
    };
    let held = |mvcc: &MVCC| -> usize { mvcc.kv.lock().unwrap().cached_blocks_for_test() };

    // ── 自动档那条路（地板 4 / 顶 8）──
    let (mvcc, path, keys) = build("budget_auto", FLOOR, CEIL);
    let (blocks, cap_default) = shape(&mvcc);
    assert!(
        blocks > CEIL * 2,
        "形状没造出来：{blocks} 块没把顶 {CEIL} 甩开 ⇒ 后面是空转"
    );
    assert_eq!(cap_default, CEIL, "没设预算就该是自动档封顶 {CEIL}");
    assert_eq!(mvcc.cache_budget_bytes(), 0, "没设过预算要读回 0");

    // ⑤② 小预算：替换那道夹 ⇒ 能压到地板 4 以下
    let cap_small = mvcc.set_cache_budget_bytes(SMALL);
    assert_eq!(
        cap_small,
        (SMALL / PRICE) as usize,
        "小预算换算不是「预算 ÷ 每槽价签」"
    );
    assert!(
        cap_small < FLOOR,
        "小预算 {cap_small} 没能压到地板 {FLOOR} 以下 ⇒ 预算没替换 clamp"
    );
    assert_eq!(mvcc.cache_budget_bytes(), SMALL, "给的字节要读得回来");
    assert_eq!(
        mvcc.tuning().get("cache_budget_bytes"),
        Some(SMALL.to_string().as_str()),
        "tuning 里看不见给的预算 = 调用方分不清「设过」与「设了没生效」"
    );

    // ④ 封顶真的被执行：小预算下走完一整轮，住着块数不得超过 1 槽
    {
        let tx = mvcc.begin_transaction();
        for k in &keys {
            assert!(tx.get(k).is_some(), "点读一把键落空");
        }
    }
    let held_small = held(&mvcc);
    assert!(
        held_small <= cap_small,
        "说容量 {cap_small} 块，走完一轮却住着 {held_small} 块 ⇒ 封顶只是名义"
    );

    // ③ 大预算：长到顶 8 以上（覆盖率 = 全库）
    let cap_big = mvcc.set_cache_budget_bytes(BIG);
    assert_eq!(
        cap_big,
        blocks.min((BIG / PRICE) as usize),
        "大预算换算不是 min(块数, 预算槽数)"
    );
    assert!(
        cap_big > CEIL,
        "给了 {BIG} B 却仍封顶 {CEIL} ⇒ 被自动档那道顶夹住"
    );

    // ③ 撤预算 ⇒ 回到没设过的那个默认数
    assert_eq!(
        mvcc.set_cache_budget_bytes(0),
        CEIL,
        "撤掉预算要回到自动档默认 {CEIL}"
    );
    assert_eq!(mvcc.cache_budget_bytes(), 0, "撤掉之后要读回 0");
    drop(mvcc);
    let _ = std::fs::remove_dir_all(&path);

    // ① 显式关掉缓存（地板 0）：预算**不许把它打开**
    let (mvcc, path, _k) = build("budget_off", 0, CEIL);
    assert_eq!(shape(&mvcc).1, 0, "地板 0 就该是关缓存");
    assert_eq!(
        mvcc.set_cache_budget_bytes(BIG),
        0,
        "调用方显式关了块缓存，预算把它打开了 ⇒ 「关」这个决定被库替客户改了"
    );
    drop(mvcc);
    let _ = std::fs::remove_dir_all(&path);

    // ② 显式容量（顶 0 ⇒ 自动档不参与）：预算只当天花板
    let (mvcc, path, _k) = build("budget_explicit", 6, 0);
    assert_eq!(shape(&mvcc).1, 6, "顶 0 ⇒ 生效容量应恒等于显式的 6 块");
    assert_eq!(
        mvcc.set_cache_budget_bytes(BIG),
        6,
        "调用方显式要过 6 块，大预算不许把它抬上去"
    );
    assert_eq!(
        mvcc.set_cache_budget_bytes(SMALL),
        cap_small,
        "显式 6 块时小预算照样要压得下去（封顶 {cap_small}）"
    );
    eprintln!(
        "[块缓存预算] 块数 {blocks}｜默认封顶 {CEIL}｜预算 {SMALL} B ⇒ {cap_small} 块（走完一轮住着 \
             {held_small} 块）｜预算 {BIG} B ⇒ {cap_big} 块（顶没夹住）｜撤掉 ⇒ {CEIL}｜关缓存 ⇒ 0\
             ｜显式 6 块：大预算 6 / 小预算 {cap_small}"
    );
    drop(mvcc);
    // 台架自己造的目录要自己收掉 —— 这颗基座的临时现场没有 Drop 守卫，漏一次就积一次
    let _ = std::fs::remove_dir_all(&path);
}
