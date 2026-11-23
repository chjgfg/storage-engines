// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn version_point_bounds_save_one_allocation_per_lookup() {
    const READS: usize = 32;
    let path = tmp_db("point_bound_allocations");
    let db = MVCC::open(&path);
    db.set_vacuum_auto_slice(0);
    let writer = db.begin_transaction();
    assert!(writer.set_raw(b"k", b"exact".to_vec()));
    assert!(writer.set_raw(b"k\0suffix", b"prefix-extension".to_vec()));
    let keys: Vec<Vec<u8>> = (0..READS)
        .map(|i| (i as u64).to_be_bytes().to_vec())
        .collect();
    for key in &keys {
        assert!(writer.set_raw(key, b"value".to_vec()));
    }
    writer.commit();

    let reader = db.begin_transaction();
    let kv = reader.kv.lock().unwrap();
    let old_prefix = Transaction::find_version_back_reference(&kv, b"k", |_| true);
    let new_prefix = Transaction::find_version_back(&kv, b"k", |_| true);
    let (old, old_allocations) = crate::mem_probe::count_thread_allocations(|| {
        let mut last = None;
        for key in &keys {
            last = Transaction::find_version_back_reference(
                &kv,
                std::hint::black_box(key.as_slice()),
                |_| true,
            );
            std::hint::black_box(&last);
        }
        last
    });
    let (new, new_allocations) = crate::mem_probe::count_thread_allocations(|| {
        let mut last = None;
        for key in &keys {
            last =
                Transaction::find_version_back(&kv, std::hint::black_box(key.as_slice()), |_| true);
            std::hint::black_box(&last);
        }
        last
    });
    drop(kv);
    reader.commit();
    assert_eq!(
        old_prefix, new_prefix,
        "prefix-interleaved lookup must be preserved"
    );
    assert_eq!(new, old, "combined bounds must preserve lookup results");
    assert_eq!(
        old_allocations,
        new_allocations + READS as u64,
        "one old bound allocation must disappear for each lookup"
    );
}

#[test]
fn test_point_op_cost_independent_of_chain_length() {
    fn probe(chain_len: u64) -> (u64, u64, u64) {
        let path = tmp_db(&format!("probe_{chain_len}"));
        let (get_blocks, set_blocks, scan_blocks);
        {
            let mvcc = MVCC::open(&path);
            for i in 0..chain_len {
                let tx = mvcc.begin_transaction();
                assert!(
                    tx.set(b"hot", format!("v{i}").into_bytes()),
                    "第 {i} 次写入被拒"
                );
                tx.commit();
            }
            // 必须落到 SST：否则读全在 memtable 里，量不到块访问
            mvcc.flush();

            let tx = mvcc.begin_transaction();
            let before = crate::lsm_tree::sst::block_reads();
            assert_eq!(
                tx.get(b"hot"),
                Some(format!("v{}", chain_len - 1).into_bytes())
            );
            get_blocks = crate::lsm_tree::sst::block_reads() - before;

            let before = crate::lsm_tree::sst::block_reads();
            assert!(tx.set(b"hot", b"newer".to_vec()));
            set_blocks = crate::lsm_tree::sst::block_reads() - before;
            tx.rollback();

            // 参照量：旧实现走的就是 range_scan（整段物化 + 每源从第 0 块起扫），
            // 它就摆在这里，所以「改前」是实测而不是推断。
            let kvengine = mvcc.kv.lock().unwrap();
            let before = crate::lsm_tree::sst::block_reads();
            let rows = kvengine
                .range_scan(encode_key(b"hot", 0), encode_key(b"hot", u64::MAX))
                .len();
            scan_blocks = crate::lsm_tree::sst::block_reads() - before;
            assert_eq!(rows as u64, chain_len);
        }
        cleanup(&path);
        (get_blocks, set_blocks, scan_blocks)
    }

    let (g_short, s_short, scan_short) = probe(50);
    let (g_long, s_long, scan_long) = probe(2000);
    eprintln!(
        "点读块访问：链长 50 → {g_short} 块，链长 2000 → {g_long} 块\n\
             点写块访问：链长 50 → {s_short} 块，链长 2000 → {s_long} 块\n\
             旧路径 range_scan 参照：链长 50 → {scan_short} 块，链长 2000 → {scan_long} 块"
    );
    assert!(
        g_long <= g_short + 8,
        "点读的块访问不应随链长/库总量增长（短链 {g_short}，长链 {g_long}）"
    );
    assert!(
        s_long <= s_short + 8,
        "点写的块访问不应随链长/库总量增长（短链 {s_short}，长链 {s_long}）"
    );
    // 反向定位必须明显便宜于整段物化。注意分母是**已经享受了起点定位改造**的
    // range_scan（本次同批修的：它原先是从文件第 0 块逐条 continue 到 low），
    // 所以真实改造前的差距比这里显示的更大 —— 旧实现还要把每行的 value 读出来并克隆。
    assert!(
        g_long * 4 <= scan_long,
        "find_back 应当明显便宜于整段 range_scan（实际 {g_long} 块 vs {scan_long} 块）"
    );
}

// ---- 下游报告的形状：点查墙钟时间基线（手动 release 跑） -------------------
//
// 下游把本 crate 嵌进去之后实测：**单次点查 0.86 ms**（5002 键 / 40B 键 / 60B 值 /
// 10005 行 / max_chain=3 / active_txns=0，全 release）。而 `health()` 与
// `test_point_op_cost_independent_of_chain_length` 那套守卫给的都是**块访问次数** ——
// 它把块缓存命中和未命中算成同一个数，所以"链没失控、块访问只有 4 次"和"每次点查快 1 毫秒"
// 这两件事**能同时成立**。这个基线就是补上漏掉的那一环。
//
// 本机（Windows / NTFS / release）复现到的分配：
//   改造前  乱序 289.0 µs/次（未命中 1.61/次）  vs  有序 45.6 µs/次（未命中 0.15/次）
//           ⇒ 差值 243 µs ÷ 1.46 次未命中 ≈ **一次 File::open ≈ 167 µs**
//             —— 旧 `read_slice` 每次未命中都重开一次文件，而这个 open 比读那几 KB 贵一个量级。
//   改造后 乱序 120.3 µs/次、有序 21.0 µs/次、multi_get 19.5、一次全量 scan 19.1 µs/行
//           ⇒ 未命中次数没变（1.61/0.15），墙钟少了一半多：省掉的正是那次重开文件。
//           剩下 乱序/有序 还差 5.7 倍，那部分不是 open，而是 **4 块/次**（定位一趟 +
//           定论一趟）× 未命中的解码，属于下一条里程碑（一次定论 + 索引二分 + 缓存容量）。
// 结论"用法也有份"的另一半：读全部键时一次 `scan` 是 19 µs/行，逐条 `get` 是它的 1~6 倍。
//
// M23 把这条通道量到底，靠的是同一个台架换两个旋钮：`LSM_POINT_ROWS` 换规模、
// `LSM_CACHE_AUTO=0` 关掉块缓存自动档（= 改造前那个钉死的 256 槽）。同构建、同库目录的读法：
//   5002 键（314 块）   钉死 256：乱序 27.9 µs/次、未命中 0.39  vs  自动档 314：**8.5**、0.03
//                      块/次两边都是 4.00、条目比较/次 128 ⇒ 省掉的只有"其中几次真读盘"
//                      "启动后只读 4 条"这一格最扎眼：57.5 → 11.5 µs/次（下游的进程启动形状）
//   40000 键（2681 块） 256：117.3、未命中 1.81  vs  自动档 1024：90.6、1.24
//                      vs 上限放到 4096（全库驻留 18.7 MB）：21.0、0.03
//                      ⇒ 残差 ≈ 未命中次数 × 一次随机读（这台上 ≈ 54 µs），上限是**预算**不是正确性
//   ⚠️ 第一版自动档在这里抓到自己的回归：容量 256→1024 后**未命中次数不动而有序读 22.2→36.0**
//     —— `BlockCache` 每次命中都 `order.iter().position()`，代价 O(驻留块数)。换成定长槽 +
//     双向链表（O(1)）之后同一档有序读 17.1 ≈ 地板那臂的 17.4。守卫见 `lru_touch_cost_...`。
//
// 复现命令（本仓库）：
//   cargo test --release --lib -- --ignored --nocapture point_read_baseline
//   LSM_POINT_ROWS=40000 LSM_CACHE_AUTO=0 cargo test --release --lib -- --ignored \
//     --nocapture --test-threads=1 point_read_baseline   # 关掉自动档做对照
#[test]
#[ignore = "墙钟基线，手动 release 跑：cargo test --release --lib -- --ignored --nocapture point_read_baseline"]
fn test_lsm_point_read_baseline_5000_keys() {
    use std::time::Instant;

    // 键数可由 `LSM_POINT_ROWS` 调（默认 5002 = 下游那份现场）：M23 要判"点读单价随什么长"，
    // 只能同一份构建跑两个规模比每单位成本 —— 单规模只能看见"贵"，看不见"超线性"。
    #[allow(non_snake_case)]
    let N: usize = std::env::var("LSM_POINT_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5002);
    type Snap = (u64, u64, u64, u64);

    // 键形状照抄下游：1 字节标签 + 4 字节项目 id + 路径文本 ≈ 40 字节
    fn make_key(i: usize) -> Vec<u8> {
        let tag = b'1' + (i % 3) as u8;
        let pid = 1000 + i % 7;
        let mut k = Vec::new();
        k.push(tag);
        k.extend_from_slice(format!("#{pid:04}#worldquant/datafields/{i}/f.json").as_bytes());
        while k.len() < 40 {
            k.push(b'0');
        }
        k
    }
    // 值 ≈ 60 字节：必须远小于 blob 阈值 4 KiB，否则就不是同一条路径
    fn make_val(i: usize) -> Vec<u8> {
        format!("{{\"i\":{i},\"h\":\"{:0<24}\",\"n\":97}}", i * 7).into_bytes()
    }

    let path = tmp_db("point_baseline_5k");
    let keys: Vec<Vec<u8>> = (0..N).map(make_key).collect();
    let vals: Vec<Vec<u8>> = (0..N).map(make_val).collect();
    assert!(
        vals.iter().all(|v| v.len() < 4 * 1024),
        "值必须小到不进 blob 旁路"
    );

    // 写路径照抄下游：begin → batch_set(2000) → commit，两轮全量 + 一轮单键
    // ⇒ total_rows = 5002×2+1 = 10005、live_keys = 5002、max_chain = 3
    {
        let mvcc = MVCC::open(&path);
        for round in 0..3 {
            let upto = if round == 2 { 1 } else { N };
            for base in (0..upto).step_by(2000) {
                let end = (base + 2000).min(upto);
                let items: Vec<(Vec<u8>, Vec<u8>)> = (base..end)
                    .map(|i| (keys[i].clone(), vals[i].clone()))
                    .collect();
                let tx = mvcc.begin_transaction();
                if let Err(bad) = tx.batch_set(&items) {
                    panic!("batch_set 冲突: {}", String::from_utf8_lossy(&bad));
                }
                tx.commit();
            }
        }
        // 让数据真的落到 SST（下游是重开进程后读的；flush 是同一条路的近似）
        mvcc.flush();
        let h = mvcc.health();
        eprintln!(
            "形状: rows={} live_keys={} max_chain={} active_txns={}",
            h.total_rows, h.live_keys, h.max_chain, h.active_txns
        );
        // ⚠️ 物理行数与链长都只断"不超过"：自动回收是按提交记账触发的，N 大了它就在写过程中
        // 已经把部分链压平了（实测 40000 键那一档 rows=68505 < 80001、max_chain=2；
        // 200k 那一档 rows=341868 < 400001）。这条台架量的是**读代价**，回收的口径由 M4
        // 那批守卫守着；这里唯一硬合同是 live_keys —— 键不能丢也不能凭空多。
        assert!(
            (h.total_rows as usize) <= N * 2 + 1,
            "物理行数超过形状上限：{} > {}",
            h.total_rows,
            N * 2 + 1
        );
        assert_eq!(h.live_keys as usize, N);
        assert!(
            h.max_chain <= 3,
            "链长不该超过写入轮数 3：实测 {}",
            h.max_chain
        );
    }

    // 重新打开，模拟"进程启动后读"
    //
    // `LSM_CACHE_AUTO=0` ⇒ 关掉块缓存自动档，回到改造前那个"钉死 256 槽"。M23 的 A/B
    // 只能这样做：同一份构建、同一个库目录，只换这一档 ⇒ "点读单价随规模涨"到底是不是缓存容量
    // 造成的，才有凭据（墙钟在本机 run 间能差 ±2 倍，结论只能落在计数上）。
    let auto_max = std::env::var("LSM_CACHE_AUTO")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or_else(|| LSMTreeOptions::for_mvcc().block_cache_auto_max);
    let mvcc = MVCC::try_open_with(
        &path,
        Some(LSMTreeOptions {
            block_cache_auto_max: auto_max,
            ..LSMTreeOptions::for_mvcc()
        }),
    )
    .map_err(|e| format!("重开失败: {e}"))
    .unwrap();
    let (stats, cap, blocks) = {
        let kv = mvcc.kv.lock().unwrap();
        (
            kv.stats(),
            kv.block_cache_capacity(),
            kv.sst_blocks_for_test(),
        )
    };
    let sst_bytes: u64 = stats.level_bytes.iter().sum();
    // 缓存的价位用"实测每块平均多少字节"来推，不靠估：块是按条数切的，条子大小才是分母
    let per_block = if blocks == 0 {
        0.0
    } else {
        sst_bytes as f64 / blocks as f64
    };
    eprintln!(
        "布局: levels={:?} total_sst={} l0={} mem_entries={} 块={blocks} 容量={cap} \
             自动档={auto_max} SST={:.1} MB 每块均值={per_block:.0} B ⇒ 满缓存价位≈{:.1} MB",
        stats.level_files,
        stats.total_sst,
        stats.l0_files,
        stats.mem_entries,
        sst_bytes as f64 / 1048576.0,
        cap as f64 * per_block / 1048576.0,
    );
    // A/B 的有效性凭据：这一档必须真的落到容量上了，否则两个臂量的是同一件事
    let floor = LSMTreeOptions::for_mvcc().block_cache_blocks;
    let expect = if auto_max == 0 {
        floor
    } else {
        blocks.clamp(floor, auto_max)
    };
    assert_eq!(
        cap, expect,
        "自动档={auto_max} 下容量该是 {expect}（块 {blocks}，地板 {floor}），实测 {cap}"
    );

    // 乱序（≈ 目录序 / NTFS 哈希序）与有序两种访问顺序，分开量：局部性影响块缓存命中率
    let mut shuffled: Vec<usize> = (0..N).collect();
    let mut s = 0x853c_49e6_77b9_5b3eu64;
    for i in (1..N).rev() {
        s = s
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let j = (s >> 33) as usize % (i + 1);
        shuffled.swap(i, j);
    }

    let tx = mvcc.begin_transaction();

    let report = |name: &str, dur: std::time::Duration, n: usize, a: Snap, b: Snap| {
        let us = dur.as_micros() as f64;
        let per = |x: u64| x as f64 / n.max(1) as f64;
        eprintln!(
            "{name:<22} 合计 {:>9.1} ms | 单次 {:>8.1} µs | 块/次 {:>5.2} | 未命中/次 {:>5.2} \
                 | 探文件/次 {:>5.2} | 条目比较/次 {:>9.1}",
            us / 1000.0,
            if n == 0 { 0.0 } else { us / n as f64 },
            per(b.0 - a.0),
            per(b.1 - a.1),
            per(b.2 - a.2),
            per(b.3 - a.3),
        );
    };
    let snap = || {
        (
            crate::lsm_tree::sst::block_reads(),
            crate::lsm_tree::sst::block_misses(),
            crate::lsm_tree::sst::sst_probes(),
            crate::lsm_tree::sst::sst_entry_walks(),
        )
    };

    // (a) 同一个事务里连读 5002 次 —— 乱序
    let a = snap();
    let t = Instant::now();
    for &i in &shuffled {
        assert!(tx.get(&keys[i]).is_some(), "乱序读第 {i} 个键落空");
    }
    report("点读×N 乱序", t.elapsed(), N, a, snap());

    // (b) 同一个事务里连读 5002 次 —— 有序（键升序 = 块升序）
    let a = snap();
    let t = Instant::now();
    for k in &keys {
        assert!(tx.get(k).is_some(), "有序读落空");
    }
    report("点读×N 有序", t.elapsed(), N, a, snap());

    // (c) multi_get：门面把它实现成 `map(get)`，所以理论上和 (a) 同价 —— 量出来才算数
    let a = snap();
    let t = Instant::now();
    let got = tx.multi_get(&keys);
    assert_eq!(got.len(), N);
    report("multi_get×N", t.elapsed(), N, a, snap());

    // (d) 一次全量 scan —— "干脆整段读回来"那条路
    let a = snap();
    let t = Instant::now();
    let rows = tx.scan(None, None);
    eprintln!("全量 scan 命中 {} 行", rows.len());
    report("一次全量 scan", t.elapsed(), rows.len().max(1), a, snap());

    // (e) 只要 4 条（下游"进程启动 + 打开 + 扫 4 条"那一段）
    let a = snap();
    let t = Instant::now();
    for k in &keys[..4] {
        assert!(tx.get(k).is_some());
    }
    report("点读×4", t.elapsed(), 4, a, snap());

    drop(tx);
    drop(mvcc);
    cleanup(&path);

    // (f) 单次点查的代价 = **两趟**（定位 + 定论）× **含这个键的 SST 数** × 未命中。
    //     所以真正该盯的量是"同一把键的版本散在几个文件里"：库里每多一层/每多一个覆盖
    //     该键的 SST，点查就多探一次块。compact 的意义就是把这件事压回层数。
    //     造法：每轮全量写一遍再 flush ⇒ 每个键在每个文件里都有一份。
    eprintln!("--- 覆盖同一把键的 SST 数 → 单次点查代价 ---");
    if N > 50_000 {
        // 这一段要按 1/2/4/7 轮**全量重写**来造分散度，N=200k 时是 320 万行写 + 压实，
        // 一台架跑不动；分散度那一档已经在 5002 键上量过（M10 的结论就是从它来的）。
        eprintln!("    （跳过：N={N} > 50000，这段要全量重写 1+2+4+7 轮）");
    }
    for files in [1usize, 2, 4, 7]
        .into_iter()
        .take(if N > 50_000 { 0 } else { 4 })
    {
        let p2 = tmp_db(&format!("point_spread_{files}"));
        {
            let mv = MVCC::open(&p2);
            // 关掉自动回收：7 轮全量写会把 pending 推过切片宽度，实验形状会被库自己改掉
            mv.set_vacuum_auto_slice(0);
            for _round in 0..files {
                for base in (0..N).step_by(2000) {
                    let end = (base + 2000).min(N);
                    let items: Vec<(Vec<u8>, Vec<u8>)> = (base..end)
                        .map(|i| (keys[i].clone(), vals[i].clone()))
                        .collect();
                    let tx = mv.begin_transaction();
                    if let Err(bad) = tx.batch_set(&items) {
                        panic!("batch_set 冲突: {}", String::from_utf8_lossy(&bad));
                    }
                    tx.commit();
                }
                mv.flush();
            }
            let st = mv.kv.lock().unwrap().stats();
            let h = mv.health();
            let tx = mv.begin_transaction();
            let a = snap();
            let t = Instant::now();
            for &i in &shuffled {
                assert!(tx.get(&keys[i]).is_some());
            }
            let d = t.elapsed();
            report(&format!("乱序点读 (每键 {files} 份)"), d, N, a, snap());
            eprintln!(
                "    ↳ total_sst={} levels={:?} rows={} max_chain={}",
                st.total_sst, st.level_files, h.total_rows, h.max_chain
            );
        }
        cleanup(&p2);
    }
}
