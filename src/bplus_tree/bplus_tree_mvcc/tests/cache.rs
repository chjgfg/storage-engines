// 该文件集中验证对应职责，保留原测试名称和字面值。

/// 只读走查**不许把点读的工作集挤出去**。
///
/// 归因实测：20000 行的库有 1257 页，而页缓存上限 1024 —— 一次 `health()` 走查
/// 换出 1600 页、顺带 flush 765 个脏页，150 ms；紧接着的第二遍命中数只有 1。
/// 走查对每片叶子只碰一次，回填它不会有下一次命中，却会把真正在用的热页逐出。
/// 现在走查走 `read_page_for_walk`：满了就不回填 ⇒ 换出次数必须是 0（与库大小无关）。
#[test]
fn test_full_walk_does_not_evict_the_working_set() {
    const KEYS: u64 = 400;
    const CACHE_PAGES: usize = 8; // 故意远小于页数：让"回填就会换出"成立
    let path = tmp_db("walk_no_evict");
    {
        let mvcc = MVCC::open(&path, 16, CACHE_PAGES);
        for i in 0..KEYS {
            let tx = mvcc.begin_transaction();
            assert!(tx.set(format!("k{i:05}").as_bytes(), format!("v{i}").into_bytes()));
            tx.commit();
        }
        mvcc.flush();

        // 预热：点读一遍，把能装的页装进缓存（此后缓存是满的）
        {
            let tx = mvcc.begin_transaction();
            for i in 0..KEYS {
                assert!(tx.get(&format!("k{i:05}").into_bytes()).is_some());
            }
        }
        let evict0 = crate::bplus_tree::page::page_evictions();
        let read0 = crate::bplus_tree::page::page_reads();

        {
            let tx = mvcc.begin_transaction();
            let rows = tx.scan(None, None);
            assert_eq!(rows.len() as u64, KEYS, "全量 scan 应折出全部键");
        }
        let evicted = crate::bplus_tree::page::page_evictions() - evict0;
        let walked = crate::bplus_tree::page::page_reads() - read0;
        eprintln!("走查 {walked} 次页访问，换出 {evicted} 页（缓存上限 {CACHE_PAGES} 页）");
        // 合同是"与树高同阶"，不是"绝对 0"：起点下降到那片叶子要读 root/内部页，
        // 那几个导航页未命中时会回填（实测 2 次），这是常数级、与库大小无关。
        // 真正的病灶是"每走一片叶子换出一批"——旧实现 53 次页访问换出 50 页。
        assert!(
            evicted * 8 <= walked,
            "换出 {evicted} 页对 {walked} 次页访问太多：走查在逐页挤掉工作集"
        );
        assert!(
            evicted as usize <= CACHE_PAGES,
            "换出必须停在树高量级（实测 {evicted}，缓存上限 {CACHE_PAGES}）"
        );
    }
    cleanup(&path);
}

/// M22 守卫②：**根与内部节点不参与换出** —— 判据是"重读同一把键，只许缺自己那一片叶子"。
///
/// 病灶（2026-10-17 的 200k 归因台架）：走查之前一次随机点读 45.2 µs、走查+批量写之后
/// **58.6 µs**，而把容量开到装得下全库是 8.5 µs —— 差的是"下降路上那几层又被读了一遍、
/// 又被重解析了一遍"。根与内部节点是每次点读都要用的，却和叶子排在同一个 LRU 里一起被挤。
///
/// ⚠️ 两个当时想错的地方，都以实测为准改在这里：
/// - **"上层只占全库 3%"不是常数**。同一份五千行的库，35 B 键时上层 36/1210 页 = 3.0%，
///   3.3 KB 键时上层 1305/2606 页 = **50%**（内部节点也装不下几个长键 ⇒ 扇出掉到 2、树深涨到 12）。
///   所以钉住预算必须**跟容量挂钩**（≤ 容量 ÷ 2），只钉一个绝对页数会在长键库上把整个缓存钉死。
/// - 真正服务下降的是**解析缓存**（`node_of`），它不受页换出影响 ⇒ 钉住对**聚合缺页**的影响
///   比预想的小（实测同一形状：钉住 1.46 页/读、不钉 1.75 页/读）。所以这条守卫同时断三件事，
///   缺一件就有一种退化溜过去：
///   ① 机制：`pinned > 0` 且 `pinned ≤ 内部节点数`、`pinned × 2 ≤ 容量`（撤掉钉住 ⇒ 第一条就红）
///   ② 聚合代价：缺页数 ≤ 1.6 × 读次数（实测不钉是 1.75 ⇒ 红；余量 12%，计数是确定性的）
///   ③ 绝对地板：**冲掉工作集之后单独重读一把早就读过的键**，最多缺 2 页（叶子那一片 + 边角）
#[test]
fn test_upper_levels_survive_a_full_walk() {
    const ROWS: u64 = 1200;
    const CACHE_PAGES: usize = 16;
    const THRASH: usize = 300;
    let path = tmp_db("pin_upper");
    {
        let mvcc = MVCC::open(&path, 16, CACHE_PAGES);
        for i in 0..ROWS {
            let tx = mvcc.begin_transaction();
            assert!(tx.set(format!("k{i:06}").as_bytes(), format!("v{i}").into_bytes()));
            tx.commit();
        }
        mvcc.flush();
        let (depth, internal, leaves, total) = {
            let mut kv = mvcc.kv.lock().unwrap();
            kv.shape()
        };
        eprintln!("[形状] 行={ROWS} 总页={total} 深={depth} 内部={internal} 叶子={leaves}");
        assert!(
            depth >= 3,
            "这份库得有 {depth} 层才测得出下降路上被挤掉几页"
        );
        assert!(
            internal > 0 && leaves > CACHE_PAGES,
            "叶子得比容量多才测得出换出"
        );

        // 先把上层钉住：下降一遍就够（第一趟本来就要读它们）
        const PROBE: u64 = 0;
        {
            let tx = mvcc.begin_transaction();
            for i in 0..8u64 {
                assert!(
                    tx.get(&format!("k{i:06}").into_bytes()).is_some(),
                    "预热读 {i}"
                );
            }
        }
        let (cap, pinned, _) = mvcc.cache_policy();
        assert!(
            pinned > 0 && pinned <= internal,
            "预热后应钉住若干上层页（实测 {pinned}，内部节点共 {internal}）"
        );
        assert!(
            pinned * 2 <= cap,
            "钉住 {pinned} 页超过容量 {cap} 的一半 ⇒ 叶子那边一页都装不下，比不钉还糟"
        );

        // 把 LRU 转好几圈：全库走查 + 一堆乱序点读（都不该动到钉住的页）
        {
            let tx = mvcc.begin_transaction();
            let rows = tx.scan(None, None);
            assert_eq!(rows.len() as u64, ROWS, "全量 scan 要折出所有键");
        }
        let tm0 = crate::bplus_tree::page::cache_misses();
        {
            let tx = mvcc.begin_transaction();
            let mut idx: Vec<u64> = (1..ROWS).collect();
            let mut s = 0x853c_49e6_77b9_5b3eu64;
            for i in (1..idx.len()).rev() {
                s = s
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                let j = (s >> 33) as usize % (i + 1);
                idx.swap(i, j);
            }
            for &i in idx.iter().take(THRASH) {
                assert!(
                    tx.get(&format!("k{i:06}").into_bytes()).is_some(),
                    "冲击读 {i}"
                );
            }
        }
        let tmisses = crate::bplus_tree::page::cache_misses() - tm0;
        eprintln!(
            "[钉上层 聚合] {THRASH} 次乱序点读缺页 {tmisses}（{:.2} 页/读；实测钉住 1.46、不钉 1.75，树深 {depth}）",
            tmisses as f64 / THRASH as f64
        );
        // ② 聚合缺页 ≤ 1.6 页/读。计数是确定性的（同一个 LCG、同一份形状），
        // 所以这条界能真的把两种形状分开；余量 12%，不是拍脑袋。
        assert!(
            tmisses * 5 <= THRASH as u64 * 8,
            "乱序点读缺页 {:.2} 页/读 ⇒ 下降路上的层又被读了一遍（上层没钉住，\
                 或解析缓存超限时又整表丢了）",
            tmisses as f64 / THRASH as f64
        );

        // 决定性的一次：**单独**重读那把早就读过的键
        let r0 = crate::bplus_tree::page::page_reads();
        let m0 = crate::bplus_tree::page::cache_misses();
        let p0 = crate::bplus_tree::bplus_tree::node_parses();
        {
            let tx = mvcc.begin_transaction();
            assert!(
                tx.get(&format!("k{PROBE:06}").into_bytes()).is_some(),
                "重读探针键读空"
            );
        }
        let reads = crate::bplus_tree::page::page_reads() - r0;
        let misses = crate::bplus_tree::page::cache_misses() - m0;
        let parses = crate::bplus_tree::bplus_tree::node_parses() - p0;
        eprintln!(
            "[钉上层] 冲掉 {THRASH} 次随机点读之后，重读同一把键：页 {reads} / 缺页 {misses} / 重解析 {parses}（树深 {depth}）"
        );
        // 钉住生效 ⇒ 这一次读最多缺"叶子那 1 页"。没钉住 ⇒ 整条下降路都要重来（≈树深）。
        // 余量给到 2：允许叶子的解析结果也早被丢掉（那是叶子该管的事，不是上层的账）。
        assert!(
            misses <= 2,
            "重读一把早就读过的键要缺 {misses} 页（树深 {depth}）⇒ 上层页被换出了，\
                 每次点读都得从盘上把根与内部节点重新拼一遍"
        );
        assert!(
            parses <= 2,
            "重读同一把键要重解析 {parses} 个节点 ⇒ 解析缓存超限时把上层页的结果整表丢了"
        );
    }
    cleanup(&path);
} // 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn eviction_failure_blocks_commit_and_checkpoint_preserves_recovery() {
    for fault in 1..=5 {
        let path = tmp_db("eviction_failure");
        {
            let db = MVCC::open(&path, 4, 2);
            db.set_vacuum_auto_slice(0);
            let seed = db.begin_transaction();
            assert!(seed.set_raw(b"committed", b"keep".to_vec()));
            seed.try_commit().unwrap();
            drop(seed);
            let tx = db.begin_transaction();
            let _fault = crate::bplus_tree::storage::dblwr_probe::Scope::fault(fault);
            let mut failed = false;
            for i in 0u32..100 {
                if tx.try_set_raw(&i.to_be_bytes(), vec![7; 512]).is_err() {
                    failed = true;
                    break;
                }
            }
            assert!(failed, "small cache must reach the injected write failure");
            assert!(matches!(
                tx.try_commit(),
                Err(crate::CommitError::WriteFailed(
                    crate::WriteError::StorageWriteFailed
                ))
            ));
            assert!(db.try_checkpoint().is_err());
            let wal_before = std::fs::read(wal_path(&path)).unwrap();
            assert!(tx.try_set_raw(b"after", b"no".to_vec()).is_err());
            tx.rollback();
            assert!(db.active_txn.lock().unwrap().contains_key(&tx.version()));
            drop(tx);
            drop(db);
            assert_eq!(
                std::fs::read(wal_path(&path)).unwrap(),
                wal_before,
                "failed close must preserve recovery WAL"
            );
        }
        let db = MVCC::open(&path, 4, 2);
        let tx = db.begin_transaction();
        assert_eq!(tx.get(b"committed"), Some(b"keep".to_vec()));
        for i in 0u32..100 {
            assert_eq!(tx.get(&i.to_be_bytes()), None);
        }
        assert_eq!(tx.get(b"after"), None);
        tx.try_commit().unwrap();
    }
}

/// M22 守卫①：**页缓存的自动档要跟着库长大**，长大的那笔钱还得说清楚。
///
/// 病灶（2026-10-17 的 200k 基线）：`max_cache_pages` 以前只有一个写死的 1024。
/// 归因台架量的就是这件事 —— 同一个 1024 页：
/// - 20k 行 = 1210 页 ⇒ 覆盖率 84.6% ⇒ 随机点读 **11.9 µs/次**
/// - 200k 行 = 12129 页 ⇒ 覆盖率 8.4% ⇒ 随机点读 **45.2 µs/次**
/// 一个常数在两个规模上是两种东西，所以它不该是常数。自动档 = 按库的页数给容量，
/// 夹在 `[1024, 2048]`：**下限就是要改之前那个写死值**（换成自动档不许让任何人的工作集变小），
/// 上限是"最多替你多占 8 MiB"（一页 4 KB）—— 库再大要更全的覆盖必须由调用方点名，
/// 因为那时缓存整个库的常驻 ≈ 数据文件本身（200k 行实测 47 MB），那已经不是缓存而是复制。
///
/// 尺子是 [`MVCC::cache_policy`]（生效容量 / 钉住的页数 / 是否自动档），不是墙钟。
/// ⚠️ 这份库是**用长键撑页数**的（一把键 ≈ 3.3 KB ⇒ 一页只装一条）：越过 1024 页那条下限
/// 只要一千多行。用大值撑不动页数 —— bplus 的 MVCC 把大值走 blob 旁路，叶子只留指针
/// （实测 2600 行 × 2 KB 值 = 85 页，压根没长起来）。
#[test]
fn test_cache_capacity_follows_the_library() {
    const ROWS: usize = 1300;
    let path = tmp_db("cache_auto");
    {
        let mvcc = MVCC::open(&path, 64, 0); // 0 = 自动档
        let (cap0, _, auto) = mvcc.cache_policy();
        assert!(auto, "传 0 就该进自动档");
        assert_eq!(
            cap0, 1024,
            "空库的自动档必须停在旧默认值（实际 {cap0}）—— 换成自动档不许让谁的工作集变小"
        );

        let pad = vec![b'k'; 3300];
        for i in 0..ROWS {
            let mut key = format!("k{i:06}_").into_bytes();
            key.extend_from_slice(&pad);
            let tx = mvcc.begin_transaction();
            assert!(tx.set(&key, b"v".to_vec()), "灌第 {i} 行");
            tx.commit();
        }
        // 重新评估就挂在刷盘这条路径上（checkpoint / bulk 收尾都走它）
        mvcc.flush();

        let total = mvcc.kv.lock().unwrap().next_page_id();
        let (depth, internal, leaves, _) = {
            let mut kv = mvcc.kv.lock().unwrap();
            kv.shape()
        };
        let (cap, pinned, _) = mvcc.cache_policy();
        eprintln!(
            "[自动档] 行={ROWS} 总页={total} 深={depth} 内部={internal} 叶子={leaves} \
                 ⇒ 生效容量={cap} 钉住={pinned}"
        );
        assert!(
            total > 1024,
            "这份库得有 1024 页以上才测得出'库长过了旧默认值'（实际 {total} 页）"
        );
        assert!(
            cap > 1024,
            "刷过盘之后容量还停在 1024 ⇒ 自动档没跟着库长大（就是这次要修的那个洞）"
        );
        assert_eq!(
            cap,
            (total as usize).min(2048),
            "容量应当正好贴着页数（上限 2048 之内），实际 {cap} / 页数 {total}"
        );
        assert!(
            pinned > 0 && (pinned as u64) <= internal as u64,
            "上层页要钉住：实测 {pinned} 页，内部节点共 {internal} 页"
        );
        // 底线：钉住的那部分**不许把叶子的工作集吃光**（长键库的上层能占到全库 33%）
        assert!(
            pinned * 2 <= cap,
            "钉住 {pinned} 页 / 容量 {cap} 页 ⇒ 叶子那边连一半都不到，等于缓存没在用"
        );
    }
    // 显式传数不许被自动档吃掉
    {
        let mvcc = MVCC::open(&path, 64, 512);
        let (cap, _, auto) = mvcc.cache_policy();
        assert!(!auto, "显式传了数就不该是自动档");
        assert_eq!(cap, 512, "显式容量被自动档覆盖了：生效 {cap}");
    }
    cleanup(&path);
}

/// 自动档的**上限可以抬**，抬了要真的一个字节都不再读盘，而且**不重建缓存**（M32）。
///
/// 为什么单独一条：`test_cache_capacity_follows_the_library` 钉的是"容量跟着库长、到 2048 封顶"，
/// 但 2048 这个顶在 600k 行的库上只剩 5.6% 覆盖率 ⇒ 每次点读都要付一次读盘 + 重解析（M31 量过：
/// 页/次 = 1.03）。顶是**预算**不是正确性，所以"要不要把库复制进 RAM"得调用方点名 ——
/// 而"能点名"这件事本身需要一条守卫，否则它只是一个写在注释里的承诺。
///
/// 判据全部用计数，量在**磁盘那一层**（`common::disk::disk_page_reads`）：那才是"全驻留"的定义。
/// 实测这份 1300 把长键（≈ 2606 页）的库 ——
/// 冷启一趟落盘 2605 次 ｜ 默认顶 2048 焐热一趟后再走仍落盘 **1621** 次（顶真的绑住了）｜
/// 抬到不设顶 ⇒ 容量 2606 = 页数，焐热一趟后再走落盘 **0** 次、页缓存未命中 **0** 次，
/// 而住着 1986 页、抬前抬后**同一个数**（没重建）。
/// ⚠️ 两个当时想错的地方，都改在这里而不是删掉：
/// - 原本打算"压到下限 1024 再抬"，理由是"默认顶装得下这个形状"。**错**：默认顶下焐热一趟
///   仍有 1621 次落盘 —— 钉住的上层页预算是容量的一半（`min(1024, cap/2)` = 1024 页），
///   住的 1986 页里 1024 页是上层，留给 1300 片叶子的只有 962 个槽。所以根本不需要主动压顶
///   —— 少跑两趟，形状自己的证据反而更硬。
/// - 原本怀疑"`page_reads` 数不到东西是因为 `node_cache` 在页缓存之上把点读全吸收了"。
///   实测**落盘数 = 页缓存未命中数**（1621 = 1621），页缓存这一层的尺子一直是准的；真正
///   让那条守卫"卡死不动"的是另一件事 —— 见上面 `resident` 那段：同一条语句里锁两次
///   同一把 `Mutex` = 自死锁，进程 CPU 冻住、一行都不打印，看起来像"读路径慢了几十倍"。
/// 变异对照（都真跑过，红的位置就是它该管的那件事）：
/// - A「只记值、不落到生效容量」⇒ 本条红在 `cap1 == total`（实测 left 2048 / right 2606），
///   LSM 同一条变异红在 `cap_after == blocks`（left 8 / right 26）。⚠️ 这时**门面那条守卫
///   仍然绿** —— 它只核对"读回来的值"，效应必须留在引擎里，这就是"每一层都要有自己的守卫"。
/// - B「把改容量写成重建缓存」⇒ 本条红在"住着几页"（left 0 / right 1986），
///   LSM 红在 `0 / 8`。
#[test]
fn test_cache_auto_max_is_raisable_and_misses_go_to_zero() {
    use crate::bplus_tree::page::cache_misses;
    use crate::common::disk::disk_page_reads;
    const ROWS: usize = 1300; // 长键形状：一把键 ≈ 3.3 KB ⇒ 一页一条 ⇒ 页数跟着行数长
    let path = tmp_db("cache_raise");
    {
        let mvcc = MVCC::open(&path, 64, 0);
        let pad = vec![b'k'; 3300];
        let keys: Vec<Vec<u8>> = (0..ROWS)
            .map(|i| {
                let mut key = format!("k{i:06}_").into_bytes();
                key.extend_from_slice(&pad);
                key
            })
            .collect();
        for key in &keys {
            let tx = mvcc.begin_transaction();
            assert!(tx.set(key, b"v".to_vec()), "灌一把键失败");
            tx.commit();
        }
        mvcc.flush();

        let total = mvcc.kv.lock().unwrap().next_page_id() as usize;
        let (cap0, _, auto) = mvcc.cache_policy();
        assert!(auto, "传 0 就该是自动档");
        assert!(
            total > 2048,
            "形状没造出来：{total} 页没超过默认上限 2048 ⇒ 顶根本没绑住，后面全是空转"
        );
        assert_eq!(cap0, 2048, "封顶时生效容量必须是 2048，实际 {cap0}");
        assert_eq!(mvcc.cache_auto_max(), 2048, "上限本身要读得回来");

        let mut order: Vec<usize> = (0..ROWS).collect();
        let mut s = 0x853c_49e6_77b9_5b3eu64;
        for i in (1..order.len()).rev() {
            s = s
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let j = (s >> 33) as usize % (i + 1);
            order.swap(i, j);
        }
        // 一趟随机点读，返回（这一趟落盘读了一几页, 页缓存未命中几次）
        let read_pass = |mvcc: &MVCC| -> (u64, u64) {
            let tx = mvcc.begin_transaction();
            let d0 = disk_page_reads();
            let m0 = cache_misses();
            for &i in &order {
                assert_eq!(tx.get(&keys[i]), Some(b"v".to_vec()), "读第 {i} 把键不对");
            }
            drop(tx);
            (disk_page_reads() - d0, cache_misses() - m0)
        };
        // 缓存里"住着几页 / 钉住几页"要一次锁取两个数。
        // ⚠️ 不能写成 `mvcc.kv.lock().unwrap().a()` 和 `mvcc.kv.lock().unwrap().b()` 放在
        // **同一条语句**的两个参数里：临时量（第一个 MutexGuard）要到语句结束才释放，第二次
        // `lock()` 就是自死锁 —— `std::sync::Mutex` 不可重入，单线程直接冻住（CPU 不涨、
        // 什么都不打印）。这条守卫就是这么"卡死"过一次，三个现象全部指向错的地方。
        let resident = |mvcc: &MVCC| -> (usize, usize) {
            let tree = mvcc.kv.lock().unwrap();
            (tree.cached_pages_for_test(), tree.cache_policy().1)
        };

        // ⚠️ 必须**冷重开**再量：写阶段会把摸过的页连同解析结果留在 `node_cache` 里，
        // 不重开量的就不是生产形状。重开之后两个缓存都是空的。
        drop(mvcc);
        let mvcc = MVCC::open(&path, 64, 0);

        // 尺子自己先被证明数得到：冷启动第一趟不可能一个字节都不读盘
        let (cold, cold_miss) = read_pass(&mvcc);
        assert!(
            cold > 0,
            "这把尺子没数到落盘读 ⇒ 空转（冷重开后第一趟读 {ROWS} 把键却要 0 次落盘，不可能）"
        );

        // ① 默认顶 2048：焐热一趟之后**仍然要落盘** —— 这就是病灶本身（顶是预算，
        //    工作集一旦超过它，每次点读都在付"读页 + 重解析"），也是 600k 行上看到 5.6% 覆盖率的形状
        let (warm_default, warm_default_miss) = read_pass(&mvcc);
        assert!(
            warm_default > 0,
            "容量 2048 / 全库 {total} 页，焐热一趟后一轮点读零落盘 ⇒ 这个顶根本没绑住，\
                 后面「抬顶」的对照就是空转（形状变了要重挑 ROWS/键长）"
        );

        // ② 抬到不设顶：容量贴到总页数；且**不重建**（住着几页，抬完还住着几页）
        let (resident_before, pinned) = resident(&mvcc);
        assert!(
            resident_before > 0,
            "焐过一轮后缓存是空的 ⇒ 下面那条「没重建」会空转"
        );
        mvcc.set_cache_auto_max(usize::MAX);
        let (cap1, _, _) = mvcc.cache_policy();
        assert_eq!(
            cap1, total,
            "抬到不设顶 ⇒ 容量必须贴到总页数 {total}，实际 {cap1}"
        );
        assert_eq!(mvcc.cache_auto_max(), usize::MAX, "抬上去的上限要读得回来");
        let (resident_after, _) = resident(&mvcc);
        assert_eq!(
            resident_after, resident_before,
            "抬容量把缓存从 {resident_before} 页变成 {resident_after} 页 ⇒ 走回了「重建缓存」"
        );

        // ③ 新容量下焐热一趟，第二趟必须**一次盘都不读** —— 否则"抬上来的容量"是假的
        let _ = read_pass(&mvcc);
        let (raised_reads, raised_miss) = read_pass(&mvcc);
        assert_eq!(
            raised_reads, 0,
            "容量已经 = 页数 {total}，第二趟随机点读仍落盘 {raised_reads} 次 ⇒ 抬上来的容量没生效"
        );
        assert_eq!(
            raised_miss, 0,
            "容量 = 页数 {total} 时第二趟仍有 {raised_miss} 次页缓存未命中 ⇒ 抬上来的容量没生效"
        );
        let t = mvcc.tuning();
        assert_eq!(
            t.get("total_pages"),
            Some(total.to_string().as_str()),
            "分母要看得见，否则调用方算不出覆盖率：{t}"
        );
        assert_eq!(
            t.get("max_cache_pages"),
            Some(total.to_string().as_str()),
            "tuning 里的生效容量必须与 cache_policy 一致：{t}"
        );
        eprintln!(
            "[压顶→抬顶] 页数 {total}：冷启落盘 {cold}（缺 {cold_miss}）| 默认顶 2048 焐热后落盘 \
                 {warm_default}（缺 {warm_default_miss}）| 抬到不设顶后容量 {cap1}，焐热一趟再走一趟落盘 \
                 {raised_reads}（缺 {raised_miss}）；住着 {resident_before} → {resident_after} 页\
                 （钉住 {pinned}，没重建）"
        );
    }
    cleanup(&path);
}
