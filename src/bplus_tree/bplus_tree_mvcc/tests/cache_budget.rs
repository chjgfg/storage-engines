// 验证缓存字节预算和内部页钉住策略，保留原测试入口。

/// M76 的守卫：缓存档可以**按内存预算（字节）**给，而预算真的当约束 —— 不是又一颗"记了个数"的旋钮。
///
/// 存在的理由：这台基座要住的机器给的是**字节**约束（"这进程还能住 32 MB"），而此前能调的两颗
/// 旋钮都以槽数计 ⇒ 下游要自己乘价签。价签是实测的 **≈ 6.8 KB/槽**（乱序点读姿势，页缓存连
/// 解析缓存一起算），不是页的名义 4 KiB —— 按 4 KiB 乘会把常驻买到 **1.66 倍**，正是"内存爆炸"那个方向。
///
/// 六格各抓一种退化（少一格就有一种溜过去）：
/// ① 没设预算时读回 `0`（不许看起来像设过）；
/// ② 小预算换算 = `预算 ÷ 每槽价签`，且**替换**自动档那道 `clamp` ⇒ 能落到地板 1024 以下
///    （否则"只准住 2 MB"被地板吃掉）；
/// ③ 大预算能长到顶 2048 **以上**（否则"给我 30 MB"被那道顶夹回 8 MB）；
/// ④ 撤预算（`0`）回到**没设过的那个默认数** 2048，不是停在最后一次换算值上；
/// ⑤ 显式容量那条路上预算**只当天花板**：大预算不许抬调用方定过的 500，小预算照样压得下去；
/// ⑥ 容量真的被执行：小预算下走完一整轮点读，**住着页数 ≤ 预算槽数**（否则封顶只是名义）。
#[test]
fn test_cache_budget_bytes_prices_slots_and_is_enforced() {
    const ROWS: usize = 1300; // 与上一条同一形状：一把键 ≈ 3.3 KB ⇒ 页数跟着行数长
    /// 换算尺子在这里**重述一遍**价签：库里的常数被改小便红 —— 那正是"按名义页大小乘"的事故形状
    const PRICE: u64 = 7_000;
    let path = tmp_db("cache_budget");
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
        assert!(
            total > 2048,
            "形状没造出来：{total} 页 ≤ 默认顶 ⇒ ③④都是空转（要重挑 ROWS/键长）"
        );
        let resident = |mvcc: &MVCC| -> usize { mvcc.kv.lock().unwrap().cached_pages_for_test() };
        let sweep = |mvcc: &MVCC| {
            let tx = mvcc.begin_transaction();
            for key in &keys {
                assert!(tx.get(key).is_some(), "点读一把键读不到");
            }
            drop(tx);
        };

        // ① 默认：自动档封顶 2048，且预算读回 0
        assert_eq!(mvcc.cache_policy().0, 2048, "没设预算就该是自动档的 2048");
        assert_eq!(mvcc.cache_budget_bytes(), 0, "没设过预算要读回 0");

        // ② 小预算：换算 285 槽，必须压到地板 1024 以下
        let small: u64 = 2_000_000;
        let cap_small = mvcc.set_cache_budget_bytes(small);
        assert_eq!(
            cap_small,
            (small / PRICE) as usize,
            "小预算的换算不是「预算 ÷ 每槽价签」（{small} / {PRICE}）"
        );
        assert!(
            cap_small < 1024,
            "小预算 {cap_small} 页没能压到自动档地板 1024 以下 ⇒ 预算没替换那道 clamp"
        );
        assert_eq!(mvcc.cache_budget_bytes(), small, "给的字节要读得回来");
        assert_eq!(
            mvcc.tuning().get("cache_budget_bytes"),
            Some(small.to_string().as_str()),
            "tuning 里必须看得见给的预算，否则调用方分不清「设过」与「设了没生效」"
        );

        // ⑥ 封顶真的被执行：冷重开之后走完一轮，住着页数不得超过预算槽数
        drop(mvcc);
        let mvcc = MVCC::open(&path, 64, 0);
        assert_eq!(
            mvcc.cache_budget_bytes(),
            0,
            "预算是**会话级**旋钮：重开之后不该带着上次的值（带着就等于库替进程记了个数）"
        );
        mvcc.set_cache_budget_bytes(small);
        sweep(&mvcc);
        let held = resident(&mvcc);
        assert!(
            held <= cap_small,
            "预算档说容量 {cap_small} 页，走完一轮却住着 {held} 页 ⇒ 容量只是名义，封顶没被执行"
        );

        // ③ 大预算：长到顶 2048 以上（覆盖率 = min(总页数, 预算槽数)）
        let big: u64 = 30_000_000;
        let cap_big = mvcc.set_cache_budget_bytes(big);
        assert_eq!(
            cap_big,
            total.min((big / PRICE) as usize),
            "大预算的换算不是「min(总页数, 预算 ÷ 价签)」：总页数 {total}、预算槽 {} 页",
            (big / PRICE) as usize
        );
        assert!(
            cap_big > 2048,
            "给了 {big} B 却只配 2048 页 ⇒ 预算被自动档那道顶夹住了（实际 {cap_big}）"
        );

        // ④ 撤预算 ⇒ 回到没设过的那个默认数
        assert_eq!(
            mvcc.set_cache_budget_bytes(0),
            2048,
            "撤掉预算（0）必须回到自动档默认 2048，不许停在最后一次换算值上"
        );
        assert_eq!(mvcc.cache_budget_bytes(), 0, "撤掉之后要读回 0");

        // ⑤ 显式容量那条路：预算只当天花板，不抬已经定过的数
        drop(mvcc);
        let mvcc = MVCC::open(&path, 64, 500);
        assert_eq!(mvcc.cache_policy().0, 500, "显式容量 500 页没生效");
        assert_eq!(
            mvcc.set_cache_budget_bytes(big),
            500,
            "调用方显式要过 500 页，大预算不许把它抬上去"
        );
        assert_eq!(
            mvcc.set_cache_budget_bytes(small),
            cap_small,
            "显式容量 500 页时小预算照样要压得下去（封顶 285）"
        );
        eprintln!(
            "[缓存预算] 总页数 {total}｜默认 2048｜预算 {small} B ⇒ {cap_small} 页\
                 （走完一轮住着 {held} 页，≤ 封顶）｜预算 {big} B ⇒ {cap_big} 页（顶 2048 没夹住它）\
                 ｜撤掉 ⇒ 2048｜显式 500 页时：大预算仍是 500、小预算 {cap_small}"
        );
    }
    cleanup(&path);
}

/// M49 的守卫：**钉住预算这一档交出去**，而且交出去要有实测的效果 —— 不是"多一个能写的字段"。
///
/// 病灶（M32 记下、当时没碰的第二点）：抬顶之后叶子**拿不到整个容量**。同一份长键库里
/// 2048 页的顶下住着 1986 页，其中**钉住 1024 页全是上层**，留给叶子只有 962 页，
/// 而那份库有 1300 片叶子 ⇒ 一轮随机点读要缺 ~338 片叶子加下降路上的上层页。
/// 上层值多少由键长决定（35 B 键时占 3%、3.3 KB 键时占 50%），这个决定库替调用方做不了。
///
/// 四件事一起断（少断一件就有一种退化溜过去）：
/// ① **默认与改前一字不差**：不调时预算 = 1024、生效值 = `min(1024, 容量/2)`；
/// ② 旋钮真的落到生效值上（`set` 之后 `pin_limit_pages()` 跟着变，不是只记了个数）；
/// ③ **调小会放开已经钉住的页**（否则对一份焐热的缓存完全没效果 —— 那正是这个旋钮存在的理由）；
/// ④ 那条底线不许被突破：传一个比容量一半还大的数，生效值仍是 `容量/2`
///    （否则"抬顶"+"调大钉住"合起来能把整份缓存钉死、叶子一页装不下）。
/// 判据取**计数**：`disk_page_reads()` / `cache_misses()`，同一份库、同一把乱序，两臂只差这个旋钮。
#[test]
fn test_pin_budget_is_configurable_and_leaves_get_the_room() {
    use crate::bplus_tree::page::cache_misses;
    use crate::common::disk::disk_page_reads;
    const ROWS: usize = 1300; // 与 M32 那条同一形状：一把键 ≈ 3.3 KB ⇒ 页数跟着行数长
    let path = tmp_db("pin_budget");
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
        assert!(
            total > 2048,
            "形状没造出来：{total} 页没超过默认顶 2048 ⇒ 容量根本没绑住，两臂差的就不是同一件事"
        );

        let mut order: Vec<usize> = (0..ROWS).collect();
        let mut s = 0x853c_49e6_77b9_5b3eu64;
        for i in (1..order.len()).rev() {
            s = s
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let j = (s >> 33) as usize % (i + 1);
            order.swap(i, j);
        }
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
        // ⚠️ 一次锁取三个数：`Mutex` 不可重入，同一条语句里第二次 `lock()` 就是自死锁
        //（M32 那条守卫冻住过一次；三个现象全指向错的地方，是最贵的一条教训）。
        let state = |mvcc: &MVCC| -> (usize, usize, usize) {
            let tree = mvcc.kv.lock().unwrap();
            (
                tree.cached_pages_for_test(),
                tree.cache_policy().1,
                tree.pin_limit_pages(),
            )
        };

        // ① 默认那一档：不调就等于改前
        let (cap, _, _) = mvcc.cache_policy();
        assert_eq!(mvcc.pin_budget_pages(), 1024, "默认预算必须还是 1024 页");
        assert_eq!(
            mvcc.pin_limit_pages(),
            1024.min(cap / 2).max(1),
            "默认生效上限不等于 `min(1024, 容量/2)`（容量 {cap}）"
        );

        // ⓪ 尺子先自证：冷重开第一趟不可能一字节不读
        drop(mvcc);
        let mvcc = MVCC::open(&path, 64, 0);
        let cold = read_pass(&mvcc).0;
        assert!(
            cold > 0,
            "这把尺子没数到落盘读 ⇒ 空转（冷重开读 {ROWS} 把键却 0 次落盘）"
        );

        // ── 臂 1：默认预算（焐热一趟，再量一趟）──
        let _ = read_pass(&mvcc);
        let (d_def, m_def) = read_pass(&mvcc);
        let (res_def, pinned_def, lim_def) = state(&mvcc);
        eprintln!(
            "[钉住预算] 页数 {total} / 容量 {cap}｜默认 1024：生效上限 {lim_def}、钉住 {pinned_def} 页、\
                 住着 {res_def} 页 ⇒ 焐热一趟再走一趟 落盘 {d_def}、缺 {m_def}"
        );
        assert!(
            d_def > 0,
            "默认预算下焐热一趟就一次盘都不读 ⇒ 病灶没在场，臂 2 的\"变好\"没有对照（形状要重挑）"
        );
        assert!(
            pinned_def > 0,
            "默认预算下一页都没钉住 ⇒ 这个旋钮没有东西可调（臂 2 会空转）"
        );

        // ── ② 旋钮真的落到生效值上 + ④ 那条底线不许被突破 ──
        mvcc.set_pin_budget_pages(usize::MAX);
        assert_eq!(
            mvcc.pin_limit_pages(),
            cap / 2,
            "传 `usize::MAX` 就把生效上限顶破了容量的一半 ⇒ 钉住能把整份缓存钉死（叶子一页装不下）"
        );
        mvcc.set_pin_budget_pages(48);
        assert_eq!(
            mvcc.pin_limit_pages(),
            48,
            "生效值没跟着给的走 ⇒ 只记了个数"
        );

        // ── ③ 调小要**放开已经钉住的页**（否则对焐热的缓存毫无效果）──
        let (_, pinned_after_shrink, _) = state(&mvcc);
        assert!(
            pinned_after_shrink <= 48,
            "预算从 1024 调到 48 之后仍钉着 {pinned_after_shrink} 页 ⇒ 旧钉住没被放开，\
                 叶子照样进不来（这个旋钮就白给了）"
        );

        // ── 臂 2：同一份库、同一把乱序，只差这个旋钮 ──
        let _ = read_pass(&mvcc);
        let (d_small, m_small) = read_pass(&mvcc);
        let (res_small, pinned_small, lim_small) = state(&mvcc);
        eprintln!(
            "[钉住预算] 调到 48：生效上限 {lim_small}、钉住 {pinned_small} 页、住着 {res_small} 页 ⇒ \
                 焐热一趟再走一趟 落盘 {d_small}、缺 {m_small}（默认那一臂是 {d_def} / {m_def}）"
        );

        // ── 臂 3：完全不钉（`0`）—— 这一臂是"这条队列项到底有没有解"的下界 ──
        mvcc.set_pin_budget_pages(0);
        let _ = read_pass(&mvcc);
        let (d_none, m_none) = read_pass(&mvcc);
        let (res_none, pinned_none, lim_none) = state(&mvcc);
        eprintln!(
            "[钉住预算] 调到 0：生效上限 {lim_none}、钉住 {pinned_none} 页、住着 {res_none} 页 ⇒ \
                 焐热一趟再走一趟 落盘 {d_none}、缺 {m_none}"
        );
        let t = mvcc.tuning();
        assert_eq!(
            t.get("cache_pin_budget"),
            Some("0"),
            "`tuning()` 里看不到给的那一档 ⇒ 下游日志里\"设了没生效\"和\"生效了但不知道是多少\"又长一样了：{t}"
        );
        assert_eq!(
            t.get("cache_pin_limit"),
            Some("0"),
            "`tuning()` 里看不到生效值：{t}"
        );
        assert_eq!(
            pinned_none, 0,
            "预算给 0 还钉着 {pinned_none} 页 ⇒ 旋钮没落到换出判定上"
        );
        // ⚠️ 这里**只断机制**，不断"调大/调小谁更快"。两件事叠在一起：
        // ① 代价的方向在这个形状上是**非单调**的（台架 `bplus_pin_budget_sweep`，每臂都冷重开、
        //   升序与降序两遍读数一字不差）：预算 `0 → 2972`、`48 → 1490`、`256 → 2577`、
        //   `1024 → 1590` 页落盘/趟。钉住集合是**按到达顺序填、永不替换**的，所以预算越大
        //   越会冻住"冷内部节点"，把容量白占掉 ⇒ 不是"钉得越多越省"。
        // ② 守卫这一趟是**同一个 open 里换三次旋钮**量的，读数带着上一臂的历史
        //   （同一批预算读出来是 `0 → 1342`、`48 → 2632`、`1024 → 1581`，与①对不上）。
        //   把②那种数写进断言就是拿历史当因果 ⇒ 判据只留机制四条，代价一律去台架读。
        let _ = res_none;
    }
    cleanup(&path);
}

/// M49 的代价台架（`#[ignore]`，手动 release 单线程跑）：钉住预算这一档**到底值多少**。
///
/// 为什么要两种顺序：守卫那一趟读到的是 `0 页钉住 → 1342 落盘`、`48 → 2632`、`1024 → 1581`
/// —— 不单调。不单调只有两种解释：① 预算真的非单调地影响换出；② 读数里混着"这一臂之前
/// 缓存里住着什么"（历史）。把同一批预算按**升序**和**降序**各跑一遍，如果同一个预算在两种
/// 顺序下给出不同的数，那就是②，任何"调小更省/更费"的结论都不许写进文档。
///
/// 每一臂：先焐热 `WARM` 趟，再量一趟 —— 量的那一趟的 `disk_page_reads()` 与 `cache_misses()`
/// 是判据（计数，不取墙钟）。
///
/// ```text
/// BPLUS_PIN_SWEEP_ROWS=1300 cargo test --release --lib -- --ignored --nocapture --test-threads=1 bplus_pin_budget_sweep
/// ```
#[test]
#[ignore = "钉住预算的代价台架，手动 release 单线程跑：BPLUS_PIN_SWEEP_ROWS=1300 cargo test --release --lib -- --ignored --nocapture --test-threads=1 bplus_pin_budget_sweep"]
fn bplus_pin_budget_sweep() {
    use crate::bplus_tree::page::cache_misses;
    use crate::common::disk::disk_page_reads;
    let rows: usize = std::env::var("BPLUS_PIN_SWEEP_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1300);
    const WARM: usize = 3;
    let path = tmp_db("pin_sweep");
    {
        let mvcc = MVCC::open(&path, 64, 0);
        let pad = vec![b'k'; 3300];
        let keys: Vec<Vec<u8>> = (0..rows)
            .map(|i| {
                let mut key = format!("k{i:06}_").into_bytes();
                key.extend_from_slice(&pad);
                key
            })
            .collect();
        for key in &keys {
            let tx = mvcc.begin_transaction();
            assert!(tx.set(key, b"v".to_vec()));
            tx.commit();
        }
        mvcc.flush();
        let total = mvcc.kv.lock().unwrap().next_page_id() as usize;
        let cap = mvcc.cache_policy().0;

        let mut order: Vec<usize> = (0..rows).collect();
        let mut s = 0x853c_49e6_77b9_5b3eu64;
        for i in (1..order.len()).rev() {
            s = s
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let j = (s >> 33) as usize % (i + 1);
            order.swap(i, j);
        }
        let read_pass = |mvcc: &MVCC| -> (u64, u64) {
            let tx = mvcc.begin_transaction();
            let d0 = disk_page_reads();
            let m0 = cache_misses();
            for &i in &order {
                assert_eq!(tx.get(&keys[i]), Some(b"v".to_vec()));
            }
            drop(tx);
            (disk_page_reads() - d0, cache_misses() - m0)
        };
        let state = |mvcc: &MVCC| -> (usize, usize) {
            let tree = mvcc.kv.lock().unwrap();
            (tree.cached_pages_for_test(), tree.cache_policy().1)
        };

        let budgets: Vec<usize> = vec![0, 48, 256, 1024];
        // 臂 = (预算, 顺序)：同一批预算按升序与降序各跑一遍，**每一臂都冷重开**清掉历史
        // ⚠️ 先把建库那一趟的句柄 drop 掉：`FileLock` 是**进程内也认**的（同一进程里
        // 两个 MVCC 同时 open 同一个库，第二个直接报"数据库锁被占用"），不放就开不了第二臂。
        drop(mvcc);
        for (desc, ascending) in [("升序", true), ("降序", false)] {
            let mut list = budgets.clone();
            if !ascending {
                list.reverse();
            }
            eprintln!("=== 钉住预算扫描 {desc}｜页数 {total} / 容量 {cap} / {rows} 把键 ===");
            for b in list {
                // ⚠️ 每一臂都重新 open：上一臂留在页缓存与解析缓存里的东西会替这一臂说话
                //（`std::mem::forget` 那种"跳过 Drop"的写法也不行 —— 文件锁要靠 Drop 释放，
                // 不释放下一臂根本打不开库）。
                let (d, m, res, pinned, limit);
                {
                    let arm = MVCC::open(&path, 64, 0);
                    arm.set_pin_budget_pages(b);
                    limit = arm.pin_limit_pages();
                    for _ in 0..WARM {
                        let _ = read_pass(&arm);
                    }
                    let one = read_pass(&arm);
                    d = one.0;
                    m = one.1;
                    let s = state(&arm);
                    res = s.0;
                    pinned = s.1;
                }
                eprintln!(
                    "  预算 {b:>5} ⇒ 生效上限 {limit:>5}、钉住 {pinned:>5} 页、住着 {res:>5} 页｜量一趟 落盘 {d:>5}、缺 {m:>5}（{:.3} 页/读）",
                    d as f64 / rows as f64
                );
            }
        }
    }
    cleanup(&path);
}
