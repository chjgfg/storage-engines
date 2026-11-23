// 该文件集中验证对应职责，保留原测试名称和字面值。

#[test]
fn test_point_op_cost_independent_of_chain_length() {
    fn probe(chain_len: u64) -> (u64, u64) {
        let path = tmp_db(&format!("probe_{chain_len}"));
        let (get_pages, set_pages);
        {
            let mvcc = MVCC::open(&path, 4, 16);
            for i in 0..chain_len {
                let tx = mvcc.begin_transaction();
                assert!(
                    tx.set(b"hot", format!("v{i}").into_bytes()),
                    "第 {i} 次写入被拒"
                );
                tx.commit();
            }

            let tx = mvcc.begin_transaction();
            let before = crate::bplus_tree::page::page_reads();
            assert_eq!(
                tx.get(b"hot"),
                Some(format!("v{}", chain_len - 1).into_bytes())
            );
            get_pages = crate::bplus_tree::page::page_reads() - before;

            let before = crate::bplus_tree::page::page_reads();
            assert!(tx.set(b"hot", b"newer".to_vec()));
            set_pages = crate::bplus_tree::page::page_reads() - before;
            tx.rollback();
        }
        cleanup(&path);
        (get_pages, set_pages)
    }

    let (g_short, s_short) = probe(50);
    let (g_long, s_long) = probe(2000);
    eprintln!(
        "点读访问页数：链长 50 → {g_short} 页，链长 2000 → {g_long} 页\n\
             点写访问页数：链长 50 → {s_short} 页，链长 2000 → {s_long} 页"
    );
    // 判据是「只能随树高（log 规模）涨，不许随链长（线性）涨」：
    // order=4 时链长 50 → 2000 会让树高多约 3 层，所以给 8 页容差；
    // 而未修实现同样的两个点是 34 → 1015 页（涨 981 页），远大于任何对数级容差。
    assert!(
        g_long <= g_short + 8,
        "点读的页数不应随版本链长度线性增长（短链 {g_short}，长链 {g_long}）"
    );
    assert!(
        s_long <= s_short + 8,
        "点写的页数不应随版本链长度线性增长（短链 {s_short}，长链 {s_long}）"
    );
}

/// 一次"未命中"的单价拆开裸测（M31 的归因，`#[ignore]`）。
///
/// 为什么还要再拆一层：`bplus_read_cost_attribution` 已经证明**一次随机点读只缺 ~1 页**
/// （600k 档：未命中/次 = 1.01、命中/次 = 0.024、`node_cache` 的 retain 扫描每读只走
/// 1.1 个条目 ⇒ M22 那句 `retain` 不是钱的去处），但 600k 仍是 35.9 µs/次 而 20k 满覆盖时
/// 只要 5.2 µs。⇒ 钱在"那一次未命中怎么办"里，而它有四步是叠在一起的：
/// ①定位读一页（`DiskFile::read_page` = `metadata()` + `seek` + `read_exact` = **3 次系统调用**
/// 外加一次 4 KiB 分配）
/// ②CRC（表驱动逐字节，4096 次查表）
/// ③`unpack_page`（校验 + 把载荷再复制一份 `Vec`）
/// ④`bincode::deserialize::<BTreeNode>`（一页 ~50 条，每条一个 `Vec<u8>` ⇒ 上百次小分配）
/// 这里**绕过引擎**直接对同一个库文件逐段计时，并且跑两遍（第二遍 OS 页缓存是热的）⇒
/// 真盘延迟与纯 CPU 那部分分得开。哪一段占大头，M31 的刀就落在哪一段上。
///
/// # 量出来的答案（200k/600k 两档，µs/页）
///
/// - **`read_page` 里那句 `self.len()` 值 ≈ 4.4 µs/页**：同一轮里"读页 7.08~7.45"
///   vs "读页 + 多问一次 len 11.00~12.27"。旧写法每读一页都要问一遍文件长度，只为判断
///   "这页在不在 EOF 之外"—— 那次 `File::metadata()` 是一次系统调用，占掉整页读的三到四成。
/// - **CRC32 逐字节 → slice-by-8 = 4.3×**（11.73/12.02 → 2.71/2.82 µs/页）。
/// - 于是"一次未命中"从 ~15-19 µs/页 降到 ~7 µs/页 + 解析。
///
/// ⚠️ **方法学（这条台架存在的真正理由）**：单次配对**在这台机器上会连方向都翻**。
/// 同一份代码、同一次运行里，"读页"这项实测过 5.45 与 14.60 µs/页（2.7 倍），
/// CRC 的配对比值先后报过 1.23×、0.81×（**0.81 那次是"新实现更慢"**）。
/// 改成**交错跑 5 轮、各取最小值**之后，两轮独立测得 4.33× 与 4.27× —— 干扰只会让一次
/// 变慢，不会让最小值变慢，所以只有最小值可比。**在这台机器上，任何 µs 级 A/B 都必须
/// 交错 + 取最小，单次配对不算凭据**（跨构建比 µs 的 1.3~1.9× 漂移早就知道，这次发现
/// 同一进程内的相邻两段也躲不开）。
///
/// ```text
/// BPLUS_PRICE_ROWS=200000 cargo test --release --lib -- --ignored --nocapture \
///     --test-threads=1 bplus_miss_price_probe
/// ```
#[test]
#[ignore = "未命中单价拆分，手动 release 单线程跑：BPLUS_PRICE_ROWS=200000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 bplus_miss_price_probe"]
fn bplus_miss_price_probe() {
    use crate::bplus_tree::storage::{DiskFile, PAGE_SIZE, unpack_page};
    use std::time::Instant;

    #[allow(non_snake_case)]
    let N: usize = std::env::var("BPLUS_PRICE_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200_000);
    const READS: usize = 2000;
    fn key(i: usize) -> Vec<u8> {
        format!("{i:08}#/worldquant/datafields/x.json").into_bytes()
    }
    fn val(i: usize) -> Vec<u8> {
        format!("{{\"i\":{i},\"h\":\"{:0<28}\",\"v\":1}}", i * 7).into_bytes()
    }

    let path = tmp_db("price");
    // 只造库，不带着句柄量：这一段要的是"裸读同一个文件"的单价
    {
        let mvcc = MVCC::open(&path, 64, 0);
        for base in (0..N).step_by(1000) {
            let end = (base + 1000).min(N);
            let items: Vec<(Vec<u8>, Vec<u8>)> = (base..end).map(|i| (key(i), val(i))).collect();
            let tx = mvcc.begin_transaction();
            if let Err(bad) = tx.batch_set(&items) {
                panic!("batch_set 冲突: {}", String::from_utf8_lossy(&bad));
            }
            tx.commit();
        }
        mvcc.flush();
    }

    let mut file = DiskFile::open_paged(&path, PAGE_SIZE).unwrap();
    let pages = (file.len().unwrap() / PAGE_SIZE as u64) as usize;
    assert!(
        pages > READS,
        "库太小（{pages} 页）⇒ 取样会大量重复，测不出随机读"
    );
    // 确定性伪随机（与 `bplus_read_cost_attribution` 同一个 LCG 写法）
    let mut order: Vec<usize> = (1..pages).collect();
    let mut s = 0x853c_49e6_77b9_5b3eu64;
    for i in (1..order.len()).rev() {
        s = s
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let j = (s >> 33) as usize % (i + 1);
        order.swap(i, j);
    }
    order.truncate(READS);

    for label in ["第一遍(冷)", "第二遍(热)"] {
        let t = Instant::now();
        let mut raws: Vec<Vec<u8>> = Vec::with_capacity(READS);
        for &p in &order {
            raws.push(file.read_page(p as u64).unwrap());
        }
        let d_read = t.elapsed().as_secs_f64() * 1e6 / READS as f64;

        // ⚠️ 结果必须**用起来**：第一版这里写的是 `let _ = crc32(raw)`，release 下整个循环被判成
        // 无副作用直接删掉，报出来是「CRC 0.00 µs/页」—— 那是**测量错**不是结论（本轮就栽过一次）。
        let mut crc_sink = 0u32;
        for raw in &raws {
            crc_sink = crc_sink.wrapping_add(crate::common::disk::crc32(std::hint::black_box(
                raw.as_slice(),
            )));
        }
        let d_crc = t.elapsed().as_secs_f64() * 1e6 / READS as f64;
        // **同轮配对 A/B**：同一批缓冲、同一个进程里再跑一遍逐字节参照实现。
        // 跨轮比 µs 在这台机器上不作数（同一条点读台架实测 35.9~68.6 µs/次的轮间差就是
        // 这个量级），只有前后脚跑在同一批数据上的两个数能说出"快几倍"。
        let t = Instant::now();
        let mut crc_sink_slow = 0u32;
        for raw in &raws {
            crc_sink_slow = crc_sink_slow.wrapping_add(crate::common::disk::crc32_bytewise(
                std::hint::black_box(raw.as_slice()),
            ));
        }
        let d_crc_slow = t.elapsed().as_secs_f64() * 1e6 / READS as f64;
        assert_eq!(
            crc_sink, crc_sink_slow,
            "两份 CRC 实现对同一批页算出不同校验和 ⇒ 有一方是错的（这不是性能问题，是数据问题）"
        );

        let t = Instant::now();
        let mut payloads: Vec<Vec<u8>> = Vec::with_capacity(READS);
        for raw in &raws {
            if let Ok((_id, pl)) = unpack_page(std::hint::black_box(raw.as_slice())) {
                payloads.push(pl);
            }
        }
        let d_unpack = t.elapsed().as_secs_f64() * 1e6 / READS as f64;

        let t = Instant::now();
        for p in &order {
            let raw = file.read_page(*p as u64).unwrap();
            let _ = unpack_page(std::hint::black_box(&raw));
        }
        let d_chain = t.elapsed().as_secs_f64() * 1e6 / READS as f64;
        // 配对 A/B：读一页时**多问一次文件长度** —— 那就是被删掉的第 3 次系统调用，
        // 与上面"读页"那栏前后脚跑，差值就是它一页的价（跨轮比不出这个差）。
        let t = Instant::now();
        let mut sink = 0usize;
        for p in &order {
            let _ = file.len().unwrap();
            let raw = file.read_page(*p as u64).unwrap();
            sink += std::hint::black_box(raw.len());
        }
        let d_read_with_len = t.elapsed().as_secs_f64() * 1e6 / READS as f64;
        std::hint::black_box(sink);

        assert!(
            d_crc <= d_crc_slow * 1.5,
            "同轮同缓冲上，slice-by-8 版反而比逐字节参照慢 {d_crc:.2} > {d_crc_slow:.2} µs/页"
        );
        // **交错 5 轮、各取最小值**：这台机器的干扰能把同一份代码的两次读数拉开 2~3 倍
        // （"读页"这一项实测 5.45 与 14.60 µs/页），单次配对方向都会翻。
        // 干扰只会让一次**变慢**，所以两边的最小值才是可比的。
        let mut best_fast = f64::MAX;
        let mut best_slow = f64::MAX;
        for _ in 0..5 {
            let t = Instant::now();
            for raw in &raws {
                crc_sink = crc_sink.wrapping_add(crate::common::disk::crc32(std::hint::black_box(
                    raw.as_slice(),
                )));
            }
            best_fast = best_fast.min(t.elapsed().as_secs_f64() * 1e6 / READS as f64);
            let t = Instant::now();
            for raw in &raws {
                crc_sink_slow = crc_sink_slow.wrapping_add(crate::common::disk::crc32_bytewise(
                    std::hint::black_box(raw.as_slice()),
                ));
            }
            best_slow = best_slow.min(t.elapsed().as_secs_f64() * 1e6 / READS as f64);
        }
        assert_eq!(crc_sink, crc_sink_slow, "交错后两份实现又对不上了");
        eprintln!(
            "CRC 交错 5 轮取最小：快 {best_fast:.2} vs 逐字节 {best_slow:.2} µs/页 ⇒ {:.2}×",
            best_slow / best_fast,
        );
        assert!(
            best_fast <= best_slow,
            "交错取最小之后 slice-by-8 仍然更慢（{best_fast:.2} > {best_slow:.2}）⇒ 这次改动该退回去"
        );

        eprintln!(
            // ④（把载荷解析成节点）故意不在这里量：`BTreeNode` 是模块私有类型，从外面量就得
            // 造一个"看着一样"的替身结构 —— 那是假数。它的价 = 引擎层每读成本
            // （`bplus_read_cost_attribution` 的 32~36 µs）减去这里这几段，剩下那一块。
            // ⚠️ 一整行写完：上一版用续行，结果续行符在字符串里成了真反斜杠 + 真换行，
            // 后半段被拆到别的物理行、读输出的人根本看不到 —— 量具自己的错也会骗人。
            "{label:<12} 读页 {d_read:>6.2} | CRC快 {d_crc:>6.2} | CRC逐字节参照 {d_crc_slow:>6.2} ⇒ {:.2}× | unpack {d_unpack:>6.2} | 读+unpack串 {d_chain:>6.2} | 读页+多问一次len {d_read_with_len:>6.2} µs/页 (sink={})",
            d_crc_slow / d_crc.max(1e-9),
            std::hint::black_box(crc_sink + payloads.len() as u32)
        );
    }
    // `metadata()` 那一笔单独定个价：`read_page` 每次都要问一遍文件长度
    let t = Instant::now();
    for _ in 0..READS {
        let _ = file.len().unwrap();
    }
    eprintln!(
        "只问文件长度(metadata) {:>6.2} µs/次 ⇒ 这是 read_page 里那第 3 次系统调用的价",
        t.elapsed().as_secs_f64() * 1e6 / READS as f64
    );
    cleanup(&path);
}

//
// 大库基线里 bplus 点读 33.6 µs/次（bitcask 4.3）、`search 首页` 518 ms、`health()` 110 ms。
// `page_reads()` 把「缓存命中」和「未命中要读盘 + CRC」算成同一个数，所以它守得住
// "代价不随链长涨"，却答不出**钱花在哪**。这条基线分开数 hits/misses，并且**同一份数据
// 连测两遍**：第二遍缓存是热的 ⇒ 两遍之差就是真读盘那部分，剩下的才是每页重解析的成本。
//
// M22 起再加两件：
// - **行数可由 `BPLUS_ROWS` 调**（默认 20000，跑 200000 就是大库那一档）——
//   覆盖率这件事只能在两个规模之间才看得见，单规模只能看见"贵"。
// - **打印树的形状**（树高 / 内部节点数 / 叶子数 / 总页数 / 默认缓存的覆盖率）——
//   上层页是**结构性小**（叶子数 ÷ order）还是跟库一起长，决定的是两件完全不同的事：
//   前者该常驻（钉住，代价与库大小无关），后者才谈得上容量与覆盖率。
//
// 跑法：
//   BPLUS_ROWS=200000 cargo test --release --lib -- --ignored --nocapture bplus_read_cost_attribution
#[test]
#[ignore = "墙钟归因，手动 release 跑：BPLUS_ROWS=20000 cargo test --release --lib -- --ignored --nocapture bplus_read_cost_attribution"]
fn test_bplus_read_cost_attribution() {
    use std::time::Instant;

    // `N` 沿用这条台架里其余地方的写法（其它台架都是 `const N`），所以要压掉命名警告
    #[allow(non_snake_case)]
    let N: usize = std::env::var("BPLUS_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20000);
    // 缓存档可由 `BPLUS_CACHE` 调：**默认 0 = 自动档**，与门面 `EngineConfig::default()`
    // 交给下游的那一档一致（第一版这里钉死 1024，量的其实不是生产形状）。
    #[allow(non_snake_case)]
    let CACHE_PAGES: usize = std::env::var("BPLUS_CACHE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    fn key(i: usize) -> Vec<u8> {
        format!("{i:08}#/worldquant/datafields/x.json").into_bytes()
    }
    fn val(i: usize) -> Vec<u8> {
        format!("{{\"i\":{i},\"h\":\"{:0<28}\",\"v\":1}}", i * 7).into_bytes()
    }

    let mut order: Vec<usize> = (0..N).collect();
    let mut s = 0x853c_49e6_77b9_5b3eu64;
    for i in (1..N).rev() {
        s = s
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let j = (s >> 33) as usize % (i + 1);
        order.swap(i, j);
    }

    let path = tmp_db("attrib");
    {
        let mvcc = MVCC::open(&path, 64, CACHE_PAGES);
        for base in (0..N).step_by(1000) {
            let end = (base + 1000).min(N);
            let items: Vec<(Vec<u8>, Vec<u8>)> = (base..end).map(|i| (key(i), val(i))).collect();
            let tx = mvcc.begin_transaction();
            if let Err(bad) = tx.batch_set(&items) {
                panic!("batch_set 冲突: {}", String::from_utf8_lossy(&bad));
            }
            tx.commit();
        }
        mvcc.flush();
        let h = mvcc.health();
        eprintln!(
            "形状: rows={} live_keys={} max_chain={}",
            h.total_rows, h.live_keys, h.max_chain
        );

        // 树形：树高 / 内部节点 / 叶子 / 总页数 / 默认缓存的覆盖率
        // ⚠️ 这一趟要摸每一页，所以必须在下面打点**之前**跑完（它自己会灌一堆计数）。
        {
            let mut kv = mvcc.kv.lock().unwrap();
            let (depth, internal, leaves, total) = kv.shape();
            let (cap, pinned, auto) = kv.cache_policy();
            eprintln!(
                "树形: 高={depth} 内部={internal} 叶子={leaves} 总页={total}                      ⇒ 上层占全库 {:.2}% | 缓存 传入={CACHE_PAGES} 生效={cap}({}档) 钉住上层={pinned}                      ⇒ 覆盖率 {:.1}%",
                internal as f64 * 100.0 / total.max(1) as f64,
                if auto { "自动" } else { "显式" },
                cap as f64 * 100.0 / total.max(1) as f64
            );
            assert!(internal > 0 && leaves > 0, "树形探针没数到东西");
        }

        // 冷重开：缓存清空，模拟"进程启动后第一次读"
        drop(mvcc);
        let mvcc = MVCC::open(&path, 64, CACHE_PAGES);

        let probe = |label: &str, mvcc: &MVCC| {
            let tx = mvcc.begin_transaction();
            let (p0, hit0, miss0) = (
                crate::bplus_tree::page::page_reads(),
                crate::bplus_tree::page::cache_hits(),
                crate::bplus_tree::page::cache_misses(),
            );
            let parse0 = crate::bplus_tree::bplus_tree::node_parses();
            let sweep0 = crate::bplus_tree::bplus_tree::node_cache_sweeps();
            let visit0 = crate::bplus_tree::bplus_tree::node_cache_visited();
            let t = Instant::now();
            for &i in &order {
                assert!(tx.get(&key(i)).is_some(), "第 {i} 个键读空");
            }
            let d = t.elapsed().as_secs_f64() * 1e6;
            let (dp, dh, dm, dn) = (
                crate::bplus_tree::page::page_reads() - p0,
                crate::bplus_tree::page::cache_hits() - hit0,
                crate::bplus_tree::page::cache_misses() - miss0,
                crate::bplus_tree::bplus_tree::node_parses() - parse0,
            );
            let (ds, dv) = (
                crate::bplus_tree::bplus_tree::node_cache_sweeps() - sweep0,
                crate::bplus_tree::bplus_tree::node_cache_visited() - visit0,
            );
            eprintln!(
                "{label:<16} {:>7.1} µs/次 | 页/次 {:>4.2} | 解析/次 {:>5.2} | 命中 {:>7} |                      未命中 {:>6} | node_cache 扫 {ds} 趟走过 {dv} 条目 ⇒ 每读 {:.1} 个条目",
                d / N as f64,
                dp as f64 / N as f64,
                dn as f64 / N as f64,
                dh,
                dm,
                dv as f64 / N as f64,
            );
            (dp, dn)
        };

        eprintln!("--- 点读：同一份数据连测两遍（第二遍缓存是热的）---");
        let (_, cold_parses) = probe("第一遍(冷)", &mvcc);
        let (warm_pages, warm_parses) = probe("第二遍(热)", &mvcc);
        let _ = warm_pages; // 页访问随两遍的缓存清除时机浮动，只对绝对量做断言
        assert!(
            cold_parses > 0,
            "解析计数没在数东西 —— 那下面那条断言就是空转（探针自己也要自证会红）"
        );
        // 合同按**绝对量**写，不跟"第一遍 vs 第二遍"比（两遍谁冷谁热取决于执行顺序，
        // 相对断言会假红）。一次点读走 4 层 ⇒ 没有解析缓存时每层解析一次 ≈ 4N 次；
        // 生效之后热态必须压到 ≤1.5N（实测 0.48/次 = 9.6k）。残余的那点是
        // "这页本遍第一次见"的必付成本 —— 1257 页 > max_cache_pages 1024，
        // 工作集本来就装不下，剩下要收得靠"容量可配"（#17）。
        assert!(
            warm_parses * 2 <= (N as u64) * 3,
            "点读还在按层重解析：{N} 次读解析了 {warm_parses} 次（无缓存基线约 4N = {}）",
            4 * N as u64
        );

        // 五个计数一起快照：命中/未命中只说"有没有读盘"，换出与批量写盘才说得出
        // "走一遍全库被顺带 flush 了多少脏页"（脏页换出要写双写缓冲 + sync）。
        type Snap = (u64, u64, u64, u64, u64, u64);
        let snap = || -> Snap {
            (
                crate::bplus_tree::page::page_reads(),
                crate::bplus_tree::page::cache_hits(),
                crate::bplus_tree::page::cache_misses(),
                crate::bplus_tree::page::page_evictions(),
                crate::bplus_tree::page::batch_flushed_pages(),
                crate::bplus_tree::page::lru_compactions(),
            )
        };
        let report = |label: &str, per: f64, unit: &str, a: Snap, b: Snap| {
            eprintln!(
                "{label:<16} {per:>9.1} {unit:<7} | 页 {:>6} 命中 {:>6} 未命中 {:>5} 换出 {:>5} 批量写盘 {:>5} LRU压缩 {:>4}",
                b.0 - a.0,
                b.1 - a.1,
                b.2 - a.2,
                b.3 - a.3,
                b.4 - a.4,
                b.5 - a.5
            );
        };

        // 一次全量 scan
        let tx = mvcc.begin_transaction();
        let a = snap();
        let t = Instant::now();
        let rows = tx.scan(None, None);
        let d = t.elapsed().as_secs_f64() * 1e6;
        report(
            "一次全量 scan",
            d / rows.len().max(1) as f64,
            "µs/行",
            a,
            snap(),
        );
        drop(tx);
        drop(rows);

        // ⚠️ 复现台架的顺序：health() 之前有一轮"更新 2000 键"，那会把页弄脏。
        //    复测表里 health() 从 110 ms 涨到 251 ms，这一组就是去分"每页更贵"还是"页更多"。
        let a = snap();
        let t = Instant::now();
        for i in 0..2000usize {
            let tx = mvcc.begin_transaction();
            assert!(tx.set(&key(i), val(i + N)), "更新第 {i} 个键失败");
            tx.commit();
        }
        report(
            "更新 2000 键",
            t.elapsed().as_secs_f64() * 1e6 / 2000.0,
            "µs/次",
            a,
            snap(),
        );

        let a = snap();
        let t = Instant::now();
        let _ = mvcc.health();
        report(
            "health() 紧跟",
            t.elapsed().as_secs_f64() * 1e6,
            "µs/次",
            a,
            snap(),
        );

        // 再量一遍：此时脏页已经flush过，若这一遍明显快 ⇒ 上一遍付的是flush脏页/换出的钱
        let a = snap();
        let t = Instant::now();
        let _ = mvcc.health();
        report(
            "health() 第二遍",
            t.elapsed().as_secs_f64() * 1e6,
            "µs/次",
            a,
            snap(),
        );

        let q = crate::common::SearchQuery::contains(b"", 0, 100);
        let tx = mvcc.begin_transaction();
        let a = snap();
        let t = Instant::now();
        let _ = tx.search_keys(&q);
        report(
            "search 首页",
            t.elapsed().as_secs_f64() * 1e6,
            "µs/次",
            a,
            snap(),
        );
        drop(tx);

        // 决定性的一条：走查之后点读的工作集还在不在。
        // 修复前 —— 上面那几趟全库走查已经把缓存换出几千页，这一遍几乎全未命中；
        // 修复后 —— 走查不回填，点读留下的热页应该原封不动。
        eprintln!("--- 走查之后的点读（工作集是否被毁）---");
        probe("点读(走查后)", &mvcc);

        // 页缓存开大到能装下全库，看未命中是否归零（这决定"贵"是 IO 还是 CPU）
        drop(mvcc);
        let mvcc = MVCC::open(&path, 64, 65536);
        eprintln!("--- max_cache_pages=65536 ---");
        probe("点读(大缓存)", &mvcc);
        drop(mvcc);
    }
    cleanup(&path);
}
