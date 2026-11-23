// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// M58 的证据台架：**深翻到同一页，两条门差多少**（`#[ignore]`，手动 release 跑）。
///
/// 为什么要这一格：门面这边有两条翻页门 —— `search_keys` 是 **offset** 分页
/// （`SearchQuery::contains(.., offset, len)`），`scan_page` 是**游标**分页。
/// M15/M21/M51 已经把 offset 那条路的三笔浪费砍掉了（不为 total 走全库、游标下推到起点、末页不物化），
/// 但**剩下那一笔是结构性的**：offset 的第 D 页必须从库头**跳过 D×len 行** ⇒ 单价 ∝ D，
/// 连翻 K 页是 **O(K²)**；游标每页只付"上一页之后那 len 行" ⇒ 连翻 K 页是 **O(K)**。
/// ⇒ "深翻慢"的正解不在库里，在**调用方选哪条门**上。这一格就是把那两条门的价当场读出来，
///   用来支撑"默认姿势 = 游标翻页"那句写进文档的建议。
///
/// 三段读数（同一份库、同一深度）：
/// ① 单页到末页：`A(offset, 免 total)` vs `B(游标从库头走到同一页)` —— 这两条**应当差不多**，
///    它们钉的是"换成游标不会更贵"这一半（如果 B 明显更贵，那句建议就不该写）；
/// ② 连翻 K 页：`A = K 次 search_keys(各自跳过)` vs `B = 一次游标走 K 页` —— 这才是二次方与线性的差，
///    报总耗时与 **µs/页**；两臂各跑两遍取最小（本机墙钟跨轮漂 1.3~1.9×，比值才可信）。
/// ③ 硬判据一条：**两条门交出的必须是同一页**（键序列逐条相等）—— 不等就说明"同一深度"这个前提没立住，
///    那上面两个比值全都无意义。
///
/// ```text
/// DEEP_PAGE_ROWS=600000 cargo test --release --lib -- --ignored --nocapture \
///     --test-threads=1 deep_paging_posture_ab
/// ```
#[test]
#[ignore = "深翻两条门的价：手动 release 单线程跑：DEEP_PAGE_ROWS=600000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 deep_paging_posture_ab"]
fn deep_paging_posture_ab() {
    let rows: usize = std::env::var("DEEP_PAGE_ROWS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200_000);
    let page_size: usize = std::env::var("DEEP_PAGE_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);
    let k: usize = std::env::var("DEEP_PAGE_K")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50);
    fn key(i: usize) -> Vec<u8> {
        format!("{i:08}#/worldquant/datafields/x.json").into_bytes()
    }
    fn val(i: usize) -> Vec<u8> {
        format!("{{\"i\":{i},\"h\":\"{:0<28}\",\"v\":1}}", i * 7).into_bytes()
    }
    let pages_total = rows / page_size;
    let last = pages_total - 1;
    eprintln!(
        "=== M58 深翻两条门 rows={rows} 页宽={page_size} 总页数={pages_total} 连翻 K={k} ==="
    );

    for kind in [
        EngineKind::Bitcask,
        EngineKind::BPlusTree,
        EngineKind::LsmTree,
        EngineKind::Memory,
    ] {
        let label = kind.as_str();
        let dir = tmp_dir(&format!("deep_page_{label}"));
        let arg = match kind {
            EngineKind::Memory => None,
            EngineKind::BPlusTree => Some(dir.join("bplus.db")),
            _ => Some(dir.to_path_buf()),
        };
        let arg = arg.as_deref();
        let db = open_engine(kind, arg).unwrap();
        for base in (0..rows).step_by(1000) {
            let end = (base + 1000).min(rows);
            let items: Vec<(Vec<u8>, Vec<u8>)> = (base..end).map(|i| (key(i), val(i))).collect();
            let tx = db.begin();
            if let Err(bad) = tx.batch_set(&items) {
                panic!("{label}: batch_set 冲突 {}", String::from_utf8_lossy(&bad));
            }
            tx.commit();
        }
        db.flush().unwrap();
        db.checkpoint().unwrap();

        // ① 单页到末页：A = offset，B = 游标从库头走到同一页
        let (d_a, _, pk_a, pa) = timed(|| {
            let tx = db.begin();
            tx.search_keys(
                &crate::common::SearchQuery::contains(b"", last, page_size).without_total(),
            )
        });
        let (d_b, _, pk_b, pb) = timed(|| {
            let tx = db.begin();
            let mut cursor: Option<Vec<u8>> = None;
            let mut chunk: Vec<crate::common::ExportRecord> = Vec::new();
            for _ in 0..=last {
                let (c, next) = tx.scan_page(cursor.as_deref(), page_size);
                chunk = c;
                let Some(n) = next else { break };
                cursor = Some(n);
            }
            chunk
        });
        // 硬判据：两条门必须交出**同一页**（不然下面那个比值没有意义）
        let ka: Vec<Vec<u8>> = pa.items.iter().map(|r| r.key.clone()).collect();
        let kb: Vec<Vec<u8>> = pb.iter().map(|r| r.key.clone()).collect();
        assert_eq!(ka, kb, "{label}: offset 末页与游标末页不是同一页");

        // ② 连翻 K 页：A = K 次 offset 翻页（每次各自从头跳过），B = 一次游标走 K 页
        let mut best_a = f64::MAX;
        let mut best_b = f64::MAX;
        for _ in 0..2u8 {
            let (d, _, _, got) = timed(|| {
                let tx = db.begin();
                let mut n = 0usize;
                for j in 0..k {
                    n += tx
                        .search_keys(
                            &crate::common::SearchQuery::contains(b"", j, page_size)
                                .without_total(),
                        )
                        .items
                        .len();
                }
                n
            });
            best_a = best_a.min(d.as_secs_f64() * 1e6);
            let (d, _, _, n) = timed(|| {
                let tx = db.begin();
                let mut cursor: Option<Vec<u8>> = None;
                let mut n = 0usize;
                for _ in 0..k {
                    let (chunk, next) = tx.scan_page(cursor.as_deref(), page_size);
                    n += chunk.len();
                    let Some(x) = next else { break };
                    cursor = Some(x);
                }
                n
            });
            best_b = best_b.min(d.as_secs_f64() * 1e6);
            assert_eq!(got, k * page_size, "{label}: offset 连翻 K 页行数不对");
            assert_eq!(n, k * page_size, "{label}: 游标连翻 K 页行数不对");
        }
        eprintln!(
            "{label:<10} 单页到末页(第 {last} 页) A(offset) {:>9.1} ms 峰值+{:.1} MB ｜ B(游标走到同一页) {:>9.1} ms 峰值+{:.1} MB ｜ B/A = {:.2}×\n\
                 {:<10} 连翻 K={k} 页        A 总 {:>9.1} ms（{:.0} µs/页）｜ B 总 {:>9.1} ms（{:.0} µs/页）⇒ **A/B = {:.1}×**",
            d_a.as_secs_f64() * 1e3,
            pk_a as f64 / 1e6,
            d_b.as_secs_f64() * 1e3,
            pk_b as f64 / 1e6,
            d_b.as_secs_f64() / d_a.as_secs_f64(),
            "",
            best_a / 1e3,
            best_a / k as f64,
            best_b / 1e3,
            best_b / k as f64,
            best_a / best_b,
        );
        if kind != EngineKind::Memory {
            let _ = std::fs::remove_dir_all(&dir);
        }
    }
    eprintln!(
        "⚠️ 墙钟只当量级（本机跨轮漂 1.3~1.9×、且 twosync 那条 `cargo bench` 今晚也在跑），要用的数是 A/B 比值。"
    );
}

/// M75 第一步：**只量不改** —— 标准表里 `search 末页(深翻)` 那 7.46 s 到底花在哪几段。
///
/// 那一格的写法是 `contains(b"", last_page, 100)` 且**要精确 total** ⇒ 它一次付三笔：
/// ① 数全库要 total；② offset 跳过 `last_page × 页宽` 行；③ 取那一页。
/// 分开量才知道该修哪一段（M51 治的是"把前面每一页都攒在堆上"的**内存**，没动时间；
/// M58 只是把默认姿势写成游标，没让 offset 变便宜）。
///
/// 每段两遍取最小（本机单样本 µs 不可信，M50/M62b 那条规矩）；内存列不看这里，标准表有。
///
/// **M75b 之后这张表怎么读**（改的是判据，不是台架）：①②③在同一次进程里跑 ⇒ 是**同构建 A/B**。
/// 旧形状 `③ ≈ ① + ②`（实测差 −3.7%~+5.3%，即"一次调用走两遍全库"）；合成一趟之后判据换成
/// **`③/① ≈ 1×`** —— 这一列打在哪一臂上，一眼就能看出是不是又分家成两趟了。
///
/// 跑法：`SCALE_ROWS=600000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 deep_page_stage_split`
#[test]
#[ignore = "深翻分段归因，手动 release 单线程跑：SCALE_ROWS=600000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 deep_page_stage_split"]
fn deep_page_stage_split() {
    let rows: usize = std::env::var("SCALE_ROWS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(600_000);
    let page_size = 100usize;
    let last_page = scale_last_page_index(rows, page_size);

    fn key(i: usize) -> Vec<u8> {
        format!("{i:08}#/worldquant/datafields/x.json").into_bytes()
    }
    fn val(i: usize) -> Vec<u8> {
        format!("{{\"i\":{i},\"h\":\"{:0<28}\",\"v\":1}}", i * 7).into_bytes()
    }
    /// 跑 n 遍取最小；返回 (最快耗时, 最后一次的结果)
    fn min_of<T>(n: usize, mut f: impl FnMut() -> T) -> (f64, T) {
        let mut best = f64::MAX;
        let mut last = None;
        for _ in 0..n {
            let t = std::time::Instant::now();
            let out = f();
            let us = t.elapsed().as_secs_f64() * 1e6;
            if us < best {
                best = us;
            }
            last = Some(out);
        }
        (best, last.unwrap())
    }

    eprintln!(
        "=== 深翻分段归因 rows={rows} 页宽={page_size} 末页号={last_page}（每段两遍取最小，µs）==="
    );
    for kind in [
        EngineKind::Bitcask,
        EngineKind::BPlusTree,
        EngineKind::LsmTree,
        EngineKind::Memory,
    ] {
        let label = kind.as_str();
        let dir = tmp_dir(&format!("deep_page_{label}"));
        let arg = match kind {
            EngineKind::Memory => None,
            EngineKind::BPlusTree => Some(dir.join("bplus.db")),
            _ => Some(dir.to_path_buf()),
        };
        let db = open_engine(kind, arg.as_deref()).unwrap();
        for base in (0..rows).step_by(1000) {
            let end = (base + 1000).min(rows);
            let items: Vec<(Vec<u8>, Vec<u8>)> = (base..end).map(|i| (key(i), val(i))).collect();
            let tx = db.begin();
            if let Err(bad) = tx.batch_set(&items) {
                panic!("{label}: batch_set 冲突 {}", String::from_utf8_lossy(&bad));
            }
            tx.commit();
        }
        db.flush().unwrap();
        db.checkpoint().unwrap();

        // ① 只要 total，不翻页（页宽 1、第 0 页）⇒ 这一格 ≈ 数全库的净代价
        let (t_total, one_row) = min_of(2, || {
            let tx = db.begin();
            let p = tx.search_keys(&crate::common::SearchQuery::contains(b"", 0, 1));
            tx.commit();
            p
        });
        // ② 免 total、翻到末页 ⇒ 这一格 ≈ offset 跳过的净代价（+ 取 100 条）
        let (t_skip, _) = min_of(2, || {
            let tx = db.begin();
            let p = tx.search_keys(
                &crate::common::SearchQuery::contains(b"", last_page, page_size).without_total(),
            );
            tx.commit();
            p
        });
        // ③ 两笔都要 = 标准表里那一格
        let (t_both, deep) = min_of(2, || {
            let tx = db.begin();
            let p = tx.search_keys(&crate::common::SearchQuery::contains(
                b"", last_page, page_size,
            ));
            tx.commit();
            p
        });
        // ④ 游标一跳（拿到末页起点的游标之后，只量**一跳**）
        let (_warm, next) = {
            let tx = db.begin();
            let out = tx.scan_page(None, last_page * page_size);
            tx.commit();
            out
        };
        let (t_hop, hopped) = min_of(2, || {
            let tx = db.begin();
            let out = tx.scan_page(next.as_deref(), page_size);
            tx.commit();
            out
        });
        // ⑤ 对照：纯数一遍（不取任何键值）
        let (t_count, n_keys) = min_of(2, || {
            let tx = db.begin();
            let c = tx.key_count(&key(0)[..2]);
            tx.commit();
            c
        });

        eprintln!(
            "{label:<10} ①total(页宽1) {t_total:>11.1}  ②offset免total {t_skip:>11.1}  \
                 ③两者都要(表里那格) {t_both:>11.1}  ④游标一跳 {t_hop:>9.1}  ⑤key_count {t_count:>11.1}"
        );
        eprintln!(
            "{:<10}   折算：①每键 {:.3} µs  ②每跳过行 {:.3} µs  ③/① {:.2}×  ③=①+②? 差 {:.1}%  \
                 ④单页 {} 条(命中末页={})  ⑤数到 {} 键",
            "",
            (t_total - one_row.items.len() as f64 * 0.0) / rows as f64,
            t_skip / (last_page * page_size) as f64,
            if t_total > 0.0 { t_both / t_total } else { 0.0 },
            if t_both > 0.0 {
                (t_both - (t_total + t_skip)) / t_both * 100.0
            } else {
                0.0
            },
            hopped.0.len(),
            deep.items.len(),
            n_keys,
        );
        drop(db);
        cleanup(vec![Some(dir)]);
    }
    eprintln!("DEEP PAGE DONE");
}

/// M62b：**单次调用**的跨档比值，把**结构计数**摆在一起看（`#[ignore]`，手动 release 单线程跑）。
///
/// 为什么要单独一把尺子：复测表里 memory `seek_prev` 读 2.52×（17.8 → 44.8 µs/次）、
/// lsm `vacuum_bounded 一片` 读 2.21×（2.08 → 4.61 ms/片），而这两格在 `scale_baseline` 里都是
/// `timed()` 里的**一次调用** ⇒ 按 M50 那条规矩（µs 级单次采样在本机不作 A/B 凭据）它们只能算"登记"。
/// 这里把同一次动作量 **2000 次**、并把计数尺一起交出来：
/// - **memory**：`seek_prev` 往回退过几把键（`common::seek_prev_steps`）。memory 不落盘、没有"缓存容量"
///   那一格 ⇒ 若步数平而 µs 涨，涨的只能是库变大之后的**内存局部性**（BTreeMap 更高、更散），不是新通道。
/// - **lsm**：一片实际交出/访问多少行（`sst::light_visits`）、读了几块（`sst::block_reads`）、
///   开了几个文件（`common::disk::file_opens`）+ 盘上有几篇 SST。M29 那句"一片的预算行数是常数"要在这里当场可见，
///   而"每多一篇文件就多一次冷启动"那笔（M15b）必须落在**文件数/块数**上才算归因完成。
/// - **lsm `seek_prev`**（M78 阶梯加的那条臂）：大档阶梯里这一格 60 万 → 1000 万读 **28.5×**（391.5 →
///   11150.2 µs/次）而行数只涨 16.7×，是四家里唯一"单次调用的钱自己在超线性涨"的格子 ⇒ 必须当场看
///   步数/块数/开文件数有没有跟着涨，才能分清"结构（文件/层数）"还是"局部性"。
///
/// 判据只看计数；µs 两轮取最小只用来把数字摆在一起（本机跨轮漂 1.3~1.9×）。
///
/// ```text
/// SINGLE_SHAPE_ROWS=20000,600000 cargo test --release --lib -- --ignored --nocapture \
///   --test-threads=1 single_call_shape_attribution
/// ```
#[test]
#[ignore = "单次调用的结构归因，手动 release 单线程跑：SINGLE_SHAPE_ROWS=20000,600000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 single_call_shape_attribution"]
fn single_call_shape_attribution() {
    let tiers: Vec<usize> = std::env::var("SINGLE_SHAPE_ROWS")
        .unwrap_or_else(|_| "20000,600000".to_string())
        .split(',')
        .filter_map(|s| s.trim().parse::<usize>().ok())
        .collect();
    // 形状照抄 scale_baseline：键 ≈35 B、值 ≈60 B、1000 行一事务、再更新前 10% 的键（⇒ 版本链长 2）
    fn key(i: usize) -> Vec<u8> {
        format!("{i:08}#/worldquant/datafields/x.json").into_bytes()
    }
    fn val(i: usize) -> Vec<u8> {
        format!("{{\"i\":{i},\"h\":\"{:0<28}\",\"v\":1}}", i * 7).into_bytes()
    }
    const CALLS: usize = 2000;
    const SLICES: usize = 3;
    eprintln!(
        "=== 单次调用的结构归因 tiers={tiers:?}（seek_prev {CALLS} 次 × 2 轮；一片 × {SLICES} 次）==="
    );
    for &rows in &tiers {
        let updates = (rows / 10).max(1);
        // ---------------- memory：seek_prev ----------------
        {
            let db = open_engine(EngineKind::Memory, None).unwrap();
            for base in (0..rows).step_by(1000) {
                let end = (base + 1000).min(rows);
                let items: Vec<(Vec<u8>, Vec<u8>)> =
                    (base..end).map(|i| (key(i), val(i))).collect();
                let tx = db.begin();
                tx.batch_set(&items).unwrap();
                tx.commit();
            }
            for base in (0..updates).step_by(500) {
                let end = (base + 500).min(updates);
                let items: Vec<(Vec<u8>, Vec<u8>)> = (base..end)
                    .map(|i| (key(i), format!("u{i}").into_bytes()))
                    .collect();
                let tx = db.begin();
                tx.batch_set(&items).unwrap();
                tx.commit();
            }
            let mid = key(rows / 2);
            // 焐一次：第一次要把那一带的页/节点摸进缓存，与台架里"冷单次"不同形状
            {
                let tx = db.begin();
                assert!(tx.seek_prev(&mid).is_some(), "seek_prev 必须命中中间那把键");
            }
            let s0 = crate::common::store::seek_prev_steps();
            let mut best = f64::MAX;
            let mut last = f64::MAX;
            for _ in 0..2 {
                let t = std::time::Instant::now();
                for _ in 0..CALLS {
                    let tx = db.begin();
                    assert!(tx.seek_prev(&mid).is_some());
                }
                last = t.elapsed().as_secs_f64() * 1e6 / CALLS as f64;
                best = best.min(last);
            }
            let steps = crate::common::store::seek_prev_steps() - s0;
            eprintln!(
                "[memory rows={rows}] seek_prev：{best:.1} µs/次（另一轮 {last:.1}）｜ 步/次 = {:.2} \
                     （累计 {steps} ÷ {}）",
                steps as f64 / (CALLS as f64 * 2.0),
                CALLS * 2,
            );
        }
        // ---------------- lsm：vacuum_bounded 一片 ----------------
        {
            let dir = tmp_dir("shape_lsm");
            let db = open_engine(EngineKind::LsmTree, Some(&dir)).unwrap();
            for base in (0..rows).step_by(1000) {
                let end = (base + 1000).min(rows);
                let items: Vec<(Vec<u8>, Vec<u8>)> =
                    (base..end).map(|i| (key(i), val(i))).collect();
                let tx = db.begin();
                tx.batch_set(&items).unwrap();
                tx.commit();
            }
            for base in (0..updates).step_by(500) {
                let end = (base + 500).min(updates);
                let items: Vec<(Vec<u8>, Vec<u8>)> = (base..end)
                    .map(|i| (key(i), format!("u{i}").into_bytes()))
                    .collect();
                let tx = db.begin();
                tx.batch_set(&items).unwrap();
                tx.commit();
            }
            db.flush().unwrap();
            // lsm 的 seek_prev：阶梯里这一格从 391.5 µs/次（60 万）涨到 11150.2 µs/次（1000 万）= 28.5×，
            // 而行数只涨 16.7× ⇒ 单次调用的钱自己在超线性涨。时间和"退了几把键 / 几次轻访问 / 读几块 /
            // 开几个文件"钉在同一行，判据只看计数（本机跨轮墙钟漂 1.3~2.2×，µs 只用来把数字摆一起）。
            {
                let mid = key(rows / 2);
                {
                    let tx = db.begin();
                    assert!(tx.seek_prev(&mid).is_some(), "seek_prev 必须命中中间那把键");
                }
                crate::common::disk::file_opens_clear();
                let st0 = crate::common::store::seek_prev_steps();
                let lv0 = crate::lsm_tree::sst::light_visits();
                let br0 = crate::lsm_tree::sst::block_reads();
                let bm0 = crate::lsm_tree::sst::block_misses();
                // 篇数要在**做这次 seek 之前**数：后面那次全库 vacuum 会重排文件，
                // 用它的读数归因这次的块数就是拿两把尺子相减。
                let files = std::fs::read_dir(dir.join("sst"))
                    .map(|rd| rd.filter_map(|e| e.ok()).count())
                    .unwrap_or(0);
                let mut best = f64::MAX;
                let mut last = f64::MAX;
                for _ in 0..2 {
                    let t = std::time::Instant::now();
                    for _ in 0..CALLS {
                        let tx = db.begin();
                        assert!(tx.seek_prev(&mid).is_some());
                    }
                    last = t.elapsed().as_secs_f64() * 1e6 / CALLS as f64;
                    best = best.min(last);
                }
                let n = (CALLS * 2) as f64;
                let per = |v: u64| v as f64 / n;
                eprintln!(
                    "[lsm rows={rows}] seek_prev：{best:.1} µs/次（另一轮 {last:.1}）｜ 盘上 {files} 篇 SST ｜ \
                         步/次 = {:.2} ｜ 轻访问/次 = {:.2} ｜ 块/次 = {:.2}（其中未命中 {:.2}）｜ 开文件/次 = {:.2}",
                    per(crate::common::store::seek_prev_steps() - st0),
                    per(crate::lsm_tree::sst::light_visits() - lv0),
                    per(crate::lsm_tree::sst::block_reads() - br0),
                    per(crate::lsm_tree::sst::block_misses() - bm0),
                    per(crate::common::disk::file_opens()),
                );
            }
            // 照台架的顺序：先做一次全库 vacuum（所以后面那片"裁 0 行"），再切有界的一片
            let st = db.vacuum().unwrap();
            let ssts = std::fs::read_dir(dir.join("sst"))
                .map(|rd| rd.filter_map(|e| e.ok()).count())
                .unwrap_or(0);
            let h = db.health();
            eprintln!(
                "[lsm rows={rows}] 先全库 vacuum：裁 {} 行 ｜ 库 health rows={} live_keys={} ｜ 盘上 {ssts} 篇 SST",
                st.versions_removed, h.total_rows, h.live_keys
            );
            let mut best = f64::MAX;
            let mut shown = None;
            for n in 1..=SLICES {
                crate::common::disk::file_opens_clear();
                let lv0 = crate::lsm_tree::sst::light_visits();
                let br0 = crate::lsm_tree::sst::block_reads();
                let t = std::time::Instant::now();
                let vr = db.vacuum_bounded(None, 2048).unwrap();
                let us = t.elapsed().as_secs_f64() * 1e6;
                let visits = crate::lsm_tree::sst::light_visits() - lv0;
                let blocks = crate::lsm_tree::sst::block_reads() - br0;
                let opens = crate::common::disk::file_opens();
                best = best.min(us);
                eprintln!(
                    "  第{n} 片：{us:.1} µs ｜ 扫 {} 行、裁 {} 行 ｜ 归并吐出 {visits} 把键 \
                         ｜ 读块 {blocks} ｜ 开文件 {opens}",
                    vr.stats.rows_scanned, vr.stats.versions_removed
                );
                shown = Some((visits, blocks, opens, vr.stats.rows_scanned));
            }
            if let Some((visits, blocks, opens, scanned)) = shown {
                eprintln!(
                    "[lsm rows={rows}] 一片的最小 {best:.1} µs ｜ 每行 = {:.2} µs ｜ 键/行 = {:.2} \
                         ｜ 块/行 = {:.3} ｜ 开文件/行 = {:.4}",
                    best / scanned.max(1) as f64,
                    visits as f64 / scanned.max(1) as f64,
                    blocks as f64 / scanned.max(1) as f64,
                    opens as f64 / scanned.max(1) as f64,
                );
            }
            drop(db);
            let _ = std::fs::remove_dir_all(&dir);
        }
        eprintln!();
    }
}
