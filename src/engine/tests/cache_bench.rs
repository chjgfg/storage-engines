// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// M57 的**价签尺子**：一个缓存槽到底住多少字节（`#[ignore]`，手动 release 跑）。
///
/// 存在的理由：README 里那句"bplus 全驻留 ≈ 47 MB / 143 MB"是 `页数 × 4 KiB` 的**算术**，
/// 而 bplus 每个槽其实住着**两样东西** —— 页缓存里那份原始 4 KiB，加解析缓存里那个
/// `Arc<BTreeNode>`（`bplus_tree.rs:541`：它的上限直接取 `page_manager.cache_capacity()`，
/// 同一个数、两份内存）⇒ 拿 4 KiB 当单价去反推"6 GB 的机器能给多少页"会**系统性低估**。
/// lsm 那一侧的单价也不该抄 README 的 7.4 KB：块大小随值变（5002 键那档均值 7 403 B，
/// 600k 小值那档 ≈ 2 KB）⇒ 要按"预算 → 块数"给建议，单价必须**当场读**，不能引用常数。
///
/// **实测（release，600k 档，M57）**：bplus 每页 走查 **4 264 B** / 乱序点读 **6 838 B**
/// （全驻留总账 151.2 MB vs **237.2 MB**）；lsm 每块 **10.1 KB**，两种姿势一字不差（全驻留 **90.8 MB**）
/// ⇒ README 里那句"lsm 600k 全驻留 ≈ 18.7 MB"是**拿错了档**（那是 M23 里 40 000 键 / 2 681 块那一档）。
///
/// 读数用 `mem_probe::live()`（跟踪分配器的净活字节，不是 RSS）：先用**默认档**把整库焐一遍
/// （填满 cap 个槽），再把顶抬掉焐第二遍（补剩下的槽），两趟各除各的槽数 ⇒ 每槽真实字节。
///
/// ⚠️ **两种姿势的单价不一样，必须分开读**（`CACHE_PRICE_POSTURE=walk|point`）：
/// 走查（`scan_page`）按设计**只填页缓存**——`bplus_tree.rs:1931` 那句"每片叶子只碰一次，回填它不会有下一次命中"
/// ⇒ 它不进解析缓存；而**乱序点读**两把都填（`node_of` → `memoize_node`，上限与页缓存同源）。
/// 所以拿"走查姿势"的每槽字节去给点读型下游算预算会**系统性低估**（最多差一倍）。
/// ⚠️ 墙钟不作凭据（本机跨轮漂 1.3~1.9×），这一格只卖字节。
///
/// ```text
/// CACHE_PRICE_ROWS=600000 CACHE_PRICE_POSTURE=point cargo test --release --lib -- --ignored --nocapture \
///     --test-threads=1 cache_slot_resident_price
/// ```
#[test]
#[ignore = "缓存槽价签：手动 release 单线程跑：CACHE_PRICE_ROWS=200000 CACHE_PRICE_POSTURE=point cargo test --release --lib -- --ignored --nocapture --test-threads=1 cache_slot_resident_price"]
fn cache_slot_resident_price() {
    let rows: usize = std::env::var("CACHE_PRICE_ROWS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200_000);
    fn key(i: usize) -> Vec<u8> {
        format!("{i:08}#/worldquant/datafields/x.json").into_bytes()
    }
    fn val(i: usize) -> Vec<u8> {
        format!("{{\"i\":{i},\"h\":\"{:0<28}\",\"v\":1}}", i * 7).into_bytes()
    }
    let mb = |b: i64| b as f64 / 1024.0 / 1024.0;

    for kind in [EngineKind::BPlusTree, EngineKind::LsmTree] {
        let label = kind.as_str();
        let dir = tmp_dir(&format!("cache_price_{label}"));
        let arg = match kind {
            EngineKind::BPlusTree => Some(dir.join("bplus.db")),
            _ => Some(dir.to_path_buf()),
        };
        let arg = arg.as_deref();
        {
            let db = open_engine(kind, arg).unwrap();
            for base in (0..rows).step_by(1000) {
                let end = (base + 1000).min(rows);
                let items: Vec<(Vec<u8>, Vec<u8>)> =
                    (base..end).map(|i| (key(i), val(i))).collect();
                let tx = db.begin();
                if let Err(bad) = tx.batch_set(&items) {
                    panic!("{label}: batch_set 冲突 {}", String::from_utf8_lossy(&bad));
                }
                tx.commit();
            }
            db.flush().unwrap();
            db.checkpoint().unwrap();
        }
        // 冷重开 ⇒ 缓存是空的（焐之前一个槽都没有）
        let db = open_engine(kind, arg).unwrap();
        let t = db.tuning();
        let (cap, denom) = match kind {
            EngineKind::BPlusTree => (
                t.get("max_cache_pages").unwrap_or("0").to_string(),
                t.get("total_pages").unwrap_or("0").to_string(),
            ),
            _ => (
                t.get("block_cache_capacity").unwrap_or("0").to_string(),
                t.get("sst_blocks").unwrap_or("0").to_string(),
            ),
        };
        let cap_n: usize = cap.parse().unwrap_or(0);
        let denom_n: usize = denom.parse().unwrap_or(0);
        // ⚠️ `order` 必须**在取 `l0` 之前**分配：它是 600k × 8 B = 4.8 MB 的活字节，
        // 晚分配就把它算进每一趟的差值里（默认档那趟 4.8 MB ÷ 2048 槽 ⇒ 每槽虚高 2.3 KB，
        // 正好足够把"要不要抬顶"的结论推翻）。
        let order = shuffled(rows);
        let posture = std::env::var("CACHE_PRICE_POSTURE").unwrap_or_else(|_| "walk".to_string());
        let l0 = crate::mem_probe::live() as i64;
        // 一次焐满：走查姿势只填页缓存，点读姿势两把都填（见上面那段）
        let do_fill = |db: &dyn KvEngine, count: bool| -> usize {
            let mut touched = 0usize;
            if posture == "point" {
                let tx = db.begin();
                for &i in &order {
                    if tx.get(&key(i)).is_some() {
                        touched += 1;
                    }
                }
            } else {
                let mut after: Option<Vec<u8>> = None;
                loop {
                    let tx = db.begin();
                    let (page, next) = tx.scan_page(after.as_deref(), 512);
                    if count {
                        touched += page.len();
                    }
                    drop(page);
                    match next {
                        Some(n) => after = Some(n),
                        None => break,
                    }
                }
            }
            touched
        };
        // 第一趟：默认档焐满 cap 个槽
        let seen = do_fill(db.as_ref(), true);
        let l1 = crate::mem_probe::live() as i64;
        // 第二趟：把顶抬掉再焐一遍 ⇒ 增量就是"补上的那些槽"的字节
        db.set_cache_auto_max(usize::MAX).unwrap();
        let _ = do_fill(db.as_ref(), false);
        let l2 = crate::mem_probe::live() as i64;
        let t2 = db.tuning();
        let cap2: usize = match kind {
            EngineKind::BPlusTree => t2.get("max_cache_pages"),
            _ => t2.get("block_cache_capacity"),
        }
        .unwrap_or("0")
        .parse()
        .unwrap_or(0);
        let filled_a = cap_n.min(denom_n).max(1);
        let filled_b = cap2.saturating_sub(filled_a);
        eprintln!(
            "{label:<10} [{posture}] 默认档 cap={cap_n} 分母={denom_n} 走 {seen} 行 ｜ 焐满 cap 个槽：live {:+.1} MB ⇒ \
                 **每槽 {:.0} B**（抬顶后共住 {cap2} 槽、再多 {filled_b} 个槽花 {:+.1} MB ⇒ 每槽 {:.0} B ｜ 全驻留共 {:+.1} MB）",
            mb(l1 - l0),
            (l1 - l0) as f64 / filled_a as f64,
            mb(l2 - l1),
            if filled_b > 0 {
                (l2 - l1) as f64 / filled_b as f64
            } else {
                0.0
            },
            mb(l2 - l0),
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
    eprintln!(
        "⚠️ 姿势不同、价就不同：走查只填**页缓存**（`bplus_tree.rs:1931` 那句\"回填它不会有下一次命中\"），\
             点读两把都填 ⇒ 实测 bplus 每页 走查 4.2 KB / 点读 6.8 KB（lsm 10.1 KB，姿势无关）。\
             给点读型下游定预算要用 point 那一读数，别按 4 KiB 估。"
    );
}

/// M76 的验收台架：按**内存预算**给缓存档之后，实测常驻必须真的**不超过给的那些字节**
/// （`#[ignore]`，手动 release 单线程跑）。
///
/// 为什么这一格不能在单元守卫里量：守卫钉的是**槽数**（`容量 = 预算 ÷ 每槽价签`、封顶被执行），
/// 而"预算 = 内存上限"这句话的最终凭据是**真实字节** —— 那要 `mem_probe::live()`，
/// 它是进程级计数，并行用例互相污染（M27/M36/M52 各栽过一次），只能单线程 release 跑。
///
/// 两把尺子必须对上，这才算机制而不是巧合：
/// ① 换算尺（守卫里）：`槽数 == 预算 ÷ 价签`；
/// ② 字节尺（这里）：`焐满之后 live 增量 ≤ 预算 × 1.25`，并顺带读出**这一档的每槽实测价** ——
///   它比价签小 ⇒ 价签是保守的（宁少不多，符合"预算不许超"）；它比价签大 ⇒ 换算在超支，红。
///
/// ```text
/// CACHE_BUDGET_ROWS=600000 CACHE_BUDGET_MB=32 CACHE_BUDGET_POSTURE=point cargo test --release --lib -- \
///     --ignored --nocapture --test-threads=1 cache_budget_bytes_acceptance
/// ```
#[test]
#[ignore = "预算档验收：手动 release 单线程跑：CACHE_BUDGET_ROWS=600000 CACHE_BUDGET_MB=32 cargo test --release --lib -- --ignored --nocapture --test-threads=1 cache_budget_bytes_acceptance"]
fn cache_budget_bytes_acceptance() {
    let rows: usize = std::env::var("CACHE_BUDGET_ROWS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(600_000);
    let budget_mb: u64 = std::env::var("CACHE_BUDGET_MB")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(32);
    let budget = budget_mb * 1_000_000;
    let posture = std::env::var("CACHE_BUDGET_POSTURE").unwrap_or_else(|_| "point".to_string());
    fn key(i: usize) -> Vec<u8> {
        format!("{i:08}#/worldquant/datafields/x.json").into_bytes()
    }
    fn val(i: usize) -> Vec<u8> {
        format!("{{\"i\":{i},\"h\":\"{:0<28}\",\"v\":1}}", i * 7).into_bytes()
    }
    let mb = |b: i64| b as f64 / 1024.0 / 1024.0;

    eprintln!("=== 预算档验收 rows={rows} 预算={budget_mb} MB({budget} B) 姿势={posture} ===");
    for kind in [EngineKind::BPlusTree, EngineKind::LsmTree] {
        let label = kind.as_str();
        let dir = tmp_dir(&format!("cache_budget_accept_{label}"));
        let arg = match kind {
            EngineKind::BPlusTree => Some(dir.join("bplus.db")),
            _ => Some(dir.to_path_buf()),
        };
        let arg = arg.as_deref();
        {
            let db = open_engine(kind, arg).unwrap();
            for base in (0..rows).step_by(1000) {
                let end = (base + 1000).min(rows);
                let items: Vec<(Vec<u8>, Vec<u8>)> =
                    (base..end).map(|i| (key(i), val(i))).collect();
                let tx = db.begin();
                if let Err(bad) = tx.batch_set(&items) {
                    panic!("{label}: batch_set 冲突 {}", String::from_utf8_lossy(&bad));
                }
                tx.commit();
            }
            db.flush().unwrap();
            db.checkpoint().unwrap();
        }
        // 冷重开 ⇒ 缓存空着，量出来的才是"这一档买了多少字节"
        let db = open_engine(kind, arg).unwrap();
        // ⚠️ `order` 必须在取基线**之前**分配（M57 那条：晚分配就把它算进差值，每槽虚高 2.3 KB）
        let order = shuffled(rows);
        let slots = db.set_cache_budget_bytes(budget).expect("设预算不该报错");
        let t = db.tuning();
        let cap_key = if kind == EngineKind::BPlusTree {
            "max_cache_pages"
        } else {
            "block_cache_capacity"
        };
        let cap: usize = t.get(cap_key).unwrap_or("0").parse().unwrap_or(0);
        let denom: usize = t
            .get(if kind == EngineKind::BPlusTree {
                "total_pages"
            } else {
                "sst_blocks"
            })
            .unwrap_or("0")
            .parse()
            .unwrap_or(0);
        assert_eq!(
            cap, slots,
            "{label}: 返回的槽数与 tuning 里的生效容量不是同一个数"
        );
        let l0 = crate::mem_probe::live() as i64;
        if posture == "point" {
            let tx = db.begin();
            for &i in &order {
                assert!(tx.get(&key(i)).is_some(), "{label}: 点读落空");
            }
        } else {
            let mut after: Option<Vec<u8>> = None;
            loop {
                let tx = db.begin();
                let (page, next) = tx.scan_page(after.as_deref(), 512);
                drop(page);
                match next {
                    Some(n) => after = Some(n),
                    None => break,
                }
            }
        }
        let l1 = crate::mem_probe::live() as i64;
        let held = l1 - l0;
        eprintln!(
            "{label:<10} [{posture}] 预算 {budget} B ⇒ 容量 {cap} 槽（分母 {denom}，覆盖率 {:.1}%）｜\
                 焐满之后 live {:+.1} MB ⇒ **每槽实测 {:.0} B**（换算按价签，所以这一档只可能少买不可能多买）",
            if denom > 0 {
                cap as f64 / denom as f64 * 100.0
            } else {
                0.0
            },
            mb(held),
            if cap > 0 {
                held as f64 / cap as f64
            } else {
                0.0
            },
        );
        assert!(
            held <= (budget as i64) * 5 / 4,
            "{label}: 给了 {budget} B 的预算，焐满之后实测住了 {held} B（>预算×1.25）⇒ \
                 要么价签偏小、要么封顶没被执行 ⇒ 下游照这个数封 RSS 会爆",
        );
        drop(db);
        let _ = std::fs::remove_dir_all(&dir);
    }
    eprintln!("CACHE BUDGET DONE");
}
