// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// M24 守卫：**写集登记每写一行只许考察一把键**，不许遍历写集。
///
/// 老形状是 `writes.iter().any(|k| k == key)` ⇒ **单个事务内** O(写集²)（2026-10-17 记在案的那条：
/// 它不是灌行超线性的主导项，因为台架每 1000 行一事务，但"一个大事务塞几万行"就浮出来 ——
/// 那正是下游导历史数据的形状）。现在写集是 `HashSet`，查重与部分撤销的减法都走哈希。
///
/// 尺子 [`crate::common::txn::write_set_scans`] 长在**唯一的写集入口** `register_write` /
/// `unregister_write` 里，数的是"为判重复考察过多少把已登记的键"：哈希每次 1，遍历每次
/// 是写集当前长度。断的是**精确值**（等号），所以"先 `contains` 再 `insert"` 这种双倍查重
/// 也会报红。⚠️ 同一条用例里用 [`crate::common::txn::write_set_contains_linear`] **主动慢数一次**
/// 当对照：既证明这把尺子不是空转，也当场量出退化版本一次查重要走多少把键（M19 同一手法）。
///
/// ⚠️ **这把尺子有一处已知的盲角，所以另配一条代价尺**：探针写在 `register_write` 里面，
/// 谁把算法改回遍历、却忘了搬那行计数，计数照样读 1 次/行。堵它的是 `big_transaction_write_cost`
/// （墙钟台架，`#[ignore]`）—— 实测"只改算法、不改探针"那份退化在 bitcask 报头 200 行 9.3 /
/// 尾 200 行 **288.8 µs/行（30.9×）**当场红，而修好的形状是 0.48~1.37×。
/// 一条盯形状、一条盯代价：缺一条就有一种退化溜过去。
#[test]
fn write_set_registration_costs_a_hash_not_a_walk() {
    const K: usize = 2000;
    for entry in open_all() {
        // 整条 `entry` 不拆开：元组字段按 0→1→2 析构 ⇒ 引擎先落地、scratch 守卫后删目录。
        // 拆成 `for (name, db, dir)` 会按反向声明序走、守卫先跑 —— 而 Windows 上引擎还开着的
        // `.log`/`.lock` 那时删不掉，目录就白留在 `%TEMP%` 里（M82）。
        let (name, db) = (entry.0, &*entry.1);
        let s0 = crate::common::txn::write_set_scans();
        {
            let tx = db.begin();
            for i in 0..K {
                assert!(
                    tx.set(format!("w{i:05}").as_bytes(), b"v".to_vec()),
                    "{name}: 灌第 {i} 把"
                );
            }
            // 同一把键反复写：一个事务里只算一行账，而成本也只该多一次查重
            for _ in 0..10 {
                tx.set(b"w00000", b"v2".to_vec());
            }
            tx.commit();
        }
        let scans = crate::common::txn::write_set_scans() - s0;
        eprintln!(
            "[写集] {name}: {scans} 次查重 / {} 次写（老写法这里会是 {} 量级）",
            K + 10,
            K * K / 2
        );
        assert_eq!(
            scans,
            (K + 10) as u64,
            "{name}: 登记 {} 次却考察了 {scans} 把已登记的键 ⇒ 查重不再走哈希\
                 （等号是刻意的：多出来的那次 `contains` 也算一次退化）",
            K + 10
        );

        // 减法那条链（批量写撞墙后的部分撤销）同样不许遍历写集：
        // 49 把落地 + 第 50 把冲突 ⇒ 49 次登记 + 49 次摘账
        let holder = db.begin();
        assert!(
            holder.set(b"zzz_conflict", b"h".to_vec()),
            "{name}: holder 占住键"
        );
        let mut items: Vec<(Vec<u8>, Vec<u8>)> = (0..49)
            .map(|i| (format!("b{i:03}").into_bytes(), b"v".to_vec()))
            .collect();
        items.push((b"zzz_conflict".to_vec(), b"v".to_vec()));
        let s1 = crate::common::txn::write_set_scans();
        let (got, back) = {
            let tx = db.begin();
            let err = tx
                .batch_set(&items)
                .expect_err("{name}: batch_set 必须在占住的键上冲突");
            let walked = crate::common::txn::write_set_scans() - s1;
            tx.rollback();
            (err, walked)
        };
        holder.rollback();
        assert_eq!(got, b"zzz_conflict".to_vec(), "{name}: 冲突键");
        eprintln!("[写集 减法] {name}: 49 落账 + 49 撤账 = {back} 次查重");
        assert_eq!(
            back, 98,
            "{name}: 一批 49 把键的落账+撤账共考察 {back} 把 ⇒ 摘写集又在遍历写集了"
        );

        // 语义没被换掉：反复写的那把键读得到最后一次的值，事务收尾不留活跃事务
        let tx = db.begin();
        assert_eq!(
            tx.get(b"w00000").as_deref(),
            Some(&b"v2"[..]),
            "{name}: 去重不许丢掉后写的值"
        );
        assert_eq!(
            tx.get(b"w01999").as_deref(),
            Some(&b"v"[..]),
            "{name}: 集合里最后一把键必须照样提交（去重不许吞行）"
        );
        // 批量里第 50 把冲突 ⇒ 前 49 把（含 b048）必须整体退回，不能留在库里
        assert_eq!(
            tx.get(b"b048"),
            None,
            "{name}: 部分撤销必须把已落地的行退掉"
        );
        assert_eq!(tx.get(b"b000"), None, "{name}: 同上（第一批那把）");
        tx.commit();
        assert_eq!(db.health().active_txns, 0, "{name}: 不许留活跃事务");

        // 对照：慢算法在同规模的写集上，一次查重就走 K 把键 —— 证明这把尺子看得见东西
        let ledger: std::collections::HashSet<Vec<u8>> =
            (0..K).map(|i| format!("w{i:05}").into_bytes()).collect();
        let c0 = crate::common::txn::write_set_scans();
        assert!(
            !crate::common::txn::write_set_contains_linear(&ledger, b"nobody"),
            "{name}: 对照算法本身要能给出正确答案"
        );
        let control = crate::common::txn::write_set_scans() - c0;
        eprintln!("[写集 对照] 老写法一次查重走 {control} 把键（K={K}）");
        assert!(
            control >= K as u64,
            "{name}: 对照的慢算法只走了 {control} 把键 ⇒ 这把尺子空转，断言等于没测"
        );
    }
}

/// M24 的墙钟台架：**一个事务塞几万行**时，头几百行与尾几百行的单价必须同阶。
///
/// 计数守卫 `write_set_registration_costs_a_hash_not_a_walk` 钉的是形状（每写一行只许考察一把键），
/// 这条是把同一个形状的**代价**量出来：老写法（遍历写集判重复）在这里是 O(写集²)，
/// 一个 20 000 行的事务走到第 19 800 行时，一次查重要走过 19 800 把已登记的键。
/// ⚠️ 断言故意给得松（墙钟本机 ±2×，且这里连存储层自己的增长也算在内）——
/// 精确的那半由计数尺守着，这条只报方向。
///
/// ```text
/// BIG_TXN_ROWS=20000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 big_transaction_write_cost
/// ```
#[test]
#[ignore = "大事务墙钟台架，手动 release 单线程跑：BIG_TXN_ROWS=20000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 big_transaction_write_cost"]
fn big_transaction_write_cost() {
    let rows: usize = std::env::var("BIG_TXN_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20000);
    let edge = rows.min(200);
    for entry in open_all() {
        // 整条 `entry` 不拆开：元组字段按 0→1→2 析构 ⇒ 引擎先落地、scratch 守卫后删目录。
        // 拆成 `for (name, db, dir)` 会按反向声明序走、守卫先跑 —— 而 Windows 上引擎还开着的
        // `.log`/`.lock` 那时删不掉，目录就白留在 `%TEMP%` 里（M82）。
        let (name, db) = (entry.0, &*entry.1);
        let tx = db.begin();
        let mut head = 0u128;
        let mut tail = 0u128;
        for i in 0..rows {
            let t = std::time::Instant::now();
            assert!(
                tx.set(format!("k{i:06}").as_bytes(), b"v".to_vec()),
                "{name}: 灌第 {i} 把"
            );
            let spent = t.elapsed().as_nanos();
            if i < edge {
                head += spent;
            } else if i >= rows - edge {
                tail += spent;
            }
        }
        tx.commit();
        let head_us = head as f64 / edge as f64 / 1000.0;
        let tail_us = tail as f64 / edge as f64 / 1000.0;
        eprintln!(
            "[大事务] {name}: 头 {edge} 行 {head_us:.1} µs/行 / 尾 {edge} 行 {tail_us:.1} µs/行\
                 （同一事务共 {rows} 行，比值 {:.2}×）",
            tail_us / head_us.max(0.001)
        );
        assert!(
            tail_us <= head_us * 6.0 + 50.0,
            "{name}: 一个事务里第 {rows} 行的单价 {tail_us:.1} µs 比第 1 行 {head_us:.1} µs 贵这么多 ⇒ \
                 写集登记又在随写集长度涨（O(写集²) 回来了）"
        );
    }
}

#[test]
#[ignore = "vacuum planning old/new/new/old; release single thread"]
fn vacuum_plan_cost() {
    let rows: usize = std::env::var("VACUUM_ROWS")
        .map(|v| v.parse().unwrap())
        .unwrap_or(600_000);
    assert!(rows >= 10);
    let mut allocations = Vec::new();
    for (round, reference) in [true, false, false, true].into_iter().enumerate() {
        let db = crate::memory::memory_mvcc::MVCC::new();
        db.set_vacuum_auto_slice(0);
        for base in (0..rows).step_by(1000) {
            let tx = db.begin_transaction();
            for i in base..(base + 1000).min(rows) {
                assert!(tx.set_raw(&scale_key(i), scale_val(i)));
            }
            tx.commit();
        }
        let tx = db.begin_transaction();
        for i in 0..rows / 10 {
            assert!(tx.set_raw(&scale_key(i), scale_val(i + rows)));
        }
        tx.commit();
        let mode = reference.then(crate::common::version::vacuum_reference::Scope::start);
        let probe = crate::common::maintenance_probe::Scope::start();
        let before = crate::mem_probe::allocations();
        let start = std::time::Instant::now();
        let stats = db.vacuum();
        let seconds = start.elapsed().as_secs_f64();
        let allocs = crate::mem_probe::allocations() - before;
        allocations.push(allocs);
        eprintln!(
            "VACUUM_PLAN round={round} reference={reference} rows={rows} seconds={seconds:.6} allocations={allocs} stages={:?}",
            probe.stats()
        );
        assert_eq!(stats.versions_removed, rows / 10);
        drop(probe);
        drop(mode);
        let mut seen = 0;
        db.export_latest_visible_for_each(false, |record| {
            assert_eq!(record.key, scale_key(seen));
            assert_eq!(
                record.value,
                Some(scale_val(if seen < rows / 10 { seen + rows } else { seen }))
            );
            seen += 1;
            true
        });
        assert_eq!(seen, rows);
    }
    assert!(allocations[0] >= allocations[1] + 2 * rows as u64);
    assert!(allocations[3] >= allocations[2] + 2 * rows as u64);
}

/// M68 —— 把 `tools` 搬运层量进标准表：**三跳每跳跑两臂**（流式 vs 先物化整库），同一进程同一构建 ⇒ 比值即因果。
///
/// 为什么这条存在：M63 交了 `storage_engines::tools`，而 122 项标准表里**没有它一格**
/// （M67 记的那条"空白"）⇒ 用户问"各个功能有没有进步"时，唯一的新功能恰好没被量到；
/// 而它"不物化整库"此前只是照 `export 流式` 那一行**推**出来的，不是实测。
///
/// 台架形状 = 下游的用法：一份 root 下每个引擎一个子目录，逐跳搬
/// ① bitcask→lsm ② lsm→bplus ③ bplus→bitcask（`wipe_target = true`，就是 M63b 真编真跑那条三跳），
/// 再单独量第四格：文件往返（`export_to_file` bin → `import_from_file` bin）—— 这一格只有流式一条臂，
/// 因为"物化整库再写文件"不是任何人的用法，硬造一个对照臂 = 给自己造一个不会被读的数字。
///
/// **两臂只差读侧**：老形状 = M44 之前的 `export_latest_visible`（一次把整库做成 `Vec`），
/// 写侧与 `tools` 一字不差（同一份 `begin_bulk` + 同一批 `pack_plain` + 同样的批宽）⇒
/// 差值只归"读侧中间那份 `Vec`"。每跳都硬对拍：目标回读条数必须 == 行数（少一行就是 M45 那次静默丢数据）。
/// ⚠️ 三跳两臂的**先后顺序交替**（①新→老、②老→新、③新→老）：M52 那条"排位会冒充形状"。
///
/// 跑法：`SCALE_ROWS=600000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 tools_migrate_scale`
#[test]
#[ignore = "tools 搬运层的规模台架，手动 release 单线程跑：SCALE_ROWS=600000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 tools_migrate_scale"]
fn tools_migrate_scale() {
    let rows: usize = std::env::var("SCALE_ROWS")
        .ok()
        .and_then(|s| s.parse().ok())
        // 默认档给 200k 而不是 600k：这一格要跑**七次搬运**，600k 那档留给标准复测那一轮起环境变量
        .unwrap_or(200_000);
    fn key(i: usize) -> Vec<u8> {
        format!("{i:08}#/worldquant/datafields/x.json").into_bytes()
    }
    fn val(i: usize) -> Vec<u8> {
        format!("{{\"i\":{i},\"h\":\"{:0<28}\",\"v\":1}}", i * 7).into_bytes()
    }
    /// 路径**必须**从 `tools` 公开的那两个函数拿：它俩就是 `migrate` 自己用的那一份，
    /// 自己拼一遍就会在 bplus 那个"先认已存在的库文件名"上选错 ⇒ 凭空造一个空库当目标。
    fn path_for(engine: EngineKind, root: &std::path::Path) -> std::path::PathBuf {
        match engine {
            EngineKind::BPlusTree => crate::tools::bplus_db_path(root),
            other => crate::tools::engine_data_path(root, other),
        }
    }
    /// 老形状那一臂：读侧整库 `Vec` ⇒ 写侧与 `tools::pump_*` 同一份逻辑（批宽也对齐）。
    fn old_shape_hop(from: EngineKind, to: EngineKind, root: &std::path::Path) -> usize {
        const BATCH: usize = 8192;
        let src = open_engine(from, Some(&path_for(from, root))).expect("开源库");
        let recs = src.begin().export_latest_visible(false);
        drop(src);
        let dst_path = path_for(to, root);
        let _ = std::fs::remove_dir_all(&dst_path);
        let _ = std::fs::remove_file(&dst_path);
        let dst = open_engine(to, Some(&dst_path)).expect("开目标库");
        let mut bulk = dst.begin_bulk();
        let mut buf: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(BATCH);
        for rec in recs {
            let v = rec
                .value
                .expect("include_deleted=false 不该交出 tombstone 行");
            buf.push((rec.key, crate::common::pack_plain(&v)));
            if buf.len() >= BATCH {
                bulk.put_batch_owned(std::mem::take(&mut buf));
                buf = Vec::with_capacity(BATCH);
            }
        }
        if !buf.is_empty() {
            bulk.put_batch_owned(buf);
        }
        bulk.finish();
        let n = dst.begin().key_count(b"");
        drop(dst);
        n
    }
    fn hop_new(from: EngineKind, to: EngineKind, root: &std::path::Path) -> usize {
        crate::tools::migrate(from, to, root, false, true)
            .unwrap_or_else(|e| panic!("{}→{} 流式搬失败: {e}", from.as_str(), to.as_str()))
            .verify_live
    }

    let root = tmp_dir("tools_migrate");
    eprintln!("=== tools 搬运台架 rows={rows}（µs/行 + 常驻/峰值 MB）===");
    {
        let src = open_engine(
            EngineKind::Bitcask,
            Some(&path_for(EngineKind::Bitcask, &root)),
        )
        .expect("开源库");
        for base in (0..rows).step_by(1000) {
            let end = (base + 1000).min(rows);
            let items: Vec<(Vec<u8>, Vec<u8>)> = (base..end).map(|i| (key(i), val(i))).collect();
            let tx = src.begin();
            if let Err(bad) = tx.batch_set(&items) {
                panic!("灌源库 batch_set 冲突 {}", String::from_utf8_lossy(&bad));
            }
            tx.commit();
        }
        src.flush().unwrap();
        src.checkpoint().unwrap();
    }

    let hops = [
        (EngineKind::Bitcask, EngineKind::LsmTree),
        (EngineKind::LsmTree, EngineKind::BPlusTree),
        (EngineKind::BPlusTree, EngineKind::Bitcask),
    ];
    for (idx, (from, to)) in hops.into_iter().enumerate() {
        let tag = format!("{}→{}", from.as_str(), to.as_str());
        // 两臂交替在前：跳 0/2 先跑流式，跳 1 先跑老形状
        let order: [&str; 2] = if idx % 2 == 0 {
            ["流式", "物化Vec"]
        } else {
            ["物化Vec", "流式"]
        };
        for arm in order {
            let streaming = arm == "流式";
            let (d, live, peak, got) = timed(|| {
                if streaming {
                    hop_new(from, to, &root)
                } else {
                    old_shape_hop(from, to, &root)
                }
            });
            line(
                "tools",
                &format!("{tag} {arm}"),
                d.as_secs_f64() * 1e6 / rows as f64,
                "µs/行",
                live,
                peak,
                &format!("回读 {got} 行"),
            );
            assert_eq!(
                got, rows,
                "{tag} {arm}：目标回读 {got} 行 ≠ 源 {rows} 行 ⇒ 搬运少/多了行"
            );
        }
    }

    // 第四格：文件往返（生产走的那条：导出 bin → 导入 bin），只有流式一条臂
    let file = root.join("snap.bin");
    let (d, live, peak, ()) = timed(|| {
        crate::tools::export_to_file(
            EngineKind::Bitcask,
            &root,
            &file,
            crate::tools::SnapshotFormat::Bin,
            false,
        )
        .expect("导出 bin 失败");
    });
    line(
        "tools",
        "export bin(源=bitcask)",
        d.as_secs_f64() * 1e6 / rows as f64,
        "µs/行",
        live,
        peak,
        &format!("文件 {:.1} MB", disk_bytes(&file) as f64 / 1e6),
    );
    let (d, live, peak, got) = timed(|| {
        crate::tools::import_from_file(
            EngineKind::LsmTree,
            &root,
            &file,
            crate::tools::SnapshotFormat::Bin,
            true,
        )
        .expect("导入 bin 失败")
        .live
    });
    line(
        "tools",
        "import bin(目标=lsm)",
        d.as_secs_f64() * 1e6 / rows as f64,
        "µs/行",
        live,
        peak,
        &format!("回读 {got} 行"),
    );
    assert_eq!(got, rows, "文件往返少/多了行：{got} ≠ {rows}");

    let _ = std::fs::remove_dir_all(&root);
    eprintln!("TOOLS MIGRATE DONE");
}
