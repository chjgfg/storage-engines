// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。
// scale_workload：tests 同作用域中的职责测试分片。

// 固定规模的 IDE / cargo test 入口；共享下面的同一套台架，不复制五份负载。
include!("../../scale_bench.rs");
include!("../../lsm_allocation_bench.rs");

#[test]
#[ignore = "大库基线，手动 release 单线程跑：SCALE_ROWS=50000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 scale_baseline"]
fn scale_baseline_all_engines() {
    let rows: usize = std::env::var("SCALE_ROWS")
        .map(|s| s.parse().expect("SCALE_ROWS 必须是正整数"))
        .unwrap_or(50_000);
    run_scale_baseline(rows);
}

// 全库回收后为现有键补入旧版本，最新值保持不变。
fn scale_prepare_bounded_vacuum(db: &dyn KvEngine, rows: usize) -> (u64, usize) {
    let auto = db.vacuum_auto_slice();
    db.set_vacuum_auto_slice(0);
    let keys = rows.min(8);
    for _ in 0..2 {
        let tx = db.begin();
        for i in 0..keys {
            let key = scale_key(i);
            let value = tx.get(&key).expect("bounded seed key must already exist");
            tx.try_set_raw(&key, value).expect("bounded seed write");
        }
        tx.try_commit().expect("bounded seed commit");
    }
    (auto, keys)
}

fn scale_note_maintenance(
    db: &dyn KvEngine,
    phase: &str,
    scope: &crate::lsm_tree::lsm_tree::maintenance_probe::Scope,
) {
    let s = scope.stats();
    eprintln!(
        "SCALE_MAINT engine={} phase={phase} auto={:?} lsm_flushes={} lsm_flush_ms={:.3} lsm_compactions={} lsm_compact_ms={:.3} lsm_input_files={} lsm_input_bytes={} owned_input_rows={} legacy_input_clones={} reference_input_clones={} compaction_blocks={}",
        db.name(),
        db.reclamation_status(),
        s.flushes,
        s.flush_ns as f64 / 1e6,
        s.compactions,
        s.compact_ns as f64 / 1e6,
        s.input_files,
        s.input_bytes,
        s.owned_input_rows,
        s.legacy_input_clones,
        s.reference_input_clones,
        s.compaction_blocks
    );
    if db.name() == "lsm_tree" {
        eprintln!(
            "SCALE_LSM_MOVES phase={phase} files={} bytes={}",
            s.trivial_moves, s.moved_bytes
        );
        eprintln!(
            "SCALE_LSM_REWRITE_INPUTS phase={phase} cumulative=true files={} bytes={} excludes_trivial_moves=true",
            s.input_files - s.trivial_moves,
            s.input_bytes - s.moved_bytes
        );
        eprintln!(
            "SCALE_LSM_REWRITE_OUTPUTS phase={phase} cumulative=true files={} bytes={} includes_overlap_repair=true scope=merge_generated_sst",
            s.output_files, s.output_bytes
        );
        eprintln!(
            "SCALE_LSM_DELETE_STAGES phase={phase} cumulative=true delete_calls={} write_ms={:.3} threshold_flushes={} threshold_flush_ms={:.3} mem_wal_syncs={} mem_wal_sync_ms={:.3}",
            s.delete_calls,
            s.delete_write_ns as f64 / 1e6,
            s.threshold_flushes,
            s.threshold_flush_ns as f64 / 1e6,
            s.mem_wal_syncs,
            s.mem_wal_sync_ns as f64 / 1e6
        );
        eprintln!(
            "SCALE_LSM_STAGES phase={phase} merge_ms={:.3} repair_ms={:.3} manifest_ms={:.3} guard_ms={:.3} cleanup_ms={:.3} sst_finish_ms={:.3} sst_sync_ms={:.3} manifest_reuses={} sst_syncs={}",
            s.merge_ns as f64 / 1e6,
            s.repair_ns as f64 / 1e6,
            s.manifest_ns as f64 / 1e6,
            s.guard_ns as f64 / 1e6,
            s.cleanup_ns as f64 / 1e6,
            s.sst_finish_ns as f64 / 1e6,
            s.sst_sync_ns as f64 / 1e6,
            s.manifest_reuses,
            s.sst_syncs
        );
    }
}

fn run_scale_baseline(rows: usize) {
    let run = crate::scale_probe::RunGuard::acquire(&format!("scale_rows_{rows}"));
    run_scale_baseline_locked(rows, &run);
}

fn run_scale_baseline_locked(rows: usize, _run: &crate::scale_probe::RunGuard) {
    assert!(
        rows > 0 && rows.checked_mul(16).is_some(),
        "SCALE_ROWS 必须为非零且不溢出的行数"
    );
    // 键 ≈ 35 字节、值 ≈ 60 字节：照下游真实形状（也都远小于任何 blob 阈值）
    use self::{scale_key as key, scale_val as val};

    crate::scale_probe::fingerprint();
    eprintln!(
        "SCALE_BEGIN rows={rows} package={} os={} arch={} optimized={}",
        env!("CARGO_PKG_VERSION"),
        std::env::consts::OS,
        std::env::consts::ARCH,
        !cfg!(debug_assertions)
    );
    eprintln!("=== 大库基线 rows={rows}（µs/次 + 堆增量/堆峰值 MB）===");
    eprintln!(
        "验收政策：TTL 两格后计时外 persist 同一批辅助键；迁移期望 rows+min(rows,1000)+1，bulk 重开再加 bulk_rows；逐键逐值验收不计入每引擎33格性能记录。"
    );
    for kind in [
        EngineKind::Bitcask,
        EngineKind::BPlusTree,
        EngineKind::LsmTree,
        EngineKind::Memory,
    ] {
        let label = kind.as_str();
        let maintenance = crate::lsm_tree::lsm_tree::maintenance_probe::Scope::start();
        let scratch = ScaleScratch::new(&format!("scale_{label}"));
        let dir = scratch.0.clone();
        eprintln!("{label}: scratch={}", dir.display());
        // B+ 树的"库"是一个文件，另两家是目录；memory 从 M81 第 6 项起也拿目录 ——
        // 目录就是它启停快照的落点（`<dir>/snapshot.bxp`）。"没给目录 = 纯内存"的旧形状仍然
        // 有效，但台架这一支必须走"有快照"的那条路，否则对它来说"冷重开"还是空库、那格没意义。
        let arg = match kind {
            EngineKind::Memory => Some(dir.clone()),
            EngineKind::BPlusTree => Some(dir.join("bplus.db")),
            _ => Some(dir.clone()),
        };
        let arg = arg.as_deref();

        let (d, live, peak, db) = timed_phase(label, "open", || open_engine(kind, arg).unwrap());
        line(
            label,
            "open 空库",
            d.as_secs_f64() * 1e6,
            "µs",
            live,
            peak,
            "",
        );

        // ---- 写入：每 1000 行一个事务（commit 带 fsync） ----
        let mut insert_stages = [0_u128; 5];
        let insert_doors = crate::common::maintenance_probe::Scope::start();
        let wal_commit_probe = crate::common::wal::commit_probe::Scope::start();
        let (d, live, peak, ()) = timed_phase(label, "insert", || {
            for base in (0..rows).step_by(1000) {
                let end = (base + 1000).min(rows);
                let stage = std::time::Instant::now();
                let items: Vec<(Vec<u8>, Vec<u8>)> =
                    (base..end).map(|i| (key(i), val(i))).collect();
                insert_stages[0] += stage.elapsed().as_nanos();
                let stage = std::time::Instant::now();
                let tx = db.begin();
                insert_stages[3] += stage.elapsed().as_nanos();
                let stage = std::time::Instant::now();
                if let Err(bad) = tx.batch_set(&items) {
                    panic!("{label}: batch_set 冲突 {}", String::from_utf8_lossy(&bad));
                }
                insert_stages[1] += stage.elapsed().as_nanos();
                let stage = std::time::Instant::now();
                tx.commit();
                insert_stages[2] += stage.elapsed().as_nanos();
                let stage = std::time::Instant::now();
                drop(tx);
                drop(items);
                insert_stages[4] += stage.elapsed().as_nanos();
            }
        });
        let residual = d
            .as_nanos()
            .checked_sub(insert_stages.iter().sum())
            .expect("insert substages overlap");
        eprintln!(
            "SCALE_INSERT_STAGES engine={label} prepare_ms={:.3} batch_set_ms={:.3} commit_ms={:.3} begin_ms={:.3} release_ms={:.3} residual_ms={:.3} scope=disjoint_substages",
            insert_stages[0] as f64 / 1e6,
            insert_stages[1] as f64 / 1e6,
            insert_stages[2] as f64 / 1e6,
            insert_stages[3] as f64 / 1e6,
            insert_stages[4] as f64 / 1e6,
            residual as f64 / 1e6
        );
        eprintln!(
            "SCALE_INSERT_MAINT engine={label} doors={:?}",
            insert_doors.stats()
        );
        drop(insert_doors);
        let wal = wal_commit_probe.stats();
        let sync_avg_us = if wal.sync_calls == 0 {
            0.0
        } else {
            wal.sync_ns as f64 / wal.sync_calls as f64 / 1e3
        };
        eprintln!(
            "SCALE_WAL_COMMIT engine={label} attempts={} append_calls={} append_total_ms={:.3} append_max_us={:.3} commit_sync_calls={} commit_sync_total_ms={:.3} commit_sync_avg_us={:.3} commit_sync_max_ms={:.3} commit_sync_hist_le1ms_1to5ms_5to10ms_10to25ms_25to50ms_50to100ms_100to250ms_gt250ms={:?} fsync_calls={} fsync_total_ms={:.3} fsync_max_ms={:.3}",
            wal.attempts,
            wal.append_calls,
            wal.append_ns as f64 / 1e6,
            wal.append_max_ns as f64 / 1e3,
            wal.sync_calls,
            wal.sync_ns as f64 / 1e6,
            sync_avg_us,
            wal.sync_max_ns as f64 / 1e6,
            wal.sync_histogram,
            wal.fsync_calls,
            wal.fsync_ns as f64 / 1e6,
            wal.fsync_max_ns as f64 / 1e6
        );
        drop(wal_commit_probe);
        line(
            label,
            &format!("灌 {rows} 行(事务)"),
            d.as_secs_f64() * 1e6 / rows as f64,
            "µs/行",
            live,
            peak,
            "",
        );
        scale_note_maintenance(&*db, "insert", &maintenance);

        let (d, live, peak, ()) = timed_phase(label, "checkpoint", || {
            db.flush().unwrap();
            db.checkpoint().unwrap();
        });
        let disk = disk_bytes(arg.unwrap());
        if label == "lsm_tree" {
            scale_note_maintenance(&*db, "checkpoint", &maintenance);
        }
        line(
            label,
            "flush+checkpoint",
            d.as_secs_f64() * 1e6,
            "µs",
            live,
            peak,
            &format!("磁盘 {:.1} MB", disk as f64 / 1e6),
        );

        // ---- 冷重开：恢复要多久、恢复完常驻多少 ----
        // M81 第 6 项：memory 有启停快照了 ⇒ 这一格对它成了**真读数**（open 读快照），
        // 原来那两处"内存引擎不落盘：重开 = 空库"的特判因此拆掉（格数 130 → 132）。
        // 停止那一次写发生在 `Drop` 里、按方案的口径落在计时格外 —— 但它是实打实的价，
        // 所以补一行"不计时"的读数（与 `验收准备(不计时)` 同一套写法，不进性能格）。
        crate::scale_probe::memory(label, "first_before_stop");
        let db = {
            let st_base = open_stages_begin();
            let snapshot = (kind == EngineKind::Memory)
                .then(|| crate::common::kv_snapshot::snapshot_probe::Scope::start(false));
            let stop_phase = crate::scale_probe::Phase::begin(label, "first_stop");
            let t_stop = std::time::Instant::now();
            drop(db);
            if kind == EngineKind::Memory {
                eprintln!(
                    "{label:<10} {:<24} {:>13}   （Drop 内写出整库快照，不进性能格）",
                    "停止落盘",
                    format!("{:.1} ms", t_stop.elapsed().as_secs_f64() * 1e3)
                );
            }
            if let Some(scope) = snapshot {
                let stats = scope.stats();
                eprintln!(
                    "SCALE_SNAPSHOT engine={label} release_ms={:.3} stages={stats:?}",
                    t_stop.elapsed().as_nanos().saturating_sub(stats.total_ns) as f64 / 1e6
                );
            }
            stop_phase.finish(label, "first_stop");
            crate::scale_probe::memory(label, "first_before_reopen");
            let recovery = (kind == EngineKind::Memory)
                .then(crate::common::kv_snapshot::recovery_probe::Scope::start);
            let (d, live, peak, db) =
                timed_phase(label, "first_reopen", || open_engine(kind, arg).unwrap());
            if let Some(scope) = recovery {
                eprintln!("SCALE_RECOVERY phase=first stages={:?}", scope.stats());
            }
            crate::scale_probe::memory(label, "first_after_reopen");
            line(
                label,
                "正常关闭后重开(恢复)",
                d.as_secs_f64() * 1e6,
                "µs",
                live,
                peak,
                &format!("堆增量 {:.1} MB", live as f64 / 1e6),
            );
            open_stages_report(label, st_base);
            db
        };

        let order = shuffled(rows);

        // ---- 点读：乱序 / 有序 ----
        let (d, live, peak, ()) = timed_phase(label, "random_read", || {
            let tx = db.begin();
            let mut hits = 0;
            for &i in &order {
                if tx.get(&key(i)).is_some() {
                    hits += 1;
                }
            }
            assert_eq!(hits, rows, "{label}: 乱序点读有键没读到");
        });
        line(
            label,
            "点读×N 乱序",
            d.as_secs_f64() * 1e6 / rows as f64,
            "µs/次",
            live,
            peak,
            "",
        );

        let (d, live, peak, ()) = timed_phase(label, "ordered_read", || {
            let tx = db.begin();
            for i in 0..rows {
                assert!(tx.get(&key(i)).is_some(), "{label}: 有序点读落空");
            }
        });
        line(
            label,
            "点读×N 有序",
            d.as_secs_f64() * 1e6 / rows as f64,
            "µs/次",
            live,
            peak,
            "",
        );

        // ---- multi_get（门面是 map(get)：批量到底省不省钱，量了才知道） ----
        let chunk: Vec<Vec<u8>> = order[..1000.min(rows)].iter().map(|&i| key(i)).collect();
        let (d, live, peak, got) = timed_phase(label, "multi_get", || {
            let tx = db.begin();
            tx.multi_get(&chunk)
        });
        line(
            label,
            "multi_get×1000",
            d.as_secs_f64() * 1e6 / chunk.len() as f64,
            "µs/键",
            live,
            peak,
            &format!("读到 {} 条", got.len()),
        );

        // 命中与未命中**必须分开数**：混在一个平均值里，"点查 0.86 ms" 那种症状就归不了因
        // （M10 的教训）。这 1000 把键全是不存在的键，单价就是"查无此键"的成本。
        let miss_chunk: Vec<Vec<u8>> = (0..1000.min(rows)).map(|i| key(rows * 8 + i)).collect();
        let (d, live, peak, got) = timed_phase(label, "multi_miss", || {
            let tx = db.begin();
            tx.multi_get(&miss_chunk)
        });
        let hit = got.iter().filter(|o| o.is_some()).count();
        line(
            label,
            "multi_get×1000 全miss",
            d.as_secs_f64() * 1e6 / miss_chunk.len() as f64,
            "µs/键",
            live,
            peak,
            &format!("读到 {hit} 条(应为 0)"),
        );

        // ---- 扫描：全量 / 1% 窗口 ----
        let (d, live, peak, scanned) = timed_phase(label, "scan", || {
            let tx = db.begin();
            tx.scan(None, None)
        });
        line(
            label,
            "一次全量 scan",
            d.as_secs_f64() * 1e6 / rows as f64,
            "µs/行",
            live,
            peak,
            &format!("{} 行", scanned.len()),
        );
        drop(scanned);

        let lo = key(rows / 2);
        let hi = key(((rows / 2) + rows / 100).max(1));
        let (d, live, peak, window) = timed_phase(label, "window_scan", || {
            let tx = db.begin();
            tx.scan(Some(&lo), Some(&hi))
        });
        let w = window.len().max(1);
        line(
            label,
            "窗口 scan(1%)",
            d.as_secs_f64() * 1e6 / w as f64,
            "µs/行",
            live,
            peak,
            &format!("{w} 行"),
        );
        drop(window);

        let (d, live, peak, pref) = timed_phase(label, "prefix_scan", || {
            let tx = db.begin();
            tx.prefix_scan(&key(0)[..2])
        });
        let p = pref.len().max(1);
        line(
            label,
            "prefix_scan 2B前缀",
            d.as_secs_f64() * 1e6 / p as f64,
            "µs/行",
            live,
            peak,
            &format!("{p} 行"),
        );
        drop(pref);

        // ---- 游标翻页：把整库一页一页翻完（长跑进程真正在用的那条路，M17） ----
        let (d, live, peak, (pages, moved)) = timed_phase(label, "scan_page", || {
            let tx = db.begin();
            let mut cursor: Option<Vec<u8>> = None;
            let mut pages = 0usize;
            let mut moved = 0usize;
            // 保险丝：页宽 500 ⇒ 正常应当走 ~rows/500 页，超两倍就是走不动了
            while pages <= rows / 250 + 8 {
                let (chunk, next) = tx.scan_page(cursor.as_deref(), 500);
                moved += chunk.len();
                pages += 1;
                let Some(next) = next else { break };
                cursor = Some(next);
            }
            (pages, moved)
        });
        line(
            label,
            "scan_page 翻完整库",
            d.as_secs_f64() * 1e6 / pages.max(1) as f64,
            "µs/页",
            live,
            peak,
            &format!("{pages} 页 / {moved} 行（页宽 500）"),
        );
        assert_eq!(
            moved, rows,
            "{label}: scan_page 翻完整库必须正好交出 {rows} 行（少一行=游标走不动，多走=翻页有洞）"
        );

        // ---- 反向与定位：B+ 树只有单向 next_leaf，最容易在这两条上露形状 ----
        let (d, live, peak, back) = timed_phase(label, "seek_prev", || {
            let tx = db.begin();
            tx.seek_prev(&key(rows / 2))
        });
        line(
            label,
            "seek_prev 中间键",
            d.as_secs_f64() * 1e6,
            "µs/次",
            live,
            peak,
            &format!("命中 {}", back.is_some()),
        );

        let (d, live, peak, rwin) = timed_phase(label, "reverse_scan", || {
            let tx = db.begin();
            tx.reverse_scan(Some(&lo), Some(&hi))
        });
        let rw = rwin.len().max(1);
        line(
            label,
            "reverse_scan 1% 窗口",
            d.as_secs_f64() * 1e6 / rw as f64,
            "µs/行",
            live,
            peak,
            &format!("{rw} 行"),
        );

        // ---- 管理面：分页搜索（首页 vs 深页 —— 深翻是最常见的"一次调用打满 CPU"） ----
        let page_size = 100usize;
        let last_page = scale_last_page_index(rows, page_size);
        let (d, live, peak, one) = timed_phase(label, "search_first", || {
            let tx = db.begin();
            tx.search_keys(&crate::common::SearchQuery::contains(b"", 0, page_size))
        });
        line(
            label,
            "search 首页",
            d.as_secs_f64() * 1e6,
            "µs/次",
            live,
            peak,
            &format!("命中 {}", one.total.unwrap_or_default()),
        );

        // 同一页、只是**不要精确 total**：这一行存在的意义就是和上面那条并排看
        // （翻页的 UI 只要"还有没有下一页"，而数 total 的代价是走完整库）
        let (d, live, peak, one_nt) = timed_phase(label, "search_first_no_total", || {
            let tx = db.begin();
            tx.search_keys(&crate::common::SearchQuery::contains(b"", 0, page_size).without_total())
        });
        line(
            label,
            "search 首页(免 total)",
            d.as_secs_f64() * 1e6,
            "µs/次",
            live,
            peak,
            &format!(
                "拿了 {} 条, has_next={}",
                one_nt.items.len(),
                one_nt.has_next()
            ),
        );
        let (d, live, peak, deep) = timed_phase(label, "search_last", || {
            let tx = db.begin();
            tx.search_keys(&crate::common::SearchQuery::contains(
                b"", last_page, page_size,
            ))
        });
        scale_check_last_page(&deep, rows, page_size);
        line(
            label,
            "search 末页(深翻)",
            d.as_secs_f64() * 1e6,
            "µs/次",
            live,
            peak,
            &format!(
                "命中 {}，末页 {} 条（已逐值验收）",
                deep.total.unwrap_or_default(),
                deep.items.len()
            ),
        );
        // M81 第 4 项：末页的**免 total** 那一支，代价 = 页宽 × O(一次点读)，四家都不随库涨。
        // ⚠️ 不许与上面那格直接对拍：`search 末页(深翻)` 要精确 total（数全库那一遍是主导，
        // 20M 档 80.7 s），这一格最多取 `page_size` 行、不数；真正末页可能不足页宽。
        // M98 修订：旧台架 total 整除页宽时误请求越界空页；现在末页逐键逐值验收。
        // 覆盖情况（11-04 起）：`lsm_tree` 走反向逐步退（SST 块读）；其余三家走
        // `common::store::back_n_with`（bplus/memory 先右路下降取库尾）。**11-04 之前的构建里
        // 三家停在 trait 默认**（`reverse_scan` 截尾 = O(库)：20M 档 88~106 / 55~71 / 64~69 s、
        // 常驻 +2147.5 MB、峰值 +4771 MB）——跨这条线比这一格没有意义；那支现在只剩
        // "退过 8 Ki 步仍凑不够页宽"的兜底，由四家 `back_n_fallbacks == 0` 的计数守卫看着。
        let (d, live, peak, last_nt) = timed_phase(label, "search_last_no_total", || {
            let tx = db.begin();
            tx.search_last_page(page_size)
        });
        line(
            label,
            "search 末页(免 total)",
            d.as_secs_f64() * 1e6,
            "µs/次",
            live,
            peak,
            &format!("拿了 {} 条, total=None", last_nt.items.len()),
        );
        let (d, live, peak, cnt) = timed_phase(label, "key_count", || {
            let tx = db.begin();
            tx.key_count(&key(0)[..2])
        });
        line(
            label,
            "key_count 2B前缀",
            d.as_secs_f64() * 1e6,
            "µs/次",
            live,
            peak,
            &format!("{cnt} 键"),
        );

        // ---- TTL 往返：写 1000 个带过期时间的键，再把剩余时间读回来 ----
        // （长跑里"会话键续期"就是这个形状；只量写不量读会把信封解析的成本藏掉）
        let ttl_rows = 1000.min(rows);
        let (d, live, peak, ()) = timed_phase(label, "set_ttl", || {
            let tx = db.begin();
            for i in 0..ttl_rows {
                tx.set_with_ttl(
                    &key(rows * 4 + i),
                    val(i),
                    std::time::Duration::from_secs(3600),
                );
            }
            tx.commit();
        });
        line(
            label,
            "set_with_ttl×1000",
            d.as_secs_f64() * 1e6 / ttl_rows as f64,
            "µs/次",
            live,
            peak,
            "",
        );
        let (d, live, peak, hits) = timed_phase(label, "get_ttl", || {
            let tx = db.begin();
            (0..ttl_rows)
                .filter(|i| tx.get_ttl(&key(rows * 4 + i)).flatten().is_some())
                .count()
        });
        line(
            label,
            "get_ttl×1000",
            d.as_secs_f64() * 1e6 / ttl_rows as f64,
            "µs/次",
            live,
            peak,
            &format!("{hits}/{ttl_rows} 有 TTL"),
        );
        assert_eq!(
            hits, ttl_rows,
            "{label}: 刚写的 TTL 必须全部读得回来（读不回来=信封在某一家被吞了）"
        );
        // 性能负载与短 TTL 语义分离：不让 20M 档几个小时的墙钟决定迁移期望。
        // 保留同一批键/值/行数，但多写 ttl_rows 个辅助版本，后续不再走 TTL 分支；
        // 这不是历史 TTL 负载的逐字节同形状，必须在输出中单独交代，不能冒充性能改进。
        scale_persist_auxiliary(&*db, rows);
        eprintln!(
            "{label}: 验收准备(不计时) persist {ttl_rows} 辅助键，新增 {ttl_rows} 个版本；后续辅助键永久化、最新信封少8B，非历史TTL同形状"
        );

        // ---- TTL / 计数器 ----
        let (d, live, peak, ()) = timed_phase(label, "incr", || {
            let tx = db.begin();
            for expected in 1..=1000 {
                assert_eq!(
                    tx.incr(b"counter:hits", 1),
                    Ok(expected),
                    "{label}: incr 不许忽略失败"
                );
            }
            tx.commit();
        });
        line(
            label,
            "incr×1000",
            d.as_secs_f64() * 1e6 / 1000.0,
            "µs/次",
            live,
            peak,
            "",
        );
        assert_eq!(
            db.begin().get(b"counter:hits"),
            Some(b"1000".to_vec()),
            "{label}: incr 提交后计数器缺失"
        );

        // ---- 更新 10% 的键（制造版本链，看点写是否被链拖住） ----
        let updates: usize = (rows / 10).max(1);
        let update_stages = crate::common::maintenance_probe::Scope::start();
        let (d, live, peak, ()) = timed_phase(label, "update", || {
            for base in (0..updates).step_by(500) {
                let end = (base + 500).min(updates);
                let items: Vec<(Vec<u8>, Vec<u8>)> =
                    (base..end).map(|i| (key(i), val(i + rows))).collect();
                let tx = db.begin();
                if let Err(bad) = tx.batch_set(&items) {
                    panic!("{label}: 更新冲突 {}", String::from_utf8_lossy(&bad));
                }
                tx.commit();
            }
        });
        line(
            label,
            &format!("更新 {updates} 键"),
            d.as_secs_f64() * 1e6 / updates as f64,
            "µs/次",
            live,
            peak,
            "",
        );
        eprintln!(
            "SCALE_UPDATE_STAGES engine={label} doors={:?}",
            update_stages.stats()
        );
        drop(update_stages);
        scale_note_maintenance(&*db, "update", &maintenance);

        // ---- 观测与回收 ----
        let (d, live, peak, h) = timed_phase(label, "health", || db.health());
        line(
            label,
            "health()",
            d.as_secs_f64() * 1e6,
            "µs/次",
            live,
            peak,
            &format!("rows={} max_chain={}", h.total_rows, h.max_chain),
        );

        let vacuum_stages = crate::common::maintenance_probe::Scope::start();
        crate::scale_probe::memory(label, "before_vacuum");
        let (d, live, peak, v) = timed_phase(label, "vacuum", || db.vacuum().unwrap());
        crate::scale_probe::memory(label, "after_vacuum");
        line(
            label,
            "vacuum() 全库",
            d.as_secs_f64() * 1e6,
            "µs/次",
            live,
            peak,
            &format!("裁掉 {} 行", v.versions_removed),
        );
        eprintln!(
            "SCALE_VACUUM_STAGES engine={label} doors={:?}",
            vacuum_stages.stats()
        );
        drop(vacuum_stages);
        eprintln!(
            "SCALE_VACUUM engine={label} rows_scanned={} versions_removed={} skipped={}",
            v.rows_scanned, v.versions_removed, v.skipped_active_txn
        );
        scale_note_maintenance(&*db, "vacuum", &maintenance);

        let (_, _, _, (auto, seed_keys)) = timed_phase(label, "bounded_prepare", || {
            scale_prepare_bounded_vacuum(&*db, rows)
        });
        let bounded_stages = crate::common::maintenance_probe::Scope::start();
        let (d, live, peak, vr) = timed_phase(label, "vacuum_bounded", || {
            db.vacuum_bounded(None, 2048).unwrap()
        });
        db.set_vacuum_auto_slice(auto);
        assert!(
            vr.stats.versions_removed >= seed_keys * 2,
            "{label}: bounded vacuum must remove seeded history"
        );
        eprintln!(
            "SCALE_BOUNDED_VACUUM engine={label} seed_keys={seed_keys} added_versions={} rows_scanned={} versions_removed={}",
            seed_keys * 2,
            vr.stats.rows_scanned,
            vr.stats.versions_removed
        );
        eprintln!(
            "SCALE_BOUNDED_STAGES engine={label} doors={:?}",
            bounded_stages.stats()
        );
        drop(bounded_stages);
        line(
            label,
            "vacuum_bounded 一片",
            d.as_secs_f64() * 1e6,
            "µs/次",
            live,
            peak,
            &format!(
                "扫 {} 行、裁 {} 行（预算 2048）",
                vr.stats.rows_scanned, vr.stats.versions_removed
            ),
        );

        // M81（方案第 2 项）：`flush+merge` 拆成 `flush` + `merge` 两格。
        // 原先合成一格时 bitcask 那一格里几乎全是 merge（`docs/test1.txt`：5M 同格 21.4s，
        // 而 bplus 同格 18.5µs ⇒ flush 那一半在 bitcask 上是零头）。拆格本身一秒不省，
        // 它的作用是让"merge 已从 vacuum 拆出去"那笔省下的时间在下一格露出来。
        let (d, live, peak, ()) = timed_phase(label, "flush", || db.flush().unwrap());
        if label == "lsm_tree" {
            scale_note_maintenance(&*db, "flush", &maintenance);
        }
        let disk_after_flush = disk_bytes(arg.unwrap());
        line(
            label,
            "flush",
            d.as_secs_f64() * 1e6,
            "µs/次",
            live,
            peak,
            &format!("磁盘 {:.1} MB", disk_after_flush as f64 / 1e6),
        );

        let (d, live, peak, m) = timed_phase(label, "merge", || db.merge());
        // ⚠️ M43 那条铁律在这儿落地：`Unsupported` 是"本引擎没有这一步"，其余错**不许吞**——
        // merge 失败意味着引擎已进入隔离态，后面每一格的读数都是假的，必须当场红。
        if let Err(e) = &m {
            assert!(
                matches!(e, EngineError::Unsupported { .. }),
                "{label}: merge 失败（此后引擎处于隔离态，后面的格子全是假读数）: {e}"
            );
        }
        let disk2 = disk_bytes(arg.unwrap());
        let note = if m.is_err() {
            "（本引擎无 merge）".to_string()
        } else {
            format!(
                "磁盘 {:.1} MB（merge 前 {:.1}）",
                disk2 as f64 / 1e6,
                disk_after_flush as f64 / 1e6
            )
        };
        line(
            label,
            "merge",
            d.as_secs_f64() * 1e6,
            "µs/次",
            live,
            peak,
            &note,
        );

        // ---- bulk 导入（另一条写路径：无 WAL、无冲突检测） ----
        // ---- 物化全库的出口：`Vec` 有多大、峰值多少 MB，就是这条的意义 ----
        // （翻页请走 scan_page；这条留在台架里就是为了把"别用它"量化出来）
        let (d, live, peak, exp) =
            timed_phase(label, "export_owned", || db.export_latest_visible(false));
        let en = exp.len().max(1);
        line(
            label,
            "export 全库(物化)",
            d.as_secs_f64() * 1e6 / en as f64,
            "µs/行",
            live,
            peak,
            &format!("{en} 行进 Vec"),
        );
        drop(exp);

        // ---- 流式导出（M44）：同样走完全库、同样落到文件，但一行都不攒 ----
        // 与上面那一行是**同一件事的两种写法**，所以这一对就是这一刀的凭据：
        // 峰值不该再随库大小涨（那是调用方要的那份 `Vec`，不是遍历的账）。
        let snap = dir.join("export_stream.bin");
        // ScaleScratch 已在计时外建好目录，包括不落盘的 memory 一家。
        let (d, live, peak, n) = timed_phase(label, "export_stream", || {
            let mut w = crate::common::kv_snapshot::ExportWriter::bin(&snap, false).unwrap();
            let handed = db.export_latest_visible_for_each(false, &mut |rec| w.push(rec));
            let stats = w.finish().unwrap();
            assert_eq!(handed, stats.records, "{label}: 折出的行数与写出的行数不同");
            handed
        });
        let size = std::fs::metadata(&snap).map(|m| m.len()).unwrap_or(0);
        line(
            label,
            "export 流式(逐行落地)",
            d.as_secs_f64() * 1e6 / n.max(1) as f64,
            "µs/行",
            live,
            peak,
            &format!(
                "{n} 行 → 磁盘 {:.1} MB，内存里一行都不攒",
                size as f64 / 1e6
            ),
        );
        let _ = std::fs::remove_file(&snap);

        // ---- 跨引擎搬家：这一家 → memory（读侧 scan_page、写侧一页一事务，M17）----
        // 独立种子账：rows 个普通键 + ttl_rows 个已永久化辅助键 + 1 个计数器。
        // 绝不问源库 health 要期望：源端非 TTL 丢失也必须红，不能一起少而自洽。
        let want = (rows + ttl_rows + 1) as u64;
        let target = open_engine(EngineKind::Memory, None).unwrap();
        let migration_scope = migration_probe::Scope::start();
        let (d, live, peak, ms) =
            timed_phase(label, "migrate", || migrate(&*db, &*target, 1000).unwrap());
        let stages = migration_scope.stats();
        drop(migration_scope);
        let residual = d
            .as_nanos()
            .checked_sub(stages.ns.iter().sum())
            .expect("migration substages overlap");
        eprintln!(
            "SCALE_MIGRATION_STAGES engine={label} phase=migrate source_begin_ms={:.3} source_scan_ms={:.3} projection_ms={:.3} source_commit_ms={:.3} target_begin_ms={:.3} target_write_ms={:.3} target_commit_ms={:.3} target_rollback_ms={:.3} residual_ms={:.3} scope=disjoint_substages",
            stages.ns[0] as f64 / 1e6,
            stages.ns[1] as f64 / 1e6,
            stages.ns[2] as f64 / 1e6,
            stages.ns[3] as f64 / 1e6,
            stages.ns[4] as f64 / 1e6,
            stages.ns[5] as f64 / 1e6,
            stages.ns[6] as f64 / 1e6,
            stages.ns[7] as f64 / 1e6,
            residual as f64 / 1e6
        );
        line(
            label,
            "migrate → memory",
            d.as_secs_f64() * 1e6 / rows.max(1) as f64,
            "µs/行",
            live,
            peak,
            &format!("搬 {} 行 / {} 页 / {} B", ms.rows, ms.pages, ms.bytes),
        );
        // 验收在计时外逐行流过目标，不在 O(N) 内存的目标旁再攒第二份全库 Vec。
        scale_check_migration(&*target, rows, ms)
            .unwrap_or_else(|e| panic!("{label}: migrate 验收失败: {e}"));
        eprintln!(
            "{label}: migrate 验收通过(不计时) {want} 个唯一键，完整键/最新值/计数器/用户字节均与独立种子相符，TTL=0"
        );
        crate::scale_probe::memory(label, "migration_before_release");
        let heap_before_release = crate::mem_probe::live();
        drop(target);
        let heap_after_release = crate::mem_probe::live();
        eprintln!(
            "SCALE_MIGRATION_RELEASE engine={label} released_heap_bytes={}",
            heap_before_release.saturating_sub(heap_after_release)
        );
        crate::scale_probe::memory(label, "migration_after_release");
        let bulk_rows = (rows / 10).max(1);
        let mut bulk_stages = [0_u128; 4];
        let bulk_doors = crate::common::maintenance_probe::Scope::start();
        let (d, live, peak, ()) = timed_phase(label, "bulk", || {
            let stage = std::time::Instant::now();
            let mut bulk = db.begin_bulk();
            bulk_stages[0] += stage.elapsed().as_nanos();
            let stage = std::time::Instant::now();
            let items: Vec<(Vec<u8>, Vec<u8>)> =
                (0..bulk_rows).map(|i| (key(rows + i), val(i))).collect();
            bulk_stages[1] += stage.elapsed().as_nanos();
            let stage = std::time::Instant::now();
            bulk.put_batch_owned(items);
            bulk_stages[2] += stage.elapsed().as_nanos();
            let stage = std::time::Instant::now();
            bulk.finish();
            bulk_stages[3] += stage.elapsed().as_nanos();
        });
        let residual = d
            .as_nanos()
            .checked_sub(bulk_stages.iter().sum())
            .expect("bulk substages overlap");
        eprintln!(
            "SCALE_BULK_STAGES engine={label} begin_ms={:.3} prepare_ms={:.3} put_ms={:.3} finish_ms={:.3} residual_ms={:.3} scope=disjoint_substages",
            bulk_stages[0] as f64 / 1e6,
            bulk_stages[1] as f64 / 1e6,
            bulk_stages[2] as f64 / 1e6,
            bulk_stages[3] as f64 / 1e6,
            residual as f64 / 1e6
        );
        eprintln!(
            "SCALE_BULK_MAINT engine={label} doors={:?}",
            bulk_doors.stats()
        );
        drop(bulk_doors);
        line(
            label,
            &format!("bulk 灌 {bulk_rows} 行"),
            d.as_secs_f64() * 1e6 / bulk_rows as f64,
            "µs/行",
            live,
            peak,
            "",
        );
        if label == "lsm_tree" {
            scale_note_maintenance(&*db, "bulk", &maintenance);
        }

        // ---- bulk 正常关闭后重开一次：bulk 之后的恢复代价 ----
        // （M81 第 6 项：memory 走同一条路了 —— 这一次 drop 会把它连 bulk 一起写进快照。
        //   "停止落盘"那行不计时读数只在上面那一处打，免得同一格的两条噪声线混淆。）
        crate::scale_probe::memory(label, "bulk_before_stop");
        let db = {
            let st_base = open_stages_begin();
            let snapshot = (kind == EngineKind::Memory)
                .then(|| crate::common::kv_snapshot::snapshot_probe::Scope::start(false));
            let stop_phase = crate::scale_probe::Phase::begin(label, "bulk_stop");
            let stop = std::time::Instant::now();
            drop(db);
            if let Some(scope) = snapshot {
                let stats = scope.stats();
                eprintln!(
                    "SCALE_SNAPSHOT phase=bulk total_ms={:.3} release_ms={:.3} stages={stats:?}",
                    stop.elapsed().as_secs_f64() * 1e3,
                    stop.elapsed().as_nanos().saturating_sub(stats.total_ns) as f64 / 1e6
                );
            }
            stop_phase.finish(label, "bulk_stop");
            crate::scale_probe::memory(label, "bulk_before_reopen");
            let recovery = (kind == EngineKind::Memory)
                .then(crate::common::kv_snapshot::recovery_probe::Scope::start);
            let (d, live, peak, db) =
                timed_phase(label, "bulk_reopen", || open_engine(kind, arg).unwrap());
            if label == "lsm_tree" {
                scale_note_maintenance(&*db, "bulk_reopen", &maintenance);
            }
            if let Some(scope) = recovery {
                eprintln!("SCALE_RECOVERY phase=bulk stages={:?}", scope.stats());
            }
            crate::scale_probe::memory(label, "bulk_after_reopen");
            line(
                label,
                "bulk 正常关闭后重开",
                d.as_secs_f64() * 1e6,
                "µs",
                live,
                peak,
                &format!("堆增量 {:.1} MB", live as f64 / 1e6),
            );
            open_stages_report(label, st_base);
            db
        };

        // 独立生成完整清单，不计入恢复计时；核验
        // 所有批量新增行和原有行，不能只检查第一个键。
        scale_check_contents(&*db, rows, ScaleExpected::BulkReopen { bulk_rows })
            .unwrap_or_else(|e| panic!("{label}: bulk 重开验收失败: {e}"));
        eprintln!(
            "{label}: bulk 重开验收通过(不计时) {} 个唯一键，完整键/最新值/计数器/用户字节/TTL 均与独立种子相符",
            rows + ttl_rows + 1 + bulk_rows
        );
        drop(db);

        // 明确在成功标记之前清理；错误不能被脚本误判为完整通过。
        std::fs::remove_dir_all(&scratch.0)
            .unwrap_or_else(|e| panic!("台架清理失败 {}: {e}", scratch.0.display()));
        drop(scratch);
        eprintln!("SCALE_ENGINE_DONE rows={rows} engine={label}");
        eprintln!();
    }
    eprintln!("SCALE_DONE rows={rows} engines=4");
}
