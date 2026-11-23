// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// WAL 阈值触发必须真的把文件截住（对照「关闭自动触发」），且截断之后重开仍能读到
/// 全部已提交数据 —— 后半段守的是「先 flush（mem→L0）再收口 WAL」这个顺序。
///
/// 量测统一放在实例还活着的时候：bplus 那边 `impl Drop for MVCC` 会顺手 checkpoint，
/// 量在作用域外会让两种配置都得同样的数字、断言空转。本引擎的 drop 只落 `MVCC.NEXT`，
/// 但照同一个口径写，免得以后有人改 drop 时这条守卫静默失效。
#[test]
fn test_wal_auto_checkpoint_bounds_file_and_keeps_data() {
    const WRITES: u32 = 40;

    fn wal_bytes_after(tag: &str, limit: u64) -> u64 {
        let path = tmp_db(tag);
        let size;
        {
            let mvcc = MVCC::open(&path);
            mvcc.set_wal_checkpoint_limit(limit);
            for i in 0..WRITES {
                let key = format!("k{i}");
                let tx = mvcc.begin_transaction();
                assert!(tx.set(key.as_bytes(), vec![b'x'; 200]));
                tx.commit();
            }
            size = std::fs::metadata(wal_path(&path))
                .map(|m| m.len())
                .unwrap_or(0);
        }
        cleanup(&path);
        size
    }

    let manual = wal_bytes_after("wal_manual", 0);
    let auto = wal_bytes_after("wal_auto", 1);
    eprintln!("WAL 体积：关闭自动 checkpoint {manual} 字节 vs 阈值触发 {auto} 字节");
    assert!(
        auto * 4 <= manual,
        "阈值触发应把 WAL 截住（自动 {auto}，手动 {manual}）"
    );

    let path = tmp_db("wal_reopen");
    {
        let mvcc = MVCC::open(&path);
        mvcc.set_wal_checkpoint_limit(1);
        for i in 0..WRITES {
            let key = format!("r{i}");
            let tx = mvcc.begin_transaction();
            assert!(tx.set(key.as_bytes(), format!("v{i}").into_bytes()));
            tx.commit();
        }
    }
    {
        let mvcc = MVCC::open(&path);
        let tx = mvcc.begin_transaction();
        for i in 0..WRITES {
            let key = format!("r{i}");
            assert_eq!(
                tx.get(key.as_bytes()),
                Some(format!("v{i}").into_bytes()),
                "第 {i} 条已提交写入在 WAL 被截断后仍应读到"
            );
        }
        tx.commit();
    }
    cleanup(&path);
}

#[test]
fn test_disk_persist_across_reopen() {
    let path = tmp_db("persist");
    {
        let mvcc = MVCC::open(&path);
        let t1 = mvcc.begin_transaction();
        assert!(t1.set(b"hello", b"world".to_vec()));
        t1.commit();
        let t2 = mvcc.begin_transaction();
        assert!(t2.set(b"hello", b"rust".to_vec()));
        assert!(t2.set(b"foo", b"bar".to_vec()));
        t2.commit();
        // 确保快照落盘（commit 默认不 flush 页，靠 drop 或显式 flush）
        mvcc.flush();
    }
    {
        let mvcc = MVCC::open(&path);
        let tx = mvcc.begin_transaction();
        assert_eq!(tx.get(b"hello"), Some(b"rust".to_vec()));
        assert_eq!(tx.get(b"foo"), Some(b"bar".to_vec()));
        tx.commit();
    }
    cleanup(&path);
}

/// 反面对照（M28）：**有 redo 时 open 必须先把它落盘，再截断 WAL**。
///
/// 上面那条把 `tree.flush()` 改成有条件，就必须有一条钉住"条件为真时仍然要 flush"——
/// 否则把 flush 整个删掉也能过第一条，而那样 `seal_wal_checkpoint` 会先把重放来源
/// （WAL）截掉，重放进 memtable 的行只活在内存里，进程一没电就真的丢了。
/// 所以这条量的是**崩溃之后的第二次重开还读不读得回来**。
#[test]
fn test_open_persists_redo_before_truncating_wal() {
    let path = tmp_db("open_redo_persist");
    {
        // 空库：M28 起此时没有 redo，open 不落 SST（上一条已钉住这件事）
        drop(MVCC::open(&path));
    }
    {
        // 模拟崩溃：已提交的 Write 只在 WAL 里，SST 里没有它
        let mut wal = Wal::open(wal_path(&path)).unwrap();
        let xid = 7u64;
        wal.append(WalRecord::Begin { xid }).unwrap();
        wal.append(WalRecord::Write {
            xid,
            key: encode_key(b"crash-key", xid),
            value: Some(b"payload".to_vec()),
        })
        .unwrap();
        wal.append(WalRecord::Commit { xid }).unwrap();
        wal.sync().unwrap();
    }
    {
        let mvcc = MVCC::open(&path);
        let st = mvcc.kv.lock().unwrap().stats();
        assert_eq!(
            st.mem_entries, 0,
            "重放进 memtable 的 redo 必须在截断 WAL 之前 flush 掉（还留在 memtable = 只活在内存里）"
        );
        let tx = mvcc.begin_transaction();
        assert_eq!(tx.get(b"crash-key"), Some(b"payload".to_vec()));
        tx.commit();
    }
    // 第二次重开：WAL 已被第一次重开截断 ⇒ 这次读到的只能来自 SST
    {
        let mvcc = MVCC::open(&path);
        let tx = mvcc.begin_transaction();
        assert_eq!(
            tx.get(b"crash-key"),
            Some(b"payload".to_vec()),
            "WAL 截断后还读得回来，才说明 redo 真的落了盘"
        );
        tx.commit();
    }
    cleanup(&path);
}

/// 重开剖面（M28 归因）：**bulk 之后那次冷重开**为什么在 600k 涨到 17.5 s。
///
/// 触发它的测量（2026-10-18 三档基线，同一构建 `f20b7fd`，release，数据在 C: 的 SSD）：
/// `lsm_tree` 的第一次 `冷重开(恢复)` = 13 982.3 µs，而**第二次**（`再冷重开`）
/// 20k = 10 237.3 / 200k = 10 296.5 / **600k = 17 487 030.8 µs** ⇒ 原始 1708 倍，
/// 除掉"整库一趟"该涨的 30 倍还剩 57 倍。重开后常驻 1.6 → **12.1 MB**。
/// 两次重开之间变了只有那串写（更新 10% 键、vacuum、flush）和 `bulk 灌 rows/10`。
///
/// 三条候选，本条逐条证伪或坐实（**只量，不改任何语义**）：
/// 1. `Wal::read_all` + `analyze_recovery` 全量解析 —— 帧头/帧体各一次定位读，而
///    [`crate::common::DiskFile::read_exact_at`] 是 seek+read ⇒ **每条记录 4 次系统调用**
/// 2. open 结尾那句无条件 `tree.flush()` ⇒ redo 落成 SST 并顺带 `maybe_compact` 级联
/// 3. `load_manifest` 逐个开 SST —— bulk 模式不在写路径 compact，L0 文件数在 600k 才越过
///    `l0_compact_threshold`(8)
///
/// 拆成四条臂，各拿一份目录副本（同一份 shape，只差在臂本身）：
/// - `facade`：就走门面 `MVCC::open`，给的是台架里那根尺子 —— 用来确认剖面**复现了悬崖**
/// - `uncond_flush` / `cond_flush`：手拆 `try_open_with` 的每一步、逐段计时 + 计数器，
///   两臂只差「open 里的 flush 是无条件还是有条件」⇒ **同一轮里的 A/B**，不用跨构建比墙钟
/// - `no_bulk(有 redo)`：另一份不跑 bulk 的 shape，WAL 里留着 6 万条待重放 ⇒ 条件为真、
///   照样 flush —— 这条留着是为了**如实报剩下那笔没修的账**（见下）
///
/// # 量出来的答案（release，600k 键，数据在 C: 的 SSD）
///
/// - 悬崖**整段在 `flush` 里**：同一轮里 `uncond_flush` 总 **13 947 618.8 µs**（flush 段
///   13 938 766.2），`cond_flush` 总 **8 362.3 µs**（flush 段 0.0）⇒ **1668×**，其余九段
///   合起来 ≈ 3 ms。机制是 `LSMTree::flush()` 末尾无条件 `maybe_compact()`，而 bulk 的
///   `finish()` 把 L0 正好留在 `l0_compact_threshold`(8) 上：**8 个 L0 越线 → 打开库这件事
///   自己把 85 MB 搬进 L1/L2、写出 37 个文件**（层文件 `[8,0,0,0] → [0,34,3,0]`、盘上
///   8 个 85 MB → 37 个 74 MB；有条件那臂两处都不动）。
/// - 候选 1（WAL 全量解析）**证伪**：有 bulk 时重开前 WAL 只剩 32 B / 0 条记录（`finish()`
///   已收口），`read_all` 3.4 µs。但这条通道本身是贵的 —— `no_bulk` 臂里 60121 条记录 /
///   8.7 MB 的 `read_all` = 254.7 ms ⇒ **每条 ≈ 4.2 µs**（帧头+帧体各一次定位读，而
///   `DiskFile::read_exact_at` 是 seek+read），这是"崩溃点离上次 checkpoint 越远、重开越慢"
///   的另一笔账，量级远小于压实那笔。
/// - 200k 不炸的原因就是那条阈值：同一 shape 在 200k 只有 **4 个 L0 文件**（<8）⇒ 两臂同价
///   （9.8 ms vs 6.4 ms，都不压实）。
/// - `[facade]`（走修后的门面）= **13.3 ms**。同档修前的台架读数是 17.5~19.8 s（跨轮噪声 1.3×，
///   只当量级看）；因果凭据取上面那对**同轮**数字 + 层文件数不动。
/// - ✅ 当时"没修完的部分"：`no_bulk` 那条（有 redo 的崩溃恢复）仍然 14.4 s，其中 flush 13.7 s ——
///   因为 redo 落了盘就必须 compact。**这笔后来分两次收掉**：M30 查到时间元凶是
///   `SstBuilder::estimated_bytes()` 每交一行重求和整表（同档 14.43 s → **3.55 s**），
///   M33 把剩下的"输入整篇物化"（内存账，峰值 156.8 MB）改成流式源（→ 6.3 MB）。
///   下面 `compaction_cost_split` 的注释里有两笔的完整拆分。
///
/// 略去的写：`set_with_ttl×1000` / `incr×1000`（各 1000 行 ≈ 0.2 MB WAL，跟 60 万行那串比是零头）。
///
/// ```text
/// REOPEN_PROBE_ROWS=600000 cargo test --release --lib -- --ignored --nocapture \
///     --test-threads=1 reopen_shape_probe
/// ```
#[test]
#[ignore = "重开剖面，手动 release 单线程跑：REOPEN_PROBE_ROWS=600000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 reopen_shape_probe"]
fn reopen_shape_probe() {
    use std::time::{Duration, Instant};

    let rows: usize = std::env::var("REOPEN_PROBE_ROWS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200_000);

    // 键/值形状照抄门面台架 engine.rs 的 scale_baseline（键 ≈35 B、值 ≈60 B，都不进 blob 旁路）
    fn key(i: usize) -> Vec<u8> {
        format!("{i:08}#/worldquant/datafields/x.json").into_bytes()
    }
    fn val(i: usize) -> Vec<u8> {
        format!("{{\"i\":{i},\"h\":\"{:0<28}\",\"v\":1}}", i * 7).into_bytes()
    }
    fn us(d: Duration) -> f64 {
        d.as_secs_f64() * 1e6
    }

    fn copy_tree(from: &Path, to: &Path) -> std::io::Result<()> {
        std::fs::create_dir_all(to)?;
        for e in std::fs::read_dir(from)? {
            let e = e?;
            let dest = to.join(e.file_name());
            if e.file_type()?.is_dir() {
                copy_tree(&e.path(), &dest)?;
            } else {
                std::fs::copy(e.path(), &dest)?;
            }
        }
        Ok(())
    }

    /// `dir/sst` 里的文件数与字节数 —— 候选 3 的直接计数（不看 stats，看盘上真有几个文件）
    fn sst_dir_stats(dir: &Path) -> (usize, u64) {
        let mut n = 0usize;
        let mut b = 0u64;
        let Ok(rd) = std::fs::read_dir(dir.join("sst")) else {
            return (0, 0);
        };
        for e in rd.flatten() {
            if let Ok(m) = e.metadata() {
                if m.is_file() {
                    n += 1;
                    b += m.len();
                }
            }
        }
        (n, b)
    }

    /// 把门面台架里「两次重开之间」那串做完，返回给调用方（此刻库已 close）
    fn build_shape(dir: &Path, rows: usize, with_bulk: bool) {
        let mvcc = MVCC::open(dir);
        for base in (0..rows).step_by(1000) {
            let end = (base + 1000).min(rows);
            let items: Vec<(Vec<u8>, Vec<u8>)> = (base..end).map(|i| (key(i), val(i))).collect();
            let tx = mvcc.begin_transaction();
            if let Err(bad) = tx.batch_set(&items) {
                panic!("灌第 {base} 批冲突: {}", String::from_utf8_lossy(&bad));
            }
            tx.commit();
        }
        mvcc.flush();
        mvcc.checkpoint();
        drop(mvcc);

        // ---- 第一次冷重开（就是台架里的 冷重开(恢复)，它自己会把 WAL 收口）----
        let mvcc = MVCC::open(dir);
        // 更新 10% 的键：制造版本链，vacuum 才有得裁
        let updates = (rows / 10).max(1);
        for base in (0..updates).step_by(500) {
            let end = (base + 500).min(updates);
            let items: Vec<(Vec<u8>, Vec<u8>)> =
                (base..end).map(|i| (key(i), val(i + rows))).collect();
            let tx = mvcc.begin_transaction();
            if let Err(bad) = tx.batch_set(&items) {
                panic!("更新第 {base} 批冲突: {}", String::from_utf8_lossy(&bad));
            }
            tx.commit();
        }
        let h = mvcc.health();
        let v = mvcc.vacuum().unwrap();
        let _ = mvcc.vacuum_bounded(None, 2048);
        eprintln!(
            "  shape(rows={rows}, bulk={with_bulk}): health rows={} live={} | vacuum 裁 {} 行",
            h.total_rows, h.live_keys, v.versions_removed
        );
        mvcc.flush();
        if with_bulk {
            let bulk_rows = (rows / 10).max(1);
            let mut bulk = mvcc.begin_bulk();
            let items: Vec<(Vec<u8>, Vec<u8>)> =
                (0..bulk_rows).map(|i| (key(rows + i), val(i))).collect();
            bulk.put_batch_owned(items);
            bulk.finish();
        }
        drop(mvcc);
    }

    /// 手拆 `try_open_with`：每一步单独计时，计数打在最后。
    /// `flush_at_open` = 这次拆的是修前(真)还是修后(假)的那份序列 —— 留成同一轮里的 A/B 对照
    fn probe_open(arm: &str, dir: &Path, flush_at_open: bool) {
        let t = Instant::now();
        let opts = pin_mvcc_invariants(LSMTreeOptions::for_mvcc());
        let mut tree = LSMTree::open_with_config(dir, opts).unwrap();
        let d_tree = t.elapsed();

        let t = Instant::now();
        let mut wal = Wal::open(wal_path(dir)).unwrap();
        let d_wal_open = t.elapsed();
        let wal_bytes = wal.size_bytes().unwrap();

        let t = Instant::now();
        let entries = wal.read_all().unwrap();
        let d_read_all = t.elapsed();

        let t = Instant::now();
        let plan = analyze_recovery(&entries);
        let d_analyze = t.elapsed();

        let t = Instant::now();
        for (_xid, k, v) in &plan.committed_writes {
            tree.insert(k.clone(), v.clone());
        }
        for (_xid, k) in &plan.uncommitted_writes {
            let _ = tree.remove(k);
        }
        let d_redo = t.elapsed();

        let t = Instant::now();
        let start_ver = compute_start_version(dir, &plan);
        let d_start_ver = t.elapsed();

        let before = tree.stats();
        let (files_before, bytes_before) = sst_dir_stats(dir);
        // `has_redo` 的口径与 `try_open_with` 里那句一致：判的是"有没有东西要重放"，
        // 不是"memtable 现在空不空"（那在插过之后就已经分不出了）。
        let has_redo = !plan.committed_writes.is_empty() || !plan.uncommitted_writes.is_empty();
        let did_flush = flush_at_open || has_redo;
        let t = Instant::now();
        if did_flush {
            tree.flush().unwrap();
        }
        let d_flush = t.elapsed();
        let after = tree.stats();
        let (files_after, bytes_after) = sst_dir_stats(dir);

        let t = Instant::now();
        seal_wal_checkpoint(&mut wal, start_ver).unwrap();
        let d_seal = t.elapsed();
        let t = Instant::now();
        persist_mvcc_next(dir, start_ver);
        let d_persist = t.elapsed();
        let total = d_tree
            + d_wal_open
            + d_read_all
            + d_analyze
            + d_redo
            + d_start_ver
            + d_flush
            + d_seal
            + d_persist;

        eprintln!(
            "  [{arm}] 总计 {:>12.1} µs | tree.open_with_config {:>12.1} | Wal::open {:>8.1} \
                 | read_all {:>12.1} | analyze {:>10.1} | redo {:>10.1} | start_ver {:>8.1} \
                 | flush {:>12.1} | seal {:>8.1} | persist {:>8.1}",
            us(total),
            us(d_tree),
            us(d_wal_open),
            us(d_read_all),
            us(d_analyze),
            us(d_redo),
            us(d_start_ver),
            us(d_flush),
            us(d_seal),
            us(d_persist),
        );
        eprintln!(
            "  [{arm}] 计数: flush_at_open={flush_at_open} did_flush={did_flush} | \
                 WAL {} B / 记录 {} 条（帧均 {} B）| redo committed={} uncommitted={} \
                 | start_ver={} | 盘上 sst {} 个 {} MB → {} 个 {} MB | L0 {} → {} | 层文件 {:?} → {:?}",
            wal_bytes,
            entries.len(),
            if entries.is_empty() {
                0
            } else {
                wal_bytes.saturating_sub(16) / entries.len() as u64
            },
            plan.committed_writes.len(),
            plan.uncommitted_writes.len(),
            start_ver,
            files_before,
            bytes_before / 1_048_576,
            files_after,
            bytes_after / 1_048_576,
            before.l0_files,
            after.l0_files,
            before.level_files,
            after.level_files,
        );
    }

    eprintln!("=== 重开剖面 rows={rows}（µs，release，单线程）===");
    let base_dir = tmp_db("reopen_probe_src");
    let t = Instant::now();
    build_shape(&base_dir, rows, true);
    eprintln!("  shape 构建耗时 {:.1} s", t.elapsed().as_secs_f64());
    let (f0, b0) = sst_dir_stats(&base_dir);
    eprintln!("  构建完盘上: sst {} 个 {} MB", f0, b0 / 1_048_576);

    // 臂 1：门面重开（台架里那根尺子）
    let d_facade = tmp_db("reopen_probe_facade");
    copy_tree(&base_dir, &d_facade).unwrap();
    let t = Instant::now();
    drop(MVCC::open(&d_facade));
    eprintln!("  [facade] MVCC::open 整条 {:>12.1} µs", us(t.elapsed()));

    // 臂 2/3：同一份目录副本，只换"open 里 flush 是无条件还是有条件" ⇒ 一轮内的 A/B
    let d_uncond = tmp_db("reopen_probe_uncond");
    copy_tree(&base_dir, &d_uncond).unwrap();
    probe_open("uncond_flush", &d_uncond, true);
    let d_cond = tmp_db("reopen_probe_cond");
    copy_tree(&base_dir, &d_cond).unwrap();
    probe_open("cond_flush", &d_cond, false);

    // 臂 4：不跑 bulk 的另一份 shape（WAL 里有 6 万条待重放）⇒ 条件为真，仍然会 flush，
    // 于是这条把**有 redo 那条路径的整笔代价**单独暴露出来（当年挂的是"输入整篇物化"，
    // 时间那一半 M30 收掉、内存那一半 M33 收掉；留着这条臂是为了看两笔收完之后还剩多少）
    let d_nobulk = tmp_db("reopen_probe_nobulk");
    build_shape(&d_nobulk, rows, false);
    probe_open("no_bulk(有 redo)", &d_nobulk, false);

    for p in [&base_dir, &d_facade, &d_uncond, &d_cond, &d_nobulk] {
        cleanup(p);
    }
}
