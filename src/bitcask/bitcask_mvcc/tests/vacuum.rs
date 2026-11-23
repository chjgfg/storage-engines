// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn bitcask_vacuum_empty_blob_skips_value_reads_with_reference_control() {
    for reference in [true, false] {
        let dir = tmp_db("empty_blob_control");
        let db = MVCC::open(&dir);
        db.set_vacuum_auto_slice(0);
        for pass in 0..2 {
            let mut bulk = db.begin_bulk();
            for i in 0..64 {
                bulk.put(
                    format!("key-{i:04}").as_bytes(),
                    crate::common::pack_plain(&vec![pass; 32]),
                );
            }
            bulk.finish();
        }
        let mode = reference.then(vacuum_cost_reference::Scope::start);
        let probe = crate::common::maintenance_probe::Scope::start();
        let before = crate::bitcask::bitcask::read_value_calls();
        let stats = db.vacuum().unwrap();
        let reads = crate::bitcask::bitcask::read_value_calls() - before;
        assert_eq!(stats.versions_removed, 64);
        assert_eq!(stats.rows_scanned, 128);
        if reference {
            assert_eq!(
                reads, 64,
                "negative control must really read every retained value"
            );
        } else {
            assert_eq!(reads, 0, "empty blob must bypass retained value reads");
            let stages = probe.stats();
            assert_eq!(stages.bitcask_empty_sweeps, 1);
            assert_eq!(stages.bitcask_value_reads, 0);
            assert!(stages.calls[crate::common::maintenance_probe::Stage::VacuumScan as usize] > 0);
        }
        drop(probe);
        drop(mode);
        drop(db);
        let reopened = MVCC::open(&dir);
        let tx = reopened.begin_transaction();
        for i in 0..64 {
            assert_eq!(tx.get(format!("key-{i:04}").as_bytes()), Some(vec![1; 32]));
        }
        tx.commit();
    }
}

#[test]
fn bitcask_vacuum_blob_crc_or_main_read_failure_never_rewrites_blob() {
    use std::io::{Seek, SeekFrom, Write};
    for corrupt_blob in [true, false] {
        let dir = tmp_db("vacuum_read_failure");
        let db = MVCC::open(&dir);
        let mut bulk = db.begin_bulk();
        bulk.put(b"large", crate::common::pack_plain(&vec![7; 512]));
        bulk.finish();
        if corrupt_blob {
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .open(blob_path(&dir))
                .unwrap();
            file.seek(SeekFrom::Start(16)).unwrap();
            file.write_all(&[8]).unwrap();
        } else {
            std::fs::OpenOptions::new()
                .write(true)
                .open(data_path(&dir))
                .unwrap()
                .set_len(0)
                .unwrap();
        }
        let before = std::fs::read(blob_path(&dir)).unwrap();
        assert!(
            db.vacuum().is_err(),
            "read/decode errors must reach the caller before GC"
        );
        assert_eq!(std::fs::read(blob_path(&dir)).unwrap(), before);
    }
}

#[test]
fn bitcask_vacuum_blob_preserves_latest_large_values_and_inline_rows() {
    let dir = tmp_db("vacuum_mixed");
    let db = MVCC::open(&dir);
    for pass in 0..2 {
        let mut bulk = db.begin_bulk();
        bulk.put(b"large", crate::common::pack_plain(&vec![pass; 512]));
        bulk.put(b"small", crate::common::pack_plain(&vec![pass; 32]));
        bulk.finish();
    }
    let probe = crate::common::maintenance_probe::Scope::start();
    let stats = db.vacuum().unwrap();
    assert_eq!(stats.versions_removed, 2);
    assert_eq!(probe.stats().bitcask_reference_sweeps, 1);
    assert_eq!(probe.stats().blob_point_reads, 1);
    drop(probe);
    drop(db);
    let db = MVCC::open(&dir);
    let tx = db.begin_transaction();
    assert_eq!(tx.get(b"large"), Some(vec![1; 512]));
    assert_eq!(tx.get(b"small"), Some(vec![1; 32]));
    tx.commit();
}

/// 泄漏事务（既不 commit 也不 rollback 就丢弃）**不得**永久卡死 vacuum 的水位线。
///
/// 没有 `impl Drop for Transaction` 时，漏网的 xid 会一直挂在 `active_txn` 里，
/// `xmin = min(活跃事务 id)` 因此永久停在它身上 —— 它之后的每一次写入都变成
/// 「比 xmin 新、永远不能裁」，于是版本回收**整体停摆**，且之后每个事务的
/// 可见性快照都多带一个死 id。
///
/// 故意把泄漏放在造旧版本**之前**：这样漏网 id 是全场最小，`xmin` 被它钉死，
/// 去掉 Drop 兜底这条测试就会红（顺序反过来则仍能裁掉，断言咬不住）。
#[test]
fn test_leaked_transaction_does_not_block_vacuum() {
    let path = tmp_db("leak_vacuum");
    {
        let mvcc = MVCC::open(&path);

        // 先泄漏：只 begin + 写，然后直接丢弃
        {
            let tx = mvcc.begin_transaction();
            assert!(tx.set(b"leak", b"x".to_vec()));
        }

        for i in 0..3u64 {
            let tx = mvcc.begin_transaction();
            assert!(tx.set(b"k", format!("v{i}").into_bytes()));
            tx.commit();
        }

        assert!(
            mvcc.active_txn.lock().unwrap().is_empty(),
            "泄漏事务应已被 Drop 回收，而不是永久留在活跃表里"
        );

        let stats = mvcc.vacuum().unwrap();
        assert!(
            stats.versions_removed > 0,
            "xmin 应能推进到 next_version 并裁掉 k 的旧版本；裁不动说明活跃表里有死 id 事务"
        );

        let tx = mvcc.begin_transaction();
        assert_eq!(tx.get(b"leak"), None, "泄漏事务的写入应已被回滚");
        assert_eq!(tx.get(b"k"), Some(b"v2".to_vec()));
        tx.rollback();
    }
    cleanup(&path);
}

/// 复现「旧水位线会裁掉活跃事务快照仍需要的版本」⇒ **静默读到错值**。
///
/// 形状：`hot` 有 v1、v2 两版。B 提交 v2 **之前** T1 已经 begin ⇒ v2 落在 T1 的
/// 并发快照里、对 T1 永久不可见 ⇒ T1 只能回退到 v1。而此时
/// `xmin = min(活跃事务 id) = T1.id`，「最后一个 < xmin」的版本正好是 v2，
/// 于是按规则 v1 被裁掉 —— T1 再读就成了 None。
///
/// 这就是 12e3696 想修、当天又被 f78edbb revert 回来的那个洞。revert 信息里
/// 承认「旧水位线的已知问题随之回来了」，但仓库里没有一条测试钉住它 ——
/// 缺的就是这条。
#[test]
fn test_vacuum_keeps_versions_needed_by_active_txn() {
    let path = tmp_db("vacuum_active");
    {
        let mvcc = MVCC::open(&path);

        let t0 = mvcc.begin_transaction();
        assert!(t0.set(b"hot", b"v1".to_vec()));
        t0.commit();

        let b = mvcc.begin_transaction();
        assert!(b.set(b"hot", b"v2".to_vec()));
        let t1 = mvcc.begin_transaction(); // T1 的快照里有 b
        b.commit(); // v2 提交了，但对 T1 永久不可见

        assert_eq!(
            t1.get(b"hot"),
            Some(b"v1".to_vec()),
            "读前提：T1 应当看到 v1（v2 在它的并发快照里）"
        );

        let stats = mvcc.vacuum().unwrap();
        assert!(
            stats.skipped_active_txn,
            "有活跃事务时 vacuum 应整轮跳过，而不是按 min(活跃 id) 去裁版本"
        );
        assert_eq!(stats.versions_removed, 0, "跳过的那一轮不该裁任何东西");
        assert_eq!(
            t1.get(b"hot"),
            Some(b"v1".to_vec()),
            "vacuum 不得裁掉活跃事务快照仍需要的版本（裁错 = 静默读到旧值/None）；本次 xmin={}",
            stats.xmin
        );

        t1.rollback();

        // T1 结束之后再 vacuum：这次必须真的能裁（否则「整轮跳过」会变成永久不回收）
        let stats = mvcc.vacuum().unwrap();
        assert!(
            !stats.skipped_active_txn,
            "没有活跃事务时不该再跳过，否则版本链永远裁不掉"
        );
        assert!(
            stats.versions_removed > 0,
            "hot 的旧版本这次应被裁掉，实测 {} 次",
            stats.versions_removed
        );
        let t2 = mvcc.begin_transaction();
        assert_eq!(t2.get(b"hot"), Some(b"v2".to_vec()), "裁完应读到最新版本");
        t2.rollback();
    }
    cleanup(&path);
}

/// 长跑进程不喊 vacuum，版本链也不能无限涨（I2：垃圾必须自己回来）。
///
/// 事故那条曲线的本质就是版本行只增不减 —— 一天/一个多月里 CPU 慢慢爬。
/// 这里把切片宽度调到 8 行，写 200 版热点 key，断言：
/// 1. 没人叫过 vacuum，链也被压住（自动回收到位在跑）；
/// 2. 最新值始终读得到 —— 自动裁的是旧版本，不是可见版本；
/// 3. 默认是**开着**的（关掉它就等于把这次事故原样留给下一个项目）。
#[test]
fn test_auto_vacuum_reclaims_without_being_asked() {
    const CHAIN: u64 = 200;
    const SLICE: u64 = 8;

    let path = tmp_db("auto_vac");
    {
        let mvcc = MVCC::open(&path);
        assert_eq!(
            mvcc.vacuum_auto_slice(),
            MVCC::VACUUM_AUTO_SLICE_DEFAULT,
            "自动回收默认必须开着"
        );
        mvcc.set_vacuum_auto_slice(SLICE);

        for i in 0..CHAIN {
            let tx = mvcc.begin_transaction();
            assert!(tx.set(b"hot", format!("v{i}").into_bytes()));
            tx.commit();
        }

        let h = mvcc.health();
        assert!(
            h.max_chain <= SLICE + 2,
            "没人喊 vacuum，链却涨到 {} 行 —— 自动回收没在工作",
            h.max_chain
        );

        let tx = mvcc.begin_transaction();
        assert_eq!(tx.get(b"hot"), Some(format!("v{}", CHAIN - 1).into_bytes()));
        tx.commit();
    }
    cleanup(&path);
}

/// 关掉的开关必须真关掉：自动回收不能是「没法关掉的那种」。
///
/// 有了它就允许上层把回收完全交给自己调度（比如只在交易空档跑），
/// 也保证默认值不是「悄悄多做了一件事」。
#[test]
fn test_auto_vacuum_off_switch() {
    const CHAIN: u64 = 60;

    let path = tmp_db("auto_off");
    {
        let mvcc = MVCC::open(&path);
        mvcc.set_vacuum_auto_slice(0);
        for i in 0..CHAIN {
            let tx = mvcc.begin_transaction();
            assert!(tx.set(b"hot", format!("v{i}").into_bytes()));
            tx.commit();
        }
        assert_eq!(mvcc.health().max_chain, CHAIN, "关闭后不该裁掉任何一版");
    }
    cleanup(&path);
}

/// 有界回收：每轮只碰一小段，多轮跑完必须和一次全库 vacuum 得到**同样**的结果。
///
/// `vacuum()` 把 kv 锁按到整库扫完 —— 单核机器上「开自动回收」就会在回收那一刻
/// 制造新事故。切片后每轮读的行数有上限（偏差 = 一条链的长度：截断只能落在逻辑 key
/// 边界，半条链没法判「留哪一版」），而游标每轮必须真的前进（`rounds` 的上界就是这条）。
///
/// 键里刻意放了前缀嵌套的 `k1` / `k1x`：版本键是 `raw ‖ 8B`，真前缀会让两把 key 的
/// 版本区间互相穿插，切片最容易在这里切错。
#[test]
fn test_vacuum_bounded_slices_and_converges() {
    const CHAIN: u64 = 120;
    const KEYS: [&[u8]; 6] = [b"k1", b"k1x", b"k2", b"k3", b"k4", b"k5"];
    const SLICE: u64 = 40;

    fn fill(mvcc: &MVCC) {
        for i in 0..CHAIN {
            for k in KEYS {
                let tx = mvcc.begin_transaction();
                assert!(tx.set(k, format!("v{i}").into_bytes()), "写入被拒：{k:?}");
                tx.commit();
            }
        }
    }

    // 对照组：一次全库 vacuum 裁掉多少
    let full_path = tmp_db("bounded_full");
    let full_removed;
    {
        let mvcc = MVCC::open(&full_path);
        fill(&mvcc);
        full_removed = mvcc.vacuum().unwrap().versions_removed;
    }
    cleanup(&full_path);

    let path = tmp_db("bounded_slice");
    {
        let mvcc = MVCC::open(&path);
        fill(&mvcc);

        let mut cursor: Option<Vec<u8>> = None;
        let mut rounds = 0usize;
        let mut removed = 0usize;
        loop {
            let round = mvcc.vacuum_bounded(cursor.as_deref(), SLICE).unwrap();
            assert!(
                !round.stats.skipped_active_txn,
                "第 {rounds} 轮不该被活跃事务挡住"
            );
            assert!(
                round.stats.rows_scanned <= SLICE + CHAIN,
                "第 {rounds} 轮读了 {} 行（预算 {SLICE} + 一条链 {CHAIN}）",
                round.stats.rows_scanned
            );
            assert!(
                round.stats.versions_removed > 0,
                "第 {rounds} 轮必须裁掉东西，否则游标没前进"
            );
            removed += round.stats.versions_removed;
            rounds += 1;
            match round.next_cursor {
                Some(c) => {
                    assert_ne!(cursor.as_ref(), Some(&c), "游标必须前进（第 {rounds} 轮）");
                    cursor = Some(c);
                }
                None => break,
            }
            assert!(
                rounds < 60,
                "切片应在有限轮内收敛，别是原地打转：已 {rounds} 轮"
            );
        }

        assert!(
            rounds > 1,
            "6 把 key × {CHAIN} 版不该一轮就扫完：{rounds} 轮"
        );
        assert_eq!(removed, full_removed, "分片裁的总行数应与整库一轮一致");

        let h = mvcc.health();
        assert_eq!(h.max_chain, 1, "分片跑完后链必须压平");
        assert_eq!(h.total_rows, KEYS.len() as u64);

        let tx = mvcc.begin_transaction();
        for k in KEYS {
            assert_eq!(tx.get(k), Some(format!("v{}", CHAIN - 1).into_bytes()));
        }
        tx.commit();
    }
    cleanup(&path);
}

/// 一次全库 `vacuum()` 的瞬时内存分在哪一段（M37 的先量，`#[ignore]`）。
///
/// 为什么要拆：修好量具（M38）之后 600k 档 `vacuum()` 读到 **171.4 MB**，而这一段里叠着
/// 四件事 —— ①归组判定、②删除 + `flush` + blob `sync`、③blob GC（读存活行 +
/// `rewrite_in_place`）、④`kv.merge()` 重写主日志。"存活行清单"是①里那块，但 M30/M31
/// 两次的教训都是**最合理的那件事不是元凶**，所以先分段再决定这一刀砍哪里。
/// 每段之前都 `reset_peak()` ⇒ 报的是**这一段自己**的瞬时峰值，不是进程历史最高点。
///
/// ```text
/// BITCASK_VAC_ROWS=200000 cargo test --release --lib -- --ignored --nocapture \
///     --test-threads=1 bitcask_vacuum_stage_split
/// ```
#[test]
#[ignore = "vacuum 分段内存台架，手动 release 单线程跑：BITCASK_VAC_ROWS=200000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 bitcask_vacuum_stage_split"]
fn bitcask_vacuum_stage_split() {
    use std::time::Instant;
    let rows: u64 = std::env::var("BITCASK_VAC_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200_000);
    let mb = |b: u64| b as f64 / 1024.0 / 1024.0;
    let path = tmp_db("vac_stages");
    {
        let mvcc = MVCC::open(&path);
        // 每把键两个版本 ⇒ 第二趟写完之后第一版全都 < xmin，全部可裁
        for round in 0..2u64 {
            let tx = mvcc.begin_transaction();
            for i in 0..rows {
                let k = format!("{i:08}#/worldquant/datafields/x.json");
                assert!(
                    tx.set(k.as_bytes(), format!("round{round}").into_bytes()),
                    "第 {i} 把键写失败"
                );
            }
            tx.commit();
        }
        let t = Instant::now();
        let st = mvcc.vacuum().unwrap();
        let d = t.elapsed();
        let p = crate::mem_probe::stage_peaks();
        println!(
            "=== bitcask vacuum 分段瞬时峰值 rows={rows} 裁掉 {} 行 / 扫 {} 行，总耗时 {:.2} s ===",
            st.versions_removed,
            st.rows_scanned,
            d.as_secs_f64()
        );
        println!(
            "  ① 归组判定 {:>8.1} MB ｜ ② 删除+flush+sync {:>8.1} MB ｜ ③ blob GC {:>8.1} MB ｜ ④ merge {:>8.1} MB（M81 起 vacuum 不再自带 merge ⇒ 这一格恒 0，merge 的账去看门 M 那一扇）",
            mb(p[0]),
            mb(p[1]),
            mb(p[2]),
            mb(p[3])
        );
        println!(
            "  合计（分段各自峰值，非同时成立）{:>8.1} MB ｜ 每扫描行摊 ①={:.0} B ②={:.0} B ③={:.0} B ④={:.0} B",
            mb(p[0] + p[1] + p[2] + p[3]),
            p[0] as f64 / st.rows_scanned.max(1) as f64,
            p[1] as f64 / st.rows_scanned.max(1) as f64,
            p[2] as f64 / st.rows_scanned.max(1) as f64,
            p[3] as f64 / st.rows_scanned.max(1) as f64,
        );
    }
    cleanup(&path);
}

/// M55：**同一份库连做两趟 `vacuum()`**，分开"瞬时峰值"与"做完还住着的字节"（`#[ignore]`）。
///
/// 为什么要这一格：三档复测读到 bitcask `vacuum() 全库` 这一行 **常驻 +15.3 / 峰值 +15.6 MB**，
/// 而上一轮同一格记的是 **+8.0**，且**两遍各读各的都一字不差** ⇒ 不是量具。
/// 分段标尺（`bitcask_vacuum_stage_split`）说这笔钱全在 ④ `merge` 段（12.6 MB，正好是 M43 发布的那个数），
/// 但那只回答"哪一段"，不回答**"做完之后还剩下多少"**。
/// `mem_probe::live()` 是**跟踪分配器**统计的活字节（不是 RSS）⇒ "常驻"涨就是真有人没释放，
/// 不能推给"分配器没把页还给操作系统"。所以第二趟是关键对照：**第二趟什么都裁不到**（垃圾已在第一趟裁完），
/// 若第二趟的 `live` 差 ≈ 0 ⇒ 那 15 MB 是**一次性**的（重建出来的东西留在库里，不随每轮累积）；
/// 若第二趟同样留下十几 MB ⇒ 每裁一轮就多住十几 MB，那是**累积泄漏**，性质完全不同。
///
/// 第三个旋钮 `BITCASK_VAC2_REOPEN=1`（M55c 的因果 A/B）：两趟之前先**冷重开**一次，
/// 让第一趟开始时的索引是"hint 有序 collect 建出来的满节点"而不是"灌行时逐条 insert 的半满节点"
/// —— 复测台架那 +15.3 读的就是这种前置状态（它在 `vacuum()` 之前重开过库）。
///
/// ```text
/// BITCASK_VAC2_ROWS=600000 BITCASK_VAC2_SHAPE=big BITCASK_VAC2_REOPEN=1 \
///   cargo test --release --lib -- --ignored --nocapture --test-threads=1 bitcask_vacuum_retained_two_passes
/// ```
#[test]
#[ignore = "bitcask vacuum 两趟：瞬时峰值 vs 做完还住的字节，手动 release 单线程跑：BITCASK_VAC2_ROWS=600000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 bitcask_vacuum_retained_two_passes"]
fn bitcask_vacuum_retained_two_passes() {
    use std::time::Instant;
    let rows: u64 = std::env::var("BITCASK_VAC2_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(600_000);
    // 两种形状：`half` = 每把键写两遍（一半行可裁）；`big` = **照大库台架**：灌一遍 + 更新 10% 的键
    // ⇒ 只有 ~5.6% 行可裁。复测那条 +15.6 读的就是 `big` 形状，两趟对照必须在同一形状上做。
    let shape = std::env::var("BITCASK_VAC2_SHAPE").unwrap_or_else(|_| "big".to_string());
    let mb = |b: i64| b as f64 / 1024.0 / 1024.0;
    let key = |i: u64| format!("{i:08}#/worldquant/datafields/x.json");
    let path = tmp_db("vac_retained");
    {
        let mvcc = MVCC::open(&path);
        if shape == "half" {
            for round in 0..2u64 {
                let tx = mvcc.begin_transaction();
                for i in 0..rows {
                    assert!(
                        tx.set(key(i).as_bytes(), format!("round{round}").into_bytes()),
                        "第 {i} 把键写失败"
                    );
                }
                tx.commit();
            }
        } else {
            // 照 `scale_baseline`：1000 行一个事务灌全库，再把前 10% 的键更新一遍
            for base in (0..rows).step_by(1000) {
                let end = (base + 1000).min(rows);
                let tx = mvcc.begin_transaction();
                for i in base..end {
                    assert!(
                        tx.set(key(i).as_bytes(), format!("v{i}").into_bytes()),
                        "第 {i} 把键写失败"
                    );
                }
                tx.commit();
            }
            let updates = (rows / 10).max(1);
            for base in (0..updates).step_by(500) {
                let end = (base + 500).min(updates);
                let tx = mvcc.begin_transaction();
                for i in base..end {
                    assert!(
                        tx.set(key(i).as_bytes(), format!("u{i}").into_bytes()),
                        "第 {i} 把键更新失败"
                    );
                }
                tx.commit();
            }
        }
        mvcc.flush();
        mvcc.checkpoint();

        // M55c 的**因果 A/B**：`BITCASK_VAC2_REOPEN=1` ⇒ 两趟之前先冷重开一次。
        // 为什么要这一格：`big` 形状不重开时第一趟读到的是**负的**（裁掉的条目比建回来的多），
        // 而复测台架同一格读 **+15.3** —— 两边唯一的形状差别就是台架在 `vacuum()` 之前**重开过**：
        // 重开的索引是 hint **有序 collect** 建出来的（满节点，M52 实测 600k 键 57.7 MB），
        // 而 merge 的重扫是升序逐条 `insert` 建出来的（半满节点，同一把尺子 83.4 MB）。
        // ⇒ 若这一格把符号翻回 +15，那条"常驻"就**不是泄漏**，而是**重建索引时又付一次半满节点的税**；
        // 顺带这里打印的"重开一趟"那一行就是这笔税的反向读数（胖 → 瘦）。
        let reopen = std::env::var("BITCASK_VAC2_REOPEN").is_ok();
        let before_open = crate::mem_probe::live() as i64;
        let mvcc = if reopen {
            drop(mvcc);
            MVCC::open(&path)
        } else {
            mvcc
        };
        println!(
            "  [{shape}]{} 冷重开一趟：live {:+.1} MB（索引从{}建回来）",
            if reopen { "/reopen=1" } else { "" },
            mb(crate::mem_probe::live() as i64 - before_open),
            if reopen {
                " hint 有序 collect"
            } else {
                "（没重开）"
            }
        );

        let base_live = crate::mem_probe::live() as i64;
        let arm = format!("{shape}{}", if reopen { "/reopen" } else { "" });
        for pass in 1..=2u8 {
            let l0 = crate::mem_probe::live() as i64;
            let p0 = crate::mem_probe::reset_peak();
            let t = Instant::now();
            let st = mvcc.vacuum().unwrap();
            let d = t.elapsed();
            let l1 = crate::mem_probe::live() as i64;
            let peak = crate::mem_probe::peak() as i64;
            println!(
                "  [{arm}] 第{pass} 趟：裁掉 {} 行 / 扫 {} 行 ｜ 墙钟 {:.2} s ｜ 瞬时峰值 +{:.1} MB ｜ \
                     做完还住 +{:.1} MB（累计 +{:.1} MB）",
                st.versions_removed,
                st.rows_scanned,
                d.as_secs_f64(),
                mb((peak - p0 as i64).max(0)),
                mb(l1 - l0),
                mb(l1 - base_live),
            );
            // ⚠️ 门控开着时这一格的"瞬时峰值"不可比：分段尺子每次都把高水位压回当前活字节
            // ⇒ 它读到的是"最后一段的尖峰"而不是整段的高水位（M37 那条教训的反向应用）。
            if merge_stage_on() && st.versions_removed > 0 {
                print_merge_stages(&format!("{arm} 门V/vacuum 内第{pass}趟"));
            }
        }

        // M62 的**门 M**：复测里紧接着 `vacuum()` 的那一格 `flush+merge` —— 无条件再走一遍 merge。
        // 为什么要在同一个进程里连着跑两扇门：两格的读数差 3 倍（+5.8 vs +18.4），而"差在哪"只有
        // 在同一份库、同一个二进制、按现场顺序排下去才答得出来（M55 第六条：对照臂必须真经过被测那一步）。
        {
            let l0 = crate::mem_probe::live() as i64;
            let p0 = crate::mem_probe::reset_peak();
            let t = Instant::now();
            mvcc.flush();
            mvcc.merge().unwrap();
            let d = t.elapsed();
            let l1 = crate::mem_probe::live() as i64;
            let peak = crate::mem_probe::peak() as i64;
            println!(
                "  [{arm}] 门M/flush+merge：墙钟 {:.2} s ｜ 瞬时峰值 {}{:.1} MB ｜ 做完还住 {:+.1} MB",
                d.as_secs_f64(),
                if merge_stage_on() {
                    "（门控开着⇒此格不可比）"
                } else {
                    ""
                },
                mb((peak - p0 as i64).max(0)),
                mb(l1 - l0),
            );
            if merge_stage_on() {
                print_merge_stages(&format!("{arm} 门M/flush+merge"));
            }
        }
    }
    cleanup(&path);
}

/// vacuum 自己不该是全库最重的路径：读盘次数必须只与**留下来的行数**相关。
///
/// 旧实现里 vacuum 有两次 `kv.iter()`（每一行连 value 全读出来）。也就是说在
/// 「链已经涨到几十万行」的库上跑 vacuum，那一刻本身就是一次事故 ——
/// 这正是「先让 vacuum 变轻，再谈自动触发」这个施工顺序的理由。
#[test]
fn test_vacuum_does_not_read_every_version() {
    const CHAIN: u64 = 400;
    const KEYS: u64 = 5;

    let path = tmp_db("vacuum_cost");
    {
        let mvcc = MVCC::open(&path);
        for i in 0..CHAIN {
            for k in 0..KEYS {
                let key = format!("k{k}");
                let tx = mvcc.begin_transaction();
                assert!(tx.set(key.as_bytes(), format!("v{i}").into_bytes()));
                tx.commit();
            }
        }
        assert_eq!(mvcc.health().total_rows, CHAIN * KEYS);

        let before = crate::bitcask::bitcask::read_value_calls();
        let stats = mvcc.vacuum().unwrap();
        let used = crate::bitcask::bitcask::read_value_calls() - before;
        eprintln!(
            "vacuum 读盘次数：{used} 次（裁掉 {} 行，存活 key {KEYS} 个）",
            stats.versions_removed
        );
        assert_eq!(
            stats.versions_removed,
            ((CHAIN as usize - 1) * (KEYS as usize)) as usize
        );
        // 界取 2×存活行数：每个留下来的行最多被读两次 —— 一次查 blob 引用，
        // 一次是 merge 重写日志时把它搬走（那是 merge 的本职，不是 vacuum 的浪费）。
        // 关键是它只与**存活行数**相关；未修前这里是 2000 次（每行连 value 全读）。
        assert!(
            used <= 2 * KEYS + 4,
            "vacuum 只该读留下来的那些行：实际读盘 {used} 次，而历史行数是 {}",
            CHAIN * KEYS
        );
        assert_eq!(mvcc.health().max_chain, 1, "裁完最长链应降到 1");
    }
    cleanup(&path);
}

/// `vacuum()` 必须**按块**删除，不许把整库的待删行攒在一个 `Vec` 里（M37）。
///
/// 判据（单位：行）：库里可删的旧版**远多于一块**时，单次 flush 前最多攒的待处理行数
/// `vac_pending_max()` 必须 ≤ `2 × VAC_CHUNK_ROWS`。
/// 为什么不钉内存而钉这个数：分配计数（`mem_probe`）是**进程级**的，并行跑用例时别家分配的
/// 字节会算进来（M27 栽过、M36 又拿坏量具的读数当过凭据）；而"块界有没有生效"是个纯计数。
/// `versions_removed > VAC_CHUNK_ROWS` 那条断言保证界**真的被跨过** —— 一个小库任何一片都
/// 装得下，那样这条判据就是空转（M33 的守卫第一次就红在这种空转上）。
///
/// 变异验证：把 `to_delete.drain(..)` 从块循环里挪到"整库走完之后" ⇒
/// `vac_pending_max()` 变成全部可删行数（20 000 > 界 16 384），红。
#[test]
fn test_vacuum_deletes_in_bounded_chunks() {
    const KEYS: usize = 20_000;
    let path = tmp_db("vac_chunks");
    {
        let mvcc = MVCC::open(&path);
        // 自动回收会在提交那一刻就先裁掉一片，那这条判据就量不到手动 vacuum 的界了
        mvcc.set_vacuum_auto_slice(0);
        for _ in 0..2u64 {
            let tx = mvcc.begin_transaction();
            for i in 0..KEYS {
                let k = format!("k{i:06}");
                assert!(tx.set(k.as_bytes(), vec![b'v'; 24]), "第 {i} 把键写失败");
            }
            tx.commit();
        }
        reset_vac_pending();
        let st = mvcc.vacuum().unwrap();
        let pending = vac_pending_max();
        eprintln!(
            "[vac chunks] 裁掉 {} 行 / 扫 {} 行 ⇒ 单次 flush 前最多攒 {} 行（块界 {VAC_CHUNK_ROWS}）",
            st.versions_removed, st.rows_scanned, pending
        );
        assert!(st.rows_scanned > 0, "一行都没扫到 ⇒ 空转");
        assert!(
            st.versions_removed > VAC_CHUNK_ROWS,
            "可删行数 {} 没超过一块 ⇒ 这条判据量不到界（空转）",
            st.versions_removed
        );
        assert!(pending > 0, "这把尺子没数到待删行 ⇒ 空转");
        assert!(
            pending <= 2 * VAC_CHUNK_ROWS as u64,
            "单次 flush 前攒了 {pending} 行待处理（界 {}）⇒ 又变回整库一个 `Vec`",
            2 * VAC_CHUNK_ROWS as u64
        );
    }
    cleanup(&path);
}

#[test]
fn test_vacuum_removes_old_versions() {
    let path = tmp_db("vac");
    {
        let mvcc = MVCC::open(&path);
        let t = mvcc.begin_transaction();
        assert!(t.set(b"a", b"1".to_vec()));
        assert!(t.set(b"b", b"old".to_vec()));
        t.commit();
        let t = mvcc.begin_transaction();
        assert!(t.set(b"a", b"2".to_vec()));
        assert!(t.delete(b"b"));
        t.commit();

        let st = mvcc.vacuum().unwrap();
        assert!(
            st.versions_removed >= 2,
            "应删除 a 旧版与 b 的历史, removed={}",
            st.versions_removed
        );

        let tx = mvcc.begin_transaction();
        assert_eq!(tx.get(b"a"), Some(b"2".to_vec()));
        assert_eq!(tx.get(b"b"), None);
        tx.commit();
    }
    cleanup(&path);
}
