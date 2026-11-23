// 该文件集中验证对应职责，保留原测试名称和字面值。

#[test]
#[ignore = "same closed SST seed ABBA for full vacuum compaction scheduling"]
fn vacuum_compaction_same_seed_abba() {
    let rows = std::env::var("LSM_VACUUM_BATCH_ROWS")
        .map(|s| s.parse().unwrap())
        .unwrap_or(1_000_000);
    compare_vacuum_compaction_schedule(rows, false);
}

#[test]
#[ignore = "LSM vacuum stages; default options, release single thread"]
fn vacuum_delete_stage_cost() {
    let rows = std::env::var("LSM_VACUUM_DIAG_ROWS")
        .map(|v| v.parse().expect("positive diagnostic rows"))
        .unwrap_or(200_000);
    assert!(rows >= 100_000, "default threshold needs enough deletes");
    measure_vacuum_delete_stages(rows, false);
}

/// M41 的**先量**：lsm 全库 `vacuum()` 的分段瞬时峰值。
///
/// 为什么还要量：M37b 之后 600k 档 `vacuum()` 还剩 **+45.5 MB**，而它已经不在累加器里
/// （`to_delete` 与存活行清单都没了）—— 剩下的三段（①走查+删除 / ②`kv.flush()` /
/// ③xmin 过滤 + blob GC）里哪一段是元凶，M30/M31 两轮的教训是"最合理的那件事不是元凶"，
/// 所以先分段再动手。⚠️ 标尺带门控：不设 `VACUUM_STAGE_SPLIT` 时它是纯读数，不压高水位。
///
/// `LSM_MEM_ENTRIES` 那一档是用来做**同构建 A/B**的：①段里如果装的是 memtable 攒的墓碑，
/// 把 memtable 的条数阈值调小，①就得跟着小（阈值不变而①不动 ⇒ 元凶不是 memtable）。
#[test]
#[ignore = "lsm vacuum 分段内存台架，手动 release 单线程跑：VACUUM_STAGE_SPLIT=1 LSM_VAC_ROWS=600000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 lsm_vacuum_stage_split"]
fn lsm_vacuum_stage_split() {
    use std::time::Instant;
    let rows: u64 = std::env::var("LSM_VAC_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200_000);
    let mem_entries: usize = std::env::var("LSM_MEM_ENTRIES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let mb = |b: u64| b as f64 / 1024.0 / 1024.0;
    let path = tmp_db("vac_stages");
    {
        let mem = if mem_entries == 0 {
            LSMTreeOptions::for_mvcc().mem_threshold_entries
        } else {
            mem_entries
        };
        let opts = if mem_entries == 0 {
            None
        } else {
            Some(LSMTreeOptions {
                mem_threshold_entries: mem_entries,
                ..LSMTreeOptions::for_mvcc()
            })
        };
        let mvcc = MVCC::try_open_with(&path, opts).unwrap();
        // 自动回收会在提交那一刻先裁掉一片，那这段台架量的就不是手动 vacuum 了
        mvcc.set_vacuum_auto_slice(0);
        for round in 0..2u64 {
            let tx = mvcc.begin_transaction();
            for i in 0..rows {
                let k = format!("k{i:06}");
                assert!(
                    tx.set(k.as_bytes(), format!("round{round}").into_bytes()),
                    "第 {i} 把键写失败"
                );
            }
            tx.commit();
        }
        let t = Instant::now();
        // M42 的先量：builder 单文件最多握住多少条（reset 之后整段 vacuum 里的 flush 都算进来）
        crate::lsm_tree::sst::reset_builder_held_max();
        let st = mvcc.vacuum().unwrap();
        let d = t.elapsed();
        let p = crate::mem_probe::stage_peaks();
        let held = crate::lsm_tree::sst::builder_held_max();
        println!("  （builder 单文件最多同时握住 {held} 条未落盘条目 —— M42 的先量）");
        println!(
            "=== lsm vacuum 分段瞬时峰值 rows={rows} mem阈值={mem} 裁掉 {} 行 / 扫 {} 行，总耗时 {:.2} s ===",
            st.versions_removed,
            st.rows_scanned,
            d.as_secs_f64()
        );
        println!(
            "  ① 走查+删除 {:>8.1} MB ｜ ② flush(memtable→SST，顺带压实) {:>8.1} MB ｜ ③ xmin 过滤+blob GC {:>8.1} MB",
            mb(p[0]),
            mb(p[1]),
            mb(p[2])
        );
        println!(
            "  合计（各段自己的峰值，非同时成立）{:>8.1} MB ｜ 每扫描行摊 ①={:.0} B ②={:.0} B ③={:.0} B",
            mb(p[0] + p[1] + p[2]),
            p[0] as f64 / st.rows_scanned.max(1) as f64,
            p[1] as f64 / st.rows_scanned.max(1) as f64,
            p[2] as f64 / st.rows_scanned.max(1) as f64,
        );
    }
    cleanup(&path);
}
