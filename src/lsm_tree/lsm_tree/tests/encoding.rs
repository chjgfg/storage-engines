// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// M41b 的**校准**：memtable 里一条条目在堆上到底占多少字节。
///
/// 为什么要有这条：`maybe_flush_mem` 读的 `mem_bytes` 只数载荷账（`key.len() +
/// StoredEntry::estimate_bytes()`），一条 16 B 编码键的墓碑因此只算 **25 B**。MVCC 档写着
/// `mem_threshold_bytes: 32 MB`，可 60 万条墓碑的载荷账才 15 MB ⇒ 那一条永不触发，
/// 兜底的是条数阈值 256k，而 256k 条在堆上是 **~155 MB**（分段台架 ① 段的实测斜率）。
/// 要把"memtable 多大"这句话改成真内存，常数得是量出来的。
///
/// ⚠️ 只能单线程跑（`live()` 是进程级的，并行用例会互相污染 —— M27/M36 各栽过一次），
/// 所以它是 `#[ignore]` 台架而不是守卫。
/// M46 的内存凭据：裸引擎"全库导出"那一趟的瞬时峰值（`#[ignore]`，手动 release 单线程跑）。
///
/// 为什么要有它：M44 之后四家 CLI 只剩 `lsm_tree/bin/export_kv.rs` 还在物化整库，而它走的
/// 是**裸引擎**入口（`merge_all_at`）—— 门面那台的 `export` 读数照不到这条路。
/// 改前那一列不是这里切的臂（测量臂跑完就摘了，不属于产品）：它是同一段代码把
/// `merge_inputs_at` 里 SST 那一支换回 `iter_all().collect()` + `vec_inputs` 之后读到的，
/// 守卫 `test_merge_streams_sources_instead_of_materializing_them` 的变异用的就是这个换法
/// —— 计数器（20 000 条 vs 5 条）与这里的 MB（100.4 vs 0.4）是同一件事的两把尺子。
///
/// ```text
/// LSM_RAW_EXPORT_ROWS=600000 cargo test --release --lib -- --ignored --nocapture     ///   --test-threads=1 lsm_raw_export_peak
/// LSM_MERGE_OLD_SHAPE=1 LSM_RAW_EXPORT_ROWS=600000 cargo test --release --lib --     ///   --ignored --nocapture --test-threads=1 lsm_raw_export_peak
/// ```
#[test]
#[ignore = "裸引擎全库导出的内存台架，手动 release 单线程跑：LSM_RAW_EXPORT_ROWS=600000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 lsm_raw_export_peak"]
fn lsm_raw_export_peak() {
    use crate::lsm_tree::sst::value_materializations;
    let rows: usize = std::env::var("LSM_RAW_EXPORT_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200_000);
    let mb = |b: u64| b as f64 / 1024.0 / 1024.0;
    let dir = tmp_dir("raw_export_peak");
    {
        let mut db = LSMTree::open_with_options(&dir, usize::MAX / 8, 64).unwrap();
        for i in 0..rows {
            db.put(format!("{i:08}").into_bytes(), vec![b'v'; 24]);
        }
        db.flush().unwrap();
        let files = db.stats().total_sst;
        let base = crate::mem_probe::reset_peak();
        let m0 = value_materializations();
        let t = std::time::Instant::now();
        let n = db.export_records_with_seq_for_each(false, &mut |_k, _v, _seq| true);
        let d = t.elapsed();
        let peak = crate::mem_probe::peak().saturating_sub(base);
        let mats = value_materializations() - m0;
        eprintln!(
            "=== 裸引擎全库导出 rows={rows} 源={files} 交出 {n} 行 ⇒ 瞬时峰值 {:.1} MB、物化 {mats} 条、耗时 {:.2} s ===",
            mb(peak),
            d.as_secs_f64()
        );
    }
    let _ = fs::remove_dir_all(&dir);
}

#[test]
#[ignore = "memtable 每条真实字节数的校准，手动 release 单线程跑：LSM_MEM_CAL=200000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 lsm_memtable_bytes_per_entry"]
fn lsm_memtable_bytes_per_entry() {
    let n: usize = std::env::var("LSM_MEM_CAL")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200_000);
    let dir = tmp_dir("mem_cal");
    {
        let opts = LSMTreeOptions {
            // 两档阈值都钉到不会触发 ⇒ 全程只有一只 memtable 在长
            mem_threshold_entries: usize::MAX / 8,
            mem_threshold_bytes: usize::MAX / 8,
            enable_mem_wal: false,
            ..LSMTreeOptions::default()
        };
        let mut db = LSMTree::open_with_config(&dir, opts).unwrap();
        let base = crate::mem_probe::live();
        for i in 0..n {
            // 键长 16 B：与 MVCC 的编码键 `raw(8) ‖ version(8)` 同量级；值取空 ⇒
            // 载荷账 = 16 + (8 + 1) = 25 B，与一条墓碑同量级
            db.put(format!("k{i:015}").into_bytes(), Vec::new());
        }
        let held = crate::mem_probe::live() - base;
        assert_eq!(
            db.stats().mem_entries,
            n,
            "中途 flush 过 ⇒ 量的不是同一只 memtable"
        );
        let per = held / n as u64;
        // 载荷账按**生产口径**读（`stats().mem_bytes`），不是自己另算一遍：
        // 一条 = `key.len()` + `StoredEntry::estimate_bytes()`
        let pay = db.stats().mem_bytes as u64 / n as u64;
        println!(
            "=== memtable 校准 n={n}：真堆占用每条 {per} B，判定用的载荷账每条 {pay} B ⇒ 结构 {struct_} B ===",
            struct_ = per - pay
        );
    }
    let _ = fs::remove_dir_all(&dir);
}

/// M41b：`mem_threshold_bytes` 说的必须是 memtable 的**真内存**，不是载荷账。
///
/// 校准过的边界（每条载荷账 25 B + 结构 `MEM_ENTRY_OVERHEAD` = 476 B）：阈值钉 256 KB 时
/// - **550 条** = 261 800 B < 262 144 ⇒ 还**不该** flush；
/// - **551 条** = 262 276 B ≥ 262 144 ⇒ 必须 flush。
/// 两臂同构建、只差一行，所以既钉住"开销项进了判定"（少算 ⇒ 551 那一臂不 flush），
/// 也钉住"没把它 inflate"（多算 ⇒ 550 那一臂提前 flush）。条数阈值钉到不可能触发，
/// 让字节这一档单独说话。
///
/// 另外钉一条口径：`stats().mem_bytes` **仍然是载荷账**（对外含义没变，改的只有判定）。
///
/// 变异验证：把判定里那句 `+ mem_entries × MEM_ENTRY_OVERHEAD` 去掉 ⇒ 551 条那臂红
/// （载荷账只有 13 775 B，离 256 KB 远着呢，于是 memtable 一路涨到条数阈值那么高才 flush ——
/// 就是 600k 档 ① 段那 155 MB 的来处）。
#[test]
fn test_memtable_flush_trigger_reads_real_bytes() {
    const THRESH: usize = 256 * 1024;
    // 每条 = 载荷账 25 B（16 B 键 + `StoredEntry::estimate_bytes()` 9）+ 结构开销
    let per_entry = 25 + MEM_ENTRY_OVERHEAD;
    let n_no = THRESH / per_entry; // 差一条不到阈值
    let n_yes = n_no + 1;
    assert!(
        n_no * per_entry < THRESH && n_yes * per_entry >= THRESH,
        "边界算错了：{n_no} 条 = {} B，{n_yes} 条 = {} B，阈值 {THRESH}",
        n_no * per_entry,
        n_yes * per_entry
    );

    for (n, should_flush) in [(n_no, false), (n_yes, true)] {
        let dir = tmp_dir(&format!("mem_real_{n}"));
        {
            let opts = LSMTreeOptions {
                mem_threshold_bytes: THRESH,
                // 只让"字节"这一档说话：条数阈值高到碰不到
                mem_threshold_entries: usize::MAX / 8,
                enable_mem_wal: false,
                ..LSMTreeOptions::default()
            };
            let mut db = LSMTree::open_with_config(&dir, opts).unwrap();
            for i in 0..n {
                db.put(format!("k{i:015}").into_bytes(), Vec::new());
            }
            let st = db.stats();
            if should_flush {
                assert_eq!(
                    st.total_sst,
                    1,
                    "{n} 条 × {per_entry} B = {} B 已经过阈值 {THRESH} ⇒ 该 flush 了；\
                         没 flush 说明判定还在只读载荷",
                    n * per_entry
                );
                assert_eq!(st.mem_entries, 0, "flush 之后 memtable 应当是空的");
            } else {
                assert_eq!(
                    st.total_sst,
                    0,
                    "{n} 条 = {} B 还没到阈值 {THRESH} 就 flush 了 ⇒ 开销被 inflate 了",
                    n * per_entry
                );
                assert_eq!(st.mem_entries, n, "没 flush 却清了 memtable");
            }
            // 载荷账的口径没改（flush 之后清零）：一条 = 16 B 键 + `StoredEntry` 的 9 B。
            // ⚠️ 放在 flush 判定**之后**：先判该 flush 的有没有 flush，失败信息才说得出根因。
            assert_eq!(
                st.mem_bytes,
                if should_flush { 0 } else { n * 25 },
                "`stats().mem_bytes` 的口径被改了（对外应当仍是载荷账，flush 后清零）"
            );
            // 数据还在（不论在 memtable 还是 SST 里）
            assert_eq!(db.get(b"k000000000000000"), Some(Vec::new()));
            assert_eq!(
                db.get(format!("k{:015}", n - 1).as_bytes()),
                Some(Vec::new())
            );
        }
        let _ = fs::remove_dir_all(&dir);
    }
}
