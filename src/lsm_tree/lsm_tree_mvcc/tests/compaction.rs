// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn search_sst_no_total_stops_value_clones_with_eager_negative_control() {
    let path = tmp_db("search_deferred_sst");
    let db = MVCC::open(&path);
    db.set_vacuum_auto_slice(0);
    let tx = db.begin_transaction();
    for i in 0..10_000u64 {
        assert!(tx.set_raw(&i.to_be_bytes(), vec![9; 128]));
    }
    tx.commit();
    db.flush();
    let tx = db.begin_transaction();
    let mut query = crate::common::SearchQuery::contains(b"", 0, 100);
    query.need_total = false;
    for eager in [true, false] {
        let mode = eager.then(crate::common::store::search_materialize_reference::Scope::start);
        let before = crate::lsm_tree::sst::entry_value_clones();
        let page = tx.search_keys(&query);
        let copies = crate::lsm_tree::sst::entry_value_clones() - before;
        assert_eq!(page.items.len(), 100);
        assert!(page.has_next);
        assert_eq!(page.items[99].value, Some(vec![9; 128]));
        assert_eq!(copies, if eager { 4096 } else { 100 });
        let guard = std::panic::catch_unwind(|| assert_eq!(copies, 100));
        assert_eq!(guard.is_err(), eager);
        drop(mode);
    }
    tx.commit();
}

#[test]
#[ignore = "same SST seed ABBA for deferred search materialization"]
fn search_deferred_sst_same_seed_abba() {
    let path = tmp_db("search_deferred_abba");
    let db = MVCC::open(&path);
    db.set_vacuum_auto_slice(0);
    let tx = db.begin_transaction();
    for i in 0..20_000u64 {
        assert!(tx.set_raw(&i.to_be_bytes(), vec![9; 512]));
    }
    tx.commit();
    db.flush();
    let tx = db.begin_transaction();
    for (arm, eager) in [true, false, false, true].into_iter().enumerate() {
        let mode = eager.then(crate::common::store::search_materialize_reference::Scope::start);
        let before = crate::lsm_tree::sst::entry_value_clones();
        let mut q = crate::common::SearchQuery::contains(b"", 0, 100);
        q.need_total = false;
        let now = std::time::Instant::now();
        let (page, allocations) = crate::mem_probe::count_thread_allocations(|| tx.search_keys(&q));
        let ms = now.elapsed().as_secs_f64() * 1000.0;
        let copies = crate::lsm_tree::sst::entry_value_clones() - before;
        assert_eq!(copies, if eager { 4096 } else { 100 });
        assert_eq!(page.items.len(), 100);
        assert!(page.has_next);
        assert_eq!(page.items[0].key, 0u64.to_be_bytes());
        assert_eq!(page.items[99].value, Some(vec![9; 512]));
        eprintln!(
            "SEARCH_DEFERRED_ABBA arm={arm} eager={eager} ms={ms:.3} copies={copies} allocations={allocations}"
        );
        drop(mode);
    }
    tx.commit();
}

/// 冷重开**不许自己做写路径的活**（M28）。
///
/// 形状：bulk 收尾留下「L0 文件数正好停在 `l0_compact_threshold` 上」而 WAL 已被
/// `finish()` 收干净（redo 0 条）—— 这正是 600k 基线里那条悬崖的形状。旧的
/// `try_open_with` 无条件 `tree.flush()`，而 `LSMTree::flush()` 末尾无条件
/// `maybe_compact()` ⇒ "打开一个库"自己把整层 L0 搬进 L1/L2：600k 那档整段悬崖都在
/// `flush` 里（`reopen_shape_probe` 的同轮 A/B：无条件 **13.95 s** vs 有条件 **8.4 ms**），
/// 而 200k 只有 4 个 L0 文件（<8）不越线 ⇒ 两臂同价。**开关是 L0 文件数越没越线，
/// 跟"恢复"本身没关系。**
///
/// 尺子选**层文件数**而不是墙钟：判据是"open 没动写路径"，墙钟在 debug 里噪声太大。
#[test]
fn test_open_does_not_compact_when_nothing_to_redo() {
    let path = tmp_db("open_no_compact");
    let opts = {
        let mut o = LSMTreeOptions::for_mvcc();
        // 阈值压到 2，好让"越线"这件事在小库里也造得出来
        o.l0_compact_threshold = 2;
        o
    };
    {
        let mvcc = MVCC::try_open_with(&path, Some(opts.clone())).unwrap();
        // 两轮 bulk：每轮 `finish()` 落一个 L0 文件并收口 WAL，而 bulk 模式下不 compact
        for part in 0..2u8 {
            let mut bulk = mvcc.begin_bulk();
            let items: Vec<(Vec<u8>, Vec<u8>)> = (0..50)
                .map(|i| (format!("k{part}{i:04}").into_bytes(), b"v".to_vec()))
                .collect();
            bulk.put_batch_owned(items);
            bulk.finish();
        }
    }
    // 裸树读一遍"重开之前"的层形状当基线（`LSMTree::open_with_config` 不 compact，
    // 也不写 MANIFEST ⇒ 这一步本身不会把形状改掉）
    let before = {
        let t = LSMTree::open_with_config(&path, opts.clone()).unwrap();
        let s = t.stats();
        drop(t);
        s
    };
    assert!(
        before.level_files.first().copied().unwrap_or(0) >= 2,
        "形状没造出来：L0 只有 {:?} 个文件，够不到阈值 2 ⇒ 下面的断言会空转",
        before.level_files
    );

    {
        let mvcc = MVCC::try_open_with(&path, Some(opts)).unwrap();
        let after = mvcc.kv.lock().unwrap().stats();
        assert_eq!(
            after.level_files, before.level_files,
            "没有东西要重放时，open 不该动层 —— 压实是写路径的活，不该由\"打开库\"触发"
        );
        // 不 compact ≠ 丢数据：两轮 bulk 的键都必须读得回来
        let tx = mvcc.begin_transaction();
        for part in 0..2u8 {
            for i in 0..50 {
                let k = format!("k{part}{i:04}").into_bytes();
                assert_eq!(
                    tx.get(&k),
                    Some(b"v".to_vec()),
                    "第 {part} 轮第 {i} 把键读不回来"
                );
            }
        }
        tx.commit();
    }
    cleanup(&path);
}

#[test]
fn test_bulk_finish_with_compact() {
    let path = tmp_db("bulk_compact");
    {
        let mvcc = MVCC::open(&path);
        let mut bulk = mvcc.begin_bulk();
        // 小 batch 多次 → 多个 L0
        for batch in 0..8u32 {
            let mut items = Vec::new();
            for i in 0..50u32 {
                let n = batch * 50 + i;
                items.push((
                    format!("k{n:06}").into_bytes(),
                    format!("v{n}").into_bytes(),
                ));
            }
            bulk.put_batch_owned(items);
        }
        bulk.finish_with_compact(32);
    }
    {
        let mvcc = MVCC::open(&path);
        let tx = mvcc.begin_transaction();
        assert_eq!(tx.get(b"k000000"), Some(b"v0".to_vec()));
        assert_eq!(tx.get(b"k000399"), Some(b"v399".to_vec()));
        // 抽样中间 key
        assert_eq!(tx.get(b"k000150"), Some(b"v150".to_vec()));
        tx.commit();
    }
    cleanup(&path);
}

/// 压实的耗时与瞬时内存到底各在哪一段（M30 先量时间，M33 在同一台架上并上内存列）。
///
/// 为什么要拆：`reopen_shape_probe` 只给出"13.9 s 全在 `flush` 里"，而 `flush` 里混着
/// **三件事** —— ①把每个输入文件整篇读进 `Vec`（`iter_entries` = `iter_all().collect()`）、
/// ②k 路归并（每步克隆键与值）、③边归并边写新 SST（`SstBuilder` 侧本来就是流式的）。
/// "改成流式源"只治 ①，如果时间其实在 ② 或 ③，那这一刀就买错了东西。
///
/// 现在四段（M33 之后）：**① 物化输入** → **② 旧形状**（源=①那份 Vec）归并+写盘 →
/// **③ 新形状**（源=`iter_all()` 每源一路）归并+写盘 → **④ 纯归并不写盘**（sink 只计数）。
/// ⇒ 写 ≈ ② − ④；**整段旧形状 = ① + ②**，整段新形状 = ③ —— 报"省了多少内存"必须按整段比，
/// ⚠️ 第一版就是只比了 ② 与 ③ 两段，得出"流式反而更占内存"的反向结论（旧形的大头记在 ① 上）。
/// 每段之前调 `mem_probe::reset_peak()`：`peak` 是进程高水位，不压回去的话靠后的段只能报 0。
///
/// # 量出来的答案（release，600k 行 = 8 个输入文件 61.7 MB → 34 个输出文件 67.6 MB）
///
/// | 段 | M30 之前 | M30 之后（=现在） | 每输入行 | 峰值涨 |
/// |---|---|---|---|---|
/// | ① 物化输入 | 618 ms | 366.9 ms | 0.61 µs | **154.0 MB**（条目本身 61.7 MB） |
/// | ② 旧形状 归并+写盘 | **18 018 ms** | 1471.8 ms | 2.45 µs | 2.9 MB（叠在 ① 上） |
/// | ③ 新形状 流式源 | — | 1888.4 ms | 3.15 µs | **6.3 MB** |
/// | ④ 纯归并不写盘 | 476 ms | 307.9 ms | 0.51 µs | — |
///
/// ⇒ **时间**的病在 ②/③，占当时 ①+③ 的 94%，而那段里唯一"每行都做一次且随表长"的事是
/// `merge_to_ssts` 的 `if b.estimated_bytes() >= target_bytes` —— 旧实现每次调用都把 builder
/// 的整表从头求和一遍（一张 2 MiB 的表 ≈19 000 行 ⇒ 每行摊 ~30 µs，正是实测那个数）。
/// 修法与 M19 同一个动作：改增量账。连带效果：带 redo 的重开 14.43 s → **3.55 s**。
/// ⇒ **内存**的病在 ①：整段旧形状 ①+② = **156.8 MB** → 新形状 ③ = **6.3 MB（省 25.0×）**，
/// 时间 1838.7 → 1888.4 ms（+2.7%，本机噪声带内 ⇒ 持平）。M33 收的就是这笔。
/// 每行摊 269 B 而条目只有 108 B ⇒ 多出来的是每行两个堆分配 + `Vec` 扩容翻倍。
///
/// ```text
/// LSM_COMPACT_ROWS=600000 cargo test --release --lib -- --ignored --nocapture \
///     --test-threads=1 compaction_cost_split
/// ```
#[test]
#[ignore = "压实耗时拆分，手动 release 单线程跑：LSM_COMPACT_ROWS=600000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 compaction_cost_split"]
fn compaction_cost_split() {
    use crate::lsm_tree::sst::{
        MergeInput, SstBuilder, StoredEntry, merge_streaming, merge_to_ssts, vec_inputs,
    };
    use std::time::Instant;

    let rows: usize = std::env::var("LSM_COMPACT_ROWS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200_000);
    let files: usize = std::env::var("LSM_COMPACT_FILES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);
    fn key(i: usize) -> Vec<u8> {
        format!("{i:08}#/worldquant/datafields/x.json").into_bytes()
    }
    fn val(i: usize) -> Vec<u8> {
        format!("{{\"i\":{i},\"h\":\"{:0<28}\",\"v\":1}}", i * 7).into_bytes()
    }

    // 压实与 L0 背压都抬到不会触发：这条要在**压实之前**把输入抓在手里反复量
    let mut opts = LSMTreeOptions::for_mvcc();
    opts.l0_compact_threshold = usize::MAX / 4;
    opts.max_compactions_per_flush = 0;
    opts.l0_slowdown_trigger = usize::MAX / 8;
    opts.l0_stop_trigger = usize::MAX / 8;

    let dir = tmp_db("compact_split");
    let out_dir = tmp_db("compact_split_out");
    {
        let mvcc = MVCC::try_open_with(&dir, Some(opts.clone())).unwrap();
        let per = (rows + files - 1) / files;
        for f in 0..files {
            let lo = f * per;
            let hi = (lo + per).min(rows);
            if lo >= hi {
                break;
            }
            let mut bulk = mvcc.begin_bulk();
            let items: Vec<(Vec<u8>, Vec<u8>)> = (lo..hi).map(|i| (key(i), val(i))).collect();
            bulk.put_batch_owned(items);
            bulk.finish();
        }
        let st = mvcc.kv.lock().unwrap().stats();
        eprintln!(
            "=== 压实拆分 rows={rows} 文件={files} | L0 实际 {} 个 | 盘上 {} MB ===",
            st.l0_files,
            st.level_bytes.iter().sum::<u64>() / 1_048_576
        );
    }

    let tree = LSMTree::open_with_config(&dir, opts.clone()).unwrap();
    let n_files = tree.l0_for_test().len();

    // ── ① 物化输入（旧形状的第一步）：时间 + **这一段自己涨了多少内存** ──
    // ⚠️ `reset_peak()` 是关键：`peak` 是进程高水位，不压回去的话这一段涨的会被前面的
    // 灌入高峰盖住、报成 0（M27 的盲区）。基准取"当前活着的字节"，之后减它。
    let t = Instant::now();
    let m0 = crate::mem_probe::reset_peak();
    let live0 = crate::mem_probe::live();
    let sources: Vec<Vec<_>> = tree
        .l0_for_test()
        .iter()
        .rev()
        .map(|s| s.iter_entries())
        .collect();
    let t_materialize = t.elapsed();
    let mat_peak = crate::mem_probe::peak().saturating_sub(m0);
    let mat_live = (crate::mem_probe::live() as i64 - live0 as i64).max(0) as u64;
    let total_rows: usize = sources.iter().map(|s| s.len()).sum();
    let bytes: usize = sources
        .iter()
        .flat_map(|s| s.iter())
        .map(|(k, se)| k.len() + se.estimate_bytes())
        .sum();

    let block_entries = opts.sst_block_entries;
    let bloom = opts.enable_bloom;
    let bits = opts.bloom_bits_per_key;
    let target = opts.target_sst_bytes;
    let mut next_id = 0u64;
    let out_path = out_dir.to_path_buf();
    let mut make = move || {
        next_id += 1;
        let p = out_path.join("sst").join(format!("{next_id:08}.sst"));
        SstBuilder::with_options(next_id, p, block_entries, bloom).with_bloom_bits(bits)
    };
    // ── ② 旧形状：源 = ① 那份物化好的 Vec ──
    let t = Instant::now();
    let m1 = crate::mem_probe::reset_peak();
    crate::lsm_tree::sst::reset_builder_held_max();
    let written = merge_to_ssts(
        vec_inputs(sources),
        |_: &[u8], _: &StoredEntry| true,
        &mut make,
        target,
    )
    .unwrap();
    let t_merge_write = t.elapsed();
    let old_peak = crate::mem_probe::peak().saturating_sub(m1);
    let held_old = crate::lsm_tree::sst::builder_held_max();
    let out_bytes: u64 = written.iter().map(|s| s.size_bytes).sum();
    let n_out = written.len();
    drop(written);

    // ── ③ 新形状（生产码现在走这条）：源 = 每源一路 `iter_all()` 迭代器 ──
    // 两臂的输出必须**逐字节同口径**，否则"省了内存"可能只是"少写了东西"。
    let t = Instant::now();
    let m2 = crate::mem_probe::reset_peak();
    crate::lsm_tree::sst::reset_builder_held_max();
    let written_stream = merge_to_ssts(
        tree.l0_for_test()
            .iter()
            .rev()
            .map(|s| Box::new(s.iter_all()) as MergeInput<'_, StoredEntry>)
            .collect(),
        |_: &[u8], _: &StoredEntry| true,
        &mut make,
        target,
    )
    .unwrap();
    let t_stream = t.elapsed();
    let new_peak = crate::mem_probe::peak().saturating_sub(m2);
    let held_new = crate::lsm_tree::sst::builder_held_max();
    let out_bytes_stream: u64 = written_stream.iter().map(|s| s.size_bytes).sum();
    assert_eq!(
        written_stream.len(),
        n_out,
        "流式源改变了切文件的数量（{n_out} → {}）⇒ 判据被动过了",
        written_stream.len()
    );
    assert_eq!(
        out_bytes_stream, out_bytes,
        "两臂输出字节不同（{out_bytes} → {out_bytes_stream}）⇒ 归并结果被改动污染，不是省内存"
    );
    drop(written_stream);

    // ④ 纯归并（sink 只计数，不写盘）：源仍走物化那份，好与 ② 直接对比"写"占多少
    let mut emitted = 0usize;
    let sources3: Vec<Vec<_>> = tree
        .l0_for_test()
        .iter()
        .rev()
        .map(|s| s.iter_entries())
        .collect();
    let t = Instant::now();
    merge_streaming(
        vec_inputs(sources3),
        &mut |_: &[u8], _: &StoredEntry| true,
        |_: Vec<u8>, _| {
            emitted += 1;
        },
    );
    let t_merge_only = t.elapsed();

    let mb = |b: u64| b as f64 / 1_048_576.0;
    eprintln!(
        "  输入 {total_rows} 行 / {:.1} MB | 输出 {n_out} 个文件 {:.1} MB | 纯归并交出 {emitted} 把键",
        mb(bytes as u64),
        mb(out_bytes),
    );
    eprintln!(
        "  ① 物化输入 {:>10.1} ms ⇒ 每行 {:.3} µs | 这一段峰值涨 {:.1} MB（条目本身 {:.1} MB、\
             结束时仍活 {:.1} MB）⇒ 每行摊 {:.0} B",
        t_materialize.as_secs_f64() * 1e3,
        t_materialize.as_secs_f64() * 1e6 / total_rows as f64,
        mb(mat_peak),
        mb(bytes as u64),
        mb(mat_live),
        mat_peak as f64 / total_rows.max(1) as f64,
    );
    eprintln!(
        "  ② 旧形状 归并+写盘 {:>10.1} ms ⇒ 每输入行 {:.3} µs | 这一段峰值涨 {:.1} MB\
             （源=①那份 Vec ⇒ 输入的 154 MB 已记在 ① 上）| builder 单文件最多握住 {held_old} 条",
        t_merge_write.as_secs_f64() * 1e3,
        t_merge_write.as_secs_f64() * 1e6 / total_rows as f64,
        mb(old_peak),
    );
    eprintln!(
        "  ③ 新形状 流式源     {:>10.1} ms ⇒ 每输入行 {:.3} µs | 峰值涨 {:.1} MB\
             | builder 单文件最多握住 {held_new} 条（输出 {n_out} 个文件、平均 {:.0} 行/文件）",
        t_stream.as_secs_f64() * 1e3,
        t_stream.as_secs_f64() * 1e6 / total_rows as f64,
        mb(new_peak),
        total_rows as f64 / (n_out.max(1) as f64),
    );
    eprintln!(
        "  ⇒ builder 那块缓冲占 ③ 峰值的 {:.0}%（{held_new} 条 × 平均每条 {:.0} B = {:.1} MB，\
             段峰值 {:.1} MB）｜ 一文件切在 target={target} B",
        100.0 * held_new as f64 * (bytes as f64 / total_rows.max(1) as f64)
            / (new_peak.max(1) as f64),
        bytes as f64 / total_rows.max(1) as f64,
        mb(held_new as u64 * (bytes as u64 / total_rows.max(1) as u64)),
        mb(new_peak),
    );
    // ⚠️ 报"省了多少"必须按**整条操作**比，不能拿 ② 与 ③ 两段各截一段比 ——
    // 旧形状的大头（物化那 154 MB）在 ① 段，② 只是叠在它上面多涨的 2.9 MB。
    // 只比 ② vs ③ 会得到"新形状反而更占内存"的错误结论（第一版这句就印错了）。
    let old_total = mat_peak + old_peak;
    eprintln!(
        "  ⇒ 整段瞬时内存：旧形状 ①+② = {:.1} MB → 新形状 ③ = {:.1} MB（省 {:.1}×）\
             | 整段时间：旧 ①+② = {:.1} ms → 新 ③ = {:.1} ms",
        mb(old_total),
        mb(new_peak),
        if new_peak > 0 {
            old_total as f64 / new_peak as f64
        } else {
            0.0
        },
        (t_materialize.as_secs_f64() + t_merge_write.as_secs_f64()) * 1e3,
        t_stream.as_secs_f64() * 1e3,
    );
    eprintln!(
        "  ④ 纯归并不写盘 {:>10.1} ms ⇒ 每行 {:.3} µs | 写这一段 ≈ {:.1} ms（占 ①+② 的 {:.0}%）",
        t_merge_only.as_secs_f64() * 1e3,
        t_merge_only.as_secs_f64() * 1e6 / total_rows as f64,
        (t_merge_write.as_secs_f64() - t_merge_only.as_secs_f64()) * 1e3,
        (t_merge_write.as_secs_f64() - t_merge_only.as_secs_f64()) * 100.0
            / (t_materialize.as_secs_f64() + t_merge_write.as_secs_f64()).max(1e-9),
    );
    eprintln!("  文件数={n_files}（压实输入个数）| 输出目标 {target} B/文件");
    drop(tree);
    cleanup(&dir);
    cleanup(&out_dir);
}
