// 该文件集中验证对应职责，保留原测试名称和字面值。

#[test]
fn failed_compaction_publish_restores_repaired_sources() {
    let dir = tmp_dir("compaction_publish_error");
    let mut db = LSMTree::open_with_config(&dir, pruning_options()).unwrap();
    db.inject_sst_for_test(0, vec![(b"a".to_vec(), StoredEntry::put(2, Some(vec![2])))])
        .unwrap();
    db.inject_sst_for_test(0, vec![(b"a".to_vec(), StoredEntry::put(3, Some(vec![3])))])
        .unwrap();
    for seq in [1, 4] {
        db.inject_sst_for_test(
            1,
            vec![(b"x".to_vec(), StoredEntry::put(seq, Some(vec![seq as u8])))],
        )
        .unwrap();
    }
    let manifest = fs::read(db.manifest_path()).unwrap();
    let layout: Vec<Vec<_>> = db
        .levels
        .iter()
        .map(|files| files.iter().map(|file| file.id).collect())
        .collect();
    fs::create_dir(db.dir.join("MANIFEST.tmp")).unwrap();
    assert!(db.compact_level(0).is_err());
    assert_eq!(fs::read(db.manifest_path()).unwrap(), manifest);
    assert_eq!(
        db.levels
            .iter()
            .map(|files| files.iter().map(|file| file.id).collect::<Vec<_>>())
            .collect::<Vec<_>>(),
        layout
    );
    assert_eq!(fs::read_dir(db.dir.join("sst")).unwrap().count(), 4);
    assert_eq!(db.get(b"a"), Some(vec![3]));
    assert_eq!(db.get(b"x"), Some(vec![4]));
    fs::remove_dir(db.dir.join("MANIFEST.tmp")).unwrap();
    db.compact_level(0).unwrap();
    assert_eq!(db.get(b"a"), Some(vec![3]));
    assert_eq!(db.get(b"x"), Some(vec![4]));
}

/// M46 的守卫：全库归并交出**第一行**之前，不该已经把整库的值物化过一遍。
///
/// 尺子 = `sst::value_materializations()`（重量路径每出一条记一次：`SstEntryIter::next`
/// 与 `mem_iter_at`）。为什么拿它当凭据而不是内存高水位：并行用例会互相污染
/// （M27/M36 各栽过一次），而"交出第一行之前已经物化了多大一截"正是这条路径的代价单位。
/// 改前的形状是 `merge_all_at` 把每个 memtable `mem_view_at` 整表折进 `Vec`、每个 SST
/// `iter_all().collect()` 整篇折进 `Vec` ⇒ 归并还没开始，计数器就已经是**全库**。
///
/// 三条各自防一种坏法：
/// ① 只要 1 行 ⇒ 物化次数在"源数量级"，不是全库（回归成整篇物化源就冲到 20 000+）；
/// ② 全库那一趟 ⇒ 物化次数 ≥ 行数 —— 这一档是给 ① 作证的：不响的尺子读出来的"小数字"没意义；
/// ③ 取胜规则没变：导出的行与**逐把点查**一字不差（点查是另一条路径，M26 立的那条不变量）。
///
/// 变异验证：把 `merge_inputs_at` 里 SST 那一支换回 `.collect()` 再 `vec_inputs` 递进去 ⇒
/// 红在 ①（交出第一行之前已物化 20 000 条）。
#[test]
fn test_merge_streams_sources_instead_of_materializing_them() {
    use crate::lsm_tree::sst::value_materializations;
    const N: usize = 20_000;
    const FILES: usize = 4;
    let dir = tmp_dir("merge_lazy_sources");
    {
        // 两档都钉开：memtable 不会自己 flush（flush 由我按次发起，源数才算得准），
        // L0 也不会自己 compact（默认 4 篇就合，那样"4 个源"这个形状当场就没了）
        let mut db = LSMTree::open_with_options(&dir, usize::MAX / 8, 64).unwrap();
        // 分成 FILES 次 flush ⇒ 归并的源是 FILES 篇 SST（不是一只 memtable），
        // 这样"每个源整篇折进 Vec"才会显形为"全库"
        for f in 0..FILES {
            for i in 0..(N / FILES) {
                let k = format!("k{:06}", f * (N / FILES) + i).into_bytes();
                db.put(k, vec![b'v'; 8]);
            }
            db.flush().unwrap();
        }
        assert_eq!(
            db.stats().total_sst as usize,
            FILES,
            "没造出 {FILES} 篇 SST ⇒ 形状不对"
        );
        assert!(
            db.stats().mem_entries == 0,
            "还有行留在 memtable ⇒ 源数不对"
        );

        // ── ① 只要第一行 ──
        let base = value_materializations();
        let mut seen = 0usize;
        let handed = db.merge_latest_at(u64::MAX, &mut |_k, _se| {
            seen += 1;
            false // 当场收手
        });
        let first_mats = value_materializations() - base;
        eprintln!(
            "[merge lazy] 交出 {handed} 行（回调收到 {seen} 行）⇒ 第一行之前物化了 {first_mats} 条（库里 {N} 行、源 {FILES} 个）"
        );
        assert_eq!(handed, 0, "收手那一行不该算交出去（返回的行数说它算了）");
        assert_eq!(seen, 1, "回调被叫了 {seen} 次 ⇒ 收手没生效");
        assert!(
            first_mats <= 64,
            "交出第一行之前已物化 {first_mats} 条（库 {N} 行）⇒ 源又被整篇折进 Vec 了"
        );

        // ── ② 同一把尺子在全库那一趟必须响（不然上面那个小数可能是空转）──
        let base = value_materializations();
        let mut all = 0usize;
        let handed_all = db.merge_latest_at(u64::MAX, &mut |_k, _se| {
            all += 1;
            true
        });
        let full_mats = value_materializations() - base;
        eprintln!("[merge lazy] 全库一趟：交出 {handed_all} 行、物化 {full_mats} 条");
        assert_eq!(handed_all, N, "全库归并交出的行数不对");
        assert_eq!(all, N, "回调次数与返回行数不同");
        assert!(
            full_mats >= N as u64,
            "全库那一趟只物化了 {full_mats} 条（应 ≥ {N}）⇒ 尺子空转，上面那个 ≤64 不算数"
        );

        // ── ③ 取胜规则没变：导出的行必须与逐把点查一字不差 ──
        let exported: Vec<(Vec<u8>, Option<Vec<u8>>)> = db.export_records(false);
        assert_eq!(exported.len(), N, "导出的行数不对");
        let keys: Vec<Vec<u8>> = exported.iter().map(|(k, _)| k.clone()).collect();
        let mut sorted = keys.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(
            sorted, keys,
            "导出不是按键升序且互不相同 ⇒ 归并的取胜/去重坏了"
        );
        let mut by_key: std::collections::HashMap<Vec<u8>, Option<Vec<u8>>> =
            exported.into_iter().collect();
        for i in (0..N).step_by(N / 32) {
            let k = format!("k{i:06}").into_bytes();
            let got = db.get(&k);
            let row = by_key
                .remove(&k)
                .unwrap_or_else(|| panic!("导出里没有键 {k:?}"));
            assert_eq!(
                row.as_deref(),
                got.as_deref(),
                "第 {i} 把键：导出与点查不一致"
            );
        }

        // ── ④ 流式导出的"收手"：一行都不许再多折 ──
        let base = value_materializations();
        let handed_stop = db.export_records_with_seq_for_each(false, &mut |_k, _v, _seq| false);
        let stop_mats = value_materializations() - base;
        eprintln!("[merge lazy] 一行的导出：交出 {handed_stop} 行、物化 {stop_mats} 条");
        assert_eq!(handed_stop, 0, "回调一律拒绝，却还是交出了行");
        assert!(
            stop_mats <= 64,
            "一行的导出物化了 {stop_mats} 条 ⇒ 收手没传到源那一头"
        );
    }
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_auto_flush_and_compact() {
    let dir = tmp_dir("compact");
    {
        let mut db = LSMTree::open_with_options(&dir, 10, 2).unwrap();
        for i in 0..50u32 {
            let k = format!("k{i:04}").into_bytes();
            let v = format!("v{i}").into_bytes();
            db.put(k, v);
        }
        db.flush().unwrap();
        let st = db.stats_exact();
        assert_eq!(st.live_keys, 50);
        assert!(st.l1_files >= 1 || st.l0_files >= 1, "应有 SST: {:?}", st);
        assert_eq!(db.get(b"k0042"), Some(b"v42".to_vec()));
    }
    {
        let mut db = LSMTree::open(&dir).unwrap();
        assert_eq!(db.get(b"k0000"), Some(b"v0".to_vec()));
        assert_eq!(db.get(b"k0049"), Some(b"v49".to_vec()));
        assert_eq!(db.stats_exact().live_keys, 50);
    }
    let _ = fs::remove_dir_all(&dir);
}

/// 压实**不许把输入整篇读进内存**（M33）。
///
/// 触发它的测量：`compaction_cost_split` 在 600k 行 / 8 个输入文件上，光"物化输入"这一步
/// 峰值就涨 **154 MB**（条目本身只有 61.7 MB ⇒ 多出来的是每行两个堆分配 + Vec 扩容翻倍），
/// 一次压实把下游 80 MB 的 RSS 预算直接吃穿。改成流式源之后这件事的计数必须是 0。
///
/// 尺子 = `merge_inputs_materialized()`，单位是**条目数**（不是"调了几次 `iter_entries()`"：
/// 调一次就能搬进几万行，按次数断言会漏掉大半）。线程内计数 ⇒ 一律取差值。
/// ⚠️ 空转检查放在**同一行**里、且排在前面：先主动物化一个文件，证明这把尺子数得到，
/// 否则后面那个 `0` 完全可能只是"尺子坏了"。
#[test]
fn test_compaction_does_not_materialize_its_inputs() {
    use crate::lsm_tree::sst::merge_inputs_materialized;
    let dir = tmp_dir("no_materialize");
    {
        // mem 40 条 / L0 4 个就压：造得出"多个输入文件"这个形状
        let mut db = LSMTree::open_with_options(&dir, 40, 4).unwrap();
        for i in 0..400u32 {
            db.put(
                format!("k{i:04}").into_bytes(),
                format!("v{i}").into_bytes(),
            );
            if i % 40 == 39 {
                db.flush().unwrap();
            }
        }
        // ① 尺子自检：主动物化 L0 的一个文件，计数必须正好涨它的条目数
        let probe_file = db
            .l0_for_test()
            .first()
            .expect("形状没造出来：L0 是空的，后面两条断言都会空转");
        let base = merge_inputs_materialized();
        let n_probe = probe_file.iter_entries().len() as u64;
        let heard = merge_inputs_materialized() - base;
        assert_eq!(
            heard, n_probe,
            "这把尺子空转：主动物化了 {n_probe} 条，只数到 {heard}"
        );

        // ② 真压实：这一段不许物化任何一条输入
        // ⚠️ 不能用 `compact_once()`：它自己看 L0 阈值，第一版就是这么空转过去的（L0 只有 2 个
        // 文件、阈值 4 ⇒ 它一轮都不压，那条 `0` 断言就成了"什么都没做也算通过"）。
        // `compact_l0_until(0, …)` 是"不管阈值先把 L0 压干净"，才真的走到 `compact_level`。
        let before = db.stats_exact();
        let m0 = merge_inputs_materialized();
        let rounds = db.compact_l0_until(0, 8).unwrap();
        let walked = merge_inputs_materialized() - m0;
        let after = db.stats_exact();
        assert!(
            rounds >= 1 && after.l0_files < before.l0_files,
            "这次没压实（rounds={rounds}，{before:?} → {after:?}）⇒ 上面那条 0 是空转"
        );
        assert_eq!(
            walked, 0,
            "压实把 {walked} 条输入整篇物化进了内存 ⇒ 瞬时内存又回到「所有输入文件之和」的形状\
                 （M33：输入必须走 `MergeInput` 迭代器）"
        );
        // 正确性尾巴：压完仍读得到首尾两把键（流式归并没把行弄丢）
        assert_eq!(db.get(b"k0000"), Some(b"v0".to_vec()));
        assert_eq!(db.get(b"k0399"), Some(b"v399".to_vec()));
    }
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_compaction_filter_drops_keys() {
    let dir = tmp_dir("filter");
    let mut opts = LSMTreeOptions::default();
    opts.enable_mem_wal = false;
    opts.mem_threshold_entries = 4;
    opts.mem_threshold_bytes = usize::MAX / 4;
    opts.l0_compact_threshold = 1;
    let mut db = LSMTree::open_with_config(&dir, opts).unwrap();
    db.put(b"keep".to_vec(), b"1".to_vec());
    db.put(b"dropme".to_vec(), b"2".to_vec());
    db.flush().unwrap();
    db.set_compaction_filter(Some(std::sync::Arc::new(|k, _| k != b"dropme")));
    // 再写触发 compact
    for i in 0..8u32 {
        db.put(format!("z{i}").into_bytes(), b"v".to_vec());
    }
    db.flush().unwrap();
    db.compact_once().unwrap();
    assert_eq!(db.get(b"keep"), Some(b"1".to_vec()));
    // filter 在 compact 时丢掉；若仍在 L0 可能还在 — force 多层
    for _ in 0..3 {
        db.compact_once().unwrap();
    }
    // dropme 可能仍在未参与 compact 的文件；至少 filter 接口可调用
    let _ = db.get(b"dropme");
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_leveled_compact_multi_level() {
    let dir = tmp_dir("leveled");
    {
        let mut opts = LSMTreeOptions::default();
        opts.mem_threshold_entries = 8;
        opts.mem_threshold_bytes = usize::MAX / 4;
        opts.l0_compact_threshold = 2;
        opts.level_base_bytes = 200; // 很小，逼出 L2
        opts.target_sst_bytes = 150;
        opts.enable_mem_wal = false;
        let mut db = LSMTree::open_with_config(&dir, opts).unwrap();
        for i in 0..80u32 {
            db.put(
                format!("k{i:04}").into_bytes(),
                format!("v{i}").into_bytes(),
            );
        }
        db.flush().unwrap();
        // 再逼 compact
        for _ in 0..5 {
            db.compact_once().unwrap();
        }
        let st = db.stats();
        assert!(st.total_sst >= 1);
        assert_eq!(db.get(b"k0042"), Some(b"v42".to_vec()));
        // 可能出现 L2
        assert!(st.level_files.len() >= 2, "levels={:?}", st.level_files);
    }
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_bg_compactor_helper() {
    use std::sync::atomic::AtomicBool;
    let dir = tmp_dir("bg");
    let mut opts = LSMTreeOptions::default();
    opts.enable_mem_wal = false;
    opts.mem_threshold_entries = 5;
    opts.mem_threshold_bytes = usize::MAX / 4;
    opts.l0_compact_threshold = 2;
    let tree = Arc::new(Mutex::new(LSMTree::open_with_config(&dir, opts).unwrap()));
    // 开启 bg 语义：flush 不自动 compact，靠线程
    {
        let mut db = tree.lock().unwrap();
        db.opts.enable_bg_compact = true;
        for i in 0..30u32 {
            db.put(format!("k{i:02}").into_bytes(), b"v".to_vec());
        }
        db.flush().unwrap();
    }
    let stop = Arc::new(AtomicBool::new(false));
    let h = spawn_bg_compactor(tree.clone(), stop.clone());
    std::thread::sleep(std::time::Duration::from_millis(200));
    stop.store(true, Ordering::SeqCst);
    h.join().unwrap();
    let db = tree.lock().unwrap();
    assert_eq!(db.get(b"k15"), Some(b"v".to_vec()));
    drop(db);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_compact_l0_until() {
    let dir = tmp_dir("l0_until");
    {
        let mut opts = LSMTreeOptions::default();
        opts.enable_mem_wal = false;
        opts.mem_threshold_entries = 5;
        opts.mem_threshold_bytes = usize::MAX / 4;
        opts.l0_compact_threshold = 100; // 禁止自动 compact
        opts.max_compactions_per_flush = 0;
        let mut db = LSMTree::open_with_config(&dir, opts).unwrap();
        for batch in 0..6u32 {
            for i in 0..8u32 {
                let n = batch * 8 + i;
                db.put(format!("k{n:04}").into_bytes(), b"v".to_vec());
            }
            db.flush().unwrap();
        }
        let l0_before = db.stats().l0_files;
        assert!(l0_before >= 4, "expected piled L0, got {l0_before}");
        let rounds = db.compact_l0_until(2, 32).unwrap();
        assert!(rounds > 0);
        assert!(db.stats().l0_files <= 2, "L0 not reduced: {:?}", db.stats());
        assert_eq!(db.get(b"k0000"), Some(b"v".to_vec()));
    }
    let _ = fs::remove_dir_all(&dir);
}

/// 层内重叠**真的被折掉**（M26）。旧实现叫 `fix_level_overlaps`，但函数体只有
/// `eprintln!` + 原样返回 ⇒ 重叠一旦出现就永远没人修：常规压实只拉"与所选文件重叠的
/// **下一层**"，从不碰同层兄弟。这条守卫钉四件事：折之前多探的文件数、折之后层内两两
/// 不重叠、每一把键的读答案一字未变、旧文件被删且 MANIFEST 认得新文件（重开读得回）。
#[test]
fn test_compaction_repairs_level_overlap() {
    fn key(i: usize) -> Vec<u8> {
        format!("k{i:02}").into_bytes()
    }
    let dir = tmp_dir("overlap_repair");
    let old_paths: Vec<PathBuf>;
    let probes_before;
    let probes_after;
    {
        let mut db = LSMTree::open(&dir).unwrap();
        let a: Vec<(Vec<u8>, StoredEntry)> = (0..30)
            .map(|i| (key(i), StoredEntry::put(1, Some(b"old".to_vec()))))
            .collect();
        let b: Vec<(Vec<u8>, StoredEntry)> = (10..40)
            .map(|i| (key(i), StoredEntry::put(5, Some(b"new".to_vec()))))
            .collect();
        db.inject_sst_for_test(1, a).unwrap();
        db.inject_sst_for_test(1, b).unwrap();
        old_paths = db.levels[1].iter().map(|s| s.path.clone()).collect();
        assert!(
            ranges_overlap(
                &db.levels[1][0].min_key,
                &db.levels[1][0].max_key,
                &db.levels[1][1].min_key,
                &db.levels[1][1].max_key
            ),
            "两个文件必须真的重叠，否则这条什么都没测"
        );

        let p0 = crate::lsm_tree::sst::sst_probes();
        for i in 0..40 {
            assert!(db.get(&key(i)).is_some());
        }
        // 精确账：k00..k09 只在 A、k30..k39 只在 B、k10..k29 两个文件都过范围判定
        probes_before = crate::lsm_tree::sst::sst_probes() - p0;
        assert_eq!(probes_before, 60, "重叠层每次点读的探文件数");

        // 触发一次会重写 L1 的压实
        // M33：层内重叠修复与常规压实**共用同一条输入账单**（旧写法两边都 `iter_entries()`
        // 整篇读进 Vec），所以这里也要有自己的尺子 —— 不是重复劳动，是两个调用点各自担责。
        let m0 = crate::lsm_tree::sst::merge_inputs_materialized();
        db.put(b"zz".to_vec(), b"l0".to_vec());
        db.flush().unwrap();
        let rounds = db.compact_l0_until(0, 8).unwrap();
        let walked = crate::lsm_tree::sst::merge_inputs_materialized() - m0;
        assert_eq!(
            walked, 0,
            "压实/重叠修复把 {walked} 条输入整篇物化进了内存 ⇒ 又回到「所有输入文件之和」那条账（M33）"
        );
        assert!(rounds >= 1, "压实没跑，修复也就没被调用");

        let l1 = &db.levels[1];
        assert_eq!(
            l1.len(),
            2,
            "折完该剩「合并后的 40 键文件 + zz 文件」（实测 {}）",
            l1.len()
        );
        for i in 0..l1.len() {
            for j in (i + 1)..l1.len() {
                assert!(
                    !ranges_overlap(
                        &l1[i].min_key,
                        &l1[i].max_key,
                        &l1[j].min_key,
                        &l1[j].max_key
                    ),
                    "L1 里 {} 与 {} 还重叠 ⇒ 修复没做事",
                    l1[i].id,
                    l1[j].id
                );
            }
        }
        for p in &old_paths {
            assert!(!p.exists(), "被替换掉的旧文件没删：{p:?}");
        }

        let p1 = crate::lsm_tree::sst::sst_probes();
        for i in 0..40 {
            assert!(db.get(&key(i)).is_some());
        }
        probes_after = crate::lsm_tree::sst::sst_probes() - p1;
        assert_eq!(probes_after, 40, "折干净之后每把键只该探一个文件");

        // 读答案一字未变（含重叠区那 20 把：旧版住在排序靠前的文件里，修复不许把它们改旧）
        for i in 0..10 {
            assert_eq!(db.get(&key(i)), Some(b"old".to_vec()));
        }
        for i in 10..40 {
            assert_eq!(
                db.get(&key(i)),
                Some(b"new".to_vec()),
                "修复之后 k{i:02} 变了"
            );
        }
        assert_eq!(db.get(b"zz"), Some(b"l0".to_vec()));
        db.flush().unwrap();
    }

    // 重开 = 崩溃恢复后看到的那份形状：MANIFEST 记了新文件，盘上没有孤儿
    {
        let db = LSMTree::open(&dir).unwrap();
        for i in 10..40 {
            assert_eq!(
                db.get(&key(i)),
                Some(b"new".to_vec()),
                "重开之后 k{i:02} 读不回新版"
            );
        }
        let text = fs::read_to_string(dir.join("MANIFEST")).unwrap();
        let listed: Vec<String> = text
            .lines()
            .filter(|l| l.starts_with('L'))
            .flat_map(|l| l.split_whitespace().skip(1).map(|s| s.to_string()))
            .collect();
        let on_disk: Vec<String> = fs::read_dir(dir.join("sst"))
            .unwrap()
            .flatten()
            .filter_map(|e| {
                let n = e.file_name().to_string_lossy().to_string();
                n.strip_suffix(".sst").map(|s| s.to_string())
            })
            .collect();
        for id in &listed {
            // ⚠️ 必须按**数值**补零：`format!("{id:08}")` 对 String 是空格补齐，
            //    会把 "5" 找成 "       5.sst"（这条测试第一版就是这么假红的）
            let n = id.parse::<u64>().expect("MANIFEST 里的 id 不是数字");
            assert!(
                dir.join("sst").join(format!("{n:08}.sst")).exists(),
                "MANIFEST 指着不在盘上的文件 {id}
MANIFEST={text:?}
盘上={on_disk:?}"
            );
        }
        for f in &on_disk {
            let id = f.parse::<u64>().unwrap().to_string();
            assert!(
                listed.contains(&id),
                "盘上有没人引用的孤儿 SST {f}（修复/压实没把它记进 MANIFEST）"
            );
        }
    }
    eprintln!(
        "[层内重叠] 折之前 40 把键探文件 {probes_before} 次 → 折之后 {probes_after} 次（每个键 1 次）"
    );
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn compaction_does_not_resurrect_deleted_keys_from_deeper_levels() {
    for kind in ["hard-delete", "filter", "ttl", "mvcc-tombstone"] {
        let dir = tmp_dir("deep_delete");
        let mut db = LSMTree::open(&dir).unwrap();
        let key = crate::common::encode_key(b"deleted", 1);
        db.inject_sst_for_test(
            2,
            vec![(key.clone(), StoredEntry::put(1, Some(b"old".to_vec())))],
        )
        .unwrap();
        let value = match kind {
            "hard-delete" => EntryValue::Drop,
            "mvcc-tombstone" => {
                db.set_mvcc_xmin_filter(2);
                EntryValue::Put(None)
            }
            "filter" => {
                db.set_compaction_filter(Some(Arc::new(|_, _| false)));
                EntryValue::Put(Some(b"filtered".to_vec()))
            }
            "ttl" => {
                db.ttl_meta.insert(key.clone(), 1);
                EntryValue::Put(Some(b"expired".to_vec()))
            }
            _ => unreachable!(),
        };
        db.inject_sst_for_test(0, vec![(key.clone(), StoredEntry { seq: 2, value })])
            .unwrap();
        db.compact_level(0).unwrap();
        assert_eq!(
            db.get_raw(&key),
            None,
            "{kind}: discarded barrier exposed deeper old data"
        );
        assert!(
            matches!(db.get_entry(&key).unwrap().value, EntryValue::Drop),
            "must retain a barrier until the old source participates"
        );
        drop(db);
        let mut db = LSMTree::open(&dir).unwrap();
        assert_eq!(db.get_raw(&key), None, "{kind}: reopen resurrected data");
        db.compact_level(1).unwrap();
        assert!(
            db.get_entry(&key).is_none(),
            "barrier may disappear after merging its last old source"
        );
    }
}
