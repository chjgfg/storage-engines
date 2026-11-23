// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn scan_row_ties_keep_point_read_fallback() {
    use crate::lsm_tree::sst::ScanRow;
    let dir = tmp_dir("scan_tied_sources");
    let mut db = LSMTree::open_with_config(&dir, pruning_options()).unwrap();
    db.inject_sst_for_test(1, vec![(b"k".to_vec(), StoredEntry::put(4, Some(vec![1])))])
        .unwrap();
    db.inject_sst_for_test(0, vec![(b"k".to_vec(), StoredEntry::put(4, Some(vec![2])))])
        .unwrap();
    let (key, row) = db.row_iter_range(None, None, 4).next().unwrap();
    assert_eq!(key, b"k");
    assert!(matches!(
        row.freeze(|| key.clone()),
        ScanRow::AmbiguousLookup(_)
    ));
    assert_eq!(db.get_value_at(&key, 4), Some(Some(vec![1])));
    assert_eq!(
        db.find_back(b"k", b"k", 4, |_, _| true),
        Some((b"k".to_vec(), Some(vec![1])))
    );
    let clones = crate::lsm_tree::sst::entry_value_clones();
    assert_eq!(db.row_iter_range(None, None, 4).count(), 1);
    assert_eq!(crate::lsm_tree::sst::entry_value_clones(), clones);
}

#[test]
fn key_lookup_full_scan_is_explicit_and_keeps_the_byte_interval() {
    let dir = tmp_dir("key_back_fallback");
    let mut db = LSMTree::open_with_config(&dir, pruning_options()).unwrap();
    for i in 0..4200_u64 {
        db.mem_insert_no_wal(i.to_be_bytes().to_vec(), StoredEntry::put(1, Some(vec![1])));
    }
    db.mem_insert_no_wal(0_u64.to_be_bytes().to_vec(), StoredEntry::drop_at(2));
    let outcome = db.find_key_back(&0_u64.to_be_bytes(), &4199_u64.to_be_bytes(), 2, |key| {
        key == 1_u64.to_be_bytes()
    });
    assert!(
        matches!(outcome, KeyBackResult::FullScan(Some(ref key)) if key == &1_u64.to_be_bytes()),
        "4096 rejected keys must enter the separately typed full-scan fallback"
    );
    assert!(
        matches!(
            db.find_key_back(&0_u64.to_be_bytes(), &4199_u64.to_be_bytes(), 2, |key| key
                == 0_u64.to_be_bytes()),
            KeyBackResult::FullScan(None)
        ),
        "Drop cannot revive in fallback"
    );
}

/// M47 的守卫：三格"为一个数物化整库"（`entry_count`、`len` 的 dirty 路径、
/// `stats_exact` → `refresh_stats_cache`）改成只数不物化之后，**数要一字不差**，
/// 而 value 一次都不该被克隆。
///
/// 两把尺子各钉各的（M44 的教训：一把读 0 的尺子可能只是没接上）：
/// ① **对拍重量归并** —— 测试里直接把 `merge_all_at` 跑一遍按类数出来当基准，
///   形状必须混：存活行、空值 `Put(None)`、`Drop` 墓碑，再加"同一把键在 memtable 与
///   多份 SST 里都有"（跨源取胜才是这条路径真正在算的东西）；
/// ② `value_materializations()` 在三格上增量必须是 **0**，而同一个测试里基准那一趟
///   必须读到 ≥ 全库 ⇒ 证明这把尺子确实接在重量路径上。
#[test]
fn test_counting_paths_do_not_materialize_values() {
    use crate::lsm_tree::sst::value_materializations;
    let dir = tmp_dir("count_no_materialize");
    {
        let mut db = LSMTree::open(&dir).unwrap();
        for i in 0..400usize {
            db.put(format!("k{i:04}").into_bytes(), vec![b'v'; 4]);
        }
        for i in 100..140usize {
            db.insert(format!("e{i:04}").into_bytes(), None);
        }
        for i in 200..230usize {
            db.delete(format!("k{i:04}").into_bytes());
        }
        // `remove` 写的是第三种赢家行 `EntryValue::Drop`（`delete` 只写 `Put(None)`）：
        // 三种种类都要在场，否则 `count_kinds_at` 里那一支 match 分支量不到。
        for i in 50..62usize {
            assert!(db.remove(format!("k{i:04}").as_bytes()));
        }
        db.flush().unwrap();
        // 同一把键再写一遍 ⇒ memtable 与 SST 里都有它，跨源取胜才被判到
        for i in 300..340usize {
            db.put(format!("k{i:04}").into_bytes(), vec![b'w'; 9]);
        }
        db.insert(format!("e{:04}", 105).into_bytes(), Some(b"again".to_vec()));
        db.flush().unwrap();
        for i in 350..360usize {
            db.delete(format!("k{i:04}").into_bytes());
        }
        for i in 100..108usize {
            assert!(db.remove(format!("k{i:04}").as_bytes()));
        }
        assert!(
            db.stats().total_sst >= 2,
            "没造出两份以上 SST ⇒ 这次对拍量不到跨源"
        );

        // ── 基准：重量归并逐条数（这就是改前三格的实现）──
        let base = value_materializations();
        let all = db.merge_all_at(u64::MAX);
        let want = (
            all.iter()
                .filter(|(_, se)| matches!(se.value, EntryValue::Put(Some(_))))
                .count(),
            all.iter()
                .filter(|(_, se)| matches!(se.value, EntryValue::Put(None)))
                .count(),
            all.iter()
                .filter(|(_, se)| matches!(se.value, EntryValue::Drop))
                .count(),
        );
        let heavy = value_materializations() - base;
        eprintln!(
            "[count] 重量基准 live={} 空值={} 墓碑={}（这一趟物化 {} 条）",
            want.0, want.1, want.2, heavy
        );
        assert!(
            want.1 > 0 && want.2 > 0,
            "形状没混起来：空值 {} 墓碑 {}",
            want.1,
            want.2
        );
        assert!(
            heavy >= all.len() as u64,
            "重量基准只物化了 {} 条（全库 {} 行）⇒ 尺子没接上，下面那个 0 不算数",
            heavy,
            all.len()
        );

        // ── ① 数一字不差 ──
        assert_eq!(
            db.count_kinds_at(u64::MAX),
            want,
            "轻量计数与重量归并不一致"
        );
        assert_eq!(db.entry_count(), all.len(), "entry_count 与改前不同数");
        assert!(
            db.stats_dirty,
            "没走在 dirty 路径上 ⇒ 这一格量的不是那次全量计数"
        );
        assert_eq!(db.len(), want.0, "len（dirty 路径）与改前不同数");

        // ── ② 三格合起来一行 value 都不许克隆 ──
        let base = value_materializations();
        let n = db.entry_count();
        let m = db.len();
        let st = db.stats_exact();
        let mats = value_materializations() - base;
        eprintln!(
            "[count] 只数不物化：entry_count={} len={} live={} tomb={} ⇒ 物化 {} 条",
            n, m, st.live_keys, st.tombstones, mats
        );
        assert_eq!(
            mats, 0,
            "计数这三格克隆了 {mats} 条 value ⇒ 还在为一个数折整库"
        );
        assert_eq!(n, all.len(), "entry_count 二次调用给了不同的数");
        assert_eq!(
            st.live_keys as usize, want.0,
            "stats_exact 的 live 与基准不同"
        );
        assert_eq!(
            st.tombstones as usize, want.1,
            "stats_exact 的 tomb 与基准不同"
        );
    }
    let _ = fs::remove_dir_all(&dir);
}

/// M47 顺带挖出来的一条**先前就存在**的少算：轻量走查在"没有上界"时拿一个合成上界
/// `&[0xffu8; 8]` 去剪 SST ⇒ 一篇 `min_key` 比它更大的文件被**整篇跳过**，
/// 全库计数/首页扫描少了那些键，而且不报错。
///
/// 这一格为什么归这里：M47 把 `entry_count` 接到 `light_iter_range` 上，剪枝判据从此
/// 也在"数出全库有多少键"这条路上生效 —— 少算的不再是"某一页少几行"而是"整个数不对"。
/// 同一个判据的用户（`scan_page` / 窗口搜索 / 有界 vacuum / `health()`）一起受益。
#[test]
fn test_light_scan_without_upper_bound_keeps_files_past_the_old_sentinel() {
    let dir = tmp_dir("count_high_keys");
    {
        let mut db = LSMTree::open(&dir).unwrap();
        for i in 0..50usize {
            db.put(format!("k{i:04}").into_bytes(), vec![b'v'; 4]);
        }
        db.flush().unwrap();
        // 这一篇里**每一把键**都大于 `[0xff;8]`（12 个 0xff，末位才动）⇒ 改前那行
        // `may_overlap(low, &[0xffu8; 8])` 判定 `min_key <= high` 为假 ⇒ 整篇不读。
        for i in 0..7u8 {
            let mut k = vec![0xffu8; 12];
            k[11] = i;
            db.put(k, vec![b'w'; 4]);
        }
        db.flush().unwrap();
        assert!(
            db.stats().total_sst >= 2,
            "没造出独立的第二篇 SST ⇒ 这一格量不到剪枝"
        );

        // 重量归并不剪文件，它就是基准
        let all = db.merge_all_at(u64::MAX);
        assert_eq!(all.len(), 57, "基准本身不对：{all:?}");
        assert_eq!(
            db.entry_count(),
            all.len(),
            "全库计数漏掉了 min_key 高于旧合成上界的那篇 SST"
        );
        // 有下界、无上界的那一趟同样不许把尾部剪掉
        let tail: Vec<_> = db
            .light_iter_range(Some(b"k0049"), None, u64::MAX)
            .collect();
        assert_eq!(tail.len(), 1 + 7, "开区间走查少算：{tail:?}");
        assert_eq!(
            db.count_kinds_at(u64::MAX).0,
            all.len(),
            "计数与开区间走查口径分家"
        );
    }
    let _ = fs::remove_dir_all(&dir);
}

/// M47 的内存凭据：三格"为一个数物化整库"（`entry_count`、`len` 的 dirty 路径、
/// `stats_exact` → `refresh_stats_cache`）那一趟到底瞬时涨多少（`#[ignore]`，手动 release 单线程跑）。
///
/// 两臂在**同一个构建**里，只切 `LSM_COUNT_OLD_SHAPE=1`：那一臂把三格各自的旧写法
/// （各调一次 `merge_all_at(u64::MAX)` 再数）就地重放一遍。⚠️ 两臂都在每一格之前把
/// `stats_dirty` 压回 true —— 否则第二、三格读的是缓存，量的就不是那三扇门了
/// （M44 的教训：尺子必须钉在旧形状真正流过的那扇门上）。
///
/// ```text
/// LSM_COUNT_ROWS=600000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 lsm_counting_doors_peak
/// LSM_COUNT_OLD_SHAPE=1 LSM_COUNT_ROWS=600000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 lsm_counting_doors_peak
/// ```
#[test]
#[ignore = "三格计数的内存台架，手动 release 单线程跑：LSM_COUNT_ROWS=600000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 lsm_counting_doors_peak"]
fn lsm_counting_doors_peak() {
    use crate::lsm_tree::sst::value_materializations;
    let rows: usize = std::env::var("LSM_COUNT_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200_000);
    let old = std::env::var("LSM_COUNT_OLD_SHAPE")
        .map(|v| v == "1")
        .unwrap_or(false);
    let mb = |b: u64| b as f64 / 1024.0 / 1024.0;
    let dir = tmp_dir("counting_doors_peak");
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
        let (n_entry, n_live, st) = if old {
            // 改前：每一格各自把整库连 value 折出来再数（口径与那一版一字不差）
            db.stats_dirty = true;
            let n_entry = db.merge_all_at(u64::MAX).len();
            db.stats_dirty = true;
            let n_live = db
                .merge_all_at(u64::MAX)
                .iter()
                .filter(|(_, se)| matches!(se.value, EntryValue::Put(Some(_))))
                .count();
            db.stats_dirty = true;
            let st = {
                let all = db.merge_all_at(u64::MAX);
                let live = all
                    .iter()
                    .filter(|(_, se)| matches!(se.value, EntryValue::Put(Some(_))))
                    .count();
                let tomb = all
                    .iter()
                    .filter(|(_, se)| matches!(se.value, EntryValue::Put(None)))
                    .count();
                db.cached_live = live;
                db.cached_tomb = tomb;
                db.stats_dirty = false;
                db.stats()
            };
            (n_entry, n_live, st)
        } else {
            db.stats_dirty = true;
            let n_entry = db.entry_count();
            db.stats_dirty = true;
            let n_live = db.len();
            db.stats_dirty = true;
            let st = db.stats_exact();
            (n_entry, n_live, st)
        };
        let d = t.elapsed();
        let peak = crate::mem_probe::peak().saturating_sub(base);
        let mats = value_materializations() - m0;
        eprintln!(
            "=== 三格计数 rows={rows} 源={files} 臂={} ⇒ entry_count={n_entry} len={n_live} live={} tomb={} 瞬时峰值 {:.1} MB、物化 {mats} 条、耗时 {:.2} s ===",
            if old {
                "改前(整库折出来)"
            } else {
                "改后(只数)"
            },
            st.live_keys,
            st.tombstones,
            mb(peak),
            d.as_secs_f64()
        );
    }
    let _ = fs::remove_dir_all(&dir);
}

/// M19 守卫①：增量计数器 `mem_entries` 的**口径**必须与慢算法一字不差。
///
/// 口径是「版本条数」（Σ 每键版本数），不是「键数」—— 当年正是为此才去 `sum` 整表。
/// 计数器只要漏了一条路径（同 seq 覆盖该不该加、flush 有没有清零），这条对拍当场红。
#[test]
fn test_mem_entry_counter_matches_slow_scan() {
    let dir = tmp_dir("mem_counter");
    {
        let mut db = LSMTree::open(&dir).unwrap();
        let check = |db: &LSMTree, at: &str| {
            assert_eq!(
                db.stats().mem_entries,
                db.scan_mem_entry_count(),
                "{at}：计数器与慢口径不一致"
            );
        };
        check(&db, "空库");
        for i in 0..50 {
            db.put(format!("k{i}").into_bytes(), b"v".to_vec());
        }
        check(&db, "50 把新键");
        // 同一把键堆版本：条数按版本算，不按键算
        for _ in 0..7 {
            db.put(b"k0".to_vec(), b"v2".to_vec());
        }
        check(&db, "k0 多版本");
        db.delete(b"k1".to_vec());
        check(&db, "含墓碑");
        assert_eq!(
            db.stats().mem_entries,
            58,
            "条数口径 = 版本条数（50 + 追加 7 + 墓碑 1）"
        );
        db.flush().unwrap();
        check(&db, "flush 之后");
        assert_eq!(
            db.stats().mem_entries,
            0,
            "flush 没清零 ⇒ 下一次判定会提前触发"
        );
    }
    let _ = fs::remove_dir_all(&dir);
}

/// M19 守卫②：**插一行不该为「要不要 flush」去数整个 memtable**。
///
/// 这条是 M19 的病根。2026-10-17 的 200k 大规模基线抓到 LSM 灌行是 O(N²)：每次插入都
/// `self.mem.values().map(len).sum()` 一遍，于是裸 `insert` 每行从 89 µs 涨到 1429 µs。
/// 尺子数的不是墙钟而是**为判定走过的 memtable 条目数** —— 改成增量计数器之后它必须是 0。
///
/// 对照段是必需的：探针如果空转，`assert_eq!(steps, 0)` 会永远绿。所以同一条用例里主动
/// 慢数一次，要求它**真的**被记进账（步数 == 表大小），证明这把尺子量得到东西。
#[test]
fn test_flush_check_does_not_scan_memtable_per_insert() {
    const N: usize = 4000;
    let dir = tmp_dir("mem_counter_hot");
    {
        let mut db = LSMTree::open(&dir).unwrap();
        let s0 = mem_scan_steps();
        for i in 0..N {
            db.put(format!("key{i:06}").into_bytes(), b"x".to_vec());
        }
        let steps = mem_scan_steps() - s0;
        assert_eq!(
            steps,
            0,
            "灌 {N} 行为回答「要不要 flush」走了 {steps} 个 memtable 条目 ⇒ 热路径又变回\
                 每插一行数一遍整表（旧形状实测 ≈ {} 步）",
            (N * (N - 1) / 2) as u64
        );
        // 对照：主动慢数一次，探针必须数得到
        let slow = db.scan_mem_entry_count() as u64;
        assert_eq!(
            mem_scan_steps() - s0,
            slow,
            "探针空转：慢口径没被记进账，上一条断言不算数"
        );
        assert!(slow >= N as u64, "对照必须真数到 ≥{N} 条，实际 {slow}");
    }
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_range_scan_and_remove() {
    let dir = tmp_dir("range");
    {
        let mut db = LSMTree::open(&dir).unwrap();
        db.insert(
            b"k\x00\x00\x00\x00\x00\x00\x00\x01".to_vec(),
            Some(b"v1".to_vec()),
        );
        db.insert(
            b"k\x00\x00\x00\x00\x00\x00\x00\x02".to_vec(),
            Some(b"v2".to_vec()),
        );
        db.insert(
            b"z\x00\x00\x00\x00\x00\x00\x00\x01".to_vec(),
            Some(b"vz".to_vec()),
        );
        let r = db.range_scan(
            b"k\x00\x00\x00\x00\x00\x00\x00\x00".to_vec(),
            b"k\x00\x00\x00\x00\x00\x00\x00\xff".to_vec(),
        );
        assert_eq!(r.len(), 2);
        assert!(db.remove(b"k\x00\x00\x00\x00\x00\x00\x00\x01"));
        let r2 = db.range_scan(
            b"k\x00\x00\x00\x00\x00\x00\x00\x00".to_vec(),
            b"k\x00\x00\x00\x00\x00\x00\x00\xff".to_vec(),
        );
        assert_eq!(r2.len(), 1);
    }
    let _ = fs::remove_dir_all(&dir);
}
