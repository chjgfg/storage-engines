// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn search_borrows_nonpage_keys_and_keeps_only_page_and_chunk_cursors() {
    const ROWS: usize = 16_384;
    let db = search_seed(ROWS, 128);
    let tx = db.begin_transaction();
    for reference in [true, false] {
        let mode = reference.then(search_key_reference::Scope::start);
        let before = search_key_reference::copies();
        assert_eq!(tx.key_count(b""), ROWS);
        let copies = search_key_reference::copies() - before;
        let guard = std::panic::catch_unwind(|| {
            assert_eq!(
                copies, 3,
                "only three incomplete-chunk cursor keys may be owned"
            )
        });
        assert_eq!(
            guard.is_err(),
            reference,
            "copy-all control must fail unchanged guard"
        );
        assert_eq!(copies, if reference { ROWS as u64 } else { 3 });
        let before = search_key_reference::copies();
        let q = crate::common::SearchQuery::contains(b"", 0, 100);
        let page = tx.search_keys(&q);
        assert_eq!(page.total, Some(ROWS));
        assert!(page.has_next);
        assert_eq!(page.items[99].key, 99u64.to_be_bytes());
        assert_eq!(page.items[99].value, Some(vec![b'v'; 128]));
        assert_eq!(
            search_key_reference::copies() - before,
            if reference { ROWS as u64 } else { 103 }
        );
        drop(mode);
    }
    tx.commit();
}

#[test]
#[ignore = "same memory seed ABBA for borrowed page-external keys"]
fn search_borrowed_keys_same_seed_abba() {
    let rows = std::env::var("MEMORY_KEY_ROWS")
        .map(|s| s.parse().unwrap())
        .unwrap_or(200_000usize);
    assert!(rows >= 10_000);
    let db = search_seed(rows, 128);
    let tx = db.begin_transaction();
    assert_eq!(tx.key_count(b""), rows);
    for task in ["count", "first_total", "deep_total"] {
        for (arm, reference) in [true, false, false, true].into_iter().enumerate() {
            let mode = reference.then(search_key_reference::Scope::start);
            let before = search_key_reference::copies();
            let now = std::time::Instant::now();
            let (result, allocations) = crate::mem_probe::count_thread_allocations(|| {
                if task == "count" {
                    tx.key_count(b"")
                } else {
                    let page = if task == "deep_total" {
                        rows / 100 - 1
                    } else {
                        0
                    };
                    let q = crate::common::SearchQuery::contains(b"", page, 100);
                    let result = tx.search_keys(&q);
                    assert_eq!(result.total, Some(rows));
                    assert_eq!(result.items.len(), 100);
                    assert_eq!(result.items[0].key, ((page * 100) as u64).to_be_bytes());
                    assert_eq!(result.items[99].value, Some(vec![b'v'; 128]));
                    result.items.len()
                }
            });
            let ms = now.elapsed().as_secs_f64() * 1000.0;
            let copies = search_key_reference::copies() - before;
            assert_eq!(result, if task == "count" { rows } else { 100 });
            if reference {
                assert_eq!(copies, rows as u64);
            } else {
                assert!(copies <= (rows / 4096 + 100) as u64);
            }
            eprintln!(
                "MEMORY_KEY_ABBA rows={rows} task={task} arm={arm} reference={reference} ms={ms:.3} copies={copies} allocations={allocations}"
            );
            drop(mode);
        }
    }
    tx.commit();
}

#[test]
fn search_count_and_offset_allocate_keys_but_only_page_values() {
    const ROWS: usize = 16_384;
    const PAGE: usize = 17;
    let db = search_seed(ROWS, 1024);
    let tx = db.begin_transaction();
    // 每个最终键分配一次，另外只有有界的候选容器和游标。
    // 恢复旧值复制路径会增加 ROWS 次分配，必须触发同一守卫失败。
    let copies = search_copy_reference::copies();
    let (count, count_alloc) = crate::mem_probe::count_thread_allocations(|| tx.key_count(b""));
    assert_eq!(count, ROWS);
    assert_eq!(search_copy_reference::copies() - copies, 0);
    assert!(
        count_alloc < (ROWS + ROWS / 16) as u64,
        "count allocated {count_alloc}; stored values were copied"
    );
    let query = crate::common::SearchQuery::contains(b"", ROWS / PAGE - 2, PAGE);
    let copies = search_copy_reference::copies();
    let (page, page_alloc) = crate::mem_probe::count_thread_allocations(|| tx.search_keys(&query));
    assert_eq!(page.total, Some(ROWS));
    assert_eq!(page.items.len(), PAGE);
    assert_eq!(search_copy_reference::copies() - copies, PAGE as u64);
    assert!(
        page_alloc < (ROWS + ROWS / 16) as u64,
        "page allocated {page_alloc}; page-external values were copied"
    );
    let old = search_copy_reference::Scope::start();
    let copies = search_copy_reference::copies();
    let (old_count, old_alloc) = crate::mem_probe::count_thread_allocations(|| tx.key_count(b""));
    assert_eq!(search_copy_reference::copies() - copies, ROWS as u64);
    let copies = search_copy_reference::copies();
    let (old_page, old_page_alloc) =
        crate::mem_probe::count_thread_allocations(|| tx.search_keys(&query));
    assert_eq!(search_copy_reference::copies() - copies, ROWS as u64);
    drop(old);
    assert_eq!(old_count, count);
    assert_eq!(old_page.items, page.items);
    assert_eq!(old_page.total, page.total);
    assert!(old_alloc >= (ROWS + ROWS / 16) as u64);
    assert!(old_page_alloc >= (ROWS + ROWS / 16) as u64);
    eprintln!(
        "SEARCH_ALLOCATION_GUARD rows={ROWS} count_new={count_alloc} count_old={old_alloc} page_new={page_alloc} page_old={old_page_alloc} kept={PAGE}"
    );
}

#[test]
#[ignore = "memory text seed ABBA for immediate finalized-group consumption"]
fn search_streaming_cpu_same_seed_abba() {
    use std::format;
    let rows: usize = std::env::var("MEMORY_WINDOW_ROWS")
        .map(|s| s.parse().unwrap())
        .unwrap_or(1_000_000);
    let db = MVCC::new();
    db.set_vacuum_auto_slice(0);
    let tx = db.begin_transaction();
    for i in 0..rows {
        assert!(tx.set_raw(
            format!("{i:08}#/worldquant/datafields/x.json").as_bytes(),
            vec![9; 64]
        ));
    }
    tx.commit();
    let tx = db.begin_transaction();
    assert_eq!(tx.key_count(b""), rows);
    for (arm, old) in [true, false, false, true].into_iter().enumerate() {
        let mode = old.then(search_stream_reference::Scope::start);
        let before = fold_keys();
        let now = std::time::Instant::now();
        let page = tx.search_keys(&crate::common::SearchQuery::contains(b"", 0, 100));
        let ms = now.elapsed().as_secs_f64() * 1000.0;
        assert_eq!(page.total, Some(rows));
        assert_eq!(page.items.len(), 100);
        for (i, item) in page.items.iter().enumerate() {
            assert_eq!(
                item.key,
                format!("{i:08}#/worldquant/datafields/x.json").into_bytes()
            );
            assert_eq!(item.value, Some(vec![9; 64]));
        }
        std::eprintln!(
            "MEMORY_STREAM_ABBA rows={rows} arm={arm} old={old} ms={ms:.3} physical_visits={} all_values_verified=100",
            fold_keys() - before
        );
        drop(mode);
    }
    tx.commit();
}

#[test]
fn search_streaming_removes_chunk_arrays_with_old_path_control() {
    let db = search_seed(50_000, 64);
    let tx = db.begin_transaction();
    let q = crate::common::SearchQuery::contains(b"", 0, 100);
    let reference = search_stream_reference::Scope::start();
    let (old, old_alloc) = crate::mem_probe::count_thread_allocations(|| tx.search_keys(&q));
    drop(reference);
    let (new, new_alloc) = crate::mem_probe::count_thread_allocations(|| tx.search_keys(&q));
    assert_eq!(new.items, old.items);
    assert_eq!(new.total, old.total);
    assert_eq!(new.has_next, old.has_next);
    let allocation_guard = |measured| {
        assert!(
            measured < old_alloc,
            "direct consumption must remove real allocations"
        )
    };
    allocation_guard(new_alloc);
    let guard = std::panic::catch_unwind(|| allocation_guard(old_alloc));
    assert!(
        guard.is_err(),
        "restoring old allocation count must fail the same guard"
    );
    eprintln!(
        "MEMORY_STREAM_ARRAY_GUARD rows=50000 old_allocations={old_alloc} new_allocations={new_alloc}"
    );
}

#[test]
#[ignore = "read-only same-seed ABBA for count and offset search"]
fn search_retention_same_seed_abba() {
    const ROWS: usize = 200_000;
    let db = search_seed(ROWS, 128);
    let tx = db.begin_transaction();
    for task in [
        "count",
        "first_total",
        "deep_total",
        "first_no_total",
        "deep_no_total",
    ] {
        for (arm, improved) in [false, true, true, false].into_iter().enumerate() {
            let reference = (!improved).then(search_copy_reference::Scope::start);
            let now = std::time::Instant::now();
            let (total, allocations) = crate::mem_probe::count_thread_allocations(|| {
                if task == "count" {
                    tx.key_count(b"")
                } else {
                    let page = if task.starts_with("deep") {
                        ROWS / 100 - 1
                    } else {
                        0
                    };
                    let mut q = crate::common::SearchQuery::contains(b"", page, 100);
                    q.need_total = !task.ends_with("no_total");
                    let result = tx.search_keys(&q);
                    assert_eq!(result.items.len(), 100);
                    assert_eq!(result.items[0].key, ((page * 100) as u64).to_be_bytes());
                    assert_eq!(result.items[99].value, Some(vec![b'v'; 128]));
                    if q.need_total {
                        assert_eq!(result.total, Some(ROWS));
                    }
                    result.items.len()
                }
            });
            let ms = now.elapsed().as_secs_f64() * 1000.0;
            drop(reference);
            assert_eq!(total, if task == "count" { ROWS } else { 100 });
            eprintln!(
                "SEARCH_RETENTION_ABBA task={task} arm={arm} improved={improved} rows={ROWS} ms={ms:.3} allocations={allocations}"
            );
        }
    }
}

#[test]
fn search_reference_fails_the_same_allocation_guard() {
    let reference = search_copy_reference::Scope::start();
    let rejected =
        std::panic::catch_unwind(search_count_and_offset_allocate_keys_but_only_page_values);
    drop(reference);
    assert!(
        rejected.is_err(),
        "old eager value cloning must fail the unchanged guard"
    );
    eprintln!("SEARCH_NEGATIVE_CONTROL expected_copy_guard_failure=true");
}

#[test]
fn scan_owned_pages_preserve_expiry_progress_and_visibility() {
    let db = MVCC::new();
    db.set_vacuum_auto_slice(0);
    let tx = db.begin_transaction();
    for i in 0..1003 {
        let key = format!("{i:08}").into_bytes();
        let logical = if i % 3 == 0 {
            crate::common::pack_ttl(b"expired", 0)
        } else if i % 3 == 1 {
            crate::common::pack_ttl(&[0, 1, 255], u64::MAX)
        } else {
            crate::common::pack_plain(b"")
        };
        assert!(tx.set(&key, logical));
    }
    assert!(tx.delete(b"00000002"));
    tx.try_commit().unwrap();
    drop(tx);
    let held = db.begin_transaction();
    assert!(held.set_raw(b"00000001", vec![9]));
    let mut golden = None;
    for reference in [true, false] {
        let _mode = reference.then(crate::common::store::scan_cost_reference::Scope::start);
        let reader = db.begin_transaction();
        let full = reader.scan(None, None);
        assert_eq!(full.len(), 667);
        let mut page_rows = Vec::new();
        let mut cursor = None;
        let mut pages = 0;
        loop {
            let (page, next) = reader.scan_page(cursor.as_deref(), 1);
            pages += 1;
            assert!(pages < 1100, "expired groups must still advance the cursor");
            page_rows.extend(page);
            let Some(next) = next else {
                break;
            };
            assert!(cursor.as_ref().is_none_or(|old| old < &next));
            cursor = Some(next);
        }
        assert_eq!(page_rows, full);
        if let Some(expected) = &golden {
            assert_eq!(&full, expected);
        } else {
            golden = Some(full);
        }
        assert_eq!(page_rows[0].value, Some(vec![0, 1, 255]));
        assert_eq!(page_rows[0].expire_unix_secs, Some(u64::MAX));
    }
    held.rollback();
}

#[test]
fn ordered_writes_skip_only_proven_absent_version_searches() {
    const ROWS: u64 = 1003;
    for reference in [true, false] {
        let _mode = reference.then(replay_cost_reference::Scope::start);
        let db = MVCC::new();
        let tx = db.begin_transaction();
        let probe = crate::memory::mem_store::key_lookup_probe::Scope::start();
        for i in 0..ROWS {
            assert!(tx.set_raw(&i.to_be_bytes(), vec![1]));
        }
        let insert = probe.stats();
        assert_eq!(insert.searches, if reference { ROWS } else { 0 });
        assert_eq!(insert.end_misses, if reference { 0 } else { ROWS });
        for i in (0..ROWS).rev() {
            assert!(tx.set_raw(&i.to_be_bytes(), vec![2]));
        }
        let update = probe.stats();
        assert_eq!(update.searches - insert.searches, ROWS);
        assert_eq!(update.bound_grows, if reference { 0 } else { 1 });
        drop(probe);
        assert_eq!(tx.get(&0u64.to_be_bytes()), Some(vec![2]));
        tx.rollback();
        assert_eq!(db.health().total_rows, 0);
    }
}

/// ② 全库折叠：不该再逐行克隆 value。
///
/// 计数挂在重量路径 `MemStore::iter` 上（它每行克隆一次 value），所以将来谁把折叠
/// 改回 `iter()`，这条会当场红。内存引擎没有磁盘读也没有 WAL，② 在这里的
/// 代价单位就是 value 克隆 —— 别的引擎用读盘/页/块来量，这里只能用克隆来量。
#[test]
fn test_collect_latest_does_not_clone_values() {
    const KEYS: u64 = 8;
    const VERSIONS: u64 = 120;

    let mvcc = MVCC::new();
    for v in 0..VERSIONS {
        for k in 0..KEYS {
            let key = format!("k{k}");
            let tx = mvcc.begin_transaction();
            assert!(tx.set(key.as_bytes(), format!("v{v}").into_bytes()));
            tx.commit();
        }
    }

    let tx = mvcc.begin_transaction();
    let base = crate::memory::mem_store::value_clones();
    let recs = tx.export_latest_visible(false);
    let used = crate::memory::mem_store::value_clones() - base;
    eprintln!(
        "折叠期 value 克隆次数：{used}（存活 key {KEYS}，历史行数 {}）",
        KEYS * VERSIONS
    );
    assert_eq!(recs.len() as u64, KEYS, "存活 key 数应等于 {KEYS}");
    assert_eq!(
        used,
        0,
        "折叠只该比键，不该逐行克隆 value：实际 {used} 次，历史行数 {}",
        KEYS * VERSIONS
    );
    tx.rollback();
}

/// 窗口 scan 的代价必须随**窗口宽度**缩 —— 内存引擎的尺子是「折叠走了多少把键」。
///
/// 旧形状：`collect_latest_raw()` 折完**全库**再由调用方逐条筛界 ⇒ 10 行的窗口也要走
/// 2000 把键（大库基线实测：窗口 scan 106.3 µs/行 vs 全量 1.1 µs/行）。
/// 这条同时钉正确性：窗口结果必须与"全量之后筛"逐条一致，不能是筛错了才快。
/// `scan_foreach` 提前叫停时，**不许先把整库折一遍**（M27）。
///
/// 这条路径的文档从写下那天起就声称「不收集全库到单一大 Vec」，而实现是
/// `for … in collect_latest_raw_in(...)` —— 那一句会把界内每一行都先折出来，再逐条搬成
/// `ExportRecord`。200k 键实测因此比结果集本身还多占 ≈ 15 MB（`一次全量 scan`
/// 峰值 +45.5 MB / 常驻 30.7 MB）。现在走共用件 `fold_rows_streaming`：折一行交一行，
/// 叫停之后的键不再碰。
///
/// 内存引擎没有「取值」这一步（行就在手里），所以能数的是**折叠走过的键数**；
/// 判据同 LSM：按比例给，老写法是比例 1.0。
#[test]
fn test_scan_foreach_stops_without_folding_the_library() {
    const N: usize = 3 * 4096 + 100;
    fn ruler() -> u64 {
        crate::memory::memory_mvcc::fold_keys()
    }
    {
        let mvcc = MVCC::new();
        for base in (0..N).step_by(2000) {
            let end = (base + 2000).min(N);
            let items: Vec<(Vec<u8>, Vec<u8>)> = (base..end)
                .map(|i| {
                    (
                        format!("k{i:05}").into_bytes(),
                        format!("v{i}").into_bytes(),
                    )
                })
                .collect();
            let tx = mvcc.begin_transaction();
            if let Err(bad) = tx.batch_set(&items) {
                panic!("灌第 {base} 批: {}", String::from_utf8_lossy(&bad));
            }
            tx.commit();
        }
        let tx = mvcc.begin_transaction();
        let base = ruler();
        let mut handed = 0usize;
        tx.scan_foreach(None, None, |rec| {
            assert!(rec.value.is_some(), "交出来的行没有值");
            handed += 1;
            false // 只要第一行
        });
        let early = ruler() - base;
        let base = ruler();
        let all = tx.scan(None, None);
        let full = ruler() - base;
        tx.commit();

        assert_eq!(handed, 1, "回调明明叫停了");
        assert_eq!(all.len(), N, "参照量：全量 scan 要折出所有行");
        assert!(
            early * 2 <= full,
            "只要 1 行就走了 {early} 把键，全量也才 {full} ⇒ 没提前收手"
        );
        eprintln!("[scan_foreach 早停] 交 1 行付 {early}，全量 {N} 行付 {full}");
    }
}

#[test]
fn test_windowed_scan_cost_scales_with_window_not_library() {
    const KEYS: u64 = 2000;
    let mvcc = MVCC::new();
    for i in 0..KEYS {
        let tx = mvcc.begin_transaction();
        assert!(tx.set(format!("k{i:05}").as_bytes(), format!("v{i}").into_bytes()));
        tx.commit();
    }

    let tx = mvcc.begin_transaction();
    let before = fold_keys();
    let all = tx.scan(None, None);
    let full = fold_keys() - before;

    let lo = b"k01000".to_vec();
    let hi = b"k01010".to_vec();
    let before = fold_keys();
    let win = tx.scan(Some(&lo), Some(&hi));
    let windowed = fold_keys() - before;

    assert_eq!(all.len() as u64, KEYS, "全量应折出 {KEYS} 行");
    assert_eq!(win.len(), 10, "[k01000, k01010) 应给 10 行");
    let want: Vec<Vec<u8>> = all
        .iter()
        .filter(|r| r.key >= lo && r.key < hi)
        .map(|r| r.key.clone())
        .collect();
    let got: Vec<Vec<u8>> = win.iter().map(|r| r.key.clone()).collect();
    assert_eq!(got, want, "下推界之后窗口结果必须与全量筛一致");
    assert!(
        windowed <= 20,
        "窗口只该走窗口内的键（实际 {windowed}，全量 {full}）"
    );
    assert!(
        windowed * 10 <= full,
        "窗口折叠不该是全库量级（{windowed} vs {full}）"
    );

    // 前缀同理：k001xx ⇒ 100 个键
    let before = fold_keys();
    let pre = tx.prefix_scan(b"k001");
    let prefixed = fold_keys() - before;
    assert_eq!(pre.len(), 100, "前缀 k001 应命中 100 行");
    // 实测 200 而不是 100：哨兵是 `end ‖ 0xFF×8`，于是"以 end 为前缀的那一组键"
    // （这里是 k002xx，raw ≥ end）也会被 range 带进来，再由 `raw ≥ end ⇒ continue` 滤掉。
    // 代价合同因此是「窗口宽度 + 至多一个同前缀组」——宁可多走，不可误删
    // （`seek_prev` 的界是 `key ‖ 0x00`，收紧哨兵就会误删，见 version_key_upper_bound 的文档）。
    assert!(
        prefixed <= 250,
        "前缀扫该只走前缀覆盖的键 + 至多一个同前缀组（实际 {prefixed}，全量 {full}）"
    );
    assert!(
        prefixed * 4 <= full,
        "前缀扫的代价不该是全库量级（{prefixed} vs {full}）"
    );

    // 边界形状：前缀是**所有键的真前缀**（比第一个键还短）。lsm 曾把这种扫法做成空结果，
    // 四家都钉一遍。
    let whole = tx.prefix_scan(b"k");
    assert_eq!(whole.len() as u64, KEYS, "前缀 k 应命中全部 {KEYS} 行");
    assert_eq!(tx.key_count(b"k") as u64, KEYS, "key_count 同一个形状");

    // 上界哨兵的陷阱形状：`seek_prev` 的界是 `key ‖ 0x00`，用 `encode_key(end, 0)`
    // 当哨兵会把 `key` 自己的版本行判成越界（这条用例最初就是因为这个而红）。
    assert_eq!(tx.seek_prev(b"k01005").unwrap().key, b"k01005");
    assert_eq!(tx.seek(b"k01000").unwrap().key, b"k01000");
}
