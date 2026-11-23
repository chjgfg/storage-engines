// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn checkpoint_probe_counts_real_hint_work_and_obeys_gate() {
    use crate::bitcask::checkpoint_probe::{Scope, Stage, Timer};
    assert!(Timer::start(Stage::Commit).is_none());
    let dir = tmp_db("checkpoint_probe");
    let db = MVCC::try_open(&dir).unwrap();
    db.set_wal_checkpoint_limit(1);
    let scope = Scope::start();
    for i in 0u64..2 {
        let tx = db.begin_transaction();
        tx.try_set_raw(&i.to_be_bytes(), vec![0; 16]).unwrap();
        tx.try_commit().unwrap();
    }
    let stats = scope.stats();
    assert_eq!(stats.checkpoints, 2);
    assert_eq!(stats.commit_ns.len(), 2);
    assert_eq!(
        stats.hint_rows, 3,
        "full index rewritten at each checkpoint"
    );
    assert_eq!(stats.hint_bytes, 2 * 40 + 3 * (24 + 16));
    assert_eq!(
        std::fs::metadata(hint_path(&dir)).unwrap().len(),
        40 + 2 * (24 + 16)
    );
    assert!(stats.checkpoint_ns >= stats.flush_ns + stats.hint_ns + stats.wal_ns);
    drop(scope);
    assert!(Timer::start(Stage::Commit).is_none());
    let scope = Scope::start();
    db.set_wal_checkpoint_limit(0);
    let tx = db.begin_transaction();
    tx.try_set_raw(b"control", vec![1]).unwrap();
    tx.try_commit().unwrap();
    assert_eq!(scope.stats().checkpoints, 0);
    assert_eq!(scope.stats().hint_rows, 0);
}

/// 全库类 API（`print_all` / 扫描 / 搜索 / `exists` / `export` / `purge_expired` 都走
/// `collect_latest_raw`）的读盘次数必须只与**存活 key 数**相关，与历史行数无关。
///
/// 旧实现把每一行连 value 全读出来再挑最后一个，所以这里刻意让「历史行数」远大于
/// 「存活 key 数」，这样断言才有区分力（旧实现会读 KEYS×VERSIONS 次）。
#[test]
fn test_collect_latest_reads_scale_with_live_keys_not_rows() {
    const KEYS: u64 = 8;
    const VERSIONS: u64 = 120;

    let path = tmp_db("collect_cost");
    {
        let mvcc = MVCC::open(&path);
        for v in 0..VERSIONS {
            for k in 0..KEYS {
                let key = format!("k{k}");
                let tx = mvcc.begin_transaction();
                assert!(tx.set(key.as_bytes(), format!("v{v}").into_bytes()));
                tx.commit();
            }
        }

        let tx = mvcc.begin_transaction();
        let before = crate::bitcask::bitcask::read_value_calls();
        let all = tx.print_all();
        let used = crate::bitcask::bitcask::read_value_calls() - before;
        assert_eq!(all.len() as u64, KEYS, "存活 key 数应等于 {KEYS}");
        assert!(
            used <= KEYS + 4,
            "只应按存活 key 读盘：实际读 {} 次，而历史行数是 {}",
            used,
            KEYS * VERSIONS
        );
        tx.rollback();

        // 参照：旧实现走的就是「全行物化」（`Bitcask::iter` 每行连 value 读出来再挑），
        // 这条原语现在还在，所以「改前」是实测而不是推断。
        let before = crate::bitcask::bitcask::read_value_calls();
        let rows = mvcc.kv.lock().unwrap().iter().unwrap().len() as u64;
        let old_style = crate::bitcask::bitcask::read_value_calls() - before;
        eprintln!(
            "全库折叠读盘：新实现 {used} 次 vs 旧路径（全行物化）{old_style} 次，历史行数 {rows}"
        );
        assert!(
            used * 10 <= old_style,
            "折叠读盘应比全行物化便宜一个量级以上（{used} vs {old_style}）"
        );
    }
    cleanup(&path);
}

/// **窗口 scan 的代价必须随窗口宽度缩，不随全库缩。**
///
/// 旧形状是 `collect_latest_raw()` 折完**全库**再由调用方逐条筛界 ⇒ 10 行的窗口和
/// 2000 行的全量读盘次数一样（大库基线实测：窗口 scan 376.6 µs/行 vs 全量 3.8 µs/行）。
/// 现在界下推进折叠，这条用例同时钉两件事：① 结果与"全量之后筛"逐条一致（界判得对），
/// ② 读盘次数随窗口缩（不是又对又慢，也不是快了但筛错）。
/// `scan_foreach` 提前叫停时，**不许先把整库折一遍**（M27）。
///
/// 这条路径的文档从写下那天起就声称「不收集全库到单一大 Vec」，而实现是
/// `for … in collect_latest_raw_in(...)` —— 那一句会把界内每一行都先折出来，再逐条搬成
/// `ExportRecord`。200k 键实测因此比结果集本身还多占 ≈ 15 MB（`一次全量 scan`
/// 峰值 +45.5 MB / 常驻 30.7 MB）。现在走共用件 `fold_rows_streaming`：折一行交一行，
/// 叫停之后的键不再碰。
///
/// 判据用**取值次数**（本引擎那把尺子）：交 1 行只该取 1 次值，老写法是 N 次 ——
/// 差三个数量级，不必按各家单位调预算。
#[test]
fn test_scan_foreach_stops_without_folding_the_library() {
    const N: usize = 2000;
    fn ruler() -> u64 {
        crate::bitcask::bitcask::read_value_calls()
    }
    let path = tmp_db("foreach_stop");
    {
        let mvcc = MVCC::open(&path);
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
        assert!(full >= N as u64, "取值尺子在空转（全量只取 {full} 次）");
        assert!(
            early <= 2,
            "只要 1 行却取了 {early} 次值（老写法：先把全库 {N} 行折出来）"
        );
        eprintln!("[scan_foreach 早停] 交 1 行付 {early}，全量 {N} 行付 {full}");
    }
    cleanup(&path);
}

#[test]
fn test_windowed_scan_cost_scales_with_window_not_library() {
    const KEYS: u64 = 2000;

    let path = tmp_db("window_cost");
    {
        let mvcc = MVCC::open(&path);
        for i in 0..KEYS {
            let tx = mvcc.begin_transaction();
            assert!(tx.set(format!("k{i:05}").as_bytes(), format!("v{i}").into_bytes()));
            tx.commit();
        }

        let tx = mvcc.begin_transaction();
        let before = crate::bitcask::bitcask::read_value_calls();
        let all = tx.scan(None, None);
        let full = crate::bitcask::bitcask::read_value_calls() - before;

        let lo = b"k01000".to_vec();
        let hi = b"k01010".to_vec();
        let before = crate::bitcask::bitcask::read_value_calls();
        let win = tx.scan(Some(&lo), Some(&hi));
        let windowed = crate::bitcask::bitcask::read_value_calls() - before;

        assert_eq!(all.len() as u64, KEYS, "全量应折出 {KEYS} 行");
        assert_eq!(win.len(), 10, "[k01000, k01010) 应给 10 行");
        // 界判对了才算数：与"全量之后筛"逐条一致
        let want: Vec<Vec<u8>> = all
            .iter()
            .filter(|r| r.key.as_slice() >= lo.as_slice() && r.key.as_slice() < hi.as_slice())
            .map(|r| r.key.clone())
            .collect();
        let got: Vec<Vec<u8>> = win.iter().map(|r| r.key.clone()).collect();
        assert_eq!(got, want, "下推界之后窗口结果必须与全量筛一致");

        // 代价：窗口只有全库的 0.5%
        assert!(
            windowed <= 20,
            "10 行窗口的读盘次数应随窗口缩（实际 {windowed}，全量 {full}）"
        );
        assert!(
            windowed * 10 <= full,
            "窗口代价不该是全库量级（{windowed} vs 全量 {full}）"
        );

        // 前缀同理（twosync 那种"按路径前缀取一段"的读法）：k001xx ⇒ 100 个键
        let before = crate::bitcask::bitcask::read_value_calls();
        let pre = tx.prefix_scan(b"k001");
        let prefixed = crate::bitcask::bitcask::read_value_calls() - before;
        assert_eq!(pre.len(), 100, "前缀 k001 应命中 100 行");
        assert!(
            prefixed <= 120,
            "前缀扫的读盘应随前缀命中等同宽度（实际 {prefixed}，全量 {full}）"
        );

        // 边界形状：前缀是**所有键的真前缀**（比第一个键还短 ⇒ seek 落不到任何"键 ≥ from"
        // 的位置）。lsm 在复测表里把这种前缀扫成空结果，四家都钉一遍。
        let whole = tx.prefix_scan(b"k");
        assert_eq!(whole.len() as u64, KEYS, "前缀 k 应命中全部 {KEYS} 行");
        assert_eq!(tx.key_count(b"k") as u64, KEYS, "key_count 同一个形状");

        // 上界哨兵的陷阱形状：`seek_prev` 造出来的界是 `key ‖ 0x00`，若哨兵取
        // `encode_key(end, 0)` 就会把 `key` 自己的版本行判成越界 —— 表现为
        // `seek_prev(k)` 返回比 k 小的那一版。内存引擎先撞到，四家现在共用一个哨兵函数。
        assert_eq!(tx.seek_prev(b"k01005").unwrap().key, b"k01005");
        assert_eq!(tx.seek(b"k01000").unwrap().key, b"k01000");
        assert_eq!(tx.seek_prev(b"k01000").unwrap().key, b"k01000");
        tx.rollback();
    }
    cleanup(&path);
}

/// 冷启动三级优先：WAL 里有 Checkpoint 时**不该**再做 O(总行数) 的全库 KeyDir 扫描。
///
/// 守的是「启动一次比一次慢」那条通道：`scan_max_version` 随总行数线性，而
/// `checkpoint()` 早已把 `next_version` 写进 WAL（且 `try_open` 收尾也会补一条），
/// 之前却完全没读它。这里用调用计数证明快路真的走到了，而不只是代码看起来对。
#[test]
fn test_reopen_uses_checkpoint_instead_of_full_scan() {
    const KEYS: u32 = 60;
    let path = tmp_db("ckpt_start_ver");
    let before;
    {
        let mvcc = MVCC::open(&path);
        mvcc.set_wal_checkpoint_limit(0); // 关掉自动触发，本用例只测手动 checkpoint
        for i in 0..KEYS {
            let key = format!("k{i}");
            let tx = mvcc.begin_transaction();
            assert!(tx.set(key.as_bytes(), format!("v{i}").into_bytes()));
            tx.commit();
        }
        before = mvcc.next_version.load(Ordering::SeqCst);
        mvcc.checkpoint();
    }

    {
        let base = scan_max_calls();
        let mvcc = MVCC::open(&path);
        assert_eq!(
            scan_max_calls() - base,
            0,
            "有 Checkpoint 时重开不应再全库扫 KeyDir"
        );
        let after = mvcc.next_version.load(Ordering::SeqCst);
        assert!(
            after >= before,
            "重开后 next_version 不应回退（{after} < {before}）"
        );
        let tx = mvcc.begin_transaction();
        for i in 0..KEYS {
            let key = format!("k{i}");
            assert_eq!(tx.get(key.as_bytes()), Some(format!("v{i}").into_bytes()));
        }
        tx.commit();
    }
    cleanup(&path);
}
