// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// 崩在 `merge()` 改名之前的现场，必须能在下次 open 时被认回来。
///
/// 手工布置现场而不是真去崩进程：`data.log` 已经不存在、全部存活记录只在 `data.merge` 里。
/// 没有恢复的话 open 会**新建一个空 data.log**（`Log::new` 就是 create-if-missing），
/// 于是得到一个"打开成功、一条都没有"的空库 —— 这类失败不报错，只能靠断言钉。
#[test]
fn test_open_recovers_merge_left_behind_by_crashed_merge() {
    let path = tmp_log("merge_crash");
    {
        let mut eng = Bitcask::new(path.to_path_buf()).unwrap();
        for i in 0..20u8 {
            eng.set(format!("k{i}").as_bytes(), vec![i; 32]).unwrap();
        }
        eng.flush().unwrap();
        eng.dump_hint().unwrap();
    }
    // 布置现场：merge 产物写好、旧 data.log 已删、改名还没跑
    let merge = path.with_extension("merge");
    std::fs::rename(&path, &merge).unwrap();
    assert!(!path.exists() && merge.exists());

    {
        let mut eng = Bitcask::new(path.to_path_buf()).unwrap();
        for i in 0..20u8 {
            assert_eq!(
                eng.get(format!("k{i}").as_bytes()).unwrap(),
                Some(vec![i; 32]),
                "k{i} 本该在 merge 产物里 —— open 把它当空库开了"
            );
        }
    }
    cleanup(&path);
}

/// 两边都在的现场：留下 `data.log`，把 merge 残骸删掉。
///
/// 删残骸不是洁癖：下次 `merge()` 会以同一个名字打开那个残骸**往后追加**，
/// 两份不同布局的记录混进一个文件 —— 那才是真救不回来。
#[test]
fn test_open_discards_merge_debris_when_log_survives() {
    let path = tmp_log("merge_debris");
    {
        let mut eng = Bitcask::new(path.to_path_buf()).unwrap();
        eng.set(b"a", b"1".to_vec()).unwrap();
        eng.flush().unwrap();
    }
    let merge = path.with_extension("merge");
    std::fs::write(&merge, b"stale debris").unwrap();

    {
        let mut eng = Bitcask::new(path.to_path_buf()).unwrap();
        assert_eq!(eng.get(b"a").unwrap(), Some(b"1".to_vec()));
    }
    assert!(
        !merge.exists(),
        "data.log 还在 ⇒ 残骸该被清掉，别留给下次 merge 追加"
    );
    cleanup(&path);
}

#[test]
fn test_merge_writes_hint() {
    let path = tmp_log("merge");
    {
        let mut eng = Bitcask::new(path.to_path_buf()).unwrap();
        eng.set(b"a", b"1".to_vec()).unwrap();
        eng.set(b"a", b"2".to_vec()).unwrap();
        eng.set(b"b", b"3".to_vec()).unwrap();
        eng.delete(b"b").unwrap();
        eng.merge().unwrap();
        assert!(eng.hint_path().exists());
        assert_eq!(eng.get(b"a").unwrap(), Some(b"2".to_vec()));
        assert_eq!(eng.get(b"b").unwrap(), None);
    }
    {
        let mut eng = Bitcask::new(path.to_path_buf()).unwrap();
        assert_eq!(eng.get(b"a").unwrap(), Some(b"2".to_vec()));
        assert_eq!(eng.len(), 1);
    }
    cleanup(&path);
}

/// `merge()` 走 KeyDir 必须**按块**，不许一次把整库的 `(键, 位置)` 抄进一个 `Vec`（M40）。
///
/// 判据（单位：把键）：库里活键数远多于一块时，单块抄出的最大键数 `merge_chunk_max()`
/// 必须 ≤ `2 × MERGE_CHUNK_KEYS`。改前那份 `snapshot: Vec<(k.clone(), loc.clone())>`
/// 在 600k 键 × 2 版那档分段实测 **115.0 MB**（100 B/扫描行），是 `vacuum()` 现在唯一的
/// 大头；块化之后它变成 O(一块)（剩下的 `new_key_dir` 是替换用的新索引，不是瞬时账）。
/// 尺子为什么是"单块抄出多少把键"而不是内存：与四家 vacuum 的 `vac_pending_max` 同一条理由
/// —— `mem_probe` 的高水位是进程级的，并行用例互相污染。
///
/// **修订（M43）**：括号里那句"新索引不是瞬时账"当时是对的，但它把剩下的账留在了
/// `new_key_dir` 上 —— 那份表在走查全程和旧索引**同时活着**，600k 档分段 ④ 实测 +87.9 MB。
/// M43 已经不再抄它（索引改成重扫新日志得到），这条判据本身不变。
///
/// 变异验证：把 `MERGE_CHUNK_KEYS` 临时抬到一整库那么大 ⇒ 单块抄出 20 000 把 > 界 16 384，红。
#[test]
fn test_merge_walks_key_dir_in_bounded_chunks() {
    const KEYS: usize = 20_000;
    let path = tmp_log("merge_chunks");
    {
        let mut eng = Bitcask::new(path.to_path_buf()).unwrap();
        for i in 0..KEYS {
            let k = format!("k{i:06}");
            eng.set(k.as_bytes(), vec![b'v'; 16]).unwrap();
        }
        assert_eq!(eng.len(), KEYS, "灌完之后活键数不对 ⇒ 形状没建起来");
        reset_merge_chunk();
        eng.merge().unwrap();
        let max_chunk = merge_chunk_max();
        eprintln!(
            "[merge chunks] {} 把活键 ⇒ 单块最多抄 {} 把（块界 {MERGE_CHUNK_KEYS}）",
            eng.len(),
            max_chunk
        );
        assert!(
            KEYS > MERGE_CHUNK_KEYS * 2,
            "这条判据要求库比两块还大（否则界不会被跨过）"
        );
        assert!(max_chunk > 0, "这把尺子没数到块 ⇒ 空转");
        assert!(
            max_chunk <= 2 * MERGE_CHUNK_KEYS as u64,
            "单块抄出 {max_chunk} 把键（界 {}）⇒ 又变回整库一份快照",
            2 * MERGE_CHUNK_KEYS as u64
        );
        // 分块走之后数据必须还是完整的（顺序、条数、值）
        assert_eq!(eng.len(), KEYS, "merge 之后活键数变了");
        for i in (0..KEYS).step_by(997) {
            let k = format!("k{i:06}");
            assert_eq!(
                eng.get(k.as_bytes()).unwrap(),
                Some(vec![b'v'; 16]),
                "第 {i} 把键在分块 merge 之后读不回来"
            );
        }
    }
    {
        let eng = Bitcask::new(path.to_path_buf()).unwrap();
        assert_eq!(
            eng.len(),
            KEYS,
            "重开之后活键数不对 ⇒ 分块 merge 写坏了日志"
        );
    }
    cleanup(&path);
}

/// merge 之后那把索引必须是**重扫新日志**扫出来的，不是走查时一路抄出来的（M43）。
///
/// 为什么钉"重扫"这个动作、不钉"全场只有一份索引"：`KeyDir` 只是 `BTreeMap` 的别名，
/// 要数"堆上同时活着几把索引"就得把它包成 newtype —— 那是破坏性 API 改动。
/// "这次 merge 折进索引多少行"是同一件事的可观测面，而且回归成 M40 及之前那份
/// `new_key_dir`（边抄边记位置、根本不重扫）时它直接掉回 **0**。
///
/// 后半段钉的是**先腾掉旧索引**这半步：旧索引里那些 `pos` 说的是已经不存在的位置。
/// 如果跳过 `key_dir = KeyDir::new()` 直接在旧表上重扫，被硬删过的键不会从索引里消失，
/// 而它的 `file_id` 恰好等于新 active 的 0 ⇒ 读它会读到**别的键的字节**。这正是
/// [`Bitcask::usable`] 要拦的那一类静默错值。
#[test]
fn test_merge_rebuilds_index_by_rescanning_the_log() {
    const KEYS: usize = 20_000;
    let path = tmp_log("merge_rescan");
    {
        let mut eng = Bitcask::new(path.to_path_buf()).unwrap();
        for i in 0..KEYS {
            let k = format!("k{i:06}");
            eng.set(k.as_bytes(), vec![b'v'; 16]).unwrap();
            // 第二版：让日志里有一半以上是可回收的行，merge 才有东西要重写
            eng.set(k.as_bytes(), vec![b'w'; 24]).unwrap();
        }
        // 硬删掉一部分 —— 这些键 merge 之后必须彻底不在索引里
        for i in (0..KEYS).step_by(7) {
            eng.delete(format!("k{i:06}").as_bytes()).unwrap();
        }
        let live_before = eng.len();
        arm_merge_rescan_counter();
        eng.merge().unwrap();
        let rows = merge_rescan_rows();
        eprintln!("[merge rescan] 活键 {live_before} 把 ⇒ 这次 merge 重扫折进索引 {rows} 行");
        assert!(
            rows > 0,
            "重扫记账一次都没响 ⇒ 索引不是扫出来的（又抄回第二把了？）"
        );
        assert_eq!(
            rows,
            eng.len() as u64,
            "重扫 {rows} 行 ≠ merge 之后索引里 {live_before} 条 ⇒ 索引不来自这次重扫"
        );
        assert_eq!(eng.len(), live_before, "merge 之后活键数变了");

        for i in (0..KEYS).step_by(7) {
            let k = format!("k{i:06}");
            assert_eq!(
                eng.get(k.as_bytes()).unwrap(),
                None,
                "{k} 在 merge 之前就被硬删，之后还读得到 ⇒ 旧索引没腾掉，读到的是错位的数据"
            );
        }
        let mut checked = 0usize;
        for i in 0..KEYS {
            if i % 7 == 0 {
                continue;
            }
            let k = format!("k{i:06}");
            assert_eq!(
                eng.get(k.as_bytes()).unwrap(),
                Some(vec![b'w'; 24]),
                "第 {i} 把存活键在重扫之后读回来的不是最新版本"
            );
            checked += 1;
            if checked >= 32 {
                break;
            }
        }
    }
    {
        let mut eng = Bitcask::new(path.to_path_buf()).unwrap();
        assert_eq!(eng.len(), KEYS - KEYS.div_ceil(7), "重开之后活键数不对");
        assert_eq!(
            eng.get(b"k000000").unwrap(),
            None,
            "重开之后硬删过的键复活了 ⇒ merge 写坏了日志"
        );
        assert_eq!(
            eng.get(b"k000001").unwrap(),
            Some(vec![b'w'; 24]),
            "重开之后存活键读不回来"
        );
    }
    cleanup(&path);
}

/// M60：merge 之后重建索引必须走"批量成树"那一扇门（满节点），而且**不许**退回逐条施加。
///
/// 判据为什么钉"门走没走 + 有没有退回"，不钉"常驻少了 30 MiB"：后者要跑大库才能读出来
/// （release 台架 `bitcask_vacuum_retained_two_passes` 量的就是它），CI 里跑不动；
/// 而**形状账只在门里才成立** ⇒ 门是它的充分条件，钉门就够，代价归代价那一段去量。
///
/// ⚠️ 对照臂（`MERGE_BULK_OLD_SHAPE=1` 重跑这一条）必须**红** —— 第一版守卫如果只断言"键还在"，
/// 那退回老路也照样绿，等于没有守卫（M43 那一轮同一课）。
#[test]
fn merge_rebuild_goes_through_the_bulk_door() {
    const KEYS: usize = 4_000;
    let path = tmp_log("merge_bulk_door");
    {
        let mut eng = Bitcask::new(path.to_path_buf()).unwrap();
        for i in 0..KEYS {
            eng.set(format!("k{i:06}").as_bytes(), vec![b'v'; 16])
                .unwrap();
        }
        eng.merge().unwrap();
        let bulk = merge_bulk_rebuilds();
        let fallbacks = merge_bulk_fallbacks();
        eprintln!("[bulk door] 走批量成树 {bulk} 次 · 退回逐条 {fallbacks} 次");
        assert_eq!(
            bulk, 1,
            "merge 之后重建索引没走批量成树那一扇门 ⇒ M60 这一刀根本没生效（或对照臂开着）"
        );
        assert_eq!(
            fallbacks, 0,
            "merge 的产物里居然有硬删除 ⇒ 批量那一侧的升序前提被破坏，退回说明写入形状变了"
        );
        assert_eq!(eng.len(), KEYS, "merge 之后活键数变了");
    }
    {
        // 重开一遍核对：批量建出来的索引落进 hint、再读回来，必须与内存里那一把一致
        let mut eng = Bitcask::new(path.to_path_buf()).unwrap();
        assert_eq!(eng.len(), KEYS, "重开之后活键数不对");
        assert_eq!(
            eng.get(b"k000001").unwrap(),
            Some(vec![b'v'; 16]),
            "重开之后存活键读不回来"
        );
    }
    cleanup(&path);
}
