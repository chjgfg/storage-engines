// 该文件集中验证对应职责，保留原测试名称和字面值。

#[test]
fn all_engines_support_batch_and_scan() {
    let engines = open_all();
    let mut dirs = Vec::new();
    for (name, db, dir) in engines {
        let tx = db.begin();
        tx.batch_set(&[
            (b"k1".to_vec(), b"v1".to_vec()),
            (b"k2".to_vec(), b"v2".to_vec()),
            (b"k3".to_vec(), b"v3".to_vec()),
        ])
        .expect("batch_set");

        let got = tx.multi_get(&[b"k1".to_vec(), b"k9".to_vec()]);
        assert_eq!(got, vec![Some(b"v1".to_vec()), None], "{name}: multi_get");

        let scanned = tx.scan(Some(b"k1"), Some(b"k3"));
        let keys: Vec<_> = scanned.iter().map(|r| r.key.clone()).collect();
        assert_eq!(keys, vec![b"k1".to_vec(), b"k2".to_vec()], "{name}: scan");

        let pre = tx.prefix_scan(b"k");
        assert_eq!(pre.len(), 3, "{name}: prefix_scan");

        tx.commit();
        dirs.push(dir);
    }
    cleanup(dirs);
}

/// M20 守卫：一次 `seek_prev` 的代价 = "往回跳过几把死键"，**不是** "库里有多少把键"。
///
/// 改之前四家各写一份 `scan(None, Some(key‖0))` + `retain` + `pop`：为了拿"≤key 的最后一条
/// 活记录"，把整个前缀物化出来（200k 行实测单次 140~610 ms，还带 O(库) 的瞬时内存）。
/// 现在循环只有一份 [`crate::common::seek_prev_with`]，每家只给两个口子：反向一步 + 单键可见行。
///
/// 三条断言各抓一种**互不重叠**的退化，顺序是刻意的（②③在前、①在后：三种退化各由第一条撞上
/// 它的断言报出来，实测都能撞红，下面就是当时留下的数字）：
/// ② **步数**：共用内核的探针 `seek_prev_steps()`，断的是**精确形状** `DEAD + 1` = 9：死串每把
///    一步、答案一步。每把死键都带 5 个版本 ⇒ 按「组」跳才是 9 步。把 memory 的反向一步改成
///    交回编码键（忘记剥版本后缀）实测报 2 步 ⇒ 撞红。
/// ③ **本引擎的尺子**：抓"这一次调用又把前缀折了一遍"。老写法（不调内核 ⇒ ②读到 0）实测 ——
///    bitcask `keydir_visits` 2017 / lsm `light_visits` 2017 / memory `fold_keys` 2017 /
///    bplus `page_reads` 63；新写法 0（memory 是 `range_back` 取回的 35 行）。⚠️ 预算必须按各家
///    **单位**给：bplus 的 2000 把键只摊在 63 片叶子上，拿"键数"当预算抓不到它 ⇒ 按"每步 4 页"给。
/// ① **等价**：与"从 probe 往回逐把点读、第一把读得通的就是答案"这个语义参考比对，还要求答案
///    正是死串下面那把活键。只测快不测对，就是拿错数据换性能：把共用判定里的过期检查摘掉，
///    ②③全绿而这条报红（把已过期的那把交了出去）。
///    ⚠️ ②③ 数的是**内核圈的步数**与**折叠/取回的量**，看不见各家反向原语内部走了多少把键；
///    那层有界性靠 `take(1)` 与 bplus 的跨叶进度守卫，另有各自的测试。
#[test]
fn seek_prev_costs_a_walk_not_the_library() {
    const N: usize = 2000;
    const DEAD: usize = 8;
    const CHAIN: usize = 5;
    let dead_start = N - DEAD;
    let answer = format!("k{:05}", dead_start - 1);
    let probe = format!("k{:05}", N - 1);

    for entry in open_all() {
        // 整条 `entry` 不拆开：元组字段按 0→1→2 析构 ⇒ 引擎先落地、scratch 守卫后删目录。
        // 拆成 `for (name, db, dir)` 会按反向声明序走、守卫先跑 —— 而 Windows 上引擎还开着的
        // `.log`/`.lock` 那时删不掉，目录就白留在 `%TEMP%` 里（M82）。
        let (name, db) = (entry.0, &*entry.1);
        {
            let tx = db.begin();
            for i in 0..N {
                assert!(
                    tx.set(format!("k{i:05}").as_bytes(), b"v".to_vec()),
                    "{name}: 灌第 {i} 把"
                );
            }
            tx.commit();
        }
        // 顶上 DEAD 把（含答案键）各再写几份 ⇒ 每把都有 CHAIN 个版本：
        // 反向一步必须按"组"跳，一步只该跨过一把键的全部版本
        {
            let tx = db.begin();
            for i in dead_start..N {
                for _ in 1..CHAIN {
                    tx.set(format!("k{i:05}").as_bytes(), b"v".to_vec());
                }
            }
            for _ in 1..CHAIN {
                tx.set(answer.as_bytes(), b"v".to_vec());
            }
            tx.commit();
        }
        // 前 DEAD-1 把删除，最后一把写成"入库时就已经过期"
        // （`set` 收的是已打信封的逻辑值，`pack_ttl(…, 1)` = 1970 年到期）
        {
            let tx = db.begin();
            for i in dead_start..N - 1 {
                tx.delete(format!("k{i:05}").as_bytes());
            }
            assert!(
                tx.set(
                    format!("k{:05}", N - 1).as_bytes(),
                    crate::common::codec::pack_ttl(b"v", 1),
                ),
                "{name}: 写过期键"
            );
            tx.commit();
        }

        // 参考值只用点读算：从 probe 往回逐把 `get`，第一把读得通的就是"≤probe 的最大活键"。
        // 单独开一个事务，是为了不让这 9 次点读混进下面要量的尺子里。
        let mut want: Option<Vec<u8>> = None;
        {
            let tx = db.begin();
            for i in (0..N).rev() {
                let k = format!("k{i:05}");
                if tx.get(k.as_bytes()).is_some() {
                    want = Some(k.into_bytes());
                    break;
                }
            }
            tx.commit();
        }

        // 先量再断：退化时那一步的形状要在打印里看得见，别让它被前面一条断言挡掉
        let s0 = crate::common::store::seek_prev_steps();
        let m_rows = crate::memory::mem_store::rows_touched();
        let m_fold = crate::memory::memory_mvcc::fold_keys();
        let bc = crate::bitcask::bitcask::keydir_visits();
        let lv = crate::lsm_tree::sst::light_visits();
        let br = crate::lsm_tree::sst::block_reads();
        let pr = crate::bplus_tree::page::page_reads();
        let ph = crate::bplus_tree::page::cache_hits();
        let tx = db.begin();
        let hit = tx.seek_prev(probe.as_bytes());
        tx.commit();
        let steps = crate::common::store::seek_prev_steps() - s0;
        let d_rows = crate::memory::mem_store::rows_touched() - m_rows;
        let d_fold = crate::memory::memory_mvcc::fold_keys() - m_fold;
        let d_bc = crate::bitcask::bitcask::keydir_visits() - bc;
        let d_lv = crate::lsm_tree::sst::light_visits() - lv;
        let d_br = crate::lsm_tree::sst::block_reads() - br;
        let d_pr = crate::bplus_tree::page::page_reads() - pr;
        let d_ph = crate::bplus_tree::page::cache_hits() - ph;
        let ruler = match name {
            "memory" => d_rows.max(d_fold),
            "bitcask" => d_bc,
            "lsm_tree" => d_lv.max(d_br),
            _ => d_pr.max(d_ph),
        };
        eprintln!(
            "[seek_prev] {name}: {steps} 步 / 尺子 {ruler}（库 {N} 把、顶上 {DEAD} 把已死、每把 {CHAIN} 版）\
                 rows={d_rows} fold={d_fold} keydir={d_bc} light={d_lv} block={d_br} page={d_pr} cache={d_ph}"
        );
        // ② 步数断的是**精确形状** = 死串每把一步 + 答案一步（实测四家都是 9）。
        // 不写 `<=` 是因为这轮的代价模型没有"多走几步也算对"的余地：多步 = 有组没被界挡住，
        // 少步 = 界把活键也一起跳了（那必然同时错答案，只是 ① 先报还是 ② 先报的区别）。
        assert_eq!(
            steps,
            DEAD as u64 + 1,
            "{name}: 一次 seek_prev 往退了 {steps} 把键，而死串只有 {DEAD} 把 ⇒ \
                 界没按「组」跳（步数偏多），或者反向一步的界把活键也跳过了（步数偏少）"
        );
        // 预算按各家的**单位**分别给：bplus 数的是页，2000 把键只摊在 63 片叶子上 ⇒ 拿"键数"
        // 当预算抓不到 bplus 的老写法（实测老写法只付 63，低于 160）。
        let budget = match name {
            "bplus_tree" => steps * 4 + 8,
            _ => (DEAD * CHAIN) as u64 * 4,
        };
        assert!(
            ruler <= budget,
            "{name}: 一次 seek_prev 付了 {ruler}（本引擎尺子），预算 {budget}；库 {N} 把、死串只有 {DEAD} 把 ⇒ \
                 又在折全库（老写法实测：bitcask/lsm/memory 2017 把键、bplus 63 页）"
        );
        // ① 等价放最后：上面两条撞红时报的是"形状"，这一条报的是"数据"。三种退化各由第一条
        // 撞上它的断言报出来 —— 老写法不调共用内核（②读到 0 步），界算错但答案仍可能对（只有
        // ②看得见），判定漏了过期/墓碑（前两条都绿，答案错，只有 ①看得见）。
        assert_eq!(
            hit.as_ref().map(|r| r.key.clone()),
            want,
            "{name}: seek_prev 与「逐把点读往回找」的参考不一致"
        );
        assert_eq!(
            hit.as_ref().map(|r| r.key.clone()),
            Some(answer.as_bytes().to_vec()),
            "{name}: seek_prev 应落在死串下面第一把活键 {answer}（删掉的、过期的都不许交出去）"
        );
        assert_eq!(
            hit.as_ref().and_then(|r| r.value.clone()).as_deref(),
            Some(&b"v"[..]),
            "{name}: seek_prev 交出来的 value 不该是信封裸字节"
        );
    }
}

/// 门面层：`without_total()` 在四家必须给出**同一页的同一份内容**，而 `total` 如实是 `None`。
///
/// 分页是下游直接面对的 API：一家行为不同，wrapper 里就多一句 `if engine == ...`。
/// 这条与四家各自的窗口代价守卫不重复 —— 那边量的是"付多少"，这里对的是"给的答案一样吗"。
#[test]
fn windowed_search_agrees_across_engines() {
    const N: usize = 500;
    const PAGE: usize = 20;

    let engines = open_all();
    let mut dirs = Vec::new();
    for (name, db, dir) in engines {
        dirs.push(dir);
        {
            let tx = db.begin();
            for i in 0..N {
                assert!(
                    tx.set(
                        format!("user{i:04}").as_bytes(),
                        format!("v{i}").into_bytes()
                    ),
                    "{name}: 第 {i} 行没写进去"
                );
            }
            tx.commit();
        }

        // 事务对象有 `Drop`（漏 commit 就自动回滚），所以每次都关在自己的块里 ——
        // 不然借用活到循环体末尾，下面的 `drop(db)` 会打架。
        let q = SearchQuery::contains(b"user", 0, PAGE);
        let with = {
            let tx = db.begin();
            let page = tx.search_keys(&q);
            tx.commit();
            page
        };
        let without = {
            let tx = db.begin();
            let page = tx.search_keys(&q.clone().without_total());
            tx.commit();
            page
        };
        assert_eq!(with.total, Some(N), "{name}: 要 total 时照旧数得出来");
        assert_eq!(
            without.total, None,
            "{name}: 没数就必须是 None，不许报一个数"
        );
        assert_eq!(without.total_pages, None, "{name}: 没数就不许报页数");
        assert_eq!(
            without.items, with.items,
            "{name}: 窗口内容必须与全量同一页一致"
        );
        assert!(without.has_next(), "{name}: 首页之后当然还有");

        // 最后一页：两边都必须说"没有下一页"
        let last = N / PAGE - 1;
        let q2 = SearchQuery::contains(b"user", last, PAGE);
        let a = {
            let tx = db.begin();
            let page = tx.search_keys(&q2);
            tx.commit();
            page
        };
        let b = {
            let tx = db.begin();
            let page = tx.search_keys(&q2.clone().without_total());
            tx.commit();
            page
        };
        assert_eq!(a.items, b.items, "{name}: 尾页内容一致");
        assert_eq!(a.items.len(), PAGE, "{name}: 尾页也该是满页");
        assert!(!b.has_next(), "{name}: 最后一页之后没有东西了，不许虚报");
        drop(db);
    }
    cleanup(dirs);
}

/// 四家的 `key_count` 与"要精确 total 的 `search_keys`"都必须**只数不物化**（M35）。
///
/// 尺子 = **用户字节被拷出来的条数**（`common::user_value_copies`；两个拷贝入口都记在这一把
/// 上：search 剥壳用的 `logical_to_user`、scan 剥壳用的 `live_record_of_logical`）。判据：
/// - `key_count` 一次调用 ⇒ **0 条**。旧形状是 `prefix_scan(prefix).len()`，为了一个数把界内
///   每行的键与值都物化进 `Vec` 再丢掉（600k 档实测 **2.36 s / 峰值 +123.7 MB**）。
/// - `search_keys(要 total)` 首页 ⇒ 拷的不超过**页宽 + 1**（旧形状 = 把界内命中全物化再 `len()`，
///   实测 **2.05 s / +123.8 MB**，而免 total 只要 1.2 ms ⇒ 差三个数量级那一格就是这个）。
///
/// ⚠️ 另钉两条**语义对拍**（只数不物化不许把判定改窄或改宽）：
/// - `key_count` 必须与 `prefix_scan().len()` 同一个数，库里**同时放了墓碑行与过期行**；
/// - `search_keys` 的 `total` / `items` / `has_next` 在首页与深页上都必须与"全量物化那一条路"
///   给出的同一个数（`search_page_from` 那条老入口就是对照）。
///
/// 为什么钉计数不钉内存：分配计数是进程级的，并行用例互相污染（M27/M36 各栽过一次）；
/// 而"数一个数拷了几条 value"是个纯计数。内存那笔（+123.7 MB）留在 `--test-threads=1` 的台架里读。
///
/// 变异验证：① 把某一家的 `key_count` 换回 `prefix_scan(prefix).len()` ⇒ 那一家红在
/// "拷了 893 条用户字节"；② 把 `count_window` 里的 `logical_is_live` 换成 `logical_to_user`
/// ⇒ 四家一起红（那就不是"只数不物化"而是"数之前先拷一遍"）。
#[test]
fn all_engines_count_without_materializing_values() {
    const A_KEYS: usize = 900;
    const B_KEYS: usize = 120;
    const DEAD: usize = 7;
    const EXPIRED: usize = 5;
    const PAGE: usize = 50;
    // 前缀 `aa` 下的活键：写进去的减去硬删掉的；过期那 5 把另算，它们**不该**被数进来
    let want_a = A_KEYS - DEAD;

    let engines = open_all();
    let mut dirs = Vec::new();
    for (name, db, dir) in engines {
        dirs.push(dir);
        db.set_vacuum_auto_slice(0);
        let tx = db.begin();
        for i in 0..A_KEYS {
            assert!(
                tx.set(format!("aa{i:05}").as_bytes(), vec![b'v'; 16]),
                "{name}: 第 {i} 把 aa 键写失败"
            );
        }
        for i in 0..B_KEYS {
            assert!(
                tx.set(format!("bb{i:05}").as_bytes(), vec![b'w'; 16]),
                "{name}: 第 {i} 把 bb 键写失败"
            );
        }
        for i in 0..DEAD {
            assert!(
                tx.delete(format!("aa{i:05}").as_bytes()),
                "{name}: 第 {i} 把 aa 键删失败"
            );
        }
        let past = crate::common::now_unix_secs().saturating_sub(30);
        for i in 0..EXPIRED {
            // 走 `set`：它把传进来的字节当作**已打过信封**的逻辑值（`set_raw` 会再包一层
            // plain 信封，那就不是过期行了 —— 这里要的正是"界内有一批过期行"这个形状）
            assert!(
                tx.set(
                    format!("aa9{i:04}").as_bytes(),
                    crate::common::pack_ttl(b"gone", past)
                ),
                "{name}: 第 {i} 把过期行写失败"
            );
        }
        // 对拍的基准：走一遍**真物化**的路（那条路本来就该给出正确的数）
        let scan_a = tx.prefix_scan(b"aa").len();
        assert_eq!(
            scan_a, want_a,
            "{name}: 基准形状自己就不同（{scan_a} vs {want_a}）⇒ 库的形状没搭对"
        );

        // ① key_count：只数不物化
        let base = crate::common::user_value_copies();
        let got = tx.key_count(b"aa");
        let copies = crate::common::user_value_copies() - base;
        eprintln!(
            "[count only] {name}: key_count={got}（物化对照 {scan_a}）⇒ 期间拷出 {copies} 条用户字节"
        );
        assert_eq!(
            got, want_a,
            "{name}: key_count 与 prefix_scan 不同数 ⇒ 判定被改了"
        );
        assert_eq!(
            copies, 0,
            "{name}: key_count 拷了 {copies} 条用户字节 ⇒ 又变回'整段物化成一个 Vec 再取 len()'"
        );

        // ② search_keys（要精确 total）：只拷那一页
        let q = SearchQuery::prefix(b"aa", 0, PAGE);
        let base = crate::common::user_value_copies();
        let page = tx.search_keys(&q);
        let copies = crate::common::user_value_copies() - base;
        assert_eq!(page.total, Some(want_a), "{name}: 首页 total 不对");
        assert_eq!(page.items.len(), PAGE, "{name}: 首页不是满页");
        assert!(
            page.has_next,
            "{name}: {want_a} 条 / 页宽 {PAGE}，首页之后当然还有"
        );
        assert!(
            copies <= (PAGE + 1) as u64,
            "{name}: search_keys 首页拷了 {copies} 条用户字节（页宽 {PAGE}）⇒ total 那一步还在物化全段"
        );
        // 免 total 那条分支的 items 必须一模一样（它没被这一刀碰过）
        let p2 = tx.search_keys(&q.clone().without_total());
        assert_eq!(p2.total, None, "{name}: 免 total 那条不该给出数");
        assert_eq!(
            p2.items.len(),
            page.items.len(),
            "{name}: 两条路的首页不同长"
        );
        assert_eq!(
            p2.items[0].key, page.items[0].key,
            "{name}: 两条路的首页不同行"
        );

        // ③ 深页：total 推出来的 has_next / total_pages 要在边界上翻得对
        let last_page = (want_a + PAGE - 1) / PAGE - 1;
        let deep = tx.search_keys(&SearchQuery::prefix(b"aa", last_page, PAGE));
        assert_eq!(
            deep.items.len(),
            want_a - last_page * PAGE,
            "{name}: 末页行数不对（total={} 页宽 {PAGE}）",
            deep.total.unwrap_or(0)
        );
        assert!(!deep.has_next, "{name}: 末页还报有下一页");
        assert_eq!(
            deep.total_pages,
            Some((want_a + PAGE - 1) / PAGE),
            "{name}: total_pages 与 total/页宽不一致"
        );

        // ④ M51：末页这一趟**交出的行**必须与改前逐行相同（内存那一格在专门的守卫里钉：
        //    `deep_search_pages_do_not_materialize_earlier_pages` —— 这个库规模下动态范围不够，
        //    末页与第 0 页的高水位只差百分之几，比值判据拦不住旧形状，别在这儿装样子）。
        let deep_again = tx.search_keys(&SearchQuery::prefix(b"aa", last_page, PAGE));
        assert_eq!(
            deep_again.items.len(),
            deep.items.len(),
            "{name}: 流式取页与整段切片给出的末页行数不同"
        );
        for (i, rec) in deep_again.items.iter().enumerate() {
            assert_eq!(
                rec.key, deep.items[i].key,
                "{name}: 流式取页交出的末页不是同几行"
            );
        }

        // ⑤ 语义必须一字不差：**每一页**都等于"全量折叠"的对应切片（含末页之后那一页）。
        //    只断"末页行数对"不够 —— 跳过 `skip` 行这条逻辑一旦错位（多跳/少跳/跳错键），
        //    行数照样可能对，行却是别的行。逐行比键才拦得住。
        let truth = tx.prefix_scan(b"aa");
        assert_eq!(truth.len(), want_a, "{name}: 对拍基准那一趟少了几行");
        for p in 0..=last_page + 1 {
            let pg = tx.search_keys(&SearchQuery::prefix(b"aa", p, PAGE));
            let lo = (p * PAGE).min(truth.len());
            let hi = ((p + 1) * PAGE).min(truth.len());
            assert_eq!(pg.items.len(), hi - lo, "{name}: 第 {p} 页行数与切片不一致");
            for (i, rec) in pg.items.iter().enumerate() {
                assert_eq!(
                    rec.key,
                    truth[lo + i].key,
                    "{name}: 第 {p} 页第 {i} 行的键不是切片里那一行 ⇒ skip 跳错了"
                );
            }
            assert_eq!(
                pg.has_next,
                (p + 1) * PAGE < truth.len(),
                "{name}: 第 {p} 页 has_next 不对（免 total 那支靠它，不许把没数报成到底）"
            );
            let pg_nt = tx.search_keys(&SearchQuery::prefix(b"aa", p, PAGE).without_total());
            assert_eq!(
                pg_nt.items.len(),
                pg.items.len(),
                "{name}: 第 {p} 页在要/不要 total 两条路上不同长"
            );
            assert_eq!(
                pg_nt.has_next, pg.has_next,
                "{name}: 第 {p} 页两条路的 has_next 分家"
            );
        }
        tx.commit();
        // `db` 在每次迭代末尾自己就掉了（事务 `tx` 比它先结束），所以这里不需要显式 `drop`；
        // 写在这里是因为 Windows 上文件句柄没释放就 `remove_dir_all` 会失败。
    }
    cleanup(dirs);
}
