// 该文件集中验证对应职责，保留原测试名称和字面值。

/// 写侧归因：**灌一行**的代价为什么会随库规模涨。
///
/// 触发它的那次测量：2026-10-17 的大规模基线（release，同一份构建对照跑）显示
/// `灌 20000 行(事务)` = 38.8 µs/行、`灌 200000 行(事务)` = **227.8 µs/行** ⇒ 每行贵 5.9 倍，
/// 十倍的量花掉约五十九倍的时间。台架只报"µs/行"，报不出**为什么**，而这条通道在写侧 ——
/// 恰恰是下游最在意的那一条（进程启动后一直往里灌）。
///
/// 这条按块（默认每 2 万行）打一行：块内每行耗时 + 当时的 memtable 字节 + 每千行块读次数 +
/// `begin`/`set`/`commit` 三段各自的每事务耗时，并在块末做两个探针（见下面的排除清单）。
/// 目的只有一个：**把"每行为什么变贵"归到某一段上**，而不是让 `µs/行` 一个数替整条路径说话。
///
/// 跑法（默认 20 万行、每 2 万一行；想小一点给环境变量）：
/// ```text
/// LSM_FILL_ROWS=200000 LSM_FILL_CHUNK=20000 cargo test --release --lib -- --ignored \
///     --nocapture --test-threads=1 lsm_fill_attribution
/// ```
#[test]
#[ignore = "写侧归因，手动 release 单线程跑：LSM_FILL_ROWS=200000 LSM_FILL_CHUNK=20000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 lsm_fill_attribution"]
fn lsm_fill_attribution_per_row_cost() {
    use std::time::Instant;

    let rows: usize = std::env::var("LSM_FILL_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200_000);
    let chunk: usize = std::env::var("LSM_FILL_CHUNK")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20_000);

    // 键/值形状照抄门面台架：40 B 键、60 B 值（值必须小到不进 blob 旁路，否则不是同一条路）
    fn make_key(i: usize) -> Vec<u8> {
        let tag = b'1' + (i % 3) as u8;
        let pid = 1000 + i % 7;
        let mut k = Vec::new();
        k.push(tag);
        k.extend_from_slice(format!("#{pid:04}#worldquant/datafields/{i}/f.json").as_bytes());
        while k.len() < 40 {
            k.push(b'0');
        }
        k
    }
    fn make_val(i: usize) -> Vec<u8> {
        format!("{{\"i\":{i},\"h\":\"{:0<24}\",\"n\":97}}", i * 7).into_bytes()
    }

    // 已经用这条台架排除掉的：
    //   · 探 SST ⇒ 块读全程 0（前十万行 total_sst=0），不是"写侧冲突探测在探文件"；
    //   · 自动回收的 tick ⇒ 开/关两条曲线一模一样（55→143→259→558→1047 µs/行）；
    //   · memtable 插入本身 ⇒ 走 `insert_fast` 的 bulk 在 200k 是 2.0 µs/行（平的）；
    //   · begin/commit ⇒ 三段计时里 begin≈1 µs、commit≈2.7 ms 恒定，全部代价都在 `set` 段
    //     （每事务 69 ms → 1001 ms）。
    // 剩下两个还没分开的因子，各由一次探针认出来：
    //   A `事务内连写` —— 开一个事务连写 PROBE 行，**分别计时前 200 行与后 200 行**：
    //     后 200 明显贵 ⇒ 代价 ∝ **写集宽度**（`TxnCore::write` 里那句 `writes.iter().any(...)`
    //     线性查重）；两者一样贵、但随块号涨 ⇒ 代价 ∝ **当时的 memtable 规模**。
    //   B `裸 insert` —— 不经 MVCC（无冲突探测、无 WAL、无写集），直接 `LSMTree::insert`
    //     （仍是 `!fast`，带 live 统计维护）⇒ 若它也随规模涨，问题在存储层而不事务层。
    const PROBE: usize = 2_000;
    const EDGE: usize = 200;

    let path = tmp_db("fill_attribution");
    let mvcc = MVCC::open(&path);
    mvcc.set_vacuum_auto_slice(0);
    eprintln!("=== 灌入形状：每 1000 行一事务（自动回收关，已排除）===");
    eprintln!(
        "{:>8} {:>9} {:>9} {:>9} {:>9} {:>8} {:>11} {:>10} {:>10} {:>10}",
        "已灌行",
        "µs/行",
        "begin/事务",
        "set/事务",
        "commit/事务",
        "mem_KB",
        "块读/千行",
        "探针前200",
        "探针后200",
        "裸ins/行"
    );
    let mut done = 0usize;
    while done < rows {
        let end = (done + chunk).min(rows);
        let b0 = crate::lsm_tree::sst::block_reads();
        // 三段各自计时：begin（含自动回收 tick）/ batch_set（写序列）/ commit（WAL 收尾）。
        let (mut t_begin, mut t_write, mut t_commit, mut ntxn) = (0u128, 0u128, 0u128, 0u64);
        let t = Instant::now();
        for base in (done..end).step_by(1000) {
            let stop = (base + 1000).min(end);
            let items: Vec<(Vec<u8>, Vec<u8>)> =
                (base..stop).map(|i| (make_key(i), make_val(i))).collect();
            let s = Instant::now();
            let tx = mvcc.begin_transaction();
            t_begin += s.elapsed().as_micros();
            let s = Instant::now();
            if let Err(bad) = tx.batch_set(&items) {
                panic!("batch_set 冲突: {}", String::from_utf8_lossy(&bad));
            }
            t_write += s.elapsed().as_micros();
            let s = Instant::now();
            tx.commit();
            t_commit += s.elapsed().as_micros();
            ntxn += 1;
        }
        let ms = t.elapsed().as_secs_f64() * 1000.0;
        let n = end - done;
        let blocks = crate::lsm_tree::sst::block_reads() - b0;

        // 探针 A：同一个事务里连写 PROBE 行全新键，前 EDGE 与后 EDGE 分别计时
        let (mut first, mut last) = (0u128, 0u128);
        {
            let tx = mvcc.begin_transaction();
            for j in 0..PROBE {
                let s = Instant::now();
                tx.set_raw(&make_key(rows + 10 + j), make_val(rows + 10 + j));
                let us = s.elapsed().as_micros();
                if j < EDGE {
                    first += us;
                } else if j >= PROBE - EDGE {
                    last += us;
                }
            }
            tx.rollback();
        }

        // 探针 B：裸 insert（不经事务：无冲突探测、无 WAL、无写集）
        let s = Instant::now();
        {
            let mut kv = mvcc.kv.lock().unwrap();
            for j in 0..PROBE {
                let k = make_key(rows + 5_000 + j);
                let enc = encode_key(&k, 7);
                kv.insert(
                    enc,
                    Some(crate::common::pack_plain(&make_val(rows + 5_000 + j))),
                );
            }
        }
        let raw_us = s.elapsed().as_micros() as f64 / PROBE as f64;

        let st = mvcc.kv.lock().unwrap().stats();
        let per = |v: u128| v as f64 / ntxn.max(1) as f64;
        eprintln!(
            "{:>8} {:>9.1} {:>9.1} {:>9.1} {:>9.1} {:>8} {:>11.2} {:>10.1} {:>10.1} {:>10.1}",
            end,
            ms * 1000.0 / n as f64,
            per(t_begin),
            per(t_write),
            per(t_commit),
            st.mem_bytes / 1024,
            blocks as f64 * 1000.0 / n as f64,
            first as f64 / EDGE as f64,
            last as f64 / EDGE as f64,
            raw_us,
        );
        done = end;
    }
    let h = mvcc.health();
    eprintln!(
        "收尾: rows={} live_keys={} max_chain={}",
        h.total_rows, h.live_keys, h.max_chain
    );
    drop(mvcc);
    cleanup(&path);
}
