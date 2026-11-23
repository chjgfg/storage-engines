// 该文件集中验证对应职责，保留原测试名称和字面值。

/// bplus 全库 `vacuum()` 那 10.64 s 花在哪（M54 的先量，`#[ignore]`）。
///
/// 触发它的读数：三档复测 600k 档 `bplus vacuum()` = **10.64 s/次**（同档 bitcask ~4.5 s）。
/// M34 已经证明这**不是写侧那条根** —— 把删除路径也照抄成"走解析缓存 + 回填"反而
/// 12.27 → **15.12 s**，退回后才 10.64 s（走查形状的页每页基本只碰一次，回填换不到下一次命中）。
/// ⇒ 这一格必须单独分段量，别再顺着"最合理的假设"改代码。
///
/// 量法：**同一份库、同一个构建连做两趟 `vacuum()`**
/// - 第一趟**有行可删**：每把键写两个版本 ⇒ 一半物理行是垃圾
/// - 第二趟**没行可删**：同一形状再全库走一遍（判据、块界、flush 调用全都一样）
/// ⇒ 第二趟 = 纯"走查 + 判定"；`第一趟 − 第二趟` = "删除 + 落盘"那一半。
/// 这条对照不需要新插桩，而 M53 那把 **fsync 站点计数**正好把落盘那一半直接拆开
/// （`bplus_dblwr` / `bplus_page_flush` / `bplus_freelist` / `blob`）—— 双写缓冲每刷**一片**
/// 要付四次 fsync（预写 count=0、写完页、发布 count、clear），所以"刷了多少页"与
/// "付了多少次 fsync"必须一起读，只看一个都会归错因。
///
/// 计数尺：`page_reads`（走查读了多少页）、`cache_misses`、`batch_flushed_pages`（刷了多少页）、
/// `fsync_counts`（按站点）。**墙钟只报每行单价，因果凭据是次数**（本机跨轮漂 1.3~1.9×）。
///
/// ```text
/// BPLUS_VAC_ROWS=600000 cargo test --release --lib -- --ignored --nocapture \
///     --test-threads=1 bplus_vacuum_cost_attribution
/// ```
#[test]
#[ignore = "bplus 全库 vacuum 分段归因，手动 release 单线程跑：BPLUS_VAC_ROWS=600000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 bplus_vacuum_cost_attribution"]
fn bplus_vacuum_cost_attribution() {
    use crate::common::disk::{FSYNC_SITES, fsync_counts, fsync_counts_clear};
    use crate::engine::{EngineKind, open_engine};
    use std::time::Instant;

    let rows: usize = std::env::var("BPLUS_VAC_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200_000);
    fn key(i: usize) -> Vec<u8> {
        format!("{i:08}#/worldquant/datafields/x.json").into_bytes()
    }
    fn val(i: usize, round: usize) -> Vec<u8> {
        format!(
            "{{\"i\":{i},\"r\":{round},\"h\":\"{:0<20}\",\"v\":1}}",
            i * 7
        )
        .into_bytes()
    }
    fn site(name: &str) -> usize {
        FSYNC_SITES.iter().position(|n| *n == name).unwrap()
    }

    let dir = tmp_db("vac_attrib");
    let db = open_engine(EngineKind::BPlusTree, Some(&dir)).unwrap();

    // 灌两遍同一批键 ⇒ 2·rows 物理行，其中 rows 行是可删的旧版本
    for round in 0..2 {
        for base in (0..rows).step_by(1000) {
            let end = (base + 1000).min(rows);
            let items: Vec<(Vec<u8>, Vec<u8>)> =
                (base..end).map(|i| (key(i), val(i, round))).collect();
            let tx = db.begin();
            if let Err(bad) = tx.batch_set(&items) {
                panic!(
                    "灌第 {round} 遍 {base}..{end}: {}",
                    String::from_utf8_lossy(&bad)
                );
            }
            tx.commit();
        }
    }
    db.flush().unwrap();
    db.checkpoint().unwrap();

    let probe = || -> Vec<(String, u64)> {
        vec![
            ("页读".to_string(), crate::bplus_tree::page::page_reads()),
            ("缺页".to_string(), crate::bplus_tree::page::cache_misses()),
            // 最底层：真的向文件读了几页（缺页是"页缓存没给"，这一把才是"问了盘"）
            (
                "落盘读页".to_string(),
                crate::common::disk::disk_page_reads(),
            ),
            (
                "换出".to_string(),
                crate::bplus_tree::page::page_evictions(),
            ),
            (
                "LRU压缩".to_string(),
                crate::bplus_tree::page::lru_compactions(),
            ),
            (
                "下降次数".to_string(),
                crate::bplus_tree::bplus_tree::descent_lookups(),
            ),
            (
                "下降键比较".to_string(),
                crate::bplus_tree::bplus_tree::descent_key_comps(),
            ),
            (
                "节点解析".to_string(),
                crate::bplus_tree::bplus_tree::node_parses(),
            ),
            (
                "解析缓存清扫".to_string(),
                crate::bplus_tree::bplus_tree::node_cache_sweeps(),
            ),
            (
                "分裂产出".to_string(),
                crate::bplus_tree::bplus_tree::split_drops(),
            ),
            (
                "刷出页".to_string(),
                crate::bplus_tree::page::batch_flushed_pages(),
            ),
            // M56 的七桶：把"每删除行读几页"拆到具体哪一段在读
            (
                "页·下降".to_string(),
                crate::bplus_tree::page::page_reads_by_phase()
                    [crate::bplus_tree::page::PHASE_DESCENT as usize],
            ),
            (
                "页·回溯".to_string(),
                crate::bplus_tree::page::page_reads_by_phase()
                    [crate::bplus_tree::page::PHASE_BACKTRACK as usize],
            ),
            (
                "页·分隔键".to_string(),
                crate::bplus_tree::page::page_reads_by_phase()
                    [crate::bplus_tree::page::PHASE_SEP_REFRESH as usize],
            ),
            (
                "页·重组".to_string(),
                crate::bplus_tree::page::page_reads_by_phase()
                    [crate::bplus_tree::page::PHASE_RESTRUCTURE as usize],
            ),
            (
                "页·找祖父".to_string(),
                crate::bplus_tree::page::page_reads_by_phase()
                    [crate::bplus_tree::page::PHASE_GRANDPARENT as usize],
            ),
            (
                "页·问根".to_string(),
                crate::bplus_tree::page::page_reads_by_phase()
                    [crate::bplus_tree::page::PHASE_ROOT_CHECK as usize],
            ),
            // 重组自己的账：不做这些就没人知道"页·重组"那一段是谁花的
            (
                "真下溢".to_string(),
                crate::bplus_tree::bplus_tree::reorg_stats().0,
            ),
            (
                "借位".to_string(),
                crate::bplus_tree::bplus_tree::reorg_stats().1,
            ),
            (
                "合并".to_string(),
                crate::bplus_tree::bplus_tree::reorg_stats().2,
            ),
            (
                "问根次数".to_string(),
                crate::bplus_tree::bplus_tree::root_checks(),
            ),
            (
                "分隔键刷新".to_string(),
                crate::bplus_tree::bplus_tree::sep_refreshes(),
            ),
            // M61 的先量：合批这一刀的收益 = 删除行数 ÷ 落到的叶子游程数（升序删除时游程 == 不同叶子）
            (
                "删除行(尺子)".to_string(),
                crate::bplus_tree::bplus_tree::delete_leaf_runs().0,
            ),
            (
                "叶子游程".to_string(),
                crate::bplus_tree::bplus_tree::delete_leaf_runs().1,
            ),
        ]
    };
    let phys = rows as f64 * 2.0;
    let report = |tag: &str,
                  secs: f64,
                  p0: &[(String, u64)],
                  p1: &[(String, u64)],
                  f: [u64; 14],
                  removed: usize,
                  scanned: u64| {
        eprintln!(
            "[{tag}] 墙钟 {:.2} s = {:.3} µs/物理行（{:.1} 万物理行）；删掉 {} 行 / 扫过 {} 行",
            secs,
            secs * 1e6 / phys,
            phys / 1e4,
            removed,
            scanned
        );
        let per_del = |v: u64| -> f64 {
            if removed > 0 {
                v as f64 / removed as f64
            } else {
                0.0
            }
        };
        // 一张表两口径：每物理行（与第二趟可比）+ 每删除行（这一半的钱才是元凶那半）
        for ((name, a), (_, b)) in p0.iter().zip(p1.iter()) {
            let d = b.saturating_sub(*a);
            eprintln!(
                "        {name:<10} {:>12} 次  每物理行 {:>8.3}  每删除行 {:>8.2}",
                d,
                d as f64 / phys,
                per_del(d)
            );
        }
        // M61 的先量：k = 删除行数 ÷ 叶子游程数 = "一趟下降平均能带几行"的**上限**。
        // k≈1 ⇒ 每行本来就落在不同叶子里，合批这一刀一分钱买不到；k=4 ⇒ 下降次数最多能降到 1/4。
        let delta = |name: &str| -> u64 {
            let a = p0
                .iter()
                .find(|(n, _)| n == name)
                .map(|(_, v)| *v)
                .unwrap_or(0);
            let b = p1
                .iter()
                .find(|(n, _)| n == name)
                .map(|(_, v)| *v)
                .unwrap_or(0);
            b.saturating_sub(a)
        };
        let dr = delta("删除行(尺子)");
        let runs = delta("叶子游程");
        if dr > 0 {
            eprintln!(
                "        [合批上限 k] 这次删除尺子看到 {dr} 行、落到 {runs} 片叶子游程 ⇒ k = {:.2} 行/游程\
                     （合批之后下降次数最多降到 1/{:.2}；k≈1 就说明这一刀买不到东西）",
                dr as f64 / runs.max(1) as f64,
                dr as f64 / runs.max(1) as f64
            );
        }
        let mut parts = Vec::new();
        let mut tot = 0u64;
        for (name, n) in FSYNC_SITES.iter().zip(f.iter()) {
            if *n > 0 {
                parts.push(format!("{name} {n}"));
            }
            tot += n;
        }
        eprintln!("        fsync 合计 {tot} 次：{}", parts.join(", "));
    };

    // ── 第一趟：有行可删 ──
    fsync_counts_clear();
    let p0 = probe();
    let t = Instant::now();
    let s1 = db.vacuum().unwrap();
    let d1 = t.elapsed().as_secs_f64();
    let f1 = fsync_counts();
    let p1 = probe();
    report(
        "第一趟 有得删",
        d1,
        &p0,
        &p1,
        f1,
        s1.versions_removed,
        s1.rows_scanned,
    );
    assert!(
        s1.versions_removed > 0,
        "第一趟什么都没删掉 ⇒ 这份库没造出可删的版本，第二趟那个对照就不成立"
    );
    let dblwr1 = f1[site("bplus_dblwr")];

    // ── 第二趟：没行可删（同一形状再走一遍）──
    fsync_counts_clear();
    let q0 = probe();
    let t = Instant::now();
    let s2 = db.vacuum().unwrap();
    let d2 = t.elapsed().as_secs_f64();
    let g1 = fsync_counts();
    let q1 = probe();
    report(
        "第二趟 无得删",
        d2,
        &q0,
        &q1,
        g1,
        s2.versions_removed,
        s2.rows_scanned,
    );
    // ⚠️ 不是"必须 == 0"：删除只能落在组边界上，块界（8192）附近会有零星残留。
    // 判据要的是"第二趟删得比第一趟少一个量级以上"，否则两趟不是同一形状。
    assert!(
        s2.versions_removed * 100 <= s1.versions_removed,
        "第二趟删了 {} 行（第一趟 {}）⇒ 第一趟没走完，对照不成立",
        s2.versions_removed,
        s1.versions_removed
    );

    let dblwr2 = g1[site("bplus_dblwr")];
    eprintln!(
        "拆分：走查+判定 ≈ {:.2} s（第二趟）｜删除+落盘 ≈ {:.2} s（第一趟 − 第二趟，第一趟是第二趟的 {:.2}×）",
        d2,
        (d1 - d2).max(0.0),
        if d2 > 0.0 { d1 / d2 } else { f64::NAN }
    );
    eprintln!(
        "fsync 差值：第一趟 {} − 第二趟 {} = {} 次 ⇒ **落盘那一半的账就在这些次数里**\
             （双写那格 {} → {}，每次刷一片要付 4 次）；\
             按本机 C: 一次 fsync ≈ 2.6 ms 折算 ≈ {:.2} s，D: 口径 ×33",
        f1.iter().sum::<u64>(),
        g1.iter().sum::<u64>(),
        f1.iter()
            .sum::<u64>()
            .saturating_sub(g1.iter().sum::<u64>()),
        dblwr1,
        dblwr2,
        f1.iter()
            .sum::<u64>()
            .saturating_sub(g1.iter().sum::<u64>()) as f64
            * 0.0026,
    );
    drop(db);
    cleanup(&dir);
}
