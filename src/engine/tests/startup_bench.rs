// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// M53 的**启动税**量具：一次 `open` 到底付几次 fsync、开几个文件、**分别落在哪几处**。
///
/// 为什么拿"次数"而不拿毫秒当判据：本机 `C:` 一次 fsync ≈ 2.6 ms、`D:`（5400 转）≈ 85.8 ms
/// ⇒ "启动税是 12 ms 还是 35 ms"这句话换块盘就作废，而**次数是盘型无关的账**：下游那台单核
/// 机器拿到"一次 open 付 N 次 fsync"，乘他自己的盘价就是他的启动税。
/// 站点表在 `common::disk::FSYNC_SITES` —— 每一格都得能回答"它保证什么"，
/// 答不出保证什么的（比如锁文件只为写个 pid 就 sync 一次）才是可修的那一半。
///
/// 两种形状都要量，它们不是一回事：
/// ① **全新库**（空目录 → open）：WAL / 双写 / blob 的头部各写一次；
/// ② **重开已有库**（灌过行、干净收口过 → open）：下游"重启频繁"付的是这一档。
/// 关闭那一趟（`Drop`）单列 —— 收口 WAL 的 fsync 住在关闭里，混进 open 就会把
/// "进程退出的代价"冒充成"启动的代价"。
///
/// ```text
/// OPEN_TAX_ROWS=2000 cargo test --release --lib -- --ignored --nocapture \
///     --test-threads=1 open_startup_tax_by_site
/// ```
/// ⚠️ 必须 `--test-threads=1`：计数是进程级的（与内存台架同一条理由，M27/M36 各栽过）。
#[test]
#[ignore = "启动税按站点归因，手动 release 单线程跑：OPEN_TAX_ROWS=2000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 open_startup_tax_by_site"]
fn open_startup_tax_by_site() {
    use crate::common::disk::{file_opens, file_opens_clear, fsync_counts, fsync_counts_clear};
    let rows: usize = std::env::var("OPEN_TAX_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2_000);
    let rounds = 12usize;

    fn sites(disk: &[u64; 14]) -> String {
        crate::common::disk::FSYNC_SITES
            .iter()
            .zip(disk.iter())
            .filter(|(_, n)| **n > 0)
            .map(|(name, n)| format!("{name} {n}"))
            .collect::<Vec<_>>()
            .join(", ")
    }
    fn line(label: &str, phase: &str, us: f64, disk: &[u64; 14], opens: u64) {
        let total: u64 = disk.iter().sum();
        eprintln!(
            "{label:<10} {phase:<16} fsync {total:>3} 次  打开文件 {opens:>3} 个  最快 {us:>9.1} µs  | {}",
            sites(disk)
        );
    }
    /// 读走两把尺子并立刻清零（下一趟量的是另一段）。⚠️ 读之前必须清 —— M48 那条
    /// "槽不会自己清"的教训：第一家会把读数原样复述给第二家，那比读 0 更危险。
    fn take(disk: &mut [u64; 14], opens: &mut u64) {
        *disk = fsync_counts();
        *opens = file_opens();
        fsync_counts_clear();
        file_opens_clear();
    }

    eprintln!("=== 启动税：rounds={rounds} rows={rows}（四种引擎 × 全新库/重开/关闭）===");
    for kind in [
        EngineKind::Bitcask,
        EngineKind::BPlusTree,
        EngineKind::LsmTree,
        EngineKind::Memory,
    ] {
        let label = kind.as_str();
        let is_mem = matches!(kind, EngineKind::Memory);
        let is_bplus = matches!(kind, EngineKind::BPlusTree);

        // ── 形状①：全新库（每轮都换一个干净的库目录）──
        let mut best_open = f64::MAX;
        let mut dk_open = [0u64; 14];
        let mut op_open = 0u64;
        let mut best_close = f64::MAX;
        let mut dk_close = [0u64; 14];
        let mut op_close = 0u64;
        for _ in 0..rounds {
            let d = tmp_dir(&format!("tax_new_{label}"));
            let a: Option<std::path::PathBuf> = if is_mem {
                None
            } else if is_bplus {
                Some(d.join("bplus.db"))
            } else {
                Some(d.to_path_buf())
            };
            fsync_counts_clear();
            file_opens_clear();
            let t = std::time::Instant::now();
            let db = open_engine(kind, a.as_deref()).unwrap();
            let us = t.elapsed().as_secs_f64() * 1e6;
            let mut dk = [0u64; 14];
            let mut op = 0u64;
            take(&mut dk, &mut op);
            if us < best_open {
                best_open = us;
                dk_open = dk;
                op_open = op;
            }
            let t = std::time::Instant::now();
            drop(db);
            let us = t.elapsed().as_secs_f64() * 1e6;
            take(&mut dk_close, &mut op_close);
            best_close = best_close.min(us);
            if !is_mem {
                let _ = std::fs::remove_dir_all(&d);
            }
        }
        line(label, "全新库 open", best_open, &dk_open, op_open);
        line(label, "全新库 Drop", best_close, &dk_close, op_close);

        // ── 造一份"有内容且干净收口过"的库，再量形状② ──
        let dir = tmp_dir(&format!("tax_{label}"));
        let arg: Option<std::path::PathBuf> = if is_mem {
            None
        } else if is_bplus {
            Some(dir.join("bplus.db"))
        } else {
            Some(dir.to_path_buf())
        };
        let arg = arg.as_deref();
        {
            let db = open_engine(kind, arg).unwrap();
            for base in (0..rows).step_by(500) {
                let end = (base + 500).min(rows);
                let items: Vec<(Vec<u8>, Vec<u8>)> = (base..end)
                    .map(|i| {
                        (
                            format!("{i:08}#/tax/key").into_bytes(),
                            format!("v{i:08}").into_bytes(),
                        )
                    })
                    .collect();
                let tx = db.begin();
                tx.batch_set(&items)
                    .unwrap_or_else(|bad| panic!("{label}: batch_set {bad:?}"));
                tx.commit();
            }
            db.flush().unwrap();
            db.checkpoint().unwrap();
            drop(db);
        }
        fsync_counts_clear();
        file_opens_clear();

        let mut best = f64::MAX;
        let mut dk_best = [0u64; 14];
        let mut op_best = 0u64;
        let mut dk_close2 = [0u64; 14];
        let mut op_close2 = 0u64;
        let mut best_close2 = f64::MAX;
        let mut totals = Vec::new();
        for _ in 0..rounds {
            let t = std::time::Instant::now();
            let db = open_engine(kind, arg).unwrap();
            let us = t.elapsed().as_secs_f64() * 1e6;
            let mut dk = [0u64; 14];
            let mut op = 0u64;
            take(&mut dk, &mut op);
            totals.push(dk.iter().sum::<u64>());
            if us < best {
                best = us;
                dk_best = dk;
                op_best = op;
            }
            let t = std::time::Instant::now();
            drop(db);
            let us = t.elapsed().as_secs_f64() * 1e6;
            take(&mut dk_close2, &mut op_close2);
            best_close2 = best_close2.min(us);
        }
        totals.sort_unstable();
        totals.dedup();
        line(label, &format!("重开({rows} 行)"), best, &dk_best, op_best);
        line(
            label,
            &format!("关闭({rows} 行)"),
            best_close2,
            &dk_close2,
            op_close2,
        );
        eprintln!(
            "{label:<10}   证人：{rounds} 趟重开的 fsync 总次数取值 = {totals:?}（只有一个值 ⇒ 这一档是**常数税**，不随重开次数漂）"
        );
        if !is_mem {
            assert!(
                dk_best.iter().sum::<u64>() > 0,
                "{label}: 重开一趟一次 fsync 都没数到 ⇒ 尺子没接在门上（全新库那趟若也报 0 更说明问题），\
                     这就不许读成\"这家启动不花钱\"（M38/M48：读数 0 的第一解释是量不到）"
            );
        }
        if !is_mem {
            let _ = std::fs::remove_dir_all(&dir);
        }
    }
    eprintln!(
        "换算参考：本机 `C:` 一次 fsync ≈ 2.6 ms、`D:`（5400 转机械）≈ 85.8 ms ⇒ 次数 × 盘价才是现场代价"
    );
}
