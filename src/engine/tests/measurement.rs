// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

// ==================== 大库基线台架（四引擎 × 每种操作） ====================
//
// 前面所有守卫数的是「访问了多少块/页/盘」—— 那是**复杂度**的尺子。下游报的却是
// **墙钟与占用**（点查 0.86ms、CPU 打满、进程吃内存）。两把尺子可以同时成立：LSM 那条
// 就是活例子（每点读只有 4 次块访问，实测 0.86ms，因为未命中要重开文件）。
// 所以这里把每个引擎的每个操作都灌进大库跑一遍，三件事一起量：
//   µs/次、结束后仍活着的字节（常驻）、期间峰值字节（瞬时物化）。
//
// 跑法（默认 5 万行，想更大给环境变量）：
//   SCALE_ROWS=200000 cargo test --release --lib -- --ignored --nocapture \
//       --test-threads=1 scale_baseline_all_engines
// `--test-threads=1` 是必须的：内存计数是进程级的，并行会把别家用例的分配算进来。
// 记账分配器本身在 crate 根的 `crate::mem_probe`（M33 提到根上：一个 crate 只能有一个
// `#[global_allocator]`，而 LSM 的压实台架要用同一把尺子，不能在 lsm 那边再抄一份）。

/// 跑一次操作，返回 (墙钟, 常驻字节增量, 期间峰值字节增量, 操作返回值)
///
/// 峰值**先把高水位压回"当前活着的字节"再量**：`peak` 是进程级单调高水位，只增不减 ⇒
/// 直接取 `peak() - peak()` 之前那个高点的话，**排在后面的操作只会报 +0.0 MB**，哪怕它
/// 自己瞬时涨了 50 MB（M33 记过这个盲区，M36 又踩了一遍：那一轮 lsm/bplus/memory 三家
/// 的 `vacuum()` 峰值读数全是 0，等于没量）。压回去之后 `peak − 基准` 才是**这一段**的瞬时增量。
fn timed_phase<T>(
    engine: &str,
    phase: &str,
    f: impl FnOnce() -> T,
) -> (std::time::Duration, i64, u64, T) {
    let probe = crate::scale_probe::Phase::begin(engine, phase);
    let result = timed(f);
    probe.finish(engine, phase);
    result
}

fn timed<T>(f: impl FnOnce() -> T) -> (std::time::Duration, i64, u64, T) {
    let base = crate::mem_probe::reset_peak();
    let l0 = crate::mem_probe::live() as i64;
    let t = std::time::Instant::now();
    let out = f();
    let d = t.elapsed();
    let l1 = crate::mem_probe::live() as i64;
    (
        d,
        l1 - l0,
        crate::mem_probe::peak().saturating_sub(base),
        out,
    )
}

/// 目录（或文件）总字节
fn disk_bytes(path: &std::path::Path) -> u64 {
    if !path.exists() {
        return 0;
    }
    if path.is_file() {
        return std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    }
    let mut total = 0u64;
    let mut stack = vec![path.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&dir) else {
            continue;
        };
        for e in rd.flatten() {
            let Ok(meta) = e.metadata() else { continue };
            if meta.is_dir() {
                stack.push(e.path());
            } else {
                total += meta.len();
            }
        }
    }
    total
}

fn shuffled(n: usize) -> Vec<usize> {
    let mut v: Vec<usize> = (0..n).collect();
    let mut s = 0x853c_49e6_77b9_5b3eu64;
    for i in (1..n).rev() {
        s = s
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let j = (s >> 33) as usize % (i + 1);
        v.swap(i, j);
    }
    v
}

fn line(engine: &str, op: &str, per: f64, unit: &str, live: i64, peak: u64, extra: &str) {
    let live_mb = live as f64 / 1e6;
    let peak_mb = peak as f64 / 1e6;
    eprintln!(
        "{engine:<10} {op:<24} {per:>11.1} {unit:<7} 堆增量 {live_mb:>9.1} MB  堆峰值 +{peak_mb:>8.1} MB  {extra}"
    );
}

/// M48 的**冷启动分段**读数（门控 `OPEN_STAGE_SPLIT`，插桩在 bitcask 的 open 路径里，
/// 八段的名字与边界写在 `MVCC::try_open_with` 上面那张表）。
///
/// ⚠️ 每次量之前**必须清槽**（`open_stages_begin` 做这件事）：台架是四家排着跑的，
/// 第一版没清，bplus/lsm 就把 bitcask 的读数原样复述了一遍 —— 那比读 0 更危险，
/// 因为它看着像证据。八段全 0 时明说"这一家没插桩"，不许把它读成"不花钱"（M38 那条）。
fn open_stages_report(engine: &str, scan_max_before: u64) {
    if std::env::var("OPEN_STAGE_SPLIT").is_err() {
        return;
    }
    let s = crate::mem_probe::open_stages();
    let scanned = crate::bitcask::bitcask::merge_rescan_rows();
    let scan_max = crate::bitcask::bitcask_mvcc::scan_max_calls() - scan_max_before;
    if s.iter().all(|v| *v == 0) {
        eprintln!(
            "{engine:<10} 冷启动分段: 八段全 0 ⇒ 这一家没插桩（只有 bitcask 装了），别当成\"不花钱\""
        );
        return;
    }
    let ms = |v: u64| v as f64 / 1e6;
    let sum: u64 = s.iter().sum();
    eprintln!(
        "{engine:<10} 冷启动分段(ms): 门面锁 {:>6.1} | 认残骸+发现分片 {:>6.1} | hint反序列化 {:>7.1} | 建KeyDir {:>8.1} | Blob+Wal开 {:>6.1} | WAL读+重放 {:>7.1} | 起始版本 {:>7.1} | flush+ckpt {:>7.1} | 合计 {:>7.1}",
        ms(s[0]),
        ms(s[1]),
        ms(s[2]),
        ms(s[3]),
        ms(s[4]),
        ms(s[5]),
        ms(s[6]),
        ms(s[7]),
        ms(sum),
    );
    eprintln!(
        "{engine:<10}   同一次重开里：日志扫过 {scanned} 行、`scan_max_version` 走了 {scan_max} 次（0=没走那条 O(全库) 的退路）"
    );
}

/// 重开之前调一次：清槽 + 装上行数尺 + 取 `scan_max_calls` 的基准。门控关着返回 0。
fn open_stages_begin() -> u64 {
    if std::env::var("OPEN_STAGE_SPLIT").is_err() {
        return 0;
    }
    crate::mem_probe::open_stages_clear();
    crate::bitcask::bitcask::arm_merge_rescan_counter();
    crate::bitcask::bitcask_mvcc::scan_max_calls()
}
