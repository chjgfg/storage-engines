//! 可见行复制、分批提交和迁移完成。

use super::*;

/// 解析 `bitcask->bplus_tree` / `lsm→bitcask` / `bplus=>lsm` 等形式。
///
/// 三种箭头都吃，是因为运维脚本里三种都出现过；`EngineKind::parse` 吃的别名与下游
/// `StorageEngine::parse` 对齐（`bc` / `bplus` / `btree` / `b+tree` / `lsm` / `mem` / `in-memory`）。
pub fn parse_arrow(spec: &str) -> Result<(EngineKind, EngineKind), String> {
    let s = spec.trim();
    let (left, right) = s
        .split_once("->")
        .or_else(|| s.split_once('→'))
        .or_else(|| s.split_once("=>"))
        .ok_or_else(|| {
            format!("无法解析迁移规格 `{s}`，请用 from->to，例如 bitcask->bplus_tree")
        })?;
    let from = EngineKind::parse(left).ok_or_else(|| format!("未知源引擎: {}", left.trim()))?;
    let to = EngineKind::parse(right).ok_or_else(|| format!("未知目标引擎: {}", right.trim()))?;
    if from == to {
        return Err(format!("源与目标相同: {from}"));
    }
    Ok((from, to))
}

/// 逐条把源交出来的行灌进目标的 bulk。返回 `(账, 写侧耗秒)`。
///
/// 为什么要自己数一遍：`KvEngine::export_latest_visible_for_each` 的返回值是"**交出去**几行"，
/// 而这里要的是"**写进去**几行"。两者不等 = 有一条被静默丢掉（正是 `push` 返回 `false`
/// 那种"共享了但没生效"的洞），所以调用点必须把两把尺子都对上。
pub(super) fn pump(
    src: &dyn KvEngine,
    dst: &dyn KvEngine,
    include_deleted: bool,
) -> Result<(IoStats, f64, f64), String> {
    let mut stats = IoStats::default();
    let mut bulk = dst.begin_bulk();
    let mut batch: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(BATCH);
    let mut write_secs = 0f64;
    let t0 = Instant::now();
    let handed = src.export_latest_visible_for_each(include_deleted, &mut |rec| {
        let tw = Instant::now();
        match rec.value {
            Some(v) => {
                stats.live += 1;
                // 0.5.0：出口把 expire 一起交出来，这里就必须按它重打信封。
                // 改前这一行一律 `pack_plain` ⇒ 带 TTL 的键搬完变永久（`migrate_currently_drops_ttl`
                // 那条现状记录今天翻红，已改写成"必须保住 TTL"）。
                let logical = match rec.expire_unix_secs {
                    Some(e) => pack_ttl(&v, e),
                    None => pack_plain(&v),
                };
                batch.push((rec.key, logical));
                if batch.len() >= BATCH {
                    bulk.put_batch_owned(std::mem::take(&mut batch));
                    batch = Vec::with_capacity(BATCH);
                }
            }
            None => {
                stats.deleted += 1;
                bulk.delete(&rec.key);
            }
        }
        stats.records += 1;
        write_secs += tw.elapsed().as_secs_f64();
        true
    });
    if !batch.is_empty() {
        let tw = Instant::now();
        bulk.put_batch_owned(batch);
        write_secs += tw.elapsed().as_secs_f64();
    }
    bulk.finish();
    let total = t0.elapsed().as_secs_f64();
    if handed != stats.records {
        return Err(format!(
            "导出交出 {handed} 行、写进去 {} 行 ⇒ 有行被静默丢掉",
            stats.records
        ));
    }
    Ok((stats, total - write_secs, write_secs))
}

/// 数目标里"可见的存活记录"有多少条 —— 与下游那份的 `verify_live` **同一个定义**
/// （那份是"再导一遍全库取 `Vec::len()`"，这里换成只数不物化：同一条折叠判据、同一批行，
/// 只是不再把整库摆进内存。M35/M47 那条纪律的第三次应用）。
pub(super) fn count_visible(db: &dyn KvEngine) -> usize {
    let mut n = 0usize;
    db.export_latest_visible_for_each(false, &mut |_rec| {
        n += 1;
        true
    });
    n
}

/// 把目标收口干净再交还句柄：`flush` + `checkpoint`（bulk 走的是"关 WAL、不逐条 fsync"那条快路，
/// 收尾不落一次等于把整次搬家押在 WAL 上 —— 下游搬完就直接起进程，不留这个尾巴）。
pub(super) fn seal(dst: &dyn KvEngine) -> Result<(), String> {
    dst.flush().map_err(|e| format!("迁移后 flush 失败: {e}"))?;
    dst.checkpoint()
        .map_err(|e| format!("迁移后 checkpoint 失败: {e}"))?;
    Ok(())
}
/// 按页扫描源引擎，每页在目标引擎独立提交。
/// 复制用户原始字节并按剩余 TTL 写入；当前页失败会撤销该页，之前已提交的页仍然存在。

/// 内存直迁：`from -> to`，两侧数据根目录都是 `data_root`。
///
/// 与下游那份的**顺序差别**要说清：那边是"导出全部 → 关源 → 开目标 → 灌"，
/// 这里是"开源 → 开目标 → 边折边灌"。源与目标在不同子目录、各持自己的文件锁，
/// 同时开着没有互踩；换来的是不物化整库。⚠️ 前提仍然是**交易进程要先停**（它持着锁）。
pub fn migrate(
    from: EngineKind,
    to: EngineKind,
    data_root: impl AsRef<Path>,
    include_deleted: bool,
    wipe_target: bool,
) -> Result<MigrateStats, String> {
    let root = data_root.as_ref().to_path_buf();
    if from == to {
        return Err(format!("源与目标相同: {from}"));
    }
    let source_path = display_path(from, &root);
    let target_path = display_path(to, &root);
    // 同一份数据被自己搬一次的现场（比如 bplus 的两个候选名指向同一个文件）必须先挡掉：
    // wipe 会把源一起清掉。
    if source_path == target_path {
        return Err(format!("源与目标是同一个路径: {}", source_path.display()));
    }

    eprintln!("=== migrate_storage ===");
    eprintln!("from={from} ({})", source_path.display());
    eprintln!("to  ={to} ({})", target_path.display());
    eprintln!("include_deleted={include_deleted} wipe_target={wipe_target}");

    let src = open_source(from, &root)?;
    let dst = open_target(to, &root, wipe_target)?;
    let (stats, export_secs, import_secs) = pump(&*src, &*dst, include_deleted)?;
    seal(&*dst)?;
    let verify_live = count_visible(&*dst);
    drop(dst);
    drop(src);

    eprintln!(
        "EXPORT {} records (approx) in {:.3}s",
        stats.records, export_secs
    );
    eprintln!(
        "IMPORT records={} live={} deleted={} verify_live={} in {:.3}s",
        stats.records, stats.live, stats.deleted, verify_live, import_secs
    );

    Ok(MigrateStats {
        from,
        to,
        records: stats.records,
        live: stats.live,
        deleted: stats.deleted,
        verify_live,
        export_secs,
        import_secs,
        source_path,
        target_path,
    })
}
