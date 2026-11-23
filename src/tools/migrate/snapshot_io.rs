//! 快照底层写出与 CRC 缓冲计量。

use super::*;

impl SnapshotFormat {
    /// 解析 CLI/环境变量字符串（`jsonl`/`json`/`bin`/`binary`，大小写不敏感）
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "jsonl" | "json" => Some(Self::Jsonl),
            "bin" | "binary" => Some(Self::Bin),
            _ => None,
        }
    }

    /// 用作文件扩展名 / 日志标签
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Jsonl => "jsonl",
            Self::Bin => "bin",
        }
    }
}

/// 仅导出到文件（流式：堆上最多一行 + 写缓冲，与库大小无关）
pub fn export_to_file(
    engine: EngineKind,
    data_root: impl AsRef<Path>,
    out: impl AsRef<Path>,
    format: SnapshotFormat,
    include_deleted: bool,
) -> Result<IoStats, String> {
    pump_out(
        &*open_source(engine, data_root.as_ref())?,
        out.as_ref(),
        format,
        include_deleted,
    )
}

/// 导出主体：一条流式出口接一个写口，两个入口（按 root / 按目录）共用这一份胶。
pub(super) fn pump_out(
    src: &dyn KvEngine,
    out: &Path,
    format: SnapshotFormat,
    include_deleted: bool,
) -> Result<IoStats, String> {
    let mut w = match format {
        SnapshotFormat::Jsonl => {
            ExportWriter::jsonl(out.as_ref()).map_err(|e| format!("建导出文件失败: {e}"))?
        }
        SnapshotFormat::Bin => {
            // 0.5.0 起改走 v3：001/002 里没有地方放到期时刻，而这条门是跨库载体。
            // 读侧只认魔数 ⇒ 老文件照旧能导，新文件多 8 字节/行。
            ExportWriter::bin_v3(out).map_err(|e| format!("建导出文件失败: {e}"))?
        }
    };
    let mut written = 0usize;
    let handed = src.export_latest_visible_for_each(include_deleted, &mut |rec| {
        if w.push(rec) {
            written += 1;
            true
        } else {
            false
        }
    });
    let stats = w.finish().map_err(|e| format!("写导出文件失败: {e}"))?;
    if handed != written {
        return Err(format!(
            "导出交出 {handed} 行、写出去 {written} 行 ⇒ 文件比库少行（写入器中途收手）"
        ));
    }
    Ok(stats)
}

/// 导出到文件：源**直接给引擎目录/库文件**（CLI 的 `--dir` 形状），不做 `{root}/{engine}` 拼接。
pub fn export_to_dir(
    engine: EngineKind,
    dir: impl AsRef<Path>,
    out: impl AsRef<Path>,
    format: SnapshotFormat,
    include_deleted: bool,
) -> Result<IoStats, String> {
    let src = open_source_at(engine, dir.as_ref())?;
    pump_out(&*src, out.as_ref(), format, include_deleted)
}

/// 从文件导入：目标**直接给引擎目录/库文件**（CLI 的 `--dir` 形状）。
pub fn import_from_dir(
    engine: EngineKind,
    dir: impl AsRef<Path>,
    input: impl AsRef<Path>,
    format: SnapshotFormat,
    wipe_target: bool,
) -> Result<IoStats, String> {
    let dst = open_target_at(engine, dir.as_ref(), wipe_target)?;
    let stats = pump_file_in(&*dst, input.as_ref(), format)?;
    finish_import(&*dst, stats)
}

/// 文件版的"补齐"式导入（M81 第 6 项尾巴 A）：**目标"见过"的键一个字都不动，没见过的才插**。
///
/// 与 [`import_from_dir`] 的区别只在这一条判据上 —— 那条是覆盖语义（同键后者胜），这一条不覆盖、
/// 不复活墓碑、也不刷新已有键的 TTL。三条口径与 [`sync`] **同一份**，不是又抄一遍判定：
/// ① "见过" = `get_meta(...).is_some()`（含墓碑；用 `exists()` 会复活已删键）；
/// ② 文件里的墓碑行**不进候选**（删除不传播 —— 与 `sync` 的 `include_deleted=false` 同一条）；
/// ③ 新插入的键按文件里记的**绝对到期时刻**重打信封（v3 / JSONL 的 `expire`；001/002 文件里没这
///    个字段 ⇒ 读回永久，见 `v1_snapshot_reads_back_as_permanent`）。
///
/// ⚠️ 走**普通事务**（一批一个事务）而不是 `begin_bulk`：判"见过"要按键读目标，而 bulk 那条门
/// 是"绕过冲突检查"的，两件事凑一个句柄会把读到的快照与写入的可见性混成两套 —— 与 [`sync`] 同理由。
pub fn import_skip_existing_from_dir(
    engine: EngineKind,
    dir: impl AsRef<Path>,
    input: impl AsRef<Path>,
    format: SnapshotFormat,
) -> Result<SyncStats, String> {
    let dst = open_target_at(engine, dir.as_ref(), false)?;
    let mut stats = SyncStats::default();
    let mut batch: Vec<(Vec<u8>, Vec<u8>, Option<u64>)> = Vec::with_capacity(BATCH);
    let mut err: Option<String> = None;
    let flush = |batch: &mut Vec<(Vec<u8>, Vec<u8>, Option<u64>)>,
                 stats: &mut SyncStats|
     -> Result<(), String> {
        if batch.is_empty() {
            return Ok(());
        }
        let drained = std::mem::replace(batch, Vec::with_capacity(BATCH));
        let tx = dst.begin();
        let now = now_unix_secs();
        for (key, user, expire) in drained {
            if tx.get_meta(&key).is_some() {
                stats.skipped_existing += 1;
                continue;
            }
            let ok = match expire {
                Some(e) if e > now => {
                    let ok = tx.set_with_ttl(&key, user, Duration::from_secs(e - now));
                    if ok {
                        stats.inserted_with_ttl += 1;
                    }
                    ok
                }
                Some(_) => {
                    stats.skipped_expired += 1;
                    continue;
                }
                None => tx.set_raw(&key, user),
            };
            if !ok {
                return Err(format!(
                    "补齐导入写入失败（冲突、容量限制或存储错误）（键 {:?}）⇒ 这一批不提交",
                    String::from_utf8_lossy(&key)
                ));
            }
            stats.inserted += 1;
        }
        tx.try_commit().map_err(|e| e.to_string())?;
        Ok(())
    };
    let on = &mut |rec: KvRecord| -> bool {
        // 判据②：文件里的墓碑行不进候选（`value = None`）。它既不是"要插的东西"，
        // 也不该被当成"目标缺这把键"的证据 —— 删除不传播。
        let Some(user) = rec.value else {
            return true;
        };
        stats.scanned += 1;
        batch.push((rec.key, user, rec.expire_unix_secs));
        if batch.len() >= BATCH {
            if let Err(e) = flush(&mut batch, &mut stats) {
                err = Some(e);
                return false;
            }
        }
        true
    };
    let read = match format {
        SnapshotFormat::Jsonl => read_jsonl_with(input.as_ref(), on),
        SnapshotFormat::Bin => read_bin_with(input.as_ref(), on),
    };
    if let Err(e) = read {
        return Err(format!("读快照文件失败: {e}"));
    }
    if err.is_none() {
        if let Err(e) = flush(&mut batch, &mut stats) {
            err = Some(e);
        }
    }
    if let Some(e) = err {
        return Err(e);
    }
    let judged = stats.inserted + stats.skipped_existing + stats.skipped_expired;
    if judged != stats.scanned {
        return Err(format!(
            "文件交出 {} 行、目标判定 {judged} 行 ⇒ 有行被静默丢掉",
            stats.scanned
        ));
    }
    dst.flush().map_err(|e| format!("flush 失败: {e}"))?;
    dst.checkpoint()
        .map_err(|e| format!("checkpoint 失败: {e}"))?;
    Ok(stats)
}

///
/// ⚠️ 这里**只**挡"整批没落进去"这一格：`KvBulk::put` 没有返回值通道（见文件头那条已知局限），
/// 超出一页载荷的行只能 `log::error!` 喊出来、传不上来。不做"回读条数 == 文件条数"的硬判据，
/// 导入收尾：落盘、挡住"整批没落进去"，两个入口共用一份（这条判断抄两遍迟早有一份不判）。
///
/// ⚠️ 这里**只**挡"整批没落进去"这一格：`KvBulk::put` 没有返回值通道（见文件头那条已知局限），
/// 超出一页载荷的行只能 `log::error!` 喊出来、传不上来。不做"回读条数 == 文件条数"的硬判据，
/// 因为快照文件允许同一个键出现多行（后到者胜），那种文件本来就该报更少的存活条数 —— 拿它当
/// 断言会假红。真要逐键校验请用 [`migrate`]（它的 `verify_live` 与源侧同定义）或运维脚本抽查。
pub(super) fn finish_import(dst: &dyn KvEngine, stats: IoStats) -> Result<IoStats, String> {
    seal(dst)?;
    if stats.live > 0 && count_visible(dst) == 0 {
        return Err(format!(
            "导入自称写了 {} 条存活，回读一把键都没有 ⇒ 整批没落进去",
            stats.live
        ));
    }
    Ok(stats)
}

/// 从文件导入到引擎（流式：读一条灌一条）
pub fn import_from_file(
    engine: EngineKind,
    data_root: impl AsRef<Path>,
    input: impl AsRef<Path>,
    format: SnapshotFormat,
    wipe_target: bool,
) -> Result<IoStats, String> {
    let dst = open_target(engine, data_root.as_ref(), wipe_target)?;
    let stats = pump_file_in(&*dst, input.as_ref(), format)?;
    finish_import(&*dst, stats)
}
/// 文件 → bulk 的那一段。两条读路共用同一个灌法（一份写侧逻辑，两种载体）。
pub(super) fn pump_file_in(
    dst: &dyn KvEngine,
    input: &Path,
    format: SnapshotFormat,
) -> Result<IoStats, String> {
    let mut bulk = dst.begin_bulk();
    let mut batch: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(BATCH);
    let on = &mut |rec: KvRecord| -> bool {
        match rec.value {
            Some(v) => {
                // 文件里带 expire（v3 / JSONL 的 `expire` 字段）就按它重打信封；不带 = 永久。
                // ⚠️ 已过期但仍在文件里的行**照原样落库**（到期时刻在过去 ⇒ 读出来是"不存在"，
                // 回收会带走），不在这里做"顺手丢掉"的决定 —— 导入侧替用户判过期就等于
                // 悄悄少搬了几把键。
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
            None => bulk.delete(&rec.key),
        }
        true
    };
    let stats = match format {
        SnapshotFormat::Jsonl => {
            read_jsonl_with(input, on).map_err(|e| format!("读 jsonl 失败: {e}"))?
        }
        SnapshotFormat::Bin => read_bin_with(input, on).map_err(|e| format!("读 bin 失败: {e}"))?,
    };
    if !batch.is_empty() {
        bulk.put_batch_owned(batch);
    }
    bulk.finish();
    Ok(stats)
}
