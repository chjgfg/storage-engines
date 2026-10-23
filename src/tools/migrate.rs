//! 跨引擎搬家 + 快照文件 `export` / `import`，**一次实现**（走 [`crate::KvEngine`] 门面，四家共用）。
//!
//! 调用合同照下游 `trading-system/src/storage/tools/migrate.rs`：函数名、参数顺序、
//! [`MigrateStats`] 的字段名、以及 eprintln 的日志形状 —— 目标是下游把那份 `src/storage`
//! 整份删掉、换成依赖本 crate 时，`bin/migrate_storage.rs` 与 `tests/storage_migrate.rs`
//! **一行都不用改**。
//!
//! ## 与那份实现的三处口径差别（都是"更严"，不是"不一样"）
//! 1. **不物化整库**：那边 `export_records` 先把全库折成 `Vec<KvRecord>` 再灌（600k 档实测
//!    峰值 +123.7 MB，见 M44）。这里走 [`crate::KvEngine::export_latest_visible_for_each`]，
//!    折一行交一行 ⇒ 峰值与库大小无关，页宽 1000 只影响一次事务/批量调用的次数。
//! 2. **`bulk` 收的是 logical 字节（已打值信封），不是用户字节** —— 这条本轮**实测翻案**：
//!    四家的 `BulkLoader::put*` 都只做"存储层包装"（bitcask/bplus 加 `VAL_TAG_INLINE` 或走 blob 旁路、
//!    lsm/memory 直存），**没有**人打 MVCC 的信封；而读侧 `get` 一律 `logical_to_user` 剥一层。
//!    所以灌进去的必须是 `pack_plain(user)`。守卫 `tools_import_leaves_no_double_envelope`
//!    用一把以 `0x00` 打头的值钉这一格：少包一层 ⇒ 首字节被当标签吃掉（`[00,'v',01,02]` 读回 `[ 'v',01,02]`）；
//!    多包一层 ⇒ 读出来多一个 `0x00`。**顺带说明现有四个 MVCC `import_kv` bin 有同一处洞**（它们把
//!    文件里的用户字节直接交给 bulk），已在 M64 登记，不在这一节里偷偷改口径。
//! 3. **`verify_live` 用 `health().live_keys` 数**，不再"为了数一个数把整库再导一遍"（M35/M47 同一条纪律）。
//!
//! ## 现场约定（照下游，不是照本仓库别处）
//! - bplus 的库文件名先认 **`data.db`**（下游主进程与运维脚本都用这个名字），本仓库早先
//!   `StorageConfig::open` 用的是 `bplus.db` ⇒ 这里两个都认，**建库时按 `data.db`**（见 [`bplus_db_path`]）。
//! - 迁移开 bplus 用树阶 16、页缓存 64 页 —— 与下游主进程同一组数（它 `mvcc.rs` 里也是 16），
//!   否则同一份 `data.db` 会被两个不同的阶数来回打开。要换档就先 `open_engine_with` 自己开、
//!   再用 [`crate::migrate`]（门面级，吃句柄不吃目录名）。
//! - `memory` 不能当源也不能当目标（退出即丢，搬过去等于把生产库指向一个空句柄）。
//! - ⚠️ 继承的一条已知局限：`KvBulk::put` **没有返回值通道**，所以超出一页载荷的行（bplus）只能
//!   `log::error!` 喊出来、不会让 `import` 失败。要"当场失败"得先给 bulk 加通道（未做）。

use std::format;
use std::path::{Path, PathBuf};
use std::time::Instant;

use super::kv_snapshot::{
    read_bin_with, read_jsonl_with, ExportWriter, IoStats, KvRecord,
};
use crate::common::codec::pack_plain;
use crate::engine::{open_engine_with, EngineConfig, EngineKind, KvEngine};

/// bplus 的候选库文件名，**按优先级**：`data.db`（下游约定，也是这里新建时用的名字）
/// 在前，`bplus.db`（本仓库 `StorageConfig` 早先的约定）在后。
///
/// 为什么不是"各造一个"：同一目录里同时留下两个文件 = 同一个库有两份互不相干的历史，
/// 下一次开库选哪个变成运气。所以 [`bplus_db_path`] 只在**一个都不存在**时才新建第一个。
const BPLUS_DB_NAMES: [&str; 2] = ["data.db", "bplus.db"];
/// 迁移开 bplus 的树阶（与下游主进程一致）
const BPLUS_ORDER: usize = 16;
/// 迁移开 bplus 的页缓存页数（与下游主进程一致）
const BPLUS_CACHE: usize = 64;
/// 一次 bulk 交的批大小（与下游那份同值：换库不该逼运维重调这个数）
const BATCH: usize = 4096;

/// 中间快照文件的格式
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotFormat {
    /// 可读 JSON Lines（base64 key/value）
    Jsonl,
    /// 紧凑二进制 `BPEXP001`（带 seq 的是 `BPEXP002`）
    Bin,
}

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
/// 一次 [`migrate`] 的汇总账。字段名与下游那份一致（`bin/migrate_storage.rs` 直接打印它们）。
#[derive(Debug, Clone)]
pub struct MigrateStats {
    /// 源引擎
    pub from: EngineKind,
    /// 目标引擎
    pub to: EngineKind,
    /// 处理过的记录总数（= live + deleted）
    pub records: usize,
    /// 带 value 的条数
    pub live: usize,
    /// tombstone 条数
    pub deleted: usize,
    /// 导入后回读的实际存活 key 数（应当 == `live`）
    pub verify_live: usize,
    /// 读源侧耗时（秒）
    pub export_secs: f64,
    /// 写目标侧耗时（秒）
    pub import_secs: f64,
    /// 源数据路径
    pub source_path: PathBuf,
    /// 目标数据路径
    pub target_path: PathBuf,
}

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

/// `{root}/{engine}/` 数据目录（与 `StorageConfig::engine_dir` 同一拼法）
pub fn engine_data_path(root: &Path, engine: EngineKind) -> PathBuf {
    root.join(engine.as_str())
}

/// bplus 的库文件路径：先认已存在的那个名字，一个都不存在时给 `data.db`（下游约定）。
///
/// ⚠️ 这个"先认已存在"就是这条缝的全部难点：选错等于**凭空造一个空库**，运维看到的是
/// "迁移成功、余额全没了"。守卫 `tools_migrate_uses_the_downstream_bplus_file_name` 钉这一格。
pub fn bplus_db_path(root: &Path) -> PathBuf {
    let dir = engine_data_path(root, EngineKind::BPlusTree);
    for name in BPLUS_DB_NAMES {
        let p = dir.join(name);
        if p.exists() {
            return p;
        }
    }
    dir.join(BPLUS_DB_NAMES[0])
}

/// 迁移/导入导出用的引擎档：bplus 的树阶与页缓存按下游主进程那组数。
fn migrate_config() -> EngineConfig {
    EngineConfig {
        order: BPLUS_ORDER,
        max_cache_pages: BPLUS_CACHE,
        ..Default::default()
    }
}

/// 日志展示用路径（bplus 指到库文件，其余指到引擎目录）
fn display_path(engine: EngineKind, root: &Path) -> PathBuf {
    match engine {
        EngineKind::BPlusTree => bplus_db_path(root),
        other => engine_data_path(root, other),
    }
}

/// 打开**已存在**的库（迁移源）。目录/文件不在就报错，绝不静默建一个空库当源。
fn open_source(engine: EngineKind, root: &Path) -> Result<Box<dyn KvEngine>, String> {
    if !engine.needs_disk() {
        return Err(
            "memory 无磁盘目录，不能作为 migrate 源。正式环境请用 bitcask/bplus_tree/lsm_tree".into(),
        );
    }
    let path = display_path(engine, root);
    if !path.exists() {
        let what = if engine == EngineKind::BPlusTree {
            "源库文件不存在"
        } else {
            "源目录不存在"
        };
        return Err(format!("{what}: {}", path.display()));
    }
    open_engine_with(engine, Some(&path), &migrate_config())
        .map_err(|e| format!("打开源 {engine} 失败: {e}"))
}

/// 擦掉 bplus 的一个库及其全部 sidecar（含 `*.blob.new` —— 那是 blob 重写中途留下的半成品，
/// 不清的话下一次开库会看见一份没人引用的旧 blob）。
fn wipe_bplus_files(dir: &Path) {
    for name in BPLUS_DB_NAMES {
        let stem = dir.join(name);
        for suf in ["", ".dblwr", ".wal", ".freelist", ".lock", ".blob", ".blob.new"] {
            let p = if suf.is_empty() {
                stem.clone()
            } else {
                PathBuf::from(format!("{}{suf}", stem.display()))
            };
            let _ = std::fs::remove_file(p);
        }
    }
}

/// 打开**目标**库（不存在就建）。`wipe = true` 先清空引擎目录。
fn open_target(engine: EngineKind, root: &Path, wipe: bool) -> Result<Box<dyn KvEngine>, String> {
    if !engine.needs_disk() {
        return Err(
            "memory 无磁盘目录，不能作为 migrate 目标。正式环境请用 bitcask/bplus_tree/lsm_tree".into(),
        );
    }
    let dir = engine_data_path(root, engine);
    if wipe {
        let _ = std::fs::remove_dir_all(&dir);
    }
    std::fs::create_dir_all(&dir).map_err(|e| format!("建目录 {:} 失败: {e}", dir.display()))?;
    let path = if engine == EngineKind::BPlusTree {
        // wipe 之后目录是空的，`bplus_db_path` 会给 `data.db`；不 wipe 时它认已存在的那个名字
        if wipe {
            dir.join(BPLUS_DB_NAMES[0])
        } else {
            let p = bplus_db_path(root);
            wipe_bplus_files(&dir);
            p
        }
    } else {
        dir.clone()
    };
    open_engine_with(engine, Some(&path), &migrate_config())
        .map_err(|e| format!("打开目标 {engine} 失败: {e}"))
}

/// 逐条把源交出来的行灌进目标的 bulk。返回 `(账, 写侧耗秒)`。
///
/// 为什么要自己数一遍：`KvEngine::export_latest_visible_for_each` 的返回值是"**交出去**几行"，
/// 而这里要的是"**写进去**几行"。两者不等 = 有一条被静默丢掉（正是 `push` 返回 `false`
/// 那种"共享了但没生效"的洞），所以调用点必须把两把尺子都对上。
fn pump(
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
                batch.push((rec.key, pack_plain(&v)));
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
fn count_visible(db: &dyn KvEngine) -> usize {
    let mut n = 0usize;
    db.export_latest_visible_for_each(false, &mut |_rec| {
        n += 1;
        true
    });
    n
}

/// 把目标收口干净再交还句柄：`flush` + `checkpoint`（bulk 走的是"关 WAL、不逐条 fsync"那条快路，
/// 收尾不落一次等于把整次搬家押在 WAL 上 —— 下游搬完就直接起进程，不留这个尾巴）。
fn seal(dst: &dyn KvEngine) -> Result<(), String> {
    dst.flush().map_err(|e| format!("迁移后 flush 失败: {e}"))?;
    dst.checkpoint()
        .map_err(|e| format!("迁移后 checkpoint 失败: {e}"))?;
    Ok(())
}

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

/// 仅导出到文件（流式：堆上最多一行 + 写缓冲，与库大小无关）
pub fn export_to_file(
    engine: EngineKind,
    data_root: impl AsRef<Path>,
    out: impl AsRef<Path>,
    format: SnapshotFormat,
    include_deleted: bool,
) -> Result<IoStats, String> {
    pump_out(&*open_source(engine, data_root.as_ref())?, out.as_ref(), format, include_deleted)
}

/// 导出主体：一条流式出口接一个写口，两个入口（按 root / 按目录）共用这一份胶。
fn pump_out(
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
            ExportWriter::bin(out, false).map_err(|e| format!("建导出文件失败: {e}"))?
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

/// 按**引擎目录/库文件**打开的源（CLI 的 `--dir` 给的就是这个，不是 `{root}/{engine}`）
fn open_source_at(engine: EngineKind, path: &Path) -> Result<Box<dyn KvEngine>, String> {
    if !engine.needs_disk() {
        return Err("memory 无磁盘目录，不能作为导出源".into());
    }
    if !path.exists() {
        return Err(format!("源不存在: {}", path.display()));
    }
    open_engine_with(engine, Some(path), &migrate_config())
        .map_err(|e| format!("打开 {} 失败: {e}", engine.as_str()))
}

/// 按**引擎目录/库文件**打开的目标（不存在就建；`wipe` 只清这一个目录）
fn open_target_at(
    engine: EngineKind,
    path: &Path,
    wipe: bool,
) -> Result<Box<dyn KvEngine>, String> {
    if !engine.needs_disk() {
        return Err("memory 无磁盘目录，不能作为导入目标".into());
    }
    if wipe {
        let dir = if engine == EngineKind::BPlusTree {
            path.parent().unwrap_or(path)
        } else {
            path
        };
        let _ = std::fs::remove_dir_all(dir);
        if engine == EngineKind::BPlusTree {
            std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
            wipe_bplus_files(dir);
        }
    }
    std::fs::create_dir_all(path.parent().unwrap_or(path)).map_err(|e| e.to_string())?;
    open_engine_with(engine, Some(path), &migrate_config())
        .map_err(|e| format!("打开 {} 失败: {e}", engine.as_str()))
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

/// 导入收尾：落盘、挡住"整批没落进去"，两个入口共用一份（这条判断抄两遍迟早有一份不判）。
///
/// ⚠️ 这里**只**挡"整批没落进去"这一格：`KvBulk::put` 没有返回值通道（见文件头那条已知局限），
/// 超出一页载荷的行只能 `log::error!` 喊出来、传不上来。不做"回读条数 == 文件条数"的硬判据，
/// 因为快照文件允许同一个键出现多行（后到者胜），那种文件本来就该报更少的存活条数 —— 拿它当
/// 断言会假红。真要逐键校验请用 [`migrate`]（它的 `verify_live` 与源侧同定义）或运维脚本抽查。
fn finish_import(dst: &dyn KvEngine, stats: IoStats) -> Result<IoStats, String> {
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
fn pump_file_in(dst: &dyn KvEngine, input: &Path, format: SnapshotFormat) -> Result<IoStats, String> {
    let mut bulk = dst.begin_bulk();
    let mut batch: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(BATCH);
    let on = &mut |rec: KvRecord| -> bool {
        match rec.value {
            Some(v) => {
                batch.push((rec.key, pack_plain(&v)));
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
        SnapshotFormat::Bin => {
            read_bin_with(input, on).map_err(|e| format!("读 bin 失败: {e}"))?
        }
    };
    if !batch.is_empty() {
        bulk.put_batch_owned(batch);
    }
    bulk.finish();
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::vec;
    use crate::common::kv_snapshot::{b64_encode, read_bin, read_jsonl, write_bin, write_jsonl};

    fn root(tag: &str) -> PathBuf {
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let d = std::env::temp_dir().join(format!("se_tools_{tag}_{nanos}_{n}"));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// 往某家直接灌几把键：走门面，**不碰 tools 层** —— 自己验自己不算守卫
    fn seed(engine: EngineKind, r: &Path, rows: &[(&[u8], &[u8])]) {
        let path = display_path(engine, r);
        std::fs::create_dir_all(if engine == EngineKind::BPlusTree {
            path.parent().unwrap()
        } else {
            path.as_path()
        })
        .unwrap();
        let db = open_engine_with(engine, Some(&path), &migrate_config()).unwrap();
        for (k, v) in rows {
            let tx = db.begin();
            // 走 `set_raw` 而不是 `set`：`set` 的合同是"收已打过值信封的字节"（README 那条
            // 值信封不对称），拿用户字节喂它就等于第一字节被当标签吃掉。
            assert!(
                tx.set_raw(k, v.to_vec()),
                "写入被拒: {}",
                String::from_utf8_lossy(k)
            );
            tx.commit();
        }
        db.flush().unwrap();
        db.checkpoint().unwrap();
    }

    fn read_back(engine: EngineKind, r: &Path, k: &[u8]) -> Option<Vec<u8>> {
        let path = display_path(engine, r);
        let db = open_engine_with(engine, Some(&path), &migrate_config()).unwrap();
        let tx = db.begin();
        let v = tx.get(k);
        tx.commit();
        v
    }

    /// 下游 `bin/migrate_storage.rs` 与 `tests/storage_migrate.rs` 用的就是这一条：换引擎搬一次，
    /// 值必须逐字节回来，`MigrateStats` 的字段必须齐（少一个字段下游就编译不过）。
    #[test]
    fn tools_migrate_carries_values_byte_for_byte() {
        let r = root("carry");
        // 0x00 / 0x01 打头的值是"信封不对称"那条的现形位置：多包一层、少包一层都在这里露
        let rows: &[(&[u8], &[u8])] = &[
            (b"strat:solusdt", br#"{"in_position":true}"#),
            (b"order:1", b"NEW"),
            (b"acct:spot:USDT", b"100.5"),
            (b"zero:lead", &[0x00, 0x01, 0x02]),
            (b"ttl:lead", &[0x01, 0x00, 0x00, 0x7f]),
        ];
        seed(EngineKind::Bitcask, &r, rows);
        let st = migrate(EngineKind::Bitcask, EngineKind::LsmTree, &r, false, false).unwrap();
        assert_eq!(st.records, rows.len());
        assert_eq!(st.live, rows.len());
        assert_eq!(st.deleted, 0);
        assert_eq!(st.verify_live, st.live, "目标里可见条数必须 == 写进去的存活条数");
        assert_eq!(st.from, EngineKind::Bitcask);
        assert_eq!(st.to, EngineKind::LsmTree);
        assert!(st.export_secs >= 0.0 && st.import_secs >= 0.0);
        assert_eq!(st.source_path, engine_data_path(&r, EngineKind::Bitcask));
        assert_eq!(st.target_path, display_path(EngineKind::LsmTree, &r));
        for (k, v) in rows {
            assert_eq!(
                read_back(EngineKind::LsmTree, &r, k).as_deref(),
                Some(*v),
                "{k:?} 搬完读不回来"
            );
        }
        // 再搬一家：bplus 的库文件名按下游约定（另有专门守卫钉那一格）
        let st2 = migrate(EngineKind::LsmTree, EngineKind::BPlusTree, &r, false, false).unwrap();
        assert_eq!(st2.verify_live, rows.len());
        for (k, v) in rows {
            assert_eq!(
                read_back(EngineKind::BPlusTree, &r, k).as_deref(),
                Some(*v),
                "{k:?} 搬进 bplus 后读不回来"
            );
        }
        let _ = std::fs::remove_dir_all(&r);
    }

    /// ⚠️ 这条钉的是**会不会双信封**：本 crate 的 `bulk` 自己打值信封，而下游那份是先
    /// `pack_plain` 再灌。照抄那一步 ⇒ 读出来带一层假信封（`get` 少剥一层）。
    #[test]
    fn tools_import_leaves_no_double_envelope() {
        let r = root("envelope");
        let out = r.join("snap.jsonl");
        let user: &[u8] = &[0x00, b'v', 0x01, 0x02];
        std::fs::write(
            &out,
            format!(
                "{{\"key\":\"{}\",\"value\":\"{}\"}}\n",
                b64_encode(b"k1"),
                b64_encode(user)
            ),
        )
        .unwrap();
        import_from_file(EngineKind::Bitcask, &r, &out, SnapshotFormat::Jsonl, true).unwrap();
        let got = read_back(EngineKind::Bitcask, &r, b"k1").expect("导入的键读不回来");
        assert_eq!(
            got,
            user.to_vec(),
            "读回来的必须正好是文件里那份用户字节（多一层/少一层信封都不行）"
        );
        let _ = std::fs::remove_dir_all(&r);
    }

    /// 下游主进程、运维脚本、它自己的测试都指 `bplus_tree/data.db`；本仓库 `StorageConfig`
    /// 早先指 `bplus.db`。选错等于凭空开一个空库 ⇒ "迁移成功、余额没了"。
    #[test]
    fn tools_migrate_uses_the_downstream_bplus_file_name() {
        let r = root("dbname");
        seed(EngineKind::Bitcask, &r, &[(b"a", b"1")]);
        migrate(EngineKind::Bitcask, EngineKind::BPlusTree, &r, false, false).unwrap();
        let dir = engine_data_path(&r, EngineKind::BPlusTree);
        assert!(dir.join("data.db").exists(), "新建的 bplus 库必须叫 data.db");
        assert!(
            !dir.join("bplus.db").exists(),
            "同一目录里不能留两个库文件（下一次开库选哪个变成运气）"
        );
        // 反向：源是 bplus 时也要从同一个名字找到，而不是报"源库文件不存在"
        let back = migrate(EngineKind::BPlusTree, EngineKind::LsmTree, &r, false, false);
        assert!(back.is_ok(), "从 data.db 当源迁移失败: {:?}", back.err());
        let _ = std::fs::remove_dir_all(&r);
    }

    /// 本仓库早先那个名字（`bplus.db`）也不能变孤儿：已存在就必须被认出来。
    #[test]
    fn tools_still_finds_the_old_bplus_file_name() {
        let r = root("oldname");
        std::fs::create_dir_all(engine_data_path(&r, EngineKind::BPlusTree)).unwrap();
        let legacy = r.join("bplus_tree").join("bplus.db");
        let db = open_engine_with(EngineKind::BPlusTree, Some(&legacy), &migrate_config()).unwrap();
        {
            let tx = db.begin();
            assert!(tx.set(b"legacy", b"v".to_vec()));
            tx.commit();
        }
        db.flush().unwrap();
        db.checkpoint().unwrap();
        drop(db);
        assert_eq!(bplus_db_path(&r), legacy, "只有 bplus.db 存在时必须认它，不能新建 data.db");
        assert_eq!(
            read_back(EngineKind::BPlusTree, &r, b"legacy").as_deref(),
            Some(&b"v"[..]),
            "从旧名字的库里读不回东西 = 迁移会把它当空库"
        );
        let _ = std::fs::remove_dir_all(&r);
    }

    /// `wipe_target` 要真清干净：老键没了、sidecar（含 blob 重写留下的 `*.blob.new`）也没了。
    #[test]
    fn tools_wipe_target_removes_old_keys_and_sidecars() {
        let r = root("wipe");
        seed(EngineKind::Bitcask, &r, &[(b"new", b"N")]);
        seed(EngineKind::BPlusTree, &r, &[(b"stale", b"S")]);
        let dir = engine_data_path(&r, EngineKind::BPlusTree);
        let junk = ["data.db.wal", "data.db.dblwr", "data.db.blob.new"];
        for j in junk {
            std::fs::write(dir.join(j), b"junk").unwrap();
        }
        migrate(EngineKind::Bitcask, EngineKind::BPlusTree, &r, false, true).unwrap();
        let db =
            open_engine_with(EngineKind::BPlusTree, Some(&dir.join("data.db")), &migrate_config())
                .unwrap();
        {
            let tx = db.begin();
            assert_eq!(tx.get(b"new").as_deref(), Some(&b"N"[..]));
            assert_eq!(
                tx.get(b"stale"),
                None,
                "wipe 之后老键还在 = 目标没清，搬完是个混合库"
            );
        }
        drop(db);
        for j in junk {
            let p = dir.join(j);
            // ⚠️ 不能断"文件不存在"：引擎开库后会自己重建 `.wal`/`.dblwr`。
            // 要断的是**那份垃圾内容不在了**（要么整个没了，要么已被新库改写）。
            let still_junk = std::fs::read(&p).map(|b| b == b"junk").unwrap_or(false);
            assert!(!still_junk, "sidecar {j} 里还是 wipe 之前那份垃圾内容");
        }
        let _ = std::fs::remove_dir_all(&r);
    }

    /// 箭头三种写法、别名、两条 memory 拒绝、同源拒绝、源目录不存在拒绝。
    #[test]
    fn tools_arrow_parsing_and_rejections() {
        assert_eq!(
            parse_arrow("bitcask->bplus_tree").unwrap(),
            (EngineKind::Bitcask, EngineKind::BPlusTree)
        );
        assert_eq!(
            parse_arrow("lsm\u{2192}bitcask").unwrap(),
            (EngineKind::LsmTree, EngineKind::Bitcask)
        );
        // 下游 `StorageEngine::parse` 认 `b+tree` 这种写法 ⇒ 这里必须同样认（换库不能悄悄换引擎）
        assert_eq!(
            parse_arrow("b+tree=>lsm").unwrap(),
            (EngineKind::BPlusTree, EngineKind::LsmTree)
        );
        assert!(parse_arrow("bitcask->bitcask").is_err());
        assert!(parse_arrow("nope->bitcask").is_err());

        let r = root("reject");
        seed(EngineKind::Bitcask, &r, &[(b"a", b"1")]);
        let e = migrate(EngineKind::Memory, EngineKind::Bitcask, &r, false, false).unwrap_err();
        assert!(e.contains("不能作为 migrate 源"), "{e}");
        let e = migrate(EngineKind::Bitcask, EngineKind::Memory, &r, false, false).unwrap_err();
        assert!(e.contains("不能作为 migrate 目标"), "{e}");
        let e = migrate(EngineKind::Bitcask, EngineKind::Bitcask, &r, false, false).unwrap_err();
        assert!(e.contains("源与目标相同"), "{e}");
        // 源目录不存在必须报错，不能当空库搬走（那等于把生产库"搬"没了）
        let e = migrate(EngineKind::LsmTree, EngineKind::Bitcask, &r, false, false).unwrap_err();
        assert!(e.contains("源目录不存在"), "{e}");
        let _ = std::fs::remove_dir_all(&r);
    }

    /// 与下游那份 `BPEXP001` **逐字节**兼容：手拼一份它的输出，本 crate 要读得一模一样；
    /// 反向写出来的字节也必须正好是那个形状（多一个 trailer 下游就读不动）。
    #[test]
    fn tools_bin_format_matches_downstream_bytes() {
        let r = root("bytes");
        let mut want: Vec<u8> = Vec::new();
        want.extend_from_slice(b"BPEXP001");
        let recs = vec![
            KvRecord::new(b"ka".to_vec(), Some(b"va".to_vec())),
            KvRecord::new(b"kb".to_vec(), None),
        ];
        for rec in &recs {
            want.extend_from_slice(&(rec.key.len() as u32).to_le_bytes());
            want.extend_from_slice(&rec.key);
            match &rec.value {
                Some(v) => {
                    want.extend_from_slice(&(v.len() as u32).to_le_bytes());
                    want.extend_from_slice(v);
                }
                None => want.extend_from_slice(&u32::MAX.to_le_bytes()),
            }
        }
        let f = r.join("down.bin");
        std::fs::write(&f, &want).unwrap();
        let (back, st) = read_bin(&f).unwrap();
        assert_eq!(back, recs, "读下游那份字节的结果不对");
        assert_eq!((st.records, st.live, st.deleted), (2, 1, 1));
        let out = r.join("ours.bin");
        write_bin(&out, &recs).unwrap();
        assert_eq!(std::fs::read(&out).unwrap(), want, "bin 格式与下游那份不再是同一串字节");
        // 把那份文件灌进引擎：tombstone 那行不能变成一条活键
        import_from_file(EngineKind::Bitcask, &r, &f, SnapshotFormat::Bin, true).unwrap();
        assert_eq!(read_back(EngineKind::Bitcask, &r, b"ka").as_deref(), Some(&b"va"[..]));
        assert_eq!(read_back(EngineKind::Bitcask, &r, b"kb"), None, "tombstone 被灌成活值了");
        let _ = std::fs::remove_dir_all(&r);
    }

    /// `read_bin` / `read_jsonl` 从"边读边攒"改成"收集壳" ⇒ 壳与流式版必须给同一个答案，
    /// 且提前收手时**只**交前 N 条（不钉这条，重构就可能悄悄改了口径）。
    #[test]
    fn tools_streaming_readers_agree_with_the_collecting_shells() {
        let r = root("readers");
        let recs: Vec<KvRecord> = (0..5u8)
            .map(|i| {
                KvRecord::new(
                    format!("k{i}").into_bytes(),
                    if i % 2 == 0 { Some(vec![i]) } else { None },
                )
            })
            .collect();
        let p_bin = r.join("a.bin");
        let p_jsonl = r.join("a.jsonl");
        write_bin(&p_bin, &recs).unwrap();
        write_jsonl(&p_jsonl, &recs).unwrap();
        for p in [&p_bin, &p_jsonl] {
            let read: fn(&Path) -> std::io::Result<(Vec<KvRecord>, IoStats)> =
                if p.extension().unwrap() == "bin" { read_bin } else { read_jsonl };
            let stream: fn(&Path, &mut dyn FnMut(KvRecord) -> bool) -> std::io::Result<IoStats> =
                if p.extension().unwrap() == "bin" { read_bin_with } else { read_jsonl_with };
            let (all, st_all) = read(p).unwrap();
            assert_eq!(all, recs, "{:?}: 收集壳读回来的不等于写出去的", p);
            assert_eq!(st_all.records, 5);
            let mut got = Vec::new();
            let st_two = stream(p, &mut |rec| {
                got.push(rec);
                got.len() < 2
            })
            .unwrap();
            assert_eq!(got.len(), 2, "{:?}: 提前收手还多读了", p);
            assert_eq!(got, recs[..2]);
            assert_eq!(st_two.records, 2, "{:?}: 收口那一条不该计进账", p);
        }
        let _ = std::fs::remove_dir_all(&r);
    }

    /// 带墓碑的全库往返：`include_deleted` 两边都要认，且不许把删掉的键搬成活的。
    #[test]
    fn tools_round_trip_keeps_tombstones_dead() {
        let r = root("tomb");
        seed(EngineKind::Bitcask, &r, &[(b"keep", b"K"), (b"drop", b"D")]);
        let src = open_engine_with(
            EngineKind::Bitcask,
            Some(&engine_data_path(&r, EngineKind::Bitcask)),
            &migrate_config(),
        )
        .unwrap();
        {
            let tx = src.begin();
            assert!(tx.delete(b"drop"));
            tx.commit();
        }
        src.flush().unwrap();
        src.checkpoint().unwrap();
        drop(src);
        let snap = r.join("s.bin");
        let st = export_to_file(EngineKind::Bitcask, &r, &snap, SnapshotFormat::Bin, true).unwrap();
        assert!(st.deleted >= 1, "带墓碑导出里没有墓碑 ⇒ include_deleted 没生效");
        import_from_file(EngineKind::LsmTree, &r, &snap, SnapshotFormat::Bin, true).unwrap();
        assert_eq!(read_back(EngineKind::LsmTree, &r, b"keep").as_deref(), Some(&b"K"[..]));
        assert_eq!(read_back(EngineKind::LsmTree, &r, b"drop"), None, "墓碑被搬成活键了");
        let _ = std::fs::remove_dir_all(&r);
    }

}
