//! 本文件负责按名称和配置打开引擎。

use super::*;

/// 打开引擎，用默认 [`EngineConfig`]。入参见 [`EngineName`]（枚举或字符串均可）。
///
/// `path` 对三个磁盘引擎必填；`memory` 忽略该参数。
/// 字符串大小写不敏感，且接受常见别名：`"bplus"` / `"btree"` / `"lsm"` / `"mem"`。
///
/// ```no_run
/// use storage_engines::engine::{open_engine, EngineKind};
/// use std::path::Path;
///
/// let a = open_engine("bplus_tree", Some(Path::new("./data/bplus_tree/bplus.db")));
/// let b = open_engine(EngineKind::BPlusTree, Some(Path::new("./data/bplus_tree/bplus.db")));
/// # drop((a, b));
/// ```
pub fn open_engine<N: EngineName>(name: N, path: Option<&Path>) -> Result<Box<dyn KvEngine>> {
    open_engine_with(name, path, &EngineConfig::default())
}

/// 打开引擎，显式给参数（哪些字段被哪个引擎读，见 [`EngineConfig`] 的对照表）。
pub fn open_engine_with<N: EngineName>(
    name: N,
    path: Option<&Path>,
    config: &EngineConfig,
) -> Result<Box<dyn KvEngine>> {
    let canon = name.engine_canon()?;

    // 三个磁盘引擎必须有路径
    let disk_path = |engine: &'static str| -> Result<PathBuf> {
        path.map(|p| p.to_path_buf())
            .ok_or(EngineError::PathRequired(engine))
    };

    let db: Box<dyn KvEngine> = match canon {
        "bitcask" => Box::new(BitcaskEngine {
            inner: crate::bitcask::MVCC::try_open_with(
                disk_path("bitcask")?,
                config.bitcask_max_file_bytes,
            )?,
        }),
        "bplus_tree" => Box::new(BPlusTreeEngine {
            inner: crate::bplus_tree::MVCC::try_open(
                disk_path("bplus_tree")?,
                config.order,
                config.max_cache_pages,
            )?,
        }),
        "lsm_tree" => Box::new(LsmTreeEngine {
            inner: crate::lsm_tree::MVCC::try_open_with(
                disk_path("lsm_tree")?,
                config.lsm.clone(),
            )?,
        }),
        "memory" => Box::new(MemoryEngine {
            // M81 第 6 项：**给了目录 = 启停快照**（open 读一次、`Drop` 写一次，运行期不碰文件）；
            // 没给目录 = 纯内存，与改前一字不差（四个 CLI 与所有 `open_engine("memory", None)`
            // 的调用方走这一支，`flush`/`checkpoint` 仍是 no-op ⇒ 台架里 memory 的 flush 格不变）。
            inner: match path {
                Some(dir) => crate::memory::MVCC::open_with_snapshot(dir)?,
                None => crate::memory::MVCC::new(),
            },
        }),
        _ => unreachable!("canonical_name 已校验"),
    };

    // 后两条是**跨引擎**的旋钮，不在任何一家的 open 签名里，所以先开库、再在把句柄交出去
    // 之前立刻应用 —— 下游拿到的必须已经是"他配过的那份"，不能存在"打开成功、阈值还没生效"
    // 的中间态。`?` 也是有意的：给 memory 设 WAL 阈值要当场失败，而不是静默通过。
    if let Some(bytes) = config.wal_checkpoint_bytes {
        db.set_wal_checkpoint_limit(bytes)?;
    }
    if let Some(n) = config.cache_auto_max {
        // 用 `?` 是有意的：给 memory/bitcask 配了这个旋钮要当场失败，
        // 不能让下游以为"我配过了"（同一个理由见上面 wal_checkpoint_bytes 那段）。
        db.set_cache_auto_max(n)?;
    }
    if let Some(rows) = config.vacuum_auto_slice_rows {
        db.set_vacuum_auto_slice(rows);
    }
    if let Some(bytes) = config.cache_budget_bytes {
        // 同 `cache_auto_max` 那一条的理由：`?` 是有意的 —— 给 memory/bitcask 配了预算档要**当场失败**，
        // 不能让下游以为"我配过了"（这档限制的是估计缓存容量，不是 RSS；仍须拒绝静默失效）。
        db.set_cache_budget_bytes(bytes)?;
    }
    Ok(db)
}
