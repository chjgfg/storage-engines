//! 数据搬运的引擎目录和目标准备。

use super::*;

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
pub(super) fn migrate_config() -> EngineConfig {
    EngineConfig {
        order: BPLUS_ORDER,
        max_cache_pages: BPLUS_CACHE,
        ..Default::default()
    }
}

/// 日志展示用路径（bplus 指到库文件，其余指到引擎目录）
pub(super) fn display_path(engine: EngineKind, root: &Path) -> PathBuf {
    match engine {
        EngineKind::BPlusTree => bplus_db_path(root),
        other => engine_data_path(root, other),
    }
}

/// 打开**已存在**的库（迁移源）。目录/文件不在就报错，绝不静默建一个空库当源。
pub(super) fn open_source(engine: EngineKind, root: &Path) -> Result<Box<dyn KvEngine>, String> {
    if !engine.needs_disk() {
        return Err(
            "memory 无磁盘目录，不能作为 migrate 源。正式环境请用 bitcask/bplus_tree/lsm_tree"
                .into(),
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
pub(super) fn wipe_bplus_files(dir: &Path) {
    for name in BPLUS_DB_NAMES {
        let stem = dir.join(name);
        for suf in [
            "",
            ".dblwr",
            ".wal",
            ".freelist",
            ".lock",
            ".blob",
            ".blob.new",
        ] {
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
pub(super) fn open_target(
    engine: EngineKind,
    root: &Path,
    wipe: bool,
) -> Result<Box<dyn KvEngine>, String> {
    if !engine.needs_disk() {
        return Err(
            "memory 无磁盘目录，不能作为 migrate 目标。正式环境请用 bitcask/bplus_tree/lsm_tree"
                .into(),
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

/// 按**引擎目录/库文件**打开的源（CLI 的 `--dir` 给的就是这个，不是 `{root}/{engine}`）
pub(super) fn open_source_at(engine: EngineKind, path: &Path) -> Result<Box<dyn KvEngine>, String> {
    // ⚠️ 这里**放开 memory**，而上面 `open_source`（migrate 那扇门）继续拒 —— 两件事不冲突：
    // migrate 的合同是"换布局的一次性搬运"，下游 `bin/migrate_storage.rs` 就写着"memory 不能当
    // 源/目标"，那条不能动；而文件这一侧从 M81 第 6 项起 memory **真的有一个目录**了
    // （启停快照 `<dir>/snapshot.bxp`），"无磁盘目录"这句理由已经不成立。
    // 拒掉它的代价是把统一 CLI 的四家变三家 —— 旧 `memory-export-kv` 能导出，收拢之后不能，
    // 那是收拢做少了而不是做多了。
    if !engine.needs_disk()
        && !path
            .join(crate::memory::memory_mvcc::SNAPSHOT_FILE)
            .exists()
    {
        return Err(format!(
            "memory 在这个目录里没有快照（{}），不能作为导出源；先给它一次启停或用 --demo",
            path.display()
        ));
    }
    if !path.exists() {
        return Err(format!("源不存在: {}", path.display()));
    }
    open_engine_with(engine, Some(path), &migrate_config())
        .map_err(|e| format!("打开 {} 失败: {e}", engine.as_str()))
}

/// 按**引擎目录/库文件**打开的目标（不存在就建；`wipe` 只清这一个目录）
pub(super) fn open_target_at(
    engine: EngineKind,
    path: &Path,
    wipe: bool,
) -> Result<Box<dyn KvEngine>, String> {
    // 与上面导出源同一口径：文件这一侧放开 memory（它有快照目录了），migrate 那扇门继续拒。
    // 导入到 memory 的形状是"读旧快照 + 灌新行 + Drop 时写回"，所以 `--wipe` 就是删快照文件。
    if !engine.needs_disk() && wipe {
        let _ = std::fs::remove_file(path.join(crate::memory::memory_mvcc::SNAPSHOT_FILE));
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
