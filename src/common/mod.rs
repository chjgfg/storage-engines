//! 四个引擎共享的公共类型与编解码工具。
//!
//! 此前 `ExportRecord` / `SearchQuery` / `KeyMeta` 等类型在四个引擎里各有一份
//! 独立定义，是不同的 nominal type，无法被统一 trait 返回。这里收敛为唯一定义。

pub mod codec;
pub mod disk;
pub mod kernel;
pub mod kv_snapshot;
pub(crate) mod order;
pub mod store;
pub mod txn;
pub mod types;
pub mod version;
pub mod wal;

pub use codec::{
    decode_value_meta, key_matches, logical_is_live, logical_to_user, now_unix_secs, pack_plain,
    pack_ttl, prefix_upper_bound,
};
// M35 的尺子：用户字节被拷出来过几条（守卫拿它与"只数不物化"的那条路对拍）
#[cfg(test)]
pub(crate) use codec::user_value_copies;
// M44 的尺子：**收集门**（`fold_window`）最多同时攒了几行 ⇒ 流式那趟必须读到 0
#[cfg(test)]
pub(crate) use kernel::{collect_held_max, reset_collect_held};
pub use types::{
    DurabilityProfile, EngineTuning, ExportRecord, IncrError, KeyMatchMode, KeyMeta, MvccHealth,
    ReclamationStatus, SearchPage, SearchQuery, TuningEntry, VacuumRound, VacuumStats, ValueMeta,
};
pub use store::{
    RowValue, VersionedStore, back_n_with, collect_rows, collect_rows_as, count_window,
    export_rows_streaming, fold_lower_bound, fold_rows_streaming, live_record_of_logical,
    scan_live_records, scan_page, search_bounds, search_page, search_page_rows, search_window,
    seek_prev_with,
};
#[cfg(test)]
pub use store::{back_n_candidates, back_n_fallbacks};
pub use txn::{BulkCore, Registry, TxnCore, WalTail, WriteSide};
pub use version::{
    decode_key, encode_key, vacuum_plan, vacuum_plan_group, version_key_upper_bound, VersionedKey,
};

/// 测试专用的临时路径。**必须带进程内单调序号**，不能只靠时间戳。
///
/// Windows 的 `SystemTime` 粒度约 15.6 ms，`cargo test` 并发跑时两个线程可以拿到**同一个
/// nanos**，于是同名目录/文件 + 同一把 `.lock` ⇒ 后来者直接 `WouldBlock`，表现是随机偶发的
/// "打开数据文件失败"。这个坑在 bplus / bitcask / lsm / 门面四层里各复制过一份，
/// 现在收敛成一个函数（每份复制都要自己的守卫 —— 但守卫之外，路径规则不该复制）。
#[cfg(test)]
pub(crate) fn tmp_test_path(prefix: &str, tag: &str, suffix: &str) -> std::path::PathBuf {
    use std::format;
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let n = SEQ.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!(
        "{prefix}_{}_{tag}_{nanos}_{n}{suffix}",
        std::process::id()
    ))
}

/// **会自己消失**的临时路径（M82）：`Deref<Target = Path>`，测试把它绑在局部变量上就行，
/// 用例结束（正常收尾**或 panic 展开**）时析构里把路径本身与它的同名兄弟一起删掉。
///
/// 为什么要这个类型：一次全量 `cargo test` 会往 `%TEMP%` 漏 **158 个条目 / 25 MB**
/// （各模块的 `cleanup()` 得靠人记得写、panic 提前返回就漏），越积越多。
/// ⚠️ 别指望"挂在线程退出的钩子"那种写法：libtest 收尾走的是 `process::exit`，
/// 线程局部变量的析构**根本不会跑**（这条踩过，见 CHANGELOG M82）。RAII 必须由用例自己持有。
///
/// 兄弟文件一并删：引擎的旁挂（`.wal`/`.dblwr`/`.freelist`/`.lock`/`.blob`/`.hint`/`.merge`…）
/// 与主文件同前缀 —— 名字里带 pid+纳秒+序号，前缀匹配不会误伤别次运行。
#[cfg(test)]
pub(crate) struct ScratchPath {
    path: std::path::PathBuf,
    /// 去掉调用方给的后缀之后的基名：兄弟文件按它匹配
    base: String,
}

#[cfg(test)]
impl ScratchPath {
    /// 造一条 scratch 路径（`prefix`/`tag`/`suffix` 与 [`tmp_test_path`] 同义）
    pub(crate) fn new(prefix: &str, tag: &str, suffix: &str) -> Self {
        let path = tmp_test_path(prefix, tag, suffix);
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or_default().to_string();
        let base = name.strip_suffix(suffix).unwrap_or(&name).to_string();
        Self { path, base }
    }
}

#[cfg(test)]
impl std::ops::Deref for ScratchPath {
    type Target = std::path::Path;
    fn deref(&self) -> &std::path::Path {
        &self.path
    }
}

#[cfg(test)]
impl AsRef<std::path::Path> for ScratchPath {
    fn as_ref(&self) -> &std::path::Path {
        &self.path
    }
}

#[cfg(test)]
impl Drop for ScratchPath {
    fn drop(&mut self) {
        remove_scratch(&self.path, &self.base);
    }
}

/// 删一条 scratch：路径本身（目录或文件）+ 同前缀的兄弟（引擎的旁挂文件）。
/// 删不掉就静默放过 —— 兜底清理不该让测试结果变红。
#[cfg(test)]
fn remove_scratch(path: &std::path::Path, base: &str) {
    let _ = std::fs::remove_dir_all(path);
    let _ = std::fs::remove_file(path);
    let Some(dir) = path.parent() else { return };
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    for e in entries.flatten() {
        let name = e.file_name();
        let Some(name) = name.to_str() else { continue };
        if name.len() > base.len() && name.starts_with(base) {
            let p = e.path();
            let _ = if p.is_dir() {
                std::fs::remove_dir_all(&p)
            } else {
                std::fs::remove_file(&p)
            };
        }
    }
}

pub use kernel::{
    fold_window, fold_window_emit, is_unfinished, search_page_build, search_page_from,
    search_page_from_with_total,
    visible_at, HealthFold,
    VacuumScheduler, VacuumTick, WalPolicy,
    WinCandidate,
};
