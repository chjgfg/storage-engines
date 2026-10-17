//! 四个引擎共享的公共类型与编解码工具。
//!
//! 此前 `ExportRecord` / `SearchQuery` / `KeyMeta` 等类型在四个引擎里各有一份
//! 独立定义，是不同的 nominal type，无法被统一 trait 返回。这里收敛为唯一定义。

pub mod codec;
pub mod disk;
pub mod kernel;
pub mod kv_snapshot;
pub mod store;
pub mod txn;
pub mod types;
pub mod version;
pub mod wal;

pub use codec::{
    decode_value_meta, key_matches, logical_to_user, now_unix_secs, pack_plain, pack_ttl,
    prefix_upper_bound,
};
pub use types::{
    DurabilityProfile, EngineTuning, ExportRecord, IncrError, KeyMatchMode, KeyMeta, MvccHealth,
    SearchPage, SearchQuery, TuningEntry, VacuumRound, VacuumStats, ValueMeta,
};
pub use store::{
    RowValue, VersionedStore, collect_rows, scan_page, search_bounds, search_window,
};
pub use txn::{BulkCore, Registry, TxnCore, WalTail, WriteSide};
pub use version::{decode_key, encode_key, vacuum_plan, version_key_upper_bound, VersionedKey};

/// 测试专用的临时路径。**必须带进程内单调序号**，不能只靠时间戳。
///
/// Windows 的 `SystemTime` 粒度约 15.6 ms，`cargo test` 并发跑时两个线程可以拿到**同一个
/// nanos**，于是同名目录/文件 + 同一把 `.lock` ⇒ 后来者直接 `WouldBlock`，表现是随机偶发的
/// "打开数据文件失败"。这个坑在 bplus / bitcask / lsm / 门面四层里各复制过一份，
/// 现在收敛成一个函数（每份复制都要自己的守卫 —— 但守卫之外，路径规则不该复制）。
#[cfg(test)]
pub(crate) fn tmp_test_path(prefix: &str, tag: &str, suffix: &str) -> std::path::PathBuf {
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
pub use kernel::{
    fold_window, is_unfinished, search_page_from, visible_at, HealthFold, VacuumScheduler, WalPolicy,
    WinCandidate,
};