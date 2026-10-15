//! 四个引擎共享的公共类型与编解码工具。
//!
//! 此前 `ExportRecord` / `SearchQuery` / `KeyMeta` 等类型在四个引擎里各有一份
//! 独立定义，是不同的 nominal type，无法被统一 trait 返回。这里收敛为唯一定义。

pub mod codec;
pub mod disk;
pub mod kernel;
pub mod kv_snapshot;
pub mod types;
pub mod version;
pub mod wal;

pub use codec::{
    decode_value_meta, key_matches, logical_to_user, now_unix_secs, pack_plain, pack_ttl,
    prefix_upper_bound,
};
pub use types::{
    DurabilityProfile, ExportRecord, IncrError, KeyMatchMode, KeyMeta, MvccHealth, SearchPage,
    SearchQuery, VacuumRound, VacuumStats, ValueMeta,
};
pub use version::{decode_key, encode_key, vacuum_plan, VersionedKey};
pub use kernel::{
    is_unfinished, visible_at, HealthFold, VacuumScheduler, WalPolicy,
};