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

use std::eprintln;
use std::format;
use std::path::{Path, PathBuf};
use std::time::Instant;

use super::kv_snapshot::{ExportWriter, IoStats, KvRecord, read_bin_with, read_jsonl_with};
use crate::common::codec::{now_unix_secs, pack_plain, pack_ttl};
use crate::engine::{EngineConfig, EngineKind, KvEngine, open_engine_with};
use std::time::Duration;

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

/// 跨库**补齐**式同步的账（M81 第 6 项后半）。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SyncStats {
    /// 源侧交出的存活行数（= `inserted + skipped_existing`，两条加起来必须等于它，见守卫）
    pub scanned: usize,
    /// 真插进目标的行数
    pub inserted: usize,
    /// 目标"见过"（含墓碑）而整个不动的行数
    pub skipped_existing: usize,
    /// 判定完已经跨过到期点的行数（**不写**：插一条注定看不见的行没有意义）
    pub skipped_expired: usize,
    /// 插入时带上 TTL 的行数（其余按永久写）
    pub inserted_with_ttl: usize,
}

#[cfg(test)]
mod tests;

// 按职责组织的内部实现；公开类型与原模块路径保持一致。
mod copy;
mod directories;
mod snapshot_io;
mod sync;
use copy::count_visible;
pub use copy::migrate;
pub use copy::parse_arrow;
use copy::seal;
pub use directories::bplus_db_path;
use directories::display_path;
pub use directories::engine_data_path;
#[cfg(test)]
use directories::migrate_config;
use directories::open_source;
use directories::open_source_at;
use directories::open_target;
use directories::open_target_at;
pub use snapshot_io::export_to_dir;
pub use snapshot_io::export_to_file;
pub use snapshot_io::import_from_dir;
pub use snapshot_io::import_from_file;
pub use snapshot_io::import_skip_existing_from_dir;
pub use sync::sync;
