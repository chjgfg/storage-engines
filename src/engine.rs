//! 统一引擎门面：对象安全的 [`KvEngine`] / [`KvTxn`] / [`KvBulk`]。
//!
//! 四个引擎的 `MVCC` / `Transaction` / `BulkLoader` 结构体互不相同（各自的
//! 后端、WAL、blob 都是独立类型），但对外暴露的方法签名高度一致。本模块把
//! 这份一致性提取成 trait，于是可以按**运行时字符串**选引擎：
//!
//! ```no_run
//! use storage_engines::engine::open_engine;
//! use std::path::Path;
//!
//! let db = open_engine("lsm", Some(Path::new("./data/lsm")))?;
//! let tx = db.begin();
//! tx.set(b"k", b"v".to_vec());
//! tx.commit();
//! # Ok::<(), storage_engines::engine::EngineError>(())
//! ```
//!
//! # 设计要点
//!
//! - **对象安全**：所有方法都不带泛型参数，`&[u8]` / `Vec<u8>` 取代
//!   `impl AsRef<[u8]>`，因此 `Box<dyn KvEngine>` 可用。
//! - **生命周期**：[`KvEngine::begin`] 返回 `Box<dyn KvTxn + '_>`，事务借用
//!   引擎，避免把 `MVCC` 的内部 `Arc` 再包一层。
//! - **引擎差异**在适配层吸收，不污染各引擎自身实现：
//!   - `memory` 的 `vacuum` 不返回 `io::Result` → 适配时包成 `Ok`
//!   - `memory` 没有 `flush` / `checkpoint` → 实现为 no-op
//!   - `bplus_tree` 的 `open` 需要 `order` / `max_cache_pages` → 走
//!     [`EngineConfig`]
//!   - 只有 `bitcask` 有 `merge` → 默认实现返回 [`EngineError::Unsupported`]

use std::{fmt, write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::common::{
    ExportRecord, IncrError, KeyMeta, MvccHealth, SearchPage, SearchQuery, VacuumStats,
};

/// 门面层错误
#[derive(Debug)]
pub enum EngineError {
    /// 引擎名字不认识（见 [`ENGINE_NAMES`]）
    UnknownEngine(String),
    /// 该引擎不支持此操作（如 `memory` 不能持久化、非 bitcask 不能 merge）
    Unsupported {
        engine: &'static str,
        op: &'static str,
    },
    /// 这个引擎必须落盘，但调用方没给路径
    PathRequired(&'static str),
    Io(std::io::Error),
}

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EngineError::UnknownEngine(name) => write!(
                f,
                "unknown engine {name:?}; expected one of {}",
                ENGINE_NAMES.join(" / ")
            ),
            EngineError::Unsupported { engine, op } => {
                write!(f, "engine {engine} does not support {op}")
            }
            EngineError::PathRequired(engine) => {
                write!(f, "engine {engine} requires a database path")
            }
            EngineError::Io(e) => write!(f, "io error: {e}"),
        }
    }
}

impl std::error::Error for EngineError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            EngineError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for EngineError {
    fn from(e: std::io::Error) -> Self {
        EngineError::Io(e)
    }
}

pub type Result<T> = std::result::Result<T, EngineError>;

/// 可用引擎名（[`open_engine`] 接受的规范名）
pub const ENGINE_NAMES: &[&str] = &["bitcask", "bplus_tree", "lsm_tree", "memory"];

/// 打开引擎的可调参数。
///
/// 只有 `bplus_tree` 会用到 `order` / `max_cache_pages`，其余引擎忽略。
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// B+ 树阶数
    pub order: usize,
    /// B+ 树页缓存上限（页数）
    pub max_cache_pages: usize,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            order: 64,
            max_cache_pages: 1024,
        }
    }
}

/// 一个只读/读写事务（快照隔离）。
///
/// 方法语义与各引擎的 `Transaction` 完全一致：`set` / `delete` 返回 `false`
/// 表示写冲突，此时应当 [`KvTxn::rollback`]。
pub trait KvTxn {
    // ---- 基本读写 ----

    /// 本事务的版本号 / xid
    fn version(&self) -> u64;

    /// 写入；`false` = 写冲突
    fn set(&self, key: &[u8], value: Vec<u8>) -> bool;

    /// 带 TTL 写入；`false` = 写冲突
    fn set_with_ttl(&self, key: &[u8], value: Vec<u8>, ttl: Duration) -> bool;

    /// 逻辑删除（写 tombstone）；`false` = 写冲突
    fn delete(&self, key: &[u8]) -> bool;

    /// 读取用户 value（已剥 TTL 信封；过期/删除/不存在均为 `None`）
    fn get(&self, key: &[u8]) -> Option<Vec<u8>>;

    fn exists(&self, key: &[u8]) -> bool;

    fn get_meta(&self, key: &[u8]) -> Option<KeyMeta>;

    // ---- 批量 ----

    /// 批量写；`Err(key)` 表示在该 key 上写冲突（已写入的部分不回滚，
    /// 调用方通常应 [`KvTxn::rollback`] 整个事务）
    fn batch_set(&self, items: &[(Vec<u8>, Vec<u8>)]) -> std::result::Result<(), Vec<u8>>;

    fn batch_delete(&self, keys: &[Vec<u8>]) -> std::result::Result<(), Vec<u8>>;

    fn multi_get(&self, keys: &[Vec<u8>]) -> Vec<Option<Vec<u8>>>;

    // ---- 计数器 ----

    fn incr(&self, key: &[u8], delta: i64) -> std::result::Result<i64, IncrError>;

    fn decr(&self, key: &[u8], delta: i64) -> std::result::Result<i64, IncrError>;

    // ---- 扫描 / 搜索 ----

    /// 范围扫描 `[start, end)`；`None` 表示该端无界
    fn scan(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Vec<ExportRecord>;

    fn reverse_scan(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Vec<ExportRecord>;

    fn prefix_scan(&self, prefix: &[u8]) -> Vec<ExportRecord>;

    fn seek(&self, key: &[u8]) -> Option<ExportRecord>;

    fn seek_prev(&self, key: &[u8]) -> Option<ExportRecord>;

    fn search_keys(&self, query: &SearchQuery) -> SearchPage;

    fn key_count(&self, prefix: &[u8]) -> usize;

    // ---- TTL ----

    /// `None` = key 不存在；`Some(None)` = 永久；`Some(Some(d))` = 剩余时间
    fn get_ttl(&self, key: &[u8]) -> Option<Option<Duration>>;

    fn refresh_ttl(&self, key: &[u8], ttl: Duration) -> bool;

    /// 去掉 TTL 变永久
    fn persist_key(&self, key: &[u8]) -> bool;

    /// 物理清掉本快照可见的已过期 key，返回清理条数
    fn purge_expired(&self) -> usize;

    // ---- 导出 / 结束 ----

    fn export_latest_visible(&self, include_deleted: bool) -> Vec<ExportRecord>;

    fn commit(&self);

    fn rollback(&self);
}

/// 批量导入会话：无冲突检测、无 WAL，**单写者**假定。
///
/// 结束时必须 [`KvBulk::finish`]（`Drop` 也会兜底，但不保证 flush 顺序）。
pub trait KvBulk {
    fn put(&mut self, key: &[u8], value: Vec<u8>);

    fn put_batch(&mut self, items: &[(Vec<u8>, Vec<u8>)]);

    /// 省一次 clone 的批量写
    fn put_batch_owned(&mut self, items: Vec<(Vec<u8>, Vec<u8>)>);

    fn delete(&mut self, key: &[u8]);

    fn get(&self, key: &[u8]) -> Option<Vec<u8>>;

    /// 消费式收尾。`Box<Self>` 让它在 `dyn` 下仍可调用。
    fn finish(self: Box<Self>);
}

/// 存储引擎门面
pub trait KvEngine {
    /// 规范引擎名（`"bitcask"` / `"bplus_tree"` / `"lsm_tree"` / `"memory"`）
    fn name(&self) -> &'static str;

    /// 库路径；纯内存引擎为 `None`
    fn path(&self) -> Option<&Path>;

    /// 开启事务
    fn begin(&self) -> Box<dyn KvTxn + '_>;

    /// 开启批量导入会话
    fn begin_bulk(&self) -> Box<dyn KvBulk + '_>;

    /// 导出每个逻辑 key 的最新可见版本
    fn export_latest_visible(&self, include_deleted: bool) -> Vec<ExportRecord>;

    /// 回收旧版本
    fn vacuum(&self) -> Result<VacuumStats>;

    /// 全库健康度快照：物理行数、逻辑 key 数、**最长版本链**、活跃事务数。
    ///
    /// 这是给长跑进程做采样用的观测面 —— 这次事故的唯一征兆就是「某把热点 key 的
    /// 版本链长度」，而它此前只能靠人工数日志得到。四引擎口径一致，换引擎时指标可直接对比。
    ///
    /// 代价是 O(物理行数)（引擎内部只走元数据/键，不物化 value），所以**别放进热路径**，
    /// 按分钟级采样足够。
    fn health(&self) -> MvccHealth;

    /// 落盘数据（内存引擎为 no-op）
    fn flush(&self) -> Result<()>;

    /// 写检查点并截断 WAL（内存引擎为 no-op）
    fn checkpoint(&self) -> Result<()>;

    /// 合并/压实底层文件。仅 `bitcask` 支持，其余返回
    /// [`EngineError::Unsupported`]。
    fn merge(&self) -> Result<()> {
        Err(EngineError::Unsupported {
            engine: self.name(),
            op: "merge",
        })
    }
}

/// 按名字打开引擎，用默认 [`EngineConfig`]。
///
/// `path` 对三个磁盘引擎必填；`"memory"` 忽略该参数。
/// 名字大小写不敏感，且接受常见别名：`"bplus"` / `"btree"` / `"lsm"` / `"mem"`。
pub fn open_engine(name: &str, path: Option<&Path>) -> Result<Box<dyn KvEngine>> {
    open_engine_with(name, path, &EngineConfig::default())
}

/// 按名字打开引擎，显式给参数
pub fn open_engine_with(
    name: &str,
    path: Option<&Path>,
    config: &EngineConfig,
) -> Result<Box<dyn KvEngine>> {
    let canon = canonical_name(name)
        .ok_or_else(|| EngineError::UnknownEngine(name.to_string()))?;

    // 三个磁盘引擎必须有路径
    let disk_path = |engine: &'static str| -> Result<PathBuf> {
        path.map(|p| p.to_path_buf())
            .ok_or(EngineError::PathRequired(engine))
    };

    Ok(match canon {
        "bitcask" => Box::new(BitcaskEngine {
            inner: crate::bitcask::MVCC::try_open(disk_path("bitcask")?)?,
        }),
        "bplus_tree" => Box::new(BPlusTreeEngine {
            inner: crate::bplus_tree::MVCC::try_open(
                disk_path("bplus_tree")?,
                config.order,
                config.max_cache_pages,
            )?,
        }),
        "lsm_tree" => Box::new(LsmTreeEngine {
            inner: crate::lsm_tree::MVCC::try_open(disk_path("lsm_tree")?)?,
        }),
        "memory" => Box::new(MemoryEngine {
            inner: crate::memory::MVCC::new(),
        }),
        _ => unreachable!("canonical_name 已校验"),
    })
}

/// 把别名归一到规范名
fn canonical_name(name: &str) -> Option<&'static str> {
    match name.trim().to_ascii_lowercase().replace('-', "_").as_str() {
        "bitcask" => Some("bitcask"),
        "bplus_tree" | "bplus" | "b_plus_tree" | "btree" | "b_tree" => Some("bplus_tree"),
        "lsm_tree" | "lsm" => Some("lsm_tree"),
        "memory" | "mem" | "in_memory" => Some("memory"),
        _ => None,
    }
}

/// 为一个引擎生成 `KvEngine` / `KvTxn` / `KvBulk` 三个适配器。
///
/// 各引擎方法名一致，差异通过这几个开关吸收：
/// - `vacuum: io` → 直接透传 `io::Result`；`vacuum: plain` → 包成 `Ok`
/// - `durable: yes` → 转发 `flush` / `checkpoint`；`no` → no-op
/// - `path: $method` → 取库路径的方法名；`none` → 纯内存
/// - `merge: yes` → 转发 `merge`；`no` → 用 trait 默认（`Unsupported`）
macro_rules! impl_engine {
    (
        engine_struct: $engine:ident,
        txn_struct: $txn:ident,
        bulk_struct: $bulk:ident,
        mvcc: $mvcc:ty,
        txn_inner: $txn_inner:ty,
        bulk_inner: $bulk_inner:ty,
        name: $name:literal,
        path: $path:tt,
        vacuum: $vacuum:tt,
        durable: $durable:tt,
        merge: $merge:tt,
    ) => {
        struct $engine {
            inner: $mvcc,
        }

        struct $txn<'a> {
            inner: $txn_inner,
            _engine: std::marker::PhantomData<&'a ()>,
        }

        struct $bulk<'a> {
            inner: $bulk_inner,
            _engine: std::marker::PhantomData<&'a ()>,
        }

        impl KvEngine for $engine {
            fn name(&self) -> &'static str {
                $name
            }

            impl_engine!(@path self, $path);

            fn begin(&self) -> Box<dyn KvTxn + '_> {
                Box::new($txn {
                    inner: self.inner.begin_transaction(),
                    _engine: std::marker::PhantomData,
                })
            }

            fn begin_bulk(&self) -> Box<dyn KvBulk + '_> {
                Box::new($bulk {
                    inner: self.inner.begin_bulk(),
                    _engine: std::marker::PhantomData,
                })
            }

            fn export_latest_visible(&self, include_deleted: bool) -> Vec<ExportRecord> {
                self.inner.export_latest_visible(include_deleted)
            }

            fn health(&self) -> MvccHealth {
                self.inner.health()
            }

            impl_engine!(@vacuum $vacuum);
            impl_engine!(@durable $durable);
            impl_engine!(@merge $merge);
        }

        impl<'a> KvTxn for $txn<'a> {
            fn version(&self) -> u64 {
                self.inner.version()
            }

            fn set(&self, key: &[u8], value: Vec<u8>) -> bool {
                self.inner.set(key, value)
            }

            fn set_with_ttl(&self, key: &[u8], value: Vec<u8>, ttl: Duration) -> bool {
                self.inner.set_with_ttl(key, value, ttl)
            }

            fn delete(&self, key: &[u8]) -> bool {
                self.inner.delete(key)
            }

            fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
                self.inner.get(key)
            }

            fn exists(&self, key: &[u8]) -> bool {
                self.inner.exists(key)
            }

            fn get_meta(&self, key: &[u8]) -> Option<KeyMeta> {
                self.inner.get_meta(key)
            }

            fn batch_set(
                &self,
                items: &[(Vec<u8>, Vec<u8>)],
            ) -> std::result::Result<(), Vec<u8>> {
                self.inner.batch_set(items)
            }

            fn batch_delete(&self, keys: &[Vec<u8>]) -> std::result::Result<(), Vec<u8>> {
                self.inner.batch_delete(keys)
            }

            fn multi_get(&self, keys: &[Vec<u8>]) -> Vec<Option<Vec<u8>>> {
                self.inner.multi_get(keys)
            }

            fn incr(&self, key: &[u8], delta: i64) -> std::result::Result<i64, IncrError> {
                self.inner.incr(key, delta)
            }

            fn decr(&self, key: &[u8], delta: i64) -> std::result::Result<i64, IncrError> {
                self.inner.decr(key, delta)
            }

            fn scan(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Vec<ExportRecord> {
                self.inner.scan(start, end)
            }

            fn reverse_scan(
                &self,
                start: Option<&[u8]>,
                end: Option<&[u8]>,
            ) -> Vec<ExportRecord> {
                self.inner.reverse_scan(start, end)
            }

            fn prefix_scan(&self, prefix: &[u8]) -> Vec<ExportRecord> {
                self.inner.prefix_scan(prefix)
            }

            fn seek(&self, key: &[u8]) -> Option<ExportRecord> {
                self.inner.seek(key)
            }

            fn seek_prev(&self, key: &[u8]) -> Option<ExportRecord> {
                self.inner.seek_prev(key)
            }

            fn search_keys(&self, query: &SearchQuery) -> SearchPage {
                self.inner.search_keys(query)
            }

            fn key_count(&self, prefix: &[u8]) -> usize {
                self.inner.key_count(prefix)
            }

            fn get_ttl(&self, key: &[u8]) -> Option<Option<Duration>> {
                self.inner.get_ttl(key)
            }

            fn refresh_ttl(&self, key: &[u8], ttl: Duration) -> bool {
                self.inner.refresh_ttl(key, ttl)
            }

            fn persist_key(&self, key: &[u8]) -> bool {
                self.inner.persist(key)
            }

            fn purge_expired(&self) -> usize {
                self.inner.purge_expired()
            }

            fn export_latest_visible(&self, include_deleted: bool) -> Vec<ExportRecord> {
                self.inner.export_latest_visible(include_deleted)
            }

            fn commit(&self) {
                self.inner.commit()
            }

            fn rollback(&self) {
                self.inner.rollback()
            }
        }

        impl<'a> KvBulk for $bulk<'a> {
            fn put(&mut self, key: &[u8], value: Vec<u8>) {
                self.inner.put(key, value)
            }

            fn put_batch(&mut self, items: &[(Vec<u8>, Vec<u8>)]) {
                self.inner.put_batch(items)
            }

            fn put_batch_owned(&mut self, items: Vec<(Vec<u8>, Vec<u8>)>) {
                self.inner.put_batch_owned(items)
            }

            fn delete(&mut self, key: &[u8]) {
                self.inner.delete(key)
            }

            fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
                self.inner.get(key)
            }

            fn finish(self: Box<Self>) {
                (*self).inner.finish()
            }
        }
    };

    // ---- path 分支 ----
    (@path $self:ident, none) => {
        fn path(&self) -> Option<&Path> {
            None
        }
    };
    (@path $self:ident, $method:ident) => {
        fn path(&self) -> Option<&Path> {
            Some(self.inner.$method())
        }
    };

    // ---- vacuum 分支 ----
    (@vacuum io) => {
        fn vacuum(&self) -> Result<VacuumStats> {
            Ok(self.inner.vacuum()?)
        }
    };
    (@vacuum plain) => {
        fn vacuum(&self) -> Result<VacuumStats> {
            Ok(self.inner.vacuum())
        }
    };

    // ---- flush / checkpoint 分支 ----
    (@durable yes) => {
        fn flush(&self) -> Result<()> {
            self.inner.flush();
            Ok(())
        }

        fn checkpoint(&self) -> Result<()> {
            self.inner.checkpoint();
            Ok(())
        }
    };
    (@durable no) => {
        /// 纯内存引擎：无盘可落
        fn flush(&self) -> Result<()> {
            Ok(())
        }

        /// 纯内存引擎：无 WAL 可截断
        fn checkpoint(&self) -> Result<()> {
            Ok(())
        }
    };

    // ---- merge 分支 ----
    (@merge yes) => {
        fn merge(&self) -> Result<()> {
            self.inner.merge()?;
            Ok(())
        }
    };
    // 不支持 merge：留给 trait 默认实现（返回 Unsupported）
    (@merge no) => {};
}

impl_engine! {
    engine_struct: BitcaskEngine,
    txn_struct: BitcaskTxn,
    bulk_struct: BitcaskBulk,
    mvcc: crate::bitcask::bitcask_mvcc::MVCC,
    txn_inner: crate::bitcask::bitcask_mvcc::Transaction,
    bulk_inner: crate::bitcask::bitcask_mvcc::BulkLoader,
    name: "bitcask",
    path: db_dir,
    vacuum: io,
    durable: yes,
    merge: yes,
}

impl_engine! {
    engine_struct: BPlusTreeEngine,
    txn_struct: BPlusTreeTxn,
    bulk_struct: BPlusTreeBulk,
    mvcc: crate::bplus_tree::bplus_tree_mvcc::MVCC,
    txn_inner: crate::bplus_tree::bplus_tree_mvcc::Transaction,
    bulk_inner: crate::bplus_tree::bplus_tree_mvcc::BulkLoader,
    name: "bplus_tree",
    path: db_path,
    vacuum: io,
    durable: yes,
    merge: no,
}

impl_engine! {
    engine_struct: LsmTreeEngine,
    txn_struct: LsmTreeTxn,
    bulk_struct: LsmTreeBulk,
    mvcc: crate::lsm_tree::lsm_tree_mvcc::MVCC,
    txn_inner: crate::lsm_tree::lsm_tree_mvcc::Transaction,
    bulk_inner: crate::lsm_tree::lsm_tree_mvcc::BulkLoader,
    name: "lsm_tree",
    path: db_dir,
    vacuum: io,
    durable: yes,
    merge: no,
}

impl_engine! {
    engine_struct: MemoryEngine,
    txn_struct: MemoryTxn,
    bulk_struct: MemoryBulk,
    mvcc: crate::memory::memory_mvcc::MVCC,
    txn_inner: crate::memory::memory_mvcc::Transaction,
    bulk_inner: crate::memory::memory_mvcc::BulkLoader,
    name: "memory",
    path: none,
    vacuum: plain,
    durable: no,
    merge: no,
}

// 只有 bitcask 支持 merge：宏生成的 impl 用了 trait 的默认实现（返回
// Unsupported），这里没法再开一个 `impl KvEngine for BitcaskEngine`，所以
// merge 的 override 由宏的 `merge` 开关注入，见 impl_engine! 的调用点。

#[cfg(test)]
mod tests {
    use super::*;

    /// 在临时目录里跑一遍所有磁盘引擎 + memory
    ///
    /// 计数器不可省：Windows 上 `SystemTime` 的粒度约 15.6ms，而 `open_all()` 生成的
    /// 目录名只带 tag —— 并发跑的两个用同一 tag 的测试会拿到**同一个目录、同一把 .lock**，
    /// 后来者直接 `WouldBlock`（表现为随机的偶发红）。
    fn tmp_dir(tag: &str) -> PathBuf {
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("se_engine_{tag}_{nanos}_{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn open_all() -> Vec<(&'static str, Box<dyn KvEngine>, Option<PathBuf>)> {
        let mut out: Vec<(&'static str, Box<dyn KvEngine>, Option<PathBuf>)> = Vec::new();
        for name in ["bitcask", "bplus_tree", "lsm_tree"] {
            let dir = tmp_dir(name);
            let db = open_engine(name, Some(&dir)).expect("open disk engine");
            out.push((name, db, Some(dir)));
        }
        out.push(("memory", open_engine("memory", None).unwrap(), None));
        out
    }

    fn cleanup(dirs: Vec<Option<PathBuf>>) {
        for d in dirs.into_iter().flatten() {
            let _ = std::fs::remove_dir_all(&d);
        }
    }

    #[test]
    fn canonical_name_accepts_aliases() {
        assert_eq!(canonical_name("LSM"), Some("lsm_tree"));
        assert_eq!(canonical_name("bplus-tree"), Some("bplus_tree"));
        assert_eq!(canonical_name(" mem "), Some("memory"));
        assert_eq!(canonical_name("btree"), Some("bplus_tree"));
        assert_eq!(canonical_name("nope"), None);
    }

    /// `Box<dyn KvEngine>` 没有 `Debug`，`unwrap_err()` 用不了，手动取错误
    fn expect_err(r: Result<Box<dyn KvEngine>>) -> EngineError {
        match r {
            Ok(_) => panic!("expected an error, got an engine"),
            Err(e) => e,
        }
    }

    #[test]
    fn unknown_engine_is_reported() {
        let err = expect_err(open_engine("redis", None));
        assert!(matches!(err, EngineError::UnknownEngine(n) if n == "redis"));
    }

    #[test]
    fn disk_engines_require_path() {
        for name in ["bitcask", "bplus_tree", "lsm_tree"] {
            let err = expect_err(open_engine(name, None));
            assert!(
                matches!(err, EngineError::PathRequired(_)),
                "{name} should require a path"
            );
        }
    }

    #[test]
    fn all_engines_roundtrip_set_get_delete() {
        let engines = open_all();
        let mut dirs = Vec::new();
        for (name, db, dir) in engines {
            assert_eq!(db.name(), name);

            let tx = db.begin();
            assert!(tx.set(b"a", b"1".to_vec()), "{name}: set");
            assert_eq!(tx.get(b"a"), Some(b"1".to_vec()), "{name}: get");
            assert!(tx.exists(b"a"), "{name}: exists");
            assert!(tx.delete(b"a"), "{name}: delete");
            assert_eq!(tx.get(b"a"), None, "{name}: get after delete");
            tx.commit();

            dirs.push(dir);
        }
        cleanup(dirs);
    }

    #[test]
    fn all_engines_support_batch_and_scan() {
        let engines = open_all();
        let mut dirs = Vec::new();
        for (name, db, dir) in engines {
            let tx = db.begin();
            tx.batch_set(&[
                (b"k1".to_vec(), b"v1".to_vec()),
                (b"k2".to_vec(), b"v2".to_vec()),
                (b"k3".to_vec(), b"v3".to_vec()),
            ])
            .expect("batch_set");

            let got = tx.multi_get(&[b"k1".to_vec(), b"k9".to_vec()]);
            assert_eq!(got, vec![Some(b"v1".to_vec()), None], "{name}: multi_get");

            let scanned = tx.scan(Some(b"k1"), Some(b"k3"));
            let keys: Vec<_> = scanned.iter().map(|r| r.key.clone()).collect();
            assert_eq!(keys, vec![b"k1".to_vec(), b"k2".to_vec()], "{name}: scan");

            let pre = tx.prefix_scan(b"k");
            assert_eq!(pre.len(), 3, "{name}: prefix_scan");

            tx.commit();
            dirs.push(dir);
        }
        cleanup(dirs);
    }

    #[test]
    fn all_engines_detect_write_conflict() {
        let engines = open_all();
        let mut dirs = Vec::new();
        for (name, db, dir) in engines {
            let t1 = db.begin();
            let t2 = db.begin();
            assert!(t1.set(b"k", b"1".to_vec()), "{name}: t1 set");
            assert!(!t2.set(b"k", b"2".to_vec()), "{name}: t2 must conflict");
            t1.commit();
            t2.rollback();
            dirs.push(dir);
        }
        cleanup(dirs);
    }

    #[test]
    fn all_engines_bulk_load() {
        let engines = open_all();
        let mut dirs = Vec::new();
        for (name, db, dir) in engines {
            {
                let mut bulk = db.begin_bulk();
                bulk.put(b"a", b"1".to_vec());
                bulk.put_batch(&[(b"b".to_vec(), b"2".to_vec())]);
                bulk.put_batch_owned(vec![(b"c".to_vec(), b"3".to_vec())]);
                bulk.finish();
            }
            let tx = db.begin();
            assert_eq!(tx.get(b"a"), Some(b"1".to_vec()), "{name}: bulk a");
            assert_eq!(tx.get(b"b"), Some(b"2".to_vec()), "{name}: bulk b");
            assert_eq!(tx.get(b"c"), Some(b"3".to_vec()), "{name}: bulk c");
            tx.commit();
            dirs.push(dir);
        }
        cleanup(dirs);
    }

    #[test]
    fn all_engines_ttl_and_meta() {
        let engines = open_all();
        let mut dirs = Vec::new();
        for (name, db, dir) in engines {
            let tx = db.begin();
            assert!(tx.set_with_ttl(b"t", b"v".to_vec(), Duration::from_secs(60)));
            let ttl = tx.get_ttl(b"t").expect("{name}: key exists");
            assert!(ttl.is_some(), "{name}: should have a TTL");

            assert!(tx.persist_key(b"t"), "{name}: persist");
            assert_eq!(tx.get_ttl(b"t"), Some(None), "{name}: now permanent");

            let meta = tx.get_meta(b"t").expect("{name}: meta");
            assert_eq!(meta.key, b"t".to_vec());
            assert!(!meta.deleted, "{name}: not deleted");
            tx.commit();
            dirs.push(dir);
        }
        cleanup(dirs);
    }

    #[test]
    fn all_engines_vacuum_and_flush() {
        let engines = open_all();
        let mut dirs = Vec::new();
        for (name, db, dir) in engines {
            let t1 = db.begin();
            t1.set(b"k", b"v1".to_vec());
            t1.commit();
            let t2 = db.begin();
            t2.set(b"k", b"v2".to_vec());
            t2.commit();

            db.flush().unwrap_or_else(|e| panic!("{name}: flush: {e}"));
            db.checkpoint()
                .unwrap_or_else(|e| panic!("{name}: checkpoint: {e}"));
            let stats = db.vacuum().unwrap_or_else(|e| panic!("{name}: vacuum: {e}"));
            assert!(stats.xmin > 0, "{name}: xmin");

            let tx = db.begin();
            assert_eq!(tx.get(b"k"), Some(b"v2".to_vec()), "{name}: after vacuum");
            tx.commit();
            dirs.push(dir);
        }
        cleanup(dirs);
    }

    #[test]
    fn memory_engine_has_no_path_and_no_merge() {
        let db = open_engine("memory", None).unwrap();
        assert!(db.path().is_none());
        assert!(matches!(
            db.merge(),
            Err(EngineError::Unsupported { op: "merge", .. })
        ));
    }

    #[test]
    fn disk_engines_expose_their_path() {
        let dir = tmp_dir("path");
        let db = open_engine("bitcask", Some(&dir)).unwrap();
        assert_eq!(db.path(), Some(dir.as_path()));
        drop(db);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn engine_is_object_safe_in_collections() {
        // 关键回归：Box<dyn KvEngine> 必须能放进容器并统一驱动
        let dirs: Vec<PathBuf> = vec![tmp_dir("obj1")];
        let mut engines: Vec<Box<dyn KvEngine>> = Vec::new();
        engines.push(open_engine("memory", None).unwrap());
        engines.push(open_engine("lsm", Some(&dirs[0])).unwrap());

        for db in &engines {
            let tx = db.begin();
            tx.set(b"x", b"y".to_vec());
            tx.commit();
        }
        for db in &engines {
            let tx = db.begin();
            assert_eq!(tx.get(b"x"), Some(b"y".to_vec()), "{}", db.name());
            tx.commit();
        }
        drop(engines);
        cleanup(dirs.into_iter().map(Some).collect());
    }

    /// 门面必须让上层**不看引擎源码**就能问出「最长版本链有多长」。
    ///
    /// 换引擎只是换了涨速（LSM 一天、Bitcask 一个多月），盯的指标是同一个 ——
    /// 所以四家在这一组断言上必须给出完全相同的读数，否则跨引擎对比没有意义。
    #[test]
    fn all_engines_report_the_same_health() {
        const CHAIN: u64 = 100;

        let engines = open_all();
        let mut dirs = Vec::new();
        for (name, db, dir) in engines {
            dirs.push(dir);
            for i in 0..CHAIN {
                let tx = db.begin();
                assert!(tx.set(b"hot", format!("v{i}").into_bytes()), "{name} 第 {i} 次写入被拒");
                tx.commit();
            }
            for k in 0..2u64 {
                let tx = db.begin();
                assert!(tx.set(format!("other{k}").as_bytes(), b"x".to_vec()));
                tx.commit();
            }

            let h = db.health();
            assert_eq!(h.live_keys, 3, "{name}: 逻辑 key 数");
            assert_eq!(h.total_rows, CHAIN + 2, "{name}: 物理行数（含历史版本）");
            assert_eq!(h.max_chain, CHAIN, "{name}: 最长版本链应由库自己报出来");
            assert_eq!(h.rows_above_one, CHAIN - 1, "{name}: 多余行数");
            assert_eq!(h.active_txns, 0, "{name}: 无活跃事务");

            db.vacuum().expect("vacuum");
            let after = db.health();
            assert_eq!(after.max_chain, 1, "{name}: vacuum 后最长链应降到 1");
            assert_eq!(after.total_rows, 3, "{name}: vacuum 后应只剩每 key 一行");

            drop(db);
        }
        cleanup(dirs);
    }
}
