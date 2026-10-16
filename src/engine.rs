//! 统一引擎门面：对象安全的 [`KvEngine`] / [`KvTxn`] / [`KvBulk`]。
//!
//! 四个引擎的 `MVCC` / `Transaction` / `BulkLoader` 结构体互不相同（各自的
//! 后端、WAL、blob 都是独立类型），但对外暴露的方法签名高度一致。本模块把
//! 这份一致性提取成 trait，于是可以按**枚举或运行时字符串**选引擎：
//!
//! ```no_run
//! use storage_engines::engine::{open_engine, EngineKind};
//! use std::path::Path;
//!
//! // 编译期就知道自己用哪个引擎
//! let db = open_engine(EngineKind::LsmTree, Some(Path::new("./data/lsm")))?;
//! // 引擎名来自配置/环境变量时
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
    DurabilityProfile, EngineTuning, ExportRecord, IncrError, KeyMeta, MvccHealth, SearchPage,
    SearchQuery, VacuumRound, VacuumStats,
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
    /// [`migrate`] 中止：目标侧拒收这一行（键/值装不进一页，或写冲突）
    MigrateAborted {
        key: Vec<u8>,
        reason: &'static str,
    },
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
            EngineError::MigrateAborted { key, reason } => {
                write!(f, "migrate aborted at key {key:?}: {reason}")
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

/// 打开引擎的可调参数：四个引擎的旋钮都在这一份里。
///
/// 每个引擎只读自己那几个字段，其余**忽略**（不是报错 —— 同一份配置换引擎是常态，
/// 就像 `StorageConfig` 里的 `data_dir`）。哪些字段真的生效，用
/// [`KvEngine::tuning`] 读回来核对：它列的就是"这个引擎现在生效的那些"。
///
/// | 字段 | 生效于 |
/// |---|---|
/// | `order` / `max_cache_pages` | 只有 `bplus_tree` |
/// | `lsm` | 只有 `lsm_tree` |
/// | `bitcask_max_file_bytes` | 只有 `bitcask` |
/// | `wal_checkpoint_bytes` | 三个磁盘引擎；`memory` **没有 WAL ⇒ 传了会报错** |
/// | `vacuum_auto_slice_rows` | 四引擎都有 |
///
/// ⚠️ 新增字段是**破坏性**的：按位置/全字段字面量构造 `EngineConfig { .. }` 的老代码要加
/// `..Default::default()`。
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// B+ 树阶数（默认 64）
    pub order: usize,
    /// B+ 树页缓存上限（页数，默认 1024）
    ///
    /// 库的物理页超过它时，全库走查改成"装得下才缓存"，不再挤掉 OLTP 的热页；
    /// 想让走查也全部命中，就把容量配到 ≥ 页数。
    pub max_cache_pages: usize,
    /// LSM 引擎旋钮：`None` = 用引擎自己的 MVCC 档 [`LSMTreeOptions::for_mvcc`]。
    ///
    /// 传进来的是**整份** options，所以常规写法是拿基线做 struct-update，
    /// 只改要改的那个字段：
    ///
    /// ```no_run
    /// use storage_engines::engine::{open_engine_with, EngineConfig, EngineKind};
    /// use storage_engines::LSMTreeOptions;
    /// use std::path::Path;
    ///
    /// let cfg = EngineConfig {
    ///     lsm: Some(LSMTreeOptions {
    ///         block_cache_blocks: 1024,
    ///         ..LSMTreeOptions::for_mvcc()
    ///     }),
    ///     ..Default::default()
    /// };
    /// let db = open_engine_with(
    ///     EngineKind::LsmTree,
    ///     Some(Path::new("./data/lsm_tree")),
    ///     &cfg,
    /// )?;
    /// # Ok::<(), storage_engines::EngineError>(())
    /// ```
    ///
    /// `enable_mem_wal` / `enable_bg_compact` 会被钉回 `false`（事务 WAL 是唯一的持久化点；
    /// MVCC 档的 compact 必须同步做），传了也不算 —— 这条由守卫
    /// `lsm_mvcc_pins_the_two_fields_that_would_break_crash_safety` 钉住。
    pub lsm: Option<crate::lsm_tree::LSMTreeOptions>,
    /// bitcask 单个 active 分片的软上限（字节）；`None` = 默认
    /// [`DEFAULT_MAX_FILE_BYTES`](crate::bitcask::DEFAULT_MAX_FILE_BYTES)。
    ///
    /// 影响的是 rotate 节奏与 `merge()` 的粒度：调小 ⇒ 分片多、每次 merge  rewrite 的量小、
    /// 但 open 时要发现的文件也多。
    pub bitcask_max_file_bytes: Option<u64>,
    /// WAL 长到这么多字节就在下一次提交后自动 checkpoint；`None` = 默认 16 MiB，`Some(0)` = 关闭。
    ///
    /// 崩溃恢复是**全量扫描** WAL，所以这条不设 = 启动一次比一次慢。
    /// `memory` 没有 WAL：给它传 `Some(..)` 会直接返回 [`EngineError::Unsupported`]，
    /// 不静默忽略（"以为设了阈值、其实跑的是没有 WAL 的引擎"不能靠默认值发生）。
    pub wal_checkpoint_bytes: Option<u64>,
    /// 自动回收的切片宽度（版本行数）；`None` = 默认 16 Ki 行，`Some(0)` = 关闭自动回收。
    pub vacuum_auto_slice_rows: Option<u64>,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            order: 64,
            max_cache_pages: 1024,
            lsm: None,
            bitcask_max_file_bytes: None,
            wal_checkpoint_bytes: None,
            vacuum_auto_slice_rows: None,
        }
    }
}

/// 一个只读/读写事务（快照隔离）。
///
/// 方法语义与各引擎的 `Transaction` 完全一致：`set` / `delete` 返回 `false`
/// 表示写冲突，此时应当 [`KvTxn::rollback`]。
/// 一个只读/读写事务（快照隔离）。
///
/// `Send` 是**必须**的：句柄要能跟着任务跨线程走（交易进程里典型是"主线程开库、
/// 工作线程读写"）。刻意**不要求 `Sync`** —— 事务内部有 `Cell`（WAL 是否已开）与
/// 写集，跨线程共享一个事务对象本身就没有语义：单写者模型下"共享"只会变成竞态。
pub trait KvTxn: Send {
    // ---- 基本读写 ----

    /// 本事务的版本号 / xid
    fn version(&self) -> u64;

    /// 写入；`false` = 写冲突
    ///
    /// ⚠️ `value` 是**已打过信封**的逻辑值（历史约定，现有调用点按它写）。直接拿这个口子
    /// 存裸字节会踩坑：`0x00` 打头读回来少 1 字节，`0x01` 打头会被当成 TTL 信封少 9 字节、
    /// 甚至被判"已过期"读成 `None`。存二进制请用 [`KvTxn::set_raw`]。
    fn set(&self, key: &[u8], value: Vec<u8>) -> bool;

    /// 原样存这串字节：信封由引擎打，[`KvTxn::get`] 一字不差地还回来（空值、`0x00`/`0x01`
    /// 打头、走 blob 旁路的长值都一样）。`false` 同 `set` = 写冲突。
    ///
    /// 为什么只加写的一侧：`get` 本来就已经把信封剥掉，它就是 `set_raw` 的读侧对应物；
    /// 再造一个 `get_raw` 只是给同一个行为起第二个名字，换来的是"该用哪个"的困惑。
    /// 确实需要"不经剥壳的存储字节"时，那是另一个能力（看 TTL 信封本身），目前只有引擎内部门
    /// （`latest_visible_raw`），没有对外承诺。
    fn set_raw(&self, key: &[u8], value: Vec<u8>) -> bool;

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

    /// 自增并给这个 key 设 TTL（`ttl` 是**绝对过期时刻**之前的存活时长）
    ///
    /// 为什么单列一个方法而不是"先 incr 再 refresh_ttl"：那是两个事务，中间崩了
    /// 会留下"计数加了、TTL 没设"的行；引擎内部是一次写集里做完的。
    fn incr_with_ttl(
        &self,
        key: &[u8],
        delta: i64,
        ttl: Duration,
    ) -> std::result::Result<i64, IncrError>;

    // ---- 扫描 / 搜索 ----

    /// 范围扫描 `[start, end)`；`None` 表示该端无界
    fn scan(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Vec<ExportRecord>;

    fn reverse_scan(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Vec<ExportRecord>;

    fn prefix_scan(&self, prefix: &[u8]) -> Vec<ExportRecord>;

    fn seek(&self, key: &[u8]) -> Option<ExportRecord>;

    fn seek_prev(&self, key: &[u8]) -> Option<ExportRecord>;

    /// 游标式有界扫描：`after`（**不含**）之后最多 `limit` 行，返回 `(行, 下一页游标)`。
    /// 游标 `None` = 已经到底。
    ///
    /// 为什么要有它：[`KvTxn::scan`] 返回的是 `Vec`，`scan(start, None)` 的代价与内存占用都按
    /// **"从 start 到库尾"** 算 —— 而"下一页"这个动作本来只需要 `limit` 行。翻页、迁移、
    /// 导出这类场景请用它，别拿 `scan` 的全量返回值切片。
    fn scan_page(&self, after: Option<&[u8]>, limit: usize) -> (Vec<ExportRecord>, Option<Vec<u8>>);

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
pub trait KvBulk: Send {
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
///
/// `Send + Sync`：句柄要能放进 `Arc` 跨线程共享（交易进程里"启动线程开库、
/// 心跳线程 checkpoint、工作线程读写"是常态）。少了这两个 supertrait，门面就只能
/// 在单线程里用 —— 那正是下游宁可自己在应用里再包一层的起因。
pub trait KvEngine: Send + Sync {
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

    /// 自动回收的切片宽度（版本行数）：每攒够这么多行新版本，引擎自己裁一片这么多行。
    ///
    /// 默认开着（四引擎同一个默认值 16 Ki 行）。**设 0 才是关闭** —— 那时版本行只增不减，
    /// 得靠上层定时调 [`KvEngine::vacuum`]。关掉是有意的取舍（比如只想在交易空档回收），
    /// 但不是缺省：这次的事故本质就是「没人喊，于是垃圾永久累积」。
    fn set_vacuum_auto_slice(&self, rows: u64);

    /// 读回当前的自动回收切片宽度（`0` = 已关闭）
    fn vacuum_auto_slice(&self) -> u64;

    /// 崩溃安全画像：哪些构件在、哪些不在。
    ///
    /// 四家**不等价**，而且差异不是"等级高低"能表达的：Bitcask 缺数据记录校验和（静默损坏
    /// 发现不了）、LSM 的大 value 旁路是裸文件、只有 B+ 树有原地页写 + 双写缓冲、memory
    /// 干脆不持久。选引擎之前先看它 —— 别再靠"换个引擎"当成修好了。
    fn durability_profile(&self) -> DurabilityProfile;

    /// 本引擎**当前生效**的可调参数快照。
    ///
    /// 存在的理由：`EngineConfig` 只能"给"，给错了以前是静默的（字段名记错、引擎根本不吃
    /// 这个旋钮）。这里给一条读回的路，各引擎只列自己有的键 ⇒ 它同时是"这个引擎能调什么"
    /// 的清单。键名与 [`EngineConfig`] 的字段名对齐（`order` / `max_cache_pages` /
    /// `block_cache_blocks` / `max_file_bytes` / `wal_checkpoint_bytes` / `vacuum_auto_slice`…）。
    fn tuning(&self) -> EngineTuning;

    /// 有界回收一小轮：从 `after` 这个逻辑 key 之后开始，最多读 `budget_rows` 行版本。
    ///
    /// 这是 [`KvEngine::vacuum`] 的"不锁全库版"：手动 `vacuum()` = 一次 `budget_rows =
    /// u64::MAX` 的轮次，两者共用同一份保留规则。返回的 `next_cursor` 原样传回就接着扫，
    /// `None` 表示已扫完全库（blob 回收/日志压缩只在这一轮才做）。
    ///
    /// 长跑进程要的是这个而不是 `vacuum()`：全库一轮的代价随库变大而变大，而切片一轮
    /// 只和 `budget_rows` 有关。
    fn vacuum_bounded(&self, after: Option<&[u8]>, budget_rows: u64) -> Result<VacuumRound>;

    /// WAL 自动 checkpoint 的字节阈值（`0` = 关闭自动触发，只能手动 [`KvEngine::checkpoint`]）。
    ///
    /// `memory` 没有 WAL ⇒ [`EngineError::Unsupported`]。
    fn set_wal_checkpoint_limit(&self, bytes: u64) -> Result<()>;

    /// 读回 WAL 自动 checkpoint 阈值；`memory` 同理会返回 [`EngineError::Unsupported`]。
    fn wal_checkpoint_limit(&self) -> Result<u64>;

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

/// 引擎种类：把"传哪个字符串"变成类型。
///
/// 之前只有 [`ENGINE_NAMES`] + 字符串解析，调用方十有八九会在应用里再封一层
/// `enum`（还顺手把 `memory` 误用于实盘）。这里给出类型化的选择器 + 两个谓词，
/// 让"选型"这件事在门面层就闭环。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineKind {
    Bitcask,
    BPlusTree,
    LsmTree,
    Memory,
}

impl EngineKind {
    /// 规范名（与 [`open_engine`] 接受的名字一致）
    pub fn as_str(self) -> &'static str {
        match self {
            EngineKind::Bitcask => "bitcask",
            EngineKind::BPlusTree => "bplus_tree",
            EngineKind::LsmTree => "lsm_tree",
            EngineKind::Memory => "memory",
        }
    }

    /// 解析名字，大小写不敏感且接受常见别名（`bplus` / `btree` / `lsm` / `mem`…）
    pub fn parse(s: &str) -> Option<Self> {
        match canonical_name(s)? {
            "bitcask" => Some(EngineKind::Bitcask),
            "bplus_tree" => Some(EngineKind::BPlusTree),
            "lsm_tree" => Some(EngineKind::LsmTree),
            "memory" => Some(EngineKind::Memory),
            _ => None,
        }
    }

    /// 是否需要磁盘路径（`memory` 不需要）
    pub fn needs_disk(self) -> bool {
        self != EngineKind::Memory
    }

    /// 能不能上生产。
    ///
    /// 返回 `false` 的只有一个：`memory` —— 它没有任何持久化（见
    /// [`KvEngine::durability_profile`]：无事务 WAL、无校验和），进程一退数据就没。
    /// 它是给教学/测试当对照用的，不是"更快的选项"。
    pub fn is_production_ok(self) -> bool {
        self != EngineKind::Memory
    }
}

impl std::fmt::Display for EngineKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// 「选哪个引擎 + 数据放哪 + 引擎参数」一份配置。
///
/// 环境变量约定与常见部署写法一致：`STORAGE_ENGINE`（默认 `bitcask`）、
/// `STORAGE_DATA_DIR`（默认 `./data`）。落盘位置是 `data_dir/<engine>/`，
/// 所以**换引擎不会自动带数据** —— 要过得走导出/导入（`export_kv` / `import_kv`）。
#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub kind: EngineKind,
    pub data_dir: PathBuf,
    pub engine: EngineConfig,
}

impl StorageConfig {
    /// 从环境变量读配置。
    ///
    /// `STORAGE_ENGINE` 写了但不认识 ⇒ **报错**，不静默退回默认值："以为在跑 bitcask、
    /// 其实跑的是 memory"这种事故不能靠默认值发生。
    pub fn from_env() -> Result<Self> {
        let raw = std::env::var("STORAGE_ENGINE").unwrap_or_else(|_| "bitcask".to_string());
        let kind = EngineKind::parse(&raw).ok_or_else(|| EngineError::UnknownEngine(raw.clone()))?;
        let data_dir = std::env::var("STORAGE_DATA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("./data"));
        Ok(StorageConfig {
            kind,
            data_dir,
            engine: EngineConfig::default(),
        })
    }

    /// 本引擎的数据目录：`data_dir/<engine>`
    pub fn engine_dir(&self) -> PathBuf {
        self.data_dir.join(self.kind.as_str())
    }

    /// 建好数据目录（幂等；memory 什么都不做）
    pub fn ensure_dirs(&self) -> Result<()> {
        if self.kind.needs_disk() {
            std::fs::create_dir_all(self.engine_dir())?;
        }
        Ok(())
    }

    /// 按本配置打开引擎（内部会先建目录）
    pub fn open(&self) -> Result<Box<dyn KvEngine>> {
        self.ensure_dirs()?;
        // B+ 树的"库"是一个文件，另两家是一个目录
        let path = match self.kind {
            EngineKind::Memory => None,
            EngineKind::BPlusTree => Some(self.engine_dir().join("bplus.db")),
            _ => Some(self.engine_dir()),
        };
        open_engine_with(self.kind, path.as_deref(), &self.engine)
    }
}

/// 便捷入口：设好 `STORAGE_ENGINE` / `STORAGE_DATA_DIR` 就能起库。
pub fn open_from_env() -> Result<Box<dyn KvEngine>> {
    StorageConfig::from_env()?.open()
}

/// [`open_engine`] 的入参：枚举和字符串两种写法都收。
///
/// [`EngineKind`] 把"选哪个引擎"这件事类型化了，入口却只吃 `&str`，于是拿着枚举的调用方
/// 还得自己 `.as_str()` —— 门面层自己没闭环。补这个 trait 后四种形态都免转换：
/// `EngineKind` / `&str` / `String` / `&String`（最后一种是 `open_engine(&s, …)` 这种
/// 顺手写法，泛型入参处不会自动解引用成 `&str`，不实现就会撞一堵类型错误墙）。
pub trait EngineName {
    /// 归一到规范名。不认识 ⇒ [`EngineError::UnknownEngine`]，且错误里**原样回显**写进来的
    /// 那个名字 —— 不静默退回默认值（同 [`StorageConfig::from_env`]）。
    fn engine_canon(self) -> Result<&'static str>;
}

impl EngineName for EngineKind {
    fn engine_canon(self) -> Result<&'static str> {
        // as_str() 给的已经是规范名，过一遍 canonical_name 只是把"两张表不许漂"钉在这里；
        // 真漂了先由 engine_kind_selection_is_typed_and_honest 红。
        canonical_name(self.as_str())
            .ok_or_else(|| EngineError::UnknownEngine(self.as_str().to_string()))
    }
}

impl EngineName for &str {
    fn engine_canon(self) -> Result<&'static str> {
        canonical_name(self).ok_or_else(|| EngineError::UnknownEngine(self.to_string()))
    }
}

impl EngineName for String {
    fn engine_canon(self) -> Result<&'static str> {
        canonical_name(&self).ok_or_else(|| EngineError::UnknownEngine(self))
    }
}

impl EngineName for &String {
    fn engine_canon(self) -> Result<&'static str> {
        canonical_name(self).ok_or_else(|| EngineError::UnknownEngine(self.clone()))
    }
}

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
            inner: crate::memory::MVCC::new(),
        }),
        _ => unreachable!("canonical_name 已校验"),
    };

    // 后两条是**跨引擎**的旋钮，不在任何一家的 open 签名里，所以先开库、再在把句柄交出去
    // 之前立刻应用 —— 下游拿到的必须已经是"他配过的那份"，不能存在"打开成功、阈值还没生效"
    // 的中间态。`?` 也是有意的：给 memory 设 WAL 阈值要当场失败，而不是静默通过。
    if let Some(bytes) = config.wal_checkpoint_bytes {
        db.set_wal_checkpoint_limit(bytes)?;
    }
    if let Some(rows) = config.vacuum_auto_slice_rows {
        db.set_vacuum_auto_slice(rows);
    }
    Ok(db)
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
///   （`vacuum_bounded` 用同一个开关：两处的形状一致）
/// - `durable: yes` → 转发 `flush` / `checkpoint`；`no` → no-op
/// - `wal: yes` → 转发 WAL 阈值读写；`no` → 返回 [`EngineError::Unsupported`]
///   （memory 没有 WAL，"设了阈值"这件事对它没有意义，不能假装成功）
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
        wal: $wal:tt,
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

            fn set_vacuum_auto_slice(&self, rows: u64) {
                self.inner.set_vacuum_auto_slice(rows)
            }

            fn vacuum_auto_slice(&self) -> u64 {
                self.inner.vacuum_auto_slice()
            }

            fn durability_profile(&self) -> DurabilityProfile {
                self.inner.durability_profile()
            }

            impl_engine!(@vacuum $vacuum);
            impl_engine!(@vacuum_bounded $vacuum);
            impl_engine!(@durable $durable);
            impl_engine!(@wal $wal);
            impl_engine!(@merge $merge);

            fn tuning(&self) -> EngineTuning {
                self.inner.tuning()
            }
        }

        impl<'a> KvTxn for $txn<'a> {
            fn version(&self) -> u64 {
                self.inner.version()
            }

            fn set(&self, key: &[u8], value: Vec<u8>) -> bool {
                self.inner.set(key, value)
            }

            fn set_raw(&self, key: &[u8], value: Vec<u8>) -> bool {
                self.inner.set_raw(key, value)
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

            fn incr_with_ttl(
                &self,
                key: &[u8],
                delta: i64,
                ttl: Duration,
            ) -> std::result::Result<i64, IncrError> {
                self.inner.incr_with_ttl(key, delta, ttl)
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

            fn scan_page(
                &self,
                after: Option<&[u8]>,
                limit: usize,
            ) -> (Vec<ExportRecord>, Option<Vec<u8>>) {
                self.inner.scan_page(after, limit)
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

    // ---- 有界回收分支：形状与 vacuum 一致，所以复用同一个开关 ----
    (@vacuum_bounded io) => {
        fn vacuum_bounded(
            &self,
            after: Option<&[u8]>,
            budget_rows: u64,
        ) -> Result<VacuumRound> {
            Ok(self.inner.vacuum_bounded(after, budget_rows)?)
        }
    };
    (@vacuum_bounded plain) => {
        fn vacuum_bounded(
            &self,
            after: Option<&[u8]>,
            budget_rows: u64,
        ) -> Result<VacuumRound> {
            Ok(self.inner.vacuum_bounded(after, budget_rows))
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

    // ---- WAL 阈值分支 ----
    (@wal yes) => {
        fn set_wal_checkpoint_limit(&self, bytes: u64) -> Result<()> {
            self.inner.set_wal_checkpoint_limit(bytes);
            Ok(())
        }

        fn wal_checkpoint_limit(&self) -> Result<u64> {
            Ok(self.inner.wal_checkpoint_limit())
        }
    };
    // 没有 WAL 的引擎：这条能力对它不存在 ⇒ 明确说 unsupported，不返回"设成功了"
    (@wal no) => {
        fn set_wal_checkpoint_limit(&self, _bytes: u64) -> Result<()> {
            Err(EngineError::Unsupported {
                engine: self.name(),
                op: "wal checkpoint limit",
            })
        }

        fn wal_checkpoint_limit(&self) -> Result<u64> {
            Err(EngineError::Unsupported {
                engine: self.name(),
                op: "wal checkpoint limit",
            })
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
    wal: yes,
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
    wal: yes,
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
    wal: yes,
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
    wal: no,
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

    /// 门面入口的几种写法必须落到同一个引擎。
    ///
    /// `EngineKind::as_str` 和 `canonical_name` 是两张表，字符串时代它们只在
    /// `open_engine` 内部碰头；现在枚举能直接入参，这张用例就是那个汇合点。
    #[test]
    fn open_engine_accepts_enum_and_string_in_every_ownership_shape() {
        let mut dirs = Vec::new();
        let disk = [
            (EngineKind::Bitcask, "bitcask"),
            (EngineKind::BPlusTree, "bplus-tree"),
            (EngineKind::LsmTree, "lsm"),
        ];
        for (kind, alias) in disk {
            let dir = tmp_dir("name_kind");
            let want = kind.as_str();
            let owned = alias.to_string();
            let by_kind = open_engine(kind, Some(&dir)).expect("枚举入参").name().to_string();
            let by_str = open_engine(alias, Some(&dir)).expect("&str 入参").name().to_string();
            let by_string = open_engine(owned.clone(), Some(&dir))
                .expect("String 入参")
                .name()
                .to_string();
            // 泛型位置不会把 &String 自动解引用成 &str —— 这条就是钉住那个 impl
            let by_ref = open_engine(&owned, Some(&dir)).expect("&String 入参").name().to_string();
            for (got, how) in [
                (&by_kind, "enum"),
                (&by_str, "&str"),
                (&by_string, "String"),
                (&by_ref, "&String"),
            ] {
                assert_eq!(got, want, "{alias} 走 {how} 这条路");
            }
            dirs.push(dir);
        }
        // memory 不吃路径
        assert_eq!(open_engine(EngineKind::Memory, None).unwrap().name(), "memory");
        assert_eq!(open_engine("mem", None).unwrap().name(), "memory");
        cleanup(dirs.into_iter().map(Some).collect());
    }

    #[test]
    fn unknown_name_is_echoed_verbatim_whatever_the_shape() {
        let s = String::from("redis");
        let errs = [
            expect_err(open_engine("redis", None)),
            expect_err(open_engine(s.clone(), None)),
            expect_err(open_engine(&s, None)),
        ];
        for err in errs {
            assert!(
                matches!(err, EngineError::UnknownEngine(ref n) if n == "redis"),
                "错误里要原样回显传进来的名字，实际: {err}"
            );
        }
    }

    /// 枚举 + 漏路径 = 归「必须给路径」管，不能被名字解析那一步吃掉
    #[test]
    fn enum_without_path_is_path_required() {
        for kind in [EngineKind::Bitcask, EngineKind::BPlusTree, EngineKind::LsmTree] {
            let err = expect_err(open_engine(kind, None));
            assert!(matches!(err, EngineError::PathRequired(_)), "{kind}: {err}");
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

    /// 门面必须**能跨线程用**。
    ///
    /// 这不是锦上添花：`Box<dyn KvEngine>` 少了 `Send`/`Sync`，既进不了 `Arc` 也搬不到
    /// 别的线程，而交易进程的常态就是"开库线程 + 心跳线程 checkpoint + 工作线程读写"。
    /// 下游正是因为这个才在应用里又包了一层 trait —— 那条 wrapper 现在可以删了。
    /// 这条断言在 trait 上界被人改掉时立刻编译失败。
    #[test]
    fn facade_is_usable_across_threads() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<Box<dyn KvEngine>>();
        assert_sync::<Box<dyn KvEngine>>();

        // 1) 事务搬到别的线程去写（scoped：`Box<dyn KvTxn + '_>` 带着对引擎的借用，
        //    普通 spawn 要求 'static，所以这里用 scope —— 要证的还是 Send）
        let dir = tmp_dir("send_txn");
        {
            let db = open_engine("bitcask", Some(&dir)).unwrap();
            std::thread::scope(|sc| {
                let t = db.begin();
                sc.spawn(move || {
                    assert!(t.set(b"moved", b"txn".to_vec()));
                    t.commit();
                });
            });
            let t = db.begin();
            assert_eq!(t.get(b"moved"), Some(b"txn".to_vec()));
            t.commit();
        }
        let _ = std::fs::remove_dir_all(&dir);

        // 2) 句柄进 Arc，四个线程共用同一个库
        let db = std::sync::Arc::new(open_engine("memory", None).unwrap());
        let mut handles = Vec::new();
        for i in 0..4u8 {
            let db = db.clone();
            handles.push(std::thread::spawn(move || {
                let t = db.begin();
                // 刻意不用 0x00/0x01 打头的值：那会踩到"值信封标签"的坑，
                // 与本条要证的跨线程无关（那个坑单独记在 README 已知局限 + 待决策）
                assert!(t.set(format!("k{i}").as_bytes(), vec![i.max(1) + 0x10; 8]));
                t.commit();
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let t = db.begin();
        for i in 0..4u8 {
            assert_eq!(
                t.get(format!("k{i}").as_bytes()),
                Some(vec![i.max(1) + 0x10; 8])
            );
        }
        t.commit();
        // 引擎层的维护动作也能从别的线程调
        let db2 = db.clone();
        std::thread::spawn(move || {
            db2.flush().unwrap();
            let h = db2.health();
            assert_eq!(h.live_keys, 4);
        })
        .join()
        .unwrap();
    }

    /// 类型化选型：别名解析、"要不要路径"、"能不能上生产"。
    #[test]
    fn engine_kind_selection_is_typed_and_honest() {
        assert_eq!(EngineKind::parse("LSM"), Some(EngineKind::LsmTree));
        assert_eq!(EngineKind::parse(" b-plus-tree "), Some(EngineKind::BPlusTree));
        assert_eq!(EngineKind::parse("mem"), Some(EngineKind::Memory));
        assert_eq!(EngineKind::parse("redis"), None);
        for k in [
            EngineKind::Bitcask,
            EngineKind::BPlusTree,
            EngineKind::LsmTree,
            EngineKind::Memory,
        ] {
            assert_eq!(EngineKind::parse(k.as_str()), Some(k), "as_str/parse 必须互逆");
            assert_eq!(k.to_string(), k.as_str());
        }
        // memory 是唯一"没有持久化"的那个，选型时就得能问到
        assert!(!EngineKind::Memory.is_production_ok());
        assert!(!EngineKind::Memory.needs_disk());
        for k in [EngineKind::Bitcask, EngineKind::BPlusTree, EngineKind::LsmTree] {
            assert!(k.is_production_ok() && k.needs_disk());
        }

        // StorageConfig 自己建目录、按引擎决定"目录 or 库文件"
        let base = tmp_dir("storage_cfg");
        let cfg = StorageConfig {
            kind: EngineKind::BPlusTree,
            data_dir: base.clone(),
            engine: EngineConfig {
                order: 16,
                max_cache_pages: 32,
                ..Default::default()
            },
        };
        let db = cfg.open().expect("按配置打开 B+ 树");
        assert_eq!(db.name(), "bplus_tree");
        assert!(cfg.engine_dir().exists());
        let t = db.begin();
        assert!(t.set(b"cfg", b"ok".to_vec()));
        assert_eq!(t.incr_with_ttl(b"counter", 3, Duration::from_secs(60)).unwrap(), 3);
        assert!(t.get_ttl(b"counter").is_some_and(|o| o.is_some()), "TTL 必须真的设上");
        t.commit();
        let _ = std::fs::remove_dir_all(&base);
    }

    /// 门面层：`set_raw` 必须**一字不差**地把任意字节存进去、取回来（四家各自 + 重开之后）。
    ///
    /// 盯的是四组最容易踩的形状：空值、`0x00` 打头（读回来会少 1 字节）、`0x01` 打头且后 8
    /// 字节看着像"已过期的 TTL 信封"（读回来直接变 `None`）、超过 blob 阈值的长值（走旁路）。
    /// 同一条里必须留一组**对照**：拿 `set` 存同样的字节确实会丢 —— 没有这组对照，
    /// 就无法区分"`set_raw` 真在做事"和"它只是个换个名字的 `set`"。
    #[test]
    fn set_raw_round_trips_every_byte_pattern() {
        // 一个"看着像已过期 TTL 信封"的裸值：0x01 + 8 字节很早的 LE 时间戳
        let mut fake_expired_ttl = vec![0x01u8];
        fake_expired_ttl.extend_from_slice(&1u64.to_le_bytes());
        fake_expired_ttl.extend_from_slice(b"payload");

        let cases: Vec<(&str, Vec<u8>)> = vec![
            ("空值", Vec::new()),
            ("单个 0x00", vec![0x00]),
            ("0x00 打头", vec![0x00, b'A', b'B', b'C']),
            ("像 TTL 信封的裸值", fake_expired_ttl.clone()),
            ("0x02 打头", vec![0x02, 0x00, 0x01, 0xFF]),
            ("超过 blob 阈值的长值", vec![0xAB; 5000]),
            ("全字节表", (0u16..=255).map(|b| b as u8).collect()),
        ];

        let engines = open_all();
        let mut dirs = Vec::new();
        for (name, db, dir) in engines {
            dirs.push(dir.clone());
            for (idx, (tag, bytes)) in cases.iter().enumerate() {
                let key = format!("raw{idx}").into_bytes();
                {
                    let tx = db.begin();
                    assert!(
                        tx.set_raw(&key, bytes.clone()),
                        "{name}: {tag} 写冲突？"
                    );
                    tx.commit();
                }
                {
                    let tx = db.begin();
                    assert_eq!(
                        tx.get(&key).as_deref(),
                        Some(bytes.as_slice()),
                        "{name}: {tag} 用 set_raw 存却读不回来（一字不差是这条守卫唯一的目的）"
                    );
                    tx.commit();
                }
                // 读路径不止 `get`：scan / multi_get 走的是同一套剥壳，必须同样原样
                let tx = db.begin();
                let one_key = vec![key.clone()];
                assert_eq!(
                    tx.multi_get(&one_key)[0].as_deref(),
                    Some(bytes.as_slice()),
                    "{name}: {tag} 在 multi_get 里变了"
                );
                let scanned = tx.scan(Some(&key), Some(&[key[0], b'z' + 1][..]));
                assert!(
                    scanned.iter().any(|r| r.key == key && r.value.as_deref() == Some(bytes.as_slice())),
                    "{name}: {tag} 在 scan 里丢了或变了"
                );
                tx.commit();

                // 对照：同一批字节用 `set` 存会怎样（`0x00`/`0x01` 打头的两类必须丢，
                // 其余恰好不丢 —— 这正说明"坑"是按字节值偶发出现的，靠人记规矩记不住）
                if *tag == "0x00 打头" || *tag == "像 TTL 信封的裸值" {
                    let bad = format!("bad{idx}").into_bytes();
                    {
                        let tx = db.begin();
                        assert!(tx.set(&bad, bytes.clone()));
                        tx.commit();
                    }
                    let tx = db.begin();
                    assert_ne!(
                        tx.get(&bad).as_deref(),
                        Some(bytes.as_slice()),
                        "{name}: `set` 存 {tag} 居然没丢？那这条守卫就白写了（信封约定改了？）"
                    );
                    tx.commit();
                }
            }

            // 落盘引擎：重开之后仍然一字不差（信封是在存储编码里，不是只在内存里）
            if dir.is_some() {
                db.checkpoint().expect("checkpoint");
                drop(db);
                let reopened = open_engine(name, dir.as_deref()).expect("重开");
                {
                    let tx = reopened.begin();
                    for (idx, (tag, bytes)) in cases.iter().enumerate() {
                        assert_eq!(
                            tx.get(&format!("raw{idx}").into_bytes()).as_deref(),
                            Some(bytes.as_slice()),
                            "{name}: 重开后 {tag} 变了"
                        );
                    }
                    tx.commit();
                }
                // 路径已在循环开头登记过，这里只放句柄（文件锁要放开才能删目录）
                drop(reopened);
            }
        }
        cleanup(dirs);
    }

    /// `migrate` 的验收：跨引擎**环**一圈（bitcask → lsm → bplus → memory → bitcask），
    /// 每一跳之后"键与用户字节逐条相同、TTL 仍在、`0x00`/`0x01` 打头的裸字节没被吃掉"。
    ///
    /// 为什么绕一圈而不是只跳一次：只跳一次看不出"某一跳丢了 TTL 或改了字节"—— 源头是那家写的，
    /// 只有让它再当一次源才暴露。环上每一家都既当源又当目标。
    #[test]
    fn migrate_walks_a_ring_of_engines_byte_for_byte() {
        let ring = ["bitcask", "lsm_tree", "bplus_tree", "memory"];
        let mut dirs: Vec<Option<PathBuf>> = Vec::new();

        // 四种形状各一条：普通、`0x00` 打头、`0x01` 打头（看着像 TTL 信封）、带 TTL
        let seed: Vec<(Vec<u8>, Vec<u8>, Option<u64>)> = vec![
            (b"plain".to_vec(), b"hello".to_vec(), None),
            (b"lead00".to_vec(), vec![0x00, 0x01, 0xFE, 0x7F], None),
            (b"lead01".to_vec(), {
                let mut v = vec![0x01u8];
                v.extend_from_slice(&9_999u64.to_le_bytes());
                v.extend_from_slice(b"not-a-ttl");
                v
            }, None),
            (b"ttl".to_vec(), b"expires-later".to_vec(), Some(3600)),
        ];

        let mut base: Option<Vec<(Vec<u8>, Option<Vec<u8>>)>> = None;
        let mut prev: Option<Box<dyn KvEngine>> = None;
        for (idx, name) in ring.iter().enumerate() {
            let dir = tmp_dir("migrate");
            let path = if *name == "memory" {
                None
            } else if *name == "bplus_tree" {
                let _ = std::fs::create_dir_all(&dir);
                Some(dir.join("bplus.db"))
            } else {
                Some(dir.clone())
            };
            dirs.push(path.clone());
            let db = open_engine(*name, path.as_deref()).expect("开目标引擎");
            if let Some(src) = prev.take() {
                let stats = migrate(&*src, &*db, 2).expect("migrate 失败");
                assert_eq!(stats.rows, seed.len() as u64, "{name}: 搬过去的 key 数");
                assert_eq!(stats.with_ttl, 1, "{name}: 带 TTL 的行数");
                // page_rows = 2 < 4 行 ⇒ 必须分多页（一页走完说明游标没在用）
                assert!(stats.pages >= 2, "{name}: 页数 {stats:?}");
            } else {
                let tx = db.begin();
                for (k, v, ttl) in &seed {
                    assert!(
                        match ttl {
                            Some(secs) => {
                                tx.set_with_ttl(k, v.clone(), Duration::from_secs(*secs))
                            }
                            None => tx.set_raw(k, v.clone()),
                        },
                        "播种 {k:?} 失败"
                    );
                }
                tx.commit();
            }
            let got: Vec<(Vec<u8>, Option<Vec<u8>>)> = {
                let tx = db.begin();
                let rows = tx.export_latest_visible(false);
                let with_ttl = tx.get_ttl(b"ttl").is_some_and(|o| o.is_some());
                let lead = tx.get(b"lead00").unwrap();
                tx.commit();
                assert!(with_ttl, "{name}: TTL 必须跟着搬过来");
                assert_eq!(lead, seed[1].1, "{name}: 0x00 打头的裸字节被改了");
                rows.into_iter().map(|r| (r.key, r.value)).collect()
            };
            match &base {
                Some(want) => assert_eq!(&got, want, "{name}: 第 {idx} 跳的内容与源头不一致"),
                None => base = Some(got),
            }
            prev = Some(db);
        }
        cleanup(dirs);
    }

    /// 门面层：`without_total()` 在四家必须给出**同一页的同一份内容**，而 `total` 如实是 `None`。
    ///
    /// 分页是下游直接面对的 API：一家行为不同，wrapper 里就多一句 `if engine == ...`。
    /// 这条与四家各自的窗口代价守卫不重复 —— 那边量的是"付多少"，这里对的是"给的答案一样吗"。
    #[test]
    fn windowed_search_agrees_across_engines() {
        const N: usize = 500;
        const PAGE: usize = 20;

        let engines = open_all();
        let mut dirs = Vec::new();
        for (name, db, dir) in engines {
            dirs.push(dir);
            {
                let tx = db.begin();
                for i in 0..N {
                    assert!(
                        tx.set(format!("user{i:04}").as_bytes(), format!("v{i}").into_bytes()),
                        "{name}: 第 {i} 行没写进去"
                    );
                }
                tx.commit();
            }

            // 事务对象有 `Drop`（漏 commit 就自动回滚），所以每次都关在自己的块里 ——
            // 不然借用活到循环体末尾，下面的 `drop(db)` 会打架。
            let q = SearchQuery::contains(b"user", 0, PAGE);
            let with = {
                let tx = db.begin();
                let page = tx.search_keys(&q);
                tx.commit();
                page
            };
            let without = {
                let tx = db.begin();
                let page = tx.search_keys(&q.clone().without_total());
                tx.commit();
                page
            };
            assert_eq!(with.total, Some(N), "{name}: 要 total 时照旧数得出来");
            assert_eq!(without.total, None, "{name}: 没数就必须是 None，不许报一个数");
            assert_eq!(without.total_pages, None, "{name}: 没数就不许报页数");
            assert_eq!(without.items, with.items, "{name}: 窗口内容必须与全量同一页一致");
            assert!(without.has_next(), "{name}: 首页之后当然还有");

            // 最后一页：两边都必须说"没有下一页"
            let last = N / PAGE - 1;
            let q2 = SearchQuery::contains(b"user", last, PAGE);
            let a = {
                let tx = db.begin();
                let page = tx.search_keys(&q2);
                tx.commit();
                page
            };
            let b = {
                let tx = db.begin();
                let page = tx.search_keys(&q2.clone().without_total());
                tx.commit();
                page
            };
            assert_eq!(a.items, b.items, "{name}: 尾页内容一致");
            assert_eq!(a.items.len(), PAGE, "{name}: 尾页也该是满页");
            assert!(!b.has_next(), "{name}: 最后一页之后没有东西了，不许虚报");
            drop(db);
        }
        cleanup(dirs);
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

    /// #17 的守卫：**写进 [`EngineConfig`] 的旋钮必须在引擎里生效，而且要读得回来**。
    ///
    /// 每条断言都配一条"不设时是多少"的对照。没有对照，这个测试会在两种坏情况下照样绿：
    /// ① `tuning()` 的键名和 `EngineConfig` 字段名漂了（`.get()` 一律返回 `None`，
    /// 而我若只断言 `!= Some("64")` 就永远成立）；② 引擎根本没接这个旋钮、值是门面自己
    /// 编的。对照必须来自**同一条代码路径的默认档**，所以这里既有 `open_engine_with`
    /// 也有 `open_engine`。
    #[test]
    fn engine_config_knobs_reach_the_engine_and_are_readable_back() {
        let mut dirs: Vec<PathBuf> = Vec::new();

        // ---- bplus_tree：order / max_cache_pages（只有这一家吃）----
        let dir = tmp_dir("knob_bplus");
        dirs.push(dir.clone());
        let db = open_engine_with(
            "bplus_tree",
            Some(&dir),
            &EngineConfig {
                order: 16,
                max_cache_pages: 32,
                ..Default::default()
            },
        )
        .expect("按配置打开 B+ 树");
        let t = db.tuning();
        assert_eq!(t.get("order"), Some("16"), "{t}");
        assert_eq!(t.get("max_cache_pages"), Some("32"), "{t}");

        let ddir = tmp_dir("default_bplus");
        dirs.push(ddir.clone());
        let dflt = open_engine("bplus_tree", Some(&ddir)).expect("默认档 B+ 树");
        let d = dflt.tuning();
        assert_eq!(d.get("order"), Some("64"), "默认档必须是另一个数，上面的断言才不是空转");
        assert_eq!(d.get("max_cache_pages"), Some("1024"));

        // ---- bitcask：分片上限（open 时就定死了，之后 rotate 按它走）----
        let dir = tmp_dir("knob_bitcask");
        dirs.push(dir.clone());
        let db = open_engine_with(
            EngineKind::Bitcask,
            Some(&dir),
            &EngineConfig {
                bitcask_max_file_bytes: Some(4096),
                ..Default::default()
            },
        )
        .expect("按配置打开 bitcask");
        assert_eq!(db.tuning().get("max_file_bytes"), Some("4096"));
        // 引擎自己的读回（`tuning()` 就是从这里取的）—— 证明值进的是 Bitcask 结构体，不是门面
        assert_eq!(db.tuning().get("max_file_bytes"), Some("4096"));

        let ddir = tmp_dir("default_bitcask");
        dirs.push(ddir.clone());
        let dflt = open_engine("bitcask", Some(&ddir)).expect("默认档 bitcask");
        assert_eq!(
            dflt.tuning().get("max_file_bytes"),
            Some("67108864"),
            "bitcask 默认分片上限应是 64 MiB"
        );

        // ---- lsm_tree：整份 LSMTreeOptions 补丁 ----
        let dir = tmp_dir("knob_lsm");
        dirs.push(dir.clone());
        let db = open_engine_with(
            EngineKind::LsmTree,
            Some(&dir),
            &EngineConfig {
                lsm: Some(crate::lsm_tree::LSMTreeOptions {
                    block_cache_blocks: 7,
                    mem_threshold_bytes: 12345,
                    ..crate::lsm_tree::LSMTreeOptions::for_mvcc()
                }),
                ..Default::default()
            },
        )
        .expect("按配置打开 lsm_tree");
        let t = db.tuning();
        assert_eq!(t.get("block_cache_blocks"), Some("7"), "{t}");
        assert_eq!(t.get("mem_threshold_bytes"), Some("12345"), "{t}");
        // 没动的字段留在 MVCC 档上：证明"补丁"不是"整份替换成裸 LSM 默认值"
        assert_eq!(t.get("l0_compact_threshold"), Some("8"), "MVCC 档的 L0 阈值不该被顺手改回 4");

        let ddir = tmp_dir("default_lsm");
        dirs.push(ddir.clone());
        let dflt = open_engine("lsm_tree", Some(&ddir)).expect("默认档 lsm_tree");
        let d = dflt.tuning();
        assert_eq!(d.get("block_cache_blocks"), Some("256"), "for_mvcc 的块缓存是 256");
        assert_eq!(d.get("mem_threshold_bytes"), Some("33554432"));

        // ---- 跨引擎两条：WAL 阈值 + 自动回收切片 ----
        for name in ["bitcask", "bplus_tree", "lsm_tree"] {
            let dir = tmp_dir("knob_wal");
            dirs.push(dir.clone());
            let db = open_engine_with(
                name,
                Some(&dir),
                &EngineConfig {
                    wal_checkpoint_bytes: Some(4096),
                    vacuum_auto_slice_rows: Some(0),
                    ..Default::default()
                },
            )
            .expect("带 WAL 阈值打开磁盘引擎");
            assert_eq!(db.wal_checkpoint_limit().unwrap(), 4096, "{name}: 阈值要真生效");
            assert_eq!(db.tuning().get("wal_checkpoint_bytes"), Some("4096"));
            assert_eq!(db.vacuum_auto_slice(), 0, "{name}: 切片设 0 = 关闭自动回收，要读得回 0");
            assert_eq!(db.tuning().get("vacuum_auto_slice"), Some("0"));
        }

        // memory 没有 WAL：给它设阈值必须**当场失败**，不能静默通过（"以为设了、其实没 WAL"
        // 是这次事故的形状之一）。切片它有的是真的。
        let db = open_engine_with(
            EngineKind::Memory,
            None,
            &EngineConfig {
                vacuum_auto_slice_rows: Some(0),
                ..Default::default()
            },
        )
        .expect("内存引擎只需切片");
        assert_eq!(db.vacuum_auto_slice(), 0);
        assert!(db.tuning().get("wal_checkpoint_bytes").is_none(), "没有 WAL 就不该出现在清单里");
        let e = db.set_wal_checkpoint_limit(4096).err().expect("memory 设 WAL 阈值要报错");
        assert!(
            matches!(e, EngineError::Unsupported { engine, op }
                if engine == "memory" && op == "wal checkpoint limit"),
            "错误要指到具体的引擎与操作：{e}"
        );
        let mem_default = open_engine("memory", None).expect("默认档 memory");
        assert_eq!(mem_default.vacuum_auto_slice(), 16 * 1024, "四引擎默认切片同一个值");

        cleanup(dirs.into_iter().map(Some).collect());
    }

    /// LSM 档里 `enable_mem_wal` / `enable_bg_compact` 传 `true` 也**必须**是 `false`。
    ///
    /// 这两条一旦被下游打开，崩溃恢复就有两个互不知情的提交日志（mem.wal 与事务 WAL），
    /// 而 `set_options` 热开 bg compact 更阴：线程只在 open 时创建，热改成 `true` 只会让
    /// 写路径以为"有人替我 compact"于是自己不再 compact ⇒ L0 无限涨。
    ///
    /// 走具体类型而不是门面：门面的 `tuning()` 有意**不列**这两条（列了会被读成"能调"）。
    #[test]
    fn lsm_mvcc_pins_the_two_fields_that_would_break_crash_safety() {
        use crate::lsm_tree::LSMTreeOptions;

        let dir = tmp_dir("lsm_pin");
        let db = crate::lsm_tree::MVCC::try_open_with(
            &dir,
            Some(LSMTreeOptions {
                enable_mem_wal: true,
                enable_bg_compact: true,
                block_cache_blocks: 999,
                ..LSMTreeOptions::for_mvcc()
            }),
        )
        .expect("带补丁打开 lsm_tree");

        let got = db.options();
        assert!(!got.enable_mem_wal, "事务 WAL 是唯一的持久化点：mem.wal 不能被下游打开");
        assert!(!got.enable_bg_compact, "MVCC 档的 compact 必须同步做");
        assert_eq!(got.block_cache_blocks, 999, "钉住两条不等于连坐：其余字段要真的生效");

        // 热更新路径同样钉住
        db.set_options(LSMTreeOptions {
            enable_mem_wal: true,
            enable_bg_compact: true,
            ..got.clone()
        });
        let after = db.options();
        assert!(!after.enable_mem_wal && !after.enable_bg_compact, "set_options 也要钉");
        assert_eq!(after.block_cache_blocks, 999, "热更新不能把补丁读回原样");

        drop(db);
        cleanup(vec![Some(dir)]);
    }

    /// 门面层的有界回收：一轮的代价只和 `budget_rows` 有关、游标能接力，
    /// **扫完全库后的终态与 `vacuum()` 一致**（同一份保留规则，不是两套）。
    #[test]
    fn vacuum_bounded_is_bounded_and_ends_where_vacuum_does() {
        const KEYS: u64 = 4;
        const CHAIN: u64 = 40;
        const BUDGET: u64 = 25;
        const TOTAL: u64 = KEYS * CHAIN;

        let engines = open_all();
        let mut dirs = Vec::new();
        for (name, db, dir) in engines {
            dirs.push(dir);
            // 关掉自动回收：本例要的是"我手动一轮一轮推"这个现场
            db.set_vacuum_auto_slice(0);

            for i in 0..CHAIN {
                for k in 0..KEYS {
                    let tx = db.begin();
                    assert!(tx.set(format!("k{k}").as_bytes(), format!("v{i}").into_bytes()));
                    tx.commit();
                }
            }
            let before = db.health();
            assert_eq!(before.total_rows, TOTAL, "{name}: 灌数后的物理行数");
            assert_eq!(before.max_chain, CHAIN, "{name}: 每把链 {CHAIN} 版");

            let mut cursor: Option<Vec<u8>> = None;
            let mut rounds = 0usize;
            let mut max_scanned = 0u64;
            loop {
                let round = db
                    .vacuum_bounded(cursor.as_deref(), BUDGET)
                    .unwrap_or_else(|e| panic!("{name}: vacuum_bounded 失败 {e}"));
                assert!(!round.stats.skipped_active_txn, "{name}: 不该有活跃事务挡路");
                // 预算的余量 = 一条链：截断只发生在逻辑 key 边界（半条链不能收，
                // 判"留哪一版"要求整组版本都在手上）。
                max_scanned = max_scanned.max(round.stats.rows_scanned);
                assert!(
                    round.stats.rows_scanned <= BUDGET + CHAIN,
                    "{name}: 一轮扫了 {} 行，预算 {BUDGET} + 一条链 {CHAIN}",
                    round.stats.rows_scanned
                );
                match round.next_cursor {
                    None => break,
                    Some(next) => {
                        assert!(
                            cursor.as_ref().is_none_or(|c| next.as_slice() > c.as_slice()),
                            "{name}: 游标必须严格前进，否则每轮重读同一片"
                        );
                        cursor = Some(next);
                    }
                }
                rounds += 1;
                assert!(rounds < 64, "{name}: 游标不前进？已 {rounds} 轮");
            }
            assert!(
                rounds > 1 && max_scanned < TOTAL,
                "{name}: 预算 {BUDGET} ≪ 全库 {TOTAL} 行，一轮扫完说明根本没限流（轮数 {rounds}）"
            );
            eprintln!(
                "[vacuum_bounded] {name}: 全库 {TOTAL} 行 / 预算 {BUDGET} ⇒ {rounds} 轮，\
                 单轮最多扫 {max_scanned} 行"
            );
            let after = db.health();
            assert_eq!(after.max_chain, 1, "{name}: 接力扫完应等价于 vacuum()");
            assert_eq!(after.total_rows, KEYS, "{name}: 每个逻辑 key 只剩一行");

            drop(db);
        }
        cleanup(dirs);

        // 一次性大预算 = 手动 `vacuum()`：同一个入口，只是不分片
        let engines = open_all();
        let mut dirs = Vec::new();
        for (name, db, dir) in engines {
            dirs.push(dir);
            db.set_vacuum_auto_slice(0);
            for i in 0..CHAIN {
                for k in 0..KEYS {
                    let tx = db.begin();
                    assert!(tx.set(format!("k{k}").as_bytes(), format!("v{i}").into_bytes()));
                    tx.commit();
                }
            }
            let round = db
                .vacuum_bounded(None, u64::MAX)
                .unwrap_or_else(|e| panic!("{name}: 全库轮次失败 {e}"));
            assert_eq!(round.next_cursor, None, "{name}: 全库轮次应一次到底");
            assert_eq!(round.stats.rows_scanned, TOTAL, "{name}: 全库轮次要读满 {TOTAL} 行");
            let after = db.health();
            assert_eq!(after.total_rows, KEYS, "{name}: 与 vacuum() 同一份保留规则");
            drop(db);
        }
        cleanup(dirs);
    }

    /// 崩溃安全差异必须**写在断言里**，不能只写在文档里。
    ///
    /// 选引擎的人最终看的是代码给的答案：文档会过期，这张表不会。哪天 bitcask 补了记录
    /// 校验和、或 LSM 给 `blobs/` 加了 CRC，用例会红，逼作者同步更新画像与 CHANGELOG ——
    /// 而不是让下游继续照着过期的"已知局限"做取舍。
    #[test]
    fn durability_profiles_are_honestly_different() {
        let engines = open_all();
        let mut dirs = Vec::new();
        let mut seen: Vec<(&'static str, DurabilityProfile)> = Vec::new();
        for (name, db, dir) in engines {
            dirs.push(dir);
            let p = db.durability_profile();
            assert_eq!(p.engine, name, "画像里的引擎名必须和门面名一致");
            match name {
                // 唯一没有数据记录校验和的一家：data.log 坏一个字节不报错，只读出错值
                "bitcask" => {
                    assert!(!p.data_checksum, "bitcask 主日志仍然是裸记录");
                    assert!(p.blob_checksum && p.merge_artifact_recovery);
                    let text = p.to_string();
                    assert!(text.contains("数据记录无校验和"), "缺口必须报出来：{text}");
                    assert!(!text.contains("无事务 WAL"), "没缺的不许瞎报：{text}");
                }
                // 唯一做原地页写的一家 ⇒ 也只有它有 torn page 问题，dblwr 兜着
                "bplus_tree" => {
                    assert!(p.data_checksum && p.in_place_page_writes && p.torn_page_guard);
                }
                // SST 块有 CRC，blobs/ 却是裸文件
                "lsm_tree" => {
                    assert!(p.data_checksum);
                    assert!(!p.blob_checksum, "blobs/*.blob 至今是 fs::write 的裸文件");
                }
                // 不持久化是设计，不是疏漏
                "memory" => {
                    assert!(!p.transaction_wal && !p.fsync_on_commit);
                    assert!(p.to_string().contains("无事务 WAL"));
                }
                other => panic!("未知引擎 {other}"),
            }
            seen.push((name, p));
        }
        // 画像必须两两不同 —— 差异存在却报成一样，才是真问题
        for (i, a) in seen.iter().enumerate() {
            for b in seen.iter().skip(i + 1) {
                assert_ne!(a.1, b.1, "{} 与 {} 的崩溃安全画像不该相同", a.0, b.0);
            }
        }
        cleanup(dirs);
    }

    /// 自动回收必须**默认开着**，而且四引擎都开着 —— 只有一家开就等于把事故留给换引擎的那天。
    ///
    /// 上层只通过门面打交道，所以这条走的是 `KvEngine::{vacuum_auto_slice,
    /// set_vacuum_auto_slice, health}`：不叫 vacuum，链也不能无限涨；关掉的开关要真能关掉。
    #[test]
    fn all_engines_auto_reclaim_by_default() {
        const CHAIN: u64 = 200;
        const SLICE: u64 = 8;

        let engines = open_all();
        let mut dirs = Vec::new();
        for (name, db, dir) in engines {
            dirs.push(dir);
            assert_eq!(
                db.vacuum_auto_slice(),
                16 * 1024,
                "{name}: 自动回收默认必须开着"
            );
            db.set_vacuum_auto_slice(SLICE);

            for i in 0..CHAIN {
                let tx = db.begin();
                assert!(tx.set(b"hot", format!("v{i}").into_bytes()), "{name} 第 {i} 次写入被拒");
                tx.commit();
            }
            let h = db.health();
            assert!(
                h.max_chain <= SLICE + 2,
                "{name}: 没人喊 vacuum，链却涨到 {} 行 —— 自动回收没在工作",
                h.max_chain
            );

            // 关掉的开关必须真关掉
            db.set_vacuum_auto_slice(0);
            for i in 0..30 {
                let tx = db.begin();
                assert!(tx.set(b"cold", format!("v{i}").into_bytes()));
                tx.commit();
            }
            let off = db.health();
            assert_eq!(off.live_keys, h.live_keys + 1, "{name}: 应多出 cold 这把 key");
            assert_eq!(
                off.max_chain, 30,
                "{name}: 关闭自动回收后不该裁掉任何一版（链应等于写入次数）"
            );
            drop(db);
        }
        cleanup(dirs);
    }

    // ==================== 大库基线台架（四引擎 × 每种操作） ====================
    //
    // 前面所有守卫数的是「访问了多少块/页/盘」—— 那是**复杂度**的尺子。下游报的却是
    // **墙钟与占用**（点查 0.86ms、CPU 打满、进程吃内存）。两把尺子可以同时成立：LSM 那条
    // 就是活例子（每点读只有 4 次块访问，实测 0.86ms，因为未命中要重开文件）。
    // 所以这里把每个引擎的每个操作都灌进大库跑一遍，三件事一起量：
    //   µs/次、结束后仍活着的字节（常驻）、期间峰值字节（瞬时物化）。
    //
    // 跑法（默认 5 万行，想更大给环境变量）：
    //   SCALE_ROWS=200000 cargo test --release --lib -- --ignored --nocapture \
    //       --test-threads=1 scale_baseline_all_engines
    // `--test-threads=1` 是必须的：内存计数是进程级的，并行会把别家用例的分配算进来。

    /// 记账分配器：只统计申请/释放的字节数。`#[cfg(test)]` ⇒ 对 crate 使用者零成本。
    mod mem_probe {
        use std::alloc::{GlobalAlloc, Layout, System};
        use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

        static LIVE: AtomicU64 = AtomicU64::new(0);
        static PEAK: AtomicU64 = AtomicU64::new(0);

        pub struct Tracking;

        unsafe impl GlobalAlloc for Tracking {
            unsafe fn alloc(&self, l: Layout) -> *mut u8 {
                // SAFETY: 委托给 System，契约由调用方（Rust 运行时）保证
                let p = unsafe { System.alloc(l) };
                if !p.is_null() {
                    let now = LIVE.fetch_add(l.size() as u64, Relaxed) + l.size() as u64;
                    PEAK.fetch_max(now, Relaxed);
                }
                p
            }
            unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
                LIVE.fetch_sub(l.size() as u64, Relaxed);
                // SAFETY: 指针与布局由调用方按 GlobalAlloc 契约提供
                unsafe { System.dealloc(p, l) };
            }
        }

        #[global_allocator]
        static ALLOC: Tracking = Tracking;

        pub fn live() -> u64 {
            LIVE.load(Relaxed)
        }
        pub fn peak() -> u64 {
            PEAK.load(Relaxed)
        }
    }

    /// 跑一次操作，返回 (墙钟, 常驻字节增量, 期间峰值字节增量, 操作返回值)
    fn timed<T>(f: impl FnOnce() -> T) -> (std::time::Duration, i64, u64, T) {
        let (l0, p0) = (mem_probe::live() as i64, mem_probe::peak());
        let t = std::time::Instant::now();
        let out = f();
        let d = t.elapsed();
        let (l1, p1) = (mem_probe::live() as i64, mem_probe::peak());
        (d, l1 - l0, p1.saturating_sub(p0), out)
    }

    /// 目录（或文件）总字节
    fn disk_bytes(path: &std::path::Path) -> u64 {
        if !path.exists() {
            return 0;
        }
        if path.is_file() {
            return std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        }
        let mut total = 0u64;
        let mut stack = vec![path.to_path_buf()];
        while let Some(dir) = stack.pop() {
            let Ok(rd) = std::fs::read_dir(&dir) else { continue };
            for e in rd.flatten() {
                let Ok(meta) = e.metadata() else { continue };
                if meta.is_dir() {
                    stack.push(e.path());
                } else {
                    total += meta.len();
                }
            }
        }
        total
    }

    fn shuffled(n: usize) -> Vec<usize> {
        let mut v: Vec<usize> = (0..n).collect();
        let mut s = 0x853c_49e6_77b9_5b3eu64;
        for i in (1..n).rev() {
            s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let j = (s >> 33) as usize % (i + 1);
            v.swap(i, j);
        }
        v
    }

    fn line(engine: &str, op: &str, per: f64, unit: &str, live: i64, peak: u64, extra: &str) {
        let live_mb = live as f64 / 1e6;
        let peak_mb = peak as f64 / 1e6;
        eprintln!(
            "{engine:<10} {op:<24} {per:>11.1} {unit:<7} 常驻 {live_mb:>9.1} MB  峰值 +{peak_mb:>8.1} MB  {extra}"
        );
    }

    #[test]
    #[ignore = "大库基线，手动 release 单线程跑：SCALE_ROWS=50000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 scale_baseline"]
    fn scale_baseline_all_engines() {
        let rows: usize = std::env::var("SCALE_ROWS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(50_000);
        // 键 ≈ 35 字节、值 ≈ 60 字节：照下游真实形状（也都远小于任何 blob 阈值）
        fn key(i: usize) -> Vec<u8> {
            format!("{i:08}#/worldquant/datafields/x.json").into_bytes()
        }
        fn val(i: usize) -> Vec<u8> {
            format!("{{\"i\":{i},\"h\":\"{:0<28}\",\"v\":1}}", i * 7).into_bytes()
        }

        eprintln!("=== 大库基线 rows={rows}（µs/次 + 常驻/峰值 MB）===");
        for kind in [
            EngineKind::Bitcask,
            EngineKind::BPlusTree,
            EngineKind::LsmTree,
            EngineKind::Memory,
        ] {
            let label = kind.as_str();
            let dir = tmp_dir(&format!("scale_{label}"));
            // B+ 树的"库"是一个文件，另两家是目录，memory 不落盘
            let arg = match kind {
                EngineKind::Memory => None,
                EngineKind::BPlusTree => Some(dir.join("bplus.db")),
                _ => Some(dir.clone()),
            };
            let arg = arg.as_deref();

            let (d, live, peak, db) = timed(|| open_engine(kind, arg).unwrap());
            line(label, "open 空库", d.as_secs_f64() * 1e6, "µs", live, peak, "");

            // ---- 写入：每 1000 行一个事务（commit 带 fsync） ----
            let (d, live, peak, ()) = timed(|| {
                for base in (0..rows).step_by(1000) {
                    let end = (base + 1000).min(rows);
                    let items: Vec<(Vec<u8>, Vec<u8>)> =
                        (base..end).map(|i| (key(i), val(i))).collect();
                    let tx = db.begin();
                    if let Err(bad) = tx.batch_set(&items) {
                        panic!("{label}: batch_set 冲突 {}", String::from_utf8_lossy(&bad));
                    }
                    tx.commit();
                }
            });
            line(label, &format!("灌 {rows} 行(事务)"), d.as_secs_f64() * 1e6 / rows as f64, "µs/行", live, peak, "");

            let (d, live, peak, ()) = timed(|| {
                db.flush().unwrap();
                db.checkpoint().unwrap();
            });
            let disk = if kind == EngineKind::Memory { 0 } else { disk_bytes(arg.unwrap()) };
            line(label, "flush+checkpoint", d.as_secs_f64() * 1e6, "µs", live, peak, &format!("磁盘 {:.1} MB", disk as f64 / 1e6));

            // ---- 冷重开：恢复要多久、恢复完常驻多少 ----
            // memory 不落盘 ⇒ 对它"重开"就是新建一个空库（这正是 `is_production_ok() == false`
            // 的全部内容），量恢复时间没有意义，直接跳过并保持同一个句柄。
            let db = if kind == EngineKind::Memory {
                eprintln!("{label:<10} {:<24} {:>17}   （内存引擎不落盘：重开 = 空库）", "冷重开(恢复)", "");
                db
            } else {
                drop(db);
                let (d, live, peak, db) = timed(|| open_engine(kind, arg).unwrap());
                line(
                    label,
                    "冷重开(恢复)",
                    d.as_secs_f64() * 1e6,
                    "µs",
                    live,
                    peak,
                    &format!("常驻 {:.1} MB", live as f64 / 1e6),
                );
                db
            };

            let order = shuffled(rows);

            // ---- 点读：乱序 / 有序 ----
            let (d, live, peak, ()) = timed(|| {
                let tx = db.begin();
                let mut hits = 0;
                for &i in &order {
                    if tx.get(&key(i)).is_some() {
                        hits += 1;
                    }
                }
                assert_eq!(hits, rows, "{label}: 乱序点读有键没读到");
            });
            line(label, "点读×N 乱序", d.as_secs_f64() * 1e6 / rows as f64, "µs/次", live, peak, "");

            let (d, live, peak, ()) = timed(|| {
                let tx = db.begin();
                for i in 0..rows {
                    assert!(tx.get(&key(i)).is_some(), "{label}: 有序点读落空");
                }
            });
            line(label, "点读×N 有序", d.as_secs_f64() * 1e6 / rows as f64, "µs/次", live, peak, "");

            // ---- multi_get（门面是 map(get)：批量到底省不省钱，量了才知道） ----
            let chunk: Vec<Vec<u8>> = order[..1000.min(rows)].iter().map(|&i| key(i)).collect();
            let (d, live, peak, got) = timed(|| {
                let tx = db.begin();
                tx.multi_get(&chunk)
            });
            line(label, "multi_get×1000", d.as_secs_f64() * 1e6 / chunk.len() as f64, "µs/键", live, peak, &format!("读到 {} 条", got.len()));

            // ---- 扫描：全量 / 1% 窗口 ----
            let (d, live, peak, scanned) = timed(|| {
                let tx = db.begin();
                tx.scan(None, None)
            });
            line(label, "一次全量 scan", d.as_secs_f64() * 1e6 / rows as f64, "µs/行", live, peak, &format!("{} 行", scanned.len()));
            drop(scanned);

            let lo = key(rows / 2);
            let hi = key(((rows / 2) + rows / 100).max(1));
            let (d, live, peak, window) = timed(|| {
                let tx = db.begin();
                tx.scan(Some(&lo), Some(&hi))
            });
            let w = window.len().max(1);
            line(label, "窗口 scan(1%)", d.as_secs_f64() * 1e6 / w as f64, "µs/行", live, peak, &format!("{w} 行"));
            drop(window);

            let (d, live, peak, pref) = timed(|| {
                let tx = db.begin();
                tx.prefix_scan(&key(0)[..2])
            });
            let p = pref.len().max(1);
            line(label, "prefix_scan 2B前缀", d.as_secs_f64() * 1e6 / p as f64, "µs/行", live, peak, &format!("{p} 行"));
            drop(pref);

            // ---- 管理面：分页搜索（首页 vs 深页 —— 深翻是最常见的"一次调用打满 CPU"） ----
            let page_size = 100usize;
            let last_page = rows / page_size;
            let (d, live, peak, one) = timed(|| {
                let tx = db.begin();
                tx.search_keys(&crate::common::SearchQuery::contains(b"", 0, page_size))
            });
            line(label, "search 首页", d.as_secs_f64() * 1e6, "µs/次", live, peak, &format!("命中 {}", one.total.unwrap_or_default()));

            // 同一页、只是**不要精确 total**：这一行存在的意义就是和上面那条并排看
            // （翻页的 UI 只要"还有没有下一页"，而数 total 的代价是走完整库）
            let (d, live, peak, one_nt) = timed(|| {
                let tx = db.begin();
                tx.search_keys(&crate::common::SearchQuery::contains(b"", 0, page_size).without_total())
            });
            line(label, "search 首页(免 total)", d.as_secs_f64() * 1e6, "µs/次", live, peak,
                 &format!("拿了 {} 条, has_next={}", one_nt.items.len(), one_nt.has_next()));
            let (d, live, peak, deep) = timed(|| {
                let tx = db.begin();
                tx.search_keys(&crate::common::SearchQuery::contains(b"", last_page, page_size))
            });
            line(label, "search 末页(深翻)", d.as_secs_f64() * 1e6, "µs/次", live, peak, &format!("命中 {}", deep.total.unwrap_or_default()));
            let (d, live, peak, cnt) = timed(|| {
                let tx = db.begin();
                tx.key_count(&key(0)[..2])
            });
            line(label, "key_count 2B前缀", d.as_secs_f64() * 1e6, "µs/次", live, peak, &format!("{cnt} 键"));

            // ---- TTL / 计数器 ----
            let (d, live, peak, ()) = timed(|| {
                let tx = db.begin();
                for _ in 0..1000 {
                    let _ = tx.incr(b"counter:hits", 1);
                }
                tx.commit();
            });
            line(label, "incr×1000", d.as_secs_f64() * 1e6 / 1000.0, "µs/次", live, peak, "");

            // ---- 更新 10% 的键（制造版本链，看点写是否被链拖住） ----
            let updates: usize = (rows / 10).max(1);
            let (d, live, peak, ()) = timed(|| {
                for base in (0..updates).step_by(500) {
                    let end = (base + 500).min(updates);
                    let items: Vec<(Vec<u8>, Vec<u8>)> =
                        (base..end).map(|i| (key(i), val(i + rows))).collect();
                    let tx = db.begin();
                    if let Err(bad) = tx.batch_set(&items) {
                        panic!("{label}: 更新冲突 {}", String::from_utf8_lossy(&bad));
                    }
                    tx.commit();
                }
            });
            line(label, &format!("更新 {updates} 键"), d.as_secs_f64() * 1e6 / updates as f64, "µs/次", live, peak, "");

            // ---- 观测与回收 ----
            let (d, live, peak, h) = timed(|| db.health());
            line(label, "health()", d.as_secs_f64() * 1e6, "µs/次", live, peak, &format!("rows={} max_chain={}", h.total_rows, h.max_chain));

            let (d, live, peak, v) = timed(|| db.vacuum().unwrap());
            line(label, "vacuum() 全库", d.as_secs_f64() * 1e6, "µs/次", live, peak, &format!("裁掉 {} 行", v.versions_removed));

            let (d, live, peak, ()) = timed(|| {
                db.flush().unwrap();
                let _ = db.merge();
            });
            let disk2 = if kind == EngineKind::Memory { 0 } else { disk_bytes(arg.unwrap()) };
            line(label, "flush+merge", d.as_secs_f64() * 1e6, "µs/次", live, peak, &format!("磁盘 {:.1} MB", disk2 as f64 / 1e6));

            // ---- bulk 导入（另一条写路径：无 WAL、无冲突检测） ----
            let bulk_rows = (rows / 10).max(1);
            let (d, live, peak, ()) = timed(|| {
                let mut bulk = db.begin_bulk();
                let items: Vec<(Vec<u8>, Vec<u8>)> =
                    (0..bulk_rows).map(|i| (key(rows + i), val(i))).collect();
                bulk.put_batch_owned(items);
                bulk.finish();
            });
            line(label, &format!("bulk 灌 {bulk_rows} 行"), d.as_secs_f64() * 1e6 / bulk_rows as f64, "µs/行", live, peak, "");

            // ---- 再冷重开一次：bulk 之后的恢复代价 ----
            let db = if kind == EngineKind::Memory {
                eprintln!("{label:<10} {:<24} {:>17}", "再冷重开", "（同上，跳过）");
                db
            } else {
                drop(db);
                let (d, live, peak, db) = timed(|| open_engine(kind, arg).unwrap());
                line(
                    label,
                    "再冷重开",
                    d.as_secs_f64() * 1e6,
                    "µs",
                    live,
                    peak,
                    &format!("常驻 {:.1} MB", live as f64 / 1e6),
                );
                db
            };

            // 收尾：确认 bulk 的数据读得回来
            let tx = db.begin();
            assert!(tx.get(&key(rows)).is_some(), "{label}: bulk 写的键读不回来");
            drop(tx);
            drop(db);

            if kind != EngineKind::Memory {
                let _ = std::fs::remove_dir_all(&dir);
                let _ = std::fs::remove_file(&dir);
            }
            eprintln!();
        }
    }
}

/// 一次搬运的账。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MigrateStats {
    /// 搬过去的逻辑 key 数
    pub rows: u64,
    /// 用户 value 字节总数
    pub bytes: u64,
    /// 走了多少页（`page_rows` 那么大的一页）
    pub pages: u64,
    /// 带 TTL 的行数（这些用 `set_with_ttl` 写，按剩余时间换算）
    pub with_ttl: u64,
}

/// 把 `from` 每个可见逻辑 key 搬到 `to`：跨引擎在线搬运，不必经 CLI 的中间文件。
///
/// 读侧是 [`KvTxn::scan_page`] 一页一页走 ⇒ **内存与每页代价都只看 `page_rows`**，
/// 与库总量无关（用 `export_latest_visible()` 整库物化那种写法搬大库，单核 6 GB 会当场翻）。
/// 写侧一页一个事务（`commit` 每页一次，不是每行一次 WAL fsync）。
///
/// 语义要说清（这三条都是刻意选择，不是没做完）：
/// - **只带"当前可见的最新版"**：历史版本与墓碑不带 ⇒ 目标库的 `max_chain` 从 1 开始，
///   这通常正是搬家的目的（顺手把版本链压平）。要连墓碑一起搬得走 `include_deleted` 的导出路径。
/// - **TTL 按剩余时间续**：绝对过期时刻折成 `Duration` 交给 `set_with_ttl` ⇒ 迁移本身不
///   改变"还剩多久过期"，但目标库的过期判定从此刻起按它自己的 `now` 算。
/// - **值用 `set_raw` 写**（源侧读回来的是已剥信封的用户字节，直接进目标侧的 `set`
///   会踩信封约定 —— 这正是 [`KvTxn::set_raw`] 存在的理由）。
///
/// 目标库已有同名 key 会被覆盖成新的版本行（不是冲突）；写不进去（如 B+ 树拒收超长键）
/// 会中止并回滚当前页，返回 [`EngineError::MigrateAborted`]。
pub fn migrate(
    from: &dyn KvEngine,
    to: &dyn KvEngine,
    page_rows: usize,
) -> Result<MigrateStats> {
    let page_rows = page_rows.max(1);
    let mut stats = MigrateStats::default();
    let mut cursor: Option<Vec<u8>> = None;
    loop {
        // 读侧一页：`scan_page` + 每行一次 `get_meta`。**TTL 必须在源侧读** ——
        // 目标侧此刻还没有这把 key，拿目标事务问 meta 会一律得到"永久"，于是 TTL 静默丢
        // （这条是 migrate_walks_a_ring_of_engines 第一版就抓出来的错）。
        let (rows, next) = {
            let tx = from.begin();
            let (page, next) = tx.scan_page(cursor.as_deref(), page_rows);
            let page: Vec<(Vec<u8>, Vec<u8>, Option<u64>)> = page
                .into_iter()
                .filter_map(|rec| {
                    let value = rec.value?;
                    let expire = tx.get_meta(&rec.key).and_then(|m| m.expire_unix_secs);
                    Some((rec.key, value, expire))
                })
                .collect();
            tx.commit();
            (page, next)
        };
        if !rows.is_empty() {
            let tx = to.begin();
            for (key, value, ttl) in rows {
                stats.rows += 1;
                stats.bytes += value.len() as u64;
                let ok = match ttl {
                    Some(expire) => {
                        stats.with_ttl += 1;
                        let now = crate::common::now_unix_secs();
                        let left = expire.saturating_sub(now).max(1);
                        tx.set_with_ttl(&key, value, std::time::Duration::from_secs(left))
                    }
                    None => tx.set_raw(&key, value),
                };
                if !ok {
                    tx.rollback();
                    return Err(EngineError::MigrateAborted {
                        key,
                        reason: "目标侧写被拒（写冲突或键/值超出一页载荷）",
                    });
                }
            }
            tx.commit();
        }
        stats.pages += 1;
        match next {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }
    Ok(stats)
}
