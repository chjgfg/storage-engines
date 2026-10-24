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

use std::unreachable;
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
    /// B+ 树页缓存上限（页数）。**`0` = 自动档**（M22，也是 `Default` 给的值）。
    ///
    /// 自动档按库当前的**实际页数**给容量，夹在 `[1024, 2048]` 页之间（≈ 4 ~ 8 MiB 常驻），
    /// 并在每次刷盘（`flush` / `checkpoint` / bulk 收尾）之后重新评估 —— 库是会长大的。
    /// 为什么上限只有 2048 页：B+ 树缓存整个库的常驻 ≈ 数据文件本身（实测 200k 行 = 12129 页
    /// ≈ 47 MB），那已经不是"缓存一点热数据"而是"把库再复制一份到内存"，
    /// 那种决定该由调用方做（显式传一个数），不该由库替调用方做。
    /// 为什么下限停在 1024：那是要改之前的写死默认值 ⇒ 换成自动档不会让任何人的工作集变小。
    ///
    /// 库的物理页超过容量时，全库走查改成"装得下才缓存"，不再挤掉 OLTP 的热页；
    /// 另外根与内部节点（实测占全库 3.0%）是**钉住的**，不参与换出 —— 这两件事一起
    /// 才让"大库 + 小缓存"的随机点读不至于每次都要重新下降。
    /// 生效值随时能从引擎读回：`tuning()` 里的 `max_cache_pages` / `cache_pinned_pages`。
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
    /// 缓存自动档的上限（单位 = 该引擎自己的缓存槽位：bplus 是 4 KiB 页、lsm 是 SST 块）。
    /// `None` = 用各家默认（bplus 2048 页 ≈ 8 MiB / lsm 1024 块 ≈ 7.1 MB）；
    /// `Some(usize::MAX)` = 不设顶，等于"自动档跟着库长"，代价是把库复制进 RAM。
    /// 只有 `bplus` / `lsm` 有这个概念，给另外两家会在**打开时就失败**（不静默忽略）。
    /// 读回与覆盖率分母见 [`KvEngine::tuning`]。
    pub cache_auto_max: Option<usize>,
    /// 缓存的**内存预算**（字节）；`None`/`Some(0)` = 不设，走各家默认。只有 `bplus`/`lsm`
    /// 有这一档，给另外两家会在**打开时就失败**（不静默忽略）。
    ///
    /// 与 [`Self::cache_auto_max`] 的关系：**设了预算，自动档那道 `clamp(槽数, 地板, 顶)` 就不再参与** ——
    /// 生效容量 = `min(库的槽数, 预算 ÷ 每槽真实常驻)`。预算既允许低于地板、也允许高于那道顶
    /// （"给我 64 MB"不该被默认的 2048 页顶夹回 8 MB）。
    /// ⚠️ 调用方**显式**给过容量（`max_cache_pages != 0` / `block_cache_auto_max == 0`）时预算只当天花板：
    /// 两个数都是他定的，库不拿预算去抬他显式要的那一档。
    /// 换算价签与"别买中间档"那条实测在 README 的缓存价签一节。
    pub cache_budget_bytes: Option<u64>,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            order: 64,
            // 0 = 自动档（M22）：按库的页数给容量，夹在 [1024, 2048]。
            // 空库算出来正好是 1024 ⇒ 默认档与改之前的写死值**同一个数**，
            // 变的只是"库长大之后容量也跟着长大"。
            max_cache_pages: 0,
            lsm: None,
            bitcask_max_file_bytes: None,
            wal_checkpoint_bytes: None,
            vacuum_auto_slice_rows: None,
            cache_auto_max: None,
            cache_budget_bytes: None,
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

    /// 分页搜索。⚠️ `query.page` 是 **offset 式页码**：第 N 页要从库头**跳过 `N × page_size` 行**
    /// ⇒ 单页单价 ∝ 页码，连翻 K 页是 **O(K²)**。
    ///
    /// 所以**"翻页 / 深翻"的默认姿势是 [`KvTxn::scan_page`]（游标），不是这里**。实测两条门
    /// （台架 `deep_paging_posture_ab`，release，页宽 100，四家 × 200k/600k）：
    /// - **跳到同一页，两条门同价**：游标 ÷ offset = **0.93 ~ 1.35×** ⇒ 换成游标不收学费；
    /// - **连翻 50 页，offset 贵 29 ~ 43 倍**（600k 档 bplus 42.6×、memory 41.6×、lsm 32.2×、bitcask 29.0×）。
    ///
    /// 那为什么还留着 offset：随机跳页（用户点"第 87 页"）只有这里能做，游标给不了 —— 这一条是
    /// **能力**不是便利，删不掉。M51 已把"深翻末页"的瞬时内存从 188 MB 削到 0.6~0.8 MB，
    /// 但**时间那半属于 offset 的语义**，不是实现浪费 ⇒ 要快就换门。
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

    /// **流式**导出（M44）：折一行交一行，`on` 返回 `false` 当场收手；返回交出去的行数。
    /// 与 [`KvEngine::export_latest_visible_for_each`] 同一条合同，只是挂在事务上
    /// （事务本身带快照，"导出这个事务看得见的东西"不必再套一层事务）。
    ///
    /// ⚠️ 默认实现先物化再逐行交（不给既有实现者制造破坏性变更）；四家覆盖成真流式。
    fn export_latest_visible_for_each(
        &self,
        include_deleted: bool,
        on: &mut dyn FnMut(ExportRecord) -> bool,
    ) -> usize {
        let mut handed = 0usize;
        for rec in self.export_latest_visible(include_deleted) {
            handed += 1;
            if !on(rec) {
                break;
            }
        }
        handed
    }

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

    /// **流式**导出（M44）：折一行交一行，`on` 返回 `false` 就当场收手；返回交出去的行数。
    ///
    /// 与 [`Self::export_latest_visible`] 的分工：那条返回 `Vec<ExportRecord>`，那笔 `Vec`
    /// 就是调用方要的合同 —— 但"把库导成文件 / 搬去另一家 / 只数个数"的人要的不是那份
    /// `Vec`，而是"写出去"。600k 档实测一次 `export 全库(物化)` 峰值 **+123.7 MB**，
    /// 这一条形同 O(1)。
    ///
    /// ⚠️ 默认实现是**物化之后**再逐行交出去（为了让 trait 的既有实现者不必改代码 ⇒ 不是
    /// 破坏性变更）。本仓库四个引擎都在 `impl_engine!` 里覆盖成真流式；自己实现
    /// `KvEngine` 的人想要不物化，得自己覆盖这一条 —— 覆盖前后行为一致，只有内存差别。
    fn export_latest_visible_for_each(
        &self,
        include_deleted: bool,
        on: &mut dyn FnMut(ExportRecord) -> bool,
    ) -> usize {
        let mut handed = 0usize;
        for rec in self.export_latest_visible(include_deleted) {
            handed += 1;
            if !on(rec) {
                break;
            }
        }
        handed
    }

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

    /// 把**缓存自动档的上限**改成 `n` 个缓存单元（`bplus` = 4 KiB 页，`lsm` = SST 数据块）。
    ///
    /// 为什么单位是"该引擎自己的槽位数"而不是字节数：自动档本来就是按槽数长出来的
    /// （`clamp(库的槽数, 地板, 上限)`），换成字节只会多一层换算与另一种四舍五入；
    /// 想知道自己买到了多少常驻，用 `tuning()` 给的分子/分母自己乘（价签在 README 里）。
    ///
    /// - `usize::MAX` = **不设顶** ⇒ 自动档等于"跟着库长"，代价是把库往 RAM 里复制一份
    ///   （bplus 实测 200k 行 12 129 页 ≈ 47 MB、lsm 600k 档块全驻留 ≈ 18.7 MB）—— 那是调用方的决定。
    /// - `0`：`lsm` 表示**关掉**自动档（生效值回到 `block_cache_blocks` 配置值）；
    ///   `bplus` 的下限是 1024 页，所以给它 0 等于"保持默认下限"，不是关缓存。
    /// - `bitcask` / `memory` 没有这个旋钮（前者靠 OS page cache，后者根本没有盘）
    ///   ⇒ [`EngineError::Unsupported`]，不会静默"设置成功"。
    ///
    /// 改容量是**就地改上限，不重建缓存**：重建 = 调一次旋钮就把已经焐热的页/块全丢掉
    /// （M23 踩过，守卫按"住着几页/几块，抬前抬后同一个数"钉住）。
    fn set_cache_auto_max(&self, n: usize) -> Result<()>;

    /// 读回当前的自动档上限；语义同上，两家之外返回 [`EngineError::Unsupported`]。
    fn cache_auto_max(&self) -> Result<usize>;

    /// 按**内存预算**给缓存档（M76，新增，带默认实现 ⇒ 非破坏）：
    /// 引擎自己把字节换算成槽数（bplus 一页缓存槽实测 ≈ 6.8 KB、lsm 一块 ≈ 10.1 KB，
    /// 都是**真实常驻**而不是名义块大小），返回换算后的**生效槽数**。
    ///
    /// 为什么要有这条：那两颗旧旋钮都以槽数计，而下游给的约束从来是字节
    /// （"这进程还能住 32 MB"）。让他自己乘价签就会踩两种错：拿页的 4 KiB 去乘（⇒ 实际买到
    /// 1.66 倍常驻），或忘了"抬顶之后覆盖率还不够就说不上省钱"。换算收进库里，
    /// 并把"给的字节 / 生效槽数 / 分母"三格一起从 [`Self::tuning`] 报回去。
    ///
    /// - `bytes = 0` ⇒ **撤掉预算档**，回到自动档/显式档那条（默认行为，与不设一字不差）。
    /// - 设了预算 ⇒ 自动档那道 `clamp(槽数, 地板, 顶)` 不再参与，生效容量 =
    ///   `min(库的槽数, 预算 ÷ 每槽真实常驻)`；flush / compact / checkpoint 之后的重估继续按预算封顶。
    ///   显式容量（`max_cache_pages != 0`）那条路上预算只当天花板，不拿它去抬调用方要过的数。
    /// - `bitcask` / `memory` 没有这一档 ⇒ [`EngineError::Unsupported`]（默认实现，不静默"设置成功"）。
    ///
    /// ⚠️ 它不是"把缓存做大"的另一条路，而是**给上限定价**：要全库驻留就直接给够字节，
    /// 中间那几档不买账（实测 8.4%→16.9% 覆盖率只换 −3% 单价，38%→100% 才换 −77%，见 README 价签一节）。
    fn set_cache_budget_bytes(&self, bytes: u64) -> Result<usize> {
        let _ = bytes;
        Err(EngineError::Unsupported {
            engine: self.name(),
            op: "cache budget bytes",
        })
    }

    /// 读回当前预算（字节）；`0` = 没设。两家之外返回 [`EngineError::Unsupported`]。
    fn cache_budget_bytes(&self) -> Result<u64> {
        Err(EngineError::Unsupported {
            engine: self.name(),
            op: "cache budget bytes",
        })
    }

    /// 落盘数据（内存引擎为 no-op）
    fn flush(&self) -> Result<()>;

    /// 写检查点并截断 WAL（内存引擎为 no-op）
    fn checkpoint(&self) -> Result<()>;

    /// 合并/压实底层文件。仅 `bitcask` 支持，其余返回
    /// [`EngineError::Unsupported`]。
    ///
    /// ⚠️ bitcask 这一家从 M43 起在 `merge()` 内部要**重扫新日志**来重建内存索引（为了让峰值
    /// 只是"一份索引"而不是"两份"）。重扫失败时本方法返回 `Err`，而引擎进入隔离态：此后
    /// 读写都会返回错，**不会**把"索引里没有"说成"键不存在"。磁盘上的数据是完整的，
    /// 出路是重新 open。正常成功的那次 merge 期间调用方看不见这个状态（内部持锁）。
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
        // 不能让下游以为"我配过了"（那一档决定的是 RSS 上限，静默失效比报错危险得多）。
        db.set_cache_budget_bytes(bytes)?;
    }
    Ok(db)
}

/// 把别名归一到规范名
fn canonical_name(name: &str) -> Option<&'static str> {
    // `+` 也归一掉：下游 `StorageEngine::parse` 认 `b+tree` 这种写法（运维脚本里真出现过），
    // 而这里原先只把 `-` 换成 `_` ⇒ 同一个字符串在两边一边认、一边不认，换库就等于悄悄换引擎。
    match name
        .trim()
        .to_ascii_lowercase()
        .replace('-', "_")
        .replace('+', "_")
        .as_str()
    {
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
        cache: $cache:tt,
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

            // M44：覆盖 trait 默认实现（默认那条要先把整库物化）。这一家底下就是
            // `fold_rows_streaming` ⇒ 折一行交一行，全库不再先在内存里摆一遍。
            fn export_latest_visible_for_each(
                &self,
                include_deleted: bool,
                on: &mut dyn FnMut(ExportRecord) -> bool,
            ) -> usize {
                self.inner.export_latest_visible_for_each(include_deleted, on)
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
            impl_engine!(@cache_max $cache);

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

            // M44：覆盖 trait 默认实现（默认那条要先把整库物化）
            fn export_latest_visible_for_each(
                &self,
                include_deleted: bool,
                on: &mut dyn FnMut(ExportRecord) -> bool,
            ) -> usize {
                self.inner.export_latest_visible_for_each(include_deleted, on)
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

    // ---- 缓存自动档上限分支（M32）+ 内存预算档（M76）----
    (@cache_max yes) => {
        fn set_cache_auto_max(&self, n: usize) -> Result<()> {
            self.inner.set_cache_auto_max(n);
            Ok(())
        }

        fn cache_auto_max(&self) -> Result<usize> {
            Ok(self.inner.cache_auto_max())
        }

        fn set_cache_budget_bytes(&self, bytes: u64) -> Result<usize> {
            Ok(self.inner.set_cache_budget_bytes(bytes))
        }

        fn cache_budget_bytes(&self) -> Result<u64> {
            Ok(self.inner.cache_budget_bytes())
        }
    };
    // 没有"缓存槽数"这个概念的引擎：明说 unsupported，别回"设置成功"
    // （预算档走 trait 的默认实现，也是 Unsupported）
    (@cache_max no) => {
        fn set_cache_auto_max(&self, _n: usize) -> Result<()> {
            Err(EngineError::Unsupported {
                engine: self.name(),
                op: "cache auto max",
            })
        }

        fn cache_auto_max(&self) -> Result<usize> {
            Err(EngineError::Unsupported {
                engine: self.name(),
                op: "cache auto max",
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
    cache: no,
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
    cache: yes,
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
    cache: yes,
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
    cache: no,
}

// 只有 bitcask 支持 merge：宏生成的 impl 用了 trait 的默认实现（返回
// Unsupported），这里没法再开一个 `impl KvEngine for BitcaskEngine`，所以
// merge 的 override 由宏的 `merge` 开关注入，见 impl_engine! 的调用点。

#[cfg(test)]
mod tests {
    use super::*;
    use std::assert;
    use std::assert_eq;
    use std::assert_ne;
    use std::panic;
    use std::eprintln;
    use std::vec;
    use std::format;
    use std::matches;

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

    /// 事务内核（M18b）的跨引擎对拍：同一段事务脚本在四家跑出**同一个结果**。
    ///
    /// 为什么必须有它：`memory` 一家搬上共用内核时，另三家还各留一份自己的实现，
    /// 而每家原有的测试只测自家那一份 —— 内核写错、或搬的时候漏了一条语义，
    /// 单引擎测试全都发现不了（各家的断言本来就是对各家实现写的）。这条按"脚本 + 终态"对拍，
    /// 覆盖内核的全部四条通道：
    /// ①冲突拒写（且被拒的那一行**没落库**）②回滚撤销自己写过的行 ③批量写撞墙时的部分撤销
    /// ④未收尾即丢弃 ⇒ 自动回滚且不漏活跃事务。终态逐字节比对，四家必须一模一样。
    #[test]
    fn txn_kernel_agrees_across_engines() {
        let mut finals: Vec<(&str, Vec<(Vec<u8>, Option<Vec<u8>>)>)> = Vec::new();
        let mut actives: Vec<(&str, u64)> = Vec::new();
        for (name, db, dir) in open_all() {
            // ① 两条事务抢同一把 key：后来者拒写，且那一行不许留在库里
            let t1 = db.begin();
            let t2 = db.begin();
            assert!(t1.set(b"hot", b"v1".to_vec()), "{name}: t1 写 hot");
            assert!(
                !t2.set(b"hot", b"v2".to_vec()),
                "{name}: t2 写 hot 必须是写冲突"
            );
            // ② t2 另一把 key 写得成，回滚后要消失
            assert!(t2.set(b"mine", b"v3".to_vec()), "{name}: t2 写 mine");
            assert!(
                t2.get(b"mine").is_some(),
                "{name}: t2 自己得看得见刚写的行"
            );
            assert!(
                t1.get(b"mine").is_none(),
                "{name}: t1 不该看见 t2 未提交的行"
            );
            t1.commit();
            t2.rollback();

            // ③ 批量写撞墙：已落的那几条要退回去（部分撤销），整笔事务随后回滚
            let t3 = db.begin();
            let t4 = db.begin();
            assert!(t3.set(b"hot", b"v5".to_vec()), "{name}: t3 抢 hot");
            let err = t4
                .batch_set(&[
                    (b"first".to_vec(), b"v6".to_vec()),
                    (b"hot".to_vec(), b"v7".to_vec()),
                ])
                .expect_err("{name}: batch_set 必须在 hot 上冲突");
            assert_eq!(err, b"hot".to_vec(), "{name}: 冲突键");
            assert!(
                t4.get(b"first").is_none(),
                "{name}: 部分撤销必须把 first 也退回"
            );
            t3.rollback();
            t4.rollback();

            // ④ 未收尾即丢弃 ⇒ 自动回滚，且活跃表不许漏下这一笔
            let t5 = db.begin();
            t5.set(b"leak", b"v8".to_vec());
            let leaked_version = t5.version();
            drop(t5);
            let after_drop = db.begin();
            assert!(
                after_drop.get(b"leak").is_none(),
                "{name}: 丢弃的事务必须等于没写"
            );
            after_drop.commit();
            let _ = leaked_version;

            let tx = db.begin();
            let mut rows: Vec<(Vec<u8>, Option<Vec<u8>>)> = tx
                .scan(None, None)
                .into_iter()
                .map(|r| (r.key, r.value))
                .collect();
            rows.sort();
            tx.commit();
            finals.push((name, rows));
            actives.push((name, db.health().active_txns));
            if let Some(d) = dir {
                let _ = std::fs::remove_dir_all(&d);
            }
        }
        let want = &finals[0];
        for got in &finals[1..] {
            assert_eq!(
                want.1,
                got.1,
                "终态不一致：{} vs {}（内核搬完必须逐家对得上）",
                want.0,
                got.0
            );
        }
        for (name, n) in actives {
            assert_eq!(n, 0, "{name}: 一轮脚本跑完不许留下活跃事务（漏网会永久停掉回收）");
        }
        // 脚本本身也钉住：四家终态都是这一把 committed 的行
        assert_eq!(finals[0].1, vec![(b"hot".to_vec(), Some(b"v1".to_vec()))]);
    }

    /// M20 守卫：一次 `seek_prev` 的代价 = "往回跳过几把死键"，**不是** "库里有多少把键"。
    ///
    /// 改之前四家各写一份 `scan(None, Some(key‖0))` + `retain` + `pop`：为了拿"≤key 的最后一条
    /// 活记录"，把整个前缀物化出来（200k 行实测单次 140~610 ms，还带 O(库) 的瞬时内存）。
    /// 现在循环只有一份 [`crate::common::seek_prev_with`]，每家只给两个口子：反向一步 + 单键可见行。
    ///
    /// 三条断言各抓一种**互不重叠**的退化，顺序是刻意的（②③在前、①在后：三种退化各由第一条撞上
    /// 它的断言报出来，实测都能撞红，下面就是当时留下的数字）：
    /// ② **步数**：共用内核的探针 `seek_prev_steps()`，断的是**精确形状** `DEAD + 1` = 9：死串每把
    ///    一步、答案一步。每把死键都带 5 个版本 ⇒ 按「组」跳才是 9 步。把 memory 的反向一步改成
    ///    交回编码键（忘记剥版本后缀）实测报 2 步 ⇒ 撞红。
    /// ③ **本引擎的尺子**：抓"这一次调用又把前缀折了一遍"。老写法（不调内核 ⇒ ②读到 0）实测 ——
    ///    bitcask `keydir_visits` 2017 / lsm `light_visits` 2017 / memory `fold_keys` 2017 /
    ///    bplus `page_reads` 63；新写法 0（memory 是 `range_back` 取回的 35 行）。⚠️ 预算必须按各家
    ///    **单位**给：bplus 的 2000 把键只摊在 63 片叶子上，拿"键数"当预算抓不到它 ⇒ 按"每步 4 页"给。
    /// ① **等价**：与"从 probe 往回逐把点读、第一把读得通的就是答案"这个语义参考比对，还要求答案
    ///    正是死串下面那把活键。只测快不测对，就是拿错数据换性能：把共用判定里的过期检查摘掉，
    ///    ②③全绿而这条报红（把已过期的那把交了出去）。
    ///    ⚠️ ②③ 数的是**内核圈的步数**与**折叠/取回的量**，看不见各家反向原语内部走了多少把键；
    ///    那层有界性靠 `take(1)` 与 bplus 的跨叶进度守卫，另有各自的测试。
    #[test]
    fn seek_prev_costs_a_walk_not_the_library() {
        const N: usize = 2000;
        const DEAD: usize = 8;
        const CHAIN: usize = 5;
        let dead_start = N - DEAD;
        let answer = format!("k{:05}", dead_start - 1);
        let probe = format!("k{:05}", N - 1);

        for (name, db, dir) in open_all() {
            {
                let tx = db.begin();
                for i in 0..N {
                    assert!(
                        tx.set(format!("k{i:05}").as_bytes(), b"v".to_vec()),
                        "{name}: 灌第 {i} 把"
                    );
                }
                tx.commit();
            }
            // 顶上 DEAD 把（含答案键）各再写几份 ⇒ 每把都有 CHAIN 个版本：
            // 反向一步必须按"组"跳，一步只该跨过一把键的全部版本
            {
                let tx = db.begin();
                for i in dead_start..N {
                    for _ in 1..CHAIN {
                        tx.set(format!("k{i:05}").as_bytes(), b"v".to_vec());
                    }
                }
                for _ in 1..CHAIN {
                    tx.set(answer.as_bytes(), b"v".to_vec());
                }
                tx.commit();
            }
            // 前 DEAD-1 把删除，最后一把写成"入库时就已经过期"
            // （`set` 收的是已打信封的逻辑值，`pack_ttl(…, 1)` = 1970 年到期）
            {
                let tx = db.begin();
                for i in dead_start..N - 1 {
                    tx.delete(format!("k{i:05}").as_bytes());
                }
                assert!(
                    tx.set(
                        format!("k{:05}", N - 1).as_bytes(),
                        crate::common::codec::pack_ttl(b"v", 1),
                    ),
                    "{name}: 写过期键"
                );
                tx.commit();
            }

            // 参考值只用点读算：从 probe 往回逐把 `get`，第一把读得通的就是"≤probe 的最大活键"。
            // 单独开一个事务，是为了不让这 9 次点读混进下面要量的尺子里。
            let mut want: Option<Vec<u8>> = None;
            {
                let tx = db.begin();
                for i in (0..N).rev() {
                    let k = format!("k{i:05}");
                    if tx.get(k.as_bytes()).is_some() {
                        want = Some(k.into_bytes());
                        break;
                    }
                }
                tx.commit();
            }

            // 先量再断：退化时那一步的形状要在打印里看得见，别让它被前面一条断言挡掉
            let s0 = crate::common::store::seek_prev_steps();
            let m_rows = crate::memory::mem_store::rows_touched();
            let m_fold = crate::memory::memory_mvcc::fold_keys();
            let bc = crate::bitcask::bitcask::keydir_visits();
            let lv = crate::lsm_tree::sst::light_visits();
            let br = crate::lsm_tree::sst::block_reads();
            let pr = crate::bplus_tree::page::page_reads();
            let ph = crate::bplus_tree::page::cache_hits();
            let tx = db.begin();
            let hit = tx.seek_prev(probe.as_bytes());
            tx.commit();
            let steps = crate::common::store::seek_prev_steps() - s0;
            let d_rows = crate::memory::mem_store::rows_touched() - m_rows;
            let d_fold = crate::memory::memory_mvcc::fold_keys() - m_fold;
            let d_bc = crate::bitcask::bitcask::keydir_visits() - bc;
            let d_lv = crate::lsm_tree::sst::light_visits() - lv;
            let d_br = crate::lsm_tree::sst::block_reads() - br;
            let d_pr = crate::bplus_tree::page::page_reads() - pr;
            let d_ph = crate::bplus_tree::page::cache_hits() - ph;
            let ruler = match name {
                "memory" => d_rows.max(d_fold),
                "bitcask" => d_bc,
                "lsm_tree" => d_lv.max(d_br),
                _ => d_pr.max(d_ph),
            };
            eprintln!(
                "[seek_prev] {name}: {steps} 步 / 尺子 {ruler}（库 {N} 把、顶上 {DEAD} 把已死、每把 {CHAIN} 版）\
                 rows={d_rows} fold={d_fold} keydir={d_bc} light={d_lv} block={d_br} page={d_pr} cache={d_ph}"
            );
            // ② 步数断的是**精确形状** = 死串每把一步 + 答案一步（实测四家都是 9）。
            // 不写 `<=` 是因为这轮的代价模型没有"多走几步也算对"的余地：多步 = 有组没被界挡住，
            // 少步 = 界把活键也一起跳了（那必然同时错答案，只是 ① 先报还是 ② 先报的区别）。
            assert_eq!(
                steps,
                DEAD as u64 + 1,
                "{name}: 一次 seek_prev 往退了 {steps} 把键，而死串只有 {DEAD} 把 ⇒ \
                 界没按「组」跳（步数偏多），或者反向一步的界把活键也跳过了（步数偏少）"
            );
            // 预算按各家的**单位**分别给：bplus 数的是页，2000 把键只摊在 63 片叶子上 ⇒ 拿"键数"
            // 当预算抓不到 bplus 的老写法（实测老写法只付 63，低于 160）。
            let budget = match name {
                "bplus_tree" => steps * 4 + 8,
                _ => (DEAD * CHAIN) as u64 * 4,
            };
            assert!(
                ruler <= budget,
                "{name}: 一次 seek_prev 付了 {ruler}（本引擎尺子），预算 {budget}；库 {N} 把、死串只有 {DEAD} 把 ⇒ \
                 又在折全库（老写法实测：bitcask/lsm/memory 2017 把键、bplus 63 页）"
            );
            // ① 等价放最后：上面两条撞红时报的是"形状"，这一条报的是"数据"。三种退化各由第一条
            // 撞上它的断言报出来 —— 老写法不调共用内核（②读到 0 步），界算错但答案仍可能对（只有
            // ②看得见），判定漏了过期/墓碑（前两条都绿，答案错，只有 ①看得见）。
            assert_eq!(
                hit.as_ref().map(|r| r.key.clone()),
                want,
                "{name}: seek_prev 与「逐把点读往回找」的参考不一致"
            );
            assert_eq!(
                hit.as_ref().map(|r| r.key.clone()),
                Some(answer.as_bytes().to_vec()),
                "{name}: seek_prev 应落在死串下面第一把活键 {answer}（删掉的、过期的都不许交出去）"
            );
            assert_eq!(
                hit.as_ref().and_then(|r| r.value.clone()).as_deref(),
                Some(&b"v"[..]),
                "{name}: seek_prev 交出来的 value 不该是信封裸字节"
            );
            if let Some(d) = dir {
                let _ = std::fs::remove_dir_all(&d);
            }
        }
    }

    /// M24 守卫：**写集登记每写一行只许考察一把键**，不许遍历写集。
    ///
    /// 老形状是 `writes.iter().any(|k| k == key)` ⇒ **单个事务内** O(写集²)（2026-10-17 记在案的那条：
    /// 它不是灌行超线性的主导项，因为台架每 1000 行一事务，但"一个大事务塞几万行"就浮出来 ——
    /// 那正是下游导历史数据的形状）。现在写集是 `HashSet`，查重与部分撤销的减法都走哈希。
    ///
    /// 尺子 [`crate::common::txn::write_set_scans`] 长在**唯一的写集入口** `register_write` /
    /// `unregister_write` 里，数的是"为判重复考察过多少把已登记的键"：哈希每次 1，遍历每次
    /// 是写集当前长度。断的是**精确值**（等号），所以"先 `contains` 再 `insert"` 这种双倍查重
    /// 也会报红。⚠️ 同一条用例里用 [`crate::common::txn::write_set_contains_linear`] **主动慢数一次**
    /// 当对照：既证明这把尺子不是空转，也当场量出退化版本一次查重要走多少把键（M19 同一手法）。
    ///
    /// ⚠️ **这把尺子有一处已知的盲角，所以另配一条代价尺**：探针写在 `register_write` 里面，
    /// 谁把算法改回遍历、却忘了搬那行计数，计数照样读 1 次/行。堵它的是 `big_transaction_write_cost`
    /// （墙钟台架，`#[ignore]`）—— 实测"只改算法、不改探针"那份退化在 bitcask 报头 200 行 9.3 /
    /// 尾 200 行 **288.8 µs/行（30.9×）**当场红，而修好的形状是 0.48~1.37×。
    /// 一条盯形状、一条盯代价：缺一条就有一种退化溜过去。
    #[test]
    fn write_set_registration_costs_a_hash_not_a_walk() {
        const K: usize = 2000;
        for (name, db, dir) in open_all() {
            let s0 = crate::common::txn::write_set_scans();
            {
                let tx = db.begin();
                for i in 0..K {
                    assert!(
                        tx.set(format!("w{i:05}").as_bytes(), b"v".to_vec()),
                        "{name}: 灌第 {i} 把"
                    );
                }
                // 同一把键反复写：一个事务里只算一行账，而成本也只该多一次查重
                for _ in 0..10 {
                    tx.set(b"w00000", b"v2".to_vec());
                }
                tx.commit();
            }
            let scans = crate::common::txn::write_set_scans() - s0;
            eprintln!("[写集] {name}: {scans} 次查重 / {} 次写（老写法这里会是 {} 量级）", K + 10, K * K / 2);
            assert_eq!(
                scans,
                (K + 10) as u64,
                "{name}: 登记 {} 次却考察了 {scans} 把已登记的键 ⇒ 查重不再走哈希\
                 （等号是刻意的：多出来的那次 `contains` 也算一次退化）",
                K + 10
            );

            // 减法那条链（批量写撞墙后的部分撤销）同样不许遍历写集：
            // 49 把落地 + 第 50 把冲突 ⇒ 49 次登记 + 49 次摘账
            let holder = db.begin();
            assert!(holder.set(b"zzz_conflict", b"h".to_vec()), "{name}: holder 占住键");
            let mut items: Vec<(Vec<u8>, Vec<u8>)> = (0..49)
                .map(|i| (format!("b{i:03}").into_bytes(), b"v".to_vec()))
                .collect();
            items.push((b"zzz_conflict".to_vec(), b"v".to_vec()));
            let s1 = crate::common::txn::write_set_scans();
            let (got, back) = {
                let tx = db.begin();
                let err = tx
                    .batch_set(&items)
                    .expect_err("{name}: batch_set 必须在占住的键上冲突");
                let walked = crate::common::txn::write_set_scans() - s1;
                tx.rollback();
                (err, walked)
            };
            holder.rollback();
            assert_eq!(got, b"zzz_conflict".to_vec(), "{name}: 冲突键");
            eprintln!("[写集 减法] {name}: 49 落账 + 49 撤账 = {back} 次查重");
            assert_eq!(
                back, 98,
                "{name}: 一批 49 把键的落账+撤账共考察 {back} 把 ⇒ 摘写集又在遍历写集了"
            );

            // 语义没被换掉：反复写的那把键读得到最后一次的值，事务收尾不留活跃事务
            let tx = db.begin();
            assert_eq!(
                tx.get(b"w00000").as_deref(),
                Some(&b"v2"[..]),
                "{name}: 去重不许丢掉后写的值"
            );
            assert_eq!(
                tx.get(b"w01999").as_deref(),
                Some(&b"v"[..]),
                "{name}: 集合里最后一把键必须照样提交（去重不许吞行）"
            );
            // 批量里第 50 把冲突 ⇒ 前 49 把（含 b048）必须整体退回，不能留在库里
            assert_eq!(tx.get(b"b048"), None, "{name}: 部分撤销必须把已落地的行退掉");
            assert_eq!(tx.get(b"b000"), None, "{name}: 同上（第一批那把）");
            tx.commit();
            assert_eq!(db.health().active_txns, 0, "{name}: 不许留活跃事务");

            // 对照：慢算法在同规模的写集上，一次查重就走 K 把键 —— 证明这把尺子看得见东西
            let ledger: std::collections::HashSet<Vec<u8>> =
                (0..K).map(|i| format!("w{i:05}").into_bytes()).collect();
            let c0 = crate::common::txn::write_set_scans();
            assert!(
                !crate::common::txn::write_set_contains_linear(&ledger, b"nobody"),
                "{name}: 对照算法本身要能给出正确答案"
            );
            let control = crate::common::txn::write_set_scans() - c0;
            eprintln!("[写集 对照] 老写法一次查重走 {control} 把键（K={K}）");
            assert!(
                control >= K as u64,
                "{name}: 对照的慢算法只走了 {control} 把键 ⇒ 这把尺子空转，断言等于没测"
            );
            if let Some(d) = dir {
                let _ = std::fs::remove_dir_all(&d);
            }
        }
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

    /// 缓存自动档的**顶**这颗旋钮：两家里调得动、另外两家当场说不行（M32 的门面侧守卫）。
    ///
    /// 为什么门面层还要一条：`set_cache_auto_max` / `cache_auto_max` 走的是 `impl_engine!` 的
    /// `@cache_max yes/no` 分支 —— **分支标错不会编译失败**（把 bitcask 也写成 `yes` 只会让它
    /// 转发给一个没有这个方法的名字… 而写成 `no`/`yes` 的错配更隐蔽：调用拿到 `Ok(())`，
    /// 其实没人听）。下游要的是"要么真抬得动，要么明确说抬不动"，所以两条路（热调 / 带配置打开）
    /// 都要过一遍。
    ///
    /// 覆盖率效应（抬顶之后落盘读归零）**不在这里量**：那是各家自己的账，守卫是
    /// bplus `test_cache_auto_max_is_raisable_and_misses_go_to_zero` 与
    /// lsm `test_block_cache_auto_max_is_raisable_and_coverage_is_readable`。
    #[test]
    fn cache_auto_max_knob_routes_to_two_engines_and_is_refused_by_the_other_two() {
        let mut dirs: Vec<PathBuf> = Vec::new();

        // ---- 能调的两家：默认顶读得回来，压/抬之后读回来的是新值，tuning 里连着分母 ----
        for (name, default_ceil) in [("bplus_tree", 2048usize), ("lsm_tree", 1024)] {
            let dir = tmp_dir(&format!("cache_max_{name}"));
            dirs.push(dir.clone());
            let db = open_engine(name, Some(&dir)).expect("默认档");
            assert_eq!(db.cache_auto_max().unwrap(), default_ceil, "{name}: 默认顶要读得回来");

            db.set_cache_auto_max(default_ceil / 2).expect("压顶不该报错");
            assert_eq!(
                db.cache_auto_max().unwrap(),
                default_ceil / 2,
                "{name}: 压顶没落到引擎里"
            );

            db.set_cache_auto_max(usize::MAX).expect("抬顶不该报错");
            assert_eq!(
                db.cache_auto_max().unwrap(),
                usize::MAX,
                "{name}: 抬顶没落到引擎里"
            );

            // 顶与分母必须同时在清单里：只给"生效容量"没人能判断这是撞顶还是库就这么小
            let t = db.tuning();
            let (ceil_key, denom_key) = if name == "bplus_tree" {
                ("page_cache_auto_max", "total_pages")
            } else {
                ("block_cache_auto_max", "sst_blocks")
            };
            assert_eq!(t.get(ceil_key), Some(usize::MAX.to_string().as_str()), "{t}");
            assert!(t.get(denom_key).is_some(), "{t}: 缺分母 ⇒ 报不出覆盖率");
        }

        // ---- `EngineConfig.cache_auto_max` 那条路：打开时就带上顶 ----
        for (name, ceil) in [("bplus_tree", 4096usize), ("lsm_tree", 3000)] {
            let dir = tmp_dir(&format!("cache_max_cfg_{name}"));
            dirs.push(dir.clone());
            let db = open_engine_with(
                name,
                Some(&dir),
                &EngineConfig {
                    cache_auto_max: Some(ceil),
                    ..Default::default()
                },
            )
            .expect("带顶打开");
            assert_eq!(db.cache_auto_max().unwrap(), ceil, "{name}: 配置里的顶没进引擎");
        }

        // ---- 没有"缓存槽数"这个概念的两家：热调与带配置打开都要**当场失败** ----
        for name in ["bitcask", "memory"] {
            let dir = tmp_dir(&format!("cache_max_bad_{name}"));
            dirs.push(dir.clone());
            let db = open_engine(name, Some(&dir)).expect("默认档");
            let e = db
                .set_cache_auto_max(4096)
                .err()
                .unwrap_or_else(|| panic!("{name}: 抬顶要报错，不能静默返回成功"));
            assert!(
                matches!(e, EngineError::Unsupported { engine, op }
                    if engine == name && op == "cache auto max"),
                "错误要指到具体的引擎与操作：{e}"
            );
            assert!(
                matches!(db.cache_auto_max(), Err(EngineError::Unsupported { .. })),
                "{name}: 读顶也要报同一个错"
            );
            drop(db); // 同一个目录还要再开一次：锁要放开

            // 带配置打开也不能静默忽略（"以为设了顶、其实跑的是默认档"就是这个洞的形状）
            let cfg_dir = tmp_dir(&format!("cache_max_cfg_bad_{name}"));
            dirs.push(cfg_dir.clone());
            let bad = open_engine_with(
                name,
                Some(&cfg_dir),
                &EngineConfig {
                    cache_auto_max: Some(4096),
                    ..Default::default()
                },
            )
            .err()
            .unwrap_or_else(|| panic!("{name}: 带 cache_auto_max 打开要失败，不能静默忽略"));
            assert!(
                matches!(bad, EngineError::Unsupported { .. }),
                "{name}: 要的是 Unsupported，实际 {bad}"
            );
        }

        cleanup(dirs.into_iter().map(Some).collect());
    }

    /// 缓存**内存预算**这颗旋钮：两家里调得动、另外两家当场说不行（M76 的门面侧守卫）。
    ///
    /// 为什么门面层还要一条（与 [`cache_auto_max_knob_routes_to_two_engines_and_is_refused_by_the_other_two`]
    /// 同一个理由）：`set_cache_budget_bytes` / `cache_budget_bytes` 走的是 `impl_engine!` 的
    /// `@cache_max` 分支 + trait 默认实现 —— **分支标错不会编译失败**，而 `bitcask` 一旦走到默认实现
    /// 之外拿到 `Ok(())`，下游就以为"我给这进程封了 32 MB"，实际一颗旋钮都没动。
    /// 决定 RSS 上限的旋钮，静默失效比报错危险。
    ///
    /// 换算与"封顶被不被执行"**不在这里量**（那是各家自己的账，守卫是 bplus
    /// `test_cache_budget_bytes_prices_slots_and_is_enforced` 与 lsm
    /// `test_block_cache_budget_bytes_replaces_the_clamp_and_respects_explicit`）；这里只核两条路
    /// （热调 / 带配置打开）都真的进了引擎，且读回来的是给的那个字节数。
    #[test]
    fn cache_budget_knob_routes_to_two_engines_and_is_refused_by_the_other_two() {
        let mut dirs: Vec<PathBuf> = Vec::new();

        for name in ["bplus_tree", "lsm_tree"] {
            let dir = tmp_dir(&format!("cache_budget_{name}"));
            dirs.push(dir.clone());
            let db = open_engine(name, Some(&dir)).expect("默认档");
            // 没设过预算 ⇒ 读回 0，而不是"看起来像个数"
            assert_eq!(db.cache_budget_bytes().unwrap(), 0, "{name}: 默认档不该像设过预算");

            // 热调：返回的是**换算后的生效槽数**，读回来的是**给的那个字节数**
            let slots = db.set_cache_budget_bytes(32_000_000).expect("设预算不该报错");
            assert!(slots > 0, "{name}: 32 MB 换算出 0 槽 ⇒ 价签或除法坏了");
            assert_eq!(db.cache_budget_bytes().unwrap(), 32_000_000, "{name}: 预算没落到引擎里");
            let t = db.tuning();
            assert_eq!(
                t.get("cache_budget_bytes"),
                Some("32000000"),
                "{name}: tuning 要看得见给的预算：{t}"
            );
            let cap_key = if name == "bplus_tree" { "max_cache_pages" } else { "block_cache_capacity" };
            assert_eq!(
                t.get(cap_key).unwrap().parse::<usize>().unwrap(),
                slots,
                "{name}: 返回的槽数与 tuning 里的生效容量不是同一个数 ⇒ 报的是别的门"
            );

            // 撤掉预算 ⇒ 读回 0（不许停在最后一次换算值上）
            db.set_cache_budget_bytes(0).expect("撤预算不该报错");
            assert_eq!(db.cache_budget_bytes().unwrap(), 0, "{name}: 撤掉之后要读回 0");
            drop(db); // 同一个目录还要再开一次：锁要放开

            // 带配置打开：那条路也必须真的进引擎（"以为配过了"是这个洞的形状）
            let cfg_dir = tmp_dir(&format!("cache_budget_cfg_{name}"));
            dirs.push(cfg_dir.clone());
            let db2 = open_engine_with(
                name,
                Some(&cfg_dir),
                &EngineConfig {
                    cache_budget_bytes: Some(8_000_000),
                    ..Default::default()
                },
            )
            .expect("带预算打开");
            assert_eq!(
                db2.cache_budget_bytes().unwrap(),
                8_000_000,
                "{name}: 配置里的预算没进引擎"
            );
        }

        // 没有"缓存槽数"这个概念的两家：热调与带配置打开都要**当场失败**
        for name in ["bitcask", "memory"] {
            let dir = tmp_dir(&format!("cache_budget_bad_{name}"));
            dirs.push(dir.clone());
            let db = open_engine(name, Some(&dir)).expect("默认档");
            let e = db
                .set_cache_budget_bytes(8_000_000)
                .err()
                .unwrap_or_else(|| panic!("{name}: 设预算要报错，不能静默返回成功"));
            assert!(
                matches!(e, EngineError::Unsupported { engine, op }
                    if engine == name && op == "cache budget bytes"),
                "错误要指到具体的引擎与操作：{e}"
            );
            assert!(
                matches!(db.cache_budget_bytes(), Err(EngineError::Unsupported { .. })),
                "{name}: 读预算也要报同一个错"
            );
            assert!(
                db.tuning().get("cache_budget_bytes").is_none(),
                "{name}: 没有这一档就不该出现在清单里"
            );
            drop(db);

            let cfg_dir = tmp_dir(&format!("cache_budget_cfg_bad_{name}"));
            dirs.push(cfg_dir.clone());
            let bad = open_engine_with(
                name,
                Some(&cfg_dir),
                &EngineConfig {
                    cache_budget_bytes: Some(8_000_000),
                    ..Default::default()
                },
            )
            .err()
            .unwrap_or_else(|| panic!("{name}: 带 cache_budget_bytes 打开要失败，不能静默忽略"));
            assert!(
                matches!(bad, EngineError::Unsupported { .. }),
                "{name}: 要的是 Unsupported，实际 {bad}"
            );
        }

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

    /// 四家的 `vacuum()` 都必须**逐组**判定，不许先把全库归组攒出来（M36）。
    ///
    /// 判据两条（每个引擎各核一次，失败点名是谁）：
    /// - `vacuum_group_calls() >= 键数` —— 一把键判一次。旧形状（先把全库归组进
    ///   `BTreeMap<Vec<u8>, Vec<…>>`、再问一次 `vacuum_plan`）在这里读出 **1** ⇒ 红
    /// - `vacuum_group_max_rows() <= 每把键的版本数` —— 任何一次判定握住的行数不得超过一条链；
    ///   旧形状读出的是**全库物理行** ⇒ 红
    ///
    /// 为什么不用 `mem_probe` 的 peak 当判据：那是**进程级**高水位，并行跑用例时别家分配的字节
    /// 会算进来（内存那笔读数一直留在 `--test-threads=1` 的台架里，M27/M33 同口径 —— 600k 那档
    /// 改前 **+89.4 MB** 的读数在 README）。这条守卫钉的是**结构**：不再攒全库，内存自然跟着降。
    ///
    /// 变异验证：把任意一家的收组改回 `BTreeMap` + 一次 `vacuum_plan` ⇒ 那一家两条都红、别家绿。
    #[test]
    fn all_engines_vacuum_judges_per_group_not_whole_library() {
        const KEYS: usize = 800;
        const VERSIONS: usize = 2;

        let engines = open_all();
        let mut dirs = Vec::new();
        for (name, db, dir) in engines {
            dirs.push(dir);
            // 自动回收会先把旧版裁掉一部分，那就不剩账了 —— 这一条要手动 vacuum
            db.set_vacuum_auto_slice(0);
            for _ in 0..VERSIONS {
                let tx = db.begin();
                for i in 0..KEYS {
                    let k = format!("k{i:05}");
                    assert!(
                        tx.set(k.as_bytes(), vec![b'v'; 24]),
                        "{name}: 第 {i} 把键写失败"
                    );
                }
                tx.commit();
            }
            crate::common::version::reset_vacuum_group_counters();
            let st = db.vacuum().unwrap_or_else(|e| panic!("{name}: vacuum 失败 {e}"));
            let calls = crate::common::version::vacuum_group_calls();
            let max_rows = crate::common::version::vacuum_group_max_rows();
            eprintln!(
                "[vac groups] {name}: 判定 {calls} 次 / 最大一组 {max_rows} 行 / 裁掉 {} 行、扫 {} 行",
                st.versions_removed, st.rows_scanned
            );
            assert!(
                st.rows_scanned >= (KEYS * VERSIONS) as u64,
                "{name}: 只扫到 {} 行（应有 {} 物理行）⇒ 这个形状没被跑到",
                st.rows_scanned,
                KEYS * VERSIONS
            );
            assert!(
                st.versions_removed > 0,
                "{name}: 一轮什么都没裁 ⇒ 判据空转"
            );
            assert!(
                calls as usize >= KEYS,
                "{name}: vacuum 把判定折叠成了 {calls} 次（键有 {KEYS} 把）⇒ 它还是先把全库归组攒出来"
            );
            assert!(
                max_rows as usize <= VERSIONS,
                "{name}: 有一次判定握住了 {max_rows} 行（每把键只有 {VERSIONS} 版）⇒ 归组攒了不止一条链"
            );
            // 裁完必须还读得回最新版
            {
                let tx = db.begin();
                assert_eq!(
                    tx.get(b"k00000"),
                    Some(vec![b'v'; 24]),
                    "{name}: 裁完读不回最新版"
                );
                tx.commit();
            }
            drop(db);
        }
        cleanup(dirs);
    }

    /// 四家的 `key_count` 与"要精确 total 的 `search_keys`"都必须**只数不物化**（M35）。
    ///
    /// 尺子 = **用户字节被拷出来的条数**（`common::user_value_copies`；两个拷贝入口都记在这一把
    /// 上：search 剥壳用的 `logical_to_user`、scan 剥壳用的 `live_record_of_logical`）。判据：
    /// - `key_count` 一次调用 ⇒ **0 条**。旧形状是 `prefix_scan(prefix).len()`，为了一个数把界内
    ///   每行的键与值都物化进 `Vec` 再丢掉（600k 档实测 **2.36 s / 峰值 +123.7 MB**）。
    /// - `search_keys(要 total)` 首页 ⇒ 拷的不超过**页宽 + 1**（旧形状 = 把界内命中全物化再 `len()`，
    ///   实测 **2.05 s / +123.8 MB**，而免 total 只要 1.2 ms ⇒ 差三个数量级那一格就是这个）。
    ///
    /// ⚠️ 另钉两条**语义对拍**（只数不物化不许把判定改窄或改宽）：
    /// - `key_count` 必须与 `prefix_scan().len()` 同一个数，库里**同时放了墓碑行与过期行**；
    /// - `search_keys` 的 `total` / `items` / `has_next` 在首页与深页上都必须与"全量物化那一条路"
    ///   给出的同一个数（`search_page_from` 那条老入口就是对照）。
    ///
    /// 为什么钉计数不钉内存：分配计数是进程级的，并行用例互相污染（M27/M36 各栽过一次）；
    /// 而"数一个数拷了几条 value"是个纯计数。内存那笔（+123.7 MB）留在 `--test-threads=1` 的台架里读。
    ///
    /// 变异验证：① 把某一家的 `key_count` 换回 `prefix_scan(prefix).len()` ⇒ 那一家红在
    /// "拷了 893 条用户字节"；② 把 `count_window` 里的 `logical_is_live` 换成 `logical_to_user`
    /// ⇒ 四家一起红（那就不是"只数不物化"而是"数之前先拷一遍"）。
    #[test]
    fn all_engines_count_without_materializing_values() {
        const A_KEYS: usize = 900;
        const B_KEYS: usize = 120;
        const DEAD: usize = 7;
        const EXPIRED: usize = 5;
        const PAGE: usize = 50;
        // 前缀 `aa` 下的活键：写进去的减去硬删掉的；过期那 5 把另算，它们**不该**被数进来
        let want_a = A_KEYS - DEAD;

        let engines = open_all();
        let mut dirs = Vec::new();
        for (name, db, dir) in engines {
            dirs.push(dir);
            db.set_vacuum_auto_slice(0);
            let tx = db.begin();
            for i in 0..A_KEYS {
                assert!(
                    tx.set(format!("aa{i:05}").as_bytes(), vec![b'v'; 16]),
                    "{name}: 第 {i} 把 aa 键写失败"
                );
            }
            for i in 0..B_KEYS {
                assert!(
                    tx.set(format!("bb{i:05}").as_bytes(), vec![b'w'; 16]),
                    "{name}: 第 {i} 把 bb 键写失败"
                );
            }
            for i in 0..DEAD {
                assert!(
                    tx.delete(format!("aa{i:05}").as_bytes()),
                    "{name}: 第 {i} 把 aa 键删失败"
                );
            }
            let past = crate::common::now_unix_secs().saturating_sub(30);
            for i in 0..EXPIRED {
                // 走 `set`：它把传进来的字节当作**已打过信封**的逻辑值（`set_raw` 会再包一层
                // plain 信封，那就不是过期行了 —— 这里要的正是"界内有一批过期行"这个形状）
                assert!(
                    tx.set(format!("aa9{i:04}").as_bytes(), crate::common::pack_ttl(b"gone", past)),
                    "{name}: 第 {i} 把过期行写失败"
                );
            }
            // 对拍的基准：走一遍**真物化**的路（那条路本来就该给出正确的数）
            let scan_a = tx.prefix_scan(b"aa").len();
            assert_eq!(
                scan_a, want_a,
                "{name}: 基准形状自己就不同（{scan_a} vs {want_a}）⇒ 库的形状没搭对"
            );

            // ① key_count：只数不物化
            let base = crate::common::user_value_copies();
            let got = tx.key_count(b"aa");
            let copies = crate::common::user_value_copies() - base;
            eprintln!(
                "[count only] {name}: key_count={got}（物化对照 {scan_a}）⇒ 期间拷出 {copies} 条用户字节"
            );
            assert_eq!(got, want_a, "{name}: key_count 与 prefix_scan 不同数 ⇒ 判定被改了");
            assert_eq!(
                copies, 0,
                "{name}: key_count 拷了 {copies} 条用户字节 ⇒ 又变回'整段物化成一个 Vec 再取 len()'"
            );

            // ② search_keys（要精确 total）：只拷那一页
            let q = SearchQuery::prefix(b"aa", 0, PAGE);
            let base = crate::common::user_value_copies();
            let page = tx.search_keys(&q);
            let copies = crate::common::user_value_copies() - base;
            assert_eq!(page.total, Some(want_a), "{name}: 首页 total 不对");
            assert_eq!(page.items.len(), PAGE, "{name}: 首页不是满页");
            assert!(page.has_next, "{name}: {want_a} 条 / 页宽 {PAGE}，首页之后当然还有");
            assert!(
                copies <= (PAGE + 1) as u64,
                "{name}: search_keys 首页拷了 {copies} 条用户字节（页宽 {PAGE}）⇒ total 那一步还在物化全段"
            );
            // 免 total 那条分支的 items 必须一模一样（它没被这一刀碰过）
            let p2 = tx.search_keys(&q.clone().without_total());
            assert_eq!(p2.total, None, "{name}: 免 total 那条不该给出数");
            assert_eq!(p2.items.len(), page.items.len(), "{name}: 两条路的首页不同长");
            assert_eq!(p2.items[0].key, page.items[0].key, "{name}: 两条路的首页不同行");

            // ③ 深页：total 推出来的 has_next / total_pages 要在边界上翻得对
            let last_page = (want_a + PAGE - 1) / PAGE - 1;
            let deep = tx.search_keys(&SearchQuery::prefix(b"aa", last_page, PAGE));
            assert_eq!(
                deep.items.len(),
                want_a - last_page * PAGE,
                "{name}: 末页行数不对（total={} 页宽 {PAGE}）",
                deep.total.unwrap_or(0)
            );
            assert!(!deep.has_next, "{name}: 末页还报有下一页");
            assert_eq!(
                deep.total_pages,
                Some((want_a + PAGE - 1) / PAGE),
                "{name}: total_pages 与 total/页宽不一致"
            );

            // ④ M51：末页这一趟**交出的行**必须与改前逐行相同（内存那一格在专门的守卫里钉：
            //    `deep_search_pages_do_not_materialize_earlier_pages` —— 这个库规模下动态范围不够，
            //    末页与第 0 页的高水位只差百分之几，比值判据拦不住旧形状，别在这儿装样子）。
            let deep_again = tx.search_keys(&SearchQuery::prefix(b"aa", last_page, PAGE));
            assert_eq!(
                deep_again.items.len(),
                deep.items.len(),
                "{name}: 流式取页与整段切片给出的末页行数不同"
            );
            for (i, rec) in deep_again.items.iter().enumerate() {
                assert_eq!(rec.key, deep.items[i].key, "{name}: 流式取页交出的末页不是同几行");
            }

            // ⑤ 语义必须一字不差：**每一页**都等于"全量折叠"的对应切片（含末页之后那一页）。
            //    只断"末页行数对"不够 —— 跳过 `skip` 行这条逻辑一旦错位（多跳/少跳/跳错键），
            //    行数照样可能对，行却是别的行。逐行比键才拦得住。
            let truth = tx.prefix_scan(b"aa");
            assert_eq!(truth.len(), want_a, "{name}: 对拍基准那一趟少了几行");
            for p in 0..=last_page + 1 {
                let pg = tx.search_keys(&SearchQuery::prefix(b"aa", p, PAGE));
                let lo = (p * PAGE).min(truth.len());
                let hi = ((p + 1) * PAGE).min(truth.len());
                assert_eq!(pg.items.len(), hi - lo, "{name}: 第 {p} 页行数与切片不一致");
                for (i, rec) in pg.items.iter().enumerate() {
                    assert_eq!(
                        rec.key, truth[lo + i].key,
                        "{name}: 第 {p} 页第 {i} 行的键不是切片里那一行 ⇒ skip 跳错了"
                    );
                }
                assert_eq!(
                    pg.has_next,
                    (p + 1) * PAGE < truth.len(),
                    "{name}: 第 {p} 页 has_next 不对（免 total 那支靠它，不许把没数报成到底）"
                );
                let pg_nt = tx.search_keys(&SearchQuery::prefix(b"aa", p, PAGE).without_total());
                assert_eq!(
                    pg_nt.items.len(),
                    pg.items.len(),
                    "{name}: 第 {p} 页在要/不要 total 两条路上不同长"
                );
                assert_eq!(pg_nt.has_next, pg.has_next, "{name}: 第 {p} 页两条路的 has_next 分家");
            }
            tx.commit();
            // `db` 在每次迭代末尾自己就掉了（事务 `tx` 比它先结束），所以这里不需要显式 `drop`；
            // 写在这里是因为 Windows 上文件句柄没释放就 `remove_dir_all` 会失败。
        }
        cleanup(dirs);
    }

    /// M51 的守卫：**深翻到末页不许把前面每一页都攒在堆上**（复测抓到的那格：600k 档
    /// `search 末页(深翻)` 瞬时峰值 **188 MB**、四家同形，比"一次全量 scan"的 123.7 MB 还高）。
    ///
    /// 改之前取页 = `search_window(limit = skip + 页宽 + 1)` 再切片 ⇒ 第 159 页要先把前 7 950 行
    /// 连键带值折进一个 `Vec`。现在走流式门：跳过的行当场丢掉，堆上最多只有一页。
    ///
    /// 判据 = **收集门攒了几行**（[`crate::common::collect_held_max`]，M44 那把尺子；线程局部的计数，
    /// 不像进程高水位会被并行用例污染）：取页改走流式门之后这一格必须是 **0**，
    /// 而旧形状（`search_window(limit = skip + 页宽 + 1)`）会读到 8001。
    /// 先给尺子作证：同一趟里 `scan_page(None, ROWS)`（真走收集门）必须读到 ≈ 全库 ——
    /// ⚠️ 我第一版的对照臂错用了 `prefix_scan`，它 M27 之后就逐片走了 ⇒ 读到 0，
    /// 看着像"没攒"其实是"门不在这条路上"（读数 0 有两种解释，M38/M44 就是这个坑）。
    ///
    /// **M75b 在这条守卫上加的两格**（`need_total` 从"两步"合成"一趟"之后）：
    /// ① 用户字节只在这一页里才拷：末页读 `user_value_copies` 必须 ≤ 页宽+1。
    ///    对照臂换成**同一扇门**的"一页装得下全库"那一格（读到 8000），⚠️ 我第一版又踩了一次
    ///    同一个坑：拿 `scan_page` 作证 —— 它自己 `decode_value_meta` + `to_vec`，不经过
    ///    `logical_to_user` ⇒ 读数 0 是"门不在这条路上"。
    ///    变异：把 `seen <= skip` 那一支改成"照样剥壳" ⇒ 四家全红在 8000（＝这就是旧形状的量）。
    /// ② 口径没分家：`total` 与 `key_count`（另一条只数不取的路）同数；`items`/`has_next`
    ///    与 `without_total()` 同数；删 3 把之后末页行数必须与 `total − skip` 自洽
    ///    （旧的两步里 `total` 与 `items` 来自两个瞬间，这种自洽是碰运气）。
    #[test]
    fn deep_search_pages_do_not_materialize_earlier_pages() {
        const ROWS: usize = 8000;
        const PAGE: usize = 50;
        let engines = open_all();
        let mut dirs = Vec::new();
        for (name, db, dir) in engines {
            dirs.push(dir);
            let last = ROWS / PAGE - 1;
            let tx = db.begin();
            for i in 0..ROWS {
                assert!(
                    tx.set(format!("k{i:06}").as_bytes(), vec![b'v'; 24]),
                    "{name}: 第 {i} 把键写失败"
                );
            }
            // 作证：这把尺子在真走收集门的那条路上读得到"整段"
            crate::common::reset_collect_held();
            let (many, _next) = tx.scan_page(None, ROWS);
            let held_door = crate::common::collect_held_max();
            assert_eq!(many.len(), ROWS, "{name}: scan_page 一次交不满全库 ⇒ 对照臂没建立");
            assert!(
                held_door >= ROWS as u64,
                "{name}: 收集门在 scan_page(限 {ROWS}) 上只读到 {held_door} ⇒ 尺子没接在门上，\
                 下面那个 0 不算数"
            );

            // 判据：深翻末页不得再走收集门
            crate::common::reset_collect_held();
            let p0 = tx.search_keys(&SearchQuery::prefix(b"k", 0, PAGE));
            let held_p0 = crate::common::collect_held_max();
            crate::common::reset_collect_held();
            let pd = tx.search_keys(&SearchQuery::prefix(b"k", last, PAGE));
            let held_deep = crate::common::collect_held_max();
            eprintln!(
                "[深翻取页] {name}: {ROWS} 行 ÷ 页宽 {PAGE}｜对照臂 scan_page 收集门读到 {held_door}；\
                 第 0 页攒 {held_p0} 行、末页(第 {last} 页)攒 {held_deep} 行\
                 （旧形状那一臂末页要攒 {} 行）",
                last * PAGE + PAGE + 1
            );
            // 语义没动：两页都是满页、total 照给、末页第一行就是第 `last*PAGE` 行
            assert_eq!(p0.items.len(), PAGE, "{name}: 第 0 页不是满页");
            assert_eq!(pd.items.len(), PAGE, "{name}: 末页不是满页");
            assert_eq!(pd.total, Some(ROWS), "{name}: 末页的 total 不对");
            assert_eq!(
                pd.items[0].key,
                format!("k{:06}", last * PAGE).into_bytes(),
                "{name}: 末页起点跳错了 ⇒ `skip` 那一笔多跳/少跳"
            );
            assert!(!pd.has_next, "{name}: 末页还报有下一页");
            assert_eq!(
                held_deep, 0,
                "{name}: 末页这一趟在收集门攒了 {held_deep} 行 ⇒ 取页还在把前面每一页物化成 `Vec`"
            );
            assert_eq!(
                held_p0, 0,
                "{name}: 第 0 页也攒了 {held_p0} 行 ⇒ 流式门根本没生效"
            );

            // ---- M75b：要 total 的那条路现在是**一趟**折叠 ⇒ 用户字节只在这一页里才拷 ----
            // 先给尺子作证：对照臂必须走**同一扇门**（`search_page_walk` 里那支 `logical_to_user`），
            // 且形状要能读出大数 ⇒ 用"一页装得下全库"那一格。
            // ⚠️ 第一版的对照臂错用了 `scan_page` —— 它自己解信封（`decode_value_meta` + `to_vec`），
            // 根本不经过 `logical_to_user` ⇒ 读数 0 的解释是"门不在这条路上"而不是"没拷"
            // （M38/M44 那个坑，读数 0 有两种解释）。
            let base_probe = crate::common::user_value_copies();
            let wide = tx.search_keys(&SearchQuery::prefix(b"k", 0, ROWS));
            let probe_copies = crate::common::user_value_copies() - base_probe;
            assert_eq!(wide.items.len(), ROWS, "{name}: 对照臂那一页没装下全库 ⇒ 对照没建立");
            assert!(
                probe_copies >= ROWS as u64,
                "{name}: 拷值尺子在取页那扇门上只读到 {probe_copies} ⇒ 尺子没接上，下面的数不算"
            );

            let base_copies = crate::common::user_value_copies();
            let pd_deep = tx.search_keys(&SearchQuery::prefix(b"k", last, PAGE));
            let copies = crate::common::user_value_copies() - base_copies;
            eprintln!(
                "[深翻拷值] {name}: 末页(第 {last} 页)拷 {copies} 条；对照臂(一页装全库)拷 {probe_copies} 条\
                 ＝跳过的行也剥壳那一臂的量（变异实测读回 {probe_copies}）"
            );
            assert_eq!(pd_deep.items, pd.items, "{name}: 同一份库两次取末页内容不一致");
            assert!(
                copies <= (PAGE + 1) as u64,
                "{name}: 深翻末页拷了 {copies} 条用户值 ⇒ 又走回\"两趟全库\"或把跳过的行也拷了\
                 （一趟折叠只该拷 ≤ 页宽+1 = {}）",
                PAGE + 1
            );
            // 口径没分家：total 必须与"另一条只数不取的路"(`key_count` 走 `count_window`) 同数，
            // 而 items 必须与"免 total"那条同数
            assert_eq!(
                pd.total,
                Some(tx.key_count(b"k")),
                "{name}: search 的 total 与 key_count 不同数 ⇒ 两条可见性判定分家了"
            );
            let nt = tx.search_keys(&SearchQuery::prefix(b"k", last, PAGE).without_total());
            assert_eq!(nt.items, pd.items, "{name}: 要不要 total 取到了不同的页");
            assert_eq!(nt.has_next, pd.has_next, "{name}: 两条路 has_next 分家");

            // 墓碑也在同一条路上判：删 3 把之后 total 必须跟着掉（不是"数完再取"那两个瞬间的和）
            for i in [0usize, PAGE, last * PAGE] {
                assert!(tx.delete(format!("k{i:06}").as_bytes()), "{name}: 删 k{i} 失败");
            }
            let after = tx.search_keys(&SearchQuery::prefix(b"k", last, PAGE));
            let tot = after.total.unwrap();
            assert_eq!(tot, ROWS - 3, "{name}: 删 3 把后 total 没跟着掉");
            assert_eq!(
                after.items.len(),
                tot - last * PAGE,
                "{name}: 末页行数与 total 不自洽 ⇒ skip 与 total 不是同一趟算出来的"
            );
            tx.commit();
        }
        cleanup(dirs);
    }

    /// 导出**不必把全库摆进内存**（M44）：`export_latest_visible_for_each` 走的是流式门
    /// （`fold_window_emit`），一行都不攒；而它交出来的行必须与"整库返回"那条一模一样。
    ///
    /// 尺子是 [`crate::common::collect_held_max`]：**收集门**（`fold_window`）最多同时攒了几行。
    /// 为什么拿它当凭据而不是拿内存高水位：这条路径的差别就在"中间有没有一个 `Vec`"，
    /// 而进程高水位在并行用例里互相污染（M27/M36 各栽过一次）。三件事分开钉，因为它们
    /// 各自会坏：
    /// ① 流式那趟里收集门必须读到 **0**（在第一次回调处采一次、跑完再采一次 —— 只采末尾
    ///    抓不到"先把全库折完再逐行回调"那种混血形状）。
    /// ② 同一把尺子在**真走收集门**的路径（`scan_page`）上必须读到页宽 ⇒ 证明它接在门上，
    ///    不是空转（M35 变异第一次没红就是栽在尺子只钉新门上）。
    /// ③ 语义对拍走**另一条遍历**（`scan_page` 翻页，游标续跑，与流式导出是两份代码）。
    ///    拿"收集壳 vs 流式核"互比是恒真的 —— 壳就是核的壳，比不出东西。
    #[test]
    fn all_engines_export_streams_without_holding_the_library() {
        const KEYS: usize = 5_000;
        const DEAD: usize = 40;
        const STOP: usize = 100;
        const PAGE: usize = 700;
        let want = KEYS - DEAD;

        let engines = open_all();
        let mut dirs = Vec::new();
        for (name, db, dir) in engines {
            dirs.push(dir);
            db.set_vacuum_auto_slice(0);
            let tx = db.begin();
            for i in 0..KEYS {
                assert!(
                    tx.set(format!("k{i:05}").as_bytes(), vec![b'v'; 16]),
                    "{name}: 第 {i} 把键写失败"
                );
            }
            for i in 0..DEAD {
                assert!(
                    tx.delete(format!("k{i:05}").as_bytes()),
                    "{name}: 第 {i} 把键删失败"
                );
            }
            tx.commit();

            // ── ② 先把尺子接在真走收集门的那条路上验一次它响（不这么钉，下面那个 0 不算数）──
            crate::common::reset_collect_held();
            let tx = db.begin();
            let (rows, _next) = tx.scan_page(None, PAGE);
            let page_rows = rows.len();
            tx.commit();
            let held_page = crate::common::collect_held_max();
            eprintln!(
                "[export stream] {name}: scan_page 交出 {page_rows} 行 ⇒ 收集门读到 {held_page}"
            );
            assert!(
                held_page >= page_rows as u64 && held_page <= page_rows as u64 + 1,
                "{name}: 收集门在 scan_page 上读到 {held_page}（页宽 {page_rows}，合同里多要一条做 has_next 的证据）\
                 ⇒ 要么尺子没接在门上（那下面那个 0 不算数），要么页宽失控成了全库"
            );

            // ── ① 两扇门都必须"折一行交一行" —— 尺子 = **只交出一行时拷出去的用户值条数** ──
            // ⚠️ 第一版这里用的是 `collect_held_max()`（收集门攒了几行），而它**分不开这两种形状**：
            //    M44 之后收集门（`fold_window`）根本不在导出路径上，"悄悄退回先物化整库再逐行交"
            //    的那个 `Vec` 是调用方自己攒的 ⇒ 尺子读 0 照样绿。去掉事务层流式覆盖的那次变异
            //    第一次就没红（见 README M44 那一节）。"只要 1 行时拷了几条用户值"才真分得开：
            //    流式 = 1，先物化 = 全库。
            let base = crate::common::user_value_copies();
            let mut one_only = 0usize;
            let handed_one = db.export_latest_visible_for_each(false, &mut |_| {
                one_only += 1;
                false // 第一行就收手
            });
            let copies_one = crate::common::user_value_copies() - base;
            eprintln!(
                "[export stream] {name}: 只要 1 行 ⇒ 交出 {handed_one} 行、拷出 {copies_one} 条用户值"
            );
            assert_eq!(handed_one, 1, "{name}: 收手之后不该再交第二行（实际 {handed_one}）");
            assert_eq!(one_only, 1, "{name}: 回调次数与返回行数不同");
            assert!(
                copies_one <= 2,
                "{name}: 只要 1 行却拷出 {copies_one} 条用户值（库里 {want} 行）⇒ KvEngine 那扇门先把整库物化了"
            );

            // 同一把尺子在事务层那扇门（`KvTxn::export_latest_visible_for_each`）上再走一遍：
            // trait 上挂的是默认实现（先物化再逐行交），`impl_engine!` 里的覆盖要是漏了，
            // 编译照过、语义也对 ⇒ 只有这一支抓得到。
            let base = crate::common::user_value_copies();
            let tx = db.begin();
            let mut tx_one = 0usize;
            let handed_tx = tx.export_latest_visible_for_each(false, &mut |_| {
                tx_one += 1;
                false
            });
            let copies_tx = crate::common::user_value_copies() - base;
            tx.commit();
            eprintln!(
                "[export stream] {name}: 事务层只要 1 行 ⇒ 交出 {handed_tx} 行、拷出 {copies_tx} 条用户值"
            );
            assert_eq!(handed_tx, 1, "{name}: 事务层收手之后还在交行（{handed_tx}）");
            assert!(
                copies_tx <= 2,
                "{name}: 事务层只要 1 行却拷出 {copies_tx} 条用户值 ⇒ `impl_engine!` 的覆盖没生效，悄悄退回了物化默认实现"
            );

            // ── ①b 全库那一趟：收集门一次都不该响（第一行处采一次、收完再采一次）──
            crate::common::reset_collect_held();
            let mut streamed: Vec<ExportRecord> = Vec::new();
            let mut first_held = u64::MAX;
            let copies_base = crate::common::user_value_copies();
            let mut f = |rec: ExportRecord| {
                if streamed.is_empty() {
                    first_held = crate::common::collect_held_max();
                }
                streamed.push(rec);
                true
            };
            let handed = db.export_latest_visible_for_each(false, &mut f);
            let copies_all = crate::common::user_value_copies() - copies_base;
            let held_end = crate::common::collect_held_max();
            eprintln!(
                "[export stream] {name}: 交出 {handed} 行 ⇒ 第一行时收集门 {first_held} 行、收完 {held_end} 行、拷出 {copies_all} 条"
            );
            assert_eq!(handed, want, "{name}: 流式导出的行数 ≠ 活键数 ⇒ 漏行或重行");
            assert_eq!(streamed.len(), want, "{name}: 回调次数与返回行数不同");
            // 尺子这一档自己也要"响一次给后面那个 0 作证"：全库导出就该拷全库条用户值
            assert_eq!(
                copies_all, want as u64,
                "{name}: 全库流式导出拷出 {copies_all} 条用户值（应为 {want}）⇒ 上面那个 ≤2 可能是尺子空转"
            );
            assert_eq!(
                first_held, 0,
                "{name}: 交第一行之前收集门已经攒了 {first_held} 行 ⇒ 还是先把全库折完再逐行回调"
            );
            assert_eq!(
                held_end, 0,
                "{name}: 流式导出这一趟在收集门里攒过 {held_end} 行 ⇒ 全库又在内存里摆了一份"
            );

            // ── 语义：行数与 `health()` 对得上、硬删掉的键不许出现 ──
            let keys: Vec<Vec<u8>> = streamed.iter().map(|r| r.key.clone()).collect();
            // ⚠️ 这里**不**拿 `health().live_keys` 当对拍基准：那个口径是"库里出现过的逻辑键数"，
            // 最新一版是墓碑的键照样算一个 ⇒ 它比"导出的存活行数"大 {DEAD}。行数与
            // `KEYS - DEAD` 的对齐在上面 `handed` 那一格已经钉了；与另一条遍历的逐行对拍在下面。
            assert_eq!(
                keys.len(),
                want,
                "{name}: 导出的存活行数不等于活键数 ⇒ 两条观测口径分了家"
            );
            assert_eq!(
                keys.iter().filter(|k| **k == b"k00000".to_vec()).count(),
                0,
                "{name}: 硬删掉的键出现在导出里"
            );
            let mut sorted = keys.clone();
            sorted.sort();
            assert_eq!(sorted, keys, "{name}: 导出不是升序 ⇒ 与翻页那条路对不上");

            // ── ③ 与另一条遍历（scan_page 一路翻到底）逐行对拍 ──
            let mut paged: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::new();
            let mut cursor: Option<Vec<u8>> = None;
            loop {
                let tx = db.begin();
                let (rows, next) = tx.scan_page(cursor.as_deref(), PAGE);
                for r in &rows {
                    paged.push((r.key.clone(), r.value.clone()));
                }
                tx.commit();
                match next {
                    Some(c) => cursor = Some(c),
                    None => break,
                }
            }
            assert_eq!(
                paged.len(),
                streamed.len(),
                "{name}: 翻页与流式导出的行数不同（翻页 {} vs 导出 {}）",
                paged.len(),
                streamed.len()
            );
            for (i, (p, s)) in paged.iter().zip(streamed.iter()).enumerate() {
                assert_eq!(p.0, s.key, "{name}: 第 {i} 行两条遍历给的键不同");
                assert_eq!(p.1, s.value, "{name}: 第 {i} 行两条遍历给的值不同");
            }

            // ── 提前收手：交够 STOP 行就停，后面的键不再折 ──
            crate::common::reset_collect_held();
            let mut got: Vec<Vec<u8>> = Vec::new();
            let mut g = |rec: ExportRecord| {
                got.push(rec.key);
                got.len() < STOP
            };
            let handed_stop = db.export_latest_visible_for_each(false, &mut g);
            assert_eq!(handed_stop, STOP, "{name}: 提前收手没停在 {STOP}（实际 {handed_stop}）");
            assert_eq!(
                got,
                keys[..STOP].to_vec(),
                "{name}: 提前收手交出的不是同一批前缀 ⇒ 收手处漏了行"
            );
            assert_eq!(crate::common::collect_held_max(), 0, "{name}: 收手那一趟走了收集门");
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
    // 记账分配器本身在 crate 根的 `crate::mem_probe`（M33 提到根上：一个 crate 只能有一个
    // `#[global_allocator]`，而 LSM 的压实台架要用同一把尺子，不能在 lsm 那边再抄一份）。

    /// 跑一次操作，返回 (墙钟, 常驻字节增量, 期间峰值字节增量, 操作返回值)
    ///
    /// 峰值**先把高水位压回"当前活着的字节"再量**：`peak` 是进程级单调高水位，只增不减 ⇒
    /// 直接取 `peak() - peak()` 之前那个高点的话，**排在后面的操作只会报 +0.0 MB**，哪怕它
    /// 自己瞬时涨了 50 MB（M33 记过这个盲区，M36 又踩了一遍：那一轮 lsm/bplus/memory 三家
    /// 的 `vacuum()` 峰值读数全是 0，等于没量）。压回去之后 `peak − 基准` 才是**这一段**的瞬时增量。
    fn timed<T>(f: impl FnOnce() -> T) -> (std::time::Duration, i64, u64, T) {
        let base = crate::mem_probe::reset_peak();
        let l0 = crate::mem_probe::live() as i64;
        let t = std::time::Instant::now();
        let out = f();
        let d = t.elapsed();
        let l1 = crate::mem_probe::live() as i64;
        (d, l1 - l0, crate::mem_probe::peak().saturating_sub(base), out)
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

    /// M48 的**冷启动分段**读数（门控 `OPEN_STAGE_SPLIT`，插桩在 bitcask 的 open 路径里，
    /// 八段的名字与边界写在 `MVCC::try_open_with` 上面那张表）。
    ///
    /// ⚠️ 每次量之前**必须清槽**（`open_stages_begin` 做这件事）：台架是四家排着跑的，
    /// 第一版没清，bplus/lsm 就把 bitcask 的读数原样复述了一遍 —— 那比读 0 更危险，
    /// 因为它看着像证据。八段全 0 时明说"这一家没插桩"，不许把它读成"不花钱"（M38 那条）。
    fn open_stages_report(engine: &str, scan_max_before: u64) {
        if std::env::var("OPEN_STAGE_SPLIT").is_err() {
            return;
        }
        let s = crate::mem_probe::open_stages();
        let scanned = crate::bitcask::bitcask::merge_rescan_rows();
        let scan_max = crate::bitcask::bitcask_mvcc::scan_max_calls() - scan_max_before;
        if s.iter().all(|v| *v == 0) {
            eprintln!(
                "{engine:<10} 冷启动分段: 八段全 0 ⇒ 这一家没插桩（只有 bitcask 装了），别当成\"不花钱\""
            );
            return;
        }
        let ms = |v: u64| v as f64 / 1e6;
        let sum: u64 = s.iter().sum();
        eprintln!(
            "{engine:<10} 冷启动分段(ms): 门面锁 {:>6.1} | 认残骸+发现分片 {:>6.1} | hint反序列化 {:>7.1} | 建KeyDir {:>8.1} | Blob+Wal开 {:>6.1} | WAL读+重放 {:>7.1} | 起始版本 {:>7.1} | flush+ckpt {:>7.1} | 合计 {:>7.1}",
            ms(s[0]),
            ms(s[1]),
            ms(s[2]),
            ms(s[3]),
            ms(s[4]),
            ms(s[5]),
            ms(s[6]),
            ms(s[7]),
            ms(sum),
        );
        eprintln!(
            "{engine:<10}   同一次重开里：日志扫过 {scanned} 行、`scan_max_version` 走了 {scan_max} 次（0=没走那条 O(全库) 的退路）"
        );
    }

    /// 重开之前调一次：清槽 + 装上行数尺 + 取 `scan_max_calls` 的基准。门控关着返回 0。
    fn open_stages_begin() -> u64 {
        if std::env::var("OPEN_STAGE_SPLIT").is_err() {
            return 0;
        }
        crate::mem_probe::open_stages_clear();
        crate::bitcask::bitcask::arm_merge_rescan_counter();
        crate::bitcask::bitcask_mvcc::scan_max_calls()
    }

    /// M24 的墙钟台架：**一个事务塞几万行**时，头几百行与尾几百行的单价必须同阶。
    ///
    /// 计数守卫 `write_set_registration_costs_a_hash_not_a_walk` 钉的是形状（每写一行只许考察一把键），
    /// 这条是把同一个形状的**代价**量出来：老写法（遍历写集判重复）在这里是 O(写集²)，
    /// 一个 20 000 行的事务走到第 19 800 行时，一次查重要走过 19 800 把已登记的键。
    /// ⚠️ 断言故意给得松（墙钟本机 ±2×，且这里连存储层自己的增长也算在内）——
    /// 精确的那半由计数尺守着，这条只报方向。
    ///
    /// ```text
    /// BIG_TXN_ROWS=20000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 big_transaction_write_cost
    /// ```
    #[test]
    #[ignore = "大事务墙钟台架，手动 release 单线程跑：BIG_TXN_ROWS=20000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 big_transaction_write_cost"]
    fn big_transaction_write_cost() {
        let rows: usize = std::env::var("BIG_TXN_ROWS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(20000);
        let edge = rows.min(200);
        for (name, db, dir) in open_all() {
            let tx = db.begin();
            let mut head = 0u128;
            let mut tail = 0u128;
            for i in 0..rows {
                let t = std::time::Instant::now();
                assert!(
                    tx.set(format!("k{i:06}").as_bytes(), b"v".to_vec()),
                    "{name}: 灌第 {i} 把"
                );
                let spent = t.elapsed().as_nanos();
                if i < edge {
                    head += spent;
                } else if i >= rows - edge {
                    tail += spent;
                }
            }
            tx.commit();
            let head_us = head as f64 / edge as f64 / 1000.0;
            let tail_us = tail as f64 / edge as f64 / 1000.0;
            eprintln!(
                "[大事务] {name}: 头 {edge} 行 {head_us:.1} µs/行 / 尾 {edge} 行 {tail_us:.1} µs/行\
                 （同一事务共 {rows} 行，比值 {:.2}×）",
                tail_us / head_us.max(0.001)
            );
            assert!(
                tail_us <= head_us * 6.0 + 50.0,
                "{name}: 一个事务里第 {rows} 行的单价 {tail_us:.1} µs 比第 1 行 {head_us:.1} µs 贵这么多 ⇒ \
                 写集登记又在随写集长度涨（O(写集²) 回来了）"
            );
            if let Some(d) = dir {
                let _ = std::fs::remove_dir_all(&d);
            }
        }
    }

    /// M53 的**启动税**量具：一次 `open` 到底付几次 fsync、开几个文件、**分别落在哪几处**。
    ///
    /// 为什么拿"次数"而不拿毫秒当判据：本机 `C:` 一次 fsync ≈ 2.6 ms、`D:`（5400 转）≈ 85.8 ms
    /// ⇒ "启动税是 12 ms 还是 35 ms"这句话换块盘就作废，而**次数是盘型无关的账**：下游那台单核
    /// 机器拿到"一次 open 付 N 次 fsync"，乘他自己的盘价就是他的启动税。
    /// 站点表在 `common::disk::FSYNC_SITES` —— 每一格都得能回答"它保证什么"，
    /// 答不出保证什么的（比如锁文件只为写个 pid 就 sync 一次）才是可修的那一半。
    ///
    /// 两种形状都要量，它们不是一回事：
    /// ① **全新库**（空目录 → open）：WAL / 双写 / blob 的头部各写一次；
    /// ② **重开已有库**（灌过行、干净收口过 → open）：下游"重启频繁"付的是这一档。
    /// 关闭那一趟（`Drop`）单列 —— 收口 WAL 的 fsync 住在关闭里，混进 open 就会把
    /// "进程退出的代价"冒充成"启动的代价"。
    ///
    /// ```text
    /// OPEN_TAX_ROWS=2000 cargo test --release --lib -- --ignored --nocapture \
    ///     --test-threads=1 open_startup_tax_by_site
    /// ```
    /// ⚠️ 必须 `--test-threads=1`：计数是进程级的（与内存台架同一条理由，M27/M36 各栽过）。
    #[test]
    #[ignore = "启动税按站点归因，手动 release 单线程跑：OPEN_TAX_ROWS=2000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 open_startup_tax_by_site"]
    fn open_startup_tax_by_site() {
        use crate::common::disk::{
            file_opens, file_opens_clear, fsync_counts, fsync_counts_clear,
        };
        let rows: usize = std::env::var("OPEN_TAX_ROWS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(2_000);
        let rounds = 12usize;

        fn sites(disk: &[u64; 14]) -> String {
            crate::common::disk::FSYNC_SITES
                .iter()
                .zip(disk.iter())
                .filter(|(_, n)| **n > 0)
                .map(|(name, n)| format!("{name} {n}"))
                .collect::<Vec<_>>()
                .join(", ")
        }
        fn line(label: &str, phase: &str, us: f64, disk: &[u64; 14], opens: u64) {
            let total: u64 = disk.iter().sum();
            eprintln!(
                "{label:<10} {phase:<16} fsync {total:>3} 次  打开文件 {opens:>3} 个  最快 {us:>9.1} µs  | {}",
                sites(disk)
            );
        }
        /// 读走两把尺子并立刻清零（下一趟量的是另一段）。⚠️ 读之前必须清 —— M48 那条
        /// "槽不会自己清"的教训：第一家会把读数原样复述给第二家，那比读 0 更危险。
        fn take(disk: &mut [u64; 14], opens: &mut u64) {
            *disk = fsync_counts();
            *opens = file_opens();
            fsync_counts_clear();
            file_opens_clear();
        }

        eprintln!(
            "=== 启动税：rounds={rounds} rows={rows}（四种引擎 × 全新库/重开/关闭）==="
        );
        for kind in [
            EngineKind::Bitcask,
            EngineKind::BPlusTree,
            EngineKind::LsmTree,
            EngineKind::Memory,
        ] {
            let label = kind.as_str();
            let is_mem = matches!(kind, EngineKind::Memory);
            let is_bplus = matches!(kind, EngineKind::BPlusTree);

            // ── 形状①：全新库（每轮都换一个干净的库目录）──
            let mut best_open = f64::MAX;
            let mut dk_open = [0u64; 14];
            let mut op_open = 0u64;
            let mut best_close = f64::MAX;
            let mut dk_close = [0u64; 14];
            let mut op_close = 0u64;
            for _ in 0..rounds {
                let d = tmp_dir(&format!("tax_new_{label}"));
                let a: Option<std::path::PathBuf> = if is_mem {
                    None
                } else if is_bplus {
                    Some(d.join("bplus.db"))
                } else {
                    Some(d.clone())
                };
                fsync_counts_clear();
                file_opens_clear();
                let t = std::time::Instant::now();
                let db = open_engine(kind, a.as_deref()).unwrap();
                let us = t.elapsed().as_secs_f64() * 1e6;
                let mut dk = [0u64; 14];
                let mut op = 0u64;
                take(&mut dk, &mut op);
                if us < best_open {
                    best_open = us;
                    dk_open = dk;
                    op_open = op;
                }
                let t = std::time::Instant::now();
                drop(db);
                let us = t.elapsed().as_secs_f64() * 1e6;
                take(&mut dk_close, &mut op_close);
                best_close = best_close.min(us);
                if !is_mem {
                    let _ = std::fs::remove_dir_all(&d);
                }
            }
            line(label, "全新库 open", best_open, &dk_open, op_open);
            line(label, "全新库 Drop", best_close, &dk_close, op_close);

            // ── 造一份"有内容且干净收口过"的库，再量形状② ──
            let dir = tmp_dir(&format!("tax_{label}"));
            let arg: Option<std::path::PathBuf> = if is_mem {
                None
            } else if is_bplus {
                Some(dir.join("bplus.db"))
            } else {
                Some(dir.clone())
            };
            let arg = arg.as_deref();
            {
                let db = open_engine(kind, arg).unwrap();
                for base in (0..rows).step_by(500) {
                    let end = (base + 500).min(rows);
                    let items: Vec<(Vec<u8>, Vec<u8>)> = (base..end)
                        .map(|i| {
                            (
                                format!("{i:08}#/tax/key").into_bytes(),
                                format!("v{i:08}").into_bytes(),
                            )
                        })
                        .collect();
                    let tx = db.begin();
                    tx.batch_set(&items)
                        .unwrap_or_else(|bad| panic!("{label}: batch_set {bad:?}"));
                    tx.commit();
                }
                db.flush().unwrap();
                db.checkpoint().unwrap();
                drop(db);
            }
            fsync_counts_clear();
            file_opens_clear();

            let mut best = f64::MAX;
            let mut dk_best = [0u64; 14];
            let mut op_best = 0u64;
            let mut dk_close2 = [0u64; 14];
            let mut op_close2 = 0u64;
            let mut best_close2 = f64::MAX;
            let mut totals = Vec::new();
            for _ in 0..rounds {
                let t = std::time::Instant::now();
                let db = open_engine(kind, arg).unwrap();
                let us = t.elapsed().as_secs_f64() * 1e6;
                let mut dk = [0u64; 14];
                let mut op = 0u64;
                take(&mut dk, &mut op);
                totals.push(dk.iter().sum::<u64>());
                if us < best {
                    best = us;
                    dk_best = dk;
                    op_best = op;
                }
                let t = std::time::Instant::now();
                drop(db);
                let us = t.elapsed().as_secs_f64() * 1e6;
                take(&mut dk_close2, &mut op_close2);
                best_close2 = best_close2.min(us);
            }
            totals.sort_unstable();
            totals.dedup();
            line(label, &format!("重开({rows} 行)"), best, &dk_best, op_best);
            line(label, &format!("关闭({rows} 行)"), best_close2, &dk_close2, op_close2);
            eprintln!(
                "{label:<10}   证人：{rounds} 趟重开的 fsync 总次数取值 = {totals:?}（只有一个值 ⇒ 这一档是**常数税**，不随重开次数漂）"
            );
            if !is_mem {
                assert!(
                    dk_best.iter().sum::<u64>() > 0,
                    "{label}: 重开一趟一次 fsync 都没数到 ⇒ 尺子没接在门上（全新库那趟若也报 0 更说明问题），\
                     这就不许读成\"这家启动不花钱\"（M38/M48：读数 0 的第一解释是量不到）"
                );
            }
            if !is_mem {
                let _ = std::fs::remove_dir_all(&dir);
            }
        }
        eprintln!(
            "换算参考：本机 `C:` 一次 fsync ≈ 2.6 ms、`D:`（5400 转机械）≈ 85.8 ms ⇒ 次数 × 盘价才是现场代价"
        );
    }

    /// M58 的证据台架：**深翻到同一页，两条门差多少**（`#[ignore]`，手动 release 跑）。
    ///
    /// 为什么要这一格：门面这边有两条翻页门 —— `search_keys` 是 **offset** 分页
    /// （`SearchQuery::contains(.., offset, len)`），`scan_page` 是**游标**分页。
    /// M15/M21/M51 已经把 offset 那条路的三笔浪费砍掉了（不为 total 走全库、游标下推到起点、末页不物化），
    /// 但**剩下那一笔是结构性的**：offset 的第 D 页必须从库头**跳过 D×len 行** ⇒ 单价 ∝ D，
    /// 连翻 K 页是 **O(K²)**；游标每页只付"上一页之后那 len 行" ⇒ 连翻 K 页是 **O(K)**。
    /// ⇒ "深翻慢"的正解不在库里，在**调用方选哪条门**上。这一格就是把那两条门的价当场读出来，
    ///   用来支撑"默认姿势 = 游标翻页"那句写进文档的建议。
    ///
    /// 三段读数（同一份库、同一深度）：
    /// ① 单页到末页：`A(offset, 免 total)` vs `B(游标从库头走到同一页)` —— 这两条**应当差不多**，
    ///    它们钉的是"换成游标不会更贵"这一半（如果 B 明显更贵，那句建议就不该写）；
    /// ② 连翻 K 页：`A = K 次 search_keys(各自跳过)` vs `B = 一次游标走 K 页` —— 这才是二次方与线性的差，
    ///    报总耗时与 **µs/页**；两臂各跑两遍取最小（本机墙钟跨轮漂 1.3~1.9×，比值才可信）。
    /// ③ 硬判据一条：**两条门交出的必须是同一页**（键序列逐条相等）—— 不等就说明"同一深度"这个前提没立住，
    ///    那上面两个比值全都无意义。
    ///
    /// ```text
    /// DEEP_PAGE_ROWS=600000 cargo test --release --lib -- --ignored --nocapture \
    ///     --test-threads=1 deep_paging_posture_ab
    /// ```
    #[test]
    #[ignore = "深翻两条门的价：手动 release 单线程跑：DEEP_PAGE_ROWS=600000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 deep_paging_posture_ab"]
    fn deep_paging_posture_ab() {
        let rows: usize = std::env::var("DEEP_PAGE_ROWS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(200_000);
        let page_size: usize = std::env::var("DEEP_PAGE_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);
        let k: usize = std::env::var("DEEP_PAGE_K")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(50);
        fn key(i: usize) -> Vec<u8> {
            format!("{i:08}#/worldquant/datafields/x.json").into_bytes()
        }
        fn val(i: usize) -> Vec<u8> {
            format!("{{\"i\":{i},\"h\":\"{:0<28}\",\"v\":1}}", i * 7).into_bytes()
        }
        let pages_total = rows / page_size;
        let last = pages_total - 1;
        eprintln!(
            "=== M58 深翻两条门 rows={rows} 页宽={page_size} 总页数={pages_total} 连翻 K={k} ==="
        );

        for kind in [
            EngineKind::Bitcask,
            EngineKind::BPlusTree,
            EngineKind::LsmTree,
            EngineKind::Memory,
        ] {
            let label = kind.as_str();
            let dir = tmp_dir(&format!("deep_page_{label}"));
            let arg = match kind {
                EngineKind::Memory => None,
                EngineKind::BPlusTree => Some(dir.join("bplus.db")),
                _ => Some(dir.clone()),
            };
            let arg = arg.as_deref();
            let db = open_engine(kind, arg).unwrap();
            for base in (0..rows).step_by(1000) {
                let end = (base + 1000).min(rows);
                let items: Vec<(Vec<u8>, Vec<u8>)> = (base..end).map(|i| (key(i), val(i))).collect();
                let tx = db.begin();
                if let Err(bad) = tx.batch_set(&items) {
                    panic!("{label}: batch_set 冲突 {}", String::from_utf8_lossy(&bad));
                }
                tx.commit();
            }
            db.flush().unwrap();
            db.checkpoint().unwrap();

            // ① 单页到末页：A = offset，B = 游标从库头走到同一页
            let (d_a, _, pk_a, pa) = timed(|| {
                let tx = db.begin();
                tx.search_keys(&crate::common::SearchQuery::contains(
                    b"",
                    last,
                    page_size,
                )
                .without_total())
            });
            let (d_b, _, pk_b, pb) = timed(|| {
                let tx = db.begin();
                let mut cursor: Option<Vec<u8>> = None;
                let mut chunk: Vec<crate::common::ExportRecord> = Vec::new();
                for _ in 0..=last {
                    let (c, next) = tx.scan_page(cursor.as_deref(), page_size);
                    chunk = c;
                    let Some(n) = next else { break };
                    cursor = Some(n);
                }
                chunk
            });
            // 硬判据：两条门必须交出**同一页**（不然下面那个比值没有意义）
            let ka: Vec<Vec<u8>> = pa.items.iter().map(|r| r.key.clone()).collect();
            let kb: Vec<Vec<u8>> = pb.iter().map(|r| r.key.clone()).collect();
            assert_eq!(ka, kb, "{label}: offset 末页与游标末页不是同一页");

            // ② 连翻 K 页：A = K 次 offset 翻页（每次各自从头跳过），B = 一次游标走 K 页
            let mut best_a = f64::MAX;
            let mut best_b = f64::MAX;
            for _ in 0..2u8 {
                let (d, _, _, got) = timed(|| {
                    let tx = db.begin();
                    let mut n = 0usize;
                    for j in 0..k {
                        n += tx
                            .search_keys(&crate::common::SearchQuery::contains(
                                b"",
                                j,
                                page_size,
                            )
                            .without_total())
                            .items
                            .len();
                    }
                    n
                });
                best_a = best_a.min(d.as_secs_f64() * 1e6);
                let (d, _, _, n) = timed(|| {
                    let tx = db.begin();
                    let mut cursor: Option<Vec<u8>> = None;
                    let mut n = 0usize;
                    for _ in 0..k {
                        let (chunk, next) = tx.scan_page(cursor.as_deref(), page_size);
                        n += chunk.len();
                        let Some(x) = next else { break };
                        cursor = Some(x);
                    }
                    n
                });
                best_b = best_b.min(d.as_secs_f64() * 1e6);
                assert_eq!(got, k * page_size, "{label}: offset 连翻 K 页行数不对");
                assert_eq!(n, k * page_size, "{label}: 游标连翻 K 页行数不对");
            }
            eprintln!(
                "{label:<10} 单页到末页(第 {last} 页) A(offset) {:>9.1} ms 峰值+{:.1} MB ｜ B(游标走到同一页) {:>9.1} ms 峰值+{:.1} MB ｜ B/A = {:.2}×\n\
                 {:<10} 连翻 K={k} 页        A 总 {:>9.1} ms（{:.0} µs/页）｜ B 总 {:>9.1} ms（{:.0} µs/页）⇒ **A/B = {:.1}×**",
                d_a.as_secs_f64() * 1e3,
                pk_a as f64 / 1e6,
                d_b.as_secs_f64() * 1e3,
                pk_b as f64 / 1e6,
                d_b.as_secs_f64() / d_a.as_secs_f64(),
                "",
                best_a / 1e3,
                best_a / k as f64,
                best_b / 1e3,
                best_b / k as f64,
                best_a / best_b,
            );
            if kind != EngineKind::Memory {
                let _ = std::fs::remove_dir_all(&dir);
            }
        }
        eprintln!("⚠️ 墙钟只当量级（本机跨轮漂 1.3~1.9×、且 twosync 那条 `cargo bench` 今晚也在跑），要用的数是 A/B 比值。");
    }

    /// M57 的**价签尺子**：一个缓存槽到底住多少字节（`#[ignore]`，手动 release 跑）。
    ///
    /// 存在的理由：README 里那句"bplus 全驻留 ≈ 47 MB / 143 MB"是 `页数 × 4 KiB` 的**算术**，
    /// 而 bplus 每个槽其实住着**两样东西** —— 页缓存里那份原始 4 KiB，加解析缓存里那个
    /// `Arc<BTreeNode>`（`bplus_tree.rs:541`：它的上限直接取 `page_manager.cache_capacity()`，
    /// 同一个数、两份内存）⇒ 拿 4 KiB 当单价去反推"6 GB 的机器能给多少页"会**系统性低估**。
    /// lsm 那一侧的单价也不该抄 README 的 7.4 KB：块大小随值变（5002 键那档均值 7 403 B，
    /// 600k 小值那档 ≈ 2 KB）⇒ 要按"预算 → 块数"给建议，单价必须**当场读**，不能引用常数。
    ///
    /// **实测（release，600k 档，M57）**：bplus 每页 走查 **4 264 B** / 乱序点读 **6 838 B**
    /// （全驻留总账 151.2 MB vs **237.2 MB**）；lsm 每块 **10.1 KB**，两种姿势一字不差（全驻留 **90.8 MB**）
    /// ⇒ README 里那句"lsm 600k 全驻留 ≈ 18.7 MB"是**拿错了档**（那是 M23 里 40 000 键 / 2 681 块那一档）。
    ///
    /// 读数用 `mem_probe::live()`（跟踪分配器的净活字节，不是 RSS）：先用**默认档**把整库焐一遍
    /// （填满 cap 个槽），再把顶抬掉焐第二遍（补剩下的槽），两趟各除各的槽数 ⇒ 每槽真实字节。
    ///
    /// ⚠️ **两种姿势的单价不一样，必须分开读**（`CACHE_PRICE_POSTURE=walk|point`）：
    /// 走查（`scan_page`）按设计**只填页缓存**——`bplus_tree.rs:1931` 那句"每片叶子只碰一次，回填它不会有下一次命中"
    /// ⇒ 它不进解析缓存；而**乱序点读**两把都填（`node_of` → `memoize_node`，上限与页缓存同源）。
    /// 所以拿"走查姿势"的每槽字节去给点读型下游算预算会**系统性低估**（最多差一倍）。
    /// ⚠️ 墙钟不作凭据（本机跨轮漂 1.3~1.9×），这一格只卖字节。
    ///
    /// ```text
    /// CACHE_PRICE_ROWS=600000 CACHE_PRICE_POSTURE=point cargo test --release --lib -- --ignored --nocapture \
    ///     --test-threads=1 cache_slot_resident_price
    /// ```
    #[test]
    #[ignore = "缓存槽价签：手动 release 单线程跑：CACHE_PRICE_ROWS=200000 CACHE_PRICE_POSTURE=point cargo test --release --lib -- --ignored --nocapture --test-threads=1 cache_slot_resident_price"]
    fn cache_slot_resident_price() {
        let rows: usize = std::env::var("CACHE_PRICE_ROWS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(200_000);
        fn key(i: usize) -> Vec<u8> {
            format!("{i:08}#/worldquant/datafields/x.json").into_bytes()
        }
        fn val(i: usize) -> Vec<u8> {
            format!("{{\"i\":{i},\"h\":\"{:0<28}\",\"v\":1}}", i * 7).into_bytes()
        }
        let mb = |b: i64| b as f64 / 1024.0 / 1024.0;

        for kind in [EngineKind::BPlusTree, EngineKind::LsmTree] {
            let label = kind.as_str();
            let dir = tmp_dir(&format!("cache_price_{label}"));
            let arg = match kind {
                EngineKind::BPlusTree => Some(dir.join("bplus.db")),
                _ => Some(dir.clone()),
            };
            let arg = arg.as_deref();
            {
                let db = open_engine(kind, arg).unwrap();
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
                db.flush().unwrap();
                db.checkpoint().unwrap();
            }
            // 冷重开 ⇒ 缓存是空的（焐之前一个槽都没有）
            let db = open_engine(kind, arg).unwrap();
            let t = db.tuning();
            let (cap, denom) = match kind {
                EngineKind::BPlusTree => (
                    t.get("max_cache_pages").unwrap_or("0").to_string(),
                    t.get("total_pages").unwrap_or("0").to_string(),
                ),
                _ => (
                    t.get("block_cache_capacity").unwrap_or("0").to_string(),
                    t.get("sst_blocks").unwrap_or("0").to_string(),
                ),
            };
            let cap_n: usize = cap.parse().unwrap_or(0);
            let denom_n: usize = denom.parse().unwrap_or(0);
            // ⚠️ `order` 必须**在取 `l0` 之前**分配：它是 600k × 8 B = 4.8 MB 的活字节，
            // 晚分配就把它算进每一趟的差值里（默认档那趟 4.8 MB ÷ 2048 槽 ⇒ 每槽虚高 2.3 KB，
            // 正好足够把"要不要抬顶"的结论推翻）。
            let order = shuffled(rows);
            let posture = std::env::var("CACHE_PRICE_POSTURE").unwrap_or_else(|_| "walk".to_string());
            let l0 = crate::mem_probe::live() as i64;
            // 一次焐满：走查姿势只填页缓存，点读姿势两把都填（见上面那段）
            let do_fill = |db: &dyn KvEngine, count: bool| -> usize {
                let mut touched = 0usize;
                if posture == "point" {
                    let tx = db.begin();
                    for &i in &order {
                        if tx.get(&key(i)).is_some() {
                            touched += 1;
                        }
                    }
                } else {
                    let mut after: Option<Vec<u8>> = None;
                    loop {
                        let tx = db.begin();
                        let (page, next) = tx.scan_page(after.as_deref(), 512);
                        if count {
                            touched += page.len();
                        }
                        drop(page);
                        match next {
                            Some(n) => after = Some(n),
                            None => break,
                        }
                    }
                }
                touched
            };
            // 第一趟：默认档焐满 cap 个槽
            let seen = do_fill(db.as_ref(), true);
            let l1 = crate::mem_probe::live() as i64;
            // 第二趟：把顶抬掉再焐一遍 ⇒ 增量就是"补上的那些槽"的字节
            db.set_cache_auto_max(usize::MAX).unwrap();
            let _ = do_fill(db.as_ref(), false);
            let l2 = crate::mem_probe::live() as i64;
            let t2 = db.tuning();
            let cap2: usize = match kind {
                EngineKind::BPlusTree => t2.get("max_cache_pages"),
                _ => t2.get("block_cache_capacity"),
            }
            .unwrap_or("0")
            .parse()
            .unwrap_or(0);
            let filled_a = cap_n.min(denom_n).max(1);
            let filled_b = cap2.saturating_sub(filled_a);
            eprintln!(
                "{label:<10} [{posture}] 默认档 cap={cap_n} 分母={denom_n} 走 {seen} 行 ｜ 焐满 cap 个槽：live {:+.1} MB ⇒ \
                 **每槽 {:.0} B**（抬顶后共住 {cap2} 槽、再多 {filled_b} 个槽花 {:+.1} MB ⇒ 每槽 {:.0} B ｜ 全驻留共 {:+.1} MB）",
                mb(l1 - l0),
                (l1 - l0) as f64 / filled_a as f64,
                mb(l2 - l1),
                if filled_b > 0 {
                    (l2 - l1) as f64 / filled_b as f64
                } else {
                    0.0
                },
                mb(l2 - l0),
            );
            let _ = std::fs::remove_dir_all(&dir);
        }
        eprintln!(
            "⚠️ 姿势不同、价就不同：走查只填**页缓存**（`bplus_tree.rs:1931` 那句\"回填它不会有下一次命中\"），\
             点读两把都填 ⇒ 实测 bplus 每页 走查 4.2 KB / 点读 6.8 KB（lsm 10.1 KB，姿势无关）。\
             给点读型下游定预算要用 point 那一读数，别按 4 KiB 估。"
        );
    }

    /// M76 的验收台架：按**内存预算**给缓存档之后，实测常驻必须真的**不超过给的那些字节**
    /// （`#[ignore]`，手动 release 单线程跑）。
    ///
    /// 为什么这一格不能在单元守卫里量：守卫钉的是**槽数**（`容量 = 预算 ÷ 每槽价签`、封顶被执行），
    /// 而"预算 = 内存上限"这句话的最终凭据是**真实字节** —— 那要 `mem_probe::live()`，
    /// 它是进程级计数，并行用例互相污染（M27/M36/M52 各栽过一次），只能单线程 release 跑。
    ///
    /// 两把尺子必须对上，这才算机制而不是巧合：
    /// ① 换算尺（守卫里）：`槽数 == 预算 ÷ 价签`；
    /// ② 字节尺（这里）：`焐满之后 live 增量 ≤ 预算 × 1.25`，并顺带读出**这一档的每槽实测价** ——
    ///   它比价签小 ⇒ 价签是保守的（宁少不多，符合"预算不许超"）；它比价签大 ⇒ 换算在超支，红。
    ///
    /// ```text
    /// CACHE_BUDGET_ROWS=600000 CACHE_BUDGET_MB=32 CACHE_BUDGET_POSTURE=point cargo test --release --lib -- \
    ///     --ignored --nocapture --test-threads=1 cache_budget_bytes_acceptance
    /// ```
    #[test]
    #[ignore = "预算档验收：手动 release 单线程跑：CACHE_BUDGET_ROWS=600000 CACHE_BUDGET_MB=32 cargo test --release --lib -- --ignored --nocapture --test-threads=1 cache_budget_bytes_acceptance"]
    fn cache_budget_bytes_acceptance() {
        let rows: usize = std::env::var("CACHE_BUDGET_ROWS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(600_000);
        let budget_mb: u64 = std::env::var("CACHE_BUDGET_MB")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(32);
        let budget = budget_mb * 1_000_000;
        let posture =
            std::env::var("CACHE_BUDGET_POSTURE").unwrap_or_else(|_| "point".to_string());
        fn key(i: usize) -> Vec<u8> {
            format!("{i:08}#/worldquant/datafields/x.json").into_bytes()
        }
        fn val(i: usize) -> Vec<u8> {
            format!("{{\"i\":{i},\"h\":\"{:0<28}\",\"v\":1}}", i * 7).into_bytes()
        }
        let mb = |b: i64| b as f64 / 1024.0 / 1024.0;

        eprintln!(
            "=== 预算档验收 rows={rows} 预算={budget_mb} MB({budget} B) 姿势={posture} ==="
        );
        for kind in [EngineKind::BPlusTree, EngineKind::LsmTree] {
            let label = kind.as_str();
            let dir = tmp_dir(&format!("cache_budget_accept_{label}"));
            let arg = match kind {
                EngineKind::BPlusTree => Some(dir.join("bplus.db")),
                _ => Some(dir.clone()),
            };
            let arg = arg.as_deref();
            {
                let db = open_engine(kind, arg).unwrap();
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
                db.flush().unwrap();
                db.checkpoint().unwrap();
            }
            // 冷重开 ⇒ 缓存空着，量出来的才是"这一档买了多少字节"
            let db = open_engine(kind, arg).unwrap();
            // ⚠️ `order` 必须在取基线**之前**分配（M57 那条：晚分配就把它算进差值，每槽虚高 2.3 KB）
            let order = shuffled(rows);
            let slots = db.set_cache_budget_bytes(budget).expect("设预算不该报错");
            let t = db.tuning();
            let cap_key = if kind == EngineKind::BPlusTree { "max_cache_pages" } else { "block_cache_capacity" };
            let cap: usize = t.get(cap_key).unwrap_or("0").parse().unwrap_or(0);
            let denom: usize = t
                .get(if kind == EngineKind::BPlusTree { "total_pages" } else { "sst_blocks" })
                .unwrap_or("0")
                .parse()
                .unwrap_or(0);
            assert_eq!(cap, slots, "{label}: 返回的槽数与 tuning 里的生效容量不是同一个数");
            let l0 = crate::mem_probe::live() as i64;
            if posture == "point" {
                let tx = db.begin();
                for &i in &order {
                    assert!(tx.get(&key(i)).is_some(), "{label}: 点读落空");
                }
            } else {
                let mut after: Option<Vec<u8>> = None;
                loop {
                    let tx = db.begin();
                    let (page, next) = tx.scan_page(after.as_deref(), 512);
                    drop(page);
                    match next {
                        Some(n) => after = Some(n),
                        None => break,
                    }
                }
            }
            let l1 = crate::mem_probe::live() as i64;
            let held = l1 - l0;
            eprintln!(
                "{label:<10} [{posture}] 预算 {budget} B ⇒ 容量 {cap} 槽（分母 {denom}，覆盖率 {:.1}%）｜\
                 焐满之后 live {:+.1} MB ⇒ **每槽实测 {:.0} B**（换算按价签，所以这一档只可能少买不可能多买）",
                if denom > 0 { cap as f64 / denom as f64 * 100.0 } else { 0.0 },
                mb(held),
                if cap > 0 { held as f64 / cap as f64 } else { 0.0 },
            );
            assert!(
                held <= (budget as i64) * 5 / 4,
                "{label}: 给了 {budget} B 的预算，焐满之后实测住了 {held} B（>预算×1.25）⇒ \
                 要么价签偏小、要么封顶没被执行 ⇒ 下游照这个数封 RSS 会爆",
            );
            drop(db);
            let _ = std::fs::remove_dir_all(&dir);
        }
        eprintln!("CACHE BUDGET DONE");
    }

    /// M75 第一步：**只量不改** —— 标准表里 `search 末页(深翻)` 那 7.46 s 到底花在哪几段。
    ///
    /// 那一格的写法是 `contains(b"", last_page, 100)` 且**要精确 total** ⇒ 它一次付三笔：
    /// ① 数全库要 total；② offset 跳过 `last_page × 页宽` 行；③ 取那一页。
    /// 分开量才知道该修哪一段（M51 治的是"把前面每一页都攒在堆上"的**内存**，没动时间；
    /// M58 只是把默认姿势写成游标，没让 offset 变便宜）。
    ///
    /// 每段两遍取最小（本机单样本 µs 不可信，M50/M62b 那条规矩）；内存列不看这里，标准表有。
    ///
    /// **M75b 之后这张表怎么读**（改的是判据，不是台架）：①②③在同一次进程里跑 ⇒ 是**同构建 A/B**。
    /// 旧形状 `③ ≈ ① + ②`（实测差 −3.7%~+5.3%，即"一次调用走两遍全库"）；合成一趟之后判据换成
    /// **`③/① ≈ 1×`** —— 这一列打在哪一臂上，一眼就能看出是不是又分家成两趟了。
    ///
    /// 跑法：`SCALE_ROWS=600000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 deep_page_stage_split`
    #[test]
    #[ignore = "深翻分段归因，手动 release 单线程跑：SCALE_ROWS=600000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 deep_page_stage_split"]
    fn deep_page_stage_split() {
        let rows: usize = std::env::var("SCALE_ROWS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(600_000);
        let page_size = 100usize;
        let last_page = rows / page_size;

        fn key(i: usize) -> Vec<u8> {
            format!("{i:08}#/worldquant/datafields/x.json").into_bytes()
        }
        fn val(i: usize) -> Vec<u8> {
            format!("{{\"i\":{i},\"h\":\"{:0<28}\",\"v\":1}}", i * 7).into_bytes()
        }
        /// 跑 n 遍取最小；返回 (最快耗时, 最后一次的结果)
        fn min_of<T>(n: usize, mut f: impl FnMut() -> T) -> (f64, T) {
            let mut best = f64::MAX;
            let mut last = None;
            for _ in 0..n {
                let t = std::time::Instant::now();
                let out = f();
                let us = t.elapsed().as_secs_f64() * 1e6;
                if us < best {
                    best = us;
                }
                last = Some(out);
            }
            (best, last.unwrap())
        }

        eprintln!(
            "=== 深翻分段归因 rows={rows} 页宽={page_size} 末页号={last_page}（每段两遍取最小，µs）==="
        );
        for kind in [
            EngineKind::Bitcask,
            EngineKind::BPlusTree,
            EngineKind::LsmTree,
            EngineKind::Memory,
        ] {
            let label = kind.as_str();
            let dir = tmp_dir(&format!("deep_page_{label}"));
            let arg = match kind {
                EngineKind::Memory => None,
                EngineKind::BPlusTree => Some(dir.join("bplus.db")),
                _ => Some(dir.clone()),
            };
            let db = open_engine(kind, arg.as_deref()).unwrap();
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
            db.flush().unwrap();
            db.checkpoint().unwrap();

            // ① 只要 total，不翻页（页宽 1、第 0 页）⇒ 这一格 ≈ 数全库的净代价
            let (t_total, one_row) = min_of(2, || {
                let tx = db.begin();
                let p = tx.search_keys(&crate::common::SearchQuery::contains(b"", 0, 1));
                tx.commit();
                p
            });
            // ② 免 total、翻到末页 ⇒ 这一格 ≈ offset 跳过的净代价（+ 取 100 条）
            let (t_skip, _) = min_of(2, || {
                let tx = db.begin();
                let p = tx.search_keys(
                    &crate::common::SearchQuery::contains(b"", last_page, page_size).without_total(),
                );
                tx.commit();
                p
            });
            // ③ 两笔都要 = 标准表里那一格
            let (t_both, deep) = min_of(2, || {
                let tx = db.begin();
                let p = tx.search_keys(&crate::common::SearchQuery::contains(b"", last_page, page_size));
                tx.commit();
                p
            });
            // ④ 游标一跳（拿到末页起点的游标之后，只量**一跳**）
            let (_warm, next) = {
                let tx = db.begin();
                let out = tx.scan_page(None, last_page * page_size);
                tx.commit();
                out
            };
            let (t_hop, hopped) = min_of(2, || {
                let tx = db.begin();
                let out = tx.scan_page(next.as_deref(), page_size);
                tx.commit();
                out
            });
            // ⑤ 对照：纯数一遍（不取任何键值）
            let (t_count, n_keys) = min_of(2, || {
                let tx = db.begin();
                let c = tx.key_count(&key(0)[..2]);
                tx.commit();
                c
            });

            eprintln!(
                "{label:<10} ①total(页宽1) {t_total:>11.1}  ②offset免total {t_skip:>11.1}  \
                 ③两者都要(表里那格) {t_both:>11.1}  ④游标一跳 {t_hop:>9.1}  ⑤key_count {t_count:>11.1}"
            );
            eprintln!(
                "{:<10}   折算：①每键 {:.3} µs  ②每跳过行 {:.3} µs  ③/① {:.2}×  ③=①+②? 差 {:.1}%  \
                 ④单页 {} 条(命中末页={})  ⑤数到 {} 键",
                "",
                (t_total - one_row.items.len() as f64 * 0.0) / rows as f64,
                t_skip / (last_page * page_size) as f64,
                if t_total > 0.0 { t_both / t_total } else { 0.0 },
                if t_both > 0.0 {
                    (t_both - (t_total + t_skip)) / t_both * 100.0
                } else {
                    0.0
                },
                hopped.0.len(),
                deep.items.len(),
                n_keys,
            );
            drop(db);
            cleanup(vec![Some(dir)]);
        }
        eprintln!("DEEP PAGE DONE");
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
                let st_base = open_stages_begin();
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
                open_stages_report(label, st_base);
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

            // 命中与未命中**必须分开数**：混在一个平均值里，"点查 0.86 ms" 那种症状就归不了因
            // （M10 的教训）。这 1000 把键全是不存在的键，单价就是"查无此键"的成本。
            let miss_chunk: Vec<Vec<u8>> = (0..1000.min(rows)).map(|i| key(rows * 8 + i)).collect();
            let (d, live, peak, got) = timed(|| {
                let tx = db.begin();
                tx.multi_get(&miss_chunk)
            });
            let hit = got.iter().filter(|o| o.is_some()).count();
            line(label, "multi_get×1000 全miss", d.as_secs_f64() * 1e6 / miss_chunk.len() as f64, "µs/键", live, peak, &format!("读到 {hit} 条(应为 0)"));

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

            // ---- 游标翻页：把整库一页一页翻完（长跑进程真正在用的那条路，M17） ----
            let (d, live, peak, (pages, moved)) = timed(|| {
                let tx = db.begin();
                let mut cursor: Option<Vec<u8>> = None;
                let mut pages = 0usize;
                let mut moved = 0usize;
                // 保险丝：页宽 500 ⇒ 正常应当走 ~rows/500 页，超两倍就是走不动了
                while pages <= rows / 250 + 8 {
                    let (chunk, next) = tx.scan_page(cursor.as_deref(), 500);
                    moved += chunk.len();
                    pages += 1;
                    let Some(next) = next else { break };
                    cursor = Some(next);
                }
                (pages, moved)
            });
            line(label, "scan_page 翻完整库", d.as_secs_f64() * 1e6 / pages.max(1) as f64, "µs/页", live, peak, &format!("{pages} 页 / {moved} 行（页宽 500）"));
            assert_eq!(moved, rows, "{label}: scan_page 翻完整库必须正好交出 {rows} 行（少一行=游标走不动，多走=翻页有洞）");

            // ---- 反向与定位：B+ 树只有单向 next_leaf，最容易在这两条上露形状 ----
            let (d, live, peak, back) = timed(|| {
                let tx = db.begin();
                tx.seek_prev(&key(rows / 2))
            });
            line(label, "seek_prev 中间键", d.as_secs_f64() * 1e6, "µs/次", live, peak, &format!("命中 {}", back.is_some()));

            let (d, live, peak, rwin) = timed(|| {
                let tx = db.begin();
                tx.reverse_scan(Some(&lo), Some(&hi))
            });
            let rw = rwin.len().max(1);
            line(label, "reverse_scan 1% 窗口", d.as_secs_f64() * 1e6 / rw as f64, "µs/行", live, peak, &format!("{rw} 行"));

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

            // ---- TTL 往返：写 1000 个带过期时间的键，再把剩余时间读回来 ----
            // （长跑里"会话键续期"就是这个形状；只量写不量读会把信封解析的成本藏掉）
            let ttl_rows = 1000.min(rows);
            let (d, live, peak, ()) = timed(|| {
                let tx = db.begin();
                for i in 0..ttl_rows {
                    tx.set_with_ttl(&key(rows * 4 + i), val(i), std::time::Duration::from_secs(3600));
                }
                tx.commit();
            });
            line(label, "set_with_ttl×1000", d.as_secs_f64() * 1e6 / ttl_rows as f64, "µs/次", live, peak, "");
            let (d, live, peak, hits) = timed(|| {
                let tx = db.begin();
                (0..ttl_rows)
                    .filter(|i| tx.get_ttl(&key(rows * 4 + i)).flatten().is_some())
                    .count()
            });
            line(label, "get_ttl×1000", d.as_secs_f64() * 1e6 / ttl_rows as f64, "µs/次", live, peak, &format!("{hits}/{ttl_rows} 有 TTL"));
            assert_eq!(hits, ttl_rows, "{label}: 刚写的 TTL 必须全部读得回来（读不回来=信封在某一家被吞了）");

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

            let (d, live, peak, vr) = timed(|| db.vacuum_bounded(None, 2048).unwrap());
            line(label, "vacuum_bounded 一片", d.as_secs_f64() * 1e6, "µs/次", live, peak,
                 &format!("扫 {} 行、裁 {} 行（预算 2048）", vr.stats.rows_scanned, vr.stats.versions_removed));

            let (d, live, peak, ()) = timed(|| {
                db.flush().unwrap();
                let _ = db.merge();
            });
            let disk2 = if kind == EngineKind::Memory { 0 } else { disk_bytes(arg.unwrap()) };
            line(label, "flush+merge", d.as_secs_f64() * 1e6, "µs/次", live, peak, &format!("磁盘 {:.1} MB", disk2 as f64 / 1e6));

            // ---- bulk 导入（另一条写路径：无 WAL、无冲突检测） ----
            // ---- 物化全库的出口：`Vec` 有多大、峰值多少 MB，就是这条的意义 ----
            // （翻页请走 scan_page；这条留在台架里就是为了把"别用它"量化出来）
            let (d, live, peak, exp) = timed(|| db.export_latest_visible(false));
            let en = exp.len().max(1);
            line(label, "export 全库(物化)", d.as_secs_f64() * 1e6 / en as f64, "µs/行", live, peak, &format!("{en} 行进 Vec"));
            drop(exp);

            // ---- 流式导出（M44）：同样走完全库、同样落到文件，但一行都不攒 ----
            // 与上面那一行是**同一件事的两种写法**，所以这一对就是这一刀的凭据：
            // 峰值不该再随库大小涨（那是调用方要的那份 `Vec`，不是遍历的账）。
            let snap = dir.join("export_stream.bin");
            // `tmp_dir` 只负责清掉旧残留，**不创建目录** —— 磁盘引擎由 open 时自己建，
            // memory 一家没人建 ⇒ 这里不先 create_dir_all 就会在 memory 那一档 panic（第一版就栽了）。
            let _ = std::fs::create_dir_all(&dir);
            let (d, live, peak, n) = timed(|| {
                let mut w = crate::common::kv_snapshot::ExportWriter::bin(&snap, false).unwrap();
                let handed = db.export_latest_visible_for_each(false, &mut |rec| w.push(rec));
                let stats = w.finish().unwrap();
                assert_eq!(handed, stats.records, "{label}: 折出的行数与写出的行数不同");
                handed
            });
            let size = std::fs::metadata(&snap).map(|m| m.len()).unwrap_or(0);
            line(label, "export 流式(逐行落地)", d.as_secs_f64() * 1e6 / n.max(1) as f64, "µs/行", live, peak,
                 &format!("{n} 行 → 磁盘 {:.1} MB，内存里一行都不攒", size as f64 / 1e6));
            let _ = std::fs::remove_file(&snap);

            // ---- 跨引擎搬家：这一家 → memory（读侧 scan_page、写侧一页一事务，M17）----
            // 期望值现问源库要（`live_keys`），因为此刻库里除了灌的 {rows} 行，还多了上面
            // TTL/计数器那两条写进去的键 —— 拿 {rows} 当期望会假红。
            let want = db.health().live_keys;
            let target = open_engine(EngineKind::Memory, None).unwrap();
            let (d, live, peak, ms) = timed(|| migrate(&*db, &*target, 1000).unwrap());
            line(label, "migrate → memory", d.as_secs_f64() * 1e6 / rows.max(1) as f64, "µs/行", live, peak,
                 &format!("搬 {} 行 / {} 页 / {} B", ms.rows, ms.pages, ms.bytes));
            assert_eq!(ms.rows, want, "{label}: migrate 搬走的行数必须等于源库 live_keys（少了=游标或过滤有洞）");
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
                let st_base = open_stages_begin();
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
                open_stages_report(label, st_base);
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

    /// M68 —— 把 `tools` 搬运层量进标准表：**三跳每跳跑两臂**（流式 vs 先物化整库），同一进程同一构建 ⇒ 比值即因果。
    ///
    /// 为什么这条存在：M63 交了 `storage_engines::tools`，而 122 项标准表里**没有它一格**
    /// （M67 记的那条"空白"）⇒ 用户问"各个功能有没有进步"时，唯一的新功能恰好没被量到；
    /// 而它"不物化整库"此前只是照 `export 流式` 那一行**推**出来的，不是实测。
    ///
    /// 台架形状 = 下游的用法：一份 root 下每个引擎一个子目录，逐跳搬
    /// ① bitcask→lsm ② lsm→bplus ③ bplus→bitcask（`wipe_target = true`，就是 M63b 真编真跑那条三跳），
    /// 再单独量第四格：文件往返（`export_to_file` bin → `import_from_file` bin）—— 这一格只有流式一条臂，
    /// 因为"物化整库再写文件"不是任何人的用法，硬造一个对照臂 = 给自己造一个不会被读的数字。
    ///
    /// **两臂只差读侧**：老形状 = M44 之前的 `export_latest_visible`（一次把整库做成 `Vec`），
    /// 写侧与 `tools` 一字不差（同一份 `begin_bulk` + 同一批 `pack_plain` + 同样的批宽）⇒
    /// 差值只归"读侧中间那份 `Vec`"。每跳都硬对拍：目标回读条数必须 == 行数（少一行就是 M45 那次静默丢数据）。
    /// ⚠️ 三跳两臂的**先后顺序交替**（①新→老、②老→新、③新→老）：M52 那条"排位会冒充形状"。
    ///
    /// 跑法：`SCALE_ROWS=600000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 tools_migrate_scale`
    #[test]
    #[ignore = "tools 搬运层的规模台架，手动 release 单线程跑：SCALE_ROWS=600000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 tools_migrate_scale"]
    fn tools_migrate_scale() {
        let rows: usize = std::env::var("SCALE_ROWS")
            .ok()
            .and_then(|s| s.parse().ok())
            // 默认档给 200k 而不是 600k：这一格要跑**七次搬运**，600k 那档留给标准复测那一轮起环境变量
            .unwrap_or(200_000);
        fn key(i: usize) -> Vec<u8> {
            format!("{i:08}#/worldquant/datafields/x.json").into_bytes()
        }
        fn val(i: usize) -> Vec<u8> {
            format!("{{\"i\":{i},\"h\":\"{:0<28}\",\"v\":1}}", i * 7).into_bytes()
        }
        /// 路径**必须**从 `tools` 公开的那两个函数拿：它俩就是 `migrate` 自己用的那一份，
        /// 自己拼一遍就会在 bplus 那个"先认已存在的库文件名"上选错 ⇒ 凭空造一个空库当目标。
        fn path_for(engine: EngineKind, root: &std::path::Path) -> std::path::PathBuf {
            match engine {
                EngineKind::BPlusTree => crate::tools::bplus_db_path(root),
                other => crate::tools::engine_data_path(root, other),
            }
        }
        /// 老形状那一臂：读侧整库 `Vec` ⇒ 写侧与 `tools::pump_*` 同一份逻辑（批宽也对齐）。
        fn old_shape_hop(
            from: EngineKind,
            to: EngineKind,
            root: &std::path::Path,
        ) -> usize {
            const BATCH: usize = 8192;
            let src = open_engine(from, Some(&path_for(from, root))).expect("开源库");
            let recs = src.begin().export_latest_visible(false);
            drop(src);
            let dst_path = path_for(to, root);
            let _ = std::fs::remove_dir_all(&dst_path);
            let _ = std::fs::remove_file(&dst_path);
            let dst = open_engine(to, Some(&dst_path)).expect("开目标库");
            let mut bulk = dst.begin_bulk();
            let mut buf: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(BATCH);
            for rec in recs {
                let v = rec.value.expect("include_deleted=false 不该交出 tombstone 行");
                buf.push((rec.key, crate::common::pack_plain(&v)));
                if buf.len() >= BATCH {
                    bulk.put_batch_owned(std::mem::take(&mut buf));
                    buf = Vec::with_capacity(BATCH);
                }
            }
            if !buf.is_empty() {
                bulk.put_batch_owned(buf);
            }
            bulk.finish();
            let n = dst.begin().key_count(b"");
            drop(dst);
            n
        }
        fn hop_new(from: EngineKind, to: EngineKind, root: &std::path::Path) -> usize {
            crate::tools::migrate(from, to, root, false, true)
                .unwrap_or_else(|e| panic!("{}→{} 流式搬失败: {e}", from.as_str(), to.as_str()))
                .verify_live
        }

        let root = tmp_dir("tools_migrate");
        eprintln!("=== tools 搬运台架 rows={rows}（µs/行 + 常驻/峰值 MB）===");
        {
            let src = open_engine(EngineKind::Bitcask, Some(&path_for(EngineKind::Bitcask, &root)))
                .expect("开源库");
            for base in (0..rows).step_by(1000) {
                let end = (base + 1000).min(rows);
                let items: Vec<(Vec<u8>, Vec<u8>)> =
                    (base..end).map(|i| (key(i), val(i))).collect();
                let tx = src.begin();
                if let Err(bad) = tx.batch_set(&items) {
                    panic!("灌源库 batch_set 冲突 {}", String::from_utf8_lossy(&bad));
                }
                tx.commit();
            }
            src.flush().unwrap();
            src.checkpoint().unwrap();
        }

        let hops = [
            (EngineKind::Bitcask, EngineKind::LsmTree),
            (EngineKind::LsmTree, EngineKind::BPlusTree),
            (EngineKind::BPlusTree, EngineKind::Bitcask),
        ];
        for (idx, (from, to)) in hops.into_iter().enumerate() {
            let tag = format!("{}→{}", from.as_str(), to.as_str());
            // 两臂交替在前：跳 0/2 先跑流式，跳 1 先跑老形状
            let order: [&str; 2] = if idx % 2 == 0 {
                ["流式", "物化Vec"]
            } else {
                ["物化Vec", "流式"]
            };
            for arm in order {
                let streaming = arm == "流式";
                let (d, live, peak, got) = timed(|| {
                    if streaming {
                        hop_new(from, to, &root)
                    } else {
                        old_shape_hop(from, to, &root)
                    }
                });
                line(
                    "tools",
                    &format!("{tag} {arm}"),
                    d.as_secs_f64() * 1e6 / rows as f64,
                    "µs/行",
                    live,
                    peak,
                    &format!("回读 {got} 行"),
                );
                assert_eq!(
                    got, rows,
                    "{tag} {arm}：目标回读 {got} 行 ≠ 源 {rows} 行 ⇒ 搬运少/多了行"
                );
            }
        }

        // 第四格：文件往返（生产走的那条：导出 bin → 导入 bin），只有流式一条臂
        let file = root.join("snap.bin");
        let (d, live, peak, ()) = timed(|| {
            crate::tools::export_to_file(
                EngineKind::Bitcask,
                &root,
                &file,
                crate::tools::SnapshotFormat::Bin,
                false,
            )
            .expect("导出 bin 失败");
        });
        line(
            "tools",
            "export bin(源=bitcask)",
            d.as_secs_f64() * 1e6 / rows as f64,
            "µs/行",
            live,
            peak,
            &format!("文件 {:.1} MB", disk_bytes(&file) as f64 / 1e6),
        );
        let (d, live, peak, got) = timed(|| {
            crate::tools::import_from_file(
                EngineKind::LsmTree,
                &root,
                &file,
                crate::tools::SnapshotFormat::Bin,
                true,
            )
            .expect("导入 bin 失败")
            .live
        });
        line(
            "tools",
            "import bin(目标=lsm)",
            d.as_secs_f64() * 1e6 / rows as f64,
            "µs/行",
            live,
            peak,
            &format!("回读 {got} 行"),
        );
        assert_eq!(got, rows, "文件往返少/多了行：{got} ≠ {rows}");

        let _ = std::fs::remove_dir_all(&root);
        eprintln!("TOOLS MIGRATE DONE");
    }

    /// M62b：**单次调用**的跨档比值，把**结构计数**摆在一起看（`#[ignore]`，手动 release 单线程跑）。
    ///
    /// 为什么要单独一把尺子：复测表里 memory `seek_prev` 读 2.52×（17.8 → 44.8 µs/次）、
    /// lsm `vacuum_bounded 一片` 读 2.21×（2.08 → 4.61 ms/片），而这两格在 `scale_baseline` 里都是
    /// `timed()` 里的**一次调用** ⇒ 按 M50 那条规矩（µs 级单次采样在本机不作 A/B 凭据）它们只能算"登记"。
    /// 这里把同一次动作量 **2000 次**、并把计数尺一起交出来：
    /// - **memory**：`seek_prev` 往回退过几把键（`common::seek_prev_steps`）。memory 不落盘、没有"缓存容量"
    ///   那一格 ⇒ 若步数平而 µs 涨，涨的只能是库变大之后的**内存局部性**（BTreeMap 更高、更散），不是新通道。
    /// - **lsm**：一片实际交出/访问多少行（`sst::light_visits`）、读了几块（`sst::block_reads`）、
    ///   开了几个文件（`common::disk::file_opens`）+ 盘上有几篇 SST。M29 那句"一片的预算行数是常数"要在这里当场可见，
    ///   而"每多一篇文件就多一次冷启动"那笔（M15b）必须落在**文件数/块数**上才算归因完成。
    /// - **lsm `seek_prev`**（M78 阶梯加的那条臂）：大档阶梯里这一格 60 万 → 1000 万读 **28.5×**（391.5 →
    ///   11150.2 µs/次）而行数只涨 16.7×，是四家里唯一"单次调用的钱自己在超线性涨"的格子 ⇒ 必须当场看
    ///   步数/块数/开文件数有没有跟着涨，才能分清"结构（文件/层数）"还是"局部性"。
    ///
    /// 判据只看计数；µs 两轮取最小只用来把数字摆在一起（本机跨轮漂 1.3~1.9×）。
    ///
    /// ```text
    /// SINGLE_SHAPE_ROWS=20000,600000 cargo test --release --lib -- --ignored --nocapture \
    ///   --test-threads=1 single_call_shape_attribution
    /// ```
    #[test]
    #[ignore = "单次调用的结构归因，手动 release 单线程跑：SINGLE_SHAPE_ROWS=20000,600000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 single_call_shape_attribution"]
    fn single_call_shape_attribution() {
        let tiers: Vec<usize> = std::env::var("SINGLE_SHAPE_ROWS")
            .unwrap_or_else(|_| "20000,600000".to_string())
            .split(',')
            .filter_map(|s| s.trim().parse::<usize>().ok())
            .collect();
        // 形状照抄 scale_baseline：键 ≈35 B、值 ≈60 B、1000 行一事务、再更新前 10% 的键（⇒ 版本链长 2）
        fn key(i: usize) -> Vec<u8> {
            format!("{i:08}#/worldquant/datafields/x.json").into_bytes()
        }
        fn val(i: usize) -> Vec<u8> {
            format!("{{\"i\":{i},\"h\":\"{:0<28}\",\"v\":1}}", i * 7).into_bytes()
        }
        const CALLS: usize = 2000;
        const SLICES: usize = 3;
        eprintln!("=== 单次调用的结构归因 tiers={tiers:?}（seek_prev {CALLS} 次 × 2 轮；一片 × {SLICES} 次）===");
        for &rows in &tiers {
            let updates = (rows / 10).max(1);
            // ---------------- memory：seek_prev ----------------
            {
                let db = open_engine(EngineKind::Memory, None).unwrap();
                for base in (0..rows).step_by(1000) {
                    let end = (base + 1000).min(rows);
                    let items: Vec<(Vec<u8>, Vec<u8>)> =
                        (base..end).map(|i| (key(i), val(i))).collect();
                    let tx = db.begin();
                    tx.batch_set(&items).unwrap();
                    tx.commit();
                }
                for base in (0..updates).step_by(500) {
                    let end = (base + 500).min(updates);
                    let items: Vec<(Vec<u8>, Vec<u8>)> =
                        (base..end).map(|i| (key(i), format!("u{i}").into_bytes())).collect();
                    let tx = db.begin();
                    tx.batch_set(&items).unwrap();
                    tx.commit();
                }
                let mid = key(rows / 2);
                // 焐一次：第一次要把那一带的页/节点摸进缓存，与台架里"冷单次"不同形状
                {
                    let tx = db.begin();
                    assert!(tx.seek_prev(&mid).is_some(), "seek_prev 必须命中中间那把键");
                }
                let s0 = crate::common::store::seek_prev_steps();
                let mut best = f64::MAX;
                let mut last = f64::MAX;
                for _ in 0..2 {
                    let t = std::time::Instant::now();
                    for _ in 0..CALLS {
                        let tx = db.begin();
                        assert!(tx.seek_prev(&mid).is_some());
                    }
                    last = t.elapsed().as_secs_f64() * 1e6 / CALLS as f64;
                    best = best.min(last);
                }
                let steps = crate::common::store::seek_prev_steps() - s0;
                eprintln!(
                    "[memory rows={rows}] seek_prev：{best:.1} µs/次（另一轮 {last:.1}）｜ 步/次 = {:.2} \
                     （累计 {steps} ÷ {}）",
                    steps as f64 / (CALLS as f64 * 2.0),
                    CALLS * 2,
                );
            }
            // ---------------- lsm：vacuum_bounded 一片 ----------------
            {
                let dir = tmp_dir("shape_lsm");
                let db = open_engine(EngineKind::LsmTree, Some(&dir)).unwrap();
                for base in (0..rows).step_by(1000) {
                    let end = (base + 1000).min(rows);
                    let items: Vec<(Vec<u8>, Vec<u8>)> =
                        (base..end).map(|i| (key(i), val(i))).collect();
                    let tx = db.begin();
                    tx.batch_set(&items).unwrap();
                    tx.commit();
                }
                for base in (0..updates).step_by(500) {
                    let end = (base + 500).min(updates);
                    let items: Vec<(Vec<u8>, Vec<u8>)> =
                        (base..end).map(|i| (key(i), format!("u{i}").into_bytes())).collect();
                    let tx = db.begin();
                    tx.batch_set(&items).unwrap();
                    tx.commit();
                }
                db.flush().unwrap();
                // lsm 的 seek_prev：阶梯里这一格从 391.5 µs/次（60 万）涨到 11150.2 µs/次（1000 万）= 28.5×，
                // 而行数只涨 16.7× ⇒ 单次调用的钱自己在超线性涨。时间和"退了几把键 / 几次轻访问 / 读几块 /
                // 开几个文件"钉在同一行，判据只看计数（本机跨轮墙钟漂 1.3~2.2×，µs 只用来把数字摆一起）。
                {
                    let mid = key(rows / 2);
                    {
                        let tx = db.begin();
                        assert!(tx.seek_prev(&mid).is_some(), "seek_prev 必须命中中间那把键");
                    }
                    crate::common::disk::file_opens_clear();
                    let st0 = crate::common::store::seek_prev_steps();
                    let lv0 = crate::lsm_tree::sst::light_visits();
                    let br0 = crate::lsm_tree::sst::block_reads();
                    let bm0 = crate::lsm_tree::sst::block_misses();
                    // 篇数要在**做这次 seek 之前**数：后面那次全库 vacuum 会重排文件，
                    // 用它的读数归因这次的块数就是拿两把尺子相减。
                    let files = std::fs::read_dir(dir.join("sst"))
                        .map(|rd| rd.filter_map(|e| e.ok()).count())
                        .unwrap_or(0);
                    let mut best = f64::MAX;
                    let mut last = f64::MAX;
                    for _ in 0..2 {
                        let t = std::time::Instant::now();
                        for _ in 0..CALLS {
                            let tx = db.begin();
                            assert!(tx.seek_prev(&mid).is_some());
                        }
                        last = t.elapsed().as_secs_f64() * 1e6 / CALLS as f64;
                        best = best.min(last);
                    }
                    let n = (CALLS * 2) as f64;
                    let per = |v: u64| v as f64 / n;
                    eprintln!(
                        "[lsm rows={rows}] seek_prev：{best:.1} µs/次（另一轮 {last:.1}）｜ 盘上 {files} 篇 SST ｜ \
                         步/次 = {:.2} ｜ 轻访问/次 = {:.2} ｜ 块/次 = {:.2}（其中未命中 {:.2}）｜ 开文件/次 = {:.2}",
                        per(crate::common::store::seek_prev_steps() - st0),
                        per(crate::lsm_tree::sst::light_visits() - lv0),
                        per(crate::lsm_tree::sst::block_reads() - br0),
                        per(crate::lsm_tree::sst::block_misses() - bm0),
                        per(crate::common::disk::file_opens()),
                    );
                }
                // 照台架的顺序：先做一次全库 vacuum（所以后面那片"裁 0 行"），再切有界的一片
                let st = db.vacuum().unwrap();
                let ssts = std::fs::read_dir(dir.join("sst"))
                    .map(|rd| rd.filter_map(|e| e.ok()).count())
                    .unwrap_or(0);
                let h = db.health();
                eprintln!(
                    "[lsm rows={rows}] 先全库 vacuum：裁 {} 行 ｜ 库 health rows={} live_keys={} ｜ 盘上 {ssts} 篇 SST",
                    st.versions_removed, h.total_rows, h.live_keys
                );
                let mut best = f64::MAX;
                let mut shown = None;
                for n in 1..=SLICES {
                    crate::common::disk::file_opens_clear();
                    let lv0 = crate::lsm_tree::sst::light_visits();
                    let br0 = crate::lsm_tree::sst::block_reads();
                    let t = std::time::Instant::now();
                    let vr = db.vacuum_bounded(None, 2048).unwrap();
                    let us = t.elapsed().as_secs_f64() * 1e6;
                    let visits = crate::lsm_tree::sst::light_visits() - lv0;
                    let blocks = crate::lsm_tree::sst::block_reads() - br0;
                    let opens = crate::common::disk::file_opens();
                    best = best.min(us);
                    eprintln!(
                        "  第{n} 片：{us:.1} µs ｜ 扫 {} 行、裁 {} 行 ｜ 归并吐出 {visits} 把键 \
                         ｜ 读块 {blocks} ｜ 开文件 {opens}",
                        vr.stats.rows_scanned, vr.stats.versions_removed
                    );
                    shown = Some((visits, blocks, opens, vr.stats.rows_scanned));
                }
                if let Some((visits, blocks, opens, scanned)) = shown {
                    eprintln!(
                        "[lsm rows={rows}] 一片的最小 {best:.1} µs ｜ 每行 = {:.2} µs ｜ 键/行 = {:.2} \
                         ｜ 块/行 = {:.3} ｜ 开文件/行 = {:.4}",
                        best / scanned.max(1) as f64,
                        visits as f64 / scanned.max(1) as f64,
                        blocks as f64 / scanned.max(1) as f64,
                        opens as f64 / scanned.max(1) as f64,
                    );
                }
                drop(db);
                let _ = std::fs::remove_dir_all(&dir);
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
