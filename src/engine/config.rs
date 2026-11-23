//! 本文件负责配置与环境变量。

use super::*;

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
    /// LSM 引擎旋钮：`None` = 用引擎自己的 MVCC 档 [`crate::LSMTreeOptions::for_mvcc`]。
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
    /// 这是按样本每槽价估计容量，不是实时字节记账，更不是进程 RSS 硬上限。
    /// 大键/大值会改变每槽真实大小；后端至少保留一槽时也可能超过极小预算。
    /// 扫描返回值、事务写集、memtable、blob 重写、OS 文件缓存均不在此预算内。
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
        let kind =
            EngineKind::parse(&raw).ok_or_else(|| EngineError::UnknownEngine(raw.clone()))?;
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
