//! 本文件负责对象安全的引擎公开契约。

use super::*;

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

    /// 轻量回收账与活跃事务状态，O(活跃事务数)，不扫描库。第三方实现未接线时返回 None。
    fn reclamation_status(&self) -> Option<crate::common::ReclamationStatus> {
        None
    }

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
