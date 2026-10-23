//! 公共数据类型（唯一定义，四个引擎共享）。
//!
//! derive 与字段语义严格沿用原 bitcask 版本，保证现有 `assert_eq!` 调用点不受影响。

use std::write;
use std::writeln;

/// 导出用的一条逻辑记录（已解码 blob，无 version 后缀）
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportRecord {
    pub key: Vec<u8>,
    /// `None` 表示 tombstone（仅当 include_deleted 时出现）
    pub value: Option<Vec<u8>>,
}

/// vacuum 结果统计
#[derive(Debug, Clone)]
pub struct VacuumStats {
    /// 本次使用的回收水位（因存在活跃事务而整轮跳过时为 0）
    pub xmin: u64,
    /// 删除的旧版本行数
    pub versions_removed: usize,
    /// 是否重写/清空了 blob 文件。
    ///
    /// `None` 表示该引擎没有外置 blob 存储（lsm_tree / memory）。
    pub blob_rewritten: Option<bool>,
    /// 因存在活跃事务而**整轮跳过**，什么都没扫。
    ///
    /// 必须能和「扫了但没东西可裁」区分开：两者 `versions_removed` 都是 0，
    /// 但前者说明回收**根本没工作**（时机不对，或有事务忘了 commit），
    /// 后者说明版本链已经压到 1 —— 结论完全相反。
    pub skipped_active_txn: bool,
    /// 本轮读了多少物理行（元数据/键，不含 value）。
    ///
    /// 自动回收要拿它当「这一小轮有多贵」的读数；手动 `vacuum()` 就是全库一轮，
    /// 这个数等于 [`MvccHealth::total_rows`]。
    pub rows_scanned: u64,
}

/// 全库健康度快照 —— 让库自己说得出「版本链到底有多长」。
///
/// 这次事故的量化核心（某个热点 key 堆了 23.8 万个版本）从头到尾是靠
/// `grep data.log | uniq -c` 数出来的；一个当基座用的库不该靠这个诊断自己。
///
/// 四个引擎共用同一份定义：`health()` 的口径必须跨引擎可比 —— 换引擎只是换了
/// 涨速，盯的指标是同一个。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MvccHealth {
    /// 物理行数（含历史版本与墓碑）
    pub total_rows: u64,
    /// 不同逻辑 key 的个数
    pub live_keys: u64,
    /// 单个逻辑 key 的最长版本链
    pub max_chain: u64,
    /// 超出「每 key 一行」的多余行数 = total_rows - live_keys。
    ///
    /// 这是「链没被裁过」的量级指标，**不等于** vacuum 能裁掉多少
    /// （vacuum 还要留「最后一个 < xmin」那一版，墓碑另有规则）。
    pub rows_above_one: u64,
    /// 当前活跃事务数：> 0 时 vacuum 会整轮跳过
    pub active_txns: u64,
}

/// 引擎的崩溃安全画像。
///
/// 四个引擎**不等价**，所以这里给的是"哪些构件在/不在"，而不是一个 `Level(1..=5)`：
/// 一个整数会把"数据记录没校验和"与"根本没有 WAL"糊成同一个数字，而这两件事对下游
/// 的含义完全不同 —— 前者是**静默损坏无法发现**，后者是**提交点不存在**。
///
/// ⚠️ `false` 不一定是缺陷，要看那条能力对这个布局有没有意义：比如 `torn_page_guard`
/// 只有做**原地页写**的引擎才需要（B+ 树），Bitcask / LSM 是追加式布局，天生没有
/// torn page 这个问题 —— 所以另外还给了 `in_place_page_writes`，防止把 `false`
/// 误读成"少了个防护"。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurabilityProfile {
    pub engine: &'static str,
    /// 有没有事务 WAL（Begin / Write / Commit / Abort + 帧 CRC32 + 可截断）
    pub transaction_wal: bool,
    /// commit 是否 fsync WAL —— 「Commit 记录落盘 = 事务持久」那个持久化点
    pub fsync_on_commit: bool,
    /// 数据侧记录/块是否自带校验和。
    /// **只有 Bitcask 的主日志没有**：`data.log` 的裸记录是 `key_len | flag | key | value`，
    /// 坏一个字节不会报错，只会读出一个错值。补 CRC 要改磁盘格式，本仓库明确不采纳。
    pub data_checksum: bool,
    /// 外置大 value 是否带校验和（Bitcask / B+ 树的 blob 记录有；LSM 的 `blobs/*.blob`
    /// 是 `fs::write` 的裸文件 ⇒ 损坏同样是静默的）
    pub blob_checksum: bool,
    /// 是否存在原地页写（决定 `torn_page_guard` 这条能力有没有意义）
    pub in_place_page_writes: bool,
    /// 原地页写是否有双写缓冲兜底（半页撕裂能被 dblwr 复原）
    pub torn_page_guard: bool,
    /// 全库重写（merge / compact）中途崩掉，下次 open 能不能认回产物而不是当成空库
    pub merge_artifact_recovery: bool,
}

impl std::fmt::Display for DurabilityProfile {
    /// 只把**缺口**说成人话：全绿的部分不必占用读者注意力。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut gaps: Vec<String> = Vec::new();
        if !self.transaction_wal {
            gaps.push("无事务 WAL ⇒ 没有持久化点".into());
        }
        if !self.fsync_on_commit && self.transaction_wal {
            gaps.push("commit 不 fsync ⇒ 断电可丢最近提交".into());
        }
        if !self.data_checksum {
            gaps.push("数据记录无校验和 ⇒ 静默损坏无法发现".into());
        }
        if !self.blob_checksum {
            gaps.push("大 value 旁路无校验和".into());
        }
        if self.in_place_page_writes && !self.torn_page_guard {
            gaps.push("有原地页写但无双写缓冲 ⇒ 半页撕裂不可复原".into());
        }
        if !self.merge_artifact_recovery {
            gaps.push("全库重写中途崩掉无人认回产物".into());
        }
        if gaps.is_empty() {
            return write!(f, "{}: 崩溃安全构件齐全", self.engine);
        }
        write!(f, "{}: {}", self.engine, gaps.join("；"))
    }
}

/// [`vacuum_bounded`](crate::bitcask::MVCC::vacuum_bounded) 的一轮结果。
///
/// 手动 `vacuum()` = 一次 `budget_rows = u64::MAX` 的轮次，两者共用同一份保留规则。
#[derive(Debug, Clone)]
pub struct VacuumRound {
    pub stats: VacuumStats,
    /// 本轮停在哪个逻辑 key 上：原样作为下一轮的 `after` 传回即可接着扫。
    ///
    /// `None` ⇒ **已经扫完全库**，这一轮才是最后一轮 —— blob 回收与日志压缩只在这里做
    /// （两者都是全库代价，放进中间轮等于每轮重写一遍库）。
    pub next_cursor: Option<Vec<u8>>,
}

/// 解析逻辑 value 信封
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueMeta {
    /// 永久（或旧格式裸 value）
    Plain { user: Vec<u8> },
    /// 带绝对过期时间
    Ttl { expire_unix_secs: u64, user: Vec<u8> },
}

impl ValueMeta {
    pub fn user(&self) -> &[u8] {
        match self {
            ValueMeta::Plain { user } => user,
            ValueMeta::Ttl { user, .. } => user,
        }
    }

    pub fn expire_unix_secs(&self) -> Option<u64> {
        match self {
            ValueMeta::Plain { .. } => None,
            ValueMeta::Ttl {
                expire_unix_secs, ..
            } => Some(*expire_unix_secs),
        }
    }

    pub fn is_expired_at(&self, now: u64) -> bool {
        match self.expire_unix_secs() {
            Some(e) => now >= e,
            None => false,
        }
    }
}

/// `get_meta` 返回结构
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyMeta {
    pub key: Vec<u8>,
    /// 用户 value 字节长度（过期或不存在则无）
    pub value_len: Option<usize>,
    /// 写入该版本的事务 id / 版本号
    pub version: u64,
    /// 绝对过期 Unix 秒；None = 永久
    pub expire_unix_secs: Option<u64>,
    /// 是否已过期（存在但 TTL 到期）
    pub expired: bool,
    /// 是否逻辑删除（tombstone）
    pub deleted: bool,
}

/// 计数器自增错误
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IncrError {
    /// 当前 value 不是合法十进制整数
    NotInteger,
    /// `i64` 加减溢出
    Overflow,
    /// MVCC 写冲突或 set 失败
    WriteConflict,
}

impl std::fmt::Display for IncrError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IncrError::NotInteger => write!(f, "value is not a decimal integer"),
            IncrError::Overflow => write!(f, "i64 overflow"),
            IncrError::WriteConflict => write!(f, "write conflict"),
        }
    }
}

impl std::error::Error for IncrError {}

/// key 匹配模式
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum KeyMatchMode {
    /// 子串匹配：`pattern` 出现在 key 任意位置（默认「模糊」）
    #[default]
    Contains,
    /// 前缀匹配：`key.starts_with(pattern)`
    Prefix,
}

/// 模糊搜索 + 分页查询参数
#[derive(Debug, Clone)]
pub struct SearchQuery {
    /// 匹配模式用的字节串；**空切片表示不过滤（列出全部可见 key）**
    pub pattern: Vec<u8>,
    /// 页码，**从 0 开始**。⚠️ 这是 **offset 式**页码：第 N 页要跳过 `N × page_size` 行
    /// ⇒ 单价 ∝ N，连翻 K 页是 O(K²)。连续翻页请走 `KvTxn::scan_page`（游标），实测连翻 50 页
    /// 贵 29~43 倍、而跳到同一页两条门同价 —— 见 [`crate::engine::KvTxn::search_keys`] 那段。
    pub page: usize,
    /// 每页条数，至少为 1（若传入 0 会按 1 处理）
    pub page_size: usize,
    pub mode: KeyMatchMode,
    /// 是否包含 tombstone（`value=None`）
    pub include_deleted: bool,
    /// 要不要**精确的 total**。默认 `true`（行为与旧版一致）。
    ///
    /// 为什么这是一个开关而不是"总是数一遍"：`Contains` 要逐键比子串，数清匹配总数意味着
    /// **走完整库**，代价与库总量同阶、与页宽无关（实测 20000 键时首页也要 104 ms）。
    /// 翻页的 UI 其实只需要"还有没有下一页"，那条可以用"多取一条"探出来，代价 = 一条记录。
    /// 设 `false` 后 [`SearchPage::total`] 是 `None`（没数），`has_next` 仍然如实。
    pub need_total: bool,
}

impl SearchQuery {
    /// 子串模糊 + 分页
    pub fn contains(pattern: impl Into<Vec<u8>>, page: usize, page_size: usize) -> Self {
        Self {
            pattern: pattern.into(),
            page,
            page_size,
            mode: KeyMatchMode::Contains,
            include_deleted: false,
            need_total: true,
        }
    }

    /// 前缀匹配 + 分页
    pub fn prefix(pattern: impl Into<Vec<u8>>, page: usize, page_size: usize) -> Self {
        Self {
            pattern: pattern.into(),
            page,
            page_size,
            mode: KeyMatchMode::Prefix,
            include_deleted: false,
            need_total: true,
        }
    }

    /// 不要精确总数：只取"窗口 + 一条"就收手（翻页场景的默认姿势）
    pub fn without_total(mut self) -> Self {
        self.need_total = false;
        self
    }

    pub fn with_deleted(mut self, include: bool) -> Self {
        self.include_deleted = include;
        self
    }
}

/// 一页搜索结果
#[derive(Debug, Clone)]
pub struct SearchPage {
    /// 本页记录（按 key 字节序升序）
    pub items: Vec<ExportRecord>,
    /// 匹配总数（未分页前）。`None` = **没数**（[`SearchQuery::without_total`]），
    /// 因为数清它的代价是走完整库，而不是这一页。
    pub total: Option<usize>,
    /// 请求的页码（从 0）
    pub page: usize,
    /// 每页大小
    pub page_size: usize,
    /// 总页数；`None` 同上（没数总数就不说有"多少页"）
    pub total_pages: Option<usize>,
    /// 后面还有没有：靠"多取一条"探出来，代价 = 一条记录 ⇒ 不要 total 也答得出
    pub has_next: bool,
}

impl SearchPage {
    pub fn has_next(&self) -> bool {
        self.has_next
    }

    pub fn has_prev(&self) -> bool {
        self.page > 0 && !self.items.is_empty()
    }
}

/// 引擎当前**生效**的一个可调参数（名字 → 值）。
///
/// 值统一用字符串：四家的旋钮类型本来就不同（`usize` / `bool` / `Option<u64>`），
/// 门面要在 `dyn KvEngine` 上返回同一类型，类型化的读回只能变成"每引擎一个 enum"，
/// 那等于把引擎差异又灌回门面 —— 正是适配层要消除的东西。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TuningEntry {
    pub key: &'static str,
    pub value: String,
}

/// 引擎的可调参数快照 —— 写进去的旋钮要能读回来核对。
///
/// 存在的理由：`EngineConfig` 只能"给"，给错了（字段名拼错、类型对不上、引擎根本不吃
/// 这个旋钮）以前是**静默**的。`tuning()` 让下游和守卫测试都能问一句"现在到底生效的是
/// 什么"。各引擎只列自己有的键，所以它同时是一张"这个引擎能调什么"的清单。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EngineTuning {
    pub entries: Vec<TuningEntry>,
}

impl EngineTuning {
    /// 追加一条（由各引擎的 `tuning()` 逐条 push）
    pub fn push(&mut self, key: &'static str, value: impl std::fmt::Display) -> &mut Self {
        self.entries.push(TuningEntry {
            key,
            value: value.to_string(),
        });
        self
    }

    /// 读回某个旋钮的当前值；`None` = **这个引擎没有这个旋钮**（不是"值为空"）。
    pub fn get(&self, key: &str) -> Option<&str> {
        self.entries
            .iter()
            .find(|e| e.key == key)
            .map(|e| e.value.as_str())
    }

    pub fn keys(&self) -> impl Iterator<Item = &'static str> {
        self.entries.iter().map(|e| e.key)
    }

    /// 各引擎 `tuning()` 共用的那两条账。
    ///
    /// 四家都要列它们，所以列法只写一遍：键名一旦在四处各拼一次字符串，门面和下游核对
    /// 时就会对不上（`wal_limit` / `wal_checkpoint_bytes` 各写一半这种事）。
    /// `wal_checkpoint_bytes` 传 `None` = 该引擎没有 WAL（memory）。
    pub fn push_shared(&mut self, vacuum_auto_slice: u64, wal_checkpoint_bytes: Option<u64>) {
        self.push("vacuum_auto_slice", vacuum_auto_slice);
        if let Some(bytes) = wal_checkpoint_bytes {
            self.push("wal_checkpoint_bytes", bytes);
        }
    }
}

impl std::fmt::Display for EngineTuning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.entries.is_empty() {
            return f.write_str("(无可调参数)");
        }
        for e in &self.entries {
            writeln!(f, "{key}={value}", key = e.key, value = e.value)?;
        }
        Ok(())
    }
}
