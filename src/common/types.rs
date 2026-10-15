//! 公共数据类型（唯一定义，四个引擎共享）。
//!
//! derive 与字段语义严格沿用原 bitcask 版本，保证现有 `assert_eq!` 调用点不受影响。

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
    /// 页码，**从 0 开始**
    pub page: usize,
    /// 每页条数，至少为 1（若传入 0 会按 1 处理）
    pub page_size: usize,
    pub mode: KeyMatchMode,
    /// 是否包含 tombstone（`value=None`）
    pub include_deleted: bool,
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
        }
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
    /// 匹配总数（未分页前）
    pub total: usize,
    /// 请求的页码（从 0）
    pub page: usize,
    /// 每页大小
    pub page_size: usize,
    /// 总页数（total=0 时为 0）
    pub total_pages: usize,
}

impl SearchPage {
    pub fn has_next(&self) -> bool {
        self.total_pages > 0 && self.page + 1 < self.total_pages
    }

    pub fn has_prev(&self) -> bool {
        self.page > 0 && self.total > 0
    }
}
