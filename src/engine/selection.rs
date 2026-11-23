//! 本文件负责引擎枚举和别名解析。

use super::*;

/// 可用引擎名（[`open_engine`] 接受的规范名）
pub const ENGINE_NAMES: &[&str] = &["bitcask", "bplus_tree", "lsm_tree", "memory"];

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
    /// 返回 `false` 的只有一个：`memory` —— 它**没有崩溃安全**（见
    /// [`KvEngine::durability_profile`]：无事务 WAL、无校验和），进程被杀数据就没。
    /// ⚠️ M81 第 6 项之后这条判据的措辞要说准：给了目录的 memory 现在**优雅停止不丢**
    /// （`Drop` 写整库快照、open 读回），但快照不是 WAL、也不带校验和 ⇒ 上生产仍然不行，
    /// 判据本身一字未动。它是给教学/测试当对照用的，不是"更快的选项"。
    pub fn is_production_ok(self) -> bool {
        self != EngineKind::Memory
    }
}

impl std::fmt::Display for EngineKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
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

/// 把别名归一到规范名
pub(super) fn canonical_name(name: &str) -> Option<&'static str> {
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
