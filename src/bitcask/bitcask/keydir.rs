//! 本文件负责键目录构造。

use super::*;

impl KeyLoc {
    /// 是不是软墓碑（版本行还在，逻辑 value 为 None）。
    ///
    /// 给只需要「这一行存不存在 value」的调用方用 —— 比如 vacuum 判「要不要连这行一起删」，
    /// 它不需要 value 内容，不该为此读盘。
    pub fn is_soft_tombstone(&self) -> bool {
        matches!(self, KeyLoc::SoftTombstone { .. })
    }
}

impl KeyDirBulk {
    /// 攒法一：**不知道会有多少条**（merge 重扫那侧）⇒ 分步 `reserve_exact`，最坏过冲夹在一个步长里。
    pub(super) fn new() -> Self {
        KeyDirBulk(Vec::new())
    }

    /// 攒法二（M77，hint 门专用）：**有一条被文件字节数验过的界** ⇒ 一次预留到位，不分步。
    ///
    /// ⚠️ 探针就记在这里（不是记在调用方）：要钉的是"**门拿到界之后真的预留了这么多**"，
    /// 记在调用方就变成"有人把 `with_capacity(界)` 换回 `new()` 也照样绿"（M77 的 μA 变异试的就是这个）。
    pub(super) fn with_capacity(bounded: usize) -> Self {
        #[cfg(test)]
        HINT_RESERVE_NOTED.with(|c| c.set(bounded));
        let mut v = Vec::new();
        v.reserve_exact(bounded);
        KeyDirBulk(v)
    }

    pub(super) fn push(&mut self, entry: (Vec<u8>, KeyLoc)) {
        if self.0.len() == self.0.capacity() {
            self.0.reserve_exact(KEYDIR_BULK_STEP);
        }
        self.0.push(entry);
    }

    /// 交出树。⚠️ `into_iter().collect()` 会**复用**原来那块缓冲（M59 实测建树净增只有 1.3 MB）
    /// ⇒ 全程只有一份清单；中间插一个 `.map(..)` 就会多要一份（`spec_from_iter` 那条捷径失效）。
    pub(super) fn into_keydir(self) -> KeyDir {
        self.0.into_iter().collect()
    }
}

/// 把一条**升序**流攒成清单再一次成树；`Err` 原样交出（hint 解码失败要回退全量扫，不许在这里吞）。
///
/// 与 [`KeyDirBulk`] 分开写，是因为 hint 那侧的流吐的是 `Result<Option<..>>`（坏一条 ≠ 读不出来，
/// M25 那条纪律），而 merge 那侧是命令式的扫描回调 —— 两边共用的是**攒法与成树**，不是迭代器形状。
pub(super) fn keydir_from_ordered_stream<E>(
    mut bulk: KeyDirBulk,
    iter: impl Iterator<Item = std::result::Result<Option<(Vec<u8>, KeyLoc)>, E>>,
) -> std::result::Result<KeyDir, E> {
    for item in iter {
        if let Some(kv) = item? {
            bulk.push(kv);
        }
    }
    Ok(bulk.into_keydir())
}

/// 把一条日志记录应用到 KeyDir
pub(super) fn apply_log_record(
    key_dir: &mut KeyDir,
    file_id: u32,
    key: Vec<u8>,
    value_pos: u64,
    flag: i32,
    value_len: u32,
) {
    if flag >= 0 {
        key_dir.insert(
            key,
            KeyLoc::Live {
                file_id,
                pos: value_pos,
                len: value_len,
            },
        );
    } else if flag == FLAG_SOFT_TOMBSTONE {
        key_dir.insert(key, KeyLoc::SoftTombstone { file_id });
    } else {
        key_dir.remove(&key);
    }
}
