//! 四引擎共用的 WAL：帧格式带 CRC32，记录类型 `Begin` / `Write` / `Commit` / `Abort` / `Checkpoint`。
//!
//! 保证：**Commit 记录 fsync 成功 = 事务持久**。数据页/日志可以之后慢慢刷；反过来说，
//! WAL 一旦被截断，未落盘的行就再无重放来源 —— 所以 checkpoint 的顺序永远是
//! 「先落数据，最后才收口 WAL」。
//!
//! 原来三个磁盘引擎各有一份 ~335 行的实现，`diff` 实测彼此只差 14–16 行（注释与 import 路径），
//! 连 `WAL_MAGIC` 和帧头都是同一个 —— 也就是说这本来就是同一个格式的三份复制。
//! 崩溃恢复是最不该有三分之一的地方：一处修了截断顺序、另两处继续把刚写的 Checkpoint
//! 自己 truncate 抹掉，这类事故本仓库真的发生过。
//!
//! Checkpoint 记录里的 `root_page_id` / `next_page_id` 只有 B+ 树用得上，
//! Bitcask / LSM 恒写 0 —— 保留同一个布局，不为这个再开一个记录类型。

use std::path::Path;
use std::vec;

use serde::{Deserialize, Serialize};

use crate::common::disk::{DiskFile, SITE_WAL_HEADER, SITE_WAL_SYNC, crc32};

/// WAL 文件头魔数
const WAL_MAGIC: &[u8; 8] = b"WAL00001";
const WAL_HEADER_SIZE: usize = 32;
// header: magic(8) + next_lsn(8) + reserved(16)

/// 压测显式启用的 WAL 提交分段计时；作用域外不计量，非测试构建不编译。
#[cfg(test)]
pub(crate) mod commit_probe;

/// 记录帧：len(u32) + crc(u32) + body
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum WalRecord {
    /// 事务开始
    Begin { xid: u64 },
    /// 写入一个版本键（value=None 表示 tombstone 删除）
    Write {
        xid: u64,
        /// 已编码的存储键 encode_key(raw, version)
        key: Vec<u8>,
        value: Option<Vec<u8>>,
    },
    /// 事务提交
    Commit { xid: u64 },
    /// 事务回滚
    Abort { xid: u64 },
    /// 检查点：此 LSN 之前的数据已全部落盘，WAL 可截断
    Checkpoint {
        next_version: u64,
        /// 页号：只有 B+ 树用得上，Bitcask / LSM 恒为 0（同一个记录布局）
        root_page_id: u64,
        next_page_id: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    pub lsn: u64,
    pub record: WalRecord,
}

pub struct Wal {
    file: DiskFile,
    next_lsn: u64,
    /// 是否在本次打开后写过记录（决定 open 后要不要走恢复）
    dirty: bool,
    poisoned: bool,
}

#[cfg(test)]
std::thread_local! {
    static FAIL_COMMIT: std::cell::Cell<u8> = const { std::cell::Cell::new(0) };
}

/// 1 表示追加 Commit 记录之前；2 表示追加之后、同步之前。
#[cfg(test)]
pub(crate) fn fail_next_commit(stage: u8) {
    FAIL_COMMIT.with(|f| f.set(stage));
}

/// 恢复分析结果
#[derive(Debug, Default)]
pub struct RecoveryPlan {
    /// 已提交事务的全部 Write（按 LSN 序）
    pub committed_writes: Vec<(u64, Vec<u8>, Option<Vec<u8>>)>, // (xid, key, value)
    /// 未提交事务的 Write（需要从数据文件撤销）
    pub uncommitted_writes: Vec<(u64, Vec<u8>)>, // (xid, key)
    /// WAL 中见过的最大 xid
    pub max_xid: u64,
    /// 最后一个 Checkpoint（若有）
    pub last_checkpoint: Option<WalRecord>,
}

#[cfg(test)]
mod tests;

// 按职责组织的内部实现；公开类型与原模块路径保持一致。
mod append;
mod recovery;
pub use recovery::analyze_recovery;
