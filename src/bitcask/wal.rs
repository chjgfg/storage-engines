//! Bitcask 的 WAL —— 实现已收进 [`crate::common::wal`]，这里只保留既有路径。
//!
//! 三个磁盘引擎原先各有一份 ~335 行的逐字复制（`diff` 实测只差 14–16 行注释与 import，
//! 连 `WAL_MAGIC`、帧头大小都一样），所以合并成一份：崩溃恢复最不该有三个副本 ——
//! M3b 那次就是"checkpoint 把刚写的 Checkpoint 记录自己 truncate 抹掉了"，
//! 修一处剩两处有 bug。
//!
//! 本引擎仍然自己决定的是：WAL 文件放在哪（[`crate::bitcask::storage::wal_path`]）以及 Checkpoint 里
//! `root_page_id` / `next_page_id` 恒写 0（Bitcask 没有页）

pub use crate::common::wal::*;
