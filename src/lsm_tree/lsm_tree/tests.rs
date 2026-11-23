//! 该模块的回归与机制守卫。

use super::*;
use std::assert_eq;
use std::eprintln;
use std::panic;
use std::println;
// 仅测试使用 include!，保持历史 --exact 测试名和压测脚本入口。
include!("tests/vacuum.rs");
include!("tests/snapshot.rs");
include!("tests/contracts.rs");
include!("tests/manifest.rs");
include!("tests/direct_move.rs");
include!("tests/compaction.rs");
include!("tests/sst_pruning.rs");
include!("tests/support.rs");
include!("tests/search.rs");
include!("tests/encoding.rs");
include!("tests/recovery.rs");
include!("tests/transaction.rs");
