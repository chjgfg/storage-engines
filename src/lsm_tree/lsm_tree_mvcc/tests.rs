//! 该模块的回归与机制守卫。

use super::*;
use crate::lsm_tree::storage::lock_path;
use std::assert;
use std::assert_eq;
use std::assert_ne;
use std::matches;
use std::panic;
use std::println;
use std::vec;
// 仅测试使用 include!，保持历史 --exact 测试名和压测脚本入口。
include!("tests/support.rs");
include!("tests/vacuum.rs");
include!("tests/vacuum_bench.rs");
include!("tests/vacuum_faults.rs");
include!("tests/bounded_vacuum.rs");
include!("tests/auto_vacuum.rs");
include!("tests/search.rs");
include!("tests/compaction.rs");
include!("tests/snapshot.rs");
include!("tests/transaction.rs");
include!("tests/point_read.rs");
include!("tests/contracts.rs");
include!("tests/export.rs");
include!("tests/health.rs");
include!("tests/contracts_bench.rs");
include!("tests/cache.rs");
include!("tests/recovery.rs");
include!("tests/encoding.rs");
