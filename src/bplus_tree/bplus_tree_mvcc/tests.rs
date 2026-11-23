//! 该模块的回归与机制守卫。

use super::*;

use crate::bplus_tree::storage::{dblwr_path, wal_path};
use std::assert;
use std::assert_eq;
use std::assert_ne;
use std::matches;
use std::panic;
use std::println;
use std::vec;
// 仅测试使用 include!，保持历史 --exact 测试名和压测脚本入口。
include!("tests/contracts_bench.rs");
include!("tests/point_read.rs");
include!("tests/contracts.rs");
include!("tests/export.rs");
include!("tests/health.rs");
include!("tests/write_failures.rs");
include!("tests/cache.rs");
include!("tests/cache_budget.rs");
include!("tests/transaction.rs");
include!("tests/support.rs");
include!("tests/auto_vacuum.rs");
include!("tests/vacuum.rs");
include!("tests/vacuum_faults.rs");
include!("tests/bounded_vacuum.rs");
include!("tests/vacuum_bench.rs");
include!("tests/recovery.rs");
include!("tests/search.rs");
include!("tests/encoding.rs");
include!("tests/snapshot.rs");
