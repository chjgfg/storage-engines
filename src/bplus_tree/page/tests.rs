//! 该模块的回归与机制守卫。

use super::*;

use std::assert;
use std::assert_eq;
// 仅测试使用 include!，保持历史 --exact 测试名和压测脚本入口。
include!("tests/contracts.rs");
include!("tests/transaction.rs");
include!("tests/cache.rs");
include!("tests/support.rs");
include!("tests/encoding.rs");
include!("tests/recovery.rs");
