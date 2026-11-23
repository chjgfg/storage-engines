//! 该模块的回归与机制守卫。

use super::*;
use std::assert;
use std::assert_eq;
use std::panic;
use std::vec;
// 仅测试使用 include!，保持历史 --exact 测试名和压测脚本入口。
include!("tests/search.rs");
include!("tests/encoding.rs");
include!("tests/snapshot.rs");
include!("tests/ttl.rs");
include!("tests/contracts.rs");
