//! 该模块的回归与机制守卫。

use super::*;
use crate::engine::open_engine;
use std::assert_eq;
use std::option::Option;
use std::string::String;
// 仅测试使用 include!，保持历史 --exact 测试名和压测脚本入口。
include!("tests/support.rs");
include!("tests/contracts.rs");
include!("tests/search.rs");
include!("tests/transaction.rs");
include!("tests/vacuum.rs");
include!("tests/ttl.rs");
