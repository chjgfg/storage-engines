//! 回归测试。

use super::*;
use std::{assert, assert_eq, matches, vec};
// 仅测试使用 include!，保持历史 --exact 测试名和压测脚本入口。
include!("checked_tests/transaction.rs");
