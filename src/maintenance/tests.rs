//! 该模块的回归与机制守卫。

use super::*;
use crate::{EngineConfig, open_engine, open_engine_with};
use std::{assert, assert_eq, format, println, vec};
// 仅测试使用 include!，保持历史 --exact 测试名和压测脚本入口。
include!("tests/support.rs");
include!("tests/vacuum.rs");
include!("tests/transaction.rs");
include!("tests/contracts.rs");
