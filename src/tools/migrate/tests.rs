//! 该模块的回归与机制守卫。

use super::*;
use crate::common::kv_snapshot::{b64_encode, read_bin, read_jsonl, write_bin, write_jsonl};
use std::assert;
use std::assert_eq;
use std::vec;
// 仅测试使用 include!，保持历史 --exact 测试名和压测脚本入口。
include!("tests/support.rs");
include!("tests/contracts.rs");
include!("tests/encoding.rs");
include!("tests/search.rs");
include!("tests/vacuum.rs");
include!("tests/ttl.rs");
include!("tests/snapshot.rs");
include!("tests/transaction.rs");
