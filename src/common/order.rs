//! 在不改变 raw_key || version_be 磁盘格式的前提下提供逻辑顺序。二进制前缀可能物理交错，只有后续允许版本都不可能属于该键时组才完整；普通文本键只缓存一组，重叠窗口最坏可覆盖全库。

use std::borrow::Borrow;
use std::collections::{BTreeMap, BTreeSet};
use std::{debug_assert_eq, matches, unreachable};

#[cfg(test)]
use super::VersionedKey;
use super::version::decode_key_ref;
use super::{WinCandidate, decode_key, encode_key};

#[cfg(test)]
std::thread_local! {
    static TAIL_BUFFER_PEAK: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

pub(crate) struct KeyWindow<T, K = Vec<u8>> {
    pending: WindowStorage<T, K>,
}

/// 普通组无需树节点；二进制前缀重叠时显式升级有序窗口，最坏窗口仍可覆盖全库。
enum WindowStorage<T, K> {
    Empty,
    Single(K, T),
    Overlap(BTreeMap<K, T>),
}

/// 共享候选折叠只保留每个待定逻辑键的胜者。
pub(crate) struct CandidateFold<'a, R, K = Vec<u8>> {
    window: KeyWindow<(u64, R), K>,
    out: Vec<WinCandidate<R, K>>,
    start: Option<&'a [u8]>,
    end: Option<&'a [u8]>,
    after: Option<&'a [u8]>,
    accept: Option<&'a dyn Fn(&[u8]) -> bool>,
    max_version: u64,
    budget: usize,
    emitted: usize,
    stopped: bool,
}

/// 反向物理扫描超预算时，调用方必须显式处理全库级回退成本；空结果不是回退信号。
pub(crate) enum ReverseOutcome {
    Complete(Vec<super::ExportRecord>),
    BudgetExceeded { physical_steps: usize },
}

pub(crate) struct LogicalHealth {
    groups: KeyWindow<u64>,
    max_version: u64,
    total_rows: u64,
    keys: u64,
    max_chain: u64,
}

#[cfg(test)]
mod tests;

// 按职责组织的内部实现；公开类型与原模块路径保持一致。
mod bounds;
mod candidate_fold;
mod health;
mod key_window;
#[cfg(test)]
mod probes;
mod reverse;
#[cfg(test)]
pub(crate) use bounds::logical_floor;
pub(crate) use bounds::range_end;
pub(crate) use bounds::window_before;
#[cfg(test)]
pub(crate) use probes::note_tail_buffer;
#[cfg(test)]
pub(crate) use probes::take_tail_buffer_peak;
pub(crate) use reverse::reverse_tail_bounded;
pub(crate) use reverse::reverse_window;

#[cfg(test)]
include!("order/fold_tests.rs");
