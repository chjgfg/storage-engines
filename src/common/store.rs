//! 版本行存储的统一接口（M18 第一刀：把四份折叠收成一份）。
//!
//! 判据沿用 M5 那条：**与存储布局无关的合，有关的各留一份**。
//! "按 enc 升序折出每把逻辑 key 的胜出行、界内过滤、窗口化"这套账四家算的是同一件事
//! （版本号大端接在 key 后面 ⇒ 组内升序 ⇒ 组边界才收口），所以它该只有一份实现。
//! 而"这一行的字节到底在哪儿"是各家的布局差异（memory 在同一个 map 里、bitcask 在日志另一个
//! 位置、bplus 在叶子条目里、lsm 要跨源点查），所以那一半留在各自引擎里，做成
//! [`VersionedStore::scan_candidates`] 与 [`VersionedStore::materialize`] 两个方法。
//!
//! 搬完之后新增一个引擎只要写这两个方法，折叠/窗口/分页/界下推不用重写 —— 那正是
//! "基座"与"四份复制"的分界。

use crate::common::WinCandidate;
use crate::common::codec::{
    VisibleOwnedValue, key_matches, logical_to_user, now_unix_secs, prefix_upper_bound,
    visible_owned_value,
};
use crate::common::types::{ExportRecord, KeyMatchMode, SearchPage, SearchQuery};
use crate::common::{fold_window, fold_window_emit};
#[cfg(test)]
use std::thread_local;
use std::{matches, unreachable};

/// 一组候选取值的结果，三态（见 [`VersionedStore::materialize`]）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowValue {
    /// 读到了存储的逻辑字节（是否过期由上层剥壳时判）
    Value(Vec<u8>),
    /// 这一行是墓碑（`delete` 留下的软删标记）
    Tombstone,
    /// 这一行根本读不到：与墓碑不同，它**不该出现在结果里**
    Missing,
}

/// 搜索保留被拒绝候选以推进游标，并统计页面以外的行。
pub enum SearchRow<R = ()> {
    /// 折叠真正处理到此行时才解析拥有型凭据。
    Deferred(R),
    Excluded,
    Counted,
    Retained(Option<Vec<u8>>),
}

pub(crate) enum SearchSelection {
    Excluded,
    Counted,
    Value,
    Deleted,
}

pub struct SearchRetention {
    include_deleted: bool,
    skip: usize,
    take: usize,
}

/// 后端可以在持有读取边界时直接消费逻辑组；不支持该路径时必须显式回退，不能伪装成空结果。
pub enum SearchWalkOutcome {
    UseDefault,
    Completed {
        items: Vec<ExportRecord>,
        has_more: bool,
        total: usize,
    },
}

/// 一把逻辑 key 的**版本行**怎么读。
///
/// 只有两个必需方法，且都不含事务语义（可见性判据留在调用方，见
/// [`crate::common::visible_at`]）：
/// - [`Self::scan_candidates`]：按 enc 升序折出候选组（**不取值**）
/// - [`Self::materialize`]：把一组的凭据换成实际字节
pub trait VersionedStore {
    /// 这一家的"再拿到这一行的凭据"：memory/bplus 是顺手带出的存储字节，
    /// bitcask 是编码键；LSM 普通值保留共享块，blob/同 seq 歧义保留显式点查凭据。
    type Row;

    /// 可选的有界持锁扫描。可见性、分组和保留判断共用原判据；不支持的引擎显式返回 UseDefault。
    fn search_walk(
        &self,
        _include_deleted: bool,
        _pattern: Option<&[u8]>,
        _mode: KeyMatchMode,
        _skip: usize,
        _page_size: usize,
        _want_total: bool,
    ) -> SearchWalkOutcome {
        SearchWalkOutcome::UseDefault
    }

    /// 折出候选组：`start`/`end` 是逻辑键界（含下、不含上），`accept` 是**键级**过滤
    /// （不命中的键一个值都不取），`after` 是续跑游标（**不含**该键），`budget` 是组数上限。
    /// 返回 `(候选, 已走到界尾)`。
    ///
    /// ⚠️ 各家实现都走的是版本键 `raw ‖ version_be`，所以**界的两种判法必须分开**：
    /// - **可以 `break`（提前停）的条件是"迭代键本身"越界**：`enc ≥ encode(end, 0)`。
    ///   键流按 enc 升序，越过了就没有回来的一天。
    /// - **按 `raw_key ≥ end` 只能 `continue`，绝不能 `break`**：一把键是另一把键的
    ///   前缀时（`"ab"` 与 `"abc"`），排在前面的短键其版本组可能整体小于 `end`，
    ///   而后面的更长键仍然在界内 —— 按 raw 提前停会**静默少行**。
    /// - 起点同理：`iter_from(start)` 只保证 `enc ≥ start`，`raw < start` 的那几条要 `continue`。
    /// （`after` 为什么可以当迭代下界、又不能拼成 `after ‖ 0xFF…`，见 [`fold_lower_bound`]。）
    fn scan_candidates(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        accept: Option<&dyn Fn(&[u8]) -> bool>,
        after: Option<&[u8]>,
        budget: usize,
    ) -> (Vec<WinCandidate<Self::Row>>, bool);

    /// 取一组的值。**三态**而不是 `Option`：各家今天都把"取不到这一行"与"这行是墓碑"分开处理
    /// （lsm 的轻量视图与值存储不一致时会跳过整行，而墓碑在 `include_deleted` 下要出现在结果里），
    /// 合成一个 `Option` 就会把那两种情况混成一种 —— 折叠统一的前提是不改语义。
    fn materialize(&self, row: Self::Row) -> RowValue;

    /// 支持借用的引擎在读取边界内分类；默认路径延迟物化拥有型凭据，使免 total 页面可提前停止读值。排除行仍留在批次中以推进游标。
    fn scan_search_candidates(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        accept: Option<&dyn Fn(&[u8]) -> bool>,
        after: Option<&[u8]>,
        budget: usize,
        _retention: SearchRetention,
    ) -> (Vec<WinCandidate<SearchRow<Self::Row>>>, bool) {
        #[cfg(test)]
        if search_materialize_reference::enabled() {
            return owned_search_candidates(self, start, end, accept, after, budget, _retention);
        }
        let (rows, complete) = self.scan_candidates(start, end, accept, after, budget);
        (
            rows.into_iter()
                .map(|c| WinCandidate {
                    key: c.key,
                    version: c.version,
                    row: SearchRow::Deferred(c.row),
                })
                .collect(),
            complete,
        )
    }
}

#[cfg(test)]
pub(crate) mod search_materialize_reference;

#[cfg(test)]
mod deferred_search_tests;

#[cfg(test)]
pub(crate) mod scan_cost_reference;

// 测试专用：`seek_prev_with` 为了返回一行，往回退过几把键。
//
// M20 的复杂度尺。旧实现把 `[库头, key]` 整段折成 `Vec` 再 `pop()` ⇒ 一次调用 O(库大小) 的
// 时间和内存（200k 实测单次 140~610 ms，同库一次点读 2.9~57 µs）。改成反向逐步之后，代价只由
// "要跳过几把死键"决定，与库大小无关 ⇒ 守卫断言的是**步数**，不是墙钟。
#[cfg(test)]
thread_local! {
    static SEEK_PREV_STEPS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

// 测试专用：`back_n_with` 走过的候选键数 / "退太多、让调用方回退"的次数。
//
// 为什么钉这两个计数而不是墙钟：`search_last_page` 的代价必须**不随库涨**，而"跟着库涨"这件事
// 只有两种落地形状 —— 候选键退了一大堆（墓碑尾巴），或整条路径退化回全库物化再截尾（回退那一支）。
// 这两个计数正好一人盯一种，且与本机跨轮 1.3~2.6× 的墙钟噪声无关。
#[cfg(test)]
thread_local! {
    static BACK_N_CANDIDATES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static BACK_N_FALLBACKS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

// 按职责组织的内部实现；公开类型与原模块路径保持一致。
mod export;
mod reverse;
mod scan;
mod search;
pub use export::collect_rows;
pub use export::collect_rows_as;
pub use export::export_rows_streaming;
pub use export::fold_lower_bound;
pub use export::fold_rows_streaming;
pub use export::live_record_of_logical;
pub use export::scan_live_records;
#[cfg(test)]
pub use reverse::back_n_candidates;
#[cfg(test)]
pub use reverse::back_n_fallbacks;
pub use reverse::back_n_with;
#[cfg(test)]
pub(crate) use reverse::note_reverse_candidate;
#[cfg(test)]
pub(crate) use reverse::note_reverse_fallback;
#[cfg(test)]
pub use reverse::seek_prev_steps;
pub use reverse::seek_prev_with;
pub use scan::scan_page;
pub(crate) use search::classify_search_value;
pub use search::count_window;
#[cfg(test)]
pub use search::owned_search_candidates;
pub(crate) use search::retain_search_row;
pub use search::search_bounds;
pub use search::search_page;
pub use search::search_page_rows;
pub use search::search_page_walk;
pub use search::search_window;

#[cfg(test)]
include!("store/visibility_tests.rs");
