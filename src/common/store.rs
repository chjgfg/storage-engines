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

use crate::common::codec::{
    decode_value_meta, key_matches, logical_to_user, now_unix_secs, prefix_upper_bound,
};
use crate::common::fold_window;
use crate::common::types::{ExportRecord, KeyMatchMode};
use crate::common::WinCandidate;

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

/// 一把逻辑 key 的**版本行**怎么读。
///
/// 只有两个必需方法，且都不含事务语义（可见性判据留在调用方，见
/// [`crate::common::visible_at`]）：
/// - [`Self::scan_candidates`]：按 enc 升序折出候选组（**不取值**）
/// - [`Self::materialize`]：把一组的凭据换成实际字节
pub trait VersionedStore {
    /// 这一家的"再拿到这一行的凭据"：memory/bplus 是顺手带出的存储字节，
    /// bitcask/lsm 是编码键（值在另一处，要再读一次）。
    type Row;

    /// 折出候选组：`start`/`end` 是逻辑键界（含下、不含上），`accept` 是**键级**过滤
    /// （不命中的键一个值都不取），`after` 是续跑游标（**不含**该键），`budget` 是组数上限。
    /// 返回 `(候选, 已走到界尾)`。
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
}

/// 界内折叠 + 窗口化：`after` 之后最多 `limit` 组，返回 `(行, 是否还有更多)`。
///
/// `include_deleted = false` 时墓碑整行不参与；`limit = None` = 走到底。
/// 这一份就是原来在四个引擎里各抄一遍的那段折叠。
pub fn collect_rows<S: VersionedStore>(
    store: &S,
    include_deleted: bool,
    start: Option<&[u8]>,
    end: Option<&[u8]>,
    after: Option<&[u8]>,
    limit: Option<usize>,
) -> (Vec<(Vec<u8>, u64, Option<Vec<u8>>)>, bool) {
    // 多要一条只为了知道"后面还有没有"
    let wanted = limit.map(|n| n.saturating_add(1));
    let rows = fold_window(
        wanted,
        |round_after, budget| {
            // 首轮从调用方给的 `after` 起；驱动续跑时用它自己推进的游标（更靠后）
            let cursor = round_after.as_deref().or(after);
            store.scan_candidates(start, end, None, cursor, budget)
        },
        |c| match store.materialize(c.row) {
            RowValue::Missing => None,
            RowValue::Tombstone => {
                include_deleted.then(|| (c.key, c.version, None))
            }
            RowValue::Value(bytes) => Some((c.key, c.version, Some(bytes))),
        },
    );
    let more = wanted.is_some_and(|w| rows.len() >= w);
    let rows = match limit {
        Some(n) => rows.into_iter().take(n).collect(),
        None => rows,
    };
    (rows, more)
}

/// 模式 ⇒ 折叠界。前缀是有界的（可以把界下推给折叠），Contains 只能全库走。
pub fn search_bounds<'a>(
    pattern: Option<&'a [u8]>,
    mode: KeyMatchMode,
) -> (Option<&'a [u8]>, Option<Vec<u8>>) {
    let pat = pattern.unwrap_or_default();
    match mode {
        KeyMatchMode::Prefix => {
            let lo = if pat.is_empty() { None } else { Some(pat) };
            (lo, prefix_upper_bound(pat))
        }
        KeyMatchMode::Contains => (None, None),
    }
}

/// 窗口化的模式搜索：返回**按 key 升序**的命中行，最多 `limit` 条（多要一条给 `has_next` 用）。
///
/// 与 [`collect_rows`] 的分工：这里额外做两件事 —— 把 `pattern`/`mode` 变成折叠侧的
/// `accept`（键级，先判它才能少取值）与界，再把存储字节剥成用户 value（`logical_to_user`）。
/// 剥壳后为 `None`（过期）的行与墓碑同待：只在 `include_deleted` 时出现。
pub fn search_window<S: VersionedStore>(
    store: &S,
    include_deleted: bool,
    pattern: Option<&[u8]>,
    mode: KeyMatchMode,
    limit: Option<usize>,
) -> Vec<ExportRecord> {
    let (lo, hi) = search_bounds(pattern, mode);
    let accept = |k: &[u8]| pattern.is_none_or(|pat| key_matches(k, pat, mode));
    fold_window(
        limit,
        |after, budget| store.scan_candidates(lo, hi.as_deref(), Some(&accept), after, budget),
        |c| match store.materialize(c.row) {
            RowValue::Missing => None,
            RowValue::Tombstone => include_deleted.then(|| ExportRecord {
                key: c.key,
                value: None,
            }),
            // 逻辑值还要剥一次信封；过期与墓碑同待（只在 include_deleted 时出现）
            RowValue::Value(logical) => match logical_to_user(logical) {
                Some(user) => Some(ExportRecord {
                    key: c.key,
                    value: Some(user),
                }),
                None => include_deleted.then(|| ExportRecord {
                    key: c.key,
                    value: None,
                }),
            },
        },
    )
}

/// 游标式有界扫描：`after`（**不含**）之后最多 `limit` 行可见记录，返回 `(行, 下一页游标)`；
/// 游标 `None` = 已经到底，调用方该停了。
///
/// 存在的理由是 README 里那句"扫描返回物化 `Vec`"的解药：`scan(start, None)` 会把**从 start
/// 到库尾**的行全折出来（代价按剩余库量算），而翻页的人只要下一页。有了它，"把一个大库
/// 一页一页搬完"不必先把整库物化 —— 每页只折 `limit + 1` 组。
///
/// ⚠️ 游标取的是**折出来的行**的键（包括刚好过期、对用户不可见的那些）：过期行也必须推进游标，
/// 否则下一次又从同一批过期行撞起、永远走不动。墓碑在 `include_deleted = false` 的折叠里
/// 整行不参与，库尾若剩一串墓碑会由"折不满一页"如实判成到底。
pub fn scan_page<S: VersionedStore>(
    store: &S,
    after: Option<&[u8]>,
    limit: usize,
) -> (Vec<ExportRecord>, Option<Vec<u8>>) {
    let now = now_unix_secs();
    let (rows, more) = collect_rows(store, false, None, None, after, Some(limit.max(1)));
    let mut out = Vec::new();
    let mut cursor: Option<Vec<u8>> = None;
    for (key, _ver, raw) in rows {
        cursor = Some(key.clone());
        let Some(bytes) = raw else { continue };
        let m = decode_value_meta(&bytes);
        if m.is_expired_at(now) {
            continue;
        }
        out.push(ExportRecord {
            key,
            value: Some(m.user().to_vec()),
        });
    }
    (out, if more { cursor } else { None })
}
