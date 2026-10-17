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

/// 折叠的**迭代下界**：`start`（含）与 `after`（不含）里字节序更大的那个。
///
/// 为什么可以拿 `after` 当起点而不是只在循环里过滤它：任何 `raw > after` 的键，它的版本键
/// `raw ‖ version_be` 一定 `≥ raw > after` ⇒ 从字节界 `after` 起升序走，需要的那些行**一行都不会漏**；
/// 而 `raw == after` 那一组与"以 after 为前缀的更短键"的高版本仍会被走到，靠调用方那句
/// `raw <= after ⇒ continue` 挡掉（版本键前缀交错的坑，见 [`crate::common::version`]）。
/// 反过来**不能**用 `after ‖ 0xFF×8` 当下界：那会把 `"abcd"`（比 `"abc"` 大、字节序却小于
/// `"abc"‖FF…`）整把键跳掉 —— 静默少行。
///
/// 没有这一条，`scan_page` 每翻一页都要从库头重走：每页代价 ∝ 库大小（2026-10-17 的 200k
/// 实测：bplus 每页 12.5 ms → 189 ms），`migrate` 与 `search` 的续跑共用这条路径。
pub fn fold_lower_bound<'a>(
    start: Option<&'a [u8]>,
    after: Option<&'a [u8]>,
) -> Option<&'a [u8]> {
    match (start, after) {
        (Some(s), Some(a)) => Some(if a > s { a } else { s }),
        (None, Some(a)) => Some(a),
        (s, None) => s,
    }
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


/// 从"某把键的可见逻辑行"造出对外那一行：墓碑 / 过期 / 读不到都返回 `None`。
///
/// 这是 `scan_foreach` 里那四行的镜像。之所以要抽出来：`seek_prev` 现在按**单键**问"这把键活着吗"，
/// 那段判定再抄一遍就会出现"scan 认为它过期、seek_prev 认为它活着"这种两口不一致。
pub fn live_record_of_logical(
    key: Vec<u8>,
    logical: Option<Vec<u8>>,
    now: u64,
) -> Option<ExportRecord> {
    let bytes = logical?;
    let m = decode_value_meta(&bytes);
    if m.is_expired_at(now) {
        return None;
    }
    Some(ExportRecord {
        key,
        value: Some(m.user().to_vec()),
    })
}

// 测试专用：`seek_prev_with` 为了返回一行，往回退过几把键。
//
// M20 的复杂度尺。旧实现把 `[库头, key]` 整段折成 `Vec` 再 `pop()` ⇒ 一次调用 O(库大小) 的
// 时间和内存（200k 实测单次 140~610 ms，同库一次点读 2.9~57 µs）。改成反向逐步之后，代价只由
// "要跳过几把死键"决定，与库大小无关 ⇒ 守卫断言的是**步数**，不是墙钟。
#[cfg(test)]
thread_local! {
    static SEEK_PREV_STEPS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// 本线程 `seek_prev` 往回退过的键数（测试探针用）
#[cfg(test)]
pub fn seek_prev_steps() -> u64 {
    SEEK_PREV_STEPS.with(|c| c.get())
}

/// `seek_prev` 的一份循环：**反向一步取候选键，判定复用引擎现成的单键路径**。
///
/// - `prev_raw(bound)`：编码键 `≤ bound` 的**最大那一行**属于哪把逻辑键；`None` = 走到库头了
/// - `live(raw)`：这把键对**当前事务**可见、未过期、非墓碑时交出那一行（用
///   [`live_record_of_logical`] 配引擎的 `latest_visible_raw`，别在这里另写判定）
///
/// 上界那两条规则都不显然：
/// - 第一轮用 `version_key_upper_bound(key)` ⇒ 含 `key` **自己那一组**（语义是"≤ key"不是"< key"）；
/// - 之后用被拒的那把 raw 本身当上界：它的每行都是 `raw ‖ version_be` > `raw` ⇒ 整组自动排除，
///   而所有更小的键都还在。
///
/// ⚠️ "反向第一个见到的 raw 就是最大的那把键"依赖版本键的老合同 **`version < 2^56`**
/// （见 [`crate::common::version_key_upper_bound`]）：首字节 0x00 小于任何 ASCII，短前缀键的版本行
/// 才翻不到更长键之上。合同破了这里会**静默返回偏小的键** —— 那条合同自己有守卫，别拆。
pub fn seek_prev_with(
    at_or_below: &[u8],
    mut prev_raw: impl FnMut(&[u8]) -> Option<Vec<u8>>,
    mut live: impl FnMut(&[u8]) -> Option<ExportRecord>,
) -> Option<ExportRecord> {
    let mut bound = crate::common::version_key_upper_bound(at_or_below);
    loop {
        #[cfg(test)]
        SEEK_PREV_STEPS.with(|c| c.set(c.get() + 1));
        let raw = prev_raw(&bound)?;
        if let Some(rec) = live(&raw) {
            return Some(rec);
        }
        if raw.as_slice() >= bound.as_slice() {
            // 只有某家的 `prev_raw` 没按界排除整组时才会走到这里；宁可返回 None 也不原地打转
            return None;
        }
        bound = raw;
    }
}
