//! 本文件负责可见记录流式导出。

use super::*;

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
    let (rows, more) =
        collect_rows_as(
            store,
            start,
            end,
            after,
            limit,
            |key, version, value| match value {
                RowValue::Tombstone => include_deleted.then(|| (key, version, None)),
                RowValue::Value(bytes) => Some((key, version, Some(bytes))),
                RowValue::Missing => None,
            },
        );
    (rows, more)
}

/// [`collect_rows`] 的**通用行形状**版：调用方自己决定折出来长什么样（M27）。
///
/// 为什么要有它：`scan` / `export` / `print_all` 这些公开面要的本来就是 `ExportRecord`，
/// 走 `collect_rows` 会先攒出一份 `(key, version, 字节)` 元组 `Vec`、再逐条搬进
/// `Vec<ExportRecord>` ⇒ 两份头结构同时活着（200k 键实测那一段 ≈ 15 MB，见 README 的
/// "一次全量 scan 峰值"条目）。让映射发生在折叠内部，中间那份就不存在了。
///
/// 回调签名 `(key, version, RowValue)`：`RowValue` 保持**三态**（与
/// [`VersionedStore::materialize`] 同）。`Missing` 由驱动直接丢掉（不叫回调），墓碑与
/// 取值成功都叫 ⇒ **"墓碑要不要出现在结果里"完全由回调决定**（所以这里不需要
/// `include_deleted` 这个参数：本函数的调用方各自都有判据，塞一个布尔进来只会假装通用）。
///
/// 返回 `(行, 是否可能还有更多)`：`limit` 给了就按"多要一条"判，没给就是 `false`。
pub fn collect_rows_as<S, T, M>(
    store: &S,
    start: Option<&[u8]>,
    end: Option<&[u8]>,
    after: Option<&[u8]>,
    limit: Option<usize>,
    mut map: M,
) -> (Vec<T>, bool)
where
    S: VersionedStore,
    M: FnMut(Vec<u8>, u64, RowValue) -> Option<T>,
{
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
            rv => map(c.key, c.version, rv),
        },
    );
    let more = wanted.is_some_and(|w| rows.len() >= w);
    let rows = match limit {
        Some(n) => rows.into_iter().take(n).collect(),
        None => rows,
    };
    (rows, more)
}

/// **流式**折叠：每折出一行立刻交给 `on`，一行都不攒（M27）。
///
/// `KvTxn::scan_foreach` 的文档从写下那天起就声称"不收集全库到单一大 Vec"，
/// 而实现是 `for … in collect_latest_raw_in(...)`（先把全库元组攒出来）。这条把它兑现：
/// `on` 返回 `false` 就当场收手，**后面的键一个都不再折** —— 这一点有计数守卫钉着
/// （提前叫停时本引擎的"折过的键数"必须只与已交出的行数同阶，而不是与全库同阶）。
///
/// 与 [`collect_rows`] 的分工：值还是在这条路径上取的（`Missing` 不叫回调），
/// 但结果不进 `Vec`；`include_deleted` 同上，墓碑要不要出现由调用方在 `on` 里判。
pub fn fold_rows_streaming<S, F>(
    store: &S,
    include_deleted: bool,
    start: Option<&[u8]>,
    end: Option<&[u8]>,
    mut on: F,
) where
    S: VersionedStore,
    F: FnMut(Vec<u8>, u64, RowValue) -> bool,
{
    fold_window_emit(
        None,
        |round_after, budget| {
            let cursor = round_after;
            store.scan_candidates(start, end, None, cursor, budget)
        },
        |c| match store.materialize(c.row) {
            RowValue::Missing => None,
            rv => Some((c.key, c.version, rv)),
        },
        |(key, version, value)| {
            if !include_deleted && matches!(value, RowValue::Tombstone) {
                return true; // 墓碑不参与，但**不算交出去一行**，继续走
            }
            on(key, version, value)
        },
    );
}

/// 导出行形状**唯一那份判据**（M44）：折一行、判一行、立刻交给 `on`，一行都不攒。
///
/// 为什么要新加一条而不是让调用方继续用 `export_latest_visible`：那条返回
/// `Vec<ExportRecord>` —— 那笔 `Vec` 是"要整库的人"的合同，可**导出到文件**的人要的是
/// "写出去"，不是"在内存里再摆一份"。600k 档实测一次 `export 全库(物化)` 峰值 **+123.7 MB**，
/// 而折叠侧（[`fold_rows_streaming`]）从 M27 起就一行不攒 ⇒ 卡住的那一头在出口。
///
/// 四家的 `export_latest_visible` 现在都是**这条的收集壳**（M39 那招：判据只剩一份实现）：
/// 墓碑 / 过期或坏壳 / 读不到三类怎么处置，四家必须一字不差；它们唯一的差别是
/// "逻辑字节 → 用户字节"那一步住在哪家，所以那一步当参数进来（`unseal`）。
///
/// 返回值 = 实际交给 `on` 的行数。`on` 返回 `false` ⇒ 当场收手，**后面的键不再折**。
pub fn export_rows_streaming<S, U>(
    store: &S,
    include_deleted: bool,
    unseal: U,
    on: &mut dyn FnMut(ExportRecord) -> bool,
) -> usize
where
    S: VersionedStore,
    // 按值收：折叠交出来的那份逻辑字节本来就是 owned `Vec`，剥壳要把它拆开看 ——
    // 借成 `&[u8]` 再复制一份出来，正是这一刀要省掉的那类占用。
    U: Fn(Vec<u8>) -> Option<Vec<u8>>,
{
    let mut handed = 0usize;
    // `include_deleted` 直接交给折叠放行：为 false 时墓碑根本不会进来，为 true 时
    // "墓碑记成 value=None" 由下面这份映射判 —— 与改前 `collect_rows_as` 的判据同一份。
    fold_rows_streaming(store, include_deleted, None, None, |key, _ver, value| {
        let rec = match value {
            RowValue::Tombstone => Some(ExportRecord::new(key, None)),
            RowValue::Value(bytes) => {
                // 出口必须把 TTL 一起带出去：只交用户字节 = 导入侧无从重打信封 ⇒ 键变永久。
                // 多解一次信封（1 个标签字节 + 8 字节）换的是"搬一次少一批到期时刻"。
                // ⚠️ 取账要在 `unseal(bytes)` 把字节交出去**之前** —— 那之后 `bytes` 就没了。
                let expire = crate::common::codec::logical_expiry(&bytes);
                match unseal(bytes) {
                    Some(user) => Some(ExportRecord {
                        key,
                        value: Some(user),
                        expire_unix_secs: expire,
                    }),
                    None => {
                        if include_deleted {
                            Some(ExportRecord::new(key, None))
                        } else {
                            None
                        }
                    }
                }
            }
            // 读不到与墓碑不是一回事：整行不参与
            RowValue::Missing => None,
        };
        match rec {
            Some(r) => {
                handed += 1;
                on(r)
            }
            None => true,
        }
    });
    handed
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
pub fn fold_lower_bound<'a>(start: Option<&'a [u8]>, after: Option<&'a [u8]>) -> Option<&'a [u8]> {
    match (start, after) {
        (Some(s), Some(a)) => Some(if a > s { a } else { s }),
        (None, Some(a)) => Some(a),
        (s, None) => s,
    }
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
    #[cfg(test)]
    if scan_cost_reference::enabled() {
        let bytes = logical?;
        let m = crate::common::codec::decode_value_meta(&bytes);
        if m.is_expired_at(now) {
            return None;
        }
        crate::common::codec::note_user_value_copied();
        return Some(ExportRecord {
            key,
            value: Some(m.user().to_vec()),
            expire_unix_secs: m.expire_unix_secs(),
        });
    }
    let bytes = logical?;
    let VisibleOwnedValue::Live { user, expire } = visible_owned_value(bytes, now) else {
        return None;
    };
    Some(ExportRecord {
        key,
        value: Some(user),
        // ⚠️ 这一行就是 0.5.0 的那件事：这条门是 export / migrate / 快照文件**共用**的出口，
        // 以前只交 `m.user()` ⇒ 到期时刻在出口就被丢掉，导出去再导回来一律变永久。
        expire_unix_secs: expire,
    })
}

/// 界内**存活行**的流式扫描 —— 四家 `scan_foreach` 的唯一实现（M35 收拢）。
///
/// 四份原先是一字不差的四份复制（折叠 + `RowValue` 三态筛 + 剥壳 + 过期判定），而"存活"的
/// 判据本来就只在 [`live_record_of_logical`] 一处定义 —— 判定抄四遍的意思是"改一处漏三处"，
/// 而 `seek_prev` 与 `scan` 一旦对同一行给出不同答案，就是静默的数据不一致。
///
/// 语义与四份旧复制逐条相同：`include_deleted = false` ⇒ 墓碑与"读不到"都不参与，过期行同待；
/// `f` 返回 `false` 当场收手，后面的键不再折（M27 那条流式兑现）。
pub fn scan_live_records<S, F>(store: &S, start: Option<&[u8]>, end: Option<&[u8]>, mut f: F)
where
    S: VersionedStore,
    F: FnMut(ExportRecord) -> bool,
{
    let now = now_unix_secs();
    fold_rows_streaming(store, false, start, end, |key, _ver, value| {
        let logical = match value {
            RowValue::Value(bytes) => Some(bytes),
            // 墓碑与"读不到"都不参与（include_deleted = false）
            _ => None,
        };
        let Some(rec) = live_record_of_logical(key, logical, now) else {
            return true;
        };
        f(rec)
    });
}
