//! 本文件负责顺序、反向与维护扫描。

use super::*;

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
    #[cfg(test)]
    let reference = scan_cost_reference::enabled();
    #[cfg(not(test))]
    let reference = false;
    let mut cursor = if more && !reference {
        rows.last().map(|(key, _, _)| {
            #[cfg(test)]
            scan_cost_reference::note_page_cursor_copy();
            key.clone()
        })
    } else {
        None
    };
    for (key, _ver, raw) in rows {
        if reference {
            #[cfg(test)]
            scan_cost_reference::note_page_cursor_copy();
            cursor = Some(key.clone());
        }
        if let Some(record) = live_record_of_logical(key, raw, now) {
            out.push(record);
        }
    }
    (out, if more { cursor } else { None })
}
