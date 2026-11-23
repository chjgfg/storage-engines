//! KV、扫描、搜索、TTL 和数值命令。

use super::*;

// ── 基础读写 ────────────────────────────────────────────────────────────────

pub(super) fn cmd_get(tx: &dyn KvTxn, args: &[String]) -> (Out, TxnEnd) {
    let [k] = match one(args, "get K") {
        Ok(v) => v,
        Err(e) => return (e, TxnEnd::Commit),
    };
    match tx.get(k.as_bytes()) {
        Some(v) => {
            let t = Table {
                columns: vec!["key".into(), "value".into()],
                rows: vec![vec![show(k.as_bytes()), show(&v)]],
            };
            (Out::with_table("GET", "1 行".into(), t), TxnEnd::Commit)
        }
        None => (Out::msg("GET", "(nil)"), TxnEnd::Commit),
    }
}

pub(super) fn cmd_set(tx: &dyn KvTxn, args: &[String]) -> (Out, TxnEnd) {
    let [k, v] = match two(args, "set K V") {
        Ok(v) => v,
        Err(e) => return (e, TxnEnd::Commit),
    };
    match tx.try_set_raw(k.as_bytes(), v.as_bytes().to_vec()) {
        Ok(()) => (Out::msg("OK", "OK"), TxnEnd::Commit),
        Err(e) => (Out::err(e.to_string()), TxnEnd::Rollback),
    }
}

pub(super) fn cmd_setex(tx: &dyn KvTxn, args: &[String]) -> (Out, TxnEnd) {
    let [k, secs, v] = match three(args, "setex K 秒 V") {
        Ok(v) => v,
        Err(e) => return (e, TxnEnd::Commit),
    };
    let Ok(secs) = secs.parse::<u64>() else {
        return (
            Out::err(format!("TTL 要非负整数秒，收到 {secs:?}")),
            TxnEnd::Rollback,
        );
    };
    if tx.set_with_ttl(
        k.as_bytes(),
        v.as_bytes().to_vec(),
        std::time::Duration::from_secs(secs),
    ) {
        (
            Out::msg("OK", format!("OK（{secs}s 后过期）")),
            TxnEnd::Commit,
        )
    } else {
        (
            Out::err("写入失败（可能是冲突、容量限制或存储错误）"),
            TxnEnd::Rollback,
        )
    }
}

pub(super) fn cmd_del(tx: &dyn KvTxn, args: &[String]) -> (Out, TxnEnd) {
    let [k] = match one(args, "del K") {
        Ok(v) => v,
        Err(e) => return (e, TxnEnd::Commit),
    };
    match tx.try_delete(k.as_bytes()) {
        Ok(()) => (Out::msg("OK", "OK（写墓碑）"), TxnEnd::Commit),
        Err(e) => (Out::err(e.to_string()), TxnEnd::Rollback),
    }
}

pub(super) fn cmd_exists(tx: &dyn KvTxn, args: &[String]) -> (Out, TxnEnd) {
    let [k] = match one(args, "exists K") {
        Ok(v) => v,
        Err(e) => return (e, TxnEnd::Commit),
    };
    let yes = tx.exists(k.as_bytes());
    (
        Out::msg(
            "OK",
            if yes {
                "1（存在）"
            } else {
                "0（不存在）"
            },
        ),
        TxnEnd::Commit,
    )
}

pub(super) fn cmd_meta(tx: &dyn KvTxn, args: &[String]) -> (Out, TxnEnd) {
    let [k] = match one(args, "meta K") {
        Ok(v) => v,
        Err(e) => return (e, TxnEnd::Commit),
    };
    match tx.get_meta(k.as_bytes()) {
        None => (Out::msg("META", "(nil)"), TxnEnd::Commit),
        Some(m) => {
            let t = Table {
                columns: vec!["字段".into(), "值".into()],
                rows: vec![
                    vec!["key".into(), show(&m.key)],
                    vec!["version".into(), m.version.to_string()],
                    vec![
                        "value_len".into(),
                        m.value_len
                            .map(|n| n.to_string())
                            .unwrap_or_else(|| "—".into()),
                    ],
                    vec!["deleted".into(), m.deleted.to_string()],
                    vec!["expired".into(), m.expired.to_string()],
                    vec![
                        "expire_unix_secs".into(),
                        m.expire_unix_secs
                            .map(|s| s.to_string())
                            .unwrap_or_else(|| "永久".into()),
                    ],
                ],
            };
            (
                Out::with_table(
                    "META",
                    format!("meta {}（version {}）", show(k.as_bytes()), m.version),
                    t,
                ),
                TxnEnd::Commit,
            )
        }
    }
}

// ── 计数器 ──────────────────────────────────────────────────────────────────

pub(super) fn cmd_incr(tx: &dyn KvTxn, args: &[String], is_incr: bool) -> (Out, TxnEnd) {
    let (name, usage) = if is_incr {
        ("incr", "incr K [N]")
    } else {
        ("decr", "decr K [N]")
    };
    // 步长是可选参数，这里不能拿 `one` 卡恰好一个
    let k = match args {
        [k] | [k, _] => k.as_str(),
        _ => return (Out::err(format!("参数不对，用法: {usage}")), TxnEnd::Commit),
    };
    let delta: i64 = match args.get(1).map(|s| s.parse::<i64>()) {
        None => 1,
        Some(Ok(n)) => n,
        Some(Err(e)) => {
            return (
                Out::err(format!("{name} 的步长要是整数：{e}")),
                TxnEnd::Rollback,
            );
        }
    };
    // decr(K, N) 走自己的门；负步长交给 i64 溢出判定
    let r = if is_incr {
        tx.incr(k.as_bytes(), delta)
    } else {
        tx.decr(k.as_bytes(), delta)
    };
    match r {
        Ok(n) => (
            Out::msg("OK", format!("{} = {n}", show(k.as_bytes()))),
            TxnEnd::Commit,
        ),
        Err(IncrError::NotInteger) => (
            Out::err(format!(
                "{} 的值不是十进制整数，incr/decr 只对计数器生效",
                show(k.as_bytes())
            )),
            TxnEnd::Rollback,
        ),
        Err(IncrError::Overflow) => (Out::err("i64 溢出"), TxnEnd::Rollback),
        Err(IncrError::WriteConflict) => (
            Out::err("写入失败（可能是冲突、容量限制或存储错误）"),
            TxnEnd::Rollback,
        ),
    }
}

// ── 批量 ────────────────────────────────────────────────────────────────────

pub(super) fn cmd_mset(tx: &dyn KvTxn, args: &[String]) -> (Out, TxnEnd) {
    if args.is_empty() || args.len() % 2 != 0 {
        return (
            Out::err("mset 要偶数个参数: mset K V [K V ...]"),
            TxnEnd::Rollback,
        );
    }
    let items: Vec<(Vec<u8>, Vec<u8>)> = args
        .chunks(2)
        .map(|p| (p[0].as_bytes().to_vec(), p[1].as_bytes().to_vec()))
        .collect();
    match tx.batch_set(&items) {
        Ok(()) => (
            Out::msg("OK", format!("OK（{} 键）", items.len())),
            TxnEnd::Commit,
        ),
        Err(bad) => (
            Out::err(format!(
                "写入失败于 {} —— 未提交部分已整体回滚（batch_set 本身不回滚，回滚是这一层做的）",
                show(&bad)
            )),
            TxnEnd::Rollback,
        ),
    }
}

pub(super) fn cmd_mget(tx: &dyn KvTxn, args: &[String]) -> (Out, TxnEnd) {
    if args.is_empty() {
        return (
            Out::err("mget 要至少一个 key: mget K [K ...]"),
            TxnEnd::Commit,
        );
    }
    let keys: Vec<Vec<u8>> = args.iter().map(|k| k.as_bytes().to_vec()).collect();
    let vals = tx.multi_get(&keys);
    let rows = keys
        .iter()
        .zip(&vals)
        .map(|(k, v)| {
            vec![
                show(k),
                v.as_ref()
                    .map(|b| show(b))
                    .unwrap_or_else(|| "(nil)".into()),
            ]
        })
        .collect();
    let n = vals.iter().filter(|v| v.is_some()).count();
    let t = Table {
        columns: vec!["key".into(), "value".into()],
        rows,
    };
    (
        Out::with_table("SCAN", format!("{} 键中 {} 个有值", keys.len(), n), t),
        TxnEnd::Commit,
    )
}

// ── 扫描 / 搜索 ─────────────────────────────────────────────────────────────

/// 把一批记录折成 key/value 表；超过 [`SCAN_CAP`] 截断并在脚注说明。
/// 墓碑（`value=None`，只在 include_deleted 时出现）显示 `<deleted>`。
pub(super) fn records_table(recs: Vec<ExportRecord>, what: &str) -> Out {
    let total = recs.len();
    let truncated = total > SCAN_CAP;
    let rows: Vec<Vec<String>> = recs
        .into_iter()
        .take(SCAN_CAP)
        .map(|r| {
            vec![
                show(&r.key),
                r.value
                    .as_ref()
                    .map(|v| show(v))
                    .unwrap_or_else(|| "<deleted>".into()),
            ]
        })
        .collect();
    let mut foot = format!("{what} {total} 行");
    if truncated {
        foot.push_str(&format!(
            "（只显示前 {SCAN_CAP} 行，用范围/前缀收窄或 count 看总量）"
        ));
    }
    let t = Table {
        columns: vec!["key".into(), "value".into()],
        rows,
    };
    Out::with_table("SCAN", foot, t)
}

pub(super) fn cmd_scan(tx: &dyn KvTxn, args: &[String], reverse: bool) -> (Out, TxnEnd) {
    if args.len() > 2 {
        return (
            Out::err("scan 至多两个参数: scan [START] [END]（区间 [START, END)，两端都可省）"),
            TxnEnd::Commit,
        );
    }
    let start = args.first().map(|s| s.as_bytes().to_vec());
    let end = args.get(1).map(|s| s.as_bytes().to_vec());
    let recs = if reverse {
        tx.reverse_scan(start.as_deref(), end.as_deref())
    } else {
        tx.scan(start.as_deref(), end.as_deref())
    };
    (
        records_table(recs, if reverse { "RSCAN" } else { "SCAN" }),
        TxnEnd::Commit,
    )
}

pub(super) fn cmd_prefix(tx: &dyn KvTxn, args: &[String]) -> (Out, TxnEnd) {
    let [p] = match one(args, "prefix P") {
        Ok(v) => v,
        Err(e) => return (e, TxnEnd::Commit),
    };
    let recs = tx.prefix_scan(p.as_bytes());
    // 分隔用 CJK 引号：前缀本身常以 `:` 结尾（如 user:），再接一个 `:` 会读成两个
    (
        records_table(recs, &format!("PREFIX「{}」", show(p.as_bytes()))),
        TxnEnd::Commit,
    )
}

pub(super) fn cmd_seek(tx: &dyn KvTxn, args: &[String], prev: bool) -> (Out, TxnEnd) {
    let (name, usage) = if prev {
        ("seekprev", "seekprev K（最后一条 ≤ K）")
    } else {
        ("seek", "seek K（第一条 ≥ K）")
    };
    let [k] = match one(args, usage) {
        Ok(v) => v,
        Err(e) => return (e, TxnEnd::Commit),
    };
    let r = if prev {
        tx.seek_prev(k.as_bytes())
    } else {
        tx.seek(k.as_bytes())
    };
    match r {
        None => (Out::msg("SEEK", "(nil)"), TxnEnd::Commit),
        Some(rec) => {
            let t = Table {
                columns: vec!["key".into(), "value".into()],
                rows: vec![vec![
                    show(&rec.key),
                    rec.value
                        .as_ref()
                        .map(|v| show(v))
                        .unwrap_or_else(|| "<deleted>".into()),
                ]],
            };
            (
                Out::with_table("SEEK", format!("{name} → {}", show(&rec.key)), t),
                TxnEnd::Commit,
            )
        }
    }
}

pub(super) fn cmd_count(tx: &dyn KvTxn, args: &[String]) -> (Out, TxnEnd) {
    if args.len() > 1 {
        return (Out::err("count 至多一个前缀: count [P]"), TxnEnd::Commit);
    }
    let p = args.first().map(|s| s.as_str()).unwrap_or("");
    let n = tx.key_count(p.as_bytes());
    let label: String = if p.is_empty() {
        "全库".into()
    } else {
        format!("前缀 {p}")
    };
    (
        Out::msg("OK", format!("count({label}) = {n}")),
        TxnEnd::Commit,
    )
}

pub(super) fn cmd_find(tx: &dyn KvTxn, args: &[String]) -> (Out, TxnEnd) {
    if args.is_empty() || args.len() > 3 {
        return (
            Out::err("find 要 1~3 个参数: find 子串 [页码] [每页条数]"),
            TxnEnd::Commit,
        );
    }
    let page: usize = match args.get(1).map(|s| s.parse()) {
        None => 0,
        Some(Ok(n)) => n,
        Some(Err(_)) => return (Out::err("页码要非负整数"), TxnEnd::Commit),
    };
    let size: usize = match args.get(2).map(|s| s.parse()) {
        None => FIND_PAGE_SIZE,
        Some(Ok(0)) => FIND_PAGE_SIZE, // 传 0 引擎会按 1 处理 —— 归到默认页宽，少一个陷阱
        Some(Ok(n)) => n,
        Some(Err(_)) => return (Out::err("每页条数要正整数"), TxnEnd::Commit),
    };
    let q = SearchQuery {
        pattern: args[0].as_bytes().to_vec(),
        page,
        page_size: size,
        mode: KeyMatchMode::Contains,
        include_deleted: false,
        need_total: true,
    };
    let pg = tx.search_keys(&q);
    let rows: Vec<Vec<String>> = pg
        .items
        .iter()
        .map(|r| {
            vec![
                show(&r.key),
                r.value
                    .as_ref()
                    .map(|v| show(v))
                    .unwrap_or_else(|| "<deleted>".into()),
            ]
        })
        .collect();
    let n = pg.items.len();
    let total = pg
        .total
        .map(|t| t.to_string())
        .unwrap_or_else(|| "—".into());
    let pages = pg
        .total_pages
        .map(|t| t.to_string())
        .unwrap_or_else(|| "—".into());
    let mut foot = format!(
        "FIND 第 {} 页 / 共 {pages} 页（本页 {n} 行，全库匹配 {total}）",
        pg.page
    );
    if pg.has_next {
        foot.push_str(&format!("；下一页: find {} {}", args[0], pg.page + 1));
    }
    let t = Table {
        columns: vec!["key".into(), "value".into()],
        rows,
    };
    (Out::with_table("SCAN", foot, t), TxnEnd::Commit)
}

// ── TTL ─────────────────────────────────────────────────────────────────────

pub(super) fn cmd_ttl(tx: &dyn KvTxn, args: &[String]) -> (Out, TxnEnd) {
    let [k] = match one(args, "ttl K") {
        Ok(v) => v,
        Err(e) => return (e, TxnEnd::Commit),
    };
    match tx.get_ttl(k.as_bytes()) {
        None => (Out::msg("TTL", "(nil)（键不存在或已过期）"), TxnEnd::Commit),
        Some(None) => (Out::msg("TTL", "永久（没设 TTL）"), TxnEnd::Commit),
        Some(Some(d)) => (
            Out::msg("TTL", format!("剩 {}s（{d:?}）", d.as_secs())),
            TxnEnd::Commit,
        ),
    }
}

pub(super) fn cmd_refresh(tx: &dyn KvTxn, args: &[String]) -> (Out, TxnEnd) {
    let [k, secs] = match two(args, "refresh K 秒") {
        Ok(v) => v,
        Err(e) => return (e, TxnEnd::Commit),
    };
    let Ok(secs) = secs.parse::<u64>() else {
        return (
            Out::err(format!("TTL 要非负整数秒，收到 {secs:?}")),
            TxnEnd::Commit,
        );
    };
    if tx.refresh_ttl(k.as_bytes(), std::time::Duration::from_secs(secs)) {
        (
            Out::msg("OK", format!("OK（从现在起 {secs}s）")),
            TxnEnd::Commit,
        )
    } else {
        (Out::err("没设上（键不存在或已过期）"), TxnEnd::Commit)
    }
}

pub(super) fn cmd_persist(tx: &dyn KvTxn, args: &[String]) -> (Out, TxnEnd) {
    let [k] = match one(args, "persist K") {
        Ok(v) => v,
        Err(e) => return (e, TxnEnd::Commit),
    };
    if tx.persist_key(k.as_bytes()) {
        (Out::msg("OK", "OK（改成永久）"), TxnEnd::Commit)
    } else {
        (Out::err("没改成（键不存在或已过期）"), TxnEnd::Commit)
    }
}

pub(super) fn cmd_purge(tx: &dyn KvTxn) -> (Out, TxnEnd) {
    let n = tx.purge_expired();
    (
        Out::msg("MAINT", format!("清掉 {n} 个过期键")),
        TxnEnd::Commit,
    )
}
