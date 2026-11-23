//! 本文件负责逐页跨引擎迁移。

use super::*;

/// 一次搬运的账。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MigrateStats {
    /// 搬过去的逻辑 key 数
    pub rows: u64,
    /// 用户 value 字节总数
    pub bytes: u64,
    /// 走了多少页（`page_rows` 那么大的一页）
    pub pages: u64,
    /// 带 TTL 的行数（这些用 `set_with_ttl` 写，按剩余时间换算）
    pub with_ttl: u64,
}
/// 按页扫描源引擎，每页在目标引擎独立提交。
/// 复制用户原始字节并按剩余 TTL 写入；当前页失败会撤销该页，之前已提交的页仍然存在。

/// 把 `from` 每个可见逻辑 key 搬到 `to`：跨引擎在线搬运，不必经 CLI 的中间文件。
///
/// 读侧是 [`KvTxn::scan_page`] 一页一页走 ⇒ **内存与每页代价都只看 `page_rows`**，
/// 与库总量无关（用 `export_latest_visible()` 整库物化那种写法搬大库，单核 6 GB 会当场翻）。
/// 写侧一页一个事务（`commit` 每页一次，不是每行一次 WAL fsync）。
///
/// 语义要说清（这三条都是刻意选择，不是没做完）：
/// - **只带"当前可见的最新版"**：历史版本与墓碑不带 ⇒ 目标库的 `max_chain` 从 1 开始，
///   这通常正是搬家的目的（顺手把版本链压平）。要连墓碑一起搬得走 `include_deleted` 的导出路径。
/// - **TTL 按剩余时间续**：绝对过期时刻折成 `Duration` 交给 `set_with_ttl` ⇒ 迁移本身不
///   改变"还剩多久过期"，但目标库的过期判定从此刻起按它自己的 `now` 算。
/// - **值用 `set_raw` 写**（源侧读回来的是已剥信封的用户字节，直接进目标侧的 `set`
///   会踩信封约定 —— 这正是 [`KvTxn::set_raw`] 存在的理由）。
///
/// 目标库已有同名 key 会被覆盖成新的版本行（不是冲突）；写不进去（如 B+ 树拒收超长键）
/// 会中止并回滚当前页，返回 [`EngineError::MigrateAborted`]。
pub fn migrate(from: &dyn KvEngine, to: &dyn KvEngine, page_rows: usize) -> Result<MigrateStats> {
    let page_rows = page_rows.max(1);
    let mut stats = MigrateStats::default();
    let mut cursor: Option<Vec<u8>> = None;
    loop {
        // scan_page 已返回源侧绝对过期时间；和用户值一起携带，使每条源记录只访问一次。
        let (rows, next) = {
            #[cfg(test)]
            let begin_probe = migration_probe::Timer::start(migration_probe::Stage::SourceBegin);
            let tx = from.begin();
            #[cfg(test)]
            std::mem::drop(begin_probe);
            #[cfg(test)]
            let scan_probe = migration_probe::Timer::start(migration_probe::Stage::SourceScan);
            let (page, next) = tx.scan_page(cursor.as_deref(), page_rows);
            #[cfg(test)]
            std::mem::drop(scan_probe);
            #[cfg(test)]
            let projection_probe =
                migration_probe::Timer::start(migration_probe::Stage::Projection);
            let page: Vec<(Vec<u8>, Vec<u8>, Option<u64>)> = page
                .into_iter()
                .filter_map(|rec| {
                    rec.value
                        .map(|value| (rec.key, value, rec.expire_unix_secs))
                })
                .collect();
            #[cfg(test)]
            std::mem::drop(projection_probe);
            #[cfg(test)]
            let commit_probe = migration_probe::Timer::start(migration_probe::Stage::SourceCommit);
            tx.commit();
            #[cfg(test)]
            std::mem::drop(commit_probe);
            (page, next)
        };
        if !rows.is_empty() {
            #[cfg(test)]
            let begin_probe = migration_probe::Timer::start(migration_probe::Stage::TargetBegin);
            let tx = to.begin();
            #[cfg(test)]
            std::mem::drop(begin_probe);
            #[cfg(test)]
            let write_probe = migration_probe::Timer::start(migration_probe::Stage::TargetWrite);
            for (key, value, ttl) in rows {
                stats.rows += 1;
                stats.bytes += value.len() as u64;
                let ok = match ttl {
                    Some(expire) => {
                        stats.with_ttl += 1;
                        let now = crate::common::now_unix_secs();
                        let left = expire.saturating_sub(now).max(1);
                        tx.set_with_ttl(&key, value, std::time::Duration::from_secs(left))
                    }
                    None => tx.set_raw(&key, value),
                };
                if !ok {
                    #[cfg(test)]
                    std::mem::drop(write_probe);
                    #[cfg(test)]
                    let _rollback_probe =
                        migration_probe::Timer::start(migration_probe::Stage::TargetRollback);
                    tx.rollback();
                    return Err(EngineError::MigrateAborted {
                        key,
                        reason: "目标侧写被拒（写冲突或键/值超出一页载荷）",
                    });
                }
            }
            #[cfg(test)]
            std::mem::drop(write_probe);
            #[cfg(test)]
            let commit_probe = migration_probe::Timer::start(migration_probe::Stage::TargetCommit);
            tx.commit();
            #[cfg(test)]
            std::mem::drop(commit_probe);
        }
        stats.pages += 1;
        match next {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }
    Ok(stats)
}
