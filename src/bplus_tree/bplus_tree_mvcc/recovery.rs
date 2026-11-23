//! 本文件负责WAL 和版本起点恢复。
//! 模块边界仅组织代码：锁顺序、版本可见性、磁盘格式和发布失败的处理均沿用原契约。

use super::*;

/// 落树但**不静默**：树对"单行比一页还大"这一条只能返回 false，而 bulk 的 `put` 与 WAL 重放
/// 都没有返回值通道（`KvBulk::put` 是 `()`），所以这里把事实喊出来。
/// 两种情况都不会写坏已有数据：树那边是**一字未写**。
pub(super) fn insert_or_shout(kv: &mut BPlusTree, enc: Vec<u8>, row: RowId, site: &str) {
    if !kv.insert(enc.clone(), row) {
        log::error!(
            "{site}: 这一行装不进任何一页（编码后键 {} 字节），已拒收；数据未受影响",
            enc.len()
        );
    }
}

/// 把 WAL 收口成「只剩一条 Checkpoint」。
///
/// 顺序反直觉但必须这样：原先是 `append(Checkpoint) → sync → truncate()`，而 `truncate()`
/// 只留 header —— **刚写的 Checkpoint 被自己抹掉了**。bplus 的起点另有 `NVER` meta 兜着，
/// 但 README 写的「其次用 WAL 的 checkpoint」那一级因此永远走不到；改成先截断再写回。
///
/// 崩在截断之后、写入之前不影响正确性：那时 WAL 为空，open 退回 meta/全树扫描拿起点，只是慢。
pub(super) fn seal_wal_checkpoint(
    wal: &mut Wal,
    next_version: u64,
    root_page_id: u64,
    next_page_id: u64,
) -> std::io::Result<()> {
    wal.truncate()?;
    wal.append(WalRecord::Checkpoint {
        next_version,
        root_page_id,
        next_page_id,
    })?;
    wal.sync()
}
/// 把存储状态与下一版本起点持久化，再封存或清空可安全丢弃的 WAL。
/// 调用方要保护活动事务边界，不能清掉仍用于撤销或恢复的日志。

/// checkpoint 的实体：手动 `MVCC::checkpoint()` 与提交时的 WAL 阈值触发共用一份实现。
///
/// 顺序不能换：先把 `next_version` 记进 meta（`NVER`）再 flush 页，最后才收口 WAL ——
/// WAL 一旦被截断，未落盘的页就再无重放来源。
pub(super) fn run_checkpoint(
    kv: &Mutex<BPlusTree>,
    wal: &Mutex<Wal>,
    next_version: &AtomicU64,
) -> std::io::Result<()> {
    #[cfg(test)]
    let _probe = crate::common::maintenance_probe::Timer::start(
        crate::common::maintenance_probe::Stage::Checkpoint,
    );
    wal.lock().unwrap().ensure_healthy()?;
    let mut kv = kv.lock().unwrap();
    let mut wal = wal.lock().unwrap();
    let next_ver = next_version.load(Ordering::SeqCst);
    kv.set_next_version(next_ver);
    kv.try_flush_all()?;
    let root = kv.root_page_id();
    let next_page = kv.next_page_id();
    seal_wal_checkpoint(&mut wal, next_ver, root.0, next_page)?;
    Ok(())
}

/// 决定 open 后的 `next_version` 起点（尽量不扫全树）。
///
/// 优先级：
/// 1. meta 中的 next_version（干净 checkpoint 后）
/// 2. WAL 最后一条 Checkpoint 的 next_version
/// 3. max(WAL max_xid, 全树 max version)+1（旧库兜底）
pub(super) fn resolve_start_version(
    tree: &mut BPlusTree,
    plan: &crate::bplus_tree::wal::RecoveryPlan,
) -> u64 {
    let mut base: Option<u64> = None;

    if let Some(nv) = tree.next_version() {
        // meta 已有（含 0=空库）；与 WAL 恢复后的 max_xid 取 max
        base = Some(nv);
    }

    if let Some(WalRecord::Checkpoint { next_version, .. }) = &plan.last_checkpoint {
        base = Some(base.map(|b| b.max(*next_version)).unwrap_or(*next_version));
    }

    if let Some(b) = base {
        // 恢复可能 REDO 了更高 xid，或 meta 略旧
        return b.max(plan.max_xid.saturating_add(1)).max(1);
    }

    // 旧库：meta 无 NVER、WAL 无 checkpoint → 全树扫描（仅此路径）
    let disk_max = scan_max_version(tree);
    disk_max.max(plan.max_xid).saturating_add(1).max(1)
}

pub(super) fn scan_max_version(tree: &mut BPlusTree) -> u64 {
    let mut max_v = 0u64;
    for (enc, _) in tree.iter() {
        if let Some(vk) = decode_key(&enc) {
            max_v = max_v.max(vk.version);
        }
    }
    max_v
}
