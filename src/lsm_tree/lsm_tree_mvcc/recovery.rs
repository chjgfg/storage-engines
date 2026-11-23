//! 本文件负责WAL 和版本起点恢复。
//! 模块边界仅组织代码：锁顺序、版本可见性、磁盘格式和发布失败的处理均沿用原契约。

use super::*;

/// 把 WAL 收口成「只剩一条 Checkpoint」。
///
/// 原先的顺序是 `append(Checkpoint) → sync → truncate()`，而 `truncate()` 只留 header
/// —— 刚写的 Checkpoint 被自己抹掉，`compute_start_version` 里「用 WAL checkpoint 拿起点」
/// 那一级其实永远走不到（本引擎靠 `MVCC.NEXT` 侧文件兜住了，所以没暴露成问题）。
/// 改成先截断、再把 Checkpoint 写成新 WAL 的第一条。
pub(super) fn seal_wal_checkpoint(wal: &mut Wal, next_version: u64) -> std::io::Result<()> {
    wal.truncate()?;
    wal.append(WalRecord::Checkpoint {
        next_version,
        root_page_id: 0,
        next_page_id: 0,
    })?;
    wal.sync()
}
/// 把存储状态与下一版本起点持久化，再封存或清空可安全丢弃的 WAL。
/// 调用方要保护活动事务边界，不能清掉仍用于撤销或恢复的日志。

/// checkpoint 的实体：手动 `MVCC::checkpoint()`、提交时的阈值触发、`try_open` 收尾共用一份。
///
/// 顺序不能换：先 flush（mem→L0，必要时 compact）再收口 WAL —— WAL 一旦被截断，
/// 未落盘的内存表就再无重放来源。`MVCC.NEXT` 最后写，它是 WAL 之外的那份起点保险。
pub(super) fn run_checkpoint(
    kv: &Mutex<LSMTree>,
    wal: &Mutex<Wal>,
    next_version: &AtomicU64,
    db_dir: &Path,
) -> std::io::Result<()> {
    wal.lock().unwrap().ensure_healthy()?;
    let mut kv = kv.lock().unwrap();
    let mut wal = wal.lock().unwrap();
    kv.flush()?;
    let next_ver = next_version.load(Ordering::SeqCst);
    seal_wal_checkpoint(&mut wal, next_ver)?;
    persist_mvcc_next(db_dir, next_ver);
    Ok(())
}

/// MVCC 档里**不容下游改动**的两个字段：钉回 `false`，无论传进来的是什么。
///
/// - `enable_mem_wal`：持久化点是这层的事务 WAL（commit 时 fsync WAL 记录）。再开 mem.wal
///   就变成同一份数据有两个提交日志，崩溃后"谁重放谁、谁覆盖谁"没有定义 ——
///   而 [`MVCC::durability_profile`] 承诺的正是"只有一个"。
/// - `enable_bg_compact`：后台线程会在 MVCC 外层锁**之外**动同一棵树的层，事务 WAL 的
///   重放/截断不知道它在跑。更要命的是 [`LSMTree::set_options`] 并不会起线程（线程只在
///   open 时按当时的 option 创建），若让它被热改成 `true`，写路径会以为"有人替我 compact"
///   而**不再同步 compact** ⇒ L0 无限涨。所以这条路整个封掉。
pub(super) fn pin_mvcc_invariants(patch: LSMTreeOptions) -> LSMTreeOptions {
    let mut o = patch;
    o.enable_mem_wal = false;
    o.enable_bg_compact = false;
    o
}

/// 持久化文件：`dir/MVCC.NEXT`（WAL truncate 后仍保留 next_version）
pub(super) fn mvcc_next_path(dir: &Path) -> PathBuf {
    dir.join("MVCC.NEXT")
}

pub(super) fn load_mvcc_next(dir: &Path) -> u64 {
    let p = mvcc_next_path(dir);
    let Ok(s) = std::fs::read_to_string(p) else {
        return 0;
    };
    s.trim().parse().unwrap_or(0)
}

pub(super) fn persist_mvcc_next(dir: &Path, next: u64) {
    let p = mvcc_next_path(dir);
    let _ = std::fs::write(p, format!("{next}\n"));
}

/// 从 checkpoint / WAL / redo 键 / MVCC.NEXT 推算起始版本，**不扫 SST 全表**
pub(super) fn compute_start_version(dir: &Path, plan: &crate::lsm_tree::wal::RecoveryPlan) -> u64 {
    let mut start = 1u64;

    // 1) 上次成功 open/checkpoint 留下的元数据（WAL 可能已被 truncate）
    start = start.max(load_mvcc_next(dir));

    // 2) WAL 里最后一个 Checkpoint
    if let Some(crate::lsm_tree::wal::WalRecord::Checkpoint { next_version, .. }) =
        &plan.last_checkpoint
    {
        start = start.max(*next_version);
    }

    // 3) WAL 见过的最大 xid → 下一个可用
    start = start.max(plan.max_xid.saturating_add(1));

    // 4) redo/undo 键上的 version 后缀（仅 WAL 里的键，不是全库）
    for (_xid, key, _) in &plan.committed_writes {
        if let Some(vk) = decode_key(key) {
            start = start.max(vk.version.saturating_add(1));
        }
    }
    for (_xid, key) in &plan.uncommitted_writes {
        if let Some(vk) = decode_key(key) {
            start = start.max(vk.version.saturating_add(1));
        }
    }

    start.max(1)
}
