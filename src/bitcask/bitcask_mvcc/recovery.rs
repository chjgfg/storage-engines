//! 本文件负责WAL 和版本起点恢复。
//! 模块边界仅组织代码：锁顺序、版本可见性、磁盘格式和发布失败的处理均沿用原契约。

use super::*;

// 测试专用（M37 的先量）：一次全库 `vacuum_bounded` 的分段瞬时峰值标尺在 [`crate::mem_probe`]
// 里（`stage_base` / `stage_mark` / `stage_peaks`），M41 起 lsm 也用同一把。
//
// 为什么要分段而不是只看整段：600k 档 `vacuum()` 用修好的量具（M38）读到 **171.4 MB**，
// 而这段里叠着四件事 —— ①归组判定、②删除 + flush + blob sync、③blob GC（读存活行 +
// `rewrite_in_place`）、④`kv.merge()` 重写主日志。M30/M31 两次的教训都是"最合理的那件事
// 不是元凶"，所以先量再动手。⚠️ 标尺带门控（`VACUUM_STAGE_SPLIT`），常开会把外层台架的
// 读数锯短，见 `mem_probe` 里那段注释。

/// 把 WAL 收口成「只剩一条 Checkpoint」。
///
/// 顺序反直觉但必须这样：原先是 `append(Checkpoint) → sync → truncate()`，而 `truncate()`
/// 只留 header —— **刚写的 Checkpoint 被自己抹掉了**，于是下次 open 无从知道 `next_version`，
/// 只能退回 O(总行数) 的全库 KeyDir 重扫。改成先截断、再把 Checkpoint 写成新 WAL 的第一条。
///
/// 崩在截断之后、写入之前也不影响正确性：那时 WAL 为空，open 走全库扫拿起点，只是慢。
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

/// checkpoint 的实体：手动 `MVCC::checkpoint()` 与提交时的 WAL 阈值触发共用同一份实现。
///
/// 顺序不能换：`Commit` 记录落盘 = 事务持久，数据页可以之后慢慢刷；但 WAL 一旦被截断，
/// 未落盘的数据就再无重放来源 —— 所以先 flush 数据与 blob，最后才收口 WAL。
/// WAL 收口保持原节奏；自动调用摊销 hint 刷新，手动调用强制刷新。
pub(super) fn run_checkpoint(
    kv: &Mutex<Bitcask>,
    blob: &Mutex<BlobStore>,
    wal: &Mutex<Wal>,
    next_version: &AtomicU64,
    hint_policy: crate::bitcask::bitcask::HintPolicy,
) -> std::io::Result<()> {
    #[cfg(test)]
    let _checkpoint = crate::bitcask::checkpoint_probe::Timer::start(
        crate::bitcask::checkpoint_probe::Stage::Checkpoint,
    );
    wal.lock().unwrap().ensure_healthy()?;
    let mut kv = kv.lock().unwrap();
    let mut blob = blob.lock().unwrap();
    let mut wal = wal.lock().unwrap();
    {
        #[cfg(test)]
        let _flush = crate::bitcask::checkpoint_probe::Timer::start(
            crate::bitcask::checkpoint_probe::Stage::Flush,
        );
        kv.flush()?;
        blob.sync()?;
    }
    {
        #[cfg(test)]
        let _hint = crate::bitcask::checkpoint_probe::Timer::start(
            crate::bitcask::checkpoint_probe::Stage::Hint,
        );
        match kv.checkpoint_hint(hint_policy)? {
            crate::bitcask::bitcask::HintRefresh::Written => {}
            crate::bitcask::bitcask::HintRefresh::Deferred { unhinted_records } => {
                log::debug!(
                    "checkpoint deferred hint: {unhinted_records} changed records; recovery replays data-log tail"
                );
            }
        }
    }
    let next_ver = next_version.load(Ordering::SeqCst);
    #[cfg(test)]
    let _wal = crate::bitcask::checkpoint_probe::Timer::start(
        crate::bitcask::checkpoint_probe::Stage::Wal,
    );
    seal_wal_checkpoint(&mut wal, next_ver)?;
    Ok(())
}

/// 决定 open 后 next_version 的位置（旧库：WAL 兜底）。

pub(super) fn scan_max_version(tree: &mut Bitcask) -> std::io::Result<u64> {
    #[cfg(test)]
    SCAN_MAX_CALLS.with(|c| c.set(c.get() + 1));
    let mut max_v = 0u64;
    // 只扫 KeyDir，不读 value
    for (enc, _) in tree.iter_meta() {
        if let Some(vk) = decode_key(enc) {
            max_v = max_v.max(vk.version);
        }
    }
    Ok(max_v)
}
