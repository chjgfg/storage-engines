//! 本文件负责WAL 和版本起点恢复。
//! 模块边界仅组织代码：锁顺序、版本可见性、磁盘格式和发布失败的处理均沿用原契约。

use super::*;
/// 按事务记录重建已提交写入和未完成事务的撤销计划。
/// 只认可可完整解析的日志记录；提交与未提交记录不能混用同一恢复判据。

/// 根据 WAL 记录生成恢复计划
pub fn analyze_recovery(entries: &[WalEntry]) -> RecoveryPlan {
    use std::collections::{HashMap, HashSet};

    let mut plan = RecoveryPlan::default();
    let mut open_writes: HashMap<u64, Vec<(Vec<u8>, Option<Vec<u8>>)>> = HashMap::new();
    let mut committed: HashSet<u64> = HashSet::new();

    for e in entries {
        match &e.record {
            WalRecord::Begin { xid } => {
                plan.max_xid = plan.max_xid.max(*xid);
                open_writes.entry(*xid).or_default();
            }
            WalRecord::Write { xid, key, value } => {
                plan.max_xid = plan.max_xid.max(*xid);
                open_writes
                    .entry(*xid)
                    .or_default()
                    .push((key.clone(), value.clone()));
            }
            WalRecord::Commit { xid } => {
                plan.max_xid = plan.max_xid.max(*xid);
                if let Some(writes) = open_writes.remove(xid) {
                    for (k, v) in writes {
                        plan.committed_writes.push((*xid, k, v));
                    }
                }
                committed.insert(*xid);
            }
            WalRecord::Abort { xid } => {
                plan.max_xid = plan.max_xid.max(*xid);
                if let Some(writes) = open_writes.remove(xid) {
                    for (k, _) in writes {
                        plan.uncommitted_writes.push((*xid, k));
                    }
                }
            }
            WalRecord::Checkpoint { .. } => {
                plan.last_checkpoint = Some(e.record.clone());
            }
        }
    }

    // 仍未结束的事务 → 全部视为需要撤销
    for (xid, writes) in open_writes {
        if !committed.contains(&xid) {
            for (k, _) in writes {
                plan.uncommitted_writes.push((xid, k));
            }
        }
    }
    plan
}
