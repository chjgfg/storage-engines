//! 事务版本与活动事务的可见性判断。

use super::*;

/// 快照可见性判据：版本 `v` 对「拿着 `my_version` 快照、看见活跃表 `active_xid`」的事务可见吗。
///
/// 两条，缺一不可：
/// 1. `v ∉ active_xid` —— 快照时刻还没提交的事务，其一版**不可见**（哪怕版本号更小）。
///    少了这条就会读到别人写了一半的值。
/// 2. `v <= my_version` —— 只看得见不比自己新的版本。
///
/// ⚠️ 顺序也要留意：`is_visible` 只看版本号与快照，**不看 TTL 是否过期、不看是不是墓碑**。
/// 那两件事属于取值之后的剥壳（`logical_to_user`），别混进来。
pub fn visible_at(v: u64, my_version: u64, active_xid: &HashSet<u64>) -> bool {
    if active_xid.contains(&v) {
        return false;
    }
    v <= my_version
}

/// 漏网事务的判据：活跃表里还留着"自己"，就说明既没 `commit` 也没 `rollback`。
///
/// 为什么用成员关系而不是额外标志位：`commit` / `rollback` 都以 `remove` 收尾，
/// 所以「还在表里」就是「未终结」的充要条件，多一个标志位就多一处会写漏的可能。
///
/// ⚠️ 这里**只查不删**。删除必须留给引擎自己的 `rollback()` —— 它还要按表里那份写集
/// 把本事务写过的版本行挨个清掉；在这里删掉，`rollback()` 就拿到空写集，
/// 事务的行会留在库里（判据写成 `remove` 是我第一版的想法，看了 rollback 才否掉）。
///
/// 锁中毒（别的线程 panic 过）时返回 `false`：Drop 里 panic 会直接 abort 进程，
/// 宁可不回滚也不能把宿主进程带下去。
pub fn is_unfinished(active_txn: &Mutex<Registry>, version: u64) -> bool {
    match active_txn.lock() {
        Ok(guard) => guard.contains_key(&version),
        Err(_) => false,
    }
}
