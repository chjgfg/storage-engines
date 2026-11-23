// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn visible_at_requires_committed_and_not_newer() {
    let mut active = HashSet::new();
    active.insert(7u64);
    // 提交过的旧版本可见
    assert!(visible_at(5, 10, &active));
    // 比我新 ⇒ 不可见
    assert!(!visible_at(11, 10, &active));
    // 快照时刻还活着的事务，哪怕版本号更小也不可见（少了这条就读到别人写一半的值）
    assert!(!visible_at(7, 10, &active));
    // 边界：等于自己可见（就是本事务自己写的行）
    assert!(visible_at(10, 10, &active));
}

/// 只读事务不许进 WAL；写过一次之后，整条事务的 `Commit`/fsync 都要回来。
#[test]
fn wal_policy_opens_only_on_first_write() {
    let p = WalPolicy::default();
    assert!(!p.opened(), "刚 begin 的事务不该写 Commit、也不该 fsync");
    p.mark_opened();
    assert!(p.opened(), "写过就得写 Commit + fsync，否则持久化点丢了");
    p.mark_opened();
    assert!(
        p.opened(),
        "重复标记不许把状态打回去（M25 起这里只记账，不再有「第一次」的返回值）"
    );
}
