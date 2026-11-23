// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// M53 的守卫：**一次收口只许付一次 fsync**。
///
/// 洞：`Wal::truncate()` 改前在重写头部时自带一次 fsync，而收口的下一步 `sync()` 又一次
/// ⇒ 一次 open、一次 `Drop`、每次阈值 checkpoint 都白付一遍（大库台架是按站点数出来的：
/// `wal_header(截断里重写) 1 + wal_sync(收口) 1`）。
/// 判据是**计数**不是墙钟：本机 `C:`/`D:` 的 fsync 单价差 33 倍，毫秒没有可比性，次数有。
///
/// 证人（尺子自己得先接得上）：同一条路上新建 WAL 必须数到一次 `wal_header` ——
/// 数不到的话下面那个 `wal_sync == 1` 就只是"量不到"（M38/M48：读数 0 有两种解释）。
#[test]
fn wal_seal_fsyncs_exactly_once() {
    use crate::common::disk::{
        SITE_WAL_HEADER, SITE_WAL_SYNC, SITE_WAL_TRUNCATE, fsync_counts, fsync_counts_clear,
    };
    let path = tmp_wal("seal_fsync");
    fsync_counts_clear();
    let mut wal = Wal::open(&path).unwrap();
    let fresh = fsync_counts();
    assert_eq!(
        fresh[SITE_WAL_HEADER], 1,
        "证人失败：新建 WAL 那一次 fsync 都没数到（{:?}）⇒ 尺子没接在门上",
        fresh
    );

    // 收口的形状：三家的 `seal_wal_checkpoint` 都是这三步
    fsync_counts_clear();
    wal.truncate().unwrap();
    wal.append(WalRecord::Checkpoint {
        next_version: 7,
        root_page_id: 0,
        next_page_id: 0,
    })
    .unwrap();
    wal.sync().unwrap();
    let c = fsync_counts();
    assert_eq!(
        c[SITE_WAL_TRUNCATE], 0,
        "truncate 又自己 fsync 了 ⇒ 同一趟收口付两遍（M53 改前的形状）"
    );
    assert_eq!(
        c[SITE_WAL_SYNC], 1,
        "收口那一次 fsync 是唯一的持久化点：现在读到 {} 次（0 = 把持久化点省掉了，比慢更糟）",
        c[SITE_WAL_SYNC]
    );
    assert_eq!(c.iter().sum::<u64>(), 1, "这一趟收口总账：{:?}", c);

    // 语义一字未动：截断之后 WAL 里就剩那条 Checkpoint
    let entries = wal.read_all().unwrap();
    assert_eq!(entries.len(), 1, "截断没生效：还剩 {} 条", entries.len());
    assert!(
        matches!(
            entries[0].record,
            WalRecord::Checkpoint {
                next_version: 7,
                ..
            }
        ),
        "收口写进去的那条不是 Checkpoint：{:?}",
        entries[0].record
    );
    let _ = std::fs::remove_file(&path);
}

/// M25 守卫：**恢复计划不许依赖 `Begin`**。
///
/// 同一条事务流写两遍 —— 一遍带 `Begin`（0.4.0~M24 的形状，老库里就是这么存的），一遍不带 ——
/// 再从文件里读回来折 plan，**REDO 与 UNDO 两边必须一字不差**。
///
/// 为什么必须有它：M8 之后 `Begin` 只剩"把 xid 提前登记成一个空条目"这一件事，而 `Write`
/// 走的也是同一句 `open_writes.entry(xid).or_default()` ⇒ 它承载的信息是零。
/// **零信息的记录 + 零覆盖的假设** = 下一个人以为"事务开始在日志里有凭据"
/// （M18d 记在案：把整步作废之后全量测试一条都不红）。写侧从 M25 起不再落 `Begin`，
/// 而老日志必须还能读 —— 这条就是那半边的守卫，同时也是"变体不能删"的理由。
///
/// ⚠️ 唯一合法的差别是 `max_xid`：**只留下一条 `Begin` 就崩掉**的事务（只有老日志可能有），
/// 去掉 Begin 之后版本号起点小 1。那一笔一行都没写 ⇒ REDO/UNDO 都不受影响，
/// 但这条差别必须**看得见**，否则有人会拿它当"Begin 有用"的证据。
#[test]
fn plan_is_identical_with_or_without_begin() {
    // 一段真实形状的事务流：1 提交、2 撤销、3 崩在提交之前（只有 Write）、4 只留 Begin 就崩了
    fn script(wal: &mut Wal, with_begin: bool) {
        for xid in 1..=4u64 {
            if with_begin {
                wal.append(WalRecord::Begin { xid }).unwrap();
            }
            let rec = match xid {
                1 => WalRecord::Write {
                    xid,
                    key: b"k1".to_vec(),
                    value: Some(b"v1".to_vec()),
                },
                2 => WalRecord::Write {
                    xid,
                    key: b"k2".to_vec(),
                    value: None,
                },
                3 => WalRecord::Write {
                    xid,
                    key: b"k3".to_vec(),
                    value: Some(b"v3".to_vec()),
                },
                // 4 号只有 Begin 就崩了（老日志里那种"补了 Begin 还没写"的形状）
                _ => continue,
            };
            wal.append(rec).unwrap();
            match xid {
                1 => {
                    wal.append(WalRecord::Commit { xid }).unwrap();
                }
                2 => {
                    wal.append(WalRecord::Abort { xid }).unwrap();
                }
                _ => {}
            }
        }
        wal.sync().unwrap();
    }

    fn read_plan(tag: &str, with_begin: bool) -> (usize, RecoveryPlan) {
        let path = tmp_wal(tag);
        {
            let mut wal = Wal::open(&path).unwrap();
            script(&mut wal, with_begin);
        }
        let mut wal = Wal::open(&path).unwrap();
        let entries = wal.read_all().unwrap();
        let plan = analyze_recovery(&entries);
        let _ = std::fs::remove_file(&path);
        (entries.len(), plan)
    }

    let (n_with, with) = read_plan("begin_on", true);
    let (n_without, without) = read_plan("begin_off", false);
    // 带 Begin 的那份必须真读出 Begin（否则这份对拍是空转）
    assert!(
        n_with > n_without,
        "带 Begin 的日志只有 {n_with} 条，不带的有 {n_without} 条 ⇒ Begin 根本没写进去"
    );
    assert_eq!(
        with.committed_writes, without.committed_writes,
        "REDO 列表因为 Begin 的有无而变了 ⇒ 恢复其实依赖它"
    );
    assert_eq!(
        with.uncommitted_writes, without.uncommitted_writes,
        "UNDO 列表因为 Begin 的有无而变了 ⇒ 恢复其实依赖它"
    );
    // 唯一允许的差：那笔"只有 Begin"的事务把 max_xid 抬了一格
    assert_eq!(without.max_xid, 3, "不带 Begin 时日志里最大的 xid 是 3");
    assert_eq!(with.max_xid, 4, "Begin 把 4 号那笔空事务的 xid 也抬进来了");
    assert_eq!(with.committed_writes.len(), 1, "只有 1 号提交");
    assert_eq!(
        with.uncommitted_writes.len(),
        2,
        "2 号撤销 + 3 号崩在提交前"
    );
}

#[test]
fn test_wal_append_and_read() {
    let path = tmp_wal("basic");
    {
        let mut wal = Wal::open(&path).unwrap();
        wal.append(WalRecord::Begin { xid: 1 }).unwrap();
        wal.append(WalRecord::Write {
            xid: 1,
            key: b"k".to_vec(),
            value: Some(b"v".to_vec()),
        })
        .unwrap();
        wal.append(WalRecord::Commit { xid: 1 }).unwrap();
        wal.sync().unwrap();
    }
    {
        let mut wal = Wal::open(&path).unwrap();
        let entries = wal.read_all().unwrap();
        assert_eq!(entries.len(), 3);
        let plan = analyze_recovery(&entries);
        assert_eq!(plan.committed_writes.len(), 1);
        assert!(plan.uncommitted_writes.is_empty());
        assert_eq!(plan.max_xid, 1);
    }
    let _ = std::fs::remove_file(&path);
}
