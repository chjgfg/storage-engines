//! 版本回收（vacuum）的**安全性**与**有效性**。
//!
//! `vacuum()` 原本用「`xmin` = 活跃事务 id 最小值」当回收水位线。这条水位线**不安全**：
//! 它没有考虑活跃事务 begin 时的快照集里那些**后来提交了**的事务，会把仍被需要的版本裁掉。
//! 于是四个引擎改成「有任何活跃事务就整轮跳过」。这个文件把三条语义钉住：
//!
//! - **安全性**：vacuum 不能破坏**已经开始**的事务的快照读。
//!   裁错版本 = 静默读到旧值/空值，比多跑一轮 vacuum 危险得多。
//! - **有效性**：空闲时必须真的把链压到 1，否则「跳过」就成了永远不干活的借口。
//! - **可观测**：因活跃事务而跳过，必须能和「没东西可裁」区分开，不能静默返回 0。
//!
//! 四个引擎共用同一套 vacuum 语义，所以每条都对四个引擎各跑一遍。

use std::fs;
use std::path::PathBuf;

use storage_engines::{open_engine, KvEngine};

const ENGINES: [&str; 4] = ["memory", "bitcask", "bplus_tree", "lsm_tree"];

/// 每个用例 × 每个引擎一个独立目录，避免文件锁互相干扰。
///
/// 注意 `open_engine` 的 path 语义不统一：`bplus_tree` 要的是**库文件**路径，
/// `bitcask` / `lsm_tree` 要的是**目录**，`memory` 忽略。
fn open(engine: &str, tag: &str) -> (Box<dyn KvEngine>, PathBuf) {
    let dir = PathBuf::from(format!("./data/vacuum_{tag}_{engine}"));
    let _ = fs::remove_dir_all(&dir);
    let _ = fs::create_dir_all(&dir);
    let path = if engine == "bplus_tree" {
        dir.join("data.db")
    } else {
        dir.clone()
    };
    let db = open_engine(engine, Some(&path)).unwrap_or_else(|e| panic!("[{engine}] open: {e}"));
    (db, dir)
}

/// **核心安全性**：vacuum 不得破坏运行中事务的快照读。
///
/// 复现的时序：
///
/// ```text
/// t1 写 K=v2 但未提交 → t2 begin，快照排除 t1（应读到 v1）
/// t1 提交 → 离开活跃表，但 t2 的快照**仍然**排除它
/// 旧水位线：活跃表里只剩 t2 → xmin = t2.version
///        → K 的两个版本都 < xmin → 只保留最新的那个（t1 的 v2）
///        → 而 v2 对 t2 不可见，v1 已被删 → t2 读到 None
/// ```
fn snapshot_read_survives_vacuum(engine: &str) {
    let (db, dir) = open(engine, "snapshot");

    {
        let t0 = db.begin();
        assert!(t0.set(b"K", b"v1".to_vec()));
        t0.commit();
    }

    // t1 写新版本，先不提交
    let t1 = db.begin();
    assert!(t1.set(b"K", b"v2".to_vec()));

    // t2 的快照排除 t1 → 只能看到 v1
    let t2 = db.begin();
    assert_eq!(
        t2.get(b"K").as_deref(),
        Some(b"v1".as_slice()),
        "[{engine}] 前置：t2 的快照应排除未提交的 t1"
    );

    // t1 提交：它离开活跃表，但 t2 的快照集里仍记着它
    t1.commit();

    db.vacuum().expect("vacuum");

    assert_eq!(
        t2.get(b"K").as_deref(),
        Some(b"v1".as_slice()),
        "[{engine}] vacuum 裁掉了 t2 快照仍需要的版本 —— 静默读到错误值"
    );

    t2.commit();
    drop(t1);
    drop(t2);
    drop(db);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn snapshot_read_survives_vacuum_all_engines() {
    for engine in ENGINES {
        snapshot_read_survives_vacuum(engine);
    }
}

/// **有效性**：空闲（无活跃事务）时必须把版本链真的压到 1。
///
/// 判据不看内部结构，只看 vacuum 自己的统计：第一次应删掉 N-1 个旧版本，
/// **紧接着再来一次应无可删** —— 后者才证明链被压到了 1，而不是只删了一部分。
/// 这条同时是「修安全性时不要把正常路径一起关掉」的回归守卫。
fn idle_vacuum_collapses_chain(engine: &str) {
    let (db, dir) = open(engine, "collapse");

    const WRITES: usize = 50;
    for i in 0..WRITES {
        let tx = db.begin();
        assert!(tx.set(b"hot:key", format!("v{i}").into_bytes()));
        tx.commit();
    }

    let first = db.vacuum().expect("vacuum");
    assert_eq!(
        first.versions_removed,
        WRITES - 1,
        "[{engine}] 空闲 vacuum 应把 {WRITES} 个版本压到 1"
    );

    let second = db.vacuum().expect("vacuum");
    assert_eq!(
        second.versions_removed, 0,
        "[{engine}] 链已压到 1，再 vacuum 应无可删"
    );

    {
        let tx = db.begin();
        assert_eq!(
            tx.get(b"hot:key").as_deref(),
            Some(format!("v{}", WRITES - 1).as_bytes()),
            "[{engine}] vacuum 后最新值必须还在"
        );
        tx.commit();
    }

    drop(db);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn idle_vacuum_collapses_chain_all_engines() {
    for engine in ENGINES {
        idle_vacuum_collapses_chain(engine);
    }
}

/// **可观测性**：「因活跃事务跳过」必须和「扫了但没东西可裁」区分开。
///
/// 两种情况的 `versions_removed` 都是 0，但结论完全相反：
///
/// | | `versions_removed` | `skipped_active_txn` | 含义 |
/// |---|---|---|---|
/// | 有活跃事务 | 0 | `true` | vacuum **根本没工作** —— 时机不对，或有事务泄漏没 commit |
/// | 链已压到 1 | 0 | `false` | 正常，没有可裁的东西 |
///
/// 只看 `versions_removed == 0` 是分不出来的。对定时调用 vacuum 的使用方来说，
/// 「一直在跳过」是个必须能报警的状态：否则版本链照旧无界增长，而监控上一片平静。
fn skip_is_distinguishable_from_nothing_to_do(engine: &str) {
    let (db, dir) = open(engine, "observable");

    for i in 0..5 {
        let tx = db.begin();
        assert!(tx.set(b"counter", format!("{i}").into_bytes()));
        tx.commit();
    }

    // ① 有活跃事务 → 跳过，且必须自报跳过
    let holder = db.begin();
    let skipped = db.vacuum().expect("vacuum");
    assert!(
        skipped.skipped_active_txn,
        "[{engine}] 有活跃事务时必须自报跳过，不能静默返回 0"
    );
    assert_eq!(
        skipped.versions_removed, 0,
        "[{engine}] 跳过的那轮不该动任何版本"
    );
    holder.commit();
    drop(holder);

    // ② 空闲 → 真的裁，且不报跳过
    let did_work = db.vacuum().expect("vacuum");
    assert!(
        !did_work.skipped_active_txn,
        "[{engine}] 无活跃事务时不该跳过"
    );
    assert!(
        did_work.versions_removed > 0,
        "[{engine}] 跳过那轮什么都没裁，说明这些旧版本还在，本轮必须裁到"
    );

    // ③ 链已压到 1 → 同样是 0，但这次不是跳过
    let nothing_to_do = db.vacuum().expect("vacuum");
    assert!(
        !nothing_to_do.skipped_active_txn,
        "[{engine}] 无可裁 ≠ 跳过"
    );
    assert_eq!(
        nothing_to_do.versions_removed, 0,
        "[{engine}] 链已压到 1"
    );

    drop(db);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn skip_is_distinguishable_from_nothing_to_do_all_engines() {
    for engine in ENGINES {
        skip_is_distinguishable_from_nothing_to_do(engine);
    }
}
