// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// M82：快照重放**不扫、也不记账** —— 重放出来的行每把键只有一版，一行垃圾都没有。
///
/// 判据（计数，不看墙钟）：重开一个 N 行的快照之后，`completed_slices == 0` 且
/// `pending_rows == 0`。改前那一版：重放是"每 1000 行一个事务"的写流 ⇒ 每 16.4 个事务扫一整片
/// （1:1 跟着"写"的量走），并且留下 N 行触发账（重开后的头 N/16384 个事务每个都要扫一片）。
/// 600k 档 `冷重开` **1.317 s**（`65b9aa6` 实测）→ **4.39 s**（`docs/test2.txt`）就是这笔；
/// 20M 档白扫一整库。
/// 对照实验：摘掉 `AutoVacuumOff::pause` 或 `discard_pending` ⇒ 本条红在 `completed_slices`
/// 或 `pending_rows` 上（两处各管一半）。
#[test]
fn memory_snapshot_replay_neither_scans_nor_accrues_debt() {
    const N: usize = 20_000; // ≫ 一片（16 Ki），否则"扫过"与"没扫"分不出来
    let dir = tmp_dir("memory_snapshot_quiet");
    {
        let db = open_engine("memory", Some(&dir)).unwrap();
        let tx = db.begin();
        for i in 0..N {
            assert!(
                tx.set(format!("k{i:05}").as_bytes(), format!("v{i}").into_bytes()),
                "灌第 {i} 把"
            );
        }
        tx.commit();
        drop(tx);
        drop(db); // 停止落盘
    }
    let db = open_engine("memory", Some(&dir)).unwrap();
    let st = db.reclamation_status().expect("memory 必须能报回收状态");
    assert_eq!(
        st.completed_slices, 0,
        "重放期间扫了 {} 片 —— 新写入的行没有旧版本可回收",
        st.completed_slices
    );
    assert_eq!(
        st.pending_rows, 0,
        "重放留下 {} 行触发账 ⇒ 重开后的头几个事务要替它白扫一整轮",
        st.pending_rows
    );
    // 别把"安静"做成"停摆"：重放之后自动回收必须照常工作（账照记、片照扫）
    let tx = db.begin();
    for i in 0..N {
        assert!(
            tx.set(format!("k{i:05}").as_bytes(), format!("v{i}b").into_bytes()),
            "更新第 {i} 把"
        );
    }
    tx.commit();
    drop(tx);
    assert!(
        db.reclamation_status().unwrap().pending_rows > 0,
        "重放之后自动回收必须还在工作（新写入要照常记账）"
    );
    drop(db);
    let _ = std::fs::remove_dir_all(&dir);
}

/// M81 第 6 项：memory 的启停快照 —— **只在启停各碰一次文件**。四条判据各钉一件事，
/// 单看任何一条都像是"过了"，合起来才是这个合同：
/// ① 运行期不碰文件：写满 1000 行之后快照**还不存在**；
/// ② `flush()` 仍然是 no-op（所以台架里 memory 的 `flush` 格一个数都不变）；
/// ③ 停止写、启动读：值与 TTL 都回来，而**已过期的那把不许复活**；
/// ④ 没给目录 = 一行字节都不写（`MVCC::new()` 的既有形状，四个 CLI 与所有旧测试靠它，
///    所以下面那条 `memory_engine_has_no_path_and_no_merge` 守卫并不会红 —— 方案预计的
///    "path() 必红"只在"给了目录"那一支成立）。
#[test]
fn memory_snapshot_binary_values_survive_repeated_reopens() {
    let dir = tmp_dir("memory_snapshot_binary");
    let values = vec![
        vec![],
        vec![0],
        vec![0, 0, 65],
        vec![1, 0, 0, 0, 0, 0, 0, 0, 0, 65],
        vec![1, 255, 255, 255, 255, 255, 255, 255, 255, 65],
        (0..=255).collect::<Vec<u8>>(),
    ];
    let expires = crate::common::codec::now_unix_secs() + 3600;
    {
        let db = open_engine("memory", Some(&dir)).unwrap();
        let tx = db.begin();
        for i in 0usize..1007 {
            assert!(tx.set_raw(&i.to_be_bytes(), values[i % values.len()].clone()));
        }
        assert!(tx.set(b"ttl", crate::common::codec::pack_ttl(&values[3], expires)));
        assert!(tx.set(b"expired", crate::common::codec::pack_ttl(b"gone", 1)));
        assert!(tx.set_raw(b"deleted", vec![0, 65]));
        assert!(tx.delete(b"deleted"));
        tx.try_commit().unwrap();
    }
    for reopen in 1..=3 {
        let db = open_engine("memory", Some(&dir)).unwrap();
        let tx = db.begin();
        for i in 0usize..1007 {
            assert_eq!(
                tx.get(&i.to_be_bytes()),
                Some(values[i % values.len()].clone()),
                "reopen {reopen}, key {i}"
            );
        }
        assert_eq!(tx.get(b"ttl"), Some(values[3].clone()));
        assert_eq!(tx.get(b"expired"), None);
        assert_eq!(tx.get(b"deleted"), None);
        let records = db.export_latest_visible(true);
        assert!(
            records
                .iter()
                .any(|r| r.key == b"deleted" && r.value.is_none())
        );
        assert_eq!(
            records
                .iter()
                .find(|r| r.key == b"ttl")
                .unwrap()
                .expire_unix_secs,
            Some(expires)
        );
        tx.try_commit().unwrap();
    }
}

#[test]
fn memory_snapshot_rejects_damage_without_rewriting_source() {
    let dir = tmp_dir("memory_snapshot_damage");
    {
        let db = open_engine("memory", Some(&dir)).unwrap();
        let tx = db.begin();
        assert!(tx.set_raw(b"a", vec![0, 65]));
        assert!(tx.set_raw(b"b", vec![1; 10]));
        tx.try_commit().unwrap();
    }
    let path = dir.join("snapshot.bxp");
    let original = std::fs::read(&path).unwrap();
    for cut in 0..original.len() {
        std::fs::write(&path, &original[..cut]).unwrap();
        assert!(
            open_engine("memory", Some(&dir)).is_err(),
            "accepted cut at {cut}"
        );
        assert_eq!(
            std::fs::read(&path).unwrap(),
            original[..cut],
            "rewrote failed open"
        );
    }
    for offset in 0..original.len() {
        let mut damaged = original.clone();
        damaged[offset] ^= 1;
        std::fs::write(&path, &damaged).unwrap();
        assert!(
            open_engine("memory", Some(&dir)).is_err(),
            "accepted damage at {offset}"
        );
        assert_eq!(
            std::fs::read(&path).unwrap(),
            damaged,
            "rewrote failed open"
        );
    }
}

#[test]
fn memory_snapshot_round_trips_only_at_open_and_stop() {
    let dir = tmp_dir("memory_snapshot");
    let snap = dir.join("snapshot.bxp");
    {
        let db = open_engine("memory", Some(&dir)).unwrap();
        assert_eq!(
            db.path().as_deref(),
            Some(snap.as_path()),
            "给了目录就该报快照路径"
        );
        let tx = db.begin();
        for i in 0..1000 {
            assert!(
                tx.set(format!("k{i:04}").as_bytes(), format!("v{i}").into_bytes()),
                "灌第 {i} 把"
            );
        }
        assert!(
            tx.set_with_ttl(
                b"ttl",
                b"live".to_vec(),
                std::time::Duration::from_secs(3600)
            ),
            "写 TTL 行"
        );
        assert!(
            tx.set(b"dead", crate::common::codec::pack_ttl(b"gone", 1)),
            "写已过期行"
        );
        tx.commit();
        assert!(
            !snap.exists(),
            "① 运行期不许碰文件：还没释放句柄就不该有快照"
        );
        drop(tx); // 事务借着库，必须先放开才能 drop 引擎（快照正是在那一刻写的）
        db.flush().unwrap();
        assert!(!snap.exists(), "② flush() 必须仍是 no-op");
        drop(db); // 停止落盘发生在句柄释放时（计时格外）
    }
    assert!(snap.exists(), "③ Drop 必须写出快照（优雅停止不丢）");
    let written = std::fs::metadata(&snap).unwrap().len();
    {
        let db = open_engine("memory", Some(&dir)).unwrap();
        let tx = db.begin();
        assert_eq!(
            tx.get(b"k0000").as_deref(),
            Some(b"v0".as_ref()),
            "重开读不回第一行"
        );
        assert_eq!(
            tx.get(b"k0999").as_deref(),
            Some(b"v999".as_ref()),
            "重开读不回最后一行"
        );
        assert_eq!(
            tx.get(b"ttl").as_deref(),
            Some(b"live".as_ref()),
            "TTL 行丢了"
        );
        assert!(
            tx.get_ttl(b"ttl").flatten().is_some(),
            "TTL 行读回来变成永久了"
        );
        assert_eq!(tx.get(b"dead"), None, "已过期的行不许复活");
        for i in 0..50 {
            tx.set(format!("again{i}").as_bytes(), b"x".to_vec());
        }
        tx.commit();
        assert_eq!(
            std::fs::metadata(&snap).unwrap().len(),
            written,
            "④ 运行期再写一批，快照字节数必须一字不变"
        );
    }
    // 没给目录的那次 open：连"别人的快照"也不许碰
    let before = std::fs::metadata(&snap).unwrap().len();
    {
        let db = open_engine("memory", None).unwrap();
        assert!(db.path().is_none(), "没给目录 = 仍然没有路径（改前形状）");
        let tx = db.begin();
        assert!(tx.set(b"x", b"y".to_vec()));
        tx.commit();
    }
    assert_eq!(
        std::fs::metadata(&snap).unwrap().len(),
        before,
        "纯内存那次 open/drop 不许写任何文件"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
