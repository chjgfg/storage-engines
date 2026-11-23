// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// vacuum 期间不该发生任何 value 克隆：它只需要版本号 + 「是不是墓碑」这个标志位。
///
/// 量尺与 bplus 同一理由 —— 内存引擎没有 IO，`iter()` 的代价就是每行一次 value 克隆。
#[test]
fn test_vacuum_does_not_clone_values() {
    const CHAIN: u64 = 300;
    const KEYS: u64 = 4;

    let mvcc = MVCC::new();
    for i in 0..CHAIN {
        for k in 0..KEYS {
            let key = format!("k{k}");
            let tx = mvcc.begin_transaction();
            assert!(tx.set(key.as_bytes(), format!("v{i}").into_bytes()));
            tx.commit();
        }
    }

    let base = crate::memory::mem_store::value_clones();
    let stats = mvcc.vacuum();
    let used = crate::memory::mem_store::value_clones() - base;
    eprintln!(
        "vacuum 期间 value 克隆次数：{used}（裁掉 {} 行，历史行数 {}）",
        stats.versions_removed,
        CHAIN * KEYS
    );
    assert_eq!(
        stats.versions_removed,
        ((CHAIN - 1) * KEYS) as usize,
        "每 key 留一版，其余都该裁掉"
    );
    assert_eq!(used, 0, "vacuum 只需版本号与墓碑标志，不该克隆任何 value");
}

#[test]
fn test_vacuum_removes_old_versions() {
    let mvcc = MVCC::new();
    let t1 = mvcc.begin_transaction();
    assert!(t1.set(b"k", b"v1".to_vec()));
    t1.commit();
    let t2 = mvcc.begin_transaction();
    assert!(t2.set(b"k", b"v2".to_vec()));
    t2.commit();
    let before = mvcc.raw_len();
    assert!(before >= 2);
    let stats = mvcc.vacuum();
    assert!(stats.versions_removed >= 1);
    let after = mvcc.raw_len();
    assert!(after < before);
    let tx = mvcc.begin_transaction();
    assert_eq!(tx.get(b"k"), Some(b"v2".to_vec()));
    tx.commit();
}

/// 长跑进程不喊 vacuum，版本链也不能无限涨（I2：垃圾必须自己回来）。
///
/// 内存引擎没有磁盘空间问题，容易被当成「不用回收」—— 但事故的量是**行数**不是字节数，
/// 点读点写照样要陪跑整条链。四引擎同一个默认值、同一个守卫。
#[test]
fn test_auto_vacuum_reclaims_without_being_asked() {
    const CHAIN: u64 = 200;
    const SLICE: u64 = 8;

    let mvcc = MVCC::new();
    assert_eq!(
        mvcc.vacuum_auto_slice(),
        MVCC::VACUUM_AUTO_SLICE_DEFAULT,
        "自动回收默认必须开着"
    );
    mvcc.set_vacuum_auto_slice(SLICE);

    for i in 0..CHAIN {
        let tx = mvcc.begin_transaction();
        assert!(tx.set(b"hot", format!("v{i}").into_bytes()));
        tx.commit();
    }

    let h = mvcc.health();
    assert!(
        h.max_chain <= SLICE + 2,
        "没人喊 vacuum，链却涨到 {} 行 —— 自动回收没在工作",
        h.max_chain
    );

    let tx = mvcc.begin_transaction();
    assert_eq!(tx.get(b"hot"), Some(format!("v{}", CHAIN - 1).into_bytes()));
    tx.commit();
}

/// 关掉的开关必须真关掉：自动回收不能是「没法关掉的那种」。
#[test]
fn test_auto_vacuum_off_switch() {
    const CHAIN: u64 = 60;

    let mvcc = MVCC::new();
    mvcc.set_vacuum_auto_slice(0);
    for i in 0..CHAIN {
        let tx = mvcc.begin_transaction();
        assert!(tx.set(b"hot", format!("v{i}").into_bytes()));
        tx.commit();
    }
    assert_eq!(mvcc.health().max_chain, CHAIN, "关闭后不该裁掉任何一版");
}

/// 有界回收：每轮只碰一小段，多轮跑完必须和一次全库 vacuum 得到**同样**的结果。
///
/// `vacuum()` 把 store 锁按到整库走完；切片后每轮读的行数有上限（偏差 = 一条链的长度：
/// 截断只能落在逻辑 key 边界），游标每轮必须前进。
/// 键里放了前缀嵌套的 `k1` / `k1x`：版本键 `raw ‖ 8B` 下两把 key 的区间会互相穿插。
/// `vacuum()` 必须**按块**删除，不许把整库的待删行攒在一个 `Vec` 里（M37）。
///
/// 判据与 bitcask / lsm 那条同形同值：可删旧版远多于一块时，单次 flush 前最多攒的待删行
/// `vac_pending_max()` ≤ `2 × VAC_CHUNK_ROWS`；`versions_removed > VAC_CHUNK_ROWS` 保证块界
/// 真的被跨过（否则空转）。这一家改的收益最小（4.0 MB），但形状是同一份 —— 留下它就是
/// "改一家漏两家"那个事故形状的复刻。
///
/// 变异验证：把块界临时抬到一整库那么大 ⇒ 待删行数变成 20 000 > 界 16 384，红。
#[test]
fn test_vacuum_deletes_in_bounded_chunks() {
    const KEYS: usize = 20_000;
    let mvcc = MVCC::new();
    mvcc.set_vacuum_auto_slice(0);
    for _ in 0..2u64 {
        let tx = mvcc.begin_transaction();
        for i in 0..KEYS {
            let k = format!("k{i:06}");
            assert!(tx.set(k.as_bytes(), vec![b'v'; 24]), "第 {i} 把键写失败");
        }
        tx.commit();
    }
    reset_vac_pending();
    let st = mvcc.vacuum();
    let pending = vac_pending_max();
    eprintln!(
        "[vac chunks memory] 裁掉 {} 行 / 扫 {} 行 ⇒ 单次 flush 前最多攒 {} 行（块界 {VAC_CHUNK_ROWS}）",
        st.versions_removed, st.rows_scanned, pending
    );
    assert!(st.rows_scanned > 0, "一行都没扫到 ⇒ 空转");
    assert!(
        st.versions_removed > VAC_CHUNK_ROWS,
        "可删行数 {} 没超过一块 ⇒ 这条判据量不到界（空转）",
        st.versions_removed
    );
    assert!(pending > 0, "这把尺子没数到待删行 ⇒ 空转");
    assert!(
        pending <= 2 * VAC_CHUNK_ROWS as u64,
        "单次 flush 前攒了 {pending} 行待删（界 {}）⇒ 又变回整库一个 `Vec`",
        2 * VAC_CHUNK_ROWS as u64
    );
    let tx = mvcc.begin_transaction();
    for i in (0..KEYS).step_by(997) {
        let k = format!("k{i:06}");
        assert_eq!(
            tx.get(k.as_bytes()),
            Some(vec![b'v'; 24]),
            "第 {i} 把键在按块回收后读不回来"
        );
    }
    tx.commit();
}

#[test]
fn test_vacuum_bounded_slices_and_converges() {
    const CHAIN: u64 = 120;
    const KEYS: [&[u8]; 6] = [b"k1", b"k1x", b"k2", b"k3", b"k4", b"k5"];
    const SLICE: u64 = 40;

    fn fill(mvcc: &MVCC) {
        for i in 0..CHAIN {
            for k in KEYS {
                let tx = mvcc.begin_transaction();
                assert!(tx.set(k, format!("v{i}").into_bytes()), "写入被拒：{k:?}");
                tx.commit();
            }
        }
    }

    // 对照组
    let full = {
        let mvcc = MVCC::new();
        fill(&mvcc);
        mvcc.vacuum().versions_removed
    };

    let mvcc = MVCC::new();
    fill(&mvcc);

    let mut cursor: Option<Vec<u8>> = None;
    let mut rounds = 0usize;
    let mut removed = 0usize;
    loop {
        let round = mvcc.vacuum_bounded(cursor.as_deref(), SLICE);
        assert!(
            !round.stats.skipped_active_txn,
            "第 {rounds} 轮不该被活跃事务挡住"
        );
        assert!(
            round.stats.rows_scanned <= SLICE + CHAIN,
            "第 {rounds} 轮读了 {} 行（预算 {SLICE} + 一条链 {CHAIN}）",
            round.stats.rows_scanned
        );
        assert!(
            round.stats.versions_removed > 0,
            "第 {rounds} 轮必须裁掉东西"
        );
        removed += round.stats.versions_removed;
        rounds += 1;
        match round.next_cursor {
            Some(c) => {
                assert_ne!(cursor.as_ref(), Some(&c), "游标必须前进（第 {rounds} 轮）");
                cursor = Some(c);
            }
            None => break,
        }
        assert!(
            rounds < 60,
            "切片应在有限轮内收敛，别是原地打转：已 {rounds} 轮"
        );
    }

    assert!(
        rounds > 1,
        "6 把 key × {CHAIN} 版不该一轮就扫完：{rounds} 轮"
    );
    assert_eq!(removed, full, "分片裁的总行数应与整库一轮一致");
    let h = mvcc.health();
    assert_eq!(h.max_chain, 1, "分片跑完后链必须压平");
    assert_eq!(h.total_rows, KEYS.len() as u64);

    let tx = mvcc.begin_transaction();
    for k in KEYS {
        assert_eq!(tx.get(k), Some(format!("v{}", CHAIN - 1).into_bytes()));
    }
    tx.commit();
}
