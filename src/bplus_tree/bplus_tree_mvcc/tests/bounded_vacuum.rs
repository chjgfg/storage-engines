// 该文件集中验证对应职责，保留原测试名称和字面值。

/// 有界回收：每轮只碰一小段，多轮跑完必须和一次全库 vacuum 得到**同样**的结果。
///
/// `vacuum()` 把 kv 锁按到整条叶子链走完；切片之后每轮读的**叶子**有上限
/// （偏差 = 一条链的长度：截断只能落在逻辑 key 边界），游标每轮必须前进。
/// 键里放了前缀嵌套的 `k1` / `k1x`：版本键 `raw ‖ 8B` 下两把 key 的区间会互相穿插。
/// `vacuum()` 必须**按块**删除，不许把整库的待删行攒在一个 `Vec` 里（M37）。
///
/// 判据与另外三家同形同值（`vac_pending_max()` ≤ 2×块界，且可删行数必须大于一块防自空转）。
/// bplus 是唯一"删除要真改树"的一家（`kv.delete` 会走借键/合并那套重组），所以这条还顺手
/// 证明：**按块删除与攒到最后删除，裁出来的行完全一致**（每把键都读得回最新版 +
/// `rows_scanned`/`versions_removed` 数量对得上）。
///
/// 变异验证：把块界临时抬到一整库那么大 ⇒ 待删行数变成 20 000 > 界 16 384，红。
#[test]
fn test_vacuum_deletes_in_bounded_chunks() {
    const KEYS: usize = 20_000;
    let path = tmp_db("vac_chunks");
    {
        let mvcc = MVCC::open(&path, 64, 4096);
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
        let st = mvcc.vacuum().unwrap();
        let pending = vac_pending_max();
        eprintln!(
            "[vac chunks bplus] 裁掉 {} 行 / 扫 {} 行 ⇒ 单次 flush 前最多攒 {} 行（块界 {VAC_CHUNK_ROWS}）",
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
    cleanup(&path);
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
    let full_path = tmp_db("bounded_full");
    let full_removed;
    {
        let mvcc = MVCC::open(&full_path, 16, 32);
        fill(&mvcc);
        full_removed = mvcc.vacuum().unwrap().versions_removed;
    }
    cleanup(&full_path);

    let path = tmp_db("bounded_slice");
    {
        let mvcc = MVCC::open(&path, 16, 32);
        fill(&mvcc);

        let mut cursor: Option<Vec<u8>> = None;
        let mut rounds = 0usize;
        let mut removed = 0usize;
        loop {
            let round = mvcc.vacuum_bounded(cursor.as_deref(), SLICE).unwrap();
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
        assert_eq!(removed, full_removed, "分片裁的总行数应与整库一轮一致");
        let h = mvcc.health();
        assert_eq!(h.max_chain, 1, "分片跑完后链必须压平");
        assert_eq!(h.total_rows, KEYS.len() as u64);

        let tx = mvcc.begin_transaction();
        for k in KEYS {
            assert_eq!(tx.get(k), Some(format!("v{}", CHAIN - 1).into_bytes()));
        }
        tx.commit();
    }
    cleanup(&path);
}
