// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// M42：builder 必须**攒满一块就落盘**，不许在 `finish()` 之前握住整张表。
///
/// 判据（单位=条）：灌 N 条、块宽 B ⇒ "同时在手的未落盘条目"最多 **B** 条
/// （`builder_held_max() ≤ B`）。旧形状这里读到的就是 **N**（`compaction_cost_split` 在
/// 600k 档读到单文件 18 333 条 ≈ 4.9 MB，占那一段峰值 6.3 MB 的 78% —— 就是要拆的东西）。
///
/// 另钉一条"还剩多少、为什么留"：布隆的位图宽度 `m = 条数 × bits_per_key` 要等总条数知道
/// 才能定 ⇒ 每条还留 8 B 哈希（`builder_hashed_max()` 必须正好 = N）。这一小截是**故意**留的，
/// 写死在这里是为了让下次动它的人看见自己动的是什么（要清掉它得让调用方预估条数，
/// 那是另一个取舍：布隆的 m 一改，写出去的位图与现在就不是同一份口径了）。
///
/// 语义一起钉：文件读回来逐条一致、footer 里的 min/max 是第一把与最后一把键。
/// （**格式逐字节不变**由 `test_sst_builder_byteprint_is_stable` 那条 golden 钉着。）
///
/// 变异验证：把 `add` 末尾那句"攒满就 `emit_block`"去掉（回到攒到 finish 才写）⇒ 红在
/// "单块握住 4000 条未落盘条目（块宽 64）"。
#[test]
fn test_builder_emits_blocks_incrementally() {
    const N: usize = 4000;
    const BLOCK: usize = 64;
    let path = tmp("incr_emit");
    reset_builder_held_max();
    reset_builder_hashed_max();
    {
        let mut b = SstBuilder::with_options(3, &path, BLOCK, true);
        for i in 0..N {
            b.add(
                format!("k{i:06}").into_bytes(),
                StoredEntry::put(i as u64, Some(vec![b'v'; 20])),
            );
        }
        assert_eq!(b.len(), N, "`len()` 该是加进去的条数");
        let sst = b.finish().unwrap();
        let back = sst.iter_entries();
        assert_eq!(back.len(), N, "落盘的条目数不对 ⇒ 分块写丢行了");
        assert_eq!(back.first().unwrap().0, b"k000000".to_vec());
        assert_eq!(
            back.last().unwrap().0,
            format!("k{:06}", N - 1).into_bytes()
        );
        assert_eq!(sst.min_key, b"k000000".to_vec(), "footer 的 min_key 不对");
        assert_eq!(
            sst.max_key,
            format!("k{:06}", N - 1).into_bytes(),
            "footer 的 max_key 不对"
        );
    }
    let held = builder_held_max();
    let hashed = builder_hashed_max();
    eprintln!(
        "[incr emit] N={N} 块宽={BLOCK} ⇒ 未落盘条目最多 {held} 条、布隆哈希最多 {hashed} 条"
    );
    assert!(
        held <= BLOCK as u64,
        "单块握住 {held} 条未落盘条目（块宽 {BLOCK}）⇒ 又变回攒到 finish 才写"
    );
    assert_eq!(
        hashed, N as u64,
        "布隆哈希表不是每条一份 ⇒ 位图口径动了（那是格式变更，另说）"
    );
    let _ = std::fs::remove_file(&path);
}
