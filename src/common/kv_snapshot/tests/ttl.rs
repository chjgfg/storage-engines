// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// M81 第 6 项：三版 bin 都要读得回来，而 expire 只有 003 装得下。四条判据各挡一种"看着像过了"：
/// ① 001 加了字段之后没被顺手改形（读回一律 `None`，不是猜出个 0）；
/// ② 003 往返保住**绝对**到期时刻，且永久行读回 `None` 而不是 `Some(0)`；
/// ③ 读侧只认魔数、不照内容猜形状 ⇒ 头改坏必须报错；
/// ④ JSONL 的 `expire` 是**可选**字段：旧行缺它 = 永久（不报错），有它但值坏了 = 必须报错
///    （静默丢掉一个 TTL，就是让那把键活得比它该有的样子久）。
#[test]
fn bin_v3_carries_expire_and_all_three_versions_read_back() {
    let dir = crate::common::ScratchPath::new("kv_snapshot", "v3", "");
    std::fs::create_dir_all(&dir).unwrap();
    let recs = vec![
        KvRecord::new(b"k1".to_vec(), Some(b"v1".to_vec())),
        KvRecord::new(b"k2".to_vec(), None),
        KvRecord::new(b"k3".to_vec(), Some(b"v3".to_vec())).with_expire(1_900_000_000),
    ];
    let p1 = dir.join("a.bxp");
    let p2 = dir.join("b.bxp");
    let p3 = dir.join("c.bxp");
    write_bin(&p1, &recs).unwrap();
    write_bin(
        &p2,
        &[KvRecord::with_seq(b"s".to_vec(), Some(b"1".to_vec()), 7)],
    )
    .unwrap();
    write_bin_v3(&p3, &recs).unwrap();

    let (back1, st1) = read_bin(&p1).unwrap();
    assert_eq!(st1.records, 3);
    assert!(
        back1.iter().all(|r| r.expire_unix_secs.is_none()),
        "001 里没有放 expire 的地方，读回来必须全是 None"
    );
    let (back2, _) = read_bin(&p2).unwrap();
    assert_eq!(back2[0].seq, Some(7), "002 的 seq 不能被读丢");
    let (back3, st3) = read_bin(&p3).unwrap();
    assert_eq!(st3.records, 3);
    assert_eq!(
        back3[2].expire_unix_secs,
        Some(1_900_000_000),
        "003 的绝对到期时刻要原样回来"
    );
    assert_eq!(
        back3[0].expire_unix_secs, None,
        "永久行读回 None，不是 Some(0)"
    );
    assert_eq!(back3[1].value, None, "墓碑在 003 里还是墓碑");

    let mut bad = std::fs::read(&p3).unwrap();
    bad[5] = b'9'; // BPEXP003 → BPEXP903：不认识的版号
    let pbad = dir.join("bad.bxp");
    std::fs::write(&pbad, &bad).unwrap();
    assert!(
        read_bin(&pbad).is_err(),
        "魔数不认识就必须报错，不许照内容猜形状"
    );

    let pj = dir.join("a.jsonl");
    write_jsonl(&pj, &recs).unwrap();
    let (j, _) = read_jsonl(&pj).unwrap();
    assert_eq!(j[2].expire_unix_secs, Some(1_900_000_000));
    assert_eq!(j[0].expire_unix_secs, None);
    let pold = dir.join("old.jsonl");
    std::fs::write(&pold, "{\"key\":\"a2V5\",\"value\":\"dg==\"}\n").unwrap();
    let (o, _) = read_jsonl(&pold).unwrap();
    assert_eq!(o[0].key, b"key");
    assert_eq!(o[0].value.as_deref(), Some(b"v".as_ref()));
    assert_eq!(
        o[0].expire_unix_secs, None,
        "旧行缺 expire 字段 = 永久，不是报错"
    );
    let pbroken = dir.join("broken.jsonl");
    std::fs::write(
        &pbroken,
        "{\"key\":\"a2V5\",\"value\":\"dg==\",\"expire\":12x}\n",
    )
    .unwrap();
    assert!(
        read_jsonl(&pbroken).is_err(),
        "expire 有字段但值坏了 ⇒ 必须报错"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
