// 该文件集中验证对应职责，保留原测试名称和字面值。

/// `migrate` 的验收：跨引擎**环**一圈（bitcask → lsm → bplus → memory → bitcask），
/// 每一跳之后"键与用户字节逐条相同、TTL 仍在、`0x00`/`0x01` 打头的裸字节没被吃掉"。
///
/// 为什么绕一圈而不是只跳一次：只跳一次看不出"某一跳丢了 TTL 或改了字节"—— 源头是那家写的，
/// 只有让它再当一次源才暴露。环上每一家都既当源又当目标。
#[test]
fn migrate_walks_a_ring_of_engines_byte_for_byte() {
    let ring = ["bitcask", "lsm_tree", "bplus_tree", "memory"];

    // 四种形状各一条：普通、`0x00` 打头、`0x01` 打头（看着像 TTL 信封）、带 TTL
    let seed: Vec<(Vec<u8>, Vec<u8>, Option<u64>)> = vec![
        (b"plain".to_vec(), b"hello".to_vec(), None),
        (b"lead00".to_vec(), vec![0x00, 0x01, 0xFE, 0x7F], None),
        (
            b"lead01".to_vec(),
            {
                let mut v = vec![0x01u8];
                v.extend_from_slice(&9_999u64.to_le_bytes());
                v.extend_from_slice(b"not-a-ttl");
                v
            },
            None,
        ),
        (b"ttl".to_vec(), b"expires-later".to_vec(), Some(3600)),
    ];

    // 守卫比 `prev`/`base` 先声明 ⇒ 后析构（反向声明序）：环上最后一家的引擎放掉之后
    // 才轮到删目录。这里不能像别的用例那样"守卫活到迭代末" —— 每一跳都要把上一家的库
    // **再当一次源**读，迭代末删掉会让它读不到自己的日志。
    let mut guards: Vec<crate::common::ScratchPath> = Vec::new();
    let mut base: Option<Vec<(Vec<u8>, Option<Vec<u8>>)>> = None;
    let mut prev: Option<Box<dyn KvEngine>> = None;
    for (idx, name) in ring.iter().enumerate() {
        let dir = tmp_dir("migrate");
        let path = if *name == "memory" {
            None
        } else if *name == "bplus_tree" {
            let _ = std::fs::create_dir_all(&dir);
            Some(dir.join("bplus.db"))
        } else {
            Some(dir.to_path_buf())
        };
        guards.push(dir);
        let db = open_engine(*name, path.as_deref()).expect("开目标引擎");
        if let Some(src) = prev.take() {
            let stats = migrate(&*src, &*db, 2).expect("migrate 失败");
            assert_eq!(stats.rows, seed.len() as u64, "{name}: 搬过去的 key 数");
            assert_eq!(stats.with_ttl, 1, "{name}: 带 TTL 的行数");
            // page_rows = 2 < 4 行 ⇒ 必须分多页（一页走完说明游标没在用）
            assert!(stats.pages >= 2, "{name}: 页数 {stats:?}");
        } else {
            let tx = db.begin();
            for (k, v, ttl) in &seed {
                assert!(
                    match ttl {
                        Some(secs) => {
                            tx.set_with_ttl(k, v.clone(), Duration::from_secs(*secs))
                        }
                        None => tx.set_raw(k, v.clone()),
                    },
                    "播种 {k:?} 失败"
                );
            }
            tx.commit();
        }
        let got: Vec<(Vec<u8>, Option<Vec<u8>>)> = {
            let tx = db.begin();
            let rows = tx.export_latest_visible(false);
            let with_ttl = tx.get_ttl(b"ttl").is_some_and(|o| o.is_some());
            let lead = tx.get(b"lead00").unwrap();
            tx.commit();
            assert!(with_ttl, "{name}: TTL 必须跟着搬过来");
            assert_eq!(lead, seed[1].1, "{name}: 0x00 打头的裸字节被改了");
            rows.into_iter().map(|r| (r.key, r.value)).collect()
        };
        match &base {
            Some(want) => assert_eq!(&got, want, "{name}: 第 {idx} 跳的内容与源头不一致"),
            None => base = Some(got),
        }
        prev = Some(db);
    }
    // 最后一家的引擎还在 `prev` 里；先放掉它，守卫才删得动
    drop(prev.take());
}
