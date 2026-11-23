// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn binary_key_bitcask() {
    binary_key_regression("bitcask");
}
#[test]
fn binary_key_bplus() {
    binary_key_regression("bplus_tree");
}
#[test]
fn binary_key_lsm() {
    binary_key_regression("lsm_tree");
}
#[test]
fn binary_key_memory() {
    binary_key_regression("memory");
}

#[test]
fn binary_key_views_match_a_logical_map() {
    use std::collections::BTreeMap;
    let mut keys = vec![Vec::new(), vec![b'a', 0, 0, 0, 0, 0, 0, 0, 2]];
    for a in [0, 1, b'a', 255] {
        keys.push(vec![a]);
        for b in [0, 1, b'a', 255] {
            keys.push(vec![a, b]);
            for c in [0, 1, b'a', 255] {
                keys.push(vec![a, b, c]);
            }
        }
    }
    keys.sort();
    for entry in open_all() {
        let (name, db) = (entry.0, &*entry.1);
        db.set_vacuum_auto_slice(0);
        let mut expected = BTreeMap::new();
        for (i, key) in keys.iter().enumerate() {
            let tx = db.begin();
            assert!(tx.set_raw(key, key.clone()));
            expected.insert(key.clone(), key.clone());
            tx.commit();
            if i % 3 == 0 {
                let tx = db.begin();
                assert!(tx.set_raw(key, b"updated".to_vec()));
                expected.insert(key.clone(), b"updated".to_vec());
                tx.commit();
            }
            if i % 5 == 0 {
                let tx = db.begin();
                assert!(tx.delete(key));
                expected.remove(key);
                tx.commit();
            }
        }
        let expected_rows: Vec<_> = expected
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let check = || {
            let tx = db.begin();
            let pairs = |rows: Vec<ExportRecord>| {
                rows.into_iter()
                    .map(|r| (r.key, r.value.unwrap()))
                    .collect::<Vec<_>>()
            };
            assert_eq!(
                pairs(tx.scan(None, None)),
                expected_rows,
                "{name}: full scan"
            );
            for key in &keys {
                assert_eq!(
                    tx.get(key),
                    expected.get(key).cloned(),
                    "{name}: point {key:?}"
                );
                let prev = expected
                    .range(..=key.clone())
                    .next_back()
                    .map(|(k, _)| k.clone());
                assert_eq!(
                    tx.seek_prev(key).map(|r| r.key),
                    prev,
                    "{name}: previous {key:?}"
                );
                let prefix: Vec<_> = expected_rows
                    .iter()
                    .filter(|(k, _)| k.starts_with(key))
                    .cloned()
                    .collect();
                assert_eq!(pairs(tx.prefix_scan(key)), prefix, "{name}: prefix {key:?}");
                let below: Vec<_> = expected_rows
                    .iter()
                    .filter(|(k, _)| k < key)
                    .cloned()
                    .collect();
                assert_eq!(
                    pairs(tx.scan(None, Some(key))),
                    below,
                    "{name}: upper bound {key:?}"
                );
            }
            for limit in [1, 2, 7, 100] {
                let mut cursor = None;
                let mut seen = Vec::new();
                for _ in 0..=keys.len() {
                    let (page, next) = tx.scan_page(cursor.as_deref(), limit);
                    seen.extend(pairs(page));
                    if next.is_none() {
                        break;
                    }
                    assert_ne!(next, cursor, "{name}: cursor stalled");
                    cursor = next;
                }
                assert_eq!(seen, expected_rows, "{name}: page width {limit}");
                let tail = &expected_rows[expected_rows.len().saturating_sub(limit)..];
                assert_eq!(
                    pairs(tx.search_last_page(limit).items),
                    tail,
                    "{name}: last page {limit}"
                );
            }
            tx.commit();
        };
        check();
        let mut cursor = None;
        for round in 0..=keys.len() {
            let result = db.vacuum_bounded(cursor.as_deref(), 1).unwrap();
            if result.next_cursor.is_none() {
                break;
            }
            assert!(round < keys.len(), "{name}: vacuum did not converge");
            assert_ne!(result.next_cursor, cursor);
            cursor = result.next_cursor;
        }
        check();
        db.vacuum().unwrap();
        check();
    }
}

/// 门面层：`set_raw` 必须**一字不差**地把任意字节存进去、取回来（四家各自 + 重开之后）。
///
/// 盯的是四组最容易踩的形状：空值、`0x00` 打头（读回来会少 1 字节）、`0x01` 打头且后 8
/// 字节看着像"已过期的 TTL 信封"（读回来直接变 `None`）、超过 blob 阈值的长值（走旁路）。
/// 同一条里必须留一组**对照**：拿 `set` 存同样的字节确实会丢 —— 没有这组对照，
/// 就无法区分"`set_raw` 真在做事"和"它只是个换个名字的 `set`"。
#[test]
fn set_raw_round_trips_every_byte_pattern() {
    // 一个"看着像已过期 TTL 信封"的裸值：0x01 + 8 字节很早的 LE 时间戳
    let mut fake_expired_ttl = vec![0x01u8];
    fake_expired_ttl.extend_from_slice(&1u64.to_le_bytes());
    fake_expired_ttl.extend_from_slice(b"payload");

    let cases: Vec<(&str, Vec<u8>)> = vec![
        ("空值", Vec::new()),
        ("单个 0x00", vec![0x00]),
        ("0x00 打头", vec![0x00, b'A', b'B', b'C']),
        ("像 TTL 信封的裸值", fake_expired_ttl.clone()),
        ("0x02 打头", vec![0x02, 0x00, 0x01, 0xFF]),
        ("超过 blob 阈值的长值", vec![0xAB; 5000]),
        ("全字节表", (0u16..=255).map(|b| b as u8).collect()),
    ];

    let engines = open_all();
    for (name, db, dir) in engines {
        // 真正的清理交给守卫，这里登记的只是兜底副本
        for (idx, (tag, bytes)) in cases.iter().enumerate() {
            let key = format!("raw{idx}").into_bytes();
            {
                let tx = db.begin();
                assert!(tx.set_raw(&key, bytes.clone()), "{name}: {tag} 写冲突？");
                tx.commit();
            }
            {
                let tx = db.begin();
                assert_eq!(
                    tx.get(&key).as_deref(),
                    Some(bytes.as_slice()),
                    "{name}: {tag} 用 set_raw 存却读不回来（一字不差是这条守卫唯一的目的）"
                );
                tx.commit();
            }
            // 读路径不止 `get`：scan / multi_get 走的是同一套剥壳，必须同样原样
            let tx = db.begin();
            let one_key = vec![key.clone()];
            assert_eq!(
                tx.multi_get(&one_key)[0].as_deref(),
                Some(bytes.as_slice()),
                "{name}: {tag} 在 multi_get 里变了"
            );
            let scanned = tx.scan(Some(&key), Some(&[key[0], b'z' + 1][..]));
            assert!(
                scanned
                    .iter()
                    .any(|r| r.key == key && r.value.as_deref() == Some(bytes.as_slice())),
                "{name}: {tag} 在 scan 里丢了或变了"
            );
            tx.commit();

            // 对照：同一批字节用 `set` 存会怎样（`0x00`/`0x01` 打头的两类必须丢，
            // 其余恰好不丢 —— 这正说明"坑"是按字节值偶发出现的，靠人记规矩记不住）
            if *tag == "0x00 打头" || *tag == "像 TTL 信封的裸值" {
                let bad = format!("bad{idx}").into_bytes();
                {
                    let tx = db.begin();
                    assert!(tx.set(&bad, bytes.clone()));
                    tx.commit();
                }
                let tx = db.begin();
                assert_ne!(
                    tx.get(&bad).as_deref(),
                    Some(bytes.as_slice()),
                    "{name}: `set` 存 {tag} 居然没丢？那这条守卫就白写了（信封约定改了？）"
                );
                tx.commit();
            }
        }

        // 落盘引擎：重开之后仍然一字不差（信封是在存储编码里，不是只在内存里）
        if dir.is_some() {
            db.checkpoint().expect("checkpoint");
            drop(db);
            let reopened = open_engine(name, dir.as_deref()).expect("重开");
            {
                let tx = reopened.begin();
                for (idx, (tag, bytes)) in cases.iter().enumerate() {
                    assert_eq!(
                        tx.get(&format!("raw{idx}").into_bytes()).as_deref(),
                        Some(bytes.as_slice()),
                        "{name}: 重开后 {tag} 变了"
                    );
                }
                tx.commit();
            }
            // 先放句柄，循环末尾那条 scratch 守卫才删得动目录
            drop(reopened);
        }
    }
}
