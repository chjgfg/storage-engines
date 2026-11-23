// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn record_batch_keeps_legacy_crc_and_io_controls() {
    use super::*;
    for legacy in [true, false] {
        let mode = (!legacy).then(io_reference::Scope::start);
        let scope = snapshot_probe::Scope::start(legacy);
        let path = crate::common::ScratchPath::new("snapshot", "record_controls", ".bxp");
        atomic_write_bin_v4(&path, |w| {
            assert_eq!(
                w.write_snapshot_batched(b"k", Some(b"v"), None)?,
                RecordEncoding::DirectReference
            );
            Ok(())
        })
        .unwrap();
        assert_eq!(scope.stats().record_buffer_bytes, 0);
        assert_eq!(scope.stats().record_reference_rows, 1);
        assert_eq!(
            read_bin(&path).unwrap().0,
            vec![KvRecord::new(b"k".to_vec(), Some(b"v".to_vec()))]
        );
        drop(scope);
        drop(mode);
    }
}

#[test]
fn test_b64_known() {
    assert_eq!(b64_encode(b""), "");
    assert_eq!(b64_encode(b"f"), "Zg==");
    assert_eq!(b64_encode(b"fo"), "Zm8=");
    assert_eq!(b64_encode(b"foo"), "Zm9v");
    assert_eq!(b64_decode("Zm9v").unwrap(), b"foo");
}

/// M45 的第一条守卫：**每一个长度**都要往返一致，不是"挑几个长度"。
///
/// 为什么必须是全长度扫：`b64_decode` 原先在尾组之后又按 `=` 的个数**回头裁一次**
/// （`=` 早在 `filter` 里剥掉了 ⇒ 二次修正），于是要么全对要么按 `len % 3` 分成三档：
/// `len%3==0` 恰好没事，`==2` 少 1 字节，`==1` 少 2 字节。而旧的那条 `test_b64_known`
/// 唯一的解码断言是长度 3 —— 正好是唯一能过的那一格，所以这个 bug 一路活到今天。
/// 抽样式断言在这种"按余数分档"的编码上等于没测。
#[test]
fn test_b64_round_trips_every_length() {
    for len in 0usize..=64 {
        let raw: Vec<u8> = (0..len).map(|i| b'a' + (i % 26) as u8).collect();
        let enc = b64_encode(&raw);
        let back =
            b64_decode(&enc).unwrap_or_else(|e| panic!("长度 {len} 往返失败: {e}（enc={enc}）"));
        assert_eq!(
            back,
            raw,
            "长度 {len} 解回 {} 字节，差 {}（enc={enc}）⇒ 尾组又被裁了一次？",
            back.len(),
            raw.len() as isize - back.len() as isize
        );
    }
    // 事故形状那一格单独点名：11 字节的键 `demo:000001` 曾被截成 10 字节 `demo:00000`
    let key = b"demo:000001";
    assert_eq!(key.len(), 11, "这条点名要求键长 11（11 % 3 == 2 那一档）");
    assert_eq!(b64_decode(&b64_encode(key)).unwrap().as_slice(), key);
}

/// 尾组的"非法长度"那一支也得还在：4k+1 个字符不是合法的 base64。
/// 钉它是为了防"为了让往返一致而把长度检查一并删掉"这种修法。
#[test]
fn test_b64_decode_rejects_illegal_length() {
    assert!(b64_decode("A").is_err(), "1 个字符不是合法的 base64 尾组");
    assert!(
        b64_decode("AAAAA").is_err(),
        "5 个字符（4+1）不是合法的 base64"
    );
    // 合法尾组两种形状各自给出正确字节数
    assert_eq!(b64_decode("Zg==").unwrap(), b"f", "2 字符尾组 = 1 字节");
    assert_eq!(b64_decode("Zm8=").unwrap(), b"fo", "3 字符尾组 = 2 字节");
}

/// M45 的第二条守卫：JSONL 往返回来**键还是一把一把不同的**（事故形状）。
///
/// 为什么这条不能只靠上面那条长度扫：长度扫描证的是"编解码本身"，而现场的损失是
/// **两个错合成一个对** —— 300 把键各少一个字节 ⇒ 全都变成同一把 ⇒ 后写覆盖前写，
/// 库里只剩一把。只有"灌 N 把互不相同的键、读回来还是 N 把、每把值还对"这种断言
/// 才抓得住"塌键"这一类，而不是"截字节"本身。
#[test]
fn test_jsonl_round_trip_does_not_collapse_keys() {
    const N: usize = 300;
    let path = std::env::temp_dir().join(format!("kv_snap_jsonl_{}.jsonl", std::process::id()));
    let recs: Vec<KvRecord> = (0..N)
        .map(|i| {
            KvRecord::new(
                format!("demo:{i:06}").into_bytes(), // 11 字节 ⇒ 正是 11 % 3 == 2 那一档
                Some(format!("val-{i}").into_bytes()),
            )
        })
        .collect();
    write_jsonl(&path, &recs).unwrap();
    let (back, st) = read_jsonl(&path).unwrap();
    let _ = std::fs::remove_file(&path);

    assert_eq!(st.records, N, "写出去的行数不对");
    assert_eq!(back.len(), N, "读回来的行数不对");
    let distinct: std::collections::HashSet<&[u8]> =
        back.iter().map(|r| r.key.as_slice()).collect();
    assert_eq!(
        distinct.len(),
        N,
        "读回来只剩 {} 把不同的键 ⇒ 键被截短、互相覆盖了",
        distinct.len()
    );
    for r in &back {
        let i: usize = std::str::from_utf8(&r.key[5..])
            .expect("键的序号部分应是 ASCII 数字")
            .parse()
            .expect("键的序号部分应能解析成整数");
        assert_eq!(
            r.value.as_deref(),
            Some(format!("val-{i}").as_bytes()),
            "键 {:?} 的值读回来不是它自己那份",
            String::from_utf8_lossy(&r.key)
        );
    }
}

/// 逐行写口（M44）与整批写的**字节必须一字不差**：导出文件的格式是跨引擎的合同
/// （`import_kv` 要能读回来，旧版工具也要能读 `BPEXP001`），而 CLI 从"整批"换成了"逐行"。
///
/// ⚠️ 这一条与 `test_plain_is_bpexp001` 那份 golden 是两件事：那份钉"编码本身"，
/// 这一份钉"换写法没换格式"。两条都要 —— 只留后者的话，编码哪天动了它俩还互相对齐。
///
/// ⚠️ 没有覆盖的一支：`ExportWriter::push` 在写失败之后一律收手、错误由 `finish()` 抛。
/// 那一支要真把盘弄坏才触发（Windows 上还留着句柄的文件删不掉），恒真的模拟测试不给信息，
/// 所以留注释不留测试。
#[test]
fn test_streaming_sink_writes_identical_bytes() {
    let pid = std::process::id();
    let streamed = std::env::temp_dir().join(format!("kv_snap_stream_{pid}.bin"));
    let batched = std::env::temp_dir().join(format!("kv_snap_batch_{pid}.bin"));
    let jsonl_streamed = std::env::temp_dir().join(format!("kv_snap_stream_{pid}.jsonl"));
    let jsonl_batched = std::env::temp_dir().join(format!("kv_snap_batch_{pid}.jsonl"));
    let recs = vec![
        KvRecord::new(b"alpha".to_vec(), Some(b"1".to_vec())),
        KvRecord::new(b"beta".to_vec(), None),
        KvRecord::with_seq(b"gamma".to_vec(), Some(b"3".to_vec()), 7),
    ];

    // bin：不带 seq 的一支（CLI 走的就是这条 —— 导出的行里没有 seq）
    let plain = vec![
        KvRecord::new(b"alpha".to_vec(), Some(b"1".to_vec())),
        KvRecord::new(b"beta".to_vec(), None),
    ];
    let mut w = BinWriter::create(&streamed, false).unwrap();
    let mut handed = 0usize;
    for r in &plain {
        w.write(r).unwrap();
        handed += 1;
    }
    let st_stream = w.finish().unwrap();
    let st_batch = write_bin(&batched, &plain).unwrap();
    assert_eq!(
        std::fs::read(&streamed).unwrap(),
        std::fs::read(&batched).unwrap(),
        "逐行写的 bin 与整批写不是同一份字节"
    );
    assert_eq!(handed, 2);
    assert_eq!(st_stream.records, st_batch.records);
    assert_eq!(st_stream.live, st_batch.live);
    assert_eq!(st_stream.deleted, st_batch.deleted);

    // jsonl：同一批记录
    let mut w = JsonlWriter::create(&jsonl_streamed).unwrap();
    for r in &recs {
        w.write(r).unwrap();
    }
    w.finish().unwrap();
    write_jsonl(&jsonl_batched, &recs).unwrap();
    assert_eq!(
        std::fs::read(&jsonl_streamed).unwrap(),
        std::fs::read(&jsonl_batched).unwrap(),
        "逐行写的 jsonl 与整批写不是同一份字节"
    );

    // `ExportWriter`（CLI 用的那层胶）也必须给出同一份字节
    let glued = std::env::temp_dir().join(format!("kv_snap_glue_{pid}.bin"));
    {
        let mut g = ExportWriter::bin(&glued, false).unwrap();
        for r in &plain {
            assert!(
                g.push(ExportRecord::new(r.key.clone(), r.value.clone())),
                "逐行推送中途被拒"
            );
        }
        g.finish().unwrap();
    }
    assert_eq!(
        std::fs::read(&glued).unwrap(),
        std::fs::read(&batched).unwrap(),
        "ExportWriter 与整批写不是同一份字节"
    );

    for p in [&streamed, &batched, &jsonl_streamed, &jsonl_batched, &glued] {
        let _ = std::fs::remove_file(p);
    }
}

#[test]
fn test_jsonl_roundtrip() {
    let path = std::env::temp_dir().join(format!("kv_snap_{}.jsonl", std::process::id()));
    let recs = vec![
        KvRecord::new(b"k".to_vec(), Some(b"v".to_vec())),
        KvRecord::new(b"d".to_vec(), None),
    ];
    write_jsonl(&path, &recs).unwrap();
    let (back, st) = read_jsonl(&path).unwrap();
    assert_eq!(st.live, 1);
    assert_eq!(st.deleted, 1);
    assert_eq!(back, recs);
    let _ = std::fs::remove_file(&path);
}
