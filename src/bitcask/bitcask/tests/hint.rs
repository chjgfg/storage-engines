// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
fn test_stale_hint_crosses_rotations_without_losing_deletes() {
    let path = tmp_log("hint_rotations");
    let stale;
    {
        let mut eng = Bitcask::new(path.to_path_buf()).unwrap();
        eng.set(b"a", vec![1; 32]).unwrap();
        eng.set(b"b", vec![2; 32]).unwrap();
        eng.flush().unwrap();
        eng.dump_hint().unwrap();
        stale = std::fs::read(eng.hint_path()).unwrap();
        eng.set(b"a", vec![3; 32]).unwrap();
        eng.rotate().unwrap();
        eng.delete(b"b").unwrap();
        eng.insert(b"tomb".to_vec(), None).unwrap();
        eng.rotate().unwrap();
        eng.set(b"c", vec![4; 256]).unwrap();
        eng.flush().unwrap();
    }
    // 恢复轮转前的原始 hint；正常 Drop 会重写提示文件并掩盖这个场景。
    std::fs::write(hint_path_for(&path), stale).unwrap();
    {
        let (mut kd, id, offset) = read_hint_file(&hint_path_for(&path)).unwrap();
        let mut active = Log::new(path.to_path_buf()).unwrap();
        assert_eq!(
            Bitcask::replay_hint(
                &path,
                &discover_sealed_ids(&path),
                &mut kd,
                id,
                offset,
                &mut active
            )
            .unwrap(),
            HintReplay::Rotated {
                active_id: 2,
                sealed_segments: 2
            }
        );
    }
    let mut eng = Bitcask::new(path.to_path_buf()).unwrap();
    assert_eq!(eng.get(b"a").unwrap(), Some(vec![3; 32]));
    assert_eq!(eng.get(b"b").unwrap(), None);
    assert_eq!(eng.get(b"c").unwrap(), Some(vec![4; 256]));
    assert!(eng.key_dir.contains_key(b"tomb".as_slice()));
    assert_eq!(eng.len(), 3);
}

#[test]
fn test_corrupt_hint_falls_back() {
    let path = tmp_log("bad");
    {
        let mut eng = Bitcask::new(path.to_path_buf()).unwrap();
        eng.set(b"x", b"y".to_vec()).unwrap();
        eng.flush().unwrap();
        eng.dump_hint().unwrap();
    }
    // 写坏 hint
    std::fs::write(hint_path_for(&path), b"GARBAGE!!").unwrap();
    {
        let mut eng = Bitcask::new(path.to_path_buf()).unwrap();
        assert_eq!(eng.get(b"x").unwrap(), Some(b"y".to_vec()));
    }
    cleanup(&path);
}

#[test]
fn test_soft_tombstone_in_hint() {
    let path = tmp_log("soft");
    {
        let mut eng = Bitcask::new(path.to_path_buf()).unwrap();
        eng.insert(b"vkey".to_vec(), Some(b"val".to_vec())).unwrap();
        eng.insert(b"tomb".to_vec(), None).unwrap(); // 软 tomb
        eng.flush().unwrap();
        eng.dump_hint().unwrap();
        assert_eq!(eng.len(), 2);
        assert_eq!(eng.get_entry(b"tomb").unwrap(), Some(None));
    }
    {
        let mut eng = Bitcask::new(path.to_path_buf()).unwrap();
        assert_eq!(eng.get(b"vkey").unwrap(), Some(b"val".to_vec()));
        assert_eq!(eng.get_entry(b"tomb").unwrap(), Some(None));
        assert_eq!(eng.len(), 2);
    }
    cleanup(&path);
}

/// M77 的守卫：hint 门"一次预留多少条"这条界必须由**文件字节数**说了算，不是头部那个声明值。
///
/// 判据落在"没有这条界就不成立"的一侧：把声明值改成 `u64::MAX`，界仍然只能是"文件装得下的条数"
/// —— 否则就是"读到一个大数 ⇒ 先做一次巨大分配"，那正是 M60 立"不可信声明值一律分步预留"要挡的形状。
/// 另一侧也要钉：声明值在界内时**照声明值**（一次到位才省掉 realloc —— 省掉的那笔实测 284.2 → 173.5 ms），
/// 只钉"别太大"而不钉"别太小"会退化成"永远不分步也永远不省"。
///
/// 五格：① 界内照声明；② 声明超界 ⇒ 夹到界；③ 文件比头部还短 ⇒ 0（`saturating_sub`，不许下溢）；
/// ④ **全长度扫** `claimed = 0..=64`（M45 那条：抽样断言等于没测）；
/// ⑤ 真文件三份（v2 长键 / v1 长键 / **零长键**）各自 `min(声明, 文件容量) == 真实条数`
///   —— 零长键是**每条占得最少**的情形，也就是这条界最松的一格，它要是还能精确，别的情形只会更保守
///   （保守 = 少留 = 顶多多一次 realloc，不会超发内存）。
#[test]
fn hint_reserve_bound_is_the_file_not_the_header_claim() {
    let bound = |file: u64, header: usize, rec: u64, claimed: u64| {
        hint_reserve_bound(file, header, rec, claimed)
    };
    // ① 声明值在界内 ⇒ 照声明值（一次到位）
    assert_eq!(bound(40 + 24 * 1000, 40, 24, 1_000), 1_000);
    // ② 声明值吹到天上 ⇒ 夹到"文件物理上装得下的条数"
    assert_eq!(bound(40 + 24 * 1000, 40, 24, u64::MAX), 1_000);
    assert_eq!(bound(40 + 24 * 1000, 40, 24, 1 << 40), 1_000);
    // ③ 空正文 / 文件比头部还短 ⇒ 0，不许下溢成天文数字
    assert_eq!(bound(40, 40, 24, 10), 0);
    assert_eq!(bound(39, 40, 24, 10), 0);
    assert_eq!(bound(0, 40, 24, 10), 0);
    // ④ 全长度扫：正文只装得下 24 条 ⇒ 界 = `min(claimed, 24)`，一格都不许差
    for claimed in 0u64..=64 {
        assert_eq!(
            bound(40 + 24 * 24, 40, 24, claimed),
            (claimed as usize).min(24),
            "claimed={claimed} 这一格界算错了"
        );
    }
    // ⑤ 真文件：三种形状都要"一次到位"（界 == 真实条数）
    //   `(v2, 键长)`：v2 每条 24+键长、v1 每条 20+键长；**零长键是每条占得最少的情形**，
    //   也就是这条界最松的一格 —— 它还能精确，别的情形只会更保守（保守 = 少留 = 顶多多一次 realloc，
    //   不会超发内存）。
    let n = 500usize;
    for (v2, key_len) in [(true, 16usize), (false, 16), (true, 0), (false, 0)] {
        let path = tmp_log("hint_bound");
        let hint = hint_path_for(&path);
        let rows: Vec<(Vec<u8>, i32, u32, u64, u32)> = (0..n)
            .map(|i| {
                let mut k = vec![0u8; key_len];
                if key_len >= 8 {
                    k[..8].copy_from_slice(&(i as u64).to_be_bytes());
                }
                (k, HINT_FLAG_LIVE, 0u32, i as u64, 1u32)
            })
            .collect();
        // 零长键全同 ⇒ 树里只会有一条，`write_hint_file` 那条路要的是"树的条数"；
        // 这里走手工搭文件那条路，条数就是写进去的记录数
        let rows = if key_len == 0 {
            vec![(Vec::new(), HINT_FLAG_LIVE, 0u32, 0u64, 1u32)]
        } else {
            rows
        };
        build_hint_file(&hint, v2, &rows, None);
        let file_bytes = std::fs::metadata(&hint).unwrap().len();
        let (header, rec) = if v2 {
            (HINT_HEADER_SIZE_V2, HINT_RECORD_FIXED_BYTES_V2)
        } else {
            (HINT_HEADER_SIZE_V1, HINT_RECORD_FIXED_BYTES_V1)
        };
        // 自证分母：文件字节 = 头部 + 条数 × (定长 + 键长)，量具与文件对不上就说明搭错了
        assert_eq!(
            file_bytes as usize,
            header + rows.len() * (rec as usize + key_len),
            "v2={v2} 键长={key_len}：搭出来的 hint 形状与常数对不上"
        );
        assert_eq!(
            bound(file_bytes, header, rec, rows.len() as u64),
            rows.len(),
            "v2={v2} 键长={key_len}：界应当正好 = 真实条数（少一格就多一次 realloc），文件 {file_bytes} B"
        );
        // 真门读一遍必须**读得回来**（界只是预留，不许改变能不能读）
        hint_reserve_reset();
        let (got, _, _) = read_hint_file(&hint).expect("真 hint 要读得回来");
        assert_eq!(
            got.len(),
            rows.len(),
            "v2={v2} 键长={key_len}：读回来的条数不对"
        );
        // ⚠️ 这一条才是"门真的用了这条界"的证据（上面那些只证明**函数**算得对）
        assert_eq!(
            hint_reserve_noted(),
            rows.len(),
            "v2={v2} 键长={key_len}：门给自己预留了 {} 条，而界算出来是 {} 条 ⇒ 预留没接上这条界",
            hint_reserve_noted(),
            rows.len()
        );
        drop(got);
        let _ = std::fs::remove_file(&hint);
        let _ = std::fs::remove_file(&path);
    }

    check_oversized_hint(false);
}

#[test]
#[ignore = "process allocator delta; run alone with --test-threads=1"]
fn hint_rejected_file_heap_growth() {
    check_oversized_hint(true);
}

/// M48 的**归因**台架（只量不改）：大库台架的分段读数把 600k 档冷重开的 320 ms 里
/// 307.9 ms 钉在了 `read_hint_file` 这一格，但那一格里面叠着**三件**事：
/// ① 逐字段解记录（五次 `read_exact` + 一次带长度的读）、② 每条键一次 `vec![0u8; key_len]`、
/// ③ 每折一条就往 `KeyDir`（**`BTreeMap<Vec<u8>, _>`**，不是哈希表）插一次。
///
/// 为什么不能报一句"hint 反序列化 308 ms"就收：那一句既没说是**分配**还是**树**，
/// 也没说单价随规模怎么涨 —— 而这两条决定下一步动哪里。树那一支尤其有理由怀疑：
/// 键是变长字节串（比较是 `memcmp`），hint 里的键又正好是**升序**写出的
/// ⇒ 每次插入都要沿最右那一路走下去。三臂读**同一个文件**、只差②③做不做。
///
/// ```text
/// HINT_LOAD_ROWS=600000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 bitcask_hint_load_stage_split
/// ```
/// ⚠️ 单线程跑；每一臂跑两遍取**第二遍**（第一遍要把文件读进 OS 缓存）。
#[test]
#[ignore = "hint 反序列化的三臂拆分，手动 release 单线程跑：HINT_LOAD_ROWS=600000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 bitcask_hint_load_stage_split"]
fn bitcask_hint_load_stage_split() {
    let rows: usize = std::env::var("HINT_LOAD_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200_000);
    let path = tmp_log("hint_split");
    let hint = hint_path_for(&path);

    // 造一份与真库同形状的 hint：16 字节的键（MVCC 的编码键 = raw(8) ‖ version(8)，同宽）、
    // 按键升序（`write_hint_file` 走的就是排好序的 KeyDir，所以台架和现场同一个顺序）。
    let mut kd = KeyDir::new();
    for i in 0..rows {
        let mut k = vec![0u8; 16];
        k[..8].copy_from_slice(&(i as u64).to_be_bytes());
        k[8..].copy_from_slice(&(i as u64 + 1).to_le_bytes());
        kd.insert(
            k,
            KeyLoc::Live {
                file_id: 0,
                pos: i as u64 * 64,
                len: 24,
            },
        );
    }
    write_hint_file(&hint, &kd, 0, 1).expect("写 hint");
    let bytes = std::fs::metadata(&hint).map(|m| m.len()).unwrap_or(0);
    drop(kd);

    // 一把读法，三臂共用：`on_entry` 决定"这条键要不要留下来、留下来放哪儿"。
    // scratch 是**复用**的 ⇒ 臂 B 里每键零分配（这才把②单独摘出来）。返回条目数。
    fn walk(hint: &std::path::Path, mut on_entry: impl FnMut(&[u8], i32, u32, u64, u32)) -> usize {
        use std::io::{BufReader, Read};
        let f = std::fs::File::open(hint).unwrap();
        let mut r = BufReader::new(f);
        let mut magic = [0u8; 8];
        r.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, HINT_MAGIC_V2, "台架只量 v2 那份格式");
        let mut rest = [0u8; HINT_HEADER_SIZE_V2 - 8];
        r.read_exact(&mut rest).unwrap();
        let entry_count = u64::from_le_bytes(rest[16..24].try_into().unwrap()) as usize;
        let mut nbuf = [0u8; 4];
        let mut pbuf = [0u8; 8];
        let mut scratch: Vec<u8> = Vec::new();
        for _ in 0..entry_count {
            r.read_exact(&mut nbuf).unwrap();
            let key_len = u32::from_le_bytes(nbuf) as usize;
            r.read_exact(&mut nbuf).unwrap();
            let flag = i32::from_le_bytes(nbuf);
            r.read_exact(&mut nbuf).unwrap();
            let file_id = u32::from_le_bytes(nbuf);
            r.read_exact(&mut pbuf).unwrap();
            let pos = u64::from_le_bytes(pbuf);
            r.read_exact(&mut nbuf).unwrap();
            let len = u32::from_le_bytes(nbuf);
            scratch.resize(key_len, 0);
            r.read_exact(&mut scratch).unwrap();
            on_entry(&scratch, flag, file_id, pos, len);
        }
        // ⚠️ 这条不是走形式：循环体一次都没进的话，"读全了"和"一条没读"都报 0 ms
        assert_eq!(
            entry_count,
            rows_hinted(hint),
            "hint 里的条目数与文件大小对不上"
        );
        entry_count
    }
    /// 独立算一遍分母：(文件字节 − 头部) ÷ 每条 (24 B 定长字段 + 16 B 键)
    fn rows_hinted(hint: &std::path::Path) -> usize {
        let bytes = std::fs::metadata(hint).map(|m| m.len()).unwrap_or(0) as usize;
        (bytes - HINT_HEADER_SIZE_V2) / (24 + 16)
    }

    let ms = |d: std::time::Duration| d.as_secs_f64() * 1e3;
    let per = |d: std::time::Duration| d.as_secs_f64() * 1e6 / rows as f64;
    let mut b_best = std::time::Duration::MAX;
    let mut c_best = std::time::Duration::MAX;
    let mut a_best = std::time::Duration::MAX;
    let mut d_best = std::time::Duration::MAX;
    let mut e_best = std::time::Duration::MAX;
    let mut a_live = 0i64;
    let mut e_live = 0i64;
    let mut a_over = 0i64;
    let mut e_over = 0i64;
    let mut d_over = 0i64;
    // M59（#67 的归因臂）：F = 精确预留那份清单，G = 什么都不预留（靠翻倍扩容）
    let mut f_best = std::time::Duration::MAX;
    let mut g_best = std::time::Duration::MAX;
    let mut f_s1 = 0i64; // 清单建好时的净活字节
    let mut f_s2 = 0i64; // 树建好时的净活字节
    let mut f_over = 0i64;
    let mut g_s1 = 0i64;
    let mut g_s2 = 0i64;
    let mut g_over = 0i64;
    let mut seen = 0usize;

    // ⚠️ **四轮、每轮两种顺序交替、每臂取最小**（M52 补的方法，理由有两条，都是本机踩过的）：
    // ① 顺序偏差：五臂排在**最后**那一臂会替前面几臂的分配器碎片状态买单 —— 第一版只跑两轮
    //    固定顺序，E 臂读到 557 ms，而同一个形状在 M48 那一轮的 A 臂是 270~320 ms（1.7~2.1×），
    //    这个差跟"逐条插树"没关系，纯粹是排位。② **刚编译完那一轮不读数**（本机 1.3~1.9×）：
    //    这一趟就是紧接着 `cargo test --release` 编译起来的第一轮。取最小 = 只信最快那一轮。
    macro_rules! arm_b {
        () => {{
            let t = std::time::Instant::now();
            seen = walk(&hint, |_k, _f, _id, _p, _l| {});
            let d = t.elapsed();
            if d < b_best {
                b_best = d;
            }
        }};
    }
    macro_rules! arm_c {
        () => {{
            let mut out: Vec<(Vec<u8>, KeyLoc)> = Vec::with_capacity(rows);
            let t = std::time::Instant::now();
            walk(&hint, |k, _f, id, p, l| {
                out.push((
                    k.to_vec(),
                    KeyLoc::Live {
                        file_id: id,
                        pos: p,
                        len: l,
                    },
                ));
            });
            let d = t.elapsed();
            assert_eq!(out.len(), rows, "臂 C 少折了条目");
            drop(out);
            if d < c_best {
                c_best = d;
            }
        }};
    }
    // ── 臂 A：真门 —— `read_hint_file`（M52 之后：流式解码 + 有序 `collect` 成树）──
    // ⚠️ 顺手量两件事，都不靠墙钟：
    // ① **树建好之后还活着多少字节**（`a_live`/`e_live`）：大库台架那一格读到
    //    `冷重开 常驻 83.4 → 57.7 MB`，而两臂解出来的键数一模一样 ⇒ 差的不可能是内容，
    //    只能是形状（升序逐条 `insert` 会把每个叶/内部节点劈成半满，有序批量填则填满）。
    // ② **建的过程中超出常驻多少**（`a_over`/`e_over`）：这一格是为了回答"流式解码到底有没有
    //    把中间那份 `Vec` 省掉"—— 如果 `BTreeMap::from_iter` 自己先把项攒下来，
    //    换不换写法峰值一样，那就该选简单的那一份写法（M52c 靠这一格定去留）。
    macro_rules! arm_a {
        () => {{
            let l0 = crate::mem_probe::live() as i64;
            let pbase = crate::mem_probe::reset_peak();
            let t = std::time::Instant::now();
            let (got, _, _) = read_hint_file(&hint).unwrap();
            let d = t.elapsed();
            let alive = crate::mem_probe::live() as i64 - l0;
            let over = crate::mem_probe::peak().saturating_sub(pbase) as i64 - alive;
            assert_eq!(got.len(), rows, "臂 A 少折了条目");
            drop(got);
            if d < a_best {
                a_best = d;
                a_live = alive;
                a_over = over;
            }
        }};
    }
    // ── 臂 D：同样解 + 分配，但**先攒进有序的 `Vec`，再一次 `collect` 成树**──
    // 为什么加这一臂：hint 里的键本来就是升序写出的，而 ③ 那 74% 到底是
    // "插一棵 BTreeMap  unavoidable"，还是"逐条 insert 每次都要从根走一遍"，
    // 只有把这两件事拆开才答得了 —— 这一臂就是 M52 实际采用的形状。
    macro_rules! arm_d {
        () => {{
            let t0 = std::time::Instant::now();
            let mut acc: Vec<(Vec<u8>, KeyLoc)> = Vec::with_capacity(rows);
            walk(&hint, |k, _f, id, p, l| {
                acc.push((
                    k.to_vec(),
                    KeyLoc::Live {
                        file_id: id,
                        pos: p,
                        len: l,
                    },
                ));
            });
            let l0 = crate::mem_probe::live() as i64;
            let pbase = crate::mem_probe::reset_peak();
            let built: KeyDir = acc.into_iter().collect();
            let d = t0.elapsed();
            let alive = crate::mem_probe::live() as i64 - l0;
            let over = crate::mem_probe::peak().saturating_sub(pbase) as i64 - alive;
            assert_eq!(built.len(), rows, "臂 D 少折了条目");
            drop(built);
            if d < d_best {
                d_best = d;
                d_over = over;
            }
        }};
    }
    // ── 臂 E：M52 **改前**的真门形状（解 + 每键分配 + 逐条 `insert` 进 `BTreeMap`）──
    // 为什么留这一臂：M52 把 A 换成了 D 那个形状，于是"改前的 A"在这个二进制里再也读不到，
    // 只能拿 E 复现它。A/E 必须在**同一份构建、同一个文件**上比（跨构建的 µs 在本机漂 1.3~1.9×）。
    macro_rules! arm_e {
        () => {{
            let l0 = crate::mem_probe::live() as i64;
            let pbase = crate::mem_probe::reset_peak();
            let t = std::time::Instant::now();
            let mut old_shape = KeyDir::new();
            walk(&hint, |k, _f, id, p, l| {
                old_shape.insert(
                    k.to_vec(),
                    KeyLoc::Live {
                        file_id: id,
                        pos: p,
                        len: l,
                    },
                );
            });
            let d = t.elapsed();
            let alive = crate::mem_probe::live() as i64 - l0;
            let over = crate::mem_probe::peak().saturating_sub(pbase) as i64 - alive;
            assert_eq!(old_shape.len(), rows, "臂 E 少折了条目");
            drop(old_shape);
            if d < e_best {
                e_best = d;
                e_live = alive;
                e_over = over;
            }
        }};
    }

    // ── 臂 F / G（M59，为 #67 那笔 21.5 MB）：与 A 同一个流式解码、同一条建树门，
    //    只差"那份中间清单怎么攒"，并且**把窗口内部分成三段读数**：
    //      s1 = 清单建好（键载荷 + `Vec` 骨架），s2 = 树建好，over = 全程峰值 − s2。
    //    ⚠️ 单靠 A(50.3) 与 D(28.8) 的差判不出那 21.5 住在哪 —— 三个嫌疑：
    //      ① `Vec` 翻倍扩容留下的空槽（capacity 可以高到 len 的 ~2 倍）；
    //      ② std 的 `BTreeMap::from_iter` 自己怎么攒那份 `Vec`；
    //      ③ 有序批量填充时节点本身的攒法。
    //    F（`with_capacity(rows)`）与 G（`Vec::new()`，只靠翻倍）**除预留外一字不差**
    //    ⇒ F/G 的 over 差 = 嫌疑①；F 的 over 若仍 ≈ A ⇒ 与预留无关（嫌疑在②③）。
    macro_rules! arm_list {
        ($reserve:expr, $best:ident, $s1:ident, $s2:ident, $over:ident) => {{
            let l0 = crate::mem_probe::live() as i64;
            let pbase = crate::mem_probe::reset_peak();
            let t = std::time::Instant::now();
            let mut acc: Vec<(Vec<u8>, KeyLoc)> = $reserve;
            walk(&hint, |k, _f, id, p, l| {
                acc.push((
                    k.to_vec(),
                    KeyLoc::Live {
                        file_id: id,
                        pos: p,
                        len: l,
                    },
                ));
            });
            let a1 = crate::mem_probe::live() as i64 - l0;
            let built: KeyDir = acc.into_iter().collect();
            let a2 = crate::mem_probe::live() as i64 - l0;
            let d = t.elapsed();
            assert_eq!(built.len(), rows, "清单臂少折了条目");
            let pk = crate::mem_probe::peak();
            drop(built);
            if d < $best {
                $best = d;
                $s1 = a1;
                $s2 = a2;
                $over = pk.saturating_sub(pbase) as i64 - a2;
            }
        }};
    }

    for round in 0..4 {
        if round % 2 == 0 {
            arm_b!();
            arm_c!();
            arm_a!();
            arm_d!();
            arm_list!(Vec::with_capacity(rows), f_best, f_s1, f_s2, f_over);
            arm_list!(Vec::new(), g_best, g_s1, g_s2, g_over);
            arm_e!();
        } else {
            arm_e!();
            arm_list!(Vec::new(), g_best, g_s1, g_s2, g_over);
            arm_list!(Vec::with_capacity(rows), f_best, f_s1, f_s2, f_over);
            arm_d!();
            arm_a!();
            arm_c!();
            arm_b!();
        }
    }
    eprintln!(
        "=== hint 三臂 rows={rows} 文件 {:.1} MB（每条 {:.1} B）解出 {seen} 条｜四轮两种顺序交替、每臂取最小 ===",
        bytes as f64 / 1e6,
        bytes as f64 / rows.max(1) as f64
    );
    eprintln!(
        "  B 只解字段        {:>8.1} ms  {:>6.3} µs/条",
        ms(b_best),
        per(b_best)
    );
    eprintln!(
        "  C +每键一次分配   {:>8.1} ms  {:>6.3} µs/条  ⇒ ②分配 = {:>7.1} ms（{:.2}×）",
        ms(c_best),
        per(c_best),
        ms(c_best - b_best),
        ms(c_best) / ms(b_best)
    );
    eprintln!(
        "  A 真门 read_hint  {:>8.1} ms  {:>6.3} µs/条  ⇒ ③插 BTreeMap = {:>7.1} ms（{:.2}×）",
        ms(a_best),
        per(a_best),
        ms(a_best - c_best),
        ms(a_best) / ms(c_best).max(0.001)
    );
    eprintln!(
        "  D 攒有序 Vec 再 collect 成同一棵树 {:>8.1} ms  {:>6.3} µs/条  ⇒ 比 A {:>7.1} ms（{:.2}×）｜③里\"逐条 insert 从根走一遍\"占这一笔",
        ms(d_best),
        per(d_best),
        ms(a_best) - ms(d_best),
        ms(a_best) / ms(d_best).max(0.001)
    );
    eprintln!(
        "  E 改前形状(逐条 insert) {:>8.1} ms  {:>6.3} µs/条  ⇒ 同构建 A/B：A(现门) 比 E 省 {:>7.1} ms（{:.2}×）",
        ms(e_best),
        per(e_best),
        ms(e_best) - ms(a_best),
        ms(e_best) / ms(a_best).max(0.001)
    );
    eprintln!(
        "  建好之后**还活着**：A {:.1} MB / E {:.1} MB ⇒ 同一批 {rows} 把键，形状差 {:>5.1} MB（{:.2}×）\
             \n    ⇒ 大库台架那一格 `冷重开 常驻 83.4 → 57.7 MB` 的机制就在这里：升序逐条 `insert` 把每个节点劈成半满。",
        a_live as f64 / 1e6,
        e_live as f64 / 1e6,
        (e_live - a_live) as f64 / 1e6,
        e_live as f64 / (a_live.max(1) as f64)
    );
    // ⚠️ 这三笔 `over` 才是 M52c 的去留判据：`over` = 折树过程中**超出最终常驻**的那部分瞬时字节。
    //    600k 两轮读数一字不差：**E 逐条 insert 0.0 ｜ D 自带 `Vec` 再 collect 28.8（正好是那根骨架）
    //    ｜ A 流式 collect 50.3**。A 比我明确写出来的那根骨架还高 ⇒ "流式一定更省内存"这句**不成立**，
    //    而这 21.5 MB 差值住在哪我没归因（怀疑在 `BTreeMap` 有序批量填充自己怎么攒节点）⇒
    //    这一格**不进判据**，只登记。留流式那一版的理由只有两条实打实的：
    //    少一个 `Vec<整库记录>` 这种"中间物形状"、少一格 `with_capacity(entry_count)` 的预算陷阱
    //    （`entry_count` 从文件头读来，不可信）。时间两臂在比值噪声带里，不许当成证据。
    eprintln!(
        "  折树过程中的瞬时超出（`over`，MB）：A 流式 collect {:>6.1} ｜ D 我攒的 Vec 再 collect {:>6.1} ｜ E 逐条 insert {:>6.1}",
        a_over as f64 / 1e6,
        d_over as f64 / 1e6,
        e_over as f64 / 1e6
    );
    // M59：把 A 与 D 那 21.5 的差**拆开读**，而不是继续猜
    let mb = |b: i64| b as f64 / 1e6;
    eprintln!(
        "  M59 归因臂：F 精确预留 over {:>5.1}（清单 s1 {:>5.1} ⇒ 建树净增 {:>5.1} = 终态 {:>5.1}）\
             ｜ G 不预留（靠翻倍扩容）over {:>5.1}（s1 {:>5.1}、终态 {:>5.1}）",
        mb(f_over),
        mb(f_s1),
        mb(f_s2 - f_s1),
        mb(f_s2),
        mb(g_over),
        mb(g_s1),
        mb(g_s2),
    );
    eprintln!(
        "    ⇒ F/G 只差\"有没有预留\"这一件事 ⇒ over 差 {:>5.1} MB = 嫌疑①（翻倍扩容留下的空槽）",
        mb(g_over - f_over)
    );
    eprintln!(
        "    ⇒ A({:>5.1}) − F({:>5.1}) = {:>5.1} MB 才是剩下的嫌疑②③（std 内部怎么攒那份 Vec / 节点攒法）",
        mb(a_over),
        mb(f_over),
        mb(a_over - f_over),
    );
    assert!(
        e_live > a_live,
        "两臂解出的键数相同（各自断言过 == {rows}），常驻却不再差这么多 ⇒ \
             这一格报的是**树形状**的账（半满节点），不是内容：{a_live} vs {e_live} 字节"
    );
    eprintln!(
        "  合计拆分：① {:>5.1}% ② {:>5.1}% ③ {:>5.1}%（按 A 那一臂的墙钟）",
        100.0 * ms(b_best) / ms(a_best),
        100.0 * ms(c_best - b_best) / ms(a_best),
        100.0 * ms(a_best - c_best) / ms(a_best),
    );
    cleanup(&path);
}
