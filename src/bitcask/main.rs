//! Bitcask + MVCC 功能演示
//!
//! 覆盖：基础 set/get/delete · 批量 · multi_get
//!       计数器 incr / incr_with_ttl · 范围 scan / prefix / seek
//!       模糊搜索分页 · 元数据 · TTL · 快照隔离 · bulk · vacuum · 重启校验
//!
//! 用法：`cargo run` 或 `cargo run --release`

use bitcask::{kv_ops, SearchQuery, MVCC};
use std::fs;
use std::time::Duration;

fn show_key(k: &[u8]) -> String {
    if k.len() > 40 {
        format!("{}...({}B)", String::from_utf8_lossy(&k[..24]), k.len())
    } else {
        format!("{:?}", String::from_utf8_lossy(k))
    }
}

fn show_val(v: Option<&[u8]>) -> String {
    match v {
        None => "<none>".into(),
        Some(val) if val.len() > 32 => {
            format!("{}...({}B)", String::from_utf8_lossy(&val[..16]), val.len())
        }
        Some(val) => format!("{:?}", String::from_utf8_lossy(val)),
    }
}

fn cleanup_dir(dir: &str) {
    let _ = fs::remove_dir_all(dir);
}

fn main() {
    let db_dir = "bitcask_mvcc_demo";
    cleanup_dir(db_dir);

    let large_key = {
        let mut k = b"user:profile:".to_vec();
        k.extend(std::iter::repeat(b'x').take(200));
        k
    };
    let large_val = {
        let mut v = b"VAL:".to_vec();
        v.extend(std::iter::repeat(b'A').take(4_000));
        v
    };

    // ══════════════════════════════════════════════════════════════
    // 1. 基础 CRUD + 批量 / 扫描 / 搜索 / TTL
    // ══════════════════════════════════════════════════════════════
    {
        let mvcc = MVCC::open(db_dir);

        // ── 1.1 基础 set / get / delete / 覆盖 ──
        {
            println!("\n========== 1. 基础 set / get / delete ==========");
            let tx = mvcc.begin_transaction();

            assert!(tx.set(b"test", kv_ops::pack_plain(b"test1")));
            println!("[set]  test = test1");

            assert!(tx.set(&large_key, kv_ops::pack_plain(&large_val)));
            println!(
                "[set]  large_key(len={}) / large_val(len={})",
                large_key.len(),
                large_val.len()
            );

            assert!(tx.set(b"to_delete", kv_ops::pack_plain(b"will_gone")));
            assert_eq!(tx.get(b"test").as_deref(), Some(b"test1".as_slice()));
            let got_big = tx.get(&large_key).expect("大 value");
            assert_eq!(got_big, large_val);
            println!(
                "[get]  large 读回 len={} 前缀 {:?}",
                got_big.len(),
                String::from_utf8_lossy(&got_big[..8])
            );

            assert!(tx.delete(b"to_delete"));
            assert_eq!(tx.get(b"to_delete"), None);
            println!("[del]  to_delete → get=None");

            assert!(tx.set(b"test", kv_ops::pack_plain(b"test2")));
            assert_eq!(tx.get(b"test").as_deref(), Some(b"test2".as_slice()));
            println!("[set]  test 覆盖为 test2");

            tx.commit();
            println!("commit ok");
        }

        // ── 1.2 批量写入 / multi_get / batch_delete ──
        {
            println!("\n========== 2. 批量 batch_set / multi_get / batch_delete ==========");
            let tx = mvcc.begin_transaction();

            let items: Vec<(Vec<u8>, Vec<u8>)> = (0..8u8)
                .map(|i| {
                    (
                        format!("order:{i:02}").into_bytes(),
                        format!("order-val-{i}").into_bytes(),
                    )
                })
                .collect();
            tx.batch_set(&items).expect("batch_set");
            println!("[batch_set] 写入 {} 条 order:*", items.len());

            let keys: Vec<Vec<u8>> = items.iter().map(|(k, _)| k.clone()).collect();
            let got = tx.multi_get(&keys);
            println!(
                "[multi_get] 命中 {}/{}",
                got.iter().filter(|v| v.is_some()).count(),
                got.len()
            );
            for (k, v) in keys.iter().zip(got.iter()) {
                println!("  {} => {}", show_key(k), show_val(v.as_deref()));
            }

            let del: Vec<Vec<u8>> = (4..8u8)
                .map(|i| format!("order:{i:02}").into_bytes())
                .collect();
            tx.batch_delete(&del).expect("batch_delete");
            println!("[batch_delete] 删除 order:04..07");
            assert_eq!(tx.get(b"order:04"), None);
            assert_eq!(
                tx.get(b"order:03").as_deref(),
                Some(b"order-val-3".as_slice())
            );

            let more: Vec<(Vec<u8>, Vec<u8>)> = vec![
                (b"apple".to_vec(), b"fruit".to_vec()),
                (b"apply".to_vec(), b"verb".to_vec()),
                (b"banana".to_vec(), b"yellow".to_vec()),
                (b"user:01".to_vec(), b"alice".to_vec()),
                (b"user:02".to_vec(), b"bob".to_vec()),
                (b"user:10".to_vec(), b"carol".to_vec()),
                (b"zebra".to_vec(), b"z".to_vec()),
            ];
            tx.batch_set(&more).expect("batch more");
            tx.commit();
            println!("commit ok");
        }

        // ── 1.3 范围 scan / prefix / seek ──
        {
            println!("\n========== 3. 范围 scan / prefix_scan / seek / seek_prev ==========");
            let tx = mvcc.begin_transaction();

            let range = tx.scan(Some(b"order:"), Some(b"order:\x7f"));
            println!("[scan] [order:, order:\\x7f) → {} 条", range.len());
            for r in &range {
                println!(
                    "  {} => {}",
                    show_key(&r.key),
                    show_val(r.value.as_deref())
                );
            }

            let pref = tx.prefix_scan(b"user:");
            println!("[prefix_scan] user: → {} 条", pref.len());
            for r in &pref {
                println!("  {}", show_key(&r.key));
            }

            let rev = tx.reverse_scan(Some(b"a"), Some(b"c"));
            println!("[reverse_scan] [a,c) 逆序 首条 = {}", show_key(&rev[0].key));

            if let Some(s) = tx.seek(b"user:02") {
                println!("[seek]  >= user:02 → {}", show_key(&s.key));
            }
            if let Some(s) = tx.seek(b"user:05") {
                println!("[seek]  >= user:05 → {} (跳到 user:10)", show_key(&s.key));
            }
            if let Some(p) = tx.seek_prev(b"user:05") {
                println!("[seek_prev] <= user:05 → {}", show_key(&p.key));
            }

            println!(
                "[exists] order:01={}  gone={}",
                tx.exists(b"order:01"),
                tx.exists(b"order:04")
            );
            println!("[key_count] prefix order: = {}", tx.key_count(b"order:"));

            if let Some(m) = tx.get_meta(b"order:01") {
                println!(
                    "[get_meta] order:01 len={:?} ver={} expired={} deleted={}",
                    m.value_len, m.version, m.expired, m.deleted
                );
            }
            tx.commit();
        }

        // ── 1.4 模糊搜索 + 分页 ──
        {
            println!("\n========== 4. 模糊 / 前缀搜索 + 分页 ==========");

            let page = mvcc.search_keys(&SearchQuery::contains(b"pp", 0, 10));
            println!(
                "[search contains \"pp\"] total={} page={}/{} items={}",
                page.total,
                page.page + 1,
                page.total_pages.max(1),
                page.items.len()
            );
            for r in &page.items {
                println!(
                    "  {} => {}",
                    show_key(&r.key),
                    show_val(r.value.as_deref())
                );
            }

            println!("[search prefix \"user:\" page_size=2]");
            let mut page_idx = 0usize;
            loop {
                let p = mvcc.search_keys(&SearchQuery::prefix(b"user:", page_idx, 2));
                println!(
                    "  [page {}/{}] count={} has_next={}",
                    p.page + 1,
                    p.total_pages.max(1),
                    p.items.len(),
                    p.has_next()
                );
                for r in &p.items {
                    println!("    {}", show_key(&r.key));
                }
                if !p.has_next() {
                    break;
                }
                page_idx += 1;
            }
        }

        // ── 1.5 计数器 incr / incr_with_ttl ──
        {
            println!("\n========== 5. 计数器 incr / decr / incr_with_ttl ==========");
            let tx = mvcc.begin_transaction();

            let n = tx.incr(b"seq", 1).expect("incr");
            println!("[incr] seq +1 → {n}");
            let n = tx.incr(b"seq", 10).expect("incr");
            println!("[incr] seq +10 → {n}");
            let n = tx.decr(b"seq", 3).expect("decr");
            println!("[decr] seq -3 → {n}");
            assert_eq!(tx.get(b"seq"), Some(b"8".to_vec()));
            println!(
                "[get]  seq = {:?}",
                String::from_utf8_lossy(&tx.get(b"seq").unwrap())
            );

            let n = tx
                .incr_with_ttl(b"hits", 1, Duration::from_secs(120))
                .expect("incr_with_ttl");
            println!("[incr_with_ttl] hits +1 → {n} (TTL 120s)");
            let n = tx
                .incr_with_ttl(b"hits", 5, Duration::from_secs(120))
                .expect("incr_with_ttl");
            println!("[incr_with_ttl] hits +5 → {n}");
            assert_eq!(tx.get(b"hits"), Some(b"6".to_vec()));
            match tx.get_ttl(b"hits") {
                Some(Some(d)) => println!("[get_ttl] hits 剩余 ≈ {}s", d.as_secs()),
                other => println!("[get_ttl] hits = {other:?}"),
            }

            assert!(tx.set(b"not_num", kv_ops::pack_plain(b"abc")));
            match tx.incr(b"not_num", 1) {
                Err(e) => println!("[incr] not_num +1 → Err({e}) （预期 NotInteger）"),
                Ok(v) => panic!("should fail, got {v}"),
            }

            tx.commit();
            println!("commit ok");
        }

        // ── 1.6 TTL ──
        {
            println!("\n========== 6. TTL set_with_ttl / get_ttl / persist / purge ==========");
            let tx = mvcc.begin_transaction();

            assert!(tx.set_with_ttl(b"cache:hot", b"payload".to_vec(), Duration::from_secs(120)));
            assert!(tx.set(b"cache:forever", kv_ops::pack_plain(b"keep")));

            let stale = kv_ops::pack_ttl(b"old", kv_ops::now_unix_secs().saturating_sub(5));
            assert!(tx.set(b"cache:stale", stale));

            println!(
                "[get] cache:hot={:?}  cache:stale={:?} (过期应 None)",
                tx.get(b"cache:hot")
                    .map(|v| String::from_utf8_lossy(&v).into_owned()),
                tx.get(b"cache:stale")
            );
            println!(
                "[exists] hot={} stale={} forever={}",
                tx.exists(b"cache:hot"),
                tx.exists(b"cache:stale"),
                tx.exists(b"cache:forever")
            );

            match tx.get_ttl(b"cache:hot") {
                Some(Some(d)) => println!("[get_ttl] cache:hot 剩余 ≈ {}s", d.as_secs()),
                other => println!("[get_ttl] cache:hot = {other:?}"),
            }
            match tx.get_ttl(b"cache:forever") {
                Some(None) => println!("[get_ttl] cache:forever = 永久"),
                other => println!("[get_ttl] cache:forever = {other:?}"),
            }

            assert!(tx.refresh_ttl(b"cache:hot", Duration::from_secs(300)));
            println!("[refresh_ttl] cache:hot → 300s");
            assert!(tx.persist(b"cache:hot"));
            println!(
                "[persist] cache:hot → 永久, get_ttl={:?}",
                tx.get_ttl(b"cache:hot")
            );

            let n = tx.purge_expired();
            println!("[purge_expired] 清理 {n} 条过期 key");
            assert!(!tx.exists(b"cache:stale"));

            if let Some(m) = tx.get_meta(b"cache:hot") {
                println!(
                    "[get_meta] cache:hot len={:?} expire={:?} expired={}",
                    m.value_len, m.expire_unix_secs, m.expired
                );
            }
            tx.commit();
        }

        // SI + 冲突简要演示
        {
            println!("\n========== 6b. 快照隔离 / 写写冲突 ==========");
            let t_read = mvcc.begin_transaction();
            let t_write = mvcc.begin_transaction();
            assert!(t_write.set(b"user:01", kv_ops::pack_plain(b"alice-v2")));
            assert_eq!(
                t_read.get(b"user:01").as_deref(),
                Some(b"alice".as_slice())
            );
            println!("[SI] 写事务改 user:01 后，读事务仍见 alice");
            t_write.commit();
            t_read.commit();

            let t1 = mvcc.begin_transaction();
            let t2 = mvcc.begin_transaction();
            assert!(t1.set(b"conflict", kv_ops::pack_plain(b"t1")));
            assert!(!t2.set(b"conflict", kv_ops::pack_plain(b"t2")));
            println!("[WW] t2 写 conflict → false（冲突）");
            t1.commit();
            t2.rollback();
        }

        // bulk load
        {
            println!("\n========== 6c. Bulk load ==========");
            let mut bulk = mvcc.begin_bulk();
            for i in 0..50u32 {
                bulk.put(
                    format!("bulk:{i:03}").as_bytes(),
                    format!("v{i}").into_bytes(),
                );
            }
            bulk.finish();
            let tx = mvcc.begin_transaction();
            assert_eq!(tx.get(b"bulk:042"), Some(b"v42".to_vec()));
            println!("[bulk] 写入 50 条, bulk:042 = v42");
            tx.commit();
        }

        // vacuum
        {
            println!("\n========== 6d. Vacuum ==========");
            let st = mvcc.vacuum().expect("vacuum");
            println!(
                "[vacuum] xmin={} versions_removed={}",
                st.xmin, st.versions_removed
            );
        }

        mvcc.flush();
        mvcc.checkpoint();
        println!("\n[engine] flush + checkpoint，dir = {:?}", mvcc.db_dir());
    }

    // ══════════════════════════════════════════════════════════════
    // 2. 模拟重启
    // ══════════════════════════════════════════════════════════════
    {
        println!("\n========== 7. 重启后校验 ==========");
        let mvcc = MVCC::open(db_dir);
        let tx = mvcc.begin_transaction();

        assert_eq!(tx.get(b"test").as_deref(), Some(b"test2".as_slice()));
        println!(
            "[reopen] test = {:?}",
            String::from_utf8_lossy(&tx.get(b"test").unwrap())
        );
        assert_eq!(tx.get(b"to_delete"), None);

        let got_big = tx.get(&large_key).expect("重启后大 value");
        assert_eq!(got_big, large_val);
        println!("[reopen] large_key len={} OK", got_big.len());

        assert_eq!(
            tx.get(b"order:01").as_deref(),
            Some(b"order-val-1".as_slice())
        );
        assert_eq!(tx.get(b"order:04"), None);
        assert_eq!(tx.key_count(b"order:"), 4);
        println!("[reopen] order:* 存活 {} 条", tx.key_count(b"order:"));

        assert_eq!(tx.get(b"seq"), Some(b"8".to_vec()));
        let n = tx.incr(b"seq", 1).expect("reopen incr");
        assert_eq!(n, 9);
        println!("[reopen] seq 仍为 8，再 incr → {n}");
        assert_eq!(tx.get(b"hits"), Some(b"6".to_vec()));

        assert!(tx.exists(b"cache:hot"));
        assert!(!tx.exists(b"cache:stale"));
        println!("[reopen] TTL: hot 在 / stale 已 purge");

        assert_eq!(tx.get(b"bulk:042"), Some(b"v42".to_vec()));
        println!("[reopen] bulk:042 = v42");

        let page = mvcc.search_keys(&SearchQuery::contains(b"pp", 0, 5));
        println!(
            "[reopen] search \"pp\" → {:?}",
            page.items
                .iter()
                .map(|r| String::from_utf8_lossy(&r.key).into_owned())
                .collect::<Vec<_>>()
        );

        assert!(tx.delete(&large_key));
        assert_eq!(tx.get(&large_key), None);
        println!("[reopen] 删除 large_key 后 get=None");
        tx.commit();
    }

    println!("\n—— 磁盘布局 ——");
    println!("  {db_dir}/");
    for name in ["data.log", "data.hint", "data.blob", "bitcask.wal", "bitcask.lock"] {
        let p = format!("{db_dir}/{name}");
        if let Ok(m) = fs::metadata(&p) {
            println!("    {name:14} {:>8} B", m.len());
        }
    }

    println!("\ndemo 完成（Bitcask + MVCC + kv_ops：批量 / 扫描 / 搜索 / TTL / vacuum / bulk）。");
}
