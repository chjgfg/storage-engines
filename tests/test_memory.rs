//! memory 功能演示
//!
//! 覆盖：基础 set/get/delete · 批量 · multi_get
//!       计数器 incr / incr_with_ttl · 范围 scan / prefix / seek
//!       模糊搜索分页 · 元数据 · TTL · vacuum · bulk

// use memory::{kv_ops, MVCC, SearchQuery};
use std::{format, time::Duration};

use storage_engines::memory::{MVCC, SearchQuery, kv_ops};

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


#[test]
fn test_memory() {
    println!("========== memory 纯内存 KV 演示 ==========");
    println!("架构对标 bplus-tree：MVCC + kv_ops，存储为 BTreeMap 多版本\n");

    let mvcc = MVCC::new();

    // ── 1. 基础 set / get / delete / 覆盖 ──
    {
        println!("========== 1. 基础 set / get / delete ==========");
        let tx = mvcc.begin_transaction();

        assert!(tx.set(b"test", b"test1".to_vec()));
        println!("[set]  test = test1");

        assert!(tx.set(b"to_delete", b"will_gone".to_vec()));
        assert_eq!(tx.get(b"test").as_deref(), Some(b"test1".as_slice()));

        assert!(tx.delete(b"to_delete"));
        assert_eq!(tx.get(b"to_delete"), None);
        println!("[del]  to_delete → get=None");

        assert!(tx.set(b"test", b"test2".to_vec()));
        assert_eq!(tx.get(b"test").as_deref(), Some(b"test2".as_slice()));
        println!("[set]  test 覆盖为 test2");

        tx.commit();
        println!("commit ok\n");
    }

    // ── 2. 批量 / multi_get / batch_delete ──
    {
        println!("========== 2. 批量 batch_set / multi_get / batch_delete ==========");
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
        println!("commit ok\n");
    }

    // ── 3. 范围 scan / prefix / seek ──
    {
        println!("========== 3. 范围 scan / prefix_scan / seek / seek_prev ==========");
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
        println!();
    }

    // ── 4. 模糊搜索 + 分页 ──
    {
        println!("========== 4. 模糊 / 前缀搜索 + 分页 ==========");

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
        println!();
    }

    // ── 5. 计数器 ──
    {
        println!("========== 5. 计数器 incr / decr / incr_with_ttl ==========");
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
        println!("commit ok\n");
    }

    // ── 6. TTL ──
    {
        println!("========== 6. TTL set_with_ttl / get_ttl / persist / purge ==========");
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
        println!();
    }

    // ── 7. 快照隔离演示 ──
    {
        println!("========== 7. 快照隔离 SI ==========");
        let t1 = mvcc.begin_transaction();
        assert!(t1.set(b"si_key", b"v1".to_vec()));
        t1.commit();

        let t2 = mvcc.begin_transaction();
        let t3 = mvcc.begin_transaction();
        assert!(t2.set(b"si_key", b"v2".to_vec()));
        println!("[t2] set si_key=v2（未 commit）");
        println!(
            "[t3] get si_key = {:?} （仍见 v1）",
            t3.get(b"si_key")
                .map(|v| String::from_utf8_lossy(&v).into_owned())
        );
        t2.commit();
        println!("[t2] commit");
        println!(
            "[t3] get si_key = {:?} （commit 后仍见 v1，快照）",
            t3.get(b"si_key")
                .map(|v| String::from_utf8_lossy(&v).into_owned())
        );
        let t4 = mvcc.begin_transaction();
        println!(
            "[t4] get si_key = {:?} （新事务见 v2）",
            t4.get(b"si_key")
                .map(|v| String::from_utf8_lossy(&v).into_owned())
        );
        t4.commit();
        t3.commit();
        println!();
    }

    // ── 8. bulk + vacuum ──
    {
        println!("========== 8. bulk load + vacuum ==========");
        {
            let mut bulk = mvcc.begin_bulk();
            for i in 0..100u32 {
                let k = format!("bulk:{i:04}").into_bytes();
                let v = format!("val-{i}").into_bytes();
                bulk.put(&k, v);
            }
            println!("[bulk] put 100 条 bulk:*");
            bulk.finish();
        }
        let tx = mvcc.begin_transaction();
        assert_eq!(tx.get(b"bulk:0000").as_deref(), Some(b"val-0".as_slice()));
        assert_eq!(tx.key_count(b"bulk:"), 100);
        println!("[check] bulk: 存活 100 条");
        tx.commit();

        // 多次覆盖制造旧版本
        for round in 0..5 {
            let tx = mvcc.begin_transaction();
            assert!(tx.set(b"hot", format!("r{round}").into_bytes()));
            tx.commit();
        }
        let before = mvcc.raw_len();
        let stats = mvcc.vacuum();
        let after = mvcc.raw_len();
        println!(
            "[vacuum] xmin={} removed={} raw_len {} → {}",
            stats.xmin, stats.versions_removed, before, after
        );
        let tx = mvcc.begin_transaction();
        assert_eq!(tx.get(b"hot").as_deref(), Some(b"r4".as_slice()));
        tx.commit();
        println!();
    }

    // ── 导出 ──
    {
        println!("========== 9. export_latest_visible ==========");
        let recs = mvcc.export_latest_visible(false);
        println!("[export] 存活逻辑 key 共 {} 条", recs.len());
        let sample: Vec<_> = recs.iter().take(5).collect();
        for r in sample {
            println!(
                "  {} => {}",
                show_key(&r.key),
                show_val(r.value.as_deref())
            );
        }
        if recs.len() > 5 {
            println!("  ...");
        }
    }

    println!("\n========== demo 完成 ==========");
}
