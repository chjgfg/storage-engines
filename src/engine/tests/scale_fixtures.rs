// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

// 这份种子同时供台架与独立验收使用；期望不能从源库 health/scan 推导，
// 否则源库与 migrate 一起漏行时会自洽地通过。
fn scale_key(i: usize) -> Vec<u8> {
    format!("{i:08}#/worldquant/datafields/x.json").into_bytes()
}

fn scale_val(i: usize) -> Vec<u8> {
    format!("{{\"i\":{i},\"h\":\"{:0<28}\",\"v\":1}}", i * 7).into_bytes()
}

fn scale_last_page_index(rows: usize, page_size: usize) -> usize {
    rows.saturating_sub(1) / page_size
}

fn scale_check_last_page(page: &SearchPage, rows: usize, page_size: usize) {
    let pages = rows.div_ceil(page_size);
    let count = if rows == 0 {
        0
    } else {
        (rows - 1) % page_size + 1
    };
    assert_eq!(page.total, Some(rows));
    assert_eq!(page.total_pages, Some(pages));
    assert_eq!(page.page, pages.saturating_sub(1));
    assert_eq!(page.page_size, page_size);
    assert!(!page.has_next());
    assert_eq!(
        page.items.len(),
        count,
        "last page must contain its seeded rows"
    );
    for (record, i) in page.items.iter().zip(rows - count..rows) {
        assert_eq!(record.key, scale_key(i));
        assert_eq!(record.value, Some(scale_val(i)));
        assert_eq!(record.expire_unix_secs, None);
    }
}

/// TTL 两格仍量真实 set/get；紧接着在计时外去掉期限，而非把期限调得更长。
/// 必须先验值再 persist（不是 set_raw 重播种），缺失/过期/损坏不能被修补后蒙混过关。
fn scale_persist_auxiliary(db: &dyn KvEngine, rows: usize) {
    let tx = db.begin();
    for i in 0..1000.min(rows) {
        let key = scale_key(rows * 4 + i);
        assert_eq!(
            tx.get(&key),
            Some(scale_val(i)),
            "{}: TTL 辅助键 {i} 丢失或值损坏",
            db.name()
        );
        assert!(
            tx.get_ttl(&key).flatten().is_some(),
            "{}: TTL 辅助键 {i} 没有期限",
            db.name()
        );
        assert!(
            tx.persist_key(&key),
            "{}: TTL 辅助键 {i} 永久化失败",
            db.name()
        );
    }
    tx.commit();
    let tx = db.begin();
    for i in 0..1000.min(rows) {
        let key = scale_key(rows * 4 + i);
        assert_eq!(
            tx.get(&key),
            Some(scale_val(i)),
            "{}: persist 改了辅助键 {i} 的值",
            db.name()
        );
        assert_eq!(
            tx.get_ttl(&key),
            Some(None),
            "{}: persist 后辅助键 {i} 仍有期限",
            db.name()
        );
    }
    tx.commit();
}

/// 对目标逐行验完整键与最新用户字节；只留前一把键，额外内存 O(1)。
/// 合法键域 + 严格递增（不重不乱）+ 独立总数，才能证明没有遗漏；仅比较两次计数不够。
fn scale_check_migration(
    target: &dyn KvEngine,
    rows: usize,
    stats: MigrateStats,
) -> std::result::Result<(), String> {
    scale_check_contents(target, rows, ScaleExpected::Migration(stats))
}

enum ScaleExpected {
    Migration(MigrateStats),
    BulkReopen { bulk_rows: usize },
}

fn scale_check_contents(
    target: &dyn KvEngine,
    rows: usize,
    phase: ScaleExpected,
) -> std::result::Result<(), String> {
    let ttl_rows = 1000.min(rows);
    let bulk_rows = match &phase {
        ScaleExpected::Migration(_) => 0,
        ScaleExpected::BulkReopen { bulk_rows } => *bulk_rows,
    };
    let want = (rows + ttl_rows + 1 + bulk_rows) as u64;
    if let ScaleExpected::Migration(stats) = &phase {
        if stats.rows != want || stats.with_ttl != 0 {
            return Err(format!("迁移统计应为 {want} 行、0 TTL，实际 {stats:?}"));
        }
    }
    let mut seen = 0u64;
    let mut bytes = 0u64;
    let mut previous: Option<Vec<u8>> = None;
    let mut error = None;
    let handed = target.export_latest_visible_for_each(false, &mut |rec| {
        let expected = if rec.key == b"counter:hits" {
            Some(b"1000".to_vec())
        } else {
            std::str::from_utf8(&rec.key)
                .ok()
                .and_then(|s| s.split_once('#'))
                .and_then(|(n, _)| n.parse::<usize>().ok())
                .filter(|&i| rec.key == scale_key(i))
                .and_then(|i| {
                    if i < rows {
                        Some(scale_val(if i < (rows / 10).max(1) { i + rows } else { i }))
                    } else if i - rows < bulk_rows {
                        Some(scale_val(i - rows))
                    } else if i >= rows * 4 && i - rows * 4 < ttl_rows {
                        Some(scale_val(i - rows * 4))
                    } else {
                        None
                    }
                })
        };
        let reason = if previous.as_ref().is_some_and(|p| p >= &rec.key) {
            Some("键重复或逆序")
        } else if expected.is_none() {
            Some("出现种子之外的键")
        } else if rec.expire_unix_secs.is_some() {
            Some("永久化后的行仍有 TTL")
        } else if rec.value != expected {
            Some("用户值不符（含更新/计数器）")
        } else {
            None
        };
        if let Some(reason) = reason {
            error = Some(format!("{reason}: {}", String::from_utf8_lossy(&rec.key)));
            return false;
        }
        seen += 1;
        bytes += expected.as_ref().unwrap().len() as u64;
        previous = Some(rec.key);
        true
    });
    if let Some(error) = error {
        return Err(error);
    }
    if seen != want || handed as u64 != seen {
        return Err(format!(
            "目标应有 {want} 个唯一合法键，实际 {seen}（导出报告 {handed}）"
        ));
    }
    if let ScaleExpected::Migration(stats) = &phase {
        if stats.bytes != bytes {
            return Err(format!("用户字节应为 {bytes}，迁移报告 {}", stats.bytes));
        }
    }
    Ok(())
}

/// 只拥有本次新建的子目录；SCALE_SCRATCH_DIR 可把大台架放到有空间的盘。
/// guard 必须先于引擎声明，正常结束或 panic 都先释放文件锁再删本次目录。
struct ScaleScratch(PathBuf);

// 小回归使用同一键/值账，直接播种「更新结束」状态，不跑性能循环。
fn scale_seed_test_fixture(db: &dyn KvEngine, rows: usize) {
    let tx = db.begin();
    for i in 0..rows {
        let v = scale_val(if i < (rows / 10).max(1) { i + rows } else { i });
        assert!(tx.set_raw(&scale_key(i), v));
    }
    for i in 0..1000.min(rows) {
        assert!(tx.set_with_ttl(
            &scale_key(rows * 4 + i),
            scale_val(i),
            Duration::from_secs(3600)
        ));
    }
    assert!(tx.set_raw(b"counter:hits", b"1000".to_vec()));
    tx.commit();
}

fn scale_seed_bulk(db: &dyn KvEngine, rows: usize, bulk_rows: usize) {
    scale_seed_test_fixture(db, rows);
    scale_persist_auxiliary(db, rows);
    let mut bulk = db.begin_bulk();
    bulk.put_batch_owned(
        (0..bulk_rows)
            .map(|i| (scale_key(rows + i), scale_val(i)))
            .collect(),
    );
    bulk.finish();
}
