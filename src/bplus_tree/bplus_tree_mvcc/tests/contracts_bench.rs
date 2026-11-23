// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
#[ignore = "same closed seed ABBA for reclamation freelist publication"]
fn reclamation_freelist_same_seed_abba() {
    use crate::bplus_tree::page::reclamation_reference;
    use crate::common::disk::{
        SITE_BPLUS_DBLWR, SITE_BPLUS_FREELIST, SITE_BPLUS_PAGE_FLUSH, fsync_counts,
    };
    use std::{assert, assert_eq, format, vec};
    fn key(i: usize) -> Vec<u8> {
        format!("key:{i:08}").into_bytes()
    }
    fn value(i: usize, round: usize) -> Vec<u8> {
        let mut out = vec![round as u8; 64];
        out[..8].copy_from_slice(&(i as u64).to_le_bytes());
        out
    }
    fn sidecar(path: &std::path::Path, suffix: &str) -> std::path::PathBuf {
        let mut name = path.as_os_str().to_os_string();
        name.push(suffix);
        name.into()
    }
    let rows = 12_000_usize;
    let seed = crate::common::ScratchPath::new("bplus", "reclamation_seed", ".db");
    {
        let db = MVCC::open(&seed, 64, 128);
        db.set_vacuum_auto_slice(0);
        for round in 0..2 {
            for base in (0..rows).step_by(500) {
                let tx = db.begin_transaction();
                for i in base..(base + 500).min(rows) {
                    assert!(tx.set_raw(&key(i), value(i, round)));
                }
                tx.commit();
            }
        }
        {
            let tx = db.begin_transaction();
            for i in 0..rows {
                assert_eq!(tx.get(&key(i)), Some(value(i, 1)));
            }
        }
        db.checkpoint();
    }
    let mut measured = Vec::new();
    for (arm, improved) in [false, true, true, false].into_iter().enumerate() {
        let target = crate::common::ScratchPath::new("bplus", "reclamation_arm", ".db");
        for suffix in ["", ".wal", ".dblwr", ".freelist", ".blob"] {
            let source = sidecar(&seed, suffix);
            if source.exists() {
                std::fs::copy(&source, sidecar(&target, suffix)).unwrap();
            }
        }
        let db = MVCC::open(&target, 64, 128);
        db.set_vacuum_auto_slice(0);
        let reference = (!improved).then(reclamation_reference::Scope::start);
        let before = fsync_counts();
        let t = std::time::Instant::now();
        let stats = db.vacuum().unwrap();
        let ms = t.elapsed().as_secs_f64() * 1000.0;
        let after = fsync_counts();
        drop(reference);
        let freelist = after[SITE_BPLUS_FREELIST] - before[SITE_BPLUS_FREELIST];
        assert_eq!(stats.versions_removed, rows);
        {
            let tx = db.begin_transaction();
            for i in 0..rows {
                assert_eq!(tx.get(&key(i)), Some(value(i, 1)));
            }
        }
        drop(db);
        let db = MVCC::open(&target, 64, 128);
        {
            let tx = db.begin_transaction();
            for i in 0..rows {
                assert_eq!(tx.get(&key(i)), Some(value(i, 1)));
            }
        }
        drop(db);
        std::eprintln!(
            "RECLAMATION_ABBA arm={arm} improved={improved} rows={rows} ms={ms:.3} freelist_sync={freelist} page_sync={} dblwr_sync={} removed={} full_values_verified={rows} reopened_values_verified={rows}",
            after[SITE_BPLUS_PAGE_FLUSH] - before[SITE_BPLUS_PAGE_FLUSH],
            after[SITE_BPLUS_DBLWR] - before[SITE_BPLUS_DBLWR],
            stats.versions_removed
        );
        measured.push((improved, ms, freelist));
    }
    assert!(
        measured.iter().filter(|x| !x.0).all(|x| x.2 > 1),
        "seed must exercise repeated old freelist publication"
    );
    assert!(measured.iter().filter(|x| x.0).all(|x| x.2 == 1));
}
