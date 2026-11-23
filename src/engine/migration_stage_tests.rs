//! 迁移分段计量回归与专项入口。
use super::*;
use std::{assert, assert_eq, eprintln, vec};
fn run(rows: usize, print: bool) {
    use migration_probe::{Scope, Stage, Timer};
    assert!(Timer::start(Stage::SourceScan).is_none());
    let source = open_engine(EngineKind::Memory, None).unwrap();
    source.set_vacuum_auto_slice(0);
    let tx = source.begin();
    for i in 0..rows {
        assert!(tx.set_raw(&(i as u64).to_be_bytes(), vec![i as u8; 128]));
    }
    tx.commit();
    drop(tx);
    let target = open_engine(EngineKind::Memory, None).unwrap();
    target.set_vacuum_auto_slice(0);
    let scope = Scope::start();
    let now = std::time::Instant::now();
    let migrated = migrate(&*source, &*target, 1000).unwrap();
    let total_ns = now.elapsed().as_nanos();
    let s = scope.stats();
    let residual = total_ns
        .checked_sub(s.ns.iter().sum())
        .expect("migration stages overlap");
    assert_eq!(migrated.rows, rows as u64);
    assert_eq!(s.calls[Stage::SourceScan as usize], migrated.pages);
    assert_eq!(s.calls[Stage::SourceBegin as usize], migrated.pages);
    assert_eq!(s.calls[Stage::SourceCommit as usize], migrated.pages);
    assert_eq!(
        s.calls[Stage::TargetWrite as usize],
        s.calls[Stage::TargetCommit as usize]
    );
    assert_eq!(
        s.calls[Stage::TargetWrite as usize],
        rows.div_ceil(1000) as u64
    );
    assert_eq!(s.calls[Stage::TargetRollback as usize], 0);
    drop(scope);
    assert!(Timer::start(Stage::TargetWrite).is_none());
    let mut seen = 0;
    target.export_latest_visible_for_each(false, &mut |rec| {
        assert_eq!(rec.key, (seen as u64).to_be_bytes());
        assert_eq!(rec.value, Some(vec![seen as u8; 128]));
        seen += 1;
        true
    });
    assert_eq!(seen, rows);
    if print {
        eprintln!(
            "MIGRATION_COST rows={rows} total_ms={:.3} stages_ns={:?} calls={:?} residual_ms={:.3} all_values_verified={seen}",
            total_ns as f64 / 1e6,
            s.ns,
            s.calls,
            residual as f64 / 1e6
        );
    }
}
#[test]
fn migration_stage_accounting_uses_actual_scan_write_commit_gates() {
    run(2050, false);
}
#[test]
#[ignore = "memory migration source/target cost, release single thread"]
fn memory_migration_stage_cost() {
    let rows = std::env::var("MEMORY_MIGRATION_ROWS")
        .map(|s| s.parse().unwrap())
        .unwrap_or(200_000usize);
    run(rows, true);
}
