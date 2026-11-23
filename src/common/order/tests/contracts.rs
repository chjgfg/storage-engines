// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

#[test]
#[ignore = "same-build logical health allocation/time comparison; release single thread"]
fn logical_health_cost() {
    use std::{eprintln, format, time::Instant};
    let rows = 600_000;
    let physical: Vec<_> = (0..rows)
        .map(|i| {
            encode_key(
                format!("{i:08}#/worldquant/datafields/x.json").as_bytes(),
                1,
            )
        })
        .collect();
    let mut expected = None;
    for old in [true, false, false, true] {
        let alloc = crate::mem_probe::allocations();
        let start = Instant::now();
        let result = if old {
            reference_health(&physical, 1000)
        } else {
            let mut fold = LogicalHealth::new(1000);
            for enc in &physical {
                fold.push(enc);
            }
            fold.finish(0)
        };
        let elapsed = start.elapsed();
        let allocations = crate::mem_probe::allocations() - alloc;
        if let Some(want) = &expected {
            assert_eq!(&result, want);
        } else {
            expected = Some(result);
        }
        if old {
            assert!(
                allocations >= rows as u64,
                "reference must expose per-row allocation"
            );
        } else {
            assert!(
                allocations < 100,
                "ordinary health must reuse its key buffer"
            );
        }
        eprintln!(
            "HEALTH_COST old={old} rows={rows} ms={:.3} allocations={allocations}",
            elapsed.as_secs_f64() * 1e3
        );
    }
}
