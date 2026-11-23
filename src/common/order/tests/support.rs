// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

fn reference_health(physical: &[Vec<u8>], ceiling: u64) -> super::super::MvccHealth {
    let mut pending = BTreeMap::<Vec<u8>, u64>::new();
    let mut out = super::super::MvccHealth::default();
    for enc in physical {
        // 保留原泛型 map 路径以及原始的下界比较，作为独立对照。
        let max = ceiling.to_be_bytes();
        let split = (0..enc.len())
            .find(|&i| max.as_slice() >= &enc[i..])
            .unwrap_or(enc.len());
        while pending
            .first_key_value()
            .is_some_and(|(k, _)| k.as_slice() < &enc[..split])
        {
            let (_, count) = pending.pop_first().unwrap();
            out.live_keys += 1;
            out.max_chain = out.max_chain.max(count);
        }
        if let Some(vk) = decode_key(enc) {
            *pending.entry(vk.raw_key).or_default() += 1;
            out.total_rows += 1;
        }
    }
    for count in pending.into_values() {
        out.live_keys += 1;
        out.max_chain = out.max_chain.max(count);
    }
    out.rows_above_one = out.total_rows - out.live_keys;
    out
}
