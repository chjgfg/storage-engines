//! 只读统计版本链长度、逻辑键数量和活动事务，不执行版本回收。
//! 先释放活动事务表锁，再读取后端；扫描可见的物理行但不复制逻辑值。

use super::*;

impl MVCC {
    /// 全库健康度快照。**只走惰性的轻量归并**（[`LSMTree::light_iter_range`]：版本号 + 是不是
    /// 墓碑），一趟、不物化任何 value，也不把任何一个源整篇读进 `Vec`（M39 之前它走
    /// `scan_latest_light`，那个会把每个 SST 整篇折进内存 ⇒ 瞬时 +126.1 MB @600k）。
    ///
    /// `kind == 2`（`crate::lsm_tree::lsm_tree::EntryValue::Drop`，已被硬删的行）不计入
    /// —— 和 [`MVCC::vacuum`] 看到的行集合严格一致，否则「vacuum 把 max_chain 压到 1」
    /// 这条守卫会对不上。口径与 [`crate::bitcask::MVCC::health`] 一致。
    pub fn health(&self) -> MvccHealth {
        // 一次只持一把锁：先读完 `active_txn` 计数就放，再拿 `kv`。
        // 嵌套持有会和 [`MVCC::vacuum`]（`active_txn → kv`）构成反向嵌套。
        let active_txns = self.active_txn.lock().unwrap().len() as u64;
        let kv = self.kv.lock().unwrap();
        // 统计完整逻辑组，二进制前缀交错也使用同一判据。
        let mut fold =
            crate::common::order::LogicalHealth::new(self.next_version.load(Ordering::SeqCst));
        //
        // ⚠️ M39：必须走**惰性**的 `light_iter_range`，不能走 `scan_latest_light` —— 后者以前
        // 把每个 SST 用 `SstFile::light_view` 整篇折进 `Vec`、所有源同时活着，600k 档一次
        // `health()` 瞬时 **+126.1 MB**（旧内存量具报 +0.0，直到 M38 修好尺子才看见）。
        // 守卫：`test_health_does_not_materialize_its_sources`。
        for (enc, le) in kv.light_iter_range(None, None, u64::MAX) {
            if le.kind == 2 {
                continue;
            }
            fold.push(&enc);
        }
        fold.finish(active_txns)
    }
}
