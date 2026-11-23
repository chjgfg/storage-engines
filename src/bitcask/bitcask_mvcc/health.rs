//! 只读统计版本链长度、逻辑键数量和活动事务，不执行版本回收。
//! 先释放活动事务表锁，再读取后端；扫描可见的物理行但不复制逻辑值。

use super::*;

impl MVCC {
    /// 全库健康度快照。**只走 KeyDir 元数据**（`iter_meta`），不读 value、不碰 blob。
    ///
    /// 代价是 O(物理行数) 的内存遍历，属管理面调用：别放进热路径，长跑里按分钟级采样就够。
    pub fn health(&self) -> MvccHealth {
        // 一次只持一把锁：`active_txn` 读完就放，再拿 `kv`。
        // 嵌套拿锁会和 [`MVCC::vacuum`]（`active_txn → kv → blob`）构成反向嵌套。
        let active_txns = self.active_txn.lock().unwrap().len() as u64;
        let kv = self.kv.lock().unwrap();
        // 统计完整逻辑组，二进制前缀交错也使用同一判据。
        let mut fold =
            crate::common::order::LogicalHealth::new(self.next_version.load(Ordering::SeqCst));
        for (enc, _loc) in kv.iter_meta() {
            fold.push(&enc);
        }
        fold.finish(active_txns)
    }
}
