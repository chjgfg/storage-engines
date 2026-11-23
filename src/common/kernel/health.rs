//! 多版本链健康统计。

use super::*;

impl HealthFold {
    /// 喂进一行（只给 `raw_key`，不需要 value —— health 不该物化任何值）
    pub fn push(&mut self, raw_key: &[u8]) {
        self.total_rows += 1;
        if self.prev_raw.as_deref() == Some(raw_key) {
            self.cur_chain += 1;
        } else {
            self.max_chain = self.max_chain.max(self.cur_chain);
            self.live_keys += 1;
            self.cur_chain = 1;
            self.prev_raw = Some(raw_key.to_vec());
        }
    }

    /// 收尾并把活跃事务数填进去（各引擎的 `active_txn` 表是自己的，由调用方给）
    pub fn finish(mut self, active_txns: u64) -> MvccHealth {
        self.max_chain = self.max_chain.max(self.cur_chain);
        MvccHealth {
            total_rows: self.total_rows,
            live_keys: self.live_keys,
            max_chain: self.max_chain,
            rows_above_one: self.total_rows.saturating_sub(self.live_keys),
            active_txns,
        }
    }
}
