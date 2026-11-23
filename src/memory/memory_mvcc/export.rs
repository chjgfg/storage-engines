//! 本文件负责可见记录流式导出。

use super::*;

impl MVCC {
    /// 导出当前快照下每个逻辑 key 的最新可见版本
    pub fn export_latest_visible(&self, include_deleted: bool) -> Vec<ExportRecord> {
        // M44：收集壳 —— 判据与遍历只有一份，在 `export_latest_visible_for_each`。
        // ⚠️ 返回整库的 `Vec` 是这条的合同（600k 档实测 +123.7 MB）；只需要"把行交出去"的
        // 人（导出成文件、搬去另一家、数个数）请走流式那条。
        let mut out: Vec<ExportRecord> = Vec::new();
        self.export_latest_visible_for_each(include_deleted, |rec| {
            out.push(rec);
            true
        });
        out
    }

    /// 流式导出（M44）：开一个快照事务，折一行交一行，**一行都不攒**；`on` 返回 `false`
    /// 当场收手。返回实际交出去的行数。
    ///
    /// 为什么库侧要提供这条：折叠那一段早就是流式的（M27），但出口只有返回 `Vec` 的那条 ——
    /// 于是"把 600k 键的库导成文件"必须先在内存里摆一整套 `ExportRecord`，而这套东西
    /// 存在的唯一目的就是等着被逐行写出去。`export_kv` 那几个 CLI 原先还在这之上**又**
    /// `map` 成一份 `Vec<KvRecord>` ⇒ 全库同时在内存里摆了两遍。
    pub fn export_latest_visible_for_each(
        &self,
        include_deleted: bool,
        mut on: impl FnMut(ExportRecord) -> bool,
    ) -> usize {
        let tx = self.begin_transaction();
        let handed = tx.export_latest_visible_for_each(include_deleted, &mut on);
        tx.commit();
        handed
    }
}
