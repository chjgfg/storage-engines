//! 内存存储层：有序多版本 KV（替代 bplus-tree 的 B+ 树 + 页 + WAL）。
//!
//! ## 键约定
//!
//! 与 MVCC 层约定一致：`enc_key = raw_key || version.to_be_bytes()`。
//! 本层只认「字节序键 → 可选 value」，**不理解** 版本 / 可见性 / tombstone 语义。
//!
//! ## 值约定
//!
//! - `Some(bytes)`：逻辑 value（可能含 TTL 信封，由 kv_ops 解析）
//! - `None`：tombstone（逻辑删除）

use std::collections::BTreeMap;
use std::ops::Bound;

// 测试专用：本线程被访问的**行数**（`range_scan` / `range_back` 返回的行数累加）。
//
// 内存引擎没有磁盘读，所以「代价」的对应物就是扫过多少行 —— 用来把
// 「一次点操作的代价不随版本链长度增长」这条不变量钉成断言。
#[cfg(test)]
thread_local! {
    static ROWS_TOUCHED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// 当前线程已访问的行数（测试探针用）
#[cfg(test)]
pub(crate) fn rows_touched() -> u64 {
    ROWS_TOUCHED.with(|c| c.get())
}

#[cfg(test)]
fn count_rows(n: usize) {
    ROWS_TOUCHED.with(|c| c.set(c.get() + n as u64));
}

/// 内存有序多版本表
#[derive(Debug, Default, Clone)]
pub struct MemStore {
    /// enc_key → logical value（None = tombstone）
    data: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

impl MemStore {
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
        }
    }

    /// 插入 / 覆盖一行（含 tombstone）
    pub fn insert(&mut self, key: Vec<u8>, value: Option<Vec<u8>>) {
        self.data.insert(key, value);
    }

    /// 删除精确 key（编码后的版本键），返回是否存在
    pub fn delete(&mut self, key: &[u8]) -> bool {
        self.data.remove(key).is_some()
    }

    /// 精确查找
    pub fn get(&self, key: &[u8]) -> Option<&Option<Vec<u8>>> {
        self.data.get(key)
    }

    /// 区间扫描 **[low, high]**（两端包含）
    ///
    /// 与 bplus `range_scan` 对齐：调用方用 `encode(key,0)` / `encode(key,MAX)` 圈住同 raw_key 的全部版本。
    pub fn range_scan(&self, low: &[u8], high: &[u8]) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        let rows = self
            .data
            .range((Bound::Included(low.to_vec()), Bound::Included(high.to_vec())))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<Vec<_>>();
        #[cfg(test)]
        count_rows(rows.len());
        rows
    }

    /// **[low, high] 内从大到小最多取 `max` 行**（区间末尾，不物化整段）。
    ///
    /// 返回行数 `< max` 说明已走到区间下界 —— 调用方由此可以断定「整段都看过了」，
    /// 从而在只需要区间末尾几行时提前退出。
    ///
    /// MVCC 用它取版本链的**链尾**：版本键 `raw_key ‖ version.to_be_bytes()` 大端有序，
    /// 所以同一逻辑 key 的最新版本一定排在最后。
    pub fn range_back(&self, low: &[u8], high: &[u8], max: usize) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        let rows = self
            .data
            .range((Bound::Included(low.to_vec()), Bound::Included(high.to_vec())))
            .rev()
            .take(max)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<Vec<_>>();
        #[cfg(test)]
        count_rows(rows.len());
        rows
    }

    /// 只遍历键（**不克隆 value**），供上层折叠「每个逻辑 key 的胜出行」用。
    ///
    /// `iter` 会把每一行的键与 value 都克隆出来；全库折叠只需要先比键、最后才取
    /// 少数几行的 value，所以这里刻意提供一个不带 value 的视图。
    pub fn iter_keys(&self) -> impl Iterator<Item = &Vec<u8>> {
        self.data.keys()
    }

    /// 全表有序迭代（克隆 value，教学清晰）
    pub fn iter(&self) -> impl Iterator<Item = (Vec<u8>, Option<Vec<u8>>)> + '_ {
        self.data.iter().map(|(k, v)| (k.clone(), v.clone()))
    }

    /// 行数（含 tombstone 与历史版本）
    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// 清空
    pub fn clear(&mut self) {
        self.data.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_get_delete() {
        let mut s = MemStore::new();
        s.insert(b"a".to_vec(), Some(b"1".to_vec()));
        assert_eq!(s.get(b"a"), Some(&Some(b"1".to_vec())));
        s.insert(b"a".to_vec(), None);
        assert_eq!(s.get(b"a"), Some(&None));
        assert!(s.delete(b"a"));
        assert_eq!(s.get(b"a"), None);
    }

    #[test]
    fn test_range_scan_order() {
        let mut s = MemStore::new();
        s.insert(b"a\x00\x00\x00\x00\x00\x00\x00\x01".to_vec(), Some(b"v1".to_vec()));
        s.insert(b"a\x00\x00\x00\x00\x00\x00\x00\x02".to_vec(), Some(b"v2".to_vec()));
        s.insert(b"b\x00\x00\x00\x00\x00\x00\x00\x01".to_vec(), Some(b"x".to_vec()));

        let low = b"a\x00\x00\x00\x00\x00\x00\x00\x00".to_vec();
        let high = b"a\xff\xff\xff\xff\xff\xff\xff\xff".to_vec();
        let rows = s.range_scan(&low, &high);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].1, Some(b"v1".to_vec()));
        assert_eq!(rows[1].1, Some(b"v2".to_vec()));
    }
}
