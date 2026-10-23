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

#[cfg(test)]
use std::thread_local;
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

// 测试专用：本线程「把带 value 的条目整个克隆出来」的次数。
//
// 内存引擎没有磁盘读也没有 WAL，所以 ② 那条通道在这里的代价单位就是 value 克隆：
// 重量路径（`iter`）每行克隆一次，轻量路径（`iter_keys`）一次都不克隆。
#[cfg(test)]
thread_local! {
    static VALUE_CLONES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// 当前线程已发生的 value 克隆次数（测试探针用）
#[cfg(test)]
pub(crate) fn value_clones() -> u64 {
    VALUE_CLONES.with(|c| c.get())
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

    /// 全表有序遍历，按**引用**把 `(键, value)` 交给回调 —— 一个 value 都不克隆。
    ///
    /// 给「只需要版本号 + 是不是墓碑」的调用方用（vacuum 就是），
    /// 它们不该为了判断 `is_none()` 而把每行 value 克隆一份。
    pub fn scan_entries_with(&self, mut f: impl FnMut(&[u8], &Option<Vec<u8>>)) {
        for (k, v) in &self.data {
            f(k, v);
        }
    }

    /// 从 `from`（**含**，落在键的字节域上）起的有序遍历，按引用交给回调；
    /// 回调返回 `true` 表示「够了，停在这里」。
    ///
    /// 起点是键域下界而不是逻辑 key：调用方若要「跳过某个逻辑 key 的整组」，
    /// 得自己按 `raw_key > from` 过滤。给有界回收（`vacuum_bounded`）用 ——
    /// 全表遍历会把 store 锁按到整库扫完，切片之后每轮只按一小段。
    pub fn scan_entries_from(
        &self,
        from: Option<&[u8]>,
        mut f: impl FnMut(&[u8], &Option<Vec<u8>>) -> bool,
    ) {
        use std::ops::Bound;
        let lo = match from {
            Some(k) => Bound::Included(k),
            None => Bound::Unbounded,
        };
        for (k, v) in self.data.range::<[u8], _>((lo, Bound::Unbounded)) {
            if f(k, v) {
                break;
            }
        }
    }

    /// 只遍历键（**不克隆 value**），供上层折叠「每个逻辑 key 的胜出行」用。
    ///
    /// `iter` 会把每一行的键与 value 都克隆出来；全库折叠只需要先比键、最后才取
    /// 少数几行的 value，所以这里刻意提供一个不带 value 的视图。
    pub fn iter_keys(&self) -> impl Iterator<Item = &Vec<u8>> {
        self.data.keys()
    }

    /// `[from, to)` 内的键有序遍历（都不给 ⇒ 等价于 [`Self::iter_keys`]）。
    ///
    /// 界打在**版本键**的字节序上（`from` 含、`to` 不含），所以调用方要自己兜住
    /// "raw_key 与界的边角"（版本键前缀交错）；提供它的理由是让「每个逻辑 key 的胜出行」
    /// 那次折叠能只走窗口内的键 —— 全表折叠出来的话，10 行的窗口和 2000 行的全量一样贵。
    pub fn iter_keys_in(
        &self,
        from: Option<&[u8]>,
        to: Option<&[u8]>,
    ) -> impl Iterator<Item = &Vec<u8>> + '_ {
        use std::ops::Bound;
        let lo = match from {
            Some(k) => Bound::Included(k),
            None => Bound::Unbounded,
        };
        let hi = match to {
            Some(k) => Bound::Excluded(k),
            None => Bound::Unbounded,
        };
        self.data
            .range::<[u8], _>((lo, hi))
            .map(|(k, _v)| k)
    }

    /// 全表有序迭代（克隆 value，教学清晰）
    ///
    /// 注意这是**重量路径**：每一行的 value 都被克隆一次。全库折叠请改用
    /// [`Self::iter_keys`] + 只对胜出行取值，别用这个。
    pub fn iter(&self) -> impl Iterator<Item = (Vec<u8>, Option<Vec<u8>>)> + '_ {
        self.data.iter().map(|(k, v)| {
            #[cfg(test)]
            VALUE_CLONES.with(|c| c.set(c.get() + 1));
            (k.clone(), v.clone())
        })
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
    use std::assert;
    use std::assert_eq;

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
