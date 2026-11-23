//! 本文件负责对象安全的批量载入公开契约。

/// 批量导入会话：无冲突检测、无 WAL，**单写者**假定。
///
/// 结束时必须 [`KvBulk::finish`]（`Drop` 也会兜底，但不保证 flush 顺序）。
pub trait KvBulk: Send {
    fn put(&mut self, key: &[u8], value: Vec<u8>);

    fn put_batch(&mut self, items: &[(Vec<u8>, Vec<u8>)]);

    /// 省一次 clone 的批量写
    fn put_batch_owned(&mut self, items: Vec<(Vec<u8>, Vec<u8>)>);

    fn delete(&mut self, key: &[u8]);

    fn get(&self, key: &[u8]) -> Option<Vec<u8>>;

    /// 消费式收尾。`Box<Self>` 让它在 `dyn` 下仍可调用。
    fn finish(self: Box<Self>);
}
