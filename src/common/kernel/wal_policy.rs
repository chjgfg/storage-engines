//! WAL 阈值与 checkpoint 策略。

use super::*;

impl WalPolicy {
    /// 第一次写之前调用：把"这条事务进过 WAL"记下来。幂等 —— 调几次都是同一个状态。
    ///
    /// M25 之前它返回"是不是第一次"，因为第一次要补一条 `Begin`；`Begin` 不再落盘之后
    /// 这个返回值没有调用方了，所以改成纯粹的置位（留着返回 `bool` 只会让人以为
    /// "返回 true 的那一次要做点什么"）。
    pub fn mark_opened(&self) {
        self.opened.set(true);
    }

    /// 本事务是否进过 WAL ⇒ `commit` 要不要写 `Commit` 并 fsync、`rollback` 要不要写 `Abort`
    pub fn opened(&self) -> bool {
        self.opened.get()
    }
}
