//! 本文件负责测试计量与机制对照。

use super::*;

/// 当前线程已发生的 value 解码次数（测试探针用）
#[cfg(test)]
pub(crate) fn value_decodes() -> u64 {
    VALUE_DECODES.with(|c| c.get())
}
