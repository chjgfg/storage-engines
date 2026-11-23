//! 事务结果由明确类型表达；不确定提交不能盲目重试。

use std::{fmt, write};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteError {
    Conflict,
    RowTooLarge,
    ValueEncodingFailed,
    WalWriteFailed,
    StorageWriteFailed,
    TransactionClosed,
    Unsupported,
}

impl fmt::Display for WriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for WriteError {}

#[derive(Debug)]
pub enum CommitError {
    /// 本事务此前发生写入失败，重试前必须先回滚。
    WriteFailed(WriteError),
    TransactionClosed,
    /// 提交记录或同步失败时事务仍不可见，并阻止相关版本被回收；关闭全部句柄后重开，依靠 WAL 恢复。
    Indeterminate {
        version: u64,
        source: std::io::Error,
    },
    /// WAL 提交成功后，可选刷盘或 checkpoint 才失败；事务已经提交，不得重试其写入。
    CommittedMaintenance {
        version: u64,
        source: std::io::Error,
    },
    /// 第三方 KvTxn 尚未实现可返回错误的提交契约。
    Unsupported,
}

impl fmt::Display for CommitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WriteFailed(e) => {
                write!(f, "transaction has a failed write: {e}; rollback required")
            }
            Self::TransactionClosed => write!(f, "transaction is closed"),
            Self::Indeterminate { version, source } => write!(
                f,
                "commit {version} is indeterminate: {source}; reopen and reconcile before retrying"
            ),
            Self::CommittedMaintenance { version, source } => write!(
                f,
                "transaction {version} committed, but maintenance failed: {source}; do not retry writes"
            ),
            Self::Unsupported => write!(f, "checked commit is not supported by this transaction"),
        }
    }
}
impl std::error::Error for CommitError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Indeterminate { source, .. } | Self::CommittedMaintenance { source, .. } => {
                Some(source)
            }
            _ => None,
        }
    }
}
