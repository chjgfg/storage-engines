//! Typed transaction outcomes. An uncertain commit must not be blindly retried.

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
    /// A write failed earlier. Roll back this transaction before retrying.
    WriteFailed(WriteError),
    TransactionClosed,
    /// Commit record or fsync failed. The transaction remains invisible and
    /// pins reclamation. Close all handles and reopen for WAL recovery.
    Indeterminate {
        version: u64,
        source: std::io::Error,
    },
    /// WAL commit succeeded; an optional flush/checkpoint failed afterwards.
    /// The transaction is committed. Do not retry its writes.
    CommittedMaintenance {
        version: u64,
        source: std::io::Error,
    },
    /// A third-party KvTxn has not implemented a checked commit contract.
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
