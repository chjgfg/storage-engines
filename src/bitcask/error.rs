//! 统一错误类型

use std::write;
use std::fmt;
use std::io;

/// 引擎统一错误
#[derive(Debug)]
pub enum Error {
    /// 底层 IO
    Io(io::Error),
    /// MVCC 写写冲突
    Conflict,
    /// 格式/魔数/截断等
    InvalidData(String),
    /// 数据库锁被占用
    Locked(String),
    /// 其它
    Other(String),
}

impl Error {
    pub fn invalid(msg: impl Into<String>) -> Self {
        Error::InvalidData(msg.into())
    }

    pub fn other(msg: impl Into<String>) -> Self {
        Error::Other(msg.into())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "io: {e}"),
            Error::Conflict => write!(f, "write conflict"),
            Error::InvalidData(s) => write!(f, "invalid data: {s}"),
            Error::Locked(s) => write!(f, "locked: {s}"),
            Error::Other(s) => write!(f, "{s}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        if e.kind() == io::ErrorKind::WouldBlock {
            Error::Locked(e.to_string())
        } else {
            Error::Io(e)
        }
    }
}

impl From<Error> for io::Error {
    fn from(e: Error) -> Self {
        match e {
            Error::Io(e) => e,
            Error::Locked(s) => io::Error::new(io::ErrorKind::WouldBlock, s),
            Error::InvalidData(s) => io::Error::new(io::ErrorKind::InvalidData, s),
            Error::Conflict => io::Error::new(io::ErrorKind::AlreadyExists, "write conflict"),
            Error::Other(s) => io::Error::other(s),
        }
    }
}

/// 库内统一 Result
pub type Result<T> = std::result::Result<T, Error>;
