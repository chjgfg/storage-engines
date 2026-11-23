//! 本文件负责统一门面的错误类型。

use super::*;

/// 门面层错误
#[derive(Debug)]
pub enum EngineError {
    /// 引擎名字不认识（见 [`ENGINE_NAMES`]）
    UnknownEngine(String),
    /// 该引擎不支持此操作（如 `memory` 不能持久化、非 bitcask 不能 merge）
    Unsupported {
        engine: &'static str,
        op: &'static str,
    },
    /// 这个引擎必须落盘，但调用方没给路径
    PathRequired(&'static str),
    /// [`migrate`] 中止：目标侧拒收这一行（键/值装不进一页，或写冲突）
    MigrateAborted {
        key: Vec<u8>,
        reason: &'static str,
    },
    Io(std::io::Error),
}

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EngineError::UnknownEngine(name) => write!(
                f,
                "unknown engine {name:?}; expected one of {}",
                ENGINE_NAMES.join(" / ")
            ),
            EngineError::Unsupported { engine, op } => {
                write!(f, "engine {engine} does not support {op}")
            }
            EngineError::PathRequired(engine) => {
                write!(f, "engine {engine} requires a database path")
            }
            EngineError::MigrateAborted { key, reason } => {
                write!(f, "migrate aborted at key {key:?}: {reason}")
            }
            EngineError::Io(e) => write!(f, "io error: {e}"),
        }
    }
}

impl std::error::Error for EngineError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            EngineError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for EngineError {
    fn from(e: std::io::Error) -> Self {
        EngineError::Io(e)
    }
}

pub type Result<T> = std::result::Result<T, EngineError>;
