//! B+ 数据文件及旁文件路径规则。

use super::*;

// ─── 路径辅助 ───────────────────────────────────────────────────────────────

/// 数据文件 data.db 对应页双写旁文件 data.db.dblwr。
pub fn dblwr_path(db_path: &Path) -> PathBuf {
    let mut s = db_path.as_os_str().to_os_string();
    s.push(".dblwr");
    PathBuf::from(s)
}

/// 数据文件 data.db 对应事务日志 data.db.wal。
pub fn wal_path(db_path: &Path) -> PathBuf {
    let mut s = db_path.as_os_str().to_os_string();
    s.push(".wal");
    PathBuf::from(s)
}

/// 数据文件 data.db 对应空闲页清单 data.db.freelist。
pub fn freelist_path(db_path: &Path) -> PathBuf {
    let mut s = db_path.as_os_str().to_os_string();
    s.push(".freelist");
    PathBuf::from(s)
}

/// 数据文件 data.db 对应单写者锁 data.db.lock。
pub fn lock_path(db_path: &Path) -> PathBuf {
    let mut s = db_path.as_os_str().to_os_string();
    s.push(".lock");
    PathBuf::from(s)
}

/// 数据文件 data.db 对应大值旁文件 data.db.blob。
pub fn blob_path(db_path: &Path) -> PathBuf {
    let mut s = db_path.as_os_str().to_os_string();
    s.push(".blob");
    PathBuf::from(s)
}
