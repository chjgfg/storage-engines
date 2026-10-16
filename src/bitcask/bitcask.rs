//! Bitcask：append-only 日志 + 内存索引（KeyDir）
//!
//! ## 记录布局
//! ```text
//! +-------------+-------------+----------------+----------------+
//! | key_len(4)  | val_flag(4) | key(var)       | value(var)     |
//! +-------------+-------------+----------------+----------------+
//! ```
//!
//! `val_flag`（i32 BE）：
//! - `>= 0`：存活 value，长度为该值
//! - `-1`：**硬删除**（从 KeyDir 移除）— 经典 Bitcask `delete` / MVCC `remove`
//! - `-2`：**软 tombstone**（KeyDir 保留，value 视为 `None`）— MVCC 逻辑删除的版本行
//!
//! ## 性能
//!
//! - 写路径维护 `end_pos` + 内存写缓冲（默认 4MB），攒满再一次 `write_all`
//! - [`Bitcask::insert_batch`]：批量编码后一次刷缓冲，适合 1000 万级 bulk
//! - 读前自动 flush 缓冲，保证 offset 可读
//! - **Hint 文件**（`*.hint`）：merge / [`Bitcask::dump_hint`] 写出 KeyDir 快照；
//!   open 优先读 hint，再只扫 `log_size..end` 增量补索引，避免全量扫 value

use fs4::FileExt;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::matches;
use std::ops::Bound;
use std::path::{Path, PathBuf};

const KEY_VAL_HEADER_LEN: u32 = 4;
const MERGE_FILE_EXT: &str = "merge";
/// 写缓冲目标大小：攒够再落盘，减少 syscall
const WRITE_BUF_TARGET: usize = 4 * 1024 * 1024;
// ─── Hint 文件格式 ───────────────────────────────────────────────────────────
//
// ```text
// HINT0001（旧，兼容）：无 file_id，一律视为 file_id=0
// header (32B):
//   magic[8] = "HINT0001"
//   log_size:u64 LE
//   entry_count:u64 LE
//   reserved:u64
// entry: key_len|flag|pos|len|key
//
// HINT0002（多分片）：
// header (40B):
//   magic[8] = "HINT0002"
//   active_id:u32 LE | pad:u32
//   active_log_size:u64 LE
//   entry_count:u64 LE
//   reserved:u64
// entry: key_len|flag|file_id:u32|pos:u64|len:u32|key
// ```

const HINT_MAGIC_V1: &[u8; 8] = b"HINT0001";
const HINT_MAGIC_V2: &[u8; 8] = b"HINT0002";
const HINT_HEADER_SIZE_V1: usize = 32;
const HINT_HEADER_SIZE_V2: usize = 40;
/// hint 条目中 Live 的 flag 标记
const HINT_FLAG_LIVE: i32 = 0;

/// 默认单个 data 分片软上限（超过则 rotate）
pub const DEFAULT_MAX_FILE_BYTES: u64 = 64 * 1024 * 1024;

/// 硬删除：从 KeyDir 抹掉
const FLAG_HARD_DELETE: i32 = -1;
/// 软 tombstone：KeyDir 保留，value = None（MVCC 版本行）
const FLAG_SOFT_TOMBSTONE: i32 = -2;

/// 索引中一条键的位置信息
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyLoc {
    /// 存活：分片 file_id + value offset + 长度
    Live {
        file_id: u32,
        pos: u64,
        len: u32,
    },
    /// 软删除：版本行仍存在，逻辑 value 为 None
    SoftTombstone { file_id: u32 },
}

impl KeyLoc {
    /// 是不是软墓碑（版本行还在，逻辑 value 为 None）。
    ///
    /// 给只需要「这一行存不存在 value」的调用方用 —— 比如 vacuum 判「要不要连这行一起删」，
    /// 它不需要 value 内容，不该为此读盘。
    pub fn is_soft_tombstone(&self) -> bool {
        matches!(self, KeyLoc::SoftTombstone { .. })
    }
}

/// 内存索引：key → 最新位置
pub type KeyDir = std::collections::BTreeMap<Vec<u8>, KeyLoc>;

// 测试专用：本线程 `Log::read_value` 的调用次数，即**真实读盘行数**。
//
// 用来把「一次操作的代价不随版本链长度增长」这条不变量钉成断言 —— 计时断言在不同
// 机器上会飘，读盘次数不会。做成 thread_local 是为了不和 `cargo test` 的并行用例互相计数。
#[cfg(test)]
thread_local! {
    static READ_VALUE_CALLS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// 当前线程已发生的「读一行盘」次数（测试探针用）
#[cfg(test)]
pub(crate) fn read_value_calls() -> u64 {
    READ_VALUE_CALLS.with(|c| c.get())
}

// 测试专用：一轮候选折叠里**看了多少把编码键**（不是读了几次盘）。
// 为什么必须有它：`read_value_calls()` 只数取值，而窗口分页要证明的是"键流没走到底"——
// 第一版守卫就是拿取值次数当尺子，把预算整个忽略掉它照样绿。
#[cfg(test)]
thread_local! {
    static KEYDIR_VISITS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn note_keydir_visit() {
    KEYDIR_VISITS.with(|c| c.set(c.get() + 1));
}

/// 当前线程折候选时检查过的编码键数（测试探针用）
#[cfg(test)]
pub(crate) fn keydir_visits() -> u64 {
    KEYDIR_VISITS.with(|c| c.get())
}

/// IO 结果
pub type Result<T> = std::result::Result<T, std::io::Error>;

/// 把一条日志记录应用到 KeyDir
fn apply_log_record(
    key_dir: &mut KeyDir,
    file_id: u32,
    key: Vec<u8>,
    value_pos: u64,
    flag: i32,
    value_len: u32,
) {
    if flag >= 0 {
        key_dir.insert(
            key,
            KeyLoc::Live {
                file_id,
                pos: value_pos,
                len: value_len,
            },
        );
    } else if flag == FLAG_SOFT_TOMBSTONE {
        key_dir.insert(key, KeyLoc::SoftTombstone { file_id });
    } else {
        key_dir.remove(&key);
    }
}

/// 由数据文件路径推导 hint：`foo.log` → `foo.hint`
pub fn hint_path_for(data_path: &Path) -> PathBuf {
    data_path.with_extension("hint")
}

/// 密封分片路径：`data.log` + id → `data.{id}.log`（active 始终是 `data.log` 本身）
pub fn segment_path(active_path: &Path, file_id: u32) -> PathBuf {
    // data.log → data.0.log / data.3.log
    let stem = active_path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("data");
    let parent = active_path.parent().unwrap_or_else(|| Path::new("."));
    parent.join(format!("{stem}.{file_id}.log"))
}

/// 扫描目录中已有密封分片 id（不含 active 文件本身）
fn discover_sealed_ids(active_path: &Path) -> Vec<u32> {
    let stem = active_path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("data");
    let parent = match active_path.parent() {
        Some(p) => p,
        None => return Vec::new(),
    };
    let mut ids = Vec::new();
    let Ok(rd) = std::fs::read_dir(parent) else {
        return ids;
    };
    let prefix = format!("{stem}.");
    for ent in rd.flatten() {
        let name = ent.file_name();
        let name = name.to_string_lossy();
        // data.12.log
        if let Some(rest) = name.strip_prefix(&prefix) {
            if let Some(num) = rest.strip_suffix(".log") {
                if let Ok(id) = num.parse::<u32>() {
                    ids.push(id);
                }
            }
        }
    }
    ids.sort_unstable();
    ids
}

/// hint 里的每条记录是否都指向**还在**的文件（active 或某个密封分片）。
///
/// 崩在 `merge()` 中途会留下这种 hint：它记的是合并前的布局，而那批 `data.{id}.log`
/// 已经被删掉了。增量扫只补 active 文件里新增的行，指向失踪分片的键**永远不会被补回来**
/// ⇒ 悄悄少数据，比慢严重。所以这里当作 hint 失效处理，回退全量扫。
fn hint_files_exist(kd: &KeyDir, active_id: u32, sealed_ids: &[u32]) -> bool {
    kd.values().all(|loc| {
        let fid = match loc {
            KeyLoc::Live { file_id, .. } => *file_id,
            KeyLoc::SoftTombstone { file_id } => *file_id,
        };
        fid == active_id || sealed_ids.contains(&fid)
    })
}

/// 写出 hint v2（原子）
pub fn write_hint_file(
    path: &Path,
    key_dir: &KeyDir,
    active_id: u32,
    active_log_size: u64,
) -> Result<()> {
    let tmp = {
        let mut p = path.as_os_str().to_os_string();
        p.push(".tmp");
        PathBuf::from(p)
    };
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            let _ = std::fs::create_dir_all(parent);
        }
    }
    {
        let f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)?;
        let mut w = BufWriter::new(f);
        let count = key_dir.len() as u64;
        let mut hdr = [0u8; HINT_HEADER_SIZE_V2];
        hdr[0..8].copy_from_slice(HINT_MAGIC_V2);
        hdr[8..12].copy_from_slice(&active_id.to_le_bytes());
        // pad 12..16 = 0
        hdr[16..24].copy_from_slice(&active_log_size.to_le_bytes());
        hdr[24..32].copy_from_slice(&count.to_le_bytes());
        // reserved 32..40 = 0
        w.write_all(&hdr)?;

        for (key, loc) in key_dir.iter() {
            let (flag, file_id, pos, len) = match loc {
                KeyLoc::Live {
                    file_id,
                    pos,
                    len,
                } => (HINT_FLAG_LIVE, *file_id, *pos, *len),
                KeyLoc::SoftTombstone { file_id } => {
                    (FLAG_SOFT_TOMBSTONE, *file_id, 0u64, 0u32)
                }
            };
            w.write_all(&(key.len() as u32).to_le_bytes())?;
            w.write_all(&flag.to_le_bytes())?;
            w.write_all(&file_id.to_le_bytes())?;
            w.write_all(&pos.to_le_bytes())?;
            w.write_all(&len.to_le_bytes())?;
            w.write_all(key)?;
        }
        w.flush()?;
        w.into_inner()
            .map_err(|e| e.into_error())?
            .sync_all()?;
    }
    if path.exists() {
        let _ = std::fs::remove_file(path);
    }
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// 读取 hint。成功返回 `(KeyDir, active_id, active_log_size)`
pub fn read_hint_file(path: &Path) -> Result<(KeyDir, u32, u64)> {
    let mut f = std::fs::File::open(path)?;
    let mut magic = [0u8; 8];
    f.read_exact(&mut magic)?;
    if &magic == HINT_MAGIC_V2 {
        let mut rest = [0u8; HINT_HEADER_SIZE_V2 - 8];
        f.read_exact(&mut rest)?;
        let active_id = u32::from_le_bytes(rest[0..4].try_into().unwrap());
        let active_log_size = u64::from_le_bytes(rest[8..16].try_into().unwrap());
        let entry_count = u64::from_le_bytes(rest[16..24].try_into().unwrap());
        let mut key_dir = KeyDir::new();
        let mut r = BufReader::new(f);
        for _ in 0..entry_count {
            let mut nbuf = [0u8; 4];
            r.read_exact(&mut nbuf)?;
            let key_len = u32::from_le_bytes(nbuf) as usize;
            r.read_exact(&mut nbuf)?;
            let flag = i32::from_le_bytes(nbuf);
            r.read_exact(&mut nbuf)?;
            let file_id = u32::from_le_bytes(nbuf);
            let mut pbuf = [0u8; 8];
            r.read_exact(&mut pbuf)?;
            let pos = u64::from_le_bytes(pbuf);
            r.read_exact(&mut nbuf)?;
            let len = u32::from_le_bytes(nbuf);
            if key_len > 16 * 1024 * 1024 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "hint key_len 异常",
                ));
            }
            let mut key: Vec<u8> = vec![0u8; key_len];
            r.read_exact(&mut key)?;
            match flag {
                HINT_FLAG_LIVE => {
                    key_dir.insert(
                        key,
                        KeyLoc::Live {
                            file_id,
                            pos,
                            len,
                        },
                    );
                }
                FLAG_SOFT_TOMBSTONE => {
                    key_dir.insert(key, KeyLoc::SoftTombstone { file_id });
                }
                _ => {}
            }
        }
        Ok((key_dir, active_id, active_log_size))
    } else if &magic == HINT_MAGIC_V1 {
        // 兼容旧 hint：file_id 全 0，header 剩余 24 字节
        let mut rest = [0u8; HINT_HEADER_SIZE_V1 - 8];
        f.read_exact(&mut rest)?;
        let log_size = u64::from_le_bytes(rest[0..8].try_into().unwrap());
        let entry_count = u64::from_le_bytes(rest[8..16].try_into().unwrap());
        let mut key_dir = KeyDir::new();
        let mut r = BufReader::new(f);
        for _ in 0..entry_count {
            let mut nbuf = [0u8; 4];
            r.read_exact(&mut nbuf)?;
            let key_len = u32::from_le_bytes(nbuf) as usize;
            r.read_exact(&mut nbuf)?;
            let flag = i32::from_le_bytes(nbuf);
            let mut pbuf = [0u8; 8];
            r.read_exact(&mut pbuf)?;
            let pos = u64::from_le_bytes(pbuf);
            r.read_exact(&mut nbuf)?;
            let len = u32::from_le_bytes(nbuf);
            let mut key: Vec<u8> = vec![0u8; key_len];
            r.read_exact(&mut key)?;
            match flag {
                HINT_FLAG_LIVE => {
                    key_dir.insert(
                        key,
                        KeyLoc::Live {
                            file_id: 0,
                            pos,
                            len,
                        },
                    );
                }
                FLAG_SOFT_TOMBSTONE => {
                    key_dir.insert(key, KeyLoc::SoftTombstone { file_id: 0 });
                }
                _ => {}
            }
        }
        Ok((key_dir, 0, log_size))
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "非法 hint 魔数",
        ))
    }
}

pub struct Log {
    path: PathBuf,
    file: File,
    /// 逻辑文件末尾（含未 flush 的缓冲字节）
    end_pos: u64,
    /// 待落盘写缓冲
    buf: Vec<u8>,
}

impl Drop for Log {
    fn drop(&mut self) {
        let _ = self.flush_buf();
    }
}

impl Log {
    pub fn new(path: PathBuf) -> Result<Self> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;
        // 排他锁，防止并发写同一数据文件
        file.try_lock_exclusive()?;
        let end_pos = file.metadata()?.len();
        Ok(Self {
            path,
            file,
            end_pos,
            // 缓冲**首次写入时**才按目标大小分配：只读/空库不该一打开就留 4 MiB
            // （基线实测：两处各 4 MiB ⇒ `open 空库` 就"常驻 8.4 MB"）
            buf: Vec::new(),
        })
    }

    /// 把写缓冲落盘（不 fsync）
    pub fn flush_buf(&mut self) -> Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        // end_pos 已在 append 时推进；实际文件长度 = end_pos - buf.len()
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&self.buf)?;
        self.buf.clear();
        Ok(())
    }

    /// 从 `from` 扫到 `to`，把记录合并进 `key_dir`（后写覆盖先写）
    fn scan_log_into(
        &mut self,
        key_dir: &mut KeyDir,
        file_id: u32,
        from: u64,
        to: u64,
    ) -> Result<()> {
        if from >= to {
            return Ok(());
        }
        let mut len_buf = [0u8; KEY_VAL_HEADER_LEN as usize];
        let mut r = BufReader::new(&mut self.file);
        let mut pos: u64 = r.seek(SeekFrom::Start(from))?;

        while pos < to {
            let read_one = || -> Result<(Vec<u8>, u64, i32, u32)> {
                r.read_exact(&mut len_buf)?;
                let key_len = u32::from_be_bytes(len_buf);
                r.read_exact(&mut len_buf)?;
                let flag = i32::from_be_bytes(len_buf);
                let value_pos = pos + KEY_VAL_HEADER_LEN as u64 * 2 + key_len as u64;

                let mut key: Vec<u8> = vec![0; key_len as usize];
                r.read_exact(&mut key)?;

                let value_len = if flag >= 0 { flag as u32 } else { 0 };
                if flag >= 0 {
                    r.seek_relative(value_len as i64)?;
                }
                Ok((key, value_pos, flag, value_len))
            }();

            match read_one {
                Ok((key, value_pos, flag, value_len)) => {
                    apply_log_record(key_dir, file_id, key, value_pos, flag, value_len);
                    if flag >= 0 {
                        pos = value_pos + value_len as u64;
                    } else {
                        pos = value_pos;
                    }
                }
                Err(err) => return Err(err),
            }
        }
        Ok(())
    }

    /// 扫描整文件构建 KeyDir（先 flush 缓冲）
    pub fn load_index(&mut self, file_id: u32) -> Result<KeyDir> {
        self.flush_buf()?;
        let file_len = self.file.metadata()?.len();
        self.end_pos = file_len;
        let mut key_dir = KeyDir::new();
        self.scan_log_into(&mut key_dir, file_id, 0, file_len)?;
        Ok(key_dir)
    }

    /// 从 `from_offset` 扫到文件末尾，增量更新 KeyDir
    pub fn load_index_from(
        &mut self,
        key_dir: &mut KeyDir,
        file_id: u32,
        from_offset: u64,
    ) -> Result<()> {
        self.flush_buf()?;
        let file_len = self.file.metadata()?.len();
        self.end_pos = file_len;
        if from_offset > file_len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("hint log_size={from_offset} > data.log len={file_len}"),
            ));
        }
        self.scan_log_into(key_dir, file_id, from_offset, file_len)
    }

    fn read_value(&mut self, value_pos: u64, value_len: u32) -> Result<Vec<u8>> {
        #[cfg(test)]
        READ_VALUE_CALLS.with(|c| c.set(c.get() + 1));
        // 读路径前必须 flush，offset 才落在真实文件上
        self.flush_buf()?;
        let mut value: Vec<u8> = vec![0; value_len as usize];
        self.file.seek(SeekFrom::Start(value_pos))?;
        self.file.read_exact(&mut value)?;
        Ok(value)
    }

    /// 编码一条记录到 `out`，返回 (value_pos, value_len_or_0, flag)
    fn encode_entry_to(
        out: &mut Vec<u8>,
        base_offset: u64,
        key: &[u8],
        value: Option<&[u8]>,
        hard_delete: bool,
    ) -> (u64, u32, i32) {
        let key_len = key.len() as u32;
        let (flag, value_len, payload): (i32, u32, Option<&[u8]>) = if hard_delete {
            (FLAG_HARD_DELETE, 0, None)
        } else {
            match value {
                Some(v) => (v.len() as i32, v.len() as u32, Some(v)),
                None => (FLAG_SOFT_TOMBSTONE, 0, None),
            }
        };
        let value_pos = base_offset + KEY_VAL_HEADER_LEN as u64 * 2 + key_len as u64;
        out.extend_from_slice(&key_len.to_be_bytes());
        out.extend_from_slice(&flag.to_be_bytes());
        out.extend_from_slice(key);
        if let Some(v) = payload {
            out.extend_from_slice(v);
        }
        (value_pos, value_len, flag)
    }

    /// 追加一条记录到写缓冲。返回 `(value_pos, value_len)`（硬删/软 tomb 时 len=0）
    fn append_entry(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        hard_delete: bool,
    ) -> Result<(u64, u32, i32)> {
        let base = self.end_pos;
        let before = self.buf.len();
        // 攒够 WRITE_BUF_TARGET 才落盘的规矩不变，但缓冲改成**首次写入时**才一次到位：
        // 既不让只读/空库白留 4 MiB，也不让大写入反复 realloc（flush_buf 是 clear，容量保留）
        if self.buf.capacity() < WRITE_BUF_TARGET {
            self.buf.reserve(WRITE_BUF_TARGET.saturating_sub(before));
        }
        let (value_pos, value_len, flag) =
            Self::encode_entry_to(&mut self.buf, base, key, value, hard_delete);
        let written = (self.buf.len() - before) as u64;
        self.end_pos += written;
        if self.buf.len() >= WRITE_BUF_TARGET {
            self.flush_buf()?;
        }
        Ok((value_pos, value_len, flag))
    }

    /// 经典：`value=None` → 硬删除
    pub fn write_entry(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<(u64, u32)> {
        match value {
            Some(v) => {
                let (pos, len, _) = self.append_entry(key, Some(v), false)?;
                // 兼容旧 API：返回 (entry_offset, entry_total_len)
                let entry_len = KEY_VAL_HEADER_LEN * 2 + key.len() as u32 + len;
                let offset = pos - KEY_VAL_HEADER_LEN as u64 * 2 - key.len() as u64;
                Ok((offset, entry_len))
            }
            None => self.write_hard_delete(key),
        }
    }

    pub fn write_hard_delete(&mut self, key: &[u8]) -> Result<(u64, u32)> {
        let (pos, _, _) = self.append_entry(key, None, true)?;
        let entry_len = KEY_VAL_HEADER_LEN * 2 + key.len() as u32;
        let offset = pos - KEY_VAL_HEADER_LEN as u64 * 2 - key.len() as u64;
        Ok((offset, entry_len))
    }

    pub fn write_soft_tombstone(&mut self, key: &[u8]) -> Result<(u64, u32)> {
        let (pos, _, _) = self.append_entry(key, None, false)?;
        let entry_len = KEY_VAL_HEADER_LEN * 2 + key.len() as u32;
        let offset = pos - KEY_VAL_HEADER_LEN as u64 * 2 - key.len() as u64;
        Ok((offset, entry_len))
    }
}

/// 经典扫描迭代器：只返回存活键（跳过软 tombstone）
///
/// 教学实现：先物化匹配的 KeyLoc，再按需从多分片读 value。
pub struct ScanIterator<'a> {
    items: Vec<(Vec<u8>, KeyLoc)>,
    idx: usize,
    rev: bool,
    eng: &'a mut Bitcask,
}

impl<'a> ScanIterator<'a> {
    fn read_item(&mut self, key: Vec<u8>, loc: KeyLoc) -> Option<<Self as Iterator>::Item> {
        match loc {
            KeyLoc::SoftTombstone { .. } => None,
            KeyLoc::Live {
                file_id,
                pos,
                len,
            } => {
                let value = self.eng.read_live(file_id, pos, len);
                Some(value.map(|v| (key, v)))
            }
        }
    }
}

impl<'a> Iterator for ScanIterator<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.rev {
            // 若混用 next/next_back，按当前位置前进
            while self.idx < self.items.len() {
                let (key, loc) = self.items[self.idx].clone();
                self.idx += 1;
                if let Some(mapped) = self.read_item(key, loc) {
                    return Some(mapped);
                }
            }
            return None;
        }
        while self.idx < self.items.len() {
            let (key, loc) = self.items[self.idx].clone();
            self.idx += 1;
            if let Some(mapped) = self.read_item(key, loc) {
                return Some(mapped);
            }
        }
        None
    }
}

impl<'a> DoubleEndedIterator for ScanIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.rev = true;
        while !self.items.is_empty() && self.idx < self.items.len() {
            // 从末尾弹：items[idx..len] 为尚未消费区间
            let last = self.items.len() - 1;
            if last < self.idx {
                break;
            }
            let (key, loc) = self.items.remove(last);
            if let Some(mapped) = self.read_item(key, loc) {
                return Some(mapped);
            }
        }
        None
    }
}

pub struct Bitcask {
    /// Active 路径（始终是 `data.log` 等 stem 路径）
    base_path: PathBuf,
    active: Log,
    active_id: u32,
    key_dir: KeyDir,
    max_file_bytes: u64,
    /// 已打开的密封分片（lazy）
    sealed: std::collections::HashMap<u32, Log>,
}

impl Drop for Bitcask {
    fn drop(&mut self) {
        if let Err(e) = self.flush() {
            log::error!("failed to flush file: {e:?}");
        }
        // 进程退出时尽量写出 hint，加速下次 open（失败只记日志）
        if let Err(e) = self.dump_hint() {
            log::error!("dump_hint on drop failed: {e:?}");
        }
    }
}

impl Bitcask {
    /// 打开/创建数据文件并构建 KeyDir（默认分片上限 [`DEFAULT_MAX_FILE_BYTES`]）。
    pub fn new(path: PathBuf) -> Result<Self> {
        Self::with_max_file_bytes(path, DEFAULT_MAX_FILE_BYTES)
    }

    /// 指定单个 active 分片软上限（字节）；写满后 rotate。
    pub fn with_max_file_bytes(path: PathBuf, max_file_bytes: u64) -> Result<Self> {
        let base_path = path;
        // 先认回收崩在中间的 merge 产物，再谈别的：`discover_sealed_ids` 与后面的
        // `Log::new(data.log)` 都会**创建**缺失的文件，一旦让它们在恢复之前跑，
        // 「data.log 被删了、真数据还在 data.merge 里」这个现场就被永久抹平 ——
        // 表现是打开一个空库，谁都不报错。
        Self::recover_merge_artifact(&base_path)?;
        let sealed_ids = discover_sealed_ids(&base_path);
        let hint_path = hint_path_for(&base_path);

        // 尝试 hint 快路径
        let mut from_hint: Option<(KeyDir, u32, u64)> = None;
        if let Ok((kd, active_id, active_size)) = read_hint_file(&hint_path) {
            from_hint = Some((kd, active_id, active_size));
        }

        let (key_dir, active_id, active) = if let Some((mut kd, active_id, active_size)) = from_hint
        {
            let mut active = Log::new(base_path.clone())?;
            let file_len = active.file.metadata()?.len();
            if active_size > file_len {
                log::warn!(
                    "hint active_size={} > data.log len={}，回退全量扫",
                    active_size,
                    file_len
                );
                drop(active);
                Self::full_rebuild(&base_path, &sealed_ids)?
            } else if let Err(e) = active.load_index_from(&mut kd, active_id, active_size) {
                log::warn!("hint 增量扫失败: {e}，回退全量扫");
                drop(active);
                Self::full_rebuild(&base_path, &sealed_ids)?
            } else if !hint_files_exist(&kd, active_id, &sealed_ids) {
                // hint 指的是向已经不在的分片 ⇒ 增量扫只补 active 里新增的行，那些行**永远不在**
                // 这次扫描里，于是键悄悄变少 —— 比启动慢严重得多，所以退回全量扫重建。
                log::warn!("hint 引用了已不存在的分片，回退全量扫（崩在 merge 中间会留这种 hint）");
                drop(active);
                Self::full_rebuild(&base_path, &sealed_ids)?
            } else {
                (kd, active_id, active)
            }
        } else {
            Self::full_rebuild(&base_path, &sealed_ids)?
        };

        Ok(Self {
            base_path,
            active,
            active_id,
            key_dir,
            max_file_bytes,
            sealed: std::collections::HashMap::new(),
        })
    }

    /// 认回 / 清理崩在中间的 merge 产物，让 `merge()` 里那几步删除变得可恢复。
    ///
    /// `merge()` 的收口是「删密封分片 → 删旧 `data.log` → 把 `data.merge` 改名成 `data.log`」。
    /// 第三步之前崩掉，磁盘上就只剩 `data.merge` 装着全部存活记录，而 open 找的是 `data.log`
    /// —— 原样打开会得到一个**空库**：文件都在，谁都不报错，数据却"没了"。
    ///
    /// 两种现场，两种处置：
    /// - `data.log` 不在、`data.merge` 在 ⇒ 改名认正。merge 的产物本来就包含全部存活键，
    ///   且 merge 期间是单写者（文件锁 + 同一个 `&mut self`）⇒ 不会有任何行只在 `data.log` 里。
    /// - 两者都在 ⇒ 崩在删旧文件之前，两边都是完整快照；留下 `data.log`（原路径继续走），
    ///   把 `data.merge` 删掉。⚠️ 必须删：下次 merge 会以同一个名字打开那个残骸**往后追加**，
    ///   把两份不同的记录混成一个文件 —— 那才是真的救不回来。
    fn recover_merge_artifact(base_path: &Path) -> Result<()> {
        let mut merge_path = base_path.to_path_buf();
        merge_path.set_extension(MERGE_FILE_EXT);
        if !merge_path.exists() {
            return Ok(());
        }
        if base_path.exists() {
            log::warn!(
                "发现未完成 merge 的残骸 {}（{} 仍在），删掉残骸按原日志打开",
                merge_path.display(),
                base_path.display()
            );
            std::fs::remove_file(&merge_path)?;
        } else {
            log::warn!(
                "{} 缺失而 {} 存在 ⇒ 上次 merge 崩在改名之前，把 merge 产物认作数据日志",
                base_path.display(),
                merge_path.display()
            );
            std::fs::rename(&merge_path, base_path)?;
        }
        Ok(())
    }

    /// 全量重建：扫所有密封分片 + active
    fn full_rebuild(base_path: &Path, sealed_ids: &[u32]) -> Result<(KeyDir, u32, Log)> {
        let mut key_dir = KeyDir::new();
        for &id in sealed_ids {
            let p = segment_path(base_path, id);
            if !p.exists() {
                continue;
            }
            let mut log = Log::new(p)?;
            let partial = log.load_index(id)?;
            for (k, v) in partial {
                key_dir.insert(k, v);
            }
            // 释放锁；读路径会 lazy reopen
            drop(log);
        }

        let active = Log::new(base_path.to_path_buf())?;
        // active_id：无密封时 0；有密封取 max+1（若 active 为空且从未 rotate，仍可能是 0）
        let active_id = if sealed_ids.is_empty() {
            0
        } else {
            sealed_ids.iter().copied().max().unwrap_or(0).saturating_add(1)
        };
        let partial = {
            // 需要 mut active 扫
            let mut active_mut = active;
            let kd = active_mut.load_index(active_id)?;
            for (k, v) in kd {
                key_dir.insert(k, v);
            }
            (key_dir, active_id, active_mut)
        };
        Ok(partial)
    }

    pub fn path(&self) -> &std::path::Path {
        &self.base_path
    }

    pub fn active_id(&self) -> u32 {
        self.active_id
    }

    /// 当前生效的单分片软上限（门面读回用：证明设进去的值真到了引擎里）
    pub fn max_file_bytes(&self) -> u64 {
        self.max_file_bytes
    }

    /// 对应的 hint 文件路径
    pub fn hint_path(&self) -> PathBuf {
        hint_path_for(&self.base_path)
    }

    /// 把当前 KeyDir 写出到 hint（HINT0002：active_id + active_log_size）。
    ///
    /// 普通写路径不自动调用；适合：
    /// - [`merge`] 之后（自动）
    /// - 进程优雅退出 / 运维主动 checkpoint
    /// - bulk load `finish` 后（可由上层调用）
    pub fn dump_hint(&mut self) -> Result<()> {
        self.active.flush_buf()?;
        let log_size = self.active.end_pos;
        let path = self.hint_path();
        write_hint_file(&path, &self.key_dir, self.active_id, log_size)
    }

    /// 从指定分片读 value
    fn read_live(&mut self, file_id: u32, pos: u64, len: u32) -> Result<Vec<u8>> {
        if file_id == self.active_id {
            return self.active.read_value(pos, len);
        }
        if !self.sealed.contains_key(&file_id) {
            let p = segment_path(&self.base_path, file_id);
            let log = Log::new(p)?;
            self.sealed.insert(file_id, log);
        }
        let log = self
            .sealed
            .get_mut(&file_id)
            .expect("sealed just inserted");
        log.read_value(pos, len)
    }

    /// active 写满后密封：`data.log` → `data.{active_id}.log`，再开新 active
    fn maybe_rotate(&mut self) -> Result<()> {
        if self.active.end_pos < self.max_file_bytes {
            return Ok(());
        }
        self.rotate()
    }

    fn rotate(&mut self) -> Result<()> {
        self.active.flush_buf()?;
        let _ = self.active.file.sync_all();

        let sealed_path = segment_path(&self.base_path, self.active_id);
        // Windows：必须先 drop 句柄再 rename
        let old = std::mem::replace(
            &mut self.active,
            // 临时占位，马上会被新 Log 替换
            Log {
                path: PathBuf::new(),
                file: {
                    // 用一个临时 hold 文件占位（避免 Option）
                    let hold = self.base_path.with_extension("rotate_hold");
                    std::fs::OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .open(&hold)?
                },
                end_pos: 0,
                buf: Vec::new(),
            },
        );
        let old_path = old.path.clone();
        drop(old);

        if sealed_path.exists() {
            let _ = std::fs::remove_file(&sealed_path);
        }
        std::fs::rename(&old_path, &sealed_path)?;

        // 新 active
        self.active = Log::new(self.base_path.clone())?;
        self.active_id = self.active_id.saturating_add(1);

        let hold = self.base_path.with_extension("rotate_hold");
        let _ = std::fs::remove_file(&hold);
        Ok(())
    }

    /// 压缩：把 KeyDir 中存活 + 软 tomb 重写到新 data.log（active_id=0），删除所有密封分片
    pub fn merge(&mut self) -> Result<()> {
        self.active.flush_buf()?;

        let target = self.base_path.clone();
        let mut merge_path = target.clone();
        merge_path.set_extension(MERGE_FILE_EXT);

        let snapshot: Vec<(Vec<u8>, KeyLoc)> = self
            .key_dir
            .iter()
            .map(|(k, loc)| (k.clone(), loc.clone()))
            .collect();

        let mut new_log = Log::new(merge_path)?;
        let mut new_key_dir = KeyDir::new();
        let new_file_id = 0u32;

        for (key, loc) in snapshot {
            match loc {
                KeyLoc::Live {
                    file_id,
                    pos,
                    len,
                } => {
                    let value = self.read_live(file_id, pos, len)?;
                    let (value_pos, value_len, _) =
                        new_log.append_entry(&key, Some(&value), false)?;
                    new_key_dir.insert(
                        key,
                        KeyLoc::Live {
                            file_id: new_file_id,
                            pos: value_pos,
                            len: value_len,
                        },
                    );
                }
                KeyLoc::SoftTombstone { .. } => {
                    new_log.write_soft_tombstone(&key)?;
                    new_key_dir.insert(key, KeyLoc::SoftTombstone { file_id: new_file_id });
                }
            }
        }
        new_log.flush_buf()?;
        new_log.file.sync_all()?;
        let new_path = new_log.path.clone();
        let new_end = new_log.end_pos;
        drop(new_log);

        // 关闭所有密封句柄
        self.sealed.clear();

        // 释放 active 锁
        let hold_path = target.with_extension("merge_hold");
        let old_active = std::mem::replace(
            &mut self.active,
            Log {
                path: PathBuf::new(),
                file: {
                    let f = std::fs::OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .open(&hold_path)?;
                    f
                },
                end_pos: 0,
                buf: Vec::new(),
            },
        );
        drop(old_active);

        // 删除密封分片
        //
        // ⚠️ 从这里到 `rename(new_path, &target)` 之间崩掉，磁盘上只剩 `data.merge`。
        // 这不是"运气好没碰上"而是必然要碰的：单核机器上 merge 是全库重写，窗口够长。
        // 兜底不在这里（顺序不能改：旧 active 句柄还挂着时 Windows 不让改名覆盖），
        // 而在 open 那边的 [`Bitcask::recover_merge_artifact`]。
        for id in discover_sealed_ids(&target) {
            let p = segment_path(&target, id);
            let _ = std::fs::remove_file(&p);
        }
        // 删除旧 active（若仍存在）
        if target.exists() {
            let _ = std::fs::remove_file(&target);
        }

        std::fs::rename(&new_path, &target)?;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&target)?;
        file.try_lock_exclusive()?;
        self.active = Log {
            path: target,
            file,
            end_pos: new_end,
            buf: Vec::new(), // 同 `Log::open`：首次写入才分配
        };
        self.active_id = 0;
        self.key_dir = new_key_dir;
        let _ = std::fs::remove_file(&hold_path);

        let _ = self.dump_hint();
        Ok(())
    }

    // ── 经典 KV API ──────────────────────────────────────────────────────────

    pub fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        let fid = self.active_id;
        let (value_pos, value_len, _) = self.active.append_entry(key, Some(&value), false)?;
        self.key_dir.insert(
            key.to_vec(),
            KeyLoc::Live {
                file_id: fid,
                pos: value_pos,
                len: value_len,
            },
        );
        self.maybe_rotate()?;
        Ok(())
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.key_dir.get(key).cloned() {
            Some(KeyLoc::Live {
                file_id,
                pos,
                len,
            }) => {
                let val = self.read_live(file_id, pos, len)?;
                Ok(Some(val))
            }
            Some(KeyLoc::SoftTombstone { .. }) | None => Ok(None),
        }
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.active.write_hard_delete(key)?;
        self.key_dir.remove(key);
        self.maybe_rotate()?;
        Ok(())
    }

    /// flush 写缓冲 + fsync 数据文件
    pub fn flush(&mut self) -> Result<()> {
        self.active.flush_buf()?;
        self.active.file.sync_all()?;
        Ok(())
    }

    /// 只 flush 写缓冲（不 fsync）— bulk 中途降低峰值缓冲
    pub fn flush_buf(&mut self) -> Result<()> {
        self.active.flush_buf()
    }

    pub fn scan(&mut self, range: impl std::ops::RangeBounds<Vec<u8>>) -> ScanIterator<'_> {
        let _ = self.active.flush_buf();
        let items: Vec<(Vec<u8>, KeyLoc)> = self
            .key_dir
            .range(range)
            .map(|(k, loc)| (k.clone(), loc.clone()))
            .collect();
        ScanIterator {
            items,
            idx: 0,
            rev: false,
            eng: self,
        }
    }

    pub fn scan_prefix(&mut self, prefix: &[u8]) -> ScanIterator<'_> {
        let start = Bound::Included(prefix.to_vec());
        let mut bound_prefix = prefix.to_vec();
        if let Some(last) = bound_prefix.iter_mut().last() {
            *last = last.saturating_add(1);
        }
        let end = Bound::Excluded(bound_prefix);
        self.scan((start, end))
    }

    // ── MVCC 后端 API ────────────────────────────────────────────────────────

    /// 写入版本键。`value=None` → 软 tombstone（KeyDir 保留）。
    pub fn insert(&mut self, key: Vec<u8>, value: Option<Vec<u8>>) -> Result<()> {
        let fid = self.active_id;
        match value {
            Some(v) => {
                let (value_pos, value_len, _) =
                    self.active.append_entry(&key, Some(&v), false)?;
                self.key_dir.insert(
                    key,
                    KeyLoc::Live {
                        file_id: fid,
                        pos: value_pos,
                        len: value_len,
                    },
                );
            }
            None => {
                self.active.write_soft_tombstone(&key)?;
                self.key_dir
                    .insert(key, KeyLoc::SoftTombstone { file_id: fid });
            }
        }
        self.maybe_rotate()?;
        Ok(())
    }

    /// 批量写入（一次缓冲，适合 bulk load）。消费 items，避免 clone。
    pub fn insert_batch(
        &mut self,
        items: impl IntoIterator<Item = (Vec<u8>, Option<Vec<u8>>)>,
    ) -> Result<()> {
        for (key, value) in items {
            let fid = self.active_id;
            match value {
                Some(v) => {
                    let (value_pos, value_len, _) =
                        self.active.append_entry(&key, Some(&v), false)?;
                    self.key_dir.insert(
                        key,
                        KeyLoc::Live {
                            file_id: fid,
                            pos: value_pos,
                            len: value_len,
                        },
                    );
                }
                None => {
                    self.active.write_soft_tombstone(&key)?;
                    self.key_dir
                        .insert(key, KeyLoc::SoftTombstone { file_id: fid });
                }
            }
            self.maybe_rotate()?;
        }
        Ok(())
    }

    /// 硬删除版本键（rollback / vacuum / WAL UNDO）
    pub fn remove(&mut self, key: &[u8]) -> Result<()> {
        self.active.write_hard_delete(key)?;
        self.key_dir.remove(key);
        self.maybe_rotate()?;
        Ok(())
    }

    /// `None` = 键不在索引；`Some(None)` = 软 tombstone；`Some(Some(v))` = 存活
    pub fn get_entry(&mut self, key: &[u8]) -> Result<Option<Option<Vec<u8>>>> {
        match self.key_dir.get(key).cloned() {
            None => Ok(None),
            Some(KeyLoc::SoftTombstone { .. }) => Ok(Some(None)),
            Some(KeyLoc::Live {
                file_id,
                pos,
                len,
            }) => {
                let v = self.read_live(file_id, pos, len)?;
                Ok(Some(Some(v)))
            }
        }
    }

    /// 闭区间 `[low, high]` 扫描（含软 tombstone）
    pub fn range_scan(
        &mut self,
        low: Vec<u8>,
        high: Vec<u8>,
    ) -> Result<Vec<(Vec<u8>, Option<Vec<u8>>)>> {
        self.active.flush_buf()?;
        let keys: Vec<(Vec<u8>, KeyLoc)> = self
            .key_dir
            .range(low..=high)
            .map(|(k, loc)| (k.clone(), loc.clone()))
            .collect();
        let mut out = Vec::with_capacity(keys.len());
        for (k, loc) in keys {
            match loc {
                KeyLoc::SoftTombstone { .. } => out.push((k, None)),
                KeyLoc::Live {
                    file_id,
                    pos,
                    len,
                } => {
                    let v = self.read_live(file_id, pos, len)?;
                    out.push((k, Some(v)));
                }
            }
        }
        Ok(out)
    }

    /// 闭区间 `[low, high]` 内**从大到小**最多取 `max` 条索引项，**不读 value**。
    ///
    /// 返回条数 `< max` 说明已走到区间下界 —— 调用方由此可以断定「整段都看过了」，
    /// 于是在只需要区间末尾几条时提前退出，而不必像 [`Self::range_scan`] 那样物化整段并逐行读盘。
    pub fn range_meta_back(&self, low: &[u8], high: &[u8], max: usize) -> Vec<(Vec<u8>, KeyLoc)> {
        self.key_dir
            .range(low.to_vec()..=high.to_vec())
            .rev()
            .take(max)
            .map(|(k, loc)| (k.clone(), loc.clone()))
            .collect()
    }

    /// 全量扫描（含软 tombstone）
    pub fn iter(&mut self) -> Result<Vec<(Vec<u8>, Option<Vec<u8>>)>> {
        self.active.flush_buf()?;
        let keys: Vec<(Vec<u8>, KeyLoc)> = self
            .key_dir
            .iter()
            .map(|(k, loc)| (k.clone(), loc.clone()))
            .collect();
        let mut out = Vec::with_capacity(keys.len());
        for (k, loc) in keys {
            match loc {
                KeyLoc::SoftTombstone { .. } => out.push((k, None)),
                KeyLoc::Live {
                    file_id,
                    pos,
                    len,
                } => {
                    let v = self.read_live(file_id, pos, len)?;
                    out.push((k, Some(v)));
                }
            }
        }
        Ok(out)
    }

    /// 只扫 KeyDir 元数据，不读 value（vacuum / 统计用）
    pub fn iter_meta(&self) -> impl Iterator<Item = (&Vec<u8>, &KeyLoc)> {
        self.key_dir.iter()
    }

    /// 从 `from`（**含**）起的 KeyDir 元数据升序视图，不读 value。
    ///
    /// 下界落在 enc 域（`raw_key ‖ version`），不是逻辑 key 域：以 `from` 为真前缀
    /// 的其它 key 可能排在 `from` 之前，所以想「跳过某个逻辑 key 的整组」必须由调用方
    /// 自己按 `raw_key > from` 过滤，光靠这个下界会漏行。
    pub fn iter_meta_from(&self, from: Option<&[u8]>) -> impl Iterator<Item = (&Vec<u8>, &KeyLoc)> {
        let lo = match from {
            Some(k) => std::ops::Bound::Included(k),
            None => std::ops::Bound::Unbounded,
        };
        self.key_dir
            .range::<[u8], _>((lo, std::ops::Bound::Unbounded))
    }

    pub fn len(&self) -> usize {
        self.key_dir.len()
    }

    pub fn is_empty(&self) -> bool {
        self.key_dir.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_log(tag: &str) -> PathBuf {
        crate::common::tmp_test_path("bitcask_hint", tag, ".log")
    }

    fn cleanup(path: &Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(hint_path_for(path));
        let _ = std::fs::remove_file(path.with_extension("merge"));
        let _ = std::fs::remove_file(path.with_extension("merge_hold"));
        let _ = std::fs::remove_file(path.with_extension("rotate_hold"));
        // 密封分片
        for id in discover_sealed_ids(path) {
            let _ = std::fs::remove_file(segment_path(path, id));
        }
    }

    /// 崩在 `merge()` 改名之前的现场，必须能在下次 open 时被认回来。
    ///
    /// 手工布置现场而不是真去崩进程：`data.log` 已经不存在、全部存活记录只在 `data.merge` 里。
    /// 没有恢复的话 open 会**新建一个空 data.log**（`Log::new` 就是 create-if-missing），
    /// 于是得到一个"打开成功、一条都没有"的空库 —— 这类失败不报错，只能靠断言钉。
    #[test]
    fn test_open_recovers_merge_left_behind_by_crashed_merge() {
        let path = tmp_log("merge_crash");
        {
            let mut eng = Bitcask::new(path.clone()).unwrap();
            for i in 0..20u8 {
                eng.set(format!("k{i}").as_bytes(), vec![i; 32]).unwrap();
            }
            eng.flush().unwrap();
            eng.dump_hint().unwrap();
        }
        // 布置现场：merge 产物写好、旧 data.log 已删、改名还没跑
        let merge = path.with_extension("merge");
        std::fs::rename(&path, &merge).unwrap();
        assert!(!path.exists() && merge.exists());

        {
            let mut eng = Bitcask::new(path.clone()).unwrap();
            for i in 0..20u8 {
                assert_eq!(
                    eng.get(format!("k{i}").as_bytes()).unwrap(),
                    Some(vec![i; 32]),
                    "k{i} 本该在 merge 产物里 —— open 把它当空库开了"
                );
            }
        }
        cleanup(&path);
    }

    /// 两边都在的现场：留下 `data.log`，把 merge 残骸删掉。
    ///
    /// 删残骸不是洁癖：下次 `merge()` 会以同一个名字打开那个残骸**往后追加**，
    /// 两份不同布局的记录混进一个文件 —— 那才是真救不回来。
    #[test]
    fn test_open_discards_merge_debris_when_log_survives() {
        let path = tmp_log("merge_debris");
        {
            let mut eng = Bitcask::new(path.clone()).unwrap();
            eng.set(b"a", b"1".to_vec()).unwrap();
            eng.flush().unwrap();
        }
        let merge = path.with_extension("merge");
        std::fs::write(&merge, b"stale debris").unwrap();

        {
            let mut eng = Bitcask::new(path.clone()).unwrap();
            assert_eq!(eng.get(b"a").unwrap(), Some(b"1".to_vec()));
        }
        assert!(!merge.exists(), "data.log 还在 ⇒ 残骸该被清掉，别留给下次 merge 追加");
        cleanup(&path);
    }

    /// hint 指的是向已经不存在的分片 ⇒ 必须回退全量扫，不能悄悄少键。
    ///
    /// 增量扫只补 active 文件里 `active_size` 之后的新增行；键的位置要是被 hint 记成
    /// 「在某个已删掉的分片上」，它就**永远不会**被这次扫描补回来。
    /// 真实来源就是崩在 merge 中途：合并后的 data.log 是全的，hint 却还记着合并前的布局。
    #[test]
    fn test_hint_naming_missing_segment_falls_back_to_full_scan() {
        let path = tmp_log("stale_hint");
        {
            // 小分片上限 ⇒ 会 rotate，active 里是最后几行
            let mut eng = Bitcask::with_max_file_bytes(path.clone(), 64).unwrap();
            for i in 0..12u8 {
                eng.set(format!("k{i}").as_bytes(), vec![i; 40]).unwrap();
            }
            eng.flush().unwrap();
            eng.dump_hint().unwrap();
        }
        let hint = hint_path_for(&path);
        let (mut kd, active_id, active_size) = read_hint_file(&hint).unwrap();
        // 把最后写入的那个键伪造成"在分片 999 上"：它的真实记录确实在 data.log 里
        let last = format!("k{}", 11u8).into_bytes();
        kd.insert(last, KeyLoc::Live { file_id: 999, pos: 0, len: 40 });
        write_hint_file(&hint, &kd, active_id, active_size).unwrap();

        {
            let mut eng = Bitcask::new(path.clone()).unwrap();
            for i in 0..12u8 {
                assert_eq!(
                    eng.get(format!("k{i}").as_bytes()).unwrap(),
                    Some(vec![i; 40]),
                    "k{i} 该由全量扫补回来（hint 里有一条指向失踪分片的位置）"
                );
            }
        }
        cleanup(&path);
    }

    #[test]
    fn test_hint_roundtrip_and_fast_open() {
        let path = tmp_log("rt");
        {
            let mut eng = Bitcask::new(path.clone()).unwrap();
            eng.set(b"a", b"1".to_vec()).unwrap();
            eng.set(b"b", b"2".to_vec()).unwrap();
            eng.set(b"c", b"3".to_vec()).unwrap();
            eng.delete(b"b").unwrap();
            eng.flush().unwrap();
            eng.dump_hint().unwrap();
            assert!(eng.hint_path().exists());
            assert_eq!(eng.len(), 2);
        }
        // 再 open：应走 hint 快路径
        {
            let mut eng = Bitcask::new(path.clone()).unwrap();
            assert_eq!(eng.get(b"a").unwrap(), Some(b"1".to_vec()));
            assert_eq!(eng.get(b"b").unwrap(), None);
            assert_eq!(eng.get(b"c").unwrap(), Some(b"3".to_vec()));
            assert_eq!(eng.len(), 2);
        }
        cleanup(&path);
    }

    #[test]
    fn test_hint_incremental_tail_scan() {
        let path = tmp_log("incr");
        {
            let mut eng = Bitcask::new(path.clone()).unwrap();
            eng.set(b"k1", b"v1".to_vec()).unwrap();
            eng.set(b"k2", b"v2".to_vec()).unwrap();
            eng.flush().unwrap();
            eng.dump_hint().unwrap();
        }
        // 写更多数据但不更新 hint → open 应 hint + 扫尾部
        {
            let mut eng = Bitcask::new(path.clone()).unwrap();
            eng.set(b"k3", b"v3".to_vec()).unwrap();
            eng.set(b"k1", b"v1b".to_vec()).unwrap(); // 覆盖
            eng.flush().unwrap();
            // 故意不 dump_hint
            drop(eng);
        }
        {
            let mut eng = Bitcask::new(path.clone()).unwrap();
            assert_eq!(eng.get(b"k1").unwrap(), Some(b"v1b".to_vec()));
            assert_eq!(eng.get(b"k2").unwrap(), Some(b"v2".to_vec()));
            assert_eq!(eng.get(b"k3").unwrap(), Some(b"v3".to_vec()));
            assert_eq!(eng.len(), 3);
        }
        cleanup(&path);
    }

    #[test]
    fn test_merge_writes_hint() {
        let path = tmp_log("merge");
        {
            let mut eng = Bitcask::new(path.clone()).unwrap();
            eng.set(b"a", b"1".to_vec()).unwrap();
            eng.set(b"a", b"2".to_vec()).unwrap();
            eng.set(b"b", b"3".to_vec()).unwrap();
            eng.delete(b"b").unwrap();
            eng.merge().unwrap();
            assert!(eng.hint_path().exists());
            assert_eq!(eng.get(b"a").unwrap(), Some(b"2".to_vec()));
            assert_eq!(eng.get(b"b").unwrap(), None);
        }
        {
            let mut eng = Bitcask::new(path.clone()).unwrap();
            assert_eq!(eng.get(b"a").unwrap(), Some(b"2".to_vec()));
            assert_eq!(eng.len(), 1);
        }
        cleanup(&path);
    }

    #[test]
    fn test_corrupt_hint_falls_back() {
        let path = tmp_log("bad");
        {
            let mut eng = Bitcask::new(path.clone()).unwrap();
            eng.set(b"x", b"y".to_vec()).unwrap();
            eng.flush().unwrap();
            eng.dump_hint().unwrap();
        }
        // 写坏 hint
        std::fs::write(hint_path_for(&path), b"GARBAGE!!").unwrap();
        {
            let mut eng = Bitcask::new(path.clone()).unwrap();
            assert_eq!(eng.get(b"x").unwrap(), Some(b"y".to_vec()));
        }
        cleanup(&path);
    }

    #[test]
    fn test_soft_tombstone_in_hint() {
        let path = tmp_log("soft");
        {
            let mut eng = Bitcask::new(path.clone()).unwrap();
            eng.insert(b"vkey".to_vec(), Some(b"val".to_vec()))
                .unwrap();
            eng.insert(b"tomb".to_vec(), None).unwrap(); // 软 tomb
            eng.flush().unwrap();
            eng.dump_hint().unwrap();
            assert_eq!(eng.len(), 2);
            assert_eq!(eng.get_entry(b"tomb").unwrap(), Some(None));
        }
        {
            let mut eng = Bitcask::new(path.clone()).unwrap();
            assert_eq!(eng.get(b"vkey").unwrap(), Some(b"val".to_vec()));
            assert_eq!(eng.get_entry(b"tomb").unwrap(), Some(None));
            assert_eq!(eng.len(), 2);
        }
        cleanup(&path);
    }

    #[test]
    fn test_rotate_and_reopen() {
        let path = tmp_log("rotate");
        {
            // 很小上限强制 rotate
            let mut eng = Bitcask::with_max_file_bytes(path.clone(), 256).unwrap();
            for i in 0..40u32 {
                let k = format!("k{i:03}");
                let v = format!("value-padding-{:03}-xxxxxxxx", i);
                eng.set(k.as_bytes(), v.into_bytes()).unwrap();
            }
            eng.flush().unwrap();
            eng.dump_hint().unwrap();
            assert!(
                eng.active_id() > 0,
                "expected rotate, active_id={}",
                eng.active_id()
            );
            // 密封文件应存在
            let sealed = discover_sealed_ids(&path);
            assert!(!sealed.is_empty(), "expected sealed segments");
            assert_eq!(eng.get(b"k000").unwrap(), Some(b"value-padding-000-xxxxxxxx".to_vec()));
            assert_eq!(eng.get(b"k039").unwrap(), Some(b"value-padding-039-xxxxxxxx".to_vec()));
        }
        {
            let mut eng = Bitcask::new(path.clone()).unwrap();
            assert_eq!(eng.get(b"k000").unwrap(), Some(b"value-padding-000-xxxxxxxx".to_vec()));
            assert_eq!(eng.get(b"k020").unwrap(), Some(b"value-padding-020-xxxxxxxx".to_vec()));
            assert_eq!(eng.get(b"k039").unwrap(), Some(b"value-padding-039-xxxxxxxx".to_vec()));
            assert_eq!(eng.len(), 40);
        }
        // merge 后应只剩 active
        {
            let mut eng = Bitcask::new(path.clone()).unwrap();
            eng.merge().unwrap();
            assert_eq!(eng.active_id(), 0);
            assert!(discover_sealed_ids(&path).is_empty());
            assert_eq!(eng.get(b"k010").unwrap(), Some(b"value-padding-010-xxxxxxxx".to_vec()));
        }
        cleanup(&path);
    }

    /// 写缓冲必须是**懒分配**：打开一个空库不该先占 4 MiB。
    ///
    /// 大库基线台架量到 `open 空库` 就"常驻 8.4 MB"，来源正是数据日志缓冲与 blob 缓冲各
    /// 一次 `Vec::with_capacity(4 MiB)` —— 库里一个键都没有也要先占 8 MiB（下游那台单核机器
    /// 同时只跑得起几百 MB 预算）。改成"首次写入才一步到位"之后：
    /// ① 打开时容量为 0；② 第一次写入就涨满，所以"攒够 4 MiB 才落盘"的 syscall 优化不变。
    #[test]
    fn test_write_buffer_is_allocated_lazily() {
        let path = tmp_log("lazy_buf");
        {
            let mut log = Log::new(path.clone()).unwrap();
            assert_eq!(log.buf.capacity(), 0, "打开空库不该预留写缓冲");
            log.append_entry(b"k", Some(b"v"), false).unwrap();
            assert!(
                log.buf.capacity() >= WRITE_BUF_TARGET,
                "首次写入后应一步涨到目标大小（实际 {}）",
                log.buf.capacity()
            );
        }
        cleanup(&path);

        // blob 旁路同一个规矩（这里的 4 MiB 与 storage.rs 的 BLOB_BUF_TARGET 同值）
        let dir = tmp_log("lazy_blob_dir");
        {
            std::fs::create_dir_all(&dir).unwrap();
            let mut blob = crate::bitcask::storage::BlobStore::open(&dir).unwrap();
            assert_eq!(blob.buf_capacity_for_test(), 0, "打开空库不该预留 blob 缓冲");
            blob.append(b"payload").unwrap();
            assert!(
                blob.buf_capacity_for_test() >= 4 * 1024 * 1024,
                "首次写入后应一步涨到目标大小（实际 {}）",
                blob.buf_capacity_for_test()
            );
        }
        cleanup(&dir);
    }
}
