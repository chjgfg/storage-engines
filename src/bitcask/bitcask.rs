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

/// M40：一次 `merge()` 从 KeyDir 抄块的**块大小**（把键）。
///
/// 为什么要有块：`read_live` 要 `&mut self`，而走 `key_dir` 是按 `&self` 借用 ⇒ 原先只能先把
/// 整库抄成一份 `Vec<(key.clone(), loc.clone())>` 快照（600k 键 × 2 版那档 **115.0 MB**）。
/// 块化之后那份重复的键表是 O(本常数)。取 8192 与四家 vacuum 的块界同值，好记也好钉
/// （守卫 `test_merge_walks_key_dir_in_bounded_chunks`）。
const MERGE_CHUNK_KEYS: usize = 8192;
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

// 测试专用（M40 的尺子）：一次 `merge()` 里**单块从 KeyDir 抄出多少把键**的历史最大值。
//
// 为什么钉这个而不是钉内存：`mem_probe` 的高水位是进程级的、并行用例互相污染（M27/M36 各栽过
// 一次），而"块界有没有生效"是个纯计数 —— 与四家 vacuum 那条 `vac_pending_max` 同形。
#[cfg(test)]
thread_local! {
    static MERGE_CHUNK_MAX: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn note_merge_chunk(n: usize) {
    MERGE_CHUNK_MAX.with(|c| {
        let n = n as u64;
        if n > c.get() {
            c.set(n);
        }
    });
}

#[cfg(test)]
pub(crate) fn merge_chunk_max() -> u64 {
    MERGE_CHUNK_MAX.with(|c| c.get())
}

#[cfg(test)]
pub(crate) fn reset_merge_chunk() {
    MERGE_CHUNK_MAX.with(|c| c.set(0));
}

// 测试专用（M43 的尺子）：一次 `merge()` 的**重扫**往索引里折了多少行日志。
//
// 为什么钉这个而不是钉"同时活着几把索引"：后者要数堆上同时存在的 `KeyDir`，而 `KeyDir` 只是
// `BTreeMap` 的别名，没有地方挂钩子（做成 newtype 就是破坏性 API 改动）。"索引由重扫得到"是
// 同一件事的可观测面 —— 回归成"走查时一路抄出第二把索引"（M40 及之前的形状）就让这个数掉回 0。
#[cfg(test)]
thread_local! {
    static MERGE_RESCAN_ROWS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

// 只有 `merge()` 的重扫那一趟在数：它把计数器上弦、扫完立刻退弦。
// 放在 `scan_log_into` 里按弦位过滤，是因为同一个函数也服务 open 时的全量/hint 扫 ——
// 那些不该算进「这次 merge 扫了多少行」。做成 thread_local：`cargo test` 的并行用例
// 各数各的，别人 open 时的扫描不会串到我这一格。
#[cfg(test)]
thread_local! {
    static MERGE_RESCAN_ARMING: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
pub(crate) fn arm_merge_rescan_counter() {
    MERGE_RESCAN_ROWS.with(|c| c.set(0));
    MERGE_RESCAN_ARMING.with(|a| a.set(true));
}

#[cfg(test)]
pub(crate) fn disarm_merge_rescan_counter() {
    MERGE_RESCAN_ARMING.with(|a| a.set(false));
}

#[cfg(test)]
pub(crate) fn merge_rescan_rows() -> u64 {
    let n = MERGE_RESCAN_ROWS.with(|c| c.get());
    MERGE_RESCAN_ARMING.with(|a| a.set(false));
    n
}

#[cfg(test)]
pub(crate) fn note_merge_rescan_row() {
    MERGE_RESCAN_ARMING.with(|armed| {
        if armed.get() {
            MERGE_RESCAN_ROWS.with(|c| c.set(c.get() + 1));
        }
    });
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
// M53 记账
        #[cfg(test)]
        crate::common::disk::note_fsync(crate::common::disk::SITE_BITCASK_HINT);
    }
    if path.exists() {
        let _ = std::fs::remove_file(path);
    }
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// hint 记录的**流式解码器**（M52c）。两份格式共用一把读法：v2 带 `file_id`，v1 一律 0。
///
/// 为什么是迭代器而不是"先解成 `Vec<记录>` 再折成树"（那是本格的第一版写法）：那份 `Vec`
/// 是**整库形状**的中间物 —— 600k 条 × 48 B 的骨架就是 28.8 MB，而树本身才 57.7 MB，
/// 等于让"读索引"这一步临时多要半棵索引。`collect()` 直接从这个迭代器取，
/// `BTreeMap` 的有序批量填充照旧生效（台架 A 臂读数没掉回 E 臂那一档就是证据）。
///
/// ⚠️ `entry_count` 是从文件头读来的，**不可信**：改前第一版给它做 `with_capacity`，
/// 一个写 2^60 的坏 hint 就会让我们先申请几 GB。迭代器这一版没有那一格 —— 预留由不上了，
/// 真正的记录数由读取本身说话：撞 EOF ⇒ `Err` ⇒ 调用方的处置是回退全量扫。
/// 同一处还修掉一条旧的不一致：v1 那份循环漏了 `key_len` 的上界检查，会先 `vec![0u8; key_len]`
/// 再去读；共用之后两份都先检查（越界 ⇒ 报错），一个荒谬的键长不再先把自己撑爆。
struct HintEntries<'a, R> {
    r: &'a mut R,
    /// 还剩几条没解（由文件头声明，读不满就报错）
    left: u64,
    with_file_id: bool,
}

impl<'a, R: std::io::Read> HintEntries<'a, R> {
    fn new(r: &'a mut R, entry_count: u64, with_file_id: bool) -> Self {
        Self {
            r,
            left: entry_count,
            with_file_id,
        }
    }

    /// 解一条记录。`Ok(None)` = **未知种类** ⇒ 丢掉但继续往下读（沿用改前口径，不许变成"当作存活"）；
    /// `Err` = 读不出来 / 坏键长 ⇒ 调用方据此回退全量扫。
    fn one(&mut self) -> Result<Option<(Vec<u8>, KeyLoc)>> {
        let mut nbuf = [0u8; 4];
        let mut pbuf = [0u8; 8];
        self.r.read_exact(&mut nbuf)?;
        let key_len = u32::from_le_bytes(nbuf) as usize;
        self.r.read_exact(&mut nbuf)?;
        let flag = i32::from_le_bytes(nbuf);
        let file_id = if self.with_file_id {
            self.r.read_exact(&mut nbuf)?;
            u32::from_le_bytes(nbuf)
        } else {
            0
        };
        self.r.read_exact(&mut pbuf)?;
        let pos = u64::from_le_bytes(pbuf);
        self.r.read_exact(&mut nbuf)?;
        let len = u32::from_le_bytes(nbuf);
        if key_len > 16 * 1024 * 1024 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "hint key_len 异常",
            ));
        }
        let mut key: Vec<u8> = vec![0u8; key_len];
        self.r.read_exact(&mut key)?;
        Ok(match flag {
            HINT_FLAG_LIVE => Some((key, KeyLoc::Live { file_id, pos, len })),
            FLAG_SOFT_TOMBSTONE => Some((key, KeyLoc::SoftTombstone { file_id })),
            _ => None,
        })
    }
}

impl<R: std::io::Read> Iterator for HintEntries<'_, R> {
    /// 项里套 `Option` 是为了让"丢掉这一条"和"这一条读不出来"分成两件事；
    /// 上面用 `.map(transpose).flatten()` 把它们收拢成 `io::Result<(键, 位置)>` 的流。
    type Item = Result<Option<(Vec<u8>, KeyLoc)>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        Some(self.one())
    }
}

/// hint 记录 → `KeyDir`。
///
/// ⚠️ 这里**不是一条一条 `insert`**（M52a）。hint 里的键本来就是按键升序写出的
/// （`write_hint_file` 遍历的就是排好序的 `KeyDir`），而 `BTreeMap::insert` 对有序输入
/// 每次都要从根沿最右一路走到底，还会把沿途每个节点**劈成半满**：
/// 同构建 A/B（`bitcask_hint_load_stage_split`，四轮两种顺序取最小）600k 档
/// 时间 **405.3 → 205.6 ms（1.97×）**、200k 档 **180.6 → 62.8 ms（2.87×）**；
/// 建好之后**还活着** 65.4 → 39.7 MB —— 差的 **25.7 MB** 与大库台架那一格
/// `冷重开 常驻 83.4 → 57.7 MB` **一字不差**，而两臂解出的键数相同 ⇒
/// 那 25.7 MB 纯粹是**半满节点**，不是内容。重复键的取舍与逐条 `insert` 相同（后者胜）。
fn hint_entries_to_keydir<R: std::io::Read>(
    r: &mut R,
    entry_count: u64,
    with_file_id: bool,
) -> Result<KeyDir> {
    // 台架对照臂（门控 `HINT_OLD_SHAPE`，`#[cfg(test)]` ⇒ 不随 crate 发布）：**同一个解码器**，
    // 只把折树那一步退回逐条 `insert`。共用解码器是故意的 —— 两臂只差一件事，
    // 读出来的比值才只归因于形状（大库台架那一趟读的是现场形状：版本键混着软墓碑、
    // 进程已经分配过 1 GB），而臂级台架读的是一份干净的 600k 升序键。
    #[cfg(test)]
    if std::env::var("HINT_OLD_SHAPE").is_ok() {
        let mut key_dir = KeyDir::new();
        for item in HintEntries::new(r, entry_count, with_file_id) {
            if let Some((key, loc)) = item? {
                key_dir.insert(key, loc);
            }
        }
        return Ok(key_dir);
    }
    HintEntries::new(r, entry_count, with_file_id)
        .map(std::result::Result::transpose)
        .flatten()
        .collect()
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
        let mut r = BufReader::new(f);
        let key_dir = hint_entries_to_keydir(&mut r, entry_count, true)?;
        Ok((key_dir, active_id, active_log_size))
    } else if &magic == HINT_MAGIC_V1 {
        // 兼容旧 hint：file_id 全 0，header 剩余 24 字节
        let mut rest = [0u8; HINT_HEADER_SIZE_V1 - 8];
        f.read_exact(&mut rest)?;
        let log_size = u64::from_le_bytes(rest[0..8].try_into().unwrap());
        let entry_count = u64::from_le_bytes(rest[8..16].try_into().unwrap());
        let mut r = BufReader::new(f);
        let key_dir = hint_entries_to_keydir(&mut r, entry_count, false)?;
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
    /// 自上一次 fsync 之后，这个进程有没有**写过**（含已经 `flush_buf` 落进文件、但还没 fsync 的字节）。
    /// M53：`flush()` 在账上是干净的时候不再付那一次 fsync —— 判据必须是这条账，不能只看
    /// `buf.is_empty()`：写满 4 MiB 时 `append_entry` 会自己 `flush_buf`，那时缓冲是空的、
    /// 文件里却躺着没 fsync 的字节。
    dirty: bool,
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
            dirty: false,
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
        // 字节进了文件、还没 fsync ⇒ 仍然欠一次
        self.dirty = true;
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
                    #[cfg(test)]
                    note_merge_rescan_row();
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
        if written > 0 {
            self.dirty = true;
        }
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
    /// 内存索引正处于"腾掉旧的、还没重扫完"的窗口（M43）。
    ///
    /// 为什么要这个状态：`merge()` 为了不把峰值抬到两份索引，必须在重扫**之前**把旧索引丢掉 ——
    /// 而旧索引的条目指向的 `data.log`/`data.{id}.log` 此刻已经被删除或改名了。留着它，读路径
    /// 会对每个键返回"不存在"（悄悄错值，正是这批一路在治的东西）；清掉它，读路径至少还能
    /// **报错**。这个标志就是把"清掉"变成"明说现在不能服务"。
    ///
    /// ⚠️ 正常成功的那次 merge 里没人看得见它：置位和复位都在 `merge()` 内部，而门面把 `kv`
    /// 锁在同一把 `Mutex` 里 ⇒ 并发的读请求阻塞在锁上，不会走进窗口。它只服务一种现场：
    /// **重扫失败、`merge()` 带着 Err 返回** —— 此后引擎被隔离，除了重新 open 没有别的出路。
    /// 只能报错的门（返回 `Result` 的那些）报错；`len()` / `iter_meta*` 这几扇不带 `Result` 的门
    /// 只能看到"空索引"，那会让它们什么也不做（不删、不写），但会把"库是空的"当成真话 ——
    /// 这就是 B1 的代价，写在 README M43 那一节。
    index_rebuilding: bool,
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
        // ── M48 冷启动分段耗时标尺（门控 `OPEN_STAGE_SPLIT`；八张槽的名字都写在
        // `MVCC::try_open_with` 上面那张表里，这里占 1/2/3 三格）──
        // ⚠️ 只 mark、不 t0：基准由最外面那层放（门面那扇 open 才是被量的对象）。
        let sealed_ids = discover_sealed_ids(&base_path);
        let hint_path = hint_path_for(&base_path);
        #[cfg(test)]
        crate::mem_probe::open_stage_mark(1);

        // 尝试 hint 快路径
        let mut from_hint: Option<(KeyDir, u32, u64)> = None;
        if let Ok((kd, active_id, active_size)) = read_hint_file(&hint_path) {
            from_hint = Some((kd, active_id, active_size));
        }
        #[cfg(test)]
        crate::mem_probe::open_stage_mark(2);
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
        #[cfg(test)]
        crate::mem_probe::open_stage_mark(3);

        Ok(Self {
            base_path,
            active,
            active_id,
            key_dir,
            max_file_bytes,
            sealed: std::collections::HashMap::new(),
            index_rebuilding: false,
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

    /// 索引可用吗？—— 见 [`Bitcask::index_rebuilding`]：重扫窗口里**所有**依赖 KeyDir 的门
    /// 一律报错，不返回"键不存在"。
    fn usable(&self) -> Result<()> {
        if self.index_rebuilding {
            return Err(std::io::Error::other(
                "bitcask: 内存索引正在重建（上一次 merge 在重扫处失败），请重新打开数据库",
            ));
        }
        Ok(())
    }

    /// 测试专用：把引擎推进"重扫窗口"那个现场 —— 与 `merge()` 里那两步**一字不差**：
    /// 索引腾掉 + 置标志。真机上只有 merge 的重扫失败才进得去。
    ///
    /// 必须连索引一起腾：只置标志、索引还满着，`get` 就会读出**对的**值，这条守卫测的
    /// 就不是那个现场了。窗口里真正的危险是"索引空了而磁盘上全都有"。
    #[cfg(test)]
    pub(crate) fn quarantine_index_for_test(&mut self) {
        self.key_dir = KeyDir::new();
        self.index_rebuilding = true;
    }

    #[cfg(test)]
    pub(crate) fn release_index_for_test(&mut self) {
        self.index_rebuilding = false;
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
        // ⚠️ 这道守卫是**数据**守卫，不是性能守卫：hint 是内存索引的持久化副本。重扫窗口里
        // 把"空索引"写成 hint 落盘，下次 open 就把它当真索引读回来 ⇒ 整库在磁盘上完好，却没人
        // 知道有哪些键。open 那边的 `hint_files_exist` 兜不住这一支（空表里挑不出"指向已删分片"
        // 的条目，而"新日志比旧日志长"的时候连长度检查都会放行）。
        self.usable()?;
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
// M53 记账
        #[cfg(test)]
        crate::common::disk::note_fsync(crate::common::disk::SITE_BITCASK_LOG_FLUSH);
        if self.active.file.sync_all().is_ok() {
            self.active.dirty = false;
        }

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
                // M53：占位 ⇒ 账上没有未 fsync 的写入
                dirty: false,
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

        // M40：**按块走 KeyDir**。这里原先是一次 `.collect()` 出整库的
        // `(key.clone(), loc.clone())` 快照 —— 600k 键 × 2 版那档分段实测 **115.0 MB**
        // （100 B/扫描行），是 `vacuum()` 当时唯一的大头。为什么要快照：循环里 `read_live`
        // 要 `&mut self`，不能一边借用 `key_dir` 一边读盘 ⇒ 只能先把范围抄出来。
        // 每块只抄 `MERGE_CHUNK_KEYS` 把键（借用一结束就处理这一块），那份重复的键表就从
        // O(全库) 变成 O(一块)。
        //
        // M43（B1）：抄完这一块之后**不再往第二把索引里记位置**。原先每个存活键都要往
        // `new_key_dir` 插一条，那份表本身就是 O(库) 且活到函数结束 ⇒ 走查期间磁盘上真正的
        // 峰值是"旧索引 + 新索引"两份同时在（600k 档分段 ④ 实测 **+87.9 MB**）。现在改成
        // 抄完 + fsync + 删旧 + rename 之后**原地重扫新日志**（下面 `load_index_from`），
        // 全程只有一份索引。
        // ⚠️ 代价不是"多扫一遍日志"（多扫那一遍本来就省不掉，索引必须来自日志），而是
        // **旧索引必须先腾掉**才能重扫 ⇒ 中间有一段窗口里 KeyDir 指向的文件已被删除/改名。
        // 那段窗口用 [`Bitcask::index_rebuilding`] 明说"现在不服务"，而不是让读路径把
        // "索引里没有"当成"键不存在"。
        let mut new_log = Log::new(merge_path)?;
        // 块间下界在键的字节域：`key ‖ 0x00` 是**严格后继方向**（`key‖0x00 > key`，且任何
        // `K > key` 都有 `K ≥ key‖0x00`）⇒ 一行不漏、一行不重。⚠️ 不能用 `‖ 0xFF×8` 那种
        // 右界写法（会把以 `key` 为前缀的更长键整把跳掉）—— 与 M37 那条 vacuum 块界同一个理由。
        let mut lower: Vec<u8> = Vec::new();
        loop {
            let mut chunk: Vec<(Vec<u8>, KeyLoc)> = Vec::with_capacity(MERGE_CHUNK_KEYS + 1);
            let mut next_lower: Option<Vec<u8>> = None;
            {
                use std::ops::Bound::{Included, Unbounded};
                for (k, loc) in self
                    .key_dir
                    .range::<[u8], _>((Included(lower.as_slice()), Unbounded))
                {
                    chunk.push((k.clone(), loc.clone()));
                    if chunk.len() >= MERGE_CHUNK_KEYS {
                        let mut bound = k.clone();
                        bound.push(0u8);
                        next_lower = Some(bound);
                        break;
                    }
                }
            }
            #[cfg(test)]
            note_merge_chunk(chunk.len());
            for (key, loc) in chunk {
                match loc {
                    KeyLoc::Live {
                        file_id,
                        pos,
                        len,
                    } => {
                        let value = self.read_live(file_id, pos, len)?;
                        // 位置不进任何索引：新日志里的 offset 由后面的重扫自己扫出来
                        let _ = new_log.append_entry(&key, Some(&value), false)?;
                    }
                    KeyLoc::SoftTombstone { .. } => {
                        let _ = new_log.write_soft_tombstone(&key)?;
                    }
                }
            }
            match next_lower {
                Some(l) => lower = l,
                None => break,
            }
        }
        new_log.flush_buf()?;
// M53 记账
        #[cfg(test)]
        crate::common::disk::note_fsync(crate::common::disk::SITE_BITCASK_LOG_FLUSH);
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
                // M53：占位 ⇒ 账上没有未 fsync 的写入
                dirty: false,
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
            // M53：刚 fsync 过 ⇒ 账上没有未 fsync 的写入
            dirty: false,
        };
        self.active_id = 0;
        // M43：索引不再"抄"出来，而是**重扫新日志**扫出来。三件事的顺序是死的：
        //
        // 1. 先删 hint —— hint 是旧索引的持久化副本，旧索引里那些 `pos` 说的是已被删掉/改名的
        //    文件。留着它，万一就崩在下面那趟重扫中间，下次 open 会拿它当真索引（长度检查、
        //    `hint_files_exist` 两道兜底都不是为这一支设计的，见 `dump_hint` 上那段注释）。
        //    hint 只是缓存，删掉的代价是下次 open 多扫一遍日志。
        // 2. 再腾掉旧索引 —— 这一步才把"两份索引同时在"变成"一份"，是这一刀的收益本身。
        // 3. 置标志 → 原地重扫 → 成功才放标志。重扫失败就带着 Err 返回并保持隔离：
        //    此时磁盘上的日志是完整的，只是内存里没人知道有哪些键 ⇒ 除了重新 open 没有第二条路。
        let _ = std::fs::remove_file(self.hint_path());
        self.key_dir = KeyDir::new();
        self.index_rebuilding = true;
        #[cfg(test)]
        arm_merge_rescan_counter();
        let rebuilt = self.active.load_index_from(&mut self.key_dir, 0, 0);
        #[cfg(test)]
        disarm_merge_rescan_counter();
        if let Err(e) = rebuilt {
            log::error!(
                "bitcask: merge 后的索引重扫失败（{}），引擎隔离中：{}",
                self.base_path.display(),
                e
            );
            return Err(e);
        }
        self.index_rebuilding = false;

        let _ = std::fs::remove_file(&hold_path);

        let _ = self.dump_hint();
        Ok(())
    }

    // ── 经典 KV API ──────────────────────────────────────────────────────────

    pub fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.usable()?;
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
        // 这一道守卫挡的是**静默的"键不存在"**：隔离期里 KeyDir 是空的，不拦就得给每个键
        // 回 `Ok(None)` —— 门面会把它当成"这版本没有值"，往上就是数据消失。
        self.usable()?;
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
        self.usable()?;
        self.active.write_hard_delete(key)?;
        self.key_dir.remove(key);
        self.maybe_rotate()?;
        Ok(())
    }

    /// flush 写缓冲 + fsync 数据文件
    pub fn flush(&mut self) -> Result<()> {
        self.active.flush_buf()?;
        // M53：**账上干净就不付 fsync**（改前每次 open 末尾那一句 `tree.flush()` 都要白付一次——
        // 干净重开一个没人写过的库，文件里没有任何未落盘的字节）。
        // ⚠️ 反向的那半不能省：写过就必须 fsync，否则这条"优化"会把持久化点一起省掉
        // （M8 的 `Wal::append` 是同一课）。守卫见 `test_flush_skips_fsync_only_when_nothing_was_written`。
        if !self.active.dirty {
            return Ok(());
        }
        // M53 记账
        #[cfg(test)]
        crate::common::disk::note_fsync(crate::common::disk::SITE_BITCASK_LOG_FLUSH);
        self.active.file.sync_all()?;
        self.active.dirty = false;
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
        // 隔离期不许落盘：索引是空的、也不会再重扫，写进去的行没人记账 ⇒ 回 `Ok` 就是骗人
        self.usable()?;
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
        self.usable()?;
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
        self.usable()?;
        self.active.write_hard_delete(key)?;
        self.key_dir.remove(key);
        self.maybe_rotate()?;
        Ok(())
    }

    /// `None` = 键不在索引；`Some(None)` = 软 tombstone；`Some(Some(v))` = 存活
    pub fn get_entry(&mut self, key: &[u8]) -> Result<Option<Option<Vec<u8>>>> {
        // `None` 在门面里是"这版本没这一项"，不是错误 ⇒ 隔离期必须拦，否则等于宣称全库空了
        self.usable()?;
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
        self.usable()?;
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
        self.usable()?;
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

    /// `merge()` 走 KeyDir 必须**按块**，不许一次把整库的 `(键, 位置)` 抄进一个 `Vec`（M40）。
    ///
    /// 判据（单位：把键）：库里活键数远多于一块时，单块抄出的最大键数 `merge_chunk_max()`
    /// 必须 ≤ `2 × MERGE_CHUNK_KEYS`。改前那份 `snapshot: Vec<(k.clone(), loc.clone())>`
    /// 在 600k 键 × 2 版那档分段实测 **115.0 MB**（100 B/扫描行），是 `vacuum()` 现在唯一的
    /// 大头；块化之后它变成 O(一块)（剩下的 `new_key_dir` 是替换用的新索引，不是瞬时账）。
    /// 尺子为什么是"单块抄出多少把键"而不是内存：与四家 vacuum 的 `vac_pending_max` 同一条理由
    /// —— `mem_probe` 的高水位是进程级的，并行用例互相污染。
    ///
    /// **修订（M43）**：括号里那句"新索引不是瞬时账"当时是对的，但它把剩下的账留在了
    /// `new_key_dir` 上 —— 那份表在走查全程和旧索引**同时活着**，600k 档分段 ④ 实测 +87.9 MB。
    /// M43 已经不再抄它（索引改成重扫新日志得到），这条判据本身不变。
    ///
    /// 变异验证：把 `MERGE_CHUNK_KEYS` 临时抬到一整库那么大 ⇒ 单块抄出 20 000 把 > 界 16 384，红。
    #[test]
    fn test_merge_walks_key_dir_in_bounded_chunks() {
        const KEYS: usize = 20_000;
        let path = tmp_log("merge_chunks");
        {
            let mut eng = Bitcask::new(path.clone()).unwrap();
            for i in 0..KEYS {
                let k = format!("k{i:06}");
                eng.set(k.as_bytes(), vec![b'v'; 16]).unwrap();
            }
            assert_eq!(eng.len(), KEYS, "灌完之后活键数不对 ⇒ 形状没建起来");
            reset_merge_chunk();
            eng.merge().unwrap();
            let max_chunk = merge_chunk_max();
            eprintln!(
                "[merge chunks] {} 把活键 ⇒ 单块最多抄 {} 把（块界 {MERGE_CHUNK_KEYS}）",
                eng.len(),
                max_chunk
            );
            assert!(KEYS > MERGE_CHUNK_KEYS * 2, "这条判据要求库比两块还大（否则界不会被跨过）");
            assert!(max_chunk > 0, "这把尺子没数到块 ⇒ 空转");
            assert!(
                max_chunk <= 2 * MERGE_CHUNK_KEYS as u64,
                "单块抄出 {max_chunk} 把键（界 {}）⇒ 又变回整库一份快照",
                2 * MERGE_CHUNK_KEYS as u64
            );
            // 分块走之后数据必须还是完整的（顺序、条数、值）
            assert_eq!(eng.len(), KEYS, "merge 之后活键数变了");
            for i in (0..KEYS).step_by(997) {
                let k = format!("k{i:06}");
                assert_eq!(
                    eng.get(k.as_bytes()).unwrap(),
                    Some(vec![b'v'; 16]),
                    "第 {i} 把键在分块 merge 之后读不回来"
                );
            }
        }
        {
            let eng = Bitcask::new(path.clone()).unwrap();
            assert_eq!(eng.len(), KEYS, "重开之后活键数不对 ⇒ 分块 merge 写坏了日志");
        }
        cleanup(&path);
    }

    /// merge 之后那把索引必须是**重扫新日志**扫出来的，不是走查时一路抄出来的（M43）。
    ///
    /// 为什么钉"重扫"这个动作、不钉"全场只有一份索引"：`KeyDir` 只是 `BTreeMap` 的别名，
    /// 要数"堆上同时活着几把索引"就得把它包成 newtype —— 那是破坏性 API 改动。
    /// "这次 merge 折进索引多少行"是同一件事的可观测面，而且回归成 M40 及之前那份
    /// `new_key_dir`（边抄边记位置、根本不重扫）时它直接掉回 **0**。
    ///
    /// 后半段钉的是**先腾掉旧索引**这半步：旧索引里那些 `pos` 说的是已经不存在的位置。
    /// 如果跳过 `key_dir = KeyDir::new()` 直接在旧表上重扫，被硬删过的键不会从索引里消失，
    /// 而它的 `file_id` 恰好等于新 active 的 0 ⇒ 读它会读到**别的键的字节**。这正是
    /// [`Bitcask::usable`] 要拦的那一类静默错值。
    #[test]
    fn test_merge_rebuilds_index_by_rescanning_the_log() {
        const KEYS: usize = 20_000;
        let path = tmp_log("merge_rescan");
        {
            let mut eng = Bitcask::new(path.clone()).unwrap();
            for i in 0..KEYS {
                let k = format!("k{i:06}");
                eng.set(k.as_bytes(), vec![b'v'; 16]).unwrap();
                // 第二版：让日志里有一半以上是可回收的行，merge 才有东西要重写
                eng.set(k.as_bytes(), vec![b'w'; 24]).unwrap();
            }
            // 硬删掉一部分 —— 这些键 merge 之后必须彻底不在索引里
            for i in (0..KEYS).step_by(7) {
                eng.delete(format!("k{i:06}").as_bytes()).unwrap();
            }
            let live_before = eng.len();
            arm_merge_rescan_counter();
            eng.merge().unwrap();
            let rows = merge_rescan_rows();
            eprintln!(
                "[merge rescan] 活键 {live_before} 把 ⇒ 这次 merge 重扫折进索引 {rows} 行"
            );
            assert!(rows > 0, "重扫记账一次都没响 ⇒ 索引不是扫出来的（又抄回第二把了？）");
            assert_eq!(
                rows,
                eng.len() as u64,
                "重扫 {rows} 行 ≠ merge 之后索引里 {live_before} 条 ⇒ 索引不来自这次重扫"
            );
            assert_eq!(eng.len(), live_before, "merge 之后活键数变了");

            for i in (0..KEYS).step_by(7) {
                let k = format!("k{i:06}");
                assert_eq!(
                    eng.get(k.as_bytes()).unwrap(),
                    None,
                    "{k} 在 merge 之前就被硬删，之后还读得到 ⇒ 旧索引没腾掉，读到的是错位的数据"
                );
            }
            let mut checked = 0usize;
            for i in 0..KEYS {
                if i % 7 == 0 {
                    continue;
                }
                let k = format!("k{i:06}");
                assert_eq!(
                    eng.get(k.as_bytes()).unwrap(),
                    Some(vec![b'w'; 24]),
                    "第 {i} 把存活键在重扫之后读回来的不是最新版本"
                );
                checked += 1;
                if checked >= 32 {
                    break;
                }
            }
        }
        {
            let mut eng = Bitcask::new(path.clone()).unwrap();
            assert_eq!(eng.len(), KEYS - KEYS.div_ceil(7), "重开之后活键数不对");
            assert_eq!(
                eng.get(b"k000000").unwrap(),
                None,
                "重开之后硬删过的键复活了 ⇒ merge 写坏了日志"
            );
            assert_eq!(
                eng.get(b"k000001").unwrap(),
                Some(vec![b'w'; 24]),
                "重开之后存活键读不回来"
            );
        }
        cleanup(&path);
    }

    /// 重扫窗口里每一扇**能报错的门**都必须报错，而不是回「键不存在」（M43）。
    ///
    /// 这一条买的是 B1 的那笔代价：为了不把峰值抬到两份索引，旧索引必须在重扫之前丢掉，
    /// 而丢掉的这段时间里磁盘上明明有全库、内存里却没人知道有哪些键。让它回 `Ok(None)`
    /// 就是宣称「这些键不存在」—— 门面会当真是（`get_entry` 的 `None` 在上层是「这版本没
    /// 这一项」）。宁可让调用方拿到一个错。
    ///
    /// hint 那一支单独钉：hint 是索引的**持久化副本**，空索引写成 hint 落盘，下次 open 就把它
    /// 当真索引读回来 —— 那是把「一次失败」变成「整库看不见」。open 侧的 `hint_files_exist`
    /// 兜不住（空表里挑不出指向缺失分片的条目）。
    ///
    /// 变异验证：① 去掉 `get` 里的 `usable()?` ⇒ 第一条断言红在「回了 Some」；
    /// ② 去掉 `insert`/`set` 那两道 ⇒ 红在「被拒的写入还是落盘了」；
    /// ③ 去掉 `dump_hint` 那道 ⇒ 红在「隔离期把 hint 落盘了」。
    #[test]
    fn test_quarantined_engine_errors_instead_of_claiming_keys_missing() {
        let path = tmp_log("quarantine");
        let hint = hint_path_for(&path);
        {
            let mut eng = Bitcask::new(path.clone()).unwrap();
            eng.set(b"a", b"1".to_vec()).unwrap();
            eng.merge().unwrap(); // 正常路径走完：此时索引完整、可读写
            eng.flush().unwrap();
            let len_before = std::fs::metadata(&path).unwrap().len();

            eng.quarantine_index_for_test();

            // ── 读侧：不许把「索引里没有」说成「键不存在」──
            let g = eng.get(b"a");
            assert!(
                g.is_err(),
                "隔离期 get 回了「{:?}」⇒ 空索引被当成「键都不存在」",
                g.ok()
            );
            let ge = eng.get_entry(b"a");
            assert!(
                ge.is_err(),
                "隔离期 get_entry 回了「{:?}」⇒ 上层会把它当成「这版本没有值」",
                ge.ok()
            );
            assert!(
                eng.range_scan(b"a".to_vec(), b"z".to_vec()).is_err(),
                "隔离期 range_scan 回了 Ok(空)"
            );
            assert!(eng.iter().is_err(), "隔离期 iter 回了 Ok(空)");

            // ── 写侧：一条都不许落盘 ──
            assert!(eng.set(b"b", b"2".to_vec()).is_err(), "隔离期 set 竟然成功");
            assert!(
                eng.insert(b"c".to_vec(), Some(b"3".to_vec())).is_err(),
                "隔离期 insert 竟然成功"
            );
            assert!(
                eng.insert_batch(vec![(b"d".to_vec(), Some(b"4".to_vec()))])
                    .is_err(),
                "隔离期 insert_batch 竟然成功"
            );
            assert!(eng.delete(b"a").is_err(), "隔离期 delete 竟然成功");
            assert!(eng.remove(b"a").is_err(), "隔离期 remove 竟然成功");

            // ── hint：不许把空索引写下去 ──
            let _ = std::fs::remove_file(&hint);
            assert!(eng.dump_hint().is_err(), "隔离期 dump_hint 回了 Ok");
            assert!(!hint.exists(), "隔离期把 hint 落盘了 ⇒ 下次 open 会当真索引读回来");

            eng.flush().unwrap();
            let len_after = std::fs::metadata(&path).unwrap().len();
            assert_eq!(
                len_before,
                len_after,
                "被拒的写入还是落盘了（差 {} 字节）",
                len_after.saturating_sub(len_before)
            );

            // ── 拦下来的是那个**状态**，不是把对象弄坏了 ──
            eng.release_index_for_test();
            eng.set(b"e", b"5".to_vec()).unwrap();
            eng.flush().unwrap();
            assert!(
                std::fs::metadata(&path).unwrap().len() > len_after,
                "解除隔离之后新写入没落盘 ⇒ 守卫动到了文件"
            );
            assert_eq!(
                eng.get(b"e").unwrap(),
                Some(b"5".to_vec()),
                "解除隔离之后同一句柄的新写入读不回来"
            );
            // 旧键此时在内存里确实没有了 —— 窗口里索引本来就是空的，这正是每一扇门都要
            // 拦下来的原因（让它悄悄回 `Ok(None)` 就等于宣称「这键不存在」）。
            assert_eq!(eng.get(b"a").unwrap(), None, "解除隔离之后旧键不该凭空回来：出路只有重新 open");

            // ── 退回隔离，然后让它出作用域：Drop 会 flush + dump_hint，后者必须写不下去 ──
            eng.quarantine_index_for_test();
            eng.flush().unwrap();
        }
        // ── 重新 open 是唯一出路，而它真的救得回来 ──
        assert!(
            !hint.exists(),
            "带着隔离状态 drop 之后 hint 落盘了 ⇒ 下次 open 会把空索引当真"
        );
        {
            let mut eng = Bitcask::new(path.clone()).unwrap();
            assert_eq!(
                eng.get(b"a").unwrap(),
                Some(b"1".to_vec()),
                "重新 open 读不回旧键 ⇒ 隔离期把日志写坏了"
            );
            assert_eq!(
                eng.get(b"e").unwrap(),
                Some(b"5".to_vec()),
                "重新 open 读不回隔离解除后的新写入"
            );
            assert_eq!(eng.len(), 2);
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

    /// M53 的守卫：`flush()` 那一刀要**正反两面**都钉住。
    ///
    /// - 正面（省下来的）：账上没有未落盘的写入时不许付 fsync —— 改前每次 open 末尾那句
    ///   无条件 `tree.flush()` 都要白付一次，干净重开一个没人动过的库也是。
    /// - 反面（不许省过头）：**写过就必须 fsync**。M8 那一课是反方向的同一个错：
    ///   一个"优化"把提交点的 fsync 一起省掉了。所以第三条断言钉 `dirty` 被正确清掉，
    ///   第四条钉"只 `flush_buf` 不 fsync"之后 `flush()` 必须补上那一次
    ///   （bulk 路径正是靠这个：字节已经在文件里、还没 fsync，此时 `buf.is_empty()` 是空的、
    ///   账却不干净 —— 这就是判据用 `dirty` 而不是用 `buf.is_empty()` 的原因）。
    #[test]
    fn test_flush_fsyncs_only_when_something_was_written() {
        use crate::common::disk::{fsync_counts, fsync_counts_clear};
        let path = tmp_log("flush_fsync");
        let mut kv = Bitcask::new(path.clone()).unwrap();

        // ① 什么都没写 ⇒ flush 不付 fsync
        fsync_counts_clear();
        kv.flush().unwrap();
        assert_eq!(
            fsync_counts().iter().sum::<u64>(),
            0,
            "空账还 fsync ⇒ 每次 open 白付一次（M53 数出来的那一格）"
        );

        // ② 写过就必须付，而且只付一次
        kv.insert(b"k1".to_vec(), Some(b"v1".to_vec())).unwrap();
        fsync_counts_clear();
        kv.flush().unwrap();
        let after_write = fsync_counts();
        assert!(
            after_write.iter().sum::<u64>() >= 1,
            "写过之后 flush 没有 fsync ⇒ 持久化点被这条\"优化\"省掉了（比慢更糟）"
        );

        // ③ 账已清 ⇒ 再 flush 不重复付
        fsync_counts_clear();
        kv.flush().unwrap();
        assert_eq!(
            fsync_counts().iter().sum::<u64>(),
            0,
            "dirty 没被清 ⇒ 每次 flush 都重付一遍 fsync"
        );

        // ④ 只把缓冲落盘（bulk 中途的做法）不等于 fsync 过：随后 flush() 必须补那一次
        kv.insert(b"k2".to_vec(), Some(b"v2".to_vec())).unwrap();
        kv.flush_buf().unwrap();
        fsync_counts_clear();
        kv.flush().unwrap();
        assert!(
            fsync_counts().iter().sum::<u64>() >= 1,
            "`flush_buf` 写进文件的字节被当成\"已经 fsync\"了 ⇒ 崩溃会丢这批行"
        );
        drop(kv);
        let _ = std::fs::remove_dir_all(&path);
    }

    /// M52 的守卫：hint 读侧从"逐条 `insert`"换成"攒有序 `Vec` 再一次 `collect`"之后，
    /// 解出来的 `KeyDir` 必须与**逐条 insert 的那一份一字不差**。
    ///
    /// 为什么不只测"写一遍再读回来对得上"：那只证明**新写新读自洽**（M42 立过这条：格式类改动
    /// 的凭据得是独立基准），证明不了换解码形状没动语义。所以基准在测试里**手工逐条 insert** 出来。
    ///
    /// 覆盖面（少一种就有一种退化溜过去）：
    /// - 存活行 / 软墓碑行 / **未知种类**的行（改前是静默丢弃 ⇒ 必须继续丢弃，不能变成"当作存活"）
    /// - **重复键**（`collect` 与逐条 `insert` 同为"后者胜"，这条不对称一旦不同就是静默改数据）
    /// - v1 与 v2 两份格式（v1 没有 `file_id`，一律 0）
    /// - **降序**写出的一份 hint：`write_hint_file` 永远不会产出降序，但读侧不该依赖它 ——
    ///   依赖"有序"只是性能优化，正确性必须与顺序无关。
    #[test]
    fn test_hint_read_matches_record_by_record_insert() {
        let path = tmp_log("hint_shape");
        let hint = hint_path_for(&path);
        // (键, flag, file_id, pos, len)
        let rows: Vec<(Vec<u8>, i32, u32, u64, u32)> = vec![
            (b"aa".to_vec(), HINT_FLAG_LIVE, 3, 64, 11),
            (
                b"bb".to_vec(),
                FLAG_SOFT_TOMBSTONE,
                4,
                0,
                0,
            ),
            (b"cc".to_vec(), 99, 5, 128, 7), // 未知种类 ⇒ 两边都不许把它当存活键
            (b"dup".to_vec(), HINT_FLAG_LIVE, 1, 200, 3),
            (b"dup".to_vec(), FLAG_SOFT_TOMBSTONE, 2, 0, 0), // 重复键 ⇒ 后者胜
            (b"ee".to_vec(), HINT_FLAG_LIVE, 6, 300, 9),
        ];
        for (v2, order) in [(true, false), (true, true), (false, false)] {
            let mut feed = rows.clone();
            if order {
                feed.reverse();
            }
            build_hint_file(&hint, v2, &feed, None);
            let (got, active_id, log_size) = read_hint_file(&hint).unwrap_or_else(|e| {
                panic!("读不出自己搭的 hint（v2={v2} 降序={order}）：{e}");
            });
            // 基准：逐条 insert（= 改前的读法）
            let mut want = KeyDir::new();
            for (k, flag, fid, pos, len) in &feed {
                let fid = if v2 { *fid } else { 0 };
                match *flag {
                    HINT_FLAG_LIVE => {
                        want.insert(
                            k.clone(),
                            KeyLoc::Live {
                                file_id: fid,
                                pos: *pos,
                                len: *len,
                            },
                        );
                    }
                    FLAG_SOFT_TOMBSTONE => {
                        want.insert(k.clone(), KeyLoc::SoftTombstone { file_id: fid });
                    }
                    _ => {}
                }
            }
            assert_eq!(
                got, want,
                "解出来的 KeyDir 与逐条 insert 那份不同（v2={v2} 降序={order}）"
            );
            let want_id = if v2 { 7 } else { 0 };
            assert_eq!(active_id, want_id, "active_id（v2={v2}）");
            assert_eq!(log_size, 4096, "active_log_size（v2={v2}）");
        }
        // 坏 `key_len`：两份格式都必须报错（改前 v1 那份漏了这道检查 ⇒ 现在一并拦下），
        // 调用方据此回退全量扫。先信了荒谬的键长，就会先分配一个大 `Vec` 再去读。
        for v2 in [true, false] {
            build_hint_file(&hint, v2, &rows, Some(600 << 20));
            let err = read_hint_file(&hint).unwrap_err();
            assert_eq!(
                err.kind(),
                std::io::ErrorKind::InvalidData,
                "坏 key_len 没被拦住（v2={v2}）"
            );
        }
        cleanup(&path);
    }

    /// 手工按文档布局搭一份 hint（不经 `write_hint_file`，为的是能造出未知种类 / 重复键 / 降序）。
    /// `lie_last_key_len` = 把**最后一条**记录声明的键长改成这个值（造坏 hint 用）。
    fn build_hint_file(
        path: &Path,
        v2: bool,
        rows: &[(Vec<u8>, i32, u32, u64, u32)],
        lie_last_key_len: Option<usize>,
    ) {
        let mut bytes = Vec::new();
        if v2 {
            bytes.extend_from_slice(HINT_MAGIC_V2);
            bytes.extend_from_slice(&7u32.to_le_bytes());
            bytes.extend_from_slice(&0u32.to_le_bytes());
            bytes.extend_from_slice(&4096u64.to_le_bytes());
            bytes.extend_from_slice(&(rows.len() as u64).to_le_bytes());
            bytes.extend_from_slice(&0u64.to_le_bytes());
        } else {
            bytes.extend_from_slice(HINT_MAGIC_V1);
            bytes.extend_from_slice(&4096u64.to_le_bytes());
            bytes.extend_from_slice(&(rows.len() as u64).to_le_bytes());
            bytes.extend_from_slice(&0u64.to_le_bytes());
        }
        for (i, (k, flag, fid, pos, len)) in rows.iter().enumerate() {
            let declared = match lie_last_key_len {
                Some(n) if i + 1 == rows.len() => n,
                _ => k.len(),
            };
            bytes.extend_from_slice(&(declared as u32).to_le_bytes());
            bytes.extend_from_slice(&flag.to_le_bytes());
            if v2 {
                bytes.extend_from_slice(&fid.to_le_bytes());
            }
            bytes.extend_from_slice(&pos.to_le_bytes());
            bytes.extend_from_slice(&len.to_le_bytes());
            bytes.extend_from_slice(k);
        }
        std::fs::write(path, bytes).unwrap();
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

    /// M48 的**归因**台架（只量不改）：大库台架的分段读数把 600k 档冷重开的 320 ms 里
    /// 307.9 ms 钉在了 `read_hint_file` 这一格，但那一格里面叠着**三件**事：
    /// ① 逐字段解记录（五次 `read_exact` + 一次带长度的读）、② 每条键一次 `vec![0u8; key_len]`、
    /// ③ 每折一条就往 `KeyDir`（**`BTreeMap<Vec<u8>, _>`**，不是哈希表）插一次。
    ///
    /// 为什么不能报一句"hint 反序列化 308 ms"就收：那一句既没说是**分配**还是**树**，
    /// 也没说单价随规模怎么涨 —— 而这两条决定下一步动哪里。树那一支尤其有理由怀疑：
    /// 键是变长字节串（比较是 `memcmp`），hint 里的键又正好是**升序**写出的
    /// ⇒ 每次插入都要沿最右那一路走下去。三臂读**同一个文件**、只差②③做不做。
    ///
    /// ```text
    /// HINT_LOAD_ROWS=600000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 bitcask_hint_load_stage_split
    /// ```
    /// ⚠️ 单线程跑；每一臂跑两遍取**第二遍**（第一遍要把文件读进 OS 缓存）。
    #[test]
    #[ignore = "hint 反序列化的三臂拆分，手动 release 单线程跑：HINT_LOAD_ROWS=600000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 bitcask_hint_load_stage_split"]
    fn bitcask_hint_load_stage_split() {
        let rows: usize = std::env::var("HINT_LOAD_ROWS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(200_000);
        let path = tmp_log("hint_split");
        let hint = hint_path_for(&path);

        // 造一份与真库同形状的 hint：16 字节的键（MVCC 的编码键 = raw(8) ‖ version(8)，同宽）、
        // 按键升序（`write_hint_file` 走的就是排好序的 KeyDir，所以台架和现场同一个顺序）。
        let mut kd = KeyDir::new();
        for i in 0..rows {
            let mut k = vec![0u8; 16];
            k[..8].copy_from_slice(&(i as u64).to_be_bytes());
            k[8..].copy_from_slice(&(i as u64 + 1).to_le_bytes());
            kd.insert(
                k,
                KeyLoc::Live {
                    file_id: 0,
                    pos: i as u64 * 64,
                    len: 24,
                },
            );
        }
        write_hint_file(&hint, &kd, 0, 1).expect("写 hint");
        let bytes = std::fs::metadata(&hint).map(|m| m.len()).unwrap_or(0);
        drop(kd);

        // 一把读法，三臂共用：`on_entry` 决定"这条键要不要留下来、留下来放哪儿"。
        // scratch 是**复用**的 ⇒ 臂 B 里每键零分配（这才把②单独摘出来）。返回条目数。
        fn walk(hint: &std::path::Path, mut on_entry: impl FnMut(&[u8], i32, u32, u64, u32)) -> usize {
            use std::io::{BufReader, Read};
            let f = std::fs::File::open(hint).unwrap();
            let mut r = BufReader::new(f);
            let mut magic = [0u8; 8];
            r.read_exact(&mut magic).unwrap();
            assert_eq!(&magic, HINT_MAGIC_V2, "台架只量 v2 那份格式");
            let mut rest = [0u8; HINT_HEADER_SIZE_V2 - 8];
            r.read_exact(&mut rest).unwrap();
            let entry_count = u64::from_le_bytes(rest[16..24].try_into().unwrap()) as usize;
            let mut nbuf = [0u8; 4];
            let mut pbuf = [0u8; 8];
            let mut scratch: Vec<u8> = Vec::new();
            for _ in 0..entry_count {
                r.read_exact(&mut nbuf).unwrap();
                let key_len = u32::from_le_bytes(nbuf) as usize;
                r.read_exact(&mut nbuf).unwrap();
                let flag = i32::from_le_bytes(nbuf);
                r.read_exact(&mut nbuf).unwrap();
                let file_id = u32::from_le_bytes(nbuf);
                r.read_exact(&mut pbuf).unwrap();
                let pos = u64::from_le_bytes(pbuf);
                r.read_exact(&mut nbuf).unwrap();
                let len = u32::from_le_bytes(nbuf);
                scratch.resize(key_len, 0);
                r.read_exact(&mut scratch).unwrap();
                on_entry(&scratch, flag, file_id, pos, len);
            }
            // ⚠️ 这条不是走形式：循环体一次都没进的话，"读全了"和"一条没读"都报 0 ms
            assert_eq!(entry_count, rows_hinted(hint), "hint 里的条目数与文件大小对不上");
            entry_count
        }
        /// 独立算一遍分母：(文件字节 − 头部) ÷ 每条 (24 B 定长字段 + 16 B 键)
        fn rows_hinted(hint: &std::path::Path) -> usize {
            let bytes = std::fs::metadata(hint).map(|m| m.len()).unwrap_or(0) as usize;
            (bytes - HINT_HEADER_SIZE_V2) / (24 + 16)
        }

        let ms = |d: std::time::Duration| d.as_secs_f64() * 1e3;
        let per = |d: std::time::Duration| d.as_secs_f64() * 1e6 / rows as f64;
        let mut b_best = std::time::Duration::MAX;
        let mut c_best = std::time::Duration::MAX;
        let mut a_best = std::time::Duration::MAX;
        let mut d_best = std::time::Duration::MAX;
        let mut e_best = std::time::Duration::MAX;
        let mut a_live = 0i64;
        let mut e_live = 0i64;
        let mut a_over = 0i64;
        let mut e_over = 0i64;
        let mut d_over = 0i64;
        // M59（#67 的归因臂）：F = 精确预留那份清单，G = 什么都不预留（靠翻倍扩容）
        let mut f_best = std::time::Duration::MAX;
        let mut g_best = std::time::Duration::MAX;
        let mut f_s1 = 0i64; // 清单建好时的净活字节
        let mut f_s2 = 0i64; // 树建好时的净活字节
        let mut f_over = 0i64;
        let mut g_s1 = 0i64;
        let mut g_s2 = 0i64;
        let mut g_over = 0i64;
        let mut seen = 0usize;

        // ⚠️ **四轮、每轮两种顺序交替、每臂取最小**（M52 补的方法，理由有两条，都是本机踩过的）：
        // ① 顺序偏差：五臂排在**最后**那一臂会替前面几臂的分配器碎片状态买单 —— 第一版只跑两轮
        //    固定顺序，E 臂读到 557 ms，而同一个形状在 M48 那一轮的 A 臂是 270~320 ms（1.7~2.1×），
        //    这个差跟"逐条插树"没关系，纯粹是排位。② **刚编译完那一轮不读数**（本机 1.3~1.9×）：
        //    这一趟就是紧接着 `cargo test --release` 编译起来的第一轮。取最小 = 只信最快那一轮。
        macro_rules! arm_b {
            () => {{
                let t = std::time::Instant::now();
                seen = walk(&hint, |_k, _f, _id, _p, _l| {});
                let d = t.elapsed();
                if d < b_best {
                    b_best = d;
                }
            }};
        }
        macro_rules! arm_c {
            () => {{
                let mut out: Vec<(Vec<u8>, KeyLoc)> = Vec::with_capacity(rows);
                let t = std::time::Instant::now();
                walk(&hint, |k, _f, id, p, l| {
                    out.push((k.to_vec(), KeyLoc::Live { file_id: id, pos: p, len: l }));
                });
                let d = t.elapsed();
                assert_eq!(out.len(), rows, "臂 C 少折了条目");
                drop(out);
                if d < c_best {
                    c_best = d;
                }
            }};
        }
        // ── 臂 A：真门 —— `read_hint_file`（M52 之后：流式解码 + 有序 `collect` 成树）──
        // ⚠️ 顺手量两件事，都不靠墙钟：
        // ① **树建好之后还活着多少字节**（`a_live`/`e_live`）：大库台架那一格读到
        //    `冷重开 常驻 83.4 → 57.7 MB`，而两臂解出来的键数一模一样 ⇒ 差的不可能是内容，
        //    只能是形状（升序逐条 `insert` 会把每个叶/内部节点劈成半满，有序批量填则填满）。
        // ② **建的过程中超出常驻多少**（`a_over`/`e_over`）：这一格是为了回答"流式解码到底有没有
        //    把中间那份 `Vec` 省掉"—— 如果 `BTreeMap::from_iter` 自己先把项攒下来，
        //    换不换写法峰值一样，那就该选简单的那一份写法（M52c 靠这一格定去留）。
        macro_rules! arm_a {
            () => {{
                let l0 = crate::mem_probe::live() as i64;
                let pbase = crate::mem_probe::reset_peak();
                let t = std::time::Instant::now();
                let (got, _, _) = read_hint_file(&hint).unwrap();
                let d = t.elapsed();
                let alive = crate::mem_probe::live() as i64 - l0;
                let over = crate::mem_probe::peak().saturating_sub(pbase) as i64 - alive;
                assert_eq!(got.len(), rows, "臂 A 少折了条目");
                drop(got);
                if d < a_best {
                    a_best = d;
                    a_live = alive;
                    a_over = over;
                }
            }};
        }
        // ── 臂 D：同样解 + 分配，但**先攒进有序的 `Vec`，再一次 `collect` 成树**──
        // 为什么加这一臂：hint 里的键本来就是升序写出的，而 ③ 那 74% 到底是
        // "插一棵 BTreeMap  unavoidable"，还是"逐条 insert 每次都要从根走一遍"，
        // 只有把这两件事拆开才答得了 —— 这一臂就是 M52 实际采用的形状。
        macro_rules! arm_d {
            () => {{
                let t0 = std::time::Instant::now();
                let mut acc: Vec<(Vec<u8>, KeyLoc)> = Vec::with_capacity(rows);
                walk(&hint, |k, _f, id, p, l| {
                    acc.push((k.to_vec(), KeyLoc::Live { file_id: id, pos: p, len: l }));
                });
                let l0 = crate::mem_probe::live() as i64;
                let pbase = crate::mem_probe::reset_peak();
                let built: KeyDir = acc.into_iter().collect();
                let d = t0.elapsed();
                let alive = crate::mem_probe::live() as i64 - l0;
                let over = crate::mem_probe::peak().saturating_sub(pbase) as i64 - alive;
                assert_eq!(built.len(), rows, "臂 D 少折了条目");
                drop(built);
                if d < d_best {
                    d_best = d;
                    d_over = over;
                }
            }};
        }
        // ── 臂 E：M52 **改前**的真门形状（解 + 每键分配 + 逐条 `insert` 进 `BTreeMap`）──
        // 为什么留这一臂：M52 把 A 换成了 D 那个形状，于是"改前的 A"在这个二进制里再也读不到，
        // 只能拿 E 复现它。A/E 必须在**同一份构建、同一个文件**上比（跨构建的 µs 在本机漂 1.3~1.9×）。
        macro_rules! arm_e {
            () => {{
                let l0 = crate::mem_probe::live() as i64;
                let pbase = crate::mem_probe::reset_peak();
                let t = std::time::Instant::now();
                let mut old_shape = KeyDir::new();
                walk(&hint, |k, _f, id, p, l| {
                    old_shape.insert(
                        k.to_vec(),
                        KeyLoc::Live {
                            file_id: id,
                            pos: p,
                            len: l,
                        },
                    );
                });
                let d = t.elapsed();
                let alive = crate::mem_probe::live() as i64 - l0;
                let over = crate::mem_probe::peak().saturating_sub(pbase) as i64 - alive;
                assert_eq!(old_shape.len(), rows, "臂 E 少折了条目");
                drop(old_shape);
                if d < e_best {
                    e_best = d;
                    e_live = alive;
                    e_over = over;
                }
            }};
        }

        // ── 臂 F / G（M59，为 #67 那笔 21.5 MB）：与 A 同一个流式解码、同一条建树门，
        //    只差"那份中间清单怎么攒"，并且**把窗口内部分成三段读数**：
        //      s1 = 清单建好（键载荷 + `Vec` 骨架），s2 = 树建好，over = 全程峰值 − s2。
        //    ⚠️ 单靠 A(50.3) 与 D(28.8) 的差判不出那 21.5 住在哪 —— 三个嫌疑：
        //      ① `Vec` 翻倍扩容留下的空槽（capacity 可以高到 len 的 ~2 倍）；
        //      ② std 的 `BTreeMap::from_iter` 自己怎么攒那份 `Vec`；
        //      ③ 有序批量填充时节点本身的攒法。
        //    F（`with_capacity(rows)`）与 G（`Vec::new()`，只靠翻倍）**除预留外一字不差**
        //    ⇒ F/G 的 over 差 = 嫌疑①；F 的 over 若仍 ≈ A ⇒ 与预留无关（嫌疑在②③）。
        macro_rules! arm_list {
            ($reserve:expr, $best:ident, $s1:ident, $s2:ident, $over:ident) => {{
                let l0 = crate::mem_probe::live() as i64;
                let pbase = crate::mem_probe::reset_peak();
                let t = std::time::Instant::now();
                let mut acc: Vec<(Vec<u8>, KeyLoc)> = $reserve;
                walk(&hint, |k, _f, id, p, l| {
                    acc.push((k.to_vec(), KeyLoc::Live {
                        file_id: id,
                        pos: p,
                        len: l,
                    }));
                });
                let a1 = crate::mem_probe::live() as i64 - l0;
                let built: KeyDir = acc.into_iter().collect();
                let a2 = crate::mem_probe::live() as i64 - l0;
                let d = t.elapsed();
                assert_eq!(built.len(), rows, "清单臂少折了条目");
                let pk = crate::mem_probe::peak();
                drop(built);
                if d < $best {
                    $best = d;
                    $s1 = a1;
                    $s2 = a2;
                    $over = pk.saturating_sub(pbase) as i64 - a2;
                }
            }};
        }

        for round in 0..4 {
            if round % 2 == 0 {
                arm_b!();
                arm_c!();
                arm_a!();
                arm_d!();
                arm_list!(Vec::with_capacity(rows), f_best, f_s1, f_s2, f_over);
                arm_list!(Vec::new(), g_best, g_s1, g_s2, g_over);
                arm_e!();
            } else {
                arm_e!();
                arm_list!(Vec::new(), g_best, g_s1, g_s2, g_over);
                arm_list!(Vec::with_capacity(rows), f_best, f_s1, f_s2, f_over);
                arm_d!();
                arm_a!();
                arm_c!();
                arm_b!();
            }
        }
        eprintln!(
            "=== hint 三臂 rows={rows} 文件 {:.1} MB（每条 {:.1} B）解出 {seen} 条｜四轮两种顺序交替、每臂取最小 ===",
            bytes as f64 / 1e6,
            bytes as f64 / rows.max(1) as f64
        );
        eprintln!(
            "  B 只解字段        {:>8.1} ms  {:>6.3} µs/条",
            ms(b_best),
            per(b_best)
        );
        eprintln!(
            "  C +每键一次分配   {:>8.1} ms  {:>6.3} µs/条  ⇒ ②分配 = {:>7.1} ms（{:.2}×）",
            ms(c_best),
            per(c_best),
            ms(c_best - b_best),
            ms(c_best) / ms(b_best)
        );
        eprintln!(
            "  A 真门 read_hint  {:>8.1} ms  {:>6.3} µs/条  ⇒ ③插 BTreeMap = {:>7.1} ms（{:.2}×）",
            ms(a_best),
            per(a_best),
            ms(a_best - c_best),
            ms(a_best) / ms(c_best).max(0.001)
        );
        eprintln!(
            "  D 攒有序 Vec 再 collect 成同一棵树 {:>8.1} ms  {:>6.3} µs/条  ⇒ 比 A {:>7.1} ms（{:.2}×）｜③里\"逐条 insert 从根走一遍\"占这一笔"
            ,
            ms(d_best),
            per(d_best),
            ms(a_best) - ms(d_best),
            ms(a_best) / ms(d_best).max(0.001)
        );
        eprintln!(
            "  E 改前形状(逐条 insert) {:>8.1} ms  {:>6.3} µs/条  ⇒ 同构建 A/B：A(现门) 比 E 省 {:>7.1} ms（{:.2}×）",
            ms(e_best),
            per(e_best),
            ms(e_best) - ms(a_best),
            ms(e_best) / ms(a_best).max(0.001)
        );
        eprintln!(
            "  建好之后**还活着**：A {:.1} MB / E {:.1} MB ⇒ 同一批 {rows} 把键，形状差 {:>5.1} MB（{:.2}×）\
             \n    ⇒ 大库台架那一格 `冷重开 常驻 83.4 → 57.7 MB` 的机制就在这里：升序逐条 `insert` 把每个节点劈成半满。",
            a_live as f64 / 1e6,
            e_live as f64 / 1e6,
            (e_live - a_live) as f64 / 1e6,
            e_live as f64 / (a_live.max(1) as f64)
        );
        // ⚠️ 这三笔 `over` 才是 M52c 的去留判据：`over` = 折树过程中**超出最终常驻**的那部分瞬时字节。
        //    600k 两轮读数一字不差：**E 逐条 insert 0.0 ｜ D 自带 `Vec` 再 collect 28.8（正好是那根骨架）
        //    ｜ A 流式 collect 50.3**。A 比我明确写出来的那根骨架还高 ⇒ "流式一定更省内存"这句**不成立**，
        //    而这 21.5 MB 差值住在哪我没归因（怀疑在 `BTreeMap` 有序批量填充自己怎么攒节点）⇒
        //    这一格**不进判据**，只登记。留流式那一版的理由只有两条实打实的：
        //    少一个 `Vec<整库记录>` 这种"中间物形状"、少一格 `with_capacity(entry_count)` 的预算陷阱
        //    （`entry_count` 从文件头读来，不可信）。时间两臂在比值噪声带里，不许当成证据。
        eprintln!(
            "  折树过程中的瞬时超出（`over`，MB）：A 流式 collect {:>6.1} ｜ D 我攒的 Vec 再 collect {:>6.1} ｜ E 逐条 insert {:>6.1}",
            a_over as f64 / 1e6,
            d_over as f64 / 1e6,
            e_over as f64 / 1e6
        );
        // M59：把 A 与 D 那 21.5 的差**拆开读**，而不是继续猜
        let mb = |b: i64| b as f64 / 1e6;
        eprintln!(
            "  M59 归因臂：F 精确预留 over {:>5.1}（清单 s1 {:>5.1} ⇒ 建树净增 {:>5.1} = 终态 {:>5.1}）\
             ｜ G 不预留（靠翻倍扩容）over {:>5.1}（s1 {:>5.1}、终态 {:>5.1}）",
            mb(f_over),
            mb(f_s1),
            mb(f_s2 - f_s1),
            mb(f_s2),
            mb(g_over),
            mb(g_s1),
            mb(g_s2),
        );
        eprintln!(
            "    ⇒ F/G 只差\"有没有预留\"这一件事 ⇒ over 差 {:>5.1} MB = 嫌疑①（翻倍扩容留下的空槽）",
            mb(g_over - f_over)
        );
        eprintln!(
            "    ⇒ A({:>5.1}) − F({:>5.1}) = {:>5.1} MB 才是剩下的嫌疑②③（std 内部怎么攒那份 Vec / 节点攒法）",
            mb(a_over),
            mb(f_over),
            mb(a_over - f_over),
        );
        assert!(
            e_live > a_live,
            "两臂解出的键数相同（各自断言过 == {rows}），常驻却不再差这么多 ⇒ \
             这一格报的是**树形状**的账（半满节点），不是内容：{a_live} vs {e_live} 字节"
        );
        eprintln!(
            "  合计拆分：① {:>5.1}% ② {:>5.1}% ③ {:>5.1}%（按 A 那一臂的墙钟）",
            100.0 * ms(b_best) / ms(a_best),
            100.0 * ms(c_best - b_best) / ms(a_best),
            100.0 * ms(a_best - c_best) / ms(a_best),
        );
        cleanup(&path);
    }
}
