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
use std::format;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::matches;
use std::ops::Bound;
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::thread_local;
use std::vec;

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
    Live { file_id: u32, pos: u64, len: u32 },
    /// 软删除：版本行仍存在，逻辑 value 为 None
    SoftTombstone { file_id: u32 },
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

// 测试专用：一轮候选折叠里**看了多少把编码键**（不是读了几次盘）。
// 为什么必须有它：`read_value_calls()` 只数取值，而窗口分页要证明的是"键流没走到底"——
// 第一版守卫就是拿取值次数当尺子，把预算整个忽略掉它照样绿。
#[cfg(test)]
thread_local! {
    static KEYDIR_VISITS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

// 测试专用（M40 的尺子）：一次 `merge()` 里**单块从 KeyDir 抄出多少把键**的历史最大值。
//
// 为什么钉这个而不是钉内存：`mem_probe` 的高水位是进程级的、并行用例互相污染（M27/M36 各栽过
// 一次），而"块界有没有生效"是个纯计数 —— 与四家 vacuum 那条 `vac_pending_max` 同形。
#[cfg(test)]
thread_local! {
    static MERGE_CHUNK_MAX: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
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

// ── M60 的尺子：merge 之后重建索引**走没走批量那一扇门** ──
//
// 为什么钉这个而不是钉"常驻少了 30 MiB"：后者要跑大库（`bitcask_vacuum_retained_two_passes` 那把
// release 台架量的就是它），CI 里跑不动；而"走了批量门"是形状成立的**前提** —— 回归成逐条 `insert`
// （把 `MERGE_BULK_OLD_SHAPE` 打开，或者把门拆掉）这个数就掉回 0。
#[cfg(test)]
thread_local! {
    static MERGE_BULK_REBUILDS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static MERGE_BULK_FALLBACKS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

// ── M62：`merge()` **内部**的分段账（门控 `MERGE_STAGE_SPLIT`，`#[cfg(test)]` ⇒ 不随 crate 发布）──
//
// 要回答的问题：复测里同一句 `merge()` 从两扇门进来读出两个瞬时峰值（vacuum 那扇 +5.8 MB /
// `flush+merge` 那扇 +18.4 MB）。外层那把尺子只能看到"这一整段涨了多少" ⇒ 看不见**什么时候腾掉旧的、
// 什么时候清单攒满、什么时候树建完** ⇒ 按 M56 那一课（一把总桶 + 猜哪段花钱 = 会把因果读反），先拆段。
//
// 每段记两个数，都是**相对 merge 进门那一刻**的活字节：
//   live[slot] = 段末还活着多少（净账，会随"腾掉旧索引"变成负的）
//   peak[slot] = 这一段内部相对段首最多涨到多少（瞬时账，段末就 reset 回去）
// ⇒ 两栏分开才分得清"这一段的净增"与"这一段中途的尖峰"（M38/M59 那条：净增 0 不代表没花过）。
//
// ⚠️ 门控必须严：`reset_peak()` 是把水位**压低**，常开会锯短外层 `timed()` 对同一段的读数
// （M37 那条教训，实测 171.4 → 47.2）。不设这个环境变量时这里完全是 no-op，一把尺子都不动。
#[cfg(test)]
static MERGE_LIVE: [std::sync::atomic::AtomicI64; 4] =
    [const { std::sync::atomic::AtomicI64::new(0) }; 4];
#[cfg(test)]
static MERGE_PEAK: [std::sync::atomic::AtomicU64; 4] =
    [const { std::sync::atomic::AtomicU64::new(0) }; 4];
#[cfg(test)]
static MERGE_ENTRY_LIVE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
#[cfg(test)]
static MERGE_SEG_BASE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[cfg(test)]
thread_local! {
    static MERGE_BULK_LEN: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

// 五格累计纳秒：0=walk（KeyDir 抄块）1=read_live+append 2=pre 3=rescan 4=hint。
// ⚠️ 五格必须**首尾相接**（每格从上一格的终点起算），否则加起来不等于外层墙钟 ⇒ 百分比全错。
// M81 第一版在这里栽过一次：交错的 walk/rw 用"只累加、不动水位"的记法，而收尾那格从**进门**
// 那一刻起算 ⇒ 把 walk+rw 又算进 `pre` 一遍，五格之和 2.901 s 对不上 bench 独立读到的 1.713 s。
// 现在改成一律走 `merge_t_mark`（累加 + 推水位），并在 dump 里拿"五格之和 vs 外层墙钟"自校。
#[cfg(test)]
thread_local! {
    static MERGE_T_NS: std::cell::Cell<[u64; 5]> = const { std::cell::Cell::new([0; 5]) };
    static MERGE_T_LAST: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static MERGE_T_ENTRY: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

// 第 3 项的尺子（测试专用，吃同一个 `MERGE_TIME_SPLIT` 门控）：**每个分片的磁盘字节 vs 活字节**。
//
// 为什么先量这个而不是直接写"两趟按段增量"：方案把那一刀的立项判据从"rw 占比 > 60%"换成了
// "预期省几秒 + 内存代价 + 恢复复杂度"，而"省几秒"只取决于**脏段里有多少活字节** ——
// 今天的 merge 把全部活行重写一遍，按段增量的收益上限就是"只重写脏段那些活行"。
// 分布不量出来，"值不值得为此改掉 merge 的输出形状"（今天合并完只剩一个文件、`active_id = 0`，
// 而按段保留意味着索引重扫要吃多个文件、M60 那条"按键升序批量成树"的快路径也就不成立了）
// 就是在猜。
#[cfg(test)]
thread_local! {
    static MERGE_SEG_SIZE: std::cell::RefCell<std::collections::BTreeMap<u32, u64>> =
        const { std::cell::RefCell::new(std::collections::BTreeMap::new()) };
    static MERGE_SEG_LIVE: std::cell::RefCell<std::collections::BTreeMap<u32, (u64, u64)>> =
        const { std::cell::RefCell::new(std::collections::BTreeMap::new()) };
}

// 测试专用（M77 的尺子）：hint 门**这一次给清单预留了几条**。
//
// 为什么钉这个数而不是钉时间：省下来的那 110 ms 是墙钟（本机跨轮漂 1.3~1.9×），
// 而"门到底有没有用上那条界"是个纯计数 —— 谁把 `with_capacity(界)` 换回 `new()`（或换成步长值），
// 这里立刻读到不一样的数。光有 [`hint_reserve_bound`] 的算术守卫不够：那只证明函数算得对，
// 不证明**有人调用它**（M35/M44 那条"尺子必须钉在旧形状真正流过的那扇门"）。
#[cfg(test)]
thread_local! {
    static HINT_RESERVE_NOTED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// 批量成树时一次多预留多少条清单格子（M60）。///
/// 为什么不是 `with_capacity(声明的条数)`：那个数从文件头/调用方来，**不可信** ⇒ 信它就等于
/// "读到一个大数就先做一次巨大分配"（M59 那句）。分步 `reserve_exact` 把最坏过冲夹在一个步长里：
/// 65 536 条 × 骨架 48 B ≈ **3 MB**。
///
/// 为什么不是直接 `.collect()`：M59 实测流式 `collect` 的瞬时超出 = 清单骨架 + **`Vec` 翻倍扩容留下的空槽**
/// （600k 档 `over` 50.3 MB，其中 **21.5 MB 是空槽** —— `Vec::from_iter` 拿不到 `size_hint` 就只会翻倍）。
const KEYDIR_BULK_STEP: usize = 65_536;

/// 一条 hint 记录里**定长字段**的字节数。键长可以为 0 ⇒ 这是**每条的最小占用**：
/// v2 = `key_len` 4 + `flag` 4 + `file_id` 4 + `pos` 8 + `len` 4；v1 少一个 `file_id`。
const HINT_RECORD_FIXED_BYTES_V2: u64 = 24;
const HINT_RECORD_FIXED_BYTES_V1: u64 = 20;

/// 一份**按键升序**的 `(键, 位置)` 清单，攒完再一次成树 ⇒ 满节点。
///
/// 存在的理由是 M55 那笔税：升序逐条 `insert` 把沿途每个节点劈成半满（600k 档实测 **30.5 MiB 常驻**），
/// 而 `BTreeMap` 的有序批量填充给的是满节点。两个用户共用它：hint 载入（[`hint_entries_to_keydir`]）
/// 与 merge 之后的索引重扫（[`Log::load_keydir_sorted`]）—— **一份格式不该有两种攒法**。
///
/// ⚠️ 只在输入升序**且不含硬删除**时才有资格用："后到者胜"要能对上"删除按顺序施加"，
/// 所以调用方一扫到硬删除必须整份退回逐条施加（见 [`Log::load_keydir_sorted`]）。
struct KeyDirBulk(Vec<(Vec<u8>, KeyLoc)>);

/// IO 结果
pub type Result<T> = std::result::Result<T, std::io::Error>;

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

#[derive(Clone, Copy)]
pub(crate) enum HintPolicy {
    Force,
    Amortized,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum HintRefresh {
    Written,
    Deferred { unhinted_records: u64 },
}

#[derive(Debug, PartialEq, Eq)]
enum HintReplay {
    ActiveTail,
    Rotated {
        active_id: u32,
        sealed_segments: usize,
    },
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
    /// 计入插入、覆盖、两类墓碑，以及回滚和回收。
    unhinted_records: u64,
}

#[cfg(test)]
mod tests;

// 按职责组织的内部实现；公开类型与原模块路径保持一致。
mod hint;
mod keydir;
mod lifecycle;
mod merge;
#[cfg(test)]
mod probes;
mod read;
mod record_log;
mod rotation;
mod scan;
mod write;
use hint::discover_sealed_ids;
pub use hint::hint_path_for;
#[cfg(test)]
use hint::hint_reserve_bound;
pub use hint::read_hint_file;
pub use hint::segment_path;
pub use hint::write_hint_file;
use keydir::apply_log_record;
use keydir::keydir_from_ordered_stream;
#[cfg(test)]
pub(crate) use probes::arm_merge_rescan_counter;
#[cfg(test)]
pub(crate) use probes::disarm_merge_rescan_counter;
#[cfg(test)]
pub(crate) use probes::hint_reserve_noted;
#[cfg(test)]
pub(crate) use probes::hint_reserve_reset;
#[cfg(test)]
pub(crate) use probes::keydir_visits;
#[cfg(test)]
pub(crate) use probes::merge_bulk_fallbacks;
#[cfg(test)]
pub(crate) use probes::merge_bulk_len;
#[cfg(test)]
pub(crate) use probes::merge_bulk_rebuilds;
#[cfg(test)]
pub(crate) use probes::merge_chunk_max;
#[cfg(test)]
pub(crate) use probes::merge_probe_mark;
#[cfg(test)]
pub(crate) use probes::merge_probe_read;
#[cfg(test)]
pub(crate) use probes::merge_probe_start;
#[cfg(test)]
pub(crate) use probes::merge_probe_stop;
#[cfg(test)]
pub(crate) use probes::merge_rescan_rows;
#[cfg(test)]
use probes::merge_seg_begin;
#[cfg(test)]
use probes::merge_seg_note_live;
#[cfg(test)]
use probes::merge_t_dump;
#[cfg(test)]
use probes::merge_t_mark;
#[cfg(test)]
use probes::merge_t_start;
#[cfg(test)]
pub(crate) use probes::note_keydir_visit;
#[cfg(test)]
pub(crate) use probes::note_merge_bulk_fallback;
#[cfg(test)]
pub(crate) use probes::note_merge_bulk_rebuild;
#[cfg(test)]
pub(crate) use probes::note_merge_chunk;
#[cfg(test)]
pub(crate) use probes::note_merge_rescan_row;
#[cfg(test)]
pub(crate) use probes::read_value_calls;
#[cfg(test)]
pub(crate) use probes::reset_merge_chunk;
pub use scan::ScanIterator;
