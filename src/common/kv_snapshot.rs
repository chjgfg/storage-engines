//! 迁移快照编解码：四个引擎的 `import_kv` / `export_kv` 共用。
//!
//! 不属于任何引擎核心，只服务于跨引擎搬数据。两种格式：
//!
//! - **JSONL**：每行 `{"key":"<base64>","value":"<base64>"|null[,"seq":N]}`
//! - **二进制**：magic + 重复 `(key_len, key, val_len|tombstone, value?[, seq])`
//!
//! `BPEXP001` 不带元数据，`BPEXP002` 带 seq，`BPEXP003` 带 TTL。
//! memory 启停使用 `BPEXP004`（003 的记录形状 + 完整性尾部）；通用导出仍为 003。
//! [`write_bin`] 按记录里是否有 `seq` 自动选择，所以不带 seq 的导出与
//! 旧版 `BPEXP001` **逐字节一致**；[`read_bin`] 两种都认。

use crate::common::types::ExportRecord;
use std::format;
use std::writeln;
use std::{
    fs::File,
    io::{BufRead, BufReader, BufWriter, Write},
    path::Path,
};

pub const BIN_MAGIC: &[u8; 8] = b"BPEXP001";
pub const BIN_MAGIC_V2: &[u8; 8] = b"BPEXP002";
/// M81 第 6 项：`BPEXP003` = `BPEXP001` 的字段序 + 每条 8 字节 `expire_unix_secs`（**0 = 永久**）。
///
/// 为什么要有第三版而不是往 002 上叠：002 的 8 字节是 LSM 的写入 seq，语义完全不同；而
/// "seq + expire 同时带"今天没有任何调用方（统一工具明确不收 `--with-seq`，seq 只对 lsm 裸层
/// 有意义，带着它跨库互导就是埋漂移）⇒ 与其造一个没人读的四种组合，不如一条版号一件事。
/// ⚠️ 001 的字节形状**一个字都不许变**：`test_bin_format_matches_downstream_bytes` 钉着它。
pub const BIN_MAGIC_V3: &[u8; 8] = b"BPEXP003";
/// memory 快照由 v3 记录和 MAX:u32、count:u64、CRC32:u32 尾部组成。校验覆盖魔数与全部记录，旧导出格式字节保持不变。
pub const BIN_MAGIC_V4: &[u8; 8] = b"BPEXP004";
pub const TOMBSTONE_LEN: u32 = u32::MAX;

/// 一条快照记录。`value: None` 是 tombstone。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvRecord {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    /// LSM 的写入 seq，仅 `--with-seq` 导出时有值；其他引擎一律 `None`
    pub seq: Option<u64>,
    /// 绝对到期时刻（unix 秒）。`None` = 永久。
    ///
    /// 为什么存**绝对值**而不是剩余时长：快照与 `migrate` 同一条口径 —— 导出记的是
    /// "expire − now"要落地时才换算（`migrate` 第一版丢 TTL 被环测抓出来的教训见
    /// `engine.rs` 的 `migrate` 注释）。剩余时长在文件里躺一小时就少一小时。
    pub expire_unix_secs: Option<u64>,
}

impl KvRecord {
    /// 不带 seq，三个非 LSM 引擎用这个
    pub fn new(key: Vec<u8>, value: Option<Vec<u8>>) -> Self {
        Self {
            key,
            value,
            seq: None,
            expire_unix_secs: None,
        }
    }

    /// 带 LSM 写入 seq
    pub fn with_seq(key: Vec<u8>, value: Option<Vec<u8>>, seq: u64) -> Self {
        Self {
            key,
            value,
            seq: Some(seq),
            expire_unix_secs: None,
        }
    }

    /// 带上绝对到期时刻（`BPEXP003` / JSONL 的 `expire` 字段唯一能携带它的途径）
    pub fn with_expire(mut self, expire_unix_secs: u64) -> Self {
        self.expire_unix_secs = Some(expire_unix_secs);
        self
    }
}

/// 读写条数统计
#[derive(Debug, Clone, Default)]
pub struct IoStats {
    pub records: usize,
    pub live: usize,
    pub deleted: usize,
}

impl IoStats {
    fn push(&mut self, value: &Option<Vec<u8>>) {
        self.push_live(value.is_some());
    }
    fn push_live(&mut self, live: bool) {
        self.records += 1;
        if live {
            self.live += 1;
        } else {
            self.deleted += 1;
        }
    }
}

// ─── 写 ─────────────────────────────────────────────────────────────────────

/// bin 导出格式的**逐行**写口（M44）。
///
/// 为什么要有它：`write_bin(path, &[KvRecord])` 要求调用方先把整库装进一个 `Vec`。
/// 库侧的折叠早就是流式的（`fold_rows_streaming`，一行都不攒），但出口这里逼着调用方
/// 再攒一份 —— 600k 档那一次导出实测峰值 **+123.7 MB**，其中一半只是"等着被写出去"。
/// 有了写口，导出可以是"折一行、写一行"，峰值 = 当前这一行 + 写缓冲。
///
/// ⚠️ `with_seq` 必须在 `create` 时就定：魔数是文件的**第一**八个小节，而 v2 多出来的
/// 每条 8 B 序号也是在这里决定的 —— 逐行写的人没法先看完全部记录再回头改头部。
/// 收集壳 `write_bin` 自己扫一遍切片来定，所以它的行为一字没变。
pub struct BinWriter {
    w: BufWriter<SnapshotFile>,
    stats: IoStats,
    with_seq: bool,
    with_expire: bool,
    record_batch: RecordBatch,
}

pub(crate) const RECORD_BATCH_BYTES: usize = 512 * 1024;

/// 停机记录在一个有界缓冲中组装；大记录保留流式输出，空快照不分配记录缓冲。
#[derive(Default)]
struct RecordBatch {
    bytes: Vec<u8>,
    pending: IoStats,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RecordEncoding {
    Buffered,
    DirectLarge,
    #[cfg(test)]
    DirectReference,
}

/// CRC 在缓冲层下按文件真正接收的字节累计；短记录字段共用一次更新，页脚明确绕过此层。
struct SnapshotFile {
    file: File,
    checksum: Option<u32>,
    #[cfg(test)]
    legacy_fields: bool,
    #[cfg(test)]
    record_reference: bool,
    #[cfg(test)]
    measured: bool,
    #[cfg(test)]
    stats: snapshot_probe::Stats,
}

#[cfg(test)]
pub(crate) mod snapshot_probe;

#[cfg(test)]
pub(crate) mod io_reference;

/// 停机对照冻结 M117 的记录字段和逻辑窗口边界。
#[cfg(test)]
pub(crate) mod snapshot_emit_reference;

#[cfg(test)]
pub(crate) mod recovery_probe;

/// jsonl 导出的**逐行**写口，与 [`BinWriter`] 同一条理由。
pub struct JsonlWriter {
    w: BufWriter<File>,
    stats: IoStats,
}

// ─── 流式导出接盘（M44）─────────────────────────────────────────────────────

/// 折出来的行往哪儿写：两种导出格式各一个，接口一样。
pub enum Sink {
    Jsonl(JsonlWriter),
    Bin(BinWriter),
}

/// 把"折一行交一行"的导出接到 [`Sink`] 上的那层胶（M44）。
///
/// 为什么要在共用件里给一份，而不是让四个 CLI 各写各的：导出侧回调的签名是
/// `FnMut(ExportRecord) -> bool`（`false` = 收手），**回调里没有 `Result`**，所以写盘失败
/// 只能"记下来、当场收手、最后再抛"。这三步每个调用方都要做一遍 —— 少做一步就是
/// "文件写坏了而 CLI 报成功"（四家各抄一遍正是这个项目一路在收的东西）。
///
/// 行是**移动**进文件的（`ExportRecord` 的 key/value 直接交给 `KvRecord`），全程没有复制，
/// 也不存在"先把整库摆一份"的那一步。
pub struct ExportWriter {
    sink: Sink,
    err: Option<std::io::Error>,
}

/// 解码值可预留 memory 的一字节信封空间，但不改变长度和内容；TTL 需要的八字节以后另付。
#[derive(Clone, Copy)]
pub(crate) enum ValueBuffer {
    Exact,
    PlainEnvelope,
}

enum RecordRead {
    End,
    Truncated,
    Record(KvRecord),
}

struct RecordInput<R> {
    reader: R,
    remaining: u64,
    values: ValueBuffer,
}

#[cfg(test)]
mod tests;

// 按职责组织的内部实现；公开类型与原模块路径保持一致。
mod atomic_snapshot;
mod base64;
mod binary_reader;
mod binary_writer;
mod export;
mod jsonl;
mod record_encoding;
mod snapshot_io;
pub(crate) use atomic_snapshot::atomic_write_bin_v4;
pub use base64::b64_decode;
pub use base64::b64_encode;
pub use binary_reader::read_bin;
#[cfg(test)]
use binary_reader::read_bin_records;
pub use binary_reader::read_bin_with;
pub(crate) use binary_reader::read_snapshot_with;
pub use export::write_bin;
pub use export::write_bin_v3;
pub use jsonl::read_jsonl;
pub use jsonl::read_jsonl_with;
pub use jsonl::write_jsonl;
use record_encoding::encode_record;
use record_encoding::record_size;
use snapshot_io::snapshot_buffer_size;
