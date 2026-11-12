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

use std::format;
use std::writeln;
use crate::common::types::ExportRecord;
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
/// Memory snapshots: v3 records followed by MAX:u32, count:u64, CRC32:u32.
/// The checksum covers the magic and all records. Legacy export bytes stay unchanged.
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

// ─── Base64 ─────────────────────────────────────────────────────────────────
//
// 自己实现是为了不引第三方依赖（全项目只有 5 个 dep）。

pub fn b64_encode(data: &[u8]) -> String {
    const T: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity((data.len() + 2) / 3 * 4);
    let mut i = 0;
    while i + 3 <= data.len() {
        let n = ((data[i] as u32) << 16) | ((data[i + 1] as u32) << 8) | (data[i + 2] as u32);
        out.push(T[((n >> 18) & 63) as usize] as char);
        out.push(T[((n >> 12) & 63) as usize] as char);
        out.push(T[((n >> 6) & 63) as usize] as char);
        out.push(T[(n & 63) as usize] as char);
        i += 3;
    }
    match data.len() - i {
        1 => {
            let n = (data[i] as u32) << 16;
            out.push(T[((n >> 18) & 63) as usize] as char);
            out.push(T[((n >> 12) & 63) as usize] as char);
            out.push('=');
            out.push('=');
        }
        2 => {
            let n = ((data[i] as u32) << 16) | ((data[i + 1] as u32) << 8);
            out.push(T[((n >> 18) & 63) as usize] as char);
            out.push(T[((n >> 12) & 63) as usize] as char);
            out.push(T[((n >> 6) & 63) as usize] as char);
            out.push('=');
        }
        _ => {}
    }
    out
}

pub fn b64_decode(s: &str) -> Result<Vec<u8>, String> {
    const T: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut table = [255u8; 256];
    for (i, &c) in T.iter().enumerate() {
        table[c as usize] = i as u8;
    }
    let bytes: Vec<u8> = s.bytes().filter(|&b| b != b'=').collect();
    let mut out = Vec::with_capacity(bytes.len() * 3 / 4 + 2);
    let mut i = 0;
    while i + 4 <= bytes.len() {
        let n = ((table[bytes[i] as usize] as u32) << 18)
            | ((table[bytes[i + 1] as usize] as u32) << 12)
            | ((table[bytes[i + 2] as usize] as u32) << 6)
            | (table[bytes[i + 3] as usize] as u32);
        out.push(((n >> 16) & 0xFF) as u8);
        out.push(((n >> 8) & 0xFF) as u8);
        out.push((n & 0xFF) as u8);
        i += 4;
    }
    // 尾组按**剩余字符数**给字节：2 个字符 = 1 字节、3 个字符 = 2 字节。
    // ⚠️ 这里**不能再按 `=` 的个数回头裁剪**（M45 修的就是这个）：`=` 在上面那行 `filter`
    // 里已经被剥掉了，长度信息已经体现在 `rem` 上 —— 再按 pad 减一次就是**二次修正**，
    // 于是"长度不是 3 的倍数"的每一个键/值都少 1~2 字节。现场的形状是：11 字节的键
    // `demo:000001` 解回 10 字节 `demo:00000` ⇒ JSONL 导入把 300 把互不相同的键
    // **截成同一把**，后写覆盖前写，`IMPORT records=301` 而库里只剩 32 把。
    let rem = bytes.len() - i;
    if rem == 2 {
        let n = (table[bytes[i] as usize] as u32) << 18 | ((table[bytes[i + 1] as usize] as u32) << 12);
        out.push(((n >> 16) & 0xFF) as u8);
    } else if rem == 3 {
        let n = (table[bytes[i] as usize] as u32) << 18
            | ((table[bytes[i + 1] as usize] as u32) << 12)
            | ((table[bytes[i + 2] as usize] as u32) << 6);
        out.push(((n >> 16) & 0xFF) as u8);
        out.push(((n >> 8) & 0xFF) as u8);
    } else if rem != 0 {
        return Err("非法 base64 长度".into());
    }
    Ok(out)
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
}

/// CRC follows the bytes accepted by the file, below buffering. Short record
/// fields share one CRC update; footer bytes bypass this layer deliberately.
struct SnapshotFile {
    file: File,
    checksum: Option<u32>,
    #[cfg(test)]
    legacy_fields: bool,
    #[cfg(test)]
    measured: bool,
    #[cfg(test)]
    stats: snapshot_probe::Stats,
}

impl Write for SnapshotFile {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        #[cfg(test)]
        let start = self.measured.then(std::time::Instant::now);
        let n = self.file.write(bytes)?;
        #[cfg(test)]
        if let Some(start) = start {
            self.stats.write_ns += start.elapsed().as_nanos();
            self.stats.write_calls += 1;
        }
        #[cfg(test)]
        if self.legacy_fields { return Ok(n); }
        if let Some(crc) = &mut self.checksum {
            #[cfg(test)]
            let start = self.measured.then(std::time::Instant::now);
            *crc = crate::common::disk::crc32_extend(*crc, &bytes[..n]);
            #[cfg(test)]
            if let Some(start) = start {
                self.stats.crc_ns += start.elapsed().as_nanos();
                self.stats.crc_calls += 1;
            }
        }
        Ok(n)
    }
    fn flush(&mut self) -> std::io::Result<()> { self.file.flush() }
}

#[cfg(test)]
pub(crate) mod snapshot_probe {
    use std::{assert, cell::RefCell, thread_local};
    #[derive(Clone, Debug, Default)]
    pub(crate) struct Stats {
        pub body_ns: u128,
        pub write_ns: u128,
        pub crc_ns: u128,
        pub sync_ns: u128,
        pub publish_ns: u128,
        pub total_ns: u128,
        pub crc_calls: u64,
        pub write_calls: u64,
        pub stop_mode: &'static str,
        pub visited_rows: u64,
        pub consumed_rows: u64,
        /// Disjoint input-pull and fold/emit spans inside body_ns. Fold/emit
        /// still includes write_ns and crc_ns; these nested spans cannot be added.
        pub pull_ns: u128,
        pub fold_emit_ns: u128,
        pub input_batches: u64,
        pub input_buffer_bytes: usize,
    }
    thread_local! { static ACTIVE: RefCell<Option<(bool, Stats)>> = const { RefCell::new(None) }; }
    pub(crate) struct Scope;
    impl Scope {
        pub fn start(legacy_fields: bool) -> Self {
            ACTIVE.with(|s| { assert!(s.borrow().is_none()); *s.borrow_mut() = Some((legacy_fields, Stats::default())); });
            Self
        }
        pub fn stats(&self) -> Stats { ACTIVE.with(|s| s.borrow().as_ref().unwrap().1.clone()) }
    }
    impl Drop for Scope { fn drop(&mut self) { ACTIVE.with(|s| *s.borrow_mut() = None); } }
    pub(super) fn options() -> Option<bool> { ACTIVE.with(|s| s.borrow().as_ref().map(|s| s.0)) }
    pub(crate) fn is_active() -> bool { options().is_some() }
    pub(crate) fn note(f: impl FnOnce(&mut Stats)) {
        ACTIVE.with(|s| { if let Some((_, stats)) = s.borrow_mut().as_mut() { f(stats); } });
    }
}

#[cfg(test)]
pub(crate) mod io_reference {
    use std::{assert, cell::Cell, thread_local};
    thread_local! { static ENABLED: Cell<bool> = const { Cell::new(false) }; }
    pub fn enabled() -> bool { ENABLED.with(|c| c.get()) }
    pub struct Scope;
    impl Scope { pub fn start() -> Self { ENABLED.with(|c| assert!(!c.replace(true))); Self } }
    impl Drop for Scope { fn drop(&mut self) { ENABLED.with(|c| c.set(false)); } }
}

fn snapshot_buffer_size(snapshot: bool) -> usize {
    #[cfg(test)]
    if io_reference::enabled() { return 8 * 1024; }
    if snapshot { 256 * 1024 } else { 8 * 1024 }
}

#[cfg(test)]
pub(crate) mod recovery_probe {
    use std::{assert, cell::RefCell, thread_local, time::Instant};
    #[derive(Debug, Clone, Default)]
    pub struct Stats {
        pub total_ns: u128,
        pub verify_ns: u128,
        pub decode_ns: u128,
        pub replay_ns: u128,
        pub replay_setup_ns: u128,
        pub replay_encode_ns: u128,
        pub replay_apply_ns: u128,
        pub replay_commit_ns: u128,
        pub replay_legacy_ns: u128,
        pub replay_pages: u64,
        pub replay_input_rows: u64,
        pub replay_registry_lookups: u64,
        pub replay_tail_reads: u64,
        pub reads: u64,
    }
    thread_local! { static ACTIVE: RefCell<Option<Stats>> = const { RefCell::new(None) }; }
    pub struct Scope;
    impl Scope {
        pub fn start() -> Self { ACTIVE.with(|s| { assert!(s.borrow().is_none()); *s.borrow_mut() = Some(Stats::default()); }); Self }
        pub fn stats(&self) -> Stats { ACTIVE.with(|s| s.borrow().as_ref().unwrap().clone()) }
    }
    impl Drop for Scope { fn drop(&mut self) { ACTIVE.with(|s| *s.borrow_mut() = None); } }
    pub fn note(f: impl FnOnce(&mut Stats)) {
        ACTIVE.with(|s| if let Some(stats) = s.borrow_mut().as_mut() { f(stats); });
    }
    pub enum Kind { Total, Verify, Records, Replay, ReplaySetup, ReplayEncode, ReplayApply, ReplayCommit, ReplayLegacy }
    pub struct Timer { kind: Kind, start: Instant, replay_before: u128, registry_before: u64, tail_before: u64 }
    impl Timer {
        pub fn start(kind: Kind) -> Option<Self> {
            ACTIVE.with(|s| s.borrow().as_ref().map(|stats| Self {
                kind, start: Instant::now(), replay_before: stats.replay_ns,
                registry_before: crate::common::txn::write_registry_lookups(),
                tail_before: crate::memory::mem_store::key_lookup_probe::tail_reads(),
            }))
        }
    }
    impl Drop for Timer {
        fn drop(&mut self) {
            let ns = self.start.elapsed().as_nanos();
            ACTIVE.with(|s| if let Some(stats) = s.borrow_mut().as_mut() {
                match self.kind {
                    Kind::Total => stats.total_ns += ns,
                    Kind::Verify => stats.verify_ns += ns,
                    Kind::Records => stats.decode_ns += ns.saturating_sub(stats.replay_ns - self.replay_before),
                    Kind::Replay => stats.replay_ns += ns,
                    Kind::ReplaySetup => stats.replay_setup_ns += ns,
                    Kind::ReplayEncode => stats.replay_encode_ns += ns,
                    Kind::ReplayApply => {
                        stats.replay_apply_ns += ns;
                        stats.replay_registry_lookups += crate::common::txn::write_registry_lookups() - self.registry_before;
                        stats.replay_tail_reads += crate::memory::mem_store::key_lookup_probe::tail_reads() - self.tail_before;
                    },
                    Kind::ReplayCommit => stats.replay_commit_ns += ns,
                    Kind::ReplayLegacy => stats.replay_legacy_ns += ns,
                }
            });
        }
    }
    pub struct Input(pub std::fs::File);
    impl std::io::Read for Input {
        fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
            ACTIVE.with(|s| if let Some(stats) = s.borrow_mut().as_mut() { stats.reads += 1; });
            std::io::Read::read(&mut self.0, bytes)
        }
    }
    impl std::io::Seek for Input {
        fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> { std::io::Seek::seek(&mut self.0, pos) }
    }
}

impl BinWriter {
    pub fn create(path: &Path, with_seq: bool) -> std::io::Result<Self> {
        Self::create_shape(path, with_seq, false)
    }

    /// `BPEXP003`：001 的字段序 + 每条 8 字节 `expire_unix_secs`（**0 = 永久**）。M81 第 6 项。
    pub fn create_v3(path: &Path) -> std::io::Result<Self> {
        Self::create_shape(path, false, true)
    }

    fn create_shape(path: &Path, with_seq: bool, with_expire: bool) -> std::io::Result<Self> {
        if with_seq && with_expire {
            // 没有"既带 seq 又带 expire"的版本：那是两条互不相干的元数据叠成一个组合爆炸，
            // 而今天没有任何调用方需要它（统一工具不收 --with-seq，理由见 BIN_MAGIC_V3 注释）。
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "BPEXP 没有同时带 seq 与 expire 的版本（002=seq，003=expire）",
            ));
        }
        let f = File::create(path)?;
        let magic: &[u8; 8] = if with_seq {
            BIN_MAGIC_V2
        } else if with_expire {
            BIN_MAGIC_V3
        } else {
            BIN_MAGIC
        };
        Self::from_file(f, magic, with_seq, with_expire, false)
    }

    fn from_file(f: File, magic: &[u8; 8], with_seq: bool, with_expire: bool,
        checked: bool) -> std::io::Result<Self> {
        #[cfg(test)]
        let options = snapshot_probe::options();
        let sink = SnapshotFile {
            file: f,
            checksum: checked.then_some(0),
            #[cfg(test)]
            legacy_fields: options == Some(true),
            #[cfg(test)]
            measured: options.is_some(),
            #[cfg(test)]
            stats: snapshot_probe::Stats::default(),
        };
        let mut w = BufWriter::with_capacity(snapshot_buffer_size(checked), sink);
        w.write_all(magic)?;
        #[cfg(test)]
        if w.get_ref().legacy_fields && checked {
            w.get_mut().checksum = Some(crate::common::disk::crc32(magic));
            w.get_mut().stats.crc_calls += 1;
        }
        Ok(Self {
            w,
            stats: IoStats::default(),
            with_seq,
            with_expire,
        })
    }

    fn bytes(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.w.write_all(bytes)?;
        #[cfg(test)]
        if self.w.get_ref().legacy_fields {
            let sink = self.w.get_mut();
            if let Some(crc) = &mut sink.checksum {
                *crc = crate::common::disk::crc32_extend(*crc, bytes);
                sink.stats.crc_calls += 1;
            }
        }
        Ok(())
    }

    pub fn write(&mut self, rec: &KvRecord) -> std::io::Result<()> {
        self.write_borrowed(&rec.key, rec.value.as_deref(), rec.seq, rec.expire_unix_secs)
    }

    pub(crate) fn write_borrowed(&mut self, key: &[u8], value: Option<&[u8]>, seq: Option<u64>, expire: Option<u64>) -> std::io::Result<()> {
        if key.len() >= u32::MAX as usize || value.is_some_and(|v| v.len() >= u32::MAX as usize) {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "snapshot record too large"));
        }
        self.bytes(&(key.len() as u32).to_le_bytes())?;
        self.bytes(key)?;
        match value {
            Some(v) => {
                self.bytes(&(v.len() as u32).to_le_bytes())?;
                self.bytes(v)?;
            }
            None => self.bytes(&TOMBSTONE_LEN.to_le_bytes())?,
        }
        if self.with_seq {
            self.bytes(&seq.unwrap_or(0).to_le_bytes())?;
        }
        if self.with_expire {
            self.bytes(&expire.unwrap_or(0).to_le_bytes())?;
        }
        self.stats.push_live(value.is_some());
        Ok(())
    }

    pub fn finish(mut self) -> std::io::Result<IoStats> {
        // Flush the entire body before reading its checksum. The footer itself
        // is not covered by CRC, exactly as in BPEXP004 before this optimization.
        self.w.flush()?;
        let sink = self.w.get_mut();
        if let Some(crc) = sink.checksum {
            sink.file.write_all(&u32::MAX.to_le_bytes())?;
            sink.file.write_all(&(self.stats.records as u64).to_le_bytes())?;
            sink.file.write_all(&crc.to_le_bytes())?;
            #[cfg(test)]
            let start = sink.measured.then(std::time::Instant::now);
            sink.file.sync_all()?;
            #[cfg(test)]
            if let Some(start) = start { sink.stats.sync_ns += start.elapsed().as_nanos(); }
        }
        #[cfg(test)]
        snapshot_probe::note(|s| *s = sink.stats.clone());
        Ok(self.stats)
    }
}

/// Publish a complete memory snapshot on the same filesystem. Any error before
/// rename leaves the previous snapshot intact. Unix also syncs the parent directory.
pub(crate) fn atomic_write_bin_v4(path: &Path,
    write: impl FnOnce(&mut BinWriter) -> std::io::Result<()>) -> std::io::Result<IoStats> {
    #[cfg(test)]
    let measured = snapshot_probe::options().is_some();
    #[cfg(test)]
    let total = measured.then(std::time::Instant::now);
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    struct Staging(std::path::PathBuf);
    impl Drop for Staging {
        fn drop(&mut self) { let _ = std::fs::remove_file(&self.0); }
    }
    let (staging, file) = loop {
        let mut name = path.as_os_str().to_os_string();
        name.push(format!(".{}.{}.tmp", std::process::id(), SEQ.fetch_add(1, Ordering::Relaxed)));
        let staging = std::path::PathBuf::from(name);
        match File::options().write(true).create_new(true).open(&staging) {
            Ok(file) => break (Staging(staging), file),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e),
        }
    };
    let mut writer = BinWriter::from_file(file, BIN_MAGIC_V4, false, true, true)?;
    #[cfg(test)]
    let body = measured.then(std::time::Instant::now);
    write(&mut writer)?;
    #[cfg(test)]
    let body_ns = body.map(|s| s.elapsed().as_nanos()).unwrap_or(0);
    let stats = writer.finish()?;
    #[cfg(test)]
    let publish = measured.then(std::time::Instant::now);
    // Never remove the destination first: rename performs replacement.
    std::fs::rename(&staging.0, path)?;
    #[cfg(unix)]
    File::open(path.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new(".")))?.sync_all()?;
    #[cfg(test)]
    if let (Some(total), Some(publish)) = (total, publish) {
        snapshot_probe::note(|s| {
            s.body_ns = body_ns;
            s.publish_ns = publish.elapsed().as_nanos();
            s.total_ns = total.elapsed().as_nanos();
        });
    }
    Ok(stats)
}

/// jsonl 导出的**逐行**写口，与 [`BinWriter`] 同一条理由。
pub struct JsonlWriter {
    w: BufWriter<File>,
    stats: IoStats,
}

impl JsonlWriter {
    pub fn create(path: &Path) -> std::io::Result<Self> {
        let f = File::create(path)?;
        Ok(Self {
            w: BufWriter::new(f),
            stats: IoStats::default(),
        })
    }

    pub fn write(&mut self, rec: &KvRecord) -> std::io::Result<()> {
        let k = b64_encode(&rec.key);
        // 可选元数据按"存在才加"拼在尾部：缺字段 = 读侧当 `None` ⇒ 旧版工具读新版行只会**少看**
        // 一个字段，不会读错；而两个字段都不在时，这一行的字节与改动前一字不差。
        let mut extra = String::new();
        if let Some(seq) = rec.seq {
            extra.push_str(&format!(",\"seq\":{seq}"));
        }
        if let Some(e) = rec.expire_unix_secs {
            extra.push_str(&format!(",\"expire\":{e}"));
        }
        match &rec.value {
            Some(v) => {
                let v = b64_encode(v);
                writeln!(self.w, "{{\"key\":\"{k}\",\"value\":\"{v}\"{extra}}}")?;
            }
            None => {
                writeln!(self.w, "{{\"key\":\"{k}\",\"value\":null{extra}}}")?;
            }
        }
        self.stats.push(&rec.value);
        Ok(())
    }

    pub fn finish(mut self) -> std::io::Result<IoStats> {
        self.w.flush()?;
        Ok(self.stats)
    }
}

/// 收集壳：行形状与编码只有一份（[`BinWriter::write`]）。
pub fn write_bin(path: &Path, records: &[KvRecord]) -> std::io::Result<IoStats> {
    let with_seq = records.iter().any(|r| r.seq.is_some());
    let mut w = BinWriter::create(path, with_seq)?;
    for rec in records {
        w.write(rec)?;
    }
    w.finish()
}

/// `BPEXP003` 的收集壳（M81 第 6 项）：与 [`write_bin`] 同一份行编码，只是版号定死成带 expire。
/// 走 003 的理由见 [`KvRecord::expire_unix_secs`] —— 001/002 里没有地方放 TTL，
/// 而"今天走文件同步必丢 TTL"正是这一项要堵的那件事。
pub fn write_bin_v3(path: &Path, records: &[KvRecord]) -> std::io::Result<IoStats> {
    let mut w = BinWriter::create_v3(path)?;
    for rec in records {
        w.write(rec)?;
    }
    w.finish()
}

/// 收集壳：行形状与编码只有一份（[`JsonlWriter::write`]）—— 改这里就等于改出第二种格式。
pub fn write_jsonl(path: &Path, records: &[KvRecord]) -> std::io::Result<IoStats> {
    let mut w = JsonlWriter::create(path)?;
    for rec in records {
        w.write(rec)?;
    }
    w.finish()
}

// ─── 流式导出接盘（M44）─────────────────────────────────────────────────────

/// 折出来的行往哪儿写：两种导出格式各一个，接口一样。
pub enum Sink {
    Jsonl(JsonlWriter),
    Bin(BinWriter),
}

impl Sink {
    pub fn jsonl(path: &Path) -> std::io::Result<Self> {
        Ok(Self::Jsonl(JsonlWriter::create(path)?))
    }

    pub fn bin(path: &Path, with_seq: bool) -> std::io::Result<Self> {
        Ok(Self::Bin(BinWriter::create(path, with_seq)?))
    }

    /// `BPEXP003`（带 expire）的流式导出写口：整库不另攒一份 `Vec`
    /// （M44 那条"导出不必再把整库摆进内存"的教训，对快照同样是账）。
    pub fn bin_v3(path: &Path) -> std::io::Result<Self> {
        Ok(Self::Bin(BinWriter::create_v3(path)?))
    }

    pub fn write(&mut self, rec: &KvRecord) -> std::io::Result<()> {
        match self {
            Self::Jsonl(w) => w.write(rec),
            Self::Bin(w) => w.write(rec),
        }
    }

    /// 收尾：落盘 + 返回条数账（两个写口都有 `finish`，这里只是把它们并到一条门上）。
    pub fn finish(self) -> std::io::Result<IoStats> {
        match self {
            Self::Jsonl(w) => w.finish(),
            Self::Bin(w) => w.finish(),
        }
    }
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

impl ExportWriter {
    pub fn jsonl(path: &Path) -> std::io::Result<Self> {
        Ok(Self {
            sink: Sink::jsonl(path)?,
            err: None,
        })
    }

    pub fn bin(path: &Path, with_seq: bool) -> std::io::Result<Self> {
        Ok(Self {
            sink: Sink::bin(path, with_seq)?,
            err: None,
        })
    }

    /// 写**第三版** bin（`BPEXP003` = 001 的字段序 + 每条 8 字节绝对到期时刻）。
    /// 快照文件是跨库载体 ⇒ `tools::export_to_file` / 统一 CLI 走这条：不带 TTL 的文件
    /// 等于"搬一次少一批到期时刻"。读侧三个版本都认（只认魔数），所以老文件继续能导。
    pub fn bin_v3(path: &Path) -> std::io::Result<Self> {
        Ok(Self {
            sink: Sink::bin_v3(path)?,
            err: None,
        })
    }

    /// 交给导出回调的那一步：`true` = 接着折。已经错过就一律 `false`（一行都不再写）。
    ///
    /// 门面导出（`ExportRecord`：只有键与已剥壳的值）走这条；裸 LSM 引擎那条给的是
    /// `(键, 值, seq)` 三元组，用 [`Self::push_record`] —— 两者都只是行形状不同，
    /// 编码与"错过就收手"这套胶只有一份。
    pub fn push(&mut self, rec: ExportRecord) -> bool {
        let mut kv = KvRecord::new(rec.key, rec.value);
        // ⚠️ TTL 必须在这一行过桥：以前这里把 `ExportRecord` 折成 `KvRecord` 时只搬键与值，
        // 于是"文件里没地方放 expire"这件事被写成"这条门天生不带 TTL"。003 就位之后，
        // 过桥时漏填才是唯一会丢的东西。
        if let Some(e) = rec.expire_unix_secs {
            kv = kv.with_expire(e);
        }
        self.push_record(kv)
    }

    /// [`Self::push`] 的通用版：行已经是 `KvRecord`（可以带 seq）时用这条。
    pub fn push_record(&mut self, rec: KvRecord) -> bool {
        if self.err.is_some() {
            return false;
        }
        match self.sink.write(&rec) {
            Ok(()) => true,
            Err(e) => {
                self.err = Some(e);
                false
            }
        }
    }

    /// 收口：中途写过错就抛那个错（并把缓冲冲下去）
    pub fn finish(self) -> std::io::Result<IoStats> {
        let stats = match self.sink {
            Sink::Jsonl(w) => w.finish()?,
            Sink::Bin(w) => w.finish()?,
        };
        match self.err {
            Some(e) => Err(e),
            None => Ok(stats),
        }
    }
}

// ─── 读 ─────────────────────────────────────────────────────────────────────

/// **流式**读二进制快照：解出一条就交给 `on`，`on` 返回 `false` 当场停止解码。
/// 004 在回调前用固定缓冲预验整份文件 CRC；001–003 不需要这次预读。
///
/// 存在的理由与 [`crate::KvEngine::export_latest_visible_for_each`] 同一件事的两端：
/// `import_kv` 那侧要的是"边读边写进引擎"，把整份文件先摆进 `Vec` 只是为等着被逐条丢掉
/// （600k 档实测那笔 `Vec` 是 123 MB 的瞬时）。[`read_bin`] 从这一条改成收集壳，
/// **口径一字不变**：同一种截断处理（尾部不完整就停，不报错）、同一套魔数判定。
pub fn read_bin_with(
    path: &Path,
    on: &mut dyn FnMut(KvRecord) -> bool,
) -> std::io::Result<IoStats> {
    read_bin_policy(path, on, false, ValueBuffer::Exact)
}

/// Raw decoded values may reserve the one-byte memory envelope without
/// changing their length or byte contents. TTL pays its extra eight bytes later.
#[derive(Clone, Copy)]
pub(crate) enum ValueBuffer { Exact, PlainEnvelope }

impl ValueBuffer {
    fn spare(self) -> usize { match self { Self::Exact => 0, Self::PlainEnvelope => 1 } }
}

pub(crate) fn read_snapshot_with(path: &Path, values: ValueBuffer,
    on: &mut dyn FnMut(KvRecord) -> bool) -> std::io::Result<IoStats> {
    read_bin_policy(path, on, true, values)
}

fn read_bin_policy(path: &Path, on: &mut dyn FnMut(KvRecord) -> bool,
    strict: bool, values: ValueBuffer) -> std::io::Result<IoStats> {
    use std::io::{Read, Seek, SeekFrom};
    let file = File::open(path)?;
    let length = file.metadata()?.len();
    #[cfg(test)]
    let file = recovery_probe::Input(file);
    let mut reader = BufReader::with_capacity(snapshot_buffer_size(strict), file);
    if length < 8 { return Err(snapshot_invalid("文件过短")); }
    let mut magic = [0u8; 8];
    reader.read_exact(&mut magic)?;
    let (with_seq, with_expire) = match &magic {
        BIN_MAGIC => (false, false),
        BIN_MAGIC_V2 => (true, false),
        BIN_MAGIC_V3 | BIN_MAGIC_V4 => (false, true),
        _ => return Err(snapshot_invalid("非法导出文件魔数")),
    };
    // Checked snapshots are verified before the first callback. This bounded
    // prepass prevents an importer from committing a corrupt file's prefix.
    let checked = &magic == BIN_MAGIC_V4;
    let (body_len, expected) = if checked {
        #[cfg(test)]
        let _timer = recovery_probe::Timer::start(recovery_probe::Kind::Verify);
        if length < 24 { return Err(snapshot_invalid("missing snapshot footer")); }
        reader.seek(SeekFrom::Start(0))?;
        let mut remaining = length - 16;
        let mut buffer = [0u8; 64 * 1024];
        let mut crc = 0;
        while remaining > 0 {
            let n = remaining.min(buffer.len() as u64) as usize;
            reader.read_exact(&mut buffer[..n])?;
            crc = crate::common::disk::crc32_extend(crc, &buffer[..n]);
            remaining -= n as u64;
        }
        let mut footer = [0u8; 16];
        reader.read_exact(&mut footer)?;
        if footer[..4] != u32::MAX.to_le_bytes() ||
            crc != u32::from_le_bytes(footer[12..].try_into().unwrap()) {
            return Err(snapshot_invalid("snapshot checksum/footer mismatch"));
        }
        reader.seek(SeekFrom::Start(8))?;
        (length - 24, Some(u64::from_le_bytes(footer[4..12].try_into().unwrap())))
    } else { (length - 8, None) };
    #[cfg(test)]
    let _timer = recovery_probe::Timer::start(recovery_probe::Kind::Records);
    read_bin_records(&mut reader, body_len, with_seq, with_expire, strict || checked, expected, values, on)
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

impl<R: std::io::Read> RecordInput<R> {
    fn fixed<const N: usize>(&mut self) -> std::io::Result<Option<[u8; N]>> {
        if self.remaining < N as u64 { return Ok(None); }
        let mut bytes = [0; N];
        self.reader.read_exact(&mut bytes)?;
        self.remaining -= N as u64;
        Ok(Some(bytes))
    }

    fn bytes(&mut self, len: u32, spare: usize) -> std::io::Result<Option<Vec<u8>>> {
        if len as u64 > self.remaining { return Ok(None); }
        let mut bytes = Vec::new();
        // An empty payload allocates once, when its actual envelope is known.
        let spare = if len == 0 { 0 } else { spare };
        let capacity = (len as usize).checked_add(spare)
            .ok_or_else(|| snapshot_invalid("snapshot value capacity overflow"))?;
        bytes.try_reserve_exact(capacity).map_err(std::io::Error::other)?;
        bytes.resize(len as usize, 0);
        self.reader.read_exact(&mut bytes)?;
        self.remaining -= len as u64;
        Ok(Some(bytes))
    }

    fn record(&mut self, with_seq: bool, with_expire: bool) -> std::io::Result<RecordRead> {
        if self.remaining == 0 { return Ok(RecordRead::End); }
        let Some(len) = self.fixed::<4>()? else { return Ok(RecordRead::Truncated); };
        let Some(key) = self.bytes(u32::from_le_bytes(len), 0)? else { return Ok(RecordRead::Truncated); };
        let Some(len) = self.fixed::<4>()? else { return Ok(RecordRead::Truncated); };
        let value = match u32::from_le_bytes(len) {
            TOMBSTONE_LEN => None,
            len => {
                let Some(value) = self.bytes(len, self.values.spare())? else { return Ok(RecordRead::Truncated); };
                Some(value)
            }
        };
        let seq = if with_seq {
            let Some(bytes) = self.fixed::<8>()? else { return Ok(RecordRead::Truncated); };
            Some(u64::from_le_bytes(bytes))
        } else { None };
        let expire_unix_secs = if with_expire {
            let Some(bytes) = self.fixed::<8>()? else { return Ok(RecordRead::Truncated); };
            let expire = u64::from_le_bytes(bytes);
            (expire != 0).then_some(expire)
        } else { None };
        Ok(RecordRead::Record(KvRecord { key, value, seq, expire_unix_secs }))
    }
}

fn read_bin_records(reader: &mut impl std::io::Read, length: u64, with_seq: bool,
    with_expire: bool, strict: bool, expected: Option<u64>, values: ValueBuffer,
    on: &mut dyn FnMut(KvRecord) -> bool) -> std::io::Result<IoStats> {
    let mut input = RecordInput { reader, remaining: length, values };
    let mut stats = IoStats::default();
    loop {
        match input.record(with_seq, with_expire)? {
            RecordRead::End => break,
            RecordRead::Truncated if strict => return Err(snapshot_invalid("incomplete snapshot record")),
            RecordRead::Truncated => break,
            RecordRead::Record(record) => {
                stats.push(&record.value);
                if !on(record) { return Ok(stats); }
            }
        }
    }
    if expected.is_some_and(|n| n != stats.records as u64) {
        return Err(snapshot_invalid("snapshot record count mismatch"));
    }
    Ok(stats)
}

fn snapshot_invalid(message: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message)
}

/// **流式**读 JSONL：与 [`read_jsonl`] 同一份行解析（`parse_jsonl_line` + `b64_decode`），
/// 只是不再攒 `Vec`。空行跳过、字段缺失/非法 base64 ⇒ `InvalidData`，两条路一致。
pub fn read_jsonl_with(
    path: &Path,
    on: &mut dyn FnMut(KvRecord) -> bool,
) -> std::io::Result<IoStats> {
    let f = File::open(path)?;
    let reader = BufReader::new(f);
    let mut stats = IoStats::default();
    for line in reader.lines() {
        let line = line?;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let rec = parse_jsonl_record(line)?;
        stats.push(&rec.value);
        if !on(rec) {
            break;
        }
    }
    Ok(stats)
}

/// 行里可选的无符号整数字段（今天只有 `"expire":N`）。找不到 = `None`；
/// 有字段但后面不是数字 ⇒ 报错。为什么不敢静默丢掉：`JSONL` 那条路历史上已经因为"静默少读"
/// 丢过一次数据（`b64_decode` 截断那次，301 行进库只剩 32 把）—— 少读一个 TTL 是同一类事故：
/// 键会活得比它该有的样子久。
///
/// ⚠️ 字段名带冒号一起匹配，而 key/value 都是 base64（字母表里没有 `"` 和 `:`）⇒ 不会在
/// 值内部误命中。`seq` 今天仍然**不**从 JSONL 读回（改动前就是这样，本条不顺手扩大范围）。
fn jsonl_u64_field(line: &str, tag: &str) -> std::io::Result<Option<u64>> {
    let Some(i) = line.find(tag) else {
        return Ok(None);
    };
    let rest = &line[i + tag.len()..];
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    // 数字后面必须正好收尾在 JSON 的分隔上。少了这一步，`"expire":12x}` 会被读成 12 ——
    // 那就是"静默少读"换个地方重演（本文件那条 base64 截断事故的同一类）。
    let terminated = match rest[digits.len()..].chars().next() {
        None => true,
        Some(c) => matches!(c, ',' | '}' | ' ' | '\t' | '\r' | '\n'),
    };
    if !terminated {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("{tag} 的值不是干净的整数: {line}"),
        ));
    }
    let n: u64 = digits.parse().map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("{tag} {e}: {line}"),
        )
    })?;
    Ok((n != 0).then_some(n))
}

/// 一行 JSONL → 一条记录（解码失败带 `InvalidData`，两条读路共用同一个错形）
fn parse_jsonl_record(line: &str) -> std::io::Result<KvRecord> {
    let (key_b64, val_part) = parse_jsonl_line(line)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let key = b64_decode(&key_b64)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let value = match val_part {
        None => None,
        Some(vb64) => Some(
            b64_decode(&vb64)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?,
        ),
    };
    Ok(KvRecord {
        key,
        value,
        seq: None,
        expire_unix_secs: jsonl_u64_field(line, "\"expire\":")?,
    })
}

pub fn read_bin(path: &Path) -> std::io::Result<(Vec<KvRecord>, IoStats)> {
    let mut out = Vec::new();
    let stats = read_bin_with(path, &mut |rec| {
        out.push(rec);
        true
    })?;
    Ok((out, stats))
}

pub fn read_jsonl(path: &Path) -> std::io::Result<(Vec<KvRecord>, IoStats)> {
    let mut out = Vec::new();
    let stats = read_jsonl_with(path, &mut |rec| {
        out.push(rec);
        true
    })?;
    Ok((out, stats))
}

fn parse_jsonl_line(line: &str) -> Result<(String, Option<String>), String> {
    let key_tag = "\"key\":\"";
    let val_tag = "\"value\":";
    let ki = line
        .find(key_tag)
        .ok_or_else(|| format!("缺 key 字段: {line}"))?;
    let ks = ki + key_tag.len();
    let ke = line[ks..]
        .find('"')
        .ok_or_else(|| "key 未闭合".to_string())?
        + ks;
    let key = line[ks..ke].to_string();
    let vi = line
        .find(val_tag)
        .ok_or_else(|| format!("缺 value 字段: {line}"))?;
    let rest = line[vi + val_tag.len()..].trim_start();
    if rest.starts_with("null") {
        return Ok((key, None));
    }
    if !rest.starts_with('"') {
        return Err(format!("value 格式错误: {line}"));
    }
    let vs = 1;
    let ve = rest[vs..]
        .find('"')
        .ok_or_else(|| "value 未闭合".to_string())?
        + vs;
    Ok((key, Some(rest[vs..ve].to_string())))
}

#[cfg(test)]
mod tests {
    #[test]
    fn snapshot_io_buffer_bounds_real_writes() {
        use super::*;
        let path = crate::common::ScratchPath::new("snapshot", "io_buffer", ".bxp");
        let mut reference_bytes = None;
        for reference in [true, false] {
            let _mode = reference.then(io_reference::Scope::start);
            let scope = snapshot_probe::Scope::start(false);
            atomic_write_bin_v4(&path, |w| {
                for i in 0u64..20_000 {
                    w.write(&KvRecord::new(i.to_be_bytes().to_vec(), Some(vec![7; 64])))?;
                }
                Ok(())
            }).unwrap();
            let writes = scope.stats().write_calls;
            if reference { assert!(writes > 100); } else { assert!(writes <= 8); }
            let bytes = std::fs::read(&path).unwrap();
            if reference { reference_bytes = Some(bytes); }
            else { assert_eq!(Some(bytes), reference_bytes); }
            let recovery = recovery_probe::Scope::start();
            let mut seen = 0u64;
            read_snapshot_with(&path, ValueBuffer::Exact, &mut |row| {
                assert_eq!(row.key, seen.to_be_bytes());
                assert_eq!(row.value, Some(vec![7; 64]));
                seen += 1; true
            }).unwrap();
            assert_eq!(seen, 20_000);
            let reads = recovery.stats().reads;
            if reference { assert!(reads > 100); } else { assert!(reads < 30); }
        }
    }

    #[test]
    fn snapshot_buffered_crc_matches_reference_and_batches_calls() {
        use super::*;
        let path = crate::common::ScratchPath::new("snapshot", "crc_blocks", ".bxp");
        let mut reference = None;
        for legacy in [true, false] {
            let scope = snapshot_probe::Scope::start(legacy);
            atomic_write_bin_v4(&path, |w| {
                for i in 0u64..2000 {
                    let len = if i == 123 { 100_003 } else { 61 };
                    w.write(&KvRecord::new(i.to_be_bytes().to_vec(), Some(vec![i as u8; len])))?;
                }
                w.write(&KvRecord::new(b"tombstone".to_vec(), None))?;
                Ok(())
            }).unwrap();
            let stats = scope.stats();
            drop(scope);
            let bytes = std::fs::read(&path).unwrap();
            let footer = bytes.len() - 16;
            assert_eq!(u32::from_le_bytes(bytes[footer+12..].try_into().unwrap()),
                crate::common::disk::crc32(&bytes[..footer]));
            if legacy {
                assert!(stats.crc_calls > 10_000, "old per-field shape must exceed the new bound");
                reference = Some(bytes);
            } else {
                assert_eq!(Some(bytes), reference, "BPEXP004 bytes must not change");
                assert!(stats.crc_calls < 100, "CRC was not moved below buffering");
                assert!(stats.sync_ns > 0, "checked snapshot must still sync");
            }
            let mut seen = 0u64;
            read_snapshot_with(&path, ValueBuffer::Exact, &mut |r| {
                if seen < 2000 {
                    assert_eq!(r.key, seen.to_be_bytes());
                    assert_eq!(r.value, Some(vec![seen as u8; if seen == 123 { 100_003 } else { 61 }]));
                } else { assert_eq!(r.value, None); }
                seen += 1;
                true
            }).unwrap();
            assert_eq!(seen, 2001);
        }
    }

    #[test]
    #[ignore = "allocator peak probe; run alone with --test-threads=1"]
    fn snapshot_streaming_memory_probe() {
        use super::*;
        let path = crate::common::ScratchPath::new("snapshot", "stream_peak", ".bxp");
        let rows = 200_000;
        atomic_write_bin_v4(&path, |w| {
            for i in 0u64..rows {
                w.write(&KvRecord::new(i.to_be_bytes().to_vec(), Some(vec![0; 64])))?;
            }
            Ok(())
        }).unwrap();
        let file_bytes = std::fs::metadata(&path).unwrap().len();
        let measure = |whole_file: bool| {
            let baseline = crate::mem_probe::reset_peak();
            let control = whole_file.then(|| std::fs::read(&path).unwrap());
            let mut seen = 0u64;
            read_snapshot_with(&path, ValueBuffer::Exact, &mut |r| {
                assert_eq!(r.key, seen.to_be_bytes());
                assert_eq!(r.value.as_deref(), Some([0u8; 64].as_slice()));
                seen += 1;
                true
            }).unwrap();
            assert_eq!(seen, rows);
            std::hint::black_box(&control);
            crate::mem_probe::peak() - baseline
        };
        let control = measure(true);
        let streaming = measure(false);
        eprintln!("SNAPSHOT_READ rows={rows} file_bytes={file_bytes} whole_file_peak={control} streaming_peak={streaming}");
        assert!(control >= file_bytes, "control did not hold the whole file");
        assert!(streaming < 256 * 1024, "reader retained more than a buffer and current record");
    }

    #[test]
    fn snapshot_streaming_stops_before_the_next_record() {
        use super::*;
        struct FirstOnly(std::io::Cursor<Vec<u8>>);
        impl std::io::Read for FirstOnly {
            fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
                if self.0.position() == self.0.get_ref().len() as u64 {
                    return Err(std::io::Error::other("read past first record"));
                }
                std::io::Read::read(&mut self.0, out)
            }
        }
        let mut body = Vec::new();
        body.extend_from_slice(&1u32.to_le_bytes()); body.push(b'k');
        body.extend_from_slice(&1u32.to_le_bytes()); body.push(b'v');
        let length = body.len() as u64 + 1000;
        let mut reader = FirstOnly(std::io::Cursor::new(body.clone()));
        assert_eq!(read_bin_records(&mut reader, length, false, false, false, None, ValueBuffer::Exact, &mut |_| false).unwrap().records, 1);
        let mut reader = FirstOnly(std::io::Cursor::new(body));
        assert!(read_bin_records(&mut reader, length, false, false, false, None, ValueBuffer::Exact, &mut |_| true).is_err());
    }

    #[test]
    fn atomic_snapshot_preserves_previous_on_write_or_publish_failure() {
        use super::*;
        let dir = crate::common::ScratchPath::new("snapshot", "atomic", "");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("snapshot.bxp");
        let old = KvRecord::new(b"old".to_vec(), Some(b"good".to_vec()));
        atomic_write_bin_v4(&path, |w| w.write(&old)).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        assert!(atomic_write_bin_v4(&path, |w| {
            w.write(&KvRecord::new(b"partial".to_vec(), Some(vec![7; 20000])))?;
            Err(std::io::Error::other("injected write failure"))
        }).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        assert_eq!(read_bin(&path).unwrap().0, vec![old]);
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 1, "staging leaked");
        let blocked = dir.join("blocked");
        std::fs::create_dir(&blocked).unwrap();
        std::fs::write(blocked.join("keep"), b"keep").unwrap();
        assert!(atomic_write_bin_v4(&blocked, |_| Ok(())).is_err());
        assert_eq!(std::fs::read(blocked.join("keep")).unwrap(), b"keep");
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 2, "staging leaked");
    }

    #[test]
    fn strict_snapshot_keeps_legacy_formats_and_import_tolerance() {
        use super::*;
        let path = crate::common::ScratchPath::new("snapshot", "legacy", ".bxp");
        for version in 1..=3 {
            let mut record = KvRecord::new(b"k".to_vec(), Some(vec![0, 1, 2]));
            if version == 2 { record.seq = Some(42); }
            if version == 3 { record.expire_unix_secs = Some(123); }
            let mut writer = if version == 3 { BinWriter::create_v3(&path) }
                else { BinWriter::create(&path, version == 2) }.unwrap();
            writer.write(&record).unwrap();
            writer.finish().unwrap();
            let mut back = Vec::new();
            read_snapshot_with(&path, ValueBuffer::Exact, &mut |r| { back.push(r); true }).unwrap();
            assert_eq!(back, vec![record]);
            let mut bytes = std::fs::read(&path).unwrap();
            bytes.pop();
            std::fs::write(&path, bytes).unwrap();
            assert!(read_snapshot_with(&path, ValueBuffer::Exact, &mut |_| true).is_err());
            assert_eq!(read_bin(&path).unwrap().1.records, 0, "legacy import must tolerate partial tail");
        }
    }

    #[test]
    fn snapshot_crc_chunks_match_whole_file() {
        let bytes: Vec<u8> = (0..=255).cycle().take(1027).collect();
        for chunk in [1, 7, 8, 9, 64, 1024] {
            let mut crc = 0;
            for part in bytes.chunks(chunk) { crc = crate::common::disk::crc32_extend(crc, part); }
            assert_eq!(crc, crate::common::disk::crc32(&bytes));
        }
    }
    use super::*;
    use std::assert;
    use std::assert_eq;
    use std::panic;
    use std::vec;

    #[test]
    fn test_b64_known() {
        assert_eq!(b64_encode(b""), "");
        assert_eq!(b64_encode(b"f"), "Zg==");
        assert_eq!(b64_encode(b"fo"), "Zm8=");
        assert_eq!(b64_encode(b"foo"), "Zm9v");
        assert_eq!(b64_decode("Zm9v").unwrap(), b"foo");
    }

    /// M81 第 6 项：三版 bin 都要读得回来，而 expire 只有 003 装得下。四条判据各挡一种"看着像过了"：
    /// ① 001 加了字段之后没被顺手改形（读回一律 `None`，不是猜出个 0）；
    /// ② 003 往返保住**绝对**到期时刻，且永久行读回 `None` 而不是 `Some(0)`；
    /// ③ 读侧只认魔数、不照内容猜形状 ⇒ 头改坏必须报错；
    /// ④ JSONL 的 `expire` 是**可选**字段：旧行缺它 = 永久（不报错），有它但值坏了 = 必须报错
    ///    （静默丢掉一个 TTL，就是让那把键活得比它该有的样子久）。
    #[test]
    fn bin_v3_carries_expire_and_all_three_versions_read_back() {
        let dir = crate::common::ScratchPath::new("kv_snapshot", "v3", "");
        std::fs::create_dir_all(&dir).unwrap();
        let recs = vec![
            KvRecord::new(b"k1".to_vec(), Some(b"v1".to_vec())),
            KvRecord::new(b"k2".to_vec(), None),
            KvRecord::new(b"k3".to_vec(), Some(b"v3".to_vec())).with_expire(1_900_000_000),
        ];
        let p1 = dir.join("a.bxp");
        let p2 = dir.join("b.bxp");
        let p3 = dir.join("c.bxp");
        write_bin(&p1, &recs).unwrap();
        write_bin(&p2, &[KvRecord::with_seq(b"s".to_vec(), Some(b"1".to_vec()), 7)]).unwrap();
        write_bin_v3(&p3, &recs).unwrap();

        let (back1, st1) = read_bin(&p1).unwrap();
        assert_eq!(st1.records, 3);
        assert!(
            back1.iter().all(|r| r.expire_unix_secs.is_none()),
            "001 里没有放 expire 的地方，读回来必须全是 None"
        );
        let (back2, _) = read_bin(&p2).unwrap();
        assert_eq!(back2[0].seq, Some(7), "002 的 seq 不能被读丢");
        let (back3, st3) = read_bin(&p3).unwrap();
        assert_eq!(st3.records, 3);
        assert_eq!(back3[2].expire_unix_secs, Some(1_900_000_000), "003 的绝对到期时刻要原样回来");
        assert_eq!(back3[0].expire_unix_secs, None, "永久行读回 None，不是 Some(0)");
        assert_eq!(back3[1].value, None, "墓碑在 003 里还是墓碑");

        let mut bad = std::fs::read(&p3).unwrap();
        bad[5] = b'9'; // BPEXP003 → BPEXP903：不认识的版号
        let pbad = dir.join("bad.bxp");
        std::fs::write(&pbad, &bad).unwrap();
        assert!(read_bin(&pbad).is_err(), "魔数不认识就必须报错，不许照内容猜形状");

        let pj = dir.join("a.jsonl");
        write_jsonl(&pj, &recs).unwrap();
        let (j, _) = read_jsonl(&pj).unwrap();
        assert_eq!(j[2].expire_unix_secs, Some(1_900_000_000));
        assert_eq!(j[0].expire_unix_secs, None);
        let pold = dir.join("old.jsonl");
        std::fs::write(&pold, "{\"key\":\"a2V5\",\"value\":\"dg==\"}\n").unwrap();
        let (o, _) = read_jsonl(&pold).unwrap();
        assert_eq!(o[0].key, b"key");
        assert_eq!(o[0].value.as_deref(), Some(b"v".as_ref()));
        assert_eq!(o[0].expire_unix_secs, None, "旧行缺 expire 字段 = 永久，不是报错");
        let pbroken = dir.join("broken.jsonl");
        std::fs::write(&pbroken, "{\"key\":\"a2V5\",\"value\":\"dg==\",\"expire\":12x}\n").unwrap();
        assert!(read_jsonl(&pbroken).is_err(), "expire 有字段但值坏了 ⇒ 必须报错");

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// M45 的第一条守卫：**每一个长度**都要往返一致，不是"挑几个长度"。
    ///
    /// 为什么必须是全长度扫：`b64_decode` 原先在尾组之后又按 `=` 的个数**回头裁一次**
    /// （`=` 早在 `filter` 里剥掉了 ⇒ 二次修正），于是要么全对要么按 `len % 3` 分成三档：
    /// `len%3==0` 恰好没事，`==2` 少 1 字节，`==1` 少 2 字节。而旧的那条 `test_b64_known`
    /// 唯一的解码断言是长度 3 —— 正好是唯一能过的那一格，所以这个 bug 一路活到今天。
    /// 抽样式断言在这种"按余数分档"的编码上等于没测。
    #[test]
    fn test_b64_round_trips_every_length() {
        for len in 0usize..=64 {
            let raw: Vec<u8> = (0..len).map(|i| b'a' + (i % 26) as u8).collect();
            let enc = b64_encode(&raw);
            let back = b64_decode(&enc)
                .unwrap_or_else(|e| panic!("长度 {len} 往返失败: {e}（enc={enc}）"));
            assert_eq!(
                back, raw,
                "长度 {len} 解回 {} 字节，差 {}（enc={enc}）⇒ 尾组又被裁了一次？",
                back.len(),
                raw.len() as isize - back.len() as isize
            );
        }
        // 事故形状那一格单独点名：11 字节的键 `demo:000001` 曾被截成 10 字节 `demo:00000`
        let key = b"demo:000001";
        assert_eq!(key.len(), 11, "这条点名要求键长 11（11 % 3 == 2 那一档）");
        assert_eq!(b64_decode(&b64_encode(key)).unwrap().as_slice(), key);
    }

    /// 尾组的"非法长度"那一支也得还在：4k+1 个字符不是合法的 base64。
    /// 钉它是为了防"为了让往返一致而把长度检查一并删掉"这种修法。
    #[test]
    fn test_b64_decode_rejects_illegal_length() {
        assert!(b64_decode("A").is_err(), "1 个字符不是合法的 base64 尾组");
        assert!(b64_decode("AAAAA").is_err(), "5 个字符（4+1）不是合法的 base64");
        // 合法尾组两种形状各自给出正确字节数
        assert_eq!(b64_decode("Zg==").unwrap(), b"f", "2 字符尾组 = 1 字节");
        assert_eq!(b64_decode("Zm8=").unwrap(), b"fo", "3 字符尾组 = 2 字节");
    }

    /// M45 的第二条守卫：JSONL 往返回来**键还是一把一把不同的**（事故形状）。
    ///
    /// 为什么这条不能只靠上面那条长度扫：长度扫描证的是"编解码本身"，而现场的损失是
    /// **两个错合成一个对** —— 300 把键各少一个字节 ⇒ 全都变成同一把 ⇒ 后写覆盖前写，
    /// 库里只剩一把。只有"灌 N 把互不相同的键、读回来还是 N 把、每把值还对"这种断言
    /// 才抓得住"塌键"这一类，而不是"截字节"本身。
    #[test]
    fn test_jsonl_round_trip_does_not_collapse_keys() {
        const N: usize = 300;
        let path = std::env::temp_dir().join(format!("kv_snap_jsonl_{}.jsonl", std::process::id()));
        let recs: Vec<KvRecord> = (0..N)
            .map(|i| {
                KvRecord::new(
                    format!("demo:{i:06}").into_bytes(), // 11 字节 ⇒ 正是 11 % 3 == 2 那一档
                    Some(format!("val-{i}").into_bytes()),
                )
            })
            .collect();
        write_jsonl(&path, &recs).unwrap();
        let (back, st) = read_jsonl(&path).unwrap();
        let _ = std::fs::remove_file(&path);

        assert_eq!(st.records, N, "写出去的行数不对");
        assert_eq!(back.len(), N, "读回来的行数不对");
        let distinct: std::collections::HashSet<&[u8]> = back.iter().map(|r| r.key.as_slice()).collect();
        assert_eq!(
            distinct.len(),
            N,
            "读回来只剩 {} 把不同的键 ⇒ 键被截短、互相覆盖了",
            distinct.len()
        );
        for r in &back {
            let i: usize = std::str::from_utf8(&r.key[5..])
                .expect("键的序号部分应是 ASCII 数字")
                .parse()
                .expect("键的序号部分应能解析成整数");
            assert_eq!(
                r.value.as_deref(),
                Some(format!("val-{i}").as_bytes()),
                "键 {:?} 的值读回来不是它自己那份",
                String::from_utf8_lossy(&r.key)
            );
        }
    }

    /// 逐行写口（M44）与整批写的**字节必须一字不差**：导出文件的格式是跨引擎的合同
    /// （`import_kv` 要能读回来，旧版工具也要能读 `BPEXP001`），而 CLI 从"整批"换成了"逐行"。
    ///
    /// ⚠️ 这一条与 `test_plain_is_bpexp001` 那份 golden 是两件事：那份钉"编码本身"，
    /// 这一份钉"换写法没换格式"。两条都要 —— 只留后者的话，编码哪天动了它俩还互相对齐。
    ///
    /// ⚠️ 没有覆盖的一支：`ExportWriter::push` 在写失败之后一律收手、错误由 `finish()` 抛。
    /// 那一支要真把盘弄坏才触发（Windows 上还留着句柄的文件删不掉），恒真的模拟测试不给信息，
    /// 所以留注释不留测试。
    #[test]
    fn test_streaming_sink_writes_identical_bytes() {
        let pid = std::process::id();
        let streamed = std::env::temp_dir().join(format!("kv_snap_stream_{pid}.bin"));
        let batched = std::env::temp_dir().join(format!("kv_snap_batch_{pid}.bin"));
        let jsonl_streamed = std::env::temp_dir().join(format!("kv_snap_stream_{pid}.jsonl"));
        let jsonl_batched = std::env::temp_dir().join(format!("kv_snap_batch_{pid}.jsonl"));
        let recs = vec![
            KvRecord::new(b"alpha".to_vec(), Some(b"1".to_vec())),
            KvRecord::new(b"beta".to_vec(), None),
            KvRecord::with_seq(b"gamma".to_vec(), Some(b"3".to_vec()), 7),
        ];

        // bin：不带 seq 的一支（CLI 走的就是这条 —— 导出的行里没有 seq）
        let plain = vec![
            KvRecord::new(b"alpha".to_vec(), Some(b"1".to_vec())),
            KvRecord::new(b"beta".to_vec(), None),
        ];
        let mut w = BinWriter::create(&streamed, false).unwrap();
        let mut handed = 0usize;
        for r in &plain {
            w.write(r).unwrap();
            handed += 1;
        }
        let st_stream = w.finish().unwrap();
        let st_batch = write_bin(&batched, &plain).unwrap();
        assert_eq!(
            std::fs::read(&streamed).unwrap(),
            std::fs::read(&batched).unwrap(),
            "逐行写的 bin 与整批写不是同一份字节"
        );
        assert_eq!(handed, 2);
        assert_eq!(st_stream.records, st_batch.records);
        assert_eq!(st_stream.live, st_batch.live);
        assert_eq!(st_stream.deleted, st_batch.deleted);

        // jsonl：同一批记录
        let mut w = JsonlWriter::create(&jsonl_streamed).unwrap();
        for r in &recs {
            w.write(r).unwrap();
        }
        w.finish().unwrap();
        write_jsonl(&jsonl_batched, &recs).unwrap();
        assert_eq!(
            std::fs::read(&jsonl_streamed).unwrap(),
            std::fs::read(&jsonl_batched).unwrap(),
            "逐行写的 jsonl 与整批写不是同一份字节"
        );

        // `ExportWriter`（CLI 用的那层胶）也必须给出同一份字节
        let glued = std::env::temp_dir().join(format!("kv_snap_glue_{pid}.bin"));
        {
            let mut g = ExportWriter::bin(&glued, false).unwrap();
            for r in &plain {
                assert!(
                    g.push(ExportRecord::new(r.key.clone(), r.value.clone())),
                    "逐行推送中途被拒"
                );
            }
            g.finish().unwrap();
        }
        assert_eq!(
            std::fs::read(&glued).unwrap(),
            std::fs::read(&batched).unwrap(),
            "ExportWriter 与整批写不是同一份字节"
        );

        for p in [
            &streamed,
            &batched,
            &jsonl_streamed,
            &jsonl_batched,
            &glued,
        ] {
            let _ = std::fs::remove_file(p);
        }
    }

    #[test]
    fn test_bin_roundtrip() {
        let path = std::env::temp_dir().join(format!("kv_snap_{}.bin", std::process::id()));
        let recs = vec![
            KvRecord::new(b"a".to_vec(), Some(b"1".to_vec())),
            KvRecord::new(b"b".to_vec(), None),
        ];
        write_bin(&path, &recs).unwrap();
        let (back, st) = read_bin(&path).unwrap();
        assert_eq!(st.live, 1);
        assert_eq!(st.deleted, 1);
        assert_eq!(back, recs);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_bin_v2_roundtrip() {
        let path = std::env::temp_dir().join(format!("kv_snap_v2_{}.bin", std::process::id()));
        let recs = vec![
            KvRecord::with_seq(b"k".to_vec(), Some(b"v".to_vec()), 42),
            KvRecord::with_seq(b"d".to_vec(), None, 99),
        ];
        write_bin(&path, &recs).unwrap();
        let (back, _) = read_bin(&path).unwrap();
        assert_eq!(back, recs);
        // v2 每条多 8 字节 seq；v1 不写
        assert_eq!(back[0].seq, Some(42));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_jsonl_roundtrip() {
        let path = std::env::temp_dir().join(format!("kv_snap_{}.jsonl", std::process::id()));
        let recs = vec![
            KvRecord::new(b"k".to_vec(), Some(b"v".to_vec())),
            KvRecord::new(b"d".to_vec(), None),
        ];
        write_jsonl(&path, &recs).unwrap();
        let (back, st) = read_jsonl(&path).unwrap();
        assert_eq!(st.live, 1);
        assert_eq!(st.deleted, 1);
        assert_eq!(back, recs);
        let _ = std::fs::remove_file(&path);
    }

    /// 不带 seq 的导出与旧版 BPEXP001 逐字节一致
    #[test]
    fn test_plain_is_bpexp001() {
        let path = std::env::temp_dir().join(format!("kv_snap_plain_{}.bin", std::process::id()));
        let recs = vec![KvRecord::new(b"k".to_vec(), Some(b"v".to_vec()))];
        write_bin(&path, &recs).unwrap();
        let data = std::fs::read(&path).unwrap();
        assert_eq!(&data[..8], b"BPEXP001");
        let _ = std::fs::remove_file(&path);
    }
}
