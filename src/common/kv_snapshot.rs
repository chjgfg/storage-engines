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
        self.records += 1;
        if value.is_some() {
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
    w: BufWriter<File>,
    stats: IoStats,
    with_seq: bool,
    with_expire: bool,
    checksum: Option<u32>,
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
        let mut w = BufWriter::new(f);
        w.write_all(magic)?;
        Ok(Self {
            w,
            stats: IoStats::default(),
            with_seq,
            with_expire,
            checksum: checked.then(|| crate::common::disk::crc32(magic)),
        })
    }

    fn bytes(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.w.write_all(bytes)?;
        if let Some(crc) = &mut self.checksum {
            *crc = crate::common::disk::crc32_extend(*crc, bytes);
        }
        Ok(())
    }

    pub fn write(&mut self, rec: &KvRecord) -> std::io::Result<()> {
        if rec.key.len() >= u32::MAX as usize || rec.value.as_ref().is_some_and(|v| v.len() >= u32::MAX as usize) {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "snapshot record too large"));
        }
        self.bytes(&(rec.key.len() as u32).to_le_bytes())?;
        self.bytes(&rec.key)?;
        match &rec.value {
            Some(v) => {
                self.bytes(&(v.len() as u32).to_le_bytes())?;
                self.bytes(v)?;
            }
            None => self.bytes(&TOMBSTONE_LEN.to_le_bytes())?,
        }
        if self.with_seq {
            self.bytes(&rec.seq.unwrap_or(0).to_le_bytes())?;
        }
        if self.with_expire {
            self.bytes(&rec.expire_unix_secs.unwrap_or(0).to_le_bytes())?;
        }
        self.stats.push(&rec.value);
        Ok(())
    }

    pub fn finish(mut self) -> std::io::Result<IoStats> {
        if let Some(crc) = self.checksum {
            self.w.write_all(&u32::MAX.to_le_bytes())?;
            self.w.write_all(&(self.stats.records as u64).to_le_bytes())?;
            self.w.write_all(&crc.to_le_bytes())?;
        }
        self.w.flush()?;
        if self.checksum.is_some() {
            self.w.get_ref().sync_all()?;
        }
        Ok(self.stats)
    }
}

/// Publish a complete memory snapshot on the same filesystem. Any error before
/// rename leaves the previous snapshot intact. Unix also syncs the parent directory.
pub(crate) fn atomic_write_bin_v4(path: &Path,
    write: impl FnOnce(&mut BinWriter) -> std::io::Result<()>) -> std::io::Result<IoStats> {
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
    write(&mut writer)?;
    let stats = writer.finish()?;
    // Never remove the destination first: rename performs replacement.
    std::fs::rename(&staging.0, path)?;
    #[cfg(unix)]
    File::open(path.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new(".")))?.sync_all()?;
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

/// **流式**读二进制快照：解出一条就交给 `on`，`on` 返回 `false` 当场收手（不等读完）。
///
/// 存在的理由与 [`crate::KvEngine::export_latest_visible_for_each`] 同一件事的两端：
/// `import_kv` 那侧要的是"边读边写进引擎"，把整份文件先摆进 `Vec` 只是为等着被逐条丢掉
/// （600k 档实测那笔 `Vec` 是 123 MB 的瞬时）。[`read_bin`] 从这一条改成收集壳，
/// **口径一字不变**：同一种截断处理（尾部不完整就停，不报错）、同一套魔数判定。
pub fn read_bin_with(
    path: &Path,
    on: &mut dyn FnMut(KvRecord) -> bool,
) -> std::io::Result<IoStats> {
    read_bin_policy(path, on, false)
}

pub(crate) fn read_snapshot_with(path: &Path,
    on: &mut dyn FnMut(KvRecord) -> bool) -> std::io::Result<IoStats> {
    read_bin_policy(path, on, true)
}

fn read_bin_policy(path: &Path, on: &mut dyn FnMut(KvRecord) -> bool,
    strict: bool) -> std::io::Result<IoStats> {
    let data = std::fs::read(path)?;
    if data.len() < 8 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "文件过短",
        ));
    }
    // 读侧三版都认（M81 第 6 项）：版号只从魔数判，不看内容猜 —— 猜出来的形状会让一个截断的
    // 001 被读成 003 的前缀，那比报"非法魔数"坏得多。
    let checked = &data[..8] == BIN_MAGIC_V4;
    let expected = if checked {
        if data.len() < 24 { return Err(snapshot_invalid("missing snapshot footer")); }
        let footer = data.len() - 16;
        if data[footer..footer + 4] != u32::MAX.to_le_bytes() ||
            crate::common::disk::crc32(&data[..footer]) != u32::from_le_bytes(data[footer + 12..].try_into().unwrap()) {
            return Err(snapshot_invalid("snapshot checksum/footer mismatch"));
        }
        Some(u64::from_le_bytes(data[footer + 4..footer + 12].try_into().unwrap()))
    } else { None };
    let data = if checked { &data[..data.len() - 16] } else { &data[..] };
    let (with_seq, with_expire) = if &data[..8] == BIN_MAGIC {
        (false, false)
    } else if &data[..8] == BIN_MAGIC_V2 {
        (true, false)
    } else if &data[..8] == BIN_MAGIC_V3 || checked {
        (false, true)
    } else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "非法导出文件魔数（期望 BPEXP001/BPEXP002/BPEXP003/BPEXP004）",
        ));
    };
    let mut off = 8usize;
    let mut stats = IoStats::default();
    let mut delivered = 8;
    while off + 4 <= data.len() {
        let key_len = u32::from_le_bytes(data[off..off + 4].try_into().unwrap()) as usize;
        off += 4;
        if off + key_len + 4 > data.len() {
            break;
        }
        let key = data[off..off + key_len].to_vec();
        off += key_len;
        let val_len = u32::from_le_bytes(data[off..off + 4].try_into().unwrap());
        off += 4;
        let value = if val_len == TOMBSTONE_LEN {
            None
        } else {
            let vl = val_len as usize;
            if off + vl > data.len() {
                break;
            }
            let v = data[off..off + vl].to_vec();
            off += vl;
            Some(v)
        };
        let seq = if with_seq {
            if off + 8 > data.len() {
                break;
            }
            let s = u64::from_le_bytes(data[off..off + 8].try_into().unwrap());
            off += 8;
            Some(s)
        } else {
            None
        };
        let expire_unix_secs = if with_expire {
            if off + 8 > data.len() {
                break;
            }
            let e = u64::from_le_bytes(data[off..off + 8].try_into().unwrap());
            off += 8;
            // 0 = 永久（文件里没有一个"空的 Option"这种写法，读回来才不用猜）
            (e != 0).then_some(e)
        } else {
            None
        };
        stats.push(&value);
        delivered = off;
        if !on(KvRecord {
            key,
            value,
            seq,
            expire_unix_secs,
        }) {
            return Ok(stats);
        }
    }
    if (strict || checked) && (delivered != data.len() || expected.is_some_and(|n| n != stats.records as u64)) {
        return Err(snapshot_invalid("incomplete snapshot record/count mismatch"));
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
            read_snapshot_with(&path, &mut |r| { back.push(r); true }).unwrap();
            assert_eq!(back, vec![record]);
            let mut bytes = std::fs::read(&path).unwrap();
            bytes.pop();
            std::fs::write(&path, bytes).unwrap();
            assert!(read_snapshot_with(&path, &mut |_| true).is_err());
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
