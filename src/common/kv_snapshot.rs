//! 迁移快照编解码：四个引擎的 `import_kv` / `export_kv` 共用。
//!
//! 不属于任何引擎核心，只服务于跨引擎搬数据。两种格式：
//!
//! - **JSONL**：每行 `{"key":"<base64>","value":"<base64>"|null[,"seq":N]}`
//! - **二进制**：magic + 重复 `(key_len, key, val_len|tombstone, value?[, seq])`
//!
//! 二进制有两个魔数：`BPEXP001` 不带 seq，`BPEXP002` 每条多 8 字节 seq。
//! [`write_bin`] 按记录里是否有 `seq` 自动选择，所以不带 seq 的导出与
//! 旧版 `BPEXP001` **逐字节一致**；[`read_bin`] 两种都认。

use crate::common::types::ExportRecord;
use std::{
    fs::File,
    io::{BufRead, BufReader, BufWriter, Write},
    path::Path,
};

pub const BIN_MAGIC: &[u8; 8] = b"BPEXP001";
pub const BIN_MAGIC_V2: &[u8; 8] = b"BPEXP002";
pub const TOMBSTONE_LEN: u32 = u32::MAX;

/// 一条快照记录。`value: None` 是 tombstone。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvRecord {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    /// LSM 的写入 seq，仅 `--with-seq` 导出时有值；其他引擎一律 `None`
    pub seq: Option<u64>,
}

impl KvRecord {
    /// 不带 seq，三个非 LSM 引擎用这个
    pub fn new(key: Vec<u8>, value: Option<Vec<u8>>) -> Self {
        Self {
            key,
            value,
            seq: None,
        }
    }

    /// 带 LSM 写入 seq
    pub fn with_seq(key: Vec<u8>, value: Option<Vec<u8>>, seq: u64) -> Self {
        Self {
            key,
            value,
            seq: Some(seq),
        }
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
    let rem = bytes.len() - i;
    if rem == 2 {
        let n = ((table[bytes[i] as usize] as u32) << 18)
            | ((table[bytes[i + 1] as usize] as u32) << 12);
        out.push(((n >> 16) & 0xFF) as u8);
    } else if rem == 3 {
        let n = ((table[bytes[i] as usize] as u32) << 18)
            | ((table[bytes[i + 1] as usize] as u32) << 12)
            | ((table[bytes[i + 2] as usize] as u32) << 6);
        out.push(((n >> 16) & 0xFF) as u8);
        out.push(((n >> 8) & 0xFF) as u8);
    } else if rem != 0 {
        return Err("非法 base64 长度".into());
    }
    let pad = s.chars().rev().take_while(|&c| c == '=').count();
    if pad == 1 && !out.is_empty() {
        out.pop();
    } else if pad == 2 && out.len() >= 2 {
        out.pop();
        out.pop();
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
}

impl BinWriter {
    pub fn create(path: &Path, with_seq: bool) -> std::io::Result<Self> {
        let f = File::create(path)?;
        let mut w = BufWriter::new(f);
        w.write_all(if with_seq { BIN_MAGIC_V2 } else { BIN_MAGIC })?;
        Ok(Self {
            w,
            stats: IoStats::default(),
            with_seq,
        })
    }

    pub fn write(&mut self, rec: &KvRecord) -> std::io::Result<()> {
        self.w.write_all(&(rec.key.len() as u32).to_le_bytes())?;
        self.w.write_all(&rec.key)?;
        match &rec.value {
            Some(v) => {
                self.w.write_all(&(v.len() as u32).to_le_bytes())?;
                self.w.write_all(v)?;
            }
            None => self.w.write_all(&TOMBSTONE_LEN.to_le_bytes())?,
        }
        if self.with_seq {
            self.w.write_all(&rec.seq.unwrap_or(0).to_le_bytes())?;
        }
        self.stats.push(&rec.value);
        Ok(())
    }

    pub fn finish(mut self) -> std::io::Result<IoStats> {
        self.w.flush()?;
        Ok(self.stats)
    }
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
        match &rec.value {
            Some(v) => {
                let v = b64_encode(v);
                if let Some(seq) = rec.seq {
                    writeln!(self.w, "{{\"key\":\"{k}\",\"value\":\"{v}\",\"seq\":{seq}}}")?;
                } else {
                    writeln!(self.w, "{{\"key\":\"{k}\",\"value\":\"{v}\"}}")?;
                }
            }
            None => {
                if let Some(seq) = rec.seq {
                    writeln!(
                        self.w,
                        "{{\"key\":\"{k}\",\"value\":null,\"seq\":{seq}}}"
                    )?;
                } else {
                    writeln!(self.w, "{{\"key\":\"{k}\",\"value\":null}}")?;
                }
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

    pub fn write(&mut self, rec: &KvRecord) -> std::io::Result<()> {
        match self {
            Self::Jsonl(w) => w.write(rec),
            Self::Bin(w) => w.write(rec),
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

    /// 交给导出回调的那一步：`true` = 接着折。已经错过就一律 `false`（一行都不再写）。
    pub fn push(&mut self, rec: ExportRecord) -> bool {
        if self.err.is_some() {
            return false;
        }
        let kv = KvRecord::new(rec.key, rec.value);
        match self.sink.write(&kv) {
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

pub fn read_bin(path: &Path) -> std::io::Result<(Vec<KvRecord>, IoStats)> {
    let data = std::fs::read(path)?;
    if data.len() < 8 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "文件过短",
        ));
    }
    let with_seq = &data[..8] == BIN_MAGIC_V2;
    if &data[..8] != BIN_MAGIC && !with_seq {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "非法导出文件魔数（期望 BPEXP001/BPEXP002）",
        ));
    }
    let mut off = 8usize;
    let mut out = Vec::new();
    let mut stats = IoStats::default();
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
        stats.push(&value);
        out.push(KvRecord { key, value, seq });
    }
    Ok((out, stats))
}

pub fn read_jsonl(path: &Path) -> std::io::Result<(Vec<KvRecord>, IoStats)> {
    let f = File::open(path)?;
    let reader = BufReader::new(f);
    let mut out = Vec::new();
    let mut stats = IoStats::default();
    for line in reader.lines() {
        let line = line?;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
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
        stats.push(&value);
        out.push(KvRecord { key, value, seq: None });
    }
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
    use super::*;

    #[test]
    fn test_b64_known() {
        assert_eq!(b64_encode(b""), "");
        assert_eq!(b64_encode(b"f"), "Zg==");
        assert_eq!(b64_encode(b"fo"), "Zm8=");
        assert_eq!(b64_encode(b"foo"), "Zm9v");
        assert_eq!(b64_decode("Zm9v").unwrap(), b"foo");
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
                    g.push(ExportRecord {
                        key: r.key.clone(),
                        value: r.value.clone(),
                    }),
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
