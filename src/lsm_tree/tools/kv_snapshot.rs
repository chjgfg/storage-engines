//! 迁移快照编解码（仅供 export_kv / import_kv 使用，不属于 LSM 引擎核心）。

use std::{
    fs::File,
    io::{BufRead, BufReader, BufWriter, Write},
    path::Path,
};

pub const BIN_MAGIC: &[u8; 8] = b"BPEXP001";
pub const BIN_MAGIC_V2: &[u8; 8] = b"BPEXP002";
pub const TOMBSTONE_LEN: u32 = u32::MAX;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvRecord {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    /// LSM write seq when known (export with --with-seq)
    pub seq: Option<u64>,
}

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

pub fn write_bin(path: &Path, records: &[KvRecord]) -> std::io::Result<IoStats> {
    let with_seq = records.iter().any(|r| r.seq.is_some());
    let f = File::create(path)?;
    let mut w = BufWriter::new(f);
    w.write_all(if with_seq { BIN_MAGIC_V2 } else { BIN_MAGIC })?;
    let mut stats = IoStats::default();
    for rec in records {
        w.write_all(&(rec.key.len() as u32).to_le_bytes())?;
        w.write_all(&rec.key)?;
        match &rec.value {
            Some(v) => {
                w.write_all(&(v.len() as u32).to_le_bytes())?;
                w.write_all(v)?;
            }
            None => w.write_all(&TOMBSTONE_LEN.to_le_bytes())?,
        }
        if with_seq {
            w.write_all(&rec.seq.unwrap_or(0).to_le_bytes())?;
        }
        stats.push(&rec.value);
    }
    w.flush()?;
    Ok(stats)
}

pub fn write_jsonl(path: &Path, records: &[KvRecord]) -> std::io::Result<IoStats> {
    let f = File::create(path)?;
    let mut w = BufWriter::new(f);
    let mut stats = IoStats::default();
    for rec in records {
        let k = b64_encode(&rec.key);
        match &rec.value {
            Some(v) => {
                let v = b64_encode(v);
                if let Some(seq) = rec.seq {
                    writeln!(w, "{{\"key\":\"{k}\",\"value\":\"{v}\",\"seq\":{seq}}}")?;
                } else {
                    writeln!(w, "{{\"key\":\"{k}\",\"value\":\"{v}\"}}")?;
                }
            }
            None => {
                if let Some(seq) = rec.seq {
                    writeln!(w, "{{\"key\":\"{k}\",\"value\":null,\"seq\":{seq}}}")?;
                } else {
                    writeln!(w, "{{\"key\":\"{k}\",\"value\":null}}")?;
                }
            }
        }
        stats.push(&rec.value);
    }
    w.flush()?;
    Ok(stats)
}

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
    fn test_roundtrip() {
        let path = std::env::temp_dir().join(format!("lsm_kv_snap_{}.bin", std::process::id()));
        let recs = vec![
            KvRecord {
                key: b"a".to_vec(),
                value: Some(b"1".to_vec()),
                seq: None,
            },
            KvRecord {
                key: b"b".to_vec(),
                value: None,
                seq: None,
            },
        ];
        write_bin(&path, &recs).unwrap();
        let (back, st) = read_bin(&path).unwrap();
        assert_eq!(st.live, 1);
        assert_eq!(st.deleted, 1);
        assert_eq!(back, recs);
        let _ = std::fs::remove_file(&path);
    }
}
