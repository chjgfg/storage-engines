//! JSONL 快照写入、解析与流式读取。

use super::*;

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

/// 收集壳：行形状与编码只有一份（[`JsonlWriter::write`]）—— 改这里就等于改出第二种格式。
pub fn write_jsonl(path: &Path, records: &[KvRecord]) -> std::io::Result<IoStats> {
    let mut w = JsonlWriter::create(path)?;
    for rec in records {
        w.write(rec)?;
    }
    w.finish()
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
pub(super) fn jsonl_u64_field(line: &str, tag: &str) -> std::io::Result<Option<u64>> {
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
pub(super) fn parse_jsonl_record(line: &str) -> std::io::Result<KvRecord> {
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

pub fn read_jsonl(path: &Path) -> std::io::Result<(Vec<KvRecord>, IoStats)> {
    let mut out = Vec::new();
    let stats = read_jsonl_with(path, &mut |rec| {
        out.push(rec);
        true
    })?;
    Ok((out, stats))
}

pub(super) fn parse_jsonl_line(line: &str) -> Result<(String, Option<String>), String> {
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
