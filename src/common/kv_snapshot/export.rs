//! 本文件负责可见记录流式导出。

use super::*;

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
