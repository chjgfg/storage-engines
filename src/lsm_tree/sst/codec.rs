//! 本文件负责SST 块、索引和格式编解码。

use super::*;

pub(super) fn open_v1(
    id: u64,
    path: PathBuf,
    size_bytes: u64,
    f: &mut File,
) -> std::io::Result<SstFile> {
    let mut buf8 = [0u8; 8];
    f.read_exact(&mut buf8)?;
    let count = u64::from_le_bytes(buf8) as usize;
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let (k, v) = read_entry_v1(f)?;
        entries.push((k, StoredEntry { seq: 0, value: v }));
    }
    let (min_key, max_key) = min_max_keys_stored(&entries);
    Ok(SstFile {
        id,
        path,
        min_key,
        max_key,
        size_bytes,
        move_eligibility: MoveEligibility::Unknown,
        entries_cache: Some(entries),
        index: Vec::new(),
        bloom: Bloom::empty(),
        is_v2: false,
        has_seq: false,
        block_cache: None,
        mmap: None,
        // v1 整份在 entries_cache 里，永不按 offset 读，所以不需要句柄
        handle: Mutex::new(None),
        cache_ns: 0,
    })
}

pub(super) fn open_v2_from_footer(
    id: u64,
    path: PathBuf,
    size_bytes: u64,
) -> std::io::Result<SstFile> {
    let mut f = File::open(&path)?;
    if size_bytes < 4 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "SST 过短",
        ));
    }
    f.seek(SeekFrom::End(-4))?;
    let mut fl = [0u8; 4];
    f.read_exact(&mut fl)?;
    let footer_len = u32::from_le_bytes(fl) as u64;
    if footer_len + 4 > size_bytes {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "footer 长度非法",
        ));
    }
    f.seek(SeekFrom::End(-(4 + footer_len as i64)))?;
    let mut footer = vec![0u8; footer_len as usize];
    f.read_exact(&mut footer)?;
    if footer.len() < 12 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "footer 过短",
        ));
    }
    let crc_stored = u32::from_le_bytes(footer[footer.len() - 4..].try_into().unwrap());
    let body = &footer[..footer.len() - 4];
    if crc32(body) != crc_stored {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "footer CRC 失败",
        ));
    }
    if body.len() < 8 || &body[body.len() - 8..] != SST_MAGIC_V2 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "SST v2 魔数错误",
        ));
    }
    let meta = &body[..body.len() - 8];
    let mut off = 0usize;
    let index_off = read_u64(meta, &mut off)?;
    let index_len = read_u32(meta, &mut off)? as usize;
    let bloom_off = read_u64(meta, &mut off)?;
    let bloom_len = read_u32(meta, &mut off)? as usize;
    let _entry_count = read_u64(meta, &mut off)?;
    let min_key = read_bytes(meta, &mut off)?;
    let max_key = read_bytes(meta, &mut off)?;
    // 可选 flags：has_seq（1 字节）；旧 footer 无此字段则默认 true（本仓库写路径一直有 seq 后加）
    let has_seq = if off < meta.len() {
        meta[off] != 0
    } else {
        true
    };

    let mut index_buf = vec![0u8; index_len];
    f.seek(SeekFrom::Start(index_off))?;
    f.read_exact(&mut index_buf)?;
    let index = decode_index(&index_buf)?;

    let mut bloom_buf = vec![0u8; bloom_len];
    if bloom_len > 0 {
        f.seek(SeekFrom::Start(bloom_off))?;
        f.read_exact(&mut bloom_buf)?;
    }
    let bloom = if bloom_len == 0 {
        Bloom::empty()
    } else {
        Bloom::decode(&bloom_buf).unwrap_or_else(Bloom::empty)
    };

    Ok(SstFile {
        id,
        path,
        min_key,
        max_key,
        size_bytes,
        move_eligibility: MoveEligibility::Unknown,
        entries_cache: None,
        index,
        bloom,
        is_v2: true,
        has_seq,
        block_cache: None,
        mmap: None,
        // 打开页脚/索引/bloom 用的这个句柄直接留下：之后每次按 offset 读都省掉一次 File::open
        handle: Mutex::new(Some(f)),
        cache_ns: 0,
    })
}

pub(super) fn read_u64(buf: &[u8], off: &mut usize) -> std::io::Result<u64> {
    if *off + 8 > buf.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "eof",
        ));
    }
    let v = u64::from_le_bytes(buf[*off..*off + 8].try_into().unwrap());
    *off += 8;
    Ok(v)
}

pub(super) fn read_u32(buf: &[u8], off: &mut usize) -> std::io::Result<u32> {
    if *off + 4 > buf.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "eof",
        ));
    }
    let v = u32::from_le_bytes(buf[*off..*off + 4].try_into().unwrap());
    *off += 4;
    Ok(v)
}

pub(super) fn read_bytes(buf: &[u8], off: &mut usize) -> std::io::Result<Vec<u8>> {
    let n = read_u32(buf, off)? as usize;
    if *off + n > buf.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "eof",
        ));
    }
    let v = buf[*off..*off + n].to_vec();
    *off += n;
    Ok(v)
}

pub(super) fn decode_index(buf: &[u8]) -> std::io::Result<Vec<IndexEntry>> {
    let mut off = 0usize;
    let count = read_u32(buf, &mut off)? as usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let first_key = read_bytes(buf, &mut off)?;
        let offset = read_u64(buf, &mut off)?;
        let len = read_u32(buf, &mut off)?;
        out.push(IndexEntry {
            first_key,
            offset,
            len,
        });
    }
    Ok(out)
}

pub(super) fn encode_index(index: &[IndexEntry]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(index.len() as u32).to_le_bytes());
    for ie in index {
        out.extend_from_slice(&(ie.first_key.len() as u32).to_le_bytes());
        out.extend_from_slice(&ie.first_key);
        out.extend_from_slice(&ie.offset.to_le_bytes());
        out.extend_from_slice(&ie.len.to_le_bytes());
    }
    out
}

pub(super) fn encode_entry(k: &[u8], se: &StoredEntry, with_seq: bool) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(k.len() as u32).to_le_bytes());
    out.extend_from_slice(k);
    out.push(se.value.kind_byte());
    if with_seq {
        out.extend_from_slice(&se.seq.to_le_bytes());
    }
    if let EntryValue::Put(Some(val)) = &se.value {
        out.extend_from_slice(&(val.len() as u32).to_le_bytes());
        out.extend_from_slice(val);
    }
    out
}

pub(super) fn read_entry_from_slice(
    data: &[u8],
    off: &mut usize,
    with_seq: bool,
) -> std::io::Result<(Vec<u8>, StoredEntry)> {
    read_entry_project(data, off, with_seq, |seq, kind, value| StoredEntry {
        seq,
        value: EntryValue::from_parts(kind, value.map(<[u8]>::to_vec)).expect("validated kind"),
    })
}

pub(super) fn read_entry_project<R>(
    data: &[u8],
    off: &mut usize,
    with_seq: bool,
    project: impl Fn(u64, u8, Option<&[u8]>) -> R,
) -> std::io::Result<(Vec<u8>, R)> {
    if *off + 4 > data.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "entry",
        ));
    }
    let kl = u32::from_le_bytes(data[*off..*off + 4].try_into().unwrap()) as usize;
    *off += 4;
    if *off + kl + 1 > data.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "key",
        ));
    }
    let key = data[*off..*off + kl].to_vec();
    *off += kl;
    let kind = data[*off];
    *off += 1;
    let seq = if with_seq {
        if *off + 8 > data.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "seq",
            ));
        }
        let s = u64::from_le_bytes(data[*off..*off + 8].try_into().unwrap());
        *off += 8;
        s
    } else {
        0
    };
    let val = if kind == 0 {
        if *off + 4 > data.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "val len",
            ));
        }
        let vl = u32::from_le_bytes(data[*off..*off + 4].try_into().unwrap()) as usize;
        *off += 4;
        if *off + vl > data.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "val",
            ));
        }
        let v = &data[*off..*off + vl];
        *off += vl;
        Some(v)
    } else {
        None
    };
    if kind > 2 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "未知 kind",
        ));
    }
    Ok((key, project(seq, kind, val)))
}

pub(super) fn read_entry_v1(f: &mut File) -> std::io::Result<(Vec<u8>, EntryValue)> {
    let mut buf4 = [0u8; 4];
    f.read_exact(&mut buf4)?;
    let kl = u32::from_le_bytes(buf4) as usize;
    let mut key: Vec<u8> = vec![0u8; kl];
    f.read_exact(&mut key)?;
    let mut kind = [0u8; 1];
    f.read_exact(&mut kind)?;
    let val: Option<Vec<u8>> = if kind[0] == 0 {
        f.read_exact(&mut buf4)?;
        let vl = u32::from_le_bytes(buf4) as usize;
        let mut v: Vec<u8> = vec![0u8; vl];
        f.read_exact(&mut v)?;
        Some(v)
    } else {
        None
    };
    EntryValue::from_parts(kind[0], val)
        .map(|ev| (key, ev))
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "kind"))
}

pub(super) fn decode_block(raw: &[u8], with_seq: bool) -> std::io::Result<DataBlock> {
    decode_block_project(raw, with_seq, read_entry_from_slice).map(|entries| DataBlock { entries })
}

pub(super) fn decode_light_block(
    raw: &[u8],
    with_seq: bool,
) -> std::io::Result<Vec<(Vec<u8>, LightEntry)>> {
    decode_block_project(raw, with_seq, |data, off, seq| {
        read_entry_project(data, off, seq, |seq, kind, _| LightEntry { seq, kind })
    })
}

pub(super) fn decode_block_project<R>(
    raw: &[u8],
    with_seq: bool,
    read: impl Fn(&[u8], &mut usize, bool) -> std::io::Result<(Vec<u8>, R)>,
) -> std::io::Result<Vec<(Vec<u8>, R)>> {
    if raw.len() < 8 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "block 过短",
        ));
    }
    let count = u32::from_le_bytes(raw[0..4].try_into().unwrap()) as usize;
    let crc_stored = u32::from_le_bytes(raw[raw.len() - 4..].try_into().unwrap());
    let body = &raw[4..raw.len() - 4];
    if crc32(body) != crc_stored {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "block CRC",
        ));
    }
    let mut off = 0usize;
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        entries.push(read(body, &mut off, with_seq)?);
    }
    Ok(entries)
}

pub(super) fn encode_block(entries: &[(Vec<u8>, StoredEntry)], with_seq: bool) -> Vec<u8> {
    let mut body = Vec::new();
    for (k, se) in entries {
        body.extend_from_slice(&encode_entry(k, se, with_seq));
    }
    let checksum = crc32(&body);
    let mut out = Vec::with_capacity(8 + body.len());
    out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    out.extend_from_slice(&body);
    out.extend_from_slice(&checksum.to_le_bytes());
    out
}

pub(super) fn min_max_keys_stored(entries: &[(Vec<u8>, StoredEntry)]) -> (Vec<u8>, Vec<u8>) {
    if entries.is_empty() {
        (Vec::new(), Vec::new())
    } else {
        (
            entries.first().unwrap().0.clone(),
            entries.last().unwrap().0.clone(),
        )
    }
}
