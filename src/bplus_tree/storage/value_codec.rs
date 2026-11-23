//! 页内值与 blob 指针编解码。

use super::*;

/// 把逻辑 value 编码为叶子存储形式（可能写 blob）
pub fn encode_stored_value(
    blob: &mut BlobStore,
    value: Vec<u8>,
    sync_blob: bool,
) -> std::io::Result<Vec<u8>> {
    if value.len() <= BLOB_THRESHOLD {
        let mut out = Vec::with_capacity(1 + value.len());
        out.push(VAL_TAG_INLINE);
        out.extend_from_slice(&value);
        return Ok(out);
    }
    let offset = blob.append(&value)?;
    if sync_blob {
        blob.sync()?; // BlobStore::sync 内部已经记 SITE_BLOB 这一格
    }
    let mut out = Vec::with_capacity(1 + 8 + 4);
    out.push(VAL_TAG_BLOB);
    out.extend_from_slice(&offset.to_le_bytes());
    out.extend_from_slice(&(value.len() as u32).to_le_bytes());
    Ok(out)
}

/// 叶子存储形式 → 逻辑 value
pub fn decode_stored_value(
    blob: &mut BlobStore,
    stored: &[u8],
) -> std::io::Result<Option<Vec<u8>>> {
    #[cfg(test)]
    VALUE_DECODES.with(|c| c.set(c.get() + 1));
    if stored.is_empty() {
        return Ok(None);
    }
    match stored[0] {
        VAL_TAG_INLINE => Ok(Some(stored[1..].to_vec())),
        VAL_TAG_BLOB => {
            if stored.len() < 1 + 8 + 4 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "blob 引用截断",
                ));
            }
            let offset = u64::from_le_bytes(stored[1..9].try_into().unwrap());
            let _len = u32::from_le_bytes(stored[9..13].try_into().unwrap());
            Ok(Some(blob.read_at(offset)?))
        }
        // 兼容旧数据：无 tag 的裸 value 当内联
        _ => Ok(Some(stored.to_vec())),
    }
}
