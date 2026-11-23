//! 二进制快照记录大小与编码。

use super::*;

pub(super) fn record_size(
    key: &[u8],
    value: Option<&[u8]>,
    with_seq: bool,
    with_expire: bool,
) -> std::io::Result<usize> {
    if key.len() >= u32::MAX as usize || value.is_some_and(|v| v.len() >= u32::MAX as usize) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "snapshot record too large",
        ));
    }
    key.len()
        .checked_add(value.map_or(0, <[u8]>::len))
        .and_then(|n| n.checked_add(8 + 8 * (with_seq as usize + with_expire as usize)))
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "snapshot record size overflow",
            )
        })
}

/// 直接写入和内存缓冲记录共用同一字段布局。
pub(super) fn encode_record(
    key: &[u8],
    value: Option<&[u8]>,
    seq: Option<u64>,
    expire: Option<u64>,
    with_seq: bool,
    with_expire: bool,
    mut bytes: impl FnMut(&[u8]) -> std::io::Result<()>,
) -> std::io::Result<()> {
    bytes(&(key.len() as u32).to_le_bytes())?;
    bytes(key)?;
    bytes(
        &value
            .map_or(TOMBSTONE_LEN, |v| v.len() as u32)
            .to_le_bytes(),
    )?;
    if let Some(value) = value {
        bytes(value)?;
    }
    if with_seq {
        bytes(&seq.unwrap_or(0).to_le_bytes())?;
    }
    if with_expire {
        bytes(&expire.unwrap_or(0).to_le_bytes())?;
    }
    Ok(())
}
