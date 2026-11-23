//! 固定页头与 CRC 编解码。

use super::*;

// ─── 页头 pack / unpack ─────────────────────────────────────────────────────

/// 把 payload 打成带 CRC 的完整页（4KB）
///
/// 布局：
/// ```text
/// 0..4   magic  "BPG1"
/// 4..8   page_id (u32 LE，页号高 32 位必须为 0，教学简化)
/// 8..12  crc32(payload)
/// 12..16 reserved
/// 16..4096 payload（不足补 0）
/// ```
pub fn pack_page(page_id: u64, payload: &[u8]) -> Vec<u8> {
    assert!(payload.len() <= PAGE_PAYLOAD_SIZE, "payload 超过页载荷上限");
    let mut page = vec![0u8; PAGE_SIZE];
    page[0..4].copy_from_slice(&PAGE_MAGIC.to_le_bytes());
    page[4..8].copy_from_slice(&(page_id as u32).to_le_bytes());
    // reserved 先置 0
    let mut body = [0u8; PAGE_PAYLOAD_SIZE];
    body[..payload.len()].copy_from_slice(payload);
    let checksum = crc32(&body);
    page[8..12].copy_from_slice(&checksum.to_le_bytes());
    page[PAGE_HEADER_SIZE..].copy_from_slice(&body);
    page
}

/// 校验并拆页，返回 (page_id, payload_owned)
pub fn unpack_page(raw: &[u8]) -> Result<(u64, Vec<u8>), String> {
    if raw.len() != PAGE_SIZE {
        return Err(format!("页长度错误: {} != {}", raw.len(), PAGE_SIZE));
    }
    let magic = u32::from_le_bytes(raw[0..4].try_into().unwrap());
    if magic != PAGE_MAGIC {
        // 全 0 页视为空页（新分配尚未写有效数据）
        if raw.iter().all(|&b| b == 0) {
            return Ok((0, vec![0u8; PAGE_PAYLOAD_SIZE]));
        }
        return Err(format!("页魔数错误: {magic:#x}"));
    }
    let page_id = u32::from_le_bytes(raw[4..8].try_into().unwrap()) as u64;
    let expect_crc = u32::from_le_bytes(raw[8..12].try_into().unwrap());
    let payload = &raw[PAGE_HEADER_SIZE..];
    let actual_crc = crc32(payload);
    if expect_crc != actual_crc {
        return Err(format!(
            "页 CRC 校验失败 page_id={page_id}: expect={expect_crc:#x} actual={actual_crc:#x}"
        ));
    }
    Ok((page_id, payload.to_vec()))
}

/// 只校验 CRC，不拆 payload
pub(crate) fn maintenance_payload(raw: &[u8], wanted: u64) -> std::io::Result<&[u8]> {
    let invalid = |message| std::io::Error::new(std::io::ErrorKind::InvalidData, message);
    if raw.len() != PAGE_SIZE || raw[..4] != PAGE_MAGIC.to_le_bytes() {
        return Err(invalid(
            "maintenance page is missing or has an invalid magic",
        ));
    }
    let found = u32::from_le_bytes(raw[4..8].try_into().unwrap()) as u64;
    if found != wanted {
        return Err(invalid("maintenance page identity mismatch"));
    }
    let payload = &raw[PAGE_HEADER_SIZE..];
    let expected = u32::from_le_bytes(raw[8..12].try_into().unwrap());
    if crc32(payload) != expected {
        return Err(invalid("maintenance page CRC mismatch"));
    }
    Ok(payload)
}

/// 只校验 CRC，不拆 payload
pub fn verify_page(raw: &[u8]) -> bool {
    unpack_page(raw).is_ok()
}
