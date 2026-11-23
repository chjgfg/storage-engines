//! 磁盘页和快照的 CRC32 计算。

use super::*;

/// CRC32（IEEE，多项式 0xEDB88320）。WAL 帧、SST 数据块、blob 记录都用它。
///
/// 与逐字节版（`crc32_bytewise`，留着当参照实现）**逐位相同**，这条由
/// `crc32_slice_by_8_matches_bytewise_everywhere` 按多种长度与随机内容钉住 ——
/// 校验和算错不是"慢一点"，是每一页每个块都判成坏页。
pub fn crc32(data: &[u8]) -> u32 {
    crc32_extend(0, data)
}

/// 增量计算 IEEE CRC32，无需保留此前全部字节。
pub(crate) fn crc32_extend(previous: u32, data: &[u8]) -> u32 {
    let mut crc = !previous;
    let mut chunks = data.chunks_exact(8);
    for c in &mut chunks {
        // 小端加载：与逐字节版"先吃低位字节"的顺序一致，所以两者可比
        let a = crc ^ u32::from_le_bytes([c[0], c[1], c[2], c[3]]);
        let b = u32::from_le_bytes([c[4], c[5], c[6], c[7]]);
        let s = &CRC32_SLICES;
        crc = s[7][(a & 0xFF) as usize]
            ^ s[6][((a >> 8) & 0xFF) as usize]
            ^ s[5][((a >> 16) & 0xFF) as usize]
            ^ s[4][((a >> 24) & 0xFF) as usize]
            ^ s[3][(b & 0xFF) as usize]
            ^ s[2][((b >> 8) & 0xFF) as usize]
            ^ s[1][((b >> 16) & 0xFF) as usize]
            ^ s[0][((b >> 24) & 0xFF) as usize];
    }
    for &b in chunks.remainder() {
        crc = (crc >> 8) ^ CRC32_TABLE[((crc ^ b as u32) & 0xFF) as usize];
    }
    !crc
}

/// 逐字节参照实现（串行依赖链，慢的那个）。留着有两个用处：对拍、以及让 A/B 有另一半。
#[cfg(test)]
pub(crate) fn crc32_bytewise(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &b in data {
        crc = (crc >> 8) ^ CRC32_TABLE[((crc ^ b as u32) & 0xFF) as usize];
    }
    !crc
}
