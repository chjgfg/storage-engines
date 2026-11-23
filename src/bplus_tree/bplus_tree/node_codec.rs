//! 本文件负责节点二进制解析与借用投影。

use super::*;

impl PointInternal {
    /// 检查字节边界后建立结构化视图。
    /// 借用视图只能活在原缓冲区生命周期内，格式无效时不得按默认值继续读取。
    pub(super) fn decode(payload: &[u8]) -> std::io::Result<Self> {
        let MaintenanceNode::Internal {
            keys,
            count,
            children,
        } = MaintenanceNode::decode(payload)?
        else {
            return Err(NodeBytes::invalid());
        };
        let mut cursor = NodeBytes(keys);
        let mut key_offsets = Vec::with_capacity(count);
        for _ in 0..count {
            let start = 20 + keys.len() - cursor.0.len();
            cursor.bytes()?;
            let end = 12 + keys.len() - cursor.0.len();
            key_offsets.push((
                u16::try_from(start).map_err(|_| NodeBytes::invalid())?,
                u16::try_from(end).map_err(|_| NodeBytes::invalid())?,
            ));
        }
        let children_offset = 12 + keys.len() + 8;
        // 解析缓存不保留页面补齐的零字节。
        Ok(Self {
            payload: payload[..children_offset + children.len()].to_vec(),
            key_offsets,
            children_offset,
        })
    }

    pub(super) fn child(&self, bound: &[u8]) -> PageId {
        let index = self
            .key_offsets
            .partition_point(|&(start, end)| &self.payload[start as usize..end as usize] <= bound);
        let at = self.children_offset + index * 8;
        PageId(u64::from_le_bytes(
            self.payload[at..at + 8].try_into().unwrap(),
        ))
    }
}
impl<'a> NodeBytes<'a> {
    pub(super) fn take(&mut self, n: usize) -> std::io::Result<&'a [u8]> {
        if n > self.0.len() {
            return Err(Self::invalid());
        }
        let (head, rest) = self.0.split_at(n);
        self.0 = rest;
        Ok(head)
    }
    pub(super) fn invalid() -> std::io::Error {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid maintenance node encoding",
        )
    }
    pub(super) fn u64(&mut self) -> std::io::Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub(super) fn len(&mut self) -> std::io::Result<usize> {
        usize::try_from(self.u64()?).map_err(|_| Self::invalid())
    }
    pub(super) fn bytes(&mut self) -> std::io::Result<&'a [u8]> {
        let n = self.len()?;
        self.take(n)
    }
    pub(super) fn option(&mut self) -> std::io::Result<bool> {
        match self.take(1)?[0] {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(Self::invalid()),
        }
    }
    pub(super) fn row(&mut self) -> std::io::Result<(&'a [u8], Option<&'a [u8]>)> {
        let key = self.bytes()?;
        let value = if self.option()? {
            Some(self.bytes()?)
        } else {
            None
        };
        Ok((key, value))
    }
}

impl DeleteRead {
    pub(super) fn project(
        payload: &[u8],
        run: &[Key],
        output: &mut Vec<u8>,
    ) -> std::io::Result<Self> {
        let node = MaintenanceNode::decode(payload)?;
        let MaintenanceNode::Leaf { rows, count, .. } = node else {
            return bincode::deserialize(payload)
                .map(Self::Owned)
                .map_err(|_| NodeBytes::invalid());
        };
        output.clear();
        if output.capacity() < MAX_PAYLOAD {
            #[cfg(test)]
            crate::common::maintenance_probe::note_bplus_pack_buffer();
            output.reserve(MAX_PAYLOAD);
        }
        output.extend_from_slice(&payload[..12]);
        let mut cursor = NodeBytes(rows);
        let mut removed = 0;
        let mut min_changed = false;
        for i in 0..count {
            let before = cursor.0;
            let (key, _) = cursor.row()?;
            if run
                .binary_search_by(|candidate| candidate.as_slice().cmp(key))
                .is_ok()
            {
                removed += 1;
                min_changed |= i == 0;
            } else {
                output.extend_from_slice(&before[..before.len() - cursor.0.len()]);
            }
        }
        let remaining = count - removed;
        output[4..12].copy_from_slice(&(remaining as u64).to_le_bytes());
        output.extend_from_slice(&payload[12 + rows.len()..]);
        Ok(Self::PackedLeaf {
            remaining,
            removed,
            min_changed,
        })
    }
}

impl PointStep {
    pub(super) fn leaf<'a>(
        rows: impl DoubleEndedIterator<Item = (&'a [u8], Option<&'a [u8]>)>,
        low: &[u8],
        high: &[u8],
        accept: &mut impl FnMut(&[u8]) -> bool,
    ) -> Self {
        let mut minimum = None;
        for (key, value) in rows.rev() {
            minimum = Some(key);
            if key > high {
                continue;
            }
            if key < low {
                return Self::Missing;
            }
            if accept(key) {
                return Self::Hit(key.to_vec(), RowId(value.map(<[u8]>::to_vec)));
            }
        }
        minimum.map_or(Self::Missing, |key| Self::Previous(key.to_vec()))
    }

    pub(super) fn borrowed(
        payload: &[u8],
        low: &[u8],
        high: &[u8],
        accept: &mut impl FnMut(&[u8]) -> bool,
    ) -> std::io::Result<Self> {
        let MaintenanceNode::Leaf { rows, count, .. } = MaintenanceNode::decode(payload)? else {
            return Err(NodeBytes::invalid());
        };
        // 一行至少包含八字节键长度和一个可选值标志；偏移量留在栈上，被拒绝的行不持有键值副本。
        let mut offsets = [0_u16; MAX_PAYLOAD / 9];
        if count > offsets.len() {
            return Err(NodeBytes::invalid());
        }
        let mut cursor = NodeBytes(rows);
        for offset in &mut offsets[..count] {
            *offset = (rows.len() - cursor.0.len())
                .try_into()
                .map_err(|_| NodeBytes::invalid())?;
            cursor.row()?;
        }
        let end = offsets[..count].partition_point(|offset| {
            NodeBytes(&rows[*offset as usize..]).row().unwrap().0 <= high
        });
        if end == 0 {
            return Ok(if count == 0 {
                Self::Missing
            } else {
                Self::Previous(NodeBytes(rows).row()?.0.to_vec())
            });
        }
        Ok(Self::leaf(
            offsets[..end]
                .iter()
                .map(|offset| NodeBytes(&rows[*offset as usize..]).row().unwrap()),
            low,
            high,
            accept,
        ))
    }
}
impl<'a> MaintenanceNode<'a> {
    /// 检查字节边界后建立结构化视图。
    /// 借用视图只能活在原缓冲区生命周期内，格式无效时不得按默认值继续读取。
    pub(super) fn decode(payload: &'a [u8]) -> std::io::Result<Self> {
        let mut bytes = NodeBytes(payload);
        let tag = u32::from_le_bytes(bytes.take(4)?.try_into().unwrap());
        let count = bytes.len()?;
        let begin = bytes.0;
        let mut previous: Option<&[u8]> = None;
        if count > bytes.0.len() / 8 {
            return Err(NodeBytes::invalid());
        }
        let node = match tag {
            0 => {
                for _ in 0..count {
                    let (key, _) = bytes.row()?;
                    if previous.is_some_and(|old| old >= key) {
                        return Err(NodeBytes::invalid());
                    }
                    previous = Some(key);
                }
                let rows = &begin[..begin.len() - bytes.0.len()];
                let next = if bytes.option()? {
                    Some(PageId(bytes.u64()?))
                } else {
                    None
                };
                Self::Leaf { rows, count, next }
            }
            1 => {
                for _ in 0..count {
                    let key = bytes.bytes()?;
                    if previous.is_some_and(|old| old >= key) {
                        return Err(NodeBytes::invalid());
                    }
                    previous = Some(key);
                }
                let keys = &begin[..begin.len() - bytes.0.len()];
                let child_count = bytes.len()?;
                if count.checked_add(1) != Some(child_count) {
                    return Err(NodeBytes::invalid());
                }
                let children =
                    bytes.take(child_count.checked_mul(8).ok_or_else(NodeBytes::invalid)?)?;
                Self::Internal {
                    keys,
                    count,
                    children,
                }
            }
            _ => return Err(NodeBytes::invalid()),
        };
        if bytes.0.iter().any(|b| *b != 0) {
            return Err(NodeBytes::invalid());
        }
        Ok(node)
    }
    pub(super) fn child(
        keys: &[u8],
        count: usize,
        children: &[u8],
        lower: &[u8],
    ) -> std::io::Result<PageId> {
        let mut keys = NodeBytes(keys);
        let mut index = 0;
        for _ in 0..count {
            if keys.bytes()? <= lower {
                index += 1;
            } else {
                break;
            }
        }
        Ok(PageId(u64::from_le_bytes(
            children[index * 8..index * 8 + 8].try_into().unwrap(),
        )))
    }
}
