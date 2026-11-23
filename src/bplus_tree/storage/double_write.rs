//! 页双写区准备、发布和恢复。

use super::*;

impl DoubleWriteBuffer {
    pub fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let mut file = DiskFile::open(path)?;
        let mut state = DblwrState::Unknown;
        if file.len()? == 0 {
            // 初始化空 header
            let mut hdr = vec![0u8; DBLWR_HEADER_SIZE];
            hdr[0..8].copy_from_slice(DBLWR_MAGIC);
            file.write_all_at(0, &hdr)?;
            file.sync_at(SITE_BPLUS_DBLWR)?;
            state = DblwrState::DurableEmpty;
        }
        Ok(Self { file, state })
    }
    /// 先完整写出并同步页双写区，为主文件写回提供恢复副本。
    /// 双写容量固定，调用方负责按容量分片；记录发布顺序不能与主文件写回互换。

    /// 把一批**已 pack** 的完整页写入双写区并 fsync
    pub fn write_batch(&mut self, pages: &[(u64, Vec<u8>)]) -> std::io::Result<()> {
        assert!(pages.len() <= DBLWR_MAX_PAGES);
        for (_, p) in pages {
            assert_eq!(p.len(), PAGE_SIZE);
        }

        let mut hdr = vec![0u8; DBLWR_HEADER_SIZE];
        hdr[0..8].copy_from_slice(DBLWR_MAGIC);
        hdr[8..12].copy_from_slice(&(pages.len() as u32).to_le_bytes());

        #[cfg(test)]
        let reference = dblwr_probe::reference();
        #[cfg(not(test))]
        let reference = false;
        // 只有已确认的清空或初始化才证明头部持久为空；重开和部分写入均采用保守重置。
        let reset = self.state != DblwrState::DurableEmpty || reference;
        self.state = DblwrState::Unknown;
        if reset {
            #[cfg(test)]
            let _timer = crate::common::maintenance_probe::Timer::start(
                crate::common::maintenance_probe::Stage::DblwrReset,
            );
            let mut zero_hdr = hdr.clone();
            zero_hdr[8..12].copy_from_slice(&0u32.to_le_bytes());
            self.file.write_all_at(0, &zero_hdr)?;
            self.file.sync_at(SITE_BPLUS_DBLWR)?;
        }
        {
            #[cfg(test)]
            let _timer = crate::common::maintenance_probe::Timer::start(
                crate::common::maintenance_probe::Stage::DblwrData,
            );
            // 一次有界连续写入，原磁盘槽布局和发布屏障不变，最多64页。
            let slot_size = 8 + PAGE_SIZE;
            if reference {
                for (i, (page_id, page)) in pages.iter().enumerate() {
                    let mut slot = Vec::with_capacity(slot_size);
                    slot.extend_from_slice(&page_id.to_le_bytes());
                    slot.extend_from_slice(page);
                    #[cfg(test)]
                    dblwr_probe::note_write();
                    self.file
                        .write_all_at((DBLWR_HEADER_SIZE + i * slot_size) as u64, &slot)?;
                }
            } else {
                let mut payload = Vec::with_capacity(pages.len() * slot_size);
                for (page_id, page) in pages {
                    payload.extend_from_slice(&page_id.to_le_bytes());
                    payload.extend_from_slice(page);
                }
                if !payload.is_empty() {
                    #[cfg(test)]
                    dblwr_probe::note_write();
                    self.file.write_all_at(DBLWR_HEADER_SIZE as u64, &payload)?;
                }
            }
            self.file.sync_at(SITE_BPLUS_DBLWR)?;
        }
        #[cfg(test)]
        dblwr_probe::fail_at(1)?;
        {
            #[cfg(test)]
            let _timer = crate::common::maintenance_probe::Timer::start(
                crate::common::maintenance_probe::Stage::DblwrPublish,
            );
            self.file.write_all_at(0, &hdr)?;
            #[cfg(test)]
            dblwr_probe::fail_at(2)?;
            self.file.sync_at(SITE_BPLUS_DBLWR)?;
        }
        self.state = DblwrState::Published;
        Ok(())
    }

    /// 读取 batch 中的有效页（page_id, raw_page）
    pub fn read_batch(&mut self) -> std::io::Result<Vec<(u64, Vec<u8>)>> {
        let len = self.file.len()?;
        if len < DBLWR_HEADER_SIZE as u64 {
            return Ok(Vec::new());
        }
        let mut hdr = vec![0u8; DBLWR_HEADER_SIZE];
        self.file.read_exact_at(0, &mut hdr)?;
        if &hdr[0..8] != DBLWR_MAGIC {
            return Ok(Vec::new());
        }
        let count = u32::from_le_bytes(hdr[8..12].try_into().unwrap()) as usize;
        if count == 0 || count > DBLWR_MAX_PAGES {
            return Ok(Vec::new());
        }

        let slot_size = 8 + PAGE_SIZE;
        let mut out = Vec::with_capacity(count);
        for i in 0..count {
            let offset = (DBLWR_HEADER_SIZE + i * slot_size) as u64;
            if offset + slot_size as u64 > len {
                break;
            }
            let mut slot = vec![0u8; slot_size];
            self.file.read_exact_at(offset, &mut slot)?;
            let page_id = u64::from_le_bytes(slot[0..8].try_into().unwrap());
            let page = slot[8..].to_vec();
            // 只接受 CRC 合法的页
            if verify_page(&page) {
                out.push((page_id, page));
            }
        }
        Ok(out)
    }
    /// 主文件发布完成后清空双写区头。
    /// 清空头的持久状态决定能否复用上次空头，不能只凭内存状态跳过恢复屏障。

    /// 清空 batch（flush 主文件成功后调用）
    pub fn clear(&mut self) -> std::io::Result<()> {
        #[cfg(test)]
        let _timer = crate::common::maintenance_probe::Timer::start(
            crate::common::maintenance_probe::Stage::DblwrClear,
        );
        self.state = DblwrState::Unknown;
        let mut hdr = vec![0u8; DBLWR_HEADER_SIZE];
        hdr[0..8].copy_from_slice(DBLWR_MAGIC);
        // count = 0
        self.file.write_all_at(0, &hdr)?;
        #[cfg(test)]
        dblwr_probe::fail_at(3)?;
        self.file.sync_at(SITE_BPLUS_DBLWR)?;
        self.state = DblwrState::DurableEmpty;
        Ok(())
    }
}
