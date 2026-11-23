//! B+ 元数据、双写恢复和回收发布。

use super::*;

impl PageManager {
    /// 崩溃恢复：用 dblwr 中的完整页修复主文件
    pub(super) fn recover_from_dblwr(
        file: &mut DiskFile,
        dblwr: &mut DoubleWriteBuffer,
    ) -> std::io::Result<()> {
        let batch = dblwr.read_batch()?;
        if batch.is_empty() {
            return Ok(());
        }
        for (page_id, raw) in &batch {
            // 主文件对应页若 CRC 损坏或与 dblwr 不一致 → 用 dblwr 覆盖
            let main = file.read_page(*page_id)?;
            let main_ok = unpack_page(&main).is_ok();
            if !main_ok || main != *raw {
                file.write_page(*page_id, raw)?;
            }
        }
        file.sync_at(SITE_BPLUS_DBLWR)?;
        dblwr.clear()?;
        Ok(())
    }

    pub(super) fn load_meta(&mut self) -> std::io::Result<()> {
        let raw = self.file.read_page(META_PAGE_ID)?;
        let (_id, payload) = unpack_page(&raw).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, format!("meta 损坏: {e}"))
        })?;
        if &payload[META_OFF_MAGIC..META_OFF_MAGIC + 8] != MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("不是合法的 bplus 数据文件: {:?}", self.path),
            ));
        }
        self.next_page_id = u64::from_le_bytes(
            payload[META_OFF_NEXT..META_OFF_NEXT + 8]
                .try_into()
                .unwrap(),
        );
        let root = u64::from_le_bytes(
            payload[META_OFF_ROOT..META_OFF_ROOT + 8]
                .try_into()
                .unwrap(),
        );
        self.root_page_id = if root == 0 { None } else { Some(PageId(root)) };
        // next_version：有 "NVER" 魔数才读（兼容旧库 offset 32 起是 free 列表）
        self.next_version = if payload.len() >= META_OFF_NEXT_VERSION + 8
            && &payload[META_OFF_NVER_MAGIC..META_OFF_NVER_MAGIC + 4] == NVER_MAGIC
        {
            Some(u64::from_le_bytes(
                payload[META_OFF_NEXT_VERSION..META_OFF_NEXT_VERSION + 8]
                    .try_into()
                    .unwrap(),
            ))
        } else {
            None
        };
        // free 列表不再从 meta 内嵌字段加载（见 load_freelist）
        self.meta_dirty = false;
        Ok(())
    }

    /// 从 `{db}.freelist` 加载完整空闲页；若无 sidecar 则尝试从旧 meta 内嵌列表迁移
    pub(super) fn load_freelist(&mut self) -> std::io::Result<()> {
        self.free_page_ids.clear();
        self.free_set.clear();
        let fl_path = freelist_path(&self.path);

        if let Some(ids) = read_freelist(&fl_path)? {
            for id in ids {
                if id > META_PAGE_ID && id < self.next_page_id && self.free_set.insert(id) {
                    self.free_page_ids.push_back(PageId(id));
                }
            }
            self.freelist_dirty = false;
            return Ok(());
        }

        // 兼容旧库：meta payload offset 32 起可能有内嵌 free 列表
        if let Ok(raw) = self.file.read_page(META_PAGE_ID) {
            if let Ok((_id, payload)) = unpack_page(&raw) {
                let free_cnt = u64::from_le_bytes(
                    payload[META_OFF_FREE_CNT..META_OFF_FREE_CNT + 8]
                        .try_into()
                        .unwrap(),
                ) as usize;
                let max_old = (PAGE_PAYLOAD_SIZE - 32) / 8;
                let free_cnt = free_cnt.min(max_old);
                for i in 0..free_cnt {
                    let off = 32 + i * 8;
                    if off + 8 > payload.len() {
                        break;
                    }
                    let id = u64::from_le_bytes(payload[off..off + 8].try_into().unwrap());
                    if id > META_PAGE_ID && id < self.next_page_id && self.free_set.insert(id) {
                        self.free_page_ids.push_back(PageId(id));
                    }
                }
            }
        }

        // 若从旧 meta 迁出了 free 页，立刻写出 sidecar
        if !self.free_page_ids.is_empty() {
            self.freelist_dirty = true;
            self.persist_freelist()?;
        } else {
            self.freelist_dirty = false;
        }
        Ok(())
    }

    pub(super) fn build_meta_payload(&self) -> Vec<u8> {
        let mut payload = vec![0u8; PAGE_PAYLOAD_SIZE];
        payload[META_OFF_MAGIC..META_OFF_MAGIC + 8].copy_from_slice(MAGIC);
        payload[META_OFF_NEXT..META_OFF_NEXT + 8].copy_from_slice(&self.next_page_id.to_le_bytes());
        // free_count 仅作提示（监控用），权威在 freelist 文件
        payload[META_OFF_FREE_CNT..META_OFF_FREE_CNT + 8]
            .copy_from_slice(&(self.free_page_ids.len() as u64).to_le_bytes());
        let root = self.root_page_id.map(|p| p.0).unwrap_or(0);
        payload[META_OFF_ROOT..META_OFF_ROOT + 8].copy_from_slice(&root.to_le_bytes());
        // 仅在明确 set 过 next_version 后写 NVER，避免旧库被误写成 0 而跳过全树扫描
        if let Some(nv) = self.next_version {
            payload[META_OFF_NVER_MAGIC..META_OFF_NVER_MAGIC + 4].copy_from_slice(NVER_MAGIC);
            payload[META_OFF_NEXT_VERSION..META_OFF_NEXT_VERSION + 8]
                .copy_from_slice(&nv.to_le_bytes());
        }
        payload
    }
    /// 进入空闲页延迟发布范围，防止本轮尚未持久发布的释放页提前复用。
    /// 作用域状态决定结束时是否必须发布清单；不能把发布失败当作正常空回收。

    pub(crate) fn begin_reclamation(&mut self) -> std::io::Result<ReclamationPublication> {
        self.ensure_healthy()?;
        if self.reclamation != ReclamationState::Idle {
            return Err(std::io::Error::other(
                "reclamation publication is already active",
            ));
        }
        if self.bulk_mode {
            return Ok(ReclamationPublication::ExistingBulk);
        }
        self.reclamation = ReclamationState::Scanning;
        Ok(ReclamationPublication::DeferredFreelist)
    }
    /// 在回收范围结束时发布积累的空闲页状态。
    /// 只有发布成功才能恢复普通复用；失败保留脏状态并封锁后续维护。

    pub(crate) fn finish_reclamation(&mut self) -> std::io::Result<()> {
        self.ensure_healthy()?;
        if self.reclamation != ReclamationState::Scanning {
            let error = std::io::Error::other("reclamation returned with unpublished free pages");
            self.mark_maintenance_failed(&error);
            return Err(error);
        }
        self.reclamation = ReclamationState::Idle;
        Ok(())
    }

    pub(super) fn persist_freelist(&mut self) -> std::io::Result<()> {
        if !self.freelist_dirty {
            return Ok(());
        }
        #[cfg(test)]
        let _timer = crate::common::maintenance_probe::Timer::start(
            crate::common::maintenance_probe::Stage::Freelist,
        );
        if self.freelist_writer.is_none() {
            self.freelist_writer = Some(FreelistWriter::open(&freelist_path(&self.path))?);
        }
        self.freelist_writer
            .as_mut()
            .unwrap()
            .write(self.free_page_ids.iter().map(|p| p.0))?;
        self.freelist_dirty = false;
        if self.reclamation != ReclamationState::Idle {
            self.reclamation = ReclamationState::Scanning;
        }
        Ok(())
    }

    /// 写 meta（bulk 下直接写+可选 sync；正常路径走双写）
    pub(super) fn persist_meta_via_dblwr(&mut self) -> std::io::Result<()> {
        let payload = self.build_meta_payload();
        let packed = pack_page(META_PAGE_ID, &payload);
        if self.bulk_mode {
            self.file.write_page(META_PAGE_ID, &packed)?;
            self.file.sync_at(SITE_BPLUS_META)?;
        } else {
            self.dblwr.write_batch(&[(META_PAGE_ID, packed.clone())])?;
            self.file.write_page(META_PAGE_ID, &packed)?;
            self.file.sync_at(SITE_BPLUS_META)?;
            self.dblwr.clear()?;
        }
        self.meta_dirty = false;
        Ok(())
    }
}
