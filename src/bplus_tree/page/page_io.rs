//! B+ 页读取、双写批次和同步故障封锁。

use super::*;

impl PageManager {
    pub(super) fn page_is_live(&self, page_id: PageId) -> bool {
        page_id.0 > META_PAGE_ID
            && page_id.0 < self.next_page_id
            && !self.free_set.contains(&page_id.0)
    }

    /// 读磁盘页 → 校验 CRC → 返回 payload
    pub(super) fn read_disk_payload(&mut self, page_id: PageId) -> std::io::Result<Vec<u8>> {
        let raw = self.file.read_page(page_id.0)?;
        match unpack_page(&raw) {
            Ok((_id, payload)) => Ok(payload),
            Err(e) => {
                // 尝试从 dblwr 恢复单页
                let batch = self.dblwr.read_batch().unwrap_or_default();
                if let Some((_, raw)) = batch.into_iter().find(|(id, _)| *id == page_id.0) {
                    if let Ok((_id, payload)) = unpack_page(&raw) {
                        // 修好主文件
                        let _ = self.file.write_page(page_id.0, &raw);
                        return Ok(payload);
                    }
                }
                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("读页 {} CRC 失败: {e}", page_id.0),
                ))
            }
        }
    }

    /// 把一批脏页写到主文件。
    /// - `do_sync=true`：写完 fsync（checkpoint / 非 bulk 换出）
    /// - bulk 模式：跳过双写，仅 write；是否 sync 由调用方决定
    pub(super) fn flush_pages_batch(
        &mut self,
        page_ids: &[PageId],
        do_sync: bool,
    ) -> std::io::Result<()> {
        self.ensure_healthy()?;
        let result = self.flush_pages_batch_inner(page_ids, do_sync);
        if let Err(error) = &result {
            self.io_state = PageIoState::Failed {
                kind: error.kind(),
                message: error.to_string(),
            };
        }
        result
    }

    pub(crate) fn ensure_healthy(&self) -> std::io::Result<()> {
        match &self.io_state {
            PageIoState::Healthy => Ok(()),
            PageIoState::Failed { kind, message } => Err(std::io::Error::new(
                *kind,
                format!("B+ page I/O failed; close and reopen for recovery: {message}"),
            )),
        }
    }

    pub(crate) fn mark_maintenance_failed(&mut self, error: &std::io::Error) {
        self.io_state = PageIoState::Failed {
            kind: error.kind(),
            message: error.to_string(),
        };
    }
    /// 先将脏页与必要元数据打包，再依次发布双写区、主文件和同步结果。
    /// 普通写回只有整次发布成功后才能标记干净；bulk 的延后同步状态要在最终完成时补齐。

    pub(super) fn flush_pages_batch_inner(
        &mut self,
        page_ids: &[PageId],
        do_sync: bool,
    ) -> std::io::Result<()> {
        if page_ids.is_empty()
            && !self.meta_dirty
            && !self.freelist_dirty
            && !self.bulk_pending_sync
        {
            return Ok(());
        }

        #[cfg(test)]
        let pack_timer = crate::common::maintenance_probe::Timer::start(
            crate::common::maintenance_probe::Stage::PagePack,
        );
        let mut batch: Vec<(u64, Vec<u8>)> = Vec::with_capacity(page_ids.len() + 1);
        for &pid in page_ids {
            if let Some(entry) = self.cache.get(&pid) {
                if entry.state == PageState::Dirty {
                    batch.push((pid.0, pack_page(pid.0, &entry.data)));
                }
            }
        }
        if self.meta_dirty {
            let payload = self.build_meta_payload();
            batch.push((META_PAGE_ID, pack_page(META_PAGE_ID, &payload)));
        }
        #[cfg(test)]
        drop(pack_timer);
        if self.bulk_mode {
            // 快速路径：直接写主文件，不做双写；sync 可选
            if !batch.is_empty() {
                self.bulk_pending_sync = true;
                #[cfg(test)]
                let _timer = crate::common::maintenance_probe::Timer::start(
                    crate::common::maintenance_probe::Stage::PageWrite,
                );
                #[cfg(test)]
                crate::bplus_tree::storage::dblwr_probe::fail_at(4)?;
                for (pid, raw) in &batch {
                    self.file.write_page(*pid, raw)?;
                }
            }
        } else {
            for chunk in batch.chunks(crate::bplus_tree::storage::DBLWR_MAX_PAGES) {
                self.dblwr.write_batch(chunk)?;
                {
                    #[cfg(test)]
                    let _timer = crate::common::maintenance_probe::Timer::start(
                        crate::common::maintenance_probe::Stage::PageWrite,
                    );
                    #[cfg(test)]
                    crate::bplus_tree::storage::dblwr_probe::fail_at(4)?;
                    for (pid, raw) in chunk {
                        self.file.write_page(*pid, raw)?;
                    }
                }
                if do_sync {
                    #[cfg(test)]
                    let _timer = crate::common::maintenance_probe::Timer::start(
                        crate::common::maintenance_probe::Stage::PageSync,
                    );
                    #[cfg(test)]
                    crate::bplus_tree::storage::dblwr_probe::fail_at(5)?;
                    self.file.sync_at(SITE_BPLUS_PAGE_FLUSH)?;
                    self.bulk_pending_sync = false;
                }
                self.dblwr.clear()?;
            }
        }
        // 还需覆盖 bulk 脏页已全部换出，以及调用方在最终刷盘前退出 bulk 模式的情况。
        if do_sync && self.bulk_pending_sync {
            #[cfg(test)]
            let _timer = crate::common::maintenance_probe::Timer::start(
                crate::common::maintenance_probe::Stage::PageSync,
            );
            #[cfg(test)]
            crate::bplus_tree::storage::dblwr_probe::fail_at(5)?;
            self.file.sync_at(SITE_BPLUS_PAGE_FLUSH)?;
            self.bulk_pending_sync = false;
        }

        // 整次发布成功后才能标记干净。
        let defer = self.reclamation != ReclamationState::Idle;
        #[cfg(test)]
        let defer = defer && !reclamation_reference::enabled();
        if !defer && (!self.bulk_mode || do_sync) {
            self.persist_freelist()?;
        }
        for &pid in page_ids {
            if let Some(entry) = self.cache.get_mut(&pid) {
                if entry.state == PageState::Dirty {
                    entry.state = PageState::Clean;
                }
            }
        }
        self.meta_dirty = false;
        Ok(())
    }

    pub(super) fn flush_pages_via_dblwr(&mut self, page_ids: &[PageId]) -> std::io::Result<()> {
        self.flush_pages_batch(page_ids, true)
    }

    /// 读页 payload
    pub fn read_page(&mut self, page_id: PageId) -> Option<Vec<u8>> {
        self.read_page_project(page_id, <[u8]>::to_vec)
    }

    /// 在借用有效缓存载荷期间做投影，解码器可以直接构建拥有型结果，不先复制整页原始字节。
    pub(crate) fn read_page_project<T>(
        &mut self,
        page_id: PageId,
        project: impl FnOnce(&[u8]) -> T,
    ) -> Option<T> {
        // 测试专用：页访问次数（含页缓存命中）—— 用来断言点操作的代价不随版本链长度增长
        #[cfg(test)]
        PAGE_READS.with(|c| c.set(c.get() + 1));
        #[cfg(test)]
        note_phase_read();
        if !self.page_is_live(page_id) {
            return None;
        }
        self.clock += 1;

        if self.cache.contains_key(&page_id) {
            #[cfg(test)]
            CACHE_HITS.with(|c| c.set(c.get() + 1));
            self.update_lru(page_id);
            let entry = self.cache.get_mut(&page_id).unwrap();
            entry.last_access = self.clock;
            return Some(project(&entry.data));
        }

        #[cfg(test)]
        CACHE_MISSES.with(|c| c.set(c.get() + 1));
        let payload = match self.read_disk_payload(page_id) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("读盘失败 page={}: {}", page_id.0, e);
                return None;
            }
        };
        self.evict_if_needed();
        // 读取仍可用于检查，但不扩张已经失败的缓存。
        if self.ensure_healthy().is_err() {
            return Some(project(&payload));
        }
        self.cache.insert(
            page_id,
            CacheEntry {
                page_id,
                data: payload,
                state: PageState::Clean,
                last_access: self.clock,
            },
        );
        self.lru_queue.push_back(page_id);
        Some(project(&self.cache.get(&page_id).unwrap().data))
    }

    /// 只为**一次性走查**准备的读页：命中缓存就用，未命中时**只有在还装得下时才回填**。
    ///
    /// 为什么不能直接用 [`Self::read_page`]：全库走查（scan / prefix_scan / health /
    /// search / vacuum 的键折叠）会把每一页都摸一次。当页数超过 `max_cache_pages`
    /// （实测 20000 行 ≈ 1257 页 > 默认 1024），这趟走查就变成"逐页换出"——
    /// 刚被点读焐热的叶子全被挤掉，还可能顺带把脏页 flush 出去
    /// （实测一次 `health()` 换出 1600 页、写盘 765 页，150 ms；第二遍命中数只有 1）。
    /// 走查对每一页只碰一次，回填它并不会有下一次命中，所以这里选择**装得下才缓存**：
    /// 小库依然全部命中，大库不再毁掉 OLTP 的工作集。
    pub fn read_page_for_walk(&mut self, page_id: PageId) -> Option<Vec<u8>> {
        #[cfg(test)]
        PAGE_READS.with(|c| c.set(c.get() + 1));
        #[cfg(test)]
        note_phase_read();
        if !self.page_is_live(page_id) {
            return None;
        }
        self.clock += 1;

        if self.cache.contains_key(&page_id) {
            #[cfg(test)]
            CACHE_HITS.with(|c| c.set(c.get() + 1));
            self.update_lru(page_id);
            let entry = self.cache.get_mut(&page_id).unwrap();
            entry.last_access = self.clock;
            return Some(entry.data.clone());
        }

        #[cfg(test)]
        CACHE_MISSES.with(|c| c.set(c.get() + 1));
        let payload = match self.read_disk_payload(page_id) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("读盘失败 page={}: {}", page_id.0, e);
                return None;
            }
        };
        // 关键区别：满了就不回填，宁可不缓存也不把热页挤出去
        if self.cache.len() < self.max_cache_pages {
            self.cache.insert(
                page_id,
                CacheEntry {
                    page_id,
                    data: payload.clone(),
                    state: PageState::Clean,
                    last_access: self.clock,
                },
            );
            self.lru_queue.push_back(page_id);
        }
        Some(payload)
    }

    /// 维护读取必须区分页面不可读与扫描结束。有效脏缓存仍权威；冷读不填充缓存，并在整个扫描中复用调用方的一页缓冲。
    pub(crate) fn read_maintenance_page<'a>(
        &mut self,
        page_id: PageId,
        raw: &'a mut [u8; PAGE_SIZE],
    ) -> std::io::Result<&'a [u8]> {
        self.ensure_healthy()?;
        #[cfg(test)]
        {
            PAGE_READS.with(|c| c.set(c.get() + 1));
            note_phase_read();
        }
        if !self.page_is_live(page_id) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "maintenance reached a non-live page",
            ));
        }
        self.clock += 1;
        if self.cache.contains_key(&page_id) {
            #[cfg(test)]
            CACHE_HITS.with(|c| c.set(c.get() + 1));
            self.update_lru(page_id);
            let entry = self.cache.get_mut(&page_id).unwrap();
            entry.last_access = self.clock;
            raw[..MAX_PAYLOAD].copy_from_slice(&entry.data);
            return Ok(&raw[..MAX_PAYLOAD]);
        }
        #[cfg(test)]
        CACHE_MISSES.with(|c| c.set(c.get() + 1));
        self.file.read_page_into(page_id.0, raw)?;
        if crate::bplus_tree::storage::maintenance_payload(raw, page_id.0).is_err() {
            // 有效且已发布的双写页可修复撕裂的主页面；恢复写入成功后才能继续维护。
            let replacement = self.dblwr.read_batch()?.into_iter().find(|(id, bytes)| {
                *id == page_id.0
                    && crate::bplus_tree::storage::maintenance_payload(bytes, *id).is_ok()
            });
            if let Some((_, bytes)) = replacement {
                self.file.write_page(page_id.0, &bytes)?;
                raw.copy_from_slice(&bytes);
            }
        }
        crate::bplus_tree::storage::maintenance_payload(raw, page_id.0)
    }

    /// 写页 payload（标记 Dirty，真正落盘在 flush / 换出时）
    pub fn write_page(&mut self, page_id: PageId, data: &[u8]) -> bool {
        if data.len() > PAGE_PAYLOAD_SIZE {
            // 超载写必须**很少见到**：调用方（借键/合并/插入分裂）都要先量过字节再动盘。
            // 这里保留计数 + 首次告警：以前每条都 eprintln，真实形状（40B 键 + 60B 值）下
            // 一次 vacuum 能刷出几万行，把被测的耗时整个污染掉（台架复测时 bplus 的
            // `search 首页` 911ms 里就有它）。
            OVERSIZED_WRITES.with(|c| c.set(c.get() + 1));
            if OVERSIZED_WRITES.with(|c| c.get()) <= 3 {
                eprintln!(
                    "数据超过页载荷限制 {} > {}（同类告警只报前 3 条，其余计数）",
                    data.len(),
                    PAGE_PAYLOAD_SIZE
                );
            }
            return false;
        }
        if !self.page_is_live(page_id) {
            eprintln!("页 {} 不存在", page_id.0);
            return false;
        }
        self.clock += 1;

        if !self.cache.contains_key(&page_id) {
            self.evict_if_needed_for_insert();
            let disk_data = self
                .read_disk_payload(page_id)
                .unwrap_or_else(|_| vec![0u8; PAGE_PAYLOAD_SIZE]);
            self.cache.insert(
                page_id,
                CacheEntry {
                    page_id,
                    data: disk_data,
                    state: PageState::Clean,
                    last_access: self.clock,
                },
            );
            self.lru_queue.push_back(page_id);
        }

        let entry = self.cache.get_mut(&page_id).unwrap();
        entry.data.clear();
        entry.data.extend_from_slice(data);
        entry.data.resize(PAGE_PAYLOAD_SIZE, 0);
        entry.state = PageState::Dirty;
        entry.last_access = self.clock;
        // 内容变了 ⇒ 上层那份解析过的节点作废（按页计数，见 `page_writes`）
        *self.page_writes.entry(page_id).or_insert(0) += 1;
        self.update_lru(page_id);
        true
    }

    /// 刷单个脏页
    pub fn flush_page(&mut self, page_id: PageId) -> bool {
        if self.ensure_healthy().is_err() {
            return false;
        }
        let is_dirty = self
            .cache
            .get(&page_id)
            .map(|e| e.state == PageState::Dirty)
            .unwrap_or(false);
        if !is_dirty && !self.meta_dirty && !self.freelist_dirty {
            return true;
        }
        if let Err(e) = self.flush_pages_via_dblwr(&[page_id]) {
            eprintln!("flush_page 失败: {e}");
            return false;
        }
        true
    }

    /// 刷全部脏页 + meta + freelist。
    pub fn flush_all(&mut self) {
        if let Err(e) = self.try_flush_all() {
            eprintln!("flush_all 失败: {e}");
        }
    }

    pub fn try_flush_all(&mut self) -> std::io::Result<()> {
        let dirty: Vec<PageId> = self
            .cache
            .iter()
            .filter(|(_, e)| e.state == PageState::Dirty)
            .map(|(id, _)| *id)
            .collect();
        // 批次负责数据、元数据、空闲清单的发布及故障封锁。
        self.flush_pages_batch(&dirty, true)?;
        if let Err(error) = self.persist_freelist() {
            self.mark_maintenance_failed(&error);
            return Err(error);
        }
        Ok(())
    }
}
