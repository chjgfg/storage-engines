//! B+ 页分配、释放和使用统计。

use super::*;

impl PageManager {
    /// 分配新页
    ///
    /// 优先复用 freelist；只进缓存并标 Dirty，**不**立刻 fsync。
    pub fn allocate_page(&mut self) -> PageId {
        self.clock += 1;
        self.evict_if_needed_for_insert();

        let empty = vec![0u8; PAGE_PAYLOAD_SIZE];
        let reusable = if self.reclamation == ReclamationState::Idle {
            self.free_page_ids.pop_front()
        } else {
            None
        };
        let page_id = if let Some(page_id) = reusable {
            self.free_set.remove(&page_id.0);
            self.freelist_dirty = true;
            self.meta_dirty = true;
            page_id
        } else {
            let id = PageId(self.next_page_id);
            self.next_page_id += 1;
            self.meta_dirty = true;
            id
        };

        self.cache.insert(
            page_id,
            CacheEntry {
                page_id,
                data: empty,
                state: PageState::Dirty,
                last_access: self.clock,
            },
        );
        // 复用空闲页也算"内容换了"：上层那份按旧内容解析出来的节点必须作废
        *self.page_writes.entry(page_id).or_insert(0) += 1;
        self.lru_queue.push_back(page_id);
        page_id
    }

    /// 释放页：归还 freelist（文件不收缩，槽位可复用）
    pub fn free_page(&mut self, page_id: PageId) -> bool {
        if !self.page_is_live(page_id) {
            return false;
        }
        self.cache.remove(&page_id);
        // ⚠️ 钉住的账必须一起退：页号会被复用，一个"曾是内部节点"的页号将来可能装叶子。
        // 漏了这一步的后果不是读错数据（解析缓存按 `page_writes` 校验，复用会 bump），
        // 而是**针预算被死页占满** —— 上层再也钉不进去，等于悄悄退回修复前的形状。
        self.pinned.remove(&page_id.0);
        if self.free_set.insert(page_id.0) {
            self.free_page_ids.push_back(page_id);
            self.freelist_dirty = true;
            if self.reclamation != ReclamationState::Idle {
                self.reclamation = ReclamationState::UnpublishedFrees;
            }
            self.meta_dirty = true;
        }
        true
    }

    pub fn get_page_state(&mut self, page_id: PageId) -> Option<PageState> {
        self.cache.get(&page_id).map(|e| e.state)
    }

    /// 存储统计（监控 / 调试）
    pub fn stats(&self) -> StorageStats {
        StorageStats {
            next_page_id: self.next_page_id,
            free_pages: self.free_page_ids.len(),
            cache_pages: self.cache.len(),
            max_cache_pages: self.max_cache_pages,
            root_page_id: self.root_page_id.map(|p| p.0),
            bulk_mode: self.bulk_mode,
            file_path: self.path.display().to_string(),
        }
    }
}
