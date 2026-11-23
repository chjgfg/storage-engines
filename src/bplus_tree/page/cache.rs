//! 本文件负责缓存预算、钉住与淘汰。

use super::*;

impl PageManager {
    /// 某页已被写入多少次（上层解析缓存的校验凭据）
    pub fn write_count(&self, page_id: PageId) -> u64 {
        self.page_writes.get(&page_id).copied().unwrap_or(0)
    }

    /// 页缓存容量（上层用它给"解析缓存"定同样的上限）
    pub fn cache_capacity(&self) -> usize {
        self.max_cache_pages
    }

    /// 自动档改容量用的入口：只动上限，不动内容。
    ///
    /// 调大不需要立刻做什么（下一次回填自然装得下）；调小也不需要主动换出 ——
    /// 换出是在插入时按批做的，这里只把"什么时候算满"的判断改掉。
    pub fn set_cache_capacity(&mut self, pages: usize) {
        self.max_cache_pages = pages.max(1);
    }

    /// 钉住预算的**生效上限** = 调用方给的预算 与 **容量的一半** 取小。
    ///
    /// ⚠️ 为什么必须跟容量挂钩，而不是只钉一个绝对数：上层页占全库多少**不是常数**，
    /// 它由"一页装得下几个键"决定 —— 实测同一份五千行的库，35 B 键时上层占 3.0%
    /// （36 页 / 1210 页），而 900 B 键时占 **33%**（1246 页 / 3746 页：内部节点也塞不下几个
    /// 长键 ⇒ 扇出掉到 4 以下、树深从 3 层涨到 8 层）。只按绝对数钉，碰上后者就会把整个缓存
    /// 钉成"一动不动"，叶子一页都装不下 —— 那比不钉还糟。留一半给叶子是这条策略的底线
    /// （守卫里 `pinned × 2 ≤ 容量` 钉的就是它）。
    ///
    /// ⚠️ 这半边底线**不跟调用方商量**：`set_pin_budget` 只允许把预算**往小**调的效果，
    /// 传一个比容量一半还大的数不会突破那条线（否则"抬顶"与"调钉住预算"两个旋钮合起来
    /// 就能把整份缓存钉死，那正是上面那句话要避免的事故）。
    pub(super) fn pin_limit(&self) -> usize {
        self.pin_budget.min((self.max_cache_pages / 2).max(1))
    }

    /// 设钉住预算（页数为单位）。`0` = 一页都不钉（叶子拿到全部容量）。
    ///
    /// ⚠️ **调小会把已经钉住的页全部放开**（页还在缓存里，只是重新参与换出）：
    /// 不然"把预算调小"这一动作对一份已经焐热的缓存**完全没有效果** —— 已经钉着的那 1024 页
    /// 会继续占着容量，叶子一页都进不来，而这个旋钮存在的理由就是"让叶子住满"。
    /// 放开之后下一次下降会按新预算重新钉几页，代价是那一轮多读几页上层（有界的）。
    pub fn set_pin_budget(&mut self, pages: usize) {
        self.pin_budget = pages;
        if self.pinned.len() > self.pin_limit() {
            self.pinned.clear();
        }
    }

    /// 调用方给的那一档（不是生效值；生效值还要与容量一半取小，见 `Self::pin_limit`）
    pub fn pin_budget(&self) -> usize {
        self.pin_budget
    }

    /// 生效的钉住上限（`tuning()` 报的是这个 —— 与 M22/M32"报生效值"的口径一致）
    pub fn effective_pin_limit(&self) -> usize {
        self.pin_limit()
    }

    /// 这页被钉住了吗（上层下降路上那几个节点）
    pub fn is_pinned(&self, page_id: PageId) -> bool {
        self.pinned.contains(&page_id.0)
    }

    /// 钉住一页，让它不参与换出。**必须在页已经在缓存里之后调用**（否则钉住的是空气）。
    ///
    /// 预算满了就**不再新增**（宁可不钉，也不能让"自动档"悄悄吃内存）。
    pub fn pin(&mut self, page_id: PageId) -> bool {
        if self.pinned.contains(&page_id.0) || !self.cache.contains_key(&page_id) {
            return self.pinned.contains(&page_id.0);
        }
        if self.pinned.len() >= self.pin_limit() {
            return false;
        }
        self.pinned.insert(page_id.0);
        true
    }

    /// 当前钉住了多少页（守卫与 `tuning()` 读回用）
    pub fn pinned_pages(&self) -> usize {
        self.pinned.len()
    }
}

impl PageManager {
    pub(super) fn evict_if_needed_for_insert(&mut self) {
        self.evict_batch_until_room();
    }

    pub(super) fn evict_if_needed(&mut self) {
        self.evict_batch_until_room();
    }
    /// 保持原缓存预算与 LRU 次序，用固定容量批次腾出缓存槽。
    /// 回收时可用双写余位写回其它驻留脏页，但这些额外页不换出；元数据槽和同步故障封锁仍保留。

    /// 缓存满时一次换出多页：脏页凑一批再写盘，bulk 下不 fsync
    pub(super) fn evict_batch_until_room(&mut self) {
        // 正在执行的树操作必须完成内存结构调整；先保留全部脏页，再由树或事务报告失败。后续操作在入口处被拒绝。
        if !matches!(self.io_state, PageIoState::Healthy) {
            return;
        }
        if self.cache.len() < self.max_cache_pages {
            return;
        }
        // 目标：腾出至少 1/4 缓存或 EVICT_BATCH_TARGET 页
        let target_free = (self.max_cache_pages / 4).max(1).min(EVICT_BATCH_TARGET);
        let unique_victims = true;
        #[cfg(test)]
        let unique_victims = unique_victims && !eviction_reference::enabled();
        let reserve_meta = !self.bulk_mode && self.meta_dirty;
        #[cfg(test)]
        let reserve_meta = reserve_meta && !eviction_reference::enabled();
        let dirty_limit = crate::bplus_tree::storage::DBLWR_MAX_PAGES - usize::from(reserve_meta);
        let mut victims: Vec<PageId> = Vec::with_capacity(target_free);
        let mut dirty_victims: Vec<PageId> = Vec::new();
        let mut selected = HashSet::with_capacity(target_free);

        while victims.len() < target_free {
            let Some(pid) = self.pop_lru_valid() else {
                break;
            };
            // 近似 LRU 队列可能重复记录页面访问；受害页只计数和刷出一次，发布完成前仍留在缓存。
            if unique_victims && !selected.insert(pid) {
                continue;
            }
            if self
                .cache
                .get(&pid)
                .map(|e| e.state == PageState::Dirty)
                .unwrap_or(false)
            {
                // 元数据共用双写容量，第65页会导致第二次数据同步，应排队留给下一批。
                if dirty_victims.len() == dirty_limit {
                    self.lru_queue.push_front(pid);
                    break;
                }
                dirty_victims.push(pid);
            }
            victims.push(pid);
        }

        if !dirty_victims.is_empty() {
            // 回收只触及少量脏页，其余缓存多为干净页。利用双写余位写回其它驻留脏页，然后支付原有发布屏障；额外页仍驻留，LRU 淘汰和预算不变。
            let mut flush = dirty_victims;
            let coalesce = self.reclamation != ReclamationState::Idle && !self.bulk_mode;
            #[cfg(test)]
            let coalesce = coalesce && !reclamation_batch_reference::enabled();
            if coalesce {
                let mut extra: Vec<PageId> = self
                    .cache
                    .iter()
                    .filter(|(pid, entry)| {
                        entry.state == PageState::Dirty && !selected.contains(pid)
                    })
                    .map(|(pid, _)| *pid)
                    .collect();
                extra.sort_unstable_by_key(|pid| pid.0);
                flush.extend(
                    extra
                        .into_iter()
                        .take(dirty_limit.saturating_sub(flush.len())),
                );
            }
            // bulk：只 write 不 sync；正常：批量双写 + 一次 sync
            let sync = !self.bulk_mode;
            #[cfg(test)]
            BATCH_FLUSHED_PAGES.with(|c| c.set(c.get() + flush.len() as u64));
            if self.flush_pages_batch(&flush, sync).is_err() {
                self.lru_queue.extend(victims);
                return;
            }
        }
        #[cfg(test)]
        PAGE_EVICTIONS.with(|c| c.set(c.get() + victims.len() as u64));
        for pid in victims {
            self.cache.remove(&pid);
        }
    }

    /// 从队首弹出仍在 cache 中**且没被钉住**的页（跳过已释放/已换出的陈旧 id 与上层页）
    pub(super) fn pop_lru_valid(&mut self) -> Option<PageId> {
        while let Some(pid) = self.lru_queue.pop_front() {
            if !self.cache.contains_key(&pid) {
                continue;
            }
            // 钉住的页不参与换出。它在队列里的这一项直接丢掉：以后它也不再是需要被挑出去的候选
            if self.pinned.contains(&pid.0) {
                continue;
            }
            return Some(pid);
        }
        None
    }

    /// O(1) 近似 LRU：只 push，不 retain；陈旧项在换出时跳过
    pub(super) fn update_lru(&mut self, page_id: PageId) {
        self.lru_queue.push_back(page_id);
        // 防止队列无限膨胀：偶尔压缩
        if self.lru_queue.len() > self.max_cache_pages * 4 {
            self.compact_lru();
        }
    }

    pub(super) fn compact_lru(&mut self) {
        #[cfg(test)]
        LRU_COMPACTIONS.with(|c| c.set(c.get() + 1));
        let mut seen = HashMap::with_capacity(self.cache.len());
        let mut new_q = VecDeque::with_capacity(self.cache.len());
        // 从队尾往队首扫，保留每个 page 最后一次出现（最近）
        for pid in self.lru_queue.drain(..).rev() {
            if self.cache.contains_key(&pid) && !seen.contains_key(&pid) {
                seen.insert(pid, ());
                new_q.push_front(pid);
            }
        }
        self.lru_queue = new_q;
    }
}
