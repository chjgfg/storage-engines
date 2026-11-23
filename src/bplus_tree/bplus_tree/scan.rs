//! 本文件负责顺序、反向与维护扫描。

use super::*;

impl BPlusTree {
    /// 范围查询：[low, high]
    pub fn range_scan(&mut self, low: Key, high: Key) -> Vec<(Key, RowId)> {
        let mut result = Vec::new();
        let mut current_leaf_id = self.find_leaf_for_key(self.root, low.clone());
        while let Some(node_id) = current_leaf_id {
            // 读取叶子结点
            let node_data = self.page_manager.read_page(node_id).unwrap();
            let node: BTreeNode = bincode::deserialize(&node_data).unwrap();
            if let BTreeNode::Leaf { entries, next_leaf } = node {
                // 遍历当前叶子节点的entries
                for entry in &entries {
                    let (k, rid) = entry;
                    if k > &high {
                        // 超出上限，直接返回
                        return result;
                    }
                    if k >= &low {
                        result.push((k.clone(), rid.clone()));
                    }
                }

                // 移动到下一个叶子节点
                current_leaf_id = next_leaf;
            } else {
                // 不是叶子节点，终止
                break;
            }
        }
        result
    }

    /// 找到包含指定key的叶子节点ID
    /// 在闭区间 `[low, high]` 内**从大到小**找第一个被 `accept` 接受的行。
    ///
    /// 版本键 `raw_key ‖ version.to_be_bytes()` 大端有序 ⇒ 同一逻辑 key 的最新版本排在
    /// 区间末尾，所以「从高界往回走、第一个满足条件的就停」通常一次下降就命中，
    /// 代价只和树高有关、和链长无关。
    ///
    /// 本树的叶子之间只有单向 `next_leaf`，没有 `prev_leaf`，所以「再往前一片」不是走链表
    /// 而是**重新下降**：取本叶最小键 `m`，用它的真前缀 `m[..m.len()-1]`（一定小于 `m`，
    /// enc 键至少 8 字节所以前缀必然存在）作新上界。但「小于 m」并不保证落到更左的叶子——
    /// 已经走到最左叶子时会又回到同一片，所以额外用 `prev_leaf_min` 做**进度守卫**：
    /// 下一跳的叶最小键若不严格小于上一跳的，就说明没有更左的叶子了，直接收尾。
    /// 宁可少走，也不能原地打转。
    ///
    /// 扫描中一旦遇到 `k < low` 就返回 `None`：叶子按键升序，再往左全部落在区间之外。
    pub fn find_back(
        &mut self,
        low: &[u8],
        high: &[u8],
        mut accept: impl FnMut(&[u8]) -> bool,
    ) -> Option<(Key, RowId)> {
        #[cfg(test)]
        if point_projection_reference::enabled() {
            return self.find_back_reference(low, high, accept);
        }
        let mut bound = high.to_vec();
        let mut previous: Option<Key> = None;
        loop {
            let mut page = self.root;
            let step = loop {
                let seen = self.page_manager.write_count(page);
                let cached = self
                    .node_cache
                    .get(&page)
                    .filter(|(at, _)| *at == seen)
                    .map(|(_, node)| node.clone());
                let step = if let Some(node) = cached {
                    match node {
                        CachedNode::Owned(node) => {
                            Self::point_step(&node, low, high, &bound, &mut accept)
                        }
                        CachedNode::Internal(node) => PointStep::Child(node.child(&bound)),
                    }
                } else {
                    enum Read {
                        Internal(CachedNode),
                        Leaf(PointStep),
                    }
                    // 钉住的上层节点只解析一次并在每次下降中命中，保留其更快的拥有键二分查找；只有钉住预算以外的换入换出节点需要紧凑键。
                    let owned = self.page_manager.is_pinned(page)
                        || self.page_manager.pinned_pages()
                            < self.page_manager.effective_pin_limit();
                    #[cfg(test)]
                    let owned = owned || point_internal_reference::enabled();
                    let read = self.page_manager.read_page_project(page, |payload| {
                        if payload.get(..4) == Some(1_u32.to_le_bytes().as_slice()) {
                            if owned {
                                #[cfg(test)]
                                NODE_PARSES.with(|c| c.set(c.get() + 1));
                                return bincode::deserialize(payload).ok().map(|node| {
                                    Read::Internal(CachedNode::Owned(std::sync::Arc::new(node)))
                                });
                            }
                            let node = PointInternal::decode(payload).ok()?;
                            #[cfg(test)]
                            POINT_INTERNAL_PACKS.with(|c| c.set(c.get() + 1));
                            Some(Read::Internal(CachedNode::Internal(std::sync::Arc::new(
                                node,
                            ))))
                        } else {
                            PointStep::borrowed(payload, low, high, &mut accept)
                                .ok()
                                .map(Read::Leaf)
                        }
                    })??;
                    match read {
                        Read::Leaf(step) => step,
                        Read::Internal(node) => {
                            let step = match &node {
                                CachedNode::Owned(node) => {
                                    Self::point_step(node, low, high, &bound, &mut accept)
                                }
                                CachedNode::Internal(node) => PointStep::Child(node.child(&bound)),
                            };
                            self.memoize_cached(page, seen, node);
                            step
                        }
                    }
                };
                match step {
                    PointStep::Child(child) => page = child,
                    step => break step,
                }
            };
            match step {
                PointStep::Hit(key, value) => return Some((key, value)),
                PointStep::Previous(minimum) => {
                    if minimum.len() <= 1 || previous.as_ref().is_some_and(|p| minimum >= *p) {
                        return None;
                    }
                    bound.clear();
                    bound.extend_from_slice(&minimum[..minimum.len() - 1]);
                    previous = Some(minimum);
                }
                PointStep::Missing => return None,
                PointStep::Child(_) => unreachable!(),
            }
        }
    }

    pub(super) fn point_step(
        node: &BTreeNode,
        low: &[u8],
        high: &[u8],
        descent_bound: &Key,
        accept: &mut impl FnMut(&[u8]) -> bool,
    ) -> PointStep {
        match node {
            BTreeNode::Internal { keys, children } => {
                PointStep::Child(children[Self::child_index(keys, descent_bound)])
            }
            BTreeNode::Leaf { entries, .. } => {
                let end = entries.partition_point(|(k, _)| k.as_slice() <= high);
                if end == 0 {
                    return entries
                        .first()
                        .map_or(PointStep::Missing, |(k, _)| PointStep::Previous(k.clone()));
                }
                PointStep::leaf(
                    entries[..end]
                        .iter()
                        .map(|(k, v)| (k.as_slice(), v.0.as_deref())),
                    low,
                    high,
                    accept,
                )
            }
        }
    }

    #[cfg(test)]
    pub(super) fn find_back_reference(
        &mut self,
        low: &[u8],
        high: &[u8],
        mut accept: impl FnMut(&[u8]) -> bool,
    ) -> Option<(Key, RowId)> {
        let mut bound = high.to_vec();
        // 上一跳那片叶子的最小键。每跳都必须严格小于它，否则说明下降又回到了同一片叶子
        // （典型情形：已经走到最左叶子，它下面没有叶子了）—— 此时区间内再无未考察的行。
        let mut prev_leaf_min: Option<Key> = None;
        loop {
            let leaf_id = self.find_leaf_for_key(self.root, bound.clone())?;
            // 这条也是**点读的热路径**（MVCC 取版本链尾就走它）⇒ 同样走解析缓存
            let node = self.node_of(leaf_id)?;
            let BTreeNode::Leaf { entries, .. } = node.as_ref() else {
                log::warn!("find_back: 期望叶子却拿到内部节点，停止反向游走");
                return None;
            };

            let mut leaf_min: Option<Key> = None;
            for (k, rid) in entries.iter().rev() {
                leaf_min = Some(k.clone());
                // 首跳那片叶子可能延伸到高界之外；那些行不属于本区间
                if k.as_slice() > high {
                    continue;
                }
                // 叶子按键升序：反向走到这里，再往左全部落在区间之外
                if k.as_slice() < low {
                    return None;
                }
                if accept(k.as_slice()) {
                    return Some((k.clone(), rid.clone()));
                }
            }

            // 本叶没有命中：以更小的上界重新下降到左边的叶子
            let leaf_min = leaf_min?;
            if let Some(prev) = &prev_leaf_min {
                if leaf_min.as_slice() >= prev.as_slice() {
                    // 没有左移 ⇒ 已在最左叶子，区间内的行全部考察过了
                    return None;
                }
            }
            prev_leaf_min = Some(leaf_min.clone());
            if leaf_min.len() <= 1 {
                return None;
            }
            let mut next_bound = leaf_min;
            next_bound.truncate(next_bound.len() - 1);
            bound = next_bound;
        }
    }

    pub(super) fn find_leaf_for_key(&mut self, node_page_id: PageId, key: Key) -> Option<PageId> {
        // 下降只用来定位，不改内容 ⇒ 走解析缓存（点读、反向找链尾、区间起点都靠它）
        let node = self.node_of(node_page_id)?;
        match &*node {
            BTreeNode::Leaf { .. } => Some(node_page_id),
            BTreeNode::Internal { keys, children } => {
                let child_idx = Self::child_index(keys, &key);
                let next = children[child_idx];
                self.find_leaf_for_key(next, key)
            }
        }
    }
}

impl BPlusTree {
    pub(crate) fn scan_maintenance_from(
        &mut self,
        from: Option<&[u8]>,
        mut on: impl FnMut(&[u8], Option<&[u8]>) -> bool,
    ) -> std::io::Result<MaintenanceEnd> {
        #[cfg(test)]
        if maintenance_reference::enabled() {
            let mut stopped = false;
            self.scan_entries_break_from(from, |key, value| {
                stopped = on(key, value.as_deref());
                stopped
            });
            return Ok(if stopped {
                MaintenanceEnd::Stopped
            } else {
                MaintenanceEnd::Complete
            });
        }
        let result = self.scan_maintenance_inner(from, &mut on);
        if let Err(error) = &result {
            self.page_manager.mark_maintenance_failed(error);
        }
        result
    }

    pub(super) fn scan_maintenance_inner(
        &mut self,
        from: Option<&[u8]>,
        on: &mut impl FnMut(&[u8], Option<&[u8]>) -> bool,
    ) -> std::io::Result<MaintenanceEnd> {
        let mut raw = [0; crate::bplus_tree::storage::PAGE_SIZE];
        let mut page = self.root;
        let mut visits = 0_u64;
        let mut last_key = Vec::new();
        let mut has_last = false;
        loop {
            visits += 1;
            if visits > self.page_manager.next_page_id() {
                return Err(NodeBytes::invalid());
            }
            let payload = self.page_manager.read_maintenance_page(page, &mut raw)?;
            match MaintenanceNode::decode(payload)? {
                MaintenanceNode::Internal {
                    keys,
                    count,
                    children,
                } => {
                    page = MaintenanceNode::child(keys, count, children, from.unwrap_or(&[]))?;
                }
                MaintenanceNode::Leaf { .. } => break,
            }
        }
        loop {
            let payload = self.page_manager.read_maintenance_page(page, &mut raw)?;
            let MaintenanceNode::Leaf { rows, count, next } = MaintenanceNode::decode(payload)?
            else {
                return Err(NodeBytes::invalid());
            };
            #[cfg(test)]
            crate::common::maintenance_probe::note_bplus_maintenance_page(count);
            let mut rows = NodeBytes(rows);
            let mut tail = None;
            for _ in 0..count {
                let (key, value) = rows.row()?;
                if has_last && key <= last_key.as_slice() {
                    return Err(NodeBytes::invalid());
                }
                tail = Some(key);
                // 保持包含目标键的叶子定位语义；MVCC 续扫过滤仍负责排除已消费的行。
                if on(key, value) {
                    return Ok(MaintenanceEnd::Stopped);
                }
            }
            if let Some(key) = tail {
                last_key.clear();
                last_key.extend_from_slice(key);
                has_last = true;
            }
            let Some(next) = next else {
                return Ok(MaintenanceEnd::Complete);
            };
            page = next;
            visits += 1;
            if visits > self.page_manager.next_page_id() {
                return Err(NodeBytes::invalid());
            }
        }
    }

    /// 全表流式遍历，按**引用**把 `(键, value)` 交给回调 —— 不克隆任何 value。
    ///
    /// B+ 树的 value 就住在叶子条目里，没有 Bitcask `KeyDir` 那种独立键索引，所以
    /// 「过每一行、把整页反序列化出来」这一遍省不掉（页读取次数不变）—— 这点和
    /// Bitcask 不一样，别指望一样。真正能省的是**把每行 value 克隆出来**：
    /// `iter()` 每行克隆一次，本方法一次都不克隆。全库折叠与 vacuum 都改用它。
    pub fn scan_entries_with(&mut self, mut f: impl FnMut(&[u8], &Option<Vec<u8>>)) {
        // 先下降到最左叶子
        let mut current = self.root;
        loop {
            let Some(data) = self.page_manager.read_page(current) else {
                return;
            };
            match bincode::deserialize::<BTreeNode>(&data) {
                Ok(BTreeNode::Internal { children, .. }) => {
                    let Some(&first) = children.first() else {
                        return;
                    };
                    current = first;
                }
                Ok(BTreeNode::Leaf { .. }) => break,
                Err(_) => return,
            }
        }

        // 再沿单向 next_leaf 链表一路向右。读页/反序列化失败就止步（与现有 iter 一致地宽容）
        // 走查用 `read_page_for_walk`：每片叶子只碰一次，回填它不会有下一次命中，
        // 却会把点读焐热的页挤出去（实测一次全库走查换出 1600 页、顺带 flush 765 页脏页）。
        let mut leaf = Some(current);
        while let Some(id) = leaf {
            let Some(data) = self.page_manager.read_page_for_walk(id) else {
                break;
            };
            let Ok(node) = bincode::deserialize::<BTreeNode>(&data) else {
                break;
            };
            if let BTreeNode::Leaf { entries, next_leaf } = node {
                for (k, rid) in &entries {
                    f(k, &rid.0);
                }
                leaf = next_leaf;
            } else {
                break;
            }
        }
    }

    /// 只要键的场合（代价与理由见 [`Self::scan_entries_with`]）。
    pub fn scan_keys_with(&mut self, mut f: impl FnMut(&[u8])) {
        self.scan_entries_with(|k, _row| f(k));
    }

    /// 从 `from`（**含**，落在键的字节域上）起的流式遍历，按引用交给 `(键, value)`；
    /// 回调返回 `true` 表示「够了，止步于此」。
    ///
    /// 与 [`Self::scan_entries_with`] 的差别只有两件事：**起点**（下降到Containing叶子，
    /// 前面的叶子一篇都不读）和**止步**（回调喊停就不再往右）。起点是键域下界而不是逻辑
    /// key，所以调用方要「跳过某个逻辑 key 的整组」得自己按 `raw_key > from` 过滤。
    ///
    /// 存在的理由是锁：`BPlusTree` 挂在 `Mutex` 后面，全表遍历会把这把锁按到整库扫完，
    /// 期间点读点写全部排队。有界回收（`vacuum_bounded`）靠它把每一轮按住一小段。
    pub fn scan_entries_break_from(
        &mut self,
        from: Option<&[u8]>,
        mut f: impl FnMut(&[u8], &Option<Vec<u8>>) -> bool,
    ) {
        // 空键 ≤ 一切 ⇒ 没有起点时下降到的就是最左叶子
        let mut leaf = self.find_leaf_for_key(self.root, from.unwrap_or(&[]).to_vec());
        while let Some(id) = leaf {
            // 同上：一次性走查不回填、不换出，见 `scan_entries_with`
            let Some(data) = self.page_manager.read_page_for_walk(id) else {
                break;
            };
            let Ok(node) = bincode::deserialize::<BTreeNode>(&data) else {
                break;
            };
            match node {
                BTreeNode::Leaf { entries, next_leaf } => {
                    for (k, rid) in &entries {
                        if f(k, &rid.0) {
                            return;
                        }
                    }
                    leaf = next_leaf;
                }
                _ => break,
            }
        }
    }

    /// 树上真实存在的**最大键**（一路取最右孩子下降到叶子）—— 给"从库尾反向"当起点用（M82）。
    ///
    /// 代价 = 树高次读页（O(log n)），与 `scan_entries_with` 那个"下降到最左叶子"对称。
    /// 为什么不造 `\xFF…` 哨兵当上界：任意字节空间没有最大值，哨兵长度不够就会把更长的尾键整批漏掉。
    pub fn last_key(&mut self) -> Option<Key> {
        let mut current = self.root;
        loop {
            let data = self.page_manager.read_page(current)?;
            match bincode::deserialize::<BTreeNode>(&data).ok()? {
                BTreeNode::Internal { children, .. } => current = *children.last()?,
                BTreeNode::Leaf { entries, .. } => return entries.last().map(|(k, _)| k.clone()),
            }
        }
    }

    pub fn iter(&mut self) -> BPlusTreeIterator<'_> {
        // 找到最左边的叶子节点
        let mut current = self.root;
        loop {
            let node_data = self.page_manager.read_page(current).expect("读取页失败");
            let node: BTreeNode = bincode::deserialize(&node_data).expect("反序列化失败");

            match node {
                BTreeNode::Internal { children, .. } => {
                    current = children[0]; // 始终向左走
                }
                BTreeNode::Leaf { .. } => break,
            }
        }

        BPlusTreeIterator {
            tree: self,
            current_page_id: Some(current),
            current_idx: 0,
        }
    }
}

impl<'a> Iterator for BPlusTreeIterator<'a> {
    type Item = (Key, RowId);

    fn next(&mut self) -> Option<Self::Item> {
        let page_id = self.current_page_id?;

        // 读取当前节点（叶子链上的走查 ⇒ 不回填、不把点读的热页挤出去）
        let node_data = self.tree.page_manager.read_page_for_walk(page_id)?;
        let node: BTreeNode = bincode::deserialize(&node_data).ok()?;

        if let BTreeNode::Leaf { entries, next_leaf } = node {
            if self.current_idx < entries.len() {
                // 返回当前条目并自增索引
                #[cfg(test)]
                crate::bplus_tree::page::note_value_clone();
                let item = entries[self.current_idx].clone();
                self.current_idx += 1;
                Some(item)
            } else {
                // 当前页读完了，跳转到下一个叶子节点
                self.current_page_id = next_leaf;
                self.current_idx = 0;
                self.next() // 递归调用以获取下一页的第一个元素
            }
        } else {
            None
        }
    }
}
