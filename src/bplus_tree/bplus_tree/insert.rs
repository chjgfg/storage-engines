//! 本文件负责插入、更新与节点分裂。

use super::*;

impl InsertStep {
    pub(super) fn written(writes: Vec<(PageId, BTreeNode)>) -> Self {
        Self {
            landed: true,
            promote: None,
            writes,
            alloc: Vec::new(),
        }
    }

    pub(super) fn dropped() -> Self {
        Self {
            landed: false,
            promote: None,
            writes: Vec::new(),
            alloc: Vec::new(),
        }
    }
}

impl BPlusTree {
    /// 插入一行。超大行在修改前拒绝；页 I/O 失败也返回 false，可能已部分修改。
    /// I/O 失败后保留脏页并封锁后续写入/checkpoint，调用方必须重开恢复。
    ///
    /// 写法是**下降一趟全在内存里算，到顶才统一落盘**（`InsertStep::writes`）。老写法边算边写，
    /// 于是「孩子已经分裂写完、父亲却放不下」这一格会把刚分出去的那一半变成**没人引用的页** ——
    /// 和 M11f 那个静默丢页是同一个形状：`write_page` 超载只打一行日志，返回的 `false` 被调用链丢掉。
    pub fn insert(&mut self, key: Key, row_id: RowId) -> bool {
        if self.ensure_healthy().is_err() {
            return false;
        }
        let step = self.insert_recursive(self.root, key, row_id);
        if !step.landed {
            // 一个字节都没写 ⇒ 途中占下的页谁也不引用，回收掉别留孤儿
            for page in step.alloc {
                self.page_manager.free_page(page);
            }
            #[cfg(test)]
            SPLIT_DROPS.with(|c| c.set(c.get() + 1));
            return false;
        }
        for (page_id, node) in step.writes {
            // 到这里的每个节点都逐档量过字节 ⇒ 写失败只剩「磁盘坏了」这一种解释
            let data = bincode::serialize(&node).expect("serialize node");
            if self.page_manager.write_page(page_id, &data) {
                // 写成功才回填：内容与盘上不一致时，缓存里的旧那份比没有那份更坏
                self.cache_written(page_id, node);
            }
        }
        if let Some((promote_key, new_page)) = step.promote {
            let new_root_page_id = self.page_manager.allocate_page();
            let new_root = BTreeNode::Internal {
                keys: vec![promote_key],
                children: vec![self.root, new_page],
            };
            let root_data = bincode::serialize(&new_root).expect("serialize new root");
            if self.page_manager.write_page(new_root_page_id, &root_data) {
                self.cache_written(new_root_page_id, new_root);
            }
            self.set_root(new_root_page_id);
        }
        self.ensure_healthy().is_ok()
    }

    /// 一趟下降：返回「还没落盘的一串（页号, 内容）」与要不要往上升一层。
    /// `landed = false` 时 `writes` 必为空，`alloc` 继续往上传，由 [`Self::insert`] 统一回收。
    pub(super) fn insert_recursive(
        &mut self,
        node_page_id: PageId,
        key: Key,
        row_id: RowId,
    ) -> InsertStep {
        // 下降走解析缓存（M34），并且**只有真要改这一层时才克隆键/子数组**：
        // 顺序灌行时内部节点 40 次里只有 1 次真的变（下面分裂了），另外 39 次连一次堆分配都不必花。
        //
        // ⚠️ 这里必须是 `expect` 不能是"当失败处理"：`insert` 返回 `false` 的含义被上层读成
        // "这一行装不下"（`row_fits` 已经过了 ⇒ WAL 也已经写了）⇒ 页读不出来却回一句 false，
        // 盘上就留下一条"提交了但树里没有"的记录，正是 `row_fits` 注释里要避免的那个形状。
        let node = self.node_of(node_page_id).expect("下降取节点");
        let order = self.order;
        let cap = MAX_PAYLOAD as i64;

        match &*node {
            BTreeNode::Leaf { entries, next_leaf } => {
                let next_leaf = *next_leaf;
                let mut entries = entries.clone();
                match entries.binary_search_by(|(k, _)| k.cmp(&key)) {
                    Ok(idx) => entries[idx].1 = row_id,
                    Err(idx) => entries.insert(idx, (key, row_id)),
                }
                if entries.len() < order && Self::leaf_total_bytes(&entries, next_leaf) <= cap {
                    return InsertStep::written(vec![(
                        node_page_id,
                        BTreeNode::Leaf { entries, next_leaf },
                    )]);
                }
                let Some(i) = Self::leaf_split_point(&entries, next_leaf, order) else {
                    return InsertStep::dropped();
                };
                let promote_key = entries[i].0.clone();
                let right_entries = entries.split_off(i);
                let new_page = self.page_manager.allocate_page();
                let left = BTreeNode::Leaf {
                    entries,
                    next_leaf: Some(new_page),
                };
                let right = BTreeNode::Leaf {
                    entries: right_entries,
                    next_leaf,
                };
                InsertStep {
                    landed: true,
                    promote: Some((promote_key, new_page)),
                    writes: vec![(node_page_id, left), (new_page, right)],
                    alloc: vec![new_page],
                }
            }
            BTreeNode::Internal { keys, children } => {
                let child_idx = Self::child_index(keys, &key);
                let child_page_id = children[child_idx];
                let child = self.insert_recursive(child_page_id, key, row_id);
                if !child.landed {
                    return child; // 孩子的 writes 本来就空；alloc 继续往上传
                }
                let Some((promote_key, new_page)) = child.promote else {
                    return child; // 下面没分裂 ⇒ 孩子那条写入原样上交
                };
                // 走到这里才需要 owned 副本（这一层要插一把上提键、挂一个新子）
                let (mut keys, mut children) = (keys.clone(), children.clone());
                keys.insert(child_idx, promote_key);
                children.insert(child_idx + 1, new_page);
                if keys.len() < order && Self::internal_total_bytes(&keys, children.len()) <= cap {
                    let mut writes = child.writes;
                    writes.push((node_page_id, BTreeNode::Internal { keys, children }));
                    return InsertStep {
                        landed: true,
                        promote: None,
                        writes,
                        alloc: child.alloc,
                    };
                }
                let Some(k) = Self::internal_split_point(&keys, order) else {
                    // 塞不下又切不开 ⇒ **整条路径作废**：孩子的写入也丢掉。它们从没落过盘，
                    // 所以盘上还是各自原来的内容 —— 这比"写了一半才发现不行"强得多。
                    return InsertStep {
                        landed: false,
                        promote: None,
                        writes: Vec::new(),
                        alloc: child.alloc,
                    };
                };
                let up_key = keys[k].clone();
                let right_children = children.split_off(k + 1);
                // keys[k] 就是上提那把：先切走 [k+1..]，再把 keys[k] 从剩下的尾巴上摘掉
                let right_keys = keys.split_off(k + 1);
                keys.pop();
                let new_page = self.page_manager.allocate_page();
                let left = BTreeNode::Internal { keys, children };
                let right = BTreeNode::Internal {
                    keys: right_keys,
                    children: right_children,
                };
                let mut writes = child.writes;
                writes.push((node_page_id, left));
                writes.push((new_page, right));
                let mut alloc = child.alloc;
                alloc.push(new_page);
                InsertStep {
                    landed: true,
                    promote: Some((up_key, new_page)),
                    writes,
                    alloc,
                }
            }
        }
    }
}

impl BPlusTree {
    pub fn update(&mut self, key: Key, new_row_id: RowId) -> bool {
        self.update_recursive(self.root, key, new_row_id)
    }

    pub(super) fn update_recursive(
        &mut self,
        node_page_id: PageId,
        key: Key,
        new_row_id: RowId,
    ) -> bool {
        // 读取节点
        let node_data = self.page_manager.read_page(node_page_id).unwrap();
        let mut node: BTreeNode = bincode::deserialize(&node_data).unwrap();
        match &mut node {
            BTreeNode::Leaf { entries, .. } => {
                // 查找key
                if let Ok(idx) = entries.binary_search_by(|(k, _)| k.cmp(&key)) {
                    // 更新RowId
                    entries[idx].1 = new_row_id;
                    // 写回页：RowId 变长可能让整页超载 ⇒ 必须量过字节，装不下就报失败，
                    // 不能再走"write_page 打印一句、返回值丢掉"那条静默路径
                    return self.write_node(node_page_id, &node);
                }
                return false;
            }
            BTreeNode::Internal { keys, children } => {
                // 找到子节点
                let child_idx = Self::child_index(keys, &key);
                let child_page_id = children[child_idx];
                // 递归更新子节点
                self.update_recursive(child_page_id, key, new_row_id)
            }
        }
    }
}
