//! 本文件负责点查、索引过滤与反向定位。

use super::*;

impl BPlusTree {
    pub fn get(&mut self, key: Key) -> Option<RowId> {
        self.get_recursive(self.root, key)
    }

    pub fn get_recursive(&mut self, node_page_id: PageId, key: Key) -> Option<RowId> {
        // 走解析缓存：点读的代价大头是"每层都重新解析整个节点"，不是读盘
        let node = self.node_of(node_page_id)?;
        match &*node {
            BTreeNode::Leaf { entries, .. } => entries
                .binary_search_by(|(k, _)| k.cmp(&key))
                .ok()
                .map(|idx| entries[idx].1.clone()),
            BTreeNode::Internal { keys, children } => {
                let child_idx = Self::child_index(keys, &key);
                let next = children[child_idx];
                self.get_recursive(next, key)
            }
        }
    }
}
