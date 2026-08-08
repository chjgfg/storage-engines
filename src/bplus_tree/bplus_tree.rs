use std::{fmt, path::Path, write};

use serde::{Deserialize, Serialize};

use crate::bplus_tree::page::{MAX_PAYLOAD, PageId, PageManager};

/// 行ID：指向数据在磁盘/内存中的位置
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
// pub struct RowId(pub u64);
pub struct RowId(pub Option<Vec<u8>>);

/// B+ 树索引键
// pub type Key = u64;
pub type Key = Vec<u8>;

/// B+ 树节点类型
#[derive(Debug, Clone, Serialize, Deserialize)]
enum BTreeNode {
    /// 叶子结点
    Leaf {
        /// kv结构
        entries: Vec<(Key, RowId)>,
        /// 下一个叶子结点的页ID
        next_leaf: Option<PageId>,
    },
    /// 非叶子结点
    Internal {
        /// 索引
        keys: Vec<Key>,
        /// 子节点页ID
        children: Vec<PageId>,
    },
}

/// 带页管理的B+树
pub struct BPlusTree {
    /// 页管理器
    page_manager: PageManager,
    /// 根节点页ID
    root: PageId,
    /// 阶数
    order: usize,
}

impl BPlusTree {
    /// 打开或创建磁盘上的 B+ 树（返回 Result，便于上层处理锁冲突等）
    pub fn try_open(
        path: impl AsRef<Path>,
        order: usize,
        max_cache_pages: usize,
    ) -> std::io::Result<Self> {
        assert!(order >= 3, "B+树阶数至少3阶");
        let mut page_manager = PageManager::open(path, max_cache_pages)?;

        let root = if let Some(root) = page_manager.root_page_id() {
            root
        } else {
            let root_page_id = page_manager.allocate_page();
            let root_node = BTreeNode::Leaf {
                entries: Vec::new(),
                next_leaf: None,
            };
            let node_data = bincode::serialize(&root_node)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
            page_manager.write_page(root_page_id, &node_data);
            page_manager.set_root_page_id(root_page_id);
            page_manager.flush_all();
            root_page_id
        };

        Ok(Self {
            page_manager,
            root,
            order,
        })
    }

    /// 打开或创建磁盘上的 B+ 树
    ///
    /// - 文件不存在 / 空文件：创建新树（空叶子根）
    /// - 已有合法文件：从 meta 恢复 root，数据跨进程保留
    pub fn open(path: impl AsRef<Path>, order: usize, max_cache_pages: usize) -> Self {
        Self::try_open(path, order, max_cache_pages)
            .map_err(|e| {
                format!(
                    "打开数据文件失败: {e}\n\
    提示: 若错误为 WouldBlock/锁被占用, 请确保同进程内已 drop 全部 MVCC 与 Transaction,\
    且没有其它进程占用 data.db.lock。"
                )
            })
            .unwrap()
    }

    /// 兼容旧接口：在系统临时目录创建匿名数据文件
    pub fn new(order: usize, max_cache_pages: usize) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("bplus_anon_{}_{}.db", std::process::id(), nanos));
        Self::open(path, order, max_cache_pages)
    }

    /// 根页变化时同步到 meta（分裂升高 / 删除降层）
    fn set_root(&mut self, new_root: PageId) {
        self.root = new_root;
        self.page_manager.set_root_page_id(new_root);
    }

    /// 刷盘所有脏页 + meta，并 fsync
    pub fn flush_all(&mut self) {
        self.page_manager.set_root_page_id(self.root);
        self.page_manager.flush_all();
    }

    /// 当前根页（WAL checkpoint 使用）
    pub fn root_page_id(&self) -> PageId {
        self.root
    }

    /// 下一个将分配的页号（WAL checkpoint 使用）
    pub fn next_page_id(&self) -> u64 {
        self.page_manager.next_page_id()
    }

    /// meta 中的 next_version（旧库无 NVER 时为 None）
    pub fn next_version(&self) -> Option<u64> {
        self.page_manager.next_version()
    }

    /// 持久化 next_version 到 meta（open/checkpoint 时调用）
    pub fn set_next_version(&mut self, v: u64) {
        self.page_manager.set_next_version(v);
    }

    /// 打开/关闭 bulk 模式（委托 PageManager）
    pub fn set_bulk_mode(&mut self, on: bool) {
        self.page_manager.set_bulk_mode(on);
    }

    pub fn bulk_mode(&self) -> bool {
        self.page_manager.bulk_mode()
    }
}

impl BPlusTree {
    /// 一次序列化判定 + 写入（避免 node_fits 与 try_write 双重 bincode）
    fn write_if_fits(&mut self, page_id: PageId, node: &BTreeNode) -> bool {
        let count_ok = match node {
            BTreeNode::Leaf { entries, .. } => entries.len() < self.order,
            BTreeNode::Internal { keys, .. } => keys.len() < self.order,
        };
        if !count_ok {
            return false;
        }
        let data = match bincode::serialize(node) {
            Ok(d) => d,
            Err(_) => return false,
        };
        if data.len() > MAX_PAYLOAD {
            return false;
        }
        self.page_manager.write_page(page_id, &data)
    }

    pub fn insert(&mut self, key: Key, row_id: RowId) -> bool {
        let (split, new_key, new_node_page_id) = self.insert_recursive(self.root, key, row_id);
        if split {
            let new_root_page_id = self.page_manager.allocate_page();
            let new_root = BTreeNode::Internal {
                keys: vec![new_key],
                children: vec![self.root, new_node_page_id],
            };
            let root_data = bincode::serialize(&new_root).unwrap();
            self.page_manager.write_page(new_root_page_id, &root_data);
            self.set_root(new_root_page_id);
        }
        true
    }

    fn insert_recursive(
        &mut self,
        node_page_id: PageId,
        key: Key,
        row_id: RowId,
    ) -> (bool, Key, PageId) {
        let node_data = self.page_manager.read_page(node_page_id).unwrap();
        let mut node: BTreeNode = bincode::deserialize(&node_data).unwrap();

        match &mut node {
            BTreeNode::Leaf {
                entries,
                next_leaf: _,
            } => match entries.binary_search_by(|(k, _)| k.cmp(&key)) {
                Ok(idx) => {
                    entries[idx].1 = row_id;
                    if self.write_if_fits(node_page_id, &node) {
                        (false, vec![0], PageId(0))
                    } else {
                        self.split_leaf(node_page_id, node)
                    }
                }
                Err(idx) => {
                    entries.insert(idx, (key, row_id));
                    if self.write_if_fits(node_page_id, &node) {
                        (false, vec![0], PageId(0))
                    } else {
                        self.split_leaf(node_page_id, node)
                    }
                }
            },
            BTreeNode::Internal { keys, children } => {
                let child_idx = keys
                    .iter()
                    .position(|k| key < k.clone())
                    .unwrap_or(keys.len());
                let child_page_id = children[child_idx];
                let (split, split_key, new_child_page_id) =
                    self.insert_recursive(child_page_id, key, row_id);
                if !split {
                    return (false, vec![0], PageId(0));
                }
                keys.insert(child_idx, split_key);
                children.insert(child_idx + 1, new_child_page_id);
                if self.write_if_fits(node_page_id, &node) {
                    (false, vec![0], PageId(0))
                } else {
                    self.split_internal(node_page_id, node)
                }
            }
        }
    }

    /// 分裂叶子：返回 (true, 右节点最小键, 右页 id)
    fn split_leaf(&mut self, node_page_id: PageId, mut node: BTreeNode) -> (bool, Key, PageId) {
        let BTreeNode::Leaf {
            ref mut entries,
            ref mut next_leaf,
        } = node
        else {
            return (false, vec![0], PageId(0));
        };
        let mid = entries.len() / 2;
        let right_entries = entries.split_off(mid);
        let split_key = right_entries[0].0.clone();
        let new_leaf_page_id = self.page_manager.allocate_page();
        let right_node = BTreeNode::Leaf {
            entries: right_entries,
            next_leaf: *next_leaf,
        };
        *next_leaf = Some(new_leaf_page_id);
        // 若半页仍超载（单条超大），尽力写入；真正的大 value 需要外置存储
        let left_data = bincode::serialize(&node).unwrap();
        self.page_manager.write_page(node_page_id, &left_data);
        let right_data = bincode::serialize(&right_node).unwrap();
        self.page_manager.write_page(new_leaf_page_id, &right_data);
        (true, split_key, new_leaf_page_id)
    }

    /// 分裂内部节点
    fn split_internal(&mut self, node_page_id: PageId, mut node: BTreeNode) -> (bool, Key, PageId) {
        let BTreeNode::Internal {
            ref mut keys,
            ref mut children,
        } = node
        else {
            return (false, vec![0], PageId(0));
        };
        let mid = keys.len() / 2;
        let promote_key = keys[mid].clone();
        let right_keys = keys.split_off(mid + 1);
        let right_children = children.split_off(mid + 1);
        keys.pop(); // 移除上提的键
        let new_internal_page_id = self.page_manager.allocate_page();
        let right_node = BTreeNode::Internal {
            keys: right_keys,
            children: right_children,
        };
        let left_data = bincode::serialize(&node).unwrap();
        self.page_manager.write_page(node_page_id, &left_data);
        let right_data = bincode::serialize(&right_node).unwrap();
        self.page_manager
            .write_page(new_internal_page_id, &right_data);
        (true, promote_key, new_internal_page_id)
    }
}

impl BPlusTree {
    pub fn get(&mut self, key: Key) -> Option<RowId> {
        self.get_recursive(self.root, key)
    }

    pub fn get_recursive(&mut self, node_page_id: PageId, key: Key) -> Option<RowId> {
        let node_data = self.page_manager.read_page(node_page_id).unwrap();
        let node: BTreeNode = bincode::deserialize(&node_data).unwrap();
        match &node {
            BTreeNode::Leaf { entries, .. } => entries
                .binary_search_by(|(k, _)| k.cmp(&key))
                .ok()
                .map(|idx| entries[idx].1.clone()),
            BTreeNode::Internal { keys, children } => {
                let child_idx = keys
                    .iter()
                    .position(|k| key < k.clone())
                    .unwrap_or(keys.len());
                self.get_recursive(children[child_idx], key)
            }
        }
    }
}

impl BPlusTree {
    pub fn update(&mut self, key: Key, new_row_id: RowId) -> bool {
        self.update_recursive(self.root, key, new_row_id)
    }

    fn update_recursive(&mut self, node_page_id: PageId, key: Key, new_row_id: RowId) -> bool {
        // 读取节点
        let node_data = self.page_manager.read_page(node_page_id).unwrap();
        let mut node: BTreeNode = bincode::deserialize(&node_data).unwrap();
        match &mut node {
            BTreeNode::Leaf { entries, .. } => {
                // 查找key
                if let Ok(idx) = entries.binary_search_by(|(k, _)| k.cmp(&key)) {
                    // 更新RowId
                    entries[idx].1 = new_row_id;
                    // 写回页
                    let data = bincode::serialize(&node).unwrap();
                    self.page_manager.write_page(node_page_id, &data);
                    return true;
                }
                return false;
            }
            BTreeNode::Internal { keys, children } => {
                // 找到子节点
                let child_idx = keys
                    .iter()
                    .position(|k| key < k.clone())
                    .unwrap_or(keys.len());
                let child_page_id = children[child_idx];
                // 递归更新子节点
                self.update_recursive(child_page_id, key, new_row_id)
            }
        }
    }
}

impl BPlusTree {
    /// 删除：移除指定 key，必要时借键/合并并处理根降层
    pub fn delete(&mut self, key: Key) -> bool {
        let deleted = self.delete_recursive(self.root, key, None);

        // 根是内部节点且只剩一个子节点时降层
        if deleted {
            self.shrink_root_if_needed();
        }
        deleted
    }

    /// 最小填充数（⌈order/2⌉ - 1）；order=4 → min=1
    fn min_entries(&self) -> usize {
        (self.order + 1) / 2 - 1
    }

    fn read_node(&mut self, page_id: PageId) -> BTreeNode {
        let data = self.page_manager.read_page(page_id).expect("read page");
        bincode::deserialize(&data).expect("deserialize node")
    }

    fn write_node(&mut self, page_id: PageId, node: &BTreeNode) {
        let data = bincode::serialize(node).expect("serialize node");
        self.page_manager.write_page(page_id, &data);
    }

    fn node_key_count(node: &BTreeNode) -> usize {
        match node {
            BTreeNode::Leaf { entries, .. } => entries.len(),
            BTreeNode::Internal { keys, .. } => keys.len(),
        }
    }

    fn is_underflow(&self, node: &BTreeNode) -> bool {
        Self::node_key_count(node) < self.min_entries()
    }

    /// 在父节点中查找 child 的下标
    fn get_parent_index(&mut self, parent_page_id: PageId, child_page_id: PageId) -> Option<usize> {
        match self.read_node(parent_page_id) {
            BTreeNode::Internal { children, .. } => {
                children.iter().position(|&pid| pid == child_page_id)
            }
            _ => None,
        }
    }

    /// 优先右兄弟，否则左兄弟；返回 (sibling_id, is_left_sibling)
    fn get_sibling(&mut self, parent_page_id: PageId, child_idx: usize) -> (Option<PageId>, bool) {
        match self.read_node(parent_page_id) {
            BTreeNode::Internal { children, .. } => {
                if child_idx + 1 < children.len() {
                    (Some(children[child_idx + 1]), false)
                } else if child_idx > 0 {
                    (Some(children[child_idx - 1]), true)
                } else {
                    (None, false)
                }
            }
            _ => (None, false),
        }
    }

    /// DFS 查找 target 的父节点（正确性优先，教学实现）
    fn find_parent_of(&mut self, target: PageId) -> Option<PageId> {
        if target == self.root {
            return None;
        }
        self.find_parent_of_rec(self.root, target)
    }

    fn find_parent_of_rec(&mut self, current: PageId, target: PageId) -> Option<PageId> {
        match self.read_node(current) {
            BTreeNode::Leaf { .. } => None,
            BTreeNode::Internal { children, .. } => {
                if children.iter().any(|&c| c == target) {
                    return Some(current);
                }
                // 克隆 children 避免与后续递归的可变借用冲突
                for child in children {
                    if let Some(p) = self.find_parent_of_rec(child, target) {
                        return Some(p);
                    }
                }
                None
            }
        }
    }

    fn shrink_root_if_needed(&mut self) {
        match self.read_node(self.root) {
            BTreeNode::Internal { children, .. } if children.len() == 1 => {
                let old_root = self.root;
                self.set_root(children[0]);
                self.page_manager.free_page(old_root);
            }
            _ => {}
        }
    }

    /// 叶子：向兄弟借一条记录；不改动 next_leaf 链表
    fn leaf_borrow_key(
        &mut self,
        node_page_id: PageId,
        sibling_page_id: PageId,
        is_left_sibling: bool,
        parent_page_id: PageId,
        child_idx: usize,
    ) {
        let mut node = self.read_node(node_page_id);
        let mut sibling = self.read_node(sibling_page_id);
        let mut parent = self.read_node(parent_page_id);

        match (&mut node, &mut sibling, &mut parent) {
            (
                BTreeNode::Leaf {
                    entries: node_entries,
                    ..
                },
                BTreeNode::Leaf {
                    entries: sib_entries,
                    ..
                },
                BTreeNode::Internal {
                    keys: parent_keys, ..
                },
            ) => {
                if is_left_sibling {
                    // 左兄弟借最后一条 → 成为当前节点最小键
                    let entry = sib_entries.pop().expect("left sibling empty");
                    parent_keys[child_idx - 1] = entry.0.clone();
                    node_entries.insert(0, entry);
                } else {
                    // 右兄弟借第一条；父分隔键应更新为右兄弟新的最小键
                    let entry = sib_entries.remove(0);
                    node_entries.push(entry);
                    if !sib_entries.is_empty() {
                        parent_keys[child_idx] = sib_entries[0].0.clone();
                    }
                }
            }
            _ => return,
        }

        self.write_node(node_page_id, &node);
        self.write_node(sibling_page_id, &sibling);
        self.write_node(parent_page_id, &parent);
    }

    /// 叶子：与兄弟合并
    /// - 左兄弟：内容并入左兄弟，删 parent.keys[child_idx-1] 与 children[child_idx]
    /// - 右兄弟：右兄弟并入当前，删 parent.keys[child_idx] 与 children[child_idx+1]
    fn leaf_merge(
        &mut self,
        node_page_id: PageId,
        sibling_page_id: PageId,
        is_left_sibling: bool,
        parent_page_id: PageId,
        child_idx: usize,
    ) {
        let mut node = self.read_node(node_page_id);
        let mut sibling = self.read_node(sibling_page_id);
        let mut parent = self.read_node(parent_page_id);

        let (survivor_id, dead_id, parent_key_idx, delete_child_idx) = if is_left_sibling {
            // left=sibling, right=node → 并入 left
            match (&mut sibling, &mut node) {
                (
                    BTreeNode::Leaf {
                        entries: left_entries,
                        next_leaf: left_next,
                    },
                    BTreeNode::Leaf {
                        entries: right_entries,
                        next_leaf: right_next,
                    },
                ) => {
                    left_entries.append(right_entries);
                    *left_next = *right_next;
                }
                _ => return,
            }
            (sibling_page_id, node_page_id, child_idx - 1, child_idx)
        } else {
            // left=node, right=sibling → 并入 left(node)
            match (&mut node, &mut sibling) {
                (
                    BTreeNode::Leaf {
                        entries: left_entries,
                        next_leaf: left_next,
                    },
                    BTreeNode::Leaf {
                        entries: right_entries,
                        next_leaf: right_next,
                    },
                ) => {
                    left_entries.append(right_entries);
                    *left_next = *right_next;
                }
                _ => return,
            }
            (node_page_id, sibling_page_id, child_idx, child_idx + 1)
        };

        if let BTreeNode::Internal { keys, children } = &mut parent {
            keys.remove(parent_key_idx);
            children.remove(delete_child_idx);
        }

        let survivor = if survivor_id == node_page_id {
            &node
        } else {
            &sibling
        };
        self.write_node(survivor_id, survivor);
        self.write_node(parent_page_id, &parent);
        self.page_manager.free_page(dead_id);
    }

    /// 内部节点：向兄弟借键（经典旋转）
    fn internal_borrow_key(
        &mut self,
        node_page_id: PageId,
        sibling_page_id: PageId,
        is_left_sibling: bool,
        parent_page_id: PageId,
        child_idx: usize,
    ) {
        let mut node = self.read_node(node_page_id);
        let mut sibling = self.read_node(sibling_page_id);
        let mut parent = self.read_node(parent_page_id);

        match (&mut node, &mut sibling, &mut parent) {
            (
                BTreeNode::Internal {
                    keys: node_keys,
                    children: node_children,
                },
                BTreeNode::Internal {
                    keys: sib_keys,
                    children: sib_children,
                },
                BTreeNode::Internal {
                    keys: parent_keys, ..
                },
            ) => {
                let parent_key_idx = if is_left_sibling {
                    child_idx - 1
                } else {
                    child_idx
                };
                let parent_key = parent_keys[parent_key_idx].clone();

                if is_left_sibling {
                    // 左兄弟最后的 key 上提到父，父原 key 下沉到当前最左
                    let borrowed_key = sib_keys.pop().expect("sib keys");
                    let borrowed_child = sib_children.pop().expect("sib children");
                    parent_keys[parent_key_idx] = borrowed_key;
                    node_keys.insert(0, parent_key);
                    node_children.insert(0, borrowed_child);
                } else {
                    // 右兄弟第一个 key 上提到父，父原 key 下沉到当前最右
                    let borrowed_key = sib_keys.remove(0);
                    let borrowed_child = sib_children.remove(0);
                    parent_keys[parent_key_idx] = borrowed_key;
                    node_keys.push(parent_key);
                    node_children.push(borrowed_child);
                }
            }
            _ => return,
        }

        self.write_node(node_page_id, &node);
        self.write_node(sibling_page_id, &sibling);
        self.write_node(parent_page_id, &parent);
    }

    /// 内部节点合并
    /// - 左兄弟：并入左兄弟，删 children[child_idx]（当前节点）
    /// - 右兄弟：右兄弟并入当前，删 children[child_idx+1]
    fn internal_merge(
        &mut self,
        node_page_id: PageId,
        sibling_page_id: PageId,
        is_left_sibling: bool,
        parent_page_id: PageId,
        child_idx: usize,
    ) {
        let mut node = self.read_node(node_page_id);
        let mut sibling = self.read_node(sibling_page_id);
        let mut parent = self.read_node(parent_page_id);

        let parent_key_idx = if is_left_sibling {
            child_idx - 1
        } else {
            child_idx
        };

        let parent_key = match &mut parent {
            BTreeNode::Internal { keys, .. } => keys.remove(parent_key_idx),
            _ => return,
        };

        let (survivor_id, dead_id, delete_child_idx) = if is_left_sibling {
            // 并入左兄弟(sibling)
            match (&mut sibling, &mut node) {
                (
                    BTreeNode::Internal {
                        keys: left_keys,
                        children: left_children,
                    },
                    BTreeNode::Internal {
                        keys: right_keys,
                        children: right_children,
                    },
                ) => {
                    left_keys.push(parent_key);
                    left_keys.append(right_keys);
                    left_children.append(right_children);
                }
                _ => return,
            }
            (sibling_page_id, node_page_id, child_idx)
        } else {
            // 右兄弟并入当前(node)
            match (&mut node, &mut sibling) {
                (
                    BTreeNode::Internal {
                        keys: left_keys,
                        children: left_children,
                    },
                    BTreeNode::Internal {
                        keys: right_keys,
                        children: right_children,
                    },
                ) => {
                    left_keys.push(parent_key);
                    left_keys.append(right_keys);
                    left_children.append(right_children);
                }
                _ => return,
            }
            (node_page_id, sibling_page_id, child_idx + 1)
        };

        if let BTreeNode::Internal { children, .. } = &mut parent {
            children.remove(delete_child_idx);
        }

        let survivor = if survivor_id == node_page_id {
            &node
        } else {
            &sibling
        };
        self.write_node(survivor_id, survivor);
        self.write_node(parent_page_id, &parent);
        self.page_manager.free_page(dead_id);
    }

    /// 处理下溢：能借则借，否则合并；合并后递归处理父节点下溢
    fn handle_underflow(&mut self, node_page_id: PageId, parent_page_id: PageId) {
        if node_page_id == self.root {
            return;
        }

        let node = self.read_node(node_page_id);
        if !self.is_underflow(&node) {
            return;
        }

        let Some(child_idx) = self.get_parent_index(parent_page_id, node_page_id) else {
            return;
        };
        let (sibling_opt, is_left_sibling) = self.get_sibling(parent_page_id, child_idx);
        let Some(sibling_page_id) = sibling_opt else {
            return;
        };

        let sibling = self.read_node(sibling_page_id);
        let sibling_has_extra = Self::node_key_count(&sibling) > self.min_entries();

        let merged = if sibling_has_extra {
            match &node {
                BTreeNode::Leaf { .. } => {
                    self.leaf_borrow_key(
                        node_page_id,
                        sibling_page_id,
                        is_left_sibling,
                        parent_page_id,
                        child_idx,
                    );
                }
                BTreeNode::Internal { .. } => {
                    self.internal_borrow_key(
                        node_page_id,
                        sibling_page_id,
                        is_left_sibling,
                        parent_page_id,
                        child_idx,
                    );
                }
            }
            false
        } else {
            match &node {
                BTreeNode::Leaf { .. } => {
                    self.leaf_merge(
                        node_page_id,
                        sibling_page_id,
                        is_left_sibling,
                        parent_page_id,
                        child_idx,
                    );
                }
                BTreeNode::Internal { .. } => {
                    self.internal_merge(
                        node_page_id,
                        sibling_page_id,
                        is_left_sibling,
                        parent_page_id,
                        child_idx,
                    );
                }
            }
            true
        };

        // 合并后父节点可能下溢：用 DFS 找祖父，正确向上传播
        if merged && parent_page_id != self.root {
            if let Some(grandparent) = self.find_parent_of(parent_page_id) {
                self.handle_underflow(parent_page_id, grandparent);
            }
        } else if merged && parent_page_id == self.root {
            self.shrink_root_if_needed();
        }
    }

    /// 递归删除；parent_page_id 在下降时传入，避免错误的父节点查找
    fn delete_recursive(
        &mut self,
        node_page_id: PageId,
        key: Key,
        parent_page_id: Option<PageId>,
    ) -> bool {
        let mut node = self.read_node(node_page_id);

        match &mut node {
            BTreeNode::Leaf { entries, .. } => {
                let Ok(idx) = entries.binary_search_by(|(k, _)| k.cmp(&key)) else {
                    return false;
                };
                entries.remove(idx);
                self.write_node(node_page_id, &node);

                // 非根叶子下溢 → 再平衡
                if node_page_id != self.root {
                    if let Some(parent) = parent_page_id {
                        if self.is_underflow(&node) {
                            self.handle_underflow(node_page_id, parent);
                        }
                    }
                }
                true
            }
            BTreeNode::Internal { keys, children } => {
                let child_idx = keys.iter().position(|k| key < *k).unwrap_or(keys.len());
                let child_page_id = children[child_idx];
                let deleted = self.delete_recursive(child_page_id, key, Some(node_page_id));
                if !deleted {
                    return false;
                }

                // 子树删除后，若分隔键对应子树最小键变化则更新
                // keys[i] 是 children[i+1] 子树的下界 → 更新 keys[child_idx - 1]
                if child_idx > 0 {
                    if let Some(min_key) = self.subtree_min_key(child_page_id) {
                        // 重新读取：子路径可能已合并/借键，本节点可能已变
                        // 若本节点在合并中被释放则跳过
                        if self.page_manager.read_page(node_page_id).is_some() {
                            let mut cur = self.read_node(node_page_id);
                            if let BTreeNode::Internal {
                                keys: ks,
                                children: ch,
                            } = &mut cur
                            {
                                // child 可能已变下标，重新定位
                                if let Some(new_idx) = ch.iter().position(|&c| c == child_page_id) {
                                    if new_idx > 0 && new_idx - 1 < ks.len() {
                                        ks[new_idx - 1] = min_key;
                                        self.write_node(node_page_id, &cur);
                                    }
                                }
                            }
                        }
                    }
                }

                // 检查本节点是否下溢（合并子节点后）
                if node_page_id != self.root {
                    if let Some(parent) = parent_page_id {
                        if self.page_manager.read_page(node_page_id).is_some() {
                            let cur = self.read_node(node_page_id);
                            if self.is_underflow(&cur) {
                                self.handle_underflow(node_page_id, parent);
                            }
                        }
                    }
                } else {
                    self.shrink_root_if_needed();
                }

                true
            }
        }
    }

    /// 子树最小键（一直向左走到叶子）
    fn subtree_min_key(&mut self, node_page_id: PageId) -> Option<Key> {
        // 节点可能在合并中被释放
        let data = self.page_manager.read_page(node_page_id)?;
        let node: BTreeNode = bincode::deserialize(&data).ok()?;
        match node {
            BTreeNode::Leaf { entries, .. } => entries.first().map(|(k, _)| k.clone()),
            BTreeNode::Internal { children, .. } => children
                .first()
                .copied()
                .and_then(|c| self.subtree_min_key(c)),
        }
    }
}

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
    fn find_leaf_for_key(&mut self, node_page_id: PageId, key: Key) -> Option<PageId> {
        let node_data = self.page_manager.read_page(node_page_id).unwrap();
        let node: BTreeNode = bincode::deserialize(&node_data).unwrap();

        match node {
            BTreeNode::Leaf { .. } => Some(node_page_id),
            BTreeNode::Internal { keys, children } => {
                let child_idx = keys
                    .iter()
                    .position(|k| key < k.clone())
                    .unwrap_or(keys.len());
                self.find_leaf_for_key(children[child_idx], key)
            }
        }
    }
}

pub struct BPlusTreeIterator<'a> {
    tree: &'a mut BPlusTree,
    current_page_id: Option<PageId>,
    current_idx: usize,
}

impl BPlusTree {
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

        // 读取当前节点
        let node_data = self.tree.page_manager.read_page(page_id)?;
        let node: BTreeNode = bincode::deserialize(&node_data).ok()?;

        if let BTreeNode::Leaf { entries, next_leaf } = node {
            if self.current_idx < entries.len() {
                // 返回当前条目并自增索引
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

// 为BTreeNode添加as_ref方法（方便获取叶子节点的next_leaf）
impl AsRef<BTreeNode> for BTreeNode {
    fn as_ref(&self) -> &Self {
        self
    }
}

// 格式化输出
impl fmt::Display for BPlusTree {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "B+Tree(order={}, root_page_id={})",
            self.order, self.root.0
        )
    }
}

// ===================== 测试代码 =====================
#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;

    fn rid(v: u8) -> RowId {
        RowId(Some(vec![v]))
    }

    fn key(n: u8) -> Key {
        vec![n]
    }

    fn collect_all(tree: &mut BPlusTree) -> Vec<u8> {
        tree.iter().map(|(k, _)| k[0]).collect()
    }

    #[test]
    fn test_insert_get_update_range() {
        let mut tree = BPlusTree::new(4, 16);
        for n in [5u8, 10, 15, 20, 25, 30, 35] {
            tree.insert(key(n), rid(n));
        }

        assert_eq!(tree.get(key(10)).unwrap().0, Some(vec![10]));
        assert!(tree.get(key(99)).is_none());

        assert!(tree.update(key(10), rid(99)));
        assert_eq!(tree.get(key(10)).unwrap().0, Some(vec![99]));

        let range: Vec<u8> = tree
            .range_scan(key(5), key(20))
            .into_iter()
            .map(|(k, _)| k[0])
            .collect();
        assert_eq!(range, vec![5, 10, 15, 20]);
    }

    #[test]
    fn test_delete_and_rebalance() {
        let mut tree = BPlusTree::new(4, 32);
        // 插入足够多以触发多层
        let keys: Vec<u8> = (1..=30).collect();
        for &n in &keys {
            tree.insert(key(n), rid(n));
        }
        assert_eq!(collect_all(&mut tree), keys);

        // 删除中间键，触发借键/合并
        for n in [10u8, 11, 12, 5, 6, 7, 8, 9, 15, 16, 17, 18, 19, 20] {
            assert!(tree.delete(key(n)), "delete {n} should succeed");
            assert!(tree.get(key(n)).is_none(), "key {n} should be gone");
        }

        let remaining: Vec<u8> = (1..=30u8)
            .filter(|n| ![10, 11, 12, 5, 6, 7, 8, 9, 15, 16, 17, 18, 19, 20].contains(n))
            .collect();
        assert_eq!(collect_all(&mut tree), remaining);

        // 再删光
        for n in remaining {
            assert!(tree.delete(key(n)));
        }
        assert!(collect_all(&mut tree).is_empty());
    }

    #[test]
    fn test_delete_missing_key() {
        let mut tree = BPlusTree::new(4, 8);
        tree.insert(key(1), rid(1));
        assert!(!tree.delete(key(2)));
        assert!(tree.delete(key(1)));
        assert!(!tree.delete(key(1)));
    }

    #[test]
    fn test_iter_order() {
        let mut tree = BPlusTree::new(4, 16);
        for n in [35u8, 10, 25, 5, 30, 15, 20] {
            tree.insert(key(n), rid(n));
        }
        assert_eq!(collect_all(&mut tree), vec![5, 10, 15, 20, 25, 30, 35]);
        tree.flush_all();
    }

    #[test]
    fn test_overwrite_on_insert() {
        let mut tree = BPlusTree::new(4, 8);
        tree.insert(key(1), rid(1));
        tree.insert(key(1), rid(2));
        assert_eq!(tree.get(key(1)).unwrap().0, Some(vec![2]));
        assert_eq!(collect_all(&mut tree).len(), 1);
    }
}
