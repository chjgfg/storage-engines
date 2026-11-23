//! 本文件负责统计与计量。

use super::*;

impl BPlusTree {
    /// 树的形状（**测试/台架专用**）：`(树高, 内部节点数, 叶子数, 文件里的总页数)`。
    ///
    /// M22 需要先知道"一次随机点读要走几层、上层页占全库多少"才能定缓存策略 ——
    /// 上层是**结构性小**（叶子数 / order 量级）还是**跟库一起长**，决定了两件完全不同的事：
    /// 前者该常驻（钉住，代价与库大小无关），后者才需要谈容量与覆盖率。
    /// 走的是解析缓存，会计入 `page_reads` / `node_parses` ⇒ 台架里要在打点**之前**调它。
    #[cfg(test)]
    pub fn shape(&mut self) -> (u64, usize, usize, u64) {
        fn go(t: &mut BPlusTree, id: PageId, depth: u64, st: &mut (u64, usize, usize)) {
            st.0 = st.0.max(depth);
            match t.node_of(id) {
                Some(node) => match &*node {
                    BTreeNode::Internal { keys, children } => {
                        st.1 += 1;
                        let _ = keys;
                        for c in children.iter() {
                            go(t, *c, depth + 1, st);
                        }
                    }
                    BTreeNode::Leaf { .. } => st.2 += 1,
                },
                None => {}
            }
        }
        let mut st = (0u64, 0usize, 0usize);
        let root = self.root;
        go(self, root, 1, &mut st);
        (st.0, st.1, st.2, self.page_manager.next_page_id())
    }
}

/// 测试专用：叶子层形状画像。**重组有没有真的把空位收掉，只能按页数得出来** ——
/// 行数在任何一种结局下都不变，所以"没丢数据"那类断言对这件事完全不敏感。
#[cfg(test)]
impl BPlusTree {
    /// `(叶子片数, 行数, 单片最少条数, 单片最多条数)`；沿 `next_leaf` 走到底
    pub(crate) fn leaf_profile(&mut self) -> (usize, usize, usize, usize) {
        let mut cur = self.root;
        let (mut leaves, mut rows, mut min, mut max) = (0usize, 0usize, usize::MAX, 0usize);
        loop {
            let next = match self.read_node(cur) {
                BTreeNode::Internal { children, .. } => {
                    cur = *children.first().expect("内部节点没有子节点");
                    continue;
                }
                BTreeNode::Leaf {
                    entries, next_leaf, ..
                } => {
                    leaves += 1;
                    rows += entries.len();
                    min = min.min(entries.len());
                    max = max.max(entries.len());
                    next_leaf
                }
            };
            match next {
                Some(p) => cur = p,
                None => break,
            }
        }
        (leaves, rows, if min == usize::MAX { 0 } else { min }, max)
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
