//! 本文件负责节点借位与合并。

use super::*;

impl RebalanceInput {
    pub(super) fn load(
        self,
        tree: &mut BPlusTree,
        first: PageId,
        second: PageId,
        parent: PageId,
    ) -> (BTreeNode, BTreeNode, BTreeNode) {
        match self {
            Self::Fresh {
                first,
                second,
                parent,
            } => {
                #[cfg(test)]
                REBALANCE_REUSES.with(|c| c.set(c.get() + 1));
                (first, second, parent)
            }
            Self::Reload => {
                #[cfg(test)]
                REBALANCE_RELOADS.with(|c| c.set(c.get() + 1));
                (
                    tree.read_node(first),
                    tree.read_node(second),
                    tree.read_node(parent),
                )
            }
        }
    }

    pub(super) fn swap(self) -> Self {
        match self {
            Self::Fresh {
                first,
                second,
                parent,
            } => Self::Fresh {
                first: second,
                second: first,
                parent,
            },
            Self::Reload => Self::Reload,
        }
    }
}

impl BPlusTree {
    /// 最小填充数（⌈order/2⌉ - 1）；order=4 → min=1
    pub(super) fn min_entries(&self) -> usize {
        (self.order + 1) / 2 - 1
    }

    pub(super) fn node_key_count(node: &BTreeNode) -> usize {
        match node {
            BTreeNode::Leaf { entries, .. } => entries.len(),
            BTreeNode::Internal { keys, .. } => keys.len(),
        }
    }

    pub(super) fn is_underflow(&self, node: &BTreeNode) -> bool {
        Self::node_key_count(node) < self.min_entries()
    }

    /// 在父节点中查找 child 的下标
    #[cfg(test)]
    pub(super) fn get_parent_index(
        &mut self,
        parent_page_id: PageId,
        child_page_id: PageId,
    ) -> Option<usize> {
        match self.read_node(parent_page_id) {
            BTreeNode::Internal { children, .. } => {
                children.iter().position(|&pid| pid == child_page_id)
            }
            _ => None,
        }
    }

    /// 优先右兄弟，否则左兄弟；返回 (sibling_id, is_left_sibling)
    #[cfg(test)]
    pub(super) fn get_sibling(
        &mut self,
        parent_page_id: PageId,
        child_idx: usize,
    ) -> (Option<PageId>, bool) {
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
    // M56b：**这里原本有一个 `find_parent_of` / `find_parent_of_rec`** —— 从根开始、把整棵树
    // 逐页读一遍去找"某个节点的父节点"。200k 档实测它每次合并都要扫 2820 页、占每删除行页读的 77%，
    // 而合并只让"回溯栈上的下一帧"少一个孩子 ⇒ 那一帧自己就会查下溢，`delete()` 收尾管根降层。
    // 删掉它 ⇒ `页·找祖父` 那一只桶从此恒为 0（台架与守卫都对着这一格）。

    pub(super) fn shrink_root_if_needed(&mut self) {
        // 单独一只桶（M56）：这一句问的是"根要不要降层"—— 每行删除都会被叫到，而它每次都把根页
        // 读一遍。这与"真的在做重组"不是一回事，所以单独记账、单独归段。
        #[cfg(test)]
        note_root_check!();
        set_page_phase(PHASE_ROOT_CHECK);
        match self.read_node(self.root) {
            BTreeNode::Internal { children, .. } if children.len() == 1 => {
                let old_root = self.root;
                self.set_root(children[0]);
                self.page_manager.free_page(old_root);
            }
            _ => {}
        }
        set_page_phase(PHASE_BACKTRACK);
    }

    /// 叶子：向兄弟借一条记录；不改动 next_leaf 链表
    pub(super) fn leaf_borrow_key(
        &mut self,
        node_page_id: PageId,
        sibling_page_id: PageId,
        is_left_sibling: bool,
        parent_page_id: PageId,
        child_idx: usize,
        input: RebalanceInput,
    ) -> bool {
        let (mut node, mut sibling, mut parent) =
            input.load(self, node_page_id, sibling_page_id, parent_page_id);

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
            _ => return false,
        }

        // 借完之后三个节点都必须还装得下一页，否则**这一步作废**（locals 丢弃、盘不动），
        // 由调用方退到整对重排。下溢本身不是错误，只是填充率低一点；写失败的静默丢页才是。
        if !Self::node_fits(&node) || !Self::node_fits(&sibling) || !Self::node_fits(&parent) {
            return false;
        }
        self.write_node(node_page_id, &node);
        self.write_node(sibling_page_id, &sibling);
        self.write_node(parent_page_id, &parent);
        true
    }

    /// 相邻两叶重排：union 装得下一页就合并，装不下就按字节**重切成两个各装得下的半页**。
    ///
    /// 为什么必须有"重切"这一支：`order=64` 在 40B 键 + 60B 值下两个半页的 union 就是
    /// 7.5 KB > 4080 B ⇒ 原先只有「合并」与「装不下就整次作废」两种结局，于是 delete 密集之后
    /// 下溢的叶子永远留着、填充率一路掉（实测 `vacuum()` 206 → 466 ms）。重切不释放页、
    /// 不动父子结构，只把父节点那把分隔键换成新右半页的最小键 ⇒ 行数一个不少，填充率回到 ~50%。
    ///
    /// 落盘前一律用 `node_fits`（真实序列化）复核：字节公式只用来**选分界**，
    /// 公式与真实不符时宁可留下溢，也不写一个装不下的页。
    pub(super) fn rebalance_leaf_pair(
        &mut self,
        left_page: PageId,
        right_page: PageId,
        parent_page_id: PageId,
        sep_idx: usize,
        input: RebalanceInput,
    ) -> PairReorg {
        let (left, right, parent) = input.load(self, left_page, right_page, parent_page_id);
        let (left_entries, left_next, right_entries, right_next) = match (left, right) {
            (
                BTreeNode::Leaf {
                    entries: a,
                    next_leaf: l,
                },
                BTreeNode::Leaf {
                    entries: b,
                    next_leaf: r,
                },
            ) => (a, l, b, r),
            _ => return PairReorg::Nothing,
        };
        let (mut parent_keys, parent_children) = match parent {
            BTreeNode::Internal { keys, children } => (keys, children),
            _ => return PairReorg::Nothing,
        };
        // 形状核对：这对页必须是父里相邻的两个子节点，**且叶子链也这么认为**。
        // 三样里任何一样对不上就什么都不动 —— 照着错结构改盘，比留下溢严重得多。
        if left_next != Some(right_page)
            || parent_children.len() != parent_keys.len() + 1
            || parent_children.get(sep_idx) != Some(&left_page)
            || parent_children.get(sep_idx + 1) != Some(&right_page)
        {
            return PairReorg::Nothing;
        }

        let tail = right_next;
        let mut union = left_entries;
        union.extend(right_entries);
        // 每条的字节由探针常量导出（`RowId` 是 `Option<Vec<u8>>` ⇒ 变长，手算硬编码错一次就是
        // 写坏页，所以常量本身是量出来的，并由逐档对拍钉住）
        let cap = MAX_PAYLOAD as i64;
        // 左半页的下一叶就是右半页，右半页接原本右页的下一叶 ⇒ 两个 base 分开算
        let base_left = Self::leaf_base(Some(right_page));
        let base_right = Self::leaf_base(tail);
        // 前缀和：`prefix[i]` = 前 i 条的字节和 ⇒ 任意半页的字节都是 O(1) 拿到
        let prefix = Self::prefix_bytes(&union, Self::entry_bytes);
        let left_bytes = |i: usize| -> i64 { base_left + prefix[i] };
        let right_bytes = |i: usize| -> i64 { base_right + (prefix[union.len()] - prefix[i]) };

        // ① union 整个装得下一页 ⇒ 真合并（释放右页）。合并后的 next_leaf 就是 tail ⇒
        //    用 base_right 算，别用 base_left（差一个 Option 就是差一个字节档）
        if union.len() < self.order && base_right + prefix[union.len()] <= cap {
            let merged = BTreeNode::Leaf {
                entries: union,
                next_leaf: tail,
            };
            let mut children = parent_children;
            parent_keys.remove(sep_idx);
            children.remove(sep_idx + 1);
            let parent = BTreeNode::Internal {
                keys: parent_keys,
                children,
            };
            if !Self::node_fits(&merged) || !Self::node_fits(&parent) {
                return PairReorg::Nothing;
            }
            self.write_node(left_page, &merged);
            self.write_node(parent_page_id, &parent);
            self.page_manager.free_page(right_page);
            return PairReorg::Merged;
        }

        // ② 装不下 ⇒ 找"两半都装得下"的分界。前缀随 i 变胖、后缀随 i 变瘦 ⇒ 可行档位是
        //    一段**连续区间**，一路扫过去记住"最后一个后缀也装得下的档"就是它的最优端。
        let n = union.len();
        // 两半的条数都必须 < order：那是"以后还分裂得掉"的前提，不只是经典 B+ 树规矩
        // （`write_node` 只看字节，条数超了之后任何一次插入都分裂不开 ⇒ 必然超载写页）。
        let k_from = 1usize.max(n.saturating_sub(self.order - 1));
        let k_to = (self.order - 1).min(n - 1);
        let (_lo, hi) =
            match Self::fitting_split(k_from, k_to, cap, &|k| (left_bytes(k), right_bytes(k))) {
                Some(range) => range,
                None => return PairReorg::Nothing,
            };
        // 取**填得最满的那一档**（`hi`），不是中点：中点切法把两片都停在半满，
        // 页数量永远降不下来（这正是 466 ms 那张单的来源）。把左页压到底，剩下的
        // 挤成一小片 ⇒ 它下一次就能和**自己的**右兄弟合并成功 ⇒ 释放一页。
        // 空位不会丢：一路往库尾排，最后合并掉的就是最右那几片。
        let i = hi;
        let right_side = union.split_off(i);
        let sep_key = right_side[0].0.clone();
        let left_node = BTreeNode::Leaf {
            entries: union,
            next_leaf: Some(right_page),
        };
        let right_node = BTreeNode::Leaf {
            entries: right_side,
            next_leaf: tail,
        };
        let children = parent_children;
        parent_keys[sep_idx] = sep_key;
        let parent = BTreeNode::Internal {
            keys: parent_keys,
            children,
        };
        if !Self::node_fits(&left_node)
            || !Self::node_fits(&right_node)
            || !Self::node_fits(&parent)
        {
            return PairReorg::Nothing;
        }
        self.write_node(left_page, &left_node);
        self.write_node(right_page, &right_node);
        self.write_node(parent_page_id, &parent);
        PairReorg::Resplit
    }

    /// 内部节点：向兄弟借键（经典旋转）
    pub(super) fn internal_borrow_key(
        &mut self,
        node_page_id: PageId,
        sibling_page_id: PageId,
        is_left_sibling: bool,
        parent_page_id: PageId,
        child_idx: usize,
        input: RebalanceInput,
    ) -> bool {
        let (mut node, mut sibling, mut parent) =
            input.load(self, node_page_id, sibling_page_id, parent_page_id);

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
            _ => return false,
        }

        // 借完之后三个节点都必须还装得下一页，否则**这一步作废**（locals 丢弃、盘不动），
        // 由调用方退到整对重排。下溢本身不是错误，只是填充率低一点；写失败的静默丢页才是。
        if !Self::node_fits(&node) || !Self::node_fits(&sibling) || !Self::node_fits(&parent) {
            return false;
        }
        self.write_node(node_page_id, &node);
        self.write_node(sibling_page_id, &sibling);
        self.write_node(parent_page_id, &parent);
        true
    }

    /// 相邻两个内部节点重排：union 装得下一页就合并，装不下就按字节重切成两页。
    ///
    /// 与叶子的区别只在"分隔键是搬下来的、不是复制的"：内部节点的分隔键住在节点里，所以
    /// union 要把父那把键**拉下来**一起排，重切时再把新边界那把键**提回**父 —— 全程系统的
    /// 键数与子节点数都守恒（`k + 1 + m` 把键变 `i + 1 + (k+m-i)` 把）。
    /// 两侧都要求**至少一把键**（`i ∈ [1, n-2]`）：留一个空键的内部节点会让 `shrink_root_if_needed`
    /// 和下降逻辑多出没必要的分支。
    pub(super) fn rebalance_internal_pair(
        &mut self,
        left_page: PageId,
        right_page: PageId,
        parent_page_id: PageId,
        sep_idx: usize,
        input: RebalanceInput,
    ) -> PairReorg {
        let (left, right, parent) = input.load(self, left_page, right_page, parent_page_id);
        let (left_keys, left_children) = match left {
            BTreeNode::Internal { keys, children } => (keys, children),
            _ => return PairReorg::Nothing,
        };
        let (right_keys, right_children) = match right {
            BTreeNode::Internal { keys, children } => (keys, children),
            _ => return PairReorg::Nothing,
        };
        let (mut parent_keys, parent_children) = match parent {
            BTreeNode::Internal { keys, children } => (keys, children),
            _ => return PairReorg::Nothing,
        };
        if parent_children.len() != parent_keys.len() + 1
            || parent_children.get(sep_idx) != Some(&left_page)
            || parent_children.get(sep_idx + 1) != Some(&right_page)
        {
            return PairReorg::Nothing;
        }

        let sep = parent_keys[sep_idx].clone();
        let mut union_keys = left_keys;
        let mut union_children = left_children;
        // 父那把键大于左半全部、小于右半全部 ⇒ 接在左半末尾，再拼上右半
        union_keys.push(sep);
        union_keys.extend(right_keys);
        union_children.extend(right_children);

        let base = Self::internal_base();
        let cu = Self::internal_child_unit();
        let cap = MAX_PAYLOAD as i64;
        let n = union_keys.len();
        let pre = Self::prefix_bytes(&union_keys, Self::key_bytes);
        // 左半拿 keys[..i] + children[..i+1]，右半拿 keys[i+1..] + children[i+1..]
        // （keys[i] 本身提回父当分隔键 ⇒ 不进任何一半）
        let left_bytes = |i: usize| -> i64 { base + pre[i] + (i as i64 + 1) * cu };
        let right_bytes =
            |i: usize| -> i64 { base + (pre[n] - pre[i + 1]) + (n as i64 - i as i64) * cu };

        // ① union 整个装得下一页 ⇒ 真合并（父丢一把键与一个子节点，右页释放）
        //    合并后的节点就是 n 把键 + n+1 个子，正好等于 `left_bytes(n)`
        if n < self.order && left_bytes(n) <= cap {
            let merged = BTreeNode::Internal {
                keys: union_keys,
                children: union_children,
            };
            let mut children = parent_children;
            parent_keys.remove(sep_idx);
            children.remove(sep_idx + 1);
            let parent = BTreeNode::Internal {
                keys: parent_keys,
                children,
            };
            if !Self::node_fits(&merged) || !Self::node_fits(&parent) {
                return PairReorg::Nothing;
            }
            self.write_node(left_page, &merged);
            self.write_node(parent_page_id, &parent);
            self.page_manager.free_page(right_page);
            return PairReorg::Merged;
        }

        // ② 装不下 ⇒ 两半都装得下的 i 是一段连续区间（左随 i 变胖、右随 i 变瘦），取中点
        if n < 3 {
            // 至少要"左 1 把 + 提回父 1 把 + 右 1 把"才切得开
            return PairReorg::Nothing;
        }
        // 两侧都至少留一把键 ⇒ k ∈ [1, n-2]；条数上限同叶子（分裂后插不进）
        let k_from = 1usize.max(n.saturating_sub(self.order));
        let k_to = (self.order - 1).min(n.saturating_sub(2));
        let (_lo, hi) =
            match Self::fitting_split(k_from, k_to, cap, &|k| (left_bytes(k), right_bytes(k))) {
                Some(range) => range,
                None => return PairReorg::Nothing,
            };
        // 同叶子：取填得最满的那一档，把剩余挤成一小片好让后续合并成功
        let i = hi;

        let new_sep = union_keys[i].clone();
        let right_children = union_children.split_off(i + 1);
        let left_keys = union_keys[..i].to_vec();
        let right_keys = union_keys[i + 1..].to_vec();
        let left_node = BTreeNode::Internal {
            keys: left_keys,
            children: union_children,
        };
        let right_node = BTreeNode::Internal {
            keys: right_keys,
            children: right_children,
        };
        let children = parent_children;
        parent_keys[sep_idx] = new_sep;
        let parent = BTreeNode::Internal {
            keys: parent_keys,
            children,
        };
        if !Self::node_fits(&left_node)
            || !Self::node_fits(&right_node)
            || !Self::node_fits(&parent)
        {
            return PairReorg::Nothing;
        }
        self.write_node(left_page, &left_node);
        self.write_node(right_page, &right_node);
        self.write_node(parent_page_id, &parent);
        PairReorg::Resplit
    }

    /// 处理下溢：能借则借，否则整对重排（合并或按字节重切）。
    ///
    /// 返回**是否真释放了一页**（`PairReorg::Merged`）。M56b 起这件事只往上"报告"，不再自己往上处理：
    /// - 改前它合并完会 `find_parent_of(parent)` —— **从根走一趟整棵树的 DFS** —— 再对父节点做下溢处理。
    ///   200k 档实测这一格占**每删除行 39.69 页（77%）**，每趟 DFS 扫 2820 页，而该档的缺页(38.76)就是它扫出来的；
    /// - 而那是冗余的：合并只让"**这一层的父节点**"少一个孩子，而那个父节点正是 `delete_recursive`
    ///   回溯栈上的下一帧 —— 它紧接着就做自己那一层的下溢检查（`is_underflow(&cur)`），
    ///   一路到根；根降层由 `delete()` 收尾。⇒ DFS 归零，`真下溢/借位/合并` 三个计数一字不差。
    #[cfg(test)]
    pub(super) fn handle_underflow_reference(
        &mut self,
        node_page_id: PageId,
        parent_page_id: PageId,
    ) -> bool {
        if node_page_id == self.root {
            return false;
        }

        let node = self.read_node(node_page_id);
        if !self.is_underflow(&node) {
            return false;
        }
        // 走到这里才是"真的要做一次重组"（M56 的数：每行删除都会问一句下溢，但只有这里动手）
        #[cfg(test)]
        note_underflow!();

        let Some(child_idx) = self.get_parent_index(parent_page_id, node_page_id) else {
            return false;
        };
        let (sibling_opt, is_left_sibling) = self.get_sibling(parent_page_id, child_idx);
        let Some(sibling_page_id) = sibling_opt else {
            return false;
        };

        let sibling = self.read_node(sibling_page_id);
        let sibling_has_extra = Self::node_key_count(&sibling) > self.min_entries();

        // 先试最便宜的一招：只动两页 + 一把分隔键。借不动（兄弟条数不够）或**借完装不下**，
        // 都退到整对重排 —— 原先借完装不下就整次作废，等于"宁可一直下溢也不重排"。
        let is_leaf = matches!(&node, BTreeNode::Leaf { .. });
        if sibling_has_extra {
            let borrowed = if is_leaf {
                self.leaf_borrow_key(
                    node_page_id,
                    sibling_page_id,
                    is_left_sibling,
                    parent_page_id,
                    child_idx,
                    RebalanceInput::Reload,
                )
            } else {
                self.internal_borrow_key(
                    node_page_id,
                    sibling_page_id,
                    is_left_sibling,
                    parent_page_id,
                    child_idx,
                    RebalanceInput::Reload,
                )
            };
            // 借不释放页 ⇒ 父节点的子数不变 ⇒ 不会连锁下溢
            if borrowed {
                #[cfg(test)]
                note_borrow!();
                return false;
            }
        }

        // 相邻两页按 key 顺序是「左在右之前」，重排只需要这一对页号 + 它们之间那把分隔键
        let (left_page, right_page, sep_idx) = if is_left_sibling {
            (sibling_page_id, node_page_id, child_idx - 1)
        } else {
            (node_page_id, sibling_page_id, child_idx)
        };
        let reorg = if is_leaf {
            self.rebalance_leaf_pair(
                left_page,
                right_page,
                parent_page_id,
                sep_idx,
                RebalanceInput::Reload,
            )
        } else {
            self.rebalance_internal_pair(
                left_page,
                right_page,
                parent_page_id,
                sep_idx,
                RebalanceInput::Reload,
            )
        };
        let merged = reorg == PairReorg::Merged;
        #[cfg(test)]
        if merged {
            note_merge!();
        }
        // 合并过 ⇒ 父节点少了一个孩子：往上**报告**，由父节点自己那一帧去查下溢、由 `delete()` 收尾降根
        merged
    }

    pub(super) fn handle_underflow(
        &mut self,
        node_page_id: PageId,
        parent_page_id: PageId,
    ) -> bool {
        #[cfg(test)]
        if rebalance_read_reference::enabled() {
            return self.handle_underflow_reference(node_page_id, parent_page_id);
        }
        if node_page_id == self.root {
            return false;
        }

        let node = self.read_node(node_page_id);
        if !self.is_underflow(&node) {
            return false;
        }
        // 走到这里才是"真的要做一次重组"（M56 的数：每行删除都会问一句下溢，但只有这里动手）
        #[cfg(test)]
        note_underflow!();

        let parent = self.read_node(parent_page_id);
        let BTreeNode::Internal { children, .. } = &parent else {
            return false;
        };
        let Some(child_idx) = children.iter().position(|&id| id == node_page_id) else {
            return false;
        };
        let (sibling_page_id, is_left_sibling) = if let Some(&right) = children.get(child_idx + 1) {
            (right, false)
        } else if child_idx > 0 {
            (children[child_idx - 1], true)
        } else {
            return false;
        };

        let sibling = self.read_node(sibling_page_id);
        let sibling_has_extra = Self::node_key_count(&sibling) > self.min_entries();

        // 先试最便宜的一招：只动两页 + 一把分隔键。借不动（兄弟条数不够）或**借完装不下**，
        // 都退到整对重排 —— 原先借完装不下就整次作废，等于"宁可一直下溢也不重排"。
        let is_leaf = matches!(&node, BTreeNode::Leaf { .. });
        let mut input = RebalanceInput::Fresh {
            first: node,
            second: sibling,
            parent,
        };
        if sibling_has_extra {
            let borrow_input = std::mem::replace(&mut input, RebalanceInput::Reload);
            let borrowed = if is_leaf {
                self.leaf_borrow_key(
                    node_page_id,
                    sibling_page_id,
                    is_left_sibling,
                    parent_page_id,
                    child_idx,
                    borrow_input,
                )
            } else {
                self.internal_borrow_key(
                    node_page_id,
                    sibling_page_id,
                    is_left_sibling,
                    parent_page_id,
                    child_idx,
                    borrow_input,
                )
            };
            // 借不释放页 ⇒ 父节点的子数不变 ⇒ 不会连锁下溢
            if borrowed {
                #[cfg(test)]
                note_borrow!();
                return false;
            }
        }

        // 相邻两页按 key 顺序是「左在右之前」，重排只需要这一对页号 + 它们之间那把分隔键
        let (left_page, right_page, sep_idx) = if is_left_sibling {
            input = input.swap();
            (sibling_page_id, node_page_id, child_idx - 1)
        } else {
            (node_page_id, sibling_page_id, child_idx)
        };
        let reorg = if is_leaf {
            self.rebalance_leaf_pair(left_page, right_page, parent_page_id, sep_idx, input)
        } else {
            self.rebalance_internal_pair(left_page, right_page, parent_page_id, sep_idx, input)
        };
        let merged = reorg == PairReorg::Merged;
        #[cfg(test)]
        if merged {
            note_merge!();
        }
        // 合并过 ⇒ 父节点少了一个孩子：往上**报告**，由父节点自己那一帧去查下溢、由 `delete()` 收尾降根
        merged
    }
}
