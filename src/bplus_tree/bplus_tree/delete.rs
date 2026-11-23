//! 本文件负责删除与分隔键维护。

use super::*;

impl Del {
    const NOOP: Del = Del {
        removed: 0,
        min_changed: false,
        merged: false,
    };

    /// 什么都没动 ⇒ 祖先各层都不必回溯、也不必问根降层
    pub(super) fn is_noop(&self) -> bool {
        self.removed == 0 && !self.min_changed && !self.merged
    }
}

impl BPlusTree {
    /// 删除：移除指定 key，必要时借键/合并并处理根降层
    pub fn delete(&mut self, key: Key) -> bool {
        if self.ensure_healthy().is_err() {
            return false;
        }
        let (res, _) = self.delete_recursive(self.root, std::slice::from_ref(&key), None);

        // 根是内部节点且只剩一个子节点时降层 —— **只在本次删除真合并过一页时才问**（M56b）。
        // 没合并过 ⇒ 根的孩子数不可能变 ⇒ 那一次根页读与解析纯属白付。
        if res.merged {
            set_page_phase(PHASE_RESTRUCTURE);
            self.shrink_root_if_needed();
        }
        // M56：一趟删除结束把阶段清空 ⇒ 下一趟的桶不会串到这一趟上
        set_page_phase(PHASE_OTHER);
        res.removed > 0 && self.ensure_healthy().is_ok()
    }

    /// 批量删除（M61b）：`keys` **必须按升序给出**，同一叶子里的连续多行共用一趟下降。
    ///
    /// 为什么只在 vacuum 那条路上用：M61 读到的上限是 **k = 16.5~17.9 行/叶子**（可删行是"被更新过的
    /// 那段连续键"留下的旧版本，天然成块），而日常随机删除 k = 1 ⇒ 一行都省不到。
    /// 所以这一刀的收益**只在成块删除的形状里**，单键 [`Self::delete`] 走的是同一条递归
    /// （`run` 只有一个元素）⇒ 行为与页读账与改前一字不差。
    ///
    /// 返回真删掉的行数；不存在的键不计、也不报错（与 `delete` 返回 `false` 同口径）。
    /// ⚠️ 乱序输入不会损坏数据（每片叶子里都按序二分），但会退化成"每行一趟下降"—— 白跑一趟树，
    ///   所以顺序是调用方的义务；这里不 assert（release 里 assert 也要钱），由守卫 `delete_leaf_runs` 的 k 读数盯住。
    pub fn delete_batch(&mut self, keys: &[Key]) -> usize {
        if self.ensure_healthy().is_err() {
            return 0;
        }
        let mut i = 0usize;
        let mut removed = 0usize;
        while i < keys.len() {
            let (res, used) = self.delete_recursive(self.root, &keys[i..], None);
            removed += res.removed;
            // 合并过 ⇒ 根的孩子数可能变：**每片叶子问一次**，不是每行问一次
            if res.merged {
                set_page_phase(PHASE_RESTRUCTURE);
                self.shrink_root_if_needed();
            }
            // 递归承诺 `used ≥ 1`（哪怕整段都不在树里也会推进），所以这里不可能死循环
            i += used.max(1);
            if self.ensure_healthy().is_err() {
                break;
            }
        }
        set_page_phase(PHASE_OTHER);
        removed
    }

    pub(super) fn read_node(&mut self, page_id: PageId) -> BTreeNode {
        // M34 试过把这里改成走 `node_of`（解析缓存），**复测把它退回来了**：600k 档
        // `vacuum()` 全库 12.27 s → **15.12 s**，而同一天没被碰过的 bitcask 那条从 5.79 s 变成
        // 4.52 s（噪声朝反方向）⇒ 比值 2.12× → 3.34× 是这一处带来的。
        // 为什么删除/重组那条路和插入不同：它是**走查** —— 每页基本只碰一次，回填解析缓存换不到
        // 下一次命中，只多付了 `Arc` 分配、HashMap 插入、`pin` 检查与装满之后的整表 `retain`
        // （和 [`crate::bplus_tree::page::PageManager::read_page_for_walk`] 当年"装得下才回填"
        // 是同一个理由）。插入那条路的页会被连续几百行反复用到，所以它值得（见 `insert_recursive`）。
        #[cfg(test)]
        if delete_read_reference::enabled() {
            let data = self.page_manager.read_page(page_id).expect("read page");
            return bincode::deserialize(&data).expect("deserialize node");
        }
        self.page_manager
            .read_page_project(page_id, |data| {
                bincode::deserialize(data).expect("deserialize node")
            })
            .expect("read page")
    }

    /// 写回节点。**装不下就一个字节都不动**，并把失败告诉调用方。
    ///
    /// 为什么必须检查：`PageManager::write_page` 遇到超长 payload 只是打印 + 返回 false，
    /// 而这条链原先把那个 bool 丢了 ⇒ 页保持合并前的旧内容，父节点却已经删掉分隔键、
    /// `free_page` 掉死页 ⇒ 那批行不可达（静默丢数据），而且叶子的 `next_leaf` 还指向已释放页。
    pub(super) fn write_node(&mut self, page_id: PageId, node: &BTreeNode) -> bool {
        if !Self::node_fits(node) {
            return false;
        }
        #[cfg(test)]
        if delete_cost_reference::enabled() {
            crate::common::maintenance_probe::note_bplus_pack_buffer();
            let data = bincode::serialize(node).expect("serialize node");
            return self.page_manager.write_page(page_id, &data);
        }
        self.node_buffer.clear();
        #[cfg(test)]
        if self.node_buffer.capacity() < MAX_PAYLOAD {
            crate::common::maintenance_probe::note_bplus_pack_buffer();
        }
        self.node_buffer.reserve(MAX_PAYLOAD);
        bincode::serialize_into(&mut self.node_buffer, node).expect("serialize node");
        self.page_manager.write_page(page_id, &self.node_buffer)
    }

    /// 递归删除**一段升序键**；`parent_page_id` 在下降时传入，避免错误的父节点查找。
    ///
    /// 返回 `(Del, 这一段里被本层处理掉的键数)`：
    /// - M54b 起 `Del.min_changed` 让父节点**只在这格为真时**才去刷新分隔键。改前是无条件刷新 ——
    ///   那件事是 `subtree_min_key(child)`：**从该子树一路向左下走到叶子**，逐页读、逐页
    ///   `bincode::deserialize`（这一支不走解析缓存），而且**每一层都走一遍**。600k 档实测
    ///   "每删一行读 64.5 页"里可修的那一半就是它（台架 `bplus_vacuum_cost_attribution`）。
    /// - M56b 起 `Del.merged` 是"要不要问根降层"的唯一触发条件。
    /// - M61b 起入参从"一把键"变成"一段升序键"：**同一片叶子里的连续多行共用一趟下降**，
    ///   返回值多带 `removed` 与该层实际处理的键数（用于上层继续推进 / 提前收尾）。
    ///
    /// ⚠️ 两条不变量（守卫 `batched_deletes_really_restructure_and_stay_correct` 对着）：
    /// ① 返回的 `used ≥ 1` —— 一段里第一把键一定会被这一层处理掉（哪怕它不在树里），否则调用方死循环；
    /// ② 本层一旦做过借位/合并（结构变了），**剩下的键交回调用方从根重下一趟**，绝不拿旧的 `children` 快照继续路由。
    pub(super) fn delete_recursive(
        &mut self,
        node_page_id: PageId,
        run: &[Key],
        parent_page_id: Option<PageId>,
    ) -> (Del, usize) {
        #[cfg(test)]
        if delete_cost_reference::enabled() {
            return self.delete_recursive_reference(node_page_id, run, parent_page_id);
        }
        // M56 的段标注：下降那一次读记到"下降"，返回之后本层的所有读记到"回溯"
        set_page_phase(PHASE_DESCENT);
        #[cfg(test)]
        let packed = !delete_leaf_reference::enabled() && !delete_read_reference::enabled();
        #[cfg(not(test))]
        let packed = true;
        // 未排序调用方保留逐受害键二分查找的原路径。
        let input = if packed && run.windows(2).all(|w| w[0] <= w[1]) {
            let output = &mut self.node_buffer;
            self.page_manager
                .read_page_project(node_page_id, |payload| {
                    DeleteRead::project(payload, run, output)
                })
                .expect("read page")
                .expect("decode node")
        } else {
            DeleteRead::Owned(self.read_node(node_page_id))
        };
        let mut node = match input {
            DeleteRead::Owned(node) => node,
            DeleteRead::PackedLeaf {
                remaining,
                removed,
                mut min_changed,
            } => {
                if removed == 0 {
                    return (Del::NOOP, run.len());
                }
                #[cfg(test)]
                for _ in 0..removed {
                    note_delete_leaf(node_page_id);
                }
                if !self
                    .page_manager
                    .write_page(node_page_id, &self.node_buffer)
                {
                    return (Del::NOOP, run.len());
                }
                let mut merged = false;
                if node_page_id != self.root && remaining < self.min_entries() {
                    if let Some(parent) = parent_page_id {
                        set_page_phase(PHASE_RESTRUCTURE);
                        merged = self.handle_underflow(node_page_id, parent);
                        set_page_phase(PHASE_BACKTRACK);
                        min_changed = true;
                    }
                }
                return (
                    Del {
                        removed,
                        min_changed,
                        merged,
                    },
                    run.len(),
                );
            }
        };

        match &mut node {
            BTreeNode::Leaf { entries, .. } => {
                // 父层已经按分隔键把这一段切到"只可能在这片叶子里" ⇒ 这里一定把整段吃掉，used = run.len()
                let mut removed = 0usize;
                let mut touched_min = false;
                for key in run {
                    if let Ok(idx) = entries.binary_search_by(|(k, _)| k.cmp(key)) {
                        // 删的是本片最小键 ⇒ 本片所属子树的下界可能变了；删别的键一定没变
                        touched_min |= idx == 0;
                        entries.remove(idx);
                        removed += 1;
                        #[cfg(test)]
                        note_delete_leaf(node_page_id);
                    }
                }
                if removed == 0 {
                    return (Del::NOOP, run.len());
                }
                let mut min_changed = touched_min;
                self.write_node(node_page_id, &node);

                // 非根叶子下溢 → 再平衡（**一片叶子一次**，不是每行一次）
                let mut merged = false;
                if node_page_id != self.root {
                    if let Some(parent) = parent_page_id {
                        if self.is_underflow(&node) {
                            set_page_phase(PHASE_RESTRUCTURE);
                            merged = self.handle_underflow(node_page_id, parent);
                            set_page_phase(PHASE_BACKTRACK);
                            // 借位/合并会挪分隔键与本层下界 ⇒ 一律上报"可能变了"，让上层去核对
                            min_changed = true;
                        }
                    }
                }
                (
                    Del {
                        removed,
                        min_changed,
                        merged,
                    },
                    run.len(),
                )
            }
            BTreeNode::Internal { keys, children } => {
                // ⚠️ 路由用的分隔键/孩子表**每轮都刷新**：上一个孩子可能已经把自己合并进兄弟
                // （本层的孩子表就少了一格，`chs` 里那一页也已被释放）—— 拿旧表继续路由就会去读
                // 已释放的页。第一次用下降那一趟读到的内容（不再多读一次），之后每轮从
                // "回溯那一趟读"的结果里拿 ⇒ 每层每孩子的页读次数与改前"每行一趟"同量级，
                // 省下来的还是**下降**那一段（一趟下降带一整片叶子）。
                let mut snap = (std::mem::take(keys), std::mem::take(children));
                let mut i = 0usize;
                let mut removed = 0usize;
                let mut merged = false;
                let mut min_changed_left = false;
                while i < run.len() {
                    let (ks, chs) = &snap;
                    let child_idx = Self::child_index(ks, &run[i]);
                    // 孩子 `c` 的键区间是 [keys[c-1], keys[c])；最后一个孩子无上界
                    let hi = ks.get(child_idx);
                    let mut j = i + 1;
                    while j < run.len() && hi.map_or(true, |h| run[j].as_slice() < h.as_slice()) {
                        j += 1;
                    }
                    let child_page_id = chs[child_idx];
                    let (res, used) =
                        self.delete_recursive(child_page_id, &run[i..j], Some(node_page_id));
                    // 递归承诺 `used ≥ 1` ⇒ 一定推进，不会死循环
                    i += used.max(1).min(j - i);
                    if res.is_noop() {
                        // 那一片子里一把键都不在（真空区间）⇒ 本层什么都没变，继续下一个孩子
                        continue;
                    }
                    removed += res.removed;
                    merged |= res.merged;
                    min_changed_left |= res.min_changed && child_idx == 0;

                    // 子节点既未改变分隔键也未改变结构时，父节点无需更新；只有父节点原本未欠载才可复用当前路由快照。
                    if !res.min_changed
                        && !res.merged
                        && (node_page_id == self.root || snap.0.len() >= self.min_entries())
                    {
                        #[cfg(test)]
                        crate::common::maintenance_probe::note_bplus_backtrack_reuse();
                        continue;
                    }
                    set_page_phase(PHASE_BACKTRACK);
                    // 回溯段一次读就够（M54b：改前是"先 `read_page` 问在不在、再 `read_node` 读一遍"，
                    // 下溢检查那里**又**两遍 ⇒ 每层 4 次页读、3 次重复）。页已被合掉而读不到 ⇒ 保守上报。
                    let Some(mut cur) = self.try_read_node(node_page_id) else {
                        return (
                            Del {
                                removed,
                                min_changed: true,
                                merged: true,
                            },
                            i,
                        );
                    };

                    // 只有"该子树的最小键可能变了"才去把它重新找出来写进分隔键
                    if res.min_changed && child_idx > 0 {
                        #[cfg(test)]
                        SEP_REFRESHES.with(|c| c.set(c.get() + 1));
                        // 这一趟要一路向左下走到叶子 ⇒ 单独一个桶（M56），别混进"回溯"里
                        set_page_phase(PHASE_SEP_REFRESH);
                        let min_key = self.subtree_min_key(child_page_id);
                        set_page_phase(PHASE_BACKTRACK);
                        if let Some(min_key) = min_key {
                            if let BTreeNode::Internal {
                                keys: ks2,
                                children: ch2,
                            } = &mut cur
                            {
                                // child 可能已变下标（兄弟间挪过），重新定位
                                if let Some(new_idx) = ch2.iter().position(|&c| c == child_page_id)
                                {
                                    if new_idx > 0
                                        && new_idx - 1 < ks2.len()
                                        && ks2[new_idx - 1] != min_key
                                    {
                                        ks2[new_idx - 1] = min_key;
                                        self.write_node(node_page_id, &cur);
                                    }
                                }
                            }
                        }
                    }

                    // 检查本节点是否下溢（合并子节点后）
                    let mut restructured = false;
                    if node_page_id != self.root {
                        if let Some(parent) = parent_page_id {
                            if self.is_underflow(&cur) {
                                set_page_phase(PHASE_RESTRUCTURE);
                                merged |= self.handle_underflow(node_page_id, parent);
                                set_page_phase(PHASE_BACKTRACK);
                                restructured = true;
                            }
                        }
                    }
                    if restructured {
                        // 本层被借/合并过 ⇒ 连"这一层是谁"都可能不再是手里这一页（父层的孩子表也变了）
                        // ⇒ 剩下的键交回上层从根重下一趟，绝不拿旧快照继续路由。
                        return (
                            Del {
                                removed,
                                min_changed: true,
                                merged,
                            },
                            i,
                        );
                    }
                    // 用回溯那一趟读到的新鲜内容继续路由下一个孩子（不再多读一次页）
                    let BTreeNode::Internal {
                        keys: ks3,
                        children: ch3,
                    } = cur
                    else {
                        // 本层变成了叶子 ⇒ 结构变了，同上：交回上层重下
                        return (
                            Del {
                                removed,
                                min_changed: true,
                                merged: true,
                            },
                            i,
                        );
                    };
                    snap = (ks3, ch3);
                    // 本层是根时**不在这里**问"要不要降层"（M56b）：`delete()`/`delete_batch()` 收尾按 `merged` 问一次。
                }
                // 往上只报"本子树的下界可能变了"：动的是**最左那棵子树**、或本节点自己借/合并过。
                (
                    Del {
                        removed,
                        min_changed: min_changed_left,
                        merged,
                    },
                    i,
                )
            }
        }
    }

    #[cfg(test)]
    pub(super) fn delete_recursive_reference(
        &mut self,
        node_page_id: PageId,
        run: &[Key],
        parent_page_id: Option<PageId>,
    ) -> (Del, usize) {
        // M56 的段标注：下降那一次读记到"下降"，返回之后本层的所有读记到"回溯"
        set_page_phase(PHASE_DESCENT);
        let mut node = self.read_node(node_page_id);

        match &mut node {
            BTreeNode::Leaf { entries, .. } => {
                // 父层已经按分隔键把这一段切到"只可能在这片叶子里" ⇒ 这里一定把整段吃掉，used = run.len()
                let mut removed = 0usize;
                let mut touched_min = false;
                for key in run {
                    if let Ok(idx) = entries.binary_search_by(|(k, _)| k.cmp(key)) {
                        // 删的是本片最小键 ⇒ 本片所属子树的下界可能变了；删别的键一定没变
                        touched_min |= idx == 0;
                        entries.remove(idx);
                        removed += 1;
                        #[cfg(test)]
                        note_delete_leaf(node_page_id);
                    }
                }
                if removed == 0 {
                    return (Del::NOOP, run.len());
                }
                let mut min_changed = touched_min;
                self.write_node(node_page_id, &node);

                // 非根叶子下溢 → 再平衡（**一片叶子一次**，不是每行一次）
                let mut merged = false;
                if node_page_id != self.root {
                    if let Some(parent) = parent_page_id {
                        if self.is_underflow(&node) {
                            set_page_phase(PHASE_RESTRUCTURE);
                            merged = self.handle_underflow(node_page_id, parent);
                            set_page_phase(PHASE_BACKTRACK);
                            // 借位/合并会挪分隔键与本层下界 ⇒ 一律上报"可能变了"，让上层去核对
                            min_changed = true;
                        }
                    }
                }
                (
                    Del {
                        removed,
                        min_changed,
                        merged,
                    },
                    run.len(),
                )
            }
            BTreeNode::Internal { keys, children } => {
                // ⚠️ 路由用的分隔键/孩子表**每轮都刷新**：上一个孩子可能已经把自己合并进兄弟
                // （本层的孩子表就少了一格，`chs` 里那一页也已被释放）—— 拿旧表继续路由就会去读
                // 已释放的页。第一次用下降那一趟读到的内容（不再多读一次），之后每轮从
                // "回溯那一趟读"的结果里拿 ⇒ 每层每孩子的页读次数与改前"每行一趟"同量级，
                // 省下来的还是**下降**那一段（一趟下降带一整片叶子）。
                let mut snap = (keys.clone(), children.clone());
                let mut i = 0usize;
                let mut removed = 0usize;
                let mut merged = false;
                let mut min_changed_left = false;
                while i < run.len() {
                    let (ks, chs) = &snap;
                    let child_idx = Self::child_index(ks, &run[i]);
                    // 孩子 `c` 的键区间是 [keys[c-1], keys[c])；最后一个孩子无上界
                    let hi = ks.get(child_idx).cloned();
                    let mut j = i + 1;
                    while j < run.len()
                        && hi
                            .as_ref()
                            .map_or(true, |h| run[j].as_slice() < h.as_slice())
                    {
                        j += 1;
                    }
                    let child_page_id = chs[child_idx];
                    let (res, used) =
                        self.delete_recursive(child_page_id, &run[i..j], Some(node_page_id));
                    // 递归承诺 `used ≥ 1` ⇒ 一定推进，不会死循环
                    i += used.max(1).min(j - i);
                    if res.is_noop() {
                        // 那一片子里一把键都不在（真空区间）⇒ 本层什么都没变，继续下一个孩子
                        continue;
                    }
                    removed += res.removed;
                    merged |= res.merged;
                    min_changed_left |= res.min_changed && child_idx == 0;

                    set_page_phase(PHASE_BACKTRACK);
                    // 回溯段一次读就够（M54b：改前是"先 `read_page` 问在不在、再 `read_node` 读一遍"，
                    // 下溢检查那里**又**两遍 ⇒ 每层 4 次页读、3 次重复）。页已被合掉而读不到 ⇒ 保守上报。
                    let Some(mut cur) = self.try_read_node(node_page_id) else {
                        return (
                            Del {
                                removed,
                                min_changed: true,
                                merged: true,
                            },
                            i,
                        );
                    };

                    // 只有"该子树的最小键可能变了"才去把它重新找出来写进分隔键
                    if res.min_changed && child_idx > 0 {
                        #[cfg(test)]
                        SEP_REFRESHES.with(|c| c.set(c.get() + 1));
                        // 这一趟要一路向左下走到叶子 ⇒ 单独一个桶（M56），别混进"回溯"里
                        set_page_phase(PHASE_SEP_REFRESH);
                        let min_key = self.subtree_min_key(child_page_id);
                        set_page_phase(PHASE_BACKTRACK);
                        if let Some(min_key) = min_key {
                            if let BTreeNode::Internal {
                                keys: ks2,
                                children: ch2,
                            } = &mut cur
                            {
                                // child 可能已变下标（兄弟间挪过），重新定位
                                if let Some(new_idx) = ch2.iter().position(|&c| c == child_page_id)
                                {
                                    if new_idx > 0
                                        && new_idx - 1 < ks2.len()
                                        && ks2[new_idx - 1] != min_key
                                    {
                                        ks2[new_idx - 1] = min_key;
                                        self.write_node(node_page_id, &cur);
                                    }
                                }
                            }
                        }
                    }

                    // 检查本节点是否下溢（合并子节点后）
                    let mut restructured = false;
                    if node_page_id != self.root {
                        if let Some(parent) = parent_page_id {
                            if self.is_underflow(&cur) {
                                set_page_phase(PHASE_RESTRUCTURE);
                                merged |= self.handle_underflow(node_page_id, parent);
                                set_page_phase(PHASE_BACKTRACK);
                                restructured = true;
                            }
                        }
                    }
                    if restructured {
                        // 本层被借/合并过 ⇒ 连"这一层是谁"都可能不再是手里这一页（父层的孩子表也变了）
                        // ⇒ 剩下的键交回上层从根重下一趟，绝不拿旧快照继续路由。
                        return (
                            Del {
                                removed,
                                min_changed: true,
                                merged,
                            },
                            i,
                        );
                    }
                    // 用回溯那一趟读到的新鲜内容继续路由下一个孩子（不再多读一次页）
                    let BTreeNode::Internal {
                        keys: ks3,
                        children: ch3,
                    } = &cur
                    else {
                        // 本层变成了叶子 ⇒ 结构变了，同上：交回上层重下
                        return (
                            Del {
                                removed,
                                min_changed: true,
                                merged: true,
                            },
                            i,
                        );
                    };
                    snap = (ks3.clone(), ch3.clone());
                    // 本层是根时**不在这里**问"要不要降层"（M56b）：`delete()`/`delete_batch()` 收尾按 `merged` 问一次。
                }
                // 往上只报"本子树的下界可能变了"：动的是**最左那棵子树**、或本节点自己借/合并过。
                (
                    Del {
                        removed,
                        min_changed: min_changed_left,
                        merged,
                    },
                    i,
                )
            }
        }
    }

    /// `read_node` 的"页可能已经被合并掉"版本（M54b：把回溯段那两遍"先问在不在、再读一遍"合成一次）
    pub(super) fn try_read_node(&mut self, page_id: PageId) -> Option<BTreeNode> {
        #[cfg(test)]
        if delete_read_reference::enabled() {
            let data = self.page_manager.read_page(page_id)?;
            return bincode::deserialize(&data).ok();
        }
        self.page_manager
            .read_page_project(page_id, |data| bincode::deserialize(data).ok())
            .flatten()
    }

    /// 子树最小键（一直向左走到叶子）
    pub(super) fn subtree_min_key(&mut self, node_page_id: PageId) -> Option<Key> {
        #[cfg(test)]
        if delete_read_reference::enabled() {
            return self.subtree_min_key_reference(node_page_id);
        }
        enum Step {
            Descend(PageId),
            Minimum(Option<Key>),
        }
        let mut page = node_page_id;
        loop {
            let step = self
                .page_manager
                .read_page_project(page, |data| match MaintenanceNode::decode(data).ok()? {
                    MaintenanceNode::Leaf { rows, count, .. } => {
                        Some(Step::Minimum(if count == 0 {
                            None
                        } else {
                            Some(NodeBytes(rows).row().ok()?.0.to_vec())
                        }))
                    }
                    MaintenanceNode::Internal { children, .. } => {
                        Some(Step::Descend(PageId(NodeBytes(children).u64().ok()?)))
                    }
                })
                .flatten()?;
            match step {
                Step::Descend(child) => page = child,
                Step::Minimum(key) => return key,
            }
        }
    }

    #[cfg(test)]
    pub(super) fn subtree_min_key_reference(&mut self, node_page_id: PageId) -> Option<Key> {
        // 节点可能在合并中被释放
        let data = self.page_manager.read_page(node_page_id)?;
        let node: BTreeNode = bincode::deserialize(&data).ok()?;
        match node {
            BTreeNode::Leaf { entries, .. } => entries.first().map(|(k, _)| k.clone()),
            BTreeNode::Internal { children, .. } => children
                .first()
                .copied()
                .and_then(|c| self.subtree_min_key_reference(c)),
        }
    }
}
