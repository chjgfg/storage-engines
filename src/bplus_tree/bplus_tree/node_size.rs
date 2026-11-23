//! 本文件负责节点字节容量与分裂位置。

use super::*;

impl SizeConsts {
    pub(super) fn measure() -> SizeConsts {
        let size = |n: &BTreeNode| bincode::serialized_size(n).unwrap_or(u64::MAX) as i64;
        let leaf = |next_leaf: Option<PageId>| {
            size(&BTreeNode::Leaf {
                entries: Vec::new(),
                next_leaf,
            })
        };
        let empty_internal = size(&BTreeNode::Internal {
            keys: Vec::new(),
            children: Vec::new(),
        });
        let one_child = size(&BTreeNode::Internal {
            keys: Vec::new(),
            children: vec![PageId(1)],
        });
        let empty_entry =
            bincode::serialized_size(&(Vec::<u8>::new(), RowId(None))).unwrap_or(u64::MAX) as i64;
        let null_entry = bincode::serialized_size(&(Vec::<u8>::new(), RowId(Some(Vec::new()))))
            .unwrap_or(u64::MAX) as i64;
        SizeConsts {
            leaf_base_none: leaf(None),
            leaf_base_some: leaf(Some(PageId(0))),
            internal_base: empty_internal,
            internal_child: one_child - empty_internal,
            entry_base: empty_entry,
            entry_some: null_entry - empty_entry,
            key_unit: bincode::serialized_size(&Vec::<u8>::new()).unwrap_or(u64::MAX) as i64,
        }
    }
}

pub(super) fn size_consts() -> &'static SizeConsts {
    static CONSTS: std::sync::OnceLock<SizeConsts> = std::sync::OnceLock::new();
    CONSTS.get_or_init(SizeConsts::measure)
}

impl BPlusTree {
    /// 内部节点里该走哪个子：第一把**严格大于** `key` 的键的下标（键相等要走右边 —— 分隔键
    /// 本身归左半，`key == keys[i]` 时目标在 `i` 右边）。
    ///
    /// 为什么二分而不是 `position` 线性扫：顺序灌行时目标**永远是最后一个子**，线性扫每层
    /// 都要把整页分隔键走一遍 ⇒ 下降的代价从 O(log 扇出) 变成 O(扇出)。M34 量写路径单价时
    /// 这条是读、写两侧的公共开销（点读、`latest_version`、插入、删除、反向定位都走它）。
    pub(super) fn child_index(keys: &[Key], key: &Key) -> usize {
        #[cfg(test)]
        let mut comps = 0u64;
        let one = |k: &Key| {
            #[cfg(test)]
            {
                comps += 1;
            }
            k.as_slice().cmp(key.as_slice())
        };
        let idx = match keys.binary_search_by(one) {
            Ok(i) => i + 1,
            Err(i) => i,
        };
        #[cfg(test)]
        {
            DESCENT_LOOKUPS.with(|c| c.set(c.get() + 1));
            DESCENT_KEY_COMPS.with(|c| c.set(c.get() + comps));
            DESCENT_MAX_COMPS.with(|m| {
                if comps > m.get() {
                    m.set(comps);
                }
            });
        }
        idx
    }

    /// 节点序列化后能不能装进一页。
    ///
    /// **只能问序列化结果，不能问条数**：条目是变长的（键可达几十字节、RowId 带 flag 与
    /// offset），`order=64` 在 40B 键 + 60B 值的真实形状下是 7.5 KB，而一页载荷只有 4080 B。
    /// 原先只有插入路径问过字节，delete 的借键/合并完全按条数走 ⇒ 见
    /// `test_reorg_never_drops_oversized_pages`。
    pub(super) fn node_fits(node: &BTreeNode) -> bool {
        #[cfg(test)]
        if delete_cost_reference::enabled() {
            crate::common::maintenance_probe::note_bplus_pack_buffer();
            return bincode::serialize(node).is_ok_and(|data| data.len() <= MAX_PAYLOAD);
        }
        match bincode::serialized_size(node) {
            Ok(size) => size <= MAX_PAYLOAD as u64,
            Err(_) => false,
        }
    }

    /// 一条叶子条目在序列化结果里占多少字节 —— 常量见 [`SizeConsts`]，O(1) 且**精确**。
    pub(super) fn entry_bytes(item: &(Key, RowId)) -> i64 {
        let RowId(row) = &item.1;
        Self::entry_bytes_borrowed(&item.0, row)
    }

    /// 同上，但接**借用**：`row_fits` 是每条写都要问的一句，不能为量一次尺寸先把键与值各克隆
    /// 一份（那是事务路径上每行两次堆分配，只为了算一个数）。
    pub(super) fn entry_bytes_borrowed(key: &[u8], row: &Option<Vec<u8>>) -> i64 {
        let c = size_consts();
        let mut n = c.entry_base + key.len() as i64;
        if let Some(v) = row {
            n += c.entry_some + v.len() as i64;
        }
        n
    }

    /// 一把键（内部节点的分隔键）占多少字节
    pub(super) fn key_bytes(item: &Key) -> i64 {
        size_consts().key_unit + item.len() as i64
    }

    /// 只有 `next_leaf` 的叶子（条目为空）序列化后的字节数
    pub(super) fn leaf_base(next_leaf: Option<PageId>) -> i64 {
        if next_leaf.is_some() {
            size_consts().leaf_base_some
        } else {
            size_consts().leaf_base_none
        }
    }

    /// 空内部节点（无键无子）的字节数，与「一个子节点占的字节」——子节点是定宽 `PageId`，
    /// 所以键数与子节点数可以分开加（同样由对拍测试钉住）
    pub(super) fn internal_base() -> i64 {
        size_consts().internal_base
    }

    /// 两半**都装得下**的分界区间 `(lo, hi)`；`None` = 没有任何合法分界 ⇒ 调用方一字都不能写。
    ///
    /// 前缀随 k 变胖、后缀随 k 变瘦 ⇒ 可行档是一段连续区间，一次线性扫把两端都记住就行
    /// （不必二分：`k` 的上界就是阶数，一趟 O(order)）。
    /// 为什么不用 `serialized_size(&候选节点)` 逐档试：重组与分裂都在写路径上，一次十几档
    /// 每档克隆上百条 ⇒ delete 密集时**量尺自己变成瓶颈**；而 bincode 的 `Vec` 就是
    /// 「长度 + 逐条」，所以逐条字节相加是精确值（由 `test_reorg_size_formula_matches_bincode`
    /// 与真实序列化逐档对拍钉住 —— 公式偏了只会退回"什么都不动"，不红测试、静默不优化）。
    pub(super) fn fitting_split(
        k_from: usize,
        k_to: usize,
        cap: i64,
        bytes: &dyn Fn(usize) -> (i64, i64),
    ) -> Option<(usize, usize)> {
        if k_from > k_to {
            return None;
        }
        let mut lo = None;
        let mut hi = None;
        for k in k_from..=k_to {
            let (left, right) = bytes(k);
            if left > cap {
                break; // 再往右只会更胖，后缀那边不用看了
            }
            if right <= cap {
                if lo.is_none() {
                    lo = Some(k);
                }
                hi = Some(k);
            }
        }
        Some((lo?, hi?))
    }

    /// 一批叶子条目 + `next_leaf` 序列化后的**精确**字节数（可加性见 `fitting_split` 的注释）
    pub(super) fn leaf_total_bytes(entries: &[(Key, RowId)], next_leaf: Option<PageId>) -> i64 {
        Self::leaf_base(next_leaf) + entries.iter().map(Self::entry_bytes).sum::<i64>()
    }

    /// 内部节点的精确字节数：键是变长的、子节点是定宽 `PageId` ⇒ 两项分开加
    pub(super) fn internal_total_bytes(keys: &[Key], child_count: usize) -> i64 {
        Self::internal_base()
            + keys.iter().map(Self::key_bytes).sum::<i64>()
            + child_count as i64 * Self::internal_child_unit()
    }

    /// 前缀字节表：`pre[i]` = 前 i 条的字节和 ⇒ 任意半页的字节 O(1) 拿到
    pub(super) fn prefix_bytes<T>(items: &[T], size: fn(&T) -> i64) -> Vec<i64> {
        let mut pre = Vec::with_capacity(items.len() + 1);
        pre.push(0i64);
        for it in items {
            pre.push(pre.last().unwrap() + size(it));
        }
        pre
    }

    /// 叶子的分裂点（左半留几条）：**取可行区间的中点**。
    ///
    /// 分裂与重组的目标不同，所以分界取法也不同 —— 分裂是把一页变两页，两边都得留增长空间，
    /// 取中点；重组是要把空位挤成一小片、好让它下一轮合并成功，取最满的一档
    /// （见 `rebalance_leaf_pair`）。两处照抄都会静默地更差，所以各写各的。
    pub(super) fn leaf_split_point(
        entries: &[(Key, RowId)],
        next_leaf: Option<PageId>,
        order: usize,
    ) -> Option<usize> {
        let n = entries.len();
        let pre = Self::prefix_bytes(entries, Self::entry_bytes);
        // 左半的下一叶是"某个 Some"，与具体页号无关（PageId 定宽）
        let base_left = Self::leaf_base(Some(PageId(0)));
        let base_right = Self::leaf_base(next_leaf);
        // 两半条数都必须 < order：否则分裂完谁也插不进去，下一次写必然超载
        let k_from = 1usize.max(n.saturating_sub(order - 1));
        let k_to = (order - 1).min(n.saturating_sub(1));
        let (lo, hi) = Self::fitting_split(k_from, k_to, MAX_PAYLOAD as i64, &|k| {
            (base_left + pre[k], base_right + (pre[n] - pre[k]))
        })?;
        Some((lo + hi) / 2)
    }

    /// 内部节点的分裂点（上提键的下标 `k`：左半拿 `keys[..k]`，右半拿 `keys[k+1..]`）
    pub(super) fn internal_split_point(keys: &[Key], order: usize) -> Option<usize> {
        let n = keys.len();
        let pre = Self::prefix_bytes(keys, Self::key_bytes);
        let base = Self::internal_base();
        let cu = Self::internal_child_unit();
        // 左半 k 把键 + k+1 个子；右半 n-k-1 把键 + n-k 个子
        let k_from = 1usize.max(n.saturating_sub(order));
        let k_to = (order - 1).min(n.saturating_sub(1));
        let (lo, hi) = Self::fitting_split(k_from, k_to, MAX_PAYLOAD as i64, &|k| {
            (
                base + pre[k] + (k as i64 + 1) * cu,
                base + (pre[n] - pre[k + 1]) + (n as i64 - k as i64) * cu,
            )
        })?;
        Some((lo + hi) / 2)
    }

    pub(super) fn internal_child_unit() -> i64 {
        size_consts().internal_child
    }

    /// 这一行**住得住一页吗** —— 给上层在动 WAL 之前先问一句。
    ///
    /// 为什么要有这个谓词而不是等 `insert` 返回 `false`：事务层是「先 append WAL、再写树」，
    /// 树拒收之后 WAL 里就留下一条"提交了但这行不存在"的记录，恢复时按它重放只会再拒一次，
    /// 表现成"WAL 说有、树里查无此键"。所以拒绝必须发生在写 WAL **之前**。
    /// 判据与分裂用的是同一套字节账（`leaf_base` + `entry_bytes_borrowed`，与 `leaf_total_bytes`
    /// 逐项相同），不留两边不一致。
    ///
    /// 为什么不走 `leaf_total_bytes(&[(key.to_vec(), RowId(row.clone()))])`：那是每条写都要问
    /// 的一句，为了算一个数先把键与值各克隆一份 —— 事务路径上每行两次堆分配白花在量尺上。
    pub fn row_fits(&self, key: &[u8], row: &Option<Vec<u8>>) -> bool {
        Self::leaf_base(None) + Self::entry_bytes_borrowed(key, row) <= MAX_PAYLOAD as i64
    }
}
