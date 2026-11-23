//! 本文件负责多来源流式归并。

use super::*;

/// [`LSMTree::light_iter_range`] 的状态机：k 路归并，每次 `next()` 只往前拉一条。
///
/// 堆里只放 `(键, 源序号)`，每个源至多一个候选在堆里；真正的值在 `head` 里等着。
/// 输出**非降序** ⇒ 跨源的同键去重只需一个"待定键"的先行量：
/// - 同一键取 **seq 最大者**定论（由下面那道"待定键"比较做到 —— 不是靠堆的出队顺序）
/// - 为什么必须是 seq：层内两个范围重叠的文件里谁新谁旧，源序答不了（M26 的实测结论：
///   `merge_streaming` 那边走的是"堆的 tie-break 选胜者"，所以它必须比 seq；这边是"先出者待定、
///   后到者比 seq 换掉"，所以判据在待定那一步）
/// 没有这个先行量就会把"该键还有更新的源"的键提前吐出去。
///
/// **源的激活是延迟的**（M81 第 1 项）。`waiting` 按驻留下界排队，只有"下界 ≤ 当前堆顶键"的
/// 源才被拉第一个键 —— 而"拉一次"对 SST 就是至少一块读。为什么必须这样：首页（`to = None`）
/// 时 `sst_candidates` 的二分剪枝帮不上忙（它剪的是**上界**），旧形状在建堆时对**每一篇** SST
/// 各拉一次首键 ⇒ 首页付的是"库里篇数"的块读（5M 实测 315 篇 → `search 首页(免 total)`
/// 15.66 ms，同家末页只要 763 µs）。改成延迟激活后首页只需激活"下界压得住最小键"的那些源
/// （L0 全部 + 每层覆盖到前几把键的首篇），代价与篇数脱钩。
///
/// 判据（外部复核给的六条，逐条钉在这里）：
/// ① 源序号不因激活顺序而变（`waiting` 排的只是"什么时候拉"，`MergeNode.src` 仍是建堆时的序号）
/// ② 未激活源用**驻留**下界代表自己（SST 的 `min_key` 与 `from` 的较大者，不读块就知道）
/// ③ 取下一个真实堆节点之前，激活所有下界 ≤ 当前堆顶键的源（`activate_covering_top`）
/// ④ 堆空但仍有未激活源 ⇒ 继续激活，不许提前收（同一条函数的 `None => true` 分支）
/// ⑤ 激活后真实首键可能 > `min_key`（快照过滤就会这样），所以要拿真实键重新比一轮
///    （`activate_covering_top` 返回 `true` ⇒ 调用方 `continue` 重看堆顶）
/// ⑥ 与 eager 形状逐字节对拍：守卫
///    `test_lazy_source_activation_first_page_does_not_scale_with_files` 与
///    `test_lazy_source_activation_agrees_with_eager_everywhere`
pub struct LightMerge<'a, R = LightEntry> {
    sources: Vec<Box<dyn Iterator<Item = (Vec<u8>, R)> + 'a>>,
    /// 每个源**可能交出的最小键**（含）。取不到驻留下界的源（memtable：本来就在内存里，
    /// 拉首键不花 I/O）这里是 `None`，建堆时当场激活；`Some` 的源进 `waiting` 排队。
    /// 用 `&'a [u8]` 不复制：这些切片就是 `SstFile::min_key` / 调用方给的 `from`。
    bound: Vec<Option<&'a [u8]>>,
    /// `bound` 里仍为 `Some` 的源序号，**按下界降序**存放 ⇒ `last()` 永远是最小下界，
    /// 补齐覆盖时从尾部 `pop`，每个源至多被看一次（不是每轮全表扫描）。
    waiting: Vec<usize>,
    head: Vec<Option<(Vec<u8>, R)>>,
    heap: BinaryHeap<MergeNode>,
    pending: Option<(Vec<u8>, R)>,
}

pub(super) struct MergeNode {
    pub(super) key: Vec<u8>,
    pub(super) src: usize,
}

/// 最小键优先；同键时源序号小的先出（新→旧）。
///
/// ⚠️ 这里**不需要**像 `merge_streaming` 那样把 seq 排进比较（M26 试过，证不出来）：
/// 同键的胜者由 `next()` 里那道"待定键 + 取更大 seq"的比较定论，堆序只决定谁先来。
/// 反过来说，**那道比较才是这条路径的判据**，把它当冗余删掉就会在层内重叠时交出旧版本
/// （守卫 `test_fold_agrees_with_point_read_on_overlapping_level` 会红，实测证过）。
impl Ord for MergeNode {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match other.key.cmp(&self.key) {
            std::cmp::Ordering::Equal => other.src.cmp(&self.src),
            o => o,
        }
    }
}

impl PartialOrd for MergeNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for MergeNode {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.src == other.src
    }
}

impl Eq for MergeNode {}

impl<'a, R: MergeRow> LightMerge<'a, R> {
    /// `bound` 与 `sources` 一一对应：`Some(b)` = 先不碰这个源，`b` 是它的驻留键下界；
    /// `None` = 当场拉第一个键。长度不一致会把"什么时候拉"和"谁更新"错位，所以钉死。
    pub(super) fn build(
        mut sources: Vec<Box<dyn Iterator<Item = (Vec<u8>, R)> + 'a>>,
        bound: Vec<Option<&'a [u8]>>,
    ) -> Self {
        assert_eq!(
            bound.len(),
            sources.len(),
            "light_iter_range 构造的源和下界数量不一致 ⇒ 激活顺序与源序号会错位"
        );
        let mut head = Vec::with_capacity(sources.len());
        let mut heap = BinaryHeap::new();
        let mut waiting: Vec<usize> = Vec::new();
        for src in 0..sources.len() {
            if bound[src].is_some() {
                head.push(None);
                waiting.push(src);
                continue;
            }
            match sources[src].next() {
                Some((k, le)) => {
                    heap.push(MergeNode {
                        key: k.clone(),
                        src,
                    });
                    head.push(Some((k, le)));
                }
                None => head.push(None),
            }
        }
        // 降序存、从尾部取 ⇒ `waiting.last()` 始终是未激活源里下界最小的那个。
        waiting.sort_by(|&a, &b| bound[b].cmp(&bound[a]));
        Self {
            sources,
            bound,
            waiting,
            head,
            heap,
            pending: None,
        }
    }

    /// 不变量③④：把"下界 ≤ 当前堆顶键"的未激活源逐个拉首键。堆空时下界最小的那个必须
    /// 激活（堆空 ≠ 归并结束）。返回是否激活过至少一个 ⇒ 堆顶可能已经变了，调用方要重看。
    pub(super) fn activate_covering_top(&mut self) -> bool {
        let mut any = false;
        loop {
            let Some(&src) = self.waiting.last() else {
                break;
            };
            let covered = match self.heap.peek() {
                // 有堆顶：只有"下界压得住它"的源才可能交出 ≤ 堆顶的键，必须激活。
                // ⚠️ 必须是"下界 ≤ 堆顶"而不是"下界 < 堆顶"（方案二审的那条）：L1 相邻两篇
                // 常常 `max_key[i] == min_key[i+1]`，用严格小于就在相接端点上不触发。
                Some(node) => self.bound[src] <= Some(node.key.as_slice()),
                // 堆空但还有未激活源 ⇒ 剩下的键只可能来自它们（迭代中途耗尽一段也走这支：
                // A 段 [1..10] 交完、B 段 [20..30] 还没激活，此时堆空 —— 不激活就把 B 整段
                // 静默丢掉。B 的首键必然大于一切已吐出的键，所以单调性不破。）
                None => true,
            };
            if !covered {
                break;
            }
            self.waiting.pop();
            self.activate(src);
            any = true;
        }
        any
    }

    /// 拉源 `src` 的第一个真实键并入堆（真实键 > 下界是正常的：快照过滤、`from` 落在空区间）。
    pub(super) fn activate(&mut self, src: usize) {
        #[cfg(test)]
        note_source_activation();
        match self.sources[src].next() {
            Some((k, le)) => {
                self.heap.push(MergeNode {
                    key: k.clone(),
                    src,
                });
                self.head[src] = Some((k, le));
            }
            None => self.head[src] = None,
        }
    }

    /// 把源 `src` 的下一个候选放进堆（源 exhausted 就不放）
    pub(super) fn advance(&mut self, src: usize) {
        if let Some((k, le)) = self.sources[src].next() {
            self.heap.push(MergeNode {
                key: k.clone(),
                src,
            });
            self.head[src] = Some((k, le));
        } else {
            self.head[src] = None;
        }
    }

    /// 测试用（不变量⑥的对拍一侧）：退化成"建堆即激活全部源"的旧形状，之后按普通迭代器走。
    #[cfg(test)]
    pub(super) fn activate_all_for_test(&mut self) {
        while let Some(src) = self.waiting.pop() {
            self.activate(src);
        }
    }
}

impl<R: MergeRow> Iterator for LightMerge<'_, R> {
    type Item = (Vec<u8>, R);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // 每次真正动用堆顶之前先补齐覆盖（不变量③）：激活会带进新候选键，所以这里
            // 必须循环到"没有未激活源的下界 ≤ 堆顶"为止，否则交出的就不是全局最小键。
            if self.activate_covering_top() {
                continue;
            }
            let Some(node) = self.heap.pop() else { break };
            let src = node.src;
            let Some((key, le)) = self.head[src].take() else {
                self.advance(src);
                continue;
            };
            self.advance(src);
            match self.pending.take() {
                None => self.pending = Some((key, le)),
                Some((pk, mut ple)) if pk == key => {
                    // 同一个键：**这道比较才是胜者判据** —— 先出的是源序里更新的那个，
                    // 但层内重叠文件的"更新"只有 seq 能回答 ⇒ 后到的 seq 更大就换掉它。
                    if ple.seq() == le.seq() {
                        ple.equal_seq();
                    }
                    self.pending = Some((pk, if ple.seq() >= le.seq() { ple } else { le }));
                }
                Some((pk, ple)) => {
                    // 换键了 ⇒ 前一个键已经收齐，可以吐
                    self.pending = Some((key, le));
                    return Some((pk, ple));
                }
            }
        }
        // 走到这里 = 堆空**且**等待队列空 ⇒ 所有源都收干净了，最后一个待定键可以吐
        self.pending.take()
    }
}

/// 惰性维护归并；堆顶持有键和元数据。
pub(crate) struct VacuumMerge<'a> {
    sources: Vec<Box<dyn Iterator<Item = std::io::Result<(Vec<u8>, LightEntry)>> + 'a>>,
    bound: Vec<Option<&'a [u8]>>,
    waiting: Vec<usize>,
    heap: BinaryHeap<VacuumNode>,
    pending: Option<(Vec<u8>, LightEntry)>,
    done: bool,
}
pub(super) struct VacuumNode {
    key: Vec<u8>,
    entry: LightEntry,
    src: usize,
}
impl Ord for VacuumNode {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .key
            .cmp(&self.key)
            .then_with(|| other.src.cmp(&self.src))
    }
}
impl PartialOrd for VacuumNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for VacuumNode {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.src == other.src
    }
}
impl Eq for VacuumNode {}
impl<'a> VacuumMerge<'a> {
    pub(super) fn build(
        sources: Vec<Box<dyn Iterator<Item = std::io::Result<(Vec<u8>, LightEntry)>> + 'a>>,
        bound: Vec<Option<&'a [u8]>>,
    ) -> Self {
        assert_eq!(sources.len(), bound.len());
        let mut waiting: Vec<usize> = (0..sources.len()).collect();
        waiting.sort_by(|&a, &b| bound[b].cmp(&bound[a]));
        Self {
            sources,
            bound,
            waiting,
            heap: BinaryHeap::new(),
            pending: None,
            done: false,
        }
    }
    pub(super) fn advance(&mut self, src: usize) -> std::io::Result<()> {
        if let Some(row) = self.sources[src].next() {
            let (key, entry) = row?;
            self.heap.push(VacuumNode { key, entry, src });
        }
        Ok(())
    }
    pub(super) fn activate_covering_top(&mut self) -> std::io::Result<bool> {
        let mut activated = false;
        while let Some(&src) = self.waiting.last() {
            if self
                .heap
                .peek()
                .is_some_and(|n| self.bound[src].is_some_and(|b| b > n.key.as_slice()))
            {
                break;
            }
            self.waiting.pop();
            self.advance(src)?;
            activated = true;
        }
        Ok(activated)
    }
    pub(super) fn next_row(&mut self) -> std::io::Result<Option<(Vec<u8>, LightEntry)>> {
        loop {
            if self.activate_covering_top()? {
                continue;
            }
            let Some(node) = self.heap.pop() else {
                return Ok(self.pending.take());
            };
            self.advance(node.src)?;
            match self.pending.take() {
                None => self.pending = Some((node.key, node.entry)),
                Some((key, entry)) if key == node.key => {
                    self.pending = Some((
                        key,
                        if entry.seq >= node.entry.seq {
                            entry
                        } else {
                            node.entry
                        },
                    ));
                }
                Some(previous) => {
                    self.pending = Some((node.key, node.entry));
                    return Ok(Some(previous));
                }
            }
        }
    }
}
impl Iterator for VacuumMerge<'_> {
    type Item = std::io::Result<(Vec<u8>, LightEntry)>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        match self.next_row() {
            Ok(Some(row)) => Some(Ok(row)),
            Ok(None) => {
                self.done = true;
                None
            }
            Err(error) => {
                self.done = true;
                self.pending = None;
                self.heap.clear();
                Some(Err(error))
            }
        }
    }
}
