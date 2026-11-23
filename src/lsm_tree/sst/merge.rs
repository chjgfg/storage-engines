//! 本文件负责多来源流式归并。

use super::*;

pub trait MergeRow {
    fn seq(&self) -> u64;
    fn equal_seq(&mut self) {}
}
impl MergeRow for LightEntry {
    fn seq(&self) -> u64 {
        self.seq
    }
}
impl MergeRow for SourceRow<'_> {
    fn seq(&self) -> u64 {
        self.seq
    }
    fn equal_seq(&mut self) {
        self.ambiguous = true;
    }
}

// ─── Streaming merge ─────────────────────────────────────────────────────────

/// 归并核心只用到「键 + 序号 + 谁赢」，不需要知道 value 长什么样。
///
/// 所以把归并泛型化：`StoredEntry`（带 value 字节）和 [`LightEntry`]（只有 seq 与种类）
/// 走**同一份**实现。这样"只想按键折叠全库"不必再抄一份归并 —— 抄一份就是给
/// 「四份复制」那种病再添一份。
pub trait MergeSeq: Clone {
    fn seq(&self) -> u64;
}

impl MergeSeq for StoredEntry {
    fn seq(&self) -> u64 {
        self.seq
    }
}

impl MergeSeq for LightEntry {
    fn seq(&self) -> u64 {
        self.seq
    }
}

/// 把"本来就物化在内存里"的源接到 `MergeInput` 上（M33）。
///
/// 内存表切片（`mem_range_at` / `range_at` 返回的就是 `Vec`）走这条 —— **只是换个类型**，
/// 不复制条目。要避开的物化是 `SstFile::iter_entries()`：那是把磁盘上的整个文件读进 `Vec`，
/// 而压实/修复的输入恰好是磁盘文件。
pub fn vec_inputs<T: 'static>(srcs: Vec<Vec<(Vec<u8>, T)>>) -> Vec<MergeInput<'static, T>> {
    srcs.into_iter()
        .map(|s| Box::new(s.into_iter()) as MergeInput<T>)
        .collect()
}

pub fn merge_streaming<'a, T, F, K>(sources: Vec<MergeInput<'a, T>>, keep: K, mut emit: F)
where
    T: MergeSeq + 'a,
    F: FnMut(Vec<u8>, T),
    K: FnMut(&[u8], &T) -> bool,
{
    use std::convert::Infallible;
    let sources = sources
        .into_iter()
        .map(|s| Box::new(s.map(Ok::<_, Infallible>)) as TryMergeInput<'a, T, Infallible>)
        .collect();
    match try_merge_streaming(sources, keep, |key, row| {
        emit(key, row);
        Ok(())
    }) {
        Ok(()) => {}
        Err(impossible) => match impossible {},
    }
}

pub(super) fn try_merge_streaming<T, E, F, K>(
    mut sources: Vec<TryMergeInput<'_, T, E>>,
    mut keep: K,
    mut emit: F,
) -> Result<(), E>
where
    T: MergeSeq,
    F: FnMut(Vec<u8>, T) -> Result<(), E>,
    K: FnMut(&[u8], &T) -> bool,
{
    struct Node<T> {
        key: Vec<u8>,
        se: T,
        src: usize,
    }
    impl<T: MergeSeq> Ord for Node<T> {
        fn cmp(&self, other: &Self) -> CmpOrdering {
            match other.key.cmp(&self.key) {
                CmpOrdering::Equal => match self.se.seq().cmp(&other.se.seq()) {
                    // 同键：**seq 大的先出**；seq 也相同才回到源序（小 = 更新）
                    CmpOrdering::Equal => other.src.cmp(&self.src),
                    o => o,
                },
                o => o,
            }
        }
    }
    impl<T: MergeSeq> PartialOrd for Node<T> {
        fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
            Some(self.cmp(other))
        }
    }
    impl<T: MergeSeq> PartialEq for Node<T> {
        fn eq(&self, other: &Self) -> bool {
            self.cmp(other) == CmpOrdering::Equal
        }
    }
    impl<T: MergeSeq> Eq for Node<T> {}

    // 归并堆拥有每个已消费来源的头；推进时直接移动下一行，保留最大序号及同序号首来源规则，不复制值载荷。
    let heap_row = |key: Vec<u8>, row: T| {
        #[cfg(test)]
        if compaction_reference::enabled() {
            return (key.clone(), row.clone());
        }
        (key, row)
    };
    let mut heap = BinaryHeap::new();
    for (s, source) in sources.iter_mut().enumerate() {
        if let Some(row) = source.next() {
            let (key, se) = row?;
            let (key, se) = heap_row(key, se);
            heap.push(Node { key, se, src: s });
        }
    }
    let advance = |srcs: &mut [TryMergeInput<T, E>],
                   heap: &mut BinaryHeap<Node<T>>,
                   s: usize|
     -> Result<(), E> {
        if let Some(row) = srcs[s].next() {
            let (key, se) = row?;
            let (key, se) = heap_row(key, se);
            heap.push(Node { key, se, src: s });
        }
        Ok(())
    };
    while let Some(n) = heap.pop() {
        advance(&mut sources, &mut heap, n.src)?;

        // 收集所有同 key 条目，选最优：**seq 大者赢**；seq 相同才回到"源序小者赢"。
        let key = n.key;
        let mut best_src = n.src;
        let mut best = n.se;
        while heap.peek().map(|p| p.key == key).unwrap_or(false) {
            let m = heap.pop().unwrap();
            advance(&mut sources, &mut heap, m.src)?;
            if m.se.seq() > best.seq() || (m.se.seq() == best.seq() && m.src < best_src) {
                best_src = m.src;
                best = m.se;
            }
        }

        if keep(&key, &best) {
            #[cfg(test)]
            let key = if compaction_reference::enabled() {
                key.clone()
            } else {
                key
            };
            emit(key, best)?;
        }
    }
    Ok(())
}

/// 兼容：归并到 Vec
pub fn merge_latest_streaming<F>(
    sources: Vec<MergeInput<'_, StoredEntry>>,
    mut keep: F,
) -> Vec<(Vec<u8>, StoredEntry)>
where
    F: FnMut(&[u8], &StoredEntry) -> bool,
{
    let mut out = Vec::new();
    merge_streaming(sources, &mut keep, |k, se| out.push((k, se)));
    out
}

/// `Vec<Vec<…>>` 版的便捷入口（内存表切片、以及测试用）
pub fn merge_latest(sources: Vec<Vec<(Vec<u8>, StoredEntry)>>) -> Vec<(Vec<u8>, StoredEntry)> {
    merge_latest_streaming(vec_inputs(sources), |_, _| true)
}

/// **真流式**归并写入多个 SST：边 merge 边 `add`，达到 `target_bytes` 就 `finish` 落盘，
/// 不先物化全量 `Vec`（单 builder 缓冲不超过约一个 SST）。
pub fn merge_to_ssts<F>(
    sources: Vec<MergeInput<'_, StoredEntry>>,
    mut keep: F,
    new_builder: impl FnMut() -> SstBuilder,
    target_bytes: usize,
) -> std::io::Result<Vec<SstFile>>
where
    F: FnMut(&[u8], &StoredEntry) -> bool,
{
    let sources = sources
        .into_iter()
        .map(|s| Box::new(s.map(Ok)) as TryMergeInput<'_, StoredEntry, std::io::Error>)
        .collect();
    merge_to_ssts_with_action(
        sources,
        |key, entry| {
            if keep(key, entry) {
                CompactionAction::Keep
            } else {
                CompactionAction::Discard
            }
        },
        new_builder,
        target_bytes,
    )
}
impl Drop for UnpublishedSsts {
    fn drop(&mut self) {
        for file in self.0.drain(..) {
            let path = file.path.clone();
            drop(file);
            let _ = std::fs::remove_file(path);
        }
    }
}

pub(crate) fn merge_to_ssts_with_action(
    sources: Vec<TryMergeInput<'_, StoredEntry, std::io::Error>>,
    mut action: impl FnMut(&[u8], &StoredEntry) -> CompactionAction,
    mut new_builder: impl FnMut() -> SstBuilder,
    target_bytes: usize,
) -> std::io::Result<Vec<SstFile>> {
    let mut out = UnpublishedSsts(Vec::new());
    let mut builder: Option<SstBuilder> = None;
    try_merge_streaming(
        sources,
        |_, _| true,
        |k, mut se| {
            match action(&k, &se) {
                CompactionAction::Keep => {}
                CompactionAction::Discard => return Ok(()),
                CompactionAction::RetainDeletion => se.value = EntryValue::Drop,
            }
            if builder.is_none() {
                builder = Some(new_builder());
            }
            let b = builder.as_mut().unwrap();
            b.add(k, se);
            if b.estimated_bytes() >= target_bytes {
                out.0.push(builder.take().unwrap().finish()?);
            }
            Ok(())
        },
    )?;
    if let Some(b) = builder {
        if !b.is_empty() {
            out.0.push(b.finish()?);
        }
    }
    #[cfg(test)]
    crate::lsm_tree::lsm_tree::maintenance_probe::outputs(
        out.0.len(),
        out.0.iter().map(|f| f.size_bytes).sum(),
    );
    Ok(std::mem::take(&mut out.0))
}
