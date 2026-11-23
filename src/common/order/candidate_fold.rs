//! 按逻辑键选择可见版本候选。

use super::*;

impl<'a, R, K: Ord + Borrow<[u8]>> CandidateFold<'a, R, K> {
    pub(crate) fn new(
        start: Option<&'a [u8]>,
        end: Option<&'a [u8]>,
        after: Option<&'a [u8]>,
        accept: Option<&'a dyn Fn(&[u8]) -> bool>,
        max_version: u64,
        budget: usize,
    ) -> Self {
        Self {
            window: KeyWindow::new(),
            out: Vec::new(),
            start,
            end,
            after,
            accept,
            max_version,
            budget: budget.max(1),
            emitted: 0,
            stopped: false,
        }
    }

    pub(super) fn emit(&mut self, key: K, (version, row): (u64, R)) {
        self.out.push(WinCandidate { key, version, row });
        self.emitted += 1;
    }

    /// 立即消费已完成组，保留原切片预算并复用缓冲；二进制重叠组仍留在窗口中。
    pub(crate) fn drain_ready(&mut self) -> std::vec::Drain<'_, WinCandidate<R, K>> {
        self.out.drain(..)
    }

    /// 输出预算已满时返回 true，调用方可停止扫描。
    pub(super) fn push_with_key(
        &mut self,
        enc: &[u8],
        visible: impl FnOnce(u64) -> bool,
        row: impl FnOnce() -> R,
        key_owned: impl FnOnce() -> K,
    ) -> bool {
        while let Some((key, winner)) = self.window.pop_before(enc, self.max_version) {
            self.emit(key, winner);
            if self.emitted >= self.budget {
                self.stopped = true;
                return true;
            }
        }
        let Some((key, version)) = decode_key_ref(enc) else {
            return false;
        };
        if !visible(version)
            || self.start.is_some_and(|s| key < s)
            || self.end.is_some_and(|e| key >= e)
            || self.after.is_some_and(|a| key <= a)
            || self.accept.is_some_and(|f| !f(key))
        {
            return false;
        }
        // 同一逻辑键的物理版本单调递增。
        let mut next = Some((version, row()));
        let winner = self
            .window
            .entry_with(key, || (key_owned(), next.take().unwrap()));
        if let Some(next) = next {
            *winner = next;
        }
        false
    }

    pub(crate) fn finish(mut self) -> (Vec<WinCandidate<R, K>>, bool) {
        if !self.stopped {
            while self.emitted < self.budget {
                let Some((key, winner)) = self.window.pop_first() else {
                    break;
                };
                self.emit(key, winner);
            }
        }
        let complete = !self.stopped && self.window.is_empty();
        (self.out, complete)
    }
}

impl<'a, R> CandidateFold<'a, R> {
    pub(crate) fn push(
        &mut self,
        enc: &[u8],
        visible: impl FnOnce(u64) -> bool,
        row: impl FnOnce() -> R,
    ) -> bool {
        #[cfg(test)]
        if super::super::store::scan_cost_reference::enabled() {
            while let Some((key, winner)) = self.window.pop_before(enc, self.max_version) {
                self.emit(key, winner);
                if self.emitted >= self.budget {
                    self.stopped = true;
                    return true;
                }
            }
            let Some(VersionedKey {
                raw_key: key,
                version,
            }) = decode_key(enc)
            else {
                return false;
            };
            super::super::store::scan_cost_reference::note_key_copy();
            if !visible(version)
                || self.start.is_some_and(|s| key.as_slice() < s)
                || self.end.is_some_and(|e| key.as_slice() >= e)
                || self.after.is_some_and(|a| key.as_slice() <= a)
                || self.accept.is_some_and(|f| !f(&key))
            {
                return false;
            }
            self.window.insert(key, (version, row()));
            return false;
        }
        self.push_with_key(enc, visible, row, || {
            #[cfg(test)]
            super::super::store::scan_cost_reference::note_key_copy();
            decode_key_ref(enc)
                .expect("validated encoded key")
                .0
                .to_vec()
        })
    }
}

impl<'a, 'rows, R> CandidateFold<'a, R, &'rows [u8]> {
    /// 调用方必须保持存储读取边界，直到 finish 完成。
    pub(crate) fn push_borrowed(
        &mut self,
        enc: &'rows [u8],
        visible: impl FnOnce(u64) -> bool,
        row: impl FnOnce() -> R,
    ) -> bool {
        self.push_with_key(enc, visible, row, || &enc[..enc.len() - 8])
    }
}
