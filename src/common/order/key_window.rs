//! 逻辑键窗口与二进制前缀交错折叠。

use super::*;

impl<T, K: Ord + Borrow<[u8]>> KeyWindow<T, K> {
    pub(crate) fn new() -> Self {
        Self {
            pending: WindowStorage::Empty,
        }
    }
    pub(crate) fn is_empty(&self) -> bool {
        matches!(self.pending, WindowStorage::Empty)
    }
    pub(crate) fn insert(&mut self, key: K, value: T) {
        match &mut self.pending {
            WindowStorage::Empty => self.pending = WindowStorage::Single(key, value),
            WindowStorage::Single(old, v) if *old == key => *v = value,
            WindowStorage::Single(_, _) => {
                let old = std::mem::replace(&mut self.pending, WindowStorage::Empty);
                let WindowStorage::Single(old_key, old_value) = old else {
                    unreachable!()
                };
                let mut map = BTreeMap::new();
                map.insert(old_key, old_value);
                map.insert(key, value);
                self.pending = WindowStorage::Overlap(map);
            }
            WindowStorage::Overlap(map) => {
                map.insert(key, value);
            }
        }
    }
    pub(crate) fn pop_first(&mut self) -> Option<(K, T)> {
        match std::mem::replace(&mut self.pending, WindowStorage::Empty) {
            WindowStorage::Empty => None,
            WindowStorage::Single(key, value) => Some((key, value)),
            WindowStorage::Overlap(mut map) => {
                let out = map.pop_first();
                self.pending = if map.len() == 1 {
                    let (key, value) = map.pop_first().expect("one group");
                    WindowStorage::Single(key, value)
                } else if map.is_empty() {
                    WindowStorage::Empty
                } else {
                    WindowStorage::Overlap(map)
                };
                out
            }
        }
    }
    pub(crate) fn pop_before(&mut self, enc: &[u8], max_version: u64) -> Option<(K, T)> {
        let first: &[u8] = match &self.pending {
            WindowStorage::Empty => return None,
            WindowStorage::Single(key, _) => key.borrow(),
            WindowStorage::Overlap(map) => {
                map.first_key_value().expect("overlap nonempty").0.borrow()
            }
        };
        #[cfg(test)]
        let before = if super::super::kv_snapshot::snapshot_emit_reference::enabled() {
            first < logical_floor(enc, max_version)
        } else {
            window_before(first, enc, max_version)
        };
        #[cfg(not(test))]
        let before = window_before(first, enc, max_version);
        if before { self.pop_first() } else { None }
    }

    /// 首次插入逻辑组时才分配键和组；工厂可复用已完成组的缓冲，二进制重叠组仍保持拥有状态。
    pub(crate) fn entry_with(&mut self, key: &[u8], make: impl FnOnce() -> (K, T)) -> &mut T {
        if matches!(&self.pending, WindowStorage::Single(old, _) if old.borrow() == key) {
            return match &mut self.pending {
                WindowStorage::Single(_, value) => value,
                _ => unreachable!(),
            };
        }
        if self.is_empty() {
            let (owned, value) = make();
            debug_assert_eq!(owned.borrow(), key);
            self.pending = WindowStorage::Single(owned, value);
            return match &mut self.pending {
                WindowStorage::Single(_, value) => value,
                _ => unreachable!(),
            };
        }
        if matches!(&self.pending, WindowStorage::Single(_, _)) {
            let old = std::mem::replace(&mut self.pending, WindowStorage::Empty);
            let WindowStorage::Single(owned, value) = old else {
                unreachable!()
            };
            let mut map = BTreeMap::new();
            map.insert(owned, value);
            self.pending = WindowStorage::Overlap(map);
        }
        match &mut self.pending {
            WindowStorage::Overlap(map) => {
                if map.contains_key(key) {
                    return map.get_mut(key).unwrap();
                }
                let (owned, value) = make();
                debug_assert_eq!(owned.borrow(), key);
                map.entry(owned).or_insert(value)
            }
            _ => unreachable!(),
        }
    }
}

impl<T: Default, K: Ord + Borrow<[u8]>> KeyWindow<T, K> {
    pub(crate) fn entry(&mut self, key: K) -> &mut T {
        if let WindowStorage::Single(old, _) = &self.pending {
            if *old != key {
                let old = std::mem::replace(&mut self.pending, WindowStorage::Empty);
                let WindowStorage::Single(k, v) = old else {
                    unreachable!()
                };
                let mut map = BTreeMap::new();
                map.insert(k, v);
                self.pending = WindowStorage::Overlap(map);
            }
        }
        if self.is_empty() {
            self.pending = WindowStorage::Single(key, T::default());
            return match &mut self.pending {
                WindowStorage::Single(_, value) => value,
                _ => unreachable!(),
            };
        }
        match &mut self.pending {
            WindowStorage::Single(_, value) => value,
            WindowStorage::Overlap(map) => map.entry(key).or_default(),
            WindowStorage::Empty => unreachable!(),
        }
    }
}
