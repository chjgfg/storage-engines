//! 本文件负责共享块缓存和 LRU 管理。

use super::*;

impl BlockCache {
    pub fn new(cap: usize) -> Self {
        Self {
            cap,
            map: HashMap::new(),
            slots: Vec::new(),
            free: Vec::new(),
            head: NIL,
            tail: NIL,
        }
    }

    #[inline]
    pub(super) fn slot(&self, i: usize) -> &Slot {
        #[cfg(test)]
        note_lru_inspection();
        debug_assert!(i != NIL && i < self.slots.len());
        self.slots[i].as_ref().expect("槽位在用却为空")
    }

    #[inline]
    pub(super) fn slot_mut(&mut self, i: usize) -> &mut Slot {
        #[cfg(test)]
        note_lru_inspection();
        debug_assert!(i != NIL && i < self.slots.len());
        self.slots[i].as_mut().expect("槽位在用却为空")
    }

    /// 把槽 i 从链表摘下来（不动内容）
    pub(super) fn detach(&mut self, i: usize) {
        let (p, n) = {
            let s = self.slot(i);
            (s.prev, s.next)
        };
        match p {
            NIL => self.head = n,
            p => self.slot_mut(p).next = n,
        }
        match n {
            NIL => self.tail = p,
            n => self.slot_mut(n).prev = p,
        }
        let s = self.slot_mut(i);
        s.prev = NIL;
        s.next = NIL;
    }

    /// 把槽 i 挂到链表尾（= 最近用过）
    pub(super) fn attach_back(&mut self, i: usize) {
        // 先把 tail 抄出来：`slot_mut` 借走 self 之后不能再读 self.tail
        let t = self.tail;
        {
            let s = self.slot_mut(i);
            s.prev = t;
            s.next = NIL;
        }
        if t == NIL {
            self.head = i;
        } else {
            self.slot_mut(t).next = i;
        }
        self.tail = i;
    }

    /// 换出最久未用的那块
    pub(super) fn evict_lru(&mut self) {
        let i = self.head;
        if i == NIL {
            return;
        }
        let key = self.slot(i).key.clone();
        self.detach(i);
        self.slots[i] = None;
        self.map.remove(&key);
        self.free.push(i);
    }

    /// 取一个可用槽：先把表长到容量，再用回收来的空闲槽
    pub(super) fn take_slot(&mut self) -> usize {
        if self.slots.len() < self.cap {
            self.slots.push(None);
            return self.slots.len() - 1;
        }
        self.free
            .pop()
            .expect("未满的缓存里必须有空闲槽（不变量：slots.len()==cap=map.len()+free.len()）")
    }

    pub(super) fn get(&mut self, ns: u64, sst_id: u64, offset: u64) -> Option<CachedBlock> {
        let k = CacheKey { ns, sst_id, offset };
        let i = *self.map.get(&k)?;
        self.detach(i);
        self.attach_back(i);
        Some(self.slot(i).block.clone())
    }

    pub(super) fn put(&mut self, ns: u64, sst_id: u64, offset: u64, block: Arc<DataBlock>) {
        self.put_cached(ns, sst_id, offset, CachedBlock::Decoded(block));
    }

    pub(super) fn put_cached(&mut self, ns: u64, sst_id: u64, offset: u64, block: CachedBlock) {
        if self.cap == 0 {
            return;
        }
        let k = CacheKey { ns, sst_id, offset };
        if let Some(&i) = self.map.get(&k) {
            self.slot_mut(i).block = block;
            self.detach(i);
            self.attach_back(i);
            return;
        }
        while self.map.len() >= self.cap {
            self.evict_lru();
        }
        let i = self.take_slot();
        self.slots[i] = Some(Slot {
            key: k.clone(),
            block,
            prev: NIL,
            next: NIL,
        });
        self.map.insert(k, i);
        self.attach_back(i);
    }

    /// 改容量：**不重建缓存**，只换上限。
    ///
    /// 调大不用做事（下一次回填自然装得下）；调小按 LRU 顺序换出到上限为止。
    /// 必须有这条路，否则"跟着库长"的自动档只能靠 `BlockCache::new` 把已装进来的块全丢掉 ——
    /// 那等于每写满一次 memtable 就把缓存清空，自动档反而让点读更贵。
    pub fn set_capacity(&mut self, cap: usize) {
        self.cap = cap;
        while self.map.len() > cap {
            self.evict_lru();
        }
    }

    /// 当前上限（块数）。自动档的守卫读的是这个生效值，不是配置值。
    pub fn capacity(&self) -> usize {
        self.cap
    }

    /// 当前驻留块数
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// 与 [`Self::len`] 配对（clippy 要求 `len` 必须有 `is_empty`）
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}
