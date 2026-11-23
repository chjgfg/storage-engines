//! 本文件负责内存表写入和版本遍历。

use super::*;

impl LSMTree {
    // ─── 逻辑 KV ─────────────────────────────────────────────────────────────

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.insert(key, Some(value));
    }

    pub fn delete(&mut self, key: Vec<u8>) {
        self.insert(key, None);
    }

    // ─── 版本化 API ──────────────────────────────────────────────────────────

    pub fn insert(&mut self, key: Vec<u8>, value: Option<Vec<u8>>) {
        self.insert_inner(key, value, false);
    }

    /// 高速写入：跳过「读旧值维护 live 统计」与 TTL meta（bulk / 压测用）。
    /// 语义仍正确；`stats().live_keys` 可能略不准，可用 `stats_exact()`。
    pub fn insert_fast(&mut self, key: Vec<u8>, value: Option<Vec<u8>>) {
        self.insert_inner(key, value, true);
    }

    /// 批量高速写入：一次不抢锁外逻辑，调用方已持 `&mut self`
    pub fn insert_batch_fast(
        &mut self,
        items: impl IntoIterator<Item = (Vec<u8>, Option<Vec<u8>>)>,
    ) {
        for (k, v) in items {
            self.insert_inner(k, v, true);
        }
    }

    pub(super) fn insert_inner(&mut self, key: Vec<u8>, mut value: Option<Vec<u8>>, fast: bool) {
        // 大 value 旁路
        if let Some(ref v) = value {
            if v.len() >= self.opts.blob_threshold {
                if let Ok(ptr) = self.write_blob(v) {
                    value = Some(ptr);
                }
            }
        }

        let seq = self.write_seq.fetch_add(1, Ordering::SeqCst);
        if let Some(wal) = self.mem_wal.as_mut() {
            let _ = wal.append(MemWalRecord::Put {
                key: key.clone(),
                value: value.clone(),
                seq,
            });
            if self.opts.mem_wal_sync_on_write {
                let _ = wal.sync();
            }
        }

        if !fast {
            let had = self
                .get_entry_at(&key, u64::MAX)
                .map(|e| matches!(e.value, EntryValue::Put(Some(_))))
                .unwrap_or(false);
            self.mem_insert_no_wal(key.clone(), StoredEntry::put(seq, value.clone()));
            if matches!(value, Some(_)) {
                if !had {
                    self.cached_live = self.cached_live.saturating_add(1);
                }
            } else if had {
                self.cached_live = self.cached_live.saturating_sub(1);
                self.cached_tomb = self.cached_tomb.saturating_add(1);
            } else {
                self.cached_tomb = self.cached_tomb.saturating_add(1);
            }
            if let Some(secs) = self.opts.default_ttl_secs {
                if value.is_some() {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0);
                    self.ttl_meta.insert(key, now.saturating_add(secs));
                } else {
                    self.ttl_meta.remove(&key);
                }
            }
        } else {
            self.mem_insert_no_wal(key, StoredEntry::put(seq, value));
            // 近似：当新 key
            self.cached_live = self.cached_live.saturating_add(1);
        }

        self.dirty = true;
        self.maybe_flush_mem();
        self.maybe_stall_on_l0();
    }

    pub fn remove(&mut self, key: &[u8]) -> bool {
        // 写 Drop 版本，保留历史供 Snapshot 读取
        if self.get_entry(key).is_none() {
            return false;
        }
        let seq = self.write_seq.fetch_add(1, Ordering::SeqCst);
        if let Some(wal) = self.mem_wal.as_mut() {
            let _ = wal.append(MemWalRecord::Drop {
                key: key.to_vec(),
                seq,
            });
            if self.opts.mem_wal_sync_on_write {
                let _ = wal.sync();
            }
        }
        if self
            .get_entry_at(key, u64::MAX)
            .map(|e| matches!(e.value, EntryValue::Put(Some(_))))
            .unwrap_or(false)
        {
            self.cached_live = self.cached_live.saturating_sub(1);
        }
        self.ttl_meta.remove(key);
        self.mem_insert_no_wal(key.to_vec(), StoredEntry::drop_at(seq));
        self.dirty = true;
        self.stats_dirty = true;
        self.maybe_flush_mem();
        true
    }
    /// 利用回收扫描已经观察到的记录种类写入删除。
    /// 省去重复点查的前提是来源种类已确认；写入与阈值刷盘的错误仍向上传播。

    pub(crate) fn remove_vacuum_known(&mut self, row: VacuumDelete) -> std::io::Result<()> {
        #[cfg(test)]
        if vacuum_delete_probe::reference() {
            assert!(self.remove(&row.key));
            return Ok(());
        }
        #[cfg(test)]
        let write_timer = maintenance_probe::Timer::start(maintenance_probe::Kind::DeleteWrite);
        let seq = self.write_seq.fetch_add(1, Ordering::SeqCst);
        if let Some(wal) = self.mem_wal.as_mut() {
            wal.append(MemWalRecord::Drop {
                key: row.key.clone(),
                seq,
            })?;
            if self.opts.mem_wal_sync_on_write {
                #[cfg(test)]
                let _sync = maintenance_probe::Timer::start(maintenance_probe::Kind::MemWalSync);
                wal.sync()?;
            }
        }
        if row.live {
            self.cached_live = self.cached_live.saturating_sub(1);
        }
        self.ttl_meta.remove(&row.key);
        self.mem_insert_no_wal(row.key, StoredEntry::drop_at(seq));
        self.dirty = true;
        self.stats_dirty = true;
        #[cfg(test)]
        drop(write_timer);
        // 删除写入失败时，在 blob 回收前将本轮维护判为未完成。
        self.maybe_flush_mem_checked()?;
        #[cfg(test)]
        crate::common::maintenance_probe::note_lsm_known_delete();
        Ok(())
    }

    pub(super) fn mem_insert_no_wal(&mut self, key: Vec<u8>, val: StoredEntry) {
        let add = key.len() + val.estimate_bytes();
        let versions = self.mem.entry(key).or_default();
        let replaced = versions.insert(val.seq, val.value);
        // 上面那句之后 `versions` 的借用就结束了，所以这里才允许动 `self` 的其它字段
        match replaced {
            // 同 seq 被覆盖：条数不变，只换字节账
            Some(old) => {
                self.mem_bytes = self
                    .mem_bytes
                    .saturating_sub(old.estimate_bytes())
                    .saturating_add(add);
            }
            None => {
                self.mem_bytes = self.mem_bytes.saturating_add(add);
                self.mem_entries += 1;
            }
        }
    }

    /// 慢口径：真把整个 memtable 数一遍。**只给测试当对照**——生产路径一律读 `mem_entries`。
    ///
    /// 留着它有两个用处：① 与增量计数器对拍口径（"版本条数"不是"键数"）；
    /// ② 当复杂度守卫的探针 —— 它每被调一次就把"走过的条目数"记账，所以"热路径有没有偷偷
    /// 现算"是能直接数出来的（M19 的守卫断言灌 4000 行期间这个计数**一点没涨**）。
    #[cfg(test)]
    pub(super) fn scan_mem_entry_count(&self) -> usize {
        note_mem_scan_steps(self.mem.len() as u64);
        self.mem.values().map(|v| v.len()).sum()
    }

    pub(super) fn mem_get_at(map: &MemMap, key: &[u8], snap_seq: u64) -> Option<StoredEntry> {
        let vers = map.get(key)?;
        vers.iter()
            .rev()
            .find(|(seq, _)| **seq <= snap_seq)
            .map(|(seq, v)| StoredEntry {
                seq: *seq,
                value: v.clone(),
            })
    }

    pub(super) fn mem_range_at(
        map: &MemMap,
        low: &[u8],
        high: &[u8],
        snap_seq: u64,
    ) -> Vec<(Vec<u8>, StoredEntry)> {
        let mut out = Vec::new();
        for (k, vers) in map.range(low.to_vec()..=high.to_vec()) {
            if let Some(se) =
                vers.iter()
                    .rev()
                    .find(|(seq, _)| **seq <= snap_seq)
                    .map(|(seq, v)| StoredEntry {
                        seq: *seq,
                        value: v.clone(),
                    })
            {
                out.push((k.clone(), se));
            }
        }
        out
    }

    /// mem / immutable 的**重量**视图，但是**惰性**的（M46）：判据与改前那条 `mem_view_at`（整表折进 `Vec`，M46 起删除）
    /// 一字不差（每把键取"≤ snap 的最大 seq"那一版，`MemMap` 本来就是按键升序），
    /// 区别只有一个 —— 不再一次折成 `Vec`。
    pub(super) fn mem_iter_at<'a>(
        map: &'a MemMap,
        snap_seq: u64,
    ) -> impl Iterator<Item = (Vec<u8>, StoredEntry)> + 'a {
        map.iter().filter_map(move |(k, vers)| {
            let (seq, ev) = vers.iter().rev().find(|(s, _)| **s <= snap_seq)?;
            // 重量路径：value 被整个克隆出来。全库折叠不该走这里 —— 计数探针就是用来
            // 发现「不知不觉又用回重量路径」。守卫另靠它数"交出第一行之前已经物化了多大一截"。
            #[cfg(test)]
            crate::lsm_tree::sst::note_value_materialization();
            Some((
                k.clone(),
                StoredEntry {
                    seq: *seq,
                    value: ev.clone(),
                },
            ))
        })
    }

    /// mem / immutable 的轻量视图（惰性）：`MemMap` 每个 key 一条版本列表 ⇒ 范围内逐 key
    /// 取"≤ snap 的最大 seq"那一版，与 `mem_view_light_range` 同一判据。
    pub(super) fn mem_light_iter<'a>(
        map: &'a MemMap,
        from: Option<&'a [u8]>,
        to: Option<&'a [u8]>,
        snap_seq: u64,
    ) -> impl Iterator<Item = (Vec<u8>, LightEntry)> + 'a {
        Self::mem_projected_iter(map, from, to, snap_seq, |seq, value| LightEntry {
            seq,
            kind: value.kind_byte(),
        })
    }

    pub(super) fn mem_projected_iter<'a, R: 'a>(
        map: &'a MemMap,
        from: Option<&'a [u8]>,
        to: Option<&'a [u8]>,
        snap_seq: u64,
        project: fn(u64, &'a EntryValue) -> R,
    ) -> impl Iterator<Item = (Vec<u8>, R)> + 'a {
        use std::ops::Bound;
        let lo = match from {
            Some(k) => Bound::Included(k),
            None => Bound::Unbounded,
        };
        let hi = match to {
            Some(k) => Bound::Excluded(k),
            None => Bound::Unbounded,
        };
        map.range::<[u8], _>((lo, hi)).filter_map(move |(k, vers)| {
            let (seq, ev) = vers.iter().rev().find(|(s, _)| **s <= snap_seq)?;
            Some((k.clone(), project(*seq, ev)))
        })
    }
}
