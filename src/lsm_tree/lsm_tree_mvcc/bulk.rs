//! 本文件负责批量载入及最终发布。

use super::*;

impl BulkLoader {
    pub fn put(&mut self, key: &[u8], value: Vec<u8>) {
        let enc = encode_key(key, self.core.version);
        let mut kv = self.kv.lock().unwrap();
        kv.insert_fast(enc, Some(value));
    }

    /// 批量 put：一次加锁插入多条（clone value）
    pub fn put_batch(&mut self, items: &[(Vec<u8>, Vec<u8>)]) {
        let mut kv = self.kv.lock().unwrap();
        let ver = self.core.version;
        kv.insert_batch_fast(
            items
                .iter()
                .map(|(k, v)| (encode_key(k, ver), Some(v.clone()))),
        );
    }

    /// 零额外 clone 的批量 put（消费 items）——压测主路径
    /// 按 key 排序后 **直接写 L0 SST**，跳过 MemTable。
    pub fn put_batch_owned(&mut self, mut items: Vec<(Vec<u8>, Vec<u8>)>) {
        if items.is_empty() {
            return;
        }
        // 若已有序（顺序导入）跳过 sort
        let sorted = items.windows(2).all(|w| w[0].0 <= w[1].0);
        if !sorted {
            items.sort_by(|a, b| a.0.cmp(&b.0));
        }
        let ver = self.core.version;
        let mut out: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::with_capacity(items.len());
        for (k, v) in items {
            let enc = encode_key(&k, ver);
            if let Some((last_k, _)) = out.last() {
                if last_k == &enc {
                    out.pop();
                }
            }
            out.push((enc, Some(v)));
        }
        let mut kv = self.kv.lock().unwrap();
        let _ = kv.bulk_ingest_sorted(out);
    }

    /// 预编码 key 的批量 put（调用方已 encode_key 且排序）
    pub fn put_encoded_batch(&mut self, mut items: Vec<(Vec<u8>, Option<Vec<u8>>)>) {
        if items.is_empty() {
            return;
        }
        items.sort_by(|a, b| a.0.cmp(&b.0));
        let mut out = Vec::with_capacity(items.len());
        for (k, v) in items {
            if let Some((last_k, _)) = out.last() {
                if last_k == &k {
                    out.pop();
                }
            }
            out.push((k, v));
        }
        let mut kv = self.kv.lock().unwrap();
        let _ = kv.bulk_ingest_sorted(out);
    }

    pub fn delete(&mut self, key: &[u8]) {
        let enc = encode_key(key, self.core.version);
        let mut kv = self.kv.lock().unwrap();
        kv.insert_fast(enc, None);
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let kv = self.kv.lock().unwrap();
        let low = encode_key(key, 0);
        let high = encode_key(key, u64::MAX);
        // bulk 的读口径是「不比自己新的最大版本」：同样只定位链尾，不物化整条版本链
        kv.find_back(&low, &high, u64::MAX, |enc, _val| {
            decode_key(enc)
                .map(|vk| vk.raw_key.as_slice() == key && vk.version <= self.core.version)
                .unwrap_or(false)
        })
        .and_then(|(_, v)| v)
    }
    /// 把当前内存表转换为持久 SST，再发布当前文件集合。
    /// 刷盘与压实的清单发布状态必须明确区分，不能省掉尚未完成的持久化屏障。

    /// 中途可手动 flush，降低峰值内存
    pub fn flush_mem(&mut self) {
        let _ = self.kv.lock().unwrap().flush();
    }

    /// 结束 bulk：flush MANIFEST + WAL checkpoint。
    ///
    /// **默认不做大规模 L0 compact**（10M 级会卡很久）。
    /// 需要收口 L0 时用 [`Self::finish_with_compact`]。
    pub fn finish(self) {
        self.finish_with_compact(0);
    }

    /// 结束 bulk，并最多 compact `compact_limit` 轮 L0→L1。
    ///
    /// - `compact_limit == 0`：与 [`Self::finish`] 相同，只落盘不 compact
    /// - `compact_limit > 0`：调用 [`LSMTree::compact_l0_until`]，目标 L0 ≤ `l0_compact_threshold - 1`
    ///
    /// 大批导入后读延迟敏感时建议 `finish_with_compact(16)` 之类；纯压测吞吐用 `finish()`。
    pub fn finish_with_compact(mut self, compact_limit: usize) {
        self.finish_inner(compact_limit);
    }

    pub(super) fn finish_inner(&mut self, compact_limit: usize) {
        // 幂等守卫在内核里（`finish()` 与 `Drop` 都会调），收尾动作本身是 LSM 自己的事
        if !self.core.first_finish() {
            return;
        }
        let mut kv = self.kv.lock().unwrap();
        let mut wal = self.wal.lock().unwrap();
        // bulk 期间可能未写完 MANIFEST：flush 空 mem 并落盘清单
        let _ = kv.flush();
        kv.set_bulk_mode(false);
        if compact_limit > 0 {
            let target = kv.options().l0_compact_threshold.saturating_sub(1);
            let _ = kv.compact_l0_until(target, compact_limit);
        }
        let next_ver = self.core.next_version().load(Ordering::SeqCst);
        // M52：与 bitcask 同一个洞 —— `append(Checkpoint) → sync → truncate()` 把自己刚写的
        // 记录抹掉，下一次 open 只能退回全库扫描拿起点。收口只有一份：`seal_wal_checkpoint`。
        let _ = seal_wal_checkpoint(&mut wal, next_ver);
        persist_mvcc_next(
            // BulkLoader 没有 db_dir 字段：从 kv.dir 取
            kv.dir(),
            next_ver,
        );
    }
}

impl Drop for BulkLoader {
    fn drop(&mut self) {
        self.finish_inner(0);
    }
}
