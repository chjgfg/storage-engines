//! 本文件负责批量载入及最终发布。

use super::*;

impl BulkLoader {
    pub fn put(&mut self, key: &[u8], value: Vec<u8>) {
        let enc = encode_key(key, self.core.version);
        let stored = {
            let mut blob = self.blob.lock().unwrap();
            // bulk：不每条 fsync blob，finish 时统一 sync
            encode_stored_value(&mut blob, value, false).expect("blob encode")
        };
        let mut kv = self.kv.lock().unwrap();
        insert_or_shout(&mut kv, enc, RowId(Some(stored)), "bulk put");
    }

    /// 批量 put：一次加锁插入多条，显著降低锁/调度开销
    pub fn put_batch(&mut self, items: &[(Vec<u8>, Vec<u8>)]) {
        let mut blob = self.blob.lock().unwrap();
        let mut kv = self.kv.lock().unwrap();
        for (key, value) in items {
            let enc = encode_key(key, self.core.version);
            let stored = encode_stored_value(&mut blob, value.clone(), false).expect("blob");
            insert_or_shout(&mut kv, enc, RowId(Some(stored)), "bulk put_batch");
        }
    }

    /// 零额外 clone 的批量 put（消费 items）
    pub fn put_batch_owned(&mut self, items: Vec<(Vec<u8>, Vec<u8>)>) {
        let mut blob = self.blob.lock().unwrap();
        let mut kv = self.kv.lock().unwrap();
        for (key, value) in items {
            let enc = encode_key(&key, self.core.version);
            let stored = encode_stored_value(&mut blob, value, false).expect("blob");
            insert_or_shout(&mut kv, enc, RowId(Some(stored)), "bulk put_batch_owned");
        }
    }

    pub fn delete(&mut self, key: &[u8]) {
        let enc = encode_key(key, self.core.version);
        let mut kv = self.kv.lock().unwrap();
        insert_or_shout(&mut kv, enc, RowId(None), "bulk delete");
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut kv = self.kv.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();
        let low = encode_key(key, 0);
        let high = encode_key(key, u64::MAX);
        let mut best: Option<Vec<u8>> = None;
        for (enc, row) in kv.range_scan(low, high) {
            if let Some(vk) = decode_key(&enc) {
                if vk.raw_key.as_slice() == key && vk.version <= self.core.version {
                    best = match row.0 {
                        None => None,
                        Some(stored) => decode_stored_value(&mut blob, &stored)
                            .ok()
                            .flatten()
                            .and_then(crate::bplus_tree::kv_ops::logical_to_user),
                    };
                }
            }
        }
        best
    }

    pub fn finish(mut self) {
        self.finish_inner().expect("B+ bulk finish failed");
    }

    pub fn try_finish(mut self) -> std::io::Result<()> {
        self.finish_inner()
    }

    pub(super) fn finish_inner(&mut self) -> std::io::Result<()> {
        // 幂等守卫在内核里（`finish()` 与 `Drop` 都会调），收尾动作本身是 bplus 自己的事
        if !self.core.first_finish() {
            return Ok(());
        }
        let mut kv = self.kv.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();
        let mut wal = self.wal.lock().unwrap();
        blob.sync()?;
        let next_ver = self.core.next_version().load(Ordering::SeqCst);
        kv.set_next_version(next_ver);
        kv.try_flush_all()?;
        kv.set_bulk_mode(false);
        let root = kv.root_page_id();
        let next_page = kv.next_page_id();
        // M52：这里是 `append(Checkpoint) → sync → truncate()` —— `truncate()` 只留 header，
        // 刚写的那条 Checkpoint 被自己抹掉 ⇒ 下一次 open 拿不到起点（bplus 还有 `NVER` meta
        // 兜着，所以表现为"README 写的第二级永远走不到"）。收口只有一份：`seal_wal_checkpoint`。
        seal_wal_checkpoint(&mut wal, next_ver, root.0, next_page)
    }
}

impl Drop for BulkLoader {
    fn drop(&mut self) {
        if let Err(error) = self.finish_inner() {
            log::error!("B+ bulk drop failed; reopen for recovery: {error}");
        }
    }
}
