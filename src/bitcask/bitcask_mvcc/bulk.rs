//! 本文件负责批量载入及最终发布。

use super::*;

impl BulkLoader {
    pub fn put(&mut self, key: &[u8], value: Vec<u8>) {
        let enc = encode_key(key, self.core.version);
        let stored = {
            let mut blob = self.blob.lock().unwrap();
            // bulk：不每条 fsync blob
            encode_stored_value(&mut blob, value, false).expect("blob encode")
        };
        let mut kv = self.kv.lock().unwrap();
        let _ = kv.insert(enc, Some(stored));
    }

    /// 批量 put：一次加锁插入多条
    pub fn put_batch(&mut self, items: &[(Vec<u8>, Vec<u8>)]) {
        let mut blob = self.blob.lock().unwrap();
        let mut kv = self.kv.lock().unwrap();
        let ver = self.core.version;
        let batch: Vec<(Vec<u8>, Option<Vec<u8>>)> = items
            .iter()
            .map(|(k, v)| {
                let enc = encode_key(k, ver);
                let stored = encode_stored_value(&mut blob, v.clone(), false).expect("blob");
                (enc, Some(stored))
            })
            .collect();
        let _ = kv.insert_batch(batch);
    }

    /// 零额外 clone 的批量 put（消费 items）— 压测主路径
    pub fn put_batch_owned(&mut self, items: Vec<(Vec<u8>, Vec<u8>)>) {
        let mut blob = self.blob.lock().unwrap();
        let mut kv = self.kv.lock().unwrap();
        let ver = self.core.version;
        let batch: Vec<(Vec<u8>, Option<Vec<u8>>)> = items
            .into_iter()
            .map(|(k, v)| {
                let enc = encode_key(&k, ver);
                let stored = encode_stored_value(&mut blob, v, false).expect("blob");
                (enc, Some(stored))
            })
            .collect();
        let _ = kv.insert_batch(batch);
    }

    pub fn delete(&mut self, key: &[u8]) {
        let enc = encode_key(key, self.core.version);
        let mut kv = self.kv.lock().unwrap();
        let _ = kv.insert(enc, None);
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut kv = self.kv.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();
        let low = encode_key(key, 0);
        let high = encode_key(key, u64::MAX);
        let mut best: Option<Vec<u8>> = None;
        if let Ok(range) = kv.range_scan(low, high) {
            for (enc, val) in range {
                if let Some(vk) = decode_key(&enc) {
                    if vk.raw_key.as_slice() == key && vk.version <= self.core.version {
                        best = match val {
                            None => None,
                            Some(stored) => decode_stored_value(&mut blob, &stored)
                                .ok()
                                .flatten()
                                .and_then(crate::bitcask::kv_ops::logical_to_user),
                        };
                    }
                }
            }
        }
        best
    }

    /// 中途 flush 写缓冲（不 fsync），降低峰值内存
    pub fn flush_buf(&mut self) {
        let mut kv = self.kv.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();
        let _ = kv.flush_buf();
        let _ = blob.sync();
    }

    pub fn finish(mut self) {
        self.finish_inner();
    }

    pub(super) fn finish_inner(&mut self) {
        // 幂等守卫在内核里（`finish()` 与 `Drop` 都会调），收尾动作本身是 bitcask 自己的事
        if !self.core.first_finish() {
            return;
        }
        let mut kv = self.kv.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();
        let mut wal = self.wal.lock().unwrap();
        let _ = blob.sync();
        let _ = kv.flush();
        // bulk 结束后写 hint，避免下次 open 全量扫
        let _ = kv.dump_hint();
        let next_ver = self.core.next_version().load(Ordering::SeqCst);
        // M52：以前这里是 `append(Checkpoint) → sync → truncate` —— `truncate()` 只留 header，
        // **刚写的那条 Checkpoint 被自己抹掉**，于是下一次 open 拿不到起点 ⇒ 退到
        // `scan_max_version` = O(全库 KeyDir)（M48 实测 600k 档那一次多花 89 ms）。
        // 收口顺序是 `seal_wal_checkpoint` 的既有规矩（先截断再写回），别再抄第二份。
        let _ = seal_wal_checkpoint(&mut wal, next_ver);
    }
}

impl Drop for BulkLoader {
    fn drop(&mut self) {
        self.finish_inner();
    }
}
