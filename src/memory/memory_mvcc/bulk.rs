//! 本文件负责批量载入及最终发布。

use super::*;

impl BulkLoader {
    pub fn put(&mut self, key: &[u8], value: Vec<u8>) {
        let mut side = self.store.lock().unwrap();
        self.core.put(&mut *side, key, value);
    }

    pub fn put_batch(&mut self, items: &[(Vec<u8>, Vec<u8>)]) {
        let mut side = self.store.lock().unwrap();
        self.core.put_batch_ref(&mut *side, items);
    }

    pub fn put_batch_owned(&mut self, items: Vec<(Vec<u8>, Vec<u8>)>) {
        let mut side = self.store.lock().unwrap();
        self.core.put_batch(&mut *side, items);
    }

    pub fn delete(&mut self, key: &[u8]) {
        let mut side = self.store.lock().unwrap();
        self.core.delete(&mut *side, key);
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let store = self.store.lock().unwrap();
        let low = encode_key(key, 0);
        let high = encode_key(key, u64::MAX);
        let mut best: Option<Vec<u8>> = None;
        for (enc, row) in store.range_scan(&low, &high) {
            if let Some(vk) = decode_key(&enc) {
                if vk.raw_key.as_slice() == key && vk.version <= self.core.version {
                    best = match row {
                        None => None,
                        Some(raw) => crate::memory::kv_ops::logical_to_user(raw),
                    };
                }
            }
        }
        best
    }

    pub fn finish(mut self) {
        self.core.first_finish();
    }
}

impl Drop for BulkLoader {
    fn drop(&mut self) {
        // 忘了 `finish()` 也得把这次会话的版本号占掉（bulk 不写 WAL、不回滚，见 [`BulkCore`]）。
        // 内存引擎收尾没有别的事要做，所以这里只剩幂等守卫本身。
        self.core.first_finish();
    }
}
