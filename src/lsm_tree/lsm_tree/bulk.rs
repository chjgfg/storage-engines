//! 本文件负责批量载入及最终发布。

use super::*;

impl LSMTree {
    /// 开关 bulk 模式：写路径 flush 不 compact，L0 限流放宽
    pub fn set_bulk_mode(&mut self, on: bool) {
        self.bulk_mode = on;
        if on {
            // 进一步放大阈值
            self.opts.mem_threshold_bytes = self.opts.mem_threshold_bytes.max(128 * 1024 * 1024);
            self.opts.mem_threshold_entries = self.opts.mem_threshold_entries.max(1_000_000);
            self.opts.l0_stop_trigger = self.opts.l0_stop_trigger.max(128);
            self.opts.l0_slowdown_trigger = self.opts.l0_slowdown_trigger.max(64);
            self.opts.max_compactions_per_flush = 1;
        }
    }

    pub fn bulk_mode(&self) -> bool {
        self.bulk_mode
    }

    /// Bulk 极速路径：把 **已按 key 排序** 的条目直接写成一个 L0 SST（跳过 MemTable）。
    /// 调用方保证 batch 内 key 严格递增。
    pub fn bulk_ingest_sorted(
        &mut self,
        entries: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    ) -> std::io::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let id = self.next_sst_id.fetch_add(1, Ordering::SeqCst);
        let path = self.sst_path(id);
        // bulk：关 bloom + 大 block，减少 CPU；不 fsync
        let mut builder = SstBuilder::with_options(
            id,
            &path,
            if self.bulk_mode {
                256
            } else {
                self.opts.sst_block_entries
            },
            if self.bulk_mode {
                false
            } else {
                self.opts.enable_bloom
            },
        )
        .with_bloom_bits(self.opts.bloom_bits_per_key);
        for (k, v) in entries {
            let seq = self.write_seq.fetch_add(1, Ordering::SeqCst);
            builder.add(k, StoredEntry::put(seq, v));
        }
        let n = builder.len();
        let mut sst = builder.finish_with_sync(!self.bulk_mode)?;
        sst.set_cache_ns(Self::dir_cache_ns(&self.dir));
        sst.set_block_cache(self.block_cache.clone());
        if self.opts.use_mmap {
            let _ = sst.enable_mmap();
        }
        self.levels[0].push(sst);
        self.cached_live = self.cached_live.saturating_add(n);
        self.dirty = true;
        self.reevaluate_cache_capacity();
        if self.bulk_mode {
            let l0 = self.levels[0].len();
            if l0 >= self.opts.l0_stop_trigger {
                let _ = self.compact_level(0);
            }
            // 每次 ingest 更新 MANIFEST（bulk 下不 fsync，成本低）
            self.write_manifest()?;
        } else if self.levels[0].len() >= self.opts.l0_compact_threshold {
            let _ = self.maybe_compact();
            self.write_manifest()?;
        } else {
            self.write_manifest()?;
        }
        Ok(())
    }

    pub fn bulk_put<I>(&mut self, items: I)
    where
        I: IntoIterator<Item = (Vec<u8>, Option<Vec<u8>>)>,
    {
        for (k, v) in items {
            self.insert(k, v);
        }
    }
}
