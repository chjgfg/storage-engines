//! 本文件负责SST 流式构造与持久化完成。

use super::*;

/// `entries` 的字节数（旧算法，留着当对拍与尺子；生产路径一步都不该走它）
#[cfg(test)]
pub(super) fn sum_entries_bytes(entries: &[(Vec<u8>, StoredEntry)]) -> usize {
    BUILDER_SIZE_WALKS.with(|c| c.set(c.get() + entries.len() as u64));
    entries
        .iter()
        .map(|(k, se)| k.len() + se.estimate_bytes() + 8)
        .sum()
}

impl SstBuilder {
    pub fn new(id: u64, path: impl AsRef<Path>) -> Self {
        Self::with_options(id, path, 64, true)
    }

    pub fn with_bloom_bits(mut self, bits: usize) -> Self {
        self.bloom_bits_per_key = bits.max(4);
        self
    }

    pub fn with_options(
        id: u64,
        path: impl AsRef<Path>,
        block_entries: usize,
        enable_bloom: bool,
    ) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            id,
            block_entries: block_entries.max(1),
            enable_bloom,
            bloom_bits_per_key: 10,
            pending: Vec::with_capacity(block_entries.max(1)),
            hashes: Vec::new(),
            index: Vec::new(),
            writer: None,
            written: 0,
            tail_key: None,
            count: 0,
            est_bytes: 0,
            io_err: None,
            move_eligibility: MoveEligibility::Ready,
        }
    }

    pub fn add(&mut self, key: Vec<u8>, se: StoredEntry) {
        let previous = self
            .pending
            .last()
            .map(|(k, _)| k.as_slice())
            .or(self.tail_key.as_deref());
        if matches!(se.value, EntryValue::Drop) || previous.is_some_and(|k| k >= key.as_slice()) {
            self.move_eligibility = MoveEligibility::RewriteRequired;
        }
        // 有序断言：上一键可能在还没落盘的那半块里，也可能已经是上一块的尾键
        debug_assert!(
            match self.pending.last() {
                Some((k, _)) => k.as_slice() <= key.as_slice(),
                None => self.tail_key.as_deref().is_none_or(|k| k <= key.as_slice()),
            },
            "SST keys must be non-decreasing"
        );
        // 口径与 `sum_entries_bytes` 里那句一字不差（对拍守卫钉着这两处）
        self.est_bytes += key.len() + se.estimate_bytes() + 8;
        self.hashes.push(Bloom::hash_pair(&key));
        self.count += 1;
        self.pending.push((key, se));
        #[cfg(test)]
        BUILDER_HELD_MAX.with(|c| {
            let n = self.pending.len() as u64;
            if n > c.get() {
                c.set(n);
            }
        });
        #[cfg(test)]
        BUILDER_HASHED_MAX.with(|c| {
            let n = self.hashes.len() as u64;
            if n > c.get() {
                c.set(n);
            }
        });
        if self.pending.len() >= self.block_entries {
            if let Err(e) = self.emit_block() {
                // 只留第一个错误：后面再失败是同一件事的重复，而 `finish` 必须如实抛出那一个
                if self.io_err.is_none() {
                    self.io_err = Some(e);
                }
            }
        }
    }

    /// 把攒满的一块编码并写盘，同时追加索引与尾键。
    ///
    /// ⚠️ 块的**分组方式与偏移算法与改前逐字一致**（改前是 `finish` 里 `while i < len` 按
    /// `block_entries` 切片；这里是攒满即写）⇒ 同一个文件字节相同，由
    /// `test_sst_builder_byteprint_is_stable` 钉住。写失败时**不**清 `pending`（数据还在手里，
    /// 错误由 `add` 存下、`finish` 抛出）。
    pub(super) fn emit_block(&mut self) -> std::io::Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let block_raw = encode_block(&self.pending, true);
        self.ensure_writer()?;
        let offset = self.written;
        // `written` 自己累计 ⇒ 不必每块 `stream_position()` 把 1 MiB 缓冲冲一次
        if let Some(f) = self.writer.as_mut() {
            f.write_all(&block_raw)?;
        }
        self.written += block_raw.len() as u64;
        let first_key = self.pending[0].0.clone();
        self.tail_key = Some(self.pending[self.pending.len() - 1].0.clone());
        self.index.push(IndexEntry {
            first_key,
            offset,
            len: block_raw.len() as u32,
        });
        self.pending.clear();
        Ok(())
    }

    /// 建文件（一次）。路径仍是 `<path>.sst.tmp` → `finish` 末尾 rename，与改前同一套落盘顺序。
    pub(super) fn ensure_writer(&mut self) -> std::io::Result<()> {
        if self.writer.is_some() {
            return Ok(());
        }
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = self.path.with_extension("sst.tmp");
        let f = File::create(tmp)?;
        self.writer = Some(std::io::BufWriter::with_capacity(1024 * 1024, f));
        Ok(())
    }

    pub fn add_put(&mut self, key: Vec<u8>, seq: u64, value: Option<Vec<u8>>) {
        self.add(key, StoredEntry::put(seq, value));
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// O(1)：读增量账。**不许**改回 `entries.iter().sum()` —— 这里每被调一次都要走一遍整表的话，
    /// `merge_to_ssts` 的"每行问一次"就把压实变成 O(表内行数²)（实测 600k 行 18.0 s，94% 在这一步）。
    pub fn estimated_bytes(&self) -> usize {
        self.est_bytes
    }

    pub fn finish(self) -> std::io::Result<SstFile> {
        self.finish_with_sync(true)
    }
    /// 完成剩余块、索引、过滤器和页脚后，按调用契约同步输出 SST。
    /// 输出完成前不能发布为可读文件；失败通过错误返回并保留清理所有权。

    /// `sync=false`：不 fsync（bulk 导入用，结束时统一落盘）
    pub fn finish_with_sync(mut self, sync: bool) -> std::io::Result<SstFile> {
        #[cfg(test)]
        let _timer = crate::lsm_tree::lsm_tree::maintenance_probe::Timer::start(
            crate::lsm_tree::lsm_tree::maintenance_probe::Kind::SstFinish,
        );
        // 最后那半块
        if !self.pending.is_empty() {
            self.emit_block()?;
        }
        if let Some(e) = self.io_err.take() {
            return Err(e);
        }
        // 一条都没写过的空表：与改前一样，到这一步才 create
        self.ensure_writer()?;
        let mut f = self.writer.take().expect("ensure_writer 之后一定有");
        let index = std::mem::take(&mut self.index);
        let mut bloom = if self.enable_bloom {
            Bloom::new(self.count.max(1), self.bloom_bits_per_key)
        } else {
            Bloom::empty()
        };
        for &(h1, h2) in &self.hashes {
            bloom.set_pair(h1, h2);
        }
        let index_blob = encode_index(&index);
        let index_off = self.written;
        f.write_all(&index_blob)?;
        self.written += index_blob.len() as u64;
        let bloom_blob = if self.enable_bloom {
            bloom.encode()
        } else {
            Vec::new()
        };
        let bloom_off = self.written;
        if !bloom_blob.is_empty() {
            f.write_all(&bloom_blob)?;
            self.written += bloom_blob.len() as u64;
        }
        // min/max 与条目数从增量账拿：`index[0].first_key` 就是第一块的首键，
        // `tail_key` 就是最后一块的尾键 ⇒ 与改前 `min_max_keys_stored(&entries)` 同一个值
        let min_key = index
            .first()
            .map(|e| e.first_key.clone())
            .unwrap_or_default();
        let max_key = self.tail_key.clone().unwrap_or_default();
        let mut meta = Vec::new();
        meta.extend_from_slice(&index_off.to_le_bytes());
        meta.extend_from_slice(&(index_blob.len() as u32).to_le_bytes());
        meta.extend_from_slice(&bloom_off.to_le_bytes());
        meta.extend_from_slice(&(bloom_blob.len() as u32).to_le_bytes());
        meta.extend_from_slice(&(self.count as u64).to_le_bytes());
        meta.extend_from_slice(&(min_key.len() as u32).to_le_bytes());
        meta.extend_from_slice(&min_key);
        meta.extend_from_slice(&(max_key.len() as u32).to_le_bytes());
        meta.extend_from_slice(&max_key);
        meta.push(1u8); // has_seq flag
        meta.extend_from_slice(SST_MAGIC_V2);
        let fc = crc32(&meta);
        let mut footer = meta;
        footer.extend_from_slice(&fc.to_le_bytes());
        let footer_len = footer.len() as u32;
        f.write_all(&footer)?;
        f.write_all(&footer_len.to_le_bytes())?;
        f.flush()?;
        if sync {
            #[cfg(test)]
            let _timer = crate::lsm_tree::lsm_tree::maintenance_probe::Timer::start(
                crate::lsm_tree::lsm_tree::maintenance_probe::Kind::SstSync,
            );
            f.get_mut().sync_all()?;
        }
        let _ = std::fs::remove_file(&self.path);
        std::fs::rename(self.path.with_extension("sst.tmp"), &self.path)?;
        let mut sst = SstFile::open(self.id, &self.path)?;
        // 磁盘打开的文件资格标为未知，不改变格式，也不暗中全量扫描。
        if sync && self.count > 0 {
            sst.move_eligibility = self.move_eligibility;
        }
        Ok(sst)
    }
}

/// 没走到 `finish` 的 builder（`merge_to_ssts` 中途 IO 失败，或调用方提前把它丢了）现在会留下
/// 一个半截的 `<id>.sst.tmp` —— 改前是"什么都没写，所以什么都不留"。目录扫描不认这个扩展名、
/// 加载以 MANIFEST 为准，所以它不影响正确性，但**会一直占盘**：长跑进程里这类垃圾是要攒的。
/// `finish` 成功之后 rename 已经把那个名字移走 ⇒ 这里是一次失败的 `remove_file`，被 `let _ =` 吃掉。
impl Drop for SstBuilder {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(self.path.with_extension("sst.tmp"));
    }
}
