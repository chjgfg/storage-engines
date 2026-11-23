//! 本文件负责SST 定位读取和缓存接入。

use super::*;

// Windows seek_read 接收显式偏移，不改变文件游标；保留句柄锁以统一生命周期和平台行为。
pub(super) fn read_sst_exact(file: &mut File, offset: u64, buf: &mut [u8]) -> std::io::Result<()> {
    #[cfg(test)]
    if positioned_read_reference::enabled() {
        positioned_read_reference::note_seek();
        file.seek(SeekFrom::Start(offset))?;
        return file.read_exact(buf);
    }
    #[cfg(any(unix, windows))]
    {
        read_exact_positioned(offset, buf, |buf, offset| {
            #[cfg(unix)]
            {
                std::os::unix::fs::FileExt::read_at(file, buf, offset)
            }
            #[cfg(windows)]
            {
                std::os::windows::fs::FileExt::seek_read(file, buf, offset)
            }
        })
    }
    #[cfg(not(any(unix, windows)))]
    {
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(buf)
    }
}

#[cfg(any(unix, windows))]
pub(super) fn read_exact_positioned(
    mut offset: u64,
    mut buf: &mut [u8],
    mut read: impl FnMut(&mut [u8], u64) -> std::io::Result<usize>,
) -> std::io::Result<()> {
    while !buf.is_empty() {
        match read(buf, offset) {
            Ok(0) => return Err(std::io::ErrorKind::UnexpectedEof.into()),
            Ok(n) => {
                buf = &mut buf[n..];
                if !buf.is_empty() {
                    offset = offset
                        .checked_add(n as u64)
                        .ok_or(std::io::ErrorKind::InvalidInput)?;
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

// 手写 Debug 输出，避免展开整个内存映射。
impl std::fmt::Debug for SstFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SstFile")
            .field("id", &self.id)
            .field("path", &self.path)
            .field("size_bytes", &self.size_bytes)
            .field("is_v2", &self.is_v2)
            .field("has_seq", &self.has_seq)
            .field("mmap", &self.mmap.is_some())
            .finish()
    }
}

impl SstFile {
    pub(crate) fn move_eligibility(&self) -> MoveEligibility {
        self.move_eligibility
    }

    pub fn num_entries(&self) -> usize {
        if let Some(e) = &self.entries_cache {
            return e.len();
        }
        self.iter_all().count()
    }

    /// 这个文件里**需要缓存槽位**的数据块数（自动档用来跟着库长）。
    ///
    /// v1 文件整份常驻在 `entries_cache` 里（没有块索引、也永不按 offset 读盘）⇒ 记 0：
    /// 给它留槽位等于凭空把上限抬高，而它根本不参与块缓存。
    pub fn blocks(&self) -> usize {
        if self.entries_cache.is_some() {
            0
        } else {
            self.index.len()
        }
    }

    pub(super) fn has_entries(&self) -> bool {
        self.entries_cache
            .as_ref()
            .map_or(!self.index.is_empty(), |entries| !entries.is_empty())
    }

    pub fn may_overlap(&self, low: &[u8], high: &[u8]) -> bool {
        // 空字节串也是真键，不能靠 min/max 都为空来判空文件。
        self.has_entries()
            && low <= high
            && self.min_key.as_slice() <= high
            && self.max_key.as_slice() >= low
    }

    pub(crate) fn may_overlap_half_open(&self, from: Option<&[u8]>, to: Option<&[u8]>) -> bool {
        self.has_entries()
            && !matches!((from, to), (Some(lo), Some(hi)) if lo >= hi)
            && from.is_none_or(|lo| self.max_key.as_slice() >= lo)
            && to.is_none_or(|hi| self.min_key.as_slice() < hi)
    }

    pub(super) fn load_block(&self, index_i: usize) -> std::io::Result<Arc<DataBlock>> {
        #[cfg(test)]
        BLOCK_READS.with(|c| c.set(c.get() + 1));
        let ie = &self.index[index_i];
        if let Some(cache) = &self.block_cache {
            let mut c = cache.lock().unwrap();
            if let Some(b) = c.get(self.cache_ns, self.id, ie.offset) {
                match b {
                    CachedBlock::Decoded(b) => return Ok(b),
                    CachedBlock::Packed(b) => {
                        #[cfg(test)]
                        PACKED_PROMOTIONS.with(|c| c.set(c.get() + 1));
                        let decoded = Arc::new(b.to_decoded());
                        c.put(self.cache_ns, self.id, ie.offset, decoded.clone());
                        return Ok(decoded);
                    }
                }
            }
        }
        let raw = self.read_slice(ie.offset, ie.len as usize)?;
        let block = Arc::new(decode_block(&raw, self.has_seq)?);
        if let Some(cache) = &self.block_cache {
            cache
                .lock()
                .unwrap()
                .put(self.cache_ns, self.id, ie.offset, block.clone());
        }
        Ok(block)
    }

    pub(super) fn load_scan_block(&self, index_i: usize) -> std::io::Result<CachedBlock> {
        #[cfg(test)]
        if packed_scan_reference::enabled() {
            return self.load_block(index_i).map(CachedBlock::Decoded);
        }
        #[cfg(test)]
        BLOCK_READS.with(|c| c.set(c.get() + 1));
        let ie = &self.index[index_i];
        if let Some(cache) = &self.block_cache {
            if let Some(block) = cache.lock().unwrap().get(self.cache_ns, self.id, ie.offset) {
                return Ok(block);
            }
        }
        // 冷顺扫保留解码块路径。20M 对照中冷紧凑块虽减少分配却没有吞吐收益，因此只复用点查已创建的紧凑块。
        let raw = self.read_slice(ie.offset, ie.len as usize)?;
        let block = Arc::new(decode_block(&raw, self.has_seq)?);
        if let Some(cache) = &self.block_cache {
            cache
                .lock()
                .unwrap()
                .put(self.cache_ns, self.id, ie.offset, block.clone());
        }
        Ok(CachedBlock::Decoded(block))
    }

    pub(super) fn load_point_block(&self, index_i: usize) -> std::io::Result<CachedBlock> {
        #[cfg(test)]
        if packed_point_reference::enabled() || point_read_reference::enabled() {
            return self.load_block(index_i).map(CachedBlock::Decoded);
        }
        #[cfg(test)]
        BLOCK_READS.with(|c| c.set(c.get() + 1));
        let ie = &self.index[index_i];
        if let Some(cache) = &self.block_cache {
            if let Some(block) = cache.lock().unwrap().get(self.cache_ns, self.id, ie.offset) {
                return Ok(block);
            }
        }
        let raw = self.read_slice(ie.offset, ie.len as usize)?;
        let block = CachedBlock::Packed(Arc::new(PackedBlock::decode(raw, self.has_seq)?));
        if let Some(cache) = &self.block_cache {
            cache
                .lock()
                .unwrap()
                .put_cached(self.cache_ns, self.id, ie.offset, block.clone());
        }
        Ok(block)
    }

    pub(super) fn read_slice(&self, offset: u64, len: usize) -> std::io::Result<Vec<u8>> {
        if let Some(m) = &self.mmap {
            let start = offset as usize;
            let end = start + len;
            if end > m.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "mmap 越界",
                ));
            }
            return Ok(m[start..end].to_vec());
        }
        #[cfg(test)]
        BLOCK_MISSES.with(|c| c.set(c.get() + 1));
        // 复用同一文件句柄，读取时直接传入偏移。
        let mut guard = self.handle.lock().unwrap();
        if guard.is_none() {
            *guard = Some(File::open(&self.path)?);
        }
        let f = guard.as_mut().unwrap();
        let mut raw: Vec<u8> = vec![0u8; len];
        read_sst_exact(f, offset, &mut raw)?;
        Ok(raw)
    }

    pub fn set_block_cache(&mut self, cache: Arc<Mutex<BlockCache>>) {
        self.block_cache = Some(cache);
    }

    pub fn set_cache_ns(&mut self, ns: u64) {
        self.cache_ns = ns;
    }

    /// 用真实 mmap 映射文件（跨平台 memmap2）
    pub fn enable_mmap(&mut self) -> std::io::Result<()> {
        let f = File::open(&self.path)?;
        // SAFETY: 文件在引擎生命周期内视为不可变 SST
        let mmap = unsafe { Mmap::map(&f)? };
        self.mmap = Some(mmap);
        Ok(())
    }

    pub fn has_mmap(&self) -> bool {
        self.mmap.is_some()
    }

    pub fn open(id: u64, path: impl AsRef<Path>) -> std::io::Result<Self> {
        Self::open_with(id, path, false)
    }

    pub fn open_with(id: u64, path: impl AsRef<Path>, use_mmap: bool) -> std::io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let meta = std::fs::metadata(&path)?;
        let size_bytes = meta.len();
        let mut f = File::open(&path)?;
        let mut magic = [0u8; 8];
        f.read_exact(&mut magic)?;
        let mut sst = if &magic == SST_MAGIC_V1 {
            open_v1(id, path, size_bytes, &mut f)?
        } else {
            open_v2_from_footer(id, path, size_bytes)?
        };
        if use_mmap {
            sst.enable_mmap()?;
        }
        Ok(sst)
    }
}
