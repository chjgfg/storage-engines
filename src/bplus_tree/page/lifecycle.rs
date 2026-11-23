//! 本文件负责打开、配置、关闭及资源所有权。

use super::*;

impl PageManager {
    /// 打开或创建数据文件（同时获取单写者锁、打开 dblwr，并做 torn-page 恢复）
    pub fn open(path: impl AsRef<Path>, max_cache_pages: usize) -> std::io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        // 先抢锁，避免两个进程同时恢复/写入
        let lock = FileLock::try_acquire_at(&lock_path(&path))?;
        // 主数据文件是定宽页布局 ⇒ 声明块大小，read_page/write_page 才算得对偏移
        let mut file = DiskFile::open_paged(&path, PAGE_SIZE)?;
        let mut dblwr = DoubleWriteBuffer::open(dblwr_path(&path))?;

        // 1) 双写恢复：把 dblwr 中 CRC 合法的页覆盖回主文件
        Self::recover_from_dblwr(&mut file, &mut dblwr)?;

        let file_len = file.len()?;
        let mut pm = Self {
            _lock: lock,
            file,
            dblwr,
            path,
            cache: HashMap::new(),
            lru_queue: VecDeque::new(),
            free_page_ids: VecDeque::new(),
            free_set: HashSet::new(),
            next_page_id: 1,
            root_page_id: None,
            next_version: None,
            max_cache_pages,
            clock: 0,
            meta_dirty: false,
            freelist_dirty: false,
            freelist_writer: None,
            bulk_mode: false,
            bulk_pending_sync: false,
            io_state: PageIoState::Healthy,
            reclamation: ReclamationState::Idle,
            page_writes: HashMap::new(),
            pinned: HashSet::new(),
            pin_budget: DEFAULT_PIN_BUDGET_PAGES,
        };

        if file_len == 0 {
            pm.meta_dirty = true;
            pm.persist_meta_via_dblwr()?;
            pm.persist_freelist()?;
        } else {
            pm.load_meta()?;
            pm.load_freelist()?;
        }
        Ok(pm)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn root_page_id(&self) -> Option<PageId> {
        self.root_page_id
    }

    pub fn set_root_page_id(&mut self, root: PageId) {
        if self.root_page_id != Some(root) {
            self.root_page_id = Some(root);
            self.meta_dirty = true;
        }
    }

    pub fn next_page_id(&self) -> u64 {
        self.next_page_id
    }

    /// meta 中持久化的 `next_version`（旧库无 NVER 魔数时为 `None`）
    pub fn next_version(&self) -> Option<u64> {
        self.next_version
    }

    /// 更新 meta 中的 next_version（checkpoint / open 结束时写入）
    pub fn set_next_version(&mut self, v: u64) {
        if self.next_version != Some(v) {
            self.next_version = Some(v);
            self.meta_dirty = true;
        }
    }

    /// bulk 导入模式：跳过双写，换出时只 write 不 fsync，flush_all 末尾统一 sync
    pub fn set_bulk_mode(&mut self, on: bool) {
        self.bulk_mode = on;
    }

    pub fn bulk_mode(&self) -> bool {
        self.bulk_mode
    }
}

impl Drop for PageManager {
    fn drop(&mut self) {
        self.flush_all();
    }
}
