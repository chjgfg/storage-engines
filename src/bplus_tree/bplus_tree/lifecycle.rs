//! 本文件负责打开、配置、关闭及资源所有权。

use super::*;

impl BPlusTree {
    /// 打开或创建磁盘上的 B+ 树（返回 Result，便于上层处理锁冲突等）
    ///
    /// `max_cache_pages == 0` = **自动档**（M22）：容量按库当前的实际页数给，夹在
    /// `AUTO_MIN_PAGES` 与 `AUTO_MAX_PAGES` 之间，并在每次 `flush_all`（= 各家 checkpoint /
    /// bulk 收尾都会走到）之后重新评估 —— 库是会长大的，只在 open 时定一次容量就等于
    /// "导入完就再也没调过旋钮"。传非 0 就是显式容量，自动档完全不参与，行为与以前一致。
    pub fn try_open(
        path: impl AsRef<Path>,
        order: usize,
        max_cache_pages: usize,
    ) -> std::io::Result<Self> {
        assert!(order >= 3, "B+树阶数至少3阶");
        let auto = max_cache_pages == 0;
        let ask = if auto {
            AUTO_MIN_PAGES
        } else {
            max_cache_pages
        };
        let mut page_manager = PageManager::open(path, ask)?;

        let root = if let Some(root) = page_manager.root_page_id() {
            root
        } else {
            let root_page_id = page_manager.allocate_page();
            let root_node = BTreeNode::Leaf {
                entries: Vec::new(),
                next_leaf: None,
            };
            let node_data = bincode::serialize(&root_node)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
            page_manager.write_page(root_page_id, &node_data);
            page_manager.set_root_page_id(root_page_id);
            page_manager.flush_all();
            root_page_id
        };

        // 自动档：容量按这份库现在的页数来（meta 已加载 ⇒ next_page_id 是真的）
        if auto {
            let cap = auto_cache_pages(page_manager.next_page_id(), AUTO_MAX_PAGES);
            page_manager.set_cache_capacity(cap);
        }

        Ok(Self {
            page_manager,
            node_buffer: Vec::new(),
            root,
            order,
            node_cache: std::collections::HashMap::new(),
            auto_cache: auto,
            auto_max_pages: AUTO_MAX_PAGES,
            cache_budget_bytes: 0,
            explicit_pages: ask,
        })
    }

    /// 打开或创建磁盘上的 B+ 树
    ///
    /// - 文件不存在 / 空文件：创建新树（空叶子根）
    /// - 已有合法文件：从 meta 恢复 root，数据跨进程保留
    pub fn open(path: impl AsRef<Path>, order: usize, max_cache_pages: usize) -> Self {
        Self::try_open(path, order, max_cache_pages)
            .map_err(|e| {
                format!(
                    "打开数据文件失败: {e}\n\
    提示: 若错误为 WouldBlock/锁被占用, 请确保同进程内已 drop 全部 MVCC 与 Transaction,\
    且没有其它进程占用 data.db.lock。"
                )
            })
            .unwrap()
    }

    /// 兼容旧接口：在系统临时目录创建匿名数据文件
    pub fn new(order: usize, max_cache_pages: usize) -> Self {
        // ⚠️ 路径必须带**进程内单调序号**，不能只靠时间戳：Windows 的 SystemTime 粒度较粗，
        // 并发测试里两个线程可以拿到同一个 nanos ⇒ 第二个会在 `.lock` 上吃到 WouldBlock，
        // 表现为随机失败的"打开数据文件失败"。同一个坑在 engine.rs / 各家 tmp_db 上都出现过。
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let path = std::env::temp_dir().join(format!(
            "bplus_anon_{}_{}_{}.db",
            std::process::id(),
            nanos,
            n
        ));
        Self::open(path, order, max_cache_pages)
    }

    /// 根页变化时同步到 meta（分裂升高 / 删除降层）
    pub(super) fn set_root(&mut self, new_root: PageId) {
        self.root = new_root;
        self.page_manager.set_root_page_id(new_root);
    }

    /// 刷盘所有脏页 + meta，并 fsync
    pub fn flush_all(&mut self) {
        if let Err(e) = self.try_flush_all() {
            log::error!("flush_all failed: {e}");
        }
    }

    pub(crate) fn ensure_healthy(&self) -> std::io::Result<()> {
        self.page_manager.ensure_healthy()
    }

    pub(crate) fn with_reclamation<T>(
        &mut self,
        reclaim: impl FnOnce(&mut Self) -> std::io::Result<T>,
    ) -> std::io::Result<T> {
        match self.page_manager.begin_reclamation()? {
            crate::bplus_tree::page::ReclamationPublication::ExistingBulk => return reclaim(self),
            crate::bplus_tree::page::ReclamationPublication::DeferredFreelist => {}
        }
        match reclaim(self) {
            Ok(result) => {
                self.page_manager.finish_reclamation()?;
                Ok(result)
            }
            Err(error) => {
                self.page_manager.mark_maintenance_failed(&error);
                Err(error)
            }
        }
    }

    pub fn try_flush_all(&mut self) -> std::io::Result<()> {
        self.page_manager.set_root_page_id(self.root);
        self.page_manager.try_flush_all()?;
        // 自动档要在每次刷盘后重新评估：库是会长大的，只在 open 时定一次容量，
        // 等于"导入完就再也没调过旋钮"—— 而那正是这次要修的那个洞。
        self.reevaluate_cache_capacity();
        Ok(())
    }

    /// 当前根页（WAL checkpoint 使用）
    pub fn root_page_id(&self) -> PageId {
        self.root
    }

    /// 下一个将分配的页号（WAL checkpoint 使用）
    pub fn next_page_id(&self) -> u64 {
        self.page_manager.next_page_id()
    }

    /// meta 中的 next_version（旧库无 NVER 时为 None）
    pub fn next_version(&self) -> Option<u64> {
        self.page_manager.next_version()
    }

    /// 持久化 next_version 到 meta（open/checkpoint 时调用）
    pub fn set_next_version(&mut self, v: u64) {
        self.page_manager.set_next_version(v);
    }

    /// 打开/关闭 bulk 模式（委托 PageManager）
    pub fn set_bulk_mode(&mut self, on: bool) {
        self.page_manager.set_bulk_mode(on);
    }

    pub fn bulk_mode(&self) -> bool {
        self.page_manager.bulk_mode()
    }
}

impl BPlusTree {
    /// 当前生效的阶数（门面读回用：证明"设进去的值真到了引擎里"）
    pub fn order(&self) -> usize {
        self.order
    }

    /// 页缓存容量（同上）
    pub fn max_cache_pages(&self) -> usize {
        self.page_manager.cache_capacity()
    }
}
