//! 本文件负责四引擎到统一门面的适配。

use super::*;

/// 为一个引擎生成 `KvEngine` / `KvTxn` / `KvBulk` 三个适配器。
///
/// 各引擎方法名一致，差异通过这几个开关吸收：
/// - `vacuum: io` → 直接透传 `io::Result`；`vacuum: plain` → 包成 `Ok`
///   （`vacuum_bounded` 用同一个开关：两处的形状一致）
/// - `durable: yes` → 转发 `flush` / `checkpoint`；`no` → no-op
/// - `wal: yes` → 转发 WAL 阈值读写；`no` → 返回 [`EngineError::Unsupported`]
///   （memory 没有 WAL，"设了阈值"这件事对它没有意义，不能假装成功）
/// - `path: $method` → 取库路径的方法名；`none` → 纯内存
/// - `merge: yes` → 转发 `merge`；`no` → 用 trait 默认（`Unsupported`）
/// - `last_page: yes` → 转发 `search_last_page`（反向逐步退，O(页宽)）；
///   `no` → 用 trait 默认（= `reverse_scan` 截尾，O(库) 但正确）。
///   ⚠️ M82 起**四家都是 `yes`**：`no` 这一支只留给将来新加的引擎（那时它就是那个引擎的
///   诚实读数，不是量具坏了）
macro_rules! impl_engine {
    (
        engine_struct: $engine:ident,
        txn_struct: $txn:ident,
        bulk_struct: $bulk:ident,
        mvcc: $mvcc:ty,
        txn_inner: $txn_inner:ty,
        bulk_inner: $bulk_inner:ty,
        name: $name:literal,
        path: $path:tt,
        vacuum: $vacuum:tt,
        durable: $durable:tt,
        wal: $wal:tt,
        merge: $merge:tt,
        last_page: $last_page:tt,
        cache: $cache:tt,
    ) => {
        pub(super) struct $engine {
            pub(super) inner: $mvcc,
        }

        struct $txn<'a> {
            inner: $txn_inner,
            _engine: std::marker::PhantomData<&'a ()>,
        }

        struct $bulk<'a> {
            inner: $bulk_inner,
            _engine: std::marker::PhantomData<&'a ()>,
        }

        impl KvEngine for $engine {
            fn name(&self) -> &'static str {
                $name
            }

            impl_engine!(@path self, $path);

            fn begin(&self) -> Box<dyn KvTxn + '_> {
                Box::new($txn {
                    inner: self.inner.begin_transaction(),
                    _engine: std::marker::PhantomData,
                })
            }

            fn begin_bulk(&self) -> Box<dyn KvBulk + '_> {
                Box::new($bulk {
                    inner: self.inner.begin_bulk(),
                    _engine: std::marker::PhantomData,
                })
            }

            fn export_latest_visible(&self, include_deleted: bool) -> Vec<ExportRecord> {
                self.inner.export_latest_visible(include_deleted)
            }

            // M44：覆盖 trait 默认实现（默认那条要先把整库物化）。这一家底下就是
            // `fold_rows_streaming` ⇒ 折一行交一行，全库不再先在内存里摆一遍。
            fn export_latest_visible_for_each(
                &self,
                include_deleted: bool,
                on: &mut dyn FnMut(ExportRecord) -> bool,
            ) -> usize {
                self.inner.export_latest_visible_for_each(include_deleted, on)
            }

            fn health(&self) -> MvccHealth {
                self.inner.health()
            }

            fn reclamation_status(&self) -> Option<crate::common::ReclamationStatus> {
                Some(self.inner.reclamation_status())
            }

            fn set_vacuum_auto_slice(&self, rows: u64) {
                self.inner.set_vacuum_auto_slice(rows)
            }

            fn vacuum_auto_slice(&self) -> u64 {
                self.inner.vacuum_auto_slice()
            }

            fn durability_profile(&self) -> DurabilityProfile {
                self.inner.durability_profile()
            }

            impl_engine!(@vacuum $vacuum);
            impl_engine!(@vacuum_bounded $vacuum);
            impl_engine!(@durable $durable);
            impl_engine!(@wal $wal);
            impl_engine!(@merge $merge);
            impl_engine!(@cache_max $cache);

            fn tuning(&self) -> EngineTuning {
                self.inner.tuning()
            }
        }

        impl<'a> KvTxn for $txn<'a> {
            fn version(&self) -> u64 {
                self.inner.version()
            }

            fn set(&self, key: &[u8], value: Vec<u8>) -> bool {
                self.inner.set(key, value)
            }

            fn set_raw(&self, key: &[u8], value: Vec<u8>) -> bool {
                self.inner.set_raw(key, value)
            }

            fn set_with_ttl(&self, key: &[u8], value: Vec<u8>, ttl: Duration) -> bool {
                self.inner.set_with_ttl(key, value, ttl)
            }

            fn delete(&self, key: &[u8]) -> bool {
                self.inner.delete(key)
            }

            fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
                self.inner.get(key)
            }

            fn exists(&self, key: &[u8]) -> bool {
                self.inner.exists(key)
            }

            fn get_meta(&self, key: &[u8]) -> Option<KeyMeta> {
                self.inner.get_meta(key)
            }

            fn batch_set(
                &self,
                items: &[(Vec<u8>, Vec<u8>)],
            ) -> std::result::Result<(), Vec<u8>> {
                self.inner.batch_set(items)
            }

            fn batch_delete(&self, keys: &[Vec<u8>]) -> std::result::Result<(), Vec<u8>> {
                self.inner.batch_delete(keys)
            }

            fn multi_get(&self, keys: &[Vec<u8>]) -> Vec<Option<Vec<u8>>> {
                self.inner.multi_get(keys)
            }

            fn incr(&self, key: &[u8], delta: i64) -> std::result::Result<i64, IncrError> {
                self.inner.incr(key, delta)
            }

            fn decr(&self, key: &[u8], delta: i64) -> std::result::Result<i64, IncrError> {
                self.inner.decr(key, delta)
            }

            fn incr_with_ttl(
                &self,
                key: &[u8],
                delta: i64,
                ttl: Duration,
            ) -> std::result::Result<i64, IncrError> {
                self.inner.incr_with_ttl(key, delta, ttl)
            }

            fn scan(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Vec<ExportRecord> {
                self.inner.scan(start, end)
            }

            fn reverse_scan(
                &self,
                start: Option<&[u8]>,
                end: Option<&[u8]>,
            ) -> Vec<ExportRecord> {
                self.inner.reverse_scan(start, end)
            }

            fn prefix_scan(&self, prefix: &[u8]) -> Vec<ExportRecord> {
                self.inner.prefix_scan(prefix)
            }

            fn seek(&self, key: &[u8]) -> Option<ExportRecord> {
                self.inner.seek(key)
            }

            fn seek_prev(&self, key: &[u8]) -> Option<ExportRecord> {
                self.inner.seek_prev(key)
            }

            fn scan_page(
                &self,
                after: Option<&[u8]>,
                limit: usize,
            ) -> (Vec<ExportRecord>, Option<Vec<u8>>) {
                self.inner.scan_page(after, limit)
            }

            fn search_keys(&self, query: &SearchQuery) -> SearchPage {
                self.inner.search_keys(query)
            }

            // M82：四家都覆盖了 `search_last_page`（反向逐步退）；`no` 只留给将来的新引擎
            impl_engine!(@last_page $last_page);

            fn key_count(&self, prefix: &[u8]) -> usize {
                self.inner.key_count(prefix)
            }

            fn get_ttl(&self, key: &[u8]) -> Option<Option<Duration>> {
                self.inner.get_ttl(key)
            }

            fn refresh_ttl(&self, key: &[u8], ttl: Duration) -> bool {
                self.inner.refresh_ttl(key, ttl)
            }

            fn persist_key(&self, key: &[u8]) -> bool {
                self.inner.persist(key)
            }

            fn purge_expired(&self) -> usize {
                self.inner.purge_expired()
            }

            fn export_latest_visible(&self, include_deleted: bool) -> Vec<ExportRecord> {
                self.inner.export_latest_visible(include_deleted)
            }

            // M44：覆盖 trait 默认实现（默认那条要先把整库物化）
            fn export_latest_visible_for_each(
                &self,
                include_deleted: bool,
                on: &mut dyn FnMut(ExportRecord) -> bool,
            ) -> usize {
                self.inner.export_latest_visible_for_each(include_deleted, on)
            }

            fn commit(&self) {
                self.inner.commit()
            }

            fn try_commit(&self) -> std::result::Result<(), crate::CommitError> {
                self.inner.try_commit()
            }
            fn try_set(&self, key: &[u8], value: Vec<u8>) -> std::result::Result<(), crate::WriteError> {
                self.inner.try_set(key, value)
            }
            fn try_set_raw(&self, key: &[u8], value: Vec<u8>) -> std::result::Result<(), crate::WriteError> {
                self.inner.try_set_raw(key, value)
            }
            fn try_delete(&self, key: &[u8]) -> std::result::Result<(), crate::WriteError> {
                self.inner.try_delete(key)
            }

            fn rollback(&self) {
                self.inner.rollback()
            }
        }

        impl<'a> KvBulk for $bulk<'a> {
            fn put(&mut self, key: &[u8], value: Vec<u8>) {
                self.inner.put(key, value)
            }

            fn put_batch(&mut self, items: &[(Vec<u8>, Vec<u8>)]) {
                self.inner.put_batch(items)
            }

            fn put_batch_owned(&mut self, items: Vec<(Vec<u8>, Vec<u8>)>) {
                self.inner.put_batch_owned(items)
            }

            fn delete(&mut self, key: &[u8]) {
                self.inner.delete(key)
            }

            fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
                self.inner.get(key)
            }

            fn finish(self: Box<Self>) {
                (*self).inner.finish()
            }
        }
    };

    // ---- path 分支 ----
    (@path $self:ident, none) => {
        fn path(&self) -> Option<&Path> {
            None
        }
    };
    // M81 第 6 项：memory 的"路径"就是它的启停快照。`snapshot_path()` 自己返回 `Option`
    // （没给目录开的库 = `None`，与改前一致），所以不能走下面那条 `Some(...)` 的通用臂。
    (@path $self:ident, snapshot) => {
        fn path(&self) -> Option<&Path> {
            self.inner.snapshot_path()
        }
    };
    (@path $self:ident, $method:ident) => {
        fn path(&self) -> Option<&Path> {
            Some(self.inner.$method())
        }
    };

    // ---- vacuum 分支 ----
    (@vacuum io) => {
        fn vacuum(&self) -> Result<VacuumStats> {
            Ok(self.inner.vacuum()?)
        }
    };
    (@vacuum plain) => {
        fn vacuum(&self) -> Result<VacuumStats> {
            Ok(self.inner.vacuum())
        }
    };

    // ---- 有界回收分支：形状与 vacuum 一致，所以复用同一个开关 ----
    (@vacuum_bounded io) => {
        fn vacuum_bounded(
            &self,
            after: Option<&[u8]>,
            budget_rows: u64,
        ) -> Result<VacuumRound> {
            Ok(self.inner.vacuum_bounded(after, budget_rows)?)
        }
    };
    (@vacuum_bounded plain) => {
        fn vacuum_bounded(
            &self,
            after: Option<&[u8]>,
            budget_rows: u64,
        ) -> Result<VacuumRound> {
            Ok(self.inner.vacuum_bounded(after, budget_rows))
        }
    };

    // ---- flush / checkpoint 分支 ----
    (@durable yes) => {
        fn flush(&self) -> Result<()> {
            self.inner.try_flush()?;
            Ok(())
        }

        fn checkpoint(&self) -> Result<()> {
            self.inner.try_checkpoint()?;
            Ok(())
        }
    };
    (@durable no) => {
        /// 纯内存引擎：`flush` 仍然是 no-op。M81 第 6 项的启停快照**不走这扇门** ——
        /// 它在 open 读、在 `Drop` 写，所以台架里 memory 的 `flush` 格一个数都不变，
        /// 而"没有手动存盘口子"这条代价仍然成立（想中途落盘只能释放句柄或走 export）。
        fn flush(&self) -> Result<()> {
            Ok(())
        }

        /// 纯内存引擎：无 WAL 可截断
        fn checkpoint(&self) -> Result<()> {
            Ok(())
        }
    };

    // ---- WAL 阈值分支 ----
    (@wal yes) => {
        fn set_wal_checkpoint_limit(&self, bytes: u64) -> Result<()> {
            self.inner.set_wal_checkpoint_limit(bytes);
            Ok(())
        }

        fn wal_checkpoint_limit(&self) -> Result<u64> {
            Ok(self.inner.wal_checkpoint_limit())
        }
    };
    // 没有 WAL 的引擎：这条能力对它不存在 ⇒ 明确说 unsupported，不返回"设成功了"
    (@wal no) => {
        fn set_wal_checkpoint_limit(&self, _bytes: u64) -> Result<()> {
            Err(EngineError::Unsupported {
                engine: self.name(),
                op: "wal checkpoint limit",
            })
        }

        fn wal_checkpoint_limit(&self) -> Result<u64> {
            Err(EngineError::Unsupported {
                engine: self.name(),
                op: "wal checkpoint limit",
            })
        }
    };

    // ---- 缓存自动档上限分支（M32）+ 内存预算档（M76）----
    (@cache_max yes) => {
        fn set_cache_auto_max(&self, n: usize) -> Result<()> {
            self.inner.set_cache_auto_max(n);
            Ok(())
        }

        fn cache_auto_max(&self) -> Result<usize> {
            Ok(self.inner.cache_auto_max())
        }

        fn set_cache_budget_bytes(&self, bytes: u64) -> Result<usize> {
            Ok(self.inner.set_cache_budget_bytes(bytes))
        }

        fn cache_budget_bytes(&self) -> Result<u64> {
            Ok(self.inner.cache_budget_bytes())
        }
    };
    // 没有"缓存槽数"这个概念的引擎：明说 unsupported，别回"设置成功"
    // （预算档走 trait 的默认实现，也是 Unsupported）
    (@cache_max no) => {
        fn set_cache_auto_max(&self, _n: usize) -> Result<()> {
            Err(EngineError::Unsupported {
                engine: self.name(),
                op: "cache auto max",
            })
        }

        fn cache_auto_max(&self) -> Result<usize> {
            Err(EngineError::Unsupported {
                engine: self.name(),
                op: "cache auto max",
            })
        }
    };

    // ---- merge 分支 ----
    (@merge yes) => {
        fn merge(&self) -> Result<()> {
            self.inner.merge()?;
            Ok(())
        }
    };
    // 不支持 merge：留给 trait 默认实现（返回 Unsupported）
    (@merge no) => {};

    // ---- 末页(免 total) 分支（M81 第 4 项）----
    (@last_page yes) => {
        fn search_last_page(&self, page_size: usize) -> SearchPage {
            self.inner.search_last_page(page_size)
        }
    };
    // 没有"反向逐步退"的原语：留给 trait 默认实现（= `reverse_scan` 截尾，O(库) 但正确）
    (@last_page no) => {};
}

impl_engine! {
    engine_struct: BitcaskEngine,
    txn_struct: BitcaskTxn,
    bulk_struct: BitcaskBulk,
    mvcc: crate::bitcask::bitcask_mvcc::MVCC,
    txn_inner: crate::bitcask::bitcask_mvcc::Transaction,
    bulk_inner: crate::bitcask::bitcask_mvcc::BulkLoader,
    name: "bitcask",
    path: db_dir,
    vacuum: io,
    durable: yes,
    wal: yes,
    merge: yes,
    last_page: yes,
    cache: no,
}

impl_engine! {
    engine_struct: BPlusTreeEngine,
    txn_struct: BPlusTreeTxn,
    bulk_struct: BPlusTreeBulk,
    mvcc: crate::bplus_tree::bplus_tree_mvcc::MVCC,
    txn_inner: crate::bplus_tree::bplus_tree_mvcc::Transaction,
    bulk_inner: crate::bplus_tree::bplus_tree_mvcc::BulkLoader,
    name: "bplus_tree",
    path: db_path,
    vacuum: io,
    durable: yes,
    wal: yes,
    merge: no,
    last_page: yes,
    cache: yes,
}

impl_engine! {
    engine_struct: LsmTreeEngine,
    txn_struct: LsmTreeTxn,
    bulk_struct: LsmTreeBulk,
    mvcc: crate::lsm_tree::lsm_tree_mvcc::MVCC,
    txn_inner: crate::lsm_tree::lsm_tree_mvcc::Transaction,
    bulk_inner: crate::lsm_tree::lsm_tree_mvcc::BulkLoader,
    name: "lsm_tree",
    path: db_dir,
    vacuum: io,
    durable: yes,
    wal: yes,
    merge: no,
    last_page: yes,
    cache: yes,
}

impl_engine! {
    engine_struct: MemoryEngine,
    txn_struct: MemoryTxn,
    bulk_struct: MemoryBulk,
    mvcc: crate::memory::memory_mvcc::MVCC,
    txn_inner: crate::memory::memory_mvcc::Transaction,
    bulk_inner: crate::memory::memory_mvcc::BulkLoader,
    name: "memory",
    path: snapshot,
    vacuum: plain,
    durable: no,
    wal: no,
    merge: no,
    last_page: yes,
    cache: no,
}
