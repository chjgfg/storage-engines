//! MVCC 事务层 + WAL 崩溃恢复
//!
//! 提交协议（简化版 ARIES）：
//! 1. 写路径：先 append WAL Write，再改内存 B+ 树
//! 2. commit：append WAL Commit → **fsync WAL**（持久化点）→ 可选刷页
//! 3. 崩溃恢复：重放已提交 Write，撤销未提交 Write，再 checkpoint 截断 WAL
//!
//! **Bulk load 模式**（[`MVCC::begin_bulk`]）：
//! - 关闭 WAL / 冲突检测 / 活跃事务表
//! - 页换出跳过双写与频繁 fsync
//! - 结束时 [`BulkLoader::finish`] → flush_all + checkpoint

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use crate::bplus_tree::{BPlusTree, bplus_tree::RowId};
use crate::bplus_tree::storage::{
    decode_stored_value, encode_stored_value, wal_path, BlobStore,
};
use crate::bplus_tree::wal::{analyze_recovery, Wal, WalRecord};
pub use crate::common::{DurabilityProfile, ExportRecord, MvccHealth, VacuumRound, VacuumStats};
use crate::common::{HealthFold, VacuumScheduler};
use crate::common::vacuum_plan;
use crate::common::version_key_upper_bound;

/// MVCC 存储引擎
pub struct MVCC {
    kv: Arc<Mutex<BPlusTree>>,
    /// 大 value 外置存储（与 kv 同库目录）
    blob: Arc<Mutex<BlobStore>>,
    wal: Arc<Mutex<Wal>>,
    active_txn: Arc<Mutex<HashMap<u64, Vec<Vec<u8>>>>>,
    next_version: Arc<AtomicU64>,
    /// WAL 增长到多少字节就在下一次提交后自动 checkpoint（0 = 关闭自动触发）
    wal_limit: Arc<AtomicU64>,
    /// 自动回收：切片宽度 / 记账 / 游标（策略与实现在 [`crate::common::VacuumScheduler`]）
    vac: Arc<VacuumScheduler>,
    db_path: PathBuf,
}

/// 普通 MVCC 事务
pub struct Transaction {
    pub(crate) kv: Arc<Mutex<BPlusTree>>,
    pub(crate) blob: Arc<Mutex<BlobStore>>,
    pub(crate) wal: Arc<Mutex<Wal>>,
    pub(crate) active_txn: Arc<Mutex<HashMap<u64, Vec<Vec<u8>>>>>,
    next_version: Arc<AtomicU64>,
    wal_limit: Arc<AtomicU64>,
    /// 提交时把本事务写的版本行数记到自动回收的账上
    vac: Arc<VacuumScheduler>,
    /// 本事务是否已经进过 WAL（第一次写时才补 `Begin`）—— 见 [`crate::common::WalPolicy`]
    wal_policy: crate::common::WalPolicy,
    version: u64,
    active_xid: HashSet<u64>,
}

/// 批量导入会话：无 WAL、无冲突检测，单写者假定
pub struct BulkLoader {
    kv: Arc<Mutex<BPlusTree>>,
    blob: Arc<Mutex<BlobStore>>,
    wal: Arc<Mutex<Wal>>,
    next_version: Arc<AtomicU64>,
    version: u64,
    finished: bool,
}

/// 逻辑键 + 版本（调试用）与版本键编解码
///
/// 定义在 [`crate::common::version`]（四引擎共用）；这里 re-export 是为了
/// `crate::bplus_tree::encode_key` 这个既有路径不中断。
pub use crate::common::{decode_key, encode_key, VersionedKey};

/// 把 WAL 收口成「只剩一条 Checkpoint」。
///
/// 顺序反直觉但必须这样：原先是 `append(Checkpoint) → sync → truncate()`，而 `truncate()`
/// 只留 header —— **刚写的 Checkpoint 被自己抹掉了**。bplus 的起点另有 `NVER` meta 兜着，
/// 但 README 写的「其次用 WAL 的 checkpoint」那一级因此永远走不到；改成先截断再写回。
///
/// 崩在截断之后、写入之前不影响正确性：那时 WAL 为空，open 退回 meta/全树扫描拿起点，只是慢。
fn seal_wal_checkpoint(
    wal: &mut Wal,
    next_version: u64,
    root_page_id: u64,
    next_page_id: u64,
) -> std::io::Result<()> {
    wal.truncate()?;
    wal.append(WalRecord::Checkpoint {
        next_version,
        root_page_id,
        next_page_id,
    })?;
    wal.sync()
}

/// checkpoint 的实体：手动 `MVCC::checkpoint()` 与提交时的 WAL 阈值触发共用一份实现。
///
/// 顺序不能换：先把 `next_version` 记进 meta（`NVER`）再 flush 页，最后才收口 WAL ——
/// WAL 一旦被截断，未落盘的页就再无重放来源。
fn run_checkpoint(kv: &Mutex<BPlusTree>, wal: &Mutex<Wal>, next_version: &AtomicU64) {
    let mut kv = kv.lock().unwrap();
    let mut wal = wal.lock().unwrap();
    let next_ver = next_version.load(Ordering::SeqCst);
    kv.set_next_version(next_ver);
    kv.flush_all();
    let root = kv.root_page_id();
    let next_page = kv.next_page_id();
    let _ = seal_wal_checkpoint(&mut wal, next_ver, root.0, next_page);
}

impl MVCC {
    /// 打开/创建磁盘上的 MVCC 引擎（含 WAL 恢复）。失败时 panic（兼容旧代码）。
    pub fn open(path: impl AsRef<Path>, order: usize, max_cache_pages: usize) -> Self {
        Self::try_open(path, order, max_cache_pages).map_err(|e| format!("MVCC::open 失败: {e}")).unwrap()
    }

    /// 打开/创建，错误以 `io::Result` 返回（锁冲突、坏文件、IO 等）。
    ///
    /// **冷启动路径**（千万级 / GB 库）：
    /// - 优先用 meta 中的 `next_version`（checkpoint 时写入），**不扫全树**
    /// - 其次用 WAL 里最后一条 Checkpoint / max_xid
    /// - 仅旧库（meta 无 NVER 且 WAL 无 checkpoint）才 `scan_max_version` 全叶扫描
    pub fn try_open(
        path: impl AsRef<Path>,
        order: usize,
        max_cache_pages: usize,
    ) -> std::io::Result<Self> {
        let db_path = path.as_ref().to_path_buf();
        let mut tree = BPlusTree::try_open(&db_path, order, max_cache_pages)?;
        let blob = BlobStore::open(&db_path)?;
        let mut wal = Wal::open(wal_path(&db_path))?;

        let entries = wal.read_all()?;
        let plan = analyze_recovery(&entries);

        // REDO 已提交 / UNDO 未提交（仅当 WAL 非空有内容时）
        for (_xid, key, value) in &plan.committed_writes {
            tree.insert(key.clone(), RowId(value.clone()));
        }
        for (_xid, key) in &plan.uncommitted_writes {
            let _ = tree.delete(key.clone());
        }

        let start_ver = resolve_start_version(&mut tree, &plan);

        // 把 next_version 写入 meta，下次干净 open 可跳过全树扫描
        tree.set_next_version(start_ver);
        tree.flush_all();
        let root = tree.root_page_id();
        let next_page = tree.next_page_id();
        // 先截断、再把 Checkpoint 写成新 WAL 的第一条。原先的顺序是 append→sync→truncate，
        // 而 truncate 只留 header ⇒ 刚写的 Checkpoint 被自己抹掉，下次 open 想靠它拿起点也拿不到。
        seal_wal_checkpoint(&mut wal, start_ver, root.0, next_page)?;

        Ok(Self {
            kv: Arc::new(Mutex::new(tree)),
            blob: Arc::new(Mutex::new(blob)),
            wal: Arc::new(Mutex::new(wal)),
            active_txn: Arc::new(Mutex::new(HashMap::new())),
            next_version: Arc::new(AtomicU64::new(start_ver)),
            wal_limit: Arc::new(AtomicU64::new(MVCC::WAL_CHECKPOINT_BYTES_DEFAULT)),
            vac: Arc::new(VacuumScheduler::new(MVCC::VACUUM_AUTO_SLICE_DEFAULT)),
            db_path,
        })
    }

    pub fn new(order: usize, max_cache_pages: usize) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "mvcc_anon_{}_{}.db",
            std::process::id(),
            nanos
        ));
        Self::open(path, order, max_cache_pages)
    }

    pub fn begin_transaction(&self) -> Transaction {
        self.tick_auto_vacuum();
        Transaction::begin(
            self.kv.clone(),
            self.blob.clone(),
            self.wal.clone(),
            self.active_txn.clone(),
            self.next_version.clone(),
            self.wal_limit.clone(),
            self.vac.clone(),
        )
    }

    /// 自动回收的一小步：攒够一个切片宽度的新版本，就裁同样宽度的**一片**。
    ///
    /// 挂在 `begin_transaction` 而不是 commit 上：这一处还没有本线程的活跃事务，
    /// `vacuum_bounded` 的「有活跃事务就整轮跳过」不会被自己的调用方踩中；而且每个
    /// 事务至少 begin 一次，攒账的节奏和写路径一样。
    ///
    /// 触发量 = 切片宽度 ⇒ 回收速率与产生垃圾的速率 1:1，这是「跟得上」的下界；
    /// 一片的代价 = 走 `slice` 行叶子条目，锁也就按这么久。
    /// 游标回到库尾之后，下一片自动从头开始第二遍。
    fn tick_auto_vacuum(&self) {
        let Some(slice) = self.vac.take_tick() else {
            return;
        };
        let after = self.vac.cursor();
        if let Ok(round) = self.vacuum_bounded(after.as_deref(), slice) {
            self.vac.set_cursor(round.next_cursor);
        }
    }

    /// 自动回收的切片宽度（行数）：每攒够这么多行新版本，就裁一片这么多行。
    ///
    /// 默认 [`MVCC::VACUUM_AUTO_SLICE_DEFAULT`]；**设 0 关闭自动回收**，那时版本行只增不减，
    /// 得靠手动 `vacuum()` —— 也就是回到「热点 key 堆二十万版、点读陪跑整条链」的状态。
    pub fn set_vacuum_auto_slice(&self, rows: u64) {
        self.vac.set_slice(rows);
    }

    pub fn vacuum_auto_slice(&self) -> u64 {
        self.vac.slice()
    }

    /// 默认切片宽度 = 16 Ki 行。
    ///
    /// bplus 的一片要读叶子页（不像 Bitcask 只在内存 KeyDir 上走），页数 ≈ 行数 / order；
    /// 设这么小是为了「每次只挡一小下」，不是省总开销。事故现场那条链有 23.8 万行 ——
    /// 默认开着的话它活不到那么长。
    pub const VACUUM_AUTO_SLICE_DEFAULT: u64 = 16 * 1024;

    /// 开启 bulk load：单写者、无 WAL、无冲突检测。
    ///
    /// **崩溃不安全**：finish 之前进程挂掉可能丢数据。
    /// 导入结束后必须 `finish()`。
    pub fn begin_bulk(&self) -> BulkLoader {
        {
            let mut kv = self.kv.lock().unwrap();
            kv.set_bulk_mode(true);
        }
        let version = self.next_version.fetch_add(1, Ordering::SeqCst);
        BulkLoader {
            kv: self.kv.clone(),
            blob: self.blob.clone(),
            wal: self.wal.clone(),
            next_version: self.next_version.clone(),
            version,
            finished: false,
        }
    }

    /// 默认 WAL 阈值：16 MiB。现场见过 WAL 长到几十 MB 仍无人截断，而恢复是**全量扫描**
    /// WAL ⇒ 不截断等于启动一次比一次慢。
    pub const WAL_CHECKPOINT_BYTES_DEFAULT: u64 = 16 << 20;

    /// WAL 字节阈值可配置项：增长到这么多字节就在**下一次提交后**自动 checkpoint。
    /// 设 0 关闭自动触发（只能手动调 `checkpoint()`）。
    pub fn set_wal_checkpoint_limit(&self, bytes: u64) {
        self.wal_limit.store(bytes, Ordering::SeqCst);
    }

    pub fn wal_checkpoint_limit(&self) -> u64 {
        self.wal_limit.load(Ordering::SeqCst)
    }

    pub fn checkpoint(&self) {
        run_checkpoint(&self.kv, &self.wal, &self.next_version);
    }

    pub fn flush(&self) {
        self.kv.lock().unwrap().flush_all();
    }

    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    /// 导出当前快照下「每个逻辑 key 的最新可见版本」。
    pub fn export_latest_visible(&self, include_deleted: bool) -> Vec<ExportRecord> {
        let tx = self.begin_transaction();
        let records = tx.export_latest_visible(include_deleted);
        tx.commit();
        records
    }

    /// 崩溃安全画像 —— 四家里构件最全的一个，但"最全"不等于"没问题"：
    /// 页有 magic + CRC32、原地页写有双写缓冲兜底（防半页撕裂），freelist/blob 各自成文件。
    /// `merge_artifact_recovery` 这里**不适用**（设 false 不是缺陷）：B+ 树没有全库重写式
    /// merge，回收是原地页操作，被 dblwr 覆盖。
    pub fn durability_profile(&self) -> DurabilityProfile {
        DurabilityProfile {
            engine: "bplus_tree",
            transaction_wal: true,
            fsync_on_commit: true,
            // 页头带 magic + CRC32（pack_page）
            data_checksum: true,
            blob_checksum: true,
            in_place_page_writes: true,
            torn_page_guard: true,
            merge_artifact_recovery: false,
        }
    }

    /// 全库健康度快照。**只遍历叶子键**（`scan_keys_with`），一趟、不克隆 value、不碰 blob。
    ///
    /// 代价是 O(物理行数) 的页读取（树高另计），属管理面调用：别放进热路径，
    /// 长跑里按分钟级采样就够。口径与 [`crate::bitcask::MVCC::health`] 完全一致。
    pub fn health(&self) -> MvccHealth {
        // 一次只持一把锁：先读完 `active_txn` 计数就放，再拿 `kv`。
        // 嵌套持有会和 [`MVCC::vacuum`]（`active_txn → kv → blob`）构成反向嵌套。
        let active_txns = self.active_txn.lock().unwrap().len() as u64;
        let mut kv = self.kv.lock().unwrap();
        // 分组折叠用四引擎共用的 [`HealthFold`]：按 enc 升序 ⇒ 同 key 相邻成块 ⇒ 一趟数得出链长
        let mut fold = HealthFold::default();
        // 叶子沿 next_leaf 升序 ⇒ 同一逻辑 key 的版本块相邻
        kv.scan_keys_with(|enc| {
            let Some(vk) = decode_key(enc) else {
                return;
            };
            fold.push(&vk.raw_key);
        });
        fold.finish(active_txns)
    }

    /// 版本回收：一次扫完全库。
    ///
    /// 就是 `vacuum_bounded(None, u64::MAX)`；保留规则、跳过条件、游标语义都在
    /// [`MVCC::vacuum_bounded`] 那边。
    pub fn vacuum(&self) -> std::io::Result<VacuumStats> {
        Ok(self.vacuum_bounded(None, u64::MAX)?.stats)
    }

    /// 有界版本回收：从逻辑 key `after`（**不含**）之后开始扫，最多读 `budget_rows` 行。
    ///
    /// 保留规则和 [`MVCC::vacuum`] 完全一致，只是作用范围是一段而不是全库：
    /// - **有任何活跃事务 ⇒ 整轮跳过**（`skipped_active_txn = true`），游标原样退回
    /// - 无活跃事务时 `xmin = next_version`
    /// - 每个逻辑 key：只保留「最后一个 version < xmin」+ 全部 version >= xmin
    /// - 若 xmin 之前最终是 tombstone 且无更新版本 → 整 key 删除
    ///
    /// 截断只发生在逻辑 key 边界（半条链不能收：判「留哪一版」要求整组版本都在手上），
    /// 所以一行的预算偏差 = 那一条链的长度。
    ///
    /// blob 回收只在**全库轮次**（`budget_rows == u64::MAX`）做：它要看到全部存活行才能判断
    /// 「还有没有人引用 blob」。中间轮次照样 `flush_all`，裁掉的行不能悬着。
    ///
    /// 为什么要切片：`kv` 挂在 `Mutex` 后面，[`MVCC::vacuum`] 会把这把锁按到整库叶子链
    /// 走完，期间点读点写全部排队。自动回收要能在单核机器上跑，就得一轮只按一小段。
    ///
    /// 「跳过而不是算精确水位线」的理由与锁序说明见 [`crate::bitcask::MVCC::vacuum_bounded`]
    /// （四引擎同一套语义；旧水位线会裁掉活跃事务快照仍需要的版本 ⇒ 静默读到旧值）。
    pub fn vacuum_bounded(
        &self,
        after: Option<&[u8]>,
        budget_rows: u64,
    ) -> std::io::Result<VacuumRound> {
        let _active_guard = self.active_txn.lock().unwrap();
        if !_active_guard.is_empty() {
            return Ok(VacuumRound {
                stats: VacuumStats {
                    xmin: 0,
                    versions_removed: 0,
                    blob_rewritten: Some(false),
                    skipped_active_txn: true,
                    rows_scanned: 0,
                },
                // 游标原样退回：跳过不丢进度，下一轮从同一处接着扫
                next_cursor: after.map(|k| k.to_vec()),
            });
        }
        let xmin = self.next_version.load(Ordering::SeqCst);

        let mut kv = self.kv.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();

        // ① 一趟按引用的遍历。判「留哪一版」只需要三样：版本号、这行是不是墓碑、
        //    这行是不是 blob 指针 —— 全都在叶子条目手里就能读到，不必把 value 克隆出来。
        //    旧实现在这里有两趟 `kv.iter()`（每行克隆一次 value），第二趟纯粹为查 blob 引用。
        let budget_rows = budget_rows.max(1);
        let mut by_key: BTreeMap<Vec<u8>, Vec<(u64, Vec<u8>, bool, bool)>> = BTreeMap::new();
        let mut scanned = 0u64;
        let mut cur_raw: Option<Vec<u8>> = None;
        let mut truncated = false;
        kv.scan_entries_break_from(after, |enc, row| {
            let Some(vk) = decode_key(enc) else {
                return false;
            };
            // 下界落在键的字节域，`raw_key ≤ after` 的那些行（含 `after` 自己那一整组）
            // 还在范围内 —— 显式跳过，游标才是逻辑 key 域的。
            if after.is_some_and(|a| vk.raw_key.as_slice() <= a) {
                return false;
            }
            let at_group_start = cur_raw.as_deref() != Some(vk.raw_key.as_slice());
            if at_group_start && scanned >= budget_rows {
                // 破在这里时 `cur_raw` 那一组已经收齐（刚看到它的后继）⇒ 游标就是它
                truncated = true;
                return true;
            }
            if at_group_start {
                cur_raw = Some(vk.raw_key.clone());
            }
            scanned += 1;
            let is_tomb = row.is_none();
            let refs_blob = row
                .as_ref()
                .is_some_and(|s| s.first() == Some(&crate::bplus_tree::storage::VAL_TAG_BLOB));
            by_key
                .entry(vk.raw_key.clone())
                .or_default()
                .push((vk.version, enc.to_vec(), is_tomb, refs_blob));
            false
        });
        let next_cursor = if truncated { cur_raw } else { None };

        // 「留哪一版」是四引擎共用的判定，实现在 [`crate::common::vacuum_plan`]；
        // 这里把「保留行是否引用 blob」当作附带信息 R 一起带出来 —— Blob GC 的判据
        // 因此仍然在同一次遍历里凑齐，不用为它再走一遍全库。
        let (to_delete, kept) = vacuum_plan(by_key, xmin);
        let removed = to_delete.len();
        let has_blob_ref = kept.iter().any(|(_, refs_blob)| *refs_blob);

        for enc in &to_delete {
            let _ = kv.delete(enc.clone());
        }

        // Blob GC（教学简化）：
        // - 若已无任何大 value 引用 → clear 文件
        // - 若仍有引用 → 暂不重写（append-only 空间可能残留；完整 compact 后续可做）
        //   这样避免 Windows 上打开句柄重写导致读路径异常。
        //
        // 「有没有人引用 blob」是个全库问题，所以只在这次调用本来就是全库扫描时才处理
        // （判据用 budget 而不是游标：一把 key 的小库任何一片都正好读到库尾，
        //   用游标判断会让自动回收每一片都去动 blob 文件）。
        let blob_rewritten = if budget_rows != u64::MAX {
            false
        } else if !has_blob_ref {
            let old_len = blob.path().metadata().map(|m| m.len()).unwrap_or(0);
            if old_len > 8 {
                blob.clear_in_place()?;
                true
            } else {
                false
            }
        } else {
            false
        };

        // 把当前 next_version 一并写入 meta，避免下次 open 因 meta 过旧依赖大 WAL
        let next_ver = self.next_version.load(Ordering::SeqCst);
        kv.set_next_version(next_ver);
        kv.flush_all();
        blob.sync()?;

        Ok(VacuumRound {
            stats: VacuumStats {
                xmin,
                versions_removed: removed,
                blob_rewritten: Some(blob_rewritten),
                skipped_active_txn: false,
                rows_scanned: scanned,
            },
            next_cursor,
        })
    }
}

impl Drop for MVCC {
    /// 正常退出且无活跃事务时 checkpoint：写 meta.next_version + 截断 WAL。
    /// - 仍有未 commit/rollback 的事务时**不**截断 WAL，避免丢掉 UNDO 信息
    /// - 崩溃/kill -9 不会走到这里，仍靠 WAL REDO + meta/max_xid 恢复
    fn drop(&mut self) {
        // 忽略锁中毒与 IO 错误，避免 drop 时二次 panic
        let has_active = self
            .active_txn
            .lock()
            .map(|g| !g.is_empty())
            .unwrap_or(true);
        if has_active {
            return;
        }
        let next_ver = self.next_version.load(Ordering::SeqCst);
        if let (Ok(mut kv), Ok(mut wal)) = (self.kv.lock(), self.wal.lock()) {
            kv.set_next_version(next_ver);
            kv.flush_all();
            let root = kv.root_page_id();
            let next_page = kv.next_page_id();
            let _ = wal.append(WalRecord::Checkpoint {
                next_version: next_ver,
                root_page_id: root.0,
                next_page_id: next_page,
            });
            let _ = wal.sync();
            let _ = wal.truncate();
        }
        if let Ok(mut blob) = self.blob.lock() {
            let _ = blob.sync();
        }
    }
}

/// 决定 open 后的 `next_version` 起点（尽量不扫全树）。
///
/// 优先级：
/// 1. meta 中的 next_version（干净 checkpoint 后）
/// 2. WAL 最后一条 Checkpoint 的 next_version
/// 3. max(WAL max_xid, 全树 max version)+1（旧库兜底）
fn resolve_start_version(tree: &mut BPlusTree, plan: &crate::bplus_tree::wal::RecoveryPlan) -> u64 {
    let mut base: Option<u64> = None;

    if let Some(nv) = tree.next_version() {
        // meta 已有（含 0=空库）；与 WAL 恢复后的 max_xid 取 max
        base = Some(nv);
    }

    if let Some(WalRecord::Checkpoint { next_version, .. }) = &plan.last_checkpoint {
        base = Some(base.map(|b| b.max(*next_version)).unwrap_or(*next_version));
    }

    if let Some(b) = base {
        // 恢复可能 REDO 了更高 xid，或 meta 略旧
        return b.max(plan.max_xid.saturating_add(1)).max(1);
    }

    // 旧库：meta 无 NVER、WAL 无 checkpoint → 全树扫描（仅此路径）
    let disk_max = scan_max_version(tree);
    disk_max.max(plan.max_xid).saturating_add(1).max(1)
}

fn scan_max_version(tree: &mut BPlusTree) -> u64 {
    let mut max_v = 0u64;
    for (enc, _) in tree.iter() {
        if let Some(vk) = decode_key(&enc) {
            max_v = max_v.max(vk.version);
        }
    }
    max_v
}

impl BulkLoader {
    pub fn put(&mut self, key: &[u8], value: Vec<u8>) {
        let enc = encode_key(key, self.version);
        let stored = {
            let mut blob = self.blob.lock().unwrap();
            // bulk：不每条 fsync blob，finish 时统一 sync
            encode_stored_value(&mut blob, value, false).expect("blob encode")
        };
        let mut kv = self.kv.lock().unwrap();
        kv.insert(enc, RowId(Some(stored)));
    }

    /// 批量 put：一次加锁插入多条，显著降低锁/调度开销
    pub fn put_batch(&mut self, items: &[(Vec<u8>, Vec<u8>)]) {
        let mut blob = self.blob.lock().unwrap();
        let mut kv = self.kv.lock().unwrap();
        for (key, value) in items {
            let enc = encode_key(key, self.version);
            let stored = encode_stored_value(&mut blob, value.clone(), false).expect("blob");
            kv.insert(enc, RowId(Some(stored)));
        }
    }

    /// 零额外 clone 的批量 put（消费 items）
    pub fn put_batch_owned(&mut self, items: Vec<(Vec<u8>, Vec<u8>)>) {
        let mut blob = self.blob.lock().unwrap();
        let mut kv = self.kv.lock().unwrap();
        for (key, value) in items {
            let enc = encode_key(&key, self.version);
            let stored = encode_stored_value(&mut blob, value, false).expect("blob");
            kv.insert(enc, RowId(Some(stored)));
        }
    }

    pub fn delete(&mut self, key: &[u8]) {
        let enc = encode_key(key, self.version);
        let mut kv = self.kv.lock().unwrap();
        kv.insert(enc, RowId(None));
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut kv = self.kv.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();
        let low = encode_key(key, 0);
        let high = encode_key(key, u64::MAX);
        let mut best: Option<Vec<u8>> = None;
        for (enc, row) in kv.range_scan(low, high) {
            if let Some(vk) = decode_key(&enc) {
                if vk.raw_key.as_slice() == key && vk.version <= self.version {
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
        self.finish_inner();
    }

    fn finish_inner(&mut self) {
        if self.finished {
            return;
        }
        self.finished = true;
        let mut kv = self.kv.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();
        let mut wal = self.wal.lock().unwrap();
        let _ = blob.sync();
        let next_ver = self.next_version.load(Ordering::SeqCst);
        kv.set_next_version(next_ver);
        kv.flush_all();
        kv.set_bulk_mode(false);
        let root = kv.root_page_id();
        let next_page = kv.next_page_id();
        let _ = wal.append(WalRecord::Checkpoint {
            next_version: next_ver,
            root_page_id: root.0,
            next_page_id: next_page,
        });
        let _ = wal.sync();
        let _ = wal.truncate();
    }
}

impl Drop for BulkLoader {
    fn drop(&mut self) {
        if !self.finished {
            self.finish_inner();
        }
    }
}

/// 未终结（既没 commit 也没 rollback）就被丢弃的事务在此自动回滚。
///
/// 漏网事务会永久留在 `active_txn` 里：`xmin = min(活跃事务 id)` 被一个已死的 id 压住
/// ⇒ 版本回收永久停摆，且之后每次 `begin` 的可见性快照随漏网数线性变大。
/// 判据用「活跃表里是否还有自己」——`commit`/`rollback` 都以 `remove` 收尾，成员关系即充要条件。
impl Drop for Transaction {
    fn drop(&mut self) {
        // 判据与"只查不删"的理由见 [`crate::common::is_unfinished`]：
        // 删除必须留给下面的 rollback()，它要按表里那份 key 列表把本事务写过的版本行清掉
        if !crate::common::is_unfinished(&self.active_txn, self.version) {
            return;
        }
        if self.wal.lock().is_err() {
            return;
        }
        log::warn!("事务 {} 未 commit/rollback 即被丢弃，已自动回滚", self.version);
        self.rollback();
    }
}

impl Transaction {
    pub fn begin(
        kv: Arc<Mutex<BPlusTree>>,
        blob: Arc<Mutex<BlobStore>>,
        wal: Arc<Mutex<Wal>>,
        active_txn: Arc<Mutex<HashMap<u64, Vec<Vec<u8>>>>>,
        next_version: Arc<AtomicU64>,
        wal_limit: Arc<AtomicU64>,
        vac: Arc<VacuumScheduler>,
    ) -> Self {
        let version = next_version.fetch_add(1, Ordering::SeqCst);
        let active_txn_arc = Arc::clone(&active_txn);

        let mut guard = active_txn.lock().unwrap();
        let active_xid: HashSet<u64> = guard.keys().cloned().collect();
        guard.insert(version, Vec::new());
        drop(guard);

        // begin 不进 WAL：此刻还不知道会不会写；第一次 write() 才补 `Begin`
        // （只读事务因此一个字节都不落，详见 [`crate::common::WalPolicy`]）

        Transaction {
            kv,
            blob,
            wal,
            active_txn: active_txn_arc,
            next_version,
            wal_limit,
            vac,
            wal_policy: crate::common::WalPolicy::default(),
            version,
            active_xid,
        }
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn set(&self, key: &[u8], value: Vec<u8>) -> bool {
        self.write(key, Some(value))
    }

    pub fn delete(&self, key: &[u8]) -> bool {
        self.write(key, None)
    }

    fn write(&self, key: &[u8], value: Option<Vec<u8>>) -> bool {
        let mut active_txn = self.active_txn.lock().unwrap();
        let mut wal = self.wal.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();
        let mut kvengine = self.kv.lock().unwrap();

        if let Some(latest_version) = Self::latest_version_of(&mut kvengine, key) {
            if !self.is_visible(latest_version) {
                return false;
            }
        }

        let enc_key = encode_key(key, self.version);

        // 逻辑 value → 叶子存储编码（大 value 写 blob）
        let stored_opt = match value {
            None => None,
            Some(v) => match encode_stored_value(&mut blob, v, true) {
                Ok(s) => Some(s),
                Err(e) => {
                    eprintln!("blob encode 失败: {e}");
                    return false;
                }
            },
        };

        if self.wal_policy.open_on_first_write() {
            let _ = wal.append(WalRecord::Begin { xid: self.version });
        }
        if let Err(e) = wal.append(WalRecord::Write {
            xid: self.version,
            key: enc_key.clone(),
            value: stored_opt.clone(),
        }) {
            eprintln!("WAL append 失败: {e}");
            return false;
        }

        let writes = active_txn.entry(self.version).or_default();
        if !writes.iter().any(|k| k == key) {
            writes.push(key.to_vec());
        }

        kvengine.insert(enc_key, RowId(stored_opt));
        true
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        // 转发给 latest_visible_raw：以前这里复制过一份同样的循环，正是
        // 「改了事务层却漏掉点读」的形状。与 bitcask 保持一致。
        self.latest_visible_raw(key)
            .and_then(|(_, v)| v)
            .and_then(|raw| crate::bplus_tree::kv_ops::logical_to_user(raw))
    }

    /// 最新可见逻辑行（含 TTL 信封原始字节；tombstone 为 None）
    /// 供 [`crate::kv_ops`] 做 exists / meta / scan，不剥用户 value。
    pub(crate) fn latest_visible_raw(
        &self,
        key: &[u8],
    ) -> Option<(u64, Option<Vec<u8>>)> {
        let mut kvengine = self.kv.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();
        // 从链尾往回走，第一个可见版本就是答案；value 随叶子一起到手，不再二次读页
        let (enc, row) = Self::find_version_back(&mut kvengine, key, |v| self.is_visible(v))?;
        let version = decode_key(&enc)?.version;
        let logical = match &row.0 {
            None => None,
            Some(stored) => decode_stored_value(&mut blob, stored).ok().flatten(),
        };
        Some((version, logical))
    }

    /// 全部最新可见逻辑行（key 有序）= 无界版的 [`Self::collect_latest_raw_in`]。
    pub(crate) fn collect_latest_raw(
        &self,
        include_deleted: bool,
    ) -> Vec<(Vec<u8>, u64, Option<Vec<u8>>)> {
        self.collect_latest_raw_in(include_deleted, None, None)
    }

    /// `[start, end)` 内每个逻辑 key 的最新可见版本 —— 界**下推到树的下降与叶子链表**上。
    ///
    /// 旧形状是折完全库再由调用方逐条筛界 ⇒ 10 行的窗口也要从最左叶子走到最右叶子。
    /// 现在起点直接下降到 containing 叶子（前面的叶子一篇都不读），终点靠回调喊停
    /// （有序 ⇒ 越过上界就是尾巴）。
    ///
    /// B+ 树的 value 就住在叶子条目里，没有 Bitcask KeyDir 那样的独立键索引，所以
    /// 「把窗口覆盖的那些页反序列化出来」这一步省不掉 —— 省掉的是**页数**，不是每页的解析。
    pub(crate) fn collect_latest_raw_in(
        &self,
        include_deleted: bool,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Vec<(Vec<u8>, u64, Option<Vec<u8>>)> {
        // ① 第一遍只折「键 + 版本号」。
        let hi_enc = end.map(version_key_upper_bound);
        let mut winners: BTreeMap<Vec<u8>, (u64, Vec<u8>)> = BTreeMap::new();
        {
            let mut kvengine = self.kv.lock().unwrap();
            kvengine.scan_entries_break_from(start, |enc, _row| {
                if hi_enc.as_deref().is_some_and(|h| enc >= h) {
                    return true; // 按 enc 定界 ⇒ 有序迭代后面只会更大，可以真停
                }
                let Some(vk) = decode_key(enc) else {
                    return false;
                };
                // 前缀交错的边角：界判在原始 key 上，逐条兜
                if start.is_some_and(|s| vk.raw_key.as_slice() < s) {
                    return false;
                }
                if end.is_some_and(|e| vk.raw_key.as_slice() >= e) {
                    return false;
                }
                if !self.is_visible(vk.version) {
                    return false;
                }
                // 全表按 enc 升序 ⇒ 同一 raw_key 的版本块连续，最后一次覆盖即最大可见版本
                winners.insert(vk.raw_key.clone(), (vk.version, enc.to_vec()));
                false
            });
        }

        // ② 只给胜出行取值：每个存活 key 一次树下降 + 一次解码。
        let mut kvengine = self.kv.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();
        let mut out: Vec<(Vec<u8>, u64, Option<Vec<u8>>)> = Vec::with_capacity(winners.len());
        for (key, (ver, enc)) in winners {
            // 胜出行读不到就跳过（与旧实现「该行不存在则不参与」一致）
            let Some(row) = kvengine.get(enc) else {
                continue;
            };
            let logical = match &row.0 {
                None => None,
                Some(stored) => decode_stored_value(&mut blob, stored).ok().flatten(),
            };
            if logical.is_none() && !include_deleted {
                continue;
            }
            out.push((key, ver, logical));
        }
        out
    }

    pub fn print_all(&self) -> BTreeMap<Vec<u8>, RowId> {
        let mut records = BTreeMap::new();
        for rec in self.export_latest_visible(true) {
            records.insert(rec.key, RowId(rec.value));
        }
        records
    }

    /// 导出本事务快照下每个逻辑 key 的最新可见版本（已解码 blob / 剥 TTL）。
    ///
    /// 同 key 多版本按 version 升序扫描，后者覆盖前者 → 最终为最新可见。
    /// 模糊搜索与分页见 [`crate::kv_ops::Transaction::search_keys`]。
    pub fn export_latest_visible(&self, include_deleted: bool) -> Vec<ExportRecord> {
        self.collect_latest_raw(true)
            .into_iter()
            .filter_map(|(key, _ver, raw)| match raw {
                None => {
                    if include_deleted {
                        Some(ExportRecord {
                            key,
                            value: None,
                        })
                    } else {
                        None
                    }
                }
                Some(bytes) => match crate::bplus_tree::kv_ops::logical_to_user(bytes) {
                    Some(user) => Some(ExportRecord {
                        key,
                        value: Some(user),
                    }),
                    None => {
                        if include_deleted {
                            Some(ExportRecord {
                                key,
                                value: None,
                            })
                        } else {
                            None
                        }
                    }
                },
            })
            .collect()
    }

    pub fn commit(&self) {
        self.commit_with_options(false);
    }

    pub fn commit_with_options(&self, flush_pages: bool) {
        let rows_written = {
            let mut active_txn = self.active_txn.lock().unwrap();
            active_txn.remove(&self.version).map(|k| k.len()).unwrap_or(0)
        };
        // 给自动回收记一笔账（近似：算逻辑 key 数，宁可少算也不为了精确再走一遍写集）
        self.vac.on_commit(rows_written as u64);
        if self.wal_policy.opened() {
            let mut wal = self.wal.lock().unwrap();
            let _ = wal.append(WalRecord::Commit { xid: self.version });
            if let Err(e) = wal.sync() {
                eprintln!("WAL fsync 失败: {e}");
            }
        }
        if flush_pages {
            self.kv.lock().unwrap().flush_all();
        }

        // WAL 超阈值就顺带 checkpoint。放在提交之后是因为此刻本事务已持久、
        // 也已从活跃表摘除，checkpoint 记到的 next_version 是干净的。
        let limit = self.wal_limit.load(Ordering::SeqCst);
        if limit > 0 {
            let size = self.wal.lock().unwrap().size_bytes().unwrap_or(0);
            if size >= limit {
                run_checkpoint(&self.kv, &self.wal, &self.next_version);
            }
        }
    }

    pub fn rollback(&self) {
        let mut active_txn = self.active_txn.lock().unwrap();
        let keys = active_txn.remove(&self.version).unwrap_or_default();

        if self.wal_policy.opened() {
            let mut wal = self.wal.lock().unwrap();
            let _ = wal.append(WalRecord::Abort { xid: self.version });
            let _ = wal.sync();
        }

        if !keys.is_empty() {
            let mut kvengine = self.kv.lock().unwrap();
            for k in keys {
                let enc_key = encode_key(&k, self.version);
                let _ = kvengine.delete(enc_key);
            }
        }
    }

    fn is_visible(&self, version: u64) -> bool {
        // 判据在 [`crate::common::visible_at`]（四引擎共用的一份）
        crate::common::visible_at(version, self.version, &self.active_xid)
    }

    /// 从链尾往回找本 key 第一个被 `accept` 的版本，返回它的版本键与行。
    ///
    /// 游走本身在存储层 [`BPlusTree::find_back`] 里做（只有单向 `next_leaf`，跨叶要重新
    /// 下降）。这里负责 MVCC 的两个判据：`raw_key` 必须精确等于 `key` —— 以 `key` 为前缀的
    /// 更长 key 会落进同一个字节区间（版本号首字节 0x00 小于任何 ASCII 字节）；以及可见性。
    fn find_version_back(
        kv: &mut BPlusTree,
        key: &[u8],
        accept: impl Fn(u64) -> bool,
    ) -> Option<(Vec<u8>, RowId)> {
        let low = encode_key(key, 0);
        let high = encode_key(key, u64::MAX);
        kv.find_back(&low, &high, |enc| {
            decode_key(enc)
                .map(|vk| vk.raw_key.as_slice() == key && accept(vk.version))
                .unwrap_or(false)
        })
    }

    fn latest_version_of(kv: &mut BPlusTree, key: &[u8]) -> Option<u64> {
        Self::find_version_back(kv, key, |_| true)
            .and_then(|(enc, _)| decode_key(&enc).map(|vk| vk.version))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bplus_tree::storage::{dblwr_path, wal_path};

    fn tmp_db(tag: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("mvcc_{tag}_{nanos}.db"))
    }

    fn cleanup(path: &Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(dblwr_path(path));
        let _ = std::fs::remove_file(wal_path(path));
        let _ = std::fs::remove_file(crate::bplus_tree::storage::freelist_path(path));
        let _ = std::fs::remove_file(crate::bplus_tree::storage::lock_path(path));
        let _ = std::fs::remove_file(crate::bplus_tree::storage::blob_path(path));
    }

    /// 探针：一次点读 / 一次点写访问了多少页。
    ///
    /// 版本键大端有序 ⇒ 最新版本必然在链尾 ⇒ 下降到高界再往回一步就该停。
    /// 修复前是从 `low` 下降后沿 `next_leaf` 正向走完整个区间：N 行版本横跨
    /// N/order 片叶子，每片都要 read_page + 整节点 bincode 反序列化。
    #[test]
    fn test_point_op_cost_independent_of_chain_length() {
        fn probe(chain_len: u64) -> (u64, u64) {
            let path = tmp_db(&format!("probe_{chain_len}"));
            let (get_pages, set_pages);
            {
                let mvcc = MVCC::open(&path, 4, 16);
                for i in 0..chain_len {
                    let tx = mvcc.begin_transaction();
                    assert!(tx.set(b"hot", format!("v{i}").into_bytes()), "第 {i} 次写入被拒");
                    tx.commit();
                }

                let tx = mvcc.begin_transaction();
                let before = crate::bplus_tree::page::page_reads();
                assert_eq!(
                    tx.get(b"hot"),
                    Some(format!("v{}", chain_len - 1).into_bytes())
                );
                get_pages = crate::bplus_tree::page::page_reads() - before;

                let before = crate::bplus_tree::page::page_reads();
                assert!(tx.set(b"hot", b"newer".to_vec()));
                set_pages = crate::bplus_tree::page::page_reads() - before;
                tx.rollback();
            }
            cleanup(&path);
            (get_pages, set_pages)
        }

        let (g_short, s_short) = probe(50);
        let (g_long, s_long) = probe(2000);
        eprintln!(
            "点读访问页数：链长 50 → {g_short} 页，链长 2000 → {g_long} 页\n\
             点写访问页数：链长 50 → {s_short} 页，链长 2000 → {s_long} 页"
        );
        // 判据是「只能随树高（log 规模）涨，不许随链长（线性）涨」：
        // order=4 时链长 50 → 2000 会让树高多约 3 层，所以给 8 页容差；
        // 而未修实现同样的两个点是 34 → 1015 页（涨 981 页），远大于任何对数级容差。
        assert!(
            g_long <= g_short + 8,
            "点读的页数不应随版本链长度线性增长（短链 {g_short}，长链 {g_long}）"
        );
        assert!(
            s_long <= s_short + 8,
            "点写的页数不应随版本链长度线性增长（短链 {s_short}，长链 {s_long}）"
        );
    }

    /// WAL 阈值触发必须真的把文件截住（对照「关闭自动触发」），且截断之后重开仍能读到
    /// 全部已提交数据 —— 后半段守的是「先把 next_version 记进 meta 并 flush 页、
    /// 最后才收口 WAL」这个顺序：顺序反了就是**丢已提交写入**。
    #[test]
    fn test_wal_auto_checkpoint_bounds_file_and_keeps_data() {
        const WRITES: u32 = 40;

        // 注意量测时机：本引擎有 `impl Drop for MVCC`（析构时自带一次 checkpoint），
        // 所以必须在实例还活着的时候量，否则两种配置都会被 drop 截成 header 大小，
        // 断言就变成了空转（第一版我就踩了这个坑：量到 32 对 32）。
        fn wal_bytes_after(tag: &str, limit: u64) -> u64 {
            let path = tmp_db(tag);
            let size;
            {
                let mvcc = MVCC::open(&path, 4, 16);
                mvcc.set_wal_checkpoint_limit(limit);
                for i in 0..WRITES {
                    let key = format!("k{i}");
                    let tx = mvcc.begin_transaction();
                    assert!(tx.set(key.as_bytes(), vec![b'x'; 200]));
                    tx.commit();
                }
                size = std::fs::metadata(wal_path(&path)).map(|m| m.len()).unwrap_or(0);
            }
            cleanup(&path);
            size
        }

        let manual = wal_bytes_after("wal_manual", 0);
        let auto = wal_bytes_after("wal_auto", 1);
        eprintln!("WAL 体积：关闭自动 checkpoint {manual} 字节 vs 阈值触发 {auto} 字节");
        assert!(
            auto * 4 <= manual,
            "阈值触发应把 WAL 截住（自动 {auto}，手动 {manual}）"
        );

        let path = tmp_db("wal_reopen");
        {
            let mvcc = MVCC::open(&path, 4, 16);
            mvcc.set_wal_checkpoint_limit(1);
            for i in 0..WRITES {
                let key = format!("r{i}");
                let tx = mvcc.begin_transaction();
                assert!(tx.set(key.as_bytes(), format!("v{i}").into_bytes()));
                tx.commit();
            }
        }
        {
            let mvcc = MVCC::open(&path, 4, 16);
            let tx = mvcc.begin_transaction();
            for i in 0..WRITES {
                let key = format!("r{i}");
                assert_eq!(
                    tx.get(key.as_bytes()),
                    Some(format!("v{i}").into_bytes()),
                    "第 {i} 条已提交写入在 WAL 被截断后仍应读到"
                );
            }
            tx.commit();
        }
        cleanup(&path);
    }

    /// ② 全库折叠：value 解码次数必须只与**存活 key 数**相关，不随历史行数增长。
    ///
    /// 诚实边界：B+ 树没有独立键索引，整表走一遍的**页读取**省不掉，这条不守页读取；
    /// 它守的是「不再对每一行都做 value 解码」—— 内联值省一次 Vec 分配，
    /// 大 value 省一次 blob 读盘（后者是真 IO）。
    #[test]
    fn test_collect_latest_decodes_only_live_keys() {
        const KEYS: u64 = 8;
        const VERSIONS: u64 = 120;

        let path = tmp_db("collect_decode");
        {
            let mvcc = MVCC::open(&path, 4, 16);
            for v in 0..VERSIONS {
                for k in 0..KEYS {
                    let key = format!("k{k}");
                    let tx = mvcc.begin_transaction();
                    assert!(tx.set(key.as_bytes(), format!("v{v}").into_bytes()));
                    tx.commit();
                }
            }

            let tx = mvcc.begin_transaction();
            let base = crate::bplus_tree::storage::value_decodes();
            let recs = tx.export_latest_visible(false);
            let used = crate::bplus_tree::storage::value_decodes() - base;
            eprintln!(
                "折叠 value 解码次数：{used}（存活 key {KEYS}，历史行数 {}）",
                KEYS * VERSIONS
            );
            assert_eq!(recs.len() as u64, KEYS, "存活 key 数应等于 {KEYS}");
            assert!(
                used <= KEYS + 4,
                "只应给胜出行解码：实际 {used} 次，而历史行数是 {}",
                KEYS * VERSIONS
            );
            tx.rollback();
        }
        cleanup(&path);
    }

    /// vacuum 期间不该发生任何「把某行的 value 克隆出来」—— 它只需要版本号，
    /// 加上「是不是墓碑 / 是不是 blob 指针」两个标志位，而这三样按引用就能读到。
    ///
    /// 量尺选的是 value 克隆次数而不是页读取次数：B+ 树没有独立键索引，走一遍全库的
    /// 页读取本来就省不掉，用页计数会量不到这次改动的任何东西。
    #[test]
    fn test_vacuum_does_not_clone_values() {
        const CHAIN: u64 = 300;
        const KEYS: u64 = 4;

        let path = tmp_db("vacuum_clone");
        {
            let mvcc = MVCC::open(&path, 4, 16);
            for i in 0..CHAIN {
                for k in 0..KEYS {
                    let key = format!("k{k}");
                    let tx = mvcc.begin_transaction();
                    assert!(tx.set(key.as_bytes(), format!("v{i}").into_bytes()));
                    tx.commit();
                }
            }

            let base = crate::bplus_tree::page::value_clones();
            let stats = mvcc.vacuum().unwrap();
            let used = crate::bplus_tree::page::value_clones() - base;
            eprintln!(
                "vacuum 期间 value 克隆次数：{used}（裁掉 {} 行，历史行数 {}）",
                stats.versions_removed,
                CHAIN * KEYS
            );
            assert_eq!(
                stats.versions_removed,
                ((CHAIN - 1) * KEYS) as usize,
                "每 key 留一版，其余都该裁掉"
            );
            assert_eq!(used, 0, "vacuum 只需版本号与两个标志位，不该克隆任何 value");
        }
        cleanup(&path);
    }

    #[test]
    fn test_encode_order() {
        let k1 = encode_key(b"user", 1);
        let k2 = encode_key(b"user", 2);
        let k10 = encode_key(b"user", 10);
        assert!(k1 < k2);
        assert!(k2 < k10);
        let d = decode_key(&k10).unwrap();
        assert_eq!(d.raw_key, b"user");
        assert_eq!(d.version, 10);
    }

    #[test]
    fn test_basic_set_get_delete() {
        let path = tmp_db("basic");
        {
            let mvcc = MVCC::open(&path, 4, 16);
            let tx = mvcc.begin_transaction();
            assert!(tx.set(b"a", b"1".to_vec()));
            assert_eq!(tx.get(b"a"), Some(b"1".to_vec()));
            assert!(tx.set(b"a", b"2".to_vec()));
            assert_eq!(tx.get(b"a"), Some(b"2".to_vec()));
            assert!(tx.delete(b"a"));
            assert_eq!(tx.get(b"a"), None);
            tx.commit();
        }
        cleanup(&path);
    }

    #[test]
    fn test_snapshot_isolation() {
        let path = tmp_db("si");
        {
            let mvcc = MVCC::open(&path, 4, 16);
            let t1 = mvcc.begin_transaction();
            assert!(t1.set(b"k", b"v1".to_vec()));
            t1.commit();
            let t2 = mvcc.begin_transaction();
            let t3 = mvcc.begin_transaction();
            assert!(t2.set(b"k", b"v2".to_vec()));
            assert_eq!(t3.get(b"k"), Some(b"v1".to_vec()));
            t2.commit();
            assert_eq!(t3.get(b"k"), Some(b"v1".to_vec()));
            let t4 = mvcc.begin_transaction();
            assert_eq!(t4.get(b"k"), Some(b"v2".to_vec()));
            t4.commit();
            t3.commit();
        }
        cleanup(&path);
    }

    #[test]
    fn test_write_write_conflict() {
        let path = tmp_db("ww");
        {
            let mvcc = MVCC::open(&path, 4, 16);
            let t1 = mvcc.begin_transaction();
            let t2 = mvcc.begin_transaction();
            assert!(t1.set(b"k", b"v1".to_vec()));
            assert!(!t2.set(b"k", b"v2".to_vec()));
            t1.commit();
            t2.rollback();
        }
        cleanup(&path);
    }

    #[test]
    fn test_rollback_discards_writes() {
        let path = tmp_db("rb");
        {
            let mvcc = MVCC::open(&path, 4, 16);
            let t1 = mvcc.begin_transaction();
            assert!(t1.set(b"x", b"1".to_vec()));
            t1.rollback();
            let t2 = mvcc.begin_transaction();
            assert_eq!(t2.get(b"x"), None);
            t2.commit();
        }
        cleanup(&path);
    }

    #[test]
    fn test_print_all_latest_visible() {
        let path = tmp_db("pa");
        {
            let mvcc = MVCC::open(&path, 4, 16);
            let t1 = mvcc.begin_transaction();
            assert!(t1.set(b"a", b"1".to_vec()));
            assert!(t1.set(b"b", b"2".to_vec()));
            assert!(t1.set(b"a", b"3".to_vec()));
            let all = t1.print_all();
            assert_eq!(all.get(&b"a".to_vec()).unwrap().0, Some(b"3".to_vec()));
            assert_eq!(all.get(&b"b".to_vec()).unwrap().0, Some(b"2".to_vec()));
            t1.commit();
        }
        cleanup(&path);
    }

    #[test]
    fn test_instances_isolated() {
        let p1 = tmp_db("i1");
        let p2 = tmp_db("i2");
        {
            let m1 = MVCC::open(&p1, 4, 8);
            let m2 = MVCC::open(&p2, 4, 8);
            let t1 = m1.begin_transaction();
            let t2 = m2.begin_transaction();
            assert!(t1.set(b"k", b"1".to_vec()));
            assert!(t2.set(b"k", b"2".to_vec()));
            assert_eq!(t1.get(b"k"), Some(b"1".to_vec()));
            assert_eq!(t2.get(b"k"), Some(b"2".to_vec()));
            t1.commit();
            t2.commit();
        }
        cleanup(&p1);
        cleanup(&p2);
    }

    #[test]
    fn test_self_write_visible() {
        let path = tmp_db("self");
        {
            let mvcc = MVCC::open(&path, 4, 8);
            let tx = mvcc.begin_transaction();
            assert!(tx.set(b"k", b"v".to_vec()));
            assert_eq!(tx.get(b"k"), Some(b"v".to_vec()));
            tx.commit();
        }
        cleanup(&path);
    }

    #[test]
    fn test_conflict_after_other_commits() {
        let path = tmp_db("cmt");
        {
            let mvcc = MVCC::open(&path, 4, 8);
            let t1 = mvcc.begin_transaction();
            let t2 = mvcc.begin_transaction();
            assert!(t1.set(b"k", b"1".to_vec()));
            t1.commit();
            assert!(!t2.set(b"k", b"2".to_vec()));
            t2.rollback();
            let t3 = mvcc.begin_transaction();
            assert_eq!(t3.get(b"k"), Some(b"1".to_vec()));
            t3.commit();
        }
        cleanup(&path);
    }

    #[test]
    fn test_disk_persist_across_reopen() {
        let path = tmp_db("persist");
        {
            let mvcc = MVCC::open(&path, 4, 16);
            let t1 = mvcc.begin_transaction();
            assert!(t1.set(b"hello", b"world".to_vec()));
            t1.commit();
            let t2 = mvcc.begin_transaction();
            assert!(t2.set(b"hello", b"rust".to_vec()));
            assert!(t2.set(b"foo", b"bar".to_vec()));
            t2.commit();
        }
        {
            let mvcc = MVCC::open(&path, 4, 16);
            let tx = mvcc.begin_transaction();
            assert_eq!(tx.get(b"hello"), Some(b"rust".to_vec()));
            assert_eq!(tx.get(b"foo"), Some(b"bar".to_vec()));
            tx.commit();
        }
        cleanup(&path);
    }

    #[test]
    fn test_wal_redo_after_commit_without_page_flush() {
        let path = tmp_db("redo");
        {
            use crate::bplus_tree::wal::{Wal, WalRecord};
            cleanup(&path);
            let mut tree = crate::bplus_tree::BPlusTree::open(&path, 4, 16);
            tree.flush_all();
            drop(tree);
            let mut wal = Wal::open(wal_path(&path)).unwrap();
            let xid = 1u64;
            let enc = encode_key(b"recover", xid);
            wal.append(WalRecord::Begin { xid }).unwrap();
            wal.append(WalRecord::Write {
                xid,
                key: enc,
                value: Some(b"me".to_vec()),
            })
            .unwrap();
            wal.append(WalRecord::Commit { xid }).unwrap();
            wal.sync().unwrap();
        }
        {
            let mvcc = MVCC::open(&path, 4, 16);
            let tx = mvcc.begin_transaction();
            assert_eq!(tx.get(b"recover"), Some(b"me".to_vec()));
            tx.commit();
        }
        cleanup(&path);
    }

    #[test]
    fn test_wal_undo_uncommitted() {
        let path = tmp_db("undo");
        {
            use crate::bplus_tree::wal::{Wal, WalRecord};
            {
                let mvcc = MVCC::open(&path, 4, 16);
                drop(mvcc);
            }
            let mut wal = Wal::open(wal_path(&path)).unwrap();
            let xid = 1u64;
            {
                let mut tree = crate::bplus_tree::BPlusTree::open(&path, 4, 16);
                let enc = encode_key(b"ghost", xid);
                tree.insert(enc.clone(), RowId(Some(b"should-vanish".to_vec())));
                tree.flush_all();
                wal.append(WalRecord::Begin { xid }).unwrap();
                wal.append(WalRecord::Write {
                    xid,
                    key: enc,
                    value: Some(b"should-vanish".to_vec()),
                })
                .unwrap();
                wal.sync().unwrap();
            }
        }
        {
            let mvcc = MVCC::open(&path, 4, 16);
            let tx = mvcc.begin_transaction();
            assert_eq!(tx.get(b"ghost"), None);
            tx.commit();
        }
        cleanup(&path);
    }

    #[test]
    fn test_bulk_load_and_reopen() {
        let path = tmp_db("bulk");
        {
            let mvcc = MVCC::open(&path, 64, 256);
            let mut bulk = mvcc.begin_bulk();
            for i in 0..1000u32 {
                let k = format!("k{i:06}").into_bytes();
                let v = format!("v{i}").into_bytes();
                bulk.put(&k, v);
            }
            assert_eq!(bulk.get(b"k000042").as_deref(), Some(b"v42".as_slice()));
            bulk.finish();
        }
        {
            let mvcc = MVCC::open(&path, 64, 256);
            let tx = mvcc.begin_transaction();
            assert_eq!(tx.get(b"k000042"), Some(b"v42".to_vec()));
            assert_eq!(tx.get(b"k000999"), Some(b"v999".to_vec()));
            tx.commit();
        }
        cleanup(&path);
    }

    #[test]
    fn test_large_value_blob_roundtrip() {
        let path = tmp_db("blob");
        let big = vec![0xABu8; 10_000]; // > BLOB_THRESHOLD
        {
            let mvcc = MVCC::open(&path, 32, 64);
            let tx = mvcc.begin_transaction();
            assert!(tx.set(b"big", big.clone()));
            assert!(tx.set(b"small", b"hi".to_vec()));
            assert_eq!(tx.get(b"big"), Some(big.clone()));
            assert_eq!(tx.get(b"small"), Some(b"hi".to_vec()));
            tx.commit();
        }
        {
            let mvcc = MVCC::open(&path, 32, 64);
            let tx = mvcc.begin_transaction();
            assert_eq!(tx.get(b"big"), Some(big));
            assert_eq!(tx.get(b"small"), Some(b"hi".to_vec()));
            tx.commit();
        }
        // blob 文件应存在且大于 header
        let blob = crate::bplus_tree::storage::blob_path(&path);
        assert!(blob.exists());
        assert!(std::fs::metadata(&blob).unwrap().len() > 8);
        cleanup(&path);
    }

    #[test]
    fn test_export_latest_visible() {
        let path = tmp_db("export");
        {
            let mvcc = MVCC::open(&path, 16, 32);
            let t1 = mvcc.begin_transaction();
            assert!(t1.set(b"k1", b"v1".to_vec()));
            assert!(t1.set(b"k2", b"v2".to_vec()));
            t1.commit();
            let t2 = mvcc.begin_transaction();
            assert!(t2.set(b"k1", b"v1b".to_vec())); // 覆盖
            assert!(t2.delete(b"k2"));
            assert!(t2.set(b"k3", b"v3".to_vec()));
            t2.commit();
        }
        {
            let mvcc = MVCC::open(&path, 16, 32);
            let live = mvcc.export_latest_visible(false);
            assert_eq!(live.len(), 2);
            assert_eq!(
                live.iter().find(|r| r.key == b"k1").unwrap().value,
                Some(b"v1b".to_vec())
            );
            assert_eq!(
                live.iter().find(|r| r.key == b"k3").unwrap().value,
                Some(b"v3".to_vec())
            );
            assert!(!live.iter().any(|r| r.key == b"k2"));

            let all = mvcc.export_latest_visible(true);
            assert_eq!(all.len(), 3);
            assert!(all.iter().any(|r| r.key == b"k2" && r.value.is_none()));
        }
        cleanup(&path);
    }

    #[test]
    fn test_vacuum_removes_old_versions_and_rewrites_blob() {
        let path = tmp_db("vac");
        let big = vec![9u8; 1000];
        {
            let mvcc = MVCC::open(&path, 16, 64);
            let t = mvcc.begin_transaction();
            assert!(t.set(b"a", b"1".to_vec()));
            assert!(t.set(b"b", b"old".to_vec()));
            t.commit();
            let t = mvcc.begin_transaction();
            assert!(t.set(b"a", b"2".to_vec()));
            assert!(t.delete(b"b"));
            assert!(t.set(b"big", big.clone()));
            t.commit();

            let st = mvcc.vacuum().unwrap();
            assert!(
                st.versions_removed >= 2,
                "应删除 a 旧版与 b 的历史, removed={}",
                st.versions_removed
            );

            let tx = mvcc.begin_transaction();
            assert_eq!(tx.get(b"a"), Some(b"2".to_vec()));
            assert_eq!(tx.get(b"b"), None);
            assert_eq!(tx.get(b"big"), Some(big.clone()));
            tx.commit();
        }
        {
            let mvcc = MVCC::open(&path, 16, 64);
            let tx = mvcc.begin_transaction();
            assert_eq!(tx.get(b"a"), Some(b"2".to_vec()));
            assert_eq!(tx.get(b"big"), Some(big));
            tx.commit();

            // 删除唯一大 value 后再 vacuum → blob 应被清空
            let t = mvcc.begin_transaction();
            assert!(t.delete(b"big"));
            t.commit();
            let st = mvcc.vacuum().unwrap();
            assert!(st.versions_removed >= 1);
            // blob 无引用时应收缩
            let blob = crate::bplus_tree::storage::blob_path(&path);
            if blob.exists() {
                let len = std::fs::metadata(&blob).unwrap().len();
                assert!(len <= 16, "无 blob 引用时文件应接近仅魔数, len={len}");
            }
        }
        cleanup(&path);
    }

    #[test]
    fn test_try_open_lock_conflict() {
        let path = tmp_db("lock2");
        let m1 = MVCC::try_open(&path, 8, 16).unwrap();
        let err = MVCC::try_open(&path, 8, 16).err();
        assert!(err.is_some());
        drop(m1);
        let m2 = MVCC::try_open(&path, 8, 16).unwrap();
        drop(m2);
        cleanup(&path);
    }

    /// 干净 checkpoint 后重开：meta 持久化 next_version，version 连续且数据可读
    #[test]
    fn test_reopen_preserves_next_version_after_checkpoint() {
        let path = tmp_db("reopen_nv");
        let v1;
        {
            let mvcc = MVCC::open(&path, 16, 32);
            let t = mvcc.begin_transaction();
            v1 = t.version();
            assert!(t.set(b"k", b"v1".to_vec()));
            t.commit();
            // checkpoint 把 next_version 写入 meta 并截断 WAL
            mvcc.checkpoint();
        }
        {
            let mvcc = MVCC::open(&path, 16, 32);
            let t = mvcc.begin_transaction();
            // 新事务 version 必须严格大于上次已用 version
            assert!(t.version() > v1, "reopen version={} should > {}", t.version(), v1);
            assert_eq!(t.get(b"k"), Some(b"v1".to_vec()));
            assert!(t.set(b"k", b"v2".to_vec()));
            t.commit();
        }
        {
            let mvcc = MVCC::open(&path, 16, 32);
            let t = mvcc.begin_transaction();
            assert_eq!(t.get(b"k"), Some(b"v2".to_vec()));
            t.commit();
        }
        cleanup(&path);
    }

    /// 未手动 checkpoint：meta 可能仍是上次 open 写入的旧 next_version，
    /// 应靠 WAL 中 max_xid 把起点抬高，保证 version 单调。
    #[test]
    fn test_reopen_uses_wal_max_xid_without_checkpoint() {
        let path = tmp_db("reopen_wal");
        let mut last_v = 0u64;
        {
            let mvcc = MVCC::open(&path, 16, 32);
            for i in 0..5 {
                let t = mvcc.begin_transaction();
                last_v = t.version();
                assert!(t.set(format!("k{i}").as_bytes(), b"x".to_vec()));
                t.commit();
            }
            // 故意不 checkpoint：WAL 里保留 Begin/Write/Commit
        }
        {
            let mvcc = MVCC::open(&path, 16, 32);
            let t = mvcc.begin_transaction();
            assert!(
                t.version() > last_v,
                "version after reopen={} must > last committed {}",
                t.version(),
                last_v
            );
            assert_eq!(t.get(b"k4"), Some(b"x".to_vec()));
            t.commit();
        }
        cleanup(&path);
    }

    /// 长跑进程不喊 vacuum，版本链也不能无限涨（I2：垃圾必须自己回来）。
    ///
    /// bplus 的一片要真读叶子页，所以这里除了「链被压住」还断言**最新值始终读得到**：
    /// 自动裁的是旧版本，不是可见版本 —— 裁错就是静默数据错误。
    #[test]
    fn test_auto_vacuum_reclaims_without_being_asked() {
        const CHAIN: u64 = 200;
        const SLICE: u64 = 8;

        let path = tmp_db("auto_vac");
        {
            let mvcc = MVCC::open(&path, 16, 32);
            assert_eq!(
                mvcc.vacuum_auto_slice(),
                MVCC::VACUUM_AUTO_SLICE_DEFAULT,
                "自动回收默认必须开着"
            );
            mvcc.set_vacuum_auto_slice(SLICE);

            for i in 0..CHAIN {
                let tx = mvcc.begin_transaction();
                assert!(tx.set(b"hot", format!("v{i}").into_bytes()));
                tx.commit();
            }

            let h = mvcc.health();
            assert!(
                h.max_chain <= SLICE + 2,
                "没人喊 vacuum，链却涨到 {} 行 —— 自动回收没在工作",
                h.max_chain
            );

            let tx = mvcc.begin_transaction();
            assert_eq!(tx.get(b"hot"), Some(format!("v{}", CHAIN - 1).into_bytes()));
            tx.commit();
        }
        cleanup(&path);
    }

    /// 关掉的开关必须真关掉：自动回收不能是「没法关掉的那种」。
    #[test]
    fn test_auto_vacuum_off_switch() {
        const CHAIN: u64 = 60;

        let path = tmp_db("auto_off");
        {
            let mvcc = MVCC::open(&path, 16, 32);
            mvcc.set_vacuum_auto_slice(0);
            for i in 0..CHAIN {
                let tx = mvcc.begin_transaction();
                assert!(tx.set(b"hot", format!("v{i}").into_bytes()));
                tx.commit();
            }
            assert_eq!(mvcc.health().max_chain, CHAIN, "关闭后不该裁掉任何一版");
        }
        cleanup(&path);
    }

    /// 有界回收：每轮只碰一小段，多轮跑完必须和一次全库 vacuum 得到**同样**的结果。
    ///
    /// `vacuum()` 把 kv 锁按到整条叶子链走完；切片之后每轮读的**叶子**有上限
    /// （偏差 = 一条链的长度：截断只能落在逻辑 key 边界），游标每轮必须前进。
    /// 键里放了前缀嵌套的 `k1` / `k1x`：版本键 `raw ‖ 8B` 下两把 key 的区间会互相穿插。
    #[test]
    fn test_vacuum_bounded_slices_and_converges() {
        const CHAIN: u64 = 120;
        const KEYS: [&[u8]; 6] = [b"k1", b"k1x", b"k2", b"k3", b"k4", b"k5"];
        const SLICE: u64 = 40;

        fn fill(mvcc: &MVCC) {
            for i in 0..CHAIN {
                for k in KEYS {
                    let tx = mvcc.begin_transaction();
                    assert!(tx.set(k, format!("v{i}").into_bytes()), "写入被拒：{k:?}");
                    tx.commit();
                }
            }
        }

        // 对照组
        let full_path = tmp_db("bounded_full");
        let full_removed;
        {
            let mvcc = MVCC::open(&full_path, 16, 32);
            fill(&mvcc);
            full_removed = mvcc.vacuum().unwrap().versions_removed;
        }
        cleanup(&full_path);

        let path = tmp_db("bounded_slice");
        {
            let mvcc = MVCC::open(&path, 16, 32);
            fill(&mvcc);

            let mut cursor: Option<Vec<u8>> = None;
            let mut rounds = 0usize;
            let mut removed = 0usize;
            loop {
                let round = mvcc.vacuum_bounded(cursor.as_deref(), SLICE).unwrap();
                assert!(!round.stats.skipped_active_txn, "第 {rounds} 轮不该被活跃事务挡住");
                assert!(
                    round.stats.rows_scanned <= SLICE + CHAIN,
                    "第 {rounds} 轮读了 {} 行（预算 {SLICE} + 一条链 {CHAIN}）",
                    round.stats.rows_scanned
                );
                assert!(round.stats.versions_removed > 0, "第 {rounds} 轮必须裁掉东西");
                removed += round.stats.versions_removed;
                rounds += 1;
                match round.next_cursor {
                    Some(c) => {
                        assert_ne!(cursor.as_ref(), Some(&c), "游标必须前进（第 {rounds} 轮）");
                        cursor = Some(c);
                    }
                    None => break,
                }
                assert!(rounds < 60, "切片应在有限轮内收敛，别是原地打转：已 {rounds} 轮");
            }

            assert!(rounds > 1, "6 把 key × {CHAIN} 版不该一轮就扫完：{rounds} 轮");
            assert_eq!(removed, full_removed, "分片裁的总行数应与整库一轮一致");
            let h = mvcc.health();
            assert_eq!(h.max_chain, 1, "分片跑完后链必须压平");
            assert_eq!(h.total_rows, KEYS.len() as u64);

            let tx = mvcc.begin_transaction();
            for k in KEYS {
                assert_eq!(tx.get(k), Some(format!("v{}", CHAIN - 1).into_bytes()));
            }
            tx.commit();
        }
        cleanup(&path);
    }

    /// 只读事务不许往 WAL 落一个字节，也不许付 fsync 的代价。
    ///
    /// 反向那半同样重要：**写过就必须有 `Commit` 记录 + fsync** —— 不然这条优化会悄悄
    /// 把"提交的持久化点"一起省掉，那比慢一千倍严重。
    #[test]
    fn test_read_only_transaction_touches_no_wal() {
        const READS: u64 = 200;

        let path = tmp_db("ro_wal");
        {
            let mvcc = MVCC::open(&path, 16, 32);
            let tx = mvcc.begin_transaction();
            assert!(tx.set(b"k", b"v".to_vec()));
            tx.commit();

            let wal = wal_path(&path);
            let size0 = std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0);
            for _ in 0..READS {
                let tx = mvcc.begin_transaction();
                assert_eq!(tx.get(b"k"), Some(b"v".to_vec()));
                tx.commit();
            }
            let size1 = std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0);
            assert_eq!(size1, size0, "{READS} 次只读事务往 WAL 落了 {} 字节", size1 - size0);

            // 写过的事务照旧进 WAL
            let tx = mvcc.begin_transaction();
            assert!(tx.set(b"k", b"w".to_vec()));
            tx.commit();
            let size2 = std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0);
            assert!(size2 > size1, "写入必须留下 Begin/Write/Commit 记录，持久化点不能被省掉");

            // 只读事务也不能把数据读坏（版本号照旧推进，快照语义不变）
            let tx = mvcc.begin_transaction();
            assert_eq!(tx.get(b"k"), Some(b"w".to_vec()));
            tx.commit();
        }
        cleanup(&path);
    }

    /// 库必须能自己报出「最长版本链」。
    ///
    /// 这次事故的核心数字（某把热点 key 堆了多少个版本）一直是靠数日志行数得出来的 ——
    /// 一个当基座用的库不该靠这个诊断自己。四引擎共用同一份 `MvccHealth`，
    /// 所以换引擎时这两组断言必须照样成立（涨速不同，指标是同一个）。
    #[test]
    fn test_health_reports_chain_length() {
        const CHAIN: u64 = 500;

        let path = tmp_db("health");
        {
            let mvcc = MVCC::open(&path, 16, 32);
            for i in 0..CHAIN {
                let tx = mvcc.begin_transaction();
                assert!(tx.set(b"hot", format!("v{i}").into_bytes()), "第 {i} 次写入被拒");
                tx.commit();
            }
            for k in 0..3u64 {
                let key = format!("other{k}");
                let tx = mvcc.begin_transaction();
                assert!(tx.set(key.as_bytes(), b"x".to_vec()));
                tx.commit();
            }

            let base = crate::bplus_tree::page::value_clones();
            let h = mvcc.health();
            let clones = crate::bplus_tree::page::value_clones() - base;
            assert_eq!(h.live_keys, 4, "逻辑 key 数");
            assert_eq!(h.total_rows, CHAIN + 3, "物理行数（含历史版本）");
            assert_eq!(h.max_chain, CHAIN, "最长版本链应由库自己报出来");
            assert_eq!(h.rows_above_one, CHAIN - 1);
            assert_eq!(h.active_txns, 0);
            // 采样链长这件事本身不该制造一次全库 value 拷贝
            assert_eq!(clones, 0, "health 期间 value 克隆次数：{clones}（历史行数 {}）", h.total_rows);

            let stats = mvcc.vacuum().unwrap();
            assert!(!stats.skipped_active_txn, "无活跃事务时不该跳过");
            let after = mvcc.health();
            assert_eq!(after.max_chain, 1, "vacuum 后最长链应降到 1");
            assert_eq!(after.total_rows, 4, "vacuum 后应只剩每 key 一行");
            assert_eq!(after.live_keys, 4);
            assert_eq!(after.rows_above_one, 0);
        }
        cleanup(&path);
    }

    /// 窗口 scan 的代价必须随**窗口宽度**缩 —— B+ 树的尺子是「读了多少页」。
    ///
    /// 旧形状：`collect_latest_raw()` 从最左叶子一路走完**全表**，调用方再逐条筛界
    /// ⇒ 10 行的窗口和 3000 行的全量读一样多的页（大库基线实测：窗口 scan 3290 µs/行，
    /// 而同库一次全量 scan 是 32.9 µs/行）。现在起点下降到 containing 叶子（前面的叶子
    /// 一篇都不读）、终点靠回调喊停。
    ///
    /// B+ 树没有 Bitcask KeyDir 那样的独立键索引，"窗口覆盖到的那几页要反序列化"省不掉 ——
    /// 这条省的是**页数**，不是每页的解析成本。断言两半：① 窗口结果与"全量之后筛"逐条一致
    /// （界判得对，不是筛错换来快）；② 页数随窗口缩。
    #[test]
    fn test_windowed_scan_cost_scales_with_window_not_library() {
        const KEYS: u64 = 3000;
        let path = tmp_db("window_cost");
        {
            let mvcc = MVCC::open(&path, 64, 256);
            for i in 0..KEYS {
                let tx = mvcc.begin_transaction();
                assert!(tx.set(format!("k{i:05}").as_bytes(), format!("v{i}").into_bytes()));
                tx.commit();
            }

            let tx = mvcc.begin_transaction();
            let before = crate::bplus_tree::page::page_reads();
            let all = tx.scan(None, None);
            let full = crate::bplus_tree::page::page_reads() - before;

            let lo = b"k01000".to_vec();
            let hi = b"k01010".to_vec();
            let before = crate::bplus_tree::page::page_reads();
            let win = tx.scan(Some(&lo), Some(&hi));
            let windowed = crate::bplus_tree::page::page_reads() - before;

            assert_eq!(all.len() as u64, KEYS, "全量应折出 {KEYS} 行");
            assert_eq!(win.len(), 10, "[k01000, k01010) 应给 10 行");
            let want: Vec<Vec<u8>> = all
                .iter()
                .filter(|r| r.key.as_slice() >= lo.as_slice() && r.key.as_slice() < hi.as_slice())
                .map(|r| r.key.clone())
                .collect();
            let got: Vec<Vec<u8>> = win.iter().map(|r| r.key.clone()).collect();
            assert_eq!(got, want, "下推界之后窗口结果必须与全量筛一致");
            assert!(
                windowed * 10 <= full,
                "10 行窗口的页数不该是全库量级（{windowed} vs 全量 {full}）"
            );

            // 上界哨兵的陷阱形状（`seek_prev` 造的界是 `key ‖ 0x00`）：这条在内存引擎先红过
            assert_eq!(tx.seek_prev(b"k01005").unwrap().key, b"k01005");
            assert_eq!(tx.seek(b"k01000").unwrap().key, b"k01000");

            // 前缀同理：k001xx ⇒ 100 个键
            let before = crate::bplus_tree::page::page_reads();
            let pre = tx.prefix_scan(b"k001");
            let prefixed = crate::bplus_tree::page::page_reads() - before;
            assert_eq!(pre.len(), 100, "前缀 k001 应命中 100 行");
            assert!(
                prefixed * 4 <= full,
                "前缀扫的页数不该是全库量级（{prefixed} vs 全量 {full}）"
            );

            // 边界形状：前缀是**所有键的真前缀**（比第一个键还短 ⇒ 下降到最左叶子）。
            // lsm 曾把这种扫法做成空结果，四家都钉一遍。
            let whole = tx.prefix_scan(b"k");
            assert_eq!(whole.len() as u64, KEYS, "前缀 k 应命中全部 {KEYS} 行");
            assert_eq!(tx.key_count(b"k") as u64, KEYS, "key_count 同一个形状");
            tx.rollback();
        }
        cleanup(&path);
    }
}
