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
    collections::{BTreeMap, HashMap},
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
pub use crate::common::{
    DurabilityProfile, EngineTuning, ExportRecord, MvccHealth, VacuumRound, VacuumStats,
};
use crate::common::{HealthFold, VacuumScheduler};
use crate::common::txn::{BulkCore, Registry, TxnCore, WalTail, WriteSide};
use crate::common::vacuum_plan;
use crate::common::version_key_upper_bound;

/// MVCC 存储引擎
pub struct MVCC {
    kv: Arc<Mutex<BPlusTree>>,
    /// 大 value 外置存储（与 kv 同库目录）
    blob: Arc<Mutex<BlobStore>>,
    wal: Arc<Mutex<Wal>>,
    active_txn: Arc<Mutex<Registry>>,
    next_version: Arc<AtomicU64>,
    /// WAL 增长到多少字节就在下一次提交后自动 checkpoint（0 = 关闭自动触发）
    wal_limit: Arc<AtomicU64>,
    /// 自动回收：切片宽度 / 记账 / 游标（策略与实现在 [`crate::common::VacuumScheduler`]）
    vac: Arc<VacuumScheduler>,
    db_path: PathBuf,
}

/// 普通 MVCC 事务
///
/// 事务语义（版本、活跃快照、写集、冲突判定、整条写序列、回收记账）在 [`TxnCore`]
/// （[`crate::common::txn`]，四引擎共用）；这一家只留"字节在哪一页、大值走不走 blob、
/// 这一行装不装得下一页"。
pub struct Transaction {
    pub(crate) kv: Arc<Mutex<BPlusTree>>,
    pub(crate) blob: Arc<Mutex<BlobStore>>,
    pub(crate) wal: Arc<Mutex<Wal>>,
    next_version: Arc<AtomicU64>,
    wal_limit: Arc<AtomicU64>,
    pub(crate) core: TxnCore,
}

/// 写路径的守卫：三把锁按内核要求的顺序（活跃表 → wal → blob → kv）拿好。
///
/// 四家里只有这一家要用满 [`WriteSide`] 的槽：`encode_value`（blob 旁路）与 `row_fits`
/// （定长页的容量判定）。回滚与部分撤销复用同一个守卫（多锁一次 blob，无竞争）。
pub(crate) struct TxnSide<'w, 'b, 'k> {
    pub(crate) wal: &'w mut Wal,
    pub(crate) blob: &'b mut BlobStore,
    pub(crate) kv: &'k mut BPlusTree,
}

/// 收尾用的守卫：`commit` 只要往 WAL 落一条 `Commit` 再 fsync，不该在这期间握着 kv/blob 锁
/// （`flush_all` 与 checkpoint 由调用方在放掉这两把锁之后自己做）。
struct WalOnly<'w> {
    wal: &'w mut Wal,
}

/// 批量导入会话：无 WAL、无冲突检测，单写者假定
pub struct BulkLoader {
    kv: Arc<Mutex<BPlusTree>>,
    blob: Arc<Mutex<BlobStore>>,
    wal: Arc<Mutex<Wal>>,
    core: BulkCore,
}

/// 逻辑键 + 版本（调试用）与版本键编解码
///
/// 定义在 [`crate::common::version`]（四引擎共用）；这里 re-export 是为了
/// `crate::bplus_tree::encode_key` 这个既有路径不中断。
pub use crate::common::{decode_key, encode_key, VersionedKey};

/// 落树但**不静默**：树对"单行比一页还大"这一条只能返回 false，而 bulk 的 `put` 与 WAL 重放
/// 都没有返回值通道（`KvBulk::put` 是 `()`），所以这里把事实喊出来。
/// 两种情况都不会写坏已有数据：树那边是**一字未写**。
fn insert_or_shout(kv: &mut BPlusTree, enc: Vec<u8>, row: RowId, site: &str) {
    if !kv.insert(enc.clone(), row) {
        log::error!(
            "{site}: 这一行装不进任何一页（编码后键 {} 字节），已拒收；数据未受影响",
            enc.len()
        );
    }
}

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
            insert_or_shout(&mut tree, key.clone(), RowId(value.clone()), "WAL REDO");
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
        BulkLoader {
            kv: self.kv.clone(),
            blob: self.blob.clone(),
            wal: self.wal.clone(),
            core: BulkCore::begin(self.next_version.clone()),
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

    /// 生效的 B+ 树阶数（每个节点的子节点上限）。
    ///
    /// 只在**打开时**可设：阶数决定已落盘节点的形状，开着库改它等于让新旧页用两套规则。
    pub fn order(&self) -> usize {
        self.kv.lock().unwrap().order()
    }

    /// 生效的页缓存容量（页数）。
    ///
    /// 这是全库走查（scan / health / search / vacuum）与工作集能否焐热之间的那颗旋钮：
    /// 页数超过容量时走查只"装得下才缓存"（`read_page_for_walk`），不再挤掉 OLTP 的热页。
    pub fn max_cache_pages(&self) -> usize {
        self.kv.lock().unwrap().max_cache_pages()
    }

    /// 缓存策略读数：`(生效容量, 钉住的上层页数, 是否自动档)`。
    ///
    /// 自动档必须**看得见生效值**："设了没生效"和"生效了但不知道是多少"在下游日志里
    /// 长得一模一样，只有读回来才分得开。
    pub fn cache_policy(&self) -> (usize, usize, bool) {
        self.kv.lock().unwrap().cache_policy()
    }

    /// 本引擎能被调的东西 —— 清单在这里，不在门面。
    pub fn tuning(&self) -> EngineTuning {
        let mut t = EngineTuning::default();
        t.push_shared(self.vacuum_auto_slice(), Some(self.wal_checkpoint_limit()));
        t.push("order", self.order());
        let (cap, pinned, auto) = self.cache_policy();
        // `max_cache_pages` 报的是**生效值**：传 0（自动档）时这里是算出来的那个数
        t.push("max_cache_pages", cap);
        t.push("cache_pinned_pages", pinned);
        t.push("cache_auto", if auto { 1 } else { 0 });
        t
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
        self.finish_inner();
    }

    fn finish_inner(&mut self) {
        // 幂等守卫在内核里（`finish()` 与 `Drop` 都会调），收尾动作本身是 bplus 自己的事
        if !self.core.first_finish() {
            return;
        }
        let mut kv = self.kv.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();
        let mut wal = self.wal.lock().unwrap();
        let _ = blob.sync();
        let next_ver = self.core.next_version().load(Ordering::SeqCst);
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
        self.finish_inner();
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
        if !self.core.unfinished() {
            return;
        }
        if self.wal.lock().is_err() {
            return;
        }
        log::warn!("事务 {} 未 commit/rollback 即被丢弃，已自动回滚", self.core.version);
        self.rollback();
    }
}

impl Transaction {
    pub fn begin(
        kv: Arc<Mutex<BPlusTree>>,
        blob: Arc<Mutex<BlobStore>>,
        wal: Arc<Mutex<Wal>>,
        active_txn: Arc<Mutex<Registry>>,
        next_version: Arc<AtomicU64>,
        wal_limit: Arc<AtomicU64>,
        vac: Arc<VacuumScheduler>,
    ) -> Self {
        // begin 不进 WAL：此刻还不知道会不会写；第一次 write() 才补 `Begin`
        // （只读事务因此一个字节都不落，详见 [`crate::common::WalPolicy`]）
        let core = TxnCore::begin(active_txn, &next_version, vac);
        Transaction {
            kv,
            blob,
            wal,
            next_version,
            wal_limit,
            core,
        }
    }

    pub fn version(&self) -> u64 {
        self.core.version
    }

    pub fn set(&self, key: &[u8], value: Vec<u8>) -> bool {
        self.write(key, Some(value))
    }

    /// 原样存这串字节 —— 引擎替你打信封，`get` 会一字不差地还回来。
    ///
    /// 与 [`Self::set`] 的分工：`set` 把传进来的字节当作**已经打过信封**的逻辑值（现有调用点
    /// 按这个约定写，改它会破坏它们），所以拿 `set` 直接存裸字节会踩坑：值以 `0x00` 打头
    /// 读回来少 1 字节，以 `0x01` 打头会被当成 TTL 信封少 9 字节、甚至被判"已过期"读成 `None`。
    /// 存二进制/序列化字节（protobuf、avro、压缩块）走这个口子，不必再记信封规矩。
    ///
    /// 空值、`0x00`/`0x01` 打头、超过 blob 阈值走旁路的长值，都由每条用例里的
    /// `test_set_raw_round_trips_every_byte_pattern` 逐字节钉住；同一条里还对照断言
    /// "`set` 存同样的字节确实会少" —— 不对照就证明不了这个口子在做事。
    ///
    /// 读侧语义不变：`get` / `scan` / `multi_get` 一律照旧剥信封（剥掉的正是这里加的那一层）。
    /// 要"原样字节 + 过期时间"用 `set_with_ttl`（它内部同样已打信封）。
    pub fn set_raw(&self, key: &[u8], value: Vec<u8>) -> bool {
        self.set(key, crate::common::pack_plain(&value))
    }

    pub fn delete(&self, key: &[u8]) -> bool {
        self.write(key, None)
    }

    fn write(&self, key: &[u8], value: Option<Vec<u8>>) -> bool {
        // 锁序：活跃表 → wal → blob → kv；那八步序列在 [`TxnCore::write`]（四引擎共用的一份）
        let mut reg = self.core.lock_registry();
        let mut wal = self.wal.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();
        let mut kv = self.kv.lock().unwrap();
        self.core.write(
            &mut reg,
            &mut TxnSide {
                wal: &mut wal,
                blob: &mut blob,
                kv: &mut kv,
            },
            key,
            value,
        )
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
        crate::common::collect_rows(self, include_deleted, start, end, None, None).0
    }

    /// 取一组的值：叶子条目里带出来的就是存储字节，只有超过 blob 阈值的那部分要再解一次
    /// （内联行的"无 tag"与解码失败都按墓碑算，与改动前同口径）。
    fn read_row(&self, row: &Option<Vec<u8>>) -> crate::common::RowValue {
        use crate::common::RowValue;
        match row {
            None => RowValue::Tombstone,
            Some(stored) => {
                let mut blob = self.blob.lock().unwrap();
                match decode_stored_value(&mut blob, stored).ok().flatten() {
                    Some(bytes) => RowValue::Value(bytes),
                    None => RowValue::Tombstone,
                }
            }
        }
    }

    /// 折出**候选组**（一把逻辑 key 一组，页里的原始字节顺手带走）。
    ///
    /// 为什么值要在这里就带上：B+ 树的 value 就住在叶子条目里，遍历的时候已经在手上。
    /// 原先抄 bitcask 的两段式（① 只折键 ② 按胜出行回树取值）对 bitcask 对 ——
    /// 它的 KeyDir 只有元数据、值在日志另一处；对 B+ 树就是**每个存活 key 多付
    /// 3~4 次页访问**：实测一次全量 scan 3.06 页/行、36.2 µs/行。
    ///
    /// `after` 是续跑游标、`accept` 是键级过滤（不命中的键连字节都不带）、
    /// `budget` 是组数上限；返回的 `bool` = 已走到界尾（`false` = 被预算拦下，还能续）。
    /// ⚠️ 交出一组只能在组边界：全表按 enc 升序 ⇒ 同一 raw_key 的版本块连续，
    /// 最后一次覆盖才是最大可见版本；在半截组上收手会把旧版本当新的交出去。
    pub(crate) fn scan_candidates(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        accept: Option<&dyn Fn(&[u8]) -> bool>,
        after: Option<&[u8]>,
        budget: usize,
    ) -> (Vec<crate::common::WinCandidate<Option<Vec<u8>>>>, bool) {
        let hi_enc = end.map(version_key_upper_bound);
        // 候选按组边界依次成型，两个 Vec 用索引配对（闭包不能把 `out` 同时借走，
        // 见 memory 引擎同名方法里的说明）
        let mut keys: Vec<Vec<u8>> = Vec::new();
        let mut rows: Vec<Option<Vec<u8>>> = Vec::new();
        let mut versions: Vec<u64> = Vec::new();
        let mut stopped_by_budget = false;
        {
            let mut kvengine = self.kv.lock().unwrap();
            let mut winners: BTreeMap<Vec<u8>, (u64, Option<Vec<u8>>)> = BTreeMap::new();
            let mut pending: Option<Vec<u8>> = None;
            let accept: Option<&dyn Fn(&[u8]) -> bool> = accept;
            kvengine.scan_entries_break_from(crate::common::fold_lower_bound(start, after), |enc, row| {
                if hi_enc.as_deref().is_some_and(|h| enc >= h) {
                    return true; // 按 enc 定界 ⇒ 有序迭代后面只会更大，可以真停
                }
                let Some(vk) = decode_key(enc) else {
                    return false;
                };
                // 界判在原始 key 上，逐条兜（前缀交错的边角）
                if start.is_some_and(|s| vk.raw_key.as_slice() < s) {
                    return false;
                }
                if end.is_some_and(|e| vk.raw_key.as_slice() >= e) {
                    return false;
                }
                if after.is_some_and(|a| vk.raw_key.as_slice() <= a) {
                    return false; // 上一轮已经交出去的那组
                }
                if !self.is_visible(vk.version) {
                    return false;
                }
                if pending.as_deref() != Some(vk.raw_key.as_slice()) {
                    if let Some(key) = pending.take() {
                        if let Some((ver, stored)) = winners.remove(&key) {
                            if accept.is_none_or(|f| f(&key)) {
                                keys.push(key);
                                versions.push(ver);
                                rows.push(stored);
                                if keys.len() >= budget {
                                    stopped_by_budget = true;
                                    return true;
                                }
                            }
                        }
                    }
                    pending = Some(vk.raw_key.clone());
                }
                winners.insert(vk.raw_key.clone(), (vk.version, row.clone()));
                false
            });
            if !stopped_by_budget {
                if let Some(key) = pending {
                    if let Some((ver, stored)) = winners.remove(&key) {
                        if accept.is_none_or(|f| f(&key)) {
                            keys.push(key);
                            versions.push(ver);
                            rows.push(stored);
                        }
                    }
                }
            }
        }
        let out = keys
            .into_iter()
            .zip(versions)
            .zip(rows)
            .map(|((key, version), row)| crate::common::WinCandidate { key, version, row })
            .collect();
        (out, !stopped_by_budget)
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
        // 销账 + 回收记账 + WAL 收尾（`Commit` + fsync）在 [`TxnCore::commit`] 一份。
        // 收尾只交 wal 守卫：fsync 期间不许握着 kv/blob —— `flush_all` 与 checkpoint
        // 是放掉锁之后再做的（它们各自要再拿一次 kv 锁）。
        {
            let mut reg = self.core.lock_registry();
            let mut wal = self.wal.lock().unwrap();
            self.core.commit(&mut reg, &mut WalOnly { wal: &mut wal });
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
        // 销账 + `Abort` + 按写集撤行，都在 [`TxnCore::rollback`]（锁序与 write 一致）
        let mut reg = self.core.lock_registry();
        let mut wal = self.wal.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();
        let mut kv = self.kv.lock().unwrap();
        self.core.rollback(
            &mut reg,
            &mut TxnSide {
                wal: &mut wal,
                blob: &mut blob,
                kv: &mut kv,
            },
        );
    }

    /// 部分撤销：批量写撞墙时把已落下的那几把 key 退回（连同写集一起摘掉）。
    pub(crate) fn undo(&self, keys: &[Vec<u8>]) {
        let mut reg = self.core.lock_registry();
        let mut wal = self.wal.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();
        let mut kv = self.kv.lock().unwrap();
        self.core.undo(
            &mut reg,
            &mut TxnSide {
                wal: &mut wal,
                blob: &mut blob,
                kv: &mut kv,
            },
            keys,
        );
    }

    fn is_visible(&self, version: u64) -> bool {
        // 判据在 [`crate::common::visible_at`]（四引擎共用的一份）
        self.core.is_visible(version)
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

/// 写侧：四家里唯一用满 [`WriteSide`] 槽的一家 —— 它既要 blob 旁路（`encode_value`），
/// 又要定长页的容量判定（`row_fits`）。
///
/// ⚠️ `row_fits` 里那句 `log::error!` 是"拒收一行"唯一的对外说明（返回值只有 `false`，
/// 门面那条 `EngineError` 通道没为它开），所以留在原地，别当成冗余日志删掉。
impl WriteSide for TxnSide<'_, '_, '_> {
    fn latest_version(&mut self, key: &[u8]) -> Option<u64> {
        Transaction::latest_version_of(self.kv, key)
    }

    fn put_versioned(&mut self, enc: &[u8], value: Option<Vec<u8>>) -> bool {
        // `row_fits` 已经判过装得下，`insert` 仍可能因为别的形状失败 —— 那条 bool 就是它的答复，
        // 内核据此决定要不要登记写集（不许留下"写集里有、树里没有"的悬账）。
        self.kv.insert(enc.to_vec(), RowId(value))
    }

    fn remove_versioned(&mut self, enc: &[u8]) {
        let _ = self.kv.delete(enc.to_vec());
    }

    fn encode_value(&mut self, value: Vec<u8>) -> Option<Vec<u8>> {
        // 逻辑 value → 叶子存储编码（大 value 写 blob；事务路径每条都 fsync）
        match encode_stored_value(self.blob, value, true) {
            Ok(s) => Some(s),
            Err(e) => {
                eprintln!("blob encode 失败: {e}");
                None
            }
        }
    }

    fn row_fits(&mut self, enc: &[u8], stored: Option<&[u8]>) -> bool {
        // 先问树装不装得下，再动 WAL。树的"这一行比一页还大"必须在这里就变成 `false`：
        // 否则 WAL 里留下一条"提交了但树里没有"的记录，恢复时只会再拒一次。
        let owned = stored.map(|v| v.to_vec());
        if self.kv.row_fits(enc, &owned) {
            return true;
        }
        log::error!(
            "bplus_tree 拒收一行：键 {} 字节 + 值 {} 字节，单行已超过一页载荷 {} 字节 ⇒ 键必须短于一页",
            enc.len(),
            owned.as_ref().map_or(0, |v| v.len()),
            crate::bplus_tree::page::MAX_PAYLOAD
        );
        false
    }

    fn wal_write(&mut self, xid: u64, enc: &[u8], stored: Option<&[u8]>) -> bool {
        self.wal
            .append(WalRecord::Write {
                xid,
                key: enc.to_vec(),
                value: stored.map(|v| v.to_vec()),
            })
            .is_ok()
    }
}

/// 收尾两条：`Commit` + fsync、`Abort` + fsync。fsync 失败只喊出来（与改前一致）——
/// 事务此刻已从活跃表摘除、行也已落树，没有能回退的通道。
impl WalTail for TxnSide<'_, '_, '_> {
    fn wal_commit(&mut self, xid: u64) {
        let _ = self.wal.append(WalRecord::Commit { xid });
        if let Err(e) = self.wal.sync() {
            eprintln!("WAL fsync 失败: {e}");
        }
    }

    fn wal_abort(&mut self, xid: u64) {
        let _ = self.wal.append(WalRecord::Abort { xid });
        let _ = self.wal.sync();
    }
}

impl WalTail for WalOnly<'_> {
    fn wal_commit(&mut self, xid: u64) {
        let _ = self.wal.append(WalRecord::Commit { xid });
        if let Err(e) = self.wal.sync() {
            eprintln!("WAL fsync 失败: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bplus_tree::storage::{dblwr_path, wal_path};

    fn tmp_db(tag: &str) -> PathBuf {
        crate::common::tmp_test_path("mvcc", tag, ".db")
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
            // 老日志的形状：M25 起写侧不再落 `Begin`，这里手写一份**带 Begin** 的日志，
            // 恢复必须照样是对的那份（读侧兼容由 `plan_is_identical_with_or_without_begin` 钉死）
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
                // 同上：带 `Begin` 的老日志形状
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
            // 故意不 checkpoint：WAL 里保留 Write/Commit
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
            assert!(size2 > size1, "写入必须留下 Write/Commit 记录，持久化点不能被省掉");

            // 字节数当尺子**不够**：只落一条 `Commit` 也能让体积涨，于是"`Write` 记录整个没写"
            // 这种形状照样绿（M18d 实测：把 `wal_write` 改成不 append 直接返回成功，bitcask 那条
            // 一路通过）。判据必须精确到记录类型 —— 一次写要在 WAL 里留下一条 `Write`。
            {
                let mut reader = Wal::open(&wal).expect("打开 WAL 读记录");
                let records = reader.read_all().expect("读 WAL");
                let writes = records
                    .iter()
                    .filter(|e| matches!(&e.record, WalRecord::Write { .. }))
                    .count();
                assert_eq!(
                    writes, 2,
                    "两次真写要在 WAL 里各留一条 Write，实际 {writes} 条 \
                     （只数 Begin/Commit 的尺子抓不到丢了的 Write）"
                );
                // M25：写侧不再落 `Begin`（恢复不读它）。老日志**还能读**那半边由
                // `plan_is_identical_with_or_without_begin` 钉住，两边缺一条都是洞。
                let begins = records
                    .iter()
                    .filter(|e| matches!(&e.record, WalRecord::Begin { .. }))
                    .count();
                assert_eq!(begins, 0, "写侧不再落 Begin，实际留下 {begins} 条");
            }

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

    /// 一次全量 scan 的**页访问次数**必须与叶子页数同阶，而不是与「存活 key 数 × 树高」同阶。
    ///
    /// 这是那条两段式的账：B+ 树把 value 存在叶子条目里，折叠遍历时已经在手上；
    /// 旧实现却照抄 bitcask 的第二段「按胜出行再降一次树取值」，于是每个 key 多付
    /// 3~4 次页访问 —— 实测一次全量 scan **3.06 页/行**、36.2 µs/行；改成一趟之后是
    /// 单行装不进一页时，事务层必须**在写 WAL 之前**就拒掉：`set` 返回 `false`、WAL 里
    /// 不许留下这条 Write、重开之后也不许出现"WAL 说有、树里查无此键"。
    ///
    /// 为什么单独盯 WAL 这一半：`insert` 拒收只解决"别写坏已有数据"，而事务层是
    /// 先 append WAL 再写树 —— 顺序错了就会留下一条永久无法重放的提交记录。
    #[test]
    fn test_oversized_row_is_refused_before_the_wal() {
        let path = tmp_db("oversize");
        let big_key = vec![b'K'; 5000]; // 5 KB 键 + 8 字节版本后缀 > 一页载荷 4080 B
        {
            let mvcc = MVCC::open(&path, 64, 512);
            let tx = mvcc.begin_transaction();
            assert!(tx.set(b"small", vec![b'v'; 60]), "正常行要写得进去");
            tx.commit();

            let tx = mvcc.begin_transaction();
            assert!(
                !tx.set(&big_key, vec![b'v'; 8]),
                "装不下的行必须报 false（这里不是写冲突，是引擎拒收）"
            );
            assert!(tx.get(&big_key).is_none());
            tx.commit();

            // 判据要精确到**记录类型**：被拒的那行不许留下 `Write`（带键的记录只有 `Write` 一种，
            // 所以"容量判定必须在落 WAL 之前"这条顺序就由下面这两条计数守着 —— M25 之前还多一条
            // `Begin` 可以数，现在它整个不再落盘）。
            let mut wal = Wal::open(wal_path(&path)).expect("打开 WAL 读记录");
            let records = wal.read_all().expect("读 WAL");
            let writes_for = |prefix: &[u8]| {
                records
                    .iter()
                    .filter(|e| matches!(&e.record, WalRecord::Write { key, .. }
                        if key.starts_with(prefix)))
                    .count()
            };
            assert_eq!(writes_for(&big_key), 0, "拒收的一行不许在 WAL 里留下 Write 记录");
            // 对照：同一把尺子量得到"真写进去的那行"，否则上面那条断言是空转
            assert_eq!(writes_for(b"small"), 1, "探针必须能数到一条真实写入");
            // ⚠️ M18e 时这里断的是 `begins == 1`（"拒收的那次连 `Begin` 都不留"，用来钉
            // `row_fits` 必须在落 WAL **之前**）。M25 起写侧一个字 `Begin` 都不落，那条断言
            // 会退化成"常数等于 0"而看不见顺序 —— 顺序现在由上面那两条 `Write` 计数守着
            // （带键的记录只有 `Write` 一种，`row_fits` 一旦挪到落 WAL 之后，`writes_for(big_key)`
            // 立刻变 1）。下面这条只守新不变量："写侧不再产生 `Begin`"。
            let begins = records
                .iter()
                .filter(|e| matches!(&e.record, WalRecord::Begin { .. }))
                .count();
            assert_eq!(begins, 0, "写侧不再落 Begin，实际留下 {begins} 条");
            drop(mvcc);
        }
        // 重开：小行还在，大行不在，恢复流程也没被那条不存在的记录卡住
        let mvcc = MVCC::open(&path, 64, 512);
        let tx = mvcc.begin_transaction();
        assert!(tx.get(b"small").is_some(), "拒收一行不许牵连已有的行");
        assert!(tx.get(&big_key).is_none());
        tx.commit();
        drop(mvcc);
        let _ = std::fs::remove_dir_all(&std::path::PathBuf::from(&path).parent().unwrap());
    }

    /// 0.057 页/行、1.7 µs/行（同一份 20000 行的库）。
    /// 断言留了大余量（每 10 行不许超过 1 次页访问）：旧实现会红 30 倍以上。
    #[test]
    fn test_full_scan_pages_scale_with_leaves_not_with_keys_times_height() {
        const KEYS: u64 = 2000;
        let path = tmp_db("scan_pages");
        {
            let mvcc = MVCC::open(&path, 64, 1024);
            for i in 0..KEYS {
                let tx = mvcc.begin_transaction();
                assert!(tx.set(format!("k{i:05}").as_bytes(), format!("v{i}").into_bytes()));
                tx.commit();
            }

            let tx = mvcc.begin_transaction();
            let before = crate::bplus_tree::page::page_reads();
            let rows = tx.scan(None, None);
            let pages = crate::bplus_tree::page::page_reads() - before;
            assert_eq!(rows.len() as u64, KEYS);
            eprintln!("一次全量 scan：{KEYS} 行用了 {pages} 次页访问");
            assert!(
                pages * 10 <= KEYS,
                "{pages} 次页访问对 {KEYS} 行来说太多：折叠不该按行再降一次树（旧实现 3 页/行）"
            );
        }
        cleanup(&path);
    }

    /// 只读走查**不许把点读的工作集挤出去**。
    ///
    /// 归因实测：20000 行的库有 1257 页，而页缓存上限 1024 —— 一次 `health()` 走查
    /// 换出 1600 页、顺带 flush 765 个脏页，150 ms；紧接着的第二遍命中数只有 1。
    /// 走查对每片叶子只碰一次，回填它不会有下一次命中，却会把真正在用的热页逐出。
    /// 现在走查走 `read_page_for_walk`：满了就不回填 ⇒ 换出次数必须是 0（与库大小无关）。
    #[test]
    fn test_full_walk_does_not_evict_the_working_set() {
        const KEYS: u64 = 400;
        const CACHE_PAGES: usize = 8; // 故意远小于页数：让"回填就会换出"成立
        let path = tmp_db("walk_no_evict");
        {
            let mvcc = MVCC::open(&path, 16, CACHE_PAGES);
            for i in 0..KEYS {
                let tx = mvcc.begin_transaction();
                assert!(tx.set(format!("k{i:05}").as_bytes(), format!("v{i}").into_bytes()));
                tx.commit();
            }
            mvcc.flush();

            // 预热：点读一遍，把能装的页装进缓存（此后缓存是满的）
            {
                let tx = mvcc.begin_transaction();
                for i in 0..KEYS {
                    assert!(tx.get(&format!("k{i:05}").into_bytes()).is_some());
                }
            }
            let evict0 = crate::bplus_tree::page::page_evictions();
            let read0 = crate::bplus_tree::page::page_reads();

            {
                let tx = mvcc.begin_transaction();
                let rows = tx.scan(None, None);
                assert_eq!(rows.len() as u64, KEYS, "全量 scan 应折出全部键");
            }
            let evicted = crate::bplus_tree::page::page_evictions() - evict0;
            let walked = crate::bplus_tree::page::page_reads() - read0;
            eprintln!("走查 {walked} 次页访问，换出 {evicted} 页（缓存上限 {CACHE_PAGES} 页）");
            // 合同是"与树高同阶"，不是"绝对 0"：起点下降到那片叶子要读 root/内部页，
            // 那几个导航页未命中时会回填（实测 2 次），这是常数级、与库大小无关。
            // 真正的病灶是"每走一片叶子换出一批"——旧实现 53 次页访问换出 50 页。
            assert!(
                evicted * 8 <= walked,
                "换出 {evicted} 页对 {walked} 次页访问太多：走查在逐页挤掉工作集"
            );
            assert!(
                evicted as usize <= CACHE_PAGES,
                "换出必须停在树高量级（实测 {evicted}，缓存上限 {CACHE_PAGES}）"
            );
        }
        cleanup(&path);
    }

    /// M22 守卫①：**页缓存的自动档要跟着库长大**，长大的那笔钱还得说清楚。
    ///
    /// 病灶（2026-10-17 的 200k 基线）：`max_cache_pages` 以前只有一个写死的 1024。
    /// 归因台架量的就是这件事 —— 同一个 1024 页：
    /// - 20k 行 = 1210 页 ⇒ 覆盖率 84.6% ⇒ 随机点读 **11.9 µs/次**
    /// - 200k 行 = 12129 页 ⇒ 覆盖率 8.4% ⇒ 随机点读 **45.2 µs/次**
    /// 一个常数在两个规模上是两种东西，所以它不该是常数。自动档 = 按库的页数给容量，
    /// 夹在 `[1024, 2048]`：**下限就是要改之前那个写死值**（换成自动档不许让任何人的工作集变小），
    /// 上限是"最多替你多占 8 MiB"（一页 4 KB）—— 库再大要更全的覆盖必须由调用方点名，
    /// 因为那时缓存整个库的常驻 ≈ 数据文件本身（200k 行实测 47 MB），那已经不是缓存而是复制。
    ///
    /// 尺子是 [`MVCC::cache_policy`]（生效容量 / 钉住的页数 / 是否自动档），不是墙钟。
    /// ⚠️ 这份库是**用长键撑页数**的（一把键 ≈ 3.3 KB ⇒ 一页只装一条）：越过 1024 页那条下限
    /// 只要一千多行。用大值撑不动页数 —— bplus 的 MVCC 把大值走 blob 旁路，叶子只留指针
    /// （实测 2600 行 × 2 KB 值 = 85 页，压根没长起来）。
    #[test]
    fn test_cache_capacity_follows_the_library() {
        const ROWS: usize = 1300;
        let path = tmp_db("cache_auto");
        {
            let mvcc = MVCC::open(&path, 64, 0); // 0 = 自动档
            let (cap0, _, auto) = mvcc.cache_policy();
            assert!(auto, "传 0 就该进自动档");
            assert_eq!(
                cap0, 1024,
                "空库的自动档必须停在旧默认值（实际 {cap0}）—— 换成自动档不许让谁的工作集变小"
            );

            let pad = vec![b'k'; 3300];
            for i in 0..ROWS {
                let mut key = format!("k{i:06}_").into_bytes();
                key.extend_from_slice(&pad);
                let tx = mvcc.begin_transaction();
                assert!(tx.set(&key, b"v".to_vec()), "灌第 {i} 行");
                tx.commit();
            }
            // 重新评估就挂在刷盘这条路径上（checkpoint / bulk 收尾都走它）
            mvcc.flush();

            let total = mvcc.kv.lock().unwrap().next_page_id();
            let (depth, internal, leaves, _) = {
                let mut kv = mvcc.kv.lock().unwrap();
                kv.shape()
            };
            let (cap, pinned, _) = mvcc.cache_policy();
            eprintln!(
                "[自动档] 行={ROWS} 总页={total} 深={depth} 内部={internal} 叶子={leaves} \
                 ⇒ 生效容量={cap} 钉住={pinned}"
            );
            assert!(
                total > 1024,
                "这份库得有 1024 页以上才测得出'库长过了旧默认值'（实际 {total} 页）"
            );
            assert!(
                cap > 1024,
                "刷过盘之后容量还停在 1024 ⇒ 自动档没跟着库长大（就是这次要修的那个洞）"
            );
            assert_eq!(
                cap,
                (total as usize).min(2048),
                "容量应当正好贴着页数（上限 2048 之内），实际 {cap} / 页数 {total}"
            );
            assert!(
                pinned > 0 && (pinned as u64) <= internal as u64,
                "上层页要钉住：实测 {pinned} 页，内部节点共 {internal} 页"
            );
            // 底线：钉住的那部分**不许把叶子的工作集吃光**（长键库的上层能占到全库 33%）
            assert!(
                pinned * 2 <= cap,
                "钉住 {pinned} 页 / 容量 {cap} 页 ⇒ 叶子那边连一半都不到，等于缓存没在用"
            );
        }
        // 显式传数不许被自动档吃掉
        {
            let mvcc = MVCC::open(&path, 64, 512);
            let (cap, _, auto) = mvcc.cache_policy();
            assert!(!auto, "显式传了数就不该是自动档");
            assert_eq!(cap, 512, "显式容量被自动档覆盖了：生效 {cap}");
        }
        cleanup(&path);
    }

    /// M22 守卫②：**根与内部节点不参与换出** —— 判据是"重读同一把键，只许缺自己那一片叶子"。
    ///
    /// 病灶（2026-10-17 的 200k 归因台架）：走查之前一次随机点读 45.2 µs、走查+批量写之后
    /// **58.6 µs**，而把容量开到装得下全库是 8.5 µs —— 差的是"下降路上那几层又被读了一遍、
    /// 又被重解析了一遍"。根与内部节点是每次点读都要用的，却和叶子排在同一个 LRU 里一起被挤。
    ///
    /// ⚠️ 两个当时想错的地方，都以实测为准改在这里：
    /// - **"上层只占全库 3%"不是常数**。同一份五千行的库，35 B 键时上层 36/1210 页 = 3.0%，
    ///   3.3 KB 键时上层 1305/2606 页 = **50%**（内部节点也装不下几个长键 ⇒ 扇出掉到 2、树深涨到 12）。
    ///   所以钉住预算必须**跟容量挂钩**（≤ 容量 ÷ 2），只钉一个绝对页数会在长键库上把整个缓存钉死。
    /// - 真正服务下降的是**解析缓存**（`node_of`），它不受页换出影响 ⇒ 钉住对**聚合缺页**的影响
    ///   比预想的小（实测同一形状：钉住 1.46 页/读、不钉 1.75 页/读）。所以这条守卫同时断三件事，
    ///   缺一件就有一种退化溜过去：
    ///   ① 机制：`pinned > 0` 且 `pinned ≤ 内部节点数`、`pinned × 2 ≤ 容量`（撤掉钉住 ⇒ 第一条就红）
    ///   ② 聚合代价：缺页数 ≤ 1.6 × 读次数（实测不钉是 1.75 ⇒ 红；余量 12%，计数是确定性的）
    ///   ③ 绝对地板：**冲掉工作集之后单独重读一把早就读过的键**，最多缺 2 页（叶子那一片 + 边角）
    #[test]
    fn test_upper_levels_survive_a_full_walk() {
        const ROWS: u64 = 1200;
        const CACHE_PAGES: usize = 16;
        const THRASH: usize = 300;
        let path = tmp_db("pin_upper");
        {
            let mvcc = MVCC::open(&path, 16, CACHE_PAGES);
            for i in 0..ROWS {
                let tx = mvcc.begin_transaction();
                assert!(tx.set(format!("k{i:06}").as_bytes(), format!("v{i}").into_bytes()));
                tx.commit();
            }
            mvcc.flush();
            let (depth, internal, leaves, total) = {
                let mut kv = mvcc.kv.lock().unwrap();
                kv.shape()
            };
            eprintln!("[形状] 行={ROWS} 总页={total} 深={depth} 内部={internal} 叶子={leaves}");
            assert!(depth >= 3, "这份库得有 {depth} 层才测得出下降路上被挤掉几页");
            assert!(
                internal > 0 && leaves > CACHE_PAGES,
                "叶子得比容量多才测得出换出"
            );

            // 先把上层钉住：下降一遍就够（第一趟本来就要读它们）
            const PROBE: u64 = 0;
            {
                let tx = mvcc.begin_transaction();
                for i in 0..8u64 {
                    assert!(tx.get(&format!("k{i:06}").into_bytes()).is_some(), "预热读 {i}");
                }
            }
            let (cap, pinned, _) = mvcc.cache_policy();
            assert!(
                pinned > 0 && pinned <= internal,
                "预热后应钉住若干上层页（实测 {pinned}，内部节点共 {internal}）"
            );
            assert!(
                pinned * 2 <= cap,
                "钉住 {pinned} 页超过容量 {cap} 的一半 ⇒ 叶子那边一页都装不下，比不钉还糟"
            );

            // 把 LRU 转好几圈：全库走查 + 一堆乱序点读（都不该动到钉住的页）
            {
                let tx = mvcc.begin_transaction();
                let rows = tx.scan(None, None);
                assert_eq!(rows.len() as u64, ROWS, "全量 scan 要折出所有键");
            }
            let tm0 = crate::bplus_tree::page::cache_misses();
            {
                let tx = mvcc.begin_transaction();
                let mut idx: Vec<u64> = (1..ROWS).collect();
                let mut s = 0x853c_49e6_77b9_5b3eu64;
                for i in (1..idx.len()).rev() {
                    s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                    let j = (s >> 33) as usize % (i + 1);
                    idx.swap(i, j);
                }
                for &i in idx.iter().take(THRASH) {
                    assert!(tx.get(&format!("k{i:06}").into_bytes()).is_some(), "冲击读 {i}");
                }
            }
            let tmisses = crate::bplus_tree::page::cache_misses() - tm0;
            eprintln!(
                "[钉上层 聚合] {THRASH} 次乱序点读缺页 {tmisses}（{:.2} 页/读；实测钉住 1.46、不钉 1.75，树深 {depth}）",
                tmisses as f64 / THRASH as f64
            );
            // ② 聚合缺页 ≤ 1.6 页/读。计数是确定性的（同一个 LCG、同一份形状），
            // 所以这条界能真的把两种形状分开；余量 12%，不是拍脑袋。
            assert!(
                tmisses * 5 <= THRASH as u64 * 8,
                "乱序点读缺页 {:.2} 页/读 ⇒ 下降路上的层又被读了一遍（上层没钉住，\
                 或解析缓存超限时又整表丢了）",
                tmisses as f64 / THRASH as f64
            );

            // 决定性的一次：**单独**重读那把早就读过的键
            let r0 = crate::bplus_tree::page::page_reads();
            let m0 = crate::bplus_tree::page::cache_misses();
            let p0 = crate::bplus_tree::bplus_tree::node_parses();
            {
                let tx = mvcc.begin_transaction();
                assert!(
                    tx.get(&format!("k{PROBE:06}").into_bytes()).is_some(),
                    "重读探针键读空"
                );
            }
            let reads = crate::bplus_tree::page::page_reads() - r0;
            let misses = crate::bplus_tree::page::cache_misses() - m0;
            let parses = crate::bplus_tree::bplus_tree::node_parses() - p0;
            eprintln!(
                "[钉上层] 冲掉 {THRASH} 次随机点读之后，重读同一把键：页 {reads} / 缺页 {misses} / 重解析 {parses}（树深 {depth}）"
            );
            // 钉住生效 ⇒ 这一次读最多缺"叶子那 1 页"。没钉住 ⇒ 整条下降路都要重来（≈树深）。
            // 余量给到 2：允许叶子的解析结果也早被丢掉（那是叶子该管的事，不是上层的账）。
            assert!(
                misses <= 2,
                "重读一把早就读过的键要缺 {misses} 页（树深 {depth}）⇒ 上层页被换出了，\
                 每次点读都得从盘上把根与内部节点重新拼一遍"
            );
            assert!(
                parses <= 2,
                "重读同一把键要重解析 {parses} 个节点 ⇒ 解析缓存超限时把上层页的结果整表丢了"
            );
        }
        cleanup(&path);
    }

    // ---- B+ 树读路径的墙钟归因（手动 release 跑）----
    //
    // 大库基线里 bplus 点读 33.6 µs/次（bitcask 4.3）、`search 首页` 518 ms、`health()` 110 ms。
    // `page_reads()` 把「缓存命中」和「未命中要读盘 + CRC」算成同一个数，所以它守得住
    // "代价不随链长涨"，却答不出**钱花在哪**。这条基线分开数 hits/misses，并且**同一份数据
    // 连测两遍**：第二遍缓存是热的 ⇒ 两遍之差就是真读盘那部分，剩下的才是每页重解析的成本。
    //
    // M22 起再加两件：
    // - **行数可由 `BPLUS_ROWS` 调**（默认 20000，跑 200000 就是大库那一档）——
    //   覆盖率这件事只能在两个规模之间才看得见，单规模只能看见"贵"。
    // - **打印树的形状**（树高 / 内部节点数 / 叶子数 / 总页数 / 默认缓存的覆盖率）——
    //   上层页是**结构性小**（叶子数 ÷ order）还是跟库一起长，决定的是两件完全不同的事：
    //   前者该常驻（钉住，代价与库大小无关），后者才谈得上容量与覆盖率。
    //
    // 跑法：
    //   BPLUS_ROWS=200000 cargo test --release --lib -- --ignored --nocapture bplus_read_cost_attribution
    #[test]
    #[ignore = "墙钟归因，手动 release 跑：BPLUS_ROWS=20000 cargo test --release --lib -- --ignored --nocapture bplus_read_cost_attribution"]
    fn test_bplus_read_cost_attribution() {
        use std::time::Instant;

        // `N` 沿用这条台架里其余地方的写法（其它台架都是 `const N`），所以要压掉命名警告
        #[allow(non_snake_case)]
        let N: usize = std::env::var("BPLUS_ROWS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(20000);
        const CACHE_PAGES: usize = 1024;
        fn key(i: usize) -> Vec<u8> {
            format!("{i:08}#/worldquant/datafields/x.json").into_bytes()
        }
        fn val(i: usize) -> Vec<u8> {
            format!("{{\"i\":{i},\"h\":\"{:0<28}\",\"v\":1}}", i * 7).into_bytes()
        }

        let mut order: Vec<usize> = (0..N).collect();
        let mut s = 0x853c_49e6_77b9_5b3eu64;
        for i in (1..N).rev() {
            s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let j = (s >> 33) as usize % (i + 1);
            order.swap(i, j);
        }

        let path = tmp_db("attrib");
        {
            let mvcc = MVCC::open(&path, 64, CACHE_PAGES);
            for base in (0..N).step_by(1000) {
                let end = (base + 1000).min(N);
                let items: Vec<(Vec<u8>, Vec<u8>)> =
                    (base..end).map(|i| (key(i), val(i))).collect();
                let tx = mvcc.begin_transaction();
                if let Err(bad) = tx.batch_set(&items) {
                    panic!("batch_set 冲突: {}", String::from_utf8_lossy(&bad));
                }
                tx.commit();
            }
            mvcc.flush();
            let h = mvcc.health();
            eprintln!("形状: rows={} live_keys={} max_chain={}", h.total_rows, h.live_keys, h.max_chain);

            // 树形：树高 / 内部节点 / 叶子 / 总页数 / 默认缓存的覆盖率
            // ⚠️ 这一趟要摸每一页，所以必须在下面打点**之前**跑完（它自己会灌一堆计数）。
            {
                let mut kv = mvcc.kv.lock().unwrap();
                let (depth, internal, leaves, total) = kv.shape();
                eprintln!(
                    "树形: 高={depth} 内部={internal} 叶子={leaves} 总页={total} \
                     ⇒ 上层占全库 {:.2}%，默认缓存 {CACHE_PAGES} 页 = 覆盖率 {:.1}%",
                    internal as f64 * 100.0 / total.max(1) as f64,
                    CACHE_PAGES as f64 * 100.0 / total.max(1) as f64
                );
                assert!(internal > 0 && leaves > 0, "树形探针没数到东西");
            }

            // 冷重开：缓存清空，模拟"进程启动后第一次读"
            drop(mvcc);
            let mvcc = MVCC::open(&path, 64, CACHE_PAGES);

            let probe = |label: &str, mvcc: &MVCC| {
                let tx = mvcc.begin_transaction();
                let (p0, hit0, miss0) = (
                    crate::bplus_tree::page::page_reads(),
                    crate::bplus_tree::page::cache_hits(),
                    crate::bplus_tree::page::cache_misses(),
                );
                let parse0 = crate::bplus_tree::bplus_tree::node_parses();
                let t = Instant::now();
                for &i in &order {
                    assert!(tx.get(&key(i)).is_some(), "第 {i} 个键读空");
                }
                let d = t.elapsed().as_secs_f64() * 1e6;
                let (dp, dh, dm, dn) = (
                    crate::bplus_tree::page::page_reads() - p0,
                    crate::bplus_tree::page::cache_hits() - hit0,
                    crate::bplus_tree::page::cache_misses() - miss0,
                    crate::bplus_tree::bplus_tree::node_parses() - parse0,
                );
                eprintln!(
                    "{label:<16} {:>7.1} µs/次 | 页/次 {:>4.2} | 解析/次 {:>5.2} | 命中 {:>7} | 未命中 {:>6}",
                    d / N as f64,
                    dp as f64 / N as f64,
                    dn as f64 / N as f64,
                    dh,
                    dm
                );
                (dp, dn)
            };

            eprintln!("--- 点读：同一份数据连测两遍（第二遍缓存是热的）---");
            let (_, cold_parses) = probe("第一遍(冷)", &mvcc);
            let (warm_pages, warm_parses) = probe("第二遍(热)", &mvcc);
            let _ = warm_pages; // 页访问随两遍的缓存清除时机浮动，只对绝对量做断言
            assert!(
                cold_parses > 0,
                "解析计数没在数东西 —— 那下面那条断言就是空转（探针自己也要自证会红）"
            );
            // 合同按**绝对量**写，不跟"第一遍 vs 第二遍"比（两遍谁冷谁热取决于执行顺序，
            // 相对断言会假红）。一次点读走 4 层 ⇒ 没有解析缓存时每层解析一次 ≈ 4N 次；
            // 生效之后热态必须压到 ≤1.5N（实测 0.48/次 = 9.6k）。残余的那点是
            // "这页本遍第一次见"的必付成本 —— 1257 页 > max_cache_pages 1024，
            // 工作集本来就装不下，剩下要收得靠"容量可配"（#17）。
            assert!(
                warm_parses * 2 <= (N as u64) * 3,
                "点读还在按层重解析：{N} 次读解析了 {warm_parses} 次（无缓存基线约 4N = {}）",
                4 * N as u64
            );

            // 五个计数一起快照：命中/未命中只说"有没有读盘"，换出与批量写盘才说得出
            // "走一遍全库被顺带 flush 了多少脏页"（脏页换出要写双写缓冲 + sync）。
            type Snap = (u64, u64, u64, u64, u64, u64);
            let snap = || -> Snap {
                (
                    crate::bplus_tree::page::page_reads(),
                    crate::bplus_tree::page::cache_hits(),
                    crate::bplus_tree::page::cache_misses(),
                    crate::bplus_tree::page::page_evictions(),
                    crate::bplus_tree::page::batch_flushed_pages(),
                    crate::bplus_tree::page::lru_compactions(),
                )
            };
            let report = |label: &str, per: f64, unit: &str, a: Snap, b: Snap| {
                eprintln!(
                    "{label:<16} {per:>9.1} {unit:<7} | 页 {:>6} 命中 {:>6} 未命中 {:>5} 换出 {:>5} 批量写盘 {:>5} LRU压缩 {:>4}",
                    b.0 - a.0,
                    b.1 - a.1,
                    b.2 - a.2,
                    b.3 - a.3,
                    b.4 - a.4,
                    b.5 - a.5
                );
            };

            // 一次全量 scan
            let tx = mvcc.begin_transaction();
            let a = snap();
            let t = Instant::now();
            let rows = tx.scan(None, None);
            let d = t.elapsed().as_secs_f64() * 1e6;
            report("一次全量 scan", d / rows.len().max(1) as f64, "µs/行", a, snap());
            drop(tx);
            drop(rows);

            // ⚠️ 复现台架的顺序：health() 之前有一轮"更新 2000 键"，那会把页弄脏。
            //    复测表里 health() 从 110 ms 涨到 251 ms，这一组就是去分"每页更贵"还是"页更多"。
            let a = snap();
            let t = Instant::now();
            for i in 0..2000usize {
                let tx = mvcc.begin_transaction();
                assert!(tx.set(&key(i), val(i + N)), "更新第 {i} 个键失败");
                tx.commit();
            }
            report(
                "更新 2000 键",
                t.elapsed().as_secs_f64() * 1e6 / 2000.0,
                "µs/次",
                a,
                snap(),
            );

            let a = snap();
            let t = Instant::now();
            let _ = mvcc.health();
            report("health() 紧跟", t.elapsed().as_secs_f64() * 1e6, "µs/次", a, snap());

            // 再量一遍：此时脏页已经flush过，若这一遍明显快 ⇒ 上一遍付的是flush脏页/换出的钱
            let a = snap();
            let t = Instant::now();
            let _ = mvcc.health();
            report("health() 第二遍", t.elapsed().as_secs_f64() * 1e6, "µs/次", a, snap());

            let q = crate::common::SearchQuery::contains(b"", 0, 100);
            let tx = mvcc.begin_transaction();
            let a = snap();
            let t = Instant::now();
            let _ = tx.search_keys(&q);
            report("search 首页", t.elapsed().as_secs_f64() * 1e6, "µs/次", a, snap());
            drop(tx);

            // 决定性的一条：走查之后点读的工作集还在不在。
            // 修复前 —— 上面那几趟全库走查已经把缓存换出几千页，这一遍几乎全未命中；
            // 修复后 —— 走查不回填，点读留下的热页应该原封不动。
            eprintln!("--- 走查之后的点读（工作集是否被毁）---");
            probe("点读(走查后)", &mvcc);

            // 页缓存开大到能装下全库，看未命中是否归零（这决定"贵"是 IO 还是 CPU）
            drop(mvcc);
            let mvcc = MVCC::open(&path, 64, 65536);
            eprintln!("--- max_cache_pages=65536 ---");
            probe("点读(大缓存)", &mvcc);
            drop(mvcc);
        }
        cleanup(&path);
    }
}

impl crate::common::VersionedStore for Transaction {
    /// 
    type Row = Option<Vec<u8>>;

    fn scan_candidates(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        accept: Option<&dyn Fn(&[u8]) -> bool>,
        after: Option<&[u8]>,
        budget: usize,
    ) -> (Vec<crate::common::WinCandidate<Option<Vec<u8>>>>, bool) {
        // 走本类型上的同名 inherent 方法（inherent 优先于 trait ⇒ 不会自我递归）
        self.scan_candidates(start, end, accept, after, budget)
    }

    fn materialize(&self, row: Option<Vec<u8>>) -> crate::common::RowValue {
        self.read_row(&row)
    }
}
