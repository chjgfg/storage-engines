//! MVCC 事务层 + WAL 崩溃恢复（适配 Bitcask 后端）
//!
//! 从 `bplus-tree` / `lsm-tree` 移植：协议与语义保持一致，存储后端换成 [`Bitcask`]。
//!
//! 提交协议（简化版 ARIES）：
//! 1. 写路径：先 append WAL Write，再写 Bitcask 日志
//! 2. commit：append WAL Commit → **fsync WAL**（持久化点）→ 可选 fsync 数据文件
//! 3. 崩溃恢复：重放已提交 Write，撤销未提交 Write，再 checkpoint 截断 WAL
//!
//! **Bulk load 模式**（[`MVCC::begin_bulk`]）：
//! - 关闭 WAL / 冲突检测 / 活跃事务表
//! - 结束时 [`BulkLoader::finish`] → flush + checkpoint
//!
//! 与 bplus 的差异：
//! - 大 value（> [`crate::storage::BLOB_THRESHOLD`]）外置到 `data.blob`，主日志只存指针
//! - Checkpoint 的 `root_page_id` / `next_page_id` 固定写 0
//! - 数据目录：`dir/data.log` + `dir/data.blob` + `dir/bitcask.wal` + `dir/bitcask.lock`
//! - 版本行 tombstone 用 Bitcask **软 tombstone**（KeyDir 保留）；rollback/vacuum 用 **硬删除**
//! - bulk 路径：无 WAL + 4MB 写缓冲 + insert_batch，目标 10M / 10min

use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use crate::bitcask::{Bitcask, storage::{
    BlobStore, FileLock, VAL_TAG_BLOB, data_path, decode_stored_value, encode_stored_value, wal_path,
}};
use crate::bitcask::wal::{analyze_recovery, Wal, WalRecord};
pub use crate::common::{ExportRecord, MvccHealth, VacuumRound, VacuumStats};

/// MVCC 存储引擎（Bitcask 后端）
pub struct MVCC {
    kv: Arc<Mutex<Bitcask>>,
    blob: Arc<Mutex<BlobStore>>,
    wal: Arc<Mutex<Wal>>,
    active_txn: Arc<Mutex<HashMap<u64, Vec<Vec<u8>>>>>,
    next_version: Arc<AtomicU64>,
    /// WAL 增长到多少字节就在下一次提交后自动 checkpoint（0 = 关闭自动触发）
    wal_limit: Arc<AtomicU64>,
    /// 自动回收：切片宽度（行数），同时是「攒多少行新版本才触发一轮」。0 = 关闭
    vac_slice: Arc<AtomicU64>,
    /// 自动回收：自上一轮以来新写入的版本行数
    vac_pending: Arc<AtomicU64>,
    /// 自动回收：下一片从哪个逻辑 key 之后继续（`None` = 从头）
    vac_cursor: Arc<Mutex<Option<Vec<u8>>>>,
    db_dir: PathBuf,
    /// 持有期间保证单写者
    _lock: FileLock,
}

/// 普通 MVCC 事务
pub struct Transaction {
    pub(crate) kv: Arc<Mutex<Bitcask>>,
    pub(crate) blob: Arc<Mutex<BlobStore>>,
    wal: Arc<Mutex<Wal>>,
    pub(crate) active_txn: Arc<Mutex<HashMap<u64, Vec<Vec<u8>>>>>,
    next_version: Arc<AtomicU64>,
    wal_limit: Arc<AtomicU64>,
    /// 提交时把本事务写的版本行数记到自动回收的账上
    vac_pending: Arc<AtomicU64>,
    version: u64,
    active_xid: HashSet<u64>,
}

/// 批量导入会话：无 WAL、无冲突检测，单写者假定
pub struct BulkLoader {
    kv: Arc<Mutex<Bitcask>>,
    blob: Arc<Mutex<BlobStore>>,
    wal: Arc<Mutex<Wal>>,
    next_version: Arc<AtomicU64>,
    version: u64,
    finished: bool,
}

/// 逻辑键 + 版本（调试用）
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VersionedKey {
    pub raw_key: Vec<u8>,
    pub version: u64,
}

/// 编码：`raw_key || version.to_be_bytes()`
pub fn encode_key(raw_key: &[u8], version: u64) -> Vec<u8> {
    let mut enc = Vec::with_capacity(raw_key.len() + 8);
    enc.extend_from_slice(raw_key);
    enc.extend_from_slice(&version.to_be_bytes());
    enc
}

/// 解码
pub fn decode_key(enc: &[u8]) -> Option<VersionedKey> {
    if enc.len() < 8 {
        return None;
    }
    let split = enc.len() - 8;
    let mut ver_bytes = [0u8; 8];
    ver_bytes.copy_from_slice(&enc[split..]);
    Some(VersionedKey {
        raw_key: enc[..split].to_vec(),
        version: u64::from_be_bytes(ver_bytes),
    })
}

/// 链尾反向查找的首轮预算，不够再翻倍重试。
///
/// 常态下一个逻辑 key 只有个位数版本，32 足够一次命中；翻倍只在
/// 「整条链没有一个可见」的退化场景才付到 O(链长)，与旧的整链扫描同阶。
const VERSION_BACK_STEP: usize = 32;

// 测试专用：本线程调用 `scan_max_version`（O(总行数) 的全库 KeyDir 扫描）的次数。
// 用来守住冷启动的三级优先：有 Checkpoint 时就不该再全库扫。
#[cfg(test)]
thread_local! {
    static SCAN_MAX_CALLS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// 当前线程发生过几次「全库扫求最大版本」（测试探针用）
#[cfg(test)]
pub(crate) fn scan_max_calls() -> u64 {
    SCAN_MAX_CALLS.with(|c| c.get())
}

/// 把 WAL 收口成「只剩一条 Checkpoint」。
///
/// 顺序反直觉但必须这样：原先是 `append(Checkpoint) → sync → truncate()`，而 `truncate()`
/// 只留 header —— **刚写的 Checkpoint 被自己抹掉了**，于是下次 open 无从知道 `next_version`，
/// 只能退回 O(总行数) 的全库 KeyDir 重扫。改成先截断、再把 Checkpoint 写成新 WAL 的第一条。
///
/// 崩在截断之后、写入之前也不影响正确性：那时 WAL 为空，open 走全库扫拿起点，只是慢。
fn seal_wal_checkpoint(wal: &mut Wal, next_version: u64) -> std::io::Result<()> {
    wal.truncate()?;
    wal.append(WalRecord::Checkpoint {
        next_version,
        root_page_id: 0,
        next_page_id: 0,
    })?;
    wal.sync()
}

/// checkpoint 的实体：手动 `MVCC::checkpoint()` 与提交时的 WAL 阈值触发共用同一份实现。
///
/// 顺序不能换：`Commit` 记录落盘 = 事务持久，数据页可以之后慢慢刷；但 WAL 一旦被截断，
/// 未落盘的数据就再无重放来源 —— 所以先 flush 数据与 blob，最后才收口 WAL。
/// 顺手 `dump_hint` 是为了下次 open 不必全量扫日志。
fn run_checkpoint(
    kv: &Mutex<Bitcask>,
    blob: &Mutex<BlobStore>,
    wal: &Mutex<Wal>,
    next_version: &AtomicU64,
) {
    let mut kv = kv.lock().unwrap();
    let mut blob = blob.lock().unwrap();
    let mut wal = wal.lock().unwrap();
    let _ = kv.flush();
    let _ = blob.sync();
    let _ = kv.dump_hint();
    let next_ver = next_version.load(Ordering::SeqCst);
    let _ = seal_wal_checkpoint(&mut wal, next_ver);
}

impl MVCC {
    /// 全库健康度快照。**只走 KeyDir 元数据**（`iter_meta`），不读 value、不碰 blob。
    ///
    /// 代价是 O(物理行数) 的内存遍历，属管理面调用：别放进热路径，长跑里按分钟级采样就够。
    pub fn health(&self) -> MvccHealth {
        // 一次只持一把锁：`active_txn` 读完就放，再拿 `kv`。
        // 嵌套拿锁会和 [`MVCC::vacuum`]（`active_txn → kv → blob`）构成反向嵌套。
        let active_txns = self.active_txn.lock().unwrap().len() as u64;
        let kv = self.kv.lock().unwrap();
        let mut h = MvccHealth { active_txns, ..Default::default() };
        let mut prev_raw: Option<Vec<u8>> = None;
        let mut cur_chain = 0u64;
        // KeyDir 按 enc 升序 ⇒ 同一逻辑 key 的版本块相邻 ⇒ 顺序分组，一遍就够
        for (enc, _loc) in kv.iter_meta() {
            let Some(vk) = decode_key(enc) else {
                continue;
            };
            h.total_rows += 1;
            if prev_raw.as_ref() == Some(&vk.raw_key) {
                cur_chain += 1;
            } else {
                h.max_chain = h.max_chain.max(cur_chain);
                h.live_keys += 1;
                cur_chain = 1;
                prev_raw = Some(vk.raw_key);
            }
        }
        h.max_chain = h.max_chain.max(cur_chain);
        h.rows_above_one = h.total_rows.saturating_sub(h.live_keys);
        h
    }

    /// 打开/创建磁盘上的 MVCC 引擎（含 WAL 恢复）。失败时 panic（兼容旧代码）。
    pub fn open(dir: impl AsRef<Path>) -> Self {
        Self::try_open(dir).map_err(|e| format!("MVCC::open 失败: {e}")).unwrap()
    }

    /// 打开/创建，错误以 `io::Result` 返回（锁冲突、坏文件、IO 等）。
    pub fn try_open(dir: impl AsRef<Path>) -> std::io::Result<Self> {
        let db_dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&db_dir)?;

        let lock = FileLock::try_acquire(&db_dir)?;
        let mut tree = Bitcask::new(data_path(&db_dir))?;
        let blob = BlobStore::open(&db_dir)?;
        let mut wal = Wal::open(wal_path(&db_dir))?;

        let entries = wal.read_all()?;
        let plan = analyze_recovery(&entries);

        for (_xid, key, value) in &plan.committed_writes {
            tree.insert(key.clone(), value.clone())?;
        }
        for (_xid, key) in &plan.uncommitted_writes {
            let _ = tree.remove(key);
        }

        // 冷启动的起始版本号，三级优先（照 bplus 的口径，别让同一个优化在三处有三种成熟度）：
        // 1. WAL 里最后一个 Checkpoint 的 next_version —— checkpoint 先 flush 数据再截断 WAL，
        //    所以它之前的版本行都已落盘、编号必然小于它，它之后的记录才是新增写入；
        // 2. 没有 Checkpoint 就用 WAL 见过的最大 xid；
        // 3. 两者都没有（首次打开/老库）才退化到全库扫 KeyDir 求最大版本。
        //
        // 走 1 才让「库越大 open 越慢」这条通道断开：`scan_max_version` 是 O(总行数)。
        // 注意这里省的是**启动开销**而不是正确性 —— 即便退化到 3，扫出来的最大值也不会
        // 小于某个仍存活的行，所以不会发生版本号复用。
        let ckpt_nv = match plan.last_checkpoint.as_ref() {
            Some(WalRecord::Checkpoint { next_version, .. }) => Some(*next_version),
            _ => None,
        };
        let start_ver = match ckpt_nv {
            Some(nv) => nv.max(plan.max_xid.saturating_add(1)).max(1),
            None => {
                let disk_max = scan_max_version(&mut tree)?;
                disk_max.max(plan.max_xid).saturating_add(1).max(1)
            }
        };

        tree.flush()?;
        // 同样要「先截断再写 Checkpoint」，否则这次 open 留下的起点信息会被截断抹掉，
        // 下次 open 只能再全库扫一遍（首次打开的库尤其需要这条快路）。
        seal_wal_checkpoint(&mut wal, start_ver)?;

        Ok(Self {
            kv: Arc::new(Mutex::new(tree)),
            blob: Arc::new(Mutex::new(blob)),
            wal: Arc::new(Mutex::new(wal)),
            active_txn: Arc::new(Mutex::new(HashMap::new())),
            next_version: Arc::new(AtomicU64::new(start_ver)),
            wal_limit: Arc::new(AtomicU64::new(MVCC::WAL_CHECKPOINT_BYTES_DEFAULT)),
            vac_slice: Arc::new(AtomicU64::new(MVCC::VACUUM_AUTO_SLICE_DEFAULT)),
            vac_pending: Arc::new(AtomicU64::new(0)),
            vac_cursor: Arc::new(Mutex::new(None)),
            db_dir,
            _lock: lock,
        })
    }

    /// 在系统临时目录创建匿名库（测试用）
    pub fn new() -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "bitcask_mvcc_anon_{}_{}",
            std::process::id(),
            nanos
        ));
        Self::open(path)
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
            self.vac_pending.clone(),
        )
    }

    /// 自动回收的一小步：攒够 `vac_slice` 行新版本，就裁同样宽度的**一片**。
    ///
    /// 挂在 `begin_transaction` 而不是 commit 上：这一处还没有本线程的活跃事务，
    /// `vacuum_bounded` 的「有活跃事务就整轮跳过」不会被自己的调用方踩中；而且每个
    /// 事务至少 begin 一次，攒账的节奏和写路径一样。
    ///
    /// 触发量 = 切片宽度 ⇒ 回收速率与产生垃圾的速率 1:1，这是「跟得上」的下界；
    /// 一片的代价 = 读 `slice` 行元数据（不读 value），锁也就按这么久。
    /// 长尾那一片（库尾之后）会自动从头开始下一遍。
    fn tick_auto_vacuum(&self) {
        let slice = self.vac_slice.load(Ordering::SeqCst);
        if slice == 0 {
            return;
        }
        // 注意不能 swap：tick 每次 begin 都会跑，swap 会把还没攒够的账清零 ⇒ 永远到不了阈值
        let seen = self.vac_pending.load(Ordering::SeqCst);
        if seen < slice {
            return;
        }
        // CAS 失败说明别的线程已经取走这笔账并在裁，本轮让给它（单写者模型下走不到）
        if self
            .vac_pending
            .compare_exchange(seen, 0, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        let after = self.vac_cursor.lock().unwrap().clone();
        if let Ok(round) = self.vacuum_bounded(after.as_deref(), slice) {
            *self.vac_cursor.lock().unwrap() = round.next_cursor;
        }
    }

    /// 自动回收的切片宽度（行数）：每攒够这么多行新版本，就裁一片这么多行。
    ///
    /// 默认 [`MVCC::VACUUM_AUTO_SLICE_DEFAULT`]；**设 0 关闭自动回收**，那时版本行只增不减，
    /// 得靠手动 `vacuum()` —— 也就是回到「热点 key 堆二十万版、点读陪跑整条链」的状态。
    pub fn set_vacuum_auto_slice(&self, rows: u64) {
        self.vac_slice.store(rows, Ordering::SeqCst);
    }

    pub fn vacuum_auto_slice(&self) -> u64 {
        self.vac_slice.load(Ordering::SeqCst)
    }

    /// 默认切片宽度 = 16 Ki 行。
    ///
    /// 依据：一片的代价是读 16 Ki 行元数据（KeyDir 内存遍历，不读 value），单核机器上
    /// 亚毫秒级；而事故现场那条链有 23.8 万行 —— 默认值开着他就会在自己被需要之前
    /// 被裁掉。设这么小是为了「每次只挡一小下」，不是省总开销。
    pub const VACUUM_AUTO_SLICE_DEFAULT: u64 = 16 * 1024;

    /// 开启 bulk load：单写者、无 WAL、无冲突检测。
    ///
    /// **崩溃不安全**：finish 之前进程挂掉可能丢数据。
    /// 导入结束后必须 `finish()`。
    pub fn begin_bulk(&self) -> BulkLoader {
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

    /// 默认 WAL 阈值：16 MiB。
    ///
    /// 现场见过 WAL 长到 55MB 仍无人截断，而恢复路径是**全量扫描** WAL（`analyze_recovery`）
    /// —— 不截断等于启动一次比一次慢，连「重启止血」这个动作本身都在退化。
    pub const WAL_CHECKPOINT_BYTES_DEFAULT: u64 = 16 << 20;

    pub fn checkpoint(&self) {
        run_checkpoint(&self.kv, &self.blob, &self.wal, &self.next_version);
    }

    /// WAL 字节阈值可配置项：增长到这么多字节就在**下一次提交后**自动 checkpoint。
    /// 设 0 关闭自动触发（只能手动调 `checkpoint()`）。
    pub fn set_wal_checkpoint_limit(&self, bytes: u64) {
        self.wal_limit.store(bytes, Ordering::SeqCst);
    }

    pub fn wal_checkpoint_limit(&self) -> u64 {
        self.wal_limit.load(Ordering::SeqCst)
    }

    pub fn flush(&self) {
        let mut kv = self.kv.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();
        let _ = kv.flush();
        let _ = blob.sync();
    }

    /// 压缩数据文件 + 写出 hint
    pub fn merge(&self) -> std::io::Result<()> {
        self.kv.lock().unwrap().merge()
    }

    /// 主动写出 KeyDir hint（不 merge）
    pub fn dump_hint(&self) -> std::io::Result<()> {
        self.kv.lock().unwrap().dump_hint()
    }

    pub fn db_dir(&self) -> &Path {
        &self.db_dir
    }

    /// 导出当前快照下「每个逻辑 key 的最新可见版本」。
    pub fn export_latest_visible(&self, include_deleted: bool) -> Vec<ExportRecord> {
        let tx = self.begin_transaction();
        let records = tx.export_latest_visible(include_deleted);
        tx.commit();
        records
    }

    /// 版本回收：一次扫完全库。
    ///
    /// 就是 `vacuum_bounded(None, u64::MAX)`；保留规则、「有活跃事务就整轮跳过」的理由和
    /// 锁序说明都在 [`MVCC::vacuum_bounded`] 那边。
    pub fn vacuum(&self) -> std::io::Result<VacuumStats> {
        Ok(self.vacuum_bounded(None, u64::MAX)?.stats)
    }

    /// 有界版本回收：从逻辑 key `after`（**不含**）之后开始扫，最多读 `budget_rows` 行物理行。
    ///
    /// 保留规则和 [`MVCC::vacuum`] 完全一致，只是作用范围是一段而不是全库。
    ///
    /// **截断只发生在逻辑 key 边界**：半条链不能收 —— 「留哪一版」的判据要求整组版本都在手上。
    /// 所以一轮实际读的行数可能超出 `budget_rows`，超出量就是那一条链的长度：某把热点 key
    /// 堆了 23.8 万版时，裁它这一下就得一次读 23.8 万行元数据（走 KeyDir，不读 value）。
    /// 这是本设计明确接受的代价。
    ///
    /// blob 回收与日志压缩只在**全库轮次**（`budget_rows == u64::MAX`，也就是手动
    /// [`MVCC::vacuum`]）做：两者都是全库代价。判据不能是「游标回到 None」—— 一把 key 的
    /// 小库任何一片都正好读到库尾，那样自动回收每轮都会重写一遍库。
    ///
    /// 为什么要切片：自动回收要能在单核机器上跑，而 [`MVCC::vacuum`] 会把 kv 锁按到整库
    /// 扫完 —— 那期间点读点写全在排队。切片后每轮只按住一小段。
    ///
    /// 为什么是「整轮跳过」而不是旧的 `xmin = min(活跃事务 id)`：那个水位线**会裁掉
    /// 活跃事务快照仍需要的版本**。T 的可见判据是「v ≤ T.version 且 v ∉ T.active_xid」，
    /// 若「最后一个 < xmin」那一版本身就属于 T 的并发快照，T 得往前再退一版 ——
    /// 而那一版刚被裁掉 ⇒ T 静默读到更旧的值或 None。裁错版本是**静默数据错误**，
    /// 比多等一轮 vacuum 危险得多（守卫见 `test_vacuum_keeps_versions_needed_by_active_txn`）。
    ///
    /// 活跃表这把锁**全程持有**：确认它为空之后不会有新事务抢先拿到快照。
    /// 锁序 `active_txn → kv → blob` 与 [`Transaction::write`] 一致（那边是
    /// active_txn → wal → blob → kv，都在真正动手前拿齐，不会与这里交叉等待）。
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

        // ① 分组只走 KeyDir 元数据：判「留哪一版」只需要版本号 + 是不是软墓碑，不需要 value。
        //    旧实现是两次 `kv.iter()`（每一行连 value 全部读出来），于是 vacuum 自己成了
        //    全库最重的一条路径 —— 在单核机器上「开自动回收」反而会在回收那一刻制造新事故。
        let budget_rows = budget_rows.max(1);
        let mut by_key: BTreeMap<Vec<u8>, Vec<(u64, Vec<u8>, bool)>> = BTreeMap::new();
        let mut scanned = 0u64;
        let mut cur_raw: Option<Vec<u8>> = None;
        let mut truncated = false;
        for (enc, loc) in kv.iter_meta_from(after) {
            let Some(vk) = decode_key(enc) else {
                continue;
            };
            // 下界落在 enc 域，而 `raw_key ≤ after` 的那些行（包括 `after` 自己那一整组）
            // 都还在范围内 —— 显式跳过，游标才是逻辑 key 域的。
            if after.is_some_and(|a| vk.raw_key.as_slice() <= a) {
                continue;
            }
            let at_group_start = cur_raw.as_deref() != Some(vk.raw_key.as_slice());
            if at_group_start && scanned >= budget_rows {
                // 破在这里时 `cur_raw` 那一组已经收齐（刚看到了它的后继）⇒ 游标就是它
                truncated = true;
                break;
            }
            if at_group_start {
                cur_raw = Some(vk.raw_key.clone());
            }
            scanned += 1;
            by_key
                .entry(vk.raw_key.clone())
                .or_default()
                .push((vk.version, enc.clone(), loc.is_soft_tombstone()));
        }
        let next_cursor = if truncated { cur_raw } else { None };

        let mut removed = 0usize;
        let mut to_delete: Vec<Vec<u8>> = Vec::new();
        // 留下来的行：blob GC 只需要读这些，不再走一遍全库
        let mut keep: Vec<Vec<u8>> = Vec::new();

        for (_raw, mut versions) in by_key {
            versions.sort_by_key(|(v, _, _)| *v);
            let mut last_old: Option<usize> = None;
            for (i, (ver, _, _)) in versions.iter().enumerate() {
                if *ver < xmin {
                    last_old = Some(i);
                }
            }
            match last_old {
                // 整条链都在 xmin 之上：全部保留
                None => keep.extend(versions.into_iter().map(|(_, enc, _)| enc)),
                Some(keep_old) => {
                    for i in 0..keep_old {
                        to_delete.push(versions[i].1.clone());
                        removed += 1;
                    }
                    let old_is_tomb = versions[keep_old].2;
                    let has_newer = versions.iter().any(|(v, _, _)| *v >= xmin);
                    if old_is_tomb && !has_newer {
                        // 墓碑的最终回收：否则 tombstone 会永久累积
                        to_delete.push(versions[keep_old].1.clone());
                        removed += 1;
                    } else {
                        keep.push(versions[keep_old].1.clone());
                    }
                    for i in (keep_old + 1)..versions.len() {
                        keep.push(versions[i].1.clone());
                    }
                }
            }
        }

        for enc in &to_delete {
            let _ = kv.remove(enc);
        }
        kv.flush()?;
        blob.sync()?;

        // Blob GC / 日志压缩都是**全库代价**，只在这一次调用本来就是全库扫描时才做。
        // 判据用 `budget_rows` 而不是「游标回到 None」：一个只有一把 key 的小库，任何
        // 有界轮次都会正好读到库尾 —— 用游标判断会让自动回收每轮都重写一遍库。
        // 也就是说：有界轮次只裁行，全库收尾（blob 重写 + merge）归手动 `vacuum()`。
        //
        // Blob GC：
        // - 无任何 blob 引用 → clear
        // - 仍有引用 → rewrite_in_place 只保留存活大 value，并更新 KeyDir 中的指针
        //
        // 只读上面 `keep` 里的行（旧实现这里又走一遍全库 iter()）。
        let full_sweep = budget_rows == u64::MAX;
        let blob_rewritten = if !full_sweep {
            false
        } else {
            let mut blob_keys: Vec<(Vec<u8>, Vec<u8>)> = Vec::new(); // (enc_key, full_user_value)
            for enc in &keep {
                let Ok(Some(stored)) = kv.get_entry(enc) else {
                    continue;
                };
                let Some(stored) = stored else {
                    continue; // 软墓碑没有 value
                };
                if stored.first() != Some(&VAL_TAG_BLOB) {
                    continue;
                }
                match decode_stored_value(&mut blob, &stored) {
                    Ok(Some(user)) => blob_keys.push((enc.clone(), user)),
                    Ok(None) => {}
                    Err(e) => {
                        log::warn!("blob decode 失败 key={enc:?}: {e}");
                    }
                }
            }

            if blob_keys.is_empty() {
                let old_len = blob.path().metadata().map(|m| m.len()).unwrap_or(0);
                if old_len > 8 {
                    blob.clear_in_place()?;
                    true
                } else {
                    false
                }
            } else {
                let values: Vec<Vec<u8>> = blob_keys.iter().map(|(_, v)| v.clone()).collect();
                let new_offsets = blob.rewrite_in_place(&values)?;
                // 更新主日志里的 blob 指针（覆盖写同一 storage key）
                for ((enc, user), new_off) in blob_keys.into_iter().zip(new_offsets.into_iter()) {
                    let mut stored = Vec::with_capacity(1 + 8 + 4);
                    stored.push(VAL_TAG_BLOB);
                    stored.extend_from_slice(&new_off.to_le_bytes());
                    stored.extend_from_slice(&(user.len() as u32).to_le_bytes());
                    let _ = kv.insert(enc, Some(stored));
                }
                true
            }
        };

        // 压缩：丢掉已硬删除的空间（同样只在全库轮次）
        if full_sweep && (removed > 0 || blob_rewritten) {
            let _ = kv.merge();
        }

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

impl Default for MVCC {
    fn default() -> Self {
        Self::new()
    }
}

/// 决定 open 后 next_version 的位置（旧库：WAL 兜底）。

fn scan_max_version(tree: &mut Bitcask) -> std::io::Result<u64> {
    #[cfg(test)]
    SCAN_MAX_CALLS.with(|c| c.set(c.get() + 1));
    let mut max_v = 0u64;
    // 只扫 KeyDir，不读 value
    for (enc, _) in tree.iter_meta() {
        if let Some(vk) = decode_key(enc) {
            max_v = max_v.max(vk.version);
        }
    }
    Ok(max_v)
}

impl BulkLoader {
    pub fn put(&mut self, key: &[u8], value: Vec<u8>) {
        let enc = encode_key(key, self.version);
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
        let ver = self.version;
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
        let ver = self.version;
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
        let enc = encode_key(key, self.version);
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
                    if vk.raw_key.as_slice() == key && vk.version <= self.version {
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

    fn finish_inner(&mut self) {
        if self.finished {
            return;
        }
        self.finished = true;
        let mut kv = self.kv.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();
        let mut wal = self.wal.lock().unwrap();
        let _ = blob.sync();
        let _ = kv.flush();
        // bulk 结束后写 hint，避免下次 open 全量扫
        let _ = kv.dump_hint();
        let next_ver = self.next_version.load(Ordering::SeqCst);
        let _ = wal.append(WalRecord::Checkpoint {
            next_version: next_ver,
            root_page_id: 0,
            next_page_id: 0,
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

/// 未终结事务的兜底回滚。
///
/// 「未终结」= 既没 `commit` 也没 `rollback` 就被丢弃（提前 `return`、panic、或纯粹忘了）。
/// 这不是便利功能，是止血：漏网的事务会**永久留在 `active_txn`**，于是
/// 1. `xmin = min(活跃事务 id)` 被一个已经死掉的 id 压住 ⇒ **版本回收从此永久停摆**；
/// 2. 之后每次 `begin` 的可见性快照 `keys().cloned().collect()` 随漏网数线性变大。
///
/// 判据用「活跃表里是否还有自己」而不是额外标志位：`commit`/`rollback` 都以 `remove` 收尾，
/// 所以成员关系本身就是「未终结」的充要条件。
impl Drop for Transaction {
    fn drop(&mut self) {
        let unfinished = match self.active_txn.lock() {
            Ok(guard) => guard.contains_key(&self.version),
            // 锁中毒（别的线程 panic 过）：不能在这里 panic —— Drop 里 panic 直接 abort 进程
            Err(_) => return,
        };
        if !unfinished {
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
        kv: Arc<Mutex<Bitcask>>,
        blob: Arc<Mutex<BlobStore>>,
        wal: Arc<Mutex<Wal>>,
        active_txn: Arc<Mutex<HashMap<u64, Vec<Vec<u8>>>>>,
        next_version: Arc<AtomicU64>,
        wal_limit: Arc<AtomicU64>,
        vac_pending: Arc<AtomicU64>,
    ) -> Self {
        let version = next_version.fetch_add(1, Ordering::SeqCst);
        let active_txn_arc = Arc::clone(&active_txn);

        let mut guard = active_txn.lock().unwrap();
        let active_xid: HashSet<u64> = guard.keys().cloned().collect();
        guard.insert(version, Vec::new());
        drop(guard);

        {
            let mut w = wal.lock().unwrap();
            let _ = w.append(WalRecord::Begin { xid: version });
        }

        Transaction {
            kv,
            blob,
            wal,
            active_txn: active_txn_arc,
            next_version,
            wal_limit,
            vac_pending,
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

        // 逻辑 value → 存储编码（大 value 写 blob）
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

        if let Err(e) = kvengine.insert(enc_key, stored_opt) {
            eprintln!("Bitcask insert 失败: {e}");
            return false;
        }
        true
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.latest_visible_raw(key)
            .and_then(|(_, v)| v)
            .and_then(crate::bitcask::kv_ops::logical_to_user)
    }

    /// 最新可见逻辑行（含 TTL 信封原始字节；tombstone 为 None）。
    /// 供 [`crate::kv_ops`] 使用，不剥用户 value。
    pub(crate) fn latest_visible_raw(
        &self,
        key: &[u8],
    ) -> Option<(u64, Option<Vec<u8>>)> {
        let mut kvengine = self.kv.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();
        // 从链尾往回走，第一个可见版本就是答案 —— 只对那一行读盘解码
        let enc = Self::find_version_back(&mut kvengine, key, |v| self.is_visible(v))?;
        let version = decode_key(&enc)?.version;
        // 外层 None：索引里没有这行；内层 None：软墓碑（逻辑值为 None）
        let stored = kvengine.get_entry(&enc).ok()??;
        let logical = match stored {
            None => None,
            Some(s) => decode_stored_value(&mut blob, &s).ok().flatten(),
        };
        Some((version, logical))
    }

    /// 全部最新可见逻辑行（key 有序）：(key, version, raw_or_tombstone)
    ///
    /// 两段式：① 只走 KeyDir 元数据（`iter_meta`，不碰 value）按 enc 升序折叠出每个逻辑
    /// key 的「最后一个可见版本」那一行；② 只对这批胜出行各读一次盘并解码。
    ///
    /// 旧实现是 `kv.iter()` 把**每一行连 value** 全读出来再挑，于是走它的公开面 ——
    /// 扫描 / 搜索分页 / `exists` / `export` / `purge_expired` / `print_all` ——
    /// 每次调用都是 O(总行数) 次读盘，跟版本链有没有被回收无关。现在读盘次数 =
    /// O(存活 key 数)。
    pub(crate) fn collect_latest_raw(
        &self,
        include_deleted: bool,
    ) -> Vec<(Vec<u8>, u64, Option<Vec<u8>>)> {
        // ① 元数据折叠：enc 升序 ⇒ 同一 raw_key 最后一次 insert 就是最大可见版本
        let mut winners: BTreeMap<Vec<u8>, (u64, Vec<u8>)> = BTreeMap::new();
        {
            let kvengine = self.kv.lock().unwrap();
            for (enc, _loc) in kvengine.iter_meta() {
                let Some(vk) = decode_key(enc) else {
                    continue;
                };
                if !self.is_visible(vk.version) {
                    continue;
                }
                winners.insert(vk.raw_key, (vk.version, enc.clone()));
            }
        }

        // ② 只给胜出行读盘。软墓碑 → logical None，与旧实现一致；
        //    两阶段之间索引若已不含该行（单写者模型下不会发生）就跳过该行。
        let mut kvengine = self.kv.lock().unwrap();
        let mut blob = self.blob.lock().unwrap();
        let mut out: Vec<(Vec<u8>, u64, Option<Vec<u8>>)> = Vec::with_capacity(winners.len());
        for (key, (ver, enc)) in winners {
            let Ok(Some(stored)) = kvengine.get_entry(&enc) else {
                continue;
            };
            let logical = match stored {
                None => None,
                Some(s) => decode_stored_value(&mut blob, &s).ok().flatten(),
            };
            if logical.is_none() && !include_deleted {
                continue;
            }
            out.push((key, ver, logical));
        }
        out
    }

    pub fn print_all(&self) -> BTreeMap<Vec<u8>, Option<Vec<u8>>> {
        let mut records = BTreeMap::new();
        for rec in self.export_latest_visible(true) {
            records.insert(rec.key, rec.value);
        }
        records
    }

    /// 导出本事务快照下每个逻辑 key 的最新可见版本（已剥 TTL）。
    pub fn export_latest_visible(&self, include_deleted: bool) -> Vec<ExportRecord> {
        self.collect_latest_raw(true)
            .into_iter()
            .filter_map(|(key, _ver, raw)| match raw {
                None => {
                    if include_deleted {
                        Some(ExportRecord { key, value: None })
                    } else {
                        None
                    }
                }
                Some(bytes) => match crate::bitcask::kv_ops::logical_to_user(bytes) {
                    Some(user) => Some(ExportRecord {
                        key,
                        value: Some(user),
                    }),
                    None => {
                        if include_deleted {
                            Some(ExportRecord { key, value: None })
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

    pub fn commit_with_options(&self, flush_data: bool) {
        let rows_written = {
            let mut active_txn = self.active_txn.lock().unwrap();
            active_txn.remove(&self.version).map(|k| k.len()).unwrap_or(0)
        };
        // 给自动回收记一笔账：本事务写了多少个版本行。用「逻辑 key 数」近似行数
        // （一个 key 在同事务里覆写多次只计 1）—— 宁可少算，也不要为了精确再走一遍写集。
        if rows_written > 0 {
            self.vac_pending.fetch_add(rows_written as u64, Ordering::SeqCst);
        }
        {
            let mut wal = self.wal.lock().unwrap();
            let _ = wal.append(WalRecord::Commit { xid: self.version });
            if let Err(e) = wal.sync() {
                eprintln!("WAL fsync 失败: {e}");
            }
        }
        if flush_data {
            let mut kv = self.kv.lock().unwrap();
            let mut blob = self.blob.lock().unwrap();
            let _ = kv.flush();
            let _ = blob.sync();
        }

        // WAL 超阈值就顺带 checkpoint。放在提交之后是因为此刻本事务已持久、
        // 活跃表也已摘除，checkpoint 记到的 next_version 是干净的。
        let limit = self.wal_limit.load(Ordering::SeqCst);
        if limit > 0 {
            let size = self.wal.lock().unwrap().size_bytes().unwrap_or(0);
            if size >= limit {
                run_checkpoint(&self.kv, &self.blob, &self.wal, &self.next_version);
            }
        }
    }

    pub fn rollback(&self) {
        let mut active_txn = self.active_txn.lock().unwrap();
        let keys = active_txn.remove(&self.version).unwrap_or_default();

        {
            let mut wal = self.wal.lock().unwrap();
            let _ = wal.append(WalRecord::Abort { xid: self.version });
            let _ = wal.sync();
        }

        if !keys.is_empty() {
            let mut kvengine = self.kv.lock().unwrap();
            for k in keys {
                let enc_key = encode_key(&k, self.version);
                let _ = kvengine.remove(&enc_key);
            }
        }
    }

    fn is_visible(&self, version: u64) -> bool {
        if self.active_xid.contains(&version) {
            return false;
        }
        version <= self.version
    }

    /// 从链尾往回找本 key 第一个被 `accept` 的版本，返回它的版本键（原始 enc 字节）。
    ///
    /// 版本键是 `raw_key ‖ version.to_be_bytes()`，大端 ⇒ 同一 key 的所有版本在字节序里
    /// 相邻且按版本号升序，所以**链尾就是最新版本**：往回走第一个满足条件的即可停。
    ///
    /// `raw_key` 仍是 `key` 前缀的其它逻辑 key 会落进同一个字节区间，因此必须逐条核对
    /// `raw_key == key`（旧实现靠 `filter` 做这件事，这里保留同样的判定）。
    fn find_version_back(
        kv: &mut Bitcask,
        key: &[u8],
        accept: impl Fn(u64) -> bool,
    ) -> Option<Vec<u8>> {
        let low = encode_key(key, 0);
        let high = encode_key(key, u64::MAX);
        let mut budget = VERSION_BACK_STEP;
        loop {
            // 只取索引元数据，不碰 value；条数不足预算即说明已扫到区间下界
            let rows = kv.range_meta_back(&low, &high, budget);
            let reached_bottom = rows.len() < budget;
            for (enc, _) in rows {
                if let Some(vk) = decode_key(&enc) {
                    if vk.raw_key.as_slice() == key && accept(vk.version) {
                        return Some(enc);
                    }
                }
            }
            if reached_bottom {
                return None;
            }
            budget = budget.saturating_mul(2);
        }
    }

    fn latest_version_of(kv: &mut Bitcask, key: &[u8]) -> Option<u64> {
        Self::find_version_back(kv, key, |_| true)
            .and_then(|enc| decode_key(&enc).map(|vk| vk.version))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bitcask::storage::{blob_path, data_path, hint_path, lock_path};

    fn tmp_db(tag: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("bitcask_mvcc_{tag}_{nanos}"))
    }

    fn cleanup(path: &Path) {
        let _ = std::fs::remove_dir_all(path);
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(wal_path(path));
        let _ = std::fs::remove_file(lock_path(path));
        let _ = std::fs::remove_file(data_path(path));
        let _ = std::fs::remove_file(blob_path(path));
        let _ = std::fs::remove_file(hint_path(path));
    }

    /// 探针：一次点读 / 一次点写到底读了几行盘。
    ///
    /// 版本键 `raw_key ‖ version.to_be_bytes()` 大端有序 ⇒ 最新版本必然在链尾，
    /// 定位到它就够 —— 所以**读盘次数不该随链长增长**。
    /// 当前实现走 `scan_key_versions` → `range_scan` → 每行一次 `read_live`，
    /// 于是链长 N 就付 N 次 pread；点写更冤：`latest_version_of` 只要一个版本号，
    /// 却把 N 个 value 全读出来丢掉。
    ///
    /// 这条断言在当前代码上**必然失败**，失败信息里的两个数字就是这个 bug 的量级证明。
    #[test]
    fn test_point_op_cost_independent_of_chain_length() {
        fn probe(chain_len: u64) -> (u64, u64) {
            let path = tmp_db(&format!("probe_{chain_len}"));
            let (get_reads, set_reads);
            {
                let mvcc = MVCC::open(&path);
                for i in 0..chain_len {
                    let tx = mvcc.begin_transaction();
                    assert!(tx.set(b"hot", format!("v{i}").into_bytes()), "第 {i} 次写入被拒");
                    tx.commit();
                }

                let tx = mvcc.begin_transaction();
                let before = crate::bitcask::bitcask::read_value_calls();
                assert_eq!(tx.get(b"hot"), Some(format!("v{}", chain_len - 1).into_bytes()));
                get_reads = crate::bitcask::bitcask::read_value_calls() - before;

                let before = crate::bitcask::bitcask::read_value_calls();
                assert!(tx.set(b"hot", b"newer".to_vec()));
                set_reads = crate::bitcask::bitcask::read_value_calls() - before;
                tx.rollback();
            }
            cleanup(&path);
            (get_reads, set_reads)
        }

        let (g_short, s_short) = probe(50);
        let (g_long, s_long) = probe(2000);
        eprintln!(
            "点读读盘次数：链长 50 → {g_short} 次，链长 2000 → {g_long} 次\n\
             点写读盘次数：链长 50 → {s_short} 次，链长 2000 → {s_long} 次"
        );
        assert_eq!(g_short, g_long, "点读的读盘次数不应随版本链长度增长");
        assert_eq!(s_short, s_long, "点写的读盘次数不应随版本链长度增长（它只需要版本号）");
    }

    /// 泄漏事务（既不 commit 也不 rollback 就丢弃）**不得**永久卡死 vacuum 的水位线。
    ///
    /// 没有 `impl Drop for Transaction` 时，漏网的 xid 会一直挂在 `active_txn` 里，
    /// `xmin = min(活跃事务 id)` 因此永久停在它身上 —— 它之后的每一次写入都变成
    /// 「比 xmin 新、永远不能裁」，于是版本回收**整体停摆**，且之后每个事务的
    /// 可见性快照都多带一个死 id。
    ///
    /// 故意把泄漏放在造旧版本**之前**：这样漏网 id 是全场最小，`xmin` 被它钉死，
    /// 去掉 Drop 兜底这条测试就会红（顺序反过来则仍能裁掉，断言咬不住）。
    #[test]
    fn test_leaked_transaction_does_not_block_vacuum() {
        let path = tmp_db("leak_vacuum");
        {
            let mvcc = MVCC::open(&path);

            // 先泄漏：只 begin + 写，然后直接丢弃
            {
                let tx = mvcc.begin_transaction();
                assert!(tx.set(b"leak", b"x".to_vec()));
            }

            for i in 0..3u64 {
                let tx = mvcc.begin_transaction();
                assert!(tx.set(b"k", format!("v{i}").into_bytes()));
                tx.commit();
            }

            assert!(
                mvcc.active_txn.lock().unwrap().is_empty(),
                "泄漏事务应已被 Drop 回收，而不是永久留在活跃表里"
            );

            let stats = mvcc.vacuum().unwrap();
            assert!(
                stats.versions_removed > 0,
                "xmin 应能推进到 next_version 并裁掉 k 的旧版本；裁不动说明活跃表里有死 id 事务"
            );

            let tx = mvcc.begin_transaction();
            assert_eq!(tx.get(b"leak"), None, "泄漏事务的写入应已被回滚");
            assert_eq!(tx.get(b"k"), Some(b"v2".to_vec()));
            tx.rollback();
        }
        cleanup(&path);
    }

    /// 全库类 API（`print_all` / 扫描 / 搜索 / `exists` / `export` / `purge_expired` 都走
    /// `collect_latest_raw`）的读盘次数必须只与**存活 key 数**相关，与历史行数无关。
    ///
    /// 旧实现把每一行连 value 全读出来再挑最后一个，所以这里刻意让「历史行数」远大于
    /// 「存活 key 数」，这样断言才有区分力（旧实现会读 KEYS×VERSIONS 次）。
    #[test]
    fn test_collect_latest_reads_scale_with_live_keys_not_rows() {
        const KEYS: u64 = 8;
        const VERSIONS: u64 = 120;

        let path = tmp_db("collect_cost");
        {
            let mvcc = MVCC::open(&path);
            for v in 0..VERSIONS {
                for k in 0..KEYS {
                    let key = format!("k{k}");
                    let tx = mvcc.begin_transaction();
                    assert!(tx.set(key.as_bytes(), format!("v{v}").into_bytes()));
                    tx.commit();
                }
            }

            let tx = mvcc.begin_transaction();
            let before = crate::bitcask::bitcask::read_value_calls();
            let all = tx.print_all();
            let used = crate::bitcask::bitcask::read_value_calls() - before;
            assert_eq!(all.len() as u64, KEYS, "存活 key 数应等于 {KEYS}");
            assert!(
                used <= KEYS + 4,
                "只应按存活 key 读盘：实际读 {} 次，而历史行数是 {}",
                used,
                KEYS * VERSIONS
            );
            tx.rollback();

            // 参照：旧实现走的就是「全行物化」（`Bitcask::iter` 每行连 value 读出来再挑），
            // 这条原语现在还在，所以「改前」是实测而不是推断。
            let before = crate::bitcask::bitcask::read_value_calls();
            let rows = mvcc.kv.lock().unwrap().iter().unwrap().len() as u64;
            let old_style = crate::bitcask::bitcask::read_value_calls() - before;
            eprintln!(
                "全库折叠读盘：新实现 {used} 次 vs 旧路径（全行物化）{old_style} 次，历史行数 {rows}"
            );
            assert!(
                used * 10 <= old_style,
                "折叠读盘应比全行物化便宜一个量级以上（{used} vs {old_style}）"
            );
        }
        cleanup(&path);
    }

    /// WAL 阈值触发必须真的把文件截住（对照「关闭自动触发」），且截断之后重开仍能读到
    /// 全部已提交数据 —— 后半段守的是「先 flush 数据、再截 WAL」这个顺序：
    /// 顺序反了就是**丢已提交写入**。
    #[test]
    fn test_wal_auto_checkpoint_bounds_file_and_keeps_data() {
        const WRITES: u32 = 40;

        fn wal_bytes_after(tag: &str, limit: u64) -> u64 {
            let path = tmp_db(tag);
            {
                let mvcc = MVCC::open(&path);
                mvcc.set_wal_checkpoint_limit(limit);
                for i in 0..WRITES {
                    let key = format!("k{i}");
                    let tx = mvcc.begin_transaction();
                    assert!(tx.set(key.as_bytes(), vec![b'x'; 200]));
                    tx.commit();
                }
            }
            let size = std::fs::metadata(wal_path(&path)).map(|m| m.len()).unwrap_or(0);
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
            let mvcc = MVCC::open(&path);
            mvcc.set_wal_checkpoint_limit(1);
            for i in 0..WRITES {
                let key = format!("r{i}");
                let tx = mvcc.begin_transaction();
                assert!(tx.set(key.as_bytes(), format!("v{i}").into_bytes()));
                tx.commit();
            }
        }
        {
            let mvcc = MVCC::open(&path);
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

    /// 冷启动三级优先：WAL 里有 Checkpoint 时**不该**再做 O(总行数) 的全库 KeyDir 扫描。
    ///
    /// 守的是「启动一次比一次慢」那条通道：`scan_max_version` 随总行数线性，而
    /// `checkpoint()` 早已把 `next_version` 写进 WAL（且 `try_open` 收尾也会补一条），
    /// 之前却完全没读它。这里用调用计数证明快路真的走到了，而不只是代码看起来对。
    #[test]
    fn test_reopen_uses_checkpoint_instead_of_full_scan() {
        const KEYS: u32 = 60;
        let path = tmp_db("ckpt_start_ver");
        let before;
        {
            let mvcc = MVCC::open(&path);
            mvcc.set_wal_checkpoint_limit(0); // 关掉自动触发，本用例只测手动 checkpoint
            for i in 0..KEYS {
                let key = format!("k{i}");
                let tx = mvcc.begin_transaction();
                assert!(tx.set(key.as_bytes(), format!("v{i}").into_bytes()));
                tx.commit();
            }
            before = mvcc.next_version.load(Ordering::SeqCst);
            mvcc.checkpoint();
        }

        {
            let base = scan_max_calls();
            let mvcc = MVCC::open(&path);
            assert_eq!(
                scan_max_calls() - base,
                0,
                "有 Checkpoint 时重开不应再全库扫 KeyDir"
            );
            let after = mvcc.next_version.load(Ordering::SeqCst);
            assert!(after >= before, "重开后 next_version 不应回退（{after} < {before}）");
            let tx = mvcc.begin_transaction();
            for i in 0..KEYS {
                let key = format!("k{i}");
                assert_eq!(tx.get(key.as_bytes()), Some(format!("v{i}").into_bytes()));
            }
            tx.commit();
        }
        cleanup(&path);
    }

    /// 复现「旧水位线会裁掉活跃事务快照仍需要的版本」⇒ **静默读到错值**。
    ///
    /// 形状：`hot` 有 v1、v2 两版。B 提交 v2 **之前** T1 已经 begin ⇒ v2 落在 T1 的
    /// 并发快照里、对 T1 永久不可见 ⇒ T1 只能回退到 v1。而此时
    /// `xmin = min(活跃事务 id) = T1.id`，「最后一个 < xmin」的版本正好是 v2，
    /// 于是按规则 v1 被裁掉 —— T1 再读就成了 None。
    ///
    /// 这就是 12e3696 想修、当天又被 f78edbb revert 回来的那个洞。revert 信息里
    /// 承认「旧水位线的已知问题随之回来了」，但仓库里没有一条测试钉住它 ——
    /// 缺的就是这条。
    #[test]
    fn test_vacuum_keeps_versions_needed_by_active_txn() {
        let path = tmp_db("vacuum_active");
        {
            let mvcc = MVCC::open(&path);

            let t0 = mvcc.begin_transaction();
            assert!(t0.set(b"hot", b"v1".to_vec()));
            t0.commit();

            let b = mvcc.begin_transaction();
            assert!(b.set(b"hot", b"v2".to_vec()));
            let t1 = mvcc.begin_transaction(); // T1 的快照里有 b
            b.commit(); // v2 提交了，但对 T1 永久不可见

            assert_eq!(
                t1.get(b"hot"),
                Some(b"v1".to_vec()),
                "读前提：T1 应当看到 v1（v2 在它的并发快照里）"
            );

            let stats = mvcc.vacuum().unwrap();
            assert!(
                stats.skipped_active_txn,
                "有活跃事务时 vacuum 应整轮跳过，而不是按 min(活跃 id) 去裁版本"
            );
            assert_eq!(stats.versions_removed, 0, "跳过的那一轮不该裁任何东西");
            assert_eq!(
                t1.get(b"hot"),
                Some(b"v1".to_vec()),
                "vacuum 不得裁掉活跃事务快照仍需要的版本（裁错 = 静默读到旧值/None）；本次 xmin={}",
                stats.xmin
            );

            t1.rollback();

            // T1 结束之后再 vacuum：这次必须真的能裁（否则「整轮跳过」会变成永久不回收）
            let stats = mvcc.vacuum().unwrap();
            assert!(
                !stats.skipped_active_txn,
                "没有活跃事务时不该再跳过，否则版本链永远裁不掉"
            );
            assert!(
                stats.versions_removed > 0,
                "hot 的旧版本这次应被裁掉，实测 {} 次",
                stats.versions_removed
            );
            let t2 = mvcc.begin_transaction();
            assert_eq!(t2.get(b"hot"), Some(b"v2".to_vec()), "裁完应读到最新版本");
            t2.rollback();
        }
        cleanup(&path);
    }

    /// 长跑进程不喊 vacuum，版本链也不能无限涨（I2：垃圾必须自己回来）。
    ///
    /// 事故那条曲线的本质就是版本行只增不减 —— 一天/一个多月里 CPU 慢慢爬。
    /// 这里把切片宽度调到 8 行，写 200 版热点 key，断言：
    /// 1. 没人叫过 vacuum，链也被压住（自动回收到位在跑）；
    /// 2. 最新值始终读得到 —— 自动裁的是旧版本，不是可见版本；
    /// 3. 默认是**开着**的（关掉它就等于把这次事故原样留给下一个项目）。
    #[test]
    fn test_auto_vacuum_reclaims_without_being_asked() {
        const CHAIN: u64 = 200;
        const SLICE: u64 = 8;

        let path = tmp_db("auto_vac");
        {
            let mvcc = MVCC::open(&path);
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
    ///
    /// 有了它就允许上层把回收完全交给自己调度（比如只在交易空档跑），
    /// 也保证默认值不是「悄悄多做了一件事」。
    #[test]
    fn test_auto_vacuum_off_switch() {
        const CHAIN: u64 = 60;

        let path = tmp_db("auto_off");
        {
            let mvcc = MVCC::open(&path);
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
    /// `vacuum()` 把 kv 锁按到整库扫完 —— 单核机器上「开自动回收」就会在回收那一刻
    /// 制造新事故。切片后每轮读的行数有上限（偏差 = 一条链的长度：截断只能落在逻辑 key
    /// 边界，半条链没法判「留哪一版」），而游标每轮必须真的前进（`rounds` 的上界就是这条）。
    ///
    /// 键里刻意放了前缀嵌套的 `k1` / `k1x`：版本键是 `raw ‖ 8B`，真前缀会让两把 key 的
    /// 版本区间互相穿插，切片最容易在这里切错。
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

        // 对照组：一次全库 vacuum 裁掉多少
        let full_path = tmp_db("bounded_full");
        let full_removed;
        {
            let mvcc = MVCC::open(&full_path);
            fill(&mvcc);
            full_removed = mvcc.vacuum().unwrap().versions_removed;
        }
        cleanup(&full_path);

        let path = tmp_db("bounded_slice");
        {
            let mvcc = MVCC::open(&path);
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
                assert!(round.stats.versions_removed > 0, "第 {rounds} 轮必须裁掉东西，否则游标没前进");
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

    /// 健康度快照必须如实报出「最长版本链」。
    ///
    /// 这次事故的核心数字（某把热点 key 堆了多少个版本）一直是靠
    /// `grep data.log | uniq -c` 数出来的 —— 一个当基座用的库不该靠这个诊断自己。
    /// 顺带验证 vacuum 之后能看得见「链被压平」，那正是将来自动回收要盯的指标。
    #[test]
    fn test_health_reports_chain_length() {
        const CHAIN: u64 = 500;

        let path = tmp_db("health");
        {
            let mvcc = MVCC::open(&path);
            for i in 0..CHAIN {
                let tx = mvcc.begin_transaction();
                assert!(tx.set(b"hot", format!("v{i}").into_bytes()));
                tx.commit();
            }
            for k in 0..3u64 {
                let key = format!("other{k}");
                let tx = mvcc.begin_transaction();
                assert!(tx.set(key.as_bytes(), b"x".to_vec()));
                tx.commit();
            }

            let h = mvcc.health();
            assert_eq!(h.live_keys, 4, "逻辑 key 数");
            assert_eq!(h.total_rows, CHAIN + 3, "物理行数（含历史版本）");
            assert_eq!(h.max_chain, CHAIN, "最长版本链应由库自己报出来");
            assert_eq!(h.rows_above_one, CHAIN - 1);
            assert_eq!(h.active_txns, 0);

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

    /// vacuum 自己不该是全库最重的路径：读盘次数必须只与**留下来的行数**相关。
    ///
    /// 旧实现里 vacuum 有两次 `kv.iter()`（每一行连 value 全读出来）。也就是说在
    /// 「链已经涨到几十万行」的库上跑 vacuum，那一刻本身就是一次事故 ——
    /// 这正是「先让 vacuum 变轻，再谈自动触发」这个施工顺序的理由。
    #[test]
    fn test_vacuum_does_not_read_every_version() {
        const CHAIN: u64 = 400;
        const KEYS: u64 = 5;

        let path = tmp_db("vacuum_cost");
        {
            let mvcc = MVCC::open(&path);
            for i in 0..CHAIN {
                for k in 0..KEYS {
                    let key = format!("k{k}");
                    let tx = mvcc.begin_transaction();
                    assert!(tx.set(key.as_bytes(), format!("v{i}").into_bytes()));
                    tx.commit();
                }
            }
            assert_eq!(mvcc.health().total_rows, CHAIN * KEYS);

            let before = crate::bitcask::bitcask::read_value_calls();
            let stats = mvcc.vacuum().unwrap();
            let used = crate::bitcask::bitcask::read_value_calls() - before;
            eprintln!(
                "vacuum 读盘次数：{used} 次（裁掉 {} 行，存活 key {KEYS} 个）",
                stats.versions_removed
            );
            assert_eq!(stats.versions_removed, ((CHAIN as usize - 1) * (KEYS as usize)) as usize);
            // 界取 2×存活行数：每个留下来的行最多被读两次 —— 一次查 blob 引用，
            // 一次是 merge 重写日志时把它搬走（那是 merge 的本职，不是 vacuum 的浪费）。
            // 关键是它只与**存活行数**相关；未修前这里是 2000 次（每行连 value 全读）。
            assert!(
                used <= 2 * KEYS + 4,
                "vacuum 只该读留下来的那些行：实际读盘 {used} 次，而历史行数是 {}",
                CHAIN * KEYS
            );
            assert_eq!(mvcc.health().max_chain, 1, "裁完最长链应降到 1");
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
            let mvcc = MVCC::open(&path);
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
            let mvcc = MVCC::open(&path);
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
            let mvcc = MVCC::open(&path);
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
            let mvcc = MVCC::open(&path);
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
            let mvcc = MVCC::open(&path);
            let t1 = mvcc.begin_transaction();
            assert!(t1.set(b"a", b"1".to_vec()));
            assert!(t1.set(b"b", b"2".to_vec()));
            assert!(t1.set(b"a", b"3".to_vec()));
            let all = t1.print_all();
            assert_eq!(all.get(&b"a".to_vec()).unwrap().as_ref().unwrap(), b"3");
            assert_eq!(all.get(&b"b".to_vec()).unwrap().as_ref().unwrap(), b"2");
            t1.commit();
        }
        cleanup(&path);
    }

    #[test]
    fn test_instances_isolated() {
        let p1 = tmp_db("i1");
        let p2 = tmp_db("i2");
        {
            let m1 = MVCC::open(&p1);
            let m2 = MVCC::open(&p2);
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
            let mvcc = MVCC::open(&path);
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
            let mvcc = MVCC::open(&path);
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
            let mvcc = MVCC::open(&path);
            let t1 = mvcc.begin_transaction();
            assert!(t1.set(b"hello", b"world".to_vec()));
            t1.commit();
            let t2 = mvcc.begin_transaction();
            assert!(t2.set(b"hello", b"rust".to_vec()));
            assert!(t2.set(b"foo", b"bar".to_vec()));
            t2.commit();
            mvcc.flush();
        }
        {
            let mvcc = MVCC::open(&path);
            let tx = mvcc.begin_transaction();
            assert_eq!(tx.get(b"hello"), Some(b"rust".to_vec()));
            assert_eq!(tx.get(b"foo"), Some(b"bar".to_vec()));
            tx.commit();
        }
        cleanup(&path);
    }

    #[test]
    fn test_wal_redo_after_commit_without_data_flush() {
        let path = tmp_db("redo");
        {
            cleanup(&path);
            std::fs::create_dir_all(&path).unwrap();
            // 空数据文件
            let mut tree = Bitcask::new(data_path(&path)).unwrap();
            tree.flush().unwrap();
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
            let mvcc = MVCC::open(&path);
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
            {
                let mvcc = MVCC::open(&path);
                drop(mvcc);
            }
            // 模拟：脏写入已进数据文件，但 WAL 只有 Begin+Write 无 Commit
            let mut wal = Wal::open(wal_path(&path)).unwrap();
            let xid = 1u64;
            {
                let mut tree = Bitcask::new(data_path(&path)).unwrap();
                let enc = encode_key(b"ghost", xid);
                tree.insert(enc.clone(), Some(b"should-vanish".to_vec()))
                    .unwrap();
                tree.flush().unwrap();
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
            let mvcc = MVCC::open(&path);
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
            let mvcc = MVCC::open(&path);
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
            let mvcc = MVCC::open(&path);
            let tx = mvcc.begin_transaction();
            assert_eq!(tx.get(b"k000042"), Some(b"v42".to_vec()));
            assert_eq!(tx.get(b"k000999"), Some(b"v999".to_vec()));
            tx.commit();
        }
        cleanup(&path);
    }

    #[test]
    fn test_export_latest_visible() {
        let path = tmp_db("export");
        {
            let mvcc = MVCC::open(&path);
            let t1 = mvcc.begin_transaction();
            assert!(t1.set(b"k1", b"v1".to_vec()));
            assert!(t1.set(b"k2", b"v2".to_vec()));
            t1.commit();
            let t2 = mvcc.begin_transaction();
            assert!(t2.set(b"k1", b"v1b".to_vec()));
            assert!(t2.delete(b"k2"));
            assert!(t2.set(b"k3", b"v3".to_vec()));
            t2.commit();
        }
        {
            let mvcc = MVCC::open(&path);
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
    fn test_vacuum_removes_old_versions() {
        let path = tmp_db("vac");
        {
            let mvcc = MVCC::open(&path);
            let t = mvcc.begin_transaction();
            assert!(t.set(b"a", b"1".to_vec()));
            assert!(t.set(b"b", b"old".to_vec()));
            t.commit();
            let t = mvcc.begin_transaction();
            assert!(t.set(b"a", b"2".to_vec()));
            assert!(t.delete(b"b"));
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
            tx.commit();
        }
        cleanup(&path);
    }

    #[test]
    fn test_large_value_blob_roundtrip() {
        let path = tmp_db("blob");
        let big = vec![0xABu8; 10_000]; // > BLOB_THRESHOLD
        {
            let mvcc = MVCC::open(&path);
            let tx = mvcc.begin_transaction();
            assert!(tx.set(b"big", big.clone()));
            assert!(tx.set(b"small", b"hi".to_vec()));
            assert_eq!(tx.get(b"big"), Some(big.clone()));
            assert_eq!(tx.get(b"small"), Some(b"hi".to_vec()));
            tx.commit();
            mvcc.flush();
        }
        {
            let mvcc = MVCC::open(&path);
            let tx = mvcc.begin_transaction();
            assert_eq!(tx.get(b"big"), Some(big));
            assert_eq!(tx.get(b"small"), Some(b"hi".to_vec()));
            tx.commit();
        }
        let blob = blob_path(&path);
        assert!(blob.exists());
        assert!(std::fs::metadata(&blob).unwrap().len() > 8);
        cleanup(&path);
    }

    #[test]
    fn test_try_open_lock_conflict() {
        let path = tmp_db("lock2");
        let m1 = MVCC::try_open(&path).unwrap();
        let err = MVCC::try_open(&path).err();
        assert!(err.is_some());
        drop(m1);
        let m2 = MVCC::try_open(&path).unwrap();
        drop(m2);
        cleanup(&path);
    }
}
