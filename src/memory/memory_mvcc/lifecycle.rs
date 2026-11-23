//! 本文件负责打开、配置、关闭及资源所有权。

use super::*;

impl MVCC {
    /// 新建空库
    pub fn new() -> Self {
        Self {
            store: Arc::new(Mutex::new(MemStore::new())),
            active_txn: Arc::new(Mutex::new(HashMap::new())),
            next_version: Arc::new(AtomicU64::new(1)),
            vac: Arc::new(VacuumScheduler::new(MVCC::VACUUM_AUTO_SLICE_DEFAULT)),
            snapshot: None,
        }
    }

    pub fn begin_transaction(&self) -> Transaction {
        self.tick_auto_vacuum();
        Transaction::begin(
            self.store.clone(),
            self.active_txn.clone(),
            self.next_version.clone(),
            self.vac.clone(),
        )
    }

    /// 本引擎能被调的东西 —— 清单在这里，不在门面。
    ///
    /// 只有一条：`wal_checkpoint_bytes` 对内存引擎没有意义（连字段都不存在，见
    /// [`MVCC::durability_profile`] 的"没有任何持久化"），所以共用清单里它对的那一半不列。
    pub fn tuning(&self) -> EngineTuning {
        let mut t = EngineTuning::default();
        t.push_shared(self.vacuum_auto_slice(), None);
        t
    }

    /// 默认切片宽度 = 16 Ki 行（四引擎同一个默认值，方便跨引擎对比观测）。
    pub const VACUUM_AUTO_SLICE_DEFAULT: u64 = 16 * 1024;

    /// bulk load：单写者、无冲突检测。结束必须 `finish()`。
    pub fn begin_bulk(&self) -> BulkLoader {
        BulkLoader {
            store: self.store.clone(),
            core: BulkCore::begin(self.next_version.clone()),
        }
    }

    /// 崩溃安全画像：**全空，这是设计而非疏漏**。
    ///
    /// ⚠️ M81 第 6 项之后"全空"的含义要说准：它**没有崩溃安全**（进程被杀 / `exit` 都不跑析构，
    /// 启停快照写不出来），仍然没有事务 WAL。BPEXP004 快照有 CRC，但不提供运行期持久提交。
    /// 变的是：给了快照目录的 `open_engine("memory", Some(dir))` 现在**优雅停止不丢**
    /// （`Drop` 写整库快照、下次 open 读回）。这不改变它"对照实验"的定位：
    /// 快照既不是 WAL 也不带校验和，扛不住掉电。
    pub fn durability_profile(&self) -> DurabilityProfile {
        DurabilityProfile {
            engine: "memory",
            transaction_wal: false,
            fsync_on_commit: false,
            data_checksum: false,
            blob_checksum: false,
            in_place_page_writes: false,
            torn_page_guard: false,
            merge_artifact_recovery: false,
        }
    }

    /// 底层行数（含历史版本 / tombstone）
    pub fn raw_len(&self) -> usize {
        self.store.lock().unwrap().len()
    }

    pub fn next_version(&self) -> u64 {
        self.next_version.load(Ordering::SeqCst)
    }
}

impl Default for MVCC {
    fn default() -> Self {
        Self::new()
    }
}

/// M81 第 6 项：**停止落盘**。`snapshot` 为 `None`（裸 [`MVCC::new`]，四个 CLI 与所有既有测试的
/// 形状）时这里什么都不做 —— 一行字节都不写，改前行为一字不变。
///
/// ⚠️ 这是"优雅停止不丢"，**不是崩溃安全**：进程被杀、被 `std::process::exit` 收尾都不跑析构。
/// 所以统一 import 工具收尾必须先让句柄正常离开作用域，不能 `exit` 了事（方案第 6 项记的坑）。
impl Drop for MVCC {
    fn drop(&mut self) {
        let Some(path) = self.snapshot.clone() else {
            return;
        };
        match self.write_stop_snapshot(&path) {
            Ok(n) => log::info!("memory: 停止落盘 {n} 行 → {}", path.display()),
            Err(e) => log::warn!("memory 快照落盘失败（{}）: {e}", path.display()),
        }
    }
}
