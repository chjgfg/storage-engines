//! 自动回收调度、预算和维护租约。

use super::*;

impl Drop for VacuumLease<'_> {
    fn drop(&mut self) {
        self.scheduler
            .pending
            .fetch_add(self.reserved, Ordering::SeqCst);
        self.scheduler.running.store(false, Ordering::Release);
    }
}

impl Default for VacuumScheduler {
    fn default() -> Self {
        Self::new(0)
    }
}

impl VacuumScheduler {
    pub fn new(slice_rows: u64) -> Self {
        VacuumScheduler {
            slice: Arc::new(AtomicU64::new(slice_rows)),
            pending: Arc::new(AtomicU64::new(0)),
            cursor: Arc::new(Mutex::new(None)),
            running: AtomicBool::new(false),
            committed_rows: AtomicU64::new(0),
            completed_slices: AtomicU64::new(0),
            skipped_active: AtomicU64::new(0),
            failed_slices: AtomicU64::new(0),
            versions_removed: AtomicU64::new(0),
        }
    }

    pub fn slice(&self) -> u64 {
        self.slice.load(Ordering::SeqCst)
    }

    /// 设切片宽度；0 = 关闭自动回收（那时版本行只增不减，得靠手动 `vacuum()`）
    pub fn set_slice(&self, rows: u64) {
        self.slice.store(rows, Ordering::SeqCst);
    }

    /// 提交时记一笔账（行数是近似值：算逻辑 key 数，宁可少算也不为了精确再走一遍写集）
    pub fn on_commit(&self, rows: u64) {
        if rows > 0 {
            self.pending.fetch_add(rows, Ordering::SeqCst);
            self.committed_rows.fetch_add(rows, Ordering::SeqCst);
        }
    }

    /// 丢掉"新增版本行账"里还没被消费的部分，返回丢掉的行数（M82，快照重放用）。
    ///
    /// 为什么可以丢：这笔账的语义是"**攒了多少待回收的旧版本**"，而重放出来的行每把键只有一版
    /// —— 一行垃圾都没有。留着它只会让重放刚结束就来一整轮空扫（`slice` 行/次）：20M 档实测
    /// 1221 片 ≈ 十几秒，600k 档 `冷重开` 1.3 s → 4.4 s。⚠️ 只该在"新写入行确定不是垃圾"的场合用
    /// （今天只有快照重放这一处），别拿它当"清账"的通用口子。
    pub fn discard_pending(&self) -> u64 {
        self.pending.swap(0, Ordering::SeqCst)
    }

    /// 该裁一片了吗：**新增版本行账攒够一片才取走一片**，否则 `None`。
    ///
    /// 这条口径是 M82 定的，把 M81 第 5 项后半加进来的"游标不在库头 ⇒ 账不够也给许可（`charged = 0`）"
    /// 那一支**拿掉了**。那一支想解决的事是真的（一片把账扣到不足阈值、游标停在库中段，没有新写
    /// 就永远不再推进 ⇒ 后半库的垃圾无人认领），但它把代价挂到了 `begin()` 热路径上：写流里一轮
    /// 永远开着（账一直攒、游标一绕完就立刻再开），于是**每个事务**的 `begin()` 都扫一整片 ——
    /// 1000 行/事务放大 16×、单行事务放大 16384×。`docs/test2.txt` 五档 20/20 格 `灌`/`更新`
    /// 变慢、memory `冷重开` 1.3 s → 4.4 s、冷重开后每个单事务读格多付 8~10 ms，全是这笔税；
    /// 守卫 `maintenance::tests::auto_vacuum_scanning_keeps_pace_with_writes_not_with_transactions`
    /// 在那份实现上读出 275 片 ≈ 17 600 行（写了 512 行）。
    ///
    /// 现在的分工：`begin()` 的扫描量与写入量 1:1（攒多少扫多少，不多不少）；"账花完而一轮还在半路"
    /// 由 [`crate::maintenance::Maintenance`] 兜 —— 它有自己的游标和间隔，本来就是干这个的，
    /// 不在任何事务的路径上。普通 `open` 不跑维护器时，后半库的垃圾等下一次写或手动 `vacuum()`，
    /// 这是有意的取舍，不是漏。`sweep_in_progress`（= 游标不在库头）照旧如实报告，好让调用方
    /// 知道该不该去推一把。
    pub fn take_tick(&self) -> Option<VacuumTick> {
        let slice = self.slice.load(Ordering::SeqCst);
        if slice == 0 {
            return None;
        }
        let seen = self.pending.load(Ordering::SeqCst);
        if seen < slice {
            return None;
        }
        // 只取一片，不能把大事务剩余的账清零；下一次 begin 继续偿还积压。
        match self
            .pending
            .compare_exchange(seen, seen - slice, Ordering::SeqCst, Ordering::SeqCst)
        {
            Ok(_) => Some(VacuumTick {
                slice,
                charged: slice,
            }),
            Err(_) => None,
        }
    }

    /// 一轮是否走在半路（游标不在库头）。维护器与守卫都读这一个口子，不许自己猜。
    /// 自 M82 起它只是**状态**，不再是 `begin()` 要不要扫一片的依据。
    pub fn round_open(&self) -> bool {
        self.cursor.lock().unwrap().is_some()
    }

    /// 自动切片的唯一驱动。跳过/失败保留账与游标；并发调用不重复使用同一游标。
    pub fn run_pending<E>(
        &self,
        run: impl FnOnce(Option<&[u8]>, u64) -> Result<crate::common::VacuumRound, E>,
    ) -> Result<(), E> {
        if self.slice() == 0
            || self
                .running
                .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_err()
        {
            return Ok(());
        }
        let mut lease = VacuumLease {
            scheduler: self,
            reserved: 0,
        };
        // 能不能取一片由 `take_tick` 一处回答（账够一片才给），这里不另设预检。
        let Some(tick) = self.take_tick() else {
            return Ok(());
        };
        lease.reserved = tick.charged;
        let cursor = self.cursor();
        match run(cursor.as_deref(), tick.slice) {
            Ok(round) if round.stats.skipped_active_txn => {
                self.skipped_active.fetch_add(1, Ordering::SeqCst);
            }
            Ok(round) => {
                self.set_cursor(round.next_cursor);
                self.versions_removed
                    .fetch_add(round.stats.versions_removed as u64, Ordering::SeqCst);
                self.completed_slices.fetch_add(1, Ordering::SeqCst);
                lease.reserved = 0;
            }
            Err(e) => {
                self.failed_slices.fetch_add(1, Ordering::SeqCst);
                return Err(e);
            }
        }
        Ok(())
    }

    /// O(活跃事务数)，不读取存储页、value 或全库键。
    pub fn status(&self, active: &Registry) -> crate::common::types::ReclamationStatus {
        crate::common::types::ReclamationStatus {
            pending_rows: self.pending.load(Ordering::SeqCst),
            committed_rows: self.committed_rows.load(Ordering::SeqCst),
            completed_slices: self.completed_slices.load(Ordering::SeqCst),
            skipped_active: self.skipped_active.load(Ordering::SeqCst),
            failed_slices: self.failed_slices.load(Ordering::SeqCst),
            versions_removed: self.versions_removed.load(Ordering::SeqCst),
            running: self.running.load(Ordering::Acquire),
            sweep_in_progress: self.round_open(),
            active_txns: active.len() as u64,
            oldest_active_version: active.keys().copied().min(),
        }
    }

    pub fn cursor(&self) -> Option<Vec<u8>> {
        self.cursor.lock().unwrap().clone()
    }

    pub fn set_cursor(&self, cursor: Option<Vec<u8>>) {
        *self.cursor.lock().unwrap() = cursor;
    }
}
