//! 本文件负责对象安全的事务公开契约。

use super::*;

/// 一个只读/读写事务（快照隔离）。
///
/// 方法语义与各引擎的 `Transaction` 完全一致：`set` / `delete` 返回 `false`
/// 表示写入失败，此时应当 [`KvTxn::rollback`]；原因使用 checked API 区分。
/// 一个只读/读写事务（快照隔离）。
///
/// `Send` 是**必须**的：句柄要能跟着任务跨线程走（交易进程里典型是"主线程开库、
/// 工作线程读写"）。刻意**不要求 `Sync`** —— 事务内部有 `Cell`（WAL 是否已开）与
/// 写集，跨线程共享一个事务对象本身就没有语义：单写者模型下"共享"只会变成竞态。
/// # 资源合同
///
/// 返回 Vec 的接口按返回字节占内存（scan/reverse_scan/prefix_scan/export/multi_get），
/// 不是分页或字节限额。scan_page 只限制行数；一条大值、长键、超长历史组仍能超过估计。
/// 本库四家流式出口只保留固定候选片及当前行，但后端缓存/SST源数/当前版本组、回调自己
/// 累积的结果另计；自定义 trait 实现的默认收集型 fallback 不享有同样的内存保证。
/// search 的精确 total 需要走完整个匹配范围；offset 深翻需走过被跳过的行，连续翻页用游标。
/// 所有接口均为同步调用；快照事务长期持有会阻挡版本回收，流式不等于短事务。
pub trait KvTxn: Send {
    /// 错误可区分的接口默认返回 Unsupported，避免替第三方实现承诺尚未实现的能力。
    /// 四个内置引擎均覆盖这些方法并返回实际执行结果。
    /// 写已封装的逻辑值；原始用户字节使用 try_set_raw。
    fn try_set(&self, _key: &[u8], _value: Vec<u8>) -> std::result::Result<(), crate::WriteError> {
        Err(crate::WriteError::Unsupported)
    }
    /// 写原始字节，由引擎添加值信封。
    fn try_set_raw(
        &self,
        _key: &[u8],
        _value: Vec<u8>,
    ) -> std::result::Result<(), crate::WriteError> {
        Err(crate::WriteError::Unsupported)
    }
    fn try_delete(&self, _key: &[u8]) -> std::result::Result<(), crate::WriteError> {
        Err(crate::WriteError::Unsupported)
    }
    /// 提交成功后才发布；Indeterminate 必须重开对账，CommittedMaintenance 已提交。
    fn try_commit(&self) -> std::result::Result<(), crate::CommitError> {
        Err(crate::CommitError::Unsupported)
    }
    // ---- 基本读写 ----

    /// 本事务的版本号 / xid
    fn version(&self) -> u64;

    /// 写入；`false` = 写入失败（冲突、容量或 I/O 等）；详细原因使用 checked API
    ///
    /// ⚠️ `value` 是**已打过信封**的逻辑值（历史约定，现有调用点按它写）。直接拿这个口子
    /// 存裸字节会踩坑：`0x00` 打头读回来少 1 字节，`0x01` 打头会被当成 TTL 信封少 9 字节、
    /// 甚至被判"已过期"读成 `None`。存二进制请用 [`KvTxn::set_raw`]。
    fn set(&self, key: &[u8], value: Vec<u8>) -> bool;

    /// 原样存这串字节：信封由引擎打，[`KvTxn::get`] 一字不差地还回来（空值、`0x00`/`0x01`
    /// 打头、走 blob 旁路的长值都一样）。`false` 同 `set` 表示写入失败。
    ///
    /// 为什么只加写的一侧：`get` 本来就已经把信封剥掉，它就是 `set_raw` 的读侧对应物；
    /// 再造一个 `get_raw` 只是给同一个行为起第二个名字，换来的是"该用哪个"的困惑。
    /// 确实需要"不经剥壳的存储字节"时，那是另一个能力（看 TTL 信封本身），目前只有引擎内部门
    /// （`latest_visible_raw`），没有对外承诺。
    fn set_raw(&self, key: &[u8], value: Vec<u8>) -> bool;

    /// 带 TTL 写入；`false` = 写入失败（冲突、容量或 I/O 等）；详细原因使用 checked API
    fn set_with_ttl(&self, key: &[u8], value: Vec<u8>, ttl: Duration) -> bool;

    /// 逻辑删除（写 tombstone）；`false` = 写入失败（冲突、容量或 I/O 等）；详细原因使用 checked API
    fn delete(&self, key: &[u8]) -> bool;

    /// 读取用户 value（已剥 TTL 信封；过期/删除/不存在均为 `None`）
    fn get(&self, key: &[u8]) -> Option<Vec<u8>>;

    fn exists(&self, key: &[u8]) -> bool;

    fn get_meta(&self, key: &[u8]) -> Option<KeyMeta>;

    // ---- 批量 ----

    /// 批量写；`Err(key)` 表示在该 key 上写入失败（已写入的部分不回滚，
    /// 调用方通常应 [`KvTxn::rollback`] 整个事务）
    fn batch_set(&self, items: &[(Vec<u8>, Vec<u8>)]) -> std::result::Result<(), Vec<u8>>;

    fn batch_delete(&self, keys: &[Vec<u8>]) -> std::result::Result<(), Vec<u8>>;

    fn multi_get(&self, keys: &[Vec<u8>]) -> Vec<Option<Vec<u8>>>;

    // ---- 计数器 ----

    fn incr(&self, key: &[u8], delta: i64) -> std::result::Result<i64, IncrError>;

    fn decr(&self, key: &[u8], delta: i64) -> std::result::Result<i64, IncrError>;

    /// 自增并给这个 key 设 TTL（`ttl` 是**绝对过期时刻**之前的存活时长）
    ///
    /// 为什么单列一个方法而不是"先 incr 再 refresh_ttl"：那是两个事务，中间崩了
    /// 会留下"计数加了、TTL 没设"的行；引擎内部是一次写集里做完的。
    fn incr_with_ttl(
        &self,
        key: &[u8],
        delta: i64,
        ttl: Duration,
    ) -> std::result::Result<i64, IncrError>;

    // ---- 扫描 / 搜索 ----

    /// 范围扫描 `[start, end)`；None 表示该端无界。物化全部命中结果，内存按返回字节增长。
    /// 大库导出/搬运使用流式出口；有界读取使用 scan_page。
    fn scan(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Vec<ExportRecord>;

    fn reverse_scan(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Vec<ExportRecord>;

    fn prefix_scan(&self, prefix: &[u8]) -> Vec<ExportRecord>;

    fn seek(&self, key: &[u8]) -> Option<ExportRecord>;

    fn seek_prev(&self, key: &[u8]) -> Option<ExportRecord>;

    /// 游标式有界扫描：`after`（**不含**）之后最多 `limit` 行，返回 `(行, 下一页游标)`。
    /// 游标 `None` = 已经到底。
    ///
    /// 为什么要有它：[`KvTxn::scan`] 返回的是 `Vec`，`scan(start, None)` 的代价与内存占用都按
    /// **"从 start 到库尾"** 算 —— 而"下一页"这个动作本来只需要 `limit` 行。翻页、迁移、
    /// 导出这类场景请用它，别拿 `scan` 的全量返回值切片。
    fn scan_page(&self, after: Option<&[u8]>, limit: usize)
    -> (Vec<ExportRecord>, Option<Vec<u8>>);

    /// 分页搜索。⚠️ `query.page` 是 **offset 式页码**：第 N 页要从库头**跳过 `N × page_size` 行**
    /// ⇒ 单页单价 ∝ 页码，连翻 K 页是 **O(K²)**。
    ///
    /// 所以**"翻页 / 深翻"的默认姿势是 [`KvTxn::scan_page`]（游标），不是这里**。实测两条门
    /// （台架 `deep_paging_posture_ab`，release，页宽 100，四家 × 200k/600k）：
    /// - **跳到同一页，两条门同价**：游标 ÷ offset = **0.93 ~ 1.35×** ⇒ 换成游标不收学费；
    /// - **连翻 50 页，offset 贵 29 ~ 43 倍**（600k 档 bplus 42.6×、memory 41.6×、lsm 32.2×、bitcask 29.0×）。
    ///
    /// 那为什么还留着 offset：随机跳页（用户点"第 87 页"）只有这里能做，游标给不了 —— 这一条是
    /// **能力**不是便利，删不掉。M51 已把"深翻末页"的瞬时内存从 188 MB 削到 0.6~0.8 MB，
    /// 但**时间那半属于 offset 的语义**，不是实现浪费 ⇒ 要快就换门。
    fn search_keys(&self, query: &SearchQuery) -> SearchPage;

    /// 从**库尾**取 `page_size` 行（免 total，按 key 字节序升序交出）。
    ///
    /// 为什么要单独开这条门：[`KvTxn::search_keys`] 的 `query.page` 是 **offset 式页码** ⇒
    /// "末页"在不知道 total 时只能从头跳过前面所有行才够到，那是 O(库)。这条门反过来走：
    /// 每拿到一把逻辑键就把上界收紧成**它本身**，而版本行合同 `raw ‖ version_be > raw`
    /// 让那一组整体自动出局 ⇒ 代价 = `page_size` 次反向定位、每次 O(块)，**不随库大小涨**。
    ///
    /// 默认实现 = [`KvTxn::reverse_scan`] 截尾（O(库)，但**正确**）。⚠️ **M82 起四家内置引擎都
    /// 覆盖了它**（各家的 `kv_ops::search_last_page` 走反向逐步退：KeyDir/叶/BTreeMap 的尾键当起点，
    /// 循环在 [`crate::common::back_n_with`]）；这一支默认只留给将来新加的引擎，那时它就是那个引擎
    /// 的**诚实读数**，不是量具坏了。别把默认实现改成"用 offset 翻到最后一页"：免 total 时根本
    /// 没有末页下标可算，`skip = usize::MAX` 那种写法是一行都进不了窗口、还照样走完全库。
    ///
    /// `total` / `total_pages` 恒 `None`（没数），`has_next` 恒 `false` —— 已在库尾，后面没有东西；
    /// 合同看 [`SearchPage::has_next`]，它问的是"**后面**还有没有"，别拿"前面还有"冒充它。
    fn search_last_page(&self, page_size: usize) -> SearchPage {
        let mut items: Vec<ExportRecord> = self
            .reverse_scan(None, None)
            .into_iter()
            .take(page_size)
            .collect();
        items.reverse(); // reverse_scan 是降序，而 `SearchPage::items` 的合同是升序
        SearchPage {
            items,
            total: None,
            page: 0,
            page_size,
            total_pages: None,
            has_next: false,
        }
    }

    fn key_count(&self, prefix: &[u8]) -> usize;

    // ---- TTL ----

    /// `None` = key 不存在；`Some(None)` = 永久；`Some(Some(d))` = 剩余时间
    fn get_ttl(&self, key: &[u8]) -> Option<Option<Duration>>;

    fn refresh_ttl(&self, key: &[u8], ttl: Duration) -> bool;

    /// 去掉 TTL 变永久
    fn persist_key(&self, key: &[u8]) -> bool;

    /// 物理清掉本快照可见的已过期 key，返回清理条数
    fn purge_expired(&self) -> usize;

    // ---- 导出 / 结束 ----

    fn export_latest_visible(&self, include_deleted: bool) -> Vec<ExportRecord>;

    /// **流式**导出（M44）：折一行交一行，`on` 返回 `false` 当场收手；返回交出去的行数。
    /// 与 [`KvEngine::export_latest_visible_for_each`] 同一条合同，只是挂在事务上
    /// （事务本身带快照，"导出这个事务看得见的东西"不必再套一层事务）。
    ///
    /// ⚠️ 默认实现先物化再逐行交（不给既有实现者制造破坏性变更）；四家覆盖成真流式。
    fn export_latest_visible_for_each(
        &self,
        include_deleted: bool,
        on: &mut dyn FnMut(ExportRecord) -> bool,
    ) -> usize {
        let mut handed = 0usize;
        for rec in self.export_latest_visible(include_deleted) {
            handed += 1;
            if !on(rec) {
                break;
            }
        }
        handed
    }

    /// 兼容接口：内置引擎在提交失败时 panic。需要恢复处理时使用 try_commit。
    fn commit(&self);

    fn rollback(&self);
}
