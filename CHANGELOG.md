# 更新日志

版本号遵循 semver。`0.x` 阶段的次要版本号变更**可以包含破坏性变更**，
每次都会在这里列出「升级要改什么」。

---

## 0.4.0（2026-10-15）

这一版是为了一次现网事故：**CPU 随进程 uptime 线性升高**。根因不在某个引擎的存储结构，
而在四引擎共用的 MVCC 层 —— 七条「随时间恶化」的通道逐条堵死。
换引擎只是把爆发时间从「LSM 一天」推到「Bitcask 一个多月」，所以四条通道在每个引擎里都各自断开、
各自留了守卫，没有"先修一家止血"。

磁盘格式**未变**：0.3.0 建的库可以直接用 0.4.0 打开。

### 破坏性变更

- **`VacuumStats` 新增两个字段**：`skipped_active_txn: bool`、`rows_scanned: u64`。
  用结构体字面量构造它的代码要补字段（读字段的代码不用改）。
- **`KvEngine` trait 新增三个必需方法**：`health()`、`set_vacuum_auto_slice(rows)`、
  `vacuum_auto_slice()`。自己实现过这个 trait 的（不是用本仓库四个引擎）需要补实现。
- **`vacuum()` 的水位线语义变了**：以前取 `min(活跃事务 id)` 精确算，现在**只要有任何活跃事务
  就整轮跳过**并返回 `skipped_active_txn = true`。旧算法会裁掉活跃事务快照仍需要的版本 ⇒
  那个事务**静默读到旧值**（不报错、不 panic）；宁可回收推迟一轮，也不留静默数据错误。
  ⇒ 如果你原先靠「vacuum 一定会裁掉东西」写逻辑，改成看 `skipped_active_txn` 再决定重试。
- **`Transaction` 实现了 `Drop`**：忘了 `commit()`/`rollback()` 的事务在离开作用域时自动回滚
  并 `log::warn!`。以前它会永久钉在活跃表里，把 `xmin` 钉死 ⇒ vacuum 从此什么都裁不动。
  ⇒ 依赖「未终结事务留着」的行为要改（这种依赖本身就该视为 bug）。
- **类型位置**：`MvccHealth` / `VacuumRound` / `VersionedKey` / `encode_key` / `decode_key`
  的唯一定义移到 `crate::common`，各引擎模块仍然 `pub use` 转发 ⇒
  `crate::bitcask::encode_key` 这类既有路径**不受影响**，只有直接 import `crate::common::*`
  的写法会遇到重名。
- **`KvTxn` 新增必需方法 `scan_page(after, limit)`**、**`EngineError` 新增变体
  `MigrateAborted{key, reason}`**（M17）。前者对"自己实现 `KvTxn` 的人"是破坏性的（用本仓库
  四个引擎的不受影响）；后者对 `match EngineError { .. }` 全模式匹配是破坏性的。

- **`EngineConfig` 新增四个字段**（M13）：`lsm: Option<LSMTreeOptions>`、
  `bitcask_max_file_bytes: Option<u64>`、`wal_checkpoint_bytes: Option<u64>`、
  `vacuum_auto_slice_rows: Option<u64>`。用**全字段字面量**构造它的代码要补
  `..Default::default()`（读/写单个字段的代码不用改）。
- **`KvEngine` trait 再加四个必需方法**（M13）：`vacuum_bounded(after, budget_rows)`、
  `set_wal_checkpoint_limit(bytes)`、`wal_checkpoint_limit()`、`tuning()`。自己实现过这个
  trait 的（不是用本仓库四个引擎）要补实现。其中 WAL 阈值那两个**返回 `Result`**：
  `memory` 没有 WAL，给它设阈值返回 `Unsupported{engine:"memory", op:"wal checkpoint limit"}`
  而不是"设置成功" —— 门面上以前没有这条转发，下游只能绕开门面直接摸 `crate::lsm_tree::MVCC`，
  而那正是 wrapper 复制的起点。`open_engine_with` 里若给 `memory` 传了 `wal_checkpoint_bytes`
  也**当场失败**（不静默忽略）。
- **`SearchPage` 的 `total` / `total_pages` 变成 `Option<usize>`，并新增 `has_next: bool`**（M15）；
  `SearchQuery` 新增字段 `need_total: bool`（构造函数默认 `true`，行为与旧版一致）。
  ⇒ 读 `page.total` 的代码要改成 `page.total.unwrap()` / `unwrap_or_default()`；
  构造 `SearchQuery { .. }` 全字段字面量的代码要补 `need_total`（用
  `SearchQuery::contains(..)` / `::prefix(..)` 的不受影响）。
  为什么值得动 API：`total` 以前**总是**数一遍全库，于是"翻页"这个动作的代价与库总量同阶
  （20000 键时首页也要 100 ms 量级），而调用方真正要的只是"还有没有下一页"。
  现在 `without_total()` 把这件事分开计价，`total = None` 如实表示"没数"，
  `has_next` 由"多取一条"探出来（代价 = 一条记录）。


- **`WriteSide::wal_begin` 这个槽被删掉；WAL 里不再新增 `Begin` 记录**（M25）：
  自己实现过 `WriteSide` 的（不是用本仓库四个引擎的）要把那条 `fn wal_begin` 删掉 —— 它是**带默认实现**
  的方法，所以只有主动覆盖过的人才受影响。**磁盘格式没动**：`WalRecord::Begin` 变体与解析分支保留，
  0.4.0 建的库里那些老日志照样读（守卫 `plan_is_identical_with_or_without_begin`）。
  为什么删：这条记录不承载恢复信息（`max_xid` 与 REDO/UNDO 全从 `Write`/`Commit`/`Abort` 取），
  而 M18d 实测"整步作废 ⇒ 全量测试一条都不红"= **它一直是个没有守卫覆盖的假设**。

- **`LSMTree::scan_latest_light_window` 与 `SstFile::light_view_break_from` 被删除**（M29）：
    两个都是 `pub`，且**在已发布的 0.4.0 里存在**，所以直接调过它们的人要改。
    替代品是既有那个 `LSMTree::light_iter_range(from, to, snap_seq)` —— 惰性 k 路归并，
    调用方自己决定何时收手（`vacuum_bounded` 就是这么改的）。
    ⚠️ 这不是"顺手清理"：`scan_latest_light_window` 的切法是「**每个源各取满 `预算` 行**」，
    一片的代价因此 = `源数 × 预算` = 随库涨（实测 24 个 SST 时交出 2048 行、折 22000 把键），
    留着它等于留着一个"看着像原语、实际是陷阱"的入口。四家门面的 `vacuum_bounded` 签名与语义**没变**。

### 新增

- **自动回收，默认开着**：提交时给 `vac_pending` 记账，`begin_transaction()` 发现攒够一个切片
  宽度就当场裁一片。切片宽度默认 **16 Ki 行**（四引擎同一个值），`set_vacuum_auto_slice(0)` 关闭。
  触发量 = 切片宽度 ⇒ 回收速率与产生垃圾的速率 1:1。
- **`vacuum_bounded(after, budget_rows)` + `VacuumRound`**：带游标的有界回收，多轮跑完与一次
  全库 `vacuum()` 等价。存在的理由：`vacuum()` 会把 `kv` 锁按到整库扫完，单核机器上那一刻
  点读点写全部排队。blob 重写 / `merge()` / LSM 的 xmin 过滤**只在全库轮次**做
  （判据是 `budget_rows == u64::MAX`，不是"游标回到库尾"）。
- **`health()` / `MvccHealth`**：`total_rows` / `live_keys` / `max_chain` / `rows_above_one` /
  `active_txns`。`max_chain` 就是这次事故唯一的先行指标（现场那条链堆到 23.8 万版），
  以前只能人工数日志。四引擎口径一致，门面层转发。
- **WAL 自动收口**：增长到阈值（默认 16 MiB，`set_wal_checkpoint_limit`，设 0 关闭）就在下一次
  提交后自动 checkpoint。顺带修掉一个更糟的 bug：原先 `append(Checkpoint) → sync → truncate()`
  的顺序**把刚写的 Checkpoint 自己抹掉了**，于是"用 WAL checkpoint 拿 next_version"那条快路
  永远是死代码。现在先截断、再把 Checkpoint 写成新 WAL 的第一条。
- **`MVCC::try_open`**（三个磁盘引擎）：错误以 `io::Result` 返回，取代直接 panic。
- **`src/common/kernel.rs`：事务内核里四份逐字重复的逻辑收成一份**（`visible_at` 快照可见性判据、
  `HealthFold` 链长折叠、`VacuumScheduler` 自动回收的记账/取账/游标、`is_unfinished` 漏网事务判据）。
  内部 API，不影响调用方；收敛的边界是"是否涉及存储布局"——遍历原语、早停方式、各引擎的量尺
  仍然一家一份（README 的「哪些只剩一份,哪些必须四份」有表）。
- **门面 trait 加 `Send`/`Sync` 上界**：`KvEngine: Send + Sync`、`KvTxn: Send`、`KvBulk: Send`。
  在此之前 `Box<dyn KvEngine>` 既进不了 `Arc` 也搬不过线程，交易进程那种"开库线程 +
  心跳线程 checkpoint + 工作线程读写"根本写不出来 —— 这就是下游在应用里又包一层 trait 的
  直接原因。`KvTxn` 刻意不要求 `Sync`（内部有 `Cell` 与写集；共享一个事务对象没有语义）。
  由 `facade_is_usable_across_threads` 在编译期钉住：trait 上界被人改窄就编译失败。
- **类型化选型 + 一份配置**：`EngineKind`（`parse` 兼容别名、`as_str`、`needs_disk`、
  `is_production_ok` —— 只有一个 false：`memory` 没有持久化）、`StorageConfig`
  （`STORAGE_ENGINE` / `STORAGE_DATA_DIR`、`engine_dir()`、`ensure_dirs()`、`open()`）与
  `open_from_env()`。`STORAGE_ENGINE` 不认识时**报错**而不是静默退回默认值。
- **`open_engine` 的引擎名改为 [`EngineName`] trait**：枚举和字符串两种写法都收 ——
  `open_engine(EngineKind::BPlusTree, Some(path))` 与 `open_engine("bplus_tree", Some(path))`
  落到同一个引擎，拿着枚举不必再自己 `.as_str()`；`&str` / `String` / `&String` 三种所有权形态
  一起实现（`src/main.rs:43` 那种 `open_engine(&name, …)` 的顺手写法，泛型位置不会自动解引用，
  少了这个 impl 就是一堵类型错误墙）。不认识的名字仍然原样回显，不退回默认值。
  泛型只加在**自由函数**上，`KvEngine` 的对象安全不受影响。
  守卫三条：`open_engine_accepts_enum_and_string_in_every_ownership_shape`（`EngineKind::as_str`
  与 `canonical_name` 两张表的唯一汇合点）、`enum_without_path_is_path_required`（枚举 + 漏路径
  归「必须给路径」管，不能被名字解析那一步吃掉）、
  `unknown_name_is_echoed_verbatim_whatever_the_shape`。
- **crate 根补 re-export**：`EngineKind` / `EngineName` / `StorageConfig` / `open_from_env`，
  外加门面方法返回的那几个类型 `MvccHealth` / `DurabilityProfile` / `VacuumRound` ——
  之前 `db.health()` 的返回类型只能在 `storage_engines::common::` 下命名，下游被迫在根和
  子模块之间来回 import。
- **`KvTxn::incr_with_ttl`** 上门面（四引擎本来就有，之前只有各自的 kv_ops 能碰到）。

- **`src/common/wal.rs`：三份 WAL 收成一份**（原先 `diff` 实测只差 14–16 行注释与 import，`WAL_MAGIC` 与帧头本来就相同 —— 同一个磁盘格式的三份复制）。崩溃恢复是最不该有三分之一实现的地方：M3b 那个「`checkpoint()` 把刚写的 Checkpoint 记录自己 truncate 抹掉」的 bug，形状就是「一份改了、另两份照旧」。合并时补了一条三份原本都没有的守卫 `test_corrupt_tail_is_discarded_but_good_frames_survive`（半帧损坏必须只丢坏尾，不牵连前面的完整记录）。
- **`src/common/disk.rs`：三份逐字重复的字节级文件工具收成一份** —— `DiskFile`、`FileLock`、
  `crc32`（`diff` 实测原先 bitcask 与 lsm 的 FileLock 差 **0 行**，crc32 差 0 行）。
  合并的理由是这三样恰好都是**崩溃语义**的载体：`sync()` 是不是 `sync_all`、Windows 的
  `share_mode(0)` 与 unix 的 `flock(LOCK_EX|LOCK_NB)` 谁在挡第二个进程、锁文件里写不写 pid ——
  修一处漏两处就是"一个 bug 四个实例"。各引擎仍然只保留自己的**锁文件路径规则**。
  顺带把 `read_page`/`write_page` 从"隐含 4096"改成显式声明：文件要用
  `DiskFile::open_paged(path, block_size)` 声明定宽块，没声明就按 page_index 读写会
  **明确报错**而不是猜一个页大小（猜错 = 把数据写到错误偏移，看起来一切正常）。
  这条立刻在 dblwr 撕裂用例上抓到一处未声明的构造。引擎侧路径
  `crate::<引擎>::storage::DiskFile` 等通过 re-export 保持不变。
- **`KvEngine::durability_profile()` / `MVCC::durability_profile()`**：把四家崩溃安全的差异作为
  数据吐出来（`DurabilityProfile`：有无事务 WAL、commit 是否 fsync、数据记录/大 value 有无校验和、
  是否原地页写与双写缓冲、全库重写残骸能否认回），`Display` 只报缺口。
  刻意不给 `Level(1..=5)`：一个整数会把"数据记录没校验和"（静默损坏发现不了）和"根本没有 WAL"
  （没有持久化点）糊成同一个数字。差异由 `durability_profiles_are_honestly_different` 逐家钉住，
  所以补了 CRC 却忘了改文档这种事会当场红。
  如实记录的三条缺口：bitcask 主日志记录无校验和（补它要改磁盘格式，本仓库不采纳）、
  LSM 的 `blobs/*.blob` 是裸文件、LSM 的 MVCC 路径关掉了 mem.wal（持久化只靠事务 WAL）。
- **`EngineConfig` 装齐四家的旋钮 + 参数能读回**（M13）。在此之前门面只有 B+ 树的
  `order` / `max_cache_pages`，bitcask 的分片上限、LSM 的 `LSMTreeOptions`（memtable 阈值、
  块缓存容量、mmap、L0 三档触发、blob 阈值、SST 目标大小）、WAL 收口阈值全留在各引擎的
  `MVCC` 里，下游要调就得绕开门面自己构造 —— 基座该被依赖而不是被复制，缺一个入口就多一层复制。
  现在：`EngineConfig { order, max_cache_pages, lsm, bitcask_max_file_bytes,
  wal_checkpoint_bytes, vacuum_auto_slice_rows }`，`open_engine_with` 一路接通到
  `MVCC::try_open_with`（bitcask 收 `Option<u64>`，lsm 收整份 `Option<LSMTreeOptions>`，
  没给的字段留在 `for_mvcc()` 档上 —— 常规写法是 struct-update 从 `for_mvcc()` 抄基线）。
  `LSMTreeOptions` 与 `DEFAULT_MAX_FILE_BYTES` 提到 crate 根 / `crate::bitcask` 可见。
- **`search_keys` 的窗口分页不再为 `total` 走全库**（M15）。计数尺（本引擎"走了多少把键/页"，
  2000 键 / 页宽 20）：bitcask 2000→**22**、lsm 2000→**22**、memory 2000→**22**、
  bplus 62→**2** 页。release / 20000 行台架的墙钟（页宽 100，首页）：bitcask 48.1→**0.25 ms**、
  bplus 25.6→**0.14 ms**、memory 24.3→**0.16 ms**、lsm 64.4→**18.2 ms**。
  ⚠️ 当时把 LSM 那 18.2 ms 归给"每个 SST 源各初始化一次（读该源第一块）"，判成另一条通道、
  进了未修队列 —— **那个归因是错的**（只归对了一半）：源启动确实要一块，但真正吃掉 18 ms 的是
  **归并先把每个源整篇物化**。见下面 M15b，改完是 **0.27 ms**。
- **LSM 的跨源归并改成惰性拉取（M15b）**：新增 `LSMTree::light_iter_range(...) -> LightMerge`
  （min-heap k 路、一次只往前拉一个键，源序号小=更新这条取胜规则与 `merge_streaming` 逐条一致）
  与 `SstFile::light_iter`（一块一块读、键越过 `to` 立即收手）；`scan_latest_light_range` 的
  "每个源先 `Vec` 化再拼"退役，`scan_candidates` 改为直接走惰性区间 ⇒ 调用方的 `budget` 一停,
  **块读就跟着停**。release / 20000 行台架首页（页宽 100）：**18.2 ms → 0.27 ms**，与另三家
  （0.10～0.25 ms）同档；对"要 total"的 60.1 ms 省 **222×**（M15 当时只有 3.5×）。
  - 计数尺同步变了才抓得住：`test_search_window_costs_the_window_not_the_library`（lsm 那条）
    在键数之外**加比块读** —— `window_blocks <= 2 × (window_cost + 1)`。改前改后键数都是 22,
    只有块读数从 53 掉到 22（全库总共 ~32 块 ⇒ 53 里有一整篇是全库扫出来的）。
    把归并退回"先 `collect` 成 `Vec`" ⇒ 22 把键配 53 块、当场红。
  - 仍然躲不开的是"**源数 × 一块**"：每个可能相交的 SST 都要读进窗口起点那一块才知道有没有货
    （min/max 先挡掉整篇不相干的）⇒ 首页代价是 **O(SST 文件数)**，不随行数涨 —— 这一半才是
    M15 当时说的源启动开销，量级已经很小。
  - 顺带受益的是同一批调用方：`scan_page`（M17）、`vacuum_bounded`、前缀 `search_keys`
    以前都要为一个小窗口读完整个 SST，现在按窗口收手。
- **游标式有界扫描 `KvTxn::scan_page(after, limit)`（M17）**：返回 `(行, 下一页游标)`，
  `after` 是**不含**，游标 `None` = 到底。实现是 `common::store::scan_page` 一份泛型（建在
  M18a 的 `VersionedStore` 上），四家各一行转发。它治的是 README 里那条"扫描返回物化 `Vec`":
  `scan(start, None)` 的代价与内存按"从 start 到库尾"算，而翻页的人只要 `limit` 行。
  ⚠️ 游标取**折出来的行**的键，含刚过期、对用户不可见的那些 —— 不推进游标就会下一次又从
  同一批过期行撞起，永远走不动（这是实现里唯一不显然的一处）。
- **`storage_engines::migrate(&from, &to, page_rows)`（M17）**：跨引擎在线搬家，不必经 CLI 的
  中间文件。读侧 `scan_page` 一页一页走、写侧一页一个事务（`commit` 每页一次，不是每行一次
  fsync）；带 TTL 的行按**剩余时间**续，值用 `set_raw` 写（源侧读回来的是已剥信封的用户字节，
  直接进目标侧的 `set` 会踩信封约定）；历史版本与墓碑不带（目标是干净库，`max_chain` 从 1 开始）。
  目标侧写被拒 ⇒ 回滚当前页并返回 `EngineError::MigrateAborted`。
- **`src/common/store.rs`：版本行折叠收成一份（M18a，"基座"那一刀的第一步）**。新增
  `VersionedStore` trait（**只有两个必需方法**：`scan_candidates` 我这家的键怎么折、
  `materialize` 一组的字节在哪儿）+ 泛型 `collect_rows` / `search_window` / `search_bounds`。
  四家的 `collect_latest_raw_in` 与 `collect_for_search` 从此是"实现 trait + 一行转发"：
  界下推、组边界收口、键级过滤先于取值、窗口化、剥 TTL 信封、墓碑/过期的取舍 —— 这些**判定**
  原先在四个文件里各写一遍，现在一份。净 −80 行，且新增一个引擎只要实现那两个方法。
  - ⚠️ 中途把 `materialize` 设计成 `Option<Vec<u8>>` 是**错的**：lsm 今天把"视图里有、值里读不到"
    整行跳过，而墓碑在 `include_deleted` 下要出现在结果里 —— 合成一个 `None` 就把这两种混成一种，
    那就是"统一"顺手改语义。所以最终是**三态** `RowValue{Value, Tombstone, Missing}`。
  - 边界仍然守得住：遍历原语、早停方式、各家量尺留在各自文件里（"是否与存储布局有关"这条判据）。
  - 验收凭的就是既有那 262 条测试：scan / search / export / vacuum / 窗口分页 / 计数守卫四家各自
    跑过一遍，折叠换实现后全绿 —— 它们本身就是跨引擎对拍台架，不再另造一套。
- **`src/common/txn.rs`：事务内核收成一份（M18b 第一刀，`memory` 已用上）**。`TxnCore` 装的是
  四家算得**同一件事**的那段：begin（版本号 `next_version++` + 拍活跃快照 + 登记进活跃表）、
  `write`（冲突判定 → 写集登记 → 落行，这个顺序不能换：判定要在登记之前，否则回滚会去撤一把
  根本没写的行）、`commit`（销账 + 给自动回收记一笔近似账）、`rollback`（按写集**物理删行**，
  不是写墓碑）、`undo`（批量写撞墙时把已落下的那几把退回 + 从写集摘掉）、`unfinished`（`Drop`
  兜底判据）。引擎侧只剩 `WriteSide` 三个方法：`latest_version` / `put_versioned` /
  `remove_versioned` —— "这一行的字节住在哪"。bulk 会话另有一份 `BulkCore`（一个会话一个版本号、
  不登记活跃表、`finish` 幂等 + `Drop` 兜底）。
  - 顺手修掉一处真错：四家的 `rollback_keys` 都写成**先锁存储、再 `try_lock` 活跃表**，拿不到就
    跳过摘写集 —— 那是内核里唯一的反向嵌套点（`write`/`rollback`/`vacuum_bounded` 走的都是
    `active_txn → store`），跳过摘写集则让随后的 `rollback` 去删一批早已不存在的行、`commit`
    的回收账多算。统一进 `TxnCore::undo` 之后锁序正向且**必达**。
  - ⚠️ **说清没做的**：`bitcask` / `bplus_tree` / `lsm_tree` 还各留一份自己的副本。写侧要动的资源
    三家不同（纯内存 / WAL+blob / WAL+blob+页容量判定），所以按引擎各切一个里程碑搬，每搬一家
    就往 `WriteSide` 上加一个口子。净行数此刻是 **＋272**（内核 232 + 对拍测试 96 − memory 少掉
    的 111）：一家用上看不出收益，收益要在第三家搬完才兑现（每家还各留 ~100 行内核本体）。
    —— **当天就被 M18c 更新**：口子（槽）一次加齐了，`lsm_tree` 同一天搬上，所以"三家各留一份"
    当天变成"两家各留一份"；`＋272` 那笔账也从"一家用上"变成"两家用上"。
  - 守卫 `txn_kernel_agrees_across_engines`：同一段事务脚本在四家跑出**同一个终态**（逐字节比
    对 scan 结果）+ 跑完 `active_txns == 0`。为什么必须有它：`memory` 一家搬上共用内核时另三家
    还是各自的实现，而每家原有测试只测自家那一份 —— 内核写歪了单引擎测试看不见。四条断言各自
    证明会红（实测）：去掉冲突判定 ⇒ 红在"t2 必须是写冲突"；不登记写集 ⇒ 红在"丢弃的事务必须
    等于没写"；`undo` 变空操作 ⇒ 红在"部分撤销必须把 first 也退回"；`unfinished` 恒 `false`
    ⇒ 红在"不许留下活跃事务"。
- **内核的写序列补上 WAL / blob / 页容量三个槽，`lsm_tree` 搬上（M18c）**：
  `TxnCore::write` 从此是**整条八步序列**（冲突判定 → `encode_value` → `row_fits` → `wal_begin`
  → `wal_write` → 写集登记 → `put_versioned`），后四步全是**带默认实现的槽**：没有 blob 的人家
  不实现 `encode_value`（默认原样）、定长页之外不实现 `row_fits`（默认装得下）、没有 WAL 就
  不实现 `wal_begin` / `wal_write`（默认成功）。于是内存引擎给的守卫只有三个必需方法，
  LSM 多给两条 WAL，而"哪一步在什么之后"这份顺序只有一份实现。
  - `commit` 的收尾另立 [`WalTail`] trait而不是并进写侧：commit 只需要 WAL，硬要它交一个写侧
    守卫就等于让 LSM 在 fsync 期间一直握着 kv 锁把读者全挡在门外（改前就是先释放再收尾）。
  - 一处行为差别如实记：容量判定（`row_fits`）提到 `wal_begin` **之前**，所以 bplus 那种
    "被拒收的超大行"从此连 `Begin` 都不落（原先会留下一条开张没写的 `Begin`）。它不影响恢复
    —— 见下面第三条。
  - **三条变异测试**（每条都要证明会红）：① `commit` 无视"进过 WAL 才收尾" ⇒ 红在
    `test_read_only_transaction_touches_no_wal`；② 把 `wal_write` 挪到改库之后（崩溃安全
    不变量的反面）⇒ 红在 `test_disk_persist_across_reopen` + `test_export_latest_visible`；
    ③ **`wal_begin` 整条去掉 ⇒ 265 条全绿**。第三条是本刀的**发现**而不是遗漏：
    `analyze_recovery` 里 `Write` 走的也是 `open_writes.entry(xid).or_default()`，`Begin` 建的
    空条目不承载任何信息，`max_xid` 也从 `Write` 就够。所以 WAL 里的 `Begin` 目前是**格式的
    框架**、没有守卫覆盖 —— 谁要动它得先决定"要不要让它承载信息"，不能当成已验证的行为。
  - 复测（release / 20000 行，同一台架）没有引入回归：LSM 灌 20000 行(事务) 52.3 → 55.7 µs/行、
    点读乱序 13.1 → 15.0、有序 4.9 → 4.1、首页(免 total) 357.6 → 360.2 µs、bulk 6.1 → 5.9 µs/行；
    memory 灌行 5.0 → 4.9、点读 2.2 → 2.2。本机墙钟 run 间 ±2×，这些全在噪声带内。
  - 还没搬的：`bitcask`（要 `encode_value`）、`bplus_tree`（要 `encode_value` + `row_fits`），
    各自一个里程碑。
- **`bitcask` 搬上事务内核（M18d）**：写侧守卫 `TxnSide { wal, blob, kv }` 按内核要求的顺序
  （活跃表 → wal → blob → kv）把三把锁拿好，用到的槽是 `encode_value`（大 value 走 blob 旁路，
  失败必须在落 WAL **之前**判掉，否则 WAL 里留着一条指向不存在字节的记录）与 `wal_begin` /
  `wal_write`；`commit` 的收尾换成只锁 wal 的 `WalOnly` —— fsync 期间不该握着 kv/blob 把读者挡在门外。
  - **接上一条真漏**：`Bitcask::insert` 返回 `io::Result`，原先只 `eprintln!` 一声就丢掉，
    而写集在那之前**已经登记** ⇒ 库里没有这一行、写集里却留着，随后 `rollback` 去删不存在的行、
    `commit` 的回收账多算一条。内核因此把 `WriteSide::put_versioned` 改成**返回 bool**，并把
    写集登记挪到落行**之后**：只有真落进库的行才记账。
  - ⚠️ **三家共用的尺子修了一条**：`test_read_only_transaction_touches_no_wal` 原先拿
    **WAL 字节数**判"写入有没有进 WAL"，而只落一条 `Commit` 也能让字节涨。实测把 `wal_write`
    改成"不 append、直接返回成功"，三家**全绿** —— 也就是"`Write` 记录整个没写"这种形状一直没被
    量过（这条洞是 M18d 的变异测试撞出来的，不是搬内核搬坏的）。现在三家都按**记录类型**数
    `WalRecord::Write`（`bitcask` / `lsm_tree` 数 `wal_write` 槽、`bplus_tree` 数它自己的直接
    append），同一处变异打上去三家各自红（逐家实测）。
- **`bplus_tree` 搬上事务内核（M18e）—— 四家的写侧从此一份**：写侧守卫按 `活跃表 → wal → blob → kv`
  拿好三把锁，这一家是四家里唯一**用满** [`WriteSide`] 槽的：`encode_value`（blob 旁路）+
  `row_fits`（定长页容量判定），而 `put_versioned` 直接把 `BPlusTree::insert` 的 `bool` 交给内核
  （原先那条 `false` 被丢掉过，M14 之后它有了返回值通道，现在再接到了记账上）。
  - `row_fits` 槽里保留那句 `log::error!` —— 它是"拒收一行"唯一的对外说明（`false` 分不清冲突
    还是拒收，门面没为它开 `EngineError` 通道），搬的时候容易当成冗余日志删掉。
  - 内核把容量判定放在 `wal_begin` **之前**，于是被拒收的一行连 `Begin` 都不再留（原先会留下一条
    "开张没写"的 `Begin`）。守卫里新增 `begins == 1` 那条钉住它：把顺序退回旧形状实测红在
    "实际 2 条"。`BPlusTree::row_fits` 的签名顺手从 `&Key` 换成 `&[u8]`（内核给槽的是切片，
    而它本来就要 clone 一份来算字节账）。
  - 两处变异实测：`row_fits` 槽恒真 ⇒ 红在 `test_oversized_row_is_refused_before_the_wal`；
    `encode_value` 槽退回原样 ⇒ 红在 `test_large_value_blob_roundtrip` +
    `test_incr_and_incr_with_ttl` + `test_ttl_and_purge`。
  - 复测（release / 20000 行同一台架，与 M15b 那一轮同形状对照）没引入回归：`更新 2000 键`
    69.5 → **58.7 µs/次**、`search 首页(免 total)` 259.1 → **149.3 µs**、`health()`
    17.1 → **14.0 ms**、`vacuum() 全库` 311 → **244 ms**、`bulk 灌 2000 行` 38.2 → **34.9 µs/行**
    （本机墙钟 run 间 ±2×，这些只报方向；`灌 20000 行(事务)` 63.1 µs/行、点读乱序 14.3 µs/次
    与上一轮同档）。
  - **四家总账**（M18b~M18e，`src/` 累计 +1142 / −430，其中测试与文档占大头）：
    `write` / 写冲突检测 / `commit` / `rollback` / `rollback_keys` / bulk 版本编排现在只有
    `common/txn.rs` 一份实现；各家剩下的就是自己那几个槽（内存 3 个方法、LSM 5 个、
    bitcask 6 个、bplus 7 个）。新增第五个引擎要写的东西从"抄 100 行事务层"变成"实现这几个方法"。
- **修掉 LSM 灌数据的 O(N²)（M19）**：`LSMTree::maybe_flush_mem` 原先每次插入都
  `self.mem.values().map(|v| v.len()).sum()` 一遍 —— 只为回答"要不要 flush"。改成**增量计数器**
  `mem_entries`（`mem_insert_no_wal` 里新增版本时 +1、同 seq 覆盖不加、`flush_mem` 清零），
  `maybe_flush_mem` 与 `stats()` 都改读它，**判定语义一字未动**（阈值比的仍是"版本条数"）。
  - 怎么撞出来的：200k 大规模基线（`SCALE_ROWS=200000`）显示 lsm 灌行从 20k 的 38.8 µs/行涨到
    200k 的 **227.8 µs/行**（每行贵 5.87 倍 ⇒ 十倍量花约五十九倍时间），而 bitcask 同一项是
    **1.04× 平的** —— 所以不是机器、不是台架。新加的归因台架 `lsm_fill_attribution` 逐块排除：
    块读全程 0（前十万行 `total_sst=0`，所以**不是**"写侧冲突探测在探 SST"）、自动回收开/关
    两条曲线一模一样、三段计时里 `begin`≈1 µs 且 `commit`≈2.7 ms 恒定而 `set` 段 69 ms→1001 ms、
    **完全不经 MVCC 的裸 `insert` 曲线与灌入曲线重合**（89→1429 µs/行）⇒ 定位在存储层。
  - 复测（同一台架、release）：灌行 **20k = 14.5 µs/行、200k = 21.0 µs/行（比值 1.45×）**，
    归因整轮 82 s → **5.0 s**；顺带 `vacuum() 全库` 2050 → 1265 ms、`search 首页(要 total)`
    1086 → 687 ms（它们的落盘/重插也走同一条判定）。墙钟本机 ±2×，只报方向；
    定性结论靠计数尺。
  - 两把守卫，各自都证过会红：
    `test_mem_entry_counter_matches_slow_scan` —— 计数器与慢算法**口径对拍**（含多版本、同 seq
    覆盖、墓碑、flush 清零四个形状）；把 flush 的清零去掉 ⇒ 当场红。
    `test_flush_check_does_not_scan_memtable_per_insert` —— 复杂度尺：灌 4000 行期间"为判定走过的
    memtable 条目数"必须是 **0**；把整表 `sum` 放回热路径 ⇒ 当场红。同一条用例里**主动慢数一次**
    作为对照，证明这把尺子不是空转（`scan_mem_entry_count()` 因此是留着当对照与探针的，不是死代码）。
  - ⚠️ 顺带记下一笔还没修的：内核 `TxnCore::write` 登记写集用 `writes.iter().any(|k| k == key)`
    查重 ⇒ **单个事务内**是 O(写集²)。实测它不是灌行超线性的主导项（同一事务里第 1~200 行与
    第 1801~2000 行单价相同：1463 vs 1387 µs），但一个大事务塞几万行时它会浮出来。
- **规模台架补到 31 项/引擎，补测抓到两条新的超线性（M21 修一条、M20 修一条）**：
  新增 `multi_get` 全未命中单价、`scan_page` 翻完整库、`seek_prev`、`reverse_scan`、
  `set_with_ttl`+`get_ttl`、`vacuum_bounded` 一片、`export` 全库物化、`migrate`→memory，
  并带三条正确性断言（翻页必须正好交出 N 行 / TTL 必须 1000 个都读得回 / migrate 搬走的行数
  必须等于源库 `live_keys`）。20k 与 200k 同构建各跑一遍：
  - ⚠️→✅ **`seek_prev` 一次调用 = O(库) 的时间+内存**（→ **已由 M20 修**，见下条）：四家各一份
    `scan(None, Some(key‖0x00))` + `retain` + `pop()`。200k 实测 140~610 ms/次，而同库点读
    2.9~57 µs/次 —— 差 3~4 个数量级。这是 M3/M17 治过的"折完全库再由调用方筛"的**第四个实例**。
  - ⚠️→✅ **`scan_page` 的游标 `after` 没下推**（→ **已由 M21 修**，见下）（一条根拖三条路）：
    `scan_candidates` 只用 `start`
    定迭代起点，`after` 只是循环里的 `continue` ⇒ 翻第 K 页重走前 K−1 页，顺序翻页是
    O(库²/页宽)。每页代价实测：bplus 12.5 ms → **189 ms**（15.2×）、lsm 9.7 → 132.6 ms（13.7×）、
    bitcask 4.5 → 18.8 ms、memory 4.5 → 16.8 ms；翻完 200k 行的库 ≈ 75.7 s，而同样的库
    一次全量 `scan` 只要 0.58 s。`migrate`（bplus 36.2 → 216.2 µs/行）与 `search` 续跑共用它。
  - 反过来，这一轮也**证明了几条是干净的**：`vacuum_bounded` 一片 2048 行在 20k/200k 都是
    2.8~9.3 ms（≈1×，与库大小无关）；TTL 往返、`export` 单价、窗口 `scan` 全平。
    `multi_get` 全未命中只要 0.5~0.9 µs/键（bitcask/lsm），而命中要 9~58 µs ⇒ 未命中几乎免费，
    命中的钱全在"定位并读出值"上 —— 这把 M10 那条"点读 ∝ SST 分散度"的口径钉得更实了。
- **翻页游标下推成迭代起点（M21）—— `scan_page` 不再是 O(库²/页宽)**：`common::store.rs` 新增
  `fold_lower_bound(start, after)`，四家的 `scan_candidates` 从"只在循环里 `continue` 过滤 `after`"
  改成"把 `after` 抬成迭代下界"（`iter_meta_from` / `scan_entries_break_from` / `light_iter_range` /
  `iter_keys_in` 的起点），过滤器原样保留。
  - 为什么下界必须取 `after ‖ 0x00×8` 而不是 `after ‖ 0xFF×8`：任何 `K > after` 的版本键都
    `≥ K > after` ⇒ 一行不漏；而用 `0xFF` 上界当起点会把 `"abcd"`（比 `"abc"` 大、字节序却小于
    `"abc"‖FF…`）**整把键跳掉** —— 静默少行。`raw == after` 那一组与交错进来的更短前缀键，
    仍由原有那句 `raw <= after ⇒ continue` 挡掉（代价 = 重看游标那一组的链，摊销后是常数）。
  - 守卫四份（每家用自己的尺子）：`test_scan_page_later_pages_cost_the_page_not_the_library`，
    断言**最后一页**与首页同价。实测修完：bitcask/lsm/memory 首页 52 / 最后一页 51 把键，
    bplus 3 / 3 次页读；把 `fold_lower_bound` 退回"只按 `start`" ⇒ **四家全红**。
  - 200k 台架复测（同一构建，µs/页、页宽 500）：bplus 189 151 → **1 361**（139×）、lsm 132 601 →
    **1 522**（87×）、bitcask 18 821 → **1 495**（12.7×）、memory 16 817 → **620**（27×）
    ⇒ bplus 翻完整库从 **75.7 s 变成 0.54 s**（同库一次全量 `scan` 是 0.58 s，即翻页现在≈扫一遍）；
    `migrate` 跟着修好：bplus 216.2 → **16.3 µs/行**、lsm 167.1 → **13.6**、bitcask 27.2 → **10.6**、
    memory 27.5 → **8.0**。整个 31 项 × 4 引擎的台架从 325.7 s 跑到 **97.6 s**。
  - ⚠️ **只测首页的守卫抓不到这条**：bitcask 早就有一条 `scan_page_costs_the_page_...`，量的正是
    首页 —— 首页从库头起，游标没下推它也便宜，于是这个洞在它眼皮底下活了一整个版本。新守卫
    因此把"最后一页"写进断言名里。bplus 还额外踩到一次"余量按谁的单位给"：照抄键-counting 的
    `2*(first+PAGE)` 时，它在缺陷版上照样绿（它的尺子数页，50 行的余量够盖住 40 次页读）⇒
    按页重定成 `2*first+8` 才会红。
- **`seek_prev` 收成一份反向逐步（M20）—— 一次调用不再折回整个前缀**：四家原先各一份
  `scan(None, Some(key‖0x00))` + `retain(≤ key)` + `pop()` ⇒ 为了返回**一行**把整段前缀物化，
  代价与内存都是 O(库大小)（200k 实测单次 140~610 ms；这是"折完全库再由调用方筛"那条通道的第四个实例）。
  现在循环只有 `common::store.rs::seek_prev_with` 一份，每家只交两个口子：**反向一步**
  （mem `range_back` / bitcask `range_meta_back`（只读 KeyDir，不碰 value）/ bplus `find_back` /
  lsm `find_back` —— 都是各家现成的点读热路径原语）与**这把键活着吗**（`latest_visible_raw` 配
  新增的 `live_record_of_logical` —— 与 `scan` 同一份判定，不另写第二份，否则会出现
  "scan 认为它过期、seek_prev 认为它活着"这种两口不一致）。
  - 上界那两条规则都不显然：第一轮用 `version_key_upper_bound(key)` ⇒ **含 `key` 自己那一组**
    （语义是"≤ key"不是"< key"，`key‖0x00` 那种哨兵会把 key 自己的版本行排在界外）；之后用被拒的
    那把 **raw 本身**当上界 ⇒ 它的每行都是 `raw ‖ version_be` > `raw`，**整组自动排除**，一步跨一整把键。
  - ⚠️ 这条"反向第一个见到的 raw 就是最大的键"依赖版本键的老合同 **`version < 2^56`**（首字节恒 `0x00`
    ⇒ 短前缀键的版本行翻不到更长键之上）。**我先前判它"必须新造一把按 raw 排序的索引"是错的**：
    那个反例（`enc("ab",MAX) > enc("abc",0)`）只在合同破了的时候成立；"把正向折叠镜像一遍"判死也只对
    **不带界的镜像**成立 —— 按组退不需要收口条件，界本身就是收口。合同自己有守卫，别拆。
  - 守卫一条（四家同跑）`seek_prev_costs_a_walk_not_the_library`：库 2000 把、顶上 8 把做成死串
    （7 把删除 + 1 把**入库时就已过期**）、死串与答案键**每把都带 5 个版本**。三条断言各抓一种不重叠的
    退化，顺序是刻意的，每条都单独证过会红：
    ② 内核探针 `seek_prev_steps()` 断**精确形状** `DEAD+1` = 9 步（按组跳；实测四家都是 9）——
      把 memory 的反向一步改成交回编码键（忘记剥版本后缀）当场报 **2 步**红。
    ③ 本引擎的尺子（bitcask `keydir_visits` / lsm `light_visits` / memory `fold_keys`‖`rows_touched` /
      bplus `page_reads`‖`cache_hits`）抓"退回整段折"：老写法实测 **2017 / 2017 / 2017 / 63**，新写法
      0（memory 35）。⚠️ 预算必须按各家**单位**给 —— bplus 的 2000 把键只摊在 63 片叶子上，拿"键数"
      当预算抓不到它，改成"每步最多 4 页"（`steps*4+8` = 44）才红。
    ① 与"从 probe 往回逐把点读"的语义参考对拍，且答案必须正好是死串下面那把活键：把共用判定里的
      过期检查摘掉 ⇒ ②③全绿、①报红（把已过期的那把交了出去）。
  - 台架复测（同一构建，µs/次，20k → 200k）：bitcask 52 192→**20.6** / 382 234→**39.8**、
    bplus 32 444→**16.5** / 328 583→**98.3**、lsm 46 961→**24.9** / 610 488→**93.6**、
    memory 31 583→**10.4** / 139 960→**16.7** ⇒ 200k 便宜 **3 300~9 600 倍**，规模比值 4.4~13× 收到
    **1.6~6×**。代价单位换了：一次 `seek_prev` 现在等于**个位数次点读**（同 run 对照：1.9~6.3×），
    改之前是 **6 100~48 000×**。剩下的 1.6~6× 不是这条通道 —— bplus 是页覆盖（`max_cache_pages` 没有
    自动档）、lsm 是版本散在更多 SST，两条都是记在案的未修项。
  - ⚠️ 台架的**内存峰值**量不到这条改动的收益：`seek_prev` 改前改后都是 `峰值 +0.0 MB`，因为物化的
    `Vec` 在函数内部就析构了而采样点在调用边界上（同一次跑里"一次全量 scan"能报出 `+48 MB`，
    正因为它的 Vec 交到了调用方手里）。省掉的内存是真的（代码里不再物化），但那是**读代码**得出的，
    不是这把尺子量的 —— 记下来的同时把"尺子看不见 ≠ 没有"也记下来。
- **写集登记从 O(写集²) 收成哈希（M24）—— 一个事务塞几万行不再自我踩踏**：活跃表的值类型从
  `Vec<Vec<u8>>` 改成 `HashSet<Vec<u8>>`（`common::txn.rs` 里 `Registry` 这一个别名，四家的
  `active_txn` 字段与 `is_unfinished` 都改用它，不再各写一遍字面类型）。登记的查重原先是
  `writes.iter().any(|k| k == key)`，部分撤销的摘账原先是 `writes.retain(|w| !keys.iter().any(...))`
  ⇒ **单个事务内**两条都是 O(写集²)（与 M19 之前 LSM 的 flush 判定同一个形状，只是藏在事务里）。
  刻意**只留一份账**（不并存"顺序 `Vec` + 哈希副账"）：写集不需要顺序 —— `rollback` 是逐把自己写过的
  版本行物理删掉，删的先后互不影响；两份账就会有一份写漏。
  - 守卫 `write_set_registration_costs_a_hash_not_a_walk`（四家同跑）断的是**精确计数**：
    2010 次写 ⇒ 2010 次查重（等号是刻意的 —— 多出来那次 `contains` 也算一处退化）；
    批量写撞墙那批 49 把键 ⇒ 49 次落账 + 49 次撤账 = 98 次。尺子 `write_set_scans()` 长在唯一的
    写集入口 `register_write`/`unregister_write` 里。实测退回遍历写集：**2 019 000 次**对 2 010 ⇒ 红。
    语义没换：同一把键在一个事务里写 11 次，提交后读到最后一次的 value，其余键照样在。
  - ⚠️ **这把计数尺有一处已知的盲角，所以另配一条墙钟台架**：如果有人把查重改回遍历、**却忘了搬那行
    探针**，尺子照样读 1 次/行。新台架 `big_transaction_write_cost`（`#[ignore]`，`BIG_TXN_ROWS`）
    量的就是同一个形状的代价：一个 20 000 行的事务里，头 200 行与尾 200 行的单价。
    实测（release，µs/行）改后 bitcask 20.6→10.0、bplus 36.8→50.6、lsm 11.4→8.7、memory 1.5→1.5
    （比值 0.48~1.37×，即平的）；而"只改算法不改探针"的那份退化在 bitcask 报 **9.3 → 288.8 µs/行
    （30.9×）**当场红 —— 两条尺子互为对照，一条盯形状、一条盯代价。
  - ⚠️ 这条**不是** 200k 台架那条超线性（那边每 1000 行一事务，写集最长 1000）；它的形状是
    "下游一次导入塞一个大事务"，所以量出来的是上面那条台架，不是 `SCALE_ROWS`。
- **bplus 页缓存自动档 + 上层页钉住（M22）—— 那颗"必须有人盯页数"的旋钮不再是人守**：
  `max_cache_pages` 新增 `0` = 自动档，并成为 `EngineConfig::default()` 给的值。
  容量 = `clamp(库的实际页数, 1024, 2048)`，且在每次 `flush_all`（`flush` / `checkpoint` / bulk 收尾
  都走它）之后重新评估。传非 0 就是显式容量，自动档完全不参与 ⇒ 老代码行为不变。
  同时**根与内部节点钉住**（`PageManager::pin`，不参与换出），解析缓存超限时的策略从
  "整表 `clear()`" 改成"只丢没钉住的" —— 上层每一次点读都要用，整表丢等于每装满一批就把它们
  白丢一次。
  - 上限为什么夹在 2048 页（≈ 8 MiB）：B+ 树把整库缓存住的常驻 ≈ 数据文件本身
    （实测 200k 行 = 12129 页 ≈ 47 MB）⇒ 那是"把库复制进内存"而不是"缓存热数据"，
    这种决定该调用方做。下限停在 1024 = 要改之前那个写死值 ⇒ 换成自动档不会让任何人的工作集变小。
  - ⚠️ **钉住预算必须跟容量挂钩**（`min(1024, 容量 ÷ 2)`），因为"上层占全库多少"不是常数：
    同一份五千行的库，35 B 键时上层 3.0%（36/1210 页），3.3 KB 键时 **50%**（1305/2606 页 ——
    内部节点也装不下几个长键 ⇒ 扇出掉到 2、树深从 3 层涨到 12 层）。只按绝对页数钉，
    碰上长键就会把整个缓存钉成一动不动，叶子一页都装不下，比不钉还糟。
  - 守卫两份（各自都证过会红）：
    `test_cache_capacity_follows_the_library` —— 空库的自动档必须正好是旧默认值 1024，
    灌到 2606 页之后容量必须跟着长成 `min(页数, 2048)`；摘掉 `flush_all` 里那次重新评估 ⇒
    红在"容量还停在 1024"。同一条还断"显式传 512 就必须生效 512"（自动档不许覆盖调用方的决定）。
    `test_upper_levels_survive_a_full_walk` —— 三条各挡一种溜法：① 机制（`pinned > 0` 且
    `pinned ≤ 内部节点数`、`pinned × 2 ≤ 容量`）② 聚合代价（乱序点读缺页 ≤ 1.6 页/读，
    实测钉住 1.46、不钉 **1.75** ⇒ 撤掉钉住当场红）③ 绝对地板（冲掉工作集后单独重读一把
    早就读过的键，最多缺 2 页；实测 1 页）。⚠️ ② 与 ③ 都是**计数**，不是墙钟：这台机器上
    同一配置连跑两遍的 µs 差 30%+（且 `D:` 是 5400 转机械盘），拿 µs 当判据会假红也会假绿。
  - 效果的计数账（同一台架、同一份 1024 页容量，为了把"钉住"与"容量"两笔分开）：
    200k 行一次随机点读 **1.41 页/读 → 0.98 页/读**，重解析同样 1.41 → 0.98 ——
    少掉的那 0.43 页就是下降路上被挤掉又重新读、重新解析的上层页，剩下那一页是这次读真正要的叶子。
    20k 行本来就是 0.51 → 0.49（覆盖率已经 84.6%，没东西可省）⇒ 这条改动的受益面就是"库大过容量"那一档。
  - 规模台架复测（`SCALE_ROWS` 走门面默认档 ⇒ 自动档生效；对照的是 31 项基架那两轮同一批操作，
    M20/M21 都不碰这几条路径）。**µs/次、µs/键、MB 都是常驻**：
    | 操作 | 20k 改前 → 改后 | 200k 改前 → 改后 |
    |---|---|---|
    | 随机点读 | 28.7 → **8.5** | 53.7 → **43.8** |
    | `multi_get` 命中 | 24.9 → **6.0** µs/键 | 65.1 → **39.2** µs/键 |
    | `health()` | 23.96 → **20.66** ms | 438.2 → **357.9** ms |
    | 点读期间的常驻 | 6.8 → 8.2 MB | 4.9 → **13.3** MB |
    20k 那一档现在**正好等于台架的"全焐热地板"**（同一台架把容量开到 65536 页量到 7.8 µs/次，
    自动档给的是 8.5）⇒ 库不超过 ~8 MiB 的人不用再自己盯旋钮了。
    ⚠️ 常驻那一列就是这笔改动的**价签**：2048 页 × 4 KB ≈ 8.4 MB，实测 200k 那一档多花 8.4 MB。
    ⚠️ 规模比值反而变差（随机点读 1.9× → 5.2×）**不是回归**：小库那一档被拉满覆盖所以掉到地板价，
    大库那一档仍受 2048 页上限约束 ⇒ 两档的绝对值都在降。**判超线性要盯每单位成本的绝对值，
    比值只在你固定了"两边都没装满"这个前提时才有意义** —— 这条记进方法学。
- **LSM 块缓存自动档（M23）—— "点读单价随库长"那一段是容量，不是结构**：新增
  `LSMTreeOptions::block_cache_auto_max`，`for_mvcc()` 开成 **256 地板 / 1024 上限**。生效容量 =
  `clamp(库的 SST 块数, block_cache_blocks, block_cache_auto_max)`，在 open / `flush_one_immutable` /
  `compact_level` / `bulk_ingest_sorted` 四处重算（文件集只在这四处变），并新增
  `BlockCache::set_capacity` **就地改上限** —— 旧 `set_options` 用 `BlockCache::new` 重建，等于每改一次
  旋钮就把已驻留的块全丢掉，自动档如果沿用这条路就会自己清自己。`MVCC::tuning()` 多报两个键：
  `block_cache_auto_max`（配置）与 `block_cache_capacity`（**生效值**）。
  - 语义边界，三条都是刻意的：`block_cache_blocks: 0` 仍然**是关缓存**（自动档不参与，不许把"关"
    悄悄开成"自动"）；上限 0 = 关自动档 = 配置值即生效值（老代码行为一字不变，裸 LSM 的
    `default()`/`for_bulk_load()` 都没开）；CF 模式共享一份缓存时自动档**不动容量**（每棵树只看得见
    自己的块数，谁后算谁赢，会互相把对方的驻留块换出去）。
  - 上限为什么夹在 1024 块（40000 键那一档实测 ≈ 7.1 MB 常驻）：同一档把上限放到 4096 ⇒ 全库驻留
    （18.7 MB）、乱序点读 21.0 µs/次。残差几乎就是**未命中次数 × 一次随机读**（这台机上 ≈ 54 µs：
    (117.3−21.0)÷(1.81−0.03)）。把库整份复制进 RAM 是调用方的决定，不是引擎 defaults 该做的事 ——
    与 M22 的 B+ 树页缓存同一口径。
  - 守卫两份。`test_block_cache_capacity_follows_the_library`（把块做小到 8 条/块，让"块数"在测试里
    廉价地涨）断四件事：库比地板小 ⇒ 容量停在地板；长在中间 ⇒ 容量 = 实际块数；长过天花板 ⇒ 封顶；
    **装得下 ⇒ 第二趟乱序重读 0 未命中**，而同一条用例里把自动档关掉钉回地板，同一趟读**必须**重新
    出现未命中（后半句是尺子自检）。证红：摘掉 flush 处那次重算 ⇒ 红在
    `容量该跟着库长（块数 14，实测 4）`。
    `lru_touch_cost_does_not_scale_with_residency`（尺子 = 每次命中检查过几个槽位）实测驻留 64 → 6、
    驻留 1024 → 6；证红：在 `get` 里塞回一遍线性扫 ⇒ `7000 vs 103000`（16 倍）当场红。
  - ⚠️ **这条里程碑顺手抓到的是我自己引入的回归**：放大容量之前，`BlockCache` 的 LRU 是
    `HashMap` + `VecDeque`，每次命中都要 `order.iter().position()` ⇒ 代价 **O(驻留块数)**，而自动档
    做的正是加大驻留块数。第一版 A/B 数字里它露了出来：40000 键那一档容量 256→1024，**未命中次数
    一动不动（0.15/次），有序点读却从 22.2 涨到 36.0 µs/次** —— 省下来的未命中被自己的 LRU 吃了回去。
    改成定长槽 + 双向链表（命中/换出都 O(1)，槽表只长不缩，一个槽 ≈ 48 B）之后同一档有序读 17.1 ≈
    地板那臂的 17.4 ⇒ 平。**教训**：给一个"固定值"加自动档之前，要先问这条路径上有没有一处
    代价是按那个值长的 —— 有的话自动档会把一直潜伏的 O(n) 变成默认行为。
  - ⚠️ 台架另留了一条**否决**（写在 `get_entry_at` 的注释里，不是测试）：多源点查"在第一个含该键的
    源就收手"看着是显而易见的提速，其实不安全 —— `merge_to_ssts` 只按 `target_bytes` 切文件，切的
    粒度是**编码键**，而同一把 raw key 的每个版本都是不同的编码键 ⇒ 一个版本组会被劈进相邻两个文件；
    层内又按 `min_key` 排序、版本键大端有序 ⇒ 新版本住在**后面**那个文件。先收手是静默读回旧版本。
    （原先打算给这条性质写守卫，写完发现它在"精确编码键"层面恒真 —— 每把编码键只住在一个源里 ——
    所以删掉测试、换成注释：恒真的测试不给信息，只会让人以为这条合同有覆盖。）
  - 效果的账（`LSM_POINT_ROWS` / `LSM_CACHE_AUTO` 台架，release，**同构建、同库目录、只换这一档**，µs/次）：
    | 规模（块数 / SST） | 容量 | 乱序点读 | 未命中/次 | 有序点读 | `multi_get` | 一次全量 scan | **启动读 4 条** |
    |---|---|---|---|---|---|---|---|
    | 5002 键（314 / 2.2 MB） | 256（改前） | 27.9 | 0.39 | 10.6 | 12.7 | 10.1 | 57.5 |
    | 同上 | **314**（自动档） | **8.5** | 0.03 | **7.2** | **6.4** | **5.0** | **11.5** |
    | 40000 键（2681 / 18.7 MB） | 256（改前） | 117.3 | 1.81 | 17.4 | 17.4 | 15.1 | 167.5 |
    | 同上 | 1024（自动档，封顶） | 90.6 | 1.24 | 17.1 | 20.1 | 18.7 | 131.2 |
    | 同上 | 2681（上限放到 4096，仅诊断） | 21.0 | 0.03 | 9.9 | 9.5 | 7.6 | 20.2 |
    | 200000 键（7941 / 54.8 MB） | 256（改前） | 292.9 | 3.42 | 37.6 | 32.6 | 16.7 | **1027.5** |
    | 同上 | 1024（自动档，覆盖率 12.9%） | 209.0 | 2.53 | 26.8 | 27.8 | 12.2 | **289.5** |
    块/次在各档都是 **4.00**（200k 是 6.24）、条目比较/次 128（200k 是 82）—— 也就是"进了几个块、比了几条键"
    都没变，变的只有"其中几次真去读盘"。**下游那个 5002 键的现场正好整份装进 1024 槽** ⇒ 这一档对它收益最大
    （乱序 3.3×、启动那一格 5.0×）。⚠️ 最后一列"库空着、只读 4 条"在 200k 那档是 **1027.5 µs/次** ——
    与下游报的"单次点查 0.86 ms"同一形状，这次把它打到 289.5（3.5×）。
    一次未命中的单价从数字里可以直接算出来：40000 档 (117.3−21.0)÷(1.81−0.03) ≈ **54 µs**、
    200k 档 (292.9−209.0)÷(3.42−2.53) ≈ **94 µs**（散在 5 篇 L0 里，跨文件更贵）。
    ⚠️ 40000 那一档 `multi_get`/`scan` 两列在自动档臂略高（20.1 / 18.7 vs 17.4 / 15.1），而两臂的
    计数（块/次、未命中/次）完全相同 ⇒ 按本机 ±30%+ 的墙钟噪声看待，**不作为结论**；要当真就得给
    这两条也配一把计数尺，那是下一条里程碑的事。
    ⚠️ 200k 那一档**没有**同构建的"改前"臂 —— 表里 292.9 就是同一份构建把自动档关掉（`LSM_CACHE_AUTO=0`）
    量出来的，与 5002/40000 两档同一口径，可以直接横着比。
  - ⚠️ **方法学（当天补测，写下来免得下次又拿墙钟当证据）**：同一份构建连跑三轮门面大库基线
    （`SCALE_ROWS=20000`），**这条改动根本没碰的 bitcask 灌行**就落在 19.2 / 24.6 / 28.2 µs/行，
    而 README 那张表记的是 14.7 ⇒ 本机（`D:` 是 5400 转机械盘）跨时段能差 **1.3~1.9×**。
    第一遍读数时我差点把"lsm 灌行 30.5、点读 11.6"当成自己引入的回归写进文档 —— 安静复跑三轮
    是 20.6~23.5 与 **6.2~6.9**（记录 9.6），而且**刚编译完那一轮**（编译器还没退出）量到的就是
    那个 11.6。所以：跨构建的 µs 列一律不作因果凭据，只作历史快照；凭据是同构建 A/B 的计数。
- **lsm `health()` 不再把每个源整篇物化：600k 档瞬时 +126.1 MB → +0.1 MB（M39）**：这条是 M38
  修好内存量具之后才看见的 —— `health()` 是"报统计"的调用，一次却瞬时涨 126 MB（旧量具报 +0.0）。
  - 根因一句话：`health()` 走 `LSMTree::scan_latest_light`，而那份实现**先把每个源整篇折进 `Vec`**
    （memtable `mem_view_light` 整表遍历、每个 SST `SstFile::light_view` 整篇读完，**所有源同时活着**）
    再交给跨源归并 ⇒ 峰值 = 「2 × 全库轻量视图」。惰性归并版本 `light_iter_range` 早就存在
    （M15b/M29 那条路），只是这个调用没用它。
  - 改法：① `health()` 改走 `light_iter_range(None, None, snap)`；② `scan_latest_light` 改成它的
    **收集壳** `.collect()` ⇒ 跨源取胜规则**只剩一份实现**，公开签名没动（不是兼容层，是去重）；
    ③ 随之死掉的 `mem_view_light` 删除；④ `SstFile::light_view` 上加计数尺
    `light_view_materialized()`（**单位 = 条目数**，与 M33 那把同形）。磁盘格式没动。
  - 实测（600k 档、同一份构建、修好的量具）：瞬时 **+126.1 → +0.1 MB**，耗时 742 → **461 ms/次**。
  - 守卫 `test_health_does_not_materialize_its_sources`：一次 `health()` 期间
    `light_view_materialized()` 差值必须为 **0**；先 `flush()` 逼数据落在 SST 里（全在 memtable 时
    这条会空转，所以另加 `live_keys >= 300` 的形状断言）。⚠️ 判据仍不取分配计数 —— 那是进程级、
    并行用例互相污染（M27/M36 各栽过一次）。变异验证：把 `light_iter_range` 的 SST 源换成
    `sst.light_view(snap).into_iter()` ⇒ 红在"health 期间有 300 条被 `SstFile::light_view` 整篇物化"。
  - 邻账未动（如实）：lsm 灌行瞬时 **+106.8 MB**；bitcask `vacuum()` 分段拆出来是
    **① 归组判定 106 MB（93 B/扫描行）+ ④ `merge()` 115 MB（100 B/行）**，②③ 都是 0 ——
    即"元凶是 `to_delete`/`keep` 这两个跨全库累加器 + `merge()` 自己"，不是 blob 那一段。见 M37。
- **大库台架的内存量具修好：`timed()` 每段之前 `reset_peak()`（M38）**：`mem_probe::peak` 是
  **进程级单调高水位**，只增不减 ⇒ 旧写法 `peak() - 上一次 peak()` 让"排在后面的操作"一律报
  `+0.0 MB`，哪怕它自己瞬时涨了一百 MB。这一版改成每段之前把高水位压回"当前活着的字节"，
  `峰值` 才是**这一段**的瞬时增量。⚠️ **历史读数按定义分两段**：M38 之前的 `峰值` 列全是
  "超过历史最高点的部分"（下界、且多数段量不到），M36 那句"89.4 → 56.7 MB"就是被它废掉的（见上条）。
  修完当天同档重量出来的新账：lsm `health()` **+126.1 MB**、lsm 灌行 **+106.8 MB**、
  bitcask `vacuum()` **+171.4 MB**、四家 `export`/`scan`/`migrate` 整库物化 **+114.7~124.3 MB**。
- **四家 `vacuum()` 的保留判定改成逐组流式（结构已改；内存那笔当时用坏量具量的，见上条 M38，M36）**：
  那 89.4 MB 不在结果集也不在 blob，而在**四家各抄一遍**的一处结构 —— 判"留哪一版"之前先把
  **全库**每一物理行归组攒进 `by_key: BTreeMap<Vec<u8>, Vec<(u64, Vec<u8>, bool, R)>>`
  （每行两个 `Vec`：`raw_key` 与 `enc`）⇒ 636k 行 ≈ 90 MB，比它要裁的东西大两个数量级；
  下游 1 核 6 GB / RSS 预算 80 MB 的机器一次 `vacuum()` 就顶穿。
  - 改法：版本键按 `raw ‖ version_be` 升序到达 ⇒ 同一把键的行必然连续，于是只攒"当前这一组"，
    看到后继就判掉。共用件新增 `common::vacuum_plan_group`（**取走**那一组、返回该删/该留），
    `vacuum_plan` 改成"逐组调它"的批处理壳 ⇒ **规则仍然只有一份**（这条规则曾经有四份复制，
    是本次事故的结构性根因，不能因为流式化又拆成四份各自的循环）。公开 API 没动、磁盘格式没动。
  - 守卫一条走四家：`all_engines_vacuum_judges_per_group_not_whole_library`（`src/engine.rs`），
    尺子是 thread_local 的**判定次数**与**最大一组的行数**，判据 = 判定次数 ≥ 键数（800 把）
    且最大一组 ≤ 每把键的版本数（2 行）；实测四家都读到"800 次 / 最大 2 行"。
    ⚠️ 这条**故意不用 `mem_probe` 的 peak**：peak 是进程级高水位，`cargo test` 并行时别家用例
    分配的字节会算进来（内存读数只能待在 `--test-threads=1` 的 `#[ignore]` 台架里，M27/M33 同）。
    "内存降了"是结果，能钉住的是结构。变异验证：把 bitcask 改成"换组不判、收摊一次判全部" ⇒
    红在 `bitcask: vacuum 把判定折叠成了 1 次（键有 800 把）`，另三家照绿（各自独立）。
  - ⚠️ **本条当时报的"峰值 +89.4 MB → +56.7 MB（1.58×）"作废**（M38 修量具后重开）：大库台架的
    `timed()` 取的是**运行中的进程高水位**（`mem_probe::peak` 只增不减、不做 `reset_peak`）⇒ 一段
    操作只有"超过历史最高点的那部分"才看得见，段内真实涨幅被前面的灌行 / `scan` 吃掉。
    **这个盲区 M33 就写进过 CHANGELOG，我这次仍然拿它的读数当了凭据** —— 规矩补一条：
    **引用内存读数前先确认那把尺子在这条路径上是活的**（同一条读数若是 +0.0 MB，先怀疑量具）。
    M38 修好后同档同形状重量：bplus **+3.9 MB**、memory **+4.0 MB**（这两家的归组账确实没了）、
    lsm **+101.2 MB**、bitcask **+171.4 MB**（这一段含 `kv.merge()` 重写整库的工作集，
    不全是归组的账）。⇒ **M36 的净效果还欠一次同构建 A/B**（临时把 bitcask 改回归组形状读一遍），
    在那之前只有"结构已改 + 两家已见降"这一句可引用，**不许拿两把尺子的数相减**。
    耗时那一笔与量具无关、是干净的：bitcask 600k `vacuum()` 5.82 → **4.04 s/次**。
  - 量具修好后**新露出来的内存账**（旧量具全报 +0.0，所以一直看不见）：lsm `health()`
    **+126.1 MB**、lsm 灌行 **+106.8 MB**、四家 `export` / `scan` / `migrate` 的整库物化
    **+114.7~124.3 MB**。这些是 M38 之后的排产依据，不是新引入的回归。
  - 剩下那笔 O(存活行)（如实未修，M37）：全库轮次仍要攒"存活行清单 `keep`"交给 blob GC，而
    `rewrite_in_place` 要一次握住全部存活大 value。挡路的是**借用**：`iter_meta_from` 按 `&self`
    借住 KeyDir，走查途中调不了 `get_entry`（`&mut self`）⇒ 做不到"当场读 blob 指针、当场丢项"。
    两条出路都动语义、要他点头：把 KeyDir 与文件句柄拆成可分别借用的两个对象；或把 `vacuum()`
    内部改成按片循环（会动 `full_sweep` 门控 —— blob 重写与 merge 只该在全库轮次做）。
- **B+ 树写路径：下降改走解析缓存、写完的页直接回填（裸 insert 单价 2.4×，页读/行 2.94 → 0.00，M34）**：
  触发的是三档复测里那格 **bplus 事务灌行 114.2 µs/行**（bitcask 16.3 / lsm 25.2 / memory 4.3）与
  `vacuum()` 全库 12.27 s。磁盘格式没动。
  - **归因台架扩到五臂**（`bplus_write_cost_attribution`，`#[ignore]`，`BPLUS_WRITE_ROWS=`）：
    A 裸 insert ｜ E 与 A 同形但先 `set_cache_auto_max(usize::MAX)` ｜ B 事务内写不提交 ｜
    C 完整事务 ｜ D 空事务，每臂再带六把计数尺（解析 / 页读 / 缺页 / 解析缓存清扫 / 换出 / 刷脏，
    单位都是"次/行"）。**是计数把故事改的，不是墙钟**。
  - 改法三件：① `insert_recursive` 的下降从裸 `read_page` + `bincode::deserialize` 改成走 `node_of`
    （解析缓存，M18 之后只有读侧在用）；② 内部节点**只在下面真分裂、这一层真要改时才克隆**键与子
    数组（顺序灌行 40 次分裂才动 1 次父层）；③ `insert` 落盘成功后把手里那份节点内容塞回解析缓存
    （`cache_written` ⇒ `memoize_node` 抽出来给两条来路共用，钉住上层页的纪律也一并挪进去）——
    这一步**不花深拷**，因为内容本来就在手上，搬进 `Arc` 就完事；只有 `write_page` 返回成功才回填
    （写失败时缓存里放一份盘上没有的内容更坏）。另外 `row_fits` 改用借用版字节公式
    （`entry_bytes_borrowed`），不再为量一次尺寸把键与值各克隆一份。磁盘格式没动。
  - ⚠️ **同一刀砍到删除路径上，被复测打回、已退回**：`read_node`（删除/重组那条路，`vacuum()` 同源）
    本来也一起改了走 `node_of`，三档复测 600k 档 `bplus vacuum() 全库` 12.27 s → **15.12 s**，
    而同一天没被碰过的 bitcask 那条反而 5.79 → 4.52 s（噪声朝反方向）⇒ 比值 2.12× → 3.34× 归这一处。
    退回裸 `read_page` + 解析，理由写进注释：**走查形状的页每页基本只碰一次**，回填解析缓存换不到
    下一次命中，只多付 `Arc` 分配、HashMap 插入与装满之后的整表 `retain`（与
    `PageManager::read_page_for_walk` 的"装得下才回填"同源）。**"这条路值不值得走解析缓存"要按
    "同一页会不会被连着再问"判，不能按"是不是下降"判** —— 这次就是照抄了一半。
    退回之后同一形状再量一轮：600k `vacuum()` **10.64 s**（同期 bitcask 5.82 s ⇒ 比值 1.83×，
    带着那一刀时是 3.34×，改前基线 2.12×）—— 回归确认消除。
  - **两条归因被自己的台架推翻**（这一节的重点，M30 是第一次、M31 第二次、这是第三次）：
    1. 先点名"每行把整页条目重新问一遍 bincode"（`vec_item_bytes` = 每条 2 次 `serialized_size`）
       与"下降线性扫还每把键克隆一次"。两处都改了，改完 20k 档 A = **39.7 µs/行**，与改前 38.1 同档
       ⇒ **结构性嫌疑 ≠ 时间元凶**。字节账仍然值得改（它是 O(页) 而不是 O(1)），但它不是这一格的价目项。
    2. 于是加"抬顶"那一臂去验"是不是缓存顶不住 ⇒ 脏页换出（双写 + fsync）在收费"。200k 档
       **E 比 A 慢 5.0 µs，两臂换出都是 0.05 次/行** ⇒ 猜想作废；写路径这一格与覆盖率无关。
  - 真正把 A 砍 2.4× 的是那条**一直存在、但没有任何尺子数得到**的账：写侧下降绕过解析缓存，
    所以"解析 0.00 次/行"当时读起来像"没解析"，实际是"解析发生在尺子之外"（页读 2.94/3.80 次/行
    才是它的替身）。
  - 实测（同一份构建，µs/行只作趋势；计数是凭据）：
    20k：A 38.1 → **16.6**，页读/行 2.94 → **0.00**；C（= 台架那一格）67.5 → 60.9。
    200k：A 63.3 → **29.6**，页读/行 3.80 → **0.00**；C 126.0 → **40.7**。
    三档四引擎复测同步更新：bplus 20k 灌行 **41.9 µs/行**（改前那轮只记了 600k = 114.2 与
    20k→600k = 1.49×，反推 20k ≈ 76.7 —— 反推值不当凭据，改后是实测）。
  - 守卫两条（`src/bplus_tree/bplus_tree.rs`），都做了变异：
    `test_sequential_insert_descends_without_touching_pages` —— 尺子 `page_reads()`，判据"顺序灌
    8000 行平均摸页 < 0.1 次/行"；删掉回填 ⇒ **1.051 次/行**，红。
    写这条判据时选尺子花了心思：**不能用"解析次数"**，因为"退回裸 `read_page`"那个回归会让解析
    计数变成 0（看起来更省）—— 换尺子前先问"这个回归会让它变大还是会消失"。
    `test_child_location_is_binary_not_linear` —— 新尺子 `descent_key_comps()` /
    `descent_max_comps()`（把计数器放在真正的比较回调里，数的是实际发生的比较）；退回线性扫 ⇒
    **最坏一次 63 把**（界 log2(64)+2 = 8），红。⚠️ 这条判据**第一版是错的**：我拿"平均比较次数
    ≤ 2·log2(order) = 12"当界，退回线性扫实测只把平均推到 **11.0** ⇒ 变异抓不住；平均被树形摊薄
    （根很窄），"最坏那一次"才是线性扫真正的形状。
  - 剩下的账（如实未修）：A 仍 16.6 / 29.6 µs/行 —— 那是叶子那一页**每行都要**深拷一次条目 +
    序列化一次 + 4 KB 页拷贝，要再砍得改缓存的所有权模型；`B − A` = 31.1 / 36.1 µs/行 是 MVCC
    每行的账（`latest_version` 一趟下降 + 版本键编码 + 值编码 + WAL 追加 + 写集登记）；
    `C − B` 在两档之间正负漂（+13.3 / −25.0），而 D（空事务）只有 4–10 µs/事务 ⇒ 每事务固定开销
    摊到 1000 行/事务后可以忽略，别再去优化 commit。
- **压实的输入改流式源：一次压实的瞬时内存 156.8 MB → 6.3 MB（M33）**：M30 把"压实慢"归到
  `estimated_bytes()` 之后，剩下的"输入整篇物化"我当时写的是"**这是内存账，不是时间账**"，
  挂在待办。这一轮按内存账收掉 —— 顺带证实那句"约一个 SST"的旧注释是错的。
  - 改法：`merge_streaming` / `merge_to_ssts` / `merge_latest_streaming` 改收
    `Vec<MergeInput>`（**一路一个装箱迭代器**），压实与层内重叠修复两个调用点换成
    本来就有、但没人用的 `SstFile::iter_all()`（分块惰性读）。保留 `vec_inputs()` 给
    "源本来就物化在内存里"的调用点（内存表切片）—— 它**只是换个类型**，不复制条目。
    为什么装箱而不是泛型参数：源的形状天生混合（`Vec` 迭代器 / `SstEntryIter` / 带 `filter`），
    泛型要么要求同构要么逼每个调用点写类型名；装箱开销 = 输入文件数，与每行的克隆比可忽略。
  - 实测（同一轮两臂对照，release，600k 行 / 8 个输入文件 / 条目本身 61.7 MB）：
    旧形状 物化输入 366.9 ms ｜ 峰值涨 **154.0 MB**（每行摊 269 B 而条目只有 108 B ⇒ 多的全是
    每行两个堆分配 + `Vec` 扩容翻倍）+ 归并写盘 1471.8 ms ｜ 再涨 2.9 MB ⇒ **整段 156.8 MB**；
    新形状 流式源 **1888.4 ms ｜ 峰值涨 6.3 MB** ⇒ **省 25.0×**，时间 +2.7%（本机噪声带内 ⇒ 持平）。
    ⚠️ 下游 1 核 6 GB、RSS 预算 80 MB ⇒ 旧形状**一次压实就把预算吃穿一倍**，这是"内存高"里最该先修的。
  - ⚠️ **报"省了多少"必须按整条操作比**：第一版台架拿"② 归并写盘那段"与"③ 流式那段"比，
    印出来是"省 0.0 MB（0.5×）"—— 因为旧形状的大头（154 MB）记在**物化那一段**，② 只是叠在
    它上面多涨的 2.9 MB。只比后两段会得到"流式反而更占内存"的反向结论。判据句子印错过一次，
    改完立刻复测才发现。
  - 两臂的**输出必须逐字节同口径**由断言钉住（文件数与总字节都比对）：否则"省了内存"完全可能
    只是"少写了东西"。
  - 守卫两条，**一个调用点一条**（共用件改一处两家受益，但每个调用点要有自己的尺子）：
    新条 `test_compaction_does_not_materialize_its_inputs` 与既有
    `test_compaction_repairs_level_overlap` 里加的那条。尺子 =
    `merge_inputs_materialized()`，**单位是条目数**（不是"`iter_entries()` 调了几次"：
    调一次就搬进几万行）。变异证明：两处分别退回物化版 ⇒ 红在 `压实把 80 条输入整篇物化进了内存`
    （left 80 / right 0）与 `重叠修复把 61 条…`（left 61 / right 0）。
  - ⚠️ **新守卫第一次跑就红在自己身上**：我用 `compact_once()` 触发压实，而它**自己看 L0 阈值**
    （L0 只有 2 个文件、阈值 4）⇒ 一轮都不压，那条"物化 0 条"是空转出来的。补了形状断言
    （`rounds >= 1 && after.l0_files < before.l0_files`）才逼出 `compact_l0_until(0, 8)`。
    **凡是断言"某段没发生"的守卫，都要在同一行里证明"它本可以发生"** —— 这是 M23/M28 之后第四次栽在同一类上。
  - ⚠️ **`drop(input_files)` 是新加的一格**：迭代器借的是输入文件，而压实之后要**删这些文件**；
    Windows 上句柄没放就删不掉。旧写法"源是 `Vec`"顺带满足了这个前提，**不是巧合**，
    改成流式时必须显式写出来（LSM 与修复两处都有这行 + 注释）。
  - **本轮同时做的三档全量复测**（20k / 200k / 600k × 四引擎 × 31 项，同一份构建）另外排出三条
    待修（见 README"本轮全量复测"那张表）：bplus 事务灌行 **114.2 µs/行**（bitcask 的 7 倍）
    与 bplus 全库 `vacuum()` **12.27 s** 疑似同一条根；`key_count` / `search 首页(要 total)` 的
    **每行常数 2.75 µs**（连 `memory` 引擎都比它自己的 `scan` 贵 1.8 倍）；bitcask 全库
    `vacuum()` 瞬时 **峰值 +89.4 MB**。
- **缓存自动档的"顶"改成调用方可抬，覆盖率的分母同时可读（M32）**：M22/M23 给两家缓存加了自动档，
  但**顶本身是写死的**（bplus 2048 页 ≈ 8 MiB、lsm 1024 块 ≈ 7.1 MB）。600k 档实测覆盖率只剩 5.6%
  ⇒ 每次点读多付一次"读页 + 重解析"。抬顶等于"把库复制进 RAM"，这个决定归调用方，不归库默认 ——
  而"能决定"这件事必须是一条有守卫的 API，不是一个写在注释里的承诺。
  - 新增：`KvEngine::set_cache_auto_max(n)` / `cache_auto_max()`（bplus + lsm 生效，**bitcask / memory
    两条路都当场返回 `Unsupported`** —— 热调与带 `EngineConfig.cache_auto_max` 打开都一样，
    不静默"设置成功"）；`EngineConfig.cache_auto_max`（`None` = 各家默认顶，`Some(usize::MAX)` = 不设顶）；
    `tuning()` 补上**顶与分母**（bplus `page_cache_auto_max` + `total_pages`，
    lsm `block_cache_auto_max` + `sst_blocks`）—— 只报"生效容量 1024"没人分得清是撞上顶还是库就这么小。
  - ⚠️ **改容量不许重建**（M23 的教训，这次两家都按同一条纪律实现）：bplus 走
    `PageManager::set_cache_capacity`（只动"什么时候算满"的判断）、lsm 走
    `reevaluate_cache_capacity`（`BlockCache::set_capacity`），守卫里凭据是
    **"住着几页 / 几块，抬前抬后同一个数"**。
  - 尺子加了一把，而且加在**磁盘那一层**：`common::disk::disk_page_reads`（测试专用计数，
    每次 `DiskFile::read_page` 记一次）。理由是"全驻留"这个词只有在不会说谎的那层才成立 ——
    `node_of` 在页缓存**之上**还有一份解析结果缓存，那份命中时连页缓存都不问，
    所以"页缓存零未命中"≠"没读盘"。（实测本形状两者恰好相等：一轮点读落盘 1621 = 未命中 1621。）
  - 实测读数（bplus，1300 把 3.3 KB 长键 ⇒ 2606 页，判据全是计数）：冷启一趟落盘 **2605** ｜
    默认顶 2048 焐热一趟后再走**仍落盘 1621** ⇒ 顶真的绑住了 ｜ 抬到不设顶 ⇒ 生效容量 2606 = 页数、
    再走一趟**落盘 0 / 未命中 0**、住着 1986 页抬前抬后不变。
    lsm 同形状（26 块 / 封顶 8）：焐热后一轮缺 **26** 块 ⇒ 抬到不设顶后重读缺 **0**。
  - 守卫三条：`cache_auto_max_knob_routes_to_two_engines_and_is_refused_by_the_other_two`（门面：
    能的两家压/抬都读回新值 + `tuning` 里顶与分母同在；不能的两家热调与带配置打开都报 `Unsupported`）、
    bplus `test_cache_auto_max_is_raisable_and_misses_go_to_zero`、
    lsm `test_block_cache_auto_max_is_raisable_and_coverage_is_readable`。
    变异两批都真跑过：**A「只记值、不落到生效容量」**⇒ bplus 红在 `cap1 == total`（2048 ≠ 2606）、
    lsm 红在 `cap_after == blocks`（8 ≠ 26）；**B「把改容量写成重建缓存」**⇒ 红在"住着几页"
    （0 ≠ 1986 与 0 ≠ 8）。⚠️ **这两种变异门面那条守卫都抓不到**（它只核对读回来的值）
    ⇒ 效应必须留在引擎里，这是"每一层各配自己的守卫"的一次实证，不是教条。
  - ⚠️ **本条最贵的东西是一条会冻住的守卫**：为了在一行里同时打印"住着几页 + 钉住几页"，写成
    `mvcc.kv.lock().unwrap().a()` 与 `mvcc.kv.lock().unwrap().b()` 作**同一条语句的两个参数** ——
    第一个 `MutexGuard` 临时量要到**语句结束**才释放，第二次 `lock()` 就是自死锁
    （`std::sync::Mutex` 不可重入）。现象是进程活着、CPU 不涨、一行都不打印，
    而我连续两轮把它读成"这个形状的点读慢了几十倍"，还顺手写了一条**错的**解释到注释里
    （"`node_cache` 把点读全吸收了所以页缓存尺子读不到"）—— 直到用 `Get-Process` 隔 6 s 采两次 CPU
    发现完全不涨才换方向。规矩：**CPU 不涨 = 阻塞，CPU 在涨 = 计算**，这一步排在
    "先量基线再动手"前面。改成一次锁取两个数之后整条守卫 12.7 s 跑完，判据一条没少。
    ⚠️ 顺手反查了全仓库同一条形状（一条语句里两次 `.lock()`）：**只有注释里那一处**，生产码干净。
  - **仍然没做的两点**（M32 不该碰）：默认顶不变（2048 / 1024，基座不替下游决定复制库）；
    bplus 长键库里**钉住的上层页要吃掉容量的一半**（`pin_limit = min(1024, cap/2)`）——
    实测 2048 顶下住的 1986 页里 1024 页是上层，留给叶子的只有 962 页而这份库有 1300 片叶子，
    这正是"焐热后一轮还缺 1621 页"的来源。要让叶子也住满得把 `PIN_BUDGET_PAGES` 变成可配，没做。
- **B+ 树"一次未命中"的单价拆开砍了两刀（M31）**：症状是三档基线里 bplus 随机点读
  9.0 / 39.7 / 48.7 µs/次（5.41×）、`multi_get` 5.9 / 42.5 / 66.9 µs/键（11.34×），
  而**连全 miss 都涨 3.48×** ⇒ 不在"命中/未命中"上，在每次读的定位与解码上。
  - **两个我先点名的候选都被自己的测量证伪了**（这一步比结论重要）：
    ① "M22 那句 `node_cache.retain` 每次插入扫整表"—— 新尺子实测**每读只走 1.1~2.1 个条目**
    （装满才扫一次，摊薄掉了）⇒ 不是它。
    ② "覆盖率 5.6% 所以每读缺好几页"—— 计数实测 **页/次 = 1.03、命中/次 = 0.024**
    ⇒ 上层页真的钉住了，一次随机点读**就缺 1 页（叶子）**；覆盖率只能解释"每读一次未命中"，
    解释不了"这一次为什么值 30~50 µs"。
  - **钱在哪（新台架 `bplus_miss_price_probe`，绕过引擎对同一批页裸测）**：
    ① `DiskFile::read_page` 每读一页问一遍 `File::metadata()`（只为判"这页在不在 EOF 之外"）
    = **≈4.4 µs/页**（同轮配对：读页 7.08~7.45 vs 读页+多问一次 11.00~12.27）；
    ② 表驱动逐字节 CRC32 = **11.7~12.0 µs/页**，比把页读出来还贵。
    ⇒ 改成"seek + 读到 EOF"（三种页形状的结果与旧写法**逐字节等价**）+ CRC32 换 slice-by-8
    （8 张子表打断串行依赖链）：**CRC 4.3×**（11.9 → 2.77 µs/页）。合计一次页读从 ~15-19 µs
    降到 ~7 µs 加解析。CRC 是三处共用的（WAL 帧 / SST 块 / bplus 页）⇒ 三家读路径一起收这一刀。
  - ⚠️ **方法学，本条最该记住的部分**：**单次配对 A/B 在这台机器上会连方向都翻。**
    同一份代码、同一次运行，"读页"这项报过 5.45 与 14.60 µs/页（2.7 倍），CRC 的配比值先后
    是 1.23× 和 **0.81×（＝"新实现更慢"）**。改成**交错 5 轮各取最小值**之后两轮独立测得
    **4.33× / 4.27×**。道理很简单：干扰只会让一次变慢，不会让最小值变慢。
    ⇒ **以后这台机器上 µs 级 A/B 一律交错 + 取最小；单次配对不算凭据。**
    （已知的是"跨构建 µs 漂 1.3~1.9×"，这次发现同一进程内相邻两段也躲不开。）
  - 守卫四条：`test_read_page_does_not_ask_file_length`（尺子 = 读一页时 `len()` 被调几次，
    必须 0；同一条用例里主动问一次证明尺子不是空转 —— 变异把那句 `len()` 放回去 ⇒ **红**）、
    `test_read_page_tail_shapes_match_the_old_length_guarded_form`（整页在内 / 尾部半页 /
    整页在外三种形状的**结果**必须与旧写法逐字节相同 —— 光有上一条会允许"不再问长度但把语义改了"；
    变异成"到 EOF 就报错" ⇒ **红**）、
    `crc32_slice_by_8_matches_bytewise_everywhere`（8 种长度形状 × 3 种内容 × 已知向量 ×
    单字节翻转判别力 —— 校验和算错不是慢，是**每页每块每帧都判成坏数据**）、
    以及 `bplus_miss_price_probe` 里那句"交错取最小后新实现不许更慢"的断言。
  - ⚠️ **`multi_get` 不该做"按键排序合并同页"**：600k 档 1000 把随机键落在 35 290 张叶子上，
    期望能合并掉的只有 `1000·999 / (2·35290) ≈ 14` 张 ⇒ 省 1.4%。它的 11.34× 与逐键点读同源，
    不是缺批量。真正剩下的那 1 次未命中是**覆盖率**问题 ⇒ 见 M32。
  - 同档四引擎 600k 表（M31 之后那一轮）：bplus 点读乱序 48.7 → **27.5 µs/次**、
    `multi_get` 66.9 → **39.6**、`health()` 1 150 501 → **931 994 µs/次**、lsm 点读乱序
    76.5 → 41.6（SST 块 CRC 同收益）—— ⚠️ **这些单轮 µs 只当量级看，不作因果凭据**：
    同一轮里没被这条改动碰过的 bitcask 灌行是 28.8 µs/行，正落在 README 记的噪声带
    19.2~28.2 里。**因果凭据只有上面那组交错配对数。** 294 项全绿。
- **压实的"每行重求和整表"改成增量账（M30）—— 我点名的根因被自己的拆分测量推翻，这是第二次**：
  `SstBuilder::estimated_bytes()` 从 `entries.iter().map(...).sum()` 改成 `add` 里增量维护的
  `est_bytes`。触发点是 `merge_to_ssts` 那句 `if b.estimated_bytes() >= target_bytes` ——
  **每交出一行就问一次**，而一张 2 MiB 的输出表 ≈ 19 000 行 ⇒ 每个输出文件 O(行数²)。
  - **推翻的过程要留档**：M28/M29 之后我把"带 redo 的重开仍 12~17 s"归给"压实的输入整篇物化"。
    这次先做了拆分台架 `compaction_cost_split`（①物化输入 / ②纯归并 / ③归并+写盘，release，
    600k 行 = 8 个输入文件 61 MB → 34 个输出文件 67 MB）：**① 618 ms、② 476 ms、③ 18 018 ms
    ⇒ 病在 ③，占 94%，而"整篇物化"只值 0.6 s**。要是按原先的判断直接去改流式源，买到的是内存
    不是时间，而且那 18 s 一分都不会少。**结构性嫌疑 ≠ 时间元凶，先拆段再动码**（M27 那次是
    "分片没治到峰值"，这次是"物化没治到耗时"——同一个教训的两种表现）。
  - **凭据（同构建、同形状、只换这一处）**：③ **18 018.5 → 3 113.6 ms**（5.79×），每输入行
    **30.03 → 5.19 µs**；带 redo 的崩溃恢复重开（`reopen_shape_probe` 的 `no_bulk` 臂，同一档）
    **14 427 356.2 → 3 550 107.7 µs**（4.06×），其中 flush 段 13.67 s → 2.72 s。干净重开不受影响
    （`facade` 30.8 ms，M28 那条仍然成立）。
  - 守卫三条，照 M19 那套"对拍 + 走过的条目数"写：
    `test_builder_estimated_bytes_matches_slow_scan`（**口径对拍** —— 那句 `+8` 是每行的帧头，
    搬掉它只会让"什么时候切文件"悄悄变，压测表上表现为"慢了 3%"，没人会往这里查），
    `test_builder_size_check_does_not_walk_entries_per_add`（尺子 = 为判定走过的条目数，生产路径
    必须是 **0**；⚠️ 对照那段量的是"步数"，`estimated_bytes_slow` 返回的是"字节数"，**单位不同不能直接比**，
    第一版就是这么写错的），`test_merge_to_ssts_does_not_walk_entries_for_size_check`
    （真调用点：6000 行 × 切成 ≥3 个文件的压实里判定走过 0 个条目；`files.len() >= 3` 那条同时是
    "判定真的被问过"的空转防护）。
    变异证红：把 `estimated_bytes()` 改回 `sum_entries_bytes(&self.entries)` ⇒ **两条成本守卫红**，
    而对拍那条**按设计仍然绿**（它管口径，不管代价 —— 两条守卫各管一件事才不至于"改错了也能过"）。
    全套 **291 项绿**（M30 新增三条守卫）。
  - ⚠️ **仍然挂着的两笔**：① "压实的输入整篇物化"作为**内存**账没变（瞬时 ≈ 所有输入文件的条目
    总和；`iter_all()` 本来就已惰性，物化发生在 `compact_level` 的 `collect()`，改流式源要动
    `merge_streaming` 的源类型，是独立的一刀）；② 修完之后 ③ 里还剩 2.35 s 的"写"本身
    （67 MB 编码 + 34 个文件各自 `finish()` 的 fsync 与回读）没有再往下拆。
- **有界回收一片改走惰性归并（M29）—— "每源各取满预算"就是那片单价随库涨的根因**：
  `MVCC::vacuum_bounded`（LSM）不再用 `LSMTree::scan_latest_light_window`，改用已有的惰性
  k 路归并 `LSMTree::light_iter_range`，**凑够预算就当场收手**。
  - **症状与推翻的旧结论**：三档基线（20k/200k/600k）里 `lsm vacuum_bounded 一片`（预算 2048）
    = 2 716.3 / 10 277.0 / 30 090.7 µs（600k 复跑 15 676.8）⇒ 每行单价随库涨 **5.8~11×**。
    README 里那句"平 —— 有界回收这条是干净的（**长跑该用它的依据**）"就是这么被推翻的：
    它只做了 20k↔200k 两档，比值 ≈1×，而这条通道是随 **SST 文件数**长的。
    ⚠️ 那片只交 2046 行、裁 0 行 ⇒ 14.7 µs/行，而同档 `一次全量 scan` 是 4.9 µs/行 ——
    **"有界"的一片比"无界"的全库还贵 3 倍/行**，这正是"依据"塌掉的方式。
  - **机制（新台架 `vacuum_slice_fold_attribution`，`#[ignore]`，尺子 = 一片真的折过多少把编码键）**：
    旧窗口是「**每个源各取满 `预算` 行**再归并」⇒ 取到的行数 = `源数 × 预算`。把源数当自变量实测
    （20k 行、预算 2048）：3 个源折 6096 行（3.0×）、9 个源 18384 行（9.0×）、24 个源 22000 行
    （10.7×，**已经等于整库** —— 因为每个文件只有 834 行，"取满 2048"就是把整篇读完）。
    debug 墙钟跟着折到的行数走（99 → 107 → 233 ms）。而 SST 文件数 ∝ 库大小 ⇒ 一片的单价随库涨。
  - **改后同一台架**：交出 512 行 / **折过 513 把编码键**（1.0×），源数 17 不变 —— 代价从
    `源数 × 预算` 变成 `预算`。守卫 `test_vacuum_slice_walks_only_its_budget` 就是钉这个比值
    （`折 ≤ 预算×2` **且** `交出 ≥ 预算×7/8` 两条配对，后者专门防"只走了三行也算过"的空转；
    形状处还有一条 `total_sst == f+1` 的断言，因为门面的 `flush` 是 `let _ = ...` 会吞错）。
    变异证红：把那句 `break` 拆掉（=回到"整片读完再交"）⇒ 报 `交出 16000 行 / 折过 16000 把键`，
    红在比值上。
  - **顺手删掉三处只有这条路径在用的东西**：`LSMTree::scan_latest_light_window`、私有的
    `window_once` / `mem_view_light_break`、`SstFile::light_view_break_from`、以及它们共用的
    `same_group`。**连带删掉的还有那套"窗口太窄就放大 4 倍重试"的补偿** —— 它存在的唯一理由
    是窗口右界可能把唯一的完整组切成半条链；惰性流只在组边界收手（收手的条件就是"看到了后继组
    的第一行"），所以那个 case 结构上没了，游标语义跟着变简单：自然到库尾 ⇒ `next_cursor = None`。
  - ⚠️ **造形状时踩到的坑（写下来，别再踩）**：想把压实关掉好留下"很多文件"的形状，只关
    `l0_compact_threshold` 和 `max_compactions_per_flush` **不够** —— 写路径还有一档
    `maybe_stall_on_l0`，L0 到 `l0_slowdown_trigger`（MVCC 档 = 12）会**绕过那两个旋钮**直接
    `compact_level(0)`。第一版台架就是在第 13 个文件处把 12 个文件合成 2 个，"源数"这个自变量
    当场失效，而且因为门面 `flush` 吞错，它一开始是**静默**失效的。
  - 全套 **288 项绿**（新增 `test_vacuum_slice_walks_only_its_budget` 一条）。**磁盘格式没动**：
    删掉的三个函数都是读路径上的临时结构，库里的字节一个都没变。
  - ⚠️ 另外三家没有这条通道：它们的有界回收本来就按自己的有序结构定位（bitcask 走 KeyDir、
    bplus 走叶子链、memory 走 BTreeMap），不存在"每源各取满预算"这种切法。所以这刀只动 LSM，
    不是"漏改三家"。
- **LSM 冷重开不再自带一次压实（M28）—— 一条被"只比两档"藏住的 1900× 悬崖**：
  `MVCC::try_open_with` 末尾那句 `tree.flush()` 改成**只在有 redo 要重放时**才做
  （判在把重放行插进 memtable 之前 —— 之后 memtable 非空就分不出来源了）。
  - **悬崖长什么样**：三档基线（20k / 200k / 600k，同一构建 `f20b7fd`，release，数据在 `C:` 的
    SSD）里 `lsm_tree 再冷重开` = 10 237.3 / 10 296.5 / **17 487 030.8 µs** ⇒ 原始 1708 倍；
    除掉"整库一趟"该涨的 30 倍还剩 57 倍。对照臂是 memory 引擎（不落盘、纯内存操作在这一轮
    普遍 1.6~2.7×）⇒ 那就是本机底噪，**≤3× 不当结论用**，所以这条是这一轮唯一的新悬崖。
  - **归因（新台架 `reopen_shape_probe`，`#[ignore]`，同一轮里自带 A/B）**：**同一轮两份数** ——
    `uncond_flush` 总 **13 947 618.8 µs**（其中 `flush` 段 13 938 766.2）vs `cond_flush` 总
    **8 362.3 µs**（`flush` 段 0.0）= **1668×**，其余九段合起来 ≈ 3 ms。机制：`LSMTree::flush()`
    末尾无条件 `maybe_compact()`，而 bulk 的 `finish()` 会**把 WAL 收干净（redo 0 条）却把 L0
    文件数正好留在 `l0_compact_threshold`(8) 上** ⇒ "打开一个库"自己把 85 MB 从 L0 搬进 L1/L2、
    写出 37 个文件（层文件 `[8,0,0,0] → [0,34,3,0]`、盘上 8 个 85 MB → 37 个 74 MB；有条件那臂
    两处一动不动）。200k 那一档只有 **4 个 L0 文件**（<8）不越线 ⇒ 两臂同价。**开关是文件数越没
    越线，跟"恢复"本身没关系** —— 这正是"两档比不出阈值"的意思：M27 那轮的 20k↔200k 对照里它
    看起来完全正常。
  - **顺手证伪了一条我自己点名的候选**：`Wal::read_all` + `analyze_recovery` 全量解析**不是**这条
    悬崖的成因（有 bulk 时重开前 WAL 只剩 32 B / 0 条记录，`read_all` 3.4 µs）。但同一台架把
    bulk 去掉，它立刻现形：60 121 条记录 / 8.7 MB 的 `read_all` = 254.7 ms ⇒ **每条 ≈ 4.2 µs**
    （帧头 + 帧体各一次定位读，而 `DiskFile::read_exact_at` 是 seek+read）
    ⇒ "崩溃点离上次 checkpoint 越远、重开越慢"是另一条独立的账，已单独记着。
  - **改完的凭据（同轮墙钟 + 结构 + 跨轮台架三套）**：同轮剖面 A/B **1668×**；结构凭据 =
    盘上文件 8 → 37 变成 8 → 8、层文件不动；门面台架同档跨轮 **17 487 030.8 → 9 106.7 µs**、
    重开常驻 12.1 → **1.9 MB**。守卫两条，各自证过一次红：
    `test_open_does_not_compact_when_nothing_to_redo`（尺子 = **层文件数不动**，不用墙钟；把条件
    退回无条件 ⇒ 红在 `[0,1,0,0] ≠ [2,0,0,0]`；它前面还有一条"形状没造出来就报空转"的断言，
    免得阈值没越线时这条 quietly 变绿）、`test_open_persists_redo_before_truncating_wal`
    （**反面对照**：把 flush 整个删掉也过得了第一条，所以第二条量的是"WAL 被截断**之后**的第二次
    重开还读不读得回来"+ memtable 必须已空 ⇒ 红在 `1 ≠ 0`）。288 项全绿。
  - ⚠️ **诚实边界：压实没有消失，只是不再由"打开库"付**。带 redo 的崩溃恢复仍然必须 flush ⇒
    仍然 14.4 s（跨轮 12~17 s；根因是下面 M27 末尾那条"压实的输入整篇物化"）；干净重启后第一次写或
    `checkpoint` 会把这笔 L0 压实付掉。**改的是"重启即服务"的等待时间，不是总写放大。**
  - ⚠️ **另外两家故意没跟着改**（不是"以后统一"）：量下来 bitcask 的 `flush()` = 空缓冲直接返回 +
    一次 fsync，bplus 的 `flush_all()` 还兼着把 `next_version` 落到 meta 页（redo 为空也必须做）——
    两家的 open 里都没有无界工作，加条件既没有可证收益、也造不出不空转的断言。两处代码注释里
    写明了"若将来长出随库涨的活，照哪把尺子改"。
- **全库折叠改为逐行流式 + 无界折叠逐片（M27）—— 一条"文档与实现不符"加一处我自己测错的结论**：
  共用件加三样：`fold_window_emit`（落点版驱动，每折出一行立刻交出去）、`collect_rows_as`
  （行形状由调用方定）、`fold_rows_streaming`（流式，`on` 返回 `false` 当场收手）。
  四家各自的三处调用点接上：`scan_foreach`（`scan` 走它）、`export_latest_visible`、`purge_expired`。
  顺带删掉四家已经没有调用者的 `collect_latest_raw` / `collect_latest_raw_in`（`pub(crate)`，
  不是公开 API；那段"界怎么判"的知识搬到了 `VersionedStore::scan_candidates` 的 trait 文档里 ——
  **能 `break` 的只有"迭代键本身越界"，按 `raw_key` 判越界只能 `continue`**，前缀交错时提前停会静默少行）。
  - **文档与实现不符的那句**：`scan_foreach` 一直写着"不收集全库到单一大 Vec（内部按有序 latest 迭代）",
    实现却是 `for … in collect_latest_raw_in(...)` —— 那一句先把界内**每一行**折成
    `(键, 版本, 字节)` 元组，再逐条搬成 `ExportRecord` ⇒ 两份头结构同时活着，
    而且回调提前叫停连"折"这一段都省不到。现在那句文档是真的。
  - ⚠️ **第一版修法被复测推翻，这是本条最该记住的部分**：我先做的是"无界折叠逐片"
    （`FOLD_CHUNK = 4096`，即不再一次向引擎要完整库候选），改完复测 bitcask 200k 全量 scan
    的峰值增量：**+48.2 → +45.5 MB —— 几乎没动**。于是主因不是候选片大小，而是
    "折完一批再搬一次形状"这件事本身；换成流式之后才到 **+25.0 MB**（常驻 30.7 = 结果集，没变）。
    同一条台架上四家 `export 全库` 的常驻 32.9 → **30.8 MB**。
    ⇒ **凡是"改完预期该变多少"的猜测，都要被复测过账一次**；这次如果不是先量了，
    我会把一片没治到病的改动写进发布说明。
  - 守卫五份，两次变异各自证红：
    `fold_window_unbounded_walks_in_chunks`（共用层，尺子 = 每轮要了多少组候选；把片放大成
    "一次要到底" ⇒ 红在 `只走了一轮 ⇒ 每轮上限那条断言是空转（轮数 [12295]）`，
    ⚠️ 它同时**自带空转检查** —— 单看"每轮 ≤ 片"那条在退化版上照样绿）；
    四家各一条 `test_scan_foreach_stops_without_folding_the_library`（判据按各家单位给：
    bitcask = `read_value_calls`、bplus = `value_decodes`，交 1 行只该 1 次，老写法 2000 次；
    lsm / memory 没有独立的"取值"账 ⇒ 按比例判"折过的键数"，早停 ≤ 全量的一半）。
    把 `fold_rows_streaming` 退回"先整批物化再逐条交"⇒ **四家同时红**
    （`只要 1 行却取了 2000 次值`、`只要 1 行就走了 12394 把键，全量也才 12394`）。
  - ⚠️ **这台量具的盲区（写下来，别再被它骗）**：`峰值` 是**进程高水位**，同一轮里后面的操作
    哪怕真占了大量内存，只要不超过前面的高点也只会报 `+0.0`。所以四家那三行 `+0.0` **不等于**
    "它们的 scan 不占内存"；而 25.0 MB 这个增量里还混着结果 `Vec` 自身的增长，本台架拆不开。
    要拆得做隔离台架（已列进未修队列）。
  - ⚠️ 还挂着没修的（同一条"瞬时内存"账）：压实的输入仍是整篇物化 ——
    `merge_to_ssts` 收 `Vec<Vec<(键, 条目)>>`，`SstFile::iter_entries()` 把一个文件全读进内存
    ⇒ 一次压实的瞬时内存 ≈ 所有输入文件条目之和（M26 的重叠修复共用这条账单，没有额外恶化）。
- **LSM 层内重叠真修复 + 压实换文件的顺序（M26）—— 顺手挖出一个静默丢数据的窗口**：
  `fix_level_overlaps` 更名为 `LSMTree::repair_level_overlaps`，函数体从"`eprintln!` + 原样返回"
  换成**真的把连续重叠的一段当成同层归并折掉**。分组取**传递闭包**（A∩B、B∩C 即使 A∩C=∅ 也整段
  一起折，否则折完还留新的重叠）；过滤条件沿用本次 compaction 的 `keep`（TTL / Drop / 用户 filter），
  被替换掉的旧文件由调用方在 MANIFEST 记完之后删。
  - ⚠️ **真正值得修的是顺序**：旧写法是 `compact_level` 删掉输入文件、然后**由调用方**去写 MANIFEST
    （`maybe_stall_on_l0` 的软限流那支甚至根本没写）。于是崩在"已删旧文件、还没记新文件"之间 ⇒
    MANIFEST 指着不存在的文件，而 `load_manifest` 对不在盘的条目是 `if path.exists()` **静默跳过**，
    新写出的文件也没人引用 ⇒ 重开之后那批数据一句话不报地没了。现在 `compact_level` 自己
    **先 `write_manifest()` 再删文件**（写 tmp + sync + rename 是原子换，四引擎同一份 `common/wal`
    时代的做法），留下的窗口变成"新旧同时被引用"= 层内重叠 —— 正是上面那条修复负责收拾的形状，
    读答案两边同键同 seq ⇒ 折叠与点查都不受影响。
  - **取胜判据从"源序优先"改成"seq 优先"**（`sst.rs::merge_streaming`：堆的 tie-break 与
    选胜者那一步都改了）。原因是实测而不是推理：**同层两个文件谁新谁旧，源序答不了** ——
    L1→L2 压实会用**新 id 写老数据**，而层内只按 `min_key` 排序。旧判据在重叠形状上会把旧版当
    胜者写回去，也就是"修复本身把数据改旧"。正常形状里两者同解（后建的 L0 只装更晚的 seq、
    memtable 比任何已落盘的都新），所以这次对既有路径是语义不变；变的是重叠/崩溃恢复那两种
    以前会答错的情形。seq 相同才回到源序（老 v1 文件 seq 恒 0、同一编码键被写两次 ⇒ 与改造前逐字节同解）。
  - ⚠️ **轻量折叠那条路径本来就已经对**，我照着改了一遍才发现改了等于没改，已退回原样：
    `LightMerge` 的胜者由"待定键 + 取更大 seq"那一步决定，不靠堆的出队顺序。两次变异各自实测过：
    给堆加 seq 比较 ⇒ 守卫**照样绿**（多余的复杂度，退回）；把待定那步换成"先出的就当赢" ⇒
    守卫**红**（那才是担责的位置）。⇒ 已写进注释：**两条折叠路径判据同源，但担责的位置不同，别照抄。**
  - 守卫两份，四次变异各自证过红：
    `test_fold_agrees_with_point_read_on_overlapping_level`（重量折叠 + 轻量折叠 + 点查三者逐键一致；
    退回源序 ⇒ 红在 `折叠与点查不一致："k10" left: Some([111,108,100]) right: Some([110,101,119])`，
    即 `old` vs `new`；摘掉待定的 max-seq ⇒ 红在 `轻量折叠在 "k10" 上选错了版本（seq 1，点查会给 5）`）。
    `test_compaction_repairs_level_overlap`（**精确计数**：40 把键在重叠层上点读探文件 **60** 次，
    折完 **40** 次；断言折后层内两两不重叠、旧文件已从盘上消失、每一把键的读答案一字未变、
    重开读得回、MANIFEST 与盘上文件互相没有悬挂/孤儿。退回"只告警"⇒ 红在
    `折完该剩「合并后的 40 键文件 + zz 文件」（实测 3）`；顺序退回先删后记⇒红在
    `删旧 SST 之前 MANIFEST 里还找不到新文件 5`）。⚠️ 顺序探针**读的是盘上的 MANIFEST 内容**，
    长在删除点前面，所以谁把顺序改回去都会红，不需要任何人"记得搬探针"。
  - 重叠这个形状**正常压实造不出来**（L0 整层并入、同层输出按键有序），所以测试用
    `inject_sst_for_test` 直接把两个范围重叠的文件挂进 L1 —— 那正是"崩在记 manifest 与删文件之间"
    恢复出来的内存形状。⚠️ 今天所有 20k/200k 台架的输出里 `level overlap detected` 一次都没出现，
    也就是说这条修的是**低频但会丢数据/悄悄变贵**的那类，不是热路径；写在这里免得被当成性能优化。
  - ⚠️ **顺手记下一条新的、没修的账**（写在 README 已知局限）：`merge_to_ssts` 收的是
    `Vec<Vec<(键, 条目)>>`，而 `SstFile::iter_entries()` 把一个文件**整篇**读进内存 ⇒
    一次压实的瞬时内存 ≈ 所有输入文件的条目总和，不是 `sst.rs` 注释说的"约一个 SST"
    （builder 那半确实流式）。修复重叠共用这条账单，没有额外恶化；改成真流式源是独立待办。
    ⚠️ 还有一条测试自己的假红：`format!("{id:08}")` 对 `String` 是**空格**补齐不是零补齐，
    会把 `"5"` 找成 `"       5.sst"` —— 已按数值格式化并写进注释。
- **`WalRecord::Begin` 不再落盘（M25）—— 把"零信息的记录 + 零覆盖的假设"这条洞关掉**：
  内核写序列从**八步**收成**七步**（`latest_version → 可见性判定 → encode_value → row_fits →
  wal_write → put_versioned → 写集登记`），`WriteSide::wal_begin` 这个槽整个删掉，
  `WalPolicy::open_on_first_write()`（返回"是不是第一次"）改成 `mark_opened()`（只置位 ——
  留着返回值只会让人以为"返回 true 的那一次要做点什么"）。
  - 为什么是"删"而不是"让它承载信息"：恢复要的每样东西都不经过它 —— `max_xid` 从
    `Write`/`Commit`/`Abort` 也能取到（写过的事务必然带 `Write`），`open_writes` 的条目
    `Write` 自己就建起来了，而"只留下一条 `Begin` 就崩了"的那种事务**一行都没写** ⇒ 没有东西要撤销。
    给一条记录硬派个工作（比如把快照写进去）才是新增风险。M18d 记下的是**实测**：整步作废之后
    全量测试一条都不红 —— 也就是这句话一直没有守卫覆盖。
  - **磁盘格式没动**：`Begin` 变体与解析分支都留着，0.4.0 建的库里的老日志照样读。
    两边各一条守卫，缺一条都是洞：
    ① 写侧不再产生它 —— bitcask / bplus / lsm **各自**在自己那条 WAL 形状守卫里数
      `begins == 0`（把 `Begin` 加回去 ⇒ 三家**逐家实测会红**，报 `left: 20000, right: 0`）；
    ② 读侧兼容它 —— `plan_is_identical_with_or_without_begin`（`common/wal.rs`）把同一条事务流写两遍
      （1 提交 / 2 撤销 / 3 崩在提交前 / 4 只留 Begin 就崩了），折出的 **REDO 与 UNDO 必须一字不差**；
      让恢复真去依赖 Begin（改成"没见过 Begin 的 `Write` 不算数"）⇒ 当场红在
      `left: [(1, "k1", Some("v1"))], right: []`。唯一合法的差是那笔空事务把 `max_xid` 从 3 抬到 4，
      这条也钉死了（不许有人拿它当"`Begin` 有用"的证据）。
    另外三家原有的"手写 WAL 日志"崩溃用例（redo / undo 两条通道）刻意**保留 `Begin`** ——
    那正是老库的形状，它们本身就是兼容证据，注释已写明。
  - bplus 那条"被拒收的超大行连 `Begin` 都不留"的顺序守卫（M18e 加的 `begins == 1`）**换了守法**：
    带键的记录只剩 `Write` 一种，所以顺序由 `writes_for(big_key) == 0` + 对照
    `writes_for(b"small") == 1` 守着；`begins == 1` 改成 `begins == 0`，否则它会退化成
    "常数等于 0"而看不见顺序。
  - 代价数字（台架 `wal_cost_per_single_write_txn`，`#[ignore]`，bitcask，release，
    20000 笔"只改一把键"的事务）：每笔 **118.4 B / 3.00 条记录 → 90.4 B / 2.00 条记录**
    （−23.6% 字节、−33% 记录）。挑 `wal_checkpoint_bytes` 的人要的就是这个换算 ——
    恢复重放的量按**记录数**走，不是按总字节。
- **`KvTxn::set_raw(key, value)`（M16，非破坏）**：存**任意字节**的原样写入口 —— 信封由引擎打
  （内部就是 `set(key, pack_plain(value))`），`get` 一字不差还回来。存在的理由：`set` 的约定是
  "传进来的字节已经是打过信封的逻辑值"，直接拿它存裸字节会按首字节偶发丢数据 —— `0x00` 打头少
  1 字节、`0x01` 打头被当成 TTL 信封少 9 字节甚至被判"已过期"读成 `None`；这个坑以前只能靠
  业务侧记得先 `pack_plain`（人守不靠编译器）。四个引擎 + 门面同有，`set_with_ttl` 本来就内部已打。
  ⚠️ **刻意没有 `get_raw`**：`get` 已经是 `set_raw` 的读侧对应物，再加一个名字只会有"该用哪个"
  的困惑；"不经剥壳的存储字节"是另一个能力（引擎内部 `latest_visible_raw`），未对外承诺。
  仍留着的是：`set` 本身不改语义（现有按约定打过信封的调用点会被破坏），所以"用哪个口子"
  仍然是一个选择 —— 只是选 `set_raw` 就不会错。
- **窗口折叠与分页核各收成一份（M15）**：`common::kernel.rs` 新增
  `fold_window(wanted, round, keep)` 与 `search_page_from(matched, query)` 两份共用实现，
  四个引擎的 `search_keys` 从此只剩"我这家的候选怎么折"这一段差异。
  - `fold_window` 能提前收手，并且**能续跑第二轮**：折叠在键这一层判得了"命不命中 pattern"，
    但"这行取不取到值"（blob 指针、TTL 是否过期）要取了值才知道 ⇒ 提前收手可能少数几条，
    于是从上一轮最后一个键续跑、预算翻倍 ⇒ 总代价与最终那一轮同阶，最坏退化成走完全库
    （那正是 `need_total = true` 的语义）。同一个几何重试的形状在 `vacuum_bounded` 里用过一次。
  - 取值放在 `keep` 而不是折在 `round` 里：bitcask/bplus/lsm 的取值要拿 blob 锁，
    而候选是**拿着 kv 锁走元数据**折出来的 —— 两段式就是为了不在这两把锁上打结。
  - `scan_candidates`（四家同名新方法）：折出"一把逻辑 key 一组"的候选，带 `after` 续跑游标、
    `accept` 键级过滤、`budget` 组数上限。交出一组只能发生在**组边界**（版本大端接在 key 后面
    ⇒ 组内升序 ⇒ 半截组上收手会把旧版本当最新行交出去）。
  - 新增两把尺子：bitcask `keydir_visits()`、lsm `light_visits()`。**为什么必须新加**：
    第一版守卫用的是 bitcask `read_value_calls()` / lsm `block_reads()`（取值次数），
    把预算临时忽略掉之后这两条**照样绿** —— 值确实只读了窗口那几条，而键流仍然走完了全库。
    尺子量错了等于没量，这条按"先证明会红"补的。
- **`KvEngine::tuning()`：写进去的旋钮要读得回来**（M13）。返回 `EngineTuning`
  （`Vec<{key, value:String}>` + `get(key)`，定义在 `src/common/types.rs`，与 `MvccHealth` 同层），
  各引擎的 `MVCC::tuning()` 自己列自己有的键 ⇒ 它同时是"这家能调什么"的清单，而不是门面里
  为每家开一个参数 enum（那等于把引擎差异灌回适配层）。跨引擎共用的两条键名由
  `EngineTuning::push_shared()` 给一次，不在四处各拼一遍字符串。
  存在的理由：`EngineConfig` 只能"给"，以前给错了（字段记错、引擎根本不吃这条）是**静默**的。
- **`vacuum_bounded` / WAL 阈值读写上门面**（M13）：`KvEngine::vacuum_bounded(after, budget_rows)`
  与 `set_wal_checkpoint_limit` / `wal_checkpoint_limit`，`impl_engine!` 新增 `wal: yes|no` 开关
  （`vacuum_bounded` 复用 `vacuum` 那个 io/plain 开关，形状一致）。长跑进程要的就是有界那一档：
  一轮的代价只看 `budget_rows`，`vacuum()` 的代价看整库。
- **`crate::lsm_tree::MVCC::set_options(patch)`（热更新，打开之后不用重开库）+ `options()` 读回**，
  以及 bplus 的 `MVCC::{order, max_cache_pages}`、bitcask 的 `MVCC::{max_file_bytes}` 读回。
  两条钉死项在 `pin_mvcc_invariants`：`enable_mem_wal` / `enable_bg_compact` 传 `true` 也恒为
  `false`。前者是"事务 WAL 是唯一持久化点"的定义；后者不是洁癖 —— 后台线程只在 open 时按当时的
  option 创建，若允许热改成 `true`，写路径会以为"有人替我 compact"而**自己不再 compact**，
  而线程根本不存在 ⇒ L0 无限涨。

### 性能与正确性（无 API 变更）
- **只读事务不再进 WAL**（实测 2265.8µs/次 → **15.8µs/次**，落盘字节 56 → **0**）。
  原来 `begin_transaction` 无条件 append `Begin`、`commit` 无条件 append `Commit` 并 **fsync**，
  而 `Wal::append` 是直接 `file.append` —— 所以读路径是写路径：一次纯只读事务约 2.3ms，
  等于每秒四百次读的天花板；混合负载里"点读 1.1ms"的 **97% 是 WAL fsync，与查找无关**
  （改完同样的点读约 45µs）。现在的规矩：`Begin` 推迟到第一次 `write()` 才补，
  只读事务的 `commit`/`rollback` 不写记录、不 fsync。策略只有一份实现
  （[`common::WalPolicy`]），三个磁盘引擎各自接线，各有守卫。
  ⚠️ 行为差别：WAL 里从此看不到只读事务的 `Begin`/`Commit` 记录 —— 恢复语义不变
  （`max_xid` 是从 Begin/Write/Commit/Abort 四种记录一起取的，写过的事务必然带 `Write`），
  但**按 WAL 记录数做统计或调试的脚本要改**。持久化点没动：写过就必须有 `Commit` + fsync，
  这条由各引擎的 `test_read_only_transaction_touches_no_wal` 反向钉住。

- **Bitcask 打开空库不再预留 8 MiB**：`Log::new` 与 `BlobStore::open` 各写了一次
  `Vec::with_capacity(4 MiB)`，merge 出来的新 active 再来一次 ⇒ **一个键都没有也要先占 8 MiB**
  （大库基线台架量到 `open 空库` 常驻 8.4 MB）。改成**首次写入时才一步涨到目标大小**：
  攒够 4 MiB 才落盘的 syscall 优化一字未动（`flush_buf` 是 `clear`，容量保留，所以既没有小步
  realloc 抖动，也没有多一次 syscall）。实测 `open 空库` 常驻 8.4 MB → **0.0 MB**，
  5000 键冷重开常驻 **0.7 MB**（那就是 keydir 本身）。
  守卫 `test_write_buffer_is_allocated_lazily`：打开时容量必须为 0，第一次写入后必须 ≥ 目标。
  为什么这算缺陷而不是风格：下游那台机器是单核 6 GB、RSS 预算按几十 MB 计，
  而且一个进程可能同时开好几个库 —— 固定 8 MiB/库 是白给的。

- **窗口 scan / 前缀 scan 的代价现在随窗口宽度缩（四个引擎全部接上，各带自己的守卫）**。
  根因不在遍历原语，在 MVCC 层的**用法**：`scan_foreach` 调的是 `collect_latest_raw()` ——
  折完**全库**每个 key 的胜出行，再由调用方逐条筛 `start/end`。于是 200 行的窗口和 20000 行的
  全量一样贵（大库基线实测：窗口 scan bitcask **376.6 µs/行**、memory **106.3 µs/行**，
  而同库一次全量 scan 是 3.8 / 1.1 µs/行）。新增 `collect_latest_raw_in(include_deleted, start, end)`：
  起点直接 seek、终点早停；`collect_latest_raw` 退化成它的无界特例，**调用方一字未改**。
  `search_keys` 的 **Prefix** 模式同样接上界（Contains 要逐键比子串，全库是它的本质代价，
  这里不假装优化）；`key_count(prefix)` 经 `prefix_scan` 自动受益。
  各家的接法按各自的存储布局不同：bitcask 用 `iter_meta_from` seek KeyDir、memory 用 BTreeMap
  `range`、bplus_tree 用 `scan_entries_break_from` 下降到 containing 叶子（前面的叶子一篇不读）、
  lsm_tree 要先把终点界穿进归并的每一个源 —— 新增 `SstFile::light_view_range` +
  `LSMTree::scan_latest_light_range` + `mem_view_light_range`（原来的 `scan_latest_light` 把每个
  SST **整篇块读完**再归并，所以窗口再窄也要付全库的块读代价）。四家各用自己的代价单位：
  读盘次数 / 折叠走的键数 / 页数 / SST 块读次数。
- **⚠️ 界下推的第一次复测就抓到我自己引入的回归**（如实留档）：`SstFile::light_view_range`
  起点写成 `find_block(from).unwrap_or(self.index.len())` —— 意思是"`from` 比第 0 块首键还小时
  跳过整篇"。于是**前缀是所有键的真前缀**那一类形状（`prefix_scan("0")`、`key_count("00")`）
  直接返回近乎空的结果：复测表里 lsm 的 `prefix_scan 2B前缀` 只命中 1 行、`key_count` 返回 0 键，
  而同库另三家都是 20000。改成 `unwrap_or(0)`（找不到就把第 0 块当起点），并在**四家各自的**
  窗口守卫里都补一条这个边界形状（`prefix_scan(b"k") == 全部键` + `key_count` 同形）。
  为什么以前没暴露：这类"前缀比所有键都短"的形状在既有单用例里根本没出现过，
  而台架是第一次拿真实宽度与前缀去跑四家。
- **过程中我自己引入、又被既有测试当场抓住的一个 bug**（值得留档）：第一版把终点哨兵取成
  `encode_key(end, 0)`。而 `seek_prev` 造的上界是 `key ‖ 0x00` ⇒ `key` 自己的版本行排在哨兵
  **之后** ⇒ 被判越界丢掉 ⇒ `seek_prev(k)` 返回比 k 小的那一版。内存引擎的既有用例
  `test_scan_prefix_seek` 直接跑红。修法：哨兵做成**一份共享实现**
  `common::version_key_upper_bound(end) = end ‖ 0xFF×8`，并把两条账单写进文档 ——
  ① 代价合同是「窗口宽度 + 至多一个同前缀组」（以 `end` 为前缀的键会被 range 带进来，
  再由 `raw ≥ end ⇒ continue` 滤掉；宁可多走，不可误删）；② 前提 `version < 2^56`。
  守卫三条：`upper_bound_keeps_trailing_zero_end`（钉哨兵形状本身），bitcask 与 memory 各有一条
  `test_windowed_scan_cost_scales_with_window_not_library` —— 既断言窗口结果与"全量之后筛"
  **逐条一致**，又断言计数随窗口缩（bitcask 用读盘次数，memory 用"折叠走了多少把键"，
  两家的代价单位本来就不一样）。
- **LSM 点查再降一档：稀疏索引二分 + MVCC 块缓存 64→256**。二分治的是**随库总量变贵**的那条
  纯 CPU 通道：`find_block` 原先线性扫索引（遇大于 key 才 break），代价 O(块数) = O(行数/64)。
  实测（250 块的样本）每次定位的比较次数 **125 → 7**，由
  `test_find_block_is_binary_and_agrees_with_linear` 钉住两件事：① 与线性扫**逐 key 给出同一个块**
  （块内/块边界/两块之间/文件内外都算），② 比较次数只能与 log2(块数) 同阶。新计数器 `index_probes()`。
- **量出了 LSM 点查的真正规律：代价 ∝「同一把键的版本散在几个 SST 里」**（基线用例新增的 (f) 组，
  release 实测，键集合完全相同、只改分散度）：

  | 每键分散文件数 | 块访问/次 | 未命中/次 | µs/次 |
  |---|---|---|---|
  | 1 | 2.00 | 0.19 | 12.1 |
  | 2 | 3.01 | 1.20 | 64.5 |
  | 4 | 5.02 | 3.22 | 154.6 |
  | 7 | 8.06 | 6.20 | 269.2 |

  ⇒ **`max_chain` 小不等于点查便宜**：版本链在内存里是 3 版，但只要这 3 版落在不同 SST 上，
  点查就要多探多个文件。这解释了下游"换了数据、跑 6 次差 8 倍"那一半：同一形状连跑 6 次，
  块访问与未命中次数**一次不差**（4.00 / 1.61），只有墙钟在 555–884 µs 之间飘 ⇒ 抖动来自
  **文件分散度 + OS/IO**，不是后台在跑回收或 compaction（MVCC 路径的后台压实线程默认根本没起）。
  块缓存从 64 提到 256 是按这条规律挑的：10005 行的库就有 ~157 个块，64 槽装不下工作集。
- **B+ 树 delete 路径的静默丢页（正确性缺陷，不只是日志洪水）**：`split_leaf` / 借键 / 合并
  这些重组**只按条数**判断（`order/2` 对半、`min_entries` 比较），从不量序列化字节。
  真实形状下（40 B 键 + 60 B 值、`order=64`）一页载荷 4080 B 只装得下 ~34 条，两个半页一合并
  就是 7.5 KB ⇒ `PageManager::write_page` 打印"数据超过页载荷限制"并返回 `false`，
  **而调用链没人检查这个 bool**：幸存页保持合并前的旧内容，父节点却已经删掉分隔键、
  `children.remove(...)`、`free_page(死页)` ⇒ 那批行同时失去两份副本 = 不可达。
  台架跑 bplus 的 vacuum / 删除阶段刷出的几万行那条消息就是这条路径。
  证明不是推理：把新增的字节检查临时关掉，`test_reorg_never_drops_oversized_pages` 在
  **第 36 次删除**就报"删除失败"——键真的丢了。
  修法：新增 `BPlusTree::node_fits`（问序列化结果，不问条数），四处重组（叶子/内部的借键与
  合并）在**动盘与 free_page 之前**先量，装不下就整次重组作废（locals 丢弃、盘上一字未动；
  下溢只是填充率低，不是错误）；`write_node` 改为返回 `bool`，`update_recursive` 也不再
  吞掉失败。合并路径的判断必须排在 `free_page` 前面，顺序反了就是两份副本一起没。
- **`write_page` 的超载告警改为"前 3 条打印 + 全部计数"**（新探针 `page::oversized_writes()`）。
  原来每条都 `eprintln`，一次基线刷几万行到 stderr —— 那本身在花钱，而且**污染了被测耗时**
  （bplus 的 `search 首页` 911 ms 里有它一份）。守卫就是断言 `oversized_writes()` 为 0。
  复测（release + 20k 行，同一台机器）：载荷告警 **几万行 → 0 行**；bplus 点读 57.7 → **33.6 µs/次**、
  灌数 156.8 → **93.3 µs/行**、一次全量 scan 33.3 → **24.7 µs/行**、`search 首页` 911 → **518 ms**、
  `key_count` 618 → **514 ms**。⚠️ 另一头也如实记：**`vacuum()` 206 → 450 ms** —— 放弃合并会留下
  更多下溢叶子、填充率下降，delete 密集之后的全库走查因此变贵；正解是"合并后再分裂"，尚未做。
  （**M14 已还这笔账**：页数 111 → 64 是计数证据，墙钟 `vacuum()` 落到 241~393 ms 区间，见下面那条。）
- **B+ 树的全库折叠从"每行再降一次树"改成一趟走完**（这条是 bplus 管理面贵的结构性原因）：
  `collect_latest_raw_in` 原来照抄 bitcask 的两段式 —— ① 只折键 ② 按胜出行 `kvengine.get(enc)`
  回树取值。对 bitcask 那是对的（KeyDir 只有元数据、值在日志另一处），对 B+ 树是错的：
  **value 就住在叶子条目里，遍历的时候已经在手上**。代价是每个存活 key 多付 3~4 次页访问 ——
  新加的归因基线实测：一次全量 scan **61146 次页访问 / 36.2 µs·行**；改成一趟之后
  **1146 次 / 1.7 µs·行**（同一份 20000 行的库），`search 首页` 718 ms → **34.4 ms**、
  `health()` 43.0 ms → **21.5 ms**。胜出行的 blob/TTL 解码仍然只在走完之后做一次
  （`test_collect_latest_decodes_only_live_keys` 不变）。新守卫
  `test_full_scan_pages_scale_with_leaves_not_with_keys_times_height`：2000 行只许 ≤200 次页访问
  （实测 64 次），旧实现是 6000 次，会红 30 倍以上。
- **bplus 读路径的钱到底花在哪**（新基线 `test_bplus_read_cost_attribution`，`#[ignore]`）：
  同一份数据连测两遍 —— 冷 41.8 µs/次 vs 热 41.3 µs/次，命中率 95%，把页缓存从 1024 开到
  65536 也还是 37.8 µs ⇒ **点读的 41 µs 是纯 CPU**（每次点读 4 个页访问，每个都要
  4 KB 拷贝 + bincode 解析整节点），不是在读盘。这一条决定了下一步该做"解析过的节点缓存"，
  而不是继续加大缓存。页缓存现在分开数 `cache_hits()` / `cache_misses()` ——
  原先 `page_reads()` 把两者算成同一个数，所以它守得住"代价不随链长涨"，答不出为什么慢
  （和 LSM 那次是同一个错误：代价单位选对了，冷热没分）。
- **`BPlusTree::open` 在并发测试里随机 `WouldBlock`** —— 同一类 flake 的第三处：Windows 的
  `SystemTime` 粒度约 15.6 ms，两个测试线程能拿到**同一个 nanos**，于是同名临时文件 + 同一把
  `.lock`，后来者直接失败。这次不再逐个补丁：`common::tmp_test_path()` 一份实现（进程内
  单调序号），把 bplus/bitcask/lsm 九个测试辅助函数全部接过去。
- **只读走查不再污染页缓存**（这条把上面 `health()` 变慢的账还了，而且比改前更快）。
  归因数据说话：20000 行的库有 **1257 页**，而 `max_cache_pages` 默认 **1024** ⇒ 一次全库走查
  每片叶子都要塞进缓存 ⇒ 逐页换出、脏页顺带 flush：实测一次 `health()` **换出 1600 页、
  写盘 765 页、150.2 ms**，紧接第二遍**命中数只有 1**（点读的工作集被毁）。
  新增 `PageManager::read_page_for_walk`：**未命中时只有在"还装得下"才回填**（走查对每片叶子
  只碰一次，回填没有下一次命中）；`scan_entries_with` / `scan_entries_break_from` /
  `BPlusTreeIterator::next` 三条走查改用它。改后同一份库：

  | 阶段 | 改前 | 改后 |
  |---|---|---|
  | `health()` 紧跟更新之后 | 150.2 ms（命中 189 / 换出 1600 / 写盘 765） | **21.2 ms**（命中 984 / 换出 **0** / 写盘 0） |
  | `health()` 第二遍 | 44.7 ms（命中 **1**） | **24.1 ms**（命中 1019） |
  | `search 首页` | 68.0 ms | **42.5 ms** |

  合同写成"换出与树高同阶、不随页数线性涨"，不是绝对 0：起点下降到那片叶子仍会回填几个导航页
  （小库实测 53 次页访问换出 2 页，旧实现是 ~50 页）。守卫
  `test_full_walk_does_not_evict_the_working_set`（缓存故意调到 8 页、库 53 页）+
  归因基线新增「走查之后的点读」一节。同时补了三个计数尺子：`page_evictions()` /
  `batch_flushed_pages()` / `lru_compactions()` —— 命中与未命中之外，必须能看见"这趟走查
  把多少页挤出去、顺带写了多少脏页"，否则又会犯"只数访问量、不数冷热"的老错。
- **B+ 树加了「解析过的节点」缓存**（#18，点读的账）：`BPlusTree::node_of` 只服务只读路径
  （`get_recursive` / `find_leaf_for_key` 下降 / `find_back` 的叶子读），命中就连 `read_page`
  都不走。失效**不靠手工**：`PageManager` 给每页记一个写入次数（`write_page` 与
  `allocate_page` 都 bump，后者是防"页被 free 后复用，把上一个占用者的内容当新的读"），
  缓存项存 `(这页当时的写入次数, Arc<BTreeNode>)`，次数变了才重解析。比"八个写点各清一次"
  稳 —— 漏一处就是读到脏节点，而这种 bug 通常只在特定交错下出现。
  实测（20000 行、1257 页、`max_cache_pages=1024`）：热态点读 **41.3 → 23.5 µs/次**，
  页访问 **4.00 → 1.48/次**，解析 **0.48/次**；把容量开到装得下（65536）则是
  **16.3 µs/次、页 1.06、解析 0.06** —— 剩下的差距不是算法，是**容量不够**，
  这条直接给 #17（旋钮可配）提供了量化依据，不再靠感觉。
  守卫 `test_node_cache_never_serves_stale_content`：边插边读、200 次覆盖写、
  删除后插回（走 free→allocate 复用）三段都必须立刻读到新值。
  归因基线新增 `node_parses()` 计数与「第二遍解析数必须显著低于页访问数」的断言，
  并额外断言 `cold_parses > 0` —— 探针自己不证明会红，就等于没有探针。
- **LSM 点查慢两个数量级的根因不是复杂度，是"块缓存未命中就重开文件"**：下游把本 crate 嵌进去
  实测**单次点查 0.86 ms**（5002 键 / 40B 键 / 60B 值 / 10005 行 / `max_chain=3` / 全 release），
  本仓库同形状复现到 **289 µs**。`SstFile` 原来只存 `path`，`read_slice` 每有一次块缓存未命中就
  `File::open` + seek + read 一遍。本机实测**一次 `File::open` ≈ 167 µs**（乱序 289.0 vs 有序
  45.6 µs/次，差值 ÷ 多出来的 1.46 次未命中），比读那几 KB 本身贵一个量级。
  现在 `SstFile` 持一个**懒开的常驻只读句柄**：SST 不可变、路径按 id 唯一 ⇒ 句柄可以用到对象销毁；
  Windows 下 std 打开带 `FILE_SHARE_DELETE`，所以 compact 之后删旧 SST 仍然成功（实测全绿）。
  改造后实测：乱序 289.0 → **120.3 µs/次**、有序 45.6 → **21.0 µs/次**、一次全量 `scan`
  29.2 → **19.1 µs/行**。
  配套：新增计数器 `block_misses()`（未命中次数）与基线用例
  `test_lsm_point_read_baseline_5000_keys`（`#[ignore]`，release 手跑，打印 µs/次、块访问/次、
  未命中/次）。**为什么以前的守卫没抓住**：LSM 那条守卫数的是"访问了多少个块"，命中与未命中
  算成同一个数 ⇒ "块访问只有 4 次"和"单次点查 0.86ms"能同时成立 —— 代价单位选对了（块），
  但没把"这个块的字节是从内存拿的还是从 IO 拿的"分开看。
  下游用法这一半也要说清：`multi_get` 现在就是 `map(get)`，**批量取并不省钱**（实测 19.5 vs
  21.0 µs/次）；要读一批键，一次 `scan`/`prefix_scan`（19.1 µs/行）才是对的路。
  计数器随后由 `file_opens()` 改名为 `block_misses()`：常驻句柄之后"开了几次文件"不再是代价单位。

- **点读点写从 O(链长) 降到 O(1)**：版本键大端 ⇒ 最新版在链尾 ⇒ 从高界反向走一步即命中，
  不必正向扫完整条链再逐版本读 value。实测探针：bitcask 读盘 23.8 万次→1 次；bplus 页读
  1015→8（只剩树高）；lsm 块读与链长解耦；memory 恒定。
- **全库类 API（scan / exists / 搜索 / export / purge_expired / 折叠）从 O(全库物化) 降到
  O(存活 key 数)**：先把键折出每把 key 的胜出行，只对胜出行取值。实测：读盘/解码/物化次数
  8 vs 960（120 行 × 8 版的样本库）。
- **冷启动不再全库扫**（bitcask）：优先 WAL Checkpoint 的 `next_version` → WAL 最大 xid →
  才退化到全库扫 KeyDir。以前"重启止血"这个动作本身一次比一次慢。
- **vacuum 自己不再是全库最重的路径**：分组只走元数据/键视图，不物化 value；blob GC 只读
  保留下来的行。旧实现有两次全库 `iter()`，等于「一开自动回收，就在回收那一刻制造新事故」。
- **M13 接线之后复跑 release / 20k 行的四引擎全表，没有回退**（耗时列按 ±2× 噪声读，
  结论只来自计数）：bplus 点读乱序 16.0 / 有序 6.6 µs、全量 scan 1.6 µs/行、窗口 scan
  1.9 µs/行、`health()` 25.3 ms、`vacuum()` 466 ms（这条仍是"合并后再分裂"未还的账）；
  lsm 点读 15.2 µs、全量 scan 4.4 µs/行、窗口 2.6 µs/行；bitcask 窗口 6.8 µs/行（上次 3.0，
  同一份代码 + 计数未变 ⇒ 记为计时噪声，不当结论）；memory 全部在 1~3 µs 量级。
  接线本身零成本可解释：`try_open_with(dir, None)` 走的就是原先那条分支，只是多了一个
  `Option` 的判断；`tuning()` 不在热路径上（管理面调用）。

- **B+ 树「合并后再分裂」（M14，还掉上面那张 466 ms 的单）**：相邻两页的 union 装不下一页时，
  不再整次作废，而是**按字节重切成两个各装得下的半页** —— 不释放页、不动父子结构，只把父节点
  那把分隔键换成新右半页的最小键（叶子是复制、内部节点是"拉下来再提回去"，键数与子数守恒）。
  **分界取"左页装得下的最大条数"而不是中点**：中点切法两片都停在半满，页数永远降不下来；
  压满左页会把剩余挤成一小片，那片下一轮就能和自己的右兄弟合并成功 ⇒ 真的释放一页。
  同一条通道**另一半方向也有同一个洞**（插入侧）：`split_leaf`/`split_internal` 过去按条数对半切、
  两半都不量字节就 `write_page` ⇒ 遇到一条比一页还大的行，写不进的那一半只留下一行日志，
  却已经变成**没人引用的页**（`MVCC` 那边是 `kv.insert(...)` 丢掉返回值，`set` 照样返回 `true`）。
  现在：① 分裂点由同一套字节账选出（两半都必须装得下、条数都必须 `< order`，取**中点** ——
  分裂要两边都有增长空间，与上面重组取最满一档正好相反，两处照抄都会静默更差）；
  ② 找不到合法分界 ⇒ **一字不写**，`insert` 返回 `false`；③ 插入改成"**下降一趟全在内存里算、
  到顶统一落盘**"（`InsertStep::writes`）—— 否则"孩子已分裂写完、父亲却放不下"这一格同样会
  把分出去的一半变成孤儿页；④ 新增 `BPlusTree::row_fits(key, row)` 谓词，`MVCC::set` 在
  **append WAL 之前**先问它 ⇒ 拒收不会留下"提交了但树里查无此键"的 WAL 记录；bulk 与恢复重放
  没有返回值通道，走 `insert_or_shout`（`log::error!` 点名 + 一字未写）。
  **结论只从计数拿**：同形状（4000 行插满、隔一个删一个）**叶子 222 → 64 页**，
  把重切那一支关掉是 **111 页** ⇒ 页数 1.73×，而"每页装得下几条"是当场量出来的（35 条），
  理想页数 58 —— 这条与墙钟无关，也不会被机器抖动骗过去。
  墙钟同一份构建连跑四次的区间（本机 ±2× 噪声，如实记）：`vacuum()` **241 / 252 / 300 / 393 ms**、
  `health()` 14.3~23.8 ms、`search_keys` 首页 26.8~46.5 ms、点读乱序 11.7~20.3 µs、
  灌数 72~82 µs/行（改前那一次是 466 ms / 25.3 ms / 38.2 ms / 16.0 µs / 107 µs 每行）——
  方向一致、幅度**不敢报成一个数**。另三家与此无关，全表复跑无回退。

### 修复

- **LSM：≥ `blob_threshold`（默认 4 KiB）的 value 通过 MVCC 读回来是 9 字节的 blob 指针，不是原值。**
  引擎里只有 `get` / `get_at` 解析指针，而 MVCC 的读走的是 `find_back` / `range_scan` / `iter` /
  `collect_latest_raw` 那一族 ⇒ 全都在往外发裸指针：**不报错、不 panic**，只是值悄悄变成一个小乱码。
  现在解析收成一个口子（`deref_value`），凡把存储值交给调用方的路径都过它；`get_raw_key_at`
  刻意保持裸的指针形态，因为 blob GC 就是靠指针字节认"哪个 blob 还有人引用"——解析过后反而会
  把幸存的 blob 全删掉。Bitcask / B+ 树没有这一类洞：它们的 blob 分层在 MVCC **之上**
  （`storage.rs` 的 `decode_stored_value` 是所有读法的必经之路），LSM 的分层在引擎**里面**。
- **LSM：`blobs/` 旁路以前从来没人回收**，大 value 覆盖写一次就多一个永久文件。现在由 `vacuum()`
  的全库轮次收（compact 判不了"别的层里还有更新的版本引用同一个 blob"）；有界轮次不动 blob。
- **Bitcask：崩在 `merge()` 改名之前会把库变成"打开成功但是空的"**。`merge()` 先删旧 `data.log`
  再把 `data.merge` 改名回来，中间崩掉就只剩 `data.merge`，而 open 找的是 `data.log`，
  `Log::new` 又是 create-if-missing ⇒ 新建一个空日志，全部数据"消失"且无人报错。
  现在 open 会认回这个残骸（并清掉会造成二次污染的另一半）。顺带：hint 指的是向已经不存在的
  分片时回退全量扫 —— 增量扫永远不会补回那些键。

- README 的「Bitcask merge 不是原子的，崩溃会丢数据」按 M6a 的现状改写为"不是原子但可恢复"。
- **README 新增「下游接入使用说明」**：起库的四种写法、各家"一个库"在磁盘上的形状（B+ 树是**文件**、
  bitcask/lsm 是**目录**）、事务必须知道的四条语义（写冲突返回 `false`、未 commit 就丢弃 ⇒ 自动回滚、
  只读事务不写 WAL 不 fsync、value 是打过信封的逻辑值）、跨线程能做到什么与做不到什么
  （`Arc<dyn KvEngine>` 可用，但四引擎仍是单写者 —— **句柄可共享 ≠ 并发写**）、bulk 导入、
  观测面与两个必盯的数、选型谓词。第 7 条专门列**目前还不能指望库做的事**：没有 `migrate()` 库 API、
  ~~`EngineConfig` 只暴露 B+ 树两个旋钮、`vacuum_bounded` 不在门面上~~（这两条 M13 已补，见本段末）、
  值信封仍靠业务侧守。
- **「安装」改写为「引入方式」并如实标注版本落差**：当时架上最新仍是 **0.3.0**，也就是出事故的
  那版（点读沿链全扫、无自动回收、`KvEngine` 非 `Send + Sync`、WAL 不收口、LSM 大 value 读回裸指针、
  merge 残骸认不回）⇒ 要拿修复版只能走 `path` / `git` 依赖。原先这里写的是 `storage-engines = "0.1"`。
  ⚠️ 这条已被 0.4.0 的发布部分作废，M13b 按实测改写：**0.4.0 于 2026-10-15 上架**，所以"事故修复
  只能走 path/git"不再成立；但架上那份不含 10-16 之后的 **M10–M25**（截至 2026-10-18），
  所以"最新修复在 master 上"这半句仍然成立。
- **「已知局限」里「事务不是 `Send`/`Sync` 友好的多线程设计」按 M9 划掉改写** —— 那条已经反了：
  `KvEngine: Send + Sync`、`KvTxn: Send`；同时补上门面还缺的三项，指向使用说明第 7 条。
- **M13 之后 README 的三处同步改写**：使用说明第 1 条加「旋钮：`EngineConfig` 一个结构体装齐四家」
  的字段表（谁吃哪条、默认值、能不能热改）+ `tuning()` 读回示例；第 5 条补门面上的
  `vacuum_bounded` 游标循环与实测的"160 行/预算 25 ⇒ 3 轮、单轮最多 40 行"；第 7 条把
  「调参只开了 B+ 树」「有界回收不在门面上」两条**删掉**，换上此刻真实的边界（open 档 vs 热档、
  `tuning()` 只列代价相关的旋钮、门面不暴露列族、仍无 `migrate()` 库 API）。「门面层设计」的
  trait 方法与 `impl_engine!` 开关清单同步（新增 `wal: yes|no`）。

### 测试

264 个单元测试（260 跑 + 4 个 `#[ignore]` 的大库基线/长跑基线）+ 4 个集成测试 + 5 个文档测试。新增的都是**计数断言**（读盘/页/块/解码/物化/克隆/索引比较次数/超载写次数），不用计时：原有用例结构上抓不到这类病 —— 每条用例的版本链都是个位数长。每引擎每通道各有自己的守卫，
并且复杂度守卫都先验证过"它会红"（临时短路被测代码跑一遍）才算数。
- **B+ 树重组四条（M14）**：
  - `test_reorg_packs_leaves_instead_of_leaving_them_half_empty`：**页数**是唯一对这件事敏感的尺子
    —— 行数在"作废"与"重切"两种结局下都不变，所以"没丢数据"那类断言完全看不见这个病。
    尺子直接量"一页装得下几条"（逐条加到 `node_fits` 失败为止 ⇒ 35 条），理想页数 = rows/35；
    实测 64 页 vs 理想 58（关掉重切 = 111 页，当场红）。
  - `test_reorg_size_formula_matches_bincode`：分界全靠"逐条字节相加"这个公式选，所以公式必须
    与 `bincode::serialize` 的真实长度**逐档相等**（pad 0/7/40/120/300 × 有/无 `next_leaf`，
    叶子与内部节点各一遍）。这条不是可选的：公式偏了，落盘前的 `node_fits` 只会让代码退回
    "什么都不动" —— 测试全绿、优化静默不发生。
  - `test_oversized_row_is_refused_not_dropped_silently`：8 KB 的单行必须**被拒绝**而不是被吞掉
    （`insert` 返回 `false`、新探针 `split_drops()` 计数 +1、已有 300 行一条不少、
    `oversized_writes()` 仍为 0）。把分裂退回"按条数对半切、不看两半"，它红在第一句。
  - `test_oversized_row_is_refused_before_the_wal`：拒收必须发生在 **append WAL 之前** ——
    直接读 WAL 记录数 `Write`（不是比字节数：M8 之后第一次 `write()` 会补一条 `Begin`，
    拿字节数当尺子会把这条合法的 Begin 算成罪状，我第一版就是这么误报的）。
    把守卫挪到 append 之后 ⇒ `left: 1 / right: 0` 当场红。同一条里还断言"探针数得到那条
    真实写入的 `Write`"，否则 `== 0` 是空转。
- **`scan_page` 与 `migrate` 两条（M17）**：
  - `scan_page_costs_the_page_and_walks_the_whole_library_once`（bitcask，尺子 = 折候选看了
    多少把编码键）：一页 50 行只折 **52 把**（全库 `scan` 是 2000 把），40 页走完后
    "逐条顺序与一次 `scan` 完全一致" —— 只测便宜不测等价，就是拿错数据换性能。
  - `migrate_walks_a_ring_of_engines_byte_for_byte`：跨引擎**环**一圈
    （bitcask→lsm→bplus→memory），四形状（普通 / `0x00` 打头 / `0x01` 打头像 TTL 信封 /
    真带 TTL），每一跳断言"export 内容与源头逐条相同 + TTL 仍在 + 裸字节没被吃"。
    **为什么必须绕一圈**：第一版把 `get_meta` 写在**目标**事务里（那时那把 key 还不存在，
    TTL 一律读成"永久"），只跳一次源就是 bitcask 自己 ⇒ 看不出丢；让每一家既当源又当目标才暴露。
    这条守卫第一次跑就红了。
- **`set_raw` 字节形状守卫（M16）**：`set_raw_round_trips_every_byte_pattern` 在门面上过四家，
  形状取"最容易踩"的几组：空值、单个 `0x00`、`0x00` 打头、`0x01` + 8 字节看着像"已过期的 TTL
  信封"、`0x02` 打头、5000 字节（走 blob 旁路）、0..255 全字节表；每条都断言 `get`/`multi_get`/
  `scan` 三条读路径一致，磁盘引擎还要**重开之后**再比一次。同一条里留了**对照组**：拿 `set` 存
  那两组打头的字节必须丢（否则分不清"`set_raw` 真在做事"与"它只是个改名的 `set`"）。
  证红：把四家的 `pack_plain` 去掉（退化成 `set`），立刻红在
  `left: Some([]) / right: Some([0])`。
- **窗口分页五条（M15）**：四引擎各一条 `test_search_window_costs_the_window_not_the_library`
  （尺子 = 本引擎的走键/走页计数：bitcask 22/2000、bplus 2/62 页、lsm 22/2000、memory 22/2000），
  每条都同时断言"窗口内容 == 全量同一页"、`total` 是 `None`、`has_next` 在首页为真/末页为假。
  共用件另有两条自己的守卫：`search_page_kernel_distinguishes_unmeasured_from_exhausted`
  （分页核的三条分支：`total`/`total_pages`/`has_next`，重点是**"没数"与"到底了"必须分得开** —
  写错任何一侧，翻页要么提前停、要么永远停不下来）与
  `fold_window_resumes_when_keep_drops_candidates`（驱动的续跑那一支：引擎级守卫的库里没有
  过期行，那条分支一次都踩不到，所以直接喂给它 —— 断言窗口不被交短、第二轮预算确实变大）。
  再加一条门面的 `windowed_search_agrees_across_engines`（四家必须给同一页的同一份内容 ——
  一家不同，下游 wrapper 里就多一句 `if engine == ...`）。
  证红方式：把 `fold_window` 的预算改成"永远走完全库"，四条**全红**（首页 2000 / 全量 2000）；
  把守卫换成只数取值的尺子时 bitcask 与 lsm 会**假绿**，因此另加了两把走键的计数器。

- **配置旋钮三条（M13）**：
  - `engine_config_knobs_reach_the_engine_and_are_readable_back`：`EngineConfig` 的每个字段都要
    在**那个引擎**里读得回（B+ 树 `order`/`max_cache_pages`、bitcask `max_file_bytes`、LSM
    `block_cache_blocks`/`mem_threshold_bytes`/未动的字段仍留在 `for_mvcc()` 档、`wal_checkpoint_bytes`、
    `vacuum_auto_slice`），并配**同一入口默认档的对照**（`64/1024`、`64 MiB`、`256/32 MiB`、
    `16 Ki`）—— 没有对照，键名漂了或引擎没接这条时用例照样绿。给 `memory` 设 WAL 阈值必须
    `Err(Unsupported{engine:"memory",...})`，且它的 `tuning()` 里**没有** `wal_checkpoint_bytes` 这个键。
    把 `try_open_with` 临时改成"忽略补丁"，这条立刻红（`left: 256 / right: 999`）。
  - `lsm_mvcc_pins_the_two_fields_that_would_break_crash_safety`：`enable_mem_wal` /
    `enable_bg_compact` 传 `true` 也必须是 `false`，open 与 `set_options` 两条路都要钉；
    同时断言 `block_cache_blocks` 这类**该生效的要生效**（钉两条不等于连坐）。临时把
    `pin_mvcc_invariants` 掏空，它红在第一句"事务 WAL 是唯一的持久化点"。
  - `vacuum_bounded_is_bounded_and_ends_where_vacuum_does`：160 行 / 预算 25 ⇒ 四家都是
    **3 轮、单轮最多 40 行**（余量 = 一条链，因为截断只在逻辑 key 边界），游标严格前进，
    接力扫完的终态与 `vacuum()` 一致（每 key 剩一行）；另一半断言 `budget_rows = u64::MAX`
    的**一轮**读满 160 行、`next_cursor == None` —— 证明"两个入口一套规则"不是说法。
    把预算临时放大到 1000（> 全库），断言"一轮扫完说明根本没限流"立刻红。
- **窗口/前缀扫随窗口宽度缩**：`test_windowed_scan_cost_scales_with_window_not_library` 四家各一条
  （尺子分别是读盘次数 / 折叠走的键数 / 页数 / SST 块读次数），每条都同时断言"结果与全量之后筛
  逐条一致"和"计数随窗口缩"，外加一条 `prefix_scan(所有键的真前缀)` 的边界形状。
- **哨兵形状** `upper_bound_keeps_trailing_zero_end`：钉住 `seek_prev` 那种 `key ‖ 0x00` 的上界
  为什么不能用 `encode_key(end, 0)`（这条是先把内存引擎的既有用例跑红才发现的）。
- **B+ 树重组** `test_reorg_never_drops_oversized_pages`：4000 个 35B 键 + 60B 值、隔一个删一个，
  断言超载写次数为 0 且剩下的键全部仍可读、叶子链表行数一致。把字节检查临时关掉，
  它在**第 36 次删除**就报"删除失败"——这就是"静默丢数据"的直接证据。
- **稀疏索引二分** `test_find_block_is_binary_and_agrees_with_linear`：与线性扫逐 key 对拍 + 比较次数只与 log2(块数) 同阶。
- **懒分配缓冲** `test_write_buffer_is_allocated_lazily`：打开时容量必须为 0，首次写入后必须 ≥ 目标。
- **门面入口三条**：`open_engine_accepts_enum_and_string_in_every_ownership_shape`、
  `enum_without_path_is_path_required`、`unknown_name_is_echoed_verbatim_whatever_the_shape`。
  前一条同时是 `EngineKind::as_str` 与 `canonical_name` 两张表的汇合点 —— 枚举写法漏实现会
  **编译失败**，两张表漂开会 panic，都不是"跑起来悄悄换了一家"。
- **长跑基线守卫 `test_long_run_baseline`（`#[ignore]`，release 手动跑）**：把事故形状
  （热点 key 持续更新 2 万次 + 稳定点读）跑成"开 vs 关自动回收"的对照，实测数字写进 README。
  两条从实测里改出来的认知（原先是按推理写的）：
  - 链的稳态上界是 **2 × 切片宽度**而不是 1 ×（一片被一条大链吃满 ⇒ 每两小轮才轮到它一次）；
    我第一版断言写的是 `≤ 切片+2`，跑一次就红，界按实测改正。
  - 关掉回收那列链涨到 2 万行，**点读耗时却完全没变差** ⇒ 通道 ①（点读成本）与通道 ③（回收）
    确实各自独立，"只修一条"不再有连锁反应 —— 这条现在是断言+打印，不再只是说法。

### 下游升级要点（trading-arb / trading-system 内嵌副本）

1. 内嵌的 `src/storage/*mvcc.rs` 是 0.3.0 之前的复制，缺以上全部修复 —— 症状就是那条随 uptime
   爬升的 CPU 曲线。要么改成依赖本 crate，要么把七条通道的修复逐个搬过去。
2. 搬之后**先量基线再优化**：`health().max_chain` 连续采样，看它是否从"线性爬"变成"贴着 1~切片宽度"。
3. 别急着给 `vacuum()` 加定时器：自动回收已经在跑，手动 `vacuum()` 保留给"要立刻收文件空间"的场景
   （它是唯一会触发 `merge()` / blob 重写的入口）。
4. **依赖怎么引(2026-10-17 用 crates.io API 实测后改写;原先这条写的是"架上只到 0.3.0"，那句是发布前
   写的、已过期)**：架上最新是 **0.4.0**（2026-10-15 发布，未 yank）⇒ 七条事故通道的修复与 M9 的
   门面改造用 `storage-engines = "0.4"` 就能拿到，不必再绕 `path`/`git`。
   但架上那份**不含 10-16 之后的 M10–M25**（LSM 点查常驻句柄与稀疏索引二分、四引擎窗口 scan
   界下推、bitcask 懒分配、B+ 树 delete 静默丢页修复与读路径三项、配置旋钮 + `tuning()`、
   事务内核合成一份、`migrate` + `scan_page`、LSM 灌行 O(N²)、`seek_prev`、游标下推、
   B+ 树页缓存与 LSM 块缓存两个自动档、写集哈希、WAL `Begin` 不落盘）；
   要这些只能 `path`/`git` 依赖，或等 0.4.1（发布不可逆，由作者手动执行）。
   ⚠️ 这条对下游不是纸面差别："单次点查 0.86 ms"那个现场症状正是 **M10** 修的那条通道，
   而 M10 不在架上那份 0.4.0 里 ⇒ 只把依赖升到 `"0.4"` 治不了它。
   磁盘格式 M10–M25 一个都没动，所以升级不需要重建库。
5. M13 之后，下游 wrapper 里**调参那一层也可以删了**：原先要绕过门面自己构造
   `crate::lsm_tree::MVCC` 才能设的块缓存/分片上限/WAL 阈值，现在都在 `EngineConfig` 里，
   并且 `db.tuning()` 能在启动日志里把"到底生效了没有"打出来（长跑进程调参没生效是查不出来的
   那类问题）。回收也从"只有全库一档"变成 `db.vacuum_bounded(cursor, 预算)` 可接力。
