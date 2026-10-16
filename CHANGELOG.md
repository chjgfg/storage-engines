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

- **窗口 scan / 前缀 scan 的代价开始随窗口宽度缩（bitcask + memory + bplus_tree 已接，
  lsm_tree 在下一里程碑 —— 它的归并原语还缺一个终点界）**。
  根因不在遍历原语，在 MVCC 层的**用法**：`scan_foreach` 调的是 `collect_latest_raw()` ——
  折完**全库**每个 key 的胜出行，再由调用方逐条筛 `start/end`。于是 200 行的窗口和 20000 行的
  全量一样贵（大库基线实测：窗口 scan bitcask **376.6 µs/行**、memory **106.3 µs/行**，
  而同库一次全量 scan 是 3.8 / 1.1 µs/行）。新增 `collect_latest_raw_in(include_deleted, start, end)`：
  起点直接 seek、终点早停；`collect_latest_raw` 退化成它的无界特例，**调用方一字未改**。
  `search_keys` 的 **Prefix** 模式同样接上界（Contains 要逐键比子串，全库是它的本质代价，
  这里不假装优化）；`key_count(prefix)` 经 `prefix_scan` 自动受益。
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
  `EngineConfig` 只暴露 B+ 树两个旋钮、`vacuum_bounded` 不在门面上、值信封仍靠业务侧守。
- **「安装」改写为「引入方式」并如实标注版本落差**：crates.io 上最新仍是 **0.3.0**，也就是出事故的
  那版（点读沿链全扫、无自动回收、`KvEngine` 非 `Send + Sync`、WAL 不收口、LSM 大 value 读回裸指针、
  merge 残骸认不回）⇒ 要拿修复版只能走 `path` / `git` 依赖。原先这里写的是 `storage-engines = "0.1"`。
- **「已知局限」里「事务不是 `Send`/`Sync` 友好的多线程设计」按 M9 划掉改写** —— 那条已经反了：
  `KvEngine: Send + Sync`、`KvTxn: Send`；同时补上门面还缺的三项，指向使用说明第 7 条。

### 测试

236 个单元测试 + 4 个集成测试。新增的都是**计数断言**（读盘/页/块/解码/物化/克隆次数），不用计时：
原有用例结构上抓不到这类病 —— 每条用例的版本链都是个位数长。每引擎每通道各有自己的守卫，
并且复杂度守卫都先验证过"它会红"（临时短路被测代码跑一遍）才算数。
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
4. **本 crate 在 crates.io 上最新只到 0.3.0**（0.1.0 / 0.2.0 / 0.3.0），也就是说出事故的那版才是
   下游 `storage-engines = "0.3"` 拿到的东西。要用依赖代替内嵌副本，得先把 0.4.0 发上去；
   在发布之前只能走 `path`/`git` 依赖。
