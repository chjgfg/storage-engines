# storage-engines

四个从零手写的 KV 存储引擎 + 一层统一 trait 门面。**教学 / 学习性质**,不是生产级数据库。

同一套 MVCC 事务语义(快照隔离、写冲突检测、vacuum 回收)跑在四种截然不同的存储结构上,可以直观对比 **LSM Tree / B+ Tree / Bitcask / 纯内存** 在读写放大、范围扫描、崩溃恢复上的取舍。

- 约 2.3 万行 Rust,edition 2024(需要 rustc ≥ 1.85)
- 依赖只有 5 个:`serde` / `bincode` / `memmap2` / `fs4` / `log` —— B+ 树、SST、Bloom、WAL、双写缓冲、CRC32 全部自己实现
- 273 个单元测试 + 4 个集成测试
- 当前版本 **0.4.0**:随 uptime 恶化的七条通道逐条堵死,版本回收改为自动 —— 破坏性变更与升级要点见 [CHANGELOG.md](CHANGELOG.md)。⚠️ crates.io 上的 0.4.0 是 **2026-10-15** 发的那版,它**不含** M10–M13(10-16 之后的提交:LSM 点查常驻句柄、窗口 scan 界下推、bplus 读路径与丢页修复、配置旋钮)—— 要那些得走 path/git 依赖,见[引入方式](#引入方式)
- 许可证:MIT 或 Apache-2.0 任选

---

## 引入方式

⚠️ **先看这条(2026-10-17 用 crates.io API 实测,不是凭印象)**:架上最新是 **0.4.0**
(发布于 2026-10-15,未 yank)。**这一版已经包含**七条"随 uptime 恶化"通道的修复与 M9 的门面
改造(`KvEngine: Send + Sync`、类型化选型、`EngineName`)⇒ 版本号依赖就能拿到。

但 0.4.0 那个发布件**不含 10-16 之后的 M10–M13**:LSM 点查的常驻文件句柄与稀疏索引二分
(下游实测单次点查 0.86 ms 那条就是它)、四引擎窗口/前缀 scan 的界下推、bitcask 懒分配写缓冲、
B+ 树 delete 路径的静默丢页修复 + 读路径三项、`EngineConfig` 的配置旋钮与 `tuning()`。
磁盘格式这五批**一个都没动**,所以不是"换了就得重建库"——是同一份库,行为与代价不同。

```toml
# (a) 同机项目:path 依赖 —— 拿当前 master,含 M10–M13
storage-engines = { path = "../storage-engines" }

# (b) 走远端:git 依赖(gitee 与 github 内容一致)
storage-engines = { git = "https://github.com/chjgfg/storage-engines.git", branch = "master" }

# (c) crates.io —— 0.4.0:事故修复齐全,但没有 10-16 之后的读路径/配置改动
storage-engines = "0.4"
```

`cargo install storage-engines` 装到的同样只是已发布的 0.4.0。要 M10–M13 又想要版本号依赖,
只能发 0.4.1 —— 发布不可逆(只能 yank),由作者手动执行。

MSRV:edition 2024,需要 **rustc ≥ 1.85**。依赖只有 5 个(`serde` / `bincode` / `memmap2` / `fs4` / `log`)。

---

## 快速开始

```bash
cargo run                     # 默认 memory 引擎,不落盘
cargo run -- lsm              # 库建在 ./data/lsm
cargo run -- bitcask ./mydb   # 自己指定目录
cargo test                    # 跑全部测试
```

`src/main.rs` 是一份可运行的完整示例,覆盖门面层所有能力(读写 / 批量 / 扫描 / TTL / 写冲突 / bulk 导入 / 维护操作)。

另有 12 个命令行工具用于**跨引擎搬数据**和**压测**,详见 [命令行工具](#命令行工具数据搬运与压测):

```bash
cargo run --release --bin bitcask-export-kv -- --dir ./data/bitcask --out snap.bin --format bin
cargo run --release --bin lsm-import-kv     -- --input snap.bin --dir ./data/lsm --format bin
```

### 核心用法

换引擎只改入口那一行,下面的代码一个字不用动:

```rust
use storage_engines::{open_engine, EngineKind};   // 这些名字在 crate 根也可直接 import
use std::path::Path;

// 编译期就定死用哪家
let db = open_engine(EngineKind::LsmTree, Some(Path::new("./data/lsm")))?;
// 引擎名来自配置文件/环境变量时,同一个入口换成字符串
let db = open_engine("lsm", Some(Path::new("./data/lsm")))?;

let tx = db.begin();
tx.set(b"key", b"value".to_vec());
tx.commit();
```

起库之外还要知道的:目录形状、事务的冲突/回滚语义、跨线程能做什么不能做什么、长跑进程的观测面,
以及**目前还不能指望库做的事** —— 都在下面[下游接入使用说明](#下游接入使用说明)。

引擎名大小写不敏感,`-` 和 `_` 等价,并接受别名:

| 规范名 | 别名 | 需要路径 |
|---|---|---|
| `bitcask` | — | 是 |
| `bplus_tree` | `bplus` / `btree` | 是 |
| `lsm_tree` | `lsm` | 是 |
| `memory` | `mem` | 否 |

---

## 下游接入使用说明

这一节是给"引进来直接用、不再自己包一层"的读者写的。入口名字在 **crate 根**就能 import
(`use storage_engines::{open_engine, EngineKind, ...}`),全路径 `storage_engines::engine::*` 同样可用。
方法全集(含 `scan` / `search_keys` / TTL 三态 / 计数器)在[门面层设计](#门面层设计)。

### 1. 起库:四种写法

```rust
use std::path::{Path, PathBuf};
use storage_engines::{open_engine, open_from_env, EngineConfig, EngineKind, StorageConfig};

// (a) 编译期就定死用哪家,路径自己给
let db = open_engine(EngineKind::BPlusTree, Some(Path::new("data/bplus_tree/bplus.db")))?;

// (b) 引擎名来自配置文件/环境变量:同一个入口,入参换成字符串(别名、大小写不敏感)
let db = open_engine("bplus", Some(Path::new("data/bplus_tree/bplus.db")))?;

// (c) 连目录规则一起交给库:位置恒为 data_dir/<engine>/,父目录自动建好
let db = StorageConfig {
    kind: EngineKind::BPlusTree,
    data_dir: PathBuf::from("data"),
    engine: EngineConfig::default(), // 四家的旋钮都在这一份里,取值见下面的旋钮表
}
.open()?;

// (d) 读环境变量 STORAGE_ENGINE / STORAGE_DATA_DIR
let db = open_from_env()?;
```

四种写法返回的都是 `Box<dyn KvEngine>`。第一个参数的类型是 `EngineName` trait:`EngineKind` /
`&str` / `String` / `&String` 都能直接传,拿着枚举不必自己 `.as_str()`。

#### 旋钮:`EngineConfig` 一个结构体装齐四家

| 字段 | 默认 | 生效于 | 说明 |
|---|---|---|---|
| `order` | 64 | 只有 `bplus_tree` | B+ 树阶数,**只能打开时设**(改了等于让新旧页用两套节点形状) |
| `max_cache_pages` | 1024 | 只有 `bplus_tree` | 页缓存容量(页数)。20k 行 ≈ 1257 页 > 1024,库大了就按页数往上调 |
| `lsm` | `None`(= `LSMTreeOptions::for_mvcc()`) | 只有 `lsm_tree` | 整份 options;改单个字段用 struct-update 从 `for_mvcc()` 抄基线 |
| `bitcask_max_file_bytes` | `None`(= 64 MiB) | 只有 `bitcask` | active 分片软上限,决定 rotate 与 `merge()` 的粒度 |
| `wal_checkpoint_bytes` | `None`(= 16 MiB) | 三个磁盘引擎 | 涨过就在下一次提交后自动 checkpoint;`Some(0)` = 关闭 |
| `vacuum_auto_slice_rows` | `None`(= 16 Ki 行) | 四引擎 | 自动回收切片宽度;`Some(0)` = 关闭 |

```rust
use storage_engines::{open_engine_with, EngineConfig, LSMTreeOptions};

// 只改要改的那个字段,其余留在引擎自己的 MVCC 档上
let db = open_engine_with(
    "lsm_tree",
    Some(Path::new("data/lsm_tree")),
    &EngineConfig {
        lsm: Some(LSMTreeOptions { block_cache_blocks: 1024, ..LSMTreeOptions::for_mvcc() }),
        ..Default::default()
    },
)?;
```

**写进去要能读回来**:`db.tuning()` 返回这个引擎当前生效的参数快照(键名与上表对齐),
各引擎只列自己有的键 —— 它同时是"这家能调什么"的清单。

```rust
let t = db.tuning();
assert_eq!(t.get("block_cache_blocks"), Some("1024"));
assert_eq!(t.get("wal_checkpoint_bytes"), Some("16777216")); // memory 上这条是 None
```

三条边界,都由守卫钉住(`engine_config_knobs_reach_the_engine_and_are_readable_back`、
`lsm_mvcc_pins_the_two_fields_that_would_break_crash_safety`):

- 每个引擎只读自己那几个字段,其余**忽略**;忽略不等于假装成功 —— 给 `memory` 设
  `wal_checkpoint_bytes` 会直接 `Err(Unsupported{engine:"memory", op:"wal checkpoint limit"})`,
  因为"以为设了阈值、其实那家根本没有 WAL"正是不能靠默认值发生的那类事故。
- LSM 的 `enable_mem_wal` / `enable_bg_compact` **传了也不算**,恒被钉回 `false`:事务 WAL 是唯一
  的持久化点,再开 mem.wal 就有两个互不知情的提交日志;而 `set_options` 热开后台 compact 不会
  真的起线程(线程只在 open 时创建),只会让写路径以为有人替它 compact ⇒ L0 无限涨。
- 打开之后还能热改的只有 `vacuum_auto_slice` 与 WAL 阈值(都在 `KvEngine` 上);LSM 其余旋钮的
  热改走具体类型 `crate::lsm_tree::MVCC::set_options`。

各家"一个库"的形状不同 —— `StorageConfig::open()` 替你抹平了这条差异,手写路径时得自己对上:

| 引擎 | 磁盘上是什么 | 实际文件 | `open_engine` 该传 |
|---|---|---|---|
| `bitcask` | 一个**目录** | `data.log`(active) + `data.{id}.log`(密封分片) + `data.hint` + `data.blob` + `bitcask.lock` | 目录 |
| `bplus_tree` | 一个**文件** + 同 stem 的伴生文件 | `x.db` + `x.db.wal` / `.dblwr` / `.freelist` / `.lock` / `.blob` | 文件路径 |
| `lsm_tree` | 一个**目录** | WAL + 各层 SST + `blobs/{seq}.blob` | 目录 |
| `memory` | 无 | — | `None`(给了也忽略) |

`STORAGE_ENGINE` 写了但名字不认识 ⇒ **报错** `EngineError::UnknownEngine`,不静默退回默认值
("以为在跑 bitcask、其实跑的是 memory"不能靠默认值发生)。磁盘引擎漏给路径 ⇒ `PathRequired`。

错误只有一个类型:`EngineError { UnknownEngine(String), Unsupported{engine,op}, PathRequired(&str), Io(io::Error) }`,
`Display` 直接可读。`Unsupported` 有两个来源:`merge()` 仅 bitcask 支持;WAL 阈值读写
(`set_wal_checkpoint_limit` / `wal_checkpoint_limit`)仅三个磁盘引擎有 —— 都返回错误而不是 panic。
内存引擎的 `flush()` / `checkpoint()` 是 **no-op**,不会报错 —— 别把它当"落盘成功"。

### 2. 读写:事务是唯一入口

```rust
use std::time::Duration;

let tx = db.begin();                       // Box<dyn KvTxn + '_>,借用 db
if !tx.set(b"order:1", b"payload".to_vec()) {
    tx.rollback();                         // false = 这一行没进去(冲突,或引擎拒收)
}
let token = b"session-token".to_vec();
tx.set_with_ttl(b"session:abc", token, Duration::from_secs(600));
let _ = tx.get(b"order:1");                // 已剥 TTL 信封;过期/已删/不存在都是 None
tx.commit();                               // 落 WAL + fsync(只读事务两者都不做)
```

四条必须知道的语义:

- `set` / `delete` 返回 `false`、`batch_set` / `batch_delete` 返回 `Err(key)` **都表示"这行没进去"**，
  应该 `rollback()` 整个事务重试，而不是继续写。两种成因要分清：
  ① **写冲突**（别的事务在你之后改了同一个 key）—— 四引擎都会给出；
  ② **引擎拒收**（仅 B+ 树）：单个逻辑键 > **一页载荷 4080 B**（版本键还要 +8 B，所以实际上限
  ≈ 4 KB）⇒ 这一行装不进任何一页。此时**一字未写**，`log::error!` 点名是哪把键，
  WAL 里也不会留下这条 `Write`（判据是 `BPlusTree::row_fits`，在 append WAL **之前**问）。
  值不受这条限制：超过 256 B 的 value 走 blob 旁路，树里只放指针。
- **不用保证收尾**:事务对象丢弃而未 `commit` ⇒ 自动回滚。这条是刻意设计的 —— 一次疏忽若留下
  "漏网活跃事务",`vacuum` 就会从此整轮跳过,垃圾永久累积。
- **只读事务不写 WAL、`commit` 不 fsync**(实测 2265.8µs/次 → 15.8µs/次)。所以"每 tick 做一次
  快照读"这种用法不会把磁盘拖成瓶颈。
- `value` 是打过信封的**逻辑值**:直接存裸字节会踩坑(`0x00` 打头少 1 字节、`0x01` 打头被当 TTL 信封)。
  存二进制/序列化字节请用 **`tx.set_raw(key, value)`** —— 信封由引擎打,`get` 一字不差还回来;
  要自己打也行(`use storage_engines::common::pack_plain;`),两条等价。`set_with_ttl` 内部已打信封。
- **读一段区间就交给区间,别读全库再筛**。`scan(start,end)` / `prefix_scan(prefix)` 把界下推到
  引擎的有序结构(KeyDir seek / BTreeMap range / 下降到 containing 叶子 / SST 稀疏索引定位块),
  所以代价随**窗口宽度**走;release + 20k 行实测(每行 µs,窗口 = 1% 即 200 行):

  | 引擎 | 一次全量 scan | 1% 窗口 scan(改前) | 1% 窗口 scan(改后) |
  |---|---|---|---|
  | bitcask | 3.1 | 376.6 | **3.0** |
  | bplus_tree | 24.7 | 3290.4 | **23.8** |
  | lsm_tree | 5.7 | 405.0 | **4.2** |
  | memory | 2.3 | 106.3 | **1.1** |

  改前那一列不是"慢一点",是**窗口根本没参与决定代价** —— 折完全库再由调用方筛。
  复现:`SCALE_ROWS=20000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 scale_baseline`

### 3. 跨线程

```rust
use std::sync::Arc;
let db = Arc::from(open_engine(EngineKind::Bitcask, Some(&dir))?); // KvEngine: Send + Sync
```

`Arc<dyn KvEngine>` 直接可用:典型形态是"主线程开库 + 心跳线程 `checkpoint()` + 工作线程读写"。
事务只要求 `Send`(`KvTxn: Send`,**不要求 `Sync`**):可以整个搬去别的线程,但两个线程共用一个
事务对象没有语义(内部有 `Cell` 与写集)。

要区分清楚两件事:**门面 `Send + Sync` 让句柄能被多线程持有,不等于并发写**。四个引擎都是
**单写者**模型(进程内 `Mutex` 串行化,进程间靠文件锁 —— 同一目录被两个进程打开,第二个直接
`WouldBlock`)。多线程共用一个句柄得到的是"排队",不是吞吐提升。

### 4. 批量导入(冷启动灌数据)

```rust
let mut bulk = db.begin_bulk();            // 无冲突检测、无 WAL —— 快,但崩溃不安全
bulk.put_batch_owned(rows);                // Vec<(Vec<u8>, Vec<u8>)>
bulk.finish();                             // 必须收尾(Drop 也兜底,但不保证 flush 顺序)
```

导入期间别并发读写。`import_kv` 那批 CLI 走的就是这条路。

### 5. 观测与维护:长跑进程要盯的两个数

```rust
let h = db.health();                       // total_rows / live_keys / max_chain / rows_above_one / active_txns
if h.max_chain > 64 {
    eprintln!("版本链在涨: {:?}", h);      // 事故的唯一先行指标
}
db.set_vacuum_auto_slice(8 * 1024);        // 调切片宽度;**设 0 才是关闭**(默认 16 Ki 行,四引擎同值)
let _ = db.vacuum()?;                      // 全库轮次:唯一会顺手做 merge() / blob 重写的入口
db.set_wal_checkpoint_limit(4 << 20)?;     // WAL 收口阈值(默认 16 MiB;0 = 关闭自动触发)
println!("现在生效的: {}", db.tuning());   // 引擎自报的参数快照,写进去的要读得回来
db.checkpoint()?;                          // 手动收口(不等到阈值)
println!("{}", db.durability_profile());   // Display 只报缺口
```

- `health()` 是 **O(物理行数)**(引擎内部只走元数据/键,不物化 value),按分钟级采样足够,
  **别放进热路径**。
- 正常形态:`max_chain` 贴着 `1 ~ 2×切片宽度`。它单调爬升就说明回收没在干活,先看
  `active_txns` —— 有活跃事务时 `vacuum()` **整轮跳过**(返回 `skipped_active_txn = true`,
  和"扫了但没得裁"是两种结论完全不同的 0)。
- 自动回收默认开着,提交时记账、`begin()` 时取账,触发量 = 切片宽度 ⇒ 回收速率与产生垃圾的速率
  1:1。

**有界回收 `vacuum_bounded(after, budget_rows)` 也在门上了** —— 长跑进程要的是它,不是 `vacuum()`:

```rust
let mut cursor = None;
loop {
    let round = db.vacuum_bounded(cursor.as_deref(), 4 * 1024)?;   // 一轮最多读这么多行版本
    cursor = round.next_cursor;          // None ⇒ 已扫完全库
    if cursor.is_none() { break; }
}
```

- 一轮的代价只看 `budget_rows`,不看库有多大;`next_cursor` 原样传回就接着扫。
  截断只发生在**逻辑 key 边界**(半条链不能收:判"留哪一版"要求整组版本都在手上),所以
  `rows_scanned` 的预算余量 = 一条链的长度。实测 160 行 / 预算 25 ⇒ 四家都是 3 轮、
  单轮最多 40 行;扫完的终态与 `vacuum()` 一致(每 key 剩一行)。
- `budget_rows = u64::MAX` 的那一轮**等价于** `vacuum()`(同一份保留规则,不是两套),
  也只有这一轮做 blob 回收与日志压缩 —— 两者都是全库代价,放进中间轮等于每轮重写一遍库。

**分页搜索:要不要 `total` 是两个代价**(M15)。翻页的 UI 通常只想知道"还有没有下一页",
而数清总数必须把整库折一遍 —— 以前没有开关,首页和末页一样贵:

```rust
use storage_engines::SearchQuery;

// 默认(要精确 total):走完整库,`total`/`total_pages` 是 Some(n)
let page = db.begin().search_keys(&SearchQuery::contains(b"user", 0, 100));
println!("{}/{} 页", page.page + 1, page.total_pages.unwrap());

// 只要窗口:`total` 与 `total_pages` 是 None(没数就不许报一个数),`has_next` 仍然如实
let win = db.begin().search_keys(&SearchQuery::contains(b"user", 0, 100).without_total());
if win.has_next() { /* 还有下一页 */ }
```

实测(2000 键、页宽 20,尺子=本引擎的"走了多少把键/页"),**四家都是 90 倍上下**:

| 引擎 | 首页(免 total) | 首页(要 total) | 倍 |
|---|---|---|---|
| bitcask | 22 把编码键 | 2000 | 91× |
| bplus_tree | 2 次页读 | 62 | 31× |
| lsm_tree | 22 把键 / **22 块** | 2000 把 / 2032 块 | 91×(键)· 92×(块) |
| memory | 22 把键 | 2000 | 91× |

同一件事在 release / 20000 行的台架上的墙钟(页宽 100,首页;同一轮跑的,别跨轮比):

| 引擎 | 要 total | 免 total | 省 |
|---|---|---|---|
| bitcask | 49.7 ms | **0.25 ms** | 202× |
| bplus_tree | 25.9 ms | **0.17 ms** | 150× |
| lsm_tree | 60.1 ms | **0.27 ms** | 222× |
| memory | 17.7 ms | **0.10 ms** | 171× |

**LSM 那一档是 M15b 才降下来的,数字要连着机制看**:改之前首页是 **18.2 ms**(只比"要 total"省
3.5×),因为归并**先把每个源整篇物化再拼** —— 源是 memtable/immutables/L0…L3 的每个 SST,
"取前 100 条"也要把每一篇从第一个块读到最后一个块。现在 `LSMTree::light_iter_range` 返回
`LightMerge`(min-heap 惰性 k 路归并,一次只往前拉一个键,SST 侧 `SstFile::light_iter` 一块一块读、
键越过 `to` 立即收手)⇒ 调用方的 `budget` 一停,块读就跟着停。同一把尺子两头都钉住了:
计数尺 `test_search_window_costs_the_window_not_the_library`(lsm 那条)除了比键数,还比**块读**
—— `window_blocks <= 2 × (window_cost + 1)`,把归并退回"先 `collect` 成 Vec"就红
(实测 22 把键配 53 个块 > 46 ⇒ 红),只量键数抓不到这一条(键数改前改后都是 22)。
剩下躲不开的是"**源数 × 一块**":每个可能相交的 SST 都要读进它在窗口起点那一块才知道有没有货
(min/max 先挡掉不相干的整篇)⇒ 窗口首页的复杂度是 **O(SST 文件数)**,不随行数涨。

三条要说清的边界:
- **深页照旧贵**:跳到第 25 页要把前面 24 页的命中都走过去 —— 没有索引就省不掉这一步,
  `without_total()` 省的是"为 total 而走完剩下的库",不是"任意页都便宜"。
- 窗口内**可能少于 `page_size` 条**:窗口里正好有 TTL 刚过期或被删的行时如此,`has_next`
  仍如实。要严格每页满数就设 `need_total = true`。
- 折叠是"命中几条读几条":不命中 pattern 的键**连值都不取**。四家各有守卫
  `test_search_window_costs_the_window_not_the_library`,门面另有一条
  `windowed_search_agrees_across_engines`(四家必须给同一页的同一份内容)。

**游标式有界扫描 `scan_page(after, limit)`**(M17):`scan(start, None)` 的代价与内存都按
"从 start 到库尾"算,而"下一页"只需要 `limit` 行 —— 翻页/迁移/导出请用这个:

```rust
let mut cursor = None;
let mut pages = 0;
loop {
    let tx = db.begin();
    let (rows, next) = tx.scan_page(cursor.as_deref(), 1000);   // after 是**不含**
    tx.commit();
    pages += 1;
    match next { Some(c) => cursor = Some(c), None => break }
}
```

实测(2000 键、页 50):一页只折 **52 把编码键**(全库 `scan` 要折 2000 把),40 页走完全库,
且逐条顺序与一次 `scan` **完全一致**(`scan_page_costs_the_page_and_walks_the_whole_library_once`)。
游标推进用的是折出来的行的键(**含刚过期不可见的那些**) —— 否则下一次又从同一批过期行撞起,走不动。

### 6. 选型:别只看速度

```rust
use storage_engines::{EngineError, EngineKind};

// 从配置字符串解析:`EngineKind` 是同一套别名规则,不认识就 Err(不退回默认值)
let kind = EngineKind::parse("lsm").ok_or_else(|| EngineError::UnknownEngine("lsm".into()))?;
if !kind.is_production_ok() {
    // 只有 memory 是 false:它没有任何持久化(无事务 WAL、无校验和),是对照实验,
    // 不是"更快的选项"。上实盘之前就把它挡住,别等掉电。
    return Err(EngineError::Unsupported { engine: kind.as_str(), op: "production use" });
}
let profile = db.durability_profile();     // 四家崩溃安全不等价,差异做成数据吐出来
println!("{profile}");                     // 只报缺口:少了 WAL / 校验和 / 双写缓冲
```

写密集 → `lsm_tree`(顺序写最快);范围查询密集 → `bplus_tree`(叶子链表);读密集且 key 数可控
→ `bitcask`(点查恒定 1 次 seek,但全量 keydir 常驻内存,key 数受内存限制);测试/对照 → `memory`。
引擎之间怎么换、数据搬不搬得动,看下面第 7 条与[命令行工具](#命令行工具数据搬运与压测)。形态差异详见[四个引擎对比](#四个引擎对比)。

### 7. 目前还不能指望库做的事(接入前必看)

- **换引擎不自动搬数据**:`open_engine` 只是换了一家,原来那家的数据不会跟过来。要搬有两条路:
  CLI([`export_kv` → `import_kv`](#命令行工具数据搬运与压测))或库 API
  `storage_engines::migrate(&from, &to, page_rows)`(M17) —— 后者按页走
  [`KvTxn::scan_page`](#5-观测与维护长跑进程要盯的两个数),**每页代价只看 `page_rows`**,
  不像 `export_latest_visible()` 那样整库物化。带 TTL 的行按剩余时间续,值走 `set_raw`
  (不会踩信封约定),历史版本与墓碑不带(目标是干净库)。
- **旋钮分两档**:open 时设的(`order` / `max_cache_pages` / `bitcask_max_file_bytes` / LSM 整份
  options)改一次要重开库 —— 前两者动的是**已落盘数据的形状**,不是能热调的运行时参数。
  开着库能调的只有切片宽度与 WAL 阈值(在 `KvEngine` 上),LSM 其余旋钮另走具体类型
  `crate::lsm_tree::MVCC::set_options`。
- **`tuning()` 列的是代价相关的旋钮,不是全部字段**:LSM 的 `LSMTreeOptions` 有 21 个字段,
  快照只列其中影响读写代价的 9 个;`enable_mem_wal` / `enable_bg_compact` **有意不列**
  (列了会被读成"能调",而它们恒为 `false`)。要看全量读 `crate::lsm_tree::MVCC::options()`。
- **门面不暴露列族**:多列族 `LsmDB` 与 `CF` 只在各引擎模块里,`KvEngine` 一层没有。
  需要 CF 隔离的下游得直接用 `crate::lsm_tree::LsmDB`。
- **值信封要业务侧自己守**:`set` 把 `value` 当**已打信封**的逻辑值存,`get` 一律剥标签,
  所以裸字节直接存会踩坑(`0x00` 打头少 1 字节,`0x01` 打头被当 TTL 信封)。约定是
  用 `tx.set_raw(key, value)`(或自己 `use storage_engines::common::pack_plain;` 先打再 `set`,两条等价)
  —— 引擎打的信封不会错,`set` 那个口子保留是因为现有调用点按约定写。
  —— 见[已知局限](#已知局限)。

---

## 项目结构

```
src/
├── engine.rs          统一门面:KvEngine / KvTxn / KvBulk 三个对象安全 trait
├── common/            四引擎共享:字节级文件工具(disk)、版本键与 vacuum 判定(version)、
│                      事务内核共用件(kernel)、WAL(wal)、TTL/快照编解码
├── main.rs            使用示例
│
├── lsm_tree/          LSM 树:MemTable + 分层 SST + leveled compaction
├── bplus_tree/        B+ 树:4KB 页 + 双写缓冲 + 页缓存
├── bitcask/           Bitcask:append-only 日志 + 全内存 KeyDir
└── memory/            纯内存:BTreeMap,无持久化
    └── bin/           命令行工具(import_kv / export_kv / bench)

tests/                 四个引擎的集成测试(会在 ./data/ 下建演示库)
```

每个引擎目录下的文件职责一致:

| 文件 | 职责 |
|---|---|
| `<引擎名>.rs` | 存储结构本体 |
| `<引擎名>_mvcc.rs` | MVCC 事务层(四引擎同构) |
| `kv_ops.rs` | 批量 / 扫描 / 搜索分页 / 元数据 / TTL |
| `wal.rs` `storage.rs` | 预写日志、文件锁、大 value 外置 |
| `bin/` | 命令行工具:`import_kv` / `export_kv` / `bench` |

快照编解码不在引擎目录里,四引擎共用 `src/common/kv_snapshot.rs` 一份。

---

## 分层架构

```
              应用代码
                 ↓
  ┌──────────────────────────────┐
  │  engine.rs  统一门面          │   按运行时字符串选引擎
  │  KvEngine / KvTxn / KvBulk    │   Box<dyn> 对象安全
  └──────────────────────────────┘
                 ↓
  ┌──────────────────────────────┐
  │  MVCC 事务层(四引擎同构)      │   快照隔离 / 写冲突 / vacuum
  │  版本键 = raw_key ‖ version   │   WAL: REDO 已提交 + UNDO 未提交
  └──────────────────────────────┘
                 ↓
  ┌──────────────────────────────┐
  │  common/  TTL 信封编解码      │   [tag][expire?][用户字节]
  └──────────────────────────────┘
                 ↓
  ┌────────┬────────┬────────┬────────┐
  │  LSM   │ B+Tree │Bitcask │ Memory │   四种存储结构
  └────────┴────────┴────────┴────────┘
```

关键在于**中间两层完全共用**:MVCC 事务语义和 TTL 编码只写了一遍,四个引擎只在最底层的存储结构上有差异。所以门面层的 `impl_engine!` 宏能用一套代码生成四个适配器,差异只用几个开关吸收。

---

## MVCC:四引擎共享的事务语义

### 版本键编码

```
enc_key = raw_key ‖ version.to_be_bytes()
```

**大端是必须的** —— 保证同一个 key 的多个版本在字节序索引里按版本号升序相邻。用小端的话 `version=10` 会排在 `version=2` 前面。

### 快照隔离

事务 `begin` 时做两件事,顺序很关键:

1. 先拍下当前所有活跃事务的 xid 快照(`active_xid`)
2. 再把自己登记进活跃表

先拍后登记 → 自己的 xid 不在自己的快照里 → **自己的写立刻对自己可见**。

可见性判据就两行:

```rust
fn is_visible(&self, version: u64) -> bool {
    if self.active_xid.contains(&version) { return false; }  // 并发未提交事务的写
    version <= self.version                                  // 未来事务的写
}
```

### 写冲突

`set` / `delete` 返回 `bool`,`false` 就是冲突,调用方应当 `rollback()`。两类冲突都会立即失败(乐观策略,不等锁):

1. 该 key 的最新版本属于**与我并发**的事务 —— first-writer-wins
2. 该 key 的最新版本**比我新**

第 2 类值得注意:**即使对方已经 commit 了,我仍然写失败**,因为对方的 version 在我的快照里。

回滚不需要 undo log —— 每个 `(key, version)` 只有一行,按 `active_txn` 记录的 key 列表直接物理删除即可。

### vacuum 版本回收

回收水位 `xmin`:**只要还有活跃事务就整轮跳过**(`VacuumStats::skipped_active_txn = true`),没有活跃事务时取 `next_version`。保留策略:

> 每个 key 只保留「**最后一个** `version < xmin`」+ 全部 `version >= xmin`;
> 若 xmin 之前最终是 tombstone 且无更新版本 → 整个 key 删除。

为什么必须留下那个 `last_old` 而不能删空:任何 `version >= xmin` 的事务读这个 key 时,可能正好落在这一版上——它是「最老仍可能被看到的版本」。

最后一条规则是墓碑的最终回收,否则 tombstone 会永久累积。

为什么是「整轮跳过」而不是按 `min(活跃事务 id)` 精确算水位:精确算法一旦漏算任何一个活跃事务,裁掉的正是那个事务快照还需要的版本 ⇒ 它**静默读到旧值**——不报错、不 panic,只在下游对不上账时才发现。保守跳过最多让回收推迟一轮,而错误的水位线是不可逆的数据错误。两害相权,宁可慢一轮。

⚠️ 挂着的活跃事务会让回收永久停摆,所以 `Transaction` 实现了 `Drop`:忘了 commit/rollback 的事务在离开作用域时自动回滚并退出活跃表,不再钉住 `xmin`。

### 自动回收(默认开着)

上面那套规则如果只能靠人喊 `vacuum()`,就等于什么都没修 —— 事故的本质正是「没人喊,于是垃圾永久累积」。所以四引擎都自带回收:

- 提交时按事务写集给 `vac_pending` 记一笔账(近似:算逻辑 key 数)
- `begin_transaction()` 检查这笔账:攒够一个切片宽度,就当场裁**一片**同样宽度的行
- 切片宽度默认 **16 Ki 行**,四引擎同一个值;`set_vacuum_auto_slice(0)` 关闭

触发量 = 切片宽度 ⇒ 回收速率与产生垃圾的速率 1:1,这是「跟得上」的下界。挂在 begin 而不是 commit:那一处还没有本线程的活跃事务,不会被自己的「有活跃事务就跳过」踩中。

一片的代价是读 16 Ki 行元数据(不物化 value),单核机器上亚毫秒级,锁也只按这么久。**有界轮次不做全库收尾**(Bitcask 的 blob 重写与 `merge()`、B+ 树的 blob 清空、LSM 的 `set_mvcc_xmin_filter`)——判据是「这次调用是不是全库扫描」(`budget_rows == u64::MAX`),而不是「游标有没有回到库尾」:一把 key 的小库任何一片都正好读到库尾,用游标判断会让自动回收每轮都重写一遍库。

### 有界回收 `vacuum_bounded(after, budget_rows)`

四个引擎的 `MVCC` 与门面 `KvEngine` **同名同参数**(`db.vacuum_bounded(...)` 返回
`Result<VacuumRound>`),所以"按小片推"这件事不需要知道底下是哪一家:

```rust
let mut cursor = None;
loop {
    let round = mvcc.vacuum_bounded(cursor.as_deref(), 100_000)?;
    // round.stats.rows_scanned / versions_removed / round.next_cursor
    match round.next_cursor { Some(c) => cursor = Some(c), None => break }
}
```

`vacuum()` 就是 `budget_rows = u64::MAX` 的一轮,两者共用同一份保留规则 —— 这条"一套规则、
两个入口"由门面上的 `vacuum_bounded_is_bounded_and_ends_where_vacuum_does` 钉住:160 行 /
预算 25 时四家都是 3 轮、单轮最多 40 行,接力扫完之后的终态(每 key 剩一行)与 `vacuum()` 一致。
截断只发生在**逻辑 key 边界**:半条链不能收,因为「留哪一版」的判据要求整组版本都在手上 —— 所以一片实际读的行数可能超出预算,超出量 = 那一条链的长度。这是本设计明确接受的代价(也是游标前进的下界:每轮至少前进一组)。

### health 观测

`MVCC::health()`(四引擎同名同口径,门面层 `KvEngine::health()` 直接转发)返回 `MvccHealth`:

| 字段 | 含义 |
|------|------|
| `total_rows` | 物理行数(含历史版本与墓碑) |
| `live_keys` | 逻辑 key 个数 |
| `max_chain` | 单个逻辑 key 的最长版本链 |
| `rows_above_one` | `total_rows - live_keys`,即「超出每 key 一行」的多余行数 |
| `active_txns` | 当前活跃事务数,> 0 时 vacuum 会整轮跳过 |

`max_chain` 就是那次 CPU 事故唯一的先行指标:某把热点 key 堆到 23.8 万个版本时,点读要点走 23.8 万行。此前这个数只能靠人工数日志得到,现在库自己报。

实现只走元数据/键视图,不物化任何 value(每个引擎都有对应的守卫测试),但代价仍是 **O(物理行数)**——属管理面调用:按分钟级采样足够,别放进热路径。

### 哪些只剩一份,哪些必须四份

这次事故的结构性根因是"同一套语义写了四遍"。收敛的边界按**是否涉及存储布局**划:

| 已收成一份(`src/common/`) | 必须每引擎一份 |
|---|---|
| `version.rs`:版本键编解码 + vacuum「留哪一版」判定 | 遍历原语:`iter_meta_from` / `scan_entries_break_from` / `scan_latest_light_window` / `scan_entries_from` —— KeyDir、叶子链、SST 分层、BTreeMap 根本不是一种东西 |
| `kernel.rs`:可见性判据 `visible_at`、`HealthFold`、`VacuumScheduler`(含"取账不能 swap")、`is_unfinished`、**`fold_window`**(窗口折叠驱动)、**`search_page_from`**(分页核) |
| **`store.rs`(M18a):`VersionedStore` trait + `collect_rows` + `search_window`** —— "按 enc 升序折出每把 key 的胜出行、界内过滤、取值、剥壳、窗口化"从此只有一份。**M20 又添两句**:`seek_prev_with`(反向逐步的循环 + 按组退界)与 `live_record_of_logical`(单键"活着吗"的判定,与 `scan` 同一份口径) | 有界遍历的**早停与起点**:谁能反向走、谁只有单向 `next_leaf`、谁的窗口要按源封顶。**反向一步**同一家一个样子:mem `range_back` / bitcask `range_meta_back`(只读 KeyDir,不碰 value)/ bplus `find_back`(跨叶有进度守卫)/ lsm `find_back`(逐源取最大) |
| `types.rs`:`MvccHealth` / `VacuumStats` / `VacuumRound` / `DurabilityProfile` | 各自的守卫测试:同一条通道在不同引擎的**量尺不同**(读盘次数 / value 解码次数 / 物化次数 / 克隆次数),拿一把尺量不到另外三家 |
| **`txn.rs`(M18b~M18e):`TxnCore` + `WriteSide` + `WalTail`** —— begin 的版本号与活跃快照登记、**整条写序列**(冲突判定 → 值编码 → 页容量判定 → WAL Begin → WAL Write → 落行 → 写集登记)、`commit` 销账 + 回收记账、`rollback` 按写集撤销、批量写的**部分撤销**、bulk 会话的版本与收尾幂等 | 写路径上"这一行的字节去哪、WAL 记录怎么排、大值走不走旁路、装不装得下一页" —— 那些是 `WriteSide` 的槽(有 WAL / blob / 页容量的人家只实现自己那几条,其余走默认实现 = 不做也不失败) |
| `VersionedStore` 的**两个方法**:`scan_candidates`(我这家的键怎么折)与 `materialize`(一组的字节在哪儿 —— KeyDir 在日志另一处、B+ 树在叶子里、LSM 要跨源点查、内存就在同一个 map)。**三态** `RowValue{Value,Tombstone,Missing}` 是刻意的:折叠侧必须分得开"读不到"与"墓碑", 合成 `Option` 就会把 lsm 那行"视图里有、值里没"的跳过语义改掉 |

判据、记账、折叠这些纯逻辑一旦复制,就会长成"一个 bug 四个实例"—— 自动回收那回我把 `swap`
当成取账写进了四份代码,四家一起静默停摆。所以合的是**判定与折叠的算法**,不是扫描本身:
M18a 之后四家各自剩的只有"键怎么折 + 字节在哪"这两件事(每处 ~25 行,合计比原来少 80 行),
**新增一个引擎只要实现 `VersionedStore` 那两个方法**,窗口/分页/界下推不用再抄第四遍。

事务内核那条(M18b~M18e)同一判据,但**按引擎切里程碑搬**:读侧折叠一次就四家统一了(它只有两个方法),
写侧要动的资源三家各不相同(纯内存 / WAL+blob / WAL+blob+页容量判定),所以内核把**整条写序列**收成一处、
把"这一步这一家要不要做"做成槽,四家(M18b~M18e)现在都跑在同一份内核上。搬每一家都当场证明它没改
语义 —— 靠的是 `txn_kernel_agrees_across_engines`:同一段事务脚本(抢同一把 key、
回滚自己写过的行、批量写撞墙后的部分撤销、未收尾即丢弃)在四家跑出**同一个终态**,四家原有测试
只测自家那一份副本,跨引擎对拍才看得见"内核写歪了"。另外三条用变异测试钉住序列里最贵的三处:
`commit` 无视"进过 WAL 才收尾" ⇒ 红在 `test_read_only_transaction_touches_no_wal`;把 WAL Write 挪到
改库之后 ⇒ 红在 `test_disk_persist_across_reopen` + `test_export_latest_visible`。

⚠️ 变异测试还撞出一条**一直没被量过**的洞(M18d):`test_read_only_transaction_touches_no_wal` 原先用
**WAL 字节数**判"写入有没有进 WAL",而只落一条 `Commit` 也能让字节涨 —— 把 `wal_write` 改成"不 append、
直接返回成功"之后三家**全绿**。现在三家都按**记录类型**数 `WalRecord::Write`,同一处变异打上去逐家实测会红。
这和"计数尺子必须把命中/未命中分开数"是同一类错:**字节数看不出形状**。

### 长跑基线（事故形状，实测）

一条 `#[ignore]` 的守卫把现场形状跑成可复现的对照：一把热点 key 持续更新 2 万次 + 每 4 次一次点读 + 200 把稳定 key。

```bash
cargo test --release --lib bitcask::bitcask_mvcc::tests::test_long_run_baseline -- --ignored --nocapture
```

| 自动回收 | `max_chain` | `total_rows` | 点读均耗时（前 250 次 / 后 250 次） |
|---|---|---|---|
| 开（片宽 2048 行） | **3 817** | **4 017** | 45 µs / 39 µs |
| 关 | 20 000 | 20 200 | 45 µs / 72 µs |

另一次同批量的实测把读路径本身也量了出来（`只读事务 = begin + get + commit`）：

| | 每次耗时 | 每次落 WAL |
|---|---|---|
| 改之前 | 2265.8 µs | 56 字节 |
| 改之后（`Begin` 推迟到第一次写；只读 commit 不 fsync） | **15.8 µs** | **0 字节** |

上表里那 ~1.1 ms 就是这件事：读事务原来**每次**都 append `Begin` + `Commit` 并 fsync 一次
（`Wal::append` 是直接 `file.append`，不是缓冲）。也就是说混合负载里的点读耗时 **97% 是 WAL fsync**，
跟查找无关 —— 这条改完点读从 ~1130 µs 掉到 ~45 µs。

三个结论，顺序即重要性:

1. **链长有界，但不是"贴着 1"**：稳态上界实测是 **2 × 切片宽度**。一片的截断只能落在逻辑 key
   边界，热点 key 那一组太大时会把整片吃满 ⇒ 它每两小轮才被轮到一次。默认片 16 Ki 行时
   稳态链 ≲ 32 Ki —— 现场那条 23.8 万版是它的 7 倍多，量级差在这里，但别把界说成 1。
2. **点读耗时与链长无关,两条通道是独立的**：关掉回收那列链涨到 2 万行,点读耗时**没有变差**
   （前/后 250 次几乎一样）。所以"修了反向早停"与"修了回收"各断各的通道 —— 只修一条,
   另一条照样恶化;反过来,回收偶尔落后也不会再把 CPU 顶上去。
3. **点读耗时主要是 WAL fsync,不是查找**：读事务当时同样 append `Begin`/`Commit` 并 fsync。
   这条已在同批改掉（见上表"改之前/改之后"），教训留下：**耗时数字要先归因再动手**，
   否则很容易拿"点读 1 ms"去怀疑本来已经修好的查找路径。

### 大规模复测（200k 行，release，同一份构建对照跑 20k 与 200k）

判据是**每行/每次成本的比值**：10× 数据量 ⇒ 线性就是 10×，比值 >10 才叫"随规模恶化"。
别拿 `µs/行` 一个数下结论 —— 20k 时看着正常的数字，10× 之后可能已经是 O(N²)。

| 指标 | 20k | 200k | 每行成本比 | 结论 |
|---|---|---|---|---|
| bitcask 灌行 | 14.7 | 15.3 µs/行 | 1.04× | 平 —— 写侧的参照系 |
| **lsm 灌行（M19 之前）** | 38.8 | **227.8** | **5.87×** | O(N²)：每插一行都为"要不要 flush"重数整个 memtable |
| **lsm 灌行（M19 之后）** | **14.5** | **21.0** | **1.45×** | 基本持平；残差是 flush/compaction 与 WAL checkpoint |
| bitcask 首页(免 total) | 306.8 | 290.5 µs/次 | 0.95× | 平 —— 窗口分页在大库上站住了 |
| bplus 首页(免 total) | 133.7 | 146.4 µs/次 | 1.10× | 平 |
| lsm 首页(免 total) | 266.9 | 385.0 µs/次 | 1.44× | O(SST 文件数)，M15b 之后不再随行数涨 |
| bplus `health()` | 13.2 | 272.7 ms | **2.06×** | ⚠️ 未修：200k 行约 12000 页 > `max_cache_pages` 1024 ⇒ 走查每行重读 |
| bplus 随机点读 / multi_get | 11.6 / 10.5 | 39.8 / 39.6 µs | **3.4× / 3.8×** | ⚠️ 同上；**有序**只 1.3× ⇒ 差的是覆盖率，不是复杂度 |
| lsm 随机点读 / multi_get | 9.6 / 9.5 | 50.0 / 41.6 µs | **5.2× / 4.4×** | ⚠️ 版本散在更多 SST（M10 那条通道） |
| memory 全项 | — | — | ≈1.0–1.4× | 平 |

另外量出来的一条：**200k 行做 `一次全量 scan` 的瞬时峰值 +48 MB**（返回的是物化 `Vec`，
常驻 30.7 MB ≈ 结果集本身）—— 翻页请走 `KvTxn::scan_page`，它每页只折 `limit + 1` 组。

### 补测的操作（台架补到 31 项/引擎，20k 与 200k 同构建对照）

新加进去的是**之前没量过的功能**，不是重复旧项：`multi_get` 全未命中的单价、`scan_page` 翻完整库、
`seek_prev`、`reverse_scan`、`set_with_ttl` + `get_ttl`、`vacuum_bounded` 一片、`export` 全库物化、
`migrate` → memory。**两条新的超线性就是这一轮抓到的**（M21 修一条、M20 修一条，都还留着各自的表行）：

| 操作（单位写全） | 20k | 200k | 比值 | 判读 |
|---|---|---|---|---|
| ⚠️→✅ `seek_prev` 单次（µs/次） bitcask | 52 192 → **20.6** | 382 234 → **39.8** | 改前 7.3× → **改后 1.9×** | **已修(M20)**：改前一次调用只为返回一行，却把整段前缀物化进 `Vec` ⇒ O(库)；200k 便宜 **9 600 倍** |
| ⚠️→✅ `seek_prev`（µs/次） bplus / lsm / memory | 32 444 / 46 961 / 31 583 → **16.5 / 24.9 / 10.4** | 328 583 / 610 488 / 139 960 → **98.3 / 93.6 / 16.7** | 改后 6.0× / 3.8× / 1.6×（改前 10.1× / 13× / 4.4×） | ✅ 同一份循环（便宜 3 300~9 600 倍）。⚠️ 剩下的 1.6~6× 是**各家的老账**（bplus 页覆盖 / lsm 版本散在更多 SST），不是整段物化；bplus 因此仍是四家里最贵的一次调用 |
| `scan_page` 每页（µs/页，页宽 500） bplus | 12 480 | 189 151 → **1 361**（M21 后） | 改前 15.2× → **改后 0.11×** | ✅ **已修(M21)**：游标下推成迭代起点 |
| `scan_page` 每页（µs/页） lsm / bitcask / memory | 9 685 / 4 544 / 4 489 | 132 601→**1 522** / 18 821→**1 495** / 16 817→**620** | 改后 0.16× / 0.33× / 0.14× | ✅ 已修；改前 bplus 翻完整库 400 页 × 189 ms ≈ **75.7 s**，改后 **0.54 s**（同库一次全量 `scan` 是 0.58 s ⇒ 翻页现在≈扫一遍） |
| `migrate` → memory（µs/行） bplus | 36.2 | 216.2 → **16.3** | 改后 0.45× | ✅ 同一个根，一起修好了（13 倍） |
| `migrate`（µs/行） bitcask / lsm / memory | 15.1 / 61.2 / 20.5 | 27.2→**10.6** / 167.1→**13.6** / 27.5→**8.0** | 改后 0.70× / 0.22× / 0.39× | ✅ 同上 |
| `multi_get` **全未命中**（µs/键） bitcask / lsm | 0.7 / 0.8 | 0.9 / 0.5 | ≈1× | **命中与未命中差 10~100 倍**：未命中几乎免费，命中的钱在"定位并读出值" |
| `multi_get` 命中（µs/键） bplus / lsm | 24.9 / 16.8 | 65.1 / 58.4 | 2.6× / 3.5× | 覆盖率与 SST 分散度（与点读同源） |
| `vacuum_bounded` 一片 2048 行（µs/次） 四家 | 2 809~8 603 | 3 536~9 299 | ≈1× | **平 —— 有界回收这条是干净的**（长跑该用它的依据） |
| `set_with_ttl` / `get_ttl`（µs/次） 四家 | 6.3~107 / 2.0~11.4 | 17.0~94.5 / 2.0~13.3 | ≈1× | 平；TTL 往返 1000/1000 读得回（断言就在台架里） |
| `export` 全库（µs/行） 四家 | 2.5~7.0 | 3.1~5.3 | ≈1× | 单价平，但**常驻 33 MB @200k**（物化） ⇒ 大库别用它 |

两条 ⚠️ 的形状不是猜的，是读代码读出来的：

- ⚠️→✅ `seek_prev` 四家原先都是同一份 `scan(None, Some(key‖0x00))` → `retain(≤ key)` → `pop()`；
  **已修(M20)**：循环收成 `common::store.rs` 的 `seek_prev_with` 一份，每家只交两个口子 —— 反向一步
  （mem `range_back` / bitcask `range_meta_back` / bplus `find_back` / lsm `find_back`）+ 单键可见行
  （复用现成的 `latest_visible_raw`，判定走 `live_record_of_logical`，与 `scan` 同一份口径）。
  被拒的那把 raw 直接当下一轮上界 ⇒ 它的整组版本行自动排在界外，**一步跨一整把键**。
- `scan_candidates` 的 `after` 只在循环里 `continue` 过滤，迭代起点仍用 `start` ⇒ 游标退化成过滤器。
  于是"顺序翻页"不是 O(库) 而是 **O(库²/页宽)**；`migrate` 与 `search` 的续跑共用这条路径。

复现与归因（都是 `#[ignore]`，必须 `--test-threads=1`，内存计数是进程级的）：

```bash
SCALE_ROWS=200000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 scale_baseline
# M24 的大事务台架：一个事务塞 20000 行，报头 200 行与尾 200 行的单价（µs/行）
BIG_TXN_ROWS=20000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 big_transaction_write_cost
# 写侧归因台架：报块读 / SST 数 / memtable 字节 / begin·set·commit 三段耗时 / 裸 insert 单价
LSM_FILL_ROWS=200000 LSM_FILL_CHUNK=20000 cargo test --release --lib -- --ignored --nocapture \
    --test-threads=1 lsm_fill_attribution
```

### WAL 与崩溃恢复

WAL 有 5 种记录:`Begin` / `Write` / `Commit` / `Abort` / `Checkpoint`,帧格式带 CRC32,损坏或不完整的尾部会被直接丢弃(模拟崩溃截断)。

提交协议是简化版 ARIES —— `append` **不 fsync**,只有 commit / rollback 才 sync。所以「Commit 记录落盘 = 事务持久」,数据页可以慢慢刷:

```
写:       append WAL Write → 改内存结构
commit:   移出活跃表 → append Commit → fsync WAL   ← 持久化点
rollback: append Abort → fsync → 物理删本事务版本行
```

恢复时 `analyze_recovery` 单趟扫描分出两个集合:有 `Commit` 的 **REDO**,有 `Abort` 的和崩溃时进行中的 **UNDO**(无提交记录即视为回滚)。REDO 靠版本键天然幂等(同键覆盖),重放多次结果一致。

WAL 不需要手动收口:增长到阈值(默认 16 MiB,`set_wal_checkpoint_limit` 可改,设 0 关闭)就在**下一次提交后**自动 checkpoint——写回一条 `Checkpoint` 记录并截断文件。`memory` 引擎无 WAL。阈值触发和手动 `checkpoint()` 走同一份实现,顺序都是「先落数据/页,最后才截 WAL」:WAL 一旦被截,未落盘的行就再无重放来源。

---

## 四个引擎对比

| 维度 | LSM Tree | B+ Tree | Bitcask | Memory |
|---|---|---|---|---|
| **核心结构** | MemTable + 分层 SST | 4KB 页 + 页缓存 | append-only 日志 | `BTreeMap` |
| **内存索引** | 双层 BTreeMap(多版本) | LRU 页缓存 | **全量 key 常驻** | 全部在内存 |
| **写入** | 顺序写,最快 | 页内随机写 | 顺序追加 | — |
| **点查** | 逐层查找 | O(log n) 页 IO | **恒定 1 次 seek** | O(log n) |
| **范围扫描** | 多路归并 | **叶子链表,最优** | KeyDir 有序但 value 随机 IO | 原生有序 |
| **Compaction** | leveled,可后台 | 页合并 / 借键 | 全量重写,**须手动触发** | 无 |
| **崩溃恢复** | mem.wal 重放 | **双写 + WAL + 文件锁** | 扫日志 + hint 加速 | **无** |
| **大 value 阈值** | 4096 B(可配) | 256 B | 256 B | 不外置 |
| **适合场景** | 写密集 | 范围查询密集 | 读密集 + key 数可控 | 测试 / 缓存 |

### LSM Tree

MemTable 是**双层 `BTreeMap`**(`user_key → seq → value`),不是常见的 SkipList —— 内层按 seq 保存多版本,这是 Snapshot 读的基础。

- **Flush**:内存超 4MB **或** 6.5 万条 → 冻结写成一个 L0 SST
- **分层**:leveled compaction,L0..L3。L1 基准 2MB,每层 ×10(L2=20MB, L3=200MB)
- **触发**:L0 文件数 ≥ 4,或某层字节数超限
- **限流**:L0 ≥ 8 软限流(顺带同步 compact),L0 ≥ 20 硬停写 —— RocksDB 风格
- **SST 格式**:data block(每 64 条一块) + 稀疏索引 + Bloom + footer,写 `.tmp` 再 rename 保证原子
- **Bloom**:默认 10 bits/key → 7 个哈希函数,double hashing,每个 SST 一个
- **Snapshot**:带引用计数 pin,阻止 compact 回收更旧版本
- 还有多列族(`LsmDB`)、block cache(LRU,默认 64 块)、可选 mmap(默认关)

三套预设:`Default` / `for_mvcc()`(关 mem_wal,mem 放大到 32MB)/ `for_bulk_load()`(mem 64MB,更少 compaction)。

### B+ Tree

自研磁盘 B+ 树,**4096 字节页**,page 0 是 meta 页。

- **分裂判定是双重的**:条目数超 order **或** bincode 序列化后超 4080 字节。所以 order 是软上限,变长数据下真正的约束往往是字节数
- **叶子有 `next_leaf` 单向链表** → 范围扫描顺着链走,不用回溯树,这是 B+ 树相对 LSM 的最大优势
- **下溢先借后并**:优先右兄弟;兄弟够富裕就借一条,否则合并,并递归向上传播
- **空闲页**是 FIFO 队列 + 哈希集合,持久化在独立的 `.freelist` sidecar(meta 内嵌方案上限只有 506 个)
- **页缓存**是近似 LRU:队列只 push_back 不去重,超过容量 4 倍才压缩,换出时批量 flush

**三道崩溃防线**是这个引擎最讲究的地方:

1. **页级 —— 双写缓冲防 torn page**。写页前先写 `.dblwr`,三阶段 fsync 协议:先把 count 写 0 并 sync(半写入不会被误判有效)→ 写页槽位并 sync → 最后写真实 count 并 sync。**count 字段就是提交标志**。重开时主文件 CRC 坏了就从 dblwr 捞回来
2. **事务级 —— WAL**(REDO/UNDO,见上文)
3. **并发级 —— 文件锁**,单写者,先抢锁再恢复

冷启动还有优化:优先读 meta 里的 `next_version`(靠 `NVER` 魔数标记),其次用 WAL 的 checkpoint,**只有旧库才退化到全树扫描**。

### Bitcask

经典 Bitcask:所有写都是顺序追加,内存里维护 key → 文件位置的索引。

- **KeyDir 用 `BTreeMap` 而非 HashMap** —— 刻意的选择,为了支持范围扫描(经典 Bitcask 用 HashMap 是不支持的)
- 索引项两态:`Live { file_id, pos, len }` 和 `SoftTombstone`(MVCC 的版本行语义需要保留墓碑)
- **数据文件 rotate**:单文件超 64MB 就密封成 `data.{N}.log`,新建 active。写缓冲 4MB
- **hint 文件**是 KeyDir 的磁盘快照,让 open 免于全量扫描 —— 有 hint 时只需增量扫 active 文件尾部。hint 损坏会自动降级到全量扫描
- **merge 是全量重写式**:把所有存活记录搬到新日志,坍缩成单个文件。**没有自动触发**,必须显式调用,或由 `vacuum` 在有回收时顺带触发

三级删除标记(`val_flag` 为负数):`-1` 硬删除(从 KeyDir 移除),`-2` 软墓碑(保留,value 视为 None)。

### Memory

整个存储层只有 105 行,底层就是一个 `BTreeMap<Vec<u8>, Option<Vec<u8>>>`。

没有 WAL、没有崩溃恢复、没有 BlobStore、没有文件锁。它的价值在于:**把 MVCC 事务层剥离出来单独看**——事务语义与持久化机制是正交的,这个引擎证明了这一点。也适合做单元测试的快速后端。

---

## TTL 与大 value

### TTL 信封

TTL 不是单独的元数据表,而是**编码进 value 本身**:

```
永久:   [0x00] 用户字节...
带 TTL: [0x01] expire_unix_secs(u64 LE) 用户字节...
```

存的是**绝对 Unix 秒时间戳**,不是相对存活时长。解码时首字节不认识就整段当作裸 value —— 兼容旧数据,截断的 TTL 记录会降级成 Plain 而不是 panic。

读路径统一出口 `logical_to_user`:过期的行对用户等价于「不存在」,但底层数据仍在,留给 vacuum 回收。

⚠️ TTL 最小粒度是 **1 秒**,传 0 会被当成 1 秒。

`get_ttl` 返回**三态**:

```rust
None              // key 不存在或已过期
Some(None)        // 永久
Some(Some(dur))   // 还剩 dur
```

### 大 value 外置

超过阈值的 value 不进主存储,而是写到旁路的 `.blob` 文件,主存储只留一个定长指针:

```
内联:   [0x00] payload...
blob:   [0x01] offset(u64 LE) len(u32 LE)   —— 共 13 字节
```

阈值 B+ 树和 Bitcask 都是 **256 字节**(编译期常量),LSM 是 **4096 字节**(可配置)。blob 记录自带 CRC32 校验。

所以磁盘上一个小 value 其实是**双层信封**:`[存储 tag][TTL tag][用户字节]`。

---

## 门面层设计

`src/engine.rs` 提供三个 **对象安全** 的 trait,所以能 `Box<dyn KvEngine>` 放进容器统一驱动:

| trait | 用途 |
|---|---|
| `KvEngine` | `begin` / `begin_bulk` / `vacuum` / `vacuum_bounded` / `health` / `set_vacuum_auto_slice` / `set_wal_checkpoint_limit` / `wal_checkpoint_limit` / `tuning` / `durability_profile` / `flush` / `checkpoint` / `merge` |
| `KvTxn` | 读写 / 批量 / 扫描 / 搜索分页 / TTL / `commit` / `rollback` |
| `KvBulk` | 批量导入,无冲突检测、不写 WAL |

两条**必须**遵守的上界(都是被下游真实需求逼出来的,不是设计洁癖):

- `KvEngine: Send + Sync`、`KvTxn: Send`(不要求 `Sync`)。少了前者,`Box<dyn KvEngine>` 进不了 `Arc`、也搬不到别的线程,而交易进程的常态就是"开库线程 + 心跳线程 checkpoint + 工作线程读写"。事务刻意不要求 `Sync`:内部有 `Cell`(WAL 是否已开)与写集,跨线程共享一个事务对象本身没有语义。
- 选型是类型: [`EngineKind`](`parse`/`as_str`/`needs_disk`/`is_production_ok`) + [`StorageConfig`](`STORAGE_ENGINE` / `STORAGE_DATA_DIR`,落盘 `data_dir/<engine>/`),入口 [`open_from_env()`]。**`STORAGE_ENGINE` 写了但不认识时报错**,不静默退回默认值 —— "以为在跑 bitcask、其实跑的是 memory"不能靠默认值发生。入口 [`open_engine`] 的引擎名走 [`EngineName`] trait:枚举和字符串两种写法都收(`EngineKind::LsmTree` / `"lsm"` / `String` / `&String`),拿枚举不用自己 `.as_str()`。这是**自由函数**上的泛型,`KvEngine` trait 依然对象安全。

对象安全的代价是:所有方法都用 `&[u8]` / `Vec<u8>`,不能用 `impl AsRef<[u8]>` 这类泛型参数。

四个引擎的 MVCC 层方法名本来就一致,所以用一个 `impl_engine!` 宏生成全部适配代码,**引擎差异做成开关参数**而不是写四遍:

```
vacuum:  io | plain     // memory 的 vacuum 不返回 io::Result(同形状的还有 vacuum_bounded)
durable: yes | no       // memory 没有 flush / checkpoint
wal:     yes | no       // memory 没有 WAL ⇒ 阈值读写返回 Unsupported
path:    db_dir | db_path | none
merge:   yes | no       // 只有 bitcask 有
```

`tuning()` 不在这些开关里,宏只有一行转发 —— **清单归各引擎的 `MVCC` 自己写**:门面若为每家
开一个参数 enum,就等于把"引擎差异"重新灌回适配层,正是这套宏要消除的东西。返回值
`EngineTuning` 是 `Vec<{key, value: String}>` + `get(key)`,四家共用同一份类型(定义在
`src/common/types.rs`,与 `MvccHealth` / `DurabilityProfile` 同层),键名由
`push_shared()` 统一给(`vacuum_auto_slice` / `wal_checkpoint_bytes`),各引擎只补自己独有的那几个。

不支持的操作返回 `EngineError::Unsupported` 而不是 panic —— 这是门面层在如实告知能力边界,调用方按需处理:

```rust
match db.merge() {
    Ok(()) => println!("merge 完成"),
    Err(EngineError::Unsupported { engine, op }) => {
        println!("{engine} 不支持 {op}")
    }
    Err(e) => return Err(e),
}
```

事务通过 `PhantomData` 借用引擎(`begin()` 返回 `Box<dyn KvTxn + '_>`)。底层 `Transaction` 其实是 `'static` 的,这是**刻意加严**:防止事务活得比引擎句柄久,对持有文件锁的三个磁盘引擎有实际意义。

---

## 关键常量速查

| 常量 | 值 | 所属 |
|---|---|---|
| 页大小 / 可用 payload | 4096 / 4080 B | B+ Tree |
| 双写缓冲单批上限 | 64 页 | B+ Tree |
| 数据文件 rotate 阈值 | 64 MB | Bitcask |
| 写缓冲 | 4 MB | Bitcask |
| MemTable flush 阈值 | 4 MB / 65536 条 | LSM |
| L1 基准 / 层间倍率 | 2 MB / ×10 | LSM |
| L0 compact / 软限流 / 硬停写 | 4 / 8 / 20 个文件 | LSM |
| SST block 条目数 / 目标大小 | 64 条 / 512 KB | LSM |
| Bloom bits per key | 10(→ 7 个哈希) | LSM |
| 大 value 阈值 | 256 B / 256 B / 4096 B | B+树 / Bitcask / LSM |
| 默认 order / 页缓存 | 64 / 1024 页 | B+ Tree |
| WAL 单帧 body 上限 | 16 MB | 全部 |
| **MVCC 档**（`LSMTreeOptions::for_mvcc`，与上面裸 LSM 默认值**不同**）| mem 32 MB / 262144 条、L0 8/12/32、目标 SST 2 MB、块缓存 256、`enable_mem_wal=false`、`enable_bg_compact=false` | LSM |
| 自动回收切片 | 16 Ki 行(四引擎同值;`0` = 关闭) | 全部 |
| WAL 自动收口阈值 | 16 MiB(`0` = 关闭;`memory` 没有 WAL) | 三个磁盘引擎 |

上面这几行都是**默认值**，不是常量 —— 从 M13 起它们都在 `EngineConfig` 里可配，
`KvEngine::tuning()` 读得回来(见「下游接入使用说明」第 1 条)。

**磁盘文件族**(以 B+ 树为例):

```
data.db            主文件(4KB 页,page 0 = meta)
data.db.wal        预写日志
data.db.dblwr      双写缓冲
data.db.freelist   空闲页 sidecar
data.db.blob       大 value(> 256B)
data.db.lock       单写者锁(内含 pid)
```

---

## 已知局限

这是教学项目,以下都是**有意的简化**或已知待改进项,不建议用于生产:

四家的崩溃安全**不等价**,而差异不是"等级高低":`KvEngine::durability_profile()` 把"哪些构件在、哪些不在"作为数据吐出来(位 + 一句 `Display` 只报缺口),并由 `durability_profiles_are_honestly_different` 逐家钉住 —— 下面这份清单是它的人读版,过期了会被测试抓到。

**并发**
- 四个引擎都是**单写者**模型,靠 `Arc<Mutex<...>>` 单锁 + 文件锁保证。没有行级锁、没有并发写入
- ~~事务不是 `Send`/`Sync` 友好的多线程设计~~ → 已改:`KvEngine: Send + Sync`(句柄能进 `Arc`、能跨
  线程),`KvTxn: Send`(事务能整个搬去别的线程,**刻意不要求 `Sync`** —— 内部有 `Cell` 与写集,
  两个线程共用一个事务对象没有语义)。这条是"下游不必再自己包一层门面"的前提(M9)。
  但"句柄可共享"不等于"并发写" —— 上面那条单写者没变。

**LSM**
- ~~事务式灌数据是 **O(N²)**:每次插入都为"要不要 flush"把整个 memtable 数一遍~~ **已修(M19)**:
  条数改成增量计数器 `mem_entries`(`mem_insert_no_wal` 里 +1、`flush_mem` 清零),判定语义一字未动。
  200k 大规模基线抓到它的形状是"每行代价随 memtable 规模涨"(裸 `insert` 不经 MVCC 也一样涨 89→1429 µs/行,
  所以不在事务层)。两把守卫:`test_mem_entry_counter_matches_slow_scan`(口径对拍,慢算法留着当对照 ——
  **条数是"版本条数"不是"键数"**),`test_flush_check_does_not_scan_memtable_per_insert`(计数尺:灌 4000 行
  期间"为判定走过的 memtable 条目数"必须是 **0**,同一条用例里主动慢数一次证明探针不是空转)。
  ⚠️ 归因台架 `lsm_fill_attribution`(`#[ignore]`)留着:它同时报块读、SST 数、memtable 字节和
  begin/set/commit 三段耗时 —— 这条病的教训正是"**µs/行 一个数不能替整条路径说话**"。
- `fix_level_overlaps` 发现层内 key range 重叠**只打印警告,不修复**
- compact 时每个 key 只保留最新一条,`enable_snapshot_gc` 并未真正按 snapshot 水位保留旧版本(源码注释已自陈)
- `blobs/` 旁路的回收挂在 `vacuum()` 的全库轮次上(compact 判不了"别的层里还有更新的版本引用同一个 blob");**有界轮次不动 blob**,引用关系只有全库才看得全
- `start_bg_compact` 只建控制块不起线程,真正的后台线程要显式调 `spawn_bg_compactor`

**Bitcask**
- 数据记录**没有 CRC**,存在静默损坏风险(blob 和 WAL 有 CRC)
- merge **不是原子的**(删旧文件与 rename 新文件之间有时间窗),但**可恢复**:崩在改名之前留下的 `data.merge` 会在下次 open 被认回(见 `durability_profile().merge_artifact_recovery`)。改名顺序不能反过来 —— 旧 active 句柄还挂着时 Windows 的共享锁会挡住覆盖
- 全量 key 常驻内存,key 数量受内存限制

**B+ 树**
- 大条目下的重组是**"装得下才动盘"**:借键/合并的结果若超过一页载荷(4080 B),整次重组作废
  (盘上一字未动)。以前的写法是 `write_page` 打印一句、返回的 `false` 被调用链丢掉,而父节点照样
  删分隔键 + `free_page(死页)` ⇒ 幸存页还是合并前的旧内容,那批行两份副本一起没 = 静默丢数据
  (40 B 键 + 60 B 值、`order=64` 时合并两个半页就是 7.5 KB)。守卫:
  `test_reorg_never_drops_oversized_pages`(把检查临时关掉,第 36 次删除就报"删除失败")。
- ~~放弃合并会让下溢的叶子越留越多、填充率一路掉,delete 密集之后一次全库走查更贵
  (实测 `vacuum()` 206 ms → 450 ms @ 20k 行)。正解是"合并后再分裂",还没做。~~
  **已做(M14)**:union 装不下一页时不再整次作废,而是**按字节把两页的条目重切成两个各装得下的
  半页**(不释放页、只换父节点那把分隔键)。分界取"左页能装下的最大条数"而不是中点 —— 中点切法
  两片都停在半满,页数降不下来;压满左页会把剩余挤成一小片,那片下一轮就能和自己的右兄弟合并成功
  ⇒ 真的释放一页。**同一形状(4000 行插满、隔一个删一个)叶子 222 → 64 页**(关掉重切 = 111 页,
  1.73×),每页字节容量当场量得 35 条 ⇒ 理想 58 页 —— 结论从这条**计数**拿,不从墙钟拿
  (同一份构建连跑四次 `vacuum()` = 241/252/300/393 ms,本机 ±2× 噪声;改前那一次 466 ms,
  方向一致但幅度不敢报成一个数)。守卫 `test_reorg_packs_leaves_instead_of_leaving_them_half_empty`
  (页数 ≤ 理想页数 × 1.5,尺子直接量"一页装得下几条");把重切那一支临时关掉 ⇒ 111 页、当场红,
  而"没丢数据"那条**全绿** —— 这条病只有页数看得见。字节的分档靠 bincode 逐条相加,
  由 `test_reorg_size_formula_matches_bincode` 与真实序列化逐档对拍(公式偏了只会退回"什么都不动",
  不红测试、静默不优化,所以必须对拍)。
- **单个逻辑键必须短于一页载荷(4080 B;版本键另加 8 B)**:装不进任何一页的行会被**明确拒收**
  —— `insert` 返回 `false`、`MVCC::set` 在 append WAL **之前**就返回 `false`(判据
  `BPlusTree::row_fits`),树那边**一字未写**,所以已有数据不受影响。老写法是按条数对半切、
  两半都不量就写盘 ⇒ 写不进的那一半变成没人引用的页 = 静默丢数据(与 M11f 同一个洞,方向相反)。
  插入路径现在也是"下降一趟全在内存里算、到顶统一落盘"(`InsertStep::writes`),否则
  "孩子已分裂写完、父亲放不下"这一格会重演同一个形状。守卫:
  `test_oversized_row_is_refused_not_dropped_silently`(把分裂退回按条数对半切 ⇒ 当场红)与
  `test_oversized_row_is_refused_before_the_wal`(把拒收挪到 WAL 之后 ⇒ WAL 里多出一条 `Write`,红)。
  ⚠️ 剩下没做的:BulkLoader 的 `put` 与 WAL 重放**没有返回值通道**(`KvBulk::put` 是 `()`),
  所以那两条路只能 `log::error!` 喊出来(`insert_or_shout`),不会静默、也传不上去。
- ⚠️ **页数会超过页缓存上限**:`max_cache_pages` 默认 1024,而 20000 行(40B 键)的树就有 **1257 页**。
  只读走查现在不再回填溢出的部分(`read_page_for_walk`:装不下就不缓存),所以一次全库走查
  不再把点读的热页挤出去(`health()` 150 ms → 21.2 ms、换出 1600 → **0**)。
  容量**能**按库大小调了(就在 `EngineConfig`,见「下游接入使用说明」第 1 条的旋钮表),剩下的
  缺口是**没有自动档**:库涨到多少页要人自己盯 `health()`/页数来调,引擎不会自己按页数定容量。
- blob GC 是简化版:只在完全无引用时清空,有引用则暂不重写(append-only 空间残留)
- `page_id` 在页头里被截断成 u32

**其他**
- ⚠️ **值信封不对称**:`set(key, value)` 把 `value` 当**已经打过信封**的逻辑值存,而 `get` 一律走 `logical_to_user` 剥标签。
  于是**直接用门面存裸字节**时:值以 `0x00` 打头会少 1 字节,以 `0x01` 打头会被当成 TTL 信封少 9 字节、甚至被判定"已过期"而读成 `None`。
  **已开口子(M16)**:新增 `KvTxn::set_raw(key, value)` —— 信封由引擎打,任意字节(空、`0x00` 打头、
  `0x01` 打头像过期 TTL、超过 blob 阈值的长值)都一字不差地存下并取回,守卫
  `set_raw_round_trips_every_byte_pattern`(四家 + 重开之后,并对照断言"`set` 存同样的字节确实会丢")。
  ⚠️ 刻意**没有**加 `get_raw`:`get` 本来就是它的读侧对应物,再造一个同名不同义的口只会让人问"该用哪个";
  真要"不经剥壳的存储字节"那是另一个能力(目前只有引擎内部的 `latest_visible_raw`),未对外承诺。
  仍留的坑:`set` 本身还是要求调用方自己打过信封(现有调用点按这个约定写,改它是破坏性变更)
- ~~⚠️ **`seek_prev` 是"整段物化再挑一条"**~~ **已修(M20)**：四家原先各一份
  `scan(None, Some(key‖0x00))` + `retain` + `pop` ⇒ 一次调用 = O(库大小) 的时间**和内存**
  （200k 实测单次 **140~610 ms**，同库一次点读 2.9~57 µs）。现在循环只有
  `common::store.rs::seek_prev_with` 一份，每家交两个口子：反向一步（mem `range_back` /
  bitcask `range_meta_back` / bplus `find_back` / lsm `find_back` —— 都是现成的点读热路径原语）
  与"这把键活着吗"（配 `live_record_of_logical`，判定与 `scan` 同一份，不另写第二份）。
  20k / 200k 台架复测（µs/次）：bitcask 52 192→**20.6** / 382 234→**39.8**，bplus 32 444→**16.5** /
  328 583→**98.3**，lsm 46 961→**24.9** / 610 488→**93.6**，memory 31 583→**10.4** / 139 960→**16.7**
  ⇒ 200k 便宜 **3 300~9 600 倍**，规模比值从 4.4~13× 收到 1.6~6×（剩下的那点是 bplus 页覆盖 /
  lsm 版本散在更多 SST 的**老账**，见上表，不是这条通道）。
  换掉形状之后代价单位也换了：一次 `seek_prev` 现在等于**个位数次点读**（200k 实测 1.9~6.3× 同库
  随机点读），改之前是 **6 100~48 000×**（bitcask 382 234 µs ÷ 9.6 µs、memory 139 960 ÷ 2.7）。
  ⚠️ 那条"O(库) 的**内存**"是读代码读出来的，台架量不到：`seek_prev` 的峰值一直是 `+0.0 MB` ——
  物化的 `Vec` 在函数内部就析构了，而采样发生在调用边界上（同一次跑里"一次全量 scan"能把
  `+48 MB` 报出来，正因为它的 Vec 交到了调用方手里）。尺子看不见 ≠ 没有，这条别记反了。
- ~~⚠️ **`scan_page` 的游标 `after` 没有下推成迭代下界**~~ **已修(M21)**：`common::store.rs` 新增
  `fold_lower_bound(start, after)`，四家把游标抬成**迭代起点**（过滤器原样保留）⇒ 每页只付页宽。
  计数守卫（N=2000、页宽 50）：首页 52 / 最后一页 **51** 把键，bplus 3 / 3 次页读；把下推退回
  "只按 `start`"四家**全红**。200k 台架实测每页：bplus 189 151 → **1 361 µs/页**、lsm 132 601 →
  **1 522**、bitcask 18 821 → **1 495**、memory 16 817 → **620**；连带 `migrate` bplus 216.2 →
  **16.3 µs/行**、lsm 167.1 → **13.6**。三条踩过的坑写在这里免得重犯：
  ① **只测首页的守卫抓不到这条** —— 首页本来就从库头起，bitcask 那条旧守卫量的正是首页，
  于是这个洞在它眼皮底下活了一整个版本；新守卫把"最后一页"写进了名字。
  ② 余量要按各自尺子给 —— bplus 的尺子数**页**，照抄键-counting 的 `2*(first+PAGE)` 在缺陷版上照样绿
  （50 行的余量够盖住 40 次页读），改成 `2*first+8` 才会红。
  ③ 下界必须取 `after‖0x00×8` 而不是 `after‖0xFF×8`：后者会把 `"abcd"`（比 `"abc"` 大、字节序却小于
  `"abc"‖FF…`）**整把键跳掉** —— 静默少行。
- ⚠️→✅ **`seek_prev` 为什么可以"反向走、第一个见到的 raw 就当是最大的键"**（这条判断在 M20 里翻过一次，
  记录下来免得再翻）：版本键按 `raw ‖ version_be` 排序，所以**短前缀键的高版本行**理论上会插进更长键
  的区间（`enc("ab", MAX) > enc("abc", 0)`），于是"反向第一个 raw"可能给出 `"ab"` 而不是 `"abc"`。
  但本仓库的老合同是 **`version < 2^56`**（见 `common::version_key_upper_bound` 的文档与其守卫）⇒
  版本首字节恒为 `0x00`，小于任何 ASCII，那条交错**在合同内不成立** ⇒ 反向游走是正确的，
  **不需要**新造一把"按 raw 排序、可定位、可反向"的索引。
  ⚠️ 依赖是实的：合同一破（有人把版本推到 ≥2^56），`seek_prev_with` 会**静默返回偏小的键** ——
  所以它引用的就是那条已有合同，而那条合同有自己的守卫，别拆。
  顺带订正我早先的一句判死："把正向折叠镜像一遍"当时判死是因为收口条件无法在 enc 字节序上表达 ——
  那个判断只对**不带界的镜像**成立；按组退不需要收口条件，界本身就是收口。
- ~~⚠️ **单个事务内的写集查重是 O(写集²)**~~ **已修(M24)**：内核 `TxnCore::write` 登记写集用
  `writes.iter().any(|k| k == key)` 判重复，部分撤销的摘账是 `retain` + `any` ⇒ 两条都是单事务内
  O(写集²)。活跃表的值类型现在收成 `HashSet`（`Registry` 一个别名，四家的 `active_txn` 与
  `is_unfinished` 都用它），**只留一份账**（不并存顺序 `Vec` + 哈希副账 —— 写集不需要顺序，
  两份账就会有一份写漏）。实测（一个 20 000 行的事务，release，µs/行，头 200 vs 尾 200）：
  bitcask 20.6/10.0、bplus 36.8/50.6、lsm 11.4/8.7、memory 1.5/1.5 ⇒ 平的；同一台架下把查重
  改回遍历写集，bitcask 报 **9.3 → 288.8 µs/行（30.9×）**。
  ⚠️ 这条**不是** 200k 台架那条超线性（那边每 1000 行一事务），它的形状是"一次导入塞一个大事务"。
  ⚠️ 它也只有**一条计数尺守不住**：探针长在 `register_write` 里，谁改算法却忘了搬探针，尺子照样读
  1 次/行 —— 所以形状尺（精确计数 2010/98）与代价尺（上面那条墙钟台架）是两条，互为对照。
- ~~**门面还缺的三项**~~ → 已补两项(M13):`EngineConfig` 现在装着四家的旋钮(bitcask 分片上限、
  LSM 整份 `LSMTreeOptions`、WAL 收口阈值、自动回收切片),`vacuum_bounded` / WAL 阈值读写 /
  `tuning()` 也都上了 `KvEngine`。仍缺的一项是跨引擎 `migrate()` 的库 API(只有 CLI)。
  这三项原先都是**下游 wrapper 里真在干活**的部分,缺一项就多一层该被删掉的复制
- ~~只读事务也要付一次 WAL fsync~~ → 已改:`Begin` 推迟到第一次 `write()` 才补,只读事务的 `commit` 不写 `Commit`、不 fsync(实测 2265.8µs/次 → **15.8µs/次**,WAL 落字节 56 → **0**)。策略只有一份实现:[`crate::common::WalPolicy`]
- ~~`FileLock` 在三个引擎里各有一份近乎逐字重复的实现~~ → 已收成 `src/common/disk.rs` 一份(`DiskFile` / `FileLock` / `crc32` 三样都是崩溃语义的载体,`diff` 实测原先 bitcask 与 lsm 的 FileLock 差 0 行)。各引擎只保留自己的**锁文件路径规则**(dir/bitcask.lock、`<stem>.lock`、dir/lsm.lock)
- ~~三份 `wal.rs` 仍是各自一份~~ → 已收成 `src/common/wal.rs` 一份(原先 `diff` 只差 14–16 行注释与 import,`WAL_MAGIC` 与帧头本来就更相同:同一格式的三份复制)。各引擎的 `wal.rs` 现在只是 re-export + 自己的 WAL 路径规则
- bulk load 路径**崩溃不安全**:`finish()` 之前进程挂掉可能丢数据(它关掉了 WAL 和 fsync,这正是它快的原因)。`import_kv` 走的就是这条路径
- 非 Windows / 非 Unix 平台上文件锁退化为无保护

---

## 命令行工具:数据搬运与压测

12 个可执行文件,分两类:**跨引擎搬数据**(`import_kv` / `export_kv`)和**压测**(`bench`)。

四个引擎各有一套同名工具,而 `[[bin]]` 的 name 必须全局唯一,所以统一加引擎前缀:

| 引擎 | 导出 | 导入 | 压测 |
|---|---|---|---|
| Bitcask | `bitcask-export-kv` | `bitcask-import-kv` | `bitcask-bench-bulk` |
| B+ Tree | `bplus-export-kv` | `bplus-import-kv` | `bplus-bench` |
| LSM | `lsm-export-kv` | `lsm-import-kv` | `lsm-bench-bulk` |
| Memory | `memory-export-kv` | `memory-import-kv` | `memory-bench` |

编译产物在 `target/release/`,可直接分发调用,不必每次都过 cargo。

### 跨引擎搬数据

导出导入用的是**同一套快照格式**,所以任意两个引擎之间都能互搬 —— 从 Bitcask 导出的快照可以直接灌进 LSM:

```bash
# 1. 从 bitcask 导出
cargo run --release --bin bitcask-export-kv -- --dir ./data/bitcask --out snap.bin --format bin

# 2. 灌进 lsm
cargo run --release --bin lsm-import-kv -- --input snap.bin --dir ./data/lsm --format bin
```

导出的是**当前快照下每个 key 的最新可见版本**,历史版本和已过期的 TTL 数据不会带过去 —— 搬运即压缩。

**两种格式**:

| 格式 | 说明 |
|---|---|
| `bin` | magic `BPEXP001` + 重复 `(key_len, key, val_len\|tombstone, value)`,体积小、快,**推荐** |
| `jsonl` | 每行一条 `{"key":"<base64>","value":"<base64>"\|null}`,可读、可 grep、方便和外部系统对接 |

key 和 value 都是 base64,所以任意二进制内容(含中文 UTF-8)都能无损往返。

**常用参数**:

```
导出:  --dir/--db PATH   数据目录
       --out PATH        输出文件
       --format jsonl|bin
       --include-deleted 连 tombstone 一起导(默认不导)

导入:  --input/-i PATH   输入快照
       --dir/--db PATH   目标目录
       --format/-f jsonl|bin
```

B+ 树的工具多两个参数:`--order N`(阶数)和 `--cache N`(页缓存页数)。LSM 的导出多一个 `--with-seq`。

⚠️ **导入走的是 bulk load 路径**:没有冲突检测、不写 WAL,所以快,但**单写者假定** —— 导入期间别对同一个库开其他写事务。

### 压测

```bash
cargo run --release --bin lsm-bench-bulk -- --n 1000000 --dir ./bench_lsm
cargo run --release --bin memory-bench -- --n 100000 --bulk
```

```
--n N          写入条数
--dir/--db     库路径(memory 不需要)
--batch N      批大小
--vlen N       value 字节数
--flush-every  每 N 条刷一次
```

**务必加 `--release`** —— debug 模式下差几十倍,测出来的数字没有参考价值。

### 一次实测

Bitcask 库 → 导出 → 灌进 LSM → 再导出,两份快照 236 字节**逐字节完全一致**:

```bash
bitcask-export-kv --dir ./src_db  --out snap.bin      --format bin   # 12 keys
lsm-import-kv     --input snap.bin --dir ./dst_lsm    --format bin   # live_keys=12
lsm-export-kv     --dir ./dst_lsm  --out rt.bin       --format bin
cmp snap.bin rt.bin                                                  # 一致
```

### 实现说明

快照编解码是 `src/common/kv_snapshot.rs` 一个普通的 crate 模块,12 个 bin 通过 `use storage_engines::common::kv_snapshot::{...}` 引入,全库只有这一份实现。

记录用 `KvRecord` 表示,两个构造函数对应两种快照格式:

| 构造 | 用途 | 写出的魔数 |
|---|---|---|
| `KvRecord::new(key, value)` | bitcask / bplus / memory,以及 LSM 不带 `--with-seq` 时 | `BPEXP001` |
| `KvRecord::with_seq(key, value, seq)` | LSM `--with-seq`,每条多 8 字节 LE 写入序号 | `BPEXP002` |

`write_bin` 按「是否有任一条带 seq」自动选魔数,所以不带 seq 的导出与旧格式**逐字节一致**,老快照和老工具照样能读。这条兼容性由 `test_plain_is_bpexp001` 守住。

---

## 测试

```bash
cargo test                    # 全部:273 单元 + 4 集成 + 5 文档测试
cargo test --lib engine::     # 只跑门面层(21 个,含大库基线台架;四引擎全覆盖)
SCALE_ROWS=20000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 scale_baseline
                              # 四引擎 × 每种操作的大库基线(µs/次 + 常驻/峰值 MB)
cargo test --test test_lsm_tree
```

门面层的测试是**四个引擎跑同一套断言**的,覆盖增删查、批量与扫描、写冲突、bulk 导入、TTL 与元数据、vacuum 与 flush、`health()` 读数一致、自动回收默认开着且能被关掉 —— 这也是验证「四引擎行为一致」的地方。换引擎只改变涨速,不变的是要盯的那些指标,所以这一层必须按「四家同尺度」断言。

集成测试会在 `./data/<引擎名>/` 下建演示库,不污染仓库根目录。

---

## 开发环境

- Rust 1.92+(edition 2024)
- 跨平台:Windows / Linux / macOS。文件锁在 Windows 用 `share_mode(0)`,Unix 用 `flock`

release profile 开了 `lto = true` 和 `codegen-units = 1`,压测请务必用 `--release`。
