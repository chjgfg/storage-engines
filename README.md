# storage-engines

四个从零手写的 KV 存储引擎 + 一层统一 trait 门面。**教学 / 学习性质**,不是生产级数据库。

同一套 MVCC 事务语义(快照隔离、写冲突检测、vacuum 回收)跑在四种截然不同的存储结构上,可以直观对比 **LSM Tree / B+ Tree / Bitcask / 纯内存** 在读写放大、范围扫描、崩溃恢复上的取舍。

- 约 2.3 万行 Rust,edition 2024(需要 rustc ≥ 1.85)
- 依赖只有 5 个:`serde` / `bincode` / `memmap2` / `fs4` / `log` —— B+ 树、SST、Bloom、WAL、双写缓冲、CRC32 全部自己实现
- 300 个单元测试 + 4 个集成测试
- 当前版本 **0.4.0**:随 uptime 恶化的七条通道逐条堵死,版本回收改为自动 —— 破坏性变更与升级要点见 [CHANGELOG.md](CHANGELOG.md)。⚠️ crates.io 上的 0.4.0 是 **2026-10-15** 发的那版,它**不含** 10-16 起的 **M10–M34**(LSM 点查常驻句柄、窗口 scan 界下推、bplus 读路径与丢页修复、配置旋钮、bulk/事务内核合并、`migrate`/`scan_page`、灌行 O(N²)、`seek_prev`、游标下推、页缓存与块缓存自动档、写集哈希、WAL `Begin` 不落盘、层内重叠真折+先记 MANIFEST 再删文件、全库折叠流式化、重开不再自带压实、有界回收一片改惰性归并、压实的尺寸判定改成增量账、一次页读少一次系统调用且 CRC 快 4.3 倍、缓存自动档的顶交给调用方且覆盖率分母可读、压实的输入改流式源(一次压实瞬时内存 156.8 → 6.3 MB)、bplus 写侧下降改走解析缓存且写完的页直接回填(裸 insert 单价 2.4×、页读/行 2.94 → 0.00))—— 要那些得走 path/git 依赖,见[引入方式](#引入方式)
- 许可证:MIT 或 Apache-2.0 任选

---

## 引入方式

⚠️ **先看这条(2026-10-17 用 crates.io API 实测,不是凭印象)**:架上最新是 **0.4.0**
(发布于 2026-10-15,未 yank)。**这一版已经包含**七条"随 uptime 恶化"通道的修复与 M9 的门面
改造(`KvEngine: Send + Sync`、类型化选型、`EngineName`)⇒ 版本号依赖就能拿到。

但 0.4.0 那个发布件**不含 10-16 之后的 M10–M34**:LSM 点查的常驻文件句柄与稀疏索引二分
(下游实测单次点查 0.86 ms 那条就是它)、四引擎窗口/前缀 scan 的界下推、bitcask 懒分配写缓冲、
B+ 树 delete 路径的静默丢页修复 + 读路径三项、`EngineConfig` 的配置旋钮与 `tuning()`、四引擎
事务内核合成一份、`migrate` + `scan_page`、LSM 灌行 O(N²)、`seek_prev` 整段物化、游标未下推、
B+ 树页缓存与 LSM 块缓存的自动档、单事务写集查重 O(写集²)、WAL 里那条零信息的 `Begin`、LSM 层内重叠的真折与「先记 MANIFEST 再删文件」、全库折叠改流式、重开不再自己触发压实、有界回收一片改惰性归并、压实的尺寸判定改成增量账、页读路径与 CRC32 提速、**缓存自动档的顶可以由调用方抬（`set_cache_auto_max`）且覆盖率的分母可读**、**压实的输入改流式源（一次压实瞬时内存 156.8 MB → 6.3 MB）**。
磁盘格式这二十五批**一个都没动**,所以不是"换了就得重建库"——是同一份库,行为与代价不同。

```toml
# (a) 同机项目:path 依赖 —— 拿当前 master,含 M10–M34
storage-engines = { path = "../storage-engines" }

# (b) 走远端:git 依赖(gitee 与 github 内容一致)
storage-engines = { git = "https://github.com/chjgfg/storage-engines.git", branch = "master" }

# (c) crates.io —— 0.4.0:事故修复齐全,但没有 10-16 之后的读路径/配置改动
storage-engines = "0.4"
```

`cargo install storage-engines` 装到的同样只是已发布的 0.4.0。要 M10–M34 又想要版本号依赖,
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
| `max_cache_pages` | **`0` = 自动档**(M22) | 只有 `bplus_tree` | 页缓存容量(页数)。自动档按库的实际页数给,夹在 `[1024, 2048]`(≈ 4~8 MiB 常驻),并在每次刷盘后重新评估;传一个数就是显式容量,自动档完全不参与。**上限本身可抬**(M32,见下一行 `cache_auto_max`)。生效值从 `tuning()` 读回(`max_cache_pages` / `cache_pinned_pages` / `cache_auto` / `page_cache_auto_max` / `total_pages`) |
| `lsm` | `None`(= `LSMTreeOptions::for_mvcc()`) | 只有 `lsm_tree` | 整份 options;改单个字段用 struct-update 从 `for_mvcc()` 抄基线 |
| `bitcask_max_file_bytes` | `None`(= 64 MiB) | 只有 `bitcask` | active 分片软上限,决定 rotate 与 `merge()` 的粒度 |
| `wal_checkpoint_bytes` | `None`(= 16 MiB) | 三个磁盘引擎 | 涨过就在下一次提交后自动 checkpoint;`Some(0)` = 关闭 |
| `vacuum_auto_slice_rows` | `None`(= 16 Ki 行) | 四引擎 | 自动回收切片宽度;`Some(0)` = 关闭 |
| `cache_auto_max` | `None`(= bplus 2048 页 / lsm 1024 块) | `bplus_tree` + `lsm_tree` | **自动档的顶**(单位是各家的槽位:bplus = 4 KiB 页、lsm = SST 块)。`Some(usize::MAX)` = 不设顶 ⇒ 自动档就等于"跟着库长",代价是把库复制进 RAM(bplus 200k 行 ≈ 47 MB、lsm 600k 档全驻留 ≈ 18.7 MB)。热调 `KvEngine::set_cache_auto_max` / 读回 `cache_auto_max`（M32）；**给另外两家会在打开时就返回 `Unsupported`**,不静默"设置成功" |

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

⚠️ **`*_blocks` 是配置值，`block_cache_capacity` 才是生效值**（M23）。LSM 的 MVCC 档开着块缓存
自动档：`生效容量 = clamp(库的 SST 块数, block_cache_blocks, block_cache_auto_max)`，默认
256 地板 / 1024 上限，并在每次 flush / compact 之后重算。所以长跑进程里"这个数在动"是设计如此，
不是旋钮失效；要钉死就把 `block_cache_auto_max` 设成 0（配置值即生效值）或把地板抬到上限。
**上限也能在运行时抬 / 压**（M32）：`KvEngine::set_cache_auto_max(n)` 走的是"就地改容量"，
不会重建缓存（重建 = 调一次旋钮就把焐热的块全丢掉，M23 踩过）；读回同一个数用 `cache_auto_max()`。

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
| `version.rs`:版本键编解码 + vacuum「留哪一版」判定 | 遍历原语:`iter_meta_from` / `scan_entries_break_from` / `light_iter_range` / `scan_entries_from` —— KeyDir、叶子链、SST 分层、BTreeMap 根本不是一种东西 |
| `kernel.rs`:可见性判据 `visible_at`、`HealthFold`、`VacuumScheduler`(含"取账不能 swap")、`is_unfinished`、**`fold_window`**(窗口折叠驱动)、**`search_page_from`**(分页核) |
| **`store.rs`(M18a):`VersionedStore` trait + `collect_rows` + `search_window`** —— "按 enc 升序折出每把 key 的胜出行、界内过滤、取值、剥壳、窗口化"从此只有一份。**M20 又添两句**:`seek_prev_with`(反向逐步的循环 + 按组退界)与 `live_record_of_logical`(单键"活着吗"的判定,与 `scan` 同一份口径) | 有界遍历的**早停与起点**:谁能反向走、谁只有单向 `next_leaf`、谁的早停停在堆顶而不是源尾。**反向一步**同一家一个样子:mem `range_back` / bitcask `range_meta_back`(只读 KeyDir,不碰 value)/ bplus `find_back`(跨叶有进度守卫)/ lsm `find_back`(逐源取最大) |
| `types.rs`:`MvccHealth` / `VacuumStats` / `VacuumRound` / `DurabilityProfile` | 各自的守卫测试:同一条通道在不同引擎的**量尺不同**(读盘次数 / value 解码次数 / 物化次数 / 克隆次数),拿一把尺量不到另外三家 |
| **`txn.rs`(M18b~M18e):`TxnCore` + `WriteSide` + `WalTail`** —— begin 的版本号与活跃快照登记、**整条写序列**(冲突判定 → 值编码 → 页容量判定 → WAL Write → 落行 → 写集登记；M25 把"WAL Begin"从这条序列里删了，七步)、`commit` 销账 + 回收记账、`rollback` 按写集撤销、批量写的**部分撤销**、bulk 会话的版本与收尾幂等、**写集本身(M24:`Registry` 的值类型是 `HashSet`)** | 写路径上"这一行的字节去哪、WAL 记录怎么排、大值走不走旁路、装不装得下一页" —— 那些是 `WriteSide` 的槽(有 WAL / blob / 页容量的人家只实现自己那几条,其余走默认实现 = 不做也不失败) |
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
| bplus `health()` | 20.7 | 357.9 ms | **2.06× → 17.3×** | ✅ 已修(M22)：绝对值两头都降（23.96→20.66 / 438.2→357.9 ms），**比值变大是因为小库那一头掉到地板价** —— 判超线性要看每单位成本的绝对值，比值只在"两边都没装满"时才有意义 |
| bplus 随机点读 / multi_get | 11.6 / 10.5 → **8.5 / 6.0** | 39.8 / 39.6 → **43.8 / 39.2** µs | 改后 5.2× / 6.5×（**绝对值两头都在降**） | ✅ 已修(M22)：20k 那一档现在就是台架的"全焐热地板"(7.8 µs/次)；200k 仍受自动档 2048 页上限约束 ⇒ 覆盖率 16.9%，要更快得自己把容量调大（价签：+8.4 MB 常驻） |
| lsm 随机点读 / multi_get | 9.6 / 9.5 | 50.0 / 41.6 µs | **5.2× / 4.4×** | ⚠️ 这两列是 **M23 之前那份构建**的数，别拿今天的 µs 去对照它 —— 见下面那段"噪声标定"。M23 给块缓存加了自动档，凭据是同构建 A/B 的**计数**（5002 键：乱序 27.9 → **8.5** µs/次、未命中 0.39 → 0.03，块/次两边都是 4.00；200k：一次未命中 ≈ 94 µs，冷启动读 4 条 1027.5 → **289.5**） |
| memory 全项 | — | — | ≈1.0–1.4× | 平 |

**噪声标定（M23 当天补测，方法学要留档）**：同一份构建连跑三轮 `SCALE_ROWS=20000`，
**没被这条改动碰过的 bitcask 灌行**本身就落在 19.2 / 24.6 / 28.2 µs/行（本表记的是 14.7），
lsm 灌行 20.7 / 20.6 / 23.5（记 14.5）⇒ 本机（`D:` 是 5400 转机械盘）跨构建、跨时段的墙钟
可以差到 **1.3~1.9×**。所以：① 上面这些 µs 列只在"同一轮同一份构建"内可比；
② 一条改动的因果凭据只能取**同构建 A/B 的计数**（块/次、未命中/次、槽位检查次数），
没有计数可用时（µs 级的纯 CPU 改动）必须**交错跑多轮、各取最小值**再比 —— M31 实测单次配对连方向都会翻
（同一份代码的 CRC 配比值报过 1.23× 与 **0.81×**＝"新实现更慢"；交错 5 轮取最小后两轮独立给出 4.33× / 4.27×）。
干扰只会让一次变慢，不会让最小值变慢 ⇒ **只有最小值可比**，而且同一进程内的相邻两段也躲不开这条；
③ 刚编译完的那一轮别读数（编译器还没退出时量到的 lsm 点读 11.6 µs/次，安静复跑三轮是 6.2~6.9）。

另外量出来的一条（**M27 已修**，顺手订正一处归属错误）：**200k 行做 `一次全量 scan`，只有 bitcask 的瞬时峰值会涨**
——原先那条没写是哪家，实测四家同一轮里 bplus / lsm / memory 都是 `峰值 +0.0`，bitcask 是 `+45.5 MB`
（结果集本身常驻 30.7 MB）。根因不在"返回物化 `Vec`"，而在**中间那一趟**：`scan_foreach` 的文档写着
"不收集全库到单一大 Vec"，实现却是 `for … in collect_latest_raw_in(...)` —— 先把界内每一行的
`(键, 版本, 字节)` 全折出来，再逐条搬成 `ExportRecord`，于是两份头结构同时活着，而且回调提前叫停
连"折"这一段都省不到。改成共用件 `fold_rows_streaming`（折一行交一行）+ 无界折叠逐片
（`FOLD_CHUNK = 4096` 组）之后：**峰值增量 45.5 → 25.0 MB**（同一台架、同一份 200k、常驻 30.7 不变），
四家 `export 全库` 的常驻 32.9 → **30.8 MB**。计数凭据更硬：**只要 1 行就只取 1 次值**
（老写法是 2000 次），四家各自的守卫在同一处变异下 **4/4 红**。
⚠️ 翻页仍然请走 `KvTxn::scan_page` —— 它每页只折 `limit + 1` 组，结果集本来就不该整个拿在手里；
⚠️ 剩下那 25 MB 的峰值增量里还含着"结果 `Vec` 自身的增长"，`峰值` 是进程高水位，
这条台架**拆不出** residual 具体是谁 —— 要拆得另做隔离台架（已记进未修队列）。

### 三档复测（20k / 200k / 600k，同一构建 `f20b7fd`）与那条 1900× 的悬崖（M28）

判据换了一把，因为两档不够看阈值：表里的**单价倍数 = (600k ÷ 20k 的原始倍数) ÷ 该操作的预期倍数**，
预期按操作类别给 —— 单价型（`µs/行`·`次`·`键`·`页`）1×，整库一趟（`search 首页`/`health()`/
`vacuum() 全库`/`冷重开`/`key_count`）30×。**对照臂 = memory 引擎**：它不落盘、纯内存操作在这一轮
仍普遍读到 1.6~2.7× ⇒ 那就是本机的底噪，磁盘引擎上 **≤3× 一律不当结论用**。数据目录在 `C:`
的 SSD（不是 `D:` 那块 5400 转机械盘），所以这些 µs 不含机械盘 fsync 单价。

过底噪的只有四组，其中一组是这一轮新出现的悬崖：

| 引擎 操作（单位） | 20k | 200k | 600k | 单价倍数 | 判读 |
|---|---|---|---|---|---|
| **lsm 再冷重开（µs）** | 10 237.3 | 10 296.5 | **17 487 030.8** | **56.9×** | ✅ **已修(M28)**：同一台架修后复跑 **9 106.7 µs**、重开常驻 12.1 → **1.9 MB**（跨轮数，只当量级；因果凭据取下面那对**同轮**剖面 A/B 1668× + 层文件数不动） |
| bplus multi_get / 随机点读（µs/键·次） | 5.9 / 9.0 | 42.5 / 39.7 | 66.9 / 48.7 | 11.3× / 5.4× | ✅ **已修一半(M31)**：那次未命中的单价砍掉一半多（见下面 M31 那段）。同轮复跑 39.6 / **27.5 µs**。剩的是**覆盖率**（每次点读仍缺 1 页叶子）⇒ ✅ **决定权已交出(M32)**：`set_cache_auto_max(usize::MAX)` 把顶抬掉，实测抬后一轮点读落盘 **0** 次；价签 200k 全驻留 ≈ 47 MB |
| lsm 随机点读 / multi_get（µs/次·键） | 4.7 / 4.1 | 48.8 / 42.9 | 76.5 / 71.2 | 16.3× / 17.4× | ⚠️ M23 说过的那条上限：块缓存自动档封顶 1024 块，600k 的块数早超 ⇒ 覆盖率随库缩。✅ **决定权已交出(M32)**：`set_cache_auto_max` / `EngineConfig.cache_auto_max`，`tuning()` 给分子(`block_cache_capacity`)与分母(`sst_blocks`)；全驻留价签 ≈ 18.7 MB |
| **lsm vacuum_bounded 一片（µs/次）** | 2 716.3 | 10 277.0 | 30 090.7（复跑 15 676.8） | **5.8~11×** | ✅ **已修(M29)**：修后 600k = **4 044.7 µs** ⇒ 对 20k 的 2 716.3 只剩 **1.49×**（底噪内，"一片与库大小无关"重新成立，但这回是三档 + 一条计数守卫凭的）。机制：旧窗口是「**每个源各取满 `预算` 行**再归并」⇒ 取到的行数 = `源数 × 预算`，而文件数 ∝ 库大小 |

`lsm 再冷重开` 是这一轮唯一"原始倍数 >1000×"的数，所以单独剖了一次（新台架 `reopen_shape_probe`，
`#[ignore]`，**同一轮里给「open 里的 flush 无条件 vs 有条件」两份数**，所以不用跨构建比墙钟）：
**`uncond_flush` 总 13 947 618.8 µs（其中 `flush` 段 13 938 766.2）vs `cond_flush` 总 8 362.3 µs
（`flush` 段 0.0）= 1668×**，其余九段合起来 ≈ 3 ms。计数器把机制说完了：

- 机制：`try_open_with` 末尾**无条件** `tree.flush()`，而 `LSMTree::flush()` 末尾**无条件**
  `maybe_compact()`。bulk 的 `finish()` 会把 WAL 收干净（redo 0 条）**但**把 L0 文件数正好留在
  `l0_compact_threshold`(8) 上 ⇒ "打开一个库"自己把 85 MB 从 L0 搬进 L1/L2、写出 37 个文件
  （层文件 `[8,0,0,0] → [0,34,3,0]`、盘上 8 个 85 MB → 37 个 74 MB；有条件那臂两处都一动不动）。
  200k 那一档只有 **4 个 L0 文件**（<8）不越线 ⇒ 两臂同价、都不压实。
  **开关是文件数越没越线，跟"恢复"这件事本身没关系** —— 这就是为什么它 30 倍数据涨了 1700 倍，
  也是为什么 M27 那轮"20k↔200k 两档对照"完全看不见它。
- 被证伪的候选：`Wal::read_all` + `analyze_recovery` 全量解析。有 bulk 时重开前 WAL 只剩 32 B，
  `read_all` 3.4 µs。但它**在别的形状里确实贵**：同一台架把 bulk 去掉 ⇒ 60 121 条记录 / 8.7 MB 的
  `read_all` = 254.7 ms ⇒ **每条 ≈ 4.2 µs**（帧头+帧体各一次定位读，而 `DiskFile::read_exact_at`
  是 seek+read）。⇒ "崩溃点离上次 checkpoint 越远、重开越慢"是另一条独立的账。
- 修法：`flush` 改成**只在有 redo 要重放时**做（判在插进 memtable 之前，之后分不出来源）。
  守卫两条，各证过一次红：`test_open_does_not_compact_when_nothing_to_redo`（尺子 = **层文件数不动**，
  不用墙钟；把条件退回无条件 ⇒ 红在 `[0,1,0,0] ≠ [2,0,0,0]`，且它自带"形状没造出来就报空转"的
  前置断言）、`test_open_persists_redo_before_truncating_wal`（反面对照：把 flush 整个删掉也过第一条，
  所以这条量的是**WAL 截断之后的第二次重开还读不读得回来** + memtable 必须已空 ⇒ 红在 `1 ≠ 0`）。
- ⚠️ **诚实边界**：压实没有消失，只是不再由"打开库"来做。带 redo 的崩溃恢复仍然要 flush；
  M30 之前那一步是 12~17 s，**M30 拆分计时后查到真元凶是 `SstBuilder::estimated_bytes()`
  每行重求和整表**（不是"输入整篇物化"），修完同档 **14.43 s → 3.55 s**（见下面 M30 那段）；
  而干净重启后第一次写或 `checkpoint` 会把这笔 L0 压实付掉。M28 改的是**"重启即服务"的等待时间**，
  不是总写放大。
- ⚠️ 另外两家**没跟着改**，理由是量出来的差别而不是"以后统一"：bitcask 的 `flush()` = 空缓冲直接
  返回 + 一次 fsync，bplus 的 `flush_all()` 还兼着把 `next_version` 落到 meta 页（redo 为空也必须做）。
  两家的 open 里都没有无界工作，所以那两处保持无条件，各自在代码里写明了"若将来长出随库涨的活，
  照哪把尺子改"。

`vacuum_bounded 一片`（M29，同轮第二刀）的归因走的是**计数尺而不是墙钟**（新台架
`vacuum_slice_fold_attribution`，把 SST 文件数当自变量、其余不动）：

| 源数（= SST 文件数 + memtable） | 一片交出 | 一片折过的编码键 | 折/交 |
|---|---|---|---|
| 3 | 2048 行 | 6096 | 3.0× |
| 9 | 2048 行 | 18384 | 9.0× |
| 25 | 2048 行 | 22000 | **10.7×（已等于整库）** |
| 修后 25 / 49 | 2048 行 | **2049 / 2051** | **1.0×** |

- 机制就一句话：旧的窗口实现是「**每个源各取满 `预算` 行**再归并」，所以一片的代价 =
  `源数 × 预算`；而 SST 文件数 ∝ 库大小 ⇒ "有界的一片"每行 14.7 µs，比"无界的全库"（4.9 µs/行）
  还贵 3 倍。**24 个文件那档最直白：每个文件只有 834 行，"取满 2048"= 把整篇读完。**
- 改法：`vacuum_bounded` 改走既有的惰性 k 路归并 `light_iter_range`，凑够预算当场收手。
  代价从 `源数 × 预算` 变成 `预算`。守卫 `test_vacuum_slice_walks_only_its_budget` 钉这个比值
  （`折 ≤ 预算×2` **且** `交出 ≥ 预算×7/8` —— 后一条专门防"只走了三行也算过"的空转）。
  变异证红：把那句 `break` 拆掉 ⇒ `交出 16000 行 / 折过 16000 把键`。
- ⚠️ 连带删掉一套**补偿**：旧实现里"窗口太窄就放大 4 倍重试"存在的唯一理由，是窗口右界可能
  把唯一那组版本链切成半条；惰性流只在组边界收手（收手条件就是"看到了后继组的第一行"），
  那个情形结构上不存在了。**改的是代价，不是判据**：`vacuum_bounded_is_bounded_and_ends_where_vacuum_does`
  与四家各自的 `test_vacuum_bounded_slices_and_converges` 一字未动地过了。
- ⚠️ 造形状踩到的坑（值得记）：想让"文件多"，只关 `l0_compact_threshold` 和
  `max_compactions_per_flush` **不够** —— 写路径还有一档 `maybe_stall_on_l0`，L0 到
  `l0_slowdown_trigger`（MVCC 档 = 12）会**绕过那两个旋钮**直接 `compact_level(0)`；第一版台架
  就是在第 13 个文件处把 12 个合成了 2 个，而且因为门面 `flush` 是 `let _ = ...`（吞错），
  它一开始是**静默**失效的。

`multi_get` 那 11.34× 与随机点读同源，**不是"缺批量合并"**：600k 档 1000 把随机键落在
35 290 张叶子上，能合并掉的期望只有 `1000·999/(2·35290) ≈ 14` 张 ⇒ 1.4%。这种"看着像优化"的
东西要先算一遍再决定做不做。

**M31 —— 一次未命中为什么值 30~50 µs**：先被自己的测量否掉两个猜测（`node_cache.retain`
每读只走 1.1~2.1 个条目；页/次 = 1.03 ⇒ 上层页确实钉住了，一次点读就缺 1 页叶子），然后
用 `bplus_miss_price_probe` 绕过引擎对同一批页裸测，砍掉两笔：

| 一笔未命中的构成（µs/页，同一轮配对） | 旧 | 新 |
|---|---|---|
| 定位读一页（内含一次 `File::metadata()`，只为判 EOF） | 15~17 | **7.1~7.5**（+多问一次 len 则 11.0~12.3 ⇒ 那一问 ≈4.4 µs） |
| 表驱动逐字节 CRC32（4 KiB） | 11.7~12.0 | **2.7~2.8（4.3×，slice-by-8）** |
| `unpack_page`（校验 + 载荷再复制一份） | 11.0~17.8 | 4.6 |

CRC 是 WAL 帧 / SST 块 / blob / bplus 页**共用**的，所以三家读路径一起收这一刀（同轮 lsm
点读乱序 76.5 → 41.6 µs/次）。守卫四条：`test_read_page_does_not_ask_file_length`
（读一页时 `len()` 必须被调 0 次，同条用例主动问一次证明尺子会响 —— 把 `len()` 放回去 ⇒ 红）、
`test_read_page_tail_shapes_match_the_old_length_guarded_form`（整页在内/尾部半页/整页在外
三种形状的结果逐字节不变 —— 改成"到 EOF 就报错" ⇒ 红）、
`crc32_slice_by_8_matches_bytewise_everywhere`（8 种长度形状 × 3 种内容 × 已知向量 ×
单字节翻转；**校验和算错不是慢，是每页每块每帧都判成坏数据**）、台架里"交错取最小不许更慢"。
⚠️ 上面表里的 48.7 → 27.5 µs/次这类**单轮读数只当量级**：同一轮里没被这条碰过的 bitcask
灌行 28.8 µs/行正好落在噪声带 19.2~28.2 里。因果凭据只有交错取最小那组。

**M32 —— 上限这一档的决定权交给调用方，覆盖率要能算出来**：M22/M23 给两家缓存加了自动档，
但**顶是写死的**（bplus 2048 页 ≈ 8 MiB、lsm 1024 块 ≈ 7.1 MB）。600k 档实测覆盖率只剩 5.6%
⇒ 每次点读多付一次"读页 + 重解析"。抬顶 = 把库复制进 RAM，这个决定归下游，不归库默认，所以
现在它是旋钮：`KvEngine::set_cache_auto_max` / `cache_auto_max`（bplus + lsm）、
`EngineConfig.cache_auto_max`（打开时就带上）；**bitcask / memory 两条路都当场 `Unsupported`**
—— 分支标错不会编译失败，只会让调用方拿到 `Ok(())` 而其实没人听，所以门面那条守卫
（`cache_auto_max_knob_routes_to_two_engines_and_is_refused_by_the_other_two`）把"能的读回新值、
不能的报错"钉住。`tuning()` 同时给**分子与分母**：只报"生效容量 1024"没人分得清是撞上顶还是
库就这么小（bplus `page_cache_auto_max` + `total_pages`，lsm `block_cache_auto_max` + `sst_blocks`）。

| 覆盖率这条账（计数，同一轮实测） | 数 |
|---|---|
| bplus 1300 把长键 = 2606 页，默认顶 2048 | 冷启一趟落盘 2605 ｜ **焐热一趟后再走仍落盘 1621**（= 页缓存未命中 1621，一次未命中必然落盘一次）⇒ 顶真的绑住了 |
| 同一份库抬到不设顶 | 生效容量 2606 = 页数；焐热一趟后再走 **落盘 0 / 未命中 0**；住着 1986 页 ⇒ 抬前抬后**同一个数**（没重建） |
| 为什么 2048 的顶在这份库里只剩 1024 页给叶子 | 钉住的上层页预算 = 容量的一半（`min(1024, cap/2)`），长键库里上层就占掉 1024 页 ⇒ 1300 片叶子争 1024 槽 |
| lsm 26 块 / 封顶 8（同形状的引擎侧守卫） | 焐热一趟后一轮缺 **26** 块 ⇒ 抬到不设顶后重读缺 **0** |
| 价签（抬到不设顶的常驻） | bplus 200k 行 12 129 页 ≈ **47 MB**、600k 档 ≈ **143 MB**；lsm 600k 档全驻留 ≈ **18.7 MB**（默认顶 7.1 MB） |

守卫三条 + 变异两批（都真跑过）：「只记值、不落到生效容量」⇒ bplus 红在 `cap1 == total`
（left 2048 / right 2606）、lsm 红在 `cap_after == blocks`（left 8 / right 26）；「把改容量写成
重建缓存」⇒ 红在"住着几页"（left 0 / right 1986 与 0 / 8）。⚠️ **这两种变异门面那条守卫都抓不到**
（它只核对读回来的值）⇒ 效应必须留在引擎里，这就是"每一层都要有自己的守卫"的实证。

⚠️ 本条最贵的东西不是代码，是**一条会冻住的守卫**：为了在一行里同时打印"住着几页 + 钉住几页"，
写成 `mvcc.kv.lock().unwrap().a()` 与 `mvcc.kv.lock().unwrap().b()` 作**同一条语句的两个参数** ——
第一个 `MutexGuard` 临时量要到语句结束才释放，第二次 `lock()` 就是**自死锁**（`std::sync::Mutex`
不可重入）。现象是进程活着、什么都不打印，我连续两轮把它读成"这个形状的点读慢了几十倍"，
直到用 `Get-Process` 隔 6 s 采两次 CPU 发现**完全不涨**才换方向。规矩写下来：
**CPU 不涨 = 阻塞，CPU 在涨 = 计算** —— 这一步在"先量基线再动手"之前。改成一次锁取两个数之后
整条守卫 12.7 s 跑完（判据一条没少）。

**M33 —— 压实的输入改流式源：一次压实的瞬时内存 156.8 MB → 6.3 MB（省 25.0×）**

M30 把"压实慢"的账收到 `estimated_bytes()` 之后，剩下的那句"输入是整篇物化"降级成**内存账**，
当时挂在待办里。这一轮把它量细了（同一轮里两臂对照，release，600k 行 / 8 个输入文件）：

| 段（单位：ms / MB） | 旧形状（`iter_entries()` 整篇读进 `Vec`） | 新形状（每源一路 `iter_all()`） |
|---|---|---|
| 物化输入 | 366.9 ms ｜ 峰值涨 **154.0 MB** | 不做这一步 |
| 归并 + 写盘 | 1471.8 ms ｜ 这一段再涨 2.9 MB | 1888.4 ms ｜ 峰值涨 **6.3 MB** |
| **整段** | **1838.7 ms ｜ 156.8 MB** | **1888.4 ms ｜ 6.3 MB** |

输入条目本身只有 61.7 MB，物化却涨到 154.0 MB ⇒ **每行摊 269 B 而条目只有 108 B**，
多出来的是每行两个堆分配（键 `Vec` + 值）加 `Vec` 扩容翻倍。下游那台 1 核 6 GB、RSS 预算 80 MB
的机器上，**一次压实就把预算吃穿一倍** —— 这才是"内存高"里最该先修的一条。时间持平
（+2.7%，本机噪声带内；判据是内存计数不是墙钟）。

改法是 `merge_streaming` / `merge_to_ssts` 改收 `Vec<MergeInput>`（一路一个装箱迭代器），
压实与**层内重叠修复**两个调用点都换成 `sst.iter_all()`（它本来就是分块惰性读的）。
两臂的输出字节与文件数被断言钉成**必须相同** —— 否则"省了内存"完全可能只是"少写了东西"。

守卫两条，各自管一个调用点（同一个共用件改一处两家受益，但每个调用点要有自己的尺子）：
`test_compaction_does_not_materialize_its_inputs`（新）与 `test_compaction_repairs_level_overlap`
里加的那条断言，尺子 `merge_inputs_materialized()`，**单位 = 条目数**（不是调用次数 ——
`iter_entries()` 调一次就能搬进几万行）。两处都做了变异证明：把调用点退回物化版，
分别红在 `压实把 80 条输入整篇物化进了内存`（left 80 / right 0）与
`重叠修复把 61 条…`（left 61 / right 0）。
⚠️ 新那条守卫**第一次跑就红在我自己身上**：我用 `compact_once()` 触发压实，而它自己看 L0 阈值
（L0 只有 2 个文件、阈值 4）⇒ 一轮都没压，"物化 0 条"是空转出来的。加了形状断言
（`rounds >= 1 && after.l0_files < before.l0_files`）才逼出 `compact_l0_until(0, 8)`。

**M34 —— bplus 写路径：下降改走解析缓存 + 写完的页直接回填（裸 insert 单价 2.4×，页读/行 2.94 → 0.00）**

触发它的是复测表那格 **bplus 事务灌行 114.2 µs/行**（bitcask 16.3 / lsm 25.2 / memory 4.3），
以及 `vacuum()` 全库 12.27 s —— 两条都指向树自己的写路径。归因台架 `bplus_write_cost_attribution`
（五臂，同一份构建，键 40 B / 值 47 B）第一次读回来的不是耗时而是**计数**，而计数把故事改了：

| 读数（每行） | 修前 | 修后（20k / 200k） |
|---|---|---|
| A 裸 insert 页读 | 2.94 / 3.80 | **0.00 / 0.00** |
| A 裸 insert 整节点解析 | **0.00**（尺子看不见，见下） | 0.00（全命中） |
| A 裸 insert（µs/行，跨构建只作趋势） | 38.1 / 63.3 | **16.6 / 29.6** |
| C 完整事务（µs/行） | 67.5 / 126.0 | 60.9 / **40.7** |
| 缺页 / 换出（200k） | 0.00 / — | 0.00–0.01 / **0.05（抬顶臂同样 0.05）** |

⚠️ **两次归因被自己的台架推翻**（这是本条最该记住的部分）：
1. 我先点名"每行把整页条目重新问一遍 bincode"（`vec_item_bytes` = 每条 2 次 `serialized_size`）
   和"下降线性扫还每把键克隆一次"。两处都改了（字节账换成**探针常量 + 每条两次加法**；
   定位换成 `child_index` 二分），改完 A 是 **39.7** —— 与修前 38.1 同一档。
   **结构性嫌疑 ≠ 时间元凶**，这是 M30 那个教训的第三次。
2. 于是我加了"抬顶"那一臂（E：同形状但 `set_cache_auto_max(usize::MAX)`）去验"是不是脏页换出
   在收费"。200k 档 **E 比 A 还慢 5.0 µs，且两臂换出次数都是 0.05/行** ⇒ 换出不是这一笔，
   猜想作废。
3. 真正把 A 砍掉 2.4× 的是那条**一直存在但没有任何尺子能看见**的账：写侧下降
   （`insert_recursive`）走的是裸 `read_page` + `bincode::deserialize`，**完全绕过 M18 之后的
   解析缓存** —— 所以"解析 0.00 次/行"不是"没解析"，是"解析发生在尺子之外"。

改法三件：① `insert_recursive` 的下降改走 `node_of`（解析缓存）；② **内部节点只在真要改那一层时
才克隆**键与子数组 —— 顺序灌行时下面 40 次分裂才有 1 次动到父层，另外 39 次连一次堆分配都不必花；
③ `insert` 写完一页后把**手里那份内容**塞回解析缓存（`cache_written`，写成功才回填；搬进 `Arc`
不要深拷，所以这一步是免费的）。另外 `row_fits` 改成借用版字节公式，不再为量一次尺寸把键与值
各克隆一份。磁盘格式没动。

⚠️ **同一刀砍到删除路径上，被复测打回了**：`read_node`（删除/重组那条路，`vacuum()` 同源）我本来
也一起改成了走 `node_of`，三档复测里 600k 档 `bplus vacuum() 全库` 从 12.27 s 变 **15.12 s**，
而同一天没被碰过的 bitcask 那条反而 5.79 → 4.52 s（噪声朝反方向）⇒ 比值 2.12× → 3.34× 是这一处
带来的，**已退回**裸 `read_page` + 解析，理由写进 `read_node` 的注释：走查形状的页每页基本只碰一次，
回填解析缓存换不到下一次命中，只多付 `Arc` 分配、HashMap 插入与装满之后的整表 `retain` ——
和 `PageManager::read_page_for_walk` 当年"装得下才回填"是同一个理由。**插入那条路值得走缓存是因为
同一页会被连续几百行反复用**；这两句话不一样，照抄就亏了。

守卫两条 + 变异两批（都真跑过）：
`test_sequential_insert_descends_without_touching_pages`（尺子 = `page_reads()`，判据"平均摸页
< 0.1 次/行"）与 `test_child_location_is_binary_not_linear`（尺子 = 单次定位最坏比较几把键）。
变异：删掉回填 ⇒ **1.051 页读/行**，红；把 `child_index` 退回线性扫 ⇒ **最坏一次 63 把**（界 8），红。
⚠️ 第二条判据**第一版是错的**：我拿"平均比较次数 ≤ 2·log2(order)=12"当界，退回线性扫实测只把平均
推到 **11.0** ⇒ 变异没被抓住。平均被树形摊薄（根很窄），改成"最坏那一次"才是线性扫真正的形状。
第一条判据也差点写错：如果尺子选"解析次数"，那么"退回裸 `read_page`"会让读数变成 **0**（更省的样子），
必须选"页读次数"——**换尺子时先问一遍"这个回归会让它变大还是消失"**。

剩下的账（如实，未修）：A 仍 16.6 / 29.6 µs/行，那是叶子那一页**每行都要**深拷一次条目 + 序列化
一次 + 4 KB 页拷贝；`B − A` = 31.1 / 36.1 µs/行 是 MVCC 每行的账（`latest_version` 一趟下降 +
版本键编码 + 值编码 + WAL 追加 + 写集登记），`C − B` 在本机两档之间正负漂 ⇒ 每事务固定开销
（D ≈ 4–10 µs/事务）摊到 1000 行/事务后可以忽略。

### 本轮全量复测（三档 20k / 200k / 600k × 四引擎 × 31 项，同一份构建，2026-10-19）

判据：**单价倍数 = 原始比值 ÷ 期望量级**；`memory` 引擎那一列就是本机噪声地板（1.2~1.6×）。
下表只列**过地板**或**绝对值本身痛**的项（单位写全）。数据在 `C:` SSD 上量，换盘要重标。

| 项（600k 档） | 实测 | 单价比 20k→600k | 判读 |
|---|---|---|---|
| ✅ LSM 一次压实的瞬时内存 | 旧 156.8 MB → **新 6.3 MB** | — | **已修(M33)**，见上一段 |
| ✅ bplus 事务灌行 | 改前 **114.2 µs/行**（bitcask 16.3 / lsm 25.2 / memory 4.3）｜ M34 之后同一形状两轮 **37.6 / 51.4 µs/行**（同两轮 bitcask 16.0 / 15.0） | 1.49× | **已修(M34)**：对 bitcask 的倍数从 **7.0× 降到 2.4~3.4×**（本机跨轮漂 ±1.4×，所以给两档数）；凭据是计数 —— 裸 insert 的页读/行 **2.94 → 0.00**、整节点解析/行 → 全命中。归因与**两条被推翻的猜想**见上面 M34 段 |
| ⚠️ bplus `vacuum()` 全库 | 改前 **12.27 s/次**（bitcask 5.79 / lsm 2.95 / memory 0.99）｜ M34 之后 **10.64 s**（同期 bitcask 5.82） | 2.12×（地板 1.16×）→ **1.83×** | ~~与上一条同源，一起归因~~ **M34 证明不同源**：写侧那三刀（下降走解析缓存 + 回填 + 只在真要改时克隆）**不给走查形状用** —— 把删除路径的 `read_node` 也改成走解析缓存，这一格反而 12.27 → **15.12 s**（同构建那一轮），退回后 10.64 s。剩下的每行常数在删除路径自己的字节账/重组上，**未修** |
| ⚠️ `key_count 2B前缀` | bitcask 2.48 s / lsm 1.86 s / bplus 1.20 s / **memory 1.65 s** | ≈线性 | **每行 2.75 µs 而同库 `scan` 只 1.5 µs** ⇒ 数键比扫全库还贵，是共用折叠件里"每行都解码/克隆"的常数，不是复杂度 |
| ⚠️ `search 首页`(要精确 total) | bitcask 2.81 s / lsm 1.92 s / bplus 1.45 s / memory 0.99 s | ≈线性 | 与上一条同一个常数（`免 total` 只要 206~505 µs ⇒ 差 4 个数量级全在"数 total"那一趟） |
| ⚠️ bitcask `vacuum()` 全库瞬时 | 5.79 s ｜ 峰值 **+89.4 MB** | 2.08× | 内存高：M27 流式化只治了 `scan`/`export`，全库 vacuum 的候选收集仍是整批 |
| ✅ 点读类（已知预算题） | lsm 乱序 46.2 µs/次、bplus 乱序 33.6 →（M34 后）**23.4 / 23.9 µs/次** | 覆盖率决定 | **决定权已在 M32 交出**（`set_cache_auto_max`），默认档不动。bplus 这一格顺带便宜了：灌行时回填进解析缓存的那份，点读也用得上 |
| ✅ 平的那几条 | bitcask 灌行 16.3 µs/行（1.28×）、四家 `一次全量 scan` 1.8~4.3 µs/行、`窗口 scan(1%)` 与 `reverse_scan` ≤2.3×、bulk 2.0~5.2 µs/行 | ≤地板 | 别去动 |

按"多痛"排出来的后续：~~**M34 = bplus 写路径与全库 vacuum 的每行常数**（一条根可能同拖两条）~~
**M34 已做，且"一条根同拖两条"这句被推翻**：写路径那一格便宜了 2~3 倍（灌行 114.2 → 37.6/51.4
µs/行），全库 vacuum **不同源** —— 把删除路径也照抄成"走解析缓存 + 回填"反而更慢（12.27 → 15.12 s），
退回后 10.64 s；走查形状的页不值得回填（与 `read_page_for_walk` 同一条理由）。
**M35 = 共用折叠件的每行常数**（`key_count` / `search 首页(要 total)` / `health()` 同源；
根因已定位：四家的 `key_count` 都是同一份 `prefix_scan(prefix).len()` —— 为数一个数把界内每行
的键与值都物化出来再丢掉。本轮复测 memory 仍是 767.0 ms / 600k 行，而它自己一次全量 `scan`
只 1.3 µs/行 ⇒ 数键比扫全库贵一倍），
**M36 = bitcask 全库 vacuum 的 89 MB 峰值**（本轮复测仍 **+89.4 MB**，未动）。

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
| `vacuum_bounded` 一片 2048 行（µs/次） 四家 | 2 809~8 603 | 3 536~9 299 | ≈1× | ~~**平 —— 有界回收这条是干净的**（长跑该用它的依据）~~ ⚠️ **2026-10-18 三档复测推翻，见上面"三档复测"那节**：bplus / bitcask / memory 三家仍然 ≈1×，**lsm 到 600k 涨到 15.7~30.1 ms（5.8~11×）**。原判断只做了 20k↔200k 两档 —— 两档比不出阈值。**已修(M29)**：600k 复跑 4 044.7 µs ⇒ 对 20k 只剩 1.49×，那句依据重新成立，但这回是三档 + 计数守卫凭的 |
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
# M25 的 WAL 单价台架：20000 笔"只改一把键"的事务，报每笔多少字节、几条记录（µs 不算，算记录）
WAL_COST_TXNS=20000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 wal_cost_per_single_write_txn
# 写侧归因台架：报块读 / SST 数 / memtable 字节 / begin·set·commit 三段耗时 / 裸 insert 单价
LSM_FILL_ROWS=200000 LSM_FILL_CHUNK=20000 cargo test --release --lib -- --ignored --nocapture \
    --test-threads=1 lsm_fill_attribution
# M28 的重开剖面：手拆 LSM 的 open，九段各自计时 + 计数器（WAL 字节/记录数/redo 条数/层文件数），
# 同一轮里给「flush 无条件 vs 有条件」两份数 ⇒ 不用跨构建比墙钟
REOPEN_PROBE_ROWS=600000 cargo test --release --lib -- --ignored --nocapture \
    --test-threads=1 reopen_shape_probe
# M29 的一片读放大：把 SST 文件数当自变量，数一片真的折过多少把编码键
# （计数与构建档位无关，debug 也能量 —— 墙钟才是 release 的活）
LSM_VAC_ROWS=20000 LSM_VAC_FILES=24 cargo test --release --lib -- --ignored --nocapture \
    --test-threads=1 vacuum_slice_fold_attribution
# M30 的压实耗时拆分：把「物化输入 / 纯归并 / 归并+写盘」三段分别计时 ⇒ 结构嫌疑不等于时间元凶
LSM_COMPACT_ROWS=600000 cargo test --release --lib -- --ignored --nocapture     --test-threads=1 compaction_cost_split
# M31 的一次未命中单价：绕过引擎对同一批页裸测（CRC 那一段用交错 5 轮取最小才可比）
BPLUS_PRICE_ROWS=600000 cargo test --release --lib -- --ignored --nocapture     --test-threads=1 bplus_miss_price_probe
```

### WAL 与崩溃恢复

WAL 有 5 种记录:`Begin` / `Write` / `Commit` / `Abort` / `Checkpoint`,帧格式带 CRC32,损坏或不完整的尾部会被直接丢弃(模拟崩溃截断)。

⚠️ **`Begin` 从 M25 起写侧不再落**（读侧仍接受，0.4.0 建的库里那些老日志必须还能读）：它承载的信息是零 ——
恢复要的 `max_xid` 与"哪些行要 REDO/UNDO"全从 `Write`/`Commit`/`Abort` 取，而 `Write` 自己就把 `open_writes`
的条目建起来。M18d 实测把整步作废之后**全量测试一条都不红**，也就是说"事务开始在日志里有凭据"这句话
一直**没有守卫覆盖**。留着一条零信息记录的害处不是字节（虽然也值 28 B/笔，见下），是下一个人会当它是凭据。
两边各有一条守卫：写侧不再产生它（三家各自数 `begins == 0`），读侧兼容它（`plan_is_identical_with_or_without_begin`
—— 同一条事务流带/不带 `Begin` 折出的 REDO·UNDO 必须一字不差；唯一合法的差是那笔"只留 Begin 就崩了"的
空事务把 `max_xid` 抬一格）。实测代价（bitcask，20000 笔"只改一把键"的事务，release）：
**每笔 118.4 B / 3.00 条记录 → 90.4 B / 2.00 条记录**（−23.6% 字节、−33% 记录）；跑
`WAL_COST_TXNS=20000 … wal_cost_per_single_write_txn` 复现。

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
| **MVCC 档**（`LSMTreeOptions::for_mvcc`，与上面裸 LSM 默认值**不同**）| mem 32 MB / 262144 条、L0 8/12/32、目标 SST 2 MB、块缓存 256 **地板** + 自动档上限 1024（生效值读 `tuning()` 的 `block_cache_capacity`）、`enable_mem_wal=false`、`enable_bg_compact=false` | LSM |
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
- ~~块缓存容量写死 256 槽 ⇒ 随机点读单价随库长~~ **已修(M23)**:新增 `block_cache_auto_max`,
  生效容量 = `clamp(库的 SST 块数, block_cache_blocks, 上限)`,在 flush / compact / bulk ingest /
  打开四处重算(不重建缓存,只改上限),`for_mvcc` 档默认 256 地板 / 1024 上限。
  归因是**同构建、同库目录、只换这一档**的 A/B:5002 键那一档乱序点读 **27.9 → 8.5 µs/次**
  (未命中/次 0.39 → 0.03,而块/次两边都是 **4.00** —— 省掉的不是访问次数,是访问里"真去读盘"那一步),
  下游那条"启动后只读 4 条"**57.5 → 11.5 µs/次**;40000 键(2681 块,装不下)117.3 → 90.6。
  200k 键(7941 块 / 54.8 MB)那一档只降到 209.0(未命中 3.42 → 2.53,覆盖率 12.9%)——
  ⚠️ **这一档救不了"库远大于预算"**,同一形状的冷启动读 4 条倒是从 **1027.5 → 289.5 µs/次**
  (与下游报的"单次点查 0.86 ms"同一格)。一次未命中的单价可以从表里直接除出来:
  40000 档 ≈ 54 µs、200k 档 ≈ 94 µs(散在 5 篇 L0 里,跨文件更贵)。
  ⚠️ 上限 1024 是**预算**不是正确性:同一档把上限放到 4096(= 全库驻留 ≈ 18.7 MB)实测 21.0 µs/次,
  残差 ≈ 未命中次数 × 一次随机读(这台上 ≈ 54 µs) ⇒ 要不要把库复制进 RAM 该调用方决定(同 M22 口径)。
  ⚠️ 这条顺手抓到**自己引入**的一处回归:`BlockCache` 的 LRU 原先每次命中都 `order.iter().position()`
  ⇒ O(驻留块数),容量放大就跟着变贵(40000 键有序读 17.4 → **36.0** µs/次,而未命中次数一动不动),
  已改成定长槽 + 双向链表(命中/换出都 O(1),守卫 `lru_touch_cost_does_not_scale_with_residency` 实测
  每次命中固定检查 6 个槽位,驻留 64 与 1024 同价)。
- ~~`fix_level_overlaps` 发现层内 key range 重叠**只打印警告,不修复**~~ **已修(M26)**:改名
  `repair_level_overlaps`,把连续重叠的一段当成**同层归并**真折掉(分组取传递闭包,A∩B、B∩C 即使
  A∩C=∅ 也整段一起折,否则留下新重叠)。取胜判据从"源序优先"改成 **seq 优先**(见下条),因为
  **同层两个文件谁新谁旧没有元数据可依据**(L1→L2 压实会用新 id 写老数据)。实测代价:造出来的
  重叠层里 40 把键要点 **60** 次文件,折完同样 40 把键点 **40** 次(每键 1 次)。
  ⚠️ 正常压实**造不出**重叠(L0 整层并入、同层输出按键有序)——它是"先记 MANIFEST 再删旧文件"
  那两步之间崩掉的恢复产物;而这条改动之前的顺序恰恰相反(**先删后记**),那才是真窟窿:
  `load_manifest` 对不在盘的条目是 `path.exists()` 静默跳过 ⇒ 崩在窗口里 = manifest 指着已删的
  文件、新写出的文件没人记 = **静默丢数据**。现在 `compact_level` 自己先写 MANIFEST 再删,
  并且删除点前面长了探针(`test_compaction_repairs_level_overlap`;把顺序退回先删后记,
  当场红在"删旧 SST 之前 MANIFEST 里还找不到新文件 5")。
- **折叠的胜者判据必须是 seq,不是"源序号小=更新"(M26 实测,不是推理)**:同层重叠时旧判据会把
  **旧版**当胜者交出去 —— 守卫 `test_fold_agrees_with_point_read_on_overlapping_level` 在旧写法上
  红在 `折叠与点查不一致："k10" left: Some([111,108,100]) right: Some([110,101,119])`(即 `old` vs `new`)。
  重量折叠(`merge_streaming`)改好了;**轻量折叠那条本来就已经对** —— 它的胜者由 `LightMerge`
  里"待定键 + 取更大 seq"那一步决定,不靠堆的出队顺序(我照着改了一遍才发现改了等于没改,已退回;
  把待定那步摘掉才会红,实测证过)。⇒ 两条折叠路径判据同源但**担责的位置不同**,别照抄。
- ~~`try_open_with` 末尾**无条件** `tree.flush()`,而 `flush()` 末尾**无条件** `maybe_compact()`
  ⇒ 干净重启也会自己做一次全层压实~~ **已修(M28)**:改成**只在有 redo 要重放时**才 flush。
  凭据是同构建同规模的台架 A/B:`lsm 再冷重开` 600k 档 **17 487 030.8 → 9 106.7 µs**(1921×)、
  重开常驻 12.1 → **1.9 MB**;机制是 bulk 的 `finish()` 把 L0 正好留在阈值(8)上,而 200k 只有 4 个
  文件不越线 —— **两档比不出阈值,这条就是被"只比两档"藏住的**。守卫两条(尺子=层文件数,不用墙钟),
  两次变异各自证红,见上面"三档复测"那节。
  ⚠️ **没消失,只是不再由"打开库"付**:带 redo 的崩溃恢复仍然必须 flush —— 那一步原先 12~17 s,
  **M30 查明元凶是压实里"每行重求和整表"的尺寸判定(不是下条那个整篇物化)**,修完同档 3.55 s;
  干净重启后第一次写/`checkpoint` 会把这笔压实付掉。改的是重启等待,不是总写放大。
  ⚠️ bitcask / bplus 两家 open 里那两处 flush **故意没跟着改**:量下来它们的 flush 是有界的
  (一次 fsync / 顺带把 `next_version` 落 meta 页),给它们加条件既没有可证的收益、也造不出
  不空转的断言 —— 两处的代码注释里写明了"若将来长出随库涨的活,照哪把尺子改"。
- ~~**`vacuum_bounded` 一片的每行单价随库涨(M29,未修)**~~ **已修(M29)**:600k 档那片扫 2046 行、
  裁 0 行却花 15.7~30.1 ms ⇒ **14.7 µs/行**,是同档全量 scan(4.9 µs/行)的 3 倍,而 README 早前那句
  "有界回收这条是干净的(长跑该用它的依据)"就是被它推翻的。归因用计数尺(`light_visits` 数一片
  **折过多少把编码键**):答案是"折得比交出多得多",但不是游标没下推 —— 是**每个源各取满 `预算` 行**
  再归并(3 源折 6096、9 源 18384、24 源 22000 = 整库),而源数 ∝ 库大小。改走惰性 `light_iter_range`
  后 600k 那片 **4 044.7 µs**(对 20k 只剩 1.49×,底噪内)、折/交 = 2049/2048 = **1.0×**。
  ⚠️ 连带删掉了旧实现里"窗口太窄就放大 4 倍重试"那套补偿(惰性流不会切在半组上,那个情形结构上
  不存在),以及 `scan_latest_light_window` / `light_view_break_from` 两个 pub 入口 —— **对 0.4.0 是
  破坏性变更**,已在 CHANGELOG 列出。
- **压实的尺寸判定曾按行走整表(M30 已修)**:`merge_to_ssts` 每交一行问一次
  `b.estimated_bytes() >= target`,旧实现是 `entries.iter().map(...).sum()` ⇒ 每个输出文件
  O(表内行数²)(一张 2 MiB 表 ≈19 000 行 ⇒ 每输入行摊到 30 µs)。改成在 `add` 里增量维护的账之后,
  同档"归并+写盘" **18 018.5 → 3 113.6 ms**、带 redo 的重开 **14.43 s → 3.55 s**。
  这是 M19("flush 判定每插一行数一遍 memtable")同一个病的第二次出现,这次在 SST 写侧 ——
  **同类病灶要按"每行都问一次的判定"这个形状去扫,不是按"memtable"这个名字去扫。**
- ~~**压实与修复的输入是整篇物化**(这是**内存**账,M30 之后仍然挂着)~~ **已修(M33)**：`merge_to_ssts` 与
  `merge_streaming` 现在收 `Vec<MergeInput>`（一路一个迭代器），压实/修复的输入走 `SstFile::iter_all()`
  一块一块读。改前实测（600k 行 / 8 个输入文件 / 条目本身 61.7 MB）：**物化那一步峰值涨 154.0 MB**
  （每行摊 269 B，条目只有 108 B ⇒ 多出来的是每行两个堆分配 + `Vec` 扩容翻倍），整段瞬时 **156.8 MB**
  ⇒ 改后同一形状整段 **6.3 MB（省 25.0×）**，时间 1838.7 → 1888.4 ms（+2.7%，本机噪声带内 ⇒ 持平）。
  守卫两条（压实与重叠修复各一条，尺子 `merge_inputs_materialized()` 单位=条目数），
  两处各自退回物化版实测红在 80 条 / 61 条。
  ⚠️ **原来那条"约一个 SST"的说法是错的**（`sst.rs` 的注释只写对了一半：`SstBuilder` 侧确实流式，
  输入侧不是）。这条当时的判断是"内存账，不是时间账"，M33 按内存账收掉。
  ⚠️ **但这条不是"压实慢"的原因**：M30 的拆分实测(600k 行)是 物化输入 0.62 s / 纯归并 0.48 s /
  **归并+写盘 18.0 s(占 94%)** ⇒ 时间是上面那条 `estimated_bytes()` 给的,不是这句 `collect()` 给的。
  我原先把 12~17 s 归给它,那是错的,这里留着这条修订。
- compact 时每个 key 只保留最新一条,`enable_snapshot_gc` 并未真正按 snapshot 水位保留旧版本(源码注释已自陈)
- `blobs/` 旁路的回收挂在 `vacuum()` 的全库轮次上(compact 判不了"别的层里还有更新的版本引用同一个 blob");**有界轮次不动 blob**,引用关系只有全库才看得全
- `start_bg_compact` 只建控制块不起线程,真正的后台线程要显式调 `spawn_bg_compactor`

**Bitcask**
- 数据记录**没有 CRC**,存在静默损坏风险(blob 和 WAL 有 CRC)
- merge **不是原子的**(删旧文件与 rename 新文件之间有时间窗),但**可恢复**:崩在改名之前留下的 `data.merge` 会在下次 open 被认回(见 `durability_profile().merge_artifact_recovery`)。改名顺序不能反过来 —— 旧 active 句柄还挂着时 Windows 的共享锁会挡住覆盖
- 全量 key 常驻内存,key 数量受内存限制

**B+ 树**

- ~~**一次随机点读的单价里,有一页未命中是预算题(M31 之后仍然挂着)~~ **决定权已交出(M32)**:M31 已经把这一页
  从 ~15-19 µs 砍到 ~7 µs 加解析(`read_page` 不再每次问 `File::metadata()`,CRC32 换
  slice-by-8),计数实测**一次点读就是缺 1 页(叶子)**、上层页确实被钉住了(页/次 = 1.03、
  命中/次 = 0.024)。剩下那 1 次是自动档上限(实测 600k 档覆盖率 5.6%)决定的 ⇒
  `set_cache_auto_max(usize::MAX)` 能把它换掉,实测抬顶后一轮点读落盘 0 次。
  ⚠️ **仍然挂着的两点**(M32 没碰,因为它不该碰):(a) **默认值没变** —— 顶还是 2048 页 / 1024 块,
  基座不给下游做"把库复制进 RAM"的决定;(b) **长键库里钉住的上层页要吃掉容量的一半**
  (`pin_limit = min(1024, cap/2)`)⇒ 抬顶之前叶子拿到的不是整个容量:实测 2048 顶下住着 1986 页,
  其中**钉住 1024 页是上层**,留给叶子的只有 962 页,而这份库有 1300 片叶子 ⇒ 一轮点读缺 1621 页。
  想让叶子也住满得把 `PIN_BUDGET_PAGES` 变成可配,没做。
- ⚠️ **`multi_get` 不需要"按键排序合并同页"**(M31 算过,别去做):600k 档 1000 把随机键落在
  35 290 张叶子上,能合并的期望只有 `1000·999/(2·35290) ≈ 14` 张 = 1.4%。它的 11.34× 与逐键
  点读同源,不是缺批量。

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
- ~~⚠️ **页数会超过页缓存上限，而没有自动档**~~ **已修(M22)**：`max_cache_pages` 现在支持 `0` = 自动档
  （也是 `EngineConfig::default()` 给的值）：容量按库的实际页数给，夹在 `[1024, 2048]` 页
  （≈ 4~8 MiB 常驻），并在每次刷盘（`flush` / `checkpoint` / bulk 收尾）之后重新评估 ——
  只在 open 时定一次容量等于"导入完就再也没调过旋钮"。夹上限的理由要说白：B+ 树把整库缓存住的
  常驻 ≈ 数据文件本身（实测 200k 行 = 12129 页 ≈ 47 MB），那不是"缓存一点热数据"而是"把库再
  复制一份到内存"，这种决定该由调用方做（显式传一个数），不该由库替他做。生效值从 `tuning()`
  读回（`max_cache_pages` / `cache_pinned_pages` / `cache_auto`）。
  同时加的第二件事：**根与内部节点钉住，不参与换出**（预算 = min(1024 页, 容量 ÷ 2)）。
  归因台架量的就是这笔：200k 行的库、同一个 1024 页容量下，一次随机点读原本要走
  **1.41 页/读**（下降路上那几层被 LRU 挤掉后重新读盘 + 重新解析），钉住之后是 **0.98 页/读**
  —— 剩下的那 1 页就是这次读真正要的那片叶子，它本来就该付。
  ⚠️ 两笔账都只有**计数**能看：同一配置连跑两遍墙钟差 30%+（本机 ±2× 之外，D 盘是 5400 转机械盘），
  所以这里不报 µs 的改善，只报页数与覆盖率。
  ⚠️ 覆盖率本身仍然受上限约束：200k 行 12129 页 ⇒ 自动档给出 2048 页 ≈ 16.9%，
  要"走查全命中"仍然得显式把容量配到 ≥ 页数（实测 65536 页时 0.06 页/读）。
  还有一笔留在待办：**上层占全库多少不是常数** —— 35 B 键时 3.0%，长键（3.3 KB）时能到 50%
  （内部节点也装不下几个长键 ⇒ 扇出掉到 2、树深 3 层涨到 12 层），所以钉住预算必须跟容量挂钩，
  守卫里 `pinned × 2 ≤ 容量` 钉的就是这条底线。
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
  ⚠️ **M25 更进一步：`Begin` 整条记录不再落了**（`WalPolicy::open_on_first_write` → `mark_opened`，只记账不写记录）。
  上面那句"少一条 `Begin` 不影响恢复"当时是**推理**，现在它有守卫了（`plan_is_identical_with_or_without_begin`）
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
cargo test                    # 全部:300 单元 + 4 集成 + 5 文档测试
cargo test --lib engine::     # 只跑门面层(32 个,含大库基线台架;四引擎全覆盖)
cargo test --lib cache_auto_max   # 缓存自动档"顶"这一档的三条守卫(门面路由 + bplus 覆盖率 + lsm 覆盖率)
                              # 判据全是计数:bplus 那条量在**磁盘层**(一轮点读落盘几次),
                              # lsm 那条量块缓存未命中 —— 两条都要求"抬顶之后再走一趟 = 0",
                              # 并且各自跑过"只记值不生效"与"重建缓存"两种变异证红
SCALE_ROWS=20000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 scale_baseline
                              # 四引擎 × 每种操作的大库基线(µs/次 + 常驻/峰值 MB)
LSM_POINT_ROWS=5002 cargo test --release --lib -- --ignored --nocapture --test-threads=1 point_read_baseline
LSM_CACHE_AUTO=0 LSM_POINT_ROWS=5002 cargo test --release --lib -- --ignored --nocapture \
  --test-threads=1 point_read_baseline
                              # LSM 点读台架:换规模(`LSM_POINT_ROWS`)、换块缓存自动档
                              # (`LSM_CACHE_AUTO=0` = 关掉,回到钉死 256 槽)做**同构建 A/B** ——
                              # 结论取计数(块/次、未命中/次、探文件/次、条目比较/次),不取墙钟
cargo test --test test_lsm_tree
```

门面层的测试是**四个引擎跑同一套断言**的,覆盖增删查、批量与扫描、写冲突、bulk 导入、TTL 与元数据、vacuum 与 flush、`health()` 读数一致、自动回收默认开着且能被关掉 —— 这也是验证「四引擎行为一致」的地方。换引擎只改变涨速,不变的是要盯的那些指标,所以这一层必须按「四家同尺度」断言。

集成测试会在 `./data/<引擎名>/` 下建演示库,不污染仓库根目录。

---

## 开发环境

- Rust 1.92+(edition 2024)
- 跨平台:Windows / Linux / macOS。文件锁在 Windows 用 `share_mode(0)`,Unix 用 `flock`

release profile 开了 `lto = true` 和 `codegen-units = 1`,压测请务必用 `--release`。
