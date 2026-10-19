# storage-engines

四个从零手写的 KV 存储引擎 + 一层统一 trait 门面。**教学 / 学习性质**,不是生产级数据库。

同一套 MVCC 事务语义(快照隔离、写冲突检测、vacuum 回收)跑在四种截然不同的存储结构上,可以直观对比 **LSM Tree / B+ Tree / Bitcask / 纯内存** 在读写放大、范围扫描、崩溃恢复上的取舍。

- 约 2.3 万行 Rust,edition 2024(需要 rustc ≥ 1.85)
- 依赖只有 5 个:`serde` / `bincode` / `memmap2` / `fs4` / `log` —— B+ 树、SST、Bloom、WAL、双写缓冲、CRC32 全部自己实现
- 312 个单元测试 + 4 个集成测试
- 当前版本 **0.4.0**:随 uptime 恶化的七条通道逐条堵死,版本回收改为自动 —— 破坏性变更与升级要点见 [CHANGELOG.md](CHANGELOG.md)。⚠️ crates.io 上的 0.4.0 是 **2026-10-15** 发的那版,它**不含** 10-16 起的 **M10–M46**(LSM 点查常驻句柄、窗口 scan 界下推、bplus 读路径与丢页修复、配置旋钮、bulk/事务内核合并、`migrate`/`scan_page`、灌行 O(N²)、`seek_prev`、游标下推、页缓存与块缓存自动档、写集哈希、WAL `Begin` 不落盘、层内重叠真折+先记 MANIFEST 再删文件、全库折叠流式化、重开不再自带压实、有界回收一片改惰性归并、压实的尺寸判定改成增量账、一次页读少一次系统调用且 CRC 快 4.3 倍、缓存自动档的顶交给调用方且覆盖率分母可读、压实的输入改流式源(一次压实瞬时内存 156.8 → 6.3 MB)、bplus 写侧下降改走解析缓存且写完的页直接回填(裸 insert 单价 2.4×、页读/行 2.94 → 0.00)、四家 vacuum 的保留判定改成逐组流式(bplus 瞬时 +3.9 MB / memory +4.0 MB；bitcask 仍 +171.4 MB——先前那对 89.4/56.7 是坏量具读数，M38 已作废)、大库台架的内存量具修好(`timed()` 每段前 `reset_peak`，此前"峰值"列只看得见超过历史最高点的部分)、lsm health() 不再把每个源整篇物化(600k 档瞬时 126.1 → 0.1 MB)、bitcask vacuum 改按块走查(瞬时 171.4 → 115.7 MB，① 段 106.0 → 0.5 MB)、另外三家同一刀(lsm 101.2 → 45.5 / memory 4.0 → 0.3 / bplus 3.9 → 0.9 MB)、bitcask `merge()` 的走查也按块(④ 段 115.0 → 87.9 MB、外层 `vacuum()` 115.7 → 87.2 MB；剩下那 87.9 MB 是**替换用的新索引**本身，不是瞬时账)、lsm 的 flush 不再把整张 memtable 克隆进 Vec 再排序 + memtable 的 flush 判定改读真内存(600k 键×2 版那档 `vacuum()` 的 ① 段 158.6 → 33.1 MB)、`SstBuilder` 攒满一块就落盘(一次压实瞬时 6.3 → 3.1 MB、单文件握住条目 18333 → 64；输出的 SST 逐字节不变，有 golden 钉着)、`key_count` 与"要精确 total 的 `search_keys`"改成只数不物化(600k 档峰值 +123.7 MB → 0.6~1.2 MB；四家 `search_keys`/`scan_foreach` 的复制各合成一份、bitcask `merge()` 不再另建第二把索引而是重扫新日志(④ 段 87.9 → 12.6 MB，代价是多一段"重扫窗口"，里面能报错的门一律报错、导出不必再把整库摆进内存(新增流式出口 `export_latest_visible_for_each`，`export_latest_visible` 改成它的收集壳；20k 档一次导出的常驻 3.5~3.9 MB → **0.0 MB**；三个 `export_kv` 工具不再把全库装两遍、`JSONL` 导入不再静默丢数据(`b64_decode` 在非 3 倍数长度上掉 1~2 字节，事故形状是 11 字节的键全被截成同一把 ⇒ 互相覆盖：301 行进库只剩 32 把；端到端修到 301→301→301，、裸 LSM 引擎的"全库归并"把源也改成惰性(600k 档导出瞬时 100.4 → 0.4 MB、顺手快 1.53×，四家 `export_kv` 到这才算齐)bin 格式从来不受影响))—— 要那些得走 path/git 依赖,见[引入方式](#引入方式)
- 许可证:MIT 或 Apache-2.0 任选

---

## 引入方式

⚠️ **先看这条(2026-10-17 用 crates.io API 实测,不是凭印象)**:架上最新是 **0.4.0**
(发布于 2026-10-15,未 yank)。**这一版已经包含**七条"随 uptime 恶化"通道的修复与 M9 的门面
改造(`KvEngine: Send + Sync`、类型化选型、`EngineName`)⇒ 版本号依赖就能拿到。

但 0.4.0 那个发布件**不含 10-16 之后的 M10–M41**:LSM 点查的常驻文件句柄与稀疏索引二分
(下游实测单次点查 0.86 ms 那条就是它)、四引擎窗口/前缀 scan 的界下推、bitcask 懒分配写缓冲、
B+ 树 delete 路径的静默丢页修复 + 读路径三项、`EngineConfig` 的配置旋钮与 `tuning()`、四引擎
事务内核合成一份、`migrate` + `scan_page`、LSM 灌行 O(N²)、`seek_prev` 整段物化、游标未下推、
B+ 树页缓存与 LSM 块缓存的自动档、单事务写集查重 O(写集²)、WAL 里那条零信息的 `Begin`、LSM 层内重叠的真折与「先记 MANIFEST 再删文件」、全库折叠改流式、重开不再自己触发压实、有界回收一片改惰性归并、压实的尺寸判定改成增量账、页读路径与 CRC32 提速、**缓存自动档的顶可以由调用方抬（`set_cache_auto_max`）且覆盖率的分母可读**、**压实的输入改流式源（一次压实瞬时内存 156.8 MB → 6.3 MB）**、**四家 `vacuum()` 与 bitcask `merge()` 的走查改按块**（bitcask 那一家的整段瞬时 171.4 MB → 87.2 MB）、**大库台架的内存量具修好**（此前 `峰值` 列只看得见"超过历史最高点的部分"，排在后面的操作一律报 +0.0）、**lsm 的 `flush()` 改流式吐 memtable，且 memtable 的 flush 阈值改读真内存**（600k 键 × 2 版那档 `vacuum()` 的 ① 段 158.6 MB → **33.1 MB**；"mem 32 MB"这句从此才算数）、**`SstBuilder` 攒满一块就落盘**（一次压实的瞬时 6.3 MB → **3.1 MB**、单文件握住的未落盘条目 18 333 条 → **64 条**；输出的 SST **逐字节不变**，由 golden 钉着）、**`key_count` 与"要精确 total 的 `search_keys`"改成只数不物化**（600k 档瞬时内存 +123.7 MB → **0.6~1.2 MB**；四家 `search_keys` 与四家 `scan_foreach` 的复制各合成一份）。
磁盘格式这三十八批**一个都没动**,所以不是"换了就得重建库"——是同一份库,行为与代价不同。

```toml
# (a) 同机项目:path 依赖 —— 拿当前 master,含 M10–M46
storage-engines = { path = "../storage-engines" }

# (b) 走远端:git 依赖(gitee 与 github 内容一致)
storage-engines = { git = "https://github.com/chjgfg/storage-engines.git", branch = "master" }

# (c) crates.io —— 0.4.0:事故修复齐全,但没有 10-16 之后的读路径/配置改动
storage-engines = "0.4"
```

`cargo install storage-engines` 装到的同样只是已发布的 0.4.0。要 M10–M46 又想要版本号依赖,
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
//   ⚠️ M35 起这一趟**只数不物化**：代价 = 走一遍界内 + 每行读一次值（过期判据在 value 信封里），
//      不再"把界内命中全做成 `Vec` 再取 `len()`"。600k 档因此从瞬时 **+123.8 MB** 降到 **<1.2 MB**。
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

**M36 —— 四家 vacuum 的保留判定改成逐组流式（结构已改；内存那笔当时用坏量具量的，M38 已作废重开）**

那 89.4 MB 不在结果集里，也不在 blob，而在**四家各抄一遍的一处结构**：判"留哪一版"之前，
先把**全库**每一物理行归组攒进 `by_key: BTreeMap<Vec<u8>, Vec<(u64, Vec<u8>, bool, R)>>`
（每行两个 `Vec`：`raw_key` 与 `enc`）⇒ 636k 行 ≈ 90 MB，**比它要裁的东西大两个数量级**，
下游那台 1 核 6 GB / RSS 预算 80 MB 的机器一次 `vacuum()` 就顶穿。

改法：版本键按 `raw ‖ version_be` 升序到达 ⇒ **同一把键的行必然连续**，于是只攒"当前这一组"，
看到后继就把这一组交出去判。共用件因此新增 `common::vacuum_plan_group`（取走那一组、返回
该删/该留），而 `vacuum_plan` 改成"逐组调它"的批处理壳 —— **规则仍然只有一份**，四家不可能
各判各的；公开 API 没动，磁盘格式没动。

守卫一条走四家：`all_engines_vacuum_judges_per_group_not_whole_library`（`src/engine.rs`），
尺子是 thread_local 的**"判定发生了几次"与"最大一组有几行"**，判据 = 判定次数 ≥ 键数（800 把）
且最大一组 ≤ 每把键的版本数（2 行）。实测四家都是"800 次 / 最大 2 行"。
⚠️ **为什么这条不用 `mem_probe` 的 peak 当判据**：peak 是**进程级高水位**，`cargo test` 并行跑时
别家用例分配的字节会算进来 ⇒ 只能放在 `--test-threads=1` 的 `#[ignore]` 台架里读（M27/M33 一样）。
"内存降了"是**结果**，能钉住的是**结构**：不再攒全库，内存自然跟着降。
变异验证：把 bitcask 改成"换组时不判、收摊时一次判全部" ⇒ 红在
`bitcask: vacuum 把判定折叠成了 1 次（键有 800 把）`，其余三家照绿（各自独立）。

⚠️ **M36 当时报的"89.4 MB → 56.7 MB（1.58×）"作废** —— 那两个数都是**坏量具**读出来的：
`timed()` 取的是运行中的进程高水位（只增不减）⇒ 一段操作只有"超过历史最高点的那部分"才看得见，
段内真实涨幅被前面的灌行/`scan` 吃掉了。M38 把量具修好（每段之前 `reset_peak()`）之后，
同一档、同一形状重量：

| 600k 档一次 `vacuum()` 全库的瞬时峰值（修好的量具） | 读数 | 旧量具当时报的 |
|---|---|---|
| bplus | **+3.9 MB** | +0.0 MB（量不到） |
| memory | **+4.0 MB** | +0.0 MB（量不到） |
| lsm | **+101.2 MB** | +0.0 MB（量不到） |
| bitcask | **+171.4 MB**（这一段还含 `kv.merge()` 重写整库的工作集，不全是归组的账） | +56.7 MB |

⇒ **bplus / memory 两家的归组账确实没了**（它们不攒存活行清单，只剩"当前这一组 + `to_delete`"）；
bitcask / lsm 各还有一笔大的（存活行清单 + blob 重写 + bitcask 的 `merge()`）。

**M37 的先量：把一次 `vacuum()` 拆成四段各量一次**（`bitcask_vacuum_stage_split`，`#[ignore]`，
`VACUUM_STAGE_SPLIT=1 BITCASK_VAC_ROWS=600000`）—— 600k 键 × 2 版 = 120 万物理行那一档：

| 段 | 瞬时峰值 | 每扫描行摊 |
|---|---|---|
| ① 归组判定（走查 + `to_delete`/`keep` 两个累加器） | **106.0 MB** | 93 B |
| ② 删除 + `flush` + blob `sync` | 0.0 MB | 0 B |
| ③ blob GC（这一档没有大 value，全走内联） | **0.0 MB** | 0 B |
| ④ `kv.merge()` 重写主日志 | **115.0 MB** | 100 B |

⇒ **元凶不是 blob、也不是"当前那一组"**（那是 M36 已经消掉的东西），而是 `to_delete` + `keep`
这两个**跨全库的累加器**，外加 `merge()` 自己一笔同等大小的账。M30/M31 两次"最合理的猜想被
分段计时推翻"，所以这一刀先量再动。
⚠️ 分段标尺**必须门控**：`reset_peak()` 会把进程高水位**压低**，不设 `VACUUM_STAGE_SPLIT` 时
它必须是 no-op —— 否则外层大库台架对 `vacuum()` 的读数会从 171.4 MB 被锯成 47.2 MB（那不是改进，
是锯尺子）。关掉门控已复测：外层读数回到 **171.4 MB**，与加分段标尺之前一字不差。

**#49 补上了 —— M36 的净效果，同构建 A/B（两臂同一个二进制，只切一个环境变量）**

欠的那次 A/B 做了：把 bitcask 的走查**临时**改回 M36 之前的形状（整库归组 `BTreeMap<raw, Vec<行>>`
+ 整库 `to_delete`/`kept`、不分块），用门控分段标尺读 ① 段；另一臂就是今天的代码。
两臂是**同一个构建**（`VAC_OLD_SHAPE` 一个环境变量切换），所以不存在"跨构建比内存读数"这层含糊；
那段测量代码跑完就摘了（它不属于产品）。600k 键 × 2 版（120 万物理行、60 万行可删）：

| 形状 | ① 段瞬时 | 来源 |
|---|---|---|
| M36 之前：整库归组 + 整库两份清单 | **219.1 MB**（191 B/扫描行） | 本次 A/B 的 B 臂 |
| M36 之后 / M37 之前：逐组判定，两份清单还在 | 106.0 MB（93 B/扫描行） | 上面那张 M37 分段表（同一把修好的量具） |
| 今天：逐组 + 按块，清单塌成一块 | **0.5 MB** | 本次 A/B 的 A 臂 |

⇒ **M36 自己那刀省的是 219.1 → 106.0 = 113.1 MB（2.06×）**，M37 再省 106.0 → 0.5；两刀合起来
**438×**。结构说法一直没变：归组不再攒全库、清单不再攒全库。
⚠️ 曾经那句"89.4 → 56.7 MB（1.58×）"仍是**作废**的坏量具读数 —— 上面这组数是它的替代，
不是对它的小数点修正。

同一轮量具修好后**新冒出来的内存账**（旧量具全报 +0.0，看不见）：lsm `health()` **+126.1 MB**、
lsm 灌行 **+106.8 MB**、四家 `export` / `scan` / `migrate` 的整库物化 **+114.7~124.3 MB**。
> **修订（M44）**：这一句里 `migrate` 那一笔归错了。600k 档重测：`migrate → memory` 峰值 **+116.1 MB**，
> 而读侧每片只在收集门里攒到**页宽**（守卫量到 `scan_page` 那趟是 701 行 / 页宽 700）⇒
> 那 116 MB 是**目标 memory 库自己**装下 601k 行的常驻，不是搬家物化的中间账。
> `export` / `scan` 两笔则确实是「返回整库的 `Vec`」，而那份 `Vec` 就是调用方要的合同 ——
> M44 为此加了流式出口 `export_latest_visible_for_each`（见下面那一节）。
这些是 M38 之后的新排产依据，不是回归 —— 它们一直在那儿，只是量不到。

还剩下的那笔（如实，记在 M37）：全库轮次仍要把"存活行清单 `keep`"交给 blob GC，而
`blob.rewrite_in_place` 要一次握住全部存活大 value。挡住我的不是没想到，是**借用**：
`iter_meta_from` 按 `&self` 借住 KeyDir，走查途中没法再调 `get_entry`（那是 `&mut self`），
所以做不到"当场读出 blob 指针、当场丢掉这一项"。两条出路都要他点头：把 KeyDir 与文件句柄拆成
可分别借用的两个对象；或把 `vacuum()` 内部改成按片循环 —— 后者会动 `full_sweep` 的门控
（blob 重写与 merge 只在全库轮次做），那是语义决定，不是收尾清理。

耗时那一笔是干净的（与量具无关）：bitcask 600k `vacuum()` 5.82 → **4.04 s**，少了一趟全库归组。

**M39 —— lsm `health()` 不再把每个源整篇物化：瞬时 +126.1 MB → +0.1 MB（600k 档，同一把修好的量具）**

这条是 **M38 修好量具之后才看见的**：`health()` 是个"报统计"的调用，600k 档一次却瞬时涨
**126.1 MB**（旧量具报 `+0.0`，因为高水位早被前面的灌行/`scan` 抬到顶上了 —— 见上面 M38 那段）。

根因一句话：`health()` 走 `LSMTree::scan_latest_light`，而那份实现是**先把每个源整篇折进 `Vec`**
（memtable 用 `mem_view_light` 整表遍历、每个 SST 用 `SstFile::light_view` 整篇读完，**所有源同时活着**）
再交给跨源归并 ⇒ 峰值 = 「2 × 全库轻量视图」。归并本身早就有惰性版本（`light_iter_range`，
M15b/M29 那条路），只是这个调用没用它。

改法：① `health()` 改走 `light_iter_range(None, None, snap)`；② `scan_latest_light` 改成它的
**收集壳**（`.collect()`）—— 这样跨源取胜规则**只剩一份实现**，而公开签名没动；③ 死掉的
`mem_view_light` 删除；④ `SstFile::light_view` 上加一把计数尺 `light_view_materialized()`
（**单位 = 条目数**，与 M33 那把同形）。磁盘格式没动。

实测（600k 档、同一份构建、M38 修好的量具）：`health()` 瞬时 **+126.1 MB → +0.1 MB**，
耗时 742 → **461 ms/次**。

守卫 `test_health_does_not_materialize_its_sources`：跑一次 `health()`，`light_view_materialized()`
的差值必须是 **0**；先 `flush()` 逼数据真落在 SST 里（全在 memtable 时这条是空转，所以还加了
`live_keys >= 300` 的形状断言）。⚠️ 和 M36 一样**不拿分配计数当判据**（进程级、并行会互相污染），
钉的是"走没走那条物化路径"。变异验证：把 `light_iter_range` 里 SST 那几路源临时换成
`sst.light_view(snap).into_iter()`（旧形状）⇒ 红在"health 期间有 300 条被整篇物化"。

同一轮还留着没动的邻账：lsm **灌行**瞬时 **+106.8 MB**、bitcask `vacuum()` 的 **① 归组判定 106 MB
+ ④ `merge()` 115 MB**（600k 键 × 2 版那档的分段拆分见 M37 段）—— 都是"跨全库的累加器"，不是归并惰性化能顺手带走的。

**M37 —— bitcask 的 `vacuum()` 改成按块走查：① 归组判定 106.0 MB → 0.5 MB，整段 171.4 → 115.7 MB**

先量的结论就是上面那张分段表（M37 第一步）：元凶是 `to_delete` + `keep` 这两个**跨全库的累加器**
（M36 把它们从"整库归组"缩成"当前一组 + 两份清单"，但清单还是 O(全库)）。

改法：**按块走查**。`iter_meta_from` 按 `&self` 借住 KeyDir，而删除与读值都要 `&mut kv` ⇒
每块至多 `VAC_CHUNK_ROWS = 8192` 行：块内只克隆 `(版本号, enc, 是不是软墓碑)`，`for` 里的迭代器
一 drop 就立刻把这一块判出来的行删掉、全库轮次再把保留行的 blob 引用**当场读出来、读完就丢**
（这一步就是 `keep` 那张清单原先的存在理由）。块与块之间的下界用 `enc ‖ 0x00` ——
enc 域里的**严格后继方向**；⚠️ 不是 `enc ‖ 0xFF×8`（那是右界用法，会跳掉以 `enc` 为前缀的更长键，
同一个坑在 `common::store::fold_lower_bound` 的注释里写过）。公开契约"一次扫完全库"没变、
`vacuum_bounded` 的预算/游标/`rows_scanned` 语义一字未动，磁盘格式没动。

实测（同一形状、修好的量具、门控关掉时的外层读数）：

| 600k 键 × 2 版（120 万物理行） | 改前 | 改后 |
|---|---|---|
| ① 归组判定 | 106.0 MB（93 B/行）—— ⚠️ 这已经是 **M36 之后**的读数；M36 之前同一档同一把尺子是 **219.1 MB**，见上面 #49 那张三段表 | **0.5 MB**（0 B/行）|
| ④ `kv.merge()` | 115.0 MB | 115.0 MB（**没动**，另立一条） |
| 外层 `vacuum()` 整段瞬时 | 171.4 MB | **115.7 MB** |
| 三档复测 `vacuum()` 瞬时（600k 行那档） | 171.4 MB | **115.7 MB** |

守卫 `test_vacuum_deletes_in_bounded_chunks`（尺子 = 单次 flush 前最多攒多少待处理行，判据
≤ 2×块界，且库里可删行数必须**大于一块**否则判据空转）：20 000 把键 × 2 版那档实测
"裁掉 20 000 行、单次 flush 前最多攒 **8192** 行"。变异验证：把块界抬到一整库那么大 ⇒
`vac_pending_max` 变成 20 000 > 界 16 384，红。

⚠️ 这一段标尺本身也是产物：`VAC_CHUNK_ROWS` 是模块级常量，所以守卫能引用它、
"块界生效没有"就成了一个可断言的数，不用退回去看进程级内存。

**还没做的两笔（如实）**：① bitcask `merge()` 那 115 MB（下一刀，独立归因）；
② 同根因的另外三家 `to_delete`（lsm 还多一个"存活行清单"）—— 见下面 **M37b**。
**（修订：两笔都已落地 —— ② = M37b、① = M40，M40 只砍掉快照那 27 MB，剩下的 87.9 MB 是新索引本身。
再修订（M43）：那把新索引也没了 —— 87.9 → 12.6 MB，走查不再记位置、改成重扫新日志。）**

**M37b —— 同一条块结构推进另外三家：lsm 101.2 → 45.5 MB、memory 4.0 → 0.3 MB、bplus 3.9 → 0.9 MB**

三家照 bitcask 的块结构改（`VAC_CHUNK_ROWS = 8192` 四份同值，各家钉各家的守卫）：

| 家 | 走查借用为什么必须分块 | 消掉的东西 | 600k 档 `vacuum()` 瞬时 |
|---|---|---|---|
| lsm | `LightMerge` 按 `&self` 借住树，删除要 `&mut kv` | `to_delete` + **整张存活行清单**（`gc_blobs` 拆成 `collect_blob_refs` + `gc_blobs_referenced` 两半，逐块并入引用集合；公开入口 `gc_blobs` 留成壳） | 101.2 → **45.5 MB** |
| memory | 走查持读锁、删除要写锁 ⇒ 每块**放掉读锁再单独取写锁** | `to_delete` | 4.0 → **0.3 MB** |
| bplus | `scan_entries_break_from` 借住 `kv`，块间才删得动 | `to_delete` | 3.9 → **0.9 MB** |

块间下界统一是 `enc ‖ 0x00`（严格后继方向，理由同 bitcask 那段的注释）。四家的公开契约都没变
（`vacuum()` 仍是一次扫完全库，`vacuum_bounded` 的预算/游标/`rows_scanned` 口径一字未改）。

守卫四家各一条同名同形 `test_vacuum_deletes_in_bounded_chunks`（尺子 = 单次 flush 前最多攒多少
待处理行，判据 ≤ 2×块界 + "可删行数必须大于一块"防自空转）。实测三家分别攒 4096 / 4095 / 8192 行
（bitcask 8192）。**变异一次跑四家**：只把 memory 与 bplus 的块界抬到一整库那么大 ⇒
**只有这两家红**（"最多攒 20 000 行"），bitcask / lsm 照绿 ⇒ 四条守卫互相独立、不是同一条断言的四个副本。

lsm 剩下的 **45.5 MB 已不在累加器里**（`to_delete` 与存活清单都没了），嫌疑在 `kv.flush()`
（memtable → SST）与压实那一段 —— 要定它得先给 lsm 的 `vacuum` 也装一条分段标尺（照 bitcask 那条，
**记得门控**：常开会把外层台架的读数锯短，M37 已经踩过一次）。

**M40 —— bitcask `merge()` 的走查也改按块：④ 段 115.0 → 87.9 MB，外层 `vacuum()` 115.7 → 87.2 MB**

M37 那条分段标尺指得很清楚：④ `merge()` 115.0 MB（100 B/扫描行）。归因到 `merge()` 开头那一次
`.collect()` —— 它把整个 KeyDir 抄成 `Vec<(key.clone(), loc.clone())>` 快照。为什么要快照：
循环里的 `read_live` 要 `&mut self`，不能一边按 `&self` 借用 `key_dir` 一边读盘。

改法与 M37 同一条：每块只抄 `MERGE_CHUNK_KEYS = 8192` 把键，借用一结束就处理这一块 ⇒ 那份**重复
的键表**从 O(全库) 变成 O(一块)。块间下界 `key ‖ 0x00`（严格后继方向，与四家 vacuum 的块界同一个
理由、同一条坑）。`merge()` 的可观察行为一点没变（活的/软墓碑两类记录的搬法、`merge_hold` 那段
崩溃语义、磁盘格式都不动）。

| 同一段台架（`bitcask_vacuum_stage_split`，600k 键 × 2 版，`VACUUM_STAGE_SPLIT=1`） | 改前 | 改后 |
|---|---|---|
| ④ `kv.merge()` 段瞬时 | 115.0 MB | **87.9 MB**（77 B/扫描行） |
| ① 归组判定 | 0.5 MB | 0.5 MB（M37 已清） |
| 外层 `vacuum()` 整段瞬时（大库台架 600k 档） | 115.7 MB | **87.2 MB** |

⚠️ **这一刀只买到 1.31×，因为剩下那 87.9 MB 不是瞬时账** —— 它是 `merge()` 的**产物**：
`new_key_dir` 那把替换用的新索引（O(活键数)，活到函数结束、随后就装回 `self.key_dir`）。
要连它一起省只有 B 案：原地更新 `key_dir` 里的 `pos/file_id`、不建第二把索引 —— 而那会碰
"崩在文件切换点上"的恢复语义（`merge_hold` 那段），是独立的一刀，本轮**没做**。

**（修订：那一刀 = M43，做了。同构建 A/B 把 ④ 段 87.9 → 12.6 MB；B 案落地时选的是"重扫新日志"
而不是"原地更新 pos"，因为后者要在走查途中边写边让索引指向还没改名的文件。见下面 M43 那一节。）**

守卫 `test_merge_walks_key_dir_in_bounded_chunks`（尺子 = `merge_chunk_max()`：一次 merge 里
**单块最多抄了几把键**，判据 ≤ 2×块界，外加"活键数必须大于一块"防自空转）：20 000 把键那档实测
"单块最多抄 **8192** 把"。**为什么钉计数不钉内存**：`mem_probe` 的高水位是进程级的、并行用例互相
污染（M27/M36 各栽过一次）。变异验证：块界抬到 4 000 000 ⇒ 红在"单块最多抄 20 000 把"。

⚠️ **同一条变异不能拿来当"改前"的内存臂**：块是按 `Vec::with_capacity(块界)` 预留的，那一臂
④ 报 **270.6 MB** —— 量到的是预留本身而不是走查，比旧形状还大。所以上表"改前"那一列取 M37 那版
构建上同一段台架的读数（115.0 MB）。内存高水位是**分配计数**，两臂之间只有这一个常量与一条守卫
不同、且不碰 bitcask 以外的路径 ⇒ 可以横着比；会随构建漂的是 µs，不是这一列。

**M41 第一步（先量）—— 给 lsm 的 `vacuum()` 也装分段标尺：元凶不是 `flush()`，是 ① 走查+删除那一段**

M37b 留下的假设是"lsm 剩下 45.5 MB 的嫌疑在 `kv.flush()`（memtable → SST）"。标尺装上了（从
bitcask 提到 `crate::mem_probe` 的那一把，门控 `VACUUM_STAGE_SPLIT` 照旧），量 600k 键 × 2 版那档：

| 段 | 瞬时峰值 | 每扫描行 |
|---|---|---|
| ① 走查 + 删除（120 万行扫完、60 万行裁掉） | **158.6 MB** | 139 B |
| ② `kv.flush()`（memtable → SST，顺带压实） | 18.6 MB | 16 B |
| ③ `set_mvcc_xmin_filter` + blob GC | 0.0 MB | 0 B |

**假设被打回**（本仓库第 5 次"最合理的那件事不是元凶"）：`flush()` 那段只有 18.6 MB。

顺带纠正一处口径：45.5 MB 是**大库台架**那一档的读数，而那个形状 636 425 行里只有 **35 424 行可删**；
**事故形状**（60 万行全可删）下 lsm 一次 `vacuum()` 的瞬时是 **174.5 MB**。M37b 那句"降到 45.5 MB"
没写错，但它不是最痛的那一档 —— 台架的 `vacuum()` 那一步只裁掉 5.6% 的行。

**同构建 A/B，不动代码、只转现成旋钮**（`LSM_MEM_ENTRIES` 改 `mem_threshold_entries`）：

| memtable 条数阈值 | ① 走查+删除 | ② flush |
|---|---|---|
| 4 096 | 7.0 MB | 1.1 MB |
| 32 768 | 20.3 MB | 2.3 MB |
| 256 000（MVCC 档默认） | 155.7 MB | 18.8 MB |

三点几乎共线 ⇒ **① ≈ 4.5 MB 的地板（块缓存 + 一块的暂存）+ ~0.6 KB × "一次 flush 之前 memtable
里攒的条数"**。① 不是走查自己的账，是**删除落进 memtable、而 memtable 要攒到 25 万条才 flush** 的账。

根因在 **memtable 的字节账**：`mem_bytes` 只数载荷（`key.len() + StoredEntry::estimate_bytes()` ⇒
一条 16 B 键的墓碑算 **25 B**），60 万条算出来 **15 MB**，而 `mem_threshold_bytes: 32 MB` 那条**永远不触发**；
真正兜底的是条数阈值 256k，可 256k 条在堆上是 **~155 MB**（BTreeMap 的节点、内层 map 的盒子、
键的 `Vec` 头都不在账上）⇒ "memtable 32 MB" 这句在墓碑形状上把真实占用低估了 **~19 倍**
（校准：真堆占用 **476 B/条**，载荷账只记 25 B）。同一个根还挂着另一格：lsm **灌行**瞬时 **+106.8 MB**。

> **修订（M41b 校准时改的）**：本段初版把载荷写成"一条墓碑 17 B / 60 万条 10.2 MB / 低估 ~35 倍"。
> 那个 17 是按 `key.len() + 值的长度` 口算的，而 `mem_bytes` 真正记的是
> `key.len() + StoredEntry::estimate_bytes()` = `16 + (8 + 1)` = **25 B** ⇒ 60 万条 15 MB、
> 低估倍数 476 ÷ 25 ≈ **19 倍**。结论方向不变（字节账严重低估、32 MB 那一档在墓碑形状上从不触发）。

第二笔（形状）：`flush_mem` 走的 `mem_to_sorted_entries` 把**整张 memtable 连键带值克隆**进一个
`Vec` 再 `sort_by` —— 而 `MemMap` 是 `BTreeMap<键, BTreeMap<seq, 值>>`，两级本来就是升序，
`SstBuilder::add` 要的那个序是**白拿的** ⇒ 那次排序与那份拷贝是 flush 不要的钱（上面 ~0.6 KB/条里
有它一份）。

⇒ 两刀：**M41a = flush 改成流式吐 memtable**（不暂存、不排序），**M41b = memtable 的触发判定改读
真内存**（载荷 + 条数 × 实测结构开销）。耗时那列（7.23 / 9.50 / 7.00 s）在本机是噪声，不作因果凭据。

**M41a —— lsm 的 `flush()` 改成流式吐 memtable：① 158.6 → 119.8 MB，② 18.6 → 0.0 MB**

`flush_one_immutable` 原来是 `for (k, se) in Self::mem_to_sorted_entries(&imm)` —— 那个函数把**整张
memtable 连键带值克隆**进一个 `Vec`，再 `sort_by((键, seq))`。而 `MemMap` 是
`BTreeMap<键, BTreeMap<seq, 值>>`，**两级本来就是升序** ⇒ 要的那个序是白拿的（`SstBuilder::add` 里
那句 `debug_assert(last_k <= key)` 核的就是它）。现在按值进循环，条目一条条搬进 builder：

- 那次排序与那份暂存没了；
- 每搬走一条，memtable 那一侧当场少一条（BTreeMap 的节点随消费释放）⇒ builder 涨多少，memtable 就
  瘦多少，flush 的瞬时占用不再是"整张 memtable + 它的两份拷贝"。

实测（同一段台架、默认 memtable 阈值 256k、600k 键 × 2 版）：

| | 改前 | 改后 |
|---|---|---|
| ① 走查 + 删除 | 158.6 / 155.7 MB | **119.8 MB** |
| ② `kv.flush()` | 18.6 / 18.8 MB | **0.0 MB** |

⚠️ **② 报 0.0 不是"量不到"，是"搬而不是拷"的算术**：最后那次 flush 仍要把 memtable 里剩下的
~7.5 万条写进 SST，但条目是**逐条搬**进 builder 的 ⇒ memtable 瘦多少，builder 就涨多少，
`live()` 的净值 ≈ 0（改前那一档 18.6 MB 正是"整张 memtable 还活着 + 它的一份克隆 + builder"）。
① 剩下的 119.8 MB 也不是 flush 的账，是 **memtable 自己攒到 25 万条**的账 ⇒ 那一半归 M41b。

守卫 `test_flush_moves_memtable_entries_without_cloning_them`：尺子 = `EntryValue` 的**复制次数**
（`sst::entry_value_clones` —— 手写 `Clone` 就为了在这一处能数出来），判据 = 一次 flush 期间必须
**0 条**，另配两条防自空转（flush 真发生：`mem_entries` 归零 + SST 数量涨；首/中/尾三把键读得回来 ——
去掉那次排序之后 SST 稀疏索引靠的就是 `MemMap` 的两级升序）。**为什么钉复制次数不钉内存**：分配计数
是进程级的，并行用例会互相污染（M27/M36 各栽过一次）。变异验证：把那两个 `for` 换回
`mem_to_sorted_entries(&imm) + sort_by` ⇒ 红在"flush 复制了 **5000** 条值"。

**M41b —— memtable 的 flush 判定改读真内存：① 119.8 → 33.1 MB（那句"mem 32 MB"从此才算数）**

`maybe_flush_mem` 的字节那一档原来直接读 `mem_bytes`，而 `mem_bytes` 是**载荷账**
（`key.len() + StoredEntry::estimate_bytes()` ⇒ 一条 16 B 编码键的墓碑只算 25 B）。现在读
`mem_bytes + mem_entries × MEM_ENTRY_OVERHEAD`，常数由校准台架量出来：

- `lsm_memtable_bytes_per_entry`（`#[ignore]`，单线程，读 `live()` 差值）：16 B 键 + 空值，
  **每条 476 B**，n=100k 与 n=400k 两档读到同一个数 ⇒ 线性；载荷账只记 25 B ⇒ 结构 **451 B**。
- 于是 `MEM_ENTRY_OVERHEAD = 451`。两个已知边界都朝**保守**方向：一把键堆多版本时真实开销低于
  `条数 × 451`（判定会提前 flush），键更长时开销也涨（那部分由载荷项顶着）。

实测链条（同一段台架、默认 MVCC 档、600k 键 × 2 版，裁掉 60 万行 / 扫 120 万行）：

| | ① 走查+删除 | 先算后量 |
|---|---|---|
| M41 之前 | 158.6 MB | — |
| M41a（flush 不再整张克隆再排序） | 119.8 MB | 校准 476 B × 262 144 条 = **119.2 MB** ⇒ 对上 |
| M41b（判定读真内存） | **33.1 MB** | 32 MB ÷ 476 B ≈ 70.7k 条 ⇒ 预测 **33.7 MB**，实测 33.1 ⇒ 对上 |

两档都是**先算后量、量出来对得上**，所以"memtable 32 MB"这句话现在是真的说得出口 ——
条数阈值 262 144 不再是那个兜底的（也就是把 ~155 MB 当预算的）一档。

守卫 `test_memtable_flush_trigger_reads_real_bytes`：阈值钉 256 KB、条数阈值钉到碰不到，
按 476 B/条算出**边界两侧各一条** —— 550 条（261 800 B）**不该** flush、551 条（262 276 B）**必须**
flush，两臂同构建只差一行。所以它既钉"开销项进了判定"（少算 ⇒ 551 那臂不 flush ⇒ 红），也钉
"没把它 inflate"（多算 ⇒ 550 那臂提前 flush ⇒ 红）；另外钉住 `stats().mem_bytes` 对外**仍是载荷账**
（口径没改，改的只有判定那一句）。判定还是两个增量计数器相加 ⇒ O(1)，没把 M19 的 O(N²) 请回来。
变异验证：去掉 `+ mem_entries × MEM_ENTRY_OVERHEAD` ⇒ 红在"551 条已经过阈值该 flush 了"。

⚠️ **行为变更**（不破磁盘格式、不改公开 API）：默认档与 MVCC 档的 memtable 都比改前**小** ——
默认档改前 `4 MB` 那一档从不生效（65536 条 × 25 B 载荷 = 1.6 MB，先到条数阈值），实际预算是
"65536 条 ≈ 30 MB 真内存"；现在 `4 MB` 在 ~8800 条就生效。MVCC 档同理从
"约 26 万条（≈155 MB）"变成"约 7 万条（≈33 MB）"。

**这一刀买内存、花时间 —— 同构建 A/B**（把 `MEM_ENTRY_OVERHEAD` 临时置 0 = 改前的载荷账口径，
其余一字不改，两臂各跑一遍 600k 档大库台架）：

| 600k 档 | 改前口径（overhead=0） | 改后（451） | 差 |
|---|---|---|---|
| lsm 灌 600k 行（事务） | 17.1 µs/行，峰值 **+77.0 MB** | 24.9 µs/行，峰值 **+45.6 MB** | 时间 **+46%**、峰值 **−41%** |
| lsm `vacuum()` 全库 | 2.05 s/次 | 2.48 s/次 | +21% |
| 对照：bitcask 灌行 | 15.8 µs/行 | 15.4 µs/行 | 两臂同量级（本机噪声地板） |
| 对照：memory 灌行 / bplus 灌行 | 3.9 / 35.9 µs/行 | 3.1 / 43.2 µs/行 | 这两条**没被本刀碰过**却在 ±25% 里晃 ⇒ 本机噪声地板有这么宽 |

机制（**推测，未分段计时**）：600k 行的 flush 次数从 ~2.3 次（262 144 条/次）变成 ~8.5 次
（70.7k 条/次），L0 更容易攒到压实阈值 ⇒ 压实重写更多。这条没做分段标尺，所以只按推测记；
要买回时间就把 `mem_threshold_bytes` 调大（`LSMTreeOptions { mem_threshold_bytes: 64 * 1024 * 1024,
..LSMTreeOptions::for_mvcc() }` 或 `EngineConfig.lsm`），预算与写单价的取舍从此是**明写的一个数**，
不是像改前那样"写着 32 MB、实际 155 MB"。

**M35 —— 数一个数不该把整库搬进内存：`search 首页(要 total)` 与 `key_count` 的峰值 +123.7 MB → 0.6~1.2 MB**

两件事的形状是同一个：**为了一个数，把界内每一行都物化出来再丢掉**。

- `key_count(prefix)` 四家一字不差写的是 `prefix_scan(prefix).len()` —— 物化整段，取个长度。
- `search_keys` 要精确 total 时折叠不给界（`limit = None`），把界内命中全部物化成
  `Vec<ExportRecord>`，`total = matched.len()`，而调用方最后只拿走那一页的 100 行。

改法两步，都收进共用件（`src/common/`）：

1. **`count_window`（只数不物化）**：同一条折叠、同一套判据（`Missing` 不算，墓碑与**过期行**同待），
   但每行只判"算不算一条"。落点就是 `fold_window_emit` 的返回值（它数的正是"交给 `emit` 的行数"）
   ⇒ **一份累加器都不需要**。过期判定用新加的 `codec::logical_is_live` —— ⚠️ 不能图省事用
   `decode_value_meta`：它把 user 段 `to_vec()` 出来，"数一个数"于是先付一次堆拷贝。
2. **`search_page`（四家一份）**：要 total 时拆成"数一遍 + 只取那一页"，`total` 从外面传进
   `search_page_from_with_total`；`search_page_from` 留成它的委托壳（公开签名与旧行为都不动）。

顺带合并掉的两份复制（都是"四份同形"，基座不该有这种形状）：

- 四家 `search_keys` 那 15 行 ⇒ 一行 `common::search_page(self, query)`，各家自己的
  `collect_for_search` 中间层同时删掉。
- 四家 `scan_foreach` 里那**一字不差**的"折叠 + 三态筛 + 剥壳 + 过期判定" ⇒
  `common::scan_live_records`。判据本来就只在 `live_record_of_logical` 一处定义过（M20 为
  `seek_prev` 抽的），抄四遍的意思是"改一处漏三处"，而 `scan` 与 `seek_prev` 一旦对同一行给出
  两种答案，就是静默的数据不一致。

实测（600k 档、四引擎同一场台架；⚠️ 这台机器**同一构建连跑两场的墙钟能差 ±20~25%**，所以
时间那列只作跨构建参考、不作因果凭据；**峰值那列是分配计数，可以横比**）：

| 项（600k 行） | 峰值 改前 | 峰值 改后 | 时间（跨构建参考） |
|---|---|---|---|
| `search 首页`(要 total) bitcask / bplus / lsm / memory | **+123.7~123.8 MB** 四家全中 | **+0.6 / 1.2 / 0.7 / 0.6 MB** | 2.43 / 1.19 / 1.87 / 0.65 s（改前 2.41 / 1.40 / 1.83 / 0.77） |
| `key_count 2B前缀` 四家 | **+123.7 MB** 四家全中 | **+0.6 / 0.8 / 0.6 / 0.6 MB** | 2.76 / 1.24 / 1.72 / 0.68 s（改前 2.34 / 0.94 / 1.79 / 0.77） |
| 对照：`一次全量 scan` / `prefix_scan` | +123.7 MB | +123.7 MB（**没动**） | 1.4~4.7 µs/行 |

⚠️ **我先前给的估价"预期 ~1.5× 时间"被打回**（本仓库第 6 次）：时间没怎么动，全在噪声带里。
原因是这条路上**走查与取值本来就是大头** —— 过期判据住在 value 的信封里，不读值就判不了，
M35 省掉的"结果 `Vec` + 每行用户字节拷贝"那笔是**内存**。⇒ 这一格真正卖出去的是
**123.7 MB → 1 MB 以内**：一个只数数的调用从前要按整库申请瞬时内存，比结果集本身还贵两个数量级。
返回整库的那几条（`scan` / `prefix_scan` / `export` / `migrate`）**没变** —— 它们的 `Vec` 是调用方
要的东西，不是实现的浪费。

守卫 `all_engines_count_without_materializing_values`（一条钉四家 —— 实现已经只剩一份了）：
尺子 = **用户字节被拷出来的条数**（`user_value_copies`；两个入口都记在这一把上：search 的
`logical_to_user`、scan 的 `live_record_of_logical`）。判据三条：

- `key_count` 一次调用 ⇒ **0 条**；
- `search_keys(要 total)` 首页 ⇒ 拷的不超过**页宽 + 1**；
- 两条**语义对拍**：`key_count` 必须与 `prefix_scan().len()` 同一个数（库里同时放了**墓碑行**与
  **过期行**），`total` / `items` / `has_next` / `total_pages` 在首页与**末页**都要与全量那条路一致。

实测四家都是 "key_count=893（物化对照 893）⇒ 期间拷出 **0** 条用户字节"。
**两条变异各自验证**：① 把 bitcask 的 `key_count` 换回 `prefix_scan().len()` ⇒ 只有那一家红
（"拷出 893 条"）；② 把 `count_window` 里的 `logical_is_live` 换成 `logical_to_user` ⇒ 四家一起红。
⚠️ 变异①第一次跑**没红** —— 因为那把尺子当时只钉在 `logical_to_user` 上，而 `prefix_scan` 走的是
四家各自内联的剥壳。⇒ 先把入口补齐（合并 `scan_foreach` 之后判据只剩一处，尺子也只剩一处），
变异才抓得住。**这条记下来：尺子必须钉在"旧形状真正流过的那扇门"上，钉在自己新写的那扇门上等于没钉。**

**M42 —— `SstBuilder` 攒满一块就落盘：一次压实的瞬时 6.3 → 3.1 MB（M33 那条链的最后一块）**

M33 把压实的**输入**改成流式源之后，输出侧仍是"一整张表攒在 `entries: Vec` 里，`finish()` 才编块写盘"
—— 于是 flush 与压实各剩一条"1×"下限，由 `target_sst_bytes` 说话。M42 第一步先量它握住多少条：
**单文件最多 18 333 条**（600k 行、34 个输出文件），按同一批条目实测的 **269 B/条** 算 ≈ **4.9 MB**，
而那一段的总峰值是 6.3 MB ⇒ **78% 就是这块缓冲**（条目载荷本身只 1.9 MB）。

改法：`add` 攒到 `block_entries`（默认 64）就 `emit_block()` —— 编码、写盘、追加索引、记尾键，
然后清空。三处细节决定了它不是"把循环挪个位置"：

- **布隆的位图宽度 `m = 总条数 × bits_per_key` 要等总条数知道才能定**，而位位置是 `h % m` ⇒
  边加边设位会与格式对不上。做法是加期间每条只存 **8 B 哈希对**（两个都是 crc32 值），
  `finish` 时按最终的 `m` 逐对设位 ⇒ 位位置与原来"逐键 `insert`"**一字不差**。
  ⚠️ 所以这一刀之后还剩一份 `O(文件)` 的账：18 333 条 = **143 KB**（改前那一整张表是 MB 级）。
  它被单独钉成尺子（`builder_hashed_max`），下次谁要清它，得先决定"让调用方预估条数"这笔取舍。
- **偏移自己累计**（`written`），不每块 `stream_position()` —— 那等于每 64 条把 1 MiB 缓冲冲一次。
- **`add` 没有地方返回 IO 错误**（`pub fn add(&mut self, …)` 四处调用方在用 ⇒ 改签名是破坏性的）
  ⇒ 第一个错误存下来，`finish()` 如实抛出。副作用是没走到 `finish` 的 builder 会留下半截
  `.sst.tmp`（改前什么都不写所以什么都不留）⇒ 加了 `Drop` 删掉它：加载以 MANIFEST 为准，
  那个名字不影响正确性，但**长跑进程里这种垃圾是要攒的**。

实测（`compaction_cost_split`，600k 行 / 8 个输入文件）：

| | 改前 | 改后 |
|---|---|---|
| ③ 流式源那臂的瞬时峰值 | 6.3 MB | **3.1 MB** |
| builder 单文件握住的未落盘条目 | 18 333 条 | **64 条**（= 块宽） |
| 整段（旧 ①+② vs 新 ③） | 省 25.0× | **省 49.3×** |
| ③ 时间 | 1862 ms | 1370 ms（同一场里没被碰过的 ① 也从 369 → 319 ms ⇒ 本场整体快 ~13%，扣掉之后 ≈ **−13%**，只按参考看） |

flush 侧**没变**（那一段由 memtable 说话，M41b 之后是 ~33 MB；① 33.1 → 34.0 MB 在噪声里），
② 那一格从 0.0 变 1.0 MB 是 **1 MiB 写缓冲提前到第一块就分配**，不是新账。

600k 档四引擎复测（同一场台架）没抓到回归：`bulk 灌 60 000 行` 峰值 **15.7 → 13.9 MB**、
`一次全量 scan` 2.5 µs/行、`health()` +0.3 MB、`冷重开` 12.3 ms、`vacuum_bounded 一片` 3.3 ms 都不动；
`灌 600k 行` 这一场读到 18.3 µs/行（M41b 那一场同一形状读到 24.9 —— 本机跨场 ±25%，
这数只算参考，方向与"每次 flush 之后的压实变便宜"一致）。

守卫两条：
- `test_sst_builder_byteprint_is_stable`（**格式 golden**）：1000 条固定输入 ⇒ 文件长度 **31 823**、
  全文件 crc32 **7911d92d**。这两个常数是在**改前**的实现上跑出来的（单独一个提交先入库），
  所以改完之后它还绿 = **写出去的字节与改前逐字节相同**；红就代表文件格式被动过，不是测试写错了。
- `test_builder_emits_blocks_incrementally`：灌 4000 条、块宽 64 ⇒ 未落盘条目最多 **≤ 64 条**
  （旧形状读到 4000）、布隆哈希**正好 4000 条**（这一小截是故意留的，钉死它=钉死"我知道它在哪"）、
  文件读回来逐条一致 + footer 的 min/max 是第一把与最后一把键。
  变异验证：去掉 `add` 末尾"攒满就 `emit_block`" ⇒ 红在"单块握住 4000 条未落盘条目（块宽 64）"。

**M43 —— bitcask `merge()` 不再另建第二把索引：④ 段 87.9 → 12.6 MB（600k 档，同构建 A/B）**

M40 之后 `merge()` 剩的那一笔不是快照，是**产物**：走查时每抄一行就顺手往 `new_key_dir` 插一条，
那份"替换用的新索引"与旧索引**同时活着**一直到函数末尾 ⇒ 峰值 = 两份索引。600k 键 × 2 版那档的
④ 段 87.9 MB（77 B/扫描行）就是它。

改法（B1：抄完 + sync + 删旧 + rename，然后**原地重扫新日志**）：走查只负责把存活的行抄进
`data.merge`，**不再记位置**；换名、重开句柄之后 ——

1. 先删 hint。hint 是旧索引的**持久化副本**，里面那些 `pos` 说的是已被删除/改名的文件；留着它，
   万一就崩在下面那趟重扫中间，下次 open 会把它当真索引读回来（`hint_files_exist` 兜不住这一支：
   空表里挑不出"指向缺失分片"的条目，而"新日志比旧日志长"的时候连长度检查都放行）。
2. 再 `key_dir = KeyDir::new()` 把旧索引腾掉 —— **这一步就是收益本身**：全场只剩一份索引。
3. 置 `index_rebuilding` → `self.active.load_index_from(&mut self.key_dir, 0, 0)` 原地重扫。
4. 扫成功才放标志；扫失败就带着 `Err` 返回并**保持隔离**。

同构建 A/B（同一个二进制，只切环境变量 `MERGE_OLD_SHAPE`；两臂各跑两轮，600k 键 × 2 版）：

| 段 | 改前臂（`new_key_dir` 那份形状） | 改后臂（B1） |
|---|---|---|
| ④ `kv.merge()` 瞬时峰值 | **87.9 MB**（77 B/扫描行）两轮同数 | **12.6 MB**（11 B/扫描行）两轮同数 |
| 一次 `vacuum()` 总耗时 | 6.20 / 6.55 s | 5.39 / 6.57 s |

⇒ **省 75.3 MB = 6.98×**。剩下那 12.6 MB 是走查那一段自己的工作集（写缓冲、单块键表、新索引
节点与旧索引的差额），不再含"整把索引 ×2"。

外层那两格（同一场 600k 大库基线、修好的 `timed()`；⚠️ **跨构建快照**，不作因果凭据）：

| 600k 档，`scale_baseline` | 读数 | 备注 |
|---|---|---|
| `vacuum()` 全库 瞬时峰值 | **+8.0 MB**（M40 那轮记的是 87.2） | 台架形状只有 35 424 行可删 |
| `flush+merge` 瞬时峰值 | **+13.1 MB**，磁盘 116.5 MB | 与 ④ 段那 12.6 MB 来自**两条独立台架**，对得上 |

⚠️ **时间这一列不可判定**：两臂各自的 min 差 13%，落在本机 ±25% 的墙钟地板里。结构上多出来的
是"把新日志顺序再读一遍"（600k 档 ≈ 90 MB），省掉的是"60 万次往第二把索引里插键 + 克隆键"，
两笔同量级 ⇒ 就说**平的**，既不拿它当收益也不拿它当代价。

**代价**（这就是当时要在 B1 / B2 之间挑的那个东西）：腾掉旧索引到重扫完成之间有一段**重扫窗口**，
里面磁盘上全库完好、内存里没人知道有哪些键。于是 `Bitcask::usable()` 把每一扇**能报错**的门
（`get` / `get_entry` / `range_scan` / `iter` / `set` / `insert` / `insert_batch` / `delete` /
`remove` / `dump_hint`）都拦下来报错，而不是回 `Ok(None)` —— 后者等于对每把键宣称"不存在"，
门面会把它当成"这版本没有值"。

- ⚠️ 正常那一次 merge 里**没人看得见**这个状态：门面把 `kv` 锁在同一把 `Mutex` 里、`merge()`
  全程持锁 ⇒ 并发请求只是阻塞在锁上。它只服务一种现场：**重扫失败**（`merge()` 带 Err 返回）
  ⇒ 引擎被隔离，出路只有重新 open（`full_rebuild` 扫日志）。
- 正因为这样，`vacuum_bounded` 里那句 `let _ = kv.merge();` 改成 `kv.merge()?;`。改前吞得起：
  旧形状要么全程不动 `key_dir`、要么一次性换成完整新索引，失败也留不下半截状态。改后吞它就是
  "报 `Ok` 而库已经不服务了"。**这一处跟着改法是必须的，不是顺手清理。**
- ⚠️ 诚实边界：`len` / `is_empty` / `scan` / `scan_prefix` / `iter_meta` / `iter_meta_from` /
  `range_meta_back` 这几扇门**不带 `Result`**，给它们改签名是破坏性 API 改动 ⇒ 隔离期它们只能
  看到"空索引"：不删、不写，但会把"库是空的"当真话。（另一案 B2 没有这个窗口，代价是每 8192 把
  键一次 fsync —— 600k 档 ≈ 146 次，这台机械盘 85 ms/次 ⇒ +12 s，且改的是崩溃语义。）

守卫两条：

- `test_merge_rebuilds_index_by_rescanning_the_log`（尺子 = `merge_rescan_rows()`：这次 merge 的
  重扫往索引里折了多少行日志）：20 000 把键、每把两版、硬删 1/7 ⇒ 重扫行数必须**正好等于** merge
  之后的索引条目数（实测 **17 142 行 = 17 142 条**），另外逐条断言"被硬删的键读不回来、存活键读回
  最新版本、重开之后一致"。为什么钉"重扫了多少行"、不钉"同时活着几把索引"：`KeyDir` 只是
  `BTreeMap` 的别名，要数它就得包成 newtype = 破坏性 API 改动；而"索引由重扫得到"是同一件事的
  可观测面。变异验证：去掉那趟重扫（回到"边抄边记位置"）⇒ 红在"重扫记账一次都没响"，
  同时另外 4 条 merge 守卫一起红（索引是空的）。
- `test_quarantined_engine_errors_instead_of_claiming_keys_missing`：用 `quarantine_index_for_test()`
  把引擎推进与 `merge()` 里**一字不差**的那个现场（腾掉索引 + 置标志），逐门断言报错、断言被拒的
  写入一条都没落盘、断言 hint 写不下去，最后断言"重新 open 真的救得回来"。
  ⚠️ 第一版这里栽过：setter 只置标志、索引还满着 ⇒ `get` 读出的是**对的**值，那条守卫量的根本不
  是那个现场。变异三条各自证红：`get` 去掉守卫 ⇒ 红在"回了 None"；`insert` 去掉 ⇒ 红在"被拒的
  写入还是落盘了（差 N 字节）"；`dump_hint` 去掉 ⇒ 红在"隔离期把 hint 落盘了"。

**被自己的变异推翻的一句判据（就地改，原文不删）**：这一刀的"腾掉旧索引"原本被我写成"既是收益
也是**正确性**步骤 —— 不腾掉就会读到错位的数据、已删的键会复活"。变异 B（跳过 `KeyDir::new()`、
直接在旧表上重扫）**没有红**：`delete()` 当时就把键从索引里摘了，merge 的输入索引与输出索引
**键集本来就相同**，重扫逐条覆盖掉旧位置。⇒ 腾掉索引买到的只是"窗口里不存在半新半旧的索引"
这点整洁；**正确性由 `usable()` 那一拦负责**。这条也是"先加失败模式的守卫、再改"的正面样本：
守卫跑红之前，我以为自己知道它在钉什么。

**M44 —— 导出不必再把整库摆进内存：新增流式出口 `export_latest_visible_for_each`（四家 + 门面），`export_kv` 工具不再把全库装两遍**

根因一句话：折叠那一段**早就是流式的**（M27 的 `fold_rows_streaming`，每片 `FOLD_CHUNK` 组、一行不攒），
可**出口只有一条** —— `export_latest_visible() -> Vec<ExportRecord>`。于是"把 600k 键的库导成文件"
必须在内存里先把 60 万行摆好，只为等着逐行写出去；本仓库那三个 `export_kv` 工具还在它上面
**又** `.map()` 成一份 `Vec<KvRecord>` ⇒ 一次导出，全库同时在内存里摆**两遍**。

改法（四件事，都在共用件里落一次，不在四家各抄一遍）：

| 落点 | 做了什么 |
|---|---|
| `common::export_rows_streaming` | 导出行形状的**唯一一份判据**（墓碑 / 过期或坏壳 / 读不到），引擎只供"逻辑字节 → 用户字节"那一步（`logical_to_user` 当参数进来，按值收 ⇒ 不复制） |
| 四家 `Transaction` / `MVCC` | 各加 `export_latest_visible_for_each`；原来的 `export_latest_visible` 改成它的**收集壳** ⇒ 两条路不可能各自漂移（M39 同一招） |
| `KvEngine` / `KvTxn` 两个 trait | 各加一条 `export_latest_visible_for_each`，**默认实现是"物化后再逐行交"**（⇒ 不是破坏性变更），四家在 `impl_engine!` 里覆盖成真流式 |
| `common::kv_snapshot` | `BinWriter` / `JsonlWriter` 逐行写口 + `ExportWriter` 那层胶；`write_bin` / `write_jsonl` 改成壳 ⇒ **编码只有一份实现**（`bin` 的 `with_seq` 必须在 `create` 时就定：魔数是文件头，逐行写的人没法回头补） |
| `export_kv`（bitcask / bplus / memory 三家） | 改成"折一行、写一行"，并断言"折出的行数 == 写出的行数" |
| `import_kv`（同三家）的 VERIFY | 它原先把**整库**折成 `Vec` 只为 `.len()`（与 M35 那刀里 `key_count` 同一个形状）⇒ 改成流式数行数；memory 那一支还要前 5 条抽样 ⇒ 收满 5 条就**叫停**，后面的键不再折 |

实测（同一场台架、同一个构建，两臂就是同一件事的两种写法）：

| 600k 档 `scale_baseline`（601 001 行 → 磁盘 **59.5 MB**，两臂字节数一样） | 峰值瞬时 | 跑完仍常驻 |
|---|---|---|
| `export 全库(物化)` | +123.7 ~ **+124.1 MB** | **105.0 ~ 105.4 MB** |
| `export 流式(逐行落地)` | **+0.6 ~ +0.8 MB** | **0.0 MB** |

四家逐档读数（峰值 / 常驻）：bitcask `123.7 / 105.0` → **`0.6 / 0.0`**；bplus `124.1 / 105.4` →
**`0.8 / 0.0`**；lsm `123.9 / 105.0` → **`0.7 / 0.0`**；memory `123.8 / 105.0` → **`0.6 / 0.0`**
⇒ **瞬时 206×、常驻 105 MB 整份没了**（那份 `Vec` 本来就只为等着被逐行写出去）。
20k 档同一对（先把形状钉住）：

| 20k 档 | `export 全库(物化)` | `export 流式(逐行落地)` |
|---|---|---|
| bitcask | 峰值 +4.4 MB / 常驻 3.5 MB | **+0.6 MB / 常驻 0.0** |
| bplus_tree | +4.9 / 3.9 | **+0.9 / 0.0** |
| lsm_tree | +4.7 / 3.9 | **+0.6 / 0.0** |

⚠️ **µs/行 那一列不许横着比**：物化那一臂不写文件，流式那一臂把文件也写了（600k 档 3.4~4.4 µs/行 vs
1.6~1.9，两臂差都在本机噪声地板里）⇒ 两臂做的不是同一件事，判据只有内存那一列。

⚠️ ~~**没改的那一家**：`lsm_tree/bin/export_kv.rs` 走的是**裸引擎**的 `LSMTree::export_records_with_seq`~~
**（修订（M46）：这一家也接上了。** 它当时确实半改不动 —— 大头不在写侧而在**源**：`merge_all_at`
把 memtable 与每个 SST 都先整篇折进 `Vec` 才开始归并。M46 把源改成惰性、给裸引擎配了
`export_records_with_seq_for_each`，四家 `export_kv` 到这才算齐。）**

守卫：

- `all_engines_export_streams_without_holding_the_library`（四家跑同一套断言）。**主尺子 =
  `user_value_copies()` 在"只要 1 行"那一趟里的增量**：流式那趟只剥 1 条壳（拷 1 条用户值），
  任何"先把整库折出来再逐行回调"的形状都会拷 `want` 条 ⇒ 一个数就把两种形状分开。
  判据：只要 1 行 ⇒ 交出 1 行、拷出 ≤2 条（`KvEngine` 那扇门与 `KvTxn` 那扇门**各测一遍**）；
  全库那一趟 ⇒ 拷出正好 `want` 条（这一档同时给上面那个 `≤2` 作证：尺子不是空转）。
  ⚠️ **第一版这里用的尺子是错的**：我拿 `collect_held_max()`（**收集门** `fold_window` 攒了几行）当判据，
  可 M44 之后收集门根本不在导出路径上 —— "退回物化默认实现"的那个 `Vec` 是调用方自己攒的，
  尺子读 0 照样绿。变异（把 `impl_engine!` 里事务层的流式覆盖删掉）**第一次没红**就是这个原因，
  换了尺子之后同一支变异红在"只要 1 行却拷出 4960 条用户值"。`collect_held_max` 留着当**次要**判据
  （流式那趟确实不该走收集门，第一行处采一次、收完再采一次），但它担不起主判据。
- 语义对拍走**另一条遍历**：`scan_page` 一路翻到底（游标续跑，与流式导出是两份代码）逐行比。
  ⚠️ 不拿"收集壳 vs 流式核"互比 —— 壳就是核的壳，那种对拍恒真，不给信息。
  另钉"提前收手恰好停在预算 100 行且交出的是同一批前缀""导出升序""硬删过的键不许出现"。
- `test_streaming_sink_writes_identical_bytes`：逐行写口（含 `ExportWriter` 那层胶）与整批写的
  **字节必须一字不差** —— 导出格式是跨引擎的合同（`import_kv` 要读得回来，旧工具也认 `BPEXP001`）。
  ⚠️ 它与 `test_plain_is_bpexp001` 那份编码 golden 是两件事：那份钉"编码本身"，这一份钉"换写法没换格式"。
- 变异四条，各自红在不同格：① bitcask 的 `for_each` 改回"先 `collect_rows_as` 折完全库再逐行回调" ⇒
  红在"只要 1 行却拷出 4960 条用户值"；② 吞掉调用方的收手信号 ⇒ 红在"提前收手没停在 100（实际 4960）"；
  ③ 删掉 `impl_engine!` 里**事务层**的流式覆盖（悄悄退回物化默认实现）⇒ 红在同一格的"事务层"那一条；
  ④ 删掉 **KvEngine 层**的覆盖 ⇒ 也红。
- ⚠️ 没覆盖的一支（如实）：`ExportWriter::push` 写失败之后一律收手、错误由 `finish()` 抛。
  那一支要真把盘弄坏才触发（Windows 上还挂着句柄的文件删不掉），恒真的模拟测试不给信息 ⇒ 留注释不留测试。

⚠️ **这一轮踩到的台架教训（两条，都写进方法学）**：
① 新加的读数行自己 panic 了两次都没被发现 —— 因为跑法接了管道
`cargo test ... | grep ...`，**退出码是 grep 的**（有匹配就 0），panic 被吞。两档读数各少一行才看出来。
⇒ 以后带 `--test-threads=1` 的大台架：要么不接 grep，要么接了必须另外核对 `test result:` 那一行
（本轮改成 `set -o pipefail` + grep 里带上 `test result`）。
② 台架里 `tmp_dir(tag)` **只清残留、不创建目录** —— 磁盘引擎由 open 时自己建，memory 一家没人建
⇒ 任何"往 `dir` 里写文件"的新读数行都得先 `create_dir_all`，否则只在 memory 那一档炸。

**顺手抓到一条先前就存在的缺陷（不是这一刀带来的，同一份现场在 `HEAD` 上复现过）**：
`import_kv` 的往返对不上 —— 用 `memory-export-kv --demo 300` 造一份 301 行的 jsonl，
`bitcask-import-kv` / `bplus-import-kv` 都报 `IMPORT records=301`，可再导出只有 **32 行**，
且那 32 行的键与值是**错位**的（`demo:000001 → val-1` 变成 `demo:00000 → val-`、`demo:00001 → val-19`）。
`--include-deleted` 也只有 32 ⇒ 不是"被当成过期"，是**行根本不在库里**。
嫌疑：`apply_to_db` 用 bulk 的 `put` 写**裸用户字节**，而那条门要求的是**已打信封的逻辑字节**
（`set` 同一条合同，M16 正是为此才加 `set_raw`）⇒ 写进去的字节被后面的读当成信封解析。
诊断到这里为止，**没动码**：这是导入侧的写路径与值信封合同不一致，与"导出不物化"是两件事。

**（修订（M45）：上面这句嫌疑是错的，两条都被自己的复现推翻。）**
① bulk 无罪：四家各灌 300 行裸用户字节走 `begin_bulk().put()` ⇒ `total_rows=300 live_keys=300 导出=300
点读命中=300`，值一字不差；而且 `src/bitcask/bitcask_mvcc.rs:788` 的 `BulkLoader::put` 自己就调
`encode_stored_value(...)` 打信封（与 `set_raw` 同类），**不要求调用方先打包**。
② 真凶在 `common::kv_snapshot::b64_decode`：它在尾组之后又按 `=` 的个数**回头裁一次**，而 `=` 早在
`filter` 里被剥掉了 ⇒ 二次修正 ⇒ 凡是长度不是 3 的倍数的键/值都少 1~2 字节（`len%3==2` 少 1、`==1` 少 2）。
事故形状 `demo:%06d` = 11 字节 ⇒ 全部截成 `demo:00000` 同一把键 ⇒ 后写覆盖前写，301 行只剩 32 把键。
修法与三条守卫见下面 **M45** 那一节；修完端到端 **301 → 301 → 301**。

**M45 —— `b64_decode` 在非 3 倍数的长度上掉字节（JSONL 导入静默丢数据）：端到端从「301 行进、32 行出」修到 301 → 301 → 301**

M44 的往返台架抓到一条**先前就存在**的缺陷，而 M44 当时写的"嫌疑"（bulk 的 `put` 要已打信封的字节）
被自己的复现推翻了两层，两条都在原处留了修订：

| 我怎么错的 | 复现怎么说 |
|---|---|
| 怀疑 bulk 写路径与值信封合同不一致 | 四家各灌 300 行裸用户字节走 `begin_bulk().put()` ⇒ `total_rows=300 live_keys=300 导出=300 点读命中=300`，值一字不差 |
| 以为"要调用方自己先打包"是 bulk 的合同 | `src/bitcask/bitcask_mvcc.rs:788` 的 `BulkLoader::put` 自己就调 `encode_stored_value(...)` 打信封（与 `set_raw` 同类） |

真因一句话：**`b64_decode` 把 `=` 剥掉之后已经按尾组字符数出过字节了，又按 `=` 的个数回头裁一次** ⇒ 二次修正
⇒ 凡长度不是 3 的倍数就少 1~2 字节（`len%3==2` 少 1、`len%3==1` 少 2，只有 `len%3==0` 恰好没事）。
事故形状：`demo:%06d` 是 11 字节 ⇒ 每条键都截成 `demo:00000` ⇒ **300 把互不相同的键塌成同一把**，后写覆盖前写
⇒ `IMPORT records=301`（那是"我执行了 301 次写"）而库里只剩 32 把键，值也跟着错位。

修法：删掉那段按 pad 的二次裁剪，尾组只看剩余字符数（2 字符 → 1 字节、3 字符 → 2 字节，`4k+1` 仍然报
「非法 base64 长度」）。不动 API、不动 JSONL/bin 的字节格式。

守卫三条 —— 第三条才是防住这个现场的，前两条防的是"改法本身改错"：

- `test_b64_round_trips_every_length`：**长度 0..=64 全扫**，一个都不挑。为什么必须全扫：这类 bug 按
  `len % 3` 分档，抽样断言等于没测 —— 旧的那条 `test_b64_known` 唯一的解码断言是长度 3（`"Zm9v"` → `"foo"`），
  正好是唯一能过的那一格，所以它一路绿到今天。
- `test_b64_decode_rejects_illegal_length`：`4k+1` 个字符仍然要报错（防"为了往返一致把长度检查一并删掉"），
  并钉住两种合法尾组各自的字节数（`Zg==`→`f`、`Zm8=`→`fo`）。
- `test_jsonl_round_trip_does_not_collapse_keys`：灌 **300 把 11 字节的键**写出 JSONL 再读回来 ⇒ 行数、
  **不同键的把数**、每把的值都要对。这一条是"塌键"这一类的判据：长度扫描证的是编解码本身，
  而现场的损失是**两个错合成一个对**（键截短之后互相覆盖），只有"灌 N 把不同的键、回来还是 N 把"抓得住。

变异验证：把旧的"按 pad 回头裁剪"装回去 ⇒ 三条新守卫**全红**（红在"长度 2 解回 1 字节，差 1"、
"读回来只剩 …"、`Zm8=` 那一格），而**旧的 `test_b64_known` 依然绿** —— 这条对比就是"抽样断言在按余数
分档的编码上等于没测"的正面证据。

端到端复跑（真命令、真输出，不是单测）：

```text
memory-export-kv --demo 300 --out m45.jsonl --format jsonl   ⇒ SCAN 301 keys
bitcask-import-kv --input m45.jsonl --dir m45_db --format jsonl ⇒ IMPORT records=301 / VERIFY live_keys=301
bitcask-export-kv --dir m45_db --out m45_out.jsonl             ⇒ SCAN 301 keys
```

⚠️ **影响面（下游要读的这句）**：只伤 **JSONL** 这条路 —— `import_kv --format jsonl` 以及任何自己调
`read_jsonl` 的代码。**bin 格式（`BPEXP001`/`BPEXP002`）不过 base64，从来没中过**；`migrate`（走
`scan_page` + `set_raw`）与库内读写路径都不受影响。所以已经用 JSONL 搬过数据的人，要按"键可能被截短、
互相覆盖过"来复核一次那批库。

**M46 —— 裸 `LSMTree::merge_all_at` 的源改惰性：600k 档全库导出瞬时 100.4 → 0.4 MB（同一构建两臂），四家 `export_kv` 到这才算齐**

M44 之后四家 `export_kv` 里只剩 lsm 那家还在物化整库，原因是它走的不是门面而是**裸引擎入口**
`LSMTree::export_records_with_seq` → `merge_all_at`：归并本身从 M33 起就吃 `Box<dyn Iterator>`，
坏的是**源** —— 每个 memtable 走 `mem_view_at`（整表折进 `Vec`）、每个 SST 走
`iter_all().filter(...).collect()`（整篇折进 `Vec`），于是归还没开始，**所有源已经同时活着**。
这与 M39 在 MVCC 那道门（`scan_latest_light` 把每个源整篇物化 ⇒ `health()` 瞬时 +126.1 MB）治过的是
同一个形状，只是裸引擎这道门当时没走过。

改法（M39 那一招原样搬）：新增 `mem_iter_at`（惰性重量视图，判据与 `mem_view_at` 一字不差）与
`merge_inputs_at`（每源一路 `Box<dyn Iterator>`；**源顺序与"每源按键升序"这两条取胜前提一字未改**），
`merge_all_at` 变成 `merge_latest_at` 的**收集壳**，其上再开 `export_records_with_seq_for_each`，
`lsm_tree/bin/export_kv.rs` 接上 M44 的 `ExportWriter`（带 seq 的形状走新增的 `push_record`，
编码仍然只有一份）。`mem_view_at` 删除，重量路径的探针移进 `mem_iter_at`（同一个计数器）。

同一构建两臂（`LSM_RAW_EXPORT_ROWS=600000`；改前那一臂是把 SST 那一支换回 `collect() + vec_inputs`
的临时测量代码，读完就摘，不属于产品）：

| 600k 行、1 个源、两臂都交出 600 000 行 | 瞬时峰值 | 耗时 |
|---|---|---|
| 改前（每个源整篇折进 `Vec`） | **100.4 MB** | 0.72 s |
| 改后（源惰性，逐行归并） | **0.4 MB** | **0.47 s** |

⇒ **省 100 MB（251×）**，而且**顺手快了 1.53×** —— 那次"先把源折完再归并"不只是内存账：
60 万次 `Vec` 扩容 + 每条一次克隆本身就是工作。⚠️ `merge_all_at` 另外五个调用点
（`iter` / `iter_at` / `entry_count` / `len` / `refresh_stats_cache`）共用这份收益；其中
`entry_count` 是 `merge_all_at(...).len()` —— 为一个数物化整库，与 M35 那刀里 `key_count`
同一个形状，**这一刀没顺手改**（记在队列里，别把它算进本条的收益）。→ **下一条 M47 做了**，
`iter` / `iter_at` 那两格仍是要 `Vec` 的形状（调用方要的就是整库）。

守卫 `test_merge_streams_sources_instead_of_materializing_them`（20 000 行 / 4 篇 SST，
尺子 = 已有的 `value_materializations()`：重量路径每出一条记一次）：
① **交出第一行之前物化 5 条**（= 源数量级，判据 ≤ 64）；② 同一把尺子在全库那一趟必须读到
≥ 20 000 条 ⇒ 给 ① 作证（不响的尺子读出来的"小数字"没有意义 —— M44 那条就栽在这里）；
③ 取胜规则没变：导出的行升序、互不相同，且与**逐把点查**一字不差（点查是另一条路径，M26 立的不变量）；
④ 回调一律拒绝时交出 0 行、物化仍是 5 条 ⇒ 收手传到了源那一头。
变异：把 `merge_inputs_at` 里 SST 那一支换回 `iter_all().collect()` + `vec_inputs` ⇒ ① 红在
"交出第一行之前已物化 20 000 条（库 20 000 行）⇒ 源又被整篇折进 `Vec` 了"。

**M47 —— LSM 三格"为一个数物化整库"改成只数不物化：600k 档 100.4 → 0.4 MB；顺带修掉一条先前就存在的少算**

M46 末尾自己记下那一格：`entry_count()` = `merge_all_at(u64::MAX).len()`，`len()` 的 dirty 路径和
`refresh_stats_cache` 也各折一趟整库 ⇒ **数一个数之前先把 60 万行连 value 折出来**（M35 那刀里
`key_count` 同一个形状，只是这三格住在裸引擎里、那把尺子照不到）。

改法：三条合成一条

```rust
fn count_kinds_at(&self, snap_seq: u64) -> (usize, usize, usize)  // (Put(Some), Put(None), Drop)
```

走 M39/M46 那条**惰性轻量归并**（`light_iter_range`，一行轻量记录只带 `seq` + 种类字节）⇒
**一行 value 都不克隆**。`entry_count` 取三者之和，`len` 的 dirty 路径取 `.0`，
`refresh_stats_cache` 取 `.0`/`.1`（`Drop` 两处都不计 —— 口径一字未动）。
⚠️ 合一份不只是省内存：那三格原本会各自漂移。

同一构建两臂（`LSM_COUNT_ROWS=600000`，只切 `LSM_COUNT_OLD_SHAPE=1` 就地重放改前那三行写法；
**两臂都在每一格之前把 `stats_dirty` 压回 true** —— 否则第二、三格读的是缓存，量的就不是那三扇门）：

| 600k 行 / 1 个源，两臂交出同样的三个数 | 瞬时峰值 | `EntryValue` 克隆 | 耗时（两轮 min） |
|---|---|---|---|
| 改前（三格各折一趟整库） | **100.4 MB** | **1 800 000 条** | 1.50 s |
| 改后（只数不物化） | **0.4 MB** | **0 条** | **1.01 s** |

⇒ **省 100 MB（251×）、快 1.49×**。两臂各两轮交错跑，峰值两轮都是 100.4 / 0.4 ——
内存读数没有墙钟那种 ±1.3~1.9× 的漂。台架 `lsm_counting_doors_peak`（`#[ignore]`，单线程手动跑）留在库里。

守卫 `test_counting_paths_do_not_materialize_values`（440 行 / ≥2 篇 SST，**三种赢家行都在场**：
341 条 `Put(Some)` + 79 条 `Put(None)` + 20 条 `Drop`。⚠️ `delete()` 写的是 `Put(None)`，第三种只有
`remove()` 造得出来 —— 少了它 `count_kinds_at` 里那一支 match 分支量不到）。两把尺子各钉各的：
① 与重量归并**逐类对拍**；② 三格合起来 `value_materializations()` 增量必须是 0，而同一个测试里
基准那一趟必须读到 541 ≥ 440 ⇒ 给 ② 作证。
变异 A（把 `count_kinds_at` 换回 `merge_all_at`）⇒ 红在"物化 1623 条"；
变异 B（把墓碑算进存活）⇒ 红在"轻量计数与重量归并不一致"。⚠️ **B 那一臂 `entry_count` 的总数照样是 440**
⇒ 只断总数抓不到分类漂移，必须逐类对拍（M45"两个错合成一个对"的同一课）。

顺带挖出来的一条**先前就存在**的少算（M47 把计数接到 `light_iter_range` 上才撞见的）：

```rust
let probe_high = to.unwrap_or(&[0xffu8; 8]);   // ← 改前
```

`to == None` 说的是"一直到库尾"，可这句拿一个**假的 8 字节全 `0xff` 上界**去跑 `may_overlap`
⇒ 一篇 `min_key` 比它更大的文件（键以 ≥8 个 `0xff` 开头且更长）被**整篇跳过**：少扫、少数、少收，
**不报错**。键是任意字节，任何"够大的填充值"都存在比它更大的真键。
影响面就是所有 `to = None` 的用户：`health()`（M39 的全库走查）、`key_count` / 精确 `total`（M35）、
`vacuum_bounded` 的一片（M29/M37）、`scan_latest_light`，加上 M47 新接的这三格。
**点读从来没中过**（`get_at` 用真键 `may_overlap(key, key)`）⇒ 现场的形状是"读得到、扫不到"。

修法：只在**真有上界**时做 min/max 双向剪；无上界就只剪"整篇在 `from` 之下"那一头
（`sst.max_key >= probe_low`）⇒ 全库走查（`from=None`）判据结果不变，开区间那趟不再误剪。
**不动磁盘格式、不动 API。** 守卫 `test_light_scan_without_upper_bound_keeps_files_past_the_old_sentinel`
（57 把键，其中 7 把 `[0xff;12]` 且单独成一篇 SST）：`entry_count` 必须等于重量归并的 57，
开区间走查必须交出 1+7 行。变异 C（装回合成上界）⇒ 红在 `left: 50, right: 57` —— 少的那 7 把
就是现场被整篇跳过的文件。

### 本轮全量复测（三档 20k / 200k / 600k × 四引擎 × 31 项，同一份构建，2026-10-19）判据：**单价倍数 = 原始比值 ÷ 期望量级**；`memory` 引擎那一列就是本机噪声地板（1.2~1.6×）。

下表只列**过地板**或**绝对值本身痛**的项（单位写全）。数据在 `C:` SSD 上量，换盘要重标。

⚠️ **`峰值` 那一列的定义自 M38 起才可信**：量具 `timed()` 以前取的是运行中的**进程高水位**
（`mem_probe::peak` 只增不减）⇒ 排在后面的引擎与操作一律报 `+0.0 MB`，哪怕它自己瞬时涨了几十 MB。
M36 那轮 lsm / bplus / memory 三家 `vacuum()` 峰值全是 0，就是这么来的 —— **那张表里 0.0 的读数
不代表"没涨"，代表"量不到"**。改成每段之前 `reset_peak()` 把高水位压回当前活着的字节之后，
`峰值` = 这一段自己的瞬时增量。

| 项（600k 档） | 实测 | 单价比 20k→600k | 判读 |
|---|---|---|---|
| ✅ LSM 一次压实的瞬时内存 | 旧 156.8 MB → **新 6.3 MB** | — | **已修(M33)**，见上一段 |
| ✅ bplus 事务灌行 | 改前 **114.2 µs/行**（bitcask 16.3 / lsm 25.2 / memory 4.3）｜ M34 之后同一形状两轮 **37.6 / 51.4 µs/行**（同两轮 bitcask 16.0 / 15.0） | 1.49× | **已修(M34)**：对 bitcask 的倍数从 **7.0× 降到 2.4~3.4×**（本机跨轮漂 ±1.4×，所以给两档数）；凭据是计数 —— 裸 insert 的页读/行 **2.94 → 0.00**、整节点解析/行 → 全命中。归因与**两条被推翻的猜想**见上面 M34 段 |
| ⚠️ bplus `vacuum()` 全库 | 改前 **12.27 s/次**（bitcask 5.79 / lsm 2.95 / memory 0.99）｜ M34 之后 **10.64 s**（同期 bitcask 5.82） | 2.12×（地板 1.16×）→ **1.83×** | ~~与上一条同源，一起归因~~ **M34 证明不同源**：写侧那三刀（下降走解析缓存 + 回填 + 只在真要改时克隆）**不给走查形状用** —— 把删除路径的 `read_node` 也改成走解析缓存，这一格反而 12.27 → **15.12 s**（同构建那一轮），退回后 10.64 s。剩下的每行常数在删除路径自己的字节账/重组上，**未修** |
| ⚠️→✅ `key_count 2B前缀` | M34 后两轮：bitcask 2.34 / 2.31 s、lsm 1.79 s、bplus 0.94 / 1.37 s、**memory 0.77 s** | ≈线性 | ~~**每行 2.75 µs 而同库 `scan` 只 1.5 µs** ⇒ 数键比扫全库还贵~~ **⚠️ 修订（M34 之后复测）**：memory 这轮 `key_count` = **1.28 µs/行**、同库 `scan` = **1.3 µs/行** ⇒ **同价**，那句"比扫全库贵"是拿跨轮漂 2.1× 的数比的，判据作废。留下的事实仍然成立：`key_count` 就是"走一遍全库 + 每行物化"，四家同形（`prefix_scan(prefix).len()`）⇒ 可省的是每行的键/值分配，不是复杂度。**✅ 已修(M35)：四家改走 `count_window`（只数不物化）⇒ 600k 档峰值 +123.7 MB → 0.6~0.8 MB；时间按本机噪声带（±25%）看待，没有可信的变化 —— 这一格卖的是内存** |
| ⚠️→✅ `search 首页`(要精确 total) | M34 后两轮：bitcask 2.41 / 2.37 s、lsm 1.83 s、bplus 1.40 / 1.48 s、memory 0.77 s | ≈线性 | 与上一条同一个常数（`免 total` 只要 **339~769 µs** ⇒ 差 **3 个数量级**，全在"数 total"那一趟）。这条是**用户面痛点**（首页 1.4 s），但省下来的是分配不是走查 ⇒ 见 M35 的重新估价。**✅ 已修(M35)：`total` 与"取那一页"拆成两步（数一遍不物化 + 窗口折叠），四家 `search_keys` 合成一份 ⇒ 峰值 +123.8 MB → 0.6~1.2 MB。⚠️ 我给的"预期 ~1.5× 时间"被打回：走查与取值才是大头，过期判据住在 value 信封里不读值判不了 ⇒ 时间不动，动的是瞬时内存** |
| ⚠️→✅ `vacuum()` 全库瞬时（600k 档，修好的量具） | lsm 101.2 →（M37b）**45.5** →（M41a+b）**18.7 MB**、bitcask 171.4 →（M37）115.7 →（M40）87.2 →（**M43**）**8.0 MB**、bplus →（M36+M37b）**0.9 MB**、memory →（M37b）**0.3 MB** | 2.08× | **M36 逐组判定 + M37/M37b 按块走查 + M40 merge 按块 + M41a 流式 flush + M41b memtable 真内存账 + M43 merge 不再另建索引**（块界 8192 行/键，四家各一条 `test_vacuum_deletes_in_bounded_chunks`、bitcask 另有 `test_merge_walks_key_dir_in_bounded_chunks` 与 `test_merge_rebuilds_index_by_rescanning_the_log`）把"跨全库的累加器"全清了。⚠️ 曾经那句"89.4 → 56.7 MB"是**坏量具**读数，M38 已作废。⚠️ 而这台的 **45.5 MB 是台架形状的读数**（636 425 行里只 35 424 行可删）；事故形状（60 万行全可删）改前是 **174.5 MB**，分段归因与两刀见 M41 那三段。⚠️ 87.2 → 8.0 是**跨构建快照**（同一把量具、不同二进制），因果凭据是 M43 那次**同构建** A/B 的 ④ 段 **87.9 → 12.6 MB**。 |
| ✅ 点读类（已知预算题） | lsm 乱序 46.2 µs/次、bplus 乱序 33.6 →（M34 后）**23.4 / 23.9 µs/次** | 覆盖率决定 | **决定权已在 M32 交出**（`set_cache_auto_max`），默认档不动。bplus 这一格顺带便宜了：灌行时回填进解析缓存的那份，点读也用得上 |
| ✅ 平的那几条 | bitcask 灌行 16.3 µs/行（1.28×）、四家 `一次全量 scan` 1.8~4.3 µs/行、`窗口 scan(1%)` 与 `reverse_scan` ≤2.3×、bulk 2.0~5.2 µs/行 | ≤地板 | 别去动 |

按"多痛"排出来的后续：~~**M34 = bplus 写路径与全库 vacuum 的每行常数**（一条根可能同拖两条）~~
**M34 已做，且"一条根同拖两条"这句被推翻**：写路径那一格便宜了 2~3 倍（灌行 114.2 → 37.6/51.4
µs/行），全库 vacuum **不同源** —— 把删除路径也照抄成"走解析缓存 + 回填"反而更慢（12.27 → 15.12 s），
退回后 10.64 s；走查形状的页不值得回填（与 `read_page_for_walk` 同一条理由）。
**M35 = 共用折叠件的每行常数**（`key_count` / `search 首页(要 total)` / `health()` 同源；
根因已定位：四家的 `key_count` 都是同一份 `prefix_scan(prefix).len()` —— 为数一个数把界内每行
的键与值都物化出来再丢掉。**⚠️ 但"数键比扫全库还贵"这句本轮复测被推翻**：memory 600k 档
`key_count` 0.767 s = **1.28 µs/行**，同库一次全量 `scan` **1.3 µs/行** ⇒ 两者**同价**（改前那轮
记的是 2.75 vs 1.5 µs/行，跨轮漂 2.1×，那条判据是拿漂移的数比的）。所以 M35 真正要买的不是
"比 scan 便宜"，而是"**数一个数不该把每行的值分配出来再丢**"——能省的是每行 2 次堆分配与值拷贝，
量级预计 1.5× 而不是 10×；判过期仍要读值，这条省不掉。要不要做按这个预期重排），
**⇒ M35 已做，而"预期 1.5× 时间"这句也被自己打回（第 6 次）**：时间按本机噪声带（±25%）看不出变化，
卖出的是**瞬时内存 —— 600k 档 +123.7~123.8 MB → 0.6~1.2 MB**（四家全中）。"判过期仍要读值"正是原因：
走查与取值是大头，M35 省的那笔本来就只记在内存账上。顺带把四家 `search_keys` 与四家
`scan_foreach` 的复制各合成一份（详见上面 M35 段）。
**M36 = bitcask 全库 vacuum 的 89 MB 峰值**（本轮复测仍 **+89.4 MB**，未动）
⇒ **M36 已做（结构层面，四家都改了）**：全库归组改成逐组流式。修好量具之后的同一档读数是
bplus **+3.9 MB** / memory **+4.0 MB** / lsm **+101.2 MB** / bitcask **+171.4 MB**（含 `merge()`）——
**曾经那句"89.4 → 56.7 MB"是坏量具的数，M38 已作废**，不许拿两把尺子的数相减。
剩下的记在 **M37 = 全库轮次的"存活行清单"与 blob 重写要一次握住全部存活大 value**
（挡住它的是借用：`iter_meta_from` 按 `&self` 借住 KeyDir，走查途中没法 `get_entry`（`&mut self`）
⇒ 要么把 KeyDir 与文件句柄拆成两个可分别借用的对象，要么把全库轮次内部按片循环 ——
后者会动 `full_sweep` 的门控（blob 重写与 merge 只在全库轮次做），是语义决定，不是收尾清理）。
**M38 修好量具后新排出来的内存账**（旧量具一律报 +0.0，所以之前看不见）：lsm `health()`
**+126.1 MB**、lsm 灌行 **+106.8 MB**、四家 `export`/`scan`/`migrate` 的整库物化 **+114.7~124.3 MB**
（⚠️ 修订（M44）：`migrate` 那一笔归错了 —— 实测 600k 档 +116.1 MB 是**目标 memory 库自己**的常驻，
读侧每片只攒页宽；`export`/`scan` 那两笔才是「返回整库的 `Vec`」，M44 已给流式出口）。
**（修订：前两条都落地了 —— `health()` = M39（126.1 → 0.1 MB），lsm 灌行 = M41b（同构建 A/B
77.0 → 45.6 MB，代价是灌行单价 +46%）；剩下那一族"整库物化"就是 M35 的靶子。）**
**M37 已做（bitcask 那一刀）**：按块走查把 `to_delete`/`keep` 从 O(全库) 压成 O(8192 行) ——
① 段 106.0 → **0.5 MB**、整段 171.4 → **115.7 MB**；剩下的 **④ `merge()` 115 MB** 与
**同根因的另外三家 `to_delete`**（lsm 101.2 / memory 4.0 / bplus 3.9 MB）各自另立。

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
| `KvEngine` | `begin` / `begin_bulk` / `vacuum` / `vacuum_bounded` / `health` / `set_vacuum_auto_slice` / `set_wal_checkpoint_limit` / `wal_checkpoint_limit` / `tuning` / `durability_profile` / `flush` / `checkpoint` / `merge` / `export_latest_visible` / **`export_latest_visible_for_each`**(M44,流式:折一行交一行,不必把整库摆进内存) |
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
| MemTable flush 阈值 | 4 MB / 65536 条 —— ⚠️ 那条 **MB 从 M41b 起按真内存算**（载荷账 + 条数 × 451 B）；改前只数载荷 ⇒ 小值/墓碑形状上从不触发 | LSM || L1 基准 / 层间倍率 | 2 MB / ×10 | LSM |
| L0 compact / 软限流 / 硬停写 | 4 / 8 / 20 个文件 | LSM |
| SST block 条目数 / 目标大小 | 64 条 / 512 KB —— ⚠️ M42 起「64 条」就是 `SstBuilder` 的缓冲上限（攒满一块就落盘，不再整张表攒着） | LSM |
| Bloom bits per key | 10(→ 7 个哈希) | LSM |
| 大 value 阈值 | 256 B / 256 B / 4096 B | B+树 / Bitcask / LSM |
| 默认 order / 页缓存 | 64 / 1024 页 | B+ Tree |
| WAL 单帧 body 上限 | 16 MB | 全部 |
| **MVCC 档**（`LSMTreeOptions::for_mvcc`，与上面裸 LSM 默认值**不同**）| mem 32 MB / 262144 条（M41b 起那句 32 MB 按**真内存**算 ⇒ 墓碑形状约 7 万条就 flush，条数那档不再是兜底的那个）、L0 8/12/32、目标 SST 2 MB、块缓存 256 **地板** + 自动档上限 1024（生效值读 `tuning()` 的 `block_cache_capacity`）、`enable_mem_wal=false`、`enable_bg_compact=false` | LSM |
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
- **重扫窗口**（M43 引入的那段）：`merge()` 不再另建一把索引，而是先把内存索引**腾掉**、再原地重扫新日志。腾掉到扫完之间那段窗口里，磁盘上全库完好、内存里没人知道有哪些键 ⇒ 每一扇**能报错**的门（`get` / `get_entry` / `range_scan` / `iter` / `set` / `insert` / `insert_batch` / `delete` / `remove` / `dump_hint`）一律报错，不许回 `Ok(None)`（那等于宣称"这些键不存在"）。
  ⚠️ 正常那一次 merge 里没人看得见这个状态：门面把 `kv` 锁在同一把 `Mutex` 里、`merge()` 全程持锁 ⇒ 并发请求只是阻塞。它只服务一种现场：**重扫失败、`merge()` 带着 Err 返回** ⇒ 引擎被隔离，出路只有重新 open（`vacuum()` 从那一步起不再吞 merge 的错）。
  ⚠️ 不带 `Result` 的那几扇门（`len` / `is_empty` / `scan` / `scan_prefix` / `iter_meta` / `iter_meta_from` / `range_meta_back`）改不了签名，隔离期只能看到"空索引"⇒ 它们不删、不写，但会把"库是空的"当真话。**这就是 B1 的代价**（另一案 B2 是每 8192 把键一次 fsync，机械盘上 600k 档 ≈ 146 次 × 85 ms = +12 s）。
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
- ✅ ~~同一条不对称在自家工具里也成立（M44 的往返台架抓到）~~ **已修（M45），而且 M44 那条"嫌疑"是错的**：
  现场是 `memory-export-kv --demo 300` 造 301 行 jsonl，`bitcask-import-kv` 报 `IMPORT records=301` 而库里
  只剩 **32 把键**。M44 当时写成"bulk 的 `put` 要已打信封的字节"—— 复现推翻了两点：bulk 灌 300 行裸字节
  四家全对（`live_keys=300 导出=300 点读命中=300`），且 `BulkLoader::put` 自己就打信封。
  **真因是 `common::kv_snapshot::b64_decode` 在非 3 倍数长度上系统性掉 1~2 字节**（尾组已经按字符数出过
  字节了，又按 `=` 的个数回头裁一次），于是 `demo:%06d`（11 字节）全被截成同一把键 ⇒ 互相覆盖。
  修完端到端 **301 → 301 → 301**。**影响面**：只伤 JSONL 这条路（`--format jsonl` 的导入 / 任何自己调
  `read_jsonl` 的下游）；**bin 格式（BPEXP001/002）不过 base64，从来没中过**，`migrate` 也不受影响。
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
cargo test                    # 全部:322 单元 + 4 集成 + 5 文档测试
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
LSM_COUNT_ROWS=600000 cargo test --release --lib -- --ignored --nocapture --test-threads=1 lsm_counting_doors_peak
LSM_COUNT_OLD_SHAPE=1 LSM_COUNT_ROWS=600000 cargo test --release --lib -- --ignored --nocapture \
  --test-threads=1 lsm_counting_doors_peak
                              # M47 的三格计数内存台架:同一个二进制只切 `LSM_COUNT_OLD_SHAPE`,
                              # 改前那一臂就地重放"每格各折一趟整库"的写法(峰值 100.4 → 0.4 MB)
cargo test --test test_lsm_tree
```

门面层的测试是**四个引擎跑同一套断言**的,覆盖增删查、批量与扫描、写冲突、bulk 导入、TTL 与元数据、vacuum 与 flush、`health()` 读数一致、自动回收默认开着且能被关掉 —— 这也是验证「四引擎行为一致」的地方。换引擎只改变涨速,不变的是要盯的那些指标,所以这一层必须按「四家同尺度」断言。

集成测试会在 `./data/<引擎名>/` 下建演示库,不污染仓库根目录。

---

## 开发环境

- Rust 1.92+(edition 2024)
- 跨平台:Windows / Linux / macOS。文件锁在 Windows 用 `share_mode(0)`,Unix 用 `flock`

release profile 开了 `lto = true` 和 `codegen-units = 1`,压测请务必用 `--release`。
