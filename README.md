# storage-engines

四个手写 Rust KV 引擎，共享 MVCC 快照隔离事务和统一的 `KvEngine` 门面，可通过枚举或配置选择引擎。

当前发布版本：**[0.5.2](https://crates.io/crates/storage-engines/0.5.2)**。本文按该版本的实际接口整理，文档更新日期为 2026-11-24。项目定位为教学与存储实现研究；用于实际业务时，应结合下面的持久化差异、错误处理和维护成本做选择。

[API 文档](https://docs.rs/storage-engines/0.5.2/storage_engines/) · [更新日志](CHANGELOG.md) · [源码职责索引](docs/module-layout.md) · [压测手册](docs/大规模压测手册.md)

## 阅读入口

- [选择引擎](#选择引擎)：适用场景、持久化差异和当前建议。
- [接入 Rust 项目](#接入-rust-项目)：依赖、完整写读示例、启动与停机。
- [事务与常用操作](#事务与常用操作)：错误处理、删除、TTL、分页、搜索、结构体存储。
- [打开方式与配置](#打开方式与配置)：路径、环境变量、缓存与 WAL 参数。
- [长跑维护](#长跑维护)：逻辑回收、检查点、文件整理和维护线程。
- [离线批量导入](#离线批量导入)：bulk 的使用条件及值编码。
- [备份恢复与引擎迁移](#备份恢复与引擎迁移)：CLI、文件 API、覆盖与补齐语义。
- [终端与浏览器工具](#终端与浏览器工具)：shell、Web 和演示入口。
- [批量压测](#批量压测)：五档命令、结果文件、验收口径和单引擎工具。
- [开发验证与资料索引](#开发验证与资料索引)：编译检查、测试、当前权威记录和历史存档。

## 选择引擎

| 引擎 | 适合的用途 | 内存与磁盘特点 | 持久化边界 |
|---|---|---|---|
| `bitcask` | 持仓、策略状态、事件进度等点读写 | 全部键索引驻内存，值追加到日志；历史文件需要整理 | 事务 WAL；数据日志本身缺少记录校验和 |
| `lsm_tree` | 历史数据、范围读取、键索引不宜全部驻内存的场景 | 内存表、SST 和块缓存；flush 与压实会产生同步写入成本 | 事务 WAL；页外大值文件的完整性保护有局限 |
| `bplus_tree` | 有序访问、范围读取，且可以接受页缓存和写回成本 | B+ 页树、页缓存、双写缓冲；回收不保证库文件缩短 | 事务 WAL 与页双写；当前大库同步成本较高 |
| `memory` | 可重建缓存、测试、临时计算 | 数据常驻内存；可选启停快照 | 没有逐事务 WAL；正常停机快照不提供运行期持久提交 |

对于单核 Linux 长跑交易系统，**状态库默认建议从 Bitcask + MVCC 接入**；历史与范围数据可以单独采用 LSM。先核算 Bitcask 键索引的内存占用，再决定是否把较大的数据集放到 LSM。

四个引擎的事务 API 相同，磁盘格式不同。换配置名不会自动迁移数据。可用 `db.durability_profile()` 查看恢复构件，用 `db.tuning()` 核对当前参数。这些能力不等于已经覆盖全部断电、介质损坏与生产负载验证。

最新五档压测中，20M B+ 全库 vacuum 仍耗时约 **225.013 秒**，因此目前不优先推荐它作为高频状态写入的基座。详细结果见 [当前性能账本](docs/performance-m162.json)，不要把 memory 的低延迟直接当作持久化引擎的替代收益。

## 接入 Rust 项目

### 添加依赖

在你自己的项目 `Cargo.toml` 中加入：

```toml
[dependencies]
storage-engines = "0.5.2"
```

如果要固定这次已经验证的版本，写成 `storage-engines = "=0.5.2"`；普通的 `"0.5.2"` 允许 Cargo 选择兼容的后续版本。应用项目应保存 `Cargo.lock`。

当前仓库使用 edition 2024，验证工具链为 Rust **1.92.0**；尚未声明或验证最低支持 Rust 版本。默认功能包含库和 shell 所需依赖；Web 工具需要额外启用 `web` feature。

### 一个完整的状态库示例

把以下内容放到你自己项目的 `src/main.rs`。首次运行创建库，再次使用同一路径会恢复已提交数据：

```rust
use std::{path::Path, println};
use storage_engines::{open_engine, EngineKind, KvEngine};

fn read_position(db: &dyn KvEngine) -> Option<Vec<u8>> {
    let tx = db.begin();
    let value = tx.get(b"position:BTCUSDT");
    tx.rollback(); // 只读事务结束，释放快照。
    value
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "./db/trading".to_owned());

    // 启动时打开一次；再次使用同一目录会恢复已提交的数据。
    let db = open_engine(EngineKind::Bitcask, Some(Path::new(&dir)))?;
    println!("启动时的持仓：{:?}", read_position(db.as_ref())
        .map(|v| String::from_utf8_lossy(&v).into_owned()));

    {
        let tx = db.begin();
        // 同一次业务处理中的状态和事件编号一起提交。
        for (key, value) in [
            ("position:BTCUSDT", "0.025"),
            ("strategy:last_event_id", "10001"),
        ] {
            if let Err(e) = tx.try_set_raw(key.as_bytes(), value.as_bytes().to_vec()) {
                tx.rollback(); // 写入失败，撤销本事务已经写入的部分。
                return Err(e.into());
            }
        }
        // 成功返回后才确认业务落库；失败时交给上层区分提交结果。
        tx.try_commit()?;
    }

    println!("提交后的持仓：{:?}", read_position(db.as_ref())
        .map(|v| String::from_utf8_lossy(&v).into_owned()));

    // 正常停机时，结束全部事务后做一次检查点。
    db.checkpoint()?;
    Ok(())
}
```

在你自己的项目中执行：

```powershell
cargo run --release -- ./db/trading
```

首次启动的持仓为 `None`，提交后为 `Some("0.025")`；第二次启动应直接读到上次提交的 `Some("0.025")`。

库句柄应在启动时打开并长期持有。需要跨线程共享时可使用 `Arc<dyn KvEngine>`；事务是 `Send`，没有 `Sync`，一个事务应由一个业务执行流程持有。全部接口都是同步调用，在异步应用中要安排到可以阻塞的执行线程。

## 事务与常用操作

### 提交结果必须区分

业务写入推荐 `try_set_raw` / `try_delete` / `try_commit`。兼容接口 `set`、`delete` 返回布尔值，`commit()` 在提交失败时可能 panic，不能直接拿来做可靠的错误分支。

| 返回结果 | 应用如何处理 |
|---|---|
| 写入返回 `WriteError::Conflict` 或其他写入错误 | 回滚当前事务；若业务允许重试，另开事务重新执行 |
| `CommitError::WriteFailed(..)` | 回滚；按具体写入错误决定是否另开事务重试 |
| `CommitError::Indeterminate { .. }` | 提交结果不确定；停止依赖该结果的后续业务，关闭全部相关句柄并重开恢复，用事件 ID 对账 |
| `CommitError::CommittedMaintenance { .. }` | 事务已提交，后续检查点等维护失败；处理维护故障，不能重复执行该笔业务写入 |
| `TransactionClosed` / `Unsupported` | 处理事务生命周期或能力错误，不能当作暂时冲突无限重试 |

没有显式结束的活动事务在 Drop 时回滚。提交结果不确定的事务不会靠 Drop 自动撤销；此时不能盲目重试。完整示例将错误返回给上层，实际交易程序应在上层实现上表中的分支。

只读事务读完后调用 `rollback()`，释放快照并避免走提交同步路径。长事务会阻挡历史版本回收。

### 读取、写入和删除

`get(key)` 返回 `Option<Vec<u8>>`，得到的是剥去内部信封后的用户字节。键不存在、已删除或过期时返回 `None`。

```rust
use storage_engines::KvEngine;

fn remove_position(db: &dyn KvEngine) -> Result<(), Box<dyn std::error::Error>> {
    let tx = db.begin();
    if let Err(e) = tx.try_delete(b"position:BTCUSDT") {
        tx.rollback(); // 删除失败时撤销当前事务。
        return Err(e.into());
    }
    tx.try_commit()?;
    Ok(())
}
```

多键原子写入按完整示例循环调用 `try_set_raw`，最后提交一次。批量读取可用 `multi_get(&[Vec<u8>, ...])`，返回顺序与输入键一致。

**普通用户字节不要交给 `set`、`try_set` 或 `batch_set`。**这些历史接口接收已封装的逻辑值；以 `0x00` / `0x01` 开头的裸值可能被解释为内部信封。`batch_set` 失败也不会自行撤销已经写入的前半部分。

### TTL

`set_with_ttl` 接收用户原始字节，返回布尔值，必须检查失败。TTL 按秒计，零时长按 1 秒处理：

```rust
use std::time::Duration;
use storage_engines::KvEngine;

fn save_session(db: &dyn KvEngine) -> Result<(), Box<dyn std::error::Error>> {
    let tx = db.begin();
    if !tx.set_with_ttl(b"session:demo", b"token".to_vec(), Duration::from_secs(60)) {
        tx.rollback(); // 不提交已经被拒绝的业务写入。
        return Err(std::io::Error::other("写入带 TTL 的会话失败").into());
    }
    tx.try_commit()?;
    Ok(())
}
```

`get_ttl(key)` 有三种结果：`None` 表示不存在或已过期，`Some(None)` 表示永久有效，`Some(Some(duration))` 表示剩余寿命。过期后的逻辑不可见与物理空间回收是两回事。

### 范围扫描与游标分页

`scan(start, end)` 使用左闭右开区间 `[start, end)`，`None` 表示该端无界。`scan`、`prefix_scan` 和 `reverse_scan` 会返回全部命中结果；大库遍历优先用 `scan_page`：

```rust
use storage_engines::{ExportRecord, KvEngine};

fn visit_rows(db: &dyn KvEngine, mut visit: impl FnMut(ExportRecord)) {
    let tx = db.begin();
    let mut cursor: Option<Vec<u8>> = None;
    loop {
        let (rows, next) = tx.scan_page(cursor.as_deref(), 1000);
        for row in rows {
            visit(row); // 即时消费当前页，避免自己再积累整库数据。
        }
        match next {
            Some(key) => cursor = Some(key),
            None => break,
        }
    }
    tx.rollback(); // 整次遍历结束后释放同一快照。
}
```

`after` 是排他游标，下一页原样传回 `next`；`None` 表示遍历完成。上例在一个事务中遍历，提供同一快照，但可能长期阻挡回收。若按页结束事务并另开下一页，则事务更短，跨页不再保证固定快照。

`limit` 限制行数，不是返回字节数或耗时硬上限；一条大值或很长的历史版本链仍可能产生较大成本。`seek` 和 `seek_prev` 可用于相邻键定位。

### 搜索

搜索匹配的是键。默认返回精确总数；只要当前页时使用 `without_total()`：

```rust
use storage_engines::{KvEngine, SearchPage, SearchQuery};

fn first_position_page(db: &dyn KvEngine) -> SearchPage {
    let tx = db.begin();
    let query = SearchQuery::prefix("position:", 0, 100).without_total();
    let page = tx.search_keys(&query);
    tx.rollback();
    page
}
```

`SearchQuery::contains("BTC", 0, 100)` 是包含搜索，页码从 0 开始。精确总数通常需要遍历整个匹配范围；免总数时 `total` / `total_pages` 为 `None`，仍可使用 `has_next`。

页码是 offset 分页，深翻页要跳过前面的记录；连续遍历优先使用游标。`search_last_page(page_size)` 可以免总数取末页，不代表任意中间页都能直接定位。

### 存储结构体

引擎只存字节，序列化格式由业务约定。例如在你的 `Cargo.toml` 中另外添加：

```toml
serde = { version = "1.0", features = ["derive"] }
bincode = "1.3"
```

下面的函数接收已经打开的库：

```rust
use serde::{Deserialize, Serialize};
use storage_engines::KvEngine;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Position {
    lots: i64,
    last_event_id: u64,
}

fn save_and_read(db: &dyn KvEngine) -> Result<Position, Box<dyn std::error::Error>> {
    let value = Position { lots: 25, last_event_id: 10001 };
    let bytes = bincode::serialize(&value)?;
    let tx = db.begin();
    if let Err(e) = tx.try_set_raw(b"position:encoded", bytes) {
        tx.rollback();
        return Err(e.into());
    }
    tx.try_commit()?;

    let tx = db.begin();
    let bytes = tx.get(b"position:encoded");
    tx.rollback();
    let bytes = bytes.ok_or_else(|| std::io::Error::other("提交后的持仓不存在"))?;
    Ok(bincode::deserialize(&bytes)?)
}
```

长期保存的数据应自行定义结构版本和升级规则，避免修改结构体后无法解释旧数据。键按字节排序；如需按数值或时间做范围扫描，可使用固定宽度、大端编码或固定宽度文本键。

## 打开方式与配置

### 路径要对应引擎

`open_engine(kind, path)` 直接使用给出的路径，不自动补引擎目录：

| 参数 | 实际用途 |
|---|---|
| `EngineKind::Bitcask` + `./data/bitcask` | Bitcask 库目录 |
| `EngineKind::LsmTree` + `./data/lsm_tree` | LSM 库目录 |
| `EngineKind::BPlusTree` + `./data/bplus_tree/bplus.db` | B+ 库文件；父目录应先创建 |
| `EngineKind::Memory` + `None` | 纯内存库 |
| `EngineKind::Memory` + 一个目录 | 正常关闭保存、重开恢复整库快照，仍无事务 WAL |

也可以传字符串 `"bitcask"` / `"lsm_tree"` / `"bplus_tree"` / `"memory"`；常用别名包括 `"lsm"` / `"bplus"` / `"btree"` / `"mem"`。不认识的名称返回错误。

### 使用环境配置

`open_from_env()` 读取 `STORAGE_ENGINE`（默认 `bitcask`）和 `STORAGE_DATA_DIR`（默认 `./data`）。它通过 `StorageConfig` 拼接 `<根目录>/<规范引擎名>/`，B+ 再追加 `bplus.db`；memory 始终按纯内存打开。

```powershell
$env:STORAGE_ENGINE = 'bitcask'
$env:STORAGE_DATA_DIR = './data'
```

Linux shell 中对应写法：

```bash
export STORAGE_ENGINE=bitcask
export STORAGE_DATA_DIR=./data
```

```rust
use storage_engines::{open_from_env, EngineError, KvEngine};

fn open_configured_db() -> Result<Box<dyn KvEngine>, EngineError> {
    open_from_env()
}
```

上述 Bitcask 实际目录是 `./data/bitcask`。环境配置与直接传路径是两个入口，应用中统一使用一种约定。

### 调整参数

通常先用 `EngineConfig::default()`。自定义 LSM 参数应从 `LSMTreeOptions::for_mvcc()` 出发：

```rust
use std::path::Path;
use storage_engines::{
    open_engine_with, EngineConfig, EngineError, EngineKind, KvEngine, LSMTreeOptions,
};

fn open_history(path: &Path) -> Result<Box<dyn KvEngine>, EngineError> {
    let config = EngineConfig {
        lsm: Some(LSMTreeOptions {
            block_cache_blocks: 1024,
            ..LSMTreeOptions::for_mvcc()
        }),
        cache_budget_bytes: Some(32 * 1024 * 1024),
        ..EngineConfig::default()
    };
    open_engine_with(EngineKind::LsmTree, Some(path), &config)
}
```

| 字段 | 生效范围 | 默认或需要注意的语义 |
|---|---|---|
| `order` | B+ | 默认 64；已有库应保持建库时的树参数 |
| `max_cache_pages` | B+ | 默认 0，表示自动档，不是零缓存 |
| `lsm` | LSM | 默认 MVCC 档；内层 `enable_mem_wal` / `enable_bg_compact` 被固定为 false |
| `bitcask_max_file_bytes` | Bitcask | 默认 64 MiB，控制 active 分片的软上限 |
| `wal_checkpoint_bytes` | 三个磁盘引擎 | 默认 16 MiB；`Some(0)` 关闭阈值检查点；给 memory 设置会报错 |
| `vacuum_auto_slice_rows` | 四家 | 默认 16 Ki 版本行；`Some(0)` 关闭自动切片回收 |
| `cache_auto_max` | B+ / LSM | 自动缓存档上限；给 Bitcask / memory 设置会报错 |
| `cache_budget_bytes` | B+ / LSM | 估算缓存容量预算；默认不设；给 Bitcask / memory 设置会报错 |

缓存预算不包含事务写集、扫描返回值、内存表、索引、blob 重写和操作系统文件缓存，**不是进程 RSS 或总内存的硬上限**。字段之间的实际效果用 `db.tuning()` 核对，详细契约见 [EngineConfig](https://docs.rs/storage-engines/0.5.2/storage_engines/struct.EngineConfig.html)。

## 长跑维护

普通 `open` 不创建维护线程，但默认逻辑切片回收与 WAL 阈值检查点已启用。回收切片可由后续 `begin` 触发，检查点可能发生在提交之后；它们都会带来同步操作成本。

| 操作 | 作用与限制 |
|---|---|
| `reclamation_status()` | 轻量观察版本积累和活动事务；适合日常监控 |
| `health()` | 扫描物理行统计；不要放热路径；`live_keys` 不能当成排除了 TTL / 墓碑的精确存活计数 |
| `vacuum_bounded(cursor, rows)` | 回收一个逻辑版本片；完整键组可能超过预算；返回游标到尾也不自动做全库收尾 |
| `vacuum()` | 全库逻辑回收；可能长时间持锁，各家的物理回收行为不同 |
| `merge()` | 仅 Bitcask 支持，用于重写文件；其他引擎返回 `Unsupported` |
| `flush()` | 刷底层数据；memory 为 no-op |
| `checkpoint()` | 先持久化数据，再封住并截断事务 WAL；memory 为 no-op |

长跑系统可把维护驱动接到已有的维护循环，先关闭自动全库收尾：

```rust
use storage_engines::{EngineError, Maintenance, MaintenanceConfig};

fn new_maintenance() -> Result<Maintenance, EngineError> {
    Maintenance::new(MaintenanceConfig {
        full_vacuum_interval: None, // 交易期间只做逻辑切片，盘后再安排重操作。
        ..MaintenanceConfig::default()
    })
}
```

长期持有这个驱动，在应用自己的维护时机调用 `driver.tick(db.as_ref())?`。`tick` 本身不按 `interval` 节流，手动调用频率由应用控制；有活动事务时会保留进度并跳过回收。

如需独立线程，可使用 `start_maintenance(Arc<dyn KvEngine>, config)`，必须保留返回句柄，正常停机先 `stop()` 再关闭库。启用维护器的默认全库间隔是 30 分钟；单核交易系统建议显式改为 `None`，在盘后自行安排全库操作。

盘后 Bitcask 可在结束事务后做 `vacuum()`、`merge()`、`checkpoint()`，逐项处理错误。B+ / LSM 不能照抄 `merge()`。这些调用没有延迟硬上限；B+ 回收页供复用不等于文件缩小。正常停机应结束事务、停止维护线程、完成检查点，再释放全部库句柄。

## 离线批量导入

正常在线业务使用普通事务。bulk 用于离线空库灌入：要求单写者、停止维护和并发读写，绕过事务 WAL 与冲突检查。

`KvBulk::put` 接收**已经封装的逻辑值**，普通用户字节要用 `pack_plain` 封装一次：

```rust
use storage_engines::{common::codec::pack_plain, EngineError, KvEngine};

fn import_offline(db: &dyn KvEngine) -> Result<(), EngineError> {
    let mut bulk = db.begin_bulk();
    // 二进制用户值也必须只封装一次，读回应保持开头的 0x00。
    bulk.put(b"binary:key", pack_plain(b"\x00binary"));
    bulk.put(b"position:BTCUSDT", pack_plain(b"0.025"));
    bulk.finish(); // 显式结束 bulk，再执行检查点。
    db.checkpoint()
}
```

`put_batch_owned` 的值编码约定相同。bulk 的 `put` / `finish` 没有 `Result` 错误通道，不能据此承诺失败原子性或每行成功；尤其 B+ 超大行可能被拒绝。导入后应关闭重开，逐键核对数量、值和 TTL。需要逐行处理错误时，使用 `try_set_raw` 普通事务。

## 备份恢复与引擎迁移

### 导出与恢复文件

以下命令在本仓库根目录执行。先停止业务写入并释放库句柄，再操作该库；导出文件路径由你指定：

```powershell
cargo run --release --bin export-kv -- --engine bitcask --dir ./data/bitcask --out ./state.bin --format bin
cargo run --release --bin import-kv -- --engine lsm_tree --dir ./data/lsm_tree --in ./state.bin --format bin
```

普通导入覆盖同名键，文件中没有的目标键不会被删除。恢复到新目录后要核对业务内容，再切换应用配置。

| 参数或格式 | 行为 |
|---|---|
| `--format bin` | 当前通用导出为 `BPEXP003`，带绝对到期时间；旧 `BPEXP001` / `BPEXP002` 仍可读取，无 TTL 的记录按永久有效 |
| `--format jsonl` | 一行一条；键和值用 base64，`expire` 保存到期时间；恢复时使用相同格式 |
| 导出 `--include-deleted` | 包含墓碑；默认只导出当前可见值，均不备份完整历史版本链 |
| 导入 `--skip-existing` | 只补目标没见过的键；已有值和墓碑均保留，不刷新已有 TTL、不传播源墓碑 |
| 导入 `--wipe` | 先清空目标库再导入；会删除目标数据，与 `--skip-existing` 互斥 |

补齐导入示例：

```powershell
cargo run --release --bin import-kv -- --engine lsm_tree --dir ./data/lsm_tree --in ./state.bin --format bin --skip-existing
```

导出工具的 `--demo N` 会向源库灌入样例，不要用于备份真实业务库。memory 的启停快照采用带 CRC 与计数的 `BPEXP004`，与这里的通用导出格式职责不同。

### 在程序中迁移

已经打开的库之间可以使用根门面函数：

```rust
use storage_engines::{migrate, EngineError, KvEngine, MigrateStats};

fn copy_database(
    source: &dyn KvEngine,
    target: &dyn KvEngine,
) -> Result<MigrateStats, EngineError> {
    migrate(source, target, 1000)
}
```

它按页读取源库、每页在目标普通事务提交，复制当前可见值并按剩余寿命换算 TTL。同名目标键会覆盖；当前页写入被拒时撤销该页，之前提交的页仍存在。源侧按页另开事务，持续写入期间不保证整库固定快照；切库迁移宜先停写。

当前根 `migrate` 内部使用兼容 `commit()`，提交 I/O 失败可能 panic；其 `Result` 不能代替前文的 checked 提交错误处理。需要对每页提交结果精细控制时，应自行用 `scan_page`、`try_set_raw` / `set_with_ttl` 和 `try_commit` 驱动迁移。

其他迁移入口的路径和行为如下，调用时不要混用：

| API 路径 | 路径与语义 |
|---|---|
| `storage_engines::tools::migrate(from, to, root, include_deleted, wipe_target)` | 拼接根目录下的规范引擎名；目标用 bulk；memory 不接受为源或目标 |
| `storage_engines::tools::migrate::sync(source, target)` | 已打开句柄间补齐，保留目标已见过的键与墓碑，并保持新记录的绝对到期时间 |
| `tools::migrate::{export_to_dir, import_from_dir, import_skip_existing_from_dir}` | 直接给引擎目录或 B+ 库文件，不自动补引擎名 |
| `tools::{export_to_file, import_from_file}` | 根目录型入口，内部补引擎名；与上面的 `*_from_dir` 不是同一路径约定 |

目录型工具打开 B+ 时使用 order 16 / cache 64，并兼容优先 `data.db`、其次 `bplus.db`；新库使用 `data.db`。如果自己的 B+ 库按别的参数创建，优先自己 `open_engine_with`，再使用句柄迁移。

导入、导出和迁移都不是业务停写、配置切换和内容核验的替代。bulk 导入的错误通道限制同样适用于使用 bulk 的文件导入与目录迁移。根目录型工具还会清理 B+ 目标旧库文件，即使 `wipe_target=false`；需要保留已有 B+ 数据时，使用已打开句柄或直接路径的导入入口。

## 终端与浏览器工具

### shell

在仓库根目录运行：

```powershell
cargo run --release --bin shell -- --engine bitcask --dir ./data/bitcask
```

进入终端后，可以依次输入：

```text
begin
set position:BTCUSDT 0.025
set strategy:last_event_id 10001
commit
get position:BTCUSDT
setex session:demo 60 token
ttl session:demo
prefix position:
find BTC 0 20
checkpoint
exit
```

`help` 查看命令。常用命令还包括 `del`、`mget`、`scan`、`seek` / `seekprev`、`rollback`、`info`、`health`、`vacuum` 和 `merge`。退出时未提交的挂起事务会回滚。

`--dir` 是实际库路径，B+ 要传库文件；`--fresh` 会清理演示库目录，不作为日常打开方式。

### Web 终端

```powershell
cargo run --release --features web --bin web -- --engine bitcask --dir ./data/web/bitcask --port 8083
```

在本机浏览器打开 [http://localhost:8083](http://localhost:8083)。服务绑定 `0.0.0.0`；它是没有认证的演示运维入口，应在受控网络中使用。启动命令显式选择了 Bitcask；Web 自身的默认引擎是 memory。

Web 复用 shell 命令解释器，但请求结束时会提交仍挂起的事务；不能跨 HTTP 请求保持同一事务。需要多条命令组成一个事务时，在一个请求中发送完整命令组。

### 完整演示

```powershell
cargo run --release --bin storage-engines -- bitcask ./data/demo-bitcask
```

主程序会演示写入与多种操作，使用专门的演示路径。无参数运行 `cargo run` 默认使用 memory；这是演示程序的默认值，与 `open_from_env()` 默认 Bitcask 不同。

## 批量压测

### 四个引擎、五档数据的正式命令

在你这台机器上，按原路径执行下面这一条即可；脚本由自身位置定位仓库，可从其他工作目录调用：

```powershell
& 'D:\develop\workspace\rust_workspace\storage-engines\scripts\run-scale-bench.ps1' -Rows '600000,1000000,5000000,10000000,20000000'
```

其他机器在仓库根目录执行：

```powershell
& ./scripts/run-scale-bench.ps1 -Rows '600000,1000000,5000000,10000000,20000000'
```

如果 PowerShell 执行策略拦住脚本，可以为这次进程指定策略，不修改系统设置：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/run-scale-bench.ps1 -Rows '600000,1000000,5000000,10000000,20000000'
```

需要 Windows PowerShell 5.1+ 和可用的 Cargo/Rust。默认离线构建；本机依赖尚未缓存时，先正常构建下载依赖，或显式增加 `-AllowNetwork`。默认 `-Repeat 1`，引擎和档位串行运行，任何构建或验收失败都会停止。

先验证脚本设施或跑小数据集时使用：

```powershell
& ./scripts/run-scale-bench.ps1 -SelfTest
& ./scripts/run-scale-bench.ps1 -Rows '2000,3000'
```

`-SelfTest` 只检查脚本设施，不代替引擎压测。小档用于验证入口与正确性，不外推 20M 的性能。

### 结果在哪里、如何判断完成

默认结果保存在 `data/bench-results/<时间-UUID>/`：

| 文件 | 用途 |
|---|---|
| `run.json` / `source.json` | 参数、构建、源码和可执行文件指纹 |
| `build.log` | 构建输出 |
| `rows_<行数>_pass_<轮次>.log` | 对应档位的完整输出，不截断诊断行 |
| 同名 `.csv` | 各引擎操作耗时、内存与附加信息 |
| 同名 `.diagnostics.json` | 原始诊断与累计计数差分，供归因和往返核验 |
| `summary.json` | 各档验收结果；失败时结合日志定位 |

默认专用测试数据位于 `data/bench-scratch`；脚本用 ownership marker 识别自己创建的测试库，成功后清理测试库并保留结果。不要把 `-ScratchRoot` 指向业务数据目录；与旧结果比较时尽量保持相同磁盘和参数。

正式台架覆盖四家同一组操作，检查迁移、bulk 正常关闭重开逐值核验和真实 bounded 删除。每档应完成 **132 格**，四家 bounded 都实际删除准备的旧版本，而不是删除 0 的空扫。以脚本成功退出、总结和日志证据共同判断完成。

正常关闭后重开没有清操作系统页缓存，不能叫真正冷启动或断电恢复。维护探针有嵌套计时，不能把全部耗时直接相加；具体口径见 [压测手册](docs/大规模压测手册.md)。

### 当前版本的验收状态

最近一次完整五档是 **2026-11-24**：600k / 1M / 5M / 10M / 20M 全部 PASS，共 660 格、20 次迁移、20 次 bulk 重开与 20 次真实 bounded 删除。原始目录为 `data/bench-results/20261124-123608-5ea79b86a6e946fb8e138abfe63bf3f7`，汇总记录见 [性能账本](docs/performance-m162.json)。

20M memory 精确首页由 17.604 秒降到 2.340 秒，LSM vacuum 由 48.393 秒降到 28.162 秒；全套总耗时却由 33488.603 秒增加到 35843.025 秒（+7.03%）。不能据局部收益宣称整体提速。本轮性能优化已收尾，以上命令供后续复现使用。

### 单引擎专项工具

下面使用独立测试路径。**Bitcask 与 LSM 工具启动时无条件清掉传入的测试目录，B+ 默认清测试库**，不要指向真实库：

```powershell
cargo run --release --bin bitcask-bench-bulk -- --n 10000 --dir ./data/single-bench/bitcask --batch 1000 --vlen 32 --flush-every 0
cargo run --release --bin lsm-bench-bulk -- --n 10000 --dir ./data/single-bench/lsm --batch 1000 --vlen 32
New-Item -ItemType Directory -Force ./data/single-bench/bplus
cargo run --release --bin bplus-bench -- --n 10000 --db ./data/single-bench/bplus/test.db --order 64 --cache 1024 --batch 1000 --value-size 32 --bulk
cargo run --release --bin memory-bench -- --n 10000 --bulk
```

四个工具参数不同：memory 不接受 `--dir`；B+ 使用 `--db` 文件路径，可用 `--mode` 选择工作负载、`--keep` 在运行结束后保留测试文件；`--keep` 不阻止下一次启动时清库。专项 bulk 结果不代替正式五档普通事务与完整正确性验收。

## 开发验证与资料索引

在仓库根目录执行常规验证：

```powershell
cargo check --all-targets --all-features
cargo test --lib -- --test-threads=1
cargo test --doc
```

少数大规模或专项对照测试是 ignored，需要按压测手册单独启动。不要在同一机器同时跑正式压测和其他大负载，再用其耗时作性能比较。

| 文档 | 权威范围 |
|---|---|
| [更新日志](CHANGELOG.md) | 发布范围与历次改动 |
| [源码职责索引](docs/module-layout.md) | 模块入口与职责导航 |
| [性能账本 M162](docs/performance-m162.json) | 本轮最终五档、性能结论、原始证据与限制 |
| [重构验收 M163](docs/refactor-m163.json) | 源码职责拆分、接口与格式保持、验收溯源 |
| [大规模压测手册](docs/大规模压测手册.md) | 正式脚本、诊断字段、专项复现与历史实验 |
| [README 用法验证](docs/readme-usage-052.json) | 本文 Rust 示例编译运行、工具小库往返和链接审计结果 |
| [旧 README 历史存档](docs/readme-history-20261124.md) | 整理前的 4356 行正文，保留历史过程；旧接口与阶段结论不作为当前指南 |

当前 README 只维护可操作的使用说明。历史调优细节留在更新日志、专项账本和旧 README 存档；新证据应更新对应的权威账本，避免在多个文档堆出互相矛盾的“最新结果”。

许可证：MIT OR Apache-2.0，见 [MIT](LICENSE-MIT) 和 [Apache-2.0](LICENSE-APACHE)。
