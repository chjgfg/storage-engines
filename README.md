# storage-engines

四个从零手写的 KV 存储引擎 + 一层统一 trait 门面。**教学 / 学习性质**,不是生产级数据库。

同一套 MVCC 事务语义(快照隔离、写冲突检测、vacuum 回收)跑在四种截然不同的存储结构上,可以直观对比 **LSM Tree / B+ Tree / Bitcask / 纯内存** 在读写放大、范围扫描、崩溃恢复上的取舍。

- 约 2.3 万行 Rust,edition 2024(需要 rustc ≥ 1.85)
- 依赖只有 5 个:`serde` / `bincode` / `memmap2` / `fs4` / `log` —— B+ 树、SST、Bloom、WAL、双写缓冲、CRC32 全部自己实现
- 230 个单元测试 + 4 个集成测试
- 当前版本 **0.4.0**:随 uptime 恶化的七条通道逐条堵死,版本回收改为自动 —— 破坏性变更与升级要点见 [CHANGELOG.md](CHANGELOG.md)
- 许可证:MIT 或 Apache-2.0 任选

---

## 安装

```toml
[dependencies]
storage-engines = "0.1"
```

命令行工具单独装:

```bash
cargo install storage-engines
```

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

换引擎只改一个字符串,下面的代码一个字不用动:

```rust
use storage_engines::engine::open_engine;
use std::path::Path;

let db = open_engine("lsm", Some(Path::new("./data/lsm")))?;

let tx = db.begin();
tx.set(b"key", b"value".to_vec());
tx.commit();
```

引擎名大小写不敏感,`-` 和 `_` 等价,并接受别名:

| 规范名 | 别名 | 需要路径 |
|---|---|---|
| `bitcask` | — | 是 |
| `bplus_tree` | `bplus` / `btree` | 是 |
| `lsm_tree` | `lsm` | 是 |
| `memory` | `mem` | 否 |

---

## 项目结构

```
src/
├── engine.rs          统一门面:KvEngine / KvTxn / KvBulk 三个对象安全 trait
├── common/            四引擎共享:字节级文件工具(disk)、版本键与 vacuum 判定(version)、
│                      事务内核共用件(kernel)、TTL/快照编解码
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

```rust
let mut cursor = None;
loop {
    let round = mvcc.vacuum_bounded(cursor.as_deref(), 100_000)?;
    // round.stats.rows_scanned / versions_removed / round.next_cursor
    match round.next_cursor { Some(c) => cursor = Some(c), None => break }
}
```

`vacuum()` 就是 `budget_rows = u64::MAX` 的一轮,两者共用同一份保留规则。截断只发生在**逻辑 key 边界**:半条链不能收,因为「留哪一版」的判据要求整组版本都在手上 —— 所以一片实际读的行数可能超出预算,超出量 = 那一条链的长度。这是本设计明确接受的代价(也是游标前进的下界:每轮至少前进一组)。

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
| `kernel.rs`:可见性判据 `visible_at`、`HealthFold`、`VacuumScheduler`(含"取账不能 swap")、`is_unfinished` | 有界遍历的**早停与起点**:谁能反向走、谁只有单向 `next_leaf`、谁的窗口要按源封顶 |
| `types.rs`:`MvccHealth` / `VacuumStats` / `VacuumRound` / `DurabilityProfile` | 各自的守卫测试:同一条通道在不同引擎的**量尺不同**(读盘次数 / value 解码次数 / 物化次数 / 克隆次数),拿一把尺量不到另外三家 |

判据、记账、折叠这些纯逻辑一旦复制,就会长成"一个 bug 四个实例"—— 自动回收那回我把 `swap`
当成取账写进了四份代码,四家一起静默停摆。所以合的是**判定与记账**,不是扫描。

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
| `KvEngine` | `begin` / `begin_bulk` / `vacuum` / `health` / `set_vacuum_auto_slice` / `durability_profile` / `flush` / `checkpoint` / `merge` |
| `KvTxn` | 读写 / 批量 / 扫描 / 搜索分页 / TTL / `commit` / `rollback` |
| `KvBulk` | 批量导入,无冲突检测、不写 WAL |

对象安全的代价是:所有方法都用 `&[u8]` / `Vec<u8>`,不能用 `impl AsRef<[u8]>` 这类泛型参数。

四个引擎的 MVCC 层方法名本来就一致,所以用一个 `impl_engine!` 宏生成全部适配代码,**引擎差异做成开关参数**而不是写四遍:

```
vacuum:  io | plain     // memory 的 vacuum 不返回 io::Result
durable: yes | no       // memory 没有 flush / checkpoint
path:    db_dir | db_path | none
merge:   yes | no       // 只有 bitcask 有
```

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
- 事务不是 `Send`/`Sync` 友好的多线程设计

**LSM**
- `fix_level_overlaps` 发现层内 key range 重叠**只打印警告,不修复**
- compact 时每个 key 只保留最新一条,`enable_snapshot_gc` 并未真正按 snapshot 水位保留旧版本(源码注释已自陈)
- `blobs/` 旁路的回收挂在 `vacuum()` 的全库轮次上(compact 判不了"别的层里还有更新的版本引用同一个 blob");**有界轮次不动 blob**,引用关系只有全库才看得全
- `start_bg_compact` 只建控制块不起线程,真正的后台线程要显式调 `spawn_bg_compactor`

**Bitcask**
- 数据记录**没有 CRC**,存在静默损坏风险(blob 和 WAL 有 CRC)
- merge **不是原子的**(删旧文件与 rename 新文件之间有时间窗),但**可恢复**:崩在改名之前留下的 `data.merge` 会在下次 open 被认回(见 `durability_profile().merge_artifact_recovery`)。改名顺序不能反过来 —— 旧 active 句柄还挂着时 Windows 的共享锁会挡住覆盖
- 全量 key 常驻内存,key 数量受内存限制

**B+ 树**
- blob GC 是简化版:只在完全无引用时清空,有引用则暂不重写(append-only 空间残留)
- `page_id` 在页头里被截断成 u32

**其他**
- ~~`FileLock` 在三个引擎里各有一份近乎逐字重复的实现~~ → 已收成 `src/common/disk.rs` 一份(`DiskFile` / `FileLock` / `crc32` 三样都是崩溃语义的载体,`diff` 实测原先 bitcask 与 lsm 的 FileLock 差 0 行)。各引擎只保留自己的**锁文件路径规则**(dir/bitcask.lock、`<stem>.lock`、dir/lsm.lock)
- 三份 `wal.rs` 仍是各自一份(逐字重复度约 95%),尚未合并
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
cargo test                    # 全部:230 单元 + 4 集成 + 2 文档测试
cargo test --lib engine::     # 只跑门面层(14 个,四引擎全覆盖)
cargo test --test test_lsm_tree
```

门面层的测试是**四个引擎跑同一套断言**的,覆盖增删查、批量与扫描、写冲突、bulk 导入、TTL 与元数据、vacuum 与 flush、`health()` 读数一致、自动回收默认开着且能被关掉 —— 这也是验证「四引擎行为一致」的地方。换引擎只改变涨速,不变的是要盯的那些指标,所以这一层必须按「四家同尺度」断言。

集成测试会在 `./data/<引擎名>/` 下建演示库,不污染仓库根目录。

---

## 开发环境

- Rust 1.92+(edition 2024)
- 跨平台:Windows / Linux / macOS。文件锁在 Windows 用 `share_mode(0)`,Unix 用 `flock`

release profile 开了 `lto = true` 和 `codegen-units = 1`,压测请务必用 `--release`。
