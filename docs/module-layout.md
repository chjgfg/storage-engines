# 源码职责索引

M163 将过长模块按功能拆分，并补充中文业务说明。原公开模块路径、类型、方法、压测入口和磁盘格式继续使用；实现文件采用普通 `mod`，原大文件负责类型定义与导出。

本轮验收的单一账本是 [refactor-m163.json](refactor-m163.json)，性能结论仍以 [performance-m162.json](performance-m162.json) 为准。

## 从哪里找代码

| 需要修改的功能 | 入口与实现目录 | 职责文件 |
| --- | --- | --- |
| 统一引擎接口、选择和配置 | [engine.rs](../src/engine.rs)、[engine/](../src/engine/) | `engine_api`、`txn_api`、`bulk_api` 定义接口；`selection`、`config`、`factory` 负责选择和打开；`adapters` 适配四个引擎；`migration` 负责库间迁移 |
| Bitcask 日志与索引 | [bitcask.rs](../src/bitcask/bitcask.rs)、[bitcask/](../src/bitcask/bitcask/) | 日志记录、KeyDir、hint、合并及生命周期分别维护；日志模块叫 `record_log`，避免与外部 `log` crate 混淆 |
| B+ 树结构 | [bplus_tree.rs](../src/bplus_tree/bplus_tree.rs)、[bplus_tree/](../src/bplus_tree/bplus_tree/) | 读取、扫描、插入、删除、重平衡、节点编解码、缓存和批量建树分别维护 |
| B+ 页与磁盘发布 | [page.rs](../src/bplus_tree/page.rs)、[page/](../src/bplus_tree/page/)、[storage/](../src/bplus_tree/storage/) | `page_io` 负责页读取、双写发布和同步故障封锁；`allocation`、`metadata`、`cache` 分别负责分配、元数据和缓存 |
| LSM 内存表、刷盘与压实 | [lsm_tree.rs](../src/lsm_tree/lsm_tree.rs)、[lsm_tree/](../src/lsm_tree/lsm_tree/) | `memtable`、`flush`、`compaction`、`manifest`、`background` 分别负责写入、刷盘、层间压实、清单发布和后台调度；读与扫描分开 |
| SST 文件 | [sst.rs](../src/lsm_tree/sst.rs)、[sst/](../src/lsm_tree/sst/) | 构建、块缓存、紧凑块解析、点读、扫描与流式归并分别维护 |
| 内存有序存储 | [mem_store.rs](../src/memory/mem_store.rs) | 多版本 BTreeMap 与二进制前缀查询边界 |
| KV 扩展功能 | 各引擎的 `kv_ops.rs` 与同名目录 | 批量、计数、TTL、分页、搜索和元数据等扩展方法 |
| 共享事务与候选折叠 | [common/](../src/common/) | `txn` 负责活动事务表、冲突和收尾；`order` 负责逻辑键分组；`store`、`kernel` 负责通用读取与回收驱动 |
| WAL、磁盘与快照格式 | [wal/](../src/common/wal/)、[disk/](../src/common/disk/)、[kv_snapshot/](../src/common/kv_snapshot/) | WAL 追加与恢复、磁盘文件与校验、快照读写与原子发布分别维护 |
| 终端、维护器与搬运工具 | [terminal/](../src/terminal/)、[maintenance/](../src/maintenance/)、[tools/migrate/](../src/tools/migrate/) | 参数解析与命令执行、维护调度、快照导入导出分别维护 |

## 四个 MVCC 引擎的共同结构

实现目录为 [Bitcask](../src/bitcask/bitcask_mvcc/)、[B+](../src/bplus_tree/bplus_tree_mvcc/)、[LSM](../src/lsm_tree/lsm_tree_mvcc/) 和 [memory](../src/memory/memory_mvcc/)。相同职责使用相同文件名：

| 文件 | 业务边界 |
| --- | --- |
| `transaction.rs` | 事务开始、版本写入、冲突检查、提交和回滚 |
| `search.rs` | 候选折叠、快照可见性、点查和流式搜索 |
| `vacuum.rs` | 自动回收调度、保留判据和有界版本删除 |
| `health.rs` | 只读统计版本链和活动事务，不执行回收 |
| `bulk.rs` | 批量灌入与最终完成时的持久化收尾 |
| `lifecycle.rs` | 打开、关闭、配置与检查点等引擎生命周期 |
| `export.rs` | 导出最新可见行，并保留 TTL 与墓碑语义 |
| `recovery.rs` / `snapshot.rs` | 持久引擎的恢复，或 memory 的启停快照；按引擎实际能力设置 |

同一职责下的主路径和历史对照路径放在一起；`*_reference.rs` 与 `probes.rs` 是既有测试计量和对照开关，按原来的 `cfg(test)` 编译边界保留。

## 测试与压测入口

各模块的 `tests.rs` 汇总同名 `tests/` 目录里的职责分片。**仅测试使用 `include!`**，以保留原测试命名空间，尤其是现有 `--exact` 名称；生产实现不用这种方式拼接。

[scale_bench.rs](../src/scale_bench.rs)、[lsm_allocation_bench.rs](../src/lsm_allocation_bench.rs) 和 [run-scale-bench.ps1](../scripts/run-scale-bench.ps1) 继续作为原台架入口。全库台架仍叫 `engine::tests::scale_baseline_all_engines`。拆分后的库测试逐项对照原可执行文件的 642 个入口。

常用验收命令：

```powershell
python -B -u -X utf8 scripts/validate-module-layout.py
cargo check --all-targets --all-features --offline --quiet
cargo test --release --tests --offline --quiet -- --test-threads=1
cargo doc --lib --all-features --no-deps --offline --quiet
cargo test --doc --offline --quiet
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/run-scale-bench.ps1 -Rows '2000,3000' -Repeat 1
```

代码项审计保留方法所属 impl、字面量、算法、断言和宏参数；两侧在相同作用域缩进下使用同一 `rustfmt` 规整，避免把格式化的尾逗号和导入排序当成业务变化。它忽略模块包装、导入和父级可见性，不能代替 API、条件编译和实际运行验收。

## 后续代码与中文注释约定

- 新方法放进对应职责文件，入口文件只补类型、模块声明或导出；出现独立职责时再拆文件。
- 模块头说明本文件负责什么；业务注释说明为什么这样做、约束是什么、失败后哪些状态可信。
- 提交与回滚写清 WAL、数据文件和事务销账的先后关系；区分结果不确定与“已提交但维护失败”。
- 回收写清活动快照、跨片版本组、墓碑与保留边界；同步发布写清何时允许标干净、复用页或丢弃恢复记录。
- 性能注释写明计量边界、拷贝与借用的区别、对照路径和已知代价；历史实测值注明使用场景。
- 业务说明使用中文；类型、方法、字段、测试名、错误枚举和格式标记保持原名。
- 修改后同步本索引和相关验收账本，并检查文档链接与原压测入口。
