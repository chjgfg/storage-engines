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

### 性能与正确性（无 API 变更）

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

### 测试

214 个单元测试 + 4 个集成测试。新增的都是**计数断言**（读盘/页/块/解码/物化/克隆次数），不用计时：
原有用例结构上抓不到这类病 —— 每条用例的版本链都是个位数长。每引擎每通道各有自己的守卫，
并且复杂度守卫都先验证过"它会红"（临时短路被测代码跑一遍）才算数。

### 下游升级要点（trading-arb / trading-system 内嵌副本）

1. 内嵌的 `src/storage/*mvcc.rs` 是 0.3.0 之前的复制，缺以上全部修复 —— 症状就是那条随 uptime
   爬升的 CPU 曲线。要么改成依赖本 crate，要么把七条通道的修复逐个搬过去。
2. 搬之后**先量基线再优化**：`health().max_chain` 连续采样，看它是否从"线性爬"变成"贴着 1~切片宽度"。
3. 别急着给 `vacuum()` 加定时器：自动回收已经在跑，手动 `vacuum()` 保留给"要立刻收文件空间"的场景
   （它是唯一会触发 `merge()` / blob 重写的入口）。
