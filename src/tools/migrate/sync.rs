//! 已有目标上的增量同步与 TTL 策略。

use super::*;
/// 只补目标尚缺的可见键，并保留现有目标记录及其 TTL。
/// 墓碑也属于已观察状态；该接口不是删除双向对账协议，不能保证回收后的删除标记可继续传播。

/// 跨库补齐：`from -> to`，**目标"见过"的键一个字都不动，没见过的才插**。
///
/// 与 `migrate` 的分工要说清（两件事不是同一件事，别再混用）：`migrate` 是换布局的
/// **一次性搬运**（覆盖语义 + 可选 `wipe_target`），`sync` 是"把 A 里有而 B 里没有的东西补进 B"
/// —— 幂等：同一份源同步两次，第二次 `inserted == 0`。
///
/// 三条口径是这项功能的全部难点，逐条钉死：
/// ① **"见过"含墓碑**：判据是 `get_meta(...).is_some()`，**不是 `exists()`**。墓碑的含义是
///    "我知道这把键、它被删了"，不是"没见过"；用 `exists()` 会把已删键**复活**，而同步工具
///    复活已删键比漏同步严重一个量级。四家在这条上的行为由 `sync_tombstone_counts_as_seen_in_every_engine`
///    逐家实测 —— 那一条同时钉住 `exists()` 在墓碑上是 `false`（也就是"用它当判据会复活"），
///    所以这条判据不是假设，是量出来的。
///    ⚠️ **这条判据有时效（M82 记）**：墓碑是"见过"的证据，而**回收会把孤墓碑整键删掉**
///    （四家的 `vacuum_plan_group` 都是"xmin 之前最终是墓碑且无更新版本 ⇒ 整 key 删"）⇒ 回收走过
///    那把键之后，"见过"就没了，同一份**旧**源再 sync 会把已删键复活。这是"墓碑不该永久占位"与
///    "见过要有据可查"两条合同的交点，不是 `sync` 的缺陷：要长期防复活，就在回收之前同步完，
///    或由应用层自己记删除（或走 `migrate` 的覆盖语义）。现状由
///    `sync_after_vacuum_resurrects_the_deleted_key_characterization` 钉成会翻红的断言 ——
///    哪天给墓碑加了保留策略，那一条要按新语义改写，而不是悄悄变绿。
/// ② **删除不传播**：源里的墓碑根本不进候选（`include_deleted = false`）。这是**快照补齐**，
///    不是复制协议 —— 要"把 B 里多出来的键删掉"请走 `migrate`（覆盖 + wipe）或应用层自己管。
/// ③ **TTL 跟着走，但不刷新已有的**：新插入的键按源里记的**绝对到期时刻**打 TTL 信封；
///    已存在的键（含墓碑）连 TTL 都不碰。
///    到期时刻从源侧 `get_meta(...).expire_unix_secs` 单独取（和 memory 启停快照同一扇门、
///    同一个字段）⇒ 同步过来的键该什么时候过期还是什么时候。
///    ⚠️ 这里**曾经**只能自己取：出口 `live_record_of_logical` 交的是用户字节（信封已剥）、
///    `ExportRecord` 里没有放 expire 的位置，所以 `migrate` 那时是 TTL 黑洞。0.5.0 起出口把
///    到期时刻一起交出来、`migrate` 也按它重打信封（守卫 `migrate_carries_ttl_across_engines`），
///    `sync` 这一支保留 `get_meta` 的取法不动 —— 它判"见过"本来就要读这一格，不多花一次。
///
/// 写入走**普通事务**（一批一个事务），不走 `begin_bulk`：skip-existing 要按键读目标，
/// 而 bulk 那条通道是"绕过冲突检查"的门，两件事凑在同一个句柄上会把"读到的快照"和"写入的可见性"
/// 混成两套。代价如实写在这儿：并发写同一把键时这一批可能冲突，`sync` 直接返回错误、不静默降级。
pub fn sync(from: &dyn KvEngine, to: &dyn KvEngine) -> Result<SyncStats, String> {
    let mut stats = SyncStats::default();
    // 源侧一个长事务：遍历与逐键取 expire 都在**同一个快照**上，否则"这行活着"和"它几点到期"
    // 来自两个瞬间（并发写会让插入行带上错到一半的 TTL）。
    let src = from.begin();
    let mut batch: Vec<(Vec<u8>, Vec<u8>, Option<u64>)> = Vec::with_capacity(BATCH);
    let mut err: Option<String> = None;
    let flush = |batch: &mut Vec<(Vec<u8>, Vec<u8>, Option<u64>)>,
                 stats: &mut SyncStats|
     -> Result<(), String> {
        if batch.is_empty() {
            return Ok(());
        }
        let drained = std::mem::replace(batch, Vec::with_capacity(BATCH));
        let tx = to.begin();
        let now = now_unix_secs();
        for (key, user, expire) in drained {
            // 判据①：`get_meta` 认墓碑，`exists` 不认。
            if tx.get_meta(&key).is_some() {
                stats.skipped_existing += 1;
                continue;
            }
            // ⚠️ 写的一侧只用**收用户字节**的两个口子（`set_raw` / `set_with_ttl`），信封由引擎打。
            // 不走 `batch_set`：它内部一律 `pack_plain` ⇒ 既带不了 TTL，喂已打袋的字节就是
            // "假信封"那一支静默错；也不走 `put_batch_owned`：它收的是**逻辑字节**，
            // 同一个 `Vec<(key, value)>` 换到那条门上含义就反过来（两家门风相反，正是本文件
            // 顶部点名要防的事）。
            let ok = match expire {
                Some(e) if e > now => {
                    let ok = tx.set_with_ttl(&key, user, Duration::from_secs(e - now));
                    if ok {
                        stats.inserted_with_ttl += 1;
                    }
                    ok
                }
                // 源侧出口本来就把过期行挡在外面；走到这里只剩"读到之后才跨过到期点"这一种，
                // 插一条注定看不见的行没有意义 ⇒ 如实计个数、不写。
                Some(_) => {
                    stats.skipped_expired += 1;
                    continue;
                }
                None => tx.set_raw(&key, user),
            };
            if !ok {
                return Err(format!(
                    "同步写入失败（冲突、容量限制或存储错误）（键 {:?}）⇒ 这一批不提交，整趟同步以错误收场",
                    String::from_utf8_lossy(&key)
                ));
            }
            stats.inserted += 1;
        }
        tx.try_commit().map_err(|e| e.to_string())?;
        Ok(())
    };
    let scanned = src.export_latest_visible_for_each(false, &mut |rec| {
        let Some(user) = rec.value else {
            // include_deleted=false ⇒ 墓碑不进这一支；真进了就是门变了，不能默默吞掉
            err = Some(format!("源侧交出墓碑行（键 {:?}）⇒ 候选口径变了", rec.key));
            return false;
        };
        stats.scanned += 1;
        let expire = src.get_meta(&rec.key).and_then(|m| m.expire_unix_secs);
        batch.push((rec.key, user, expire));
        if batch.len() >= BATCH {
            if let Err(e) = flush(&mut batch, &mut stats) {
                err = Some(e);
                return false;
            }
        }
        true
    });
    if err.is_none() {
        if let Err(e) = flush(&mut batch, &mut stats) {
            err = Some(e);
        }
    }
    if let Some(e) = err {
        return Err(e);
    }
    // 两把尺子必须对上：`scanned`（源**交出**几行）vs 目标侧**判定**过的行数。
    // 对不上就是有行被静默丢掉 —— 正是这个仓库一路在防的"共享了但没生效"。
    let judged = stats.inserted + stats.skipped_existing + stats.skipped_expired;
    if judged != scanned {
        return Err(format!(
            "源交出 {scanned} 行、目标判定 {judged} 行 ⇒ 有行被静默丢掉"
        ));
    }
    Ok(stats)
}
