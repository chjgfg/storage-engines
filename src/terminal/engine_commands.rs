//! 事务生命周期和引擎管理命令。

use super::*;

// ── 事务 ────────────────────────────────────────────────────────────────────

pub(super) fn cmd_begin<'e>(
    engine: &'e dyn KvEngine,
    txn: &mut Option<Box<dyn KvTxn + 'e>>,
) -> Out {
    if txn.is_some() {
        return Out::err("已有未提交事务（先 commit 或 rollback）");
    }
    let t = engine.begin();
    let v = t.version();
    *txn = Some(t);
    Out::msg("TXN", format!("BEGIN (version {v})"))
}

/// `commit=true` 提交，`false` 回滚 —— 两条只有动词不同，错误面是同一张。
pub(super) fn cmd_finish(txn: &mut Option<Box<dyn KvTxn + '_>>, commit: bool) -> Out {
    let verb = if commit { "COMMIT" } else { "ROLLBACK" };
    match txn.take() {
        None => Out::err("没有开着的事务"),
        Some(t) => {
            let v = t.version();
            if commit {
                if let Err(e) = t.try_commit() {
                    return Out::err(e.to_string());
                }
            } else {
                t.rollback();
            }
            Out::msg("TXN", format!("{verb} (version {v})"))
        }
    }
}

// ── 维护面（不走事务，直接在引擎上） ────────────────────────────────────────

pub(super) fn cmd_info(engine: &dyn KvEngine) -> Vec<Out> {
    let path = engine
        .path()
        .map(|p| p.display().to_string())
        .unwrap_or_else(|| "（内存，无路径）".into());
    let dur = engine.durability_profile();
    let head = Out::msg(
        "INFO",
        format!("引擎: {}\n路径: {}\n持久性缺口: {dur}", engine.name(), path),
    );
    let t = engine.tuning();
    let table = Table {
        columns: vec!["旋钮".into(), "值".into()],
        rows: t
            .entries
            .iter()
            .map(|e| vec![e.key.to_string(), e.value.clone()])
            .collect(),
    };
    vec![
        head,
        Out::with_table("INFO", format!("tuning（{} 项）", table.rows.len()), table),
    ]
}

pub(super) fn cmd_health(engine: &dyn KvEngine) -> Out {
    let h = engine.health();
    let t = Table {
        columns: vec!["指标".into(), "值".into()],
        rows: vec![
            vec![
                "物理行数（含旧版本与墓碑）".into(),
                h.total_rows.to_string(),
            ],
            vec!["逻辑键数".into(), h.live_keys.to_string()],
            vec!["最长版本链".into(), h.max_chain.to_string()],
            vec!["超一行多余行".into(), h.rows_above_one.to_string()],
            vec!["活跃事务数".into(), h.active_txns.to_string()],
        ],
    };
    Out::with_table("MAINT", "health（口径四家一致，可直接对比）".into(), t)
}

pub(super) fn cmd_vacuum(engine: &dyn KvEngine) -> Out {
    match engine.vacuum() {
        Err(e) => Out::err(format!("vacuum 失败: {e}")),
        Ok(s) => {
            if s.skipped_active_txn {
                return Out::msg(
                    "MAINT",
                    format!(
                        "vacuum 整轮跳过：有 {} 个活跃事务挂着（挂起的事务会让回收不敢动）",
                        engine.health().active_txns
                    ),
                );
            }
            let mut m = format!(
                "裁掉 {} 个旧版本（xmin={}，扫了 {} 物理行",
                s.versions_removed, s.xmin, s.rows_scanned
            );
            match s.blob_rewritten {
                Some(true) => m.push_str("，blob 文件已重写"),
                Some(false) => m.push_str("，blob 无需重写"),
                None => {}
            }
            m.push('）');
            Out::msg("MAINT", m)
        }
    }
}

/// flush / checkpoint / merge：同签名同错误面，一个函数派发
pub(super) fn cmd_engine_op(engine: &dyn KvEngine, op: &'static str) -> Out {
    let r = match op {
        "flush" => engine.flush(),
        "checkpoint" => engine.checkpoint(),
        _ => engine.merge(),
    };
    match r {
        Ok(()) => Out::msg("MAINT", format!("{op} OK")),
        Err(e) => Out::err(format!("{op} 失败: {e}")),
    }
}

// ── 终端命令 ────────────────────────────────────────────────────────────────

pub(super) fn cmd_engine<'e>(
    engine: &'e dyn KvEngine,
    txn: &mut Option<Box<dyn KvTxn + 'e>>,
    args: &[String],
) -> LineOutcome {
    if args.is_empty() {
        let path = engine
            .path()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "（内存）".into());
        return LineOutcome::Outputs(vec![Out::msg(
            "ENGINE",
            format!("引擎: {}  路径: {path}", engine.name()),
        )]);
    }
    if args.len() > 2 {
        return LineOutcome::Outputs(vec![Out::err("engine 至多两个参数: engine 名 [目录]")]);
    }
    let Some(kind) = EngineKind::parse(&args[0]) else {
        return LineOutcome::Outputs(vec![Out::err(format!(
            "未知引擎 {:?}（可给 bitcask / bplus_tree / lsm_tree / memory，含 bplus、lsm、mem 等别名）",
            args[0]
        ))]);
    };
    if txn.is_some() {
        return LineOutcome::Outputs(vec![Out::err(
            "有未提交事务 —— 事务借用了引擎，先 commit 或 rollback 再切",
        )]);
    }
    LineOutcome::Switch {
        kind: kind.as_str(),
        dir: args.get(1).map(PathBuf::from),
    }
}
