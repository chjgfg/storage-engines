//! 终端命令路由和自动事务收尾。

use super::*;

/// 数据命令统一走这里：挂起事务开着就复用它（归调用方继续持有），否则隐式开一条、
/// 跑完按 `f` 给的收尾方式提交或回滚。`begin` 显式开的事务**不**在这里提交 —— 那正是挂起的含义。
pub(super) fn with_tx<'e>(
    engine: &'e dyn KvEngine,
    txn: &mut Option<Box<dyn KvTxn + 'e>>,
    f: impl FnOnce(&dyn KvTxn) -> (Out, TxnEnd),
) -> Out {
    let auto = txn.is_none();
    if auto {
        *txn = Some(engine.begin());
    }
    let r = {
        let tx = txn.as_mut().unwrap();
        f(tx.as_ref())
    };
    if auto {
        if let Some(t) = txn.take() {
            match r.1 {
                TxnEnd::Commit => {
                    if let Err(e) = t.try_commit() {
                        return Out::err(e.to_string());
                    }
                }
                TxnEnd::Rollback => t.rollback(),
            }
        }
    }
    r.0
}
/// 先解析命令再选择数据操作或引擎管理路径。
/// 显式事务由用户结束，隐式事务由本次命令的结果决定提交或回滚。

// ── 主入口 ──────────────────────────────────────────────────────────────────

/// 解释一行 KV 命令。空行返回空输出；未知命令报错不 panic。
pub fn run_line<'e>(
    engine: &'e dyn KvEngine,
    txn: &mut Option<Box<dyn KvTxn + 'e>>,
    line: &str,
) -> LineOutcome {
    let tokens = tokenize(line);
    let Some(cmd) = tokens.first().cloned() else {
        return LineOutcome::Outputs(Vec::new());
    };
    let args = &tokens[1..];
    // 命令名大小写不敏感（redis 习惯），key/value 保持原样
    match cmd.to_ascii_lowercase().as_str() {
        "get" => run_data(engine, txn, args, cmd_get),
        "set" => run_data(engine, txn, args, cmd_set),
        "setex" => run_data(engine, txn, args, cmd_setex),
        "del" | "delete" | "rm" => run_data(engine, txn, args, cmd_del),
        "exists" => run_data(engine, txn, args, cmd_exists),
        "meta" => run_data(engine, txn, args, cmd_meta),
        "incr" | "decr" => {
            let is_incr = cmd.eq_ignore_ascii_case("incr");
            run_data(engine, txn, args, |tx, a| cmd_incr(tx, a, is_incr))
        }
        "mset" => run_data(engine, txn, args, cmd_mset),
        "mget" => run_data(engine, txn, args, cmd_mget),
        "scan" => run_data(engine, txn, args, |tx, a| cmd_scan(tx, a, false)),
        "rscan" => run_data(engine, txn, args, |tx, a| cmd_scan(tx, a, true)),
        "prefix" => run_data(engine, txn, args, cmd_prefix),
        "seek" => run_data(engine, txn, args, |tx, a| cmd_seek(tx, a, false)),
        "seekprev" | "seek_prev" => run_data(engine, txn, args, |tx, a| cmd_seek(tx, a, true)),
        "count" => run_data(engine, txn, args, cmd_count),
        "find" => run_data(engine, txn, args, cmd_find),
        "ttl" => run_data(engine, txn, args, cmd_ttl),
        "refresh" => run_data(engine, txn, args, cmd_refresh),
        "persist" => run_data(engine, txn, args, cmd_persist),
        "purge" => run_data(engine, txn, args, |tx, _| cmd_purge(tx)),
        "begin" => LineOutcome::Outputs(vec![cmd_begin(engine, txn)]),
        "commit" => LineOutcome::Outputs(vec![cmd_finish(txn, true)]),
        "rollback" => LineOutcome::Outputs(vec![cmd_finish(txn, false)]),
        "info" => LineOutcome::Outputs(cmd_info(engine)),
        "health" => LineOutcome::Outputs(vec![cmd_health(engine)]),
        "vacuum" => LineOutcome::Outputs(vec![cmd_vacuum(engine)]),
        "flush" => LineOutcome::Outputs(vec![cmd_engine_op(engine, "flush")]),
        "checkpoint" => LineOutcome::Outputs(vec![cmd_engine_op(engine, "checkpoint")]),
        "merge" => LineOutcome::Outputs(vec![cmd_engine_op(engine, "merge")]),
        "engine" => cmd_engine(engine, txn, args),
        "help" | "h" | "?" => LineOutcome::Outputs(vec![Out::msg("HELP", help_text())]),
        "exit" | "quit" | "bye" | "q" => LineOutcome::Exit,
        other => LineOutcome::Outputs(vec![Out::err(format!(
            "未知命令: {other}（help 看命令表）"
        ))]),
    }
}

/// 数据命令的统一入口：`with_tx` 的语法糖 —— 所有 `cmd_*(&dyn KvTxn, &[String]) -> (Out, TxnEnd)`
/// 都长一个样，没必要在 match 里逐条展开同一个包装。
pub(super) fn run_data<'e>(
    engine: &'e dyn KvEngine,
    txn: &mut Option<Box<dyn KvTxn + 'e>>,
    args: &[String],
    f: impl FnOnce(&dyn KvTxn, &[String]) -> (Out, TxnEnd),
) -> LineOutcome {
    LineOutcome::Outputs(vec![with_tx(engine, txn, |tx| f(tx, args))])
}
