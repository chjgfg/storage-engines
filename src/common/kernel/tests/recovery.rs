// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// M82 的口径：**账不足一片 ⇒ `begin()` 不扫**，哪怕一轮还开在半路。
///
/// 这是 M81 第 5 项后半那条"游标不在库头也给许可（`charged = 0`）"的**反面**——那一支让写流里
/// 每个事务都扫一整片（守卫 `auto_vacuum_scanning_keeps_pace_with_writes_not_with_transactions`
/// 钉的就是它）。半路的轮次由 `Maintenance` 推，不由下一个事务推；但 `sweep_in_progress`
/// 必须照实报 true，调用方才知道有一轮没走完。
/// 把 `take_tick` 里那一支加回来（账不够而游标非空 ⇒ `Some`）⇒ 本条立即红在"不该给许可"上。
#[test]
fn scheduler_does_not_push_an_open_round_from_begin_without_debt() {
    let s = VacuumScheduler::new(8);
    let sweep = || s.status(&Registry::new()).sweep_in_progress;
    s.on_commit(8);
    // 第一片：账够 ⇒ 取走 8，游标停在库头 ⇒ 这还没"开轮"
    let t = s.take_tick().expect("攒够一片必须给许可");
    assert_eq!((t.slice, t.charged), (8, 8));
    assert_eq!(s.pending.load(Ordering::SeqCst), 0);
    assert!(!s.round_open(), "游标还在库头 = 一轮都没走");
    assert!(
        !sweep(),
        "对外那格必须和游标同义，否则规模守卫判的是个数不是状态"
    );

    // 走到半路：游标非空 = 一轮已发起
    s.set_cursor(Some(b"mid".to_vec()));
    assert!(s.round_open());
    assert!(sweep(), "账已经花光而一轮还在半路 ⇒ 这一格必须报 true");
    // 账为 0 ⇒ **不给许可**：半路的轮次不是 begin() 的活
    assert!(
        s.take_tick().is_none(),
        "账不足一片时 begin() 不许扫，哪怕一轮开着（M82）"
    );
    assert_eq!(s.pending.load(Ordering::SeqCst), 0);
    assert!(sweep(), "没推进 ⇒ 状态照旧报'半路'，不许因为没扫就假装走完");

    // 新写把账攒够 ⇒ 从同一个游标接着扫，不从头
    s.on_commit(8);
    let t = s.take_tick().expect("账够了必须给许可");
    assert_eq!((t.slice, t.charged), (8, 8));
    assert_eq!(
        s.cursor().as_deref(),
        Some(&b"mid"[..]),
        "续扫要从停下的地方接着走"
    );

    // 推到库尾：游标归 None ⇒ 没有新写就不该触发
    s.set_cursor(None);
    assert!(s.take_tick().is_none(), "一轮走完了、账又不够 ⇒ 不该空转");
    assert!(!sweep());
}
