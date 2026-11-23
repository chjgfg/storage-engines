// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// 用例的 scratch 根：返回 **RAII 句柄**（M82），声明在引擎之前即可，用例结束
/// （含 panic 展开）自动连旁挂文件一起删 —— 此前靠每个用例末尾手写 `remove_dir_all`，
/// 而"引擎还开着就删"在 Windows 上静默失败，一次全量跑就漏一批 `se_tools_*`。
fn root(tag: &str) -> crate::common::ScratchPath {
    let d = crate::common::ScratchPath::new("se_tools", tag, "");
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// 往某家直接灌几把键：走门面，**不碰 tools 层** —— 自己验自己不算守卫
fn seed(engine: EngineKind, r: &Path, rows: &[(&[u8], &[u8])]) {
    let path = display_path(engine, r);
    std::fs::create_dir_all(if engine == EngineKind::BPlusTree {
        path.parent().unwrap()
    } else {
        path.as_path()
    })
    .unwrap();
    let db = open_engine_with(engine, Some(&path), &migrate_config()).unwrap();
    for (k, v) in rows {
        let tx = db.begin();
        // 走 `set_raw` 而不是 `set`：`set` 的合同是"收已打过值信封的字节"（README 那条
        // 值信封不对称），拿用户字节喂它就等于第一字节被当标签吃掉。
        assert!(
            tx.set_raw(k, v.to_vec()),
            "写入被拒: {}",
            String::from_utf8_lossy(k)
        );
        tx.commit();
    }
    db.flush().unwrap();
    db.checkpoint().unwrap();
}

fn read_back(engine: EngineKind, r: &Path, k: &[u8]) -> Option<Vec<u8>> {
    let path = display_path(engine, r);
    let db = open_engine_with(engine, Some(&path), &migrate_config()).unwrap();
    let tx = db.begin();
    let v = tx.get(k);
    tx.commit();
    v
}

/// 开一家引擎。memory **也要给目录**：它的启停快照（M81 第 6 项前半）就是它唯一的"跨句柄"
/// 载体 —— 不给目录 = 每个句柄一个空库，seed 与后面的 open 根本不是同一个库。
fn open_for_sync(kind: EngineKind, r: &Path) -> std::boxed::Box<dyn KvEngine> {
    let path = display_path(kind, r);
    let _ = std::fs::create_dir_all(if kind == EngineKind::BPlusTree {
        path.parent().unwrap()
    } else {
        path.as_path()
    });
    open_engine_with(kind, Some(&path), &migrate_config()).unwrap()
}

/// 带 TTL 播种（`expire` = 绝对到期时刻；`None` = 永久）。
fn seed_ttl(kind: EngineKind, r: &Path, rows: &[(&[u8], &[u8], Option<u64>)]) {
    let db = open_for_sync(kind, r);
    let now = now_unix_secs();
    for (k, v, expire) in rows {
        let tx = db.begin();
        let ok = match expire {
            Some(e) => tx.set_with_ttl(k, v.to_vec(), Duration::from_secs(e.saturating_sub(now))),
            None => tx.set_raw(k, v.to_vec()),
        };
        assert!(ok, "写入被拒: {:?}", String::from_utf8_lossy(k));
        tx.commit();
    }
    db.flush().unwrap();
    db.checkpoint().unwrap();
}
