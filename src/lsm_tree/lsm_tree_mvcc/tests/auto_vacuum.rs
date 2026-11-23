// 该文件集中验证对应职责，保留原测试名称和字面值。

/// 长跑进程不喊 vacuum，版本链也不能无限涨（I2：垃圾必须自己回来）。
///
/// 当年就是 LSM 先撑不住（一天）而 Bitcask 撑了一个多月 —— 同一条通道在不同引擎里
/// 涨速不同，所以**每个引擎都得自己默认开着**，不能靠「换引擎」当修法。
#[test]
fn test_auto_vacuum_reclaims_without_being_asked() {
    const CHAIN: u64 = 200;
    const SLICE: u64 = 8;

    let path = tmp_db("auto_vac");
    {
        let mvcc = MVCC::open(&path);
        assert_eq!(
            mvcc.vacuum_auto_slice(),
            MVCC::VACUUM_AUTO_SLICE_DEFAULT,
            "自动回收默认必须开着"
        );
        mvcc.set_vacuum_auto_slice(SLICE);

        for i in 0..CHAIN {
            let tx = mvcc.begin_transaction();
            assert!(tx.set(b"hot", format!("v{i}").into_bytes()));
            tx.commit();
            if i % 40 == 39 {
                // 中途落一次 SST：让自动回收的窗口横跨 memtable 与 SST 两种源
                mvcc.flush();
            }
        }

        let h = mvcc.health();
        assert!(
            h.max_chain <= SLICE + 2,
            "没人喊 vacuum，链却涨到 {} 行 —— 自动回收没在工作",
            h.max_chain
        );

        let tx = mvcc.begin_transaction();
        assert_eq!(tx.get(b"hot"), Some(format!("v{}", CHAIN - 1).into_bytes()));
        tx.commit();
    }
    cleanup(&path);
}

/// 关掉的开关必须真关掉：自动回收不能是「没法关掉的那种」。
#[test]
fn test_auto_vacuum_off_switch() {
    const CHAIN: u64 = 60;

    let path = tmp_db("auto_off");
    {
        let mvcc = MVCC::open(&path);
        mvcc.set_vacuum_auto_slice(0);
        for i in 0..CHAIN {
            let tx = mvcc.begin_transaction();
            assert!(tx.set(b"hot", format!("v{i}").into_bytes()));
            tx.commit();
        }
        assert_eq!(mvcc.health().max_chain, CHAIN, "关闭后不该裁掉任何一版");
    }
    cleanup(&path);
}
