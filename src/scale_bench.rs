// 大规模压测入口。由 engine::tests include，保留库内块/页/堆分配计数器的可见性。
// 五个入口只传不同 rows，负载、参数、计量和验收完全共用 run_scale_baseline。
//
// 普通 cargo test 不运行它们。单档示例（必须 release + 单线程 + exact）：
// cargo test --release --lib engine::tests::scale_bench::rows_600000 -- --exact --ignored --nocapture --test-threads=1
//
// 推荐 scripts/run-scale-bench.ps1：自动记版本/环境、保存日志/CSV/摘要、在 finally 清掉测试库。
// 单独在 IDE/终端运行此 .rs 的测试也可以：Rust RAII 清理正常结束与断言 panic 的本次目录；
// 强杀进程/断电不会执行析构；脚本运行用 -CleanRun 清理清单，直接 cargo 运行则核对日志
// 中 scratch= 指向的本次精确目录，不要通配删除其它测试库。

mod scale_bench {
    macro_rules! scale_case {
        ($name:ident, $rows:expr) => {
            #[test]
            #[ignore = "长时间/大内存压测；只在 release 下显式单独运行"]
            fn $name() {
                super::run_scale_baseline($rows);
            }
        };
    }

    scale_case!(rows_600000, 600_000);
    scale_case!(rows_1000000, 1_000_000);
    scale_case!(rows_5000000, 5_000_000);
    scale_case!(rows_10000000, 10_000_000);
    scale_case!(rows_20000000, 20_000_000);
}
