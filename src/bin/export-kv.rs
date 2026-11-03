//! 统一导出 CLI：`--engine <家> --dir <库目录> --out <文件> [--format bin|jsonl] [--include-deleted] [--demo N]`
//!
//! 为什么只有一份（M81 第 6 项）：原先四家各有一个 `export_kv.rs`，每家一份参数解析、
//! 一份格式分支、一份"写完数一遍条数"的收尾 —— 而它们调的其实是**同一个** `tools::export_to_dir`。
//! 那正是"基座被复制而不是被依赖"的形状（本仓库一路在收的就是它）。这一份只留命令行外壳，
//! 而外壳里唯一值得测的就是**参数解析** ⇒ 抽成 `parse_args` 单独测（`main` 没有分支，不测）。
//!
//! ```text
//! cargo run --release --bin export-kv -- --engine bitcask --dir ./data/bitcask --out snap.bin
//! cargo run --release --bin export-kv -- --engine memory --dir ./data/mem --out s.jsonl --format jsonl --demo 300
//! ```

use std::path::{Path, PathBuf};
use std::process::exit;

use storage_engines::tools::migrate::{SnapshotFormat, export_to_dir};
use storage_engines::EngineKind;

pub struct Args {
    pub engine: String,
    pub dir: PathBuf,
    pub out: PathBuf,
    pub format: SnapshotFormat,
    pub include_deleted: bool,
    /// 先往 `--engine/--dir` 指定的库灌 N 行样例再导出。这是旧 `memory-export-kv --demo` 那条能力
    /// （memory 没有磁盘库，不给样例就没法试导出）；收进统一工具之后四家都能用。
    pub demo: Option<usize>,
}

pub fn usage() -> String {
    "用法: export-kv --engine <bitcask|bplus_tree|lsm_tree|memory> --dir <库目录> --out <文件> \
     [--format bin|jsonl] [--include-deleted] [--demo N]"
        .to_string()
}

/// 吃 `argv`（不含程序名）。⚠️ 带值的旗标**缺值就报错**，不许悄悄按默认值跑 —— 那会把运维脚本
/// 引到另一个库上去，而这扇门是写盘的门。
pub fn parse_args(argv: &[String]) -> Result<Args, String> {
    let mut a = Args {
        engine: String::new(),
        dir: PathBuf::new(),
        out: PathBuf::new(),
        format: SnapshotFormat::Bin,
        include_deleted: false,
        demo: None,
    };
    let mut i = 0;
    while i < argv.len() {
        let flag = argv[i].as_str().to_string();
        // 带值旗标自己推进游标；越界 = 缺值
        let value = |i: &mut usize| -> Result<String, String> {
            *i += 1;
            argv.get(*i).cloned().ok_or_else(|| format!("{flag} 后面缺值"))
        };
        match flag.as_str() {
            "--engine" => a.engine = value(&mut i)?,
            "--dir" => a.dir = PathBuf::from(value(&mut i)?),
            "--out" => a.out = PathBuf::from(value(&mut i)?),
            "--format" => {
                let v = value(&mut i)?;
                a.format = match v.as_str() {
                    "jsonl" => SnapshotFormat::Jsonl,
                    "bin" => SnapshotFormat::Bin,
                    other => return Err(format!("未知 --format {other}（只认 bin / jsonl）")),
                };
            }
            "--demo" => {
                let v = value(&mut i)?;
                a.demo = Some(v.parse::<usize>().map_err(|e| format!("--demo 要非负整数，收到 {v}: {e}"))?);
            }
            "--include-deleted" => a.include_deleted = true,
            "-h" | "--help" => return Err(usage()),
            other => return Err(format!("未知参数: {other}\n{}", usage())),
        }
        i += 1;
    }
    if a.engine.is_empty() || a.dir.as_os_str().is_empty() || a.out.as_os_str().is_empty() {
        return Err(format!("缺参数：--engine / --dir / --out 三个都必给\n{}", usage()));
    }
    Ok(a)
}

/// 灌 N 行样例（键形状 `demo-000000#/x`）。四家都走门面，不在这里做引擎特判。
fn seed_demo(kind: EngineKind, dir: &Path, n: usize) -> Result<(), String> {
    use storage_engines::{EngineConfig, open_engine_with};
    let db = open_engine_with(kind, Some(dir), &EngineConfig::default())
        .map_err(|e| format!("打开 {} 失败: {e}", kind.as_str()))?;
    let items: Vec<(Vec<u8>, Vec<u8>)> = (0..n)
        .map(|i| (format!("demo-{i:06}#/x").into_bytes(), format!("v{i}").into_bytes()))
        .collect();
    let tx = db.begin();
    tx.batch_set(&items).map_err(|bad| format!("灌样例冲突 {} 把", bad.len()))?;
    tx.commit();
    drop(tx);
    db.flush().map_err(|e| format!("flush 失败: {e}"))?;
    db.checkpoint().map_err(|e| format!("checkpoint 失败: {e}"))?;
    // ⚠️ 必须先关句柄：memory 的启停快照写在 `Drop` 里，不关就导不出东西
    drop(db);
    Ok(())
}

pub fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let args = match parse_args(&argv) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("{e}");
            exit(2);
        }
    };
    let kind = match EngineKind::parse(&args.engine) {
        Some(k) => k,
        None => {
            eprintln!(
                "未知引擎: {}（可给 bitcask / bplus_tree / lsm_tree / memory，含 bc、bplus、lsm、mem 等别名）",
                args.engine
            );
            exit(2);
        }
    };
    if let Err(e) = std::fs::create_dir_all(if kind == EngineKind::BPlusTree {
        args.dir.parent().unwrap_or(&args.dir).to_path_buf()
    } else {
        args.dir.clone()
    }) {
        eprintln!("建目录失败: {e}");
        exit(1);
    }
    if let Some(n) = args.demo {
        if let Err(e) = seed_demo(kind, &args.dir, n) {
            eprintln!("灌样例失败: {e}");
            exit(1);
        }
    }
    match export_to_dir(kind, &args.dir, &args.out, args.format, args.include_deleted) {
        Err(e) => {
            eprintln!("导出失败: {e}");
            exit(1);
        }
        Ok(stats) => println!(
            "导出 {} → {}：{} 行（活 {} / 墓碑 {}）",
            kind.as_str(),
            args.out.display(),
            stats.records,
            stats.live,
            stats.deleted
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::assert_eq;
    use std::string::{String, ToString};
        use std::vec::Vec;

    fn v(xs: &[&str]) -> Vec<String> {
        xs.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn parse_flags_defaults_and_explicit() {
        let a = parse_args(&v(&["--engine", "bitcask", "--dir", "./d", "--out", "./s.bin"])).unwrap();
        assert_eq!((a.engine.as_str(), a.include_deleted, a.demo), ("bitcask", false, None));
        assert_eq!(a.format, SnapshotFormat::Bin, "默认 bin（与旧四家的默认一致）");

        let b = parse_args(&v(&[
            "--engine", "lsm", "--dir", "./d", "--out", "./s.jsonl",
            "--format", "jsonl", "--include-deleted", "--demo", "12",
        ]))
        .unwrap();
        assert!(b.include_deleted && b.demo == Some(12));
        assert_eq!(b.format, SnapshotFormat::Jsonl);
    }

    /// 三个必填各缺一次都要红 —— 少判一个就会拿默认值往错的目录上写（这扇门是写盘的门）。
    #[test]
    fn parse_requires_engine_dir_and_out() {
        for argv in [
            v(&["--dir", "./d", "--out", "./s.bin"]),
            v(&["--engine", "bitcask", "--out", "./s.bin"]),
            v(&["--engine", "bitcask", "--dir", "./d"]),
            v(&[]),
        ] {
            assert!(parse_args(&argv).is_err(), "少了东西却过了: {argv:?}");
        }
    }

    #[test]
    fn parse_rejects_bad_values_instead_of_guessing() {
        assert!(parse_args(&v(&["--engine", "bitcask", "--dir", "./d", "--out"])).is_err());
        assert!(parse_args(&v(&[
            "--engine", "bitcask", "--dir", "./d", "--out", "./s", "--format"
        ]))
        .is_err());
        assert!(parse_args(&v(&[
            "--engine", "bitcask", "--dir", "./d", "--out", "./s", "--format", "parquet"
        ]))
        .is_err());
        assert!(parse_args(&v(&[
            "--engine", "bitcask", "--dir", "./d", "--out", "./s", "--demo", "-3"
        ]))
        .is_err());
        // 导入才有的旗标不许在这边被默默吃下
        assert!(parse_args(&v(&["--engine", "bitcask", "--dir", "./d", "--out", "./s", "--wipe"])).is_err());
    }

    /// `--demo` 真跑得通一条完整链路（灌 → 关句柄 → 导出 → 文件读回）。挑 memory 与 lsm 两家：
    /// memory 验"必须先在 `Drop` 里落快照"这一支，lsm 验磁盘引擎正常支。
    #[test]
    fn demo_then_export_produces_a_readable_snapshot() {
        use storage_engines::common::kv_snapshot::read_bin;
        for kind in [EngineKind::Memory, EngineKind::LsmTree] {
            let dir = std::env::temp_dir().join(format!("se_cli_demo_{}", kind.as_str()));
            let snap = dir.join("snap.bin");
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            seed_demo(kind, &dir, 7).unwrap();
            let st = export_to_dir(kind, &dir, &snap, SnapshotFormat::Bin, false).unwrap();
            assert_eq!((st.records, st.live, st.deleted), (7, 7, 0), "{kind}：样例行数不对");
            let (recs, _) = read_bin(&snap).unwrap();
            assert_eq!(recs.len(), 7, "{kind}：文件比库少行");
            assert_eq!(recs[6].key, b"demo-000006#/x".to_vec());
            assert_eq!(recs[6].value.as_deref(), Some(&b"v6"[..]));
            let _ = std::fs::remove_dir_all(&dir);
        }
    }
}
