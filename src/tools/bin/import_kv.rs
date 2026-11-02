//! 统一导入 CLI：`--engine <家> --dir <库目录> --in <文件> [--format bin|jsonl] [--wipe]`
//!
//! 与 [`export_kv`] 同理：四家原先各有一份，调的都是同一个 `tools::import_from_dir` ⇒ 只留一份外壳。
//!
//! 语义（**覆盖**，不是补齐）：文件里有的键直接写进去（同键后到者胜），目标里文件没有的键**不删**。
//! 要"整库换掉"加 `--wipe`（先清目标库文件）。
//! 要跨库**补齐**（"目标已有的一个字不动"）走库对库的 `storage_engines::tools::sync` ——
//! 它有墓碑判据与 TTL 传递，这个文件口子没有（见下）。
//!
//! ```text
//! cargo run --release --bin import-kv -- --engine lsm_tree --dir ./data/lsm --in snap.bin
//! cargo run --release --bin import-kv -- --engine bplus --dir ./data/bplus --in snap.jsonl --format jsonl --wipe
//! ```

use std::path::PathBuf;
use std::process::exit;

use storage_engines::tools::migrate::{SnapshotFormat, import_from_dir};
use storage_engines::EngineKind;

pub struct Args {
    pub engine: String,
    pub dir: PathBuf,
    pub input: PathBuf,
    pub format: SnapshotFormat,
    pub wipe: bool,
}

pub fn usage() -> String {
    "用法: import-kv --engine <bitcask|bplus_tree|lsm_tree|memory> --dir <库目录> --in <文件> \
     [--format bin|jsonl] [--wipe]"
        .to_string()
}

/// 吃 `argv`（不含程序名）。⚠️ 带值旗标缺值就报错，不许按默认值跑 —— 这一扇门直接写库。
pub fn parse_args(argv: &[String]) -> Result<Args, String> {
    let mut a = Args {
        engine: String::new(),
        dir: PathBuf::new(),
        input: PathBuf::new(),
        format: SnapshotFormat::Bin,
        wipe: false,
    };
    let mut i = 0;
    while i < argv.len() {
        let flag = argv[i].as_str().to_string();
        let value = |i: &mut usize| -> Result<String, String> {
            *i += 1;
            argv.get(*i).cloned().ok_or_else(|| format!("{flag} 后面缺值"))
        };
        match flag.as_str() {
            "--engine" => a.engine = value(&mut i)?,
            "--dir" => a.dir = PathBuf::from(value(&mut i)?),
            // `--in` 是新名字，`--input`/`--file` 是旧四家 CLI 用过的两种写法 ⇒ 都吃，
            // 免得收拢这一步把谁的运维脚本打断（三个名字指向同一个字段，没有第二套语义）
            "--in" | "--input" | "--file" => a.input = PathBuf::from(value(&mut i)?),
            "--format" => {
                let v = value(&mut i)?;
                a.format = match v.as_str() {
                    "jsonl" => SnapshotFormat::Jsonl,
                    "bin" => SnapshotFormat::Bin,
                    other => return Err(format!("未知 --format {other}（只认 bin / jsonl）")),
                };
            }
            "--wipe" => a.wipe = true,
            "-h" | "--help" => return Err(usage()),
            other => return Err(format!("未知参数: {other}\n{}", usage())),
        }
        i += 1;
    }
    if a.engine.is_empty() || a.dir.as_os_str().is_empty() || a.input.as_os_str().is_empty() {
        return Err(format!("缺参数：--engine / --dir / --in 三个都必给\n{}", usage()));
    }
    Ok(a)
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
    match import_from_dir(kind, &args.dir, &args.input, args.format, args.wipe) {
        Err(e) => {
            eprintln!("导入失败: {e}");
            exit(1);
        }
        Ok(stats) => println!(
            "导入 {} ← {}：{} 行（活 {} / 墓碑 {}）",
            kind.as_str(),
            args.input.display(),
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
    fn parse_flags_defaults_aliases_and_wipe() {
        let a = parse_args(&v(&["--engine", "lsm", "--dir", "./d", "--in", "./s.bin"])).unwrap();
        assert!(!a.wipe);
        assert_eq!(a.format, SnapshotFormat::Bin, "默认 bin（与旧四家一致）");
        // 三个旧名字进同一个字段
        for name in ["--in", "--input", "--file"] {
            let b = parse_args(&v(&["--engine", "lsm", "--dir", "./d", name, "./s.jsonl", "--format", "jsonl", "--wipe"])).unwrap();
            assert_eq!(b.input.to_string_lossy().as_ref(), "./s.jsonl", "{name} 没被吃进来");
            assert!(b.wipe && b.format == SnapshotFormat::Jsonl);
        }
    }

    /// 三个必填各缺一次都要红；导出才有的旗标在这边也不许被默默吃下。
    #[test]
    fn parse_requires_engine_dir_and_input() {
        for argv in [
            v(&["--dir", "./d", "--in", "./s.bin"]),
            v(&["--engine", "bitcask", "--in", "./s.bin"]),
            v(&["--engine", "bitcask", "--dir", "./d"]),
            v(&["--engine", "bitcask", "--dir", "./d", "--in"]),
            v(&["--engine", "bitcask", "--dir", "./d", "--in", "./s", "--demo", "3"]),
        ] {
            assert!(parse_args(&argv).is_err(), "少了东西/串了门却过了: {argv:?}");
        }
    }

    /// 一条真往返：灌 5 行 → 导出 → 导入到另一家 → 行数与文件条数对得上。
    /// （bin 之间不能互相引用，所以这里自己灌样例 —— 与 `export-kv` 里那份同形状，只有 5 行。）
    #[test]
    fn export_then_import_lands_in_the_other_engine() {
        use storage_engines::common::kv_snapshot::read_bin;
        use storage_engines::tools::migrate::export_to_dir;
        use storage_engines::{EngineConfig, open_engine_with};

        let root = std::env::temp_dir().join("se_cli_pair");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let src = root.join("lsm_tree");
        let dst = root.join("bitcask");
        let snap = root.join("s.bin");
        {
            let db = open_engine_with(EngineKind::LsmTree, Some(&src), &EngineConfig::default()).unwrap();
            let items: Vec<(Vec<u8>, Vec<u8>)> = (0..5)
                .map(|i| (format!("demo-{i:06}#/x").into_bytes(), format!("v{i}").into_bytes()))
                .collect();
            let tx = db.begin();
            tx.batch_set(&items).unwrap();
            tx.commit();
            db.flush().unwrap();
            db.checkpoint().unwrap();
        }
        let out = export_to_dir(EngineKind::LsmTree, &src, &snap, SnapshotFormat::Bin, false).unwrap();
        assert_eq!((out.records, out.live), (5, 5), "导出的行数不对");
        let st = import_from_dir(EngineKind::Bitcask, &dst, &snap, SnapshotFormat::Bin, false).unwrap();
        assert_eq!((st.records, st.live), (5, 5));
        assert_eq!(read_bin(&snap).unwrap().0.len(), 5, "文件比库少行");
        let _ = std::fs::remove_dir_all(&root);
    }
}
