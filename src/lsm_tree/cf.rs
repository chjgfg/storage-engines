//! 多列族引擎：每个 CF 是独立的 [`LSMTree`]（独立 MANIFEST / SST / mem.wal）。
//!
//! 目录布局：
//! ```text
//! root/
//!   CURRENT          # 文本：default_cf + cf 列表
//!   default/         # 默认 CF
//!   cf/<name>/       # 其它 CF
//! ```

use std::vec;
use std::writeln;
use std::{
    collections::HashMap,
    fs::{self, File},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use crate::lsm_tree::{LSMTree, LSMTreeOptions};
use crate::lsm_tree::sst::BlockCache;

/// 多列族数据库
pub struct LsmDB {
    root: PathBuf,
    default_name: String,
    cfs: HashMap<String, Arc<Mutex<LSMTree>>>,
    default_opts: LSMTreeOptions,
    /// 跨 CF 共享 block cache
    shared_cache: Arc<Mutex<BlockCache>>,
}

impl LsmDB {
    pub fn open(root: impl AsRef<Path>) -> std::io::Result<Self> {
        Self::open_with_config(root, LSMTreeOptions::default())
    }

    pub fn open_with_config(
        root: impl AsRef<Path>,
        opts: LSMTreeOptions,
    ) -> std::io::Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        let current = root.join("CURRENT");
        let mut names = vec!["default".to_string()];
        if current.exists() {
            names = load_current(&current)?;
        } else {
            write_current(&current, &names)?;
        }

        let shared_cache = Arc::new(Mutex::new(BlockCache::new(opts.block_cache_blocks)));
        let mut cfs = HashMap::new();
        for name in &names {
            let dir = cf_dir(&root, name);
            let mut tree = LSMTree::open_with_config(&dir, opts.clone())?;
            tree.install_shared_block_cache(shared_cache.clone());
            cfs.insert(name.clone(), Arc::new(Mutex::new(tree)));
        }
        Ok(Self {
            root,
            default_name: "default".to_string(),
            cfs,
            default_opts: opts,
            shared_cache,
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn list_cfs(&self) -> Vec<String> {
        let mut v: Vec<_> = self.cfs.keys().cloned().collect();
        v.sort();
        v
    }

    pub fn default_cf(&self) -> Arc<Mutex<LSMTree>> {
        self.cf(&self.default_name).expect("default")
    }

    pub fn cf(&self, name: &str) -> Option<Arc<Mutex<LSMTree>>> {
        self.cfs.get(name).cloned()
    }

    /// 创建列族（已存在则返回已有）
    pub fn create_cf(&mut self, name: &str) -> std::io::Result<Arc<Mutex<LSMTree>>> {
        if let Some(c) = self.cfs.get(name) {
            return Ok(c.clone());
        }
        if name.is_empty() || name.contains('/') || name.contains('\\') {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "非法 CF 名",
            ));
        }
        let dir = cf_dir(&self.root, name);
        let mut tree = LSMTree::open_with_config(&dir, self.default_opts.clone())?;
        tree.install_shared_block_cache(self.shared_cache.clone());
        let arc = Arc::new(Mutex::new(tree));
        self.cfs.insert(name.to_string(), arc.clone());
        self.persist_current()?;
        Ok(arc)
    }

    /// 删除列族（不可删 default）
    pub fn drop_cf(&mut self, name: &str) -> std::io::Result<()> {
        if name == "default" {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "不能删除 default CF",
            ));
        }
        if self.cfs.remove(name).is_none() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "CF 不存在",
            ));
        }
        let dir = cf_dir(&self.root, name);
        let _ = fs::remove_dir_all(dir);
        self.persist_current()?;
        Ok(())
    }

    /// 原子性：顺序 flush 全部 CF（单线程；失败时已 flush 的不回滚）
    pub fn flush_all(&self) -> std::io::Result<()> {
        for name in self.list_cfs() {
            if let Some(cf) = self.cf(&name) {
                cf.lock().unwrap().flush()?;
            }
        }
        Ok(())
    }

    /// 热更新所有 CF 的可调参数（保留各 CF 的 mem_wal 开关）
    pub fn set_options_all(&self, opts: LSMTreeOptions) {
        for name in self.list_cfs() {
            if let Some(cf) = self.cf(&name) {
                cf.lock().unwrap().set_options(opts.clone());
            }
        }
    }

    fn persist_current(&self) -> std::io::Result<()> {
        let mut names: Vec<_> = self.cfs.keys().cloned().collect();
        names.sort();
        // default 放前
        names.retain(|n| n != "default");
        names.insert(0, "default".to_string());
        write_current(&self.root.join("CURRENT"), &names)
    }
}

fn cf_dir(root: &Path, name: &str) -> PathBuf {
    if name == "default" {
        root.join("default")
    } else {
        root.join("cf").join(name)
    }
}

fn write_current(path: &Path, names: &[String]) -> std::io::Result<()> {
    let mut f = File::create(path)?;
    writeln!(f, "LSMCF01")?;
    for n in names {
        writeln!(f, "{n}")?;
    }
    f.sync_all()?;
    Ok(())
}

fn load_current(path: &Path) -> std::io::Result<Vec<String>> {
    let f = File::open(path)?;
    let mut lines = BufReader::new(f).lines();
    let magic = lines
        .next()
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "空 CURRENT"))??;
    if magic.trim() != "LSMCF01" {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "非法 CURRENT",
        ));
    }
    let mut names = Vec::new();
    for line in lines {
        let line = line?;
        let line = line.trim();
        if !line.is_empty() {
            names.push(line.to_string());
        }
    }
    if names.is_empty() {
        names.push("default".to_string());
    }
    Ok(names)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::format;

    #[test]
    fn test_multi_cf() {
        let dir = std::env::temp_dir().join(format!(
            "lsmdb_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = fs::remove_dir_all(&dir);
        {
            let mut db = LsmDB::open(&dir).unwrap();
            {
                let def = db.default_cf();
                let mut t = def.lock().unwrap();
                t.put(b"k".to_vec(), b"d".to_vec());
                t.flush().unwrap();
            }
            let users = db.create_cf("users").unwrap();
            {
                let mut t = users.lock().unwrap();
                t.put(b"k".to_vec(), b"u".to_vec());
                t.flush().unwrap();
            }
            assert_eq!(
                db.default_cf().lock().unwrap().get(b"k"),
                Some(b"d".to_vec())
            );
            assert_eq!(
                db.cf("users").unwrap().lock().unwrap().get(b"k"),
                Some(b"u".to_vec())
            );
            let list = db.list_cfs();
            assert!(list.contains(&"default".to_string()));
            assert!(list.contains(&"users".to_string()));
        }
        {
            let db = LsmDB::open(&dir).unwrap();
            assert_eq!(
                db.cf("users").unwrap().lock().unwrap().get(b"k"),
                Some(b"u".to_vec())
            );
        }
        let _ = fs::remove_dir_all(&dir);
    }
}
