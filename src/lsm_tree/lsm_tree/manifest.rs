//! 本文件负责文件集合清单的持久化发布。
//! 模块边界仅组织代码：锁顺序、版本可见性、磁盘格式和发布失败的处理均沿用原契约。

use super::*;

#[cfg(test)]
pub(super) fn manifest_publishes_reference(text: &str, ids: &[u64]) -> bool {
    ids.iter()
        .all(|id| text.split_whitespace().any(|word| word == id.to_string()))
}

#[cfg(test)]
pub(super) fn manifest_publishes(text: &str, level: usize, ids: &[u64]) -> bool {
    let tag = format!("L{level}");
    let mut lines = text.lines().filter_map(|line| {
        let mut words = line.split_whitespace();
        (words.next() == Some(tag.as_str())).then_some(words)
    });
    let Some(words) = lines.next() else {
        return false;
    };
    // 拒绝歧义或无效的目标层清单行；元数据及其它层不能授权本次发布。
    if lines.next().is_some() {
        return false;
    }
    let Some(published) = words
        .map(|word| word.parse::<u64>().ok())
        .collect::<Option<HashSet<_>>>()
    else {
        return false;
    };
    ids.iter().all(|id| published.contains(id))
}

impl LSMTree {
    pub(super) fn manifest_path(&self) -> PathBuf {
        self.dir.join("MANIFEST")
    }

    pub(super) fn sst_path(&self, id: u64) -> PathBuf {
        self.dir.join("sst").join(format!("{id:08}.sst"))
    }

    // ─── MANIFEST ────────────────────────────────────────────────────────────

    pub(super) fn write_manifest_body(&self, mut f: impl Write) -> std::io::Result<()> {
        writeln!(f, "LSMAN002")?;
        writeln!(f, "edit {}", self.manifest_edit_id)?;
        writeln!(f, "next_id {}", self.next_sst_id.load(Ordering::SeqCst))?;
        writeln!(f, "next_seq {}", self.write_seq.load(Ordering::SeqCst))?;
        for (i, level) in self.levels.iter().enumerate() {
            write!(f, "L{i}")?;
            for sst in level {
                write!(f, " {}", sst.id)?;
            }
            writeln!(f)?;
        }
        Ok(())
    }

    pub(super) fn write_manifest_output(&self, writer: impl Write) -> std::io::Result<()> {
        #[cfg(test)]
        if manifest_reference::enabled() {
            let mut writer = writer;
            self.write_manifest_body(&mut writer)?;
            return writer.flush();
        }
        let mut buffered = BufWriter::with_capacity(64 * 1024, writer);
        self.write_manifest_body(&mut buffered)?;
        // 刷出缓冲的错误必须在同步和发布前暴露；析构无法向调用方报告它们。
        buffered.flush()
    }

    pub(super) fn write_manifest(&mut self) -> std::io::Result<()> {
        #[cfg(test)]
        let _timer = maintenance_probe::Timer::start(maintenance_probe::Kind::Manifest);
        self.manifest_edit_id = self.manifest_edit_id.saturating_add(1);
        let path = self.manifest_path();
        let tmp = self.dir.join("MANIFEST.tmp");
        {
            let mut f = File::create(&tmp)?;
            #[cfg(test)]
            self.write_manifest_output(manifest_reference::Sink(&mut f))?;
            #[cfg(not(test))]
            self.write_manifest_output(&mut f)?;
            if !self.bulk_mode {
                #[cfg(test)]
                crate::common::disk::note_fsync(crate::common::disk::SITE_LSM_MANIFEST);
                f.sync_all()?;
            }
        }
        fs::rename(&tmp, &path)?;
        // 历史记录保持尽力写入策略，先构造完整一行再追加。
        let hist = self.dir.join("MANIFEST.LOG");
        if let Ok(mut hf) = File::options().create(true).append(true).open(&hist) {
            let line = format!(
                "EDIT {} next_id={} next_seq={} levels={}\n",
                self.manifest_edit_id,
                self.next_sst_id.load(Ordering::SeqCst),
                self.write_seq.load(Ordering::SeqCst),
                self.levels
                    .iter()
                    .map(|l| l.len().to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            );
            let _ = hf.write_all(line.as_bytes());
        }
        Ok(())
    }
    /// 根据压实是否已经发布同一文件集合决定是否补写清单。
    /// 已发布状态要与当前编辑序号一致；只共享结果但未生效的情况不能算作免同步。

    /// 最后一次压实发布成功后，到此入口之间不得再改变文件或序号；每次压实仍在清理输入前发布，只省略外层同一编辑。
    pub(super) fn publish_after_maintenance(
        &mut self,
        before: u64,
    ) -> std::io::Result<MaintenanceManifest> {
        #[cfg(test)]
        let reference = manifest_repeat_reference::enabled();
        #[cfg(not(test))]
        let reference = false;
        if !reference
            && !self.bulk_mode
            && !self.opts.enable_bg_compact
            && before != self.manifest_edit_id
        {
            #[cfg(test)]
            {
                let text = fs::read_to_string(self.manifest_path())?;
                for (level, files) in self.levels.iter().enumerate() {
                    let expected = files.iter().map(|f| f.id).collect::<Vec<_>>();
                    let marker = format!("L{level}");
                    let actual = text
                        .lines()
                        .find_map(|line| {
                            let mut fields = line.split_whitespace();
                            (fields.next() == Some(marker.as_str())).then(|| {
                                fields
                                    .map(|id| id.parse::<u64>().unwrap())
                                    .collect::<Vec<_>>()
                            })
                        })
                        .expect("published level");
                    assert_eq!(
                        actual, expected,
                        "reused manifest must name exact current file set"
                    );
                }
                assert!(
                    text.lines().any(|line| line
                        == format!("next_id {}", self.next_sst_id.load(Ordering::SeqCst)))
                );
                assert!(
                    text.lines().any(|line| line
                        == format!("next_seq {}", self.write_seq.load(Ordering::SeqCst)))
                );
                maintenance_probe::manifest_reused();
            }
            return Ok(MaintenanceManifest::PublishedByCompaction);
        }
        self.write_manifest()?;
        Ok(MaintenanceManifest::PublishedHere)
    }

    pub(super) fn load_manifest(&mut self) -> std::io::Result<()> {
        let f = File::open(self.manifest_path())?;
        let mut lines = BufReader::new(f).lines();
        let magic = lines
            .next()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "空 MANIFEST"))??;
        let magic = magic.trim();
        if magic != "LSMAN001" && magic != "LSMAN002" {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "非法 MANIFEST 魔数",
            ));
        }
        for line in lines {
            let line = line?;
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let mut parts = line.split_whitespace();
            let Some(tag) = parts.next() else { continue };
            if tag == "next_id" {
                let id: u64 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(1);
                self.next_sst_id.store(id, Ordering::SeqCst);
                continue;
            }
            if tag == "next_seq" {
                let s: u64 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(1);
                self.write_seq.store(s.max(1), Ordering::SeqCst);
                continue;
            }
            if tag == "edit" {
                self.manifest_edit_id = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
                continue;
            }
            if let Some(rest) = tag.strip_prefix('L') {
                if let Ok(level) = rest.parse::<usize>() {
                    if level >= self.levels.len() {
                        // 扩展层数
                        while self.levels.len() <= level {
                            self.levels.push(Vec::new());
                        }
                    }
                    let mut files = Vec::new();
                    for id_s in parts {
                        if let Ok(id) = id_s.parse::<u64>() {
                            let path = self.sst_path(id);
                            if path.exists() {
                                let mut sst = SstFile::open_with(id, path, self.opts.use_mmap)?;
                                sst.set_block_cache(self.block_cache.clone());
                                files.push(sst);
                            }
                        }
                    }
                    self.levels[level] = files;
                    self.refresh_level_ranges(level);
                }
            }
        }
        Ok(())
    }

    pub(super) fn import_legacy_snap(&mut self, path: &Path) -> std::io::Result<()> {
        use std::io::Read;
        const SNAP_MAGIC: &[u8; 8] = b"LSMSNAP1";
        const TOMBSTONE: u32 = u32::MAX;
        let f = File::open(path)?;
        let mut r = BufReader::new(f);
        let mut magic = [0u8; 8];
        r.read_exact(&mut magic)?;
        if &magic != SNAP_MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "非法旧快照魔数",
            ));
        }
        let mut buf8 = [0u8; 8];
        r.read_exact(&mut buf8)?;
        let count = u64::from_le_bytes(buf8) as usize;
        for _ in 0..count {
            let mut buf4 = [0u8; 4];
            r.read_exact(&mut buf4)?;
            let kl = u32::from_le_bytes(buf4) as usize;
            let mut key: Vec<u8> = vec![0u8; kl];
            r.read_exact(&mut key)?;
            r.read_exact(&mut buf4)?;
            let vl = u32::from_le_bytes(buf4);
            let value: Option<Vec<u8>> = if vl == TOMBSTONE {
                None
            } else {
                let mut val: Vec<u8> = vec![0u8; vl as usize];
                r.read_exact(&mut val)?;
                Some(val)
            };
            let seq = self.write_seq.fetch_add(1, Ordering::SeqCst);
            self.mem_insert_no_wal(key, StoredEntry::put(seq, value));
        }
        self.dirty = true;
        Ok(())
    }
}
