//! 本文件负责索引提示文件的发布和回放。

use super::*;

/// hint 门能**一次性预留**多少条（M77）：声明的条数 与 "这份文件物理上装得下多少条" 取小。
///
/// 为什么 hint 这一侧敢用声明值、而 merge 那一侧不敢（M60 立的规矩是"不可信声明值一律分步预留"）：
/// 声明值旁边就躺着它的凭据 —— 文件字节数。一条记录至少 `record_fixed` 字节（键长 0 时取等），
/// 所以文件里**不可能**装下 `(字节数 − 头部) ÷ record_fixed` 条以上 ⇒ 界内预留不会比文件本身能装的更多，
/// 一个坏掉/被人改大的 `entry_count` 换不来一次超大分配。
///
/// 为什么值得动：M60 那版分步 `reserve_exact(65_536)` 在 600k 档要 realloc **9 次**、每次把整份清单搬一遍。
/// 同一构建、同一形状、同一个二进制里的两臂实测（`bitcask_hint_load_stage_split`，四轮两种顺序取最小）：
/// 真门 **284.2 ms** vs "一次攒够再成树"那臂 **173.5 ms**（1.64×），而瞬时超出反而**更小**
/// （`over` 31.5 → 28.8 MB）⇒ 两笔都是净赚，没有取舍。
pub(super) fn hint_reserve_bound(
    file_bytes: u64,
    header_bytes: usize,
    record_fixed: u64,
    entry_count: u64,
) -> usize {
    let body = file_bytes.saturating_sub(header_bytes as u64);
    let can_fit = body / record_fixed;
    // 32 位平台上 `can_fit` 可能超出 `usize` ⇒ 夹一下，别让它变成 `as usize` 的静默截断
    usize::try_from(entry_count.min(can_fit)).unwrap_or(usize::MAX)
}

/// 由数据文件路径推导 hint：`foo.log` → `foo.hint`
pub fn hint_path_for(data_path: &Path) -> PathBuf {
    data_path.with_extension("hint")
}

/// 密封分片路径：`data.log` + id → `data.{id}.log`（active 始终是 `data.log` 本身）
pub fn segment_path(active_path: &Path, file_id: u32) -> PathBuf {
    // 活动日志 data.log 封存后成为 data.0.log 或 data.3.log 等带段号文件。
    let stem = active_path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("data");
    let parent = active_path.parent().unwrap_or_else(|| Path::new("."));
    parent.join(format!("{stem}.{file_id}.log"))
}

/// 扫描目录中已有密封分片 id（不含 active 文件本身）
pub(super) fn discover_sealed_ids(active_path: &Path) -> Vec<u32> {
    let stem = active_path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("data");
    let parent = match active_path.parent() {
        Some(p) => p,
        None => return Vec::new(),
    };
    let mut ids = Vec::new();
    let Ok(rd) = std::fs::read_dir(parent) else {
        return ids;
    };
    let prefix = format!("{stem}.");
    for ent in rd.flatten() {
        let name = ent.file_name();
        let name = name.to_string_lossy();
        // data.12.log
        if let Some(rest) = name.strip_prefix(&prefix) {
            if let Some(num) = rest.strip_suffix(".log") {
                if let Ok(id) = num.parse::<u32>() {
                    ids.push(id);
                }
            }
        }
    }
    ids.sort_unstable();
    ids
}

/// hint 里的每条记录是否都指向**还在**的文件（active 或某个密封分片）。
///
/// 崩在 `merge()` 中途会留下这种 hint：它记的是合并前的布局，而那批 `data.{id}.log`
/// 已经被删掉了。增量扫只补 active 文件里新增的行，指向失踪分片的键**永远不会被补回来**
/// ⇒ 悄悄少数据，比慢严重。所以这里当作 hint 失效处理，回退全量扫。
pub(super) fn hint_files_exist(kd: &KeyDir, active_id: u32, sealed_ids: &[u32]) -> bool {
    kd.values().all(|loc| {
        let fid = match loc {
            KeyLoc::Live { file_id, .. } => *file_id,
            KeyLoc::SoftTombstone { file_id } => *file_id,
        };
        fid == active_id || sealed_ids.contains(&fid)
    })
}

/// 写出 hint v2（原子）
pub fn write_hint_file(
    path: &Path,
    key_dir: &KeyDir,
    active_id: u32,
    active_log_size: u64,
) -> Result<()> {
    #[cfg(test)]
    let mut measured_bytes = HINT_HEADER_SIZE_V2 as u64;
    let tmp = {
        let mut p = path.as_os_str().to_os_string();
        p.push(".tmp");
        PathBuf::from(p)
    };
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            let _ = std::fs::create_dir_all(parent);
        }
    }
    {
        let f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)?;
        let mut w = BufWriter::new(f);
        let count = key_dir.len() as u64;
        let mut hdr = [0u8; HINT_HEADER_SIZE_V2];
        hdr[0..8].copy_from_slice(HINT_MAGIC_V2);
        hdr[8..12].copy_from_slice(&active_id.to_le_bytes());
        // pad 12..16 = 0
        hdr[16..24].copy_from_slice(&active_log_size.to_le_bytes());
        hdr[24..32].copy_from_slice(&count.to_le_bytes());
        // reserved 32..40 = 0
        w.write_all(&hdr)?;

        for (key, loc) in key_dir.iter() {
            #[cfg(test)]
            {
                measured_bytes += HINT_RECORD_FIXED_BYTES_V2 + key.len() as u64;
            }
            let (flag, file_id, pos, len) = match loc {
                KeyLoc::Live { file_id, pos, len } => (HINT_FLAG_LIVE, *file_id, *pos, *len),
                KeyLoc::SoftTombstone { file_id } => (FLAG_SOFT_TOMBSTONE, *file_id, 0u64, 0u32),
            };
            w.write_all(&(key.len() as u32).to_le_bytes())?;
            w.write_all(&flag.to_le_bytes())?;
            w.write_all(&file_id.to_le_bytes())?;
            w.write_all(&pos.to_le_bytes())?;
            w.write_all(&len.to_le_bytes())?;
            w.write_all(key)?;
        }
        w.flush()?;
        w.into_inner().map_err(|e| e.into_error())?.sync_all()?;
        // M53 记账
        #[cfg(test)]
        crate::common::disk::note_fsync(crate::common::disk::SITE_BITCASK_HINT);
    }
    if path.exists() {
        let _ = std::fs::remove_file(path);
    }
    std::fs::rename(&tmp, path)?;
    #[cfg(test)]
    super::super::checkpoint_probe::hint_written(key_dir.len() as u64, measured_bytes);
    Ok(())
}

/// hint 记录的**流式解码器**（M52c）。两份格式共用一把读法：v2 带 `file_id`，v1 一律 0。
///
/// 为什么是迭代器而不是"先解成 `Vec<记录>` 再折成树"（那是本格的第一版写法）：那份 `Vec`
/// 是**整库形状**的中间物 —— 600k 条 × 48 B 的骨架就是 28.8 MB，而树本身才 57.7 MB，
/// 等于让"读索引"这一步临时多要半棵索引。`collect()` 直接从这个迭代器取，
/// `BTreeMap` 的有序批量填充照旧生效（台架 A 臂读数没掉回 E 臂那一档就是证据）。
///
/// ⚠️ `entry_count` 是从文件头读来的，**不可信**：改前第一版给它做 `with_capacity`，
/// 一个写 2^60 的坏 hint 就会让我们先申请几 GB。迭代器这一版没有那一格 —— 预留由不上了，
/// 真正的记录数由读取本身说话：撞 EOF ⇒ `Err` ⇒ 调用方的处置是回退全量扫。
/// 同一处还修掉一条旧的不一致：v1 那份循环漏了 `key_len` 的上界检查，会先 `vec![0u8; key_len]`
/// 再去读；共用之后两份都先检查（越界 ⇒ 报错），一个荒谬的键长不再先把自己撑爆。
pub(super) struct HintEntries<'a, R> {
    pub(super) r: &'a mut R,
    /// 还剩几条没解（由文件头声明，读不满就报错）
    pub(super) left: u64,
    pub(super) with_file_id: bool,
}

impl<'a, R: std::io::Read> HintEntries<'a, R> {
    pub(super) fn new(r: &'a mut R, entry_count: u64, with_file_id: bool) -> Self {
        Self {
            r,
            left: entry_count,
            with_file_id,
        }
    }

    /// 解一条记录。`Ok(None)` = **未知种类** ⇒ 丢掉但继续往下读（沿用改前口径，不许变成"当作存活"）；
    /// `Err` = 读不出来 / 坏键长 ⇒ 调用方据此回退全量扫。
    pub(super) fn one(&mut self) -> Result<Option<(Vec<u8>, KeyLoc)>> {
        let mut nbuf = [0u8; 4];
        let mut pbuf = [0u8; 8];
        self.r.read_exact(&mut nbuf)?;
        let key_len = u32::from_le_bytes(nbuf) as usize;
        self.r.read_exact(&mut nbuf)?;
        let flag = i32::from_le_bytes(nbuf);
        let file_id = if self.with_file_id {
            self.r.read_exact(&mut nbuf)?;
            u32::from_le_bytes(nbuf)
        } else {
            0
        };
        self.r.read_exact(&mut pbuf)?;
        let pos = u64::from_le_bytes(pbuf);
        self.r.read_exact(&mut nbuf)?;
        let len = u32::from_le_bytes(nbuf);
        if key_len > 16 * 1024 * 1024 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "hint key_len 异常",
            ));
        }
        let mut key: Vec<u8> = vec![0u8; key_len];
        self.r.read_exact(&mut key)?;
        Ok(match flag {
            HINT_FLAG_LIVE => Some((key, KeyLoc::Live { file_id, pos, len })),
            FLAG_SOFT_TOMBSTONE => Some((key, KeyLoc::SoftTombstone { file_id })),
            _ => None,
        })
    }
}

impl<R: std::io::Read> Iterator for HintEntries<'_, R> {
    /// 项里套 `Option` 是为了让"丢掉这一条"和"这一条读不出来"分成两件事；
    /// 上面用 `.map(transpose).flatten()` 把它们收拢成 `io::Result<(键, 位置)>` 的流。
    type Item = Result<Option<(Vec<u8>, KeyLoc)>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        Some(self.one())
    }
}

/// hint 记录 → `KeyDir`。
///
/// ⚠️ 这里**不是一条一条 `insert`**（M52a）。hint 里的键本来就是按键升序写出的
/// （`write_hint_file` 遍历的就是排好序的 `KeyDir`），而 `BTreeMap::insert` 对有序输入
/// 每次都要从根沿最右一路走到底，还会把沿途每个节点**劈成半满**：
/// 同构建 A/B（`bitcask_hint_load_stage_split`，四轮两种顺序取最小）600k 档
/// 时间 **405.3 → 205.6 ms（1.97×）**、200k 档 **180.6 → 62.8 ms（2.87×）**；
/// 建好之后**还活着** 65.4 → 39.7 MB —— 差的 **25.7 MB** 与大库台架那一格
/// `冷重开 常驻 83.4 → 57.7 MB` **一字不差**，而两臂解出的键数相同 ⇒
/// 那 25.7 MB 纯粹是**半满节点**，不是内容。重复键的取舍与逐条 `insert` 相同（后者胜）。
///
/// ⚠️ M60 起这一趟不再直接 `.collect()`，改走 [`KeyDirBulk`]；M77 起 hint 这一侧的攒法是
/// **一次预留到位**（界由 [`hint_reserve_bound`] 用文件字节数算出来），不再分步 ——
/// 分步那版在 600k 档要 realloc 9 次、每次搬整份清单，实测比"一次攒够"慢 1.64×（284.2 vs 173.5 ms），
/// 而瞬时超出反而更大（`over` 31.5 vs 28.8 MB）⇒ 两笔都是净赚。
/// M59 那句仍然成立且是这条界的理由：**不能凭声明值** 直接 `with_capacity(声明的条数)`
/// （读到一个大数就先做一次巨大分配），所以要拿"文件物理上装得下多少条"夹一道。
/// merge 那一侧没有这种凭据 ⇒ 继续用分步 `reserve_exact`（最坏过冲夹在一个步长里）。
pub(super) fn hint_entries_to_keydir<R: std::io::Read>(
    r: &mut R,
    entry_count: u64,
    with_file_id: bool,
    reserve_bounded: usize,
) -> Result<KeyDir> {
    // 台架对照臂（门控 `HINT_OLD_SHAPE`，`#[cfg(test)]` ⇒ 不随 crate 发布）：**同一个解码器**，
    // 只把折树那一步退回逐条 `insert`。共用解码器是故意的 —— 两臂只差一件事，
    // 读出来的比值才只归因于形状（大库台架那一趟读的是现场形状：版本键混着软墓碑、
    // 进程已经分配过 1 GB），而臂级台架读的是一份干净的 600k 升序键。
    #[cfg(test)]
    if std::env::var("HINT_OLD_SHAPE").is_ok() {
        let mut key_dir = KeyDir::new();
        for item in HintEntries::new(r, entry_count, with_file_id) {
            if let Some((key, loc)) = item? {
                key_dir.insert(key, loc);
            }
        }
        return Ok(key_dir);
    }
    keydir_from_ordered_stream(
        KeyDirBulk::with_capacity(reserve_bounded),
        HintEntries::new(r, entry_count, with_file_id),
    )
}

/// 读取 hint。成功返回 `(KeyDir, active_id, active_log_size)`
pub fn read_hint_file(path: &Path) -> Result<(KeyDir, u32, u64)> {
    let mut f = std::fs::File::open(path)?;
    // 预留的界要用**开文件时**的字节数：`BufReader` 一旦接管 `f`，元信息就只能再走一次路径查询了
    let file_bytes = f.metadata().map(|m| m.len()).unwrap_or(0);
    let mut magic = [0u8; 8];
    f.read_exact(&mut magic)?;
    if &magic == HINT_MAGIC_V2 {
        let mut rest = [0u8; HINT_HEADER_SIZE_V2 - 8];
        f.read_exact(&mut rest)?;
        let active_id = u32::from_le_bytes(rest[0..4].try_into().unwrap());
        let active_log_size = u64::from_le_bytes(rest[8..16].try_into().unwrap());
        let entry_count = u64::from_le_bytes(rest[16..24].try_into().unwrap());
        let mut r = BufReader::new(f);
        let key_dir = hint_entries_to_keydir(
            &mut r,
            entry_count,
            true,
            hint_reserve_bound(
                file_bytes,
                HINT_HEADER_SIZE_V2,
                HINT_RECORD_FIXED_BYTES_V2,
                entry_count,
            ),
        )?;
        Ok((key_dir, active_id, active_log_size))
    } else if &magic == HINT_MAGIC_V1 {
        // 兼容旧 hint：file_id 全 0，header 剩余 24 字节
        let mut rest = [0u8; HINT_HEADER_SIZE_V1 - 8];
        f.read_exact(&mut rest)?;
        let log_size = u64::from_le_bytes(rest[0..8].try_into().unwrap());
        let entry_count = u64::from_le_bytes(rest[8..16].try_into().unwrap());
        let mut r = BufReader::new(f);
        let key_dir = hint_entries_to_keydir(
            &mut r,
            entry_count,
            false,
            hint_reserve_bound(
                file_bytes,
                HINT_HEADER_SIZE_V1,
                HINT_RECORD_FIXED_BYTES_V1,
                entry_count,
            ),
        )?;
        Ok((key_dir, 0, log_size))
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "非法 hint 魔数",
        ))
    }
}

impl Bitcask {
    pub(super) fn replay_hint(
        base: &Path,
        sealed: &[u32],
        kd: &mut KeyDir,
        hint_id: u32,
        offset: u64,
        active: &mut Log,
    ) -> Result<HintReplay> {
        let current = match sealed.last() {
            Some(id) => id
                .checked_add(1)
                .ok_or_else(|| std::io::Error::other("segment id exhausted"))?,
            None => 0,
        };
        if hint_id > current || !hint_files_exist(kd, current, sealed) {
            return Err(std::io::Error::other(
                "hint references missing segment history",
            ));
        }
        if hint_id == current {
            active.load_index_from(kd, current, offset)?;
            return Ok(HintReplay::ActiveTail);
        }
        // 旧活动段已封存，先回放其尾部，再按追加顺序回放后续全部段，包括硬删除；不能把旧偏移用于今天的活动段。
        let mut expected = hint_id;
        let mut segments = 0;
        for &id in sealed.iter().filter(|&&id| id >= hint_id) {
            if id != expected {
                return Err(std::io::Error::other("gap in hint replay history"));
            }
            let mut log = Log::new(segment_path(base, id))?;
            log.load_index_from(kd, id, if id == hint_id { offset } else { 0 })?;
            expected = id + 1;
            segments += 1;
        }
        if expected != current {
            return Err(std::io::Error::other("incomplete hint replay history"));
        }
        active.load_index_from(kd, current, 0)?;
        Ok(HintReplay::Rotated {
            active_id: current,
            sealed_segments: segments,
        })
    }

    /// 认回 / 清理崩在中间的 merge 产物，让 `merge()` 里那几步删除变得可恢复。
    ///
    /// `merge()` 的收口是「删密封分片 → 删旧 `data.log` → 把 `data.merge` 改名成 `data.log`」。
    /// 第三步之前崩掉，磁盘上就只剩 `data.merge` 装着全部存活记录，而 open 找的是 `data.log`
    /// —— 原样打开会得到一个**空库**：文件都在，谁都不报错，数据却"没了"。
    ///
    /// 两种现场，两种处置：
    /// - `data.log` 不在、`data.merge` 在 ⇒ 改名认正。merge 的产物本来就包含全部存活键，
    ///   且 merge 期间是单写者（文件锁 + 同一个 `&mut self`）⇒ 不会有任何行只在 `data.log` 里。
    /// - 两者都在 ⇒ 崩在删旧文件之前，两边都是完整快照；留下 `data.log`（原路径继续走），
    ///   把 `data.merge` 删掉。⚠️ 必须删：下次 merge 会以同一个名字打开那个残骸**往后追加**，
    ///   把两份不同的记录混成一个文件 —— 那才是真的救不回来。
    pub(super) fn recover_merge_artifact(base_path: &Path) -> Result<()> {
        let mut merge_path = base_path.to_path_buf();
        merge_path.set_extension(MERGE_FILE_EXT);
        if !merge_path.exists() {
            return Ok(());
        }
        if base_path.exists() {
            log::warn!(
                "发现未完成 merge 的残骸 {}（{} 仍在），删掉残骸按原日志打开",
                merge_path.display(),
                base_path.display()
            );
            std::fs::remove_file(&merge_path)?;
        } else {
            log::warn!(
                "{} 缺失而 {} 存在 ⇒ 上次 merge 崩在改名之前，把 merge 产物认作数据日志",
                base_path.display(),
                merge_path.display()
            );
            std::fs::rename(&merge_path, base_path)?;
        }
        Ok(())
    }

    /// 全量重建：扫所有密封分片 + active
    pub(super) fn full_rebuild(base_path: &Path, sealed_ids: &[u32]) -> Result<(KeyDir, u32, Log)> {
        let mut key_dir = KeyDir::new();
        for &id in sealed_ids {
            let p = segment_path(base_path, id);
            if !p.exists() {
                continue;
            }
            let mut log = Log::new(p)?;
            let partial = log.load_index(id)?;
            for (k, v) in partial {
                key_dir.insert(k, v);
            }
            // 释放锁；读路径会 lazy reopen
            drop(log);
        }

        let active = Log::new(base_path.to_path_buf())?;
        // active_id：无密封时 0；有密封取 max+1（若 active 为空且从未 rotate，仍可能是 0）
        let active_id = if sealed_ids.is_empty() {
            0
        } else {
            sealed_ids
                .iter()
                .copied()
                .max()
                .unwrap_or(0)
                .saturating_add(1)
        };
        let partial = {
            // 需要 mut active 扫
            let mut active_mut = active;
            let kd = active_mut.load_index(active_id)?;
            for (k, v) in kd {
                key_dir.insert(k, v);
            }
            (key_dir, active_id, active_mut)
        };
        Ok(partial)
    }

    /// 对应的 hint 文件路径
    pub fn hint_path(&self) -> PathBuf {
        hint_path_for(&self.base_path)
    }

    /// 把当前 KeyDir 写出到 hint（HINT0002：active_id + active_log_size）。
    ///
    /// 普通写路径不自动调用；适合：
    /// - [`Self::merge`] 之后（自动）
    /// - 进程优雅退出 / 运维主动 checkpoint
    /// - bulk load `finish` 后（可由上层调用）
    pub fn dump_hint(&mut self) -> Result<()> {
        // ⚠️ 这道守卫是**数据**守卫，不是性能守卫：hint 是内存索引的持久化副本。重扫窗口里
        // 把"空索引"写成 hint 落盘，下次 open 就把它当真索引读回来 ⇒ 整库在磁盘上完好，却没人
        // 知道有哪些键。open 那边的 `hint_files_exist` 兜不住这一支（空表里挑不出"指向已删分片"
        // 的条目，而"新日志比旧日志长"的时候连长度检查都会放行）。
        self.usable()?;
        self.active.flush_buf()?;
        let log_size = self.active.end_pos;
        let path = self.hint_path();
        write_hint_file(&path, &self.key_dir, self.active_id, log_size)?;
        self.unhinted_records = 0;
        Ok(())
    }

    pub(crate) fn checkpoint_hint(&mut self, policy: HintPolicy) -> Result<HintRefresh> {
        self.usable()?;
        // 一次发布至多扫描累计变更记录数的两倍；显式 checkpoint、bulk 结束、merge 和析构仍立即发布。
        if matches!(policy, HintPolicy::Amortized)
            && self.unhinted_records < (self.key_dir.len() as u64).div_ceil(2).max(1)
        {
            return Ok(HintRefresh::Deferred {
                unhinted_records: self.unhinted_records,
            });
        }
        self.dump_hint()?;
        Ok(HintRefresh::Written)
    }
}
