//! 本文件负责分层压实、直接迁层和重叠修复。
//! 模块边界仅组织代码：锁顺序、版本可见性、磁盘格式和发布失败的处理均沿用原契约。

use super::*;

impl LSMTree {
    pub(super) fn maybe_stall_on_l0(&mut self) {
        if self.bulk_mode {
            // bulk：只在极端 L0 堆积时做一次 compact，避免写路径被 leveled 拖死
            let l0 = self.levels.first().map(|l| l.len()).unwrap_or(0);
            if l0 >= self.opts.l0_stop_trigger {
                let _ = self.compact_level(0);
                let _ = self.write_manifest();
            }
            return;
        }
        let l0 = self.levels.first().map(|l| l.len()).unwrap_or(0);
        if l0 >= self.opts.l0_stop_trigger {
            // 硬停：同步 compact 直到 L0 降到 slowdown 以下
            let target = self.opts.l0_slowdown_trigger.max(1);
            let mut guard = 0;
            while self.levels.first().map(|l| l.len()).unwrap_or(0) >= target && guard < 64 {
                let _ = self.compact_level(0);
                guard += 1;
            }
            let _ = self.write_manifest();
            self.notify_bg();
        } else if l0 >= self.opts.l0_slowdown_trigger {
            // 软限：顺带 compact 一轮，并通知后台
            let _ = self.compact_level(0);
            self.notify_bg();
        }
    }

    pub(super) fn maybe_compact(&mut self) -> std::io::Result<()> {
        let mut rounds = 0;
        while rounds < self.opts.max_compactions_per_flush {
            if self.levels[0].len() >= self.opts.l0_compact_threshold {
                self.compact_level(0)?;
                rounds += 1;
                continue;
            }
            let mut did = false;
            for level in 1..self.levels.len().saturating_sub(1) {
                if self.level_bytes(level) > self.level_limit_bytes(level) {
                    self.compact_level(level)?;
                    did = true;
                    rounds += 1;
                    break;
                }
            }
            if !did {
                break;
            }
        }
        Ok(())
    }

    pub(super) fn level_bytes(&self, level: usize) -> u64 {
        self.levels
            .get(level)
            .map(|l| l.iter().map(|s| s.size_bytes).sum())
            .unwrap_or(0)
    }

    pub(super) fn level_limit_bytes(&self, level: usize) -> u64 {
        if level == 0 {
            return u64::MAX;
        }
        let mut lim = self.opts.level_base_bytes as u64;
        for _ in 1..level {
            lim = lim.saturating_mul(self.opts.level_size_multiplier as u64);
        }
        lim
    }
    /// 仅在不需要改写记录且目标层范围安全时直接迁移 SST。
    /// 资格不成立时显式回到重写压实；直接迁层与重写的字节成本分别统计。

    /// 从 level 压实到 level+1。已认证的单文件可不解码、不重写直接迁层；需要语义转换或元数据不确定时走普通归并。
    pub(super) fn try_trivial_move(
        &mut self,
        level: usize,
        next: usize,
    ) -> std::io::Result<MoveOutcome> {
        #[cfg(test)]
        if trivial_move_probe::reference() {
            return Ok(MoveOutcome::Rewrite);
        }
        if self.bulk_mode || self.compaction_filter.is_some() || self.levels[level].is_empty() {
            return Ok(MoveOutcome::Rewrite);
        }
        if level == 0 && self.levels[0].len() > 1 {
            return self.try_disjoint_l0_move(next);
        }
        let idx = if level == 0 {
            0
        } else {
            self.levels[level]
                .iter()
                .enumerate()
                .max_by_key(|(_, s)| s.size_bytes)
                .unwrap()
                .0
        };
        let file = &self.levels[level][idx];
        if file.move_eligibility() != crate::lsm_tree::sst::MoveEligibility::Ready {
            return Ok(MoveOutcome::Rewrite);
        }
        // 目标层已有重叠也必须修复，即使该重叠与本次选中的文件无关。
        if self.levels[level]
            .windows(2)
            .any(|pair| pair[0].max_key >= pair[1].min_key)
            || self.levels[next]
                .windows(2)
                .any(|pair| pair[0].max_key >= pair[1].min_key)
            || self.levels[next]
                .iter()
                .any(|s| ranges_overlap(&file.min_key, &file.max_key, &s.min_key, &s.max_key))
        {
            return Ok(MoveOutcome::Rewrite);
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        if self
            .ttl_meta
            .range(file.min_key.clone()..=file.max_key.clone())
            .any(|(_, exp)| now > *exp)
        {
            return Ok(MoveOutcome::Rewrite);
        }
        let id = file.id;
        let bytes = file.size_bytes;
        #[cfg(test)]
        let _probe = maintenance_probe::Timer::start(maintenance_probe::Kind::Compact);
        #[cfg(test)]
        maintenance_probe::inputs(1, bytes);
        let file = self.levels[level].remove(idx);
        let position = self.levels[next].partition_point(|s| s.min_key < file.min_key);
        self.levels[next].insert(position, file);
        // 新旧清单引用的是同一个持久文件，不可删除它。
        if let Err(error) = self.write_manifest() {
            let file = self.levels[next].remove(position);
            self.levels[level].insert(idx, file);
            self.refresh_level_ranges(level);
            self.refresh_level_ranges(next);
            return Err(error);
        }
        self.refresh_level_ranges(level);
        self.refresh_level_ranges(next);
        self.stats_dirty = true;
        #[cfg(test)]
        {
            maintenance_probe::moved(bytes);
            let text = fs::read_to_string(self.manifest_path())?;
            assert!(manifest_publishes(&text, next, &[id]));
        }
        #[cfg(not(test))]
        let _ = (id, bytes);
        self.notify_bg();
        Ok(MoveOutcome::Moved)
    }
    /// 把已证明互不重叠且满足保留条件的 L0 文件集中移入下一层。
    /// 清单必须先持久发布再清理旧状态；资格或发布失败不能部分暴露文件集合。

    /// 互不重叠的 L0 批次在一次清单编辑中发布。失败必须保留 L0 原插入顺序，认证前不能排序或清空来源。
    pub(super) fn try_disjoint_l0_move(&mut self, next: usize) -> std::io::Result<MoveOutcome> {
        #[cfg(test)]
        if disjoint_move_reference::enabled() {
            return Ok(MoveOutcome::Rewrite);
        }
        let mut source: Vec<_> = self.levels[0].iter().collect();
        source.sort_unstable_by(|a, b| a.min_key.cmp(&b.min_key));
        if source.windows(2).any(|p| p[0].max_key >= p[1].min_key)
            || self.levels[next]
                .windows(2)
                .any(|p| p[0].max_key >= p[1].min_key)
        {
            return Ok(MoveOutcome::Rewrite);
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let mut target = 0;
        for file in &source {
            if file.move_eligibility() != crate::lsm_tree::sst::MoveEligibility::Ready
                || self
                    .ttl_meta
                    .range(file.min_key.clone()..=file.max_key.clone())
                    .any(|(_, exp)| now > *exp)
            {
                return Ok(MoveOutcome::Rewrite);
            }
            while target < self.levels[next].len()
                && self.levels[next][target].max_key < file.min_key
            {
                target += 1;
            }
            if target < self.levels[next].len() && self.levels[next][target].min_key <= file.max_key
            {
                return Ok(MoveOutcome::Rewrite);
            }
        }
        // 只记录移走文件；过滤保留目标层原排序，来源下标恢复原有从旧到新的次序。
        let positions: HashMap<_, _> = self.levels[0]
            .iter()
            .enumerate()
            .map(|(position, file)| (file.id, position))
            .collect();
        #[cfg(test)]
        let bytes: u64 = source.iter().map(|file| file.size_bytes).sum();
        drop(source);
        #[cfg(test)]
        let _probe = maintenance_probe::Timer::start(maintenance_probe::Kind::Compact);
        let source = std::mem::take(&mut self.levels[0]);
        self.levels[next].extend(source);
        self.levels[next].sort_unstable_by(|a, b| a.min_key.cmp(&b.min_key));
        if let Err(error) = self.write_manifest() {
            let (mut source, target): (Vec<_>, Vec<_>) = std::mem::take(&mut self.levels[next])
                .into_iter()
                .partition(|file| positions.contains_key(&file.id));
            source.sort_unstable_by_key(|file| positions[&file.id]);
            self.levels[0] = source;
            self.levels[next] = target;
            self.refresh_level_ranges(0);
            self.refresh_level_ranges(next);
            return Err(error);
        }
        self.refresh_level_ranges(0);
        self.refresh_level_ranges(next);
        self.stats_dirty = true;
        #[cfg(test)]
        {
            maintenance_probe::inputs(positions.len() as u64, bytes);
            for file in &self.levels[next] {
                if positions.contains_key(&file.id) {
                    maintenance_probe::moved(file.size_bytes);
                }
            }
            let text = fs::read_to_string(self.manifest_path())?;
            assert!(manifest_publishes(
                &text,
                next,
                &positions.keys().copied().collect::<Vec<_>>()
            ));
        }
        self.notify_bg();
        Ok(MoveOutcome::Moved)
    }
    /// 选择源层与目标层的重叠范围，流式归并后发布新文件集合。
    /// 旧输入必须保留到新输出和清单发布成功；失败时恢复原来源，避免丢失唯一副本。

    pub(super) fn compact_level(&mut self, level: usize) -> std::io::Result<()> {
        let next = level + 1;
        if next >= self.levels.len() {
            return Ok(());
        }
        match self.try_trivial_move(level, next)? {
            MoveOutcome::Moved => return Ok(()),
            MoveOutcome::Rewrite => {}
        }

        let original: HashMap<u64, (usize, usize)> = self
            .levels
            .iter()
            .enumerate()
            .flat_map(|(level, files)| {
                files
                    .iter()
                    .enumerate()
                    .map(move |(position, file)| (file.id, (level, position)))
            })
            .collect();

        // `input_files`：**拥有**本次压实的输入文件，顺序 = 源序（新→旧）。
        // M33 起归并收的是迭代器，所以这些文件必须活到 `merge_to_ssts` 返回为止 ——
        // 旧写法在这里就 `iter_entries()` 把它们整篇读进 `Vec` 再丢掉文件，于是瞬时内存
        // ≈ 所有输入文件之和（实测 600k 行 61.7 MB 输入 ⇒ 物化那一步峰值 +154 MB）。
        let (input_files, paths) = if level == 0 {
            // 全部 L0 + 与 L0 范围重叠的 L1
            let l0 = std::mem::take(&mut self.levels[0]);
            let (min_k, max_k) = span_keys(&l0);
            let mut l1_keep = Vec::new();
            let mut l1_merge = Vec::new();
            for sst in std::mem::take(&mut self.levels[1]) {
                if ranges_overlap(&min_k, &max_k, &sst.min_key, &sst.max_key) {
                    l1_merge.push(sst);
                } else {
                    l1_keep.push(sst);
                }
            }
            let mut files: Vec<SstFile> = Vec::with_capacity(l0.len() + l1_merge.len());
            files.extend(l0.into_iter().rev()); // L0 那个 vec 是旧→新，反转成新→旧
            files.extend(l1_merge);
            let paths: Vec<_> = files.iter().map(|s| s.path.clone()).collect();
            self.levels[1] = l1_keep;
            (files, paths)
        } else {
            // 选最大的一个文件
            let cur = &mut self.levels[level];
            if cur.is_empty() {
                return Ok(());
            }
            let idx = cur
                .iter()
                .enumerate()
                .max_by_key(|(_, s)| s.size_bytes)
                .map(|(i, _)| i)
                .unwrap();
            let picked = cur.remove(idx);
            let min_k = picked.min_key.clone();
            let max_k = picked.max_key.clone();
            let mut next_keep = Vec::new();
            let mut next_merge = Vec::new();
            for sst in std::mem::take(&mut self.levels[next]) {
                if ranges_overlap(&min_k, &max_k, &sst.min_key, &sst.max_key) {
                    next_merge.push(sst);
                } else {
                    next_keep.push(sst);
                }
            }
            let paths: Vec<_> = std::iter::once(&picked)
                .chain(next_merge.iter())
                .map(|s| s.path.clone())
                .collect();
            // 源序 = picked 先、下一层重叠段在后（与旧写法同序）
            let mut files: Vec<SstFile> = Vec::with_capacity(1 + next_merge.len());
            files.push(picked);
            files.extend(next_merge);
            self.levels[next] = next_keep;
            (files, paths)
        };

        #[cfg(test)]
        let _probe = if input_files.is_empty() {
            None
        } else {
            maintenance_probe::inputs(
                input_files.len() as u64,
                input_files.iter().map(|f| f.size_bytes).sum(),
            );
            maintenance_probe::Timer::start(maintenance_probe::Kind::Compact)
        };

        // 输入来源已从层级集合移出；只记录其它来源的范围，成本随文件数而非值数增长。保守重叠判断保留删除屏障，直到可能的旧来源参加归并。
        let mut outside: Vec<(Vec<u8>, Vec<u8>)> = self
            .levels
            .iter()
            .flatten()
            .map(|s| (s.min_key.clone(), s.max_key.clone()))
            .collect();
        for mem in std::iter::once(&self.mem).chain(self.immutables.iter()) {
            if let (Some((lo, _)), Some((hi, _))) = (mem.first_key_value(), mem.last_key_value()) {
                outside.push((lo.clone(), hi.clone()));
            }
        }
        outside.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        let mut ranges: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for (lo, hi) in outside {
            if let Some(last) = ranges.last_mut() {
                if lo <= last.1 {
                    last.1 = std::mem::take(&mut last.1).max(hi);
                    continue;
                }
            }
            ranges.push((lo, hi));
        }
        let filter = self.compaction_filter.clone();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let ttl_map = self.ttl_meta.clone();
        let keep = |k: &[u8], se: &StoredEntry| -> bool {
            if matches!(se.value, EntryValue::Drop) {
                return false;
            }
            if let Some(&exp) = ttl_map.get(k) {
                if now > exp {
                    return false;
                }
            }
            if let Some(f) = &filter {
                if !f(k, se) {
                    return false;
                }
            }
            true
        };
        let action = |k: &[u8], se: &StoredEntry| {
            if keep(k, se) {
                return CompactionAction::Keep;
            }
            let end = ranges.partition_point(|(lo, _)| lo.as_slice() <= k);
            if end > 0 && k <= ranges[end - 1].1.as_slice() {
                CompactionAction::RetainDeletion
            } else {
                CompactionAction::Discard
            }
        };

        let block_entries = self.opts.sst_block_entries;
        let enable_bloom = self.opts.enable_bloom;
        let bloom_bits = self.opts.bloom_bits_per_key;
        let next_id = &self.next_sst_id;
        let dir = self.dir.clone();
        let cache = self.block_cache.clone();
        let use_mmap = self.opts.use_mmap;
        let target = self.opts.target_sst_bytes;

        // 每个压实输入拥有当前解码块，读取错误必须向上传播。
        #[cfg(test)]
        let merge_timer = maintenance_probe::Timer::start(maintenance_probe::Kind::Merge);
        let merged = merge_to_ssts_with_action(
            input_files
                .iter()
                .map(|s| {
                    Box::new(s.compaction_input()) as TryMergeInput<'_, StoredEntry, std::io::Error>
                })
                .collect(),
            &action,
            || {
                let id = next_id.fetch_add(1, Ordering::SeqCst);
                let path = dir.join("sst").join(format!("{id:08}.sst"));
                SstBuilder::with_options(id, path, block_entries, enable_bloom)
                    .with_bloom_bits(bloom_bits)
            },
            target,
        );
        let mut new_files = match merged {
            Ok(files) => files,
            Err(error) => {
                self.rollback_compaction_inputs(&original, input_files, Vec::new());
                return Err(error);
            }
        };
        #[cfg(test)]
        drop(merge_timer);
        let ns = Self::dir_cache_ns(&self.dir);
        for sst in &mut new_files {
            sst.set_cache_ns(ns);
            sst.set_block_cache(cache.clone());
            if use_mmap {
                let _ = sst.enable_mmap();
            }
        }

        let mut all_next = std::mem::take(&mut self.levels[next]);
        all_next.append(&mut new_files);
        all_next.sort_by(|a, b| a.min_key.cmp(&b.min_key));
        // L1+：层内重叠**真的折掉**（M26）。旧实现只 `eprintln!` 告警、把文件原样交回去 ⇒
        // 重叠一旦出现就永远没人修：常规压实只会拉"与所选文件重叠的**下一层**"，从不碰同层兄弟，
        // 而这一层的输出又只在被压实到时才重写。
        let mut replaced: Vec<SstFile> = Vec::new();
        if next >= 1 {
            #[cfg(test)]
            let _timer = maintenance_probe::Timer::start(maintenance_probe::Kind::Repair);
            if let Err(error) = self.repair_level_overlaps(&mut all_next, &mut replaced, &action) {
                self.levels[next] = all_next;
                self.rollback_compaction_inputs(&original, input_files, replaced);
                return Err(error);
            }
        }
        self.levels[next] = all_next;
        self.refresh_level_ranges(next);
        self.stats_dirty = true;
        self.reevaluate_cache_capacity();
        // ⚠️ 顺序：**先把新文件集记进 MANIFEST，再删旧文件**（M26）。
        // 旧写法是"删完就等调用方去写 manifest"，中间崩掉 ⇒ manifest 还指着已经删掉的文件、
        // 而新写出的文件没人记 —— `load_manifest` 对不在盘的条目是 `path.exists()` 静默跳过，
        // 于是重开之后那批数据**既不在 manifest 里也不在盘上**，一句话不报。
        // 反过来留的窗口是"新+旧同时在 manifest 里"（重叠），那正是上面 `repair_level_overlaps`
        // 负责收拾的形状，读答案两边同键同 seq ⇒ 折叠与点查都不受影响。
        if let Err(error) = self.write_manifest() {
            self.rollback_compaction_inputs(&original, input_files, replaced);
            return Err(error);
        }
        #[cfg(test)]
        {
            let _timer = maintenance_probe::Timer::start(maintenance_probe::Kind::Guard);
            let text = fs::read_to_string(self.manifest_path())?;
            let ids: Vec<_> = self.levels[next].iter().map(|sst| sst.id).collect();
            if manifest_reference::enabled() {
                assert!(manifest_publishes_reference(&text, &ids));
            }
            assert!(
                manifest_publishes(&text, next, &ids),
                "new SST set absent from target level {next} before deleting old files"
            );
        }
        {
            #[cfg(test)]
            let _timer = maintenance_probe::Timer::start(maintenance_probe::Kind::Cleanup);
            let replaced_paths: Vec<_> = replaced.iter().map(|file| file.path.clone()).collect();
            // Windows 上先关闭文件句柄及映射，再删除已发布输入。
            drop(input_files);
            drop(replaced);
            for p in paths.iter().chain(replaced_paths.iter()) {
                let _ = fs::remove_file(p);
            }
        }
        // 通知后台
        self.notify_bg();
        Ok(())
    }

    pub(super) fn rollback_compaction_inputs(
        &mut self,
        original: &HashMap<u64, (usize, usize)>,
        mut inputs: Vec<SstFile>,
        replaced: Vec<SstFile>,
    ) {
        inputs.extend(replaced);
        for files in &mut self.levels {
            inputs.append(files);
        }
        for file in inputs {
            if let Some(&(level, _)) = original.get(&file.id) {
                self.levels[level].push(file);
            } else {
                let path = file.path.clone();
                drop(file);
                let _ = fs::remove_file(path);
            }
        }
        for files in &mut self.levels {
            files.sort_by_key(|file| original[&file.id].1);
        }
        for level in 0..self.levels.len() {
            self.refresh_level_ranges(level);
        }
        self.stats_dirty = true;
        self.reevaluate_cache_capacity();
    }

    /// 把一层里范围连续重叠的文件折成不重叠的新文件。
    ///
    /// 分组：传进来的 `files` 已按 `min_key` 排序，扫一遍把"与当前段上界相交"的文件并进同一段，
    /// 并且是**传递闭包**（A∩B、B∩C 而 A∩C=∅ 也得整段一起折，否则留下新的重叠）。
    /// 长度 1 的段原样留着 —— 那是常态：正常压实不会产生重叠（L0 全量并入、同层输出按键有序）。
    ///
    /// 取胜规则直接用 `merge_to_ssts_with_action` → `merge_streaming`：**同键取 seq 最大者**，与跨源点查
    /// `get_entry_at` 同一个判据 ⇒ 修复前后的读答案逐键相同（这条有守卫对拍，不是说法）。
    /// 过滤与保留删除屏障沿用本次 compaction 的 action（TTL / Drop / 用户 filter），
    /// 与常规合并共享同一份外部来源判定。
    ///
    /// 内存：与常规压实共用 `compaction_input`，每源一块，读取失败停止发布。
    /// 旧写法在这里也 `iter_entries()` 把段里每个文件整篇读进 `Vec`，与压实共用那条账单 ——
    /// 实测 600k 行 / 8 个输入文件时那一步峰值涨 154 MB（条目本身 61.7 MB）。
    /// 尺子见本文件 `test_compaction_repairs_level_overlap` 里那条 `merge_inputs_materialized` 断言。
    ///
    /// 被替换的旧文件按所有权保留到 MANIFEST 发布；失败可恢复原层归属和源序。
    pub(super) fn repair_level_overlaps<F>(
        &mut self,
        files: &mut Vec<SstFile>,
        replaced: &mut Vec<SstFile>,
        keep: &F,
    ) -> std::io::Result<()>
    where
        F: Fn(&[u8], &StoredEntry) -> CompactionAction,
    {
        let taken = std::mem::take(files);
        let mut runs: Vec<Vec<SstFile>> = Vec::new();
        let mut cur: Vec<SstFile> = Vec::new();
        let mut cur_hi: Vec<u8> = Vec::new();
        for f in taken {
            if cur.is_empty() || f.min_key.as_slice() <= cur_hi.as_slice() {
                if f.max_key > cur_hi {
                    cur_hi = f.max_key.clone();
                }
                cur.push(f);
            } else {
                runs.push(std::mem::take(&mut cur));
                cur_hi = f.max_key.clone();
                cur.push(f);
            }
        }
        if !cur.is_empty() {
            runs.push(cur);
        }

        let block_entries = self.opts.sst_block_entries;
        let enable_bloom = self.opts.enable_bloom;
        let bloom_bits = self.opts.bloom_bits_per_key;
        let target = self.opts.target_sst_bytes;
        let dir = self.dir.clone();
        let cache = self.block_cache.clone();
        let use_mmap = self.opts.use_mmap;
        let ns = Self::dir_cache_ns(&self.dir);
        let next_id = &self.next_sst_id;

        enum RunRewrite {
            Keep,
            Rewritten(UnpublishedSsts),
        }
        let mut planned = Vec::new();
        for run in &runs {
            if run.len() <= 1 {
                planned.push(RunRewrite::Keep);
                continue;
            }
            // M33：修复重叠与常规压实**共用同一条流式源账单**。旧写法在这里也 `iter_entries()`
            // 整篇读进 Vec，而修复恰恰多在崩溃恢复之后跑 —— 那时正是内存最紧的时候。
            let result = merge_to_ssts_with_action(
                run.iter()
                    .map(|s| {
                        Box::new(s.compaction_input())
                            as TryMergeInput<'_, StoredEntry, std::io::Error>
                    })
                    .collect(),
                keep,
                || {
                    let id = next_id.fetch_add(1, Ordering::SeqCst);
                    let path = dir.join("sst").join(format!("{id:08}.sst"));
                    SstBuilder::with_options(id, path, block_entries, enable_bloom)
                        .with_bloom_bits(bloom_bits)
                },
                target,
            );
            let mut merged = match result {
                Ok(files) => files,
                Err(error) => {
                    *files = runs.into_iter().flatten().collect();
                    return Err(error);
                }
            };
            for sst in &mut merged {
                sst.set_cache_ns(ns);
                sst.set_block_cache(cache.clone());
                if use_mmap {
                    let _ = sst.enable_mmap();
                }
            }
            planned.push(RunRewrite::Rewritten(UnpublishedSsts(merged)));
        }
        let mut out = Vec::new();
        for (run, result) in runs.into_iter().zip(planned) {
            match result {
                RunRewrite::Keep => out.extend(run),
                RunRewrite::Rewritten(mut files) => {
                    replaced.extend(run);
                    out.append(&mut files.0);
                }
            }
        }
        out.sort_by(|a, b| a.min_key.cmp(&b.min_key));
        *files = out;
        Ok(())
    }

    /// 手动触发一轮 compact（测试 / 运维）
    pub fn compact_once(&mut self) -> std::io::Result<()> {
        self.maybe_compact()?;
        self.write_manifest()?;
        self.stats_dirty = true;
        self.refresh_stats_if_needed();
        Ok(())
    }

    /// 将 L0 文件数压到 ≤ `l0_target`，最多 `max_rounds` 轮 L0→L1 compact。
    ///
    /// - bulk `finish` / 大批 flush 后 L0 可能堆积；调用方按需收口
    /// - `max_rounds == 0` 时用 `max_compactions_per_flush * 8` 作为上限
    /// - 返回实际 compact 轮数
    pub fn compact_l0_until(
        &mut self,
        l0_target: usize,
        max_rounds: usize,
    ) -> std::io::Result<usize> {
        let limit = if max_rounds == 0 {
            self.opts.max_compactions_per_flush.max(1).saturating_mul(8)
        } else {
            max_rounds
        };
        let mut rounds = 0usize;
        while rounds < limit {
            let l0 = self.levels.first().map(|l| l.len()).unwrap_or(0);
            if l0 <= l0_target {
                break;
            }
            let before = l0;
            self.compact_level(0)?;
            rounds += 1;
            let after = self.levels.first().map(|l| l.len()).unwrap_or(0);
            if after >= before {
                // 无进展（例如已是单文件或更高层阻塞）
                break;
            }
        }
        if rounds > 0 {
            self.write_manifest()?;
            self.stats_dirty = true;
        }
        Ok(rounds)
    }
}

pub(super) fn span_keys(files: &[SstFile]) -> (Vec<u8>, Vec<u8>) {
    let mut min_k = files.first().map(|s| s.min_key.clone()).unwrap_or_default();
    let mut max_k = files.first().map(|s| s.max_key.clone()).unwrap_or_default();
    for s in files {
        if s.min_key < min_k {
            min_k = s.min_key.clone();
        }
        if s.max_key > max_k {
            max_k = s.max_key.clone();
        }
    }
    (min_k, max_k)
}

pub(super) fn ranges_overlap(a0: &[u8], a1: &[u8], b0: &[u8], b1: &[u8]) -> bool {
    a0 <= b1 && b0 <= a1
}
