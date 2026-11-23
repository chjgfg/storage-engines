//! 本文件负责多来源流式归并。

use super::*;

impl Bitcask {
    /// 压缩：把 KeyDir 中存活 + 软 tomb 重写到新 data.log（active_id=0），删除所有密封分片
    pub fn merge(&mut self) -> Result<()> {
        // M62：分段账的进门（门控 `MERGE_STAGE_SPLIT`，默认 no-op ⇒ 不动外层台架的水位）
        #[cfg(test)]
        merge_probe_start();
        // M81：同一句 merge 的**时间**账（另一个门控，不碰水位）
        #[cfg(test)]
        merge_t_start();
        self.active.flush_buf()?;

        let target = self.base_path.clone();
        let mut merge_path = target.clone();
        merge_path.set_extension(MERGE_FILE_EXT);
        // 第 3 项的尺子：旧段一删就量不到了，必须在动手之前抄盘上账
        #[cfg(test)]
        merge_seg_begin(&target, self.active_id, self.active.end_pos);

        // M40：**按块走 KeyDir**。这里原先是一次 `.collect()` 出整库的
        // `(key.clone(), loc.clone())` 快照 —— 600k 键 × 2 版那档分段实测 **115.0 MB**
        // （100 B/扫描行），是 `vacuum()` 当时唯一的大头。为什么要快照：循环里 `read_live`
        // 要 `&mut self`，不能一边借用 `key_dir` 一边读盘 ⇒ 只能先把范围抄出来。
        // 每块只抄 `MERGE_CHUNK_KEYS` 把键（借用一结束就处理这一块），那份重复的键表就从
        // O(全库) 变成 O(一块)。
        //
        // M43（B1）：抄完这一块之后**不再往第二把索引里记位置**。原先每个存活键都要往
        // `new_key_dir` 插一条，那份表本身就是 O(库) 且活到函数结束 ⇒ 走查期间磁盘上真正的
        // 峰值是"旧索引 + 新索引"两份同时在（600k 档分段 ④ 实测 **+87.9 MB**）。现在改成
        // 抄完 + fsync + 删旧 + rename 之后**原地重扫新日志**（下面 `load_index_from`），
        // 全程只有一份索引。
        // ⚠️ 代价不是"多扫一遍日志"（多扫那一遍本来就省不掉，索引必须来自日志），而是
        // **旧索引必须先腾掉**才能重扫 ⇒ 中间有一段窗口里 KeyDir 指向的文件已被删除/改名。
        // 那段窗口用 [`Bitcask::index_rebuilding`] 明说"现在不服务"，而不是让读路径把
        // "索引里没有"当成"键不存在"。
        let mut new_log = Log::new(merge_path)?;
        // 块间下界在键的字节域：`key ‖ 0x00` 是**严格后继方向**（`key‖0x00 > key`，且任何
        // `K > key` 都有 `K ≥ key‖0x00`）⇒ 一行不漏、一行不重。⚠️ 不能用 `‖ 0xFF×8` 那种
        // 右界写法（会把以 `key` 为前缀的更长键整把跳掉）—— 与 M37 那条 vacuum 块界同一个理由。
        let mut lower: Vec<u8> = Vec::new();
        loop {
            let mut chunk: Vec<(Vec<u8>, KeyLoc)> = Vec::with_capacity(MERGE_CHUNK_KEYS + 1);
            let mut next_lower: Option<Vec<u8>> = None;
            // M81 时间格 0：这一块的"走 KeyDir + 抄出来"
            {
                use std::ops::Bound::{Included, Unbounded};
                for (k, loc) in self
                    .key_dir
                    .range::<[u8], _>((Included(lower.as_slice()), Unbounded))
                {
                    // ⚠️ 探针本身是 `#[cfg(test)]` ⇒ 调用点也要跟着门控，否则普通 `cargo check --lib`
                    // （不开 test cfg）会找不到函数。上一轮只跑了 `cargo test`，这一格没暴露出来。
                    #[cfg(test)]
                    if let KeyLoc::Live { file_id, len, .. } = loc {
                        merge_seg_note_live(*file_id, *len);
                    }
                    chunk.push((k.clone(), loc.clone()));
                    if chunk.len() >= MERGE_CHUNK_KEYS {
                        let mut bound = k.clone();
                        bound.push(0u8);
                        next_lower = Some(bound);
                        break;
                    }
                }
            }
            // 格 0 收尾（水位一并推到这里 ⇒ 下一格不会重复计它）
            #[cfg(test)]
            merge_t_mark(0);
            #[cfg(test)]
            note_merge_chunk(chunk.len());
            // M81 时间格 1：这一块的 `read_live`（seek + read_exact）+ `append`
            for (key, loc) in chunk {
                match loc {
                    KeyLoc::Live { file_id, pos, len } => {
                        let value = self.read_live(file_id, pos, len)?;
                        // 位置不进任何索引：新日志里的 offset 由后面的重扫自己扫出来
                        let _ = new_log.append_entry(&key, Some(&value), false)?;
                    }
                    KeyLoc::SoftTombstone { .. } => {
                        let _ = new_log.write_soft_tombstone(&key)?;
                    }
                }
            }
            #[cfg(test)]
            merge_t_mark(1);
            match next_lower {
                Some(l) => lower = l,
                None => break,
            }
        }
        new_log.flush_buf()?;
        // M53 记账
        #[cfg(test)]
        crate::common::disk::note_fsync(crate::common::disk::SITE_BITCASK_LOG_FLUSH);
        new_log.file.sync_all()?;
        let new_path = new_log.path.clone();
        let new_end = new_log.end_pos;
        drop(new_log);

        // 关闭所有密封句柄
        self.sealed.clear();

        // 释放 active 锁
        let hold_path = target.with_extension("merge_hold");
        let old_active = std::mem::replace(
            &mut self.active,
            Log {
                path: PathBuf::new(),
                file: {
                    let f = std::fs::OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .open(&hold_path)?;
                    f
                },
                end_pos: 0,
                buf: Vec::new(),
                // M53：占位 ⇒ 账上没有未 fsync 的写入
                dirty: false,
            },
        );
        drop(old_active);

        // 删除密封分片
        //
        // ⚠️ 从这里到 `rename(new_path, &target)` 之间崩掉，磁盘上只剩 `data.merge`。
        // 这不是"运气好没碰上"而是必然要碰的：单核机器上 merge 是全库重写，窗口够长。
        // 兜底不在这里（顺序不能改：旧 active 句柄还挂着时 Windows 不让改名覆盖），
        // 而在 open 那边的 [`Bitcask::recover_merge_artifact`]。
        for id in discover_sealed_ids(&target) {
            let p = segment_path(&target, id);
            let _ = std::fs::remove_file(&p);
        }
        // 删除旧 active（若仍存在）
        if target.exists() {
            let _ = std::fs::remove_file(&target);
        }

        std::fs::rename(&new_path, &target)?;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&target)?;
        file.try_lock_exclusive()?;
        self.active = Log {
            path: target,
            file,
            end_pos: new_end,
            buf: Vec::new(), // 同 `Log::open`：首次写入才分配
            // M53：刚 fsync 过 ⇒ 账上没有未 fsync 的写入
            dirty: false,
        };
        self.active_id = 0;
        // M81 时间格 2：fsync + 关密封句柄 + 删密封段 + rename + 换 active（增量 merge 也要付这一笔）
        #[cfg(test)]
        merge_t_mark(2);
        // M43：索引不再"抄"出来，而是**重扫新日志**扫出来。三件事的顺序是死的：
        //
        // 1. 先删 hint —— hint 是旧索引的持久化副本，旧索引里那些 `pos` 说的是已被删掉/改名的
        //    文件。留着它，万一就崩在下面那趟重扫中间，下次 open 会拿它当真索引（长度检查、
        //    `hint_files_exist` 两道兜底都不是为这一支设计的，见 `dump_hint` 上那段注释）。
        //    hint 只是缓存，删掉的代价是下次 open 多扫一遍日志。
        // 2. 再腾掉旧索引 —— 这一步才把"两份索引同时在"变成"一份"，是这一刀的收益本身。
        // 3. 置标志 → 原地重扫 → 成功才放标志。重扫失败就带着 Err 返回并保持隔离：
        //    此时磁盘上的日志是完整的，只是内存里没人知道有哪些键 ⇒ 除了重新 open 没有第二条路。
        //
        // M60 改的只有"重扫怎么成树"这一件事：输入是刚写出的**按键升序**日志 ⇒ 走批量成树（满节点），
        // 免掉 M55 实测那笔 30.5 MiB 的半满税；上面那三步的顺序一字未动，失败仍然保持隔离。
        let _ = std::fs::remove_file(self.hint_path());
        self.key_dir = KeyDir::new();
        // M62 段 0：走到这里 = 新日志已写出、旧索引已腾掉 ⇒ 净增应当是**负的**（腾掉的那份）
        #[cfg(test)]
        merge_probe_mark(0);
        self.index_rebuilding = true;
        #[cfg(test)]
        arm_merge_rescan_counter();
        // ⚠️ 对照臂（门控 `MERGE_BULK_OLD_SHAPE`，`#[cfg(test)]` ⇒ 不随 crate 发布）：退回逐条 `insert`。
        //    同一个 merge、同一份日志、同一个解码器（`for_each_record`）⇒ 两臂只差"攒法"这一件事。
        #[cfg(test)]
        let rebuilt = if std::env::var("MERGE_BULK_OLD_SHAPE").is_ok() {
            let mut kd = KeyDir::new();
            self.active.load_index_from(&mut kd, 0, 0).map(|_| kd)
        } else {
            self.active.load_keydir_sorted(0)
        };
        #[cfg(not(test))]
        let rebuilt = self.active.load_keydir_sorted(0);
        #[cfg(test)]
        disarm_merge_rescan_counter();
        self.key_dir = match rebuilt {
            Ok(kd) => kd,
            Err(e) => {
                log::error!(
                    "bitcask: merge 后的索引重扫失败（{}），引擎隔离中：{}",
                    self.base_path.display(),
                    e
                );
                #[cfg(test)]
                merge_probe_stop();
                // 失败也要收账：不然时间格 3 停在半路、整行读数不出现，看起来像"没走进 merge"
                #[cfg(test)]
                merge_t_mark(3);
                #[cfg(test)]
                merge_t_dump(&format!("{}(重扫失败)", self.base_path.display()));
                return Err(e);
            }
        };
        // M62 段 2：树已建完并换上 ⇒ 这里的净增就是**终态索引**相对进门时旧索引的差
        #[cfg(test)]
        merge_probe_mark(2);
        // M81 时间格 3：删旧 hint + 腾掉旧索引 + 重扫新日志建索引（部分 merge 就地改 KeyLoc ⇒ 整格不付）
        #[cfg(test)]
        merge_t_mark(3);
        self.index_rebuilding = false;

        let _ = std::fs::remove_file(&hold_path);

        let _ = self.dump_hint();
        // M62 段 3：终态（hint 也落完）—— 净增应当与段 2 一样或更小（hint 的序列化缓冲已释放）
        #[cfg(test)]
        merge_probe_mark(3);
        #[cfg(test)]
        merge_probe_stop();
        // M81 时间格 4 + 收账
        #[cfg(test)]
        merge_t_mark(4);
        #[cfg(test)]
        merge_t_dump(&self.base_path.display().to_string());
        Ok(())
    }
}
