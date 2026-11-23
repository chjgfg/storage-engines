//! 本文件负责大值旁文件读写及回收。

use super::*;

impl LSMTree {
    /// 大 value 写入 `blobs/{seq}.blob`，返回指针 value：`\x02` + seq_u64_le
    pub(super) fn write_blob(&self, data: &[u8]) -> std::io::Result<Vec<u8>> {
        let blob_dir = self.dir.join("blobs");
        fs::create_dir_all(&blob_dir)?;
        let seq = self.write_seq.load(Ordering::SeqCst);
        let path = blob_dir.join(format!("{seq:016}.blob"));
        fs::write(&path, data)?;
        let mut ptr = Vec::with_capacity(9);
        ptr.push(0x02); // BLOB pointer tag
        ptr.extend_from_slice(&seq.to_le_bytes());
        Ok(ptr)
    }

    /// 解析 blob 指针；非指针返回 None
    pub fn resolve_blob(value: &[u8], dir: &Path) -> Option<Vec<u8>> {
        if value.len() == 9 && value[0] == 0x02 {
            let seq = u64::from_le_bytes(value[1..9].try_into().ok()?);
            let path = dir.join("blobs").join(format!("{seq:016}.blob"));
            return fs::read(path).ok();
        }
        None
    }

    /// blob 旁路的空间回收：删掉**没有任何存活行引用**的 `blobs/*.blob`，返回删掉的个数。
    ///
    /// `keep` 是存活行的键（vacuum 裁完之后的那一集）。为什么只能由 vacuum 来做、compact
    /// 做不了：compact 只看得到它正在合并的那几个文件，没法判断"这个键在别的层里还有一份
    /// 更新的版本正引用着同一个 blob"。
    ///
    /// 判据只认指针字节（`\x02 + seq_u64_le`，见 `Self::write_blob`），不解析 value。
    /// 保守方向是「宁可留着也不删错」：删掉一个仍被引用的 blob 是**丢数据**，
    /// 留着一个没人引用的 blob 只是占空间。
    pub fn gc_blobs(&self, keep: &[Vec<u8>]) -> std::io::Result<usize> {
        if self.blob_gc_plan(true)? != BlobGcPlan::ScanReferences {
            return Ok(0);
        }
        let mut referenced: HashSet<u64> = HashSet::new();
        self.collect_blob_refs(keep, &mut referenced);
        self.gc_blobs_referenced(&referenced)
    }
    /// 先判断本轮是否允许回收 blob，以及目录中是否确有旁文件。
    /// 有界切片保留全库引用检查边界；目录错误必须报错，不能误当成没有文件。

    /// 每次全库回收都重新检查目录；以前为空不能证明之后没有写入 blob。调用方全程持有树锁。
    pub(crate) fn blob_gc_plan(&self, full_sweep: bool) -> std::io::Result<BlobGcPlan> {
        if !full_sweep {
            return Ok(BlobGcPlan::SkipBounded);
        }
        let plan = match fs::read_dir(self.dir.join("blobs")) {
            Ok(mut entries) => match entries.next().transpose()? {
                None => BlobGcPlan::NoFiles,
                Some(_) => BlobGcPlan::ScanReferences,
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => BlobGcPlan::NoFiles,
            Err(e) => return Err(e),
        };
        #[cfg(test)]
        if blob_reference::enabled() {
            return Ok(BlobGcPlan::ScanReferences);
        }
        Ok(plan)
    }

    /// [`Self::gc_blobs`] 的前半段：把这批保留行引用的 blob `seq` 并进 `into`。
    ///
    /// 单独露出来是给 **M37 的按块走查**用的：调用方每块把保留行喂进来就丢弃，于是
    /// "整张存活行清单"（`keep: &[Vec<u8>]`，600k 档那笔 O(全库) 的账）不需要存在；
    /// 留下的 `referenced` 只数**真有大 value 的行**（一个 `u64` 一条）。
    /// 判据与 `gc_blobs` 同一个口径（这份就是它原来的那段循环，没第二条实现）。
    pub(crate) fn collect_blob_refs(&self, keep: &[Vec<u8>], into: &mut HashSet<u64>) {
        #[cfg(test)]
        let _timer = crate::common::maintenance_probe::Timer::start(
            crate::common::maintenance_probe::Stage::VacuumBlobRefs,
        );
        for enc in keep {
            #[cfg(test)]
            crate::common::maintenance_probe::note_blob_read();
            // 外层 None = 这行已经不在；内层 None = Drop 墓碑（墓碑不引用任何 blob）
            // ⚠️ 这里刻意用**不解析**的 get_raw_key_at：GC 靠指针字节认引用，解析就认不出来了
            let Some(Some(val)) = self.get_raw_key_at(enc, u64::MAX) else {
                continue;
            };
            if let Some(seq) = Self::blob_ptr_seq(&val) {
                into.insert(seq);
            }
        }
    }
    /// 按已收集的存活引用清理大值旁文件。
    /// 引用检查和删除屏障必须先完成，避免删除仍被可见记录引用的大值。

    /// [`Self::gc_blobs`] 的后半段：拿着"还被引用的 seq 集合"去删没人引用的 blob 文件。
    /// 保守方向不变：宁可留着也不删错（删掉被引用的是**丢数据**）。
    pub(crate) fn gc_blobs_referenced(&self, referenced: &HashSet<u64>) -> std::io::Result<usize> {
        #[cfg(test)]
        let _timer = crate::common::maintenance_probe::Timer::start(
            crate::common::maintenance_probe::Stage::VacuumBlobGc,
        );
        let blob_dir = self.dir.join("blobs");
        let rd = match fs::read_dir(&blob_dir) {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(e),
        };
        let mut removed = 0usize;
        for ent in rd {
            let ent = ent?;
            let name = ent.file_name();
            let name = name.to_string_lossy();
            // 文件名是 `{seq:016}.blob`：十进制零填充
            let Some(stem) = name.strip_suffix(".blob") else {
                continue;
            };
            let Ok(seq) = stem.parse::<u64>() else {
                continue;
            };
            if referenced.contains(&seq) {
                continue;
            }
            match fs::remove_file(ent.path()) {
                Ok(()) => removed += 1,
                Err(e) => log::warn!("blob 回收跳过 {}：{e}", ent.path().display()),
            }
        }
        Ok(removed)
    }

    /// 按字节解引用；不是指针就原样返回（GC 用，见 [`Self::deref_value`] 的说明）
    pub(crate) fn blob_ptr_seq(value: &[u8]) -> Option<u64> {
        if value.len() == 9 && value[0] == 0x02 {
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&value[1..9]);
            return Some(u64::from_le_bytes(bytes));
        }
        None
    }
}
