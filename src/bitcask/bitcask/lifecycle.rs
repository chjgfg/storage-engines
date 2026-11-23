//! 本文件负责打开、配置、关闭及资源所有权。

use super::*;

impl Drop for Bitcask {
    fn drop(&mut self) {
        if let Err(e) = self.flush() {
            log::error!("failed to flush file: {e:?}");
        }
        // 进程退出时尽量写出 hint，加速下次 open（失败只记日志）
        if let Err(e) = self.dump_hint() {
            log::error!("dump_hint on drop failed: {e:?}");
        }
    }
}

impl Bitcask {
    /// 打开/创建数据文件并构建 KeyDir（默认分片上限 [`DEFAULT_MAX_FILE_BYTES`]）。
    pub fn new(path: PathBuf) -> Result<Self> {
        Self::with_max_file_bytes(path, DEFAULT_MAX_FILE_BYTES)
    }

    /// 指定单个 active 分片软上限（字节）；写满后 rotate。
    pub fn with_max_file_bytes(path: PathBuf, max_file_bytes: u64) -> Result<Self> {
        let base_path = path;
        // 先认回收崩在中间的 merge 产物，再谈别的：`discover_sealed_ids` 与后面的
        // `Log::new(data.log)` 都会**创建**缺失的文件，一旦让它们在恢复之前跑，
        // 「data.log 被删了、真数据还在 data.merge 里」这个现场就被永久抹平 ——
        // 表现是打开一个空库，谁都不报错。
        Self::recover_merge_artifact(&base_path)?;
        // ── M48 冷启动分段耗时标尺（门控 `OPEN_STAGE_SPLIT`；八张槽的名字都写在
        // `MVCC::try_open_with` 上面那张表里，这里占 1/2/3 三格）──
        // ⚠️ 只 mark、不 t0：基准由最外面那层放（门面那扇 open 才是被量的对象）。
        let sealed_ids = discover_sealed_ids(&base_path);
        let hint_path = hint_path_for(&base_path);
        #[cfg(test)]
        crate::mem_probe::open_stage_mark(1);

        // 尝试 hint 快路径
        let mut from_hint: Option<(KeyDir, u32, u64)> = None;
        if let Ok((kd, active_id, active_size)) = read_hint_file(&hint_path) {
            from_hint = Some((kd, active_id, active_size));
        }
        #[cfg(test)]
        crate::mem_probe::open_stage_mark(2);
        let (key_dir, active_id, active) = if let Some((mut kd, active_id, active_size)) = from_hint
        {
            let mut active = Log::new(base_path.clone())?;
            match Self::replay_hint(
                &base_path,
                &sealed_ids,
                &mut kd,
                active_id,
                active_size,
                &mut active,
            ) {
                Ok(HintReplay::ActiveTail) => (kd, active_id, active),
                Ok(HintReplay::Rotated { active_id, .. }) => (kd, active_id, active),
                Err(e) => {
                    // 提示文件损坏或历史缺失时必须全量重扫，不能只恢复一部分索引。
                    log::warn!("hint 增量扫失败: {e}，回退全量扫");
                    drop(active);
                    Self::full_rebuild(&base_path, &sealed_ids)?
                }
            }
        } else {
            Self::full_rebuild(&base_path, &sealed_ids)?
        };
        #[cfg(test)]
        crate::mem_probe::open_stage_mark(3);

        Ok(Self {
            base_path,
            active,
            active_id,
            unhinted_records: key_dir.len() as u64,
            key_dir,
            max_file_bytes,
            sealed: std::collections::HashMap::new(),
            index_rebuilding: false,
        })
    }

    pub fn path(&self) -> &std::path::Path {
        &self.base_path
    }

    pub fn active_id(&self) -> u32 {
        self.active_id
    }

    /// 当前生效的单分片软上限（门面读回用：证明设进去的值真到了引擎里）
    pub fn max_file_bytes(&self) -> u64 {
        self.max_file_bytes
    }

    /// 索引可用吗？—— 见 [`Bitcask::index_rebuilding`]：重扫窗口里**所有**依赖 KeyDir 的门
    /// 一律报错，不返回"键不存在"。
    pub(super) fn usable(&self) -> Result<()> {
        if self.index_rebuilding {
            return Err(std::io::Error::other(
                "bitcask: 内存索引正在重建（上一次 merge 在重扫处失败），请重新打开数据库",
            ));
        }
        Ok(())
    }

    /// 测试专用：把引擎推进"重扫窗口"那个现场 —— 与 `merge()` 里那两步**一字不差**：
    /// 索引腾掉 + 置标志。真机上只有 merge 的重扫失败才进得去。
    ///
    /// 必须连索引一起腾：只置标志、索引还满着，`get` 就会读出**对的**值，这条守卫测的
    /// 就不是那个现场了。窗口里真正的危险是"索引空了而磁盘上全都有"。
    #[cfg(test)]
    pub(crate) fn quarantine_index_for_test(&mut self) {
        self.key_dir = KeyDir::new();
        self.index_rebuilding = true;
    }

    #[cfg(test)]
    pub(crate) fn release_index_for_test(&mut self) {
        self.index_rebuilding = false;
    }
}
