//! 本文件负责日志段轮转和封存。

use super::*;

impl Bitcask {
    /// active 写满后密封：`data.log` → `data.{active_id}.log`，再开新 active
    pub(super) fn maybe_rotate(&mut self) -> Result<()> {
        self.unhinted_records = self.unhinted_records.saturating_add(1);
        if self.active.end_pos < self.max_file_bytes {
            return Ok(());
        }
        self.rotate()
    }

    pub(super) fn rotate(&mut self) -> Result<()> {
        self.active.flush_buf()?;
        // M53 记账
        #[cfg(test)]
        crate::common::disk::note_fsync(crate::common::disk::SITE_BITCASK_LOG_FLUSH);
        self.active.file.sync_all()?;
        self.active.dirty = false;

        let sealed_path = segment_path(&self.base_path, self.active_id);
        // Windows：必须先 drop 句柄再 rename
        let old = std::mem::replace(
            &mut self.active,
            // 临时占位，马上会被新 Log 替换
            Log {
                path: PathBuf::new(),
                file: {
                    // 用一个临时 hold 文件占位（避免 Option）
                    let hold = self.base_path.with_extension("rotate_hold");
                    std::fs::OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .open(&hold)?
                },
                end_pos: 0,
                buf: Vec::new(),
                // M53：占位 ⇒ 账上没有未 fsync 的写入
                dirty: false,
            },
        );
        let old_path = old.path.clone();
        drop(old);

        if sealed_path.exists() {
            let _ = std::fs::remove_file(&sealed_path);
        }
        std::fs::rename(&old_path, &sealed_path)?;

        // 新 active
        self.active = Log::new(self.base_path.clone())?;
        self.active_id = self.active_id.saturating_add(1);

        let hold = self.base_path.with_extension("rotate_hold");
        let _ = std::fs::remove_file(&hold);
        Ok(())
    }
}
