//! 本文件负责普通及批量存储写入。

use super::*;

impl Bitcask {
    // ── 经典 KV API ──────────────────────────────────────────────────────────

    pub fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.usable()?;
        let fid = self.active_id;
        let (value_pos, value_len, _) = self.active.append_entry(key, Some(&value), false)?;
        self.key_dir.insert(
            key.to_vec(),
            KeyLoc::Live {
                file_id: fid,
                pos: value_pos,
                len: value_len,
            },
        );
        self.maybe_rotate()?;
        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.usable()?;
        self.active.write_hard_delete(key)?;
        self.key_dir.remove(key);
        self.maybe_rotate()?;
        Ok(())
    }

    /// flush 写缓冲 + fsync 数据文件
    pub fn flush(&mut self) -> Result<()> {
        self.active.flush_buf()?;
        // M53：**账上干净就不付 fsync**（改前每次 open 末尾那一句 `tree.flush()` 都要白付一次——
        // 干净重开一个没人写过的库，文件里没有任何未落盘的字节）。
        // ⚠️ 反向的那半不能省：写过就必须 fsync，否则这条"优化"会把持久化点一起省掉
        // （M8 的 `Wal::append` 是同一课）。守卫见 `test_flush_skips_fsync_only_when_nothing_was_written`。
        if !self.active.dirty {
            return Ok(());
        }
        // M53 记账
        #[cfg(test)]
        crate::common::disk::note_fsync(crate::common::disk::SITE_BITCASK_LOG_FLUSH);
        self.active.file.sync_all()?;
        self.active.dirty = false;
        Ok(())
    }

    /// 只 flush 写缓冲（不 fsync）— bulk 中途降低峰值缓冲
    pub fn flush_buf(&mut self) -> Result<()> {
        self.active.flush_buf()
    }

    // ── MVCC 后端 API ────────────────────────────────────────────────────────

    /// 写入版本键。`value=None` → 软 tombstone（KeyDir 保留）。
    pub fn insert(&mut self, key: Vec<u8>, value: Option<Vec<u8>>) -> Result<()> {
        // 隔离期不许落盘：索引是空的、也不会再重扫，写进去的行没人记账 ⇒ 回 `Ok` 就是骗人
        self.usable()?;
        let fid = self.active_id;
        match value {
            Some(v) => {
                let (value_pos, value_len, _) = self.active.append_entry(&key, Some(&v), false)?;
                self.key_dir.insert(
                    key,
                    KeyLoc::Live {
                        file_id: fid,
                        pos: value_pos,
                        len: value_len,
                    },
                );
            }
            None => {
                self.active.write_soft_tombstone(&key)?;
                self.key_dir
                    .insert(key, KeyLoc::SoftTombstone { file_id: fid });
            }
        }
        self.maybe_rotate()?;
        Ok(())
    }

    /// 批量写入（一次缓冲，适合 bulk load）。消费 items，避免 clone。
    pub fn insert_batch(
        &mut self,
        items: impl IntoIterator<Item = (Vec<u8>, Option<Vec<u8>>)>,
    ) -> Result<()> {
        self.usable()?;
        for (key, value) in items {
            let fid = self.active_id;
            match value {
                Some(v) => {
                    let (value_pos, value_len, _) =
                        self.active.append_entry(&key, Some(&v), false)?;
                    self.key_dir.insert(
                        key,
                        KeyLoc::Live {
                            file_id: fid,
                            pos: value_pos,
                            len: value_len,
                        },
                    );
                }
                None => {
                    self.active.write_soft_tombstone(&key)?;
                    self.key_dir
                        .insert(key, KeyLoc::SoftTombstone { file_id: fid });
                }
            }
            self.maybe_rotate()?;
        }
        Ok(())
    }

    /// 硬删除版本键（rollback / vacuum / WAL UNDO）
    pub fn remove(&mut self, key: &[u8]) -> Result<()> {
        self.usable()?;
        self.active.write_hard_delete(key)?;
        self.key_dir.remove(key);
        self.maybe_rotate()?;
        Ok(())
    }
}
