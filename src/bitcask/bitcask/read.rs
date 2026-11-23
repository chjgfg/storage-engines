//! 本文件负责点查、索引过滤与反向定位。

use super::*;

impl Bitcask {
    /// 从指定分片读 value
    pub(super) fn read_live(&mut self, file_id: u32, pos: u64, len: u32) -> Result<Vec<u8>> {
        if file_id == self.active_id {
            return self.active.read_value(pos, len);
        }
        if !self.sealed.contains_key(&file_id) {
            let p = segment_path(&self.base_path, file_id);
            let log = Log::new(p)?;
            self.sealed.insert(file_id, log);
        }
        let log = self.sealed.get_mut(&file_id).expect("sealed just inserted");
        log.read_value(pos, len)
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // 这一道守卫挡的是**静默的"键不存在"**：隔离期里 KeyDir 是空的，不拦就得给每个键
        // 回 `Ok(None)` —— 门面会把它当成"这版本没有值"，往上就是数据消失。
        self.usable()?;
        match self.key_dir.get(key).cloned() {
            Some(KeyLoc::Live { file_id, pos, len }) => {
                let val = self.read_live(file_id, pos, len)?;
                Ok(Some(val))
            }
            Some(KeyLoc::SoftTombstone { .. }) | None => Ok(None),
        }
    }

    /// `None` = 键不在索引；`Some(None)` = 软 tombstone；`Some(Some(v))` = 存活
    pub fn get_entry(&mut self, key: &[u8]) -> Result<Option<Option<Vec<u8>>>> {
        // `None` 在门面里是"这版本没这一项"，不是错误 ⇒ 隔离期必须拦，否则等于宣称全库空了
        self.usable()?;
        match self.key_dir.get(key).cloned() {
            None => Ok(None),
            Some(KeyLoc::SoftTombstone { .. }) => Ok(Some(None)),
            Some(KeyLoc::Live { file_id, pos, len }) => {
                let v = self.read_live(file_id, pos, len)?;
                Ok(Some(Some(v)))
            }
        }
    }
}
