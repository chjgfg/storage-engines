//! 快照底层写出与 CRC 缓冲计量。

use super::*;

impl Write for SnapshotFile {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        #[cfg(test)]
        let start = self.measured.then(std::time::Instant::now);
        let n = self.file.write(bytes)?;
        #[cfg(test)]
        if let Some(start) = start {
            self.stats.write_ns += start.elapsed().as_nanos();
            self.stats.write_calls += 1;
        }
        #[cfg(test)]
        if self.legacy_fields {
            return Ok(n);
        }
        if let Some(crc) = &mut self.checksum {
            #[cfg(test)]
            let start = self.measured.then(std::time::Instant::now);
            *crc = crate::common::disk::crc32_extend(*crc, &bytes[..n]);
            #[cfg(test)]
            if let Some(start) = start {
                self.stats.crc_ns += start.elapsed().as_nanos();
                self.stats.crc_calls += 1;
            }
        }
        Ok(n)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

pub(super) fn snapshot_buffer_size(snapshot: bool) -> usize {
    #[cfg(test)]
    if io_reference::enabled() {
        return 8 * 1024;
    }
    if snapshot { 256 * 1024 } else { 8 * 1024 }
}
