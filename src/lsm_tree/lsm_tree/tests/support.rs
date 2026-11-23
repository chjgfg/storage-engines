// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// 返回 **RAII 句柄**（M82）：声明在引擎之前即可，用例结束（含 panic 展开）自动删目录。
fn tmp_dir(tag: &str) -> crate::common::ScratchPath {
    crate::common::ScratchPath::new("lsm", tag, "")
}

fn pruning_options() -> LSMTreeOptions {
    LSMTreeOptions {
        enable_mem_wal: false,
        enable_bloom: false,
        sst_block_entries: 2,
        block_cache_blocks: 0,
        max_compactions_per_flush: 0,
        ..LSMTreeOptions::default()
    }
}

fn version_key(version: u64) -> Vec<u8> {
    let mut key = b"p".to_vec();
    key.extend_from_slice(&version.to_be_bytes());
    key
}
