// 该文件集中验证对应职责，保留原测试名称和字面值。
// 按职责划分的原测试代码；测试名称和字面值保持不变。

/// crc32 的已知值：`"123456789"` 的 IEEE CRC32 是 `0xCBF43926`。
/// 三份复制实现合成一份之后，这个交叉校验只需要写一次。
#[test]
fn crc32_matches_known_vector() {
    assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    assert_eq!(crc32(b""), 0);
    assert_eq!(crc32(b"a"), crc32(b"a"));
    assert_ne!(crc32(b"a"), crc32(b"b"));
}

/// slice-by-8 必须与逐字节参照实现**逐位相同**（M31）。
///
/// 校验和算错不是"慢一点"，是**每一页、每一个 SST 块、每一条 WAL 帧都判成坏数据** ——
/// 静默数据损坏级别的后果，所以对拍要覆盖每一种长度形状：8 的整数倍、不整除的尾巴、
/// 整页 4096、一页零头 4097；内容也不能只有一种（全零/递增看不到某些进位错）。
#[test]
fn crc32_slice_by_8_matches_bytewise_everywhere() {
    assert_eq!(crc32(b"123456789"), 0xCBF4_3926, "新实现也得过那条已知向量");
    let mut buf: Vec<u8> = (0..=255u8).cycle().take(90_000).collect();
    for n in 0..=41usize {
        assert_eq!(
            crc32(&buf[..n]),
            crc32_bytewise(&buf[..n]),
            "长度 {n} 不一致"
        );
    }
    for n in [
        63usize, 64, 65, 511, 512, 513, 4095, 4096, 4097, 8192, 70_000,
    ] {
        assert_eq!(
            crc32(&buf[..n]),
            crc32_bytewise(&buf[..n]),
            "长度 {n} 不一致"
        );
    }
    // 换一份伪随机内容再对一遍
    let mut s = 0x2545_F491_4F6C_DD1Du64;
    for v in buf.iter_mut() {
        s = s
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        *v = (s >> 56) as u8;
    }
    for n in [1usize, 7, 8, 9, 1000, 4096, 8191] {
        assert_eq!(
            crc32(&buf[..n]),
            crc32_bytewise(&buf[..n]),
            "随机内容长度 {n} 不一致"
        );
    }
    // 判别力：翻一个字节必须变（否则"两个实现一致但都算错"这种共同错误抓不住）
    let before = crc32(&buf[..4096]);
    buf[2000] ^= 0xFF;
    assert_ne!(before, crc32(&buf[..4096]), "单字节翻转必须改变校验和");
}
