//! 版本键编解码 + vacuum 的「留哪一版」判定 —— 四引擎共用的一份实现。
//!
//! 这四份复制是本次事故的结构性根因：同一条判定写了四遍，于是一个 bug 有四个实例、
//! 一次修复要做四遍（而且真的会漏：内存引擎那份 `VersionedKey` 连 serde derive 都和其它
//! 三家不一样）。编解码和保留规则都是**纯逻辑**，不涉及存储结构，所以先抽出来；
//! 各引擎仍然保留自己的遍历原语和自己的守卫测试 —— 合并的是判定，不是扫描。

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// 逻辑键 + 版本（调试用）
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VersionedKey {
    pub raw_key: Vec<u8>,
    pub version: u64,
}

/// 编码：`raw_key || version.to_be_bytes()`
///
/// 大端 ⇒ 同一把 key 的版本按字节序升序排列，**最新一版在链尾**。点读点写因此可以从
/// `encode_key(key, u64::MAX)` 反向走一步就拿到最新版本，不必扫完整条链。
pub fn encode_key(raw_key: &[u8], version: u64) -> Vec<u8> {
    let mut enc = Vec::with_capacity(raw_key.len() + 8);
    enc.extend_from_slice(raw_key);
    enc.extend_from_slice(&version.to_be_bytes());
    enc
}

/// 解码
pub fn decode_key(enc: &[u8]) -> Option<VersionedKey> {
    if enc.len() < 8 {
        return None;
    }
    let split = enc.len() - 8;
    let mut ver_bytes = [0u8; 8];
    ver_bytes.copy_from_slice(&enc[split..]);
    Some(VersionedKey {
        raw_key: enc[..split].to_vec(),
        version: u64::from_be_bytes(ver_bytes),
    })
}

/// vacuum 的保留判定：给定「逻辑 key → 它的全部版本行」，算出该删哪些行、该留哪些行。
///
/// 规则（四引擎必须一致，否则换引擎等于换语义）：
/// - 每个 key 只保留「**最后一个** `version < xmin`」+ 全部 `version >= xmin`
/// - 若 xmin 之前最终那一版是墓碑、且没有更新的版本 → 整 key 删除（墓碑的终局回收）
///
/// 为什么必须留下那个 `last_old`：任何 `version >= xmin` 的事务读这把 key 时可能正好落在
/// 这一版上 —— 它是「最老仍可能被看到的版本」。⚠️ 反过来，**`xmin` 算错才是事故**：
/// 裁掉活跃事务快照仍需要的版本是静默数据错误，所以水位线的来源（有活跃事务就整轮跳过）
/// 由各引擎自己保证，这里只负责「给定 xmin 之后怎么裁」。
///
/// 泛型参数：`V` 是行标识（各引擎自己的 storage key），`R` 是引擎顺手带上的附加信息
/// （B+ 树用它标记保留行是否引用 blob；不需要就传 `()`）。
pub fn vacuum_plan<V, R>(
    by_key: BTreeMap<Vec<u8>, Vec<(u64, V, bool, R)>>,
    xmin: u64,
) -> (Vec<V>, Vec<(V, R)>)
where
    V: Clone,
    R: Clone,
{
    let mut to_delete: Vec<V> = Vec::new();
    let mut keep: Vec<(V, R)> = Vec::new();

    for (_raw, mut versions) in by_key {
        versions.sort_by_key(|(v, _, _, _)| *v);
        // 最后一个 version < xmin 的下标：它之前（更旧）的全删，它本身按墓碑规则定去留
        let last_old = versions.iter().rposition(|(ver, _, _, _)| *ver < xmin);
        match last_old {
            // 整条链都在 xmin 之上：全部保留
            None => keep.extend(versions.into_iter().map(|(_, row, _, extra)| (row, extra))),
            Some(keep_old) => {
                for i in 0..keep_old {
                    to_delete.push(versions[i].1.clone());
                }
                let old_is_tomb = versions[keep_old].2;
                let has_newer = versions.iter().any(|(v, _, _, _)| *v >= xmin);
                if old_is_tomb && !has_newer {
                    // 墓碑的终局回收：xmin 之前最终是删除且没有更新的版本 ⇒ 这一版也删掉，
                    // 否则墓碑会永久累积。注意它必须**显式进 to_delete** —— 只是"不 keep"
                    // 会让这行既不删也不留，静默躺在库里（抽出这份实现时就是这么错过的）。
                    to_delete.push(versions[keep_old].1.clone());
                } else {
                    keep.push((versions[keep_old].1.clone(), versions[keep_old].3.clone()));
                }
                for i in (keep_old + 1)..versions.len() {
                    keep.push((versions[i].1.clone(), versions[i].3.clone()));
                }
            }
        }
    }

    (to_delete, keep)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 大端编码 ⇒ 版本递增即字节序递增，链尾就是最新版
    #[test]
    fn version_key_orders_by_version_big_endian() {
        let (k1, k2, k10) = (encode_key(b"user", 1), encode_key(b"user", 2), encode_key(b"user", 10));
        assert!(k1 < k2, "小端会把 10 排在 2 之前，反向早停就不成立");
        assert!(k2 < k10);
        let d = decode_key(&k10).unwrap();
        assert_eq!(d.raw_key, b"user");
        assert_eq!(d.version, 10);
        assert!(decode_key(b"short").is_none());
    }

    /// 把「四份复制必须给出同一答案」这件事钉在唯一的一份实现上：
    /// 这条用例以前要在四个引擎文件里各写一遍，而复制本身就是漂移的来源。
    #[test]
    fn plan_keeps_last_old_and_drops_older() {
        // hot: v1..v5，xmin=6 ⇒ 只留 v5
        let mut by_key: BTreeMap<Vec<u8>, Vec<(u64, &str, bool, ())>> = BTreeMap::new();
        by_key.insert(
            b"hot".to_vec(),
            (1..=5u64).map(|v| (v, "hot", false, ())).collect(),
        );
        let (delete, keep) = vacuum_plan(by_key, 6);
        assert_eq!(delete.len(), 4);
        assert_eq!(keep.len(), 1, "每把 key 只留最老仍可能被看到的那一版");

        // 行标识各不相同 ⇒ 能断言「留的确实是最新那一版」，而不是只数行数
        const ROWS: [&str; 5] = ["v1", "v2", "v3", "v4", "v5"];
        let mut named: BTreeMap<Vec<u8>, Vec<(u64, &str, bool, ())>> = BTreeMap::new();
        named.insert(
            b"hot".to_vec(),
            (1..=5u64).map(|v| (v, ROWS[(v - 1) as usize], false, ())).collect(),
        );
        let (delete, keep) = vacuum_plan(named, 6);
        assert_eq!(delete, vec!["v1", "v2", "v3", "v4"]);
        assert_eq!(keep.iter().map(|(row, _)| *row).collect::<Vec<_>>(), vec!["v5"]);
    }

    /// 墓碑的终局回收：xmin 之前最终是墓碑、且没有更新版本 ⇒ 整 key 消失。
    ///
    /// 断言写的是**具体哪几行**而不是行数：抽这份实现的初稿把这条规则写成了「不 keep 就完事」，
    /// 于是那行墓碑既不删也不留，静默躺在库里 —— 只核行数会当场漏过去（v1 被删、行数仍是错的）。
    #[test]
    fn plan_reaps_final_tombstone_but_keeps_newer_versions() {
        let mut tomb_only: BTreeMap<Vec<u8>, Vec<(u64, &str, bool, ())>> = BTreeMap::new();
        tomb_only.insert(b"gone".to_vec(), vec![(1, "v1", false, ()), (2, "v2", true, ())]);
        let (delete, keep) = vacuum_plan(tomb_only, 5);
        assert_eq!(delete, vec!["v1", "v2"], "墓碑是最终状态 ⇒ 连同旧值一起收掉");
        assert!(keep.is_empty(), "整 key 都不该留行");

        let mut resurrected: BTreeMap<Vec<u8>, Vec<(u64, &str, bool, ())>> = BTreeMap::new();
        resurrected.insert(
            b"back".to_vec(),
            vec![(1, "v1", false, ()), (2, "v2", true, ()), (4, "v4", false, ())],
        );
        // xmin=3 ⇒ 「最后一个 < xmin」是 v2 那版墓碑，而它之上还有 v4：墓碑必须留着，
        // 因为 version ≥ xmin 的快照可能正好落在它上面（这条留错了就是静默读到旧值）
        let (delete, keep) = vacuum_plan(resurrected, 3);
        assert_eq!(delete, vec!["v1"]);
        assert_eq!(keep.iter().map(|(row, _)| *row).collect::<Vec<_>>(), vec!["v2", "v4"]);

        // 同一组数据把 xmin 推到 5：此时「最后一个 < xmin」变成活着的 v4，
        // 那版墓碑只是又老了一版，按普通旧版本被收掉
        let mut resurrected2: BTreeMap<Vec<u8>, Vec<(u64, &str, bool, ())>> = BTreeMap::new();
        resurrected2.insert(
            b"back".to_vec(),
            vec![(1, "v1", false, ()), (2, "v2", true, ()), (4, "v4", false, ())],
        );
        let (delete, keep) = vacuum_plan(resurrected2, 5);
        assert_eq!(delete, vec!["v1", "v2"]);
        assert_eq!(keep.iter().map(|(row, _)| *row).collect::<Vec<_>>(), vec!["v4"]);
    }

    /// 链全在 xmin 之上（还有活跃事务可能看到）⇒ 一行都不能动。
    #[test]
    fn plan_keeps_everything_above_xmin() {
        let mut by_key: BTreeMap<Vec<u8>, Vec<(u64, &str, bool, ())>> = BTreeMap::new();
        by_key.insert(b"new".to_vec(), vec![(7, "new", false, ()), (8, "new", false, ())]);
        let (delete, keep) = vacuum_plan(by_key, 5);
        assert!(delete.is_empty());
        assert_eq!(keep.len(), 2);
    }
}
