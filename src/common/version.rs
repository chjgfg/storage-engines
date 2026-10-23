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

/// 版本键域上的** exclusive 上界哨兵**：给定逻辑上界 `end`，返回一个字节串，
/// 保证「任何 `raw < end` 的键的版本键」都严格小于它。
///
/// 用途：把 `[start, end)` 的界下推给按版本键有序的那张表（BTreeMap range / 日志 seek），
/// 让窗口扫只走窗口内的键。
///
/// 为什么尾部是 8 个 `0xFF` 而不是 `encode_key(end, 0)`：`end` 以 `0x00` 结尾时
/// （`seek_prev` 正是这样造上界的：`key ‖ 0x00`），`encode_key(end, 0)` 会排在
/// `raw ‖ version` **前面** —— 于是"raw 明明小于 end"的那一行被判成越界**丢掉**，
/// 表现为 `seek_prev(k)` 返回比 k 小的键。实测踩到过一次，所以这个函数只有一份实现，
/// 四个引擎都用它（`test_upper_bound_keeps_trailing_zero_end` 就是那次事故）。
///
/// 这个选择的账单，两条都要知道：
/// - **可能多走一步**：以 `end` 为前缀的键（raw > end）其版本键仍可能小于哨兵，于是被
///   迭代器带进来，再由调用方的 `raw ≥ end ⇒ continue` 滤掉。所以代价是
///   「窗口宽度 + 至多一个同前缀组」而不是精确的窗口宽度 —— 宁可多走，不可误删。
/// - **前提是 `version < 2^56`**（版本键首字节为 0）。事务号是 u64 计数器，现实中到不了；
///   `u64::MAX` 只该用作 seek 的上界哨兵，不是真实版本号。
pub fn version_key_upper_bound(end: &[u8]) -> Vec<u8> {
    let mut b = Vec::with_capacity(end.len() + 8);
    b.extend_from_slice(end);
    b.extend_from_slice(&[0xffu8; 8]);
    b
}

// 测试专用（M36 的尺子）：vacuum 的判定**逐组发生了几次**、其中**最大的一组有几行**。
//
// 为什么用这两把而不是分配器计数：`mem_probe` 的 peak 是**进程级**的，并行跑用例时会被
// 别家分配的字节污染（所以 M27/M33 的内存读数一直放在 `--test-threads=1` 的 `#[ignore]`
// 台架里）。而"判定是逐组发生的、每组不超过一条链"这件事是**结构**事实，用 thread_local
// 计数就能钉住：旧形状（先把全库归组进一个 `BTreeMap` 再问一次）在这里读出来是
// `calls = 1`、`max_rows = 全库物理行` ⇒ 两条断言同时红。
#[cfg(test)]
thread_local! {
    static VACUUM_GROUP_CALLS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static VACUUM_GROUP_MAX_ROWS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn vacuum_group_calls() -> u64 {
    VACUUM_GROUP_CALLS.with(|c| c.get())
}

#[cfg(test)]
pub(crate) fn vacuum_group_max_rows() -> u64 {
    VACUUM_GROUP_MAX_ROWS.with(|c| c.get())
}

#[cfg(test)]
pub(crate) fn reset_vacuum_group_counters() {
    VACUUM_GROUP_CALLS.with(|c| c.set(0));
    VACUUM_GROUP_MAX_ROWS.with(|c| c.set(0));
}

/// 单组（**一把逻辑键**的全部版本行）的裁判 —— [`vacuum_plan`] 就是把这份逐组调用。
///
/// 为什么要把这一层单独露出来：四家的 vacuum 以前都必须先把**全库**按 key 归组攒进一个
/// `BTreeMap` 才问得动这一句 ⇒ vacuum 自己的瞬时内存 = O(全库物理行)（实测 600k 行那档
/// **+89.4 MB**，比它要裁的东西大两个数量级，下游 80 MB RSS 预算一次就顶穿）。
/// 而版本键是按 `raw ‖ version_be` 升序走查的 ⇒ **同一把键的行必然连续到达**，
/// 于是调用方只需要攒住"当前这一组"，判完就地交出去 ⇒ 峰值只与**最长的那条链**同阶。
///
/// 规则与 [`vacuum_plan`] 完全同一条（这份实现就是它的唯一内核，不存在"两份判据"）。
///
/// 组是**取走**的（`std::mem::take`）⇒ 调用方那句之后手上的 `Vec` 已经空着可以接下一把键，
/// 不需要自己再 `clear()`（四家各写一遍"判完记得清空"就是四个漏清空的機會）。
pub fn vacuum_plan_group<V, R>(
    group: &mut Vec<(u64, V, bool, R)>,
    xmin: u64,
) -> (Vec<V>, Vec<(V, R)>)
where
    V: Clone,
    R: Clone,
{
    let mut versions = std::mem::take(group);
    #[cfg(test)]
    if !versions.is_empty() {
        // 只数真判定过的那一组：空组（比如 lsm 里一整组都是 Drop 行）不算一次
        VACUUM_GROUP_CALLS.with(|c| c.set(c.get() + 1));
        VACUUM_GROUP_MAX_ROWS.with(|m| {
            let n = versions.len() as u64;
            if n > m.get() {
                m.set(n);
            }
        });
    }
    let mut to_delete: Vec<V> = Vec::new();
    let mut keep: Vec<(V, R)> = Vec::new();
    versions.sort_by_key(|(v, _, _, _)| *v);
    // 最后一个 version < xmin 的下标：它之前（更旧）的全删，它本身按墓碑规则定去留
    let last_old = versions.iter().rposition(|(ver, _, _, _)| *ver < xmin);
    match last_old {
        // 整条链都在 xmin 之上：全部保留
        None => keep.extend(versions.iter().map(|(_, row, _, extra)| (row.clone(), extra.clone()))),
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
    (to_delete, keep)
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
///
/// ⚠️ 引擎侧的 vacuum **不该用这一份**：它要求调用方先把全库按 key 归好组 ⇒ 瞬时内存
/// O(全库物理行)（M36 之前四家都是这么写的）。走查式的回收请用 [`vacuum_plan_group`]，
/// 这里保留给"组本来就已经在手上了"的调用方与测试。
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
        let (d, k) = vacuum_plan_group(&mut versions, xmin);
        to_delete.extend(d);
        keep.extend(k);
    }

    (to_delete, keep)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::vec;

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

    /// 上界哨兵必须扛住**末尾是 `0x00` 的 `end`**。
    ///
    /// `seek_prev` 造上界的形状是 `key ‖ 0x00`；若哨兵取 `encode_key(end, 0)`，那
    /// `key` 自己的版本行 `key ‖ 0x00…0x01` 会排在哨兵**之后** ⇒ 被判越界丢掉，
    /// 表现为 `seek_prev(k)` 返回比 k 小的那一版（这条就是这么被发现的）。
    #[test]
    fn upper_bound_keeps_trailing_zero_end() {
        let key = b"a2";
        let end = [b'a', b'2', 0u8];
        let enc_key = encode_key(key, 1);
        let old_sentinel = encode_key(&end, 0);
        let sentinel = version_key_upper_bound(&end);

        assert!(
            enc_key > old_sentinel,
            "旧哨兵会把 key 自己挤出界外 —— 这正是要复现的 bug"
        );
        assert!(
            enc_key < sentinel,
            "新哨兵必须把 key 的版本行留在界内（否则窗口少一行）"
        );
        // 越界方向的键仍然被挡在外面（raw ≥ end 的个别交错键由调用方逐条判）
        assert!(encode_key(b"a3", 1) > sentinel);
        // 前提：版本首字节为 0（version < 2^56）。事务号是 u64 计数器，
        // 每秒 100 万笔也要跑 228 年才碰得到 ⇒ 这个前提是成立的；
        // 越过它（比如拿 u64::MAX 当真实版本号）哨兵就不再是上界 —— 那种值只该用作 seek 的哨兵。
        let max_contract_version = 0x00ff_ffff_ffff_ffffu64;
        assert!(
            encode_key(b"a2", max_contract_version) < sentinel,
            "合同范围内的最大版本仍须在界内"
        );
    }
}
