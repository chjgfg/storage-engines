//! 本文件负责测试计量与机制对照。

use super::*;

/// 当前线程已发生的「读一行盘」次数（测试探针用）
#[cfg(test)]
pub(crate) fn read_value_calls() -> u64 {
    READ_VALUE_CALLS.with(|c| c.get())
}

#[cfg(test)]
pub(crate) fn note_keydir_visit() {
    KEYDIR_VISITS.with(|c| c.set(c.get() + 1));
}

#[cfg(test)]
pub(crate) fn note_merge_chunk(n: usize) {
    MERGE_CHUNK_MAX.with(|c| {
        let n = n as u64;
        if n > c.get() {
            c.set(n);
        }
    });
}

#[cfg(test)]
pub(crate) fn merge_chunk_max() -> u64 {
    MERGE_CHUNK_MAX.with(|c| c.get())
}

#[cfg(test)]
pub(crate) fn reset_merge_chunk() {
    MERGE_CHUNK_MAX.with(|c| c.set(0));
}

#[cfg(test)]
pub(crate) fn arm_merge_rescan_counter() {
    MERGE_RESCAN_ROWS.with(|c| c.set(0));
    MERGE_RESCAN_ARMING.with(|a| a.set(true));
}

#[cfg(test)]
pub(crate) fn disarm_merge_rescan_counter() {
    MERGE_RESCAN_ARMING.with(|a| a.set(false));
}

#[cfg(test)]
pub(crate) fn merge_rescan_rows() -> u64 {
    let n = MERGE_RESCAN_ROWS.with(|c| c.get());
    MERGE_RESCAN_ARMING.with(|a| a.set(false));
    n
}

#[cfg(test)]
pub(crate) fn note_merge_rescan_row() {
    MERGE_RESCAN_ARMING.with(|armed| {
        if armed.get() {
            MERGE_RESCAN_ROWS.with(|c| c.set(c.get() + 1));
        }
    });
}

#[cfg(test)]
pub(crate) fn note_merge_bulk_rebuild() {
    MERGE_BULK_REBUILDS.with(|c| c.set(c.get() + 1));
}

#[cfg(test)]
pub(crate) fn note_merge_bulk_fallback() {
    MERGE_BULK_FALLBACKS.with(|c| c.set(c.get() + 1));
}

/// 本线程到目前为止有几次 merge 走了批量成树（读一次清一次）
#[cfg(test)]
pub(crate) fn merge_bulk_rebuilds() -> u64 {
    MERGE_BULK_REBUILDS.with(|c| {
        let n = c.get();
        c.set(0);
        n
    })
}

/// ……其中又有几次因为扫到硬删除而整份退回逐条施加（读一次清一次）
#[cfg(test)]
pub(crate) fn merge_bulk_fallbacks() -> u64 {
    MERGE_BULK_FALLBACKS.with(|c| {
        let n = c.get();
        c.set(0);
        n
    })
}

#[cfg(test)]
pub(super) fn merge_stage_gated() -> bool {
    std::env::var("MERGE_STAGE_SPLIT").is_ok()
}

/// merge 进门：清槽、记住进门时的活字节、把高水位压到当前值（第一段从这里开始量）。
#[cfg(test)]
pub(crate) fn merge_probe_start() {
    if !merge_stage_gated() {
        return;
    }
    for s in &MERGE_LIVE {
        s.store(0, std::sync::atomic::Ordering::Relaxed);
    }
    for s in &MERGE_PEAK {
        s.store(0, std::sync::atomic::Ordering::Relaxed);
    }
    let l = crate::mem_probe::live();
    MERGE_ENTRY_LIVE.store(l, std::sync::atomic::Ordering::Relaxed);
    MERGE_SEG_BASE.store(
        crate::mem_probe::reset_peak(),
        std::sync::atomic::Ordering::Relaxed,
    );
}

/// 走完一段：记下"段末净增"与"段内尖峰"，然后把高水位压回当前值，下一段自己从头算。
#[cfg(test)]
pub(crate) fn merge_probe_mark(slot: usize) {
    if !merge_stage_gated() {
        return;
    }
    use std::sync::atomic::Ordering::Relaxed;
    let net = crate::mem_probe::live() as i64 - MERGE_ENTRY_LIVE.load(Relaxed) as i64;
    MERGE_LIVE[slot].store(net, Relaxed);
    MERGE_PEAK[slot].store(
        crate::mem_probe::peak().saturating_sub(MERGE_SEG_BASE.load(Relaxed)),
        Relaxed,
    );
    MERGE_SEG_BASE.store(crate::mem_probe::reset_peak(), Relaxed);
}

/// 出门：把高水位恢复成"进程语义"（不然外层台架读到的是被本尺子压过的水位）。
#[cfg(test)]
pub(crate) fn merge_probe_stop() {
    if !merge_stage_gated() {
        return;
    }
    crate::mem_probe::reset_peak();
}

/// 四段读数 `(净增, 段内尖峰)`，单位字节。⚠️ 读到全 0 先确认门控开没开、这一趟有没有真走进 merge。
#[cfg(test)]
pub(crate) fn merge_probe_read() -> [(i64, u64); 4] {
    use std::sync::atomic::Ordering::Relaxed;
    let mut out = [(0i64, 0u64); 4];
    for (o, (l, p)) in out.iter_mut().zip(MERGE_LIVE.iter().zip(&MERGE_PEAK)) {
        *o = (l.load(Relaxed), p.load(Relaxed));
    }
    out
}

/// 清单攒到多少条（M62：两扇门的清单必须**条数相同**才允许比瞬时峰值，否则差的是形状不是账）。
#[cfg(test)]
pub(crate) fn merge_bulk_len() -> usize {
    MERGE_BULK_LEN.with(|c| c.get())
}

// ── M81：`merge()` 内部的**时间**账（门控 `MERGE_TIME_SPLIT`，`#[cfg(test)]` ⇒ 不随 crate 发布）──
//
// 要回答的是第 3 项的立项判据：20M 档那 113.5s 里，"走 KeyDir"（纯内存 —— 改成两趟只是多走一遍，
// 本身不花钱）与"每键一次 `read_live` + `append`"（每键 seek + read_exact 两次系统调用再加一次写）
// 各占多少；后者才是"按段增量"省得掉的那笔 ⇒ `rw` 不过 60% 就不立项。
// 五格里还有两格是这条修法顺带省/不省的边界：
//   `pre`    = fsync + 删密封段 + rename + 换 active（增量 merge 也要付，只是份数少）
//   `rescan` = 腾掉旧索引后 M43 那趟"重扫新日志建索引"（**部分 merge 就地改 KeyLoc ⇒ 整趟不付**）
//   `hint`   = 收尾删旧 hint / 落新 hint
//
// ⚠️ 与 M62 那把内存尺子的分工：内存尺子会 `reset_peak()`（常开会锯短外层 `timed()` 的峰值读数，
// M37 实测 171.4 → 47.2），这一把**只加时间差、不碰任何水位** ⇒ 两个门控独立；量时间别开内存那把。
#[cfg(test)]
pub(super) fn merge_time_on() -> bool {
    std::env::var("MERGE_TIME_SPLIT").is_ok()
}

/// 进门时把"盘上有哪些分片、各多大"抄一份 —— 走查中途旧段就被删了，那时再读只剩新文件。
#[cfg(test)]
pub(super) fn merge_seg_begin(base: &std::path::Path, active_id: u32, active_end: u64) {
    if !merge_time_on() {
        return;
    }
    let mut sizes = std::collections::BTreeMap::new();
    sizes.insert(active_id, active_end);
    for id in discover_sealed_ids(base) {
        let bytes = std::fs::metadata(segment_path(base, id))
            .map(|m| m.len())
            .unwrap_or(0);
        sizes.insert(id, bytes);
    }
    MERGE_SEG_SIZE.with(|s| *s.borrow_mut() = sizes);
    MERGE_SEG_LIVE.with(|l| l.borrow_mut().clear());
}

/// 走查每抄到一个活键就给那个分片记一笔（`len` = 值长，不含记录头 ⇒ 是**下界**，如实按它算）。
#[cfg(test)]
pub(super) fn merge_seg_note_live(file_id: u32, len: u32) {
    if !merge_time_on() {
        return;
    }
    MERGE_SEG_LIVE.with(|l| {
        let mut guard = l.borrow_mut();
        let e = guard.entry(file_id).or_insert((0, 0));
        e.0 += len as u64;
        e.1 += 1;
    });
}

/// 脏段分布 + "只重写活占比 < 50% 的段"能省掉多少活字节（= `rw` 那一格的可省上限）。
#[cfg(test)]
pub(super) fn merge_seg_dump(label: &str) {
    if !merge_time_on() {
        return;
    }
    const DIRTY: f64 = 0.5;
    let sizes = MERGE_SEG_SIZE.with(|s| s.borrow().clone());
    let live = MERGE_SEG_LIVE.with(|l| l.borrow().clone());
    let (mut total_live, mut dirty_live) = (0u64, 0u64);
    let mut lines: Vec<String> = Vec::new();
    for (id, size) in &sizes {
        let (lb, lr) = live.get(id).copied().unwrap_or((0, 0));
        total_live += lb;
        let ratio = if *size > 0 {
            lb as f64 / *size as f64
        } else {
            0.0
        };
        if ratio < DIRTY {
            dirty_live += lb;
        }
        lines.push(format!(
            "  #{id} 盘 {:.1} MB / 活 {:.1} MB = {:.1}%（{lr} 行）",
            *size as f64 / 1e6,
            lb as f64 / 1e6,
            ratio * 100.0
        ));
    }
    let saved = if total_live > 0 {
        1.0 - dirty_live as f64 / total_live as f64
    } else {
        0.0
    };
    eprintln!(
        "merge 分段账 {label}: {} 段 | 活字节合计 {:.1} MB | 只重写占比 < {:.0}% 的段要重写 {:.1} MB \
         ⇒ rw 那一格可省 {:.1}%（这是**上限**：段筛选、恢复复杂度、脏段里仍要搬的活行都没扣）",
        sizes.len(),
        total_live as f64 / 1e6,
        DIRTY * 100.0,
        dirty_live as f64 / 1e6,
        saved * 100.0
    );
    for l in lines {
        eprintln!("{l}");
    }
}

#[cfg(test)]
pub(super) fn merge_t_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
pub(super) fn merge_t_start() {
    if merge_time_on() {
        let now = merge_t_now();
        MERGE_T_NS.set([0; 5]);
        MERGE_T_LAST.set(now);
        MERGE_T_ENTRY.set(now);
    }
}

/// 顺序段与交错段**共用这一条**：把"上一格"记进 slot，并把水位推到此刻 ⇒ 五格首尾相接。
#[cfg(test)]
pub(super) fn merge_t_mark(slot: usize) {
    if !merge_time_on() {
        return;
    }
    let now = merge_t_now();
    let mut s = MERGE_T_NS.get();
    s[slot] = s[slot].saturating_add(now.saturating_sub(MERGE_T_LAST.get()));
    MERGE_T_NS.set(s);
    MERGE_T_LAST.set(now);
}

#[cfg(test)]
pub(super) fn merge_t_dump(label: &str) {
    if !merge_time_on() {
        return;
    }
    let s = MERGE_T_NS.get();
    let s = [
        s[0] as f64 / 1e9,
        s[1] as f64 / 1e9,
        s[2] as f64 / 1e9,
        s[3] as f64 / 1e9,
        s[4] as f64 / 1e9,
    ];
    let total = s[0] + s[1] + s[2] + s[3] + s[4];
    // 自校：五格首尾相接 ⇒ 它们的和必须≈外层墙钟。差超过 2% 就是尺子自己坏了（M81 第一版
    // 重复计了 walk+rw，五格之和比 bench 独立读到的数高出整整一格 rw —— 那时代码没报错，
    // 只有这条自校能当场抓住）。
    let outer = (merge_t_now().saturating_sub(MERGE_T_ENTRY.get())) as f64 / 1e9;
    let drift = if outer > 0.0 {
        (total - outer) / outer * 100.0
    } else {
        0.0
    };
    let pct = |v: f64| if total > 0.0 { v / total * 100.0 } else { 0.0 };
    eprintln!(
        "merge_time {label}: walk={:.3}s rw={:.3}s pre={:.3}s rescan={:.3}s hint={:.3}s total={:.3}s | rw={:.1}% rw+rescan={:.1}%（参考线：方案写的 rw>60% 门槛，复核认定无充分依据 ⇒ 只作参考）",
        s[0],
        s[1],
        s[2],
        s[3],
        s[4],
        total,
        pct(s[1]),
        pct(s[1] + s[3])
    );
    merge_seg_dump(label);
    eprintln!(
        "merge_time 自校 {label}: 五格之和 {total:.3}s vs 外层墙钟 {outer:.3}s ⇒ 差 {drift:+.1}%{}",
        if drift.abs() > 2.0 {
            " ⚠️ 尺子不自洽，这一行的百分比不可用"
        } else {
            "（一致）"
        }
    );
}

/// 本线程 hint 门最后一次预留的条数（测试探针用；`0` = 没走过这道门）
#[cfg(test)]
pub(crate) fn hint_reserve_noted() -> usize {
    HINT_RESERVE_NOTED.with(|c| c.get())
}

/// 清槽：读一份新 hint 之前必须调一次 —— 不然上一条留下的数会让这一条的断言**看着像过了**
/// （M48 那条"槽不会自己清"，比读 0 更危险）
#[cfg(test)]
pub(crate) fn hint_reserve_reset() {
    HINT_RESERVE_NOTED.with(|c| c.set(0));
}

/// 当前线程折候选时检查过的编码键数（测试探针用）
#[cfg(test)]
pub(crate) fn keydir_visits() -> u64 {
    KEYDIR_VISITS.with(|c| c.get())
}
