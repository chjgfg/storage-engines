//! 统一门面层 [`storage_engines::engine`] 的使用示例。
//!
//! 运行：
//! ```text
//! cargo run                  # 默认用 memory，不落盘
//! cargo run -- lsm           # 换引擎，库建在 ./data/<引擎名>
//! cargo run -- bitcask ./db  # 自己指定目录
//! ```
//!
//! 可用引擎名（大小写无所谓，`-` / `_` 等价）：
//! `bitcask` / `bplus_tree`(bplus, btree) / `lsm_tree`(lsm) / `memory`(mem)

use std::println;
use std::eprintln;
use std::vec;
use std::format;
use std::path::PathBuf;
use std::time::Duration;

use storage_engines::engine::{open_engine, KvEngine};
use storage_engines::{EngineError, SearchQuery};

fn main() {
    if let Err(e) = run() {
        eprintln!("出错了: {e}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), EngineError> {
    // ---- 1. 按运行时字符串选引擎 ----------------------------------------
    //
    // 这就是门面层的意义：换引擎只改这一个字符串，下面所有代码一行不动。
    let mut args = std::env::args().skip(1);
    let name = args.next().unwrap_or_else(|| "memory".to_string());

    // 三个磁盘引擎必须给路径；memory 传 None（给了也会被忽略）
    let dir: Option<PathBuf> = match name.to_ascii_lowercase().as_str() {
        "memory" | "mem" | "in_memory" => None,
        _ => Some(
            args.next()
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("./data").join(&name)),
        ),
    };

    let db = open_engine(&name, dir.as_deref())?;

    println!("== 引擎: {} ==", db.name());
    match db.path() {
        Some(p) => println!("   库路径: {}", p.display()),
        None => println!("   纯内存，无库路径"),
    }
    println!();

    basic_ops(db.as_ref());
    batch_and_scan(db.as_ref());
    ttl_ops(db.as_ref());
    write_conflict(db.as_ref());
    bulk_load(db.as_ref());
    maintenance(db.as_ref())?;

    println!("\n全部示例跑完。");
    Ok(())
}

/// 增删改查：事务里做完,最后 commit
fn basic_ops(db: &dyn KvEngine) {
    println!("-- 1. 基本读写 --");
    let tx = db.begin();
    println!("   事务版本号 = {}", tx.version());

    // set 返回 false 表示写冲突（有别的事务先改了这个 key）
    tx.set(b"name", b"Alice".to_vec());
    tx.set(b"city", b"Beijing".to_vec());

    println!("   get(name)   = {:?}", as_str(tx.get(b"name")));
    println!("   exists(城市) = {}", tx.exists(b"city"));

    // 计数器：value 必须是十进制整数字符串
    tx.set(b"visits", b"10".to_vec());
    let n = tx.incr(b"visits", 5).expect("incr");
    println!("   incr(visits, +5) = {n}");

    // delete 写的是 tombstone（墓碑），不是物理删除
    tx.delete(b"city");
    println!("   删除后 get(city) = {:?}", tx.get(b"city"));

    // 元数据：版本号、是否删除、TTL 等
    if let Some(meta) = tx.get_meta(b"name") {
        println!(
            "   meta(name): 版本={} 长度={:?} 已删除={}",
            meta.version, meta.value_len, meta.deleted
        );
    }

    // 必须 commit，否则这个事务一直挂在活跃表里，vacuum 回收不掉旧版本
    tx.commit();
    println!();
}

/// 批量写 + 各种扫描
fn batch_and_scan(db: &dyn KvEngine) {
    println!("-- 2. 批量与扫描 --");
    let tx = db.begin();

    // batch_set 的 Err 返回的是「在哪个 key 上冲突了」
    let users: Vec<(Vec<u8>, Vec<u8>)> = (1..=5)
        .map(|i| {
            (
                format!("user:{i:02}").into_bytes(),
                format!("用户{i}").into_bytes(),
            )
        })
        .collect();
    if let Err(bad_key) = tx.batch_set(&users) {
        println!("   批量写在 {:?} 上冲突", as_str(Some(bad_key)));
        tx.rollback();
        return;
    }

    // 一次取多个，顺序和传入的 keys 一致，不存在的位置是 None
    let got = tx.multi_get(&[b"user:01".to_vec(), b"user:99".to_vec()]);
    println!("   multi_get = {:?}", got.iter().map(as_ref_str).collect::<Vec<_>>());

    // 范围扫描是左闭右开 [start, end)，所以 user:04 不在结果里
    let range = tx.scan(Some(b"user:02"), Some(b"user:04"));
    println!("   scan[user:02, user:04) = {:?}", keys_of(&range));

    println!("   prefix_scan(user:)     = {:?}", keys_of(&tx.prefix_scan(b"user:")));
    println!("   key_count(user:)       = {}", tx.key_count(b"user:"));

    // 倒序扫描，两端传 None 表示无界
    let rev = tx.reverse_scan(None, None);
    println!("   reverse_scan 前 3 个    = {:?}", keys_of(&rev[..rev.len().min(3)]));

    // seek = 找第一个 >= key 的；seek_prev = 找最后一个 <= key 的
    println!("   seek(user:03)          = {:?}", rec_key(tx.seek(b"user:03")));
    println!("   seek_prev(user:03)     = {:?}", rec_key(tx.seek_prev(b"user:03")));

    // 模糊搜索 + 分页，页码从 0 开始
    let page = tx.search_keys(&SearchQuery::contains("user", 0, 3));
    println!(
        "   搜索 \"user\" 第0页: {:?}  (共{}条 {}页, 有下一页={})",
        keys_of(&page.items),
        page.total.unwrap_or_default(),
        page.total_pages.unwrap_or_default(),
        page.has_next()
    );

    tx.commit();
    println!();
}

/// TTL：过期时间
fn ttl_ops(db: &dyn KvEngine) {
    println!("-- 3. TTL --");
    let tx = db.begin();

    // 注意：TTL 最小粒度是秒，传 0 会被当成 1 秒
    tx.set_with_ttl(b"session", b"token-abc".to_vec(), Duration::from_secs(60));

    // 返回值有三态：None=key不存在, Some(None)=永久, Some(Some(d))=还剩 d
    match tx.get_ttl(b"session") {
        None => println!("   session 不存在或已过期"),
        Some(None) => println!("   session 是永久的"),
        Some(Some(d)) => println!("   session 还剩 {} 秒", d.as_secs()),
    }

    tx.refresh_ttl(b"session", Duration::from_secs(120));
    println!("   续期后还剩 {:?} 秒", tx.get_ttl(b"session").flatten().map(|d| d.as_secs()));

    // persist_key：摘掉 TTL 变成永久
    tx.persist_key(b"session");
    println!("   persist 后 = {:?} (Some(None) 就是永久)", tx.get_ttl(b"session"));

    // 物理清掉已过期的 key，返回清理条数
    println!("   purge_expired 清理了 {} 条", tx.purge_expired());

    tx.commit();
    println!();
}

/// 写冲突：快照隔离下,两个事务改同一个 key
fn write_conflict(db: &dyn KvEngine) {
    println!("-- 4. 写冲突 --");

    let t1 = db.begin();
    let t2 = db.begin();

    println!("   t1.set(counter) -> {}", t1.set(b"counter", b"1".to_vec()));

    // t2 也想改同一个 key，这里返回 false —— 这就是写冲突
    let ok = t2.set(b"counter", b"2".to_vec());
    println!("   t2.set(counter) -> {ok}   <- false 说明撞车了");

    t1.commit();
    // 冲突的事务应该整个回滚，它已经写进去的部分会被物理删掉
    t2.rollback();

    let t3 = db.begin();
    println!("   最终值 = {:?} (t1 赢)", as_str(t3.get(b"counter")));
    t3.commit();
    println!();
}

/// 批量导入：没有冲突检测、不写 WAL,比普通事务快几个数量级
fn bulk_load(db: &dyn KvEngine) {
    println!("-- 5. 批量导入 --");
    println!("   注意：单写者假定，导入期间别开别的写事务");

    {
        let mut bulk = db.begin_bulk();
        bulk.put(b"bulk:a", b"1".to_vec());
        bulk.put_batch(&[(b"bulk:b".to_vec(), b"2".to_vec())]);
        // put_batch_owned 直接吃掉 Vec，省一次 clone，导入大数据用这个
        bulk.put_batch_owned(vec![(b"bulk:c".to_vec(), b"3".to_vec())]);

        // finish 必须调用（Drop 也会兜底，但 flush 顺序不保证）
        bulk.finish();
    }

    let tx = db.begin();
    println!("   导入结果 = {:?}", keys_of(&tx.prefix_scan(b"bulk:")));
    tx.commit();
    println!();
}

/// 落盘、检查点、版本回收
fn maintenance(db: &dyn KvEngine) -> Result<(), EngineError> {
    println!("-- 6. 维护操作 --");

    // memory 引擎这两个是空实现，磁盘引擎才真干活
    db.flush()?;
    db.checkpoint()?;
    println!("   flush + checkpoint 完成");

    // vacuum 回收旧版本。注意：有活跃事务没 commit 的话，回收水位上不去
    let stats = db.vacuum()?;
    println!(
        "   vacuum: 水位={} 清理版本={} blob重写={:?}",
        stats.xmin, stats.versions_removed, stats.blob_rewritten
    );

    // merge 只有 bitcask 支持，其他引擎会返回 Unsupported —— 这不是错误，
    // 是门面层告诉你「这个引擎没这功能」，按需处理即可
    match db.merge() {
        Ok(()) => println!("   merge 完成"),
        Err(EngineError::Unsupported { engine, op }) => {
            println!("   {engine} 不支持 {op}（正常，只有 bitcask 有）")
        }
        Err(e) => return Err(e),
    }

    // 导出当前快照下每个 key 的最新可见版本
    let all = db.export_latest_visible(false);
    println!("   当前共 {} 个可见 key", all.len());
    println!();
    Ok(())
}

// ---- 下面都是打印用的小helper，跟门面层本身无关 ----------------------

fn as_str(v: Option<Vec<u8>>) -> Option<String> {
    v.map(|b| String::from_utf8_lossy(&b).into_owned())
}

fn as_ref_str(v: &Option<Vec<u8>>) -> Option<String> {
    v.as_ref().map(|b| String::from_utf8_lossy(b).into_owned())
}

fn keys_of(records: &[storage_engines::ExportRecord]) -> Vec<String> {
    records
        .iter()
        .map(|r| String::from_utf8_lossy(&r.key).into_owned())
        .collect()
}

fn rec_key(r: Option<storage_engines::ExportRecord>) -> Option<String> {
    r.map(|r| String::from_utf8_lossy(&r.key).into_owned())
}
