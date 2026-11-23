//! 终端命令帮助。

use super::*;

pub(super) fn help_text() -> String {
    "\
命令表（key/value 含空格用引号包：\"a b\"）:
  get K                    读 K
  set K V                  写 K=V
  setex K 秒 V             写入并设 TTL
  del K                    逻辑删除（写墓碑）
  exists K                 判断存在
  meta K                   版本号 / 长度 / 删除标记 / TTL
  incr K [N] | decr K [N]  计数器（值须是十进制整数），默认步长 1
  mset K V [K V ...]       批量写（整批原子，冲突整体回滚）
  mget K [K ...]           批量读
  scan [起] [止]           范围扫描 [起, 止)，两端都可省；rscan 倒着扫
  prefix P                 按前缀扫
  seek K | seekprev K      第一条 ≥ K / 最后一条 ≤ K
  count [P]                键计数（可按前缀）
  find 子串 [页] [每页]    分页模糊找 key
  ttl K | refresh K 秒 | persist K | purge    TTL 四件套
  begin | commit | rollback   MVCC 事务：之后命令都走这条事务
  info | health | vacuum | flush | checkpoint | merge   维护面
  engine [名] [目录]       看当前引擎 / 切引擎（memory|bitcask|bplus_tree|lsm_tree）
  help | exit              本表 / 退出"
        .to_string()
}
