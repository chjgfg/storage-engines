//! 终端分词、引号处理及参数数量检查。

use super::*;

/// 一行命令切 token：空白分隔；`"..."` / `'...'` 内空白原样保留，成对引号（`''` / `""`）
/// = 一个字面引号 —— 与引擎词法器的转义规则同一套。反斜杠**不是**转义符（KV 的值里它太常见）。
/// 引号可以在 token 中段出现（`ab"cd ef"` → `abcd ef`），对终端输入足够了。
pub fn tokenize(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut quoted = false; // 当前 token 是否在引号里（决定空白是否切分）
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '"' || c == '\'' {
            if !quoted {
                quoted = true;
                continue;
            }
            // 引号内再看一个字符：成对引号 = 字面引号，否则本引号闭合
            if chars.peek() == Some(&c) {
                chars.next();
                cur.push(c);
            } else {
                quoted = false;
            }
            continue;
        }
        if !quoted && c.is_whitespace() {
            if !cur.is_empty() {
                out.push(std::mem::take(&mut cur));
            }
            continue;
        }
        cur.push(c);
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

// ── 参数小工具 ──────────────────────────────────────────────────────────────

/// 恰好 N 个参数；数不对就把用法当错误交回去（调用方 `match` 一行接住）。
pub(super) fn one<'a>(args: &'a [String], usage: &str) -> Result<[&'a str; 1], Out> {
    match args {
        [k] => Ok([k.as_str()]),
        _ => Err(Out::err(format!("参数不对，用法: {usage}"))),
    }
}

pub(super) fn two<'a>(args: &'a [String], usage: &str) -> Result<[&'a str; 2], Out> {
    match args {
        [a, b] => Ok([a.as_str(), b.as_str()]),
        _ => Err(Out::err(format!("参数不对，用法: {usage}"))),
    }
}

pub(super) fn three<'a>(args: &'a [String], usage: &str) -> Result<[&'a str; 3], Out> {
    match args {
        [a, b, c] => Ok([a.as_str(), b.as_str(), c.as_str()]),
        _ => Err(Out::err(format!("参数不对，用法: {usage}"))),
    }
}
