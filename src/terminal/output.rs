//! 终端结果表与二进制字节展示。

use super::*;

impl Out {
    /// 纯消息行（成功）
    pub fn msg(kind: &'static str, message: impl Into<String>) -> Self {
        Self {
            ok: true,
            kind,
            message: message.into(),
            table: None,
        }
    }

    /// 错误行（不中断后续命令，与逐条执行的习惯一致）
    pub fn err(message: impl Into<String>) -> Self {
        Self {
            ok: false,
            kind: "ERROR",
            message: message.into(),
            table: None,
        }
    }

    /// 消息 + 表（消息作脚注，如「SCAN 3 行」）
    pub fn with_table(kind: &'static str, message: String, table: Table) -> Self {
        Self {
            ok: true,
            kind,
            message,
            table: Some(table),
        }
    }
}

// ── 字节与 token ─────────────────────────────────────────────────────────────

/// 字节 → 可读文本：合法 UTF-8 原样（换行/制表转义，免得撑破表格行），否则 `<hex …>`。
pub fn show(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(s) => s
            .replace('\n', "\\n")
            .replace('\r', "\\r")
            .replace('\t', "\\t"),
        Err(_) => {
            let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
            format!("<hex {hex}>")
        }
    }
}
