//! JSONL 二进制键值的 Base64 表达。

// ─── Base64 ─────────────────────────────────────────────────────────────────
//
// 自己实现是为了不引第三方依赖（全项目只有 5 个 dep）。

pub fn b64_encode(data: &[u8]) -> String {
    const T: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity((data.len() + 2) / 3 * 4);
    let mut i = 0;
    while i + 3 <= data.len() {
        let n = ((data[i] as u32) << 16) | ((data[i + 1] as u32) << 8) | (data[i + 2] as u32);
        out.push(T[((n >> 18) & 63) as usize] as char);
        out.push(T[((n >> 12) & 63) as usize] as char);
        out.push(T[((n >> 6) & 63) as usize] as char);
        out.push(T[(n & 63) as usize] as char);
        i += 3;
    }
    match data.len() - i {
        1 => {
            let n = (data[i] as u32) << 16;
            out.push(T[((n >> 18) & 63) as usize] as char);
            out.push(T[((n >> 12) & 63) as usize] as char);
            out.push('=');
            out.push('=');
        }
        2 => {
            let n = ((data[i] as u32) << 16) | ((data[i + 1] as u32) << 8);
            out.push(T[((n >> 18) & 63) as usize] as char);
            out.push(T[((n >> 12) & 63) as usize] as char);
            out.push(T[((n >> 6) & 63) as usize] as char);
            out.push('=');
        }
        _ => {}
    }
    out
}

pub fn b64_decode(s: &str) -> Result<Vec<u8>, String> {
    const T: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut table = [255u8; 256];
    for (i, &c) in T.iter().enumerate() {
        table[c as usize] = i as u8;
    }
    let bytes: Vec<u8> = s.bytes().filter(|&b| b != b'=').collect();
    let mut out = Vec::with_capacity(bytes.len() * 3 / 4 + 2);
    let mut i = 0;
    while i + 4 <= bytes.len() {
        let n = ((table[bytes[i] as usize] as u32) << 18)
            | ((table[bytes[i + 1] as usize] as u32) << 12)
            | ((table[bytes[i + 2] as usize] as u32) << 6)
            | (table[bytes[i + 3] as usize] as u32);
        out.push(((n >> 16) & 0xFF) as u8);
        out.push(((n >> 8) & 0xFF) as u8);
        out.push((n & 0xFF) as u8);
        i += 4;
    }
    // 尾组按**剩余字符数**给字节：2 个字符 = 1 字节、3 个字符 = 2 字节。
    // ⚠️ 这里**不能再按 `=` 的个数回头裁剪**（M45 修的就是这个）：`=` 在上面那行 `filter`
    // 里已经被剥掉了，长度信息已经体现在 `rem` 上 —— 再按 pad 减一次就是**二次修正**，
    // 于是"长度不是 3 的倍数"的每一个键/值都少 1~2 字节。现场的形状是：11 字节的键
    // `demo:000001` 解回 10 字节 `demo:00000` ⇒ JSONL 导入把 300 把互不相同的键
    // **截成同一把**，后写覆盖前写，`IMPORT records=301` 而库里只剩 32 把。
    let rem = bytes.len() - i;
    if rem == 2 {
        let n =
            (table[bytes[i] as usize] as u32) << 18 | ((table[bytes[i + 1] as usize] as u32) << 12);
        out.push(((n >> 16) & 0xFF) as u8);
    } else if rem == 3 {
        let n = (table[bytes[i] as usize] as u32) << 18
            | ((table[bytes[i + 1] as usize] as u32) << 12)
            | ((table[bytes[i + 2] as usize] as u32) << 6);
        out.push(((n >> 16) & 0xFF) as u8);
        out.push(((n >> 8) & 0xFF) as u8);
    } else if rem != 0 {
        return Err("非法 base64 长度".into());
    }
    Ok(out)
}
