"""核对职责拆分前后 Rust 可执行代码块，忽略注释、导入和模块包装。

公开接口和条件编译另由 cargo 检查及测试入口清单验证；本脚本不替代编译器。
同一 rustfmt 在相同作用域缩进下规整两侧；字符串、算法、断言和宏参数继续比较。
"""
from __future__ import annotations

import argparse
import collections
import hashlib
import json
import re
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def lex(source: str, *, spans: bool = False) -> list:
    """词法扫描保留所有字面量，剔除嵌套注释，避免误把字符串当代码。"""
    tokens: list[str] = []
    positions: list[tuple[int, int]] = []
    i = 0
    while i < len(source):
        if source[i].isspace():
            i += 1
            continue
        if source.startswith("//", i):
            end = source.find("\n", i)
            i = len(source) if end < 0 else end
            continue
        if source.startswith("/*", i):
            depth = 1
            i += 2
            while depth:
                if i >= len(source):
                    raise ValueError("块注释未结束")
                if source.startswith("/*", i):
                    depth += 1
                    i += 2
                elif source.startswith("*/", i):
                    depth -= 1
                    i += 2
                else:
                    i += 1
            continue
        raw = re.match(r'(?:b|c)?r(#+)?"', source[i:])
        char = re.match(r"'(?:\\(?:u\{[0-9a-fA-F_]+\}|x[0-9a-fA-F]{2}|.)|[^'\\\n])'", source[i:])
        if raw:
            marker = '"' + (raw[1] or "")
            end = source.find(marker, i + len(raw[0]))
            if end < 0:
                raise ValueError("原始字符串未结束")
            end += len(marker)
        elif char:
            end = i + len(char[0])
        elif source[i] == '"':
            end = i + 1
            while end < len(source):
                if source[end] == "\\":
                    end += 2
                elif source[end] == '"':
                    end += 1
                    break
                else:
                    end += 1
        else:
            word = re.match(r"[A-Za-z_][A-Za-z_0-9]*|[0-9][A-Za-z_0-9.]*|::|->|=>|==|!=|<=|>=|&&|\|\||\S", source[i:])
            assert word is not None
            end = i + len(word[0])
        tokens.append(source[i:end])
        positions.append((i, end))
        i = end
    return positions if spans else tokens


def items(tokens: list[str]):
    """只在完整代码项边界分片，闭合括号内的分号不能截断代码项。"""
    start = 0
    braces = parens = brackets = 0
    for i, token in enumerate(tokens):
        braces += (token == "{") - (token == "}")
        parens += (token == "(") - (token == ")")
        brackets += (token == "[") - (token == "]")
        if min(braces, parens, brackets) < 0:
            raise ValueError("括号不平衡")
        if not (braces or parens or brackets) and token in ("}", ";"):
            yield tokens[start:i + 1]
            start = i + 1
    if braces or parens or brackets:
        raise ValueError("代码项未结束")
    if tokens[start:]:
        yield tokens[start:]


def normalized(tokens: list[str]) -> list[str]:
    """只归一化职责搬移所需的模块路径和父模块可见性。"""
    absolute = {
        ("crate", "::", "bitcask", "::", "bitcask", "::"): ["bitcask", "::"],
        ("crate", "::", "bitcask", "::", "checkpoint_probe", "::"): ["checkpoint_probe", "::"],
        ("crate", "::", "lsm_tree", "::", "lsm_tree", "::"): ["lsm_tree", "::"],
        ("crate", "::", "common", "::", "store", "::"): ["store", "::"],
    }
    out: list[str] = []
    i = 0
    while i < len(tokens):
        if tokens[i] == "pub":
            i += 1
            if i < len(tokens) and tokens[i] == "(":
                while tokens[i] != ")":
                    i += 1
                i += 1
            continue
        if tokens[i:i + 2] == ["super", "::"]:
            i += 2
            continue
        replacement = next(((k, v) for k, v in absolute.items() if tuple(tokens[i:i + len(k)]) == k), None)
        if replacement:
            out.extend(replacement[1])
            i += len(replacement[0])
            continue
        out.append(tokens[i])
        i += 1
    return out


def atoms(tokens: list[str], owner: tuple[str, ...] = ()):
    """展开模块与 impl 包装，保留方法所属 impl、数据定义和宏完整参数。"""
    for item in items(tokens):
        header = normalized(item)
        while header[:2] == ["#", "["]:
            depth = 1
            end = 2
            while depth:
                depth += (header[end] == "[") - (header[end] == "]")
                end += 1
            header = header[end:]
        if not header or header[0] == ";":
            continue
        if header[0] == "use" or header[:2] == ["include", "!"]:
            continue
        if header[0] == "mod":
            if "{" in item:
                open_at = item.index("{")
                close_at = len(item) - 1 - item[::-1].index("}")
                yield from atoms(item[open_at + 1:close_at], owner)
            continue
        if header[0] == "impl":
            open_at = item.index("{")
            close_at = len(item) - 1 - item[::-1].index("}")
            impl_header = tuple(normalized(item[:open_at]))
            yield from atoms(item[open_at + 1:close_at], impl_header)
            continue
        encoded = json.dumps((owner, normalized(item)), ensure_ascii=False, separators=(",", ":"))
        digest = hashlib.sha256(encoded.encode()).hexdigest()
        yield digest, " ".join(header[:12])


def raw_atoms(source: str):
    """提取完整代码项及其所属 impl，保留原词法邻接关系供格式化使用。"""
    tokens = lex(source)
    spans = lex(source, spans=True)

    def walk(low, high, owner=()):
        offset = low
        for item in items(tokens[low:high]):
            end = offset + len(item)
            header = normalized(item)
            while header[:2] == ["#", "["]:
                depth, stop = 1, 2
                while depth:
                    depth += (header[stop] == "[") - (header[stop] == "]")
                    stop += 1
                header = header[stop:]
            if header and header[0] not in (";", "use") and header[:2] != ["include", "!"]:
                if header[0] in ("mod", "impl"):
                    if "{" in item:
                        opening = item.index("{")
                        closing = len(item) - 1 - item[::-1].index("}")
                        parent = tuple(normalized(item[:opening])) if header[0] == "impl" else owner
                        yield from walk(offset + opening + 1, offset + closing, parent)
                else:
                    raw = source[spans[offset][0]:spans[end - 1][1]]
                    # 常量的结构体初始化以 } 结尾，但该项仍需消费随后独立的分号。
                    if header[0] in ("const", "static") and header[1] != "fn" and item[-1] == "}":
                        raw += ";"
                    yield owner, raw, " ".join(header[:12])
            offset = end

    yield from walk(0, len(tokens))


def format_input(source: str):
    """去掉搬迁包装与注释后再格式化，避免原嵌套深度改变尾逗号和导入排序。"""
    tokens, spans = lex(source), lex(source, spans=True)
    pieces, i = [], 0
    while i < len(tokens):
        if tokens[i] == "pub":
            i += 1
            if i < len(tokens) and tokens[i] == "(":
                while tokens[i] != ")":
                    i += 1
                i += 1
            continue
        if tokens[i:i + 2] == ["super", "::"]:
            i += 2
            continue
        # absolute 的四种等价路径也由 normalized 统一；对短路径前缀整体替换。
        length = next((n for n in (6, 4) if tokens[i] == "crate" and i + n <= len(tokens)
                       and normalized(tokens[i:i + n]) != tokens[i:i + n]), None)
        if length:
            pieces.append((" ".join(normalized(tokens[i:i + length])), spans[i][0], spans[i + length - 1][1]))
            i += length
            continue
        pieces.append((tokens[i], *spans[i]))
        i += 1
    # 字符串原样保留；仅原来相邻的词法 token 继续相邻（生命周期、位移、原始标识符等）。
    return "".join(token if n == 0 or pieces[n - 1][2] == start else
                   ("\n" if "\n" in source[pieces[n - 1][2]:start] else " ") + token
                   for n, (token, start, end) in enumerate(pieces))


def format_batch(batch):
    """在相同顶层缩进下格式化两侧代码项，不删除逗号或放宽字符串比较。"""
    text = "\n".join(f"// __layout_atom_{i}__\n"
                     + (f"impl __Layout {{ {format_input(source)} }}" if owner else format_input(source))
                     + "\n" for i, (owner, source, _, _) in enumerate(batch))
    result = subprocess.run(
        ["rustfmt", "--edition", "2024", "--config", "skip_children=true", "--emit", "stdout"],
        input=text, text=True, encoding="utf-8", capture_output=True,
    )
    if result.returncode:
        raise ValueError("代码项格式化失败：\n" + result.stderr)
    parts = re.split(r"(?m)^// __layout_atom_(\d+)__\s*$", result.stdout)
    if len(parts) != 2 * len(batch) + 1:
        raise ValueError("格式化后的代码项边界数量不一致")
    for n in range(len(batch)):
        if int(parts[2 * n + 1]) != n:
            raise ValueError("格式化后的代码项顺序不一致")
        owner, _, name, file = batch[n]
        tokens = lex(parts[2 * n + 2])
        if owner:
            tokens = tokens[tokens.index("{") + 1:-1]
        encoded = json.dumps((owner, normalized(tokens)), ensure_ascii=False, separators=(",", ":"))
        yield hashlib.sha256(encoded.encode()).hexdigest(), f"{file}: {name}"


def source_atoms(sources: dict[str, str]):
    found = collections.Counter()
    locations: dict[str, list[str]] = collections.defaultdict(list)
    rows = [(owner, source, name, file) for file, text in sources.items()
            for owner, source, name in raw_atoms(text)]
    batches = [rows[i:i + 64] for i in range(0, len(rows), 64)]
    with ThreadPoolExecutor(max_workers=4) as pool:
        for result in pool.map(lambda batch: list(format_batch(batch)), batches):
            for digest, location in result:
                found[digest] += 1
                locations[digest].append(location)
    return found, locations


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", default="5025f91f7c8b89dd9b4fbd6a650086cf108d2a65")
    parser.add_argument("--self-test", action="store_true", help="验证字面量、算法、元组及自定义宏变化仍被拒绝")
    args = parser.parse_args()
    if args.self_test:
        control = 'impl Demo { pub fn read(&self, x: (u8,)) -> usize { custom!(x,); let _ = b"a\\x00"; 64 + x.0 as usize } }'
        expected, _ = source_atoms({"control": control})
        changed = {
            "literal": control.replace('b"a\\x00"', 'b"b\\x00"'),
            "constant": control.replace("64 +", "65 +"),
            "operator": control.replace("64 +", "64 -"),
            "tuple": control.replace("(u8,)", "(u8)"),
            "macro": control.replace("custom!(x,)", "custom!(x)"),
            "missing_item": "",
        }
        results = {name: source_atoms({name: source})[0] != expected for name, source in changed.items()}
        print(json.dumps({"negative_controls_rejected": results, "pass": all(results.values())}, indent=2))
        raise SystemExit(0 if all(results.values()) else 1)
    paths = subprocess.check_output(["git", "ls-tree", "-r", "--name-only", args.baseline, "src", "tests"], cwd=ROOT).decode().splitlines()
    before = {p: subprocess.check_output(["git", "show", f"{args.baseline}:{p}"], cwd=ROOT).decode("utf-8") for p in paths if p.endswith(".rs")}
    after = {str(p.relative_to(ROOT)).replace("\\", "/"): p.read_text(encoding="utf-8") for folder in ("src", "tests") for p in (ROOT / folder).rglob("*.rs")}
    old, old_locations = source_atoms(before)
    new, new_locations = source_atoms(after)
    missing = old - new
    added = new - old
    report = {
        "baseline": args.baseline,
        "before_files": len(before),
        "after_files": len(after),
        "before_atoms": sum(old.values()),
        "after_atoms": sum(new.values()),
        "missing": [old_locations[k] for k in missing],
        "added": [new_locations[k] for k in added],
        "pass": not missing and not added,
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))
    raise SystemExit(0 if report["pass"] else 1)


if __name__ == "__main__":
    main()
