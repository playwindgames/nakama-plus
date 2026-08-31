"""扫 nakama 服务端自己会写哪些表。

🔴 这是关键字扫描，不是 AST 分析（spec D10）。
`scripts/leaderboard/check-lock-order.go` 用 go/ast 做真解析，本工具**刻意不这么做**
—— 完整分析「nk.* API → 写哪张表」的成本远超收益。
⇒ 输出是**候选**，必须人工确认。
"""
import pathlib
import re
import sys

import yaml

# 🔴 只匹配**大写**的 SQL 关键字。用 re.I 会把注释里的 "update the ..." 也吃进来
# —— 2026-08-31 实测：加了 re.I 后结果里混进 the/this/and/for 等 40+ 个英文单词。
PAT = re.compile(r'\b(?:INSERT\s+INTO|UPDATE)\s+([a-z_]+)')
# 注释行：以 // 开头（去空白后），整行跳过
COMMENT = re.compile(r'^\s*//')
# 已知假阳：SQL 里的临时别名与关键字（实测撞到过 t / success）
DENY = {'t', 'success', 'set'}


def scan(server_dir):
    hits = {}
    for p in sorted(pathlib.Path(server_dir).glob('*.go')):
        for line in p.read_text(errors='replace').splitlines():
            if COMMENT.match(line):
                continue
            for m in PAT.finditer(line):
                t = m.group(1).lower()
                if t in DENY:
                    continue
                hits.setdefault(t, set()).add(p.name)
    return hits


if __name__ == '__main__':
    hits = scan(sys.argv[1])
    print(yaml.safe_dump(
        {'framework_writes': {k: sorted(v)[:5] for k, v in sorted(hits.items())}},
        allow_unicode=True, sort_keys=False))
