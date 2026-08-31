"""扫游戏侧用了哪些 nakama runtime API。

🔴 输出是**候选**，不是结论（spec D10）。
关键字匹配看不见间接调用（`const f = nk.storageWrite; f(...)`）、
动态构造的调用名、以及经过自家封装的间接层。
⇒ 「nk.* API → 写哪张表」的映射必须**人工确认**。
"""
import pathlib
import sys

import yaml

import re

API = re.compile(r'\bnk\.([A-Za-z][A-Za-z0-9_]*)')


def scan(src_dir):
    hits = {}
    root = pathlib.Path(src_dir)
    for p in sorted(root.rglob('*.ts')):
        for m in API.finditer(p.read_text(errors='replace')):
            hits.setdefault(m.group(1), set()).add(str(p.relative_to(root)))
    return {k: sorted(v)[:3] for k, v in sorted(hits.items())}


if __name__ == '__main__':
    print(yaml.safe_dump({'nk_api_used': scan(sys.argv[1])},
                         allow_unicode=True, sort_keys=False))
