#!/usr/bin/env bash
# 提取「我方产物相对上游的非机械删除」——即「上游有、我方没有」的搜索面。
#
# 为什么只看删除：我方相对上游的【加法】基本是有意的集群能力；
# 而「上游行为被静默丢掉」这一类风险，在 diff 里的唯一表现形式就是删除行。
# 台账 F8（{{nt}} 注入被丢）与 F12（gzip 前提失效的继承）都是这么被发现的。
#
#   ./scripts/divergence-scan.sh <上游ref> <我方ref> [输出文件]
#   ./scripts/divergence-scan.sh v3.40.0 '3.40-port-20260827^{commit}' surface.txt
set -euo pipefail

UP="${1:?用法: $0 <上游ref> <我方ref> [输出]}"
OURS="${2:?}"
OUT="${3:-surface.txt}"

TMP=$(mktemp); trap 'rm -f "$TMP" "$TMP.tagged"' EXIT

git diff "$UP" "$OURS" -- 'server/*.go' 'console/*.go' 'main.go' \
  ':(exclude)*.pb.go' ':(exclude)*.pb.gw.go' > "$TMP"

awk '
/^diff --git/{ f=$3; sub("a/","",f) }
/^@@/{ h=$0 }
{ print f "\t" h "\t" $0 }
' "$TMP" > "$TMP.tagged"

python3 - "$TMP.tagged" "$OUT" <<'PY'
import sys, re
src, out = sys.argv[1], sys.argv[2]
# 机械删除的判据：import 路径重写、空行、纯注释行。
# ⚠️ 这三条是【当前已知】的机械类别。若人工定性时发现仍有机械项漏进来，
#    回到这里补规则 —— 不要在下游手工跳过，否则下次重跑又会漏。
MECH = re.compile(r'heroiclabs/nakama|nakama-common')
hunks, order = {}, []
for line in open(src, encoding='utf-8', errors='replace'):
    parts = line.rstrip('\n').split('\t', 2)
    if len(parts) < 3:
        continue
    f, h, body = parts
    if not f or not h:
        continue
    key = (f, h)
    if key not in hunks:
        hunks[key] = {'lines': [], 'real_del': 0}
        order.append(key)
    hunks[key]['lines'].append(body)
    if body.startswith('-') and not body.startswith('---'):
        s = body[1:].strip()
        if s and not s.startswith('//') and not MECH.search(body):
            hunks[key]['real_del'] += 1

# 🔴 第二道滤器：纯重排。把该 hunk 的全部非机械删除与全部加法各自去空白后拼接，
#    若两串相等 ⇒ 内容一字未变，只是换行/别名/顺序变了 ⇒ 机械。
#    覆盖两种实测形态：① gofmt 把函数签名从两行并成一行；② import 去掉别名。
#    ⚠️ 判据是【拼接后相等】，不是「相似」——差一个字符都不会被滤掉。
def is_pure_reflow(h):
    def blob(pred):
        out = []
        for b in h['lines']:
            if pred(b):
                t = b[1:]
                if t.strip() and not t.strip().startswith('//') and not MECH.search(b):
                    out.append(re.sub(r'\s+', '', t))
        return ''.join(out)
    d = blob(lambda b: b.startswith('-') and not b.startswith('---'))
    a = blob(lambda b: b.startswith('+') and not b.startswith('+++'))
    return bool(d) and d == a

kept = [k for k in order if hunks[k]['real_del'] > 0 and not is_pure_reflow(hunks[k])]
with open(out, 'w', encoding='utf-8') as fh:
    fh.write(f"# 含非机械删除的 hunk: {len(kept)} 个"
             f"，分布在 {len({k[0] for k in kept})} 个文件\n\n")
    for f, h in kept:
        fh.write(f"=== {f} :: {h}\n")
        fh.write('\n'.join(hunks[(f, h)]['lines']))
        fh.write('\n\n')
print(f"{len(kept)} 个 hunk / {len({k[0] for k in kept})} 个文件 → {out}")
PY
