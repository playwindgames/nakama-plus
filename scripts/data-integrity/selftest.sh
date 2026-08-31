#!/usr/bin/env bash
# 阴性对照：故意改一行，检查器必须**因为这一行**报红。
#
# 🔴 本脚本不通过 ⇒ 本次比对结果作废，不要读它的结论。
#
# 🔴 只看退出码是不够的（计划初稿的缺陷）：同一个库前后两次快照，
#    console_user 必然触发「L-必变 却没变」⇒ 退出码恒为非 0。
#    那样的话，compare 完全坏掉自检也会显示绿色。
#    ⇒ 必须核对失败项里**确实点名了被改的那张表**。
#
# 🔵 注入用可逆的 JSONB 增删（加一个键，再减掉），不搬运原值 ——
#    种子数据里有 50KB 的 blob，把它塞进命令行会撑爆参数表。
set -uo pipefail
cd "$(dirname "$0")"
DSN="${1:?用法: selftest.sh '<取数命令>'}"
TABLES="${2:-tables.nkmad.yaml}"
D=$(mktemp -d); trap 'rm -rf "$D"' EXIT

# 不加 eval：DSN 里都是不含空格的简单词，直接展开即可，
# 加了 eval 反而会吃掉 SQL 里 JSON 的双引号。
sql() { $DSN -e "$1"; }

KEY=$(sql "SELECT key FROM storage WHERE collection='seedtest'
           AND length(value::STRING) < 500 ORDER BY key LIMIT 1;" | tail -1)
[ -n "$KEY" ] || { echo "🔴 没有可用的种子数据 —— 先跑 seed/seed.py"; exit 1; }

# 🔴 预清理：上一次跑到一半被打断（超时/Ctrl-C）会把注入留在库里。
#    留着的话，这一次的 `||` 注入变成幂等无变化 ⇒ 检查器「没检出」，
#    自检报红，而真正的原因是残留 —— 排查方向会被带偏一整轮。
RESIDUE=$(sql "SELECT count(*) FROM storage
               WHERE collection='seedtest' AND value ? '__selftest';" | tail -1)
if [ "${RESIDUE:-0}" != "0" ]; then
  echo "⚠️  清理上次残留的注入（$RESIDUE 行）—— 说明上一次跑被打断了"
  sql "UPDATE storage SET value = value - '__selftest'
       WHERE collection='seedtest' AND value ? '__selftest';" >/dev/null
fi

# 不传 --migrations：本自检验的是 compare 的数据比对能力。
# 传了反而会因为「缺 before 侧断言结果」报红 —— 那是假阳性，不是真检出。
python3 check.py snapshot --dsn-cmd "$DSN" --tables "$TABLES" -o "$D/a.json" >/dev/null

sql "UPDATE storage SET value = value || '{\"__selftest\":1}'::JSONB
     WHERE collection='seedtest' AND key='$KEY';" >/dev/null
OUT=$(python3 check.py compare --before "$D/a.json" --dsn-cmd "$DSN" --tables "$TABLES"); rc=$?

# 还原，无论成败
sql "UPDATE storage SET value = value - '__selftest'
     WHERE collection='seedtest' AND key='$KEY';" >/dev/null

if [ $rc -eq 0 ]; then
  echo "🔴 阴性对照失败：改了数据但检查器说通过 —— 比对方法失效"; exit 1
fi
if ! grep -q 'storage: 业务字段有差异' <<<"$OUT"; then
  echo "🔴 阴性对照失败：检查器报红了，但**没点名 storage** —— 是别的原因红的，不算检出"
  sed 's/^/     /' <<<"$OUT"; exit 1
fi

# 还原后 storage 不该再有差异 —— 证明刚才的红确实只来自注入
OUT2=$(python3 check.py compare --before "$D/a.json" --dsn-cmd "$DSN" --tables "$TABLES")
if grep -q 'storage' <<<"$OUT2"; then
  echo "🔴 阴性对照失败：还原后 storage 仍有差异 —— 注入没清干净，后续结论不可信"
  sed 's/^/     /' <<<"$OUT2"; exit 1
fi

echo "✅ 阴性对照通过：检查器能发现数据变化（注入点 storage/$KEY），且注入已清除"
