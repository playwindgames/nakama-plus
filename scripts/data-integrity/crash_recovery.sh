#!/usr/bin/env bash
# 迁移中途崩溃 / 多节点并发迁移的恢复行为。
#
# 三条迁移里**有两条带显式 COMMIT**（CockroachDB 不允许事务块里做
# ALTER TABLE ... UPDATE），因此它们**不是原子的** —— 存在一个窗口：
# 第一段已落盘、migration_info 尚未写入。此时崩溃，sql-migrate 会认为
# 这条迁移没应用过，下次启动重跑它。
#
# 🔴 不用 docker kill 掐时间 —— 那个窗口是毫秒级，不可复现。
#    改为**等价构造**库状态：与真实崩溃在该点的结果完全一致。
#
# 🔴 不用 set -e：场景 C 的失败是**预期的**，-e 会让脚本在那里中止。
set -uo pipefail
cd "$(dirname "$0")"
DSN="${1:?用法: crash_recovery.sh '<取数命令>' <docker 网络名>}"
NET="${2:?}"
IMG=playwindgames/nakama-plus:3.40-port-20260827
ACL_ID='20250926112031-console-fine-grained-acl.sql'
IDX_ID='20260319134532-add-display-name-index.sql'

sql()  { $DSN -e "$1"; }
mig()  { docker run --rm --network "$NET" "$IMG" migrate "$@" --database.address root@db:26257 2>&1; }
show() { sql "SELECT username, $1 FROM console_user ORDER BY 1;" | sed 's/^/      /'; }

echo "═══════ 场景 A：ACL 迁移崩在 COMMIT 之后 ═══════"
echo "  构造：acl 列已加（全是默认 admin:false）、role 列仍在、记账未写"
sql "ALTER TABLE console_user ADD COLUMN IF NOT EXISTS acl JSONB NOT NULL DEFAULT '{\"admin\":false}'::JSONB;" >/dev/null
sql "DELETE FROM migration_info WHERE id = '$ACL_ID';" >/dev/null
echo "    崩溃点的 console_user:"; show "role, acl"
echo "  ── 重跑 migrate up ──"
mig up | grep -oE 'count":[0-9]*|"error":"[^"]{0,90}' | sed 's/^/    /'
echo "  ── 🔴 要害：adm 的权限还在不在 ──"; show "acl"

echo
echo "═══════ 场景 B：索引迁移崩在 COMMIT 之后 ═══════"
echo "  构造：pg_trgm 已装、索引已删、记账未写"
sql "DROP INDEX IF EXISTS users@users_display_name_idx;" >/dev/null 2>&1
sql "DELETE FROM migration_info WHERE id = '$IDX_ID';" >/dev/null
echo "  ── 重跑 migrate up ──"
mig up | grep -oE 'count":[0-9]*|"error":"[^"]{0,90}' | sed 's/^/    /'
echo -n "    索引: "
sql "SELECT indexname FROM pg_indexes WHERE tablename='users' AND indexname LIKE '%display_name%';" | tail -1

echo
echo "═══════ 场景 C：崩在第二段中间（role 已 DROP、记账未写）═══════"
echo "  🔴 理论上最坏：重跑会撞「role 列不存在」"
sql "DELETE FROM migration_info WHERE id = '$ACL_ID';" >/dev/null
out=$(mig up)
if grep -qiE '"error"|fatal' <<<"$out"; then
  echo "    ⚠️  重跑失败（符合预期），卡死形态："
  grep -oiE '(column|relation)[^"\\]{0,70}does not exist' <<<"$out" | head -1 | sed 's/^/        /'
  echo "    ── 人工救援：补写记账行 ──"
  sql "INSERT INTO migration_info (id, applied_at) VALUES ('$ACL_ID', now()) ON CONFLICT DO NOTHING;" >/dev/null
  mig up | grep -oE 'count":[0-9]*' | sed 's/^/        补记账后重跑: 应用 /'
  echo -n "        救援后 migration_info 行数: "; sql "SELECT count(*) FROM migration_info;" | tail -1
else
  echo "    ✅ 重跑居然成功了 —— sql-migrate 有我没想到的保护，记下来"
  echo "$out" | grep -o 'count":[0-9]*' | sed 's/^/        /'
fi

echo
echo "═══════ 场景 D：三个 migrate up 同时打同一个库 ═══════"
echo "  依据：migrate.go 无任何锁；生产每台机器各自 docker compose up"
sql "DELETE FROM migration_info WHERE id IN ('$ACL_ID','$IDX_ID','20251015150737-add-user-notes-audit-acl-templates.sql');" >/dev/null
sql "ALTER TABLE console_user ADD COLUMN IF NOT EXISTS role SMALLINT NOT NULL DEFAULT 4;" >/dev/null 2>&1
sql "UPDATE console_user SET role = CASE WHEN (acl->'admin')::BOOL = true THEN 1 ELSE 4 END;" >/dev/null 2>&1
sql "ALTER TABLE console_user DROP COLUMN IF EXISTS acl;" >/dev/null 2>&1
sql "DROP TABLE IF EXISTS users_notes, console_audit_log, console_acl_template;" >/dev/null 2>&1
sql "DROP INDEX IF EXISTS users@users_display_name_idx;" >/dev/null 2>&1
echo -n "    回到基线，migration_info = "; sql "SELECT count(*) FROM migration_info;" | tail -1
for i in 1 2 3; do mig up > "/tmp/mig-$i.log" 2>&1 & done
wait
for i in 1 2 3; do
  printf "    节点 %s: " "$i"
  r=$(grep -o 'count":[0-9]*' "/tmp/mig-$i.log" | tail -1)
  [ -n "$r" ] && echo "$r" || grep -oE '"error":"[^"]{0,80}' "/tmp/mig-$i.log" | head -1
done
echo "  ── 最终状态（这才是判据）──"
echo -n "    migration_info 行数（必须恰好 19）: "; sql "SELECT count(*) FROM migration_info;" | tail -1
echo -n "    有无重复记账: "; sql "SELECT count(*) FROM (SELECT id FROM migration_info GROUP BY id HAVING count(*) > 1);" | tail -1
echo "    🔴 adm 的权限:"; show "acl"
