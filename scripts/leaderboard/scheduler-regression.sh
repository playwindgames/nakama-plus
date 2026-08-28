#!/usr/bin/env bash
# 排行榜调度器回归套件 —— 单一入口。
#
# 为什么需要它：跑全套此前要记住 `-vet=off`、`-race`、重复次数、`-run` 过滤、
# 以及突变检查怎么跑 —— 这些散在 patches/README.md 与 openspec 的 tasks 正文里。
# 「散在文档里的步骤」等于「没人会完整跑一遍」。
#
# 用法：
#   scheduler-regression.sh <已套用补丁的 nakama-plus 工作区> [--mutate] [--quick]
#
#   --mutate   额外跑突变检查：逐个移除修复，确认对应测试确实会红。
#              **普通测试跑绿说明不了测试还有牙** —— 这一步才说明。
#              耗时约 1~2 分钟。
#   --quick    只跑一遍、不加 -race（本地快速回环用，不可作为门禁）
#
# 步骤 2（锁序）需要 tools/check-lock-order.go 与本脚本同目录。
# 它自己的自检在 tools/check-lock-order-selftest.sh —— 那一条不在本套件里，
# 因为它验的是**检查器**，不是被测代码。
#
# 环境变量：
#   GO_BIN       go 可执行路径（默认 go）
#   RUNS         重复次数（默认 3）
#   TEST_DB_URL  可选。设了就**额外**跑需要真实数据库的三条用例
#                （invokeCallback 里 tournament 的两个分支跑裸 SQL，无法 mock）。
#                不设则那三条自动 t.Skip —— 套件默认保持「无 DB、无 docker、无网络」。
#
#                建库（一次性，用任意一个可写的 cockroach/postgres）：
#                  CREATE DATABASE lbschedtest;
#                  -- 再建一张与生产同构的 leaderboard 表
#                  --（DDL 取自 SHOW CREATE TABLE leaderboard）
#                然后：
#                  TEST_DB_URL='postgresql://root@127.0.0.1:27257/lbschedtest?sslmode=disable' \
#                    scheduler-regression.sh <工作区>
#
# ⚠️ 请指向 worktree 或分支，不要用 main 的工作区 —— --mutate 会反复改写源文件。

set -euo pipefail

WT="${1:-}"
if [[ -z "$WT" || ! -f "$WT/server/leaderboard_scheduler.go" ]]; then
  echo "用法: $(basename "$0") <已套用补丁的 nakama-plus 工作区> [--mutate] [--quick]" >&2
  exit 2
fi
shift

DO_MUTATE=0
QUICK=0
for arg in "$@"; do
  case "$arg" in
    --mutate) DO_MUTATE=1 ;;
    --quick)  QUICK=1 ;;
    *) echo "未知参数: $arg" >&2; exit 2 ;;
  esac
done

GO_BIN="${GO_BIN:-go}"
RUNS="${RUNS:-3}"
# ⚠️ 必须同时匹配 TestNewLocalLeaderboardScheduler（构造函数那组）——
#    只写 TestLeaderboardScheduler 会漏掉它们，覆盖率报告里会凭空多出两个 0% 函数。
TESTS='TestLeaderboardScheduler|TestNewLocalLeaderboard|TestTournament|TestLocalLeaderboardRankCache'
COVER_TESTS='TestLeaderboardScheduler|TestNewLocalLeaderboard'
# 单次执行的时间预算。spec 把「秒级完成」定为 SHOULD，并要求
# 「要成为硬要求就必须在入口里落成断言」—— 这里就是那个断言。
BUDGET_SEC="${BUDGET_SEC:-30}"
# 调度器 Update() 的调用点数量。写死是有意的：检查器**找不到东西**和
# **代码没问题**在退出码上长得一样，钉住数量才能让「解析悄悄失效」变成红灯。
# 新增合法调用点时，确认它不持锁，然后把这个数和 design.md D3 一起改。
# 3.32 时是 12；3.40 重写调度器 + 我方集群加法后为 18。
# 这是一道「新增调用点不得悄悄溜进来」的哨兵：数字对不上就红，
# 逼人去确认新调用点是否持锁，而不是默认它安全。
EXPECT_CALL_SITES="${EXPECT_CALL_SITES:-18}"

TOOLS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fail=0

echo "调度器回归套件"
echo "  工作区   $WT"
echo "  go       $($GO_BIN version | awk '{print $3}')"
if [[ -n "${TEST_DB_URL:-}" ]]; then
  echo "  真库     已配置 —— tournament 的裸 SQL 分支会被覆盖"
else
  echo "  真库     未配置 —— invokeCallback 的 tournament 分支将 skip（设 TEST_DB_URL 开启）"
fi
echo

# ── 1. 编译 ────────────────────────────────────────────────────────────────
echo "[1/5] 编译 server 包"
if ! (cd "$WT" && $GO_BIN build ./server/ 2>&1); then
  echo "  ❌ 编译失败"; exit 1
fi
echo "  ✅"
echo


# ── 2. 锁序 ────────────────────────────────────────────────────────────────
# design.md D3 的调用点审计原本是**人工**的，会腐化 —— leaderboard_cache.go 里
# 就是 `l.Unlock()` 紧跟一行 `scheduler.Update()`，离破坏只差一次「顺手改成 defer」。
# 这一步把那份清单变成可执行的。它是静态的、秒级、无依赖，所以放在测试之前。
#
# 同一步还顺带断言 main.go 里 Start() 仍夹在 NewRuntime 与 NewLocalPeer 之间
# —— D14 选 B（ls.started 保持裸 bool）的前提就是这个顺序，见 tasks 5.6。
# ⚠️ -main 必须**显式**传：不传的话检查器只会打印一行「未检查」然后照样退出 0，
# 而 main.go 的启动顺序是 design.md D14 选 B 之后 ls.started 唯一的探测手段。
echo "[2/5] 锁序（持锁时不得调用 Update()）+ main.go 启动顺序"
if ! $GO_BIN run "$TOOLS_DIR/check-lock-order.go" -expect "$EXPECT_CALL_SITES" \
  -main "$WT/main.go" "$WT/server"; then
  echo "  ❌ 锁序检查未通过"
  fail=1
fi
echo

# ── 2. 测试 ────────────────────────────────────────────────────────────────
# ⚠️ -vet=off 不是我们的怪癖：fork 既有代码 runtime_go_nakama.go:4543 会触发
#    Go 1.25 新增的 impossible type assertion 检查，与本补丁无关，go build 不受影响。
#    上游自己的 docker-compose-tests.yml 用的也是 `go test -vet=off -v -race ./...`。
if [[ $QUICK -eq 1 ]]; then
  echo "[3/5] 测试（--quick：单次、无 -race，**不可作为门禁**）"
  RACE=""; N=1
else
  echo "[3/5] 测试（-race，重复 $RUNS 次）"
  RACE="-race"; N="$RUNS"
fi
# 先空跑一次把测试二进制编出来（-run '^$' 匹配不到任何用例）——
# 否则计时把**编译**也算进去，而 -race 编译整个 server 包要几十秒，
# 「单次执行」的数字就成了编译耗时的函数，测的不是它声称要测的东西。
(cd "$WT" && $GO_BIN test -vet=off $RACE ./server/ -run '^$' -count=1 >/dev/null 2>&1) || true
start=$(date +%s)
if ! (cd "$WT" && $GO_BIN test -vet=off $RACE ./server/ -run "$TESTS" -count="$N" 2>&1 | tail -3); then
  echo "  ❌ 测试失败"; fail=1
fi
elapsed=$(( $(date +%s) - start ))
per_run=$(( elapsed / N ))
echo "  用时 ${elapsed}s（单次约 ${per_run}s，预算 ${BUDGET_SEC}s）"
if (( per_run > BUDGET_SEC )); then
  echo "  ❌ 单次执行超出时间预算 —— 这一层测试的前提是「秒级、可当门禁」，超了就得查"
  fail=1
else
  echo "  ✅"
fi
echo

# ── 3. 覆盖盲区 ────────────────────────────────────────────────────────────
# 不设门槛，只要求「知道盲区在哪」——「以为覆盖了、其实整条分支没走到」
# 比「知道没覆盖」更危险（2.17 那个缺口就是靠读代码而不是靠覆盖率发现的）。
echo "[4/5] 覆盖盲区（不设门槛，只报告）"
cov="$(mktemp)"
if (cd "$WT" && $GO_BIN test -vet=off ./server/ -run "$COVER_TESTS" \
      -count=1 -coverprofile="$cov" >/dev/null 2>&1); then
  (cd "$WT" && $GO_BIN tool cover -func="$cov" 2>/dev/null) \
    | grep 'leaderboard_scheduler.go' \
    | awk '{ printf "  %-34s %s\n", $2, $3 }'
  # ⚠️ 不能用 `grep -c '0.0%$'` —— 那会把 90.0% 和 100.0% 也算进去（结尾恰好是 0.0%）。
  #    用 awk 取最后一列做精确比较。
  zero=$( (cd "$WT" && $GO_BIN tool cover -func="$cov" 2>/dev/null) \
    | grep 'leaderboard_scheduler.go' | awk '$NF == "0.0%"' | wc -l )
  echo "  —— 其中 $zero 个函数为 0%（已知盲区见 openspec 的 design.md Open Questions）"
else
  echo "  ⚠️ 覆盖率采集失败，跳过"
fi
rm -f "$cov"
echo

# ── 4. 突变检查 ────────────────────────────────────────────────────────────
echo "[5/5] 突变检查"
if [[ $DO_MUTATE -eq 1 ]]; then
  # 🔴 换基底到 3.40 后改调 mutate-scheduler-3.40.sh。
  #    原 mutate-scheduler-fixes.py 的十二项针对 3.32 的两 timer 架构与 01/02/04 补丁，
  #    那些代码在 3.40 已不存在 —— 继续调它会「什么都没突变到」却报通过。
  if (cd "$WT" && bash scripts/leaderboard/mutate-scheduler-3.40.sh); then
    :
  else
    echo "  ❌ 有修复没有被测试守住"; fail=1
  fi
else
  echo "  ⏭  跳过（加 --mutate 开启）"
  echo "     ⚠️ 普通测试跑绿只说明「当前代码没坏」，说明不了「测试还有牙」。"
  echo "        门禁里应当包含这一步 —— 否则测试会慢慢腐化成永远绿的摆设。"
fi
echo

if (( fail )); then
  echo "❌ 回归套件未通过"
  exit 1
fi
echo "✅ 回归套件通过"
