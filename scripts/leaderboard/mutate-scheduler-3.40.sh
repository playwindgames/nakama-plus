#!/usr/bin/env bash
# 逐个移除 L5/L6 的修复，确认对应测试确实会红。
# 普通测试跑绿说明不了测试还有牙 —— 这一步才说明。
#
# ⚠️ 原 tools/mutate-scheduler-fixes.py 的十二项对 3.40 不适用：它们针对 3.32 的
#    两 timer 架构与 01/02/04 补丁，那些代码在 3.40 已不存在。这里重挑了突变点。
set -euo pipefail

F=server/leaderboard_scheduler.go
GOMOD_DIR="${GOMOD_DIR:-/tmp/nkport-gomod}"
mkdir -p "$GOMOD_DIR"
RUN="docker run --rm --user $(id -u):$(id -g) -e HOME=/tmp -e GOPATH=/tmp/go \
  -e GOCACHE=/gocache -e GOMODCACHE=/gomod -e GOTOOLCHAIN=auto \
  -v $PWD:/src -v nkport-gocache:/gocache -v $GOMOD_DIR:/gomod -w /src golang:1.26"

# 前提：M3 用全文替换 return false，依赖它只有一处。
n=$(grep -cE '^\s*return false$' "$F")
if [ "$n" != "1" ]; then
  echo "❌ 前提不成立：'return false' 有 $n 处，全文替换会误伤。改用更精确的匹配再跑。"
  exit 1
fi

mutate() {   # $1=描述 $2=sed 表达式 $3=应变红的 -run 过滤
  cp "$F" /tmp/ls.orig
  sed -i "$2" "$F"
  if diff -q "$F" /tmp/ls.orig >/dev/null; then
    echo "❌ $1：sed 什么都没改到 —— 突变没发生，这一轮说明不了任何事"
    cp /tmp/ls.orig "$F"; exit 1
  fi
  if $RUN go test -vet=off -mod=vendor ./server/ -run "$3" >/dev/null 2>&1; then
    echo "❌ $1：突变后测试仍绿 —— 测试没牙"
    cp /tmp/ls.orig "$F"; exit 1
  fi
  echo "✅ $1：突变后变红"
  cp /tmp/ls.orig "$F"
}

mutate "M1 第一处 fnCanRun（普通榜 / tournament reset 路径）" \
  '/fnCanRun(callback.leaderboard.Id)/,+2d' 'InvokeCallbackRespectsCanRun$'

# ⚠️ M2 的目标测试不是 HashRingDispatchIsExclusive。
#    Task 7 的突变验证实证：移除第二处判定后它仍然绿 —— 它测的是 fnCanRun 的
#    派发分布，不经过 invokeCallback 的 tournament end 分支。
#    覆盖第二处的是 Task 7 专门补的 RespectsCanRunTournamentEnd。
mutate "M2 第二处 fnCanRun（tournament end 路径）" \
  '/fnCanRun(callback.id)/,+2d' 'RespectsCanRunTournamentEnd'

mutate "M3 fail-closed 反转（服务注册表取不到时应 return false）" \
  's/^\(\s*\)return false$/\1return true/' 'FnCanRunBranches'

# M4：#2538 的守卫。L6 套用后启用。
#
# 🔴 它同时承担 Task 11 Step 4 的「确认走到了目标分支」——
#    原计划用 grep 测试输出里的调度器日志来确认，实测恒为 0：P28 探针用的是
#    zap.NewNop()，Debug 日志根本不产生。突变才是可靠的手段：把守卫改回去，
#    S2 必须红；不红就说明 S2 根本没走到那个分支，是假绿。
mutate "M4 #2538 守卫（同时确认 S2 走到了目标分支）" \
  's/if expiry > 0 && nowUnix < expiry {/if expiry > 0 {/' 'EndedTournamentHides'

echo "全部突变点确认有效。"
