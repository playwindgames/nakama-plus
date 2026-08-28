#!/usr/bin/env bash
# check-lock-order 的自检 —— 对检查器本身做的「突变检查」。
#
# 一个从没红过的检查器和一个 `exit 0` 的空脚本，在 CI 里长得一模一样。
# 这里用 testdata-lock-order/ 下的夹具把每一类形态都跑一遍，断言退出码与关键输出。
#
# 覆盖的形态：
#   ok-basic              先 Unlock 再调 / 完全不持锁              → 绿
#   ok-block-scope        锁在 if 块内配对，块外调用                → 绿
#   ok-closure-async      持锁区内把闭包交给 worker 池              → 绿，但盲区清单必须点名
#   ok-recursion          调用图有环、但环上没有 Update()           → 绿，且必须终止
#   bad-direct            Lock…Update…Unlock                        → 红
#   bad-defer             defer Unlock（词法版会判反的那类）        → 红
#   bad-branch            分支内取锁、分支内调用                    → 红
#   bad-transitive        持锁 → 一层之外是 Update()                 → 红
#   bad-transitive-deep   持锁 → 三层之外是 Update()                 → 红
#   bad-interface         持锁 → 接口分派 → 实现里 Update()          → 红
#   bad-recursion-reaches 环上挂着 Update()                          → 红（环不能吞掉真命中）
#   no-sites              一个调用点都找不到                        → 红（自检）
#
# main.go 启动顺序（testdata-mainorder/，见检查器顶部说明）：
#   ok                    Start() 夹在中间                          → 绿
#   bad-reordered         Start() 被挪到 NewLocalPeer 之后          → 红
#   bad-missing           三个锚点缺一个                            → 红（不能默默判过）
#   （不传 -main）        必须打印「未检查」，不能静默跳过
#
# 用法：./check-lock-order-selftest.sh [go 可执行文件路径]

set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GO="${1:-${GO:-go}}"
CHECKER="$HERE/check-lock-order.go"
DATA="$HERE/testdata-lock-order"
MAINDATA="$HERE/testdata-mainorder"

if ! command -v "$GO" >/dev/null 2>&1; then
  echo "✗ 找不到 go 可执行文件（试过 '$GO'）。用法：$0 [/path/to/go]" >&2
  exit 2
fi

pass=0
fail=0

# run_case <夹具目录名> <期望退出码> [必须出现的子串...]
run_case() {
  local name="$1" want="$2"
  shift 2
  local out rc
  out="$("$GO" run "$CHECKER" -v "$DATA/$name" 2>&1)"
  rc=$?

  local errs=()
  if [ "$rc" != "$want" ]; then
    errs+=("退出码 $rc，期望 $want")
  fi
  local needle
  for needle in "$@"; do
    if ! printf '%s' "$out" | grep -qF -- "$needle"; then
      errs+=("输出里找不到：$needle")
    fi
  done

  if [ ${#errs[@]} -eq 0 ]; then
    echo "  ✓ $name"
    pass=$((pass + 1))
  else
    echo "  ✗ $name"
    for e in "${errs[@]}"; do echo "      $e"; done
    echo "      ----- 实际输出 -----"
    printf '%s\n' "$out" | sed 's/^/      /'
    fail=$((fail + 1))
  fi
}

echo "check-lock-order 自检"
echo

echo "应当通过的形态："
run_case ok-basic 0 "调度器 Update() 调用点：2 个" "✅ 通过"
run_case ok-block-scope 0 "调度器 Update() 调用点：1 个" "✅ 通过"
# 闭包不继承锁 ⇒ 绿；但这条假设必须被点名，否则同步回调形态就是假阴。
run_case ok-closure-async 0 "✅ 通过" "必须人工确认它不是同步回调"
# 调用图有环 ⇒ 可达性传播必须终止。超时在 run_case 外层由 CI 的 timeout 兜底，
# 这里靠「它确实返回了退出码」来体现。
run_case ok-recursion 0 "✅ 通过"

echo
echo "应当变红的形态："
run_case bad-direct 1 "🔴 违规：1 处在持锁时调用了 Update()"
# 🔴 这条是整个工具存在的理由：词法版会把它判成绿的。
run_case bad-defer 1 "🔴 违规：2 处在持锁时调用了 Update()" "Lock + defer Unlock" "RLock + defer Unlock"
run_case bad-branch 1 "🔴 违规：1 处在持锁时调用了 Update()"
run_case bad-transitive 1 "跨函数（多层，被调方已解析）" "cacheStub.Delete"
# 🔴 升级前看不见的那一类：链条深度 ≥2。
run_case bad-transitive-deep 1 "经 3 层调用才到 Update()" "deep.a → deep.b → deep.c → Update()"
# 🔴 接口分派：声明成接口、实现里才 Update()。真实代码里 cache.Delete 就是这个形状。
run_case bad-interface 1 "localCache.Delete" "跨函数（多层，被调方已解析）"
# 🔴 环不能把真命中吞掉 —— 只验 ok-recursion 的话，
#    一个「遇环就整段放弃」的实现也能跑绿。
run_case bad-recursion-reaches 1 "node.pong" "Update()"
run_case no-sites 1 "自检失败：一个调度器 Update() 调用点都没找到"

echo
echo "main.go 启动顺序："
# 拿一个已知干净的锁序夹具当载体，只让 main.go 那一项变化。
run_main_case() {
  local name="$1" want="$2"
  shift 2
  local out rc
  out="$("$GO" run "$CHECKER" -main "$MAINDATA/$name/main.go" "$DATA/ok-basic" 2>&1)"
  rc=$?
  local errs=()
  [ "$rc" = "$want" ] || errs+=("退出码 $rc，期望 $want")
  local needle
  for needle in "$@"; do
    printf '%s' "$out" | grep -qF -- "$needle" || errs+=("输出里找不到：$needle")
  done
  if [ ${#errs[@]} -eq 0 ]; then
    echo "  ✓ main:$name"
    pass=$((pass + 1))
  else
    echo "  ✗ main:$name"
    for e in "${errs[@]}"; do echo "      $e"; done
    printf '%s\n' "$out" | sed 's/^/      /'
    fail=$((fail + 1))
  fi
}

run_main_case ok 0 "✅ Start() 仍夹在 NewRuntime 与 NewLocalPeer 之间"
# 🔴 这条是加这项检查的全部理由：D14 选 B 之后，只有它能发现这个改动。
run_main_case bad-reordered 1 "顺序不对" "main.go 的启动顺序不再保证"
# 锚点找不到 ≠ 顺序正确。默默判过是最危险的。
run_main_case bad-missing 1 "找不到 Start(...)" "请人工复查"

# 不传 -main 时必须**显式**说自己没检查 —— 静默跳过等于假绿。
run_case ok-basic 0 "⏭  未检查" "这不是「通过」"

echo
echo "-expect 的自检："
run_case_expect() {
  local out rc
  out="$("$GO" run "$CHECKER" -expect 99 "$DATA/ok-basic" 2>&1)"
  rc=$?
  if [ "$rc" = 1 ] && printf '%s' "$out" | grep -qF "期望 99 个调用点，实际 2 个"; then
    echo "  ✓ expect-mismatch"
    pass=$((pass + 1))
  else
    echo "  ✗ expect-mismatch（退出码 $rc）"
    printf '%s\n' "$out" | sed 's/^/      /'
    fail=$((fail + 1))
  fi
}
run_case_expect

echo
if [ "$fail" -ne 0 ]; then
  echo "✗ $fail 项失败，$pass 项通过"
  exit 1
fi
echo "✓ 全部 $pass 项通过"
