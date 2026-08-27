#!/usr/bin/env bash
# L1：把上游模块路径重写为 doublemo fork 的路径。幂等，可重复执行。
#
# 两条映射缺一不可：
#   1. github.com/heroiclabs/nakama/v3     -> github.com/doublemo/nakama-plus/v3
#   2. github.com/heroiclabs/nakama-common -> github.com/doublemo/nakama-common
# 漏掉第 2 条，树里会同时存在两份 nakama-common，Go 视其为完全不同的类型。
#
# 🔴 三条排除规则：
#   - vendor/ 与 .git
#   - 任何含 protobuf `rawDesc` 的文件。描述符里的 go_package 是**带长度前缀**的
#     序列化数据，文本替换会让长度对不上 ⇒ go build 全过、进程一启动即
#     `panic: slice bounds out of range`。这类文件由 buf 重新生成。
#     ⚠️ 不能按文件名 glob 排除 —— apigrpc_grpc.pb.go 也以 .pb.go 结尾却不含
#     rawDesc，它**需要**被重写。判据只能是「文件里有没有 rawDesc」。
#   - 逐文件调用 sed：`xargs sed -i` 中途失败会静默跳过后面的文件。
set -euo pipefail

rewrite() {
  local old="$1" new="$2" n=0 skipped=0 files
  files=$(grep -rl "$old" --include='*.go' --include='go.mod' --include='*.proto' --include='buf.yaml' . \
            --exclude-dir=vendor --exclude-dir=.git || true)
  if [ -z "$files" ]; then
    echo "  ${old}：无需改写（已是目标路径）"
    return 0
  fi
  while IFS= read -r f; do
    if grep -q 'rawDesc' "$f" 2>/dev/null; then
      skipped=$((skipped+1)); continue
    fi
    sed -i "s|${old}|${new}|g" "$f"
    n=$((n+1))
  done <<< "$files"
  echo "  ${old}：改写 $n 个文件，跳过 $skipped 个生成物（由 buf 重新生成）"
}

rewrite 'github.com/heroiclabs/nakama-common' 'github.com/doublemo/nakama-common'
rewrite 'github.com/heroiclabs/nakama/v3'     'github.com/doublemo/nakama-plus/v3'
