# 补丁栈 · 以 heroiclabs v3.40.0 为基底

本目录是 **P-A 的交付物**：把 `doublemo/nakama-plus` 的集群能力重建在 heroiclabs
v3.40.0 之上，分层导出为可重放的补丁序列。

> 规格：`$NKMFD/docs/superpowers/specs/2026-08-27-nakama-340-port-a-design.md`
> 计划与执行记录：`$NKMFD/docs/superpowers/plans/2026-08-27-nakama-340-port-a.md`
> 问题台账：`$NKMFD/docs/superpowers/specs/2026-08-27-nakama-340-issue-register.md`

## 为什么是补丁栈而不是一次 rebase

`doublemo` 与 `heroiclabs` **没有共同历史** —— 是复制，不是 GitHub fork。四条独立佐证：
`merge-base` 为空；doublemo 根提交是 2024-06-27 的 `v2`；同名 tag `v3.32.1` 两边 sha 不同；
GitHub 跨仓 compare API 返回 404。

而且 **heroiclabs 的 3.40.0 是纯单机版**，集群能力只存在于 doublemo 那一层 ——
「以 3.40.0 为基底」拿到的是一棵没有集群的树，补丁栈因此必须存在。

## 层次

| 补丁                       | 内容                                              | 守卫                                        |
| -------------------------- | ------------------------------------------------- | ------------------------------------------- |
| `L1-module-path.patch`     | 模块路径重写（**两条映射**）+ 生成物由 buf 重生成 | 零残留 + 脚本幂等（**不是编译**）           |
| `L2-cluster-peer.patch`    | 集群 / peer 纯加法 34 个文件 + 集群依赖           | 零冲突 + 纯加法 + 自包含新增包可编译可测    |
| `L3-three-way.patch`       | 三方套用 + 4 个人判断文件                         | 🔴 **首个 `BUILD_OK` 的层** + 对照 L0 基线  |
| `L4-console-fields.patch`  | console 三字段（900+ fork 号段，不带 UI）         | 字段可读写 + 号位不与上游冲突 + 编译期守卫  |
| `L5-scheduler-hook.patch`  | 调度器集群钩子（`fnCanRun` + 两处插入点 + 观测）  | 行为测试 + 突变检查 M1~M3                   |
| `L6-upstream-2538.patch`   | 我方修复 `#2538`                                  | 哨兵 S2 + 突变检查 M4                       |
| `tooling-ci-gate.patch`    | 锁序检查器、回归套件、CI 门禁                     | 检查器自检 17 项                            |

⚠️ **`tooling-ci-gate.patch` 不是第七层。** L1~L6 是「把集群能力搬到新基底」的补丁栈，
下次升级要整套重放；工具链是搭车的基础设施，与基底版本无关。

🔴 **L1 与 L2 的中间产物既不能编译也不能运行** —— 这是结构性的，不是缺陷：
L1 之后树里用的是 doublemo 的 `nakama-common`，而它的 `runtime.NakamaModule` 比官方多
`GetPeer`、`Initializer` 多 `RegisterAfterAny`，实现它们的 `server/runtime_go.go` 属于 L3。
**任何人不得从 `3.40-L1` / `3.40-L2` 取产物。**

## 重放

```bash
git worktree add /tmp/replay v3.40.0
cd /tmp/replay
for p in L1-module-path L2-cluster-peer L3-three-way L4-console-fields \
         L5-scheduler-hook L6-upstream-2538 tooling-ci-gate; do
  git apply <本目录>/$p.patch || { echo "❌ $p"; break; }
done
```

已实测：**七份全部干净套用，结果与交付树逐文件一致。**

## 环境与版本（重放时必须一致）

- Go：基底要求 `go 1.26.5`，容器用 `golang:1.26` + `GOTOOLCHAIN=auto` + `-mod=vendor`
- 生成 protobuf 产物：**buf 1.72.0**；插件由 `build/tools.go` + `go.mod` 解析 ——
  `protoc-gen-go v1.36.11`、`protoc-gen-go-grpc 1.6.0`
- 🔴 docker 里以 `--user "$(id -u):$(id -g)"` 跑时，**三个路径都要重定向**：
  `GOCACHE`、`GOMODCACHE`、`GOPATH`（校验和数据库缓存在 `$GOPATH/pkg/sumdb`）
- 测试库：容器 `repro-cockroachdb`（cockroach v22.2.17），库 **`upstreambase`**
  （19 条 migration），端口 `127.0.0.1:27257` **仅绑回环**。
  ⚠️ 全程只用这一个库 —— 用窄 schema 的 `lbschedtest` 跑全套会报
  `relation "users" does not exist`，看起来像回归，实际是选错了库

## 实测记录

- **L0 上游测试基线（带库）**：`ok` 5 包 / 无测试 14 / `FAIL` 1；全仓 `Test` 函数 **249**
  （`server` 占 161）；唯一失败用例是 `internal/gopher-lua` 的 `TestLua`
- 🔵 **`TestLua` 是上游自带的红，不修** —— `_lua5.1-tests/libs/` 在 v3.40.0 的树里
  根本不存在（`git ls-tree` 实证），干净的上游 checkout 就是红的
- **L6 全量结果（对照 L0，判据是「不低于」）**：

  | | L0 | L6 |
  | --- | --- | --- |
  | `ok` 包 | 5 | **7** |
  | 无测试文件 | 14 | 19 |
  | `FAIL` 包 | 1 | 2 |
  | **失败用例** | **1** | **1** |
  | `Error pinging database` | 0 | 0 |

  L6 多出的 `FAIL` 包是 `internal/gocache/store/etcd`（跑满 600 秒超时，需真 etcd 服务，
  是 L2 带进来的新集成测试，**不是回归**）。**失败用例数没变，仍只有上游自带的 `TestLua`。**

- **3.40 探针**：`Form1` ✅ / `Q3` ✅ / 🔵 **`Offset` ❌ 而且红得对** ——
  该探针检验「错开边界的 `end_time` 仍会毒化 expiry」，而 `#2538` 的守卫恰恰消除了毒化。
  已做因果验证：移除守卫 → 探针转绿（毒化重现），装回去 → 探针红。
  ⚠️ **这个红记录的是「我方修好了裸 3.40 的行为」，不要去修它。**
- **双节点集成**：60 个榜同一边界到期，n1 执行 **37** / n2 执行 **23**，
  交集 **0**（不重）、差集 **0**（不漏）；n2 停机后 n1 单独执行新一批 **60/60 零遗漏**
- **哨兵**：`TestP28_S1/S2×2/S3` 在我方产物上 `-race -count=5` → **20 PASS / 0 FAIL**
- **突变检查**：M1~M4 五轮 15 次全部稳定变红

## 下次升级时

抽掉某一层后跑测试，若仍全绿，说明该层已被上游消化，可以删除。
本次即如此消化了三个自研补丁：`01` / `02` / `04`（见 spec D4 的 P28 实证）。

🔴 **但「自研补丁的测试红了」有两种含义，必须先定位守卫去哪了再决定移植与否：**
补丁真的没被消化，或者**消化了但换了层**。本次 `06`（claim-dedup）就是后者 ——
3.32 的去重在投递方法里按 `(id, ts)` 判重，3.40 的守卫在 `scheduleLoop` 里用
`lastFireUnix` 过滤。直接调 `processEndActive` 会绕过它、断言失败，
据此把补丁打回去就是给上游已解决的问题又加一层 fork 面积。
