# 这个仓库是什么

> 本文件由 playwindgames 维护，**上游没有同名文件**，因此不会在同步上游时冲突。
> 最后更新 2026-09-03。

## 🔴 一句话：一个仓库里有两条【互不相干】的历史

```
main                    ← doublemo 血统。停在 v3.32.1，与下面那条【没有共同祖先】
port-3.40-20260827      ← 3.40 线。从 heroiclabs v3.40.0 起，我方在用的就是它
```

`git merge-base main port-3.40-20260827` ⇒ **空**。两条线永远合不到一起。

### 为什么会这样

`doublemo/nakama-plus` 是 **复制** heroiclabs/nakama 而来，**不是 GitHub fork**
—— 复制的那一刻整部上游历史就丢了。本仓 fork 自 doublemo，继承了这个状态。

2026-08 的 3.40 移植没有沿用那条线，而是**重新以 `heroiclabs/v3.40.0` 为基底**
把 doublemo 的集群 delta 三方套用上去。所以 3.40 线**带着完整的上游历史**。

⇒ 🔵 **有正经上游血统的是 `port-3.40-20260827`，`main` 才是那个无历史的复制品。**
「`main` 落后 1800+ 个提交」不是说我方改了 1800 次 —— 那是 heroiclabs 自
`Initial public release.` 以来的全部历史，我方自己的改动只有 **35 个**。

## 该用哪条

| 用途 | 用什么 |
| --- | --- |
| **开发 / 取产物** | 分支 **`port-3.40-20260827`**（3.40 线的持续分支） |
| 历史溯源 | tag `3.40-L0` … `3.40-L6`（补丁栈分层）、`3.40-port-20260827`（P-A 交付锚点） |
| ~~`main`~~ | 🔴 **不要基于它开发**。它是 fork 关系的遗留，保留仅为溯源 |

⚠️ **不要拿 `3.40-port-20260827` 这个 tag 直接部署** —— 它不含后续的
console 修复（`{{nt}}` 注入、重置密码改 `crypto/rand`）。**取物以持续分支为准。**

## 补丁栈 L0~L6

3.40 线不是一坨改动，是分层的，每层一个 annotated tag：

| 层 | 内容 | 提交数 |
| --- | --- | --- |
| `3.40-L0` | **就是 heroiclabs `v3.40.0` 本身**（同一个 commit），全部上游历史由此继承 | 1829 |
| `3.40-L1` | 模块路径重写 + 生成物重生成 | 1 |
| `3.40-L2` | 集群 / peer 能力（纯加法，861 文件） | 1 |
| `3.40-L3` | 三方套用 —— **首个能编译的层** | 1 |
| `3.40-L4` | console 三个字段（900 号段） | 1 |
| `3.40-L5` | 调度器集群钩子 `fnCanRun` —— **唯一的真开发层** | 5 |
| `3.40-L6` | 修复 heroiclabs#2538 | 1 |

⚠️ **不得从 `L1` / `L2` 取产物** —— 那两层的中间产物既不能编译也不能运行。

## 怎么同步上游

🔵 **能同步 —— 这是 3.40 移植最大的收益。** 因为 `L0` 就是 `v3.40.0` 本身，
本线与 `heroiclabs/master` 有**真实的共同祖先**（`merge-base` = L0）。

```bash
git remote add heroiclabs git@github.com:heroiclabs/nakama.git   # 若尚未配置
git fetch heroiclabs --tags
git merge heroiclabs/vX.Y.Z        # 或 rebase —— 两者都可行
```

🔴 **预期会有冲突，且位置可预测**：集中在 doublemo 集群 delta 与上游 runtime
的交界处 —— `server/runtime.go` · `server/runtime_go*.go` · `go.mod` / `go.sum` /
`vendor/modules.txt`。2026-09-03 实测：合上游 3 个提交 ⇒ **6 个冲突**。

**这是自维护一棵树的永久成本。** doublemo 已于 2026-08-18 停更 ⇒
我方的集群 delta 不再增长，但上游会持续移动。

## 测试怎么跑

```bash
# 本地默认连 CockroachDB:26257；用 TEST_DB_URL 可指向别处
go test ./server/ -vet=off -count=1
```

- 🔴 **`-vet=off` 必须保留** —— 本仓有两条既有 `go vet` 错误挡着（其中一条是
  `nk.GetPeer()` 的类型断言，doublemo 自 v3.29.3 起就有）
- 🔴 `go build ./...` 在 `sample_go_module` 上会失败（Go 插件示例，`package main`
  但无 `func main()`）。判据用 `go build $(go list ./... | grep -v sample_go_module)`
- ⚠️ CI（`tests.yaml`）目前**只在 `pull_request` 触发**，而我方直推分支不开 PR
  ⇒ **CI 实际从未运行**。测试靠人跑

## 更详细的

移植过程、逐条问题登记、验证证据都在 `nkmfd-backend` 仓：

- 全景：`docs/superpowers/specs/2026-08-27-nakama-340-overview.md`
- 问题登记：`docs/superpowers/specs/2026-08-27-nakama-340-issue-register.md`
