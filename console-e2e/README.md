# console-e2e —— console UI 的行为基线

nakama 3.29.3 → 3.40 把 console UI 从 Angular **整个重写成 Vue**，
而 UI 是**闭源预编译产物**：没有源码可读、没有单测可跑。
这里是第 4 层网 —— 唯一能回答「UI 有没有正确调用 API」的设施。

**设计与判据**：`nkmfd-backend` 仓 `docs/superpowers/specs/2026-09-04-console-e2e-design.md`
**实施计划**：同仓 `docs/superpowers/plans/2026-09-04-console-e2e.md`
**问题台账**：同仓 `docs/superpowers/specs/2026-08-27-nakama-340-issue-register.md` 的 F10 / F25 / F27 / F30

## 跑起来

```bash
npm ci
npx playwright install --with-deps chromium
npx playwright test              # globalSetup 会自己起 compose + seed，跑完销毁
```

排障时留着实例：`KEEP_STACK=1 npx playwright test`

## 两层判据

| 层 | 判什么 | 为什么这么判 |
| --- | --- | --- |
| 1 | 页面标题 | 便宜、稳。⚠️ 7 条玩家子路由**标题完全相同**，光靠标题分不开 |
| 2 | 调了哪些 endpoint + 请求体的**字段名** | 才是「UI 正确调用了 API」的证据。**不记字段值** —— 值一变快照就烂 |

写流程另有 4 条，断言分两种方向，**不是都断言「发了什么」**：

- `UpdateAccount` —— **反面**断言：**不得**发 `wallet` / `password`。
  ⚠️ 字段用的是 `google.protobuf.StringValue`，**字段缺席 = 不改**
  ⇒ 危险的是**多发**，不是漏发
- `WriteStorageObject` —— 正面断言：必须发 `permission_read` / `permission_write`
- `DeleteStorageObject` —— 路径必须是 **7 段**（OCC 变体，不绕过并发控制）
- `UpdateUser` ACL —— ACL key 必须匹配 `/^[A-Z][A-Z0-9_]*$/`。
  ⚠️ `acl.New()` 遇到不认识的 key **静默 `continue`** ⇒ 拼错不报错，只是不生效

## 三条别踩的

### 1. 写流程**无条件只连本地**

`lib/target.ts` 有两道闸：非本地要 `ALLOW_REMOTE_CONSOLE=yes-i-mean-it`；
生产特征（`prod` / `console-cluster` / `pwglab.com`）**无条件拒绝**。
🔴 而**写流程连 staging 也拒绝** —— 那个库四家共享。

### 2. `ALLOWED_EXTERNAL` 应当**一直保持空**

`lib/recorder.ts` 里它是空数组，这是 **CSP（F25）的常驻守卫**：
登录页原本会向 `heroiclabs.com` 拉 RSS，CSP 上线后被拦。
⚠️ **哪天有人往这个数组里加东西，等于 CSP 破了一个口子** —— 那是要评审的事，不是改测试。

### 3. 红了先分清是哪一类

⚠️ 触发是 `push` + `paths`（**没有定时**，见 F10-7 的理由）
⇒ 两次运行可能隔很久 ⇒ **套件会在这中间腐烂**（Playwright / 浏览器 / Node / compose 都会漂）。

**变红时的顺序**：

1. 翻 `test-results/login-failure.md` —— `globalSetup` 登录失败会把现场落在这里
   （URL、全部 `/v2/console/` 响应含响应体、控制台错误、nakama 服务端日志、截图、DOM）
2. 🔴 **F30 是已知的不稳定**：2026-09-07 同一份代码首跑红、原样重跑绿，**原因未定**。
   先看现场属于哪一类，**别直接当回归**
3. 快照不一致 ⇒ 先想「是 UI 真变了，还是我改了 seed / 路由表」

## 目录

| 文件 | 干什么 |
| --- | --- |
| `global-setup.ts` | 起 compose、跑 seed、登录取 storageState 与 Bearer、建 ACL 靶子账号、写 `fixtures.json`；**登录失败时转储现场** |
| `lib/target.ts` | 两道环境闸 |
| `lib/recorder.ts` | 录 endpoint 与字段名；归一化 UUID / storage version；`settle()` 等网络安静 |
| `lib/routes.ts` | 30 条路由表，含 9 条 skip 的**实测原因** |
| `tests/routes.spec.ts` | 层 1 + 层 2 + 登录页 |
| `tests/writes.spec.ts` | 4 条写流程 |
| `snapshots/` | 行为基线 |
