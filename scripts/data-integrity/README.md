# 升级数据完整性检查器

回答一个问题:**这次服务端升级，有没有动到不该动的数据？**

## 用法

```bash
DSN="docker exec <db-容器> ./cockroach sql --insecure -d nakama --format=csv"

# ① 升级前：取快照 + 执行断言的 before 侧查询
python3 check.py snapshot --dsn-cmd "$DSN" \
    --tables tables.nkmad.yaml --migrations migrations.3.40.yaml -o before.json

# ② 执行升级（migrate up）

# ③ 升级后：比对
python3 check.py compare --dsn-cmd "$DSN" \
    --tables tables.nkmad.yaml --migrations migrations.3.40.yaml --before before.json

# ④ 若做了回滚演练：往返模式 —— 断言「回到了 before 的样子」
python3 check.py compare --dsn-cmd "$DSN" \
    --tables tables.nkmad.yaml --migrations migrations.3.40.yaml \
    --before before.json --round-trip
```

退出码 0 = 通过，1 = 有硬门禁失败。

### 完整流程（一次升级演练）

```
起基线版本 → 铺数据（e2e + seed）→ 🔴 跑阴性对照 → 重取 before 快照
   → migrate up → 换镜像 → compare（升级方向）
   → 导出会被 Down 删掉的三张表 → migrate down --limit N → 换回旧镜像
   → compare --round-trip（回滚方向）
```

⚠️ **阴性对照会改一行数据 ⇒ 跑完必须重新取 `before.json`。**

⚠️ **`migrate down` 默认 `--limit 1`**，回滚 3 条要显式写 `--limit 3`。

⚠️ **换镜像后先核对版本再往下**（`nakama --version`）——
`up -d` 不带 `--build` 时 Dockerfile 改了也不会生效，踩过。

### 项目配置的状态

| 文件                | 真库校验过                              |
| ------------------- | --------------------------------------- |
| `tables.nkmad.yaml` | ✅ 2026-08-31                           |
| `tables.nkmfd.yaml` | ❌ **首次运行要当成配置校验，不是判决** |
| `tables.nkmwd.yaml` | ❌ 同上                                 |

未校验的那两份，`must_be_empty` 全部是「服务端零调用」推出的假设。

## 每次新升级要改什么

| 文件                     | 改什么                                   |
| ------------------------ | ---------------------------------------- |
| `tables.<项目>.yaml`     | 新表分层；新迁移建的表进 `newly_created` |
| `migrations.<版本>.yaml` | 每条**改既有数据**的迁移写一组断言       |

断言的来源必须是**上游意图**（PR / commit message / 文档），
不是迁移 SQL —— 从 SQL 反推断言等于用实现验证实现，bug 会自洽通过。

## 三档判据

| 档                          | 含义                         | 结果   |
| --------------------------- | ---------------------------- | ------ |
| 业务/框架表逐行相同         | 升级不该动这些数据           | 硬门禁 |
| 仅软列（`update_time`）变化 | 有东西写过这些行，需解释     | 告警   |
| L-恒空表非空                | 存在我们不知道的写入路径     | 硬门禁 |
| L-必变表没变                | 迁移没生效                   | 硬门禁 |
| L-新建表没建出来            | 迁移没生效                   | 硬门禁 |
| 往返后没回到原样            | 迁移不可逆（`--round-trip`） | 硬门禁 |

## 🔴 结论的限度

**本地跑绿 ≠ 生产数据安全。** 合成数据的形态由我们的想象决定 ——
生产库里有我们没想到的形态（超长字段、历史遗留的空值、
早期版本写入的异常行），这些形态下的行为没有被覆盖。

其余已知盲区：

- **静态扫描只看得见服务端 runtime 调用。** 客户端可以直连 nakama 内建的
  HTTP/socket API 写表（好友、IAP subscription 等）。所以 `must_be_empty`
  是**待证伪的假设**而不是证明 —— 它报「非空」时，结论是
  「发现了未知写入路径」，不是「检查器坏了」。
- **断言只覆盖 yaml 里写下来的那几条。** 没写下来的不变量，工具不知道它存在。
- **真实数据里有 ~1MB 的单字段。** `storage` 的 `config-<版本>` 是服务端启动时
  从 CMS 拉下来缓存的游戏配置（`src/system/system.ts`），实测 998,567 字符 ——
  **每个环境都有，生产也有**，ad / fd / wd 三个项目都是这个模式。
  `check.py` 顶部的 `csv.field_size_limit(sys.maxsize)` 就是为它加的：
  Python `csv` 默认单字段上限 128KB，没有那一行 `snapshot` 会在
  「升级前取基线」这一步直接抛异常。
- **CockroachDB 的 JSONB 会规范化键顺序**：写入 `{"hp":1,"gold":1}`，
  读出 `{"gold": 1, "hp": 1}` ⇒ 「键顺序变化」本方法看不见。
  所幸那不是真的数据变化。
- **关键字扫描不是 AST 分析**：`scan_project.py` / `scan_framework.py` 的输出是
  **候选**，不是结论。间接调用（`const f = nk.storageWrite; f(...)`）看不见。
  ⇒ `nk.* API -> 表` 的映射必须人工确认。

## 阴性对照

```bash
./selftest.sh "docker exec <db-容器> ./cockroach sql --insecure -d nakama --format=csv"
```

🔴 **它不通过 ⇒ 本次比对结果作废**，不要读 `compare` 的结论。

它做三件事，缺一不可：

1. 改一行，`compare` 必须报红
2. **报红时必须点名 `storage`** —— 同库两次快照，`console_user` 必然触发
   「L-必变 却没变」⇒ 退出码恒为非 0。只看退出码的话，
   `compare` 完全坏掉自检也会显示绿色（已用两种改坏方式验证过）
3. 还原后 `storage` 不再有差异 —— 证明那次红只来自注入

跑一次约 80 秒（60 次 `docker exec` 取数）。**被打断会在库里留下注入**，
下次运行会自动清理并提示。

## 已实测的发现（3.40，2026-08-31）

### ACL 迁移在回滚方向不可逆

`20250926112031-console-fine-grained-acl` 的 Up/Down 是**有损往返**：

| 升级前 `role` | 升级后 `acl`       | 回滚后 `role` |         |
| ------------- | ------------------ | ------------- | ------- |
| 1 admin       | `{"admin": true}`  | **1**         | ✅ 还原 |
| 2 developer   | `{"admin": false}` | **4**         | ❌ 降级 |
| 3 maintainer  | `{"admin": false}` | **4**         | ❌ 降级 |
| 4 readonly    | `{"admin": false}` | 4             | ✅      |

原因：Up 把 role 2/3/4 一律压成 `admin:false`，信息在此丢失；
Down 只能把 `admin:false` 一律还原成 4。

⇒ **回滚后必须人工重设非 admin 控制台账号的角色。**
`--round-trip` 会报出这一条。

### spec D6 对 reverse 断言的论证是错的

D6 说「只查 forward 会漏掉『把所有人都设成 admin』」。**实测不成立**：
forward 用的是 `same_set` 而不是 `subset`，正向自己就抓住了（已注入验证）。
在这条迁移里 forward / reverse 的谓词互为补集，reverse 是冗余的。

真正没被覆盖的是**往返可逆性** —— 由 `--round-trip` 补上。
reverse 断言保留，因为下一条迁移的谓词未必互补。
