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
