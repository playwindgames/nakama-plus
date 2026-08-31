# 升级数据完整性验证

回答两个问题：**这次 nakama 升级有没有动到不该动的数据**，
以及**该变的数据变得对不对**。

🔴 **手工调起的验证套件，不进 CI**（见 spec D9）。
与 `scripts/leaderboard/scheduler-regression.sh` 同类 ——
日常 PR 上没有「升级前后」这个概念，放进 CI 只会每次跑空。

**spec**：`nkmfd-backend/docs/superpowers/specs/2026-08-31-upgrade-data-integrity-design.md`

## 每次升级要改什么

**只有一份**：`migrations.<新版本>.yaml`。
其余配置只在「项目加功能」或「nakama 加表」时才动。

## 用法

```bash
# 1. 升级前取快照
python3 check.py snapshot --dsn-cmd '<取数命令>' --tables tables.nkmad.yaml -o before.json

# 2. 🔴 跑阴性对照，确认检查器本身有效
./selftest.sh '<取数命令>'

# 3. 升级（按 V5/V6 计划的 D10：先迁移、验完、再部署）

# 4. 比对
python3 check.py compare --before before.json --dsn-cmd '<取数命令>' \
    --tables tables.nkmad.yaml --migrations migrations.3.40.yaml
```

`<取数命令>` 例：

```
docker exec nkmad-cockroachdb ./cockroach sql --insecure -d nkmad_local --format=csv
```

## 判据分三档（spec D2）

| 档     | 含义                                   |
| ------ | -------------------------------------- |
| 🔴 失败 | 硬门禁：业务字段变化 / 行数变化 / L-恒空 表非空 / L-必变 没变 |
| ⚠️ 告警 | 仅时间戳变化 —— 说明有东西写过这些行，**必须逐条解释** |
| ✅ 通过 | 无失败也无告警                          |

## 🔴 结论的限度

合成数据的形态**由我们的想象决定**，生产可能有造不出来的形态
（历史遗留脏数据、早期版本写入的格式）。

⇒ **「本地验证通过」不等于「生产数据安全」。** 这句话要写进每次的结论。
