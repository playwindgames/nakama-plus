"""升级数据完整性检查器。

用法见 README.md。核心是三件事：
  snapshot()        取全表快照（含表头，表不存在时为 None）
  compare()         按分层判据比对 before / after
  check_migration() 执行 spec D6 的三类不变量（守恒 / 双向映射 / 无越界修改）

🔴 结论的限度：本地跑绿 ≠ 生产数据安全。合成数据的形态由我们的想象决定。
"""
import argparse
import csv
import io
import json
import subprocess
import sys
from dataclasses import dataclass, field

import yaml

# 🔴 csv 默认单字段上限 128KB —— 生产的 storage.value 会超。
#    2026-08-31 踩过：e2e 铺完数据后 snapshot 直接抛
#    `_csv.Error: field larger than field limit (131072)`。
csv.field_size_limit(sys.maxsize)

# 表在这一侧不存在。必须与「表存在但是空的」区分开 —— 否则掩盖「表被删了」。
MISSING = None


def _run(cmd, sql=None):
    full = list(cmd) + (['-e', sql] if sql else [])
    r = subprocess.run(full, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"取数失败: {' '.join(full[:3])}… -> {r.stderr[:200]}")
    return r.stdout


def _rows(raw):
    return list(csv.reader(io.StringIO(raw)))


def snapshot(dsn_cmd, tables):
    """{table: {'header': [...], 'rows': [...]}}，表不存在时值为 MISSING。"""
    out = {}
    for t in tables:
        try:
            rows = _rows(_run(dsn_cmd, f'SELECT * FROM {t};'))
        except RuntimeError as e:
            if 'does not exist' in str(e) or 'undefined_table' in str(e).lower():
                out[t] = MISSING
                continue
            raise
        out[t] = {'header': rows[0] if rows else [], 'rows': sorted(rows[1:])}
    return out


def query_before(dsn_cmd, migrations):
    """升级**前**执行 mapping.*.before 的查询并存下结果。

    🔴 没有这一步，反向断言就只有 after 侧生效 —— 升级后 role 列已不存在，查不了。
    """
    out = {}
    for spec in (migrations.get('migrations_changing_data') or []):
        for m in spec.get('mapping', {}).values():
            sql = m.get('before')
            if sql and sql not in out:
                try:
                    out[sql] = _rows(_run(dsn_cmd, sql))[1:]
                except RuntimeError as e:
                    raise SystemExit(
                        f"🔴 断言的 before 查询跑不通：{sql}\n"
                        f"   {e}\n"
                        f"   —— 这条查询只能在**升级前**的库上执行。"
                        f"看起来这个库已经迁移过了？") from None
    return out


@dataclass
class Report:
    failures: list = field(default_factory=list)  # 硬门禁
    warnings: list = field(default_factory=list)  # 需逐条解释

    @property
    def ok(self):
        return not self.failures


def _soft(soft_columns, table):
    return set(soft_columns.get('*', [])) | set(soft_columns.get(table, []))


def _strip(tbl, soft):
    """按**列名**剔掉软列。不用下标 —— 两侧列序可能不同。"""
    keep = [i for i, c in enumerate(tbl['header']) if c not in soft]
    return sorted(tuple(r[i] for i in keep if i < len(r)) for r in tbl['rows'])


def compare(before, after, layers, soft_columns, mode='upgrade'):
    """mode='upgrade'   : after 是升级后的库
       mode='round_trip': after 是「升级再回滚」之后的库 —— 应当回到 before 的样子。

    🔴 round_trip 是 upgrade 抓不到的那一档：must_change 在升级方向只断言「变了」，
       往返方向要断言「变回来了」。3.40 的 ACL 迁移实测**过不了这一关**
       （role 2/3 -> 4，不可逆），这是工具的用途，不是工具的 bug。
    """
    r = Report()

    for t in layers.get('business', []) + layers.get('framework', []):
        b, a = before.get(t, MISSING), after.get(t, MISSING)
        if b is MISSING or a is MISSING:
            side = 'before' if b is MISSING else 'after'
            r.failures.append(f'{t}: 表在 {side} 侧不存在 —— 业务表不该凭空出现或消失')
            continue
        if b['header'] != a['header']:
            r.failures.append(f'{t}: 表结构变了 {b["header"]} -> {a["header"]}')
            continue
        if len(b['rows']) != len(a['rows']):
            r.failures.append(f'{t}: 行数 {len(b["rows"])} -> {len(a["rows"])}')
            continue
        soft = _soft(soft_columns, t)
        if _strip(b, soft) != _strip(a, soft):
            r.failures.append(f'{t}: 业务字段有差异')
        elif b['rows'] != a['rows']:
            r.warnings.append(f'{t}: 仅软列（{sorted(soft)}）变化 —— 说明有东西写过这些行，需解释')

    for t in layers.get('must_be_empty', []):
        a = after.get(t, MISSING)
        if a is MISSING:
            r.failures.append(f'{t}: L-恒空 的表不存在了')
        elif a['rows']:
            r.failures.append(f'{t}: L-恒空 却有 {len(a["rows"])} 行 —— 存在未知写入路径')

    for t in layers.get('must_change', []):
        b, a = before.get(t, MISSING), after.get(t, MISSING)
        if mode == 'round_trip':
            if b != a:
                r.failures.append(f'{t}: 往返后没有回到原样 —— 迁移不可逆')
        elif b == a:
            r.failures.append(f'{t}: L-必变 却没变')

    for t in layers.get('newly_created', []):
        b, a = before.get(t, MISSING), after.get(t, MISSING)
        if mode == 'round_trip':
            if a is not MISSING:
                r.warnings.append(f'{t}: 往返后 L-新建 的表仍在 —— Down 段没删干净')
        elif a is MISSING:
            r.failures.append(f'{t}: L-新建 的表没被迁移建出来')
        elif b is not MISSING:
            r.warnings.append(f'{t}: L-新建 的表在 before 侧就已存在 —— 分层可能过时')

    return r


def check_migration(spec, before_results, after_db, before_snap=None, after_snap=None):
    """执行 spec D6 的三类不变量，返回失败项列表。

    before_results: {sql: rows}，由升级前的 query_before() 产出
    after_db:       callable(sql) -> rows，在升级后的库上执行

    🔴 刻意不读迁移 SQL（spec D7）—— 断言的来源是上游意图，不是实现。
    """
    fails, t = [], spec['table']

    # ① 守恒律
    if spec.get('conservation', {}).get('row_count') == 'equal' and before_snap:
        b, a = before_snap.get(t), after_snap.get(t)
        if b and a and len(b['rows']) != len(a['rows']):
            fails.append(f'{t}: 行数不守恒 {len(b["rows"])} -> {len(a["rows"])}')

    # ② 映射的双向正确性
    for direction, m in spec.get('mapping', {}).items():
        if m.get('expect') != 'same_set':
            continue
        if m['before'] not in before_results:
            fails.append(f'{t}: {direction} 缺 before 侧结果 —— 升级前没跑 snapshot --migrations？')
            continue
        exp = {tuple(x) for x in before_results[m['before']]}
        got = {tuple(x) for x in after_db(m['after'])}
        if exp != got:
            fails.append(f'{t}: {direction} 断言不成立 —— '
                         f'仅 after 有 {sorted(got - exp)[:3]}，仅 before 有 {sorted(exp - got)[:3]}')

    # ③ 无越界修改
    if spec.get('untouched_columns_must_match') and before_snap:
        b, a = before_snap.get(t), after_snap.get(t)
        if b and a:
            declared = set(spec.get('declared_columns', []))
            kb = [i for i, c in enumerate(b['header']) if c not in declared]
            ka = [i for i, c in enumerate(a['header']) if c not in declared]
            nb = [b['header'][i] for i in kb]
            na = [a['header'][i] for i in ka]
            if nb != na:
                fails.append(f'{t}: 声明外的列集合变了 {nb} -> {na}')
            else:
                sb = sorted(tuple(r[i] for i in kb) for r in b['rows'])
                sa = sorted(tuple(r[i] for i in ka) for r in a['rows'])
                if sb != sa:
                    fails.append(f'{t}: 声明外的列发生变化（声称只动 {sorted(declared)}）')
    return fails


# ── CLI ────────────────────────────────────────────────────
def _db(dsn_cmd):
    return lambda sql: _rows(_run(dsn_cmd, sql))[1:]


def _all_tables(cfg):
    return [t for v in cfg['layers'].values() for t in v]


def main():
    ap = argparse.ArgumentParser(description='升级数据完整性检查')
    sub = ap.add_subparsers(dest='cmd', required=True)
    for name in ('snapshot', 'compare'):
        p = sub.add_parser(name)
        p.add_argument('--dsn-cmd', required=True, help='取数命令，空格分隔')
        p.add_argument('--tables', required=True, help='tables.<project>.yaml')
        p.add_argument('--migrations', help='migrations.<ver>.yaml')
        if name == 'snapshot':
            p.add_argument('-o', '--out', default='before.json')
        else:
            p.add_argument('--before', required=True)
            p.add_argument('--round-trip', action='store_true',
                           help='after 是「升级再回滚」后的库：断言回到了 before 的样子')
    a = ap.parse_args()

    cfg = yaml.safe_load(open(a.tables))
    tables = _all_tables(cfg)
    dsn = a.dsn_cmd.split()
    migs = yaml.safe_load(open(a.migrations)) if a.migrations else {}

    if a.cmd == 'snapshot':
        blob = {'snapshot': snapshot(dsn, tables),
                'before_results': query_before(dsn, migs) if migs else {}}
        out = a.out
        json.dump(blob, open(out, 'w'))
        n_missing = sum(1 for v in blob['snapshot'].values() if v is MISSING)
        print(f'已取 {len(tables)} 张表（{n_missing} 张不存在）'
              f'+ {len(blob["before_results"])} 条断言查询 -> {out}')
        return 0

    blob = json.load(open(a.before))
    before, before_results = blob['snapshot'], blob['before_results']
    after = snapshot(dsn, tables)
    mode = 'round_trip' if a.round_trip else 'upgrade'
    rep = compare(before, after, cfg['layers'], cfg.get('soft_columns', {}), mode)
    if mode == 'upgrade':
        for spec in (migs.get('migrations_changing_data') or []):
            rep.failures += check_migration(spec, before_results, _db(dsn), before, after)

    for f in rep.failures:
        print(f'🔴 失败: {f}')
    for w in rep.warnings:
        print(f'⚠️  告警（需解释）: {w}')
    if rep.ok and not rep.warnings:
        print('✅ 通过')
    print('\n🔴 限度：合成数据的形态由我们的想象决定 —— '
          '「本地验证通过」不等于「生产数据安全」。')
    return 0 if rep.ok else 1


if __name__ == '__main__':
    raise SystemExit(main())
