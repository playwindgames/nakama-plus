"""检查器自测。

🔴 snapshot 的返回形态在 Task 4 调整过：
   {table: {'header': [...], 'rows': [...]}}，表不存在时值为 None。
   理由：软列要按**列名**匹配（before 在 3.29.3、after 在 3.40，列序不同），
   且「表不存在」必须与「表是空的」区分开 —— 否则掩盖「表被删了」。
"""
from check import snapshot, compare, check_migration


def test_snapshot_returns_header_and_sorted_rows():
    fake = ['python3', '-c', 'print("k,v"); print("b,2"); print("a,1")']
    got = snapshot(fake, ['t'])
    assert got['t']['header'] == ['k', 'v'], got
    assert got['t']['rows'] == [['a', '1'], ['b', '2']], got


def test_snapshot_missing_table_is_none_not_empty():
    """表不存在 -> None。不能返回 []，那会和「空表」混淆。"""
    fake = ['python3', '-c',
            'import sys; sys.stderr.write("ERROR: relation \\"t\\" does not exist"); sys.exit(1)']
    assert snapshot(fake, ['t'])['t'] is None


def _tbl(header, rows):
    return {'header': header, 'rows': rows}


def test_business_field_change_is_failure():
    before = {'storage': _tbl(['k', 'v'], [['a', '1'], ['b', '2']])}
    after = {'storage': _tbl(['k', 'v'], [['a', '9'], ['b', '2']])}
    r = compare(before, after, {'business': ['storage']}, {'*': ['update_time']})
    assert r.failures, '业务字段变化必须是硬门禁'


def test_only_update_time_change_is_warning_not_failure():
    before = {'storage': _tbl(['k', 'update_time'], [['a', 'T1']])}
    after = {'storage': _tbl(['k', 'update_time'], [['a', 'T2']])}
    r = compare(before, after, {'business': ['storage']}, {'*': ['update_time']})
    assert not r.failures, '只有 update_time 变，不该是硬门禁'
    assert r.warnings, '但必须报告 —— 有东西写过这些行'


def test_create_time_change_is_failure():
    """create_time 对既有行不该变 —— 它不是软列。"""
    before = {'storage': _tbl(['k', 'create_time'], [['a', 'T1']])}
    after = {'storage': _tbl(['k', 'create_time'], [['a', 'T2']])}
    r = compare(before, after, {'business': ['storage']}, {'*': ['update_time']})
    assert r.failures, 'create_time 变化必须是硬门禁'


def test_must_be_empty_nonempty_is_failure():
    before = {'subscription': _tbl(['k'], [])}
    after = {'subscription': _tbl(['k'], [['x']])}
    r = compare(before, after, {'must_be_empty': ['subscription']}, {})
    assert r.failures, 'L-恒空 表非空必须是硬门禁'


def test_must_change_unchanged_is_failure():
    t = _tbl(['id', 'role'], [['a', '1']])
    r = compare({'console_user': t}, {'console_user': t}, {'must_change': ['console_user']}, {})
    assert r.failures, 'L-必变 没变必须是硬门禁'


def test_newly_created_missing_after_is_failure():
    """🔴 计划漏了这一档：newly_created 原本在 compare 里一次都没被引用，
    3 张表零检查，而 Task 3 的「覆盖 20 张」让它看起来是覆盖了的。"""
    r = compare({'users_notes': None}, {'users_notes': None},
                {'newly_created': ['users_notes']}, {})
    assert r.failures, '迁移该建的表没建出来，必须是硬门禁'


def test_business_table_vanished_is_failure():
    r = compare({'storage': _tbl(['k'], [['a']])}, {'storage': None},
                {'business': ['storage']}, {})
    assert r.failures, '业务表消失必须是硬门禁'


# ── 迁移断言 ────────────────────────────────────────────────
SPEC = {
    'table': 'console_user',
    'declared_columns': ['role', 'acl'],
    'conservation': {'row_count': 'equal'},
    'mapping': {
        'forward': {'before': 'Q_ROLE1', 'after': 'Q_ADMIN', 'expect': 'same_set'},
    },
    'untouched_columns_must_match': True,
}


def test_reverse_assertion_catches_over_granting():
    """只查正向会漏掉「把所有人都设成 admin」—— 反向必须抓住。"""
    spec = {'table': 'console_user', 'mapping': {
        'forward': {'before': 'Q_ROLE1', 'after': 'Q_ADMIN', 'expect': 'same_set'},
        'reverse': {'before': 'Q_NOTROLE1', 'after': 'Q_NOTADMIN', 'expect': 'same_set'}}}
    # 升级前 u1 是 role=1、u2 不是；升级后 u2 也成了 admin ← bug
    before_res = {'Q_ROLE1': [['u1']], 'Q_NOTROLE1': [['u2']]}
    after_res = {'Q_ADMIN': [['u1'], ['u2']], 'Q_NOTADMIN': []}
    fails = check_migration(spec, before_res, lambda sql: after_res[sql])
    assert fails, '反向断言必须抓住「多给了权限」'


def test_untouched_columns_change_is_failure():
    before = {'console_user': _tbl(['id', 'email', 'password', 'role', 'acl'],
                                   [['id1', 'a@x', 'pw-old', '1', '']])}
    after = {'console_user': _tbl(['id', 'email', 'password', 'role', 'acl'],
                                  [['id1', 'a@x', '', '1', '{}']])}  # password 被清空 ← bug
    fails = check_migration(SPEC, {'Q_ROLE1': []}, lambda sql: [],
                            before_snap=before, after_snap=after)
    assert any('声明外' in f for f in fails), f'password 被清空必须抓住: {fails}'


def test_untouched_columns_unchanged_passes():
    before = {'console_user': _tbl(['id', 'email', 'password', 'role', 'acl'],
                                   [['id1', 'a@x', 'pw', '1', '']])}
    after = {'console_user': _tbl(['id', 'email', 'password', 'role', 'acl'],
                                  [['id1', 'a@x', 'pw', '0', '{"admin":true}']])}
    fails = check_migration(SPEC, {'Q_ROLE1': []}, lambda sql: [],
                            before_snap=before, after_snap=after)
    assert not any('声明外' in f for f in fails), f'只动了声明列，不该报: {fails}'


# ── 往返模式 ────────────────────────────────────────────────
def test_round_trip_must_change_not_restored_is_failure():
    """3.40 的 ACL 迁移实测：role 2/3 -> 4，往返回不去。工具必须报出来。"""
    before = {'console_user': _tbl(['id', 'role'], [['a', '1'], ['b', '2']])}
    after = {'console_user': _tbl(['id', 'role'], [['a', '1'], ['b', '4']])}
    r = compare(before, after, {'must_change': ['console_user']}, {}, mode='round_trip')
    assert any('不可逆' in f for f in r.failures), f'往返没还原必须是硬门禁: {r.failures}'


def test_round_trip_restored_passes():
    t = _tbl(['id', 'role'], [['a', '1']])
    r = compare({'console_user': t}, {'console_user': t},
                {'must_change': ['console_user']}, {}, mode='round_trip')
    assert not r.failures, '往返还原了就该过'


def test_snapshot_handles_field_larger_than_csv_default_limit():
    """🔴 csv 默认单字段上限 128KB —— 生产的 storage.value 会超。

    2026-08-31 实测：storage 里有一条 998,567 字符的 `config-<版本>` ——
    服务端启动时从 CMS 拉下来缓存的游戏配置，**生产环境同样存在**。
    没有这一行 snapshot 直接抛 `field larger than field limit (131072)`。
    大字段在子进程里生成 —— 塞进命令行会 Argument list too long。
    """
    n = 200_000
    fake = ['python3', '-c', f'print("k,v"); print("a," + "y" * {n})']
    got = snapshot(fake, ['t'])
    assert got['t']['rows'] == [['a', 'y' * n]], '大字段必须能取回来'
