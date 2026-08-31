"""种子数据 —— 走真实游戏 API 造数据，覆盖边界值。

🔵 走 API 而不直接 INSERT：直接插的数据不经过序列化路径，
   而**序列化差异正是升级最可能出问题的地方**。

⚠️ 不做「重」档（好友/公会/排行榜这类有业务语义的关联数据）——
   e2e 本来就会产生，手工再造是重复劳动。
"""
import argparse
import json
import subprocess

API = 'http://127.0.0.1:7350'
CONSOLE_PORT = 7351   # 控制台端口。造墓碑只能走这里，见 seed_tombstones 的说明


def curl(method, path, token=None, key=None, body=None, port=None):
    base = API if port is None else f'http://127.0.0.1:{port}'
    cmd = ['curl', '-s', '-X', method, f'{base}{path}',
           '-H', 'Content-Type: application/json']
    if token:
        cmd += ['-H', f'Authorization: Bearer {token}']
    if key:
        cmd += ['-u', f'{key}:']
    if body:
        cmd += ['-d', json.dumps(body)]
    return subprocess.run(cmd, capture_output=True, text=True).stdout


# 「中」档的边界值 —— 每一条都有针对性
CASES = [
    ('plain', {'hp': 100}),
    ('unicode', {'名字': '测试🎮', 'emoji': '🔥💧'}),
    ('deep', {'a': {'b': {'c': {'d': {'e': [1, 2, {'f': 'g'}]}}}}}),
    ('big', {'blob': 'x' * 50_000}),
    ('nulls', {'a': None, 'b': [], 'c': {}}),
    ('extremes', {'max': 9007199254740991, 'min': -9007199254740991, 'zero': 0}),
    ('specials', {'quote': 'he said "hi"', 'back': 'a\\b', 'nl': 'x\ny'}),
]
PERMS = [(0, 0), (1, 0), (1, 1), (2, 0), (2, 1)]  # 全部权限组合


def _auth(server_key, device_id):
    raw = curl('POST', '/v2/account/authenticate/device?create=true',
               key=server_key, body={'id': device_id})
    try:
        return json.loads(raw)['token']
    except (KeyError, json.JSONDecodeError):
        return None


def one_leaderboard_id(dsn_cmd):
    """随便取一个已存在的排行榜 id；没有就返回 None（说明 e2e 没跑过）"""
    out = subprocess.run(list(dsn_cmd) + ['-e', 'SELECT id FROM leaderboard LIMIT 1;'],
                         capture_output=True, text=True).stdout.strip().splitlines()
    return out[-1] if len(out) > 1 else None


def seed_leaderboard_records(server_key, dsn_cmd, n=5):
    lb = one_leaderboard_id(dsn_cmd)
    if not lb:
        return 0, ['leaderboard 表是空的 —— 先跑一轮 e2e 建榜']
    ok, fails = 0, []
    for i in range(n):
        tok = _auth(server_key, f'seed-lbr-{i:04d}')
        if not tok:
            fails.append(f'lbr-{i}: 认证失败')
            continue
        score = [0, 1, 9007199254740991, 42, 7][i % 5]   # 含边界值
        out = curl('POST', f'/v2/leaderboard/{lb}', token=tok,
                   body={'score': str(score), 'subscore': '0',
                         'metadata': json.dumps({'seed': i, 'note': '测试🎮'})})
        if 'ownerId' in out or 'owner_id' in out:
            ok += 1
        else:
            fails.append(f'lbr-{i}: {out[:120]}')
    return ok, fails


def seed_friends(server_key, n=4):
    """🔴 POST /v2/friend 的 ids 走 query 参数，不是 body（swagger 已确认）"""
    users = []
    for i in range(n):
        tok = _auth(server_key, f'seed-fr-{i:04d}')
        if not tok:
            continue
        uid = json.loads(curl('GET', '/v2/account', token=tok)).get('user', {}).get('id')
        users.append((tok, uid))
    ok, fails = 0, []
    for i, (tok, _) in enumerate(users):        # 每人加下一个人为好友，成环
        target = users[(i + 1) % len(users)][1]
        out = curl('POST', f'/v2/friend?ids={target}', token=tok)
        if out.strip() in ('{}', ''):
            ok += 1
        else:
            fails.append(f'fr-{i}: {out[:120]}')
    return ok, fails


def seed_tombstones(server_key, dsn_cmd, console=('username', 'password'), n=2):
    """造 user_tombstone。

    🔴 **公开的 `DELETE /v2/account` 造不出墓碑** —— 2026-08-31 实测：
       它返回成功，但表里一行都没有。原因在 `server/api_account.go:99`：
       公开入口把 `recorded` 硬编码成 `false`，而墓碑只在
       `recorded=true` 时才写（`server/core_account.go:796`）。

    只有三条路能写，其中两条对本项目不通：
      - 公开 API                  -> recorded 恒为 false，永不写
      - `nk.accountDeleteId(id, true)` -> ad/fd/wd **调用数均为 0**
      - 控制台 `DELETE /v2/console/account/{id}?record_deletion=true` -> ✅ 走这条

    ⚠️ 删号会同时减少 users 与 user_device 的行数 ——
       必须在取 before 快照**之前**跑完，否则 before/after 行数对不上。
    """
    ct = json.loads(curl('POST', '/v2/console/authenticate', port=CONSOLE_PORT,
                         body={'username': console[0], 'password': console[1]})
                    or '{}').get('token')
    if not ct:
        return 0, ['控制台认证失败 —— 检查 data/local.yml 的 console.username/password']
    ok, fails = 0, []
    for i in range(n):
        if not _auth(server_key, f'seed-tomb-{i:04d}-xxxx'):
            fails.append(f'tomb-{i}: 认证失败')
            continue
    raw = subprocess.run(
        list(dsn_cmd) + ['-e', "SELECT u.id FROM users u JOIN user_device d "
                               "ON d.user_id = u.id WHERE d.id LIKE 'seed-tomb-%';"],
        capture_output=True, text=True).stdout.strip().splitlines()[1:]
    for uid in raw[:n]:
        out = curl('DELETE', f'/v2/console/account/{uid}?record_deletion=true',
                   token=ct, port=CONSOLE_PORT)
        if out.strip() in ('{}', ''):
            ok += 1
        else:
            fails.append(f'tomb-{uid[:8]}: {out[:120]}')
    return ok, fails


def main(server_key, dsn_cmd=None):
    written, failed = 0, []
    for i, (name, value) in enumerate(CASES):
        tok = _auth(server_key, f'seed-device-{name}-{i:04d}')
        if not tok:
            failed.append(f'{name}: 认证失败')
            continue
        for r, w in PERMS:
            out = curl('PUT', '/v2/storage', token=tok, body={'objects': [{
                'collection': 'seedtest', 'key': f'{name}-r{r}w{w}',
                'value': json.dumps(value),
                'permission_read': r, 'permission_write': w}]})
            if '"acks"' in out:
                written += 1
            else:
                failed.append(f'{name}-r{r}w{w}: {out[:120]}')
    print(f'storage        写入 {written} / {len(CASES) * len(PERMS)} 条')

    if dsn_cmd:
        for label, (ok, fs) in [
            ('leaderboard_record', seed_leaderboard_records(server_key, dsn_cmd)),
            ('user_edge', seed_friends(server_key)),
            ('user_tombstone', seed_tombstones(server_key, dsn_cmd)),
        ]:
            print(f'{label:14} 写入 {ok} 条')
            failed += [f'{label}: {x}' for x in fs]
    else:
        print('（未给 --dsn-cmd，跳过 leaderboard_record / user_edge / user_tombstone）')

    for f in failed:
        print(f'  🔴 {f}')
    # 🔴 写不进去本身就是发现（比如某个形态超了限制），不要静默吞掉
    return 0 if not failed else 1


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('server_key')
    ap.add_argument('--dsn-cmd', help='取数命令，空格分隔。给了才造那三张关系表')
    a = ap.parse_args()
    raise SystemExit(main(a.server_key, a.dsn_cmd.split() if a.dsn_cmd else None))
