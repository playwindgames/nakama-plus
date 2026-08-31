"""种子数据 —— 走真实游戏 API 造数据，覆盖边界值。

🔵 走 API 而不直接 INSERT：直接插的数据不经过序列化路径，
   而**序列化差异正是升级最可能出问题的地方**。

⚠️ 不做「重」档（好友/公会/排行榜这类有业务语义的关联数据）——
   e2e 本来就会产生，手工再造是重复劳动。
"""
import json
import subprocess
import sys

API = 'http://127.0.0.1:7350'


def curl(method, path, token=None, key=None, body=None):
    cmd = ['curl', '-s', '-X', method, f'{API}{path}',
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


def main(server_key):
    written, failed = 0, []
    for i, (name, value) in enumerate(CASES):
        raw = curl('POST', '/v2/account/authenticate/device?create=true',
                   key=server_key, body={'id': f'seed-device-{name}-{i:04d}'})
        try:
            tok = json.loads(raw)['token']
        except (KeyError, json.JSONDecodeError):
            failed.append(f'{name}: 认证失败 -> {raw[:120]}')
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
    print(f'写入 {written} / {len(CASES) * len(PERMS)} 条')
    for f in failed:
        print(f'  🔴 {f}')
    # 🔴 写不进去本身就是发现（比如 50KB 超了某个限制），不要静默吞掉
    return 0 if not failed else 1


if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1]))
