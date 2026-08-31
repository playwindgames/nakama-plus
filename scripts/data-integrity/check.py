"""升级数据完整性检查器。

手工调起的验证套件，**不进 CI**（见 spec D9）。
spec: nkmfd-backend/docs/superpowers/specs/2026-08-31-upgrade-data-integrity-design.md
"""
import csv
import io
import subprocess


def _run(cmd, sql=None):
    """cmd 是取数命令的前缀列表；sql 追加为 -e 参数。返回 stdout。"""
    full = list(cmd) + (['-e', sql] if sql else [])
    r = subprocess.run(full, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"取数失败: {' '.join(full[:3])}… -> {r.stderr[:200]}")
    return r.stdout


def snapshot(dsn_cmd, tables):
    """取每张表的全部行，排序后返回。dsn_cmd 例：
       ['docker','exec','nkmad-cockroachdb','./cockroach','sql','--insecure',
        '-d','nkmad_local','--format=csv']

    为什么排序：CockroachDB 不保证 SELECT 的行序 —— 不排序的话同一份数据
    两次取出可能顺序不同，比对会假红。

    为什么 SELECT *：列举列会在 nakama 加列时静默漏检（新列的变化看不见）。
    代价是列顺序依赖 schema —— 但 schema 变化本身该被 D6 的「无越界修改」抓住。
    """
    out = {}
    for t in tables:
        raw = _run(dsn_cmd, f'SELECT * FROM {t};')
        rows = list(csv.reader(io.StringIO(raw)))
        out[t] = sorted(rows[1:]) if rows else []   # 去表头
    return out
