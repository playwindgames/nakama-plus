from check import snapshot


def test_snapshot_returns_sorted_rows():
    # 用 python -c 当假取数命令：它忽略追加的 -e 参数，稳定输出乱序 CSV
    fake = ['python3', '-c', 'print("c,k"); print("b,2"); print("a,1")']
    got = snapshot(fake, ['t'])
    assert got['t'] == [['a', '1'], ['b', '2']], got
