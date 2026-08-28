//go:build lockorderfixture

// 自检：一个调用点都找不到时必须失败，而不是绿灯。
// 期望：退出 1（找不到 ≠ 没问题）
//
// 模拟的是最阴险的失效方式：调度器类型被改名、或路径传错 ——
// 检查器扫了个寂寞，退出码却是 0。这跟「代码没问题」长得一模一样。

package fixture

import "sync"

type unrelated struct{ mu sync.Mutex }

func (u *unrelated) touch() {
	u.mu.Lock()
	u.mu.Unlock()
}
