//go:build lockorderfixture

// 互相递归 + 自递归，且**都不**到达 Update()。
// 期望：退出 0，且**必须终止** —— 可达性传播里有环，写不好就是死循环。

package fixture

import "sync"

type LeaderboardScheduler interface{ Update() }

type holder struct {
	mu                   sync.RWMutex
	leaderboardScheduler LeaderboardScheduler
}

type node struct{ next *node }

// 互相递归
func (n *node) ping(d int) {
	if d > 0 {
		n.pong(d - 1)
	}
}

func (n *node) pong(d int) {
	if d > 0 {
		n.ping(d - 1)
	}
}

// 自递归
func (n *node) walk(d int) {
	if d > 0 {
		n.walk(d - 1)
	}
}

func (h *holder) sweep(n *node) {
	h.mu.Lock()
	n.ping(3) // 环里绕，但绕不到 Update()
	n.walk(3)
	h.mu.Unlock()

	h.leaderboardScheduler.Update() // 锁已释放
}
