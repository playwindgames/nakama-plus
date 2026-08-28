//go:build lockorderfixture

// 环**上**挂着 Update()：递归链绕一圈之后到达 Update()。
// 期望：退出 1 —— 环不能让传播提前收敛、把真命中吞掉
//
// ok-recursion 验的是「有环也要终止」，这条验的是「有环也不能漏报」。
// 两条缺一不可：只写前者的话，一个「遇环就整段放弃」的实现也能跑绿。

package fixture

import "sync"

type LeaderboardScheduler interface{ Update() }

type holder struct {
	mu                   sync.RWMutex
	leaderboardScheduler LeaderboardScheduler
}

type node struct{ next *node }

func (n *node) ping(s LeaderboardScheduler, d int) {
	if d > 0 {
		n.pong(s, d-1)
	}
	s.Update() // ← 环上就挂着它
}

func (n *node) pong(s LeaderboardScheduler, d int) {
	if d > 0 {
		n.ping(s, d-1)
	}
}

func (h *holder) sweep(n *node) {
	h.mu.Lock()
	n.pong(h.leaderboardScheduler, 3) // 🔴 pong → ping → Update()
	h.mu.Unlock()
}
