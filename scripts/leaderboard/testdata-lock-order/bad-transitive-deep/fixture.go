//go:build lockorderfixture

// 跨函数**三层**：持锁 → a → b → c → Update()。
// 期望：退出 1，路径完整打印出来
//
// 这是升级前看不见的那一类（旧版只做一层）。

package fixture

import "sync"

type LeaderboardScheduler interface{ Update() }

type holder struct {
	mu                   sync.RWMutex
	leaderboardScheduler LeaderboardScheduler
}

type deep struct{}

func (d *deep) a(s LeaderboardScheduler) { d.b(s) }
func (d *deep) b(s LeaderboardScheduler) { d.c(s) }
func (d *deep) c(s LeaderboardScheduler) { s.Update() }

func (h *holder) sweep(d *deep) {
	h.mu.Lock()
	d.a(h.leaderboardScheduler) // 🔴 三层之外就是 Update()
	h.mu.Unlock()
}
