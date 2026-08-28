//go:build lockorderfixture

// 分支内取锁、分支内调用。
// 期望：退出 1，1 处违规

package fixture

import "sync"

type LeaderboardScheduler interface{ Update() }

type holder struct {
	mu                   sync.RWMutex
	leaderboardScheduler LeaderboardScheduler
}

func (h *holder) refresh(force bool) {
	if force {
		h.mu.Lock()
		h.leaderboardScheduler.Update() // 🔴 持 h.mu
		h.mu.Unlock()
	}
}
